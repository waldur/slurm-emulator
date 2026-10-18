"""Lazy job lifecycle engine for submitted jobs.

Real Slurm advances a job PENDING -> RUNNING -> COMPLETED on its own.
The emulator has no daemon loop, and its REST server, SSH server and
control API run as separate processes over one JSON state file, so a
background task would need cross-process coordination. Instead state is
advanced *lazily on read*: every job list/get handler calls
``advance_job_states`` first, which derives the current state from the
elapsed time since submission and, on completion, emits a UsageRecord so
the job also shows up in the accounting (slurmdb / sacct) view.

Two clocks, selected by ``SLURM_EMULATOR_JOB_CLOCK``:

* ``wall`` (default) — transitions track real wall-clock time, so a client
  polling job status watches PENDING -> RUNNING -> COMPLETED without anyone
  advancing the emulator clock. Delays are short by default.
* ``time`` — transitions track the emulator's simulated clock, advanced
  via the time-travel API/CLI. Deterministic for scenario tests.

The accounting record is always stamped on the *simulated* clock so the
existing sacct default query window (midnight..now) still finds it,
independent of which lifecycle clock is in use.

Scheduling is a small discrete-event simulation over the job clock: at each
event (a job becomes eligible, a running job ends, background load rotates,
a reservation starts or ends) pending jobs are tried FIFO against the free
nodes (``emulator/core/cluster_model.py``). A job that does not fit stays
PENDING with the reason slurmctld would give (``Resources`` …) and starts
when nodes free up, which produces genuine queue waits. A job runs for its
``SLURM_EMULATOR_RUN_SECONDS`` (job environment) or
``SLURM_EMULATOR_JOB_RUN_DURATION``; one that would outlive its
``time_limit`` is ended TIMEOUT at the limit, as slurmctld's time-limit
check does (slurm://src/slurmctld/job_mgr.c#_job_timed_out).
``database.sched_cursor`` remembers how far the simulation has run, so a
past scheduling decision a client already saw is never revisited.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Optional

from emulator.core import cluster_model, exit_status, job_execution
from emulator.core.database import Job, SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import standard_node_raw_tres


def _clock_mode() -> str:
    return os.environ.get("SLURM_EMULATOR_JOB_CLOCK", "wall").strip().lower()


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _run_delay() -> float:
    """Seconds from submit until a job starts running."""
    return _float_env("SLURM_EMULATOR_JOB_RUN_DELAY", 2.0)


def _run_duration() -> float:
    """Seconds a job spends RUNNING before it completes."""
    return _float_env("SLURM_EMULATOR_JOB_RUN_DURATION", 8.0)


def job_clock_now(time_engine: TimeEngine) -> datetime:
    """The clock the lifecycle transitions are measured against.

    Also used by the submit handler to stamp ``submit_time`` so both ends
    agree on the same clock.
    """
    if _clock_mode() == "time":
        return time_engine.get_current_time()
    return datetime.now()  # naive local, matches the emulator's naive clock


def _period_for(ts: datetime) -> str:
    return f"{ts.year}-Q{(ts.month - 1) // 3 + 1}"


_MAX_EVENTS = 20000
_FINISHED = ("COMPLETED", "FAILED", "TIMEOUT", "CANCELLED")


def _limit_seconds(job: Job) -> Optional[float]:
    return job.time_limit * 60.0 if job.time_limit else None


def _planned(job: Job, run_duration: float) -> tuple[float, bool]:
    """(seconds the job runs, whether it ends by hitting its time limit)."""
    run = cluster_model.job_run_seconds(job, run_duration)
    limit = _limit_seconds(job)
    if limit is not None and run > limit:
        return limit, True
    return run, False


def expected_end(job: Job) -> Optional[datetime]:
    """When a RUNNING job's allocation ends, as slurmctld sees it.

    slurmctld sets a running job's ``end_time`` to start + time limit
    (slurm://src/slurmctld/job_mgr.c#_job_timed_out); a job without a
    limit is taken at its emulated run length, as the scheduler does.
    """
    if job.start_time is None:
        return None
    seconds = _limit_seconds(job) or _planned(job, _run_duration())[0]
    return job.start_time + timedelta(seconds=seconds)


def _queue_order(job: Job) -> tuple:
    jid = int(job.job_id) if str(job.job_id).isdigit() else 0
    return (job.submit_time.timestamp() if job.submit_time else 0.0, jid)


def _finish(job: Job, run_duration: float) -> None:
    """End a RUNNING job at its planned end (COMPLETED / FAILED / TIMEOUT)."""
    seconds, timed_out = _planned(job, run_duration)
    assert job.start_time is not None  # noqa: S101 - only RUNNING jobs, which have one
    job.end_time = job.start_time + timedelta(seconds=seconds)
    job.state = "TIMEOUT" if timed_out else "COMPLETED"
    if job_execution.execution_enabled():
        # The script runs once, at the end of the emulated run. A failing
        # script -- non-zero exit or killed by a signal -- ends the job
        # FAILED with its status (slurm://src/slurmctld/job_mgr.c#_job_complete);
        # a timed-out job stays TIMEOUT whatever the script returned.
        code = job_execution.execute_once(job)
        if not timed_out:
            job.exit_code = code
            if code != 0:
                job.state = "FAILED"
    if timed_out:
        # The batch script is sent SIGTERM at the limit
        # (slurm://src/slurmctld/job_mgr.c#_job_timed_out).
        job.exit_code = -exit_status.TIME_LIMIT_SIGNAL
    job.state_reason, job.state_description = exit_status.failure_reason(job.state, job.exit_code)


def _set_reason(job: Job, reason: str, description: str) -> bool:
    if (job.state_reason, job.state_description) == (reason, description):
        return False
    job.state_reason, job.state_description = reason, description
    return True


def advance_job_states(database: SlurmDatabase, time_engine: TimeEngine) -> bool:
    """Advance every job to the state its elapsed time implies.

    Returns True if anything changed, so the caller can persist once.
    Cancelled/failed jobs are left untouched.
    """
    changed = False
    job_now = job_clock_now(time_engine)
    sim_now = time_engine.get_current_time()
    run_delay = timedelta(seconds=_run_delay())
    run_duration = _run_duration()

    if cluster_model.sync_reservations(database, job_now):
        changed = True
    overrides = cluster_model.node_state_overrides()
    background = bool(cluster_model.background_fractions())
    cursor = database.sched_cursor

    def eligible(job: Job) -> datetime:
        assert job.submit_time is not None  # noqa: S101 - pending() requires it
        at = job.submit_time + run_delay
        return max(at, cursor) if cursor is not None else at

    def running() -> list[Job]:
        return [j for j in database.jobs.values() if j.state == "RUNNING" and j.start_time]

    def pending() -> list[Job]:
        # A held job (priority 0) is not considered
        # (slurm://src/slurmctld/job_scheduler.c#_job_runnable_test1).
        return sorted(
            (
                j
                for j in database.jobs.values()
                if j.state == "PENDING" and j.submit_time and j.priority != 0
            ),
            key=_queue_order,
        )

    def end_of(job: Job) -> datetime:
        assert job.start_time is not None  # noqa: S101 - running() requires it
        return job.start_time + timedelta(seconds=_planned(job, run_duration)[0])

    candidates = [end_of(j) for j in running()] + [eligible(j) for j in pending()]
    t = min(candidates) if candidates else None
    for _ in range(_MAX_EVENTS):
        if t is None or t > job_now:
            break
        # Reservations that ended by now are gone before this pass.
        if cluster_model.purge_ended_reservations(database, t):
            changed = True
        # Ends and starts at the same instant feed each other (a zero-length
        # run, freed nodes taken at once), so repeat until nothing moves.
        progressed = True
        while progressed:
            progressed = False
            for job in sorted(running(), key=end_of):
                if end_of(job) <= t:
                    _finish(job, run_duration)
                    progressed = changed = True
            blocked = False
            usage = None
            # Once a job cannot start for want of resources, the rest of the
            # pass skips its partition (or its reservation) and those jobs
            # wait with reason Priority; nodes of a failed partition are not
            # handed to other partitions either
            # (slurm://src/slurmctld/job_scheduler.c#_schedule).
            failed_parts: set[str] = set()
            failed_resvs: set[str] = set()
            unavailable: set[str] = set()
            for job in pending():
                if eligible(job) > t:
                    continue
                skipped = (
                    job.reservation in failed_resvs
                    if job.reservation
                    else job.partition in failed_parts
                )
                if skipped:
                    blocked = True
                    if job.reservation or job.state_reason in ("None", "Resources"):
                        changed = _set_reason(job, "Priority", "") or changed
                    continue
                duration = _limit_seconds(job) or _planned(job, run_duration)[0]
                if usage is None:
                    usage = cluster_model.occupancy(database, t, overrides)
                placement = cluster_model.try_allocate(
                    database, job, t, duration, usage, overrides, unavailable
                )
                if placement.nodes is None:
                    blocked = True
                    changed = _set_reason(job, placement.reason, placement.description) or changed
                    if placement.reason == "Resources":
                        if job.reservation:
                            failed_resvs.add(job.reservation)
                        else:
                            failed_parts.add(job.partition)
                            unavailable.update(cluster_model.partition_nodes(job.partition))
                    continue
                cluster_model.allocate(job, placement.nodes)
                job.state = "RUNNING"
                job.state_reason = "None"
                job.state_description = ""
                job.start_time = t
                usage = None  # occupancy changed
                progressed = changed = True

        future = [e for e in (end_of(j) for j in running()) if e > t]
        future += [e for e in (eligible(j) for j in pending()) if e > t]
        if blocked:
            if background:
                future.append(cluster_model.next_background_boundary(t))
            for resv in database.reservations.values():
                future += [b for b in (resv.start_time, resv.end_time) if b and b > t]
        t = min(future) if future else None

    if cluster_model.purge_ended_reservations(database, job_now):
        changed = True
    if pending() and (cursor is None or (job_now - cursor).total_seconds() >= 1):
        database.sched_cursor = job_now
        changed = True

    for job in list(database.jobs.values()):
        if job.state in _FINISHED and job.end_time and _ensure_usage_record(database, job, sim_now):
            changed = True

    return changed


def _ensure_usage_record(database: SlurmDatabase, job, sim_now: datetime) -> bool:
    """Mirror a finished job into accounting exactly once.

    Besides the TRES totals the record keeps what slurmdbd stores per job
    (slurm://slurm/slurmdb.h#slurmdb_job_rec_t): submit/start/end, time
    limit, allocated nodes and CPUs, requested memory, CPU time and the batch
    step's MaxRSS. CPU time and MaxRSS are synthetic -- see
    ``cluster_model.synthetic_efficiency``.
    """
    if not str(job.job_id).isdigit():
        return False
    jid = int(job.job_id)
    if any(r.job_id == jid for r in database.usage_records):
        return False

    elapsed = 0.0
    if job.start_time and job.end_time:
        elapsed = max((job.end_time - job.start_time).total_seconds(), 0.0)
    node_count = len(job.node_list) or getattr(job, "node_count", 1) or 1
    node_hours = (elapsed / 3600.0) * node_count
    # A job cancelled while pending never started: no nodes, start = end,
    # and NodeList "None assigned"
    # (slurm://src/plugins/accounting_storage/mysql/as_mysql_job.c#"None assigned").
    started = job.start_time is not None
    if job.node_list:
        node_list = cluster_model.compress_hostlist(job.node_list)
    else:
        node_list = "node001" if started else "None assigned"
    req = cluster_model.job_request(job)
    alloc_cpus = job.alloc_cpus or req.total_cpus
    mem_per_node = req.mem_per_node
    cpu_eff, mem_frac = cluster_model.job_efficiency(job)

    database.usage_records.append(
        UsageRecord(
            account=job.account,
            user=job.user,
            node_hours=node_hours,
            billing_units=node_hours,
            timestamp=sim_now,
            period=_period_for(sim_now),
            # Jobs run on standard nodes (no GRES model yet): the same
            # CPU/Mem/GPU breakdown and power-model energy as injected usage.
            raw_tres=standard_node_raw_tres(node_hours, job.partition),
            cluster=job.cluster,
            partition=job.partition,
            job_id=jid,
            state=job.state,
            uid=getattr(job, "uid", None),
            gid=getattr(job, "gid", None),
            group_name=getattr(job, "group_name", ""),
            job_name=job.name or "",
            working_directory=job.working_directory or "",
            # Accounting logs a timed-out job when the limit fires, before
            # the script is signalled (slurm://src/slurmctld/job_mgr.c#_job_timed_out).
            exit_code=0 if job.state == "TIMEOUT" else job.exit_code,
            requid=job.requid,
            standard_input=job.standard_input or "",
            standard_output=job.standard_output or "",
            standard_error=job.standard_error or "",
            submit_time=job.submit_time,
            eligible_time=job.submit_time,
            start_time=job.start_time if started else job.end_time,
            end_time=job.end_time,
            time_limit=job.time_limit,
            node_count=node_count if started else 0,
            node_list=node_list,
            alloc_cpus=alloc_cpus if started else 0,
            req_mem_mb=mem_per_node,
            cpu_time_seconds=elapsed * alloc_cpus * cpu_eff if started else 0.0,
            max_rss_bytes=cluster_model.max_rss_bytes(mem_frac, mem_per_node) if started else 0,
            tasks=job.tasks,
        )
    )
    return True
