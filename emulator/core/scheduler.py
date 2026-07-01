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

* ``wall`` (default) — transitions track real wall-clock time, so a
  FireCREST poll loop watches PENDING -> RUNNING -> COMPLETED without
  anyone advancing the emulator clock. Delays are short by default.
* ``time`` — transitions track the emulator's simulated clock, advanced
  via the time-travel API/CLI. Deterministic for scenario tests.

The accounting record is always stamped on the *simulated* clock so the
existing sacct default query window (midnight..now) still finds it,
independent of which lifecycle clock is in use.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine


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


def advance_job_states(database: SlurmDatabase, time_engine: TimeEngine) -> bool:
    """Advance every job to the state its elapsed time implies.

    Returns True if anything changed, so the caller can persist once.
    Cancelled/failed jobs are left untouched.
    """
    changed = False
    job_now = job_clock_now(time_engine)
    sim_now = time_engine.get_current_time()
    run_delay = _run_delay()
    run_duration = _run_duration()

    for job in list(database.jobs.values()):
        if (
            job.state == "PENDING"
            and job.submit_time is not None
            and (job_now - job.submit_time).total_seconds() >= run_delay
        ):
            job.state = "RUNNING"
            job.start_time = job.submit_time + timedelta(seconds=run_delay)
            changed = True
        if (
            job.state == "RUNNING"
            and job.start_time is not None
            and (job_now - job.start_time).total_seconds() >= run_duration
        ):
            job.state = "COMPLETED"
            job.end_time = job.start_time + timedelta(seconds=run_duration)
            changed = True
        if job.state == "COMPLETED" and _ensure_usage_record(database, job, sim_now):
            changed = True

    return changed


def _ensure_usage_record(database: SlurmDatabase, job, sim_now: datetime) -> bool:
    """Mirror a completed job into accounting exactly once."""
    if not str(job.job_id).isdigit():
        return False
    jid = int(job.job_id)
    if any(r.job_id == jid for r in database.usage_records):
        return False

    elapsed = 0.0
    if job.start_time and job.end_time:
        elapsed = (job.end_time - job.start_time).total_seconds()
    node_count = getattr(job, "node_count", 1) or 1
    node_hours = (elapsed / 3600.0) * node_count

    database.usage_records.append(
        UsageRecord(
            account=job.account,
            user=job.user,
            node_hours=node_hours,
            billing_units=node_hours,
            timestamp=sim_now,
            period=_period_for(sim_now),
            raw_tres={},
            cluster=job.cluster,
            job_id=jid,
            state="COMPLETED",
        )
    )
    return True
