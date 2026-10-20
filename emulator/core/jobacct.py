"""Job accounting reads: which job records a query returns, and with which times.

A port of slurmdbd's job query (slurm://src/plugins/accounting_storage/mysql/as_mysql_jobacct_process.c#_cluster_get_jobs),
shared by ``sacct`` and slurmrestd ``GET /slurmdb/{v}/jobs`` so both answer a
query the same way:

* **Window** -- a job is in ``[usage_start, usage_end)`` when it became
  eligible before ``usage_end`` and has not ended before ``usage_start``; a
  query for explicit job ids compares the submit time instead of the
  eligible time
  (slurm://src/plugins/accounting_storage/mysql/as_mysql_jobacct_process.c#setup_job_cond_limits).
  Without ``-S`` the window starts at midnight (at 0 for job ids), without
  ``-E`` it ends now; an end equal to now or to the start is moved one second
  on so the window is not empty (slurm://src/common/slurmdb_defs.c#slurmdb_job_cond_def_start_end).
* **Duplicates** -- a job ID can have several records (one per run of a
  requeued job, each with its own submit time). Unless duplicates are asked
  for, only the most recently submitted record of each job is returned, plus
  any left in ``RESIZING``, and no ``REVOKED`` federated siblings. Records
  come back ordered by job ID, except that a job's earlier records are moved
  to the front of the whole list.
* **Truncation** -- unless disabled, each job's start and end are clipped to
  the window, the window never extends past now, Elapsed is recomputed from
  the clipped times and suspend intervals inside the window are taken off.
  Without truncation Elapsed is the job's run time less its suspended time.

``sacct`` disables truncation unless ``--truncate`` is given
(slurm://src/sacct/options.c#_init_params) and returns duplicates only with
``--duplicates``. slurmrestd sets both ``show_duplicates`` and
``disable_truncate_usage_time`` when a query carries no job-condition flag at
all (slurm://src/slurmrestd/plugins/openapi/slurmdbd/jobs.c#op_handler_jobs).

Intentional deviation: the emulator has two clocks -- records of submitted
jobs are stamped on the job clock, injected usage on the simulated clock (see
``emulator/core/scheduler.py``). "Now" for truncation is the later of the two,
so neither kind of record is clipped against a clock it was not stamped on.
With ``SLURM_EMULATOR_JOB_CLOCK=time`` (or ``SLURM_EMULATOR_TIME_FOLLOW_WALL``)
there is only one clock and this matches slurmdbd exactly.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from emulator.core.database import Job, UsageRecord


@dataclass(frozen=True)
class JobCond:
    """The time and flag part of a slurmdb job condition, in epoch seconds (0 = unset).

    slurm://slurm/slurmdb.h#slurmdb_job_cond_t.usage_start,
    slurm://slurm/slurmdb.h#slurmdb_job_cond_t.usage_end,
    slurm://slurm/slurmdb.h#JOBCOND_FLAG_DUP,
    slurm://slurm/slurmdb.h#JOBCOND_FLAG_NO_TRUNC
    """

    usage_start: int = 0
    usage_end: int = 0
    duplicates: bool = False
    truncate: bool = False
    # The query names job ids (sacct -j): slurm://slurm/slurmdb.h#slurmdb_job_cond_t.step_list
    job_ids: bool = False


def default_window(start: int, end: int, now: int, midnight: int, job_ids: bool) -> tuple[int, int]:
    """``-S``/``-E`` as slurmdbd fills them in (0 = not given).

    slurm://src/common/slurmdb_defs.c#slurmdb_job_cond_def_start_end
    """
    if not start and not job_ids:
        start = midnight
    if not end:
        end = now
    if end in (start, now):
        end += 1
    return start, end


@dataclass(frozen=True)
class JobTimes:
    """The times slurmdbd returns for one job record, in epoch seconds (0 = unset).

    slurm://slurm/slurmdb.h#slurmdb_job_rec_t.submit,
    slurm://slurm/slurmdb.h#slurmdb_job_rec_t.eligible,
    slurm://slurm/slurmdb.h#slurmdb_job_rec_t.start,
    slurm://slurm/slurmdb.h#slurmdb_job_rec_t.end,
    slurm://slurm/slurmdb.h#slurmdb_job_rec_t.elapsed,
    slurm://slurm/slurmdb.h#slurmdb_job_rec_t.suspended
    """

    submit: int
    eligible: int
    start: int
    end: int
    elapsed: int
    suspended: int


def epoch(value: Optional[datetime]) -> int:
    """A naive local datetime as epoch seconds; None is 0, slurmdbd's "unset"."""
    return int(value.timestamp()) if value is not None else 0


def stored_times(
    record: UsageRecord, active: Optional[Job], now: int, job_now: Optional[int] = None
) -> JobTimes:
    """What slurmdbd returns for a record when the query does not truncate.

    ``now`` is the read time; ``job_now`` the job clock a running job's
    elapsed time is counted on (defaults to ``now``).
    """
    intervals = suspend_intervals(record, active)
    if active is not None:
        running = active.state in ("RUNNING", "SUSPENDED") and bool(active.node_list)
        submit = epoch(active.submit_time)
        start = epoch(active.start_time) if running else 0
        end = 0
    elif record.start_time is not None and record.end_time is not None:
        submit = epoch(record.submit_time or record.start_time)
        start = epoch(record.start_time)
        end = epoch(record.end_time)
    else:
        # Injected usage: node_hours of run time ending at the record timestamp.
        end = epoch(record.timestamp)
        start = submit = end - int(record.node_hours * 3600)
    if active is not None:
        eligible = job_eligible(active)
    else:
        eligible = epoch(record.eligible_time) if record.eligible_time else submit
    if end and start > end:
        start = end
    # slurm://src/plugins/accounting_storage/mysql/as_mysql_jobacct_process.c#_cluster_get_jobs:
    # without truncation the job's whole suspended time comes off its run time.
    clock = job_now if job_now is not None else now
    suspended = sum((e or clock) - s for s, e in intervals)
    if not start:
        elapsed = 0
    elif not end:
        elapsed = clock - start
    else:
        elapsed = end - start
    elapsed = max(elapsed - suspended, 0)
    return JobTimes(submit, eligible, start, end, elapsed, suspended)


def job_eligible(job: Job) -> int:
    """When a job in the controller became (or becomes) eligible to run.

    A held job is not eligible at all; a requeued one only from its begin
    time (slurm://slurm/slurm.h#job_info_t.eligible_time). sacct and
    slurmrestd leave out jobs with no eligible time unless asked for them
    by id, so a held job does not show up in a time window.
    """
    if job.priority == 0:
        return 0
    return max(epoch(job.submit_time), epoch(job.begin_time))


def suspend_intervals(record: UsageRecord, active: Optional[Job]) -> list[tuple[int, int]]:
    """Suspend intervals as (start, end) epochs; end 0 while still suspended."""
    source = active.suspend_intervals if active is not None else record.suspend_intervals
    return [(int(s), int(e)) for s, e in source]


def in_window(times: JobTimes, cond: JobCond) -> bool:
    """slurm://src/plugins/accounting_storage/mysql/as_mysql_jobacct_process.c#setup_job_cond_limits."""
    ended_in_or_after = times.end >= cond.usage_start or times.end == 0
    if cond.job_ids:
        return times.submit < cond.usage_end and ended_in_or_after
    if cond.usage_start:
        if not cond.usage_end:
            return ended_in_or_after
        return bool(times.eligible) and times.eligible < cond.usage_end and ended_in_or_after
    if cond.usage_end:
        return bool(times.eligible) and times.eligible < cond.usage_end
    return True


def truncate(
    times: JobTimes, intervals: list[tuple[int, int]], cond: JobCond, now: int
) -> JobTimes:
    """Clip a job to the window, as slurmdbd does unless JOBCOND_FLAG_NO_TRUNC is set.

    slurm://src/plugins/accounting_storage/mysql/as_mysql_jobacct_process.c#_cluster_get_jobs.
    Only closed suspend intervals inside the clipped run are taken off: an
    interval still open has no end, and slurmdbd's ``local_end`` stays 0.
    """
    usage_end = now if not cond.usage_end or cond.usage_end > now else cond.usage_end
    start, end = times.start, times.end
    if start and start < cond.usage_start:
        start = cond.usage_start
    if not end or end > usage_end:
        end = usage_end
    if not start:
        start = end
    elapsed = end - start
    suspended = 0
    for s, e in intervals:
        if not (s < usage_end and (e >= cond.usage_start or e == 0)):
            continue
        local_start, local_end = max(s, start), min(e, end)
        if local_end - local_start < 1:
            continue
        elapsed -= local_end - local_start
        suspended += local_end - local_start
    return replace(times, start=start, end=end, elapsed=max(elapsed, 0), suspended=suspended)


def query(
    candidates: list[tuple[UsageRecord, Optional[Job]]],
    cond: JobCond,
    now: int,
    job_now: Optional[int] = None,
) -> list[tuple[UsageRecord, Optional[Job], JobTimes]]:
    """Records the query returns, in slurmdbd's order, with the times it reports."""
    rows = []
    for record, active in candidates:
        if not cond.duplicates and (record.state or "COMPLETED") == "REVOKED":
            continue
        times = stored_times(record, active, now, job_now)
        if in_window(times, cond):
            rows.append((record, active, times))
    # "order by id_job, time_submit desc"; a further record of the job just
    # seen is list_prepend()ed -- to the front of the whole list, not of its
    # job (slurm://src/plugins/accounting_storage/mysql/as_mysql_jobacct_process.c#_cluster_get_jobs).
    rows.sort(key=lambda row: (int(row[0].job_id or 0), -row[2].submit))
    selected: list[tuple[UsageRecord, Optional[Job], JobTimes]] = []
    last_id: Optional[int] = None
    for row in rows:
        job_id = int(row[0].job_id or 0)
        if job_id == last_id:
            if not cond.duplicates and (row[0].state or "COMPLETED") != "RESIZING":
                continue
            selected.insert(0, row)
        else:
            selected.append(row)
        last_id = job_id
    if not cond.truncate:
        return selected
    return [
        (record, active, truncate(times, suspend_intervals(record, active), cond, now))
        for record, active, times in selected
    ]
