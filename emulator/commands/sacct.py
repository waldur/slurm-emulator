"""sacct command emulator for usage reporting.

Output formatting and exit codes mirror real Slurm 26.05:

* default fields are ``JobID,JobName,Partition,Account,AllocCPUS,
  State,ExitCode`` (slurm://src/sacct/sacct.h#DEFAULT_FIELDS) with the widths from the
  field table in slurm://src/sacct/sacct.c#fields (negative = left-aligned);
* header + dashed underline, ``-p``/``-P``/``-n`` parsable/noheader
  modes, and ``value[:width-1]+'+'`` truncation come from the shared
  print_fields module (src/common/print_fields.c semantics);
* ``Elapsed`` renders ``[D-]HH:MM:SS`` (``secs2time_str``,
  slurm://src/common/parse_time.c#secs2time_str);
* an invalid time spec prints ``Invalid time specification (pos=N):
  <str>`` to stderr — no ``sacct:`` prefix, slurm://src/common/parse_time.c#parse_time — and
  exits 1; an unknown format field prints ``sacct: error: Invalid field
  requested: "X"`` and exits 1 (slurm://src/sacct/options.c#parse_command_line);
* without ``-S`` the window starts at midnight on the simulated clock,
  without ``-E`` it stays open (slurm://src/common/slurmdb_defs.c#slurmdb_job_cond_def_start_end);
  which records a window returns, ``--truncate`` (``-T``) and
  ``--duplicates`` (``-D``) follow slurmdbd's job query -- see
  ``emulator/core/jobacct.py``. Rows are ordered by submit time, then job id
  (slurm://src/sacct/options.c#_sort_desc_submit_time); ``Suspended`` is the
  suspended time the query reports;
* one job row per usage record: numeric JobID from the database,
  standard TRES string (``cpu=...,mem=...G,node=1,billing=...``) in
  TRES-id order — the emulator-internal ``node-hours`` key is not
  exposed;
* ``ConsumedEnergyRaw`` is the job's ``energy`` TRES in joules
  (``slurmdb_find_tres_count_in_string(job->tres_alloc_str, TRES_ENERGY)``,
  slurm://src/sacct/print.c#PRINT_CONSUMED_ENERGY_RAW), ``ConsumedEnergy``
  the same value through ``convert_num_unit2(…, 1000, …)`` — aggressive
  K/M/G scaling, ``--noconvert`` keeps the raw number
  (slurm://src/common/slurm_protocol_api.c#convert_num_unit2). The value is
  ``raw_tres["energy"]`` from the power model (``emulator/core/energy.py``).
  The ``energy`` TRES is deliberately *not* added to the ReqTRES/AllocTRES
  string so the site-agent invocation shape stays byte-identical.

* ``StdIn``/``StdOut``/``StdErr`` print the paths as stored, patterns not
  expanded (slurm://src/sacct/print.c#PRINT_STDIN, ``PRINT_STDOUT``,
  ``PRINT_STDERR``; ``--expand-patterns`` is not emulated); ``WorkDir`` the
  job's working directory (slurm://src/sacct/print.c#PRINT_WORK_DIR);
  ``Eligible`` the eligible time (slurm://src/sacct/print.c#PRINT_ELIGIBLE);
  ``ReqMem`` the requested memory over the job's nodes
  (slurm://src/sacct/print.c#PRINT_REQ_MEM); ``UserCPU``/``SystemCPU`` the
  CPU time split (slurm://src/sacct/print.c#PRINT_USERCPU,
  ``PRINT_SYSTEMCPU``). The split itself is synthetic: 4% of a job's CPU time
  is counted as system time.
* a pending or running job has a row from submission on, with ``Unknown``
  for times not reached yet and no CPU time or MaxRSS until it ends; a
  cancelled job's state names the uid that cancelled it
  (slurm://src/sacct/print.c#PRINT_STATE).

Documented simplifications: ``-X``/``--allocations`` is a no-op (the
emulator has no job steps, and real ``-X`` only filters step rows);
``-a``/``--allusers`` is a no-op (every caller sees every job — the
emulator never compares the caller's uid to the job's); injected usage
records are single-node (``node001``), while records of submitted or
seeded jobs carry their real node list, CPUs, time limit, submit/eligible
times, CPU time and MaxRSS. There are no step rows: ``MaxRSS`` (a step
field in real sacct, slurm://src/sacct/print.c#PRINT_MAXRSS) prints on the
job row from its batch step, and ``TotalCPU`` is the job total. ``UID``/``GID``/``Group`` print the identity recorded at
submission (slurm://src/sacct/print.c#PRINT_UID, ``PRINT_GID``,
``PRINT_GROUP`` via ``gid_to_string``); without ``SLURM_EMULATOR_NSS``
every job is uid/gid 1000 in a group named after the user.
"""

import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from emulator.commands.print_fields import (
    FieldSpec,
    OutputMode,
    UnknownFieldError,
    parse_format_spec,
    render_table,
    resolve_format,
)
from emulator.commands.slurm_time import ParseTimeError, parse_time_spec
from emulator.core import exit_status, jobacct
from emulator.core.cluster_model import partition_has_gpus
from emulator.core.database import Job, SlurmDatabase, UsageRecord
from emulator.core.jobacct import JobCond, JobTimes, epoch
from emulator.core.time_engine import TimeEngine
from emulator.slurm_version import current

# Subset of the real field table (slurm://src/sacct/sacct.c#fields), in table
# order so prefix matching resolves like slurm://src/sacct/options.c#parse_command_line (first
# match wins, no minimum prefix length).
_REGISTRY: list[FieldSpec] = [
    FieldSpec("Account", 10),
    FieldSpec("AllocCPUS", 10, truncate=False),
    FieldSpec("AllocNodes", 10),
    FieldSpec("AllocTRES", 10),
    FieldSpec("Cluster", 10),
    FieldSpec("ConsumedEnergy", 14),
    FieldSpec("ConsumedEnergyRaw", 17, truncate=False),
    FieldSpec("Elapsed", 10),
    FieldSpec("ElapsedRaw", 10, truncate=False),
    FieldSpec("Eligible", 19),
    FieldSpec("End", 19),
    FieldSpec("ExitCode", 8),
    FieldSpec("GID", 6, truncate=False),
    FieldSpec("Group", 9),
    FieldSpec("JobID", -12),
    FieldSpec("JobIDRaw", -12),
    FieldSpec("JobName", 10),
    FieldSpec("MaxRSS", 10),
    FieldSpec("NNodes", 8, truncate=False),
    FieldSpec("NodeList", 15),
    FieldSpec("Partition", 10),
    FieldSpec("ReqMem", 10),
    FieldSpec("ReqTRES", 10),
    FieldSpec("Start", 19),
    FieldSpec("State", 10),
    FieldSpec("StdErr", 20),
    FieldSpec("StdIn", 20),
    FieldSpec("StdOut", 20),
    FieldSpec("Submit", 19),
    FieldSpec("Suspended", 10),
    FieldSpec("SystemCPU", 10),
    FieldSpec("Timelimit", 10),
    FieldSpec("TotalCPU", 10),
    # UID precedes User in the real table, so ``-o U`` resolves to UID.
    FieldSpec("UID", 6, truncate=False),
    FieldSpec("User", 9),
    FieldSpec("UserCPU", 10),
    FieldSpec("WorkDir", 20),
]

_DEFAULT_FORMAT = "JobID,JobName,Partition,Account,AllocCPUS,State,ExitCode"  # slurm://src/sacct/sacct.h#DEFAULT_FIELDS

# Standard node config used by the usage simulator
# (usage_simulator.py:156-165): fallback rates when a record carries no
# raw_tres breakdown.
_NODE_CPUS = 64
_NODE_MEM_GB = 512
_NODE_GPUS = 4


@dataclass
class _Config:
    accounts: list[str] = field(default_factory=list)
    users: list[str] = field(default_factory=list)
    clusters: list[str] = field(default_factory=list)
    jobs: list[str] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    format_spec: str = _DEFAULT_FORMAT
    mode: OutputMode = field(default_factory=OutputMode)
    noconvert: bool = False
    version: bool = False
    # --truncate / -D: slurmdbd's NO_TRUNC flag cleared, DUP flag set
    # (slurm://src/sacct/options.c#_init_params; see emulator/core/jobacct.py).
    truncate: bool = False
    duplicates: bool = False


class SacctEmulator:
    """Emulates sacct commands for usage reporting."""

    def __init__(self, database: SlurmDatabase, time_engine: TimeEngine):
        self.database = database
        self.time_engine = time_engine
        # Mirrors sacct's exit handling: 0 unless an error path ran.
        self.exit_code = 0
        # Pending/running jobs behind the rows built for them, by id.
        self._active_jobs: dict[str, Job] = {}
        # Times each returned record is reported with, by id(record).
        self.times: dict[int, JobTimes] = {}

    def handle_command(self, args: list[str]) -> str:
        """Process sacct command and return output."""
        self.exit_code = 0
        config = self._parse_args(args)

        if config.version:
            # slurm://src/sacct/options.c#print_slurm_version: "slurm <release>".
            return f"slurm {current().release}"

        try:
            fields = resolve_format(parse_format_spec(config.format_spec), _REGISTRY)
        except UnknownFieldError as e:
            # slurm://src/sacct/options.c#parse_command_line via error(): "sacct: error: ..." on stderr.
            print(f'sacct: error: Invalid field requested: "{e.token}"', file=sys.stderr)
            self.exit_code = 1
            raise SystemExit(1) from None

        # slurmctld keeps slurmdbd current; the emulator catches jobs up on read.
        from emulator.core.scheduler import advance_job_states  # noqa: PLC0415

        advance_job_states(self.database, self.time_engine)
        records = self._get_filtered_records(config)
        # sacct orders what slurmdbd returns by submit time, then job id
        # (slurm://src/sacct/options.c#_sort_desc_submit_time, despite its name ascending).
        records.sort(key=lambda r: (self.times[id(r)].submit, int(r.job_id or 0)))
        rows = [self._row(record, config) for record in records]
        return render_table(fields, rows, config.mode)

    def _parse_args(self, args: list[str]) -> _Config:
        """Parse sacct command line arguments (short and long forms)."""
        cfg = _Config()

        def _csv(value: str) -> list[str]:
            return [item.strip() for item in value.split(",") if item.strip()]

        # (short, long names, attribute or handler key)
        value_opts = {
            "-S": "start",
            "--starttime": "start",
            "-E": "end",
            "--endtime": "end",
            "-A": "accounts",
            "--accounts": "accounts",
            "--account": "accounts",
            "-u": "users",
            "--user": "users",
            "--users": "users",
            "--uid": "users",
            "-M": "clusters",
            "--cluster": "clusters",
            "--clusters": "clusters",
            "-j": "jobs",
            "--jobs": "jobs",
            "--job": "jobs",
            "-o": "format",
            "--format": "format",
            "--fields": "format",
            "--delimiter": "delimiter",
        }
        flag_opts = {
            "-V": "version",
            "--version": "version",
            "-n": "noheader",
            "--noheader": "noheader",
            "-p": "parsable",
            "--parsable": "parsable",
            "-P": "parsable2",
            "--parsable2": "parsable2",
            "-X": "noop",
            "--allocations": "noop",
            "-a": "noop",
            "--allusers": "noop",
            "--noconvert": "noconvert",
            "-T": "truncate",
            "--truncate": "truncate",
            "-D": "duplicates",
            "--duplicates": "duplicates",
            "-b": "noop",
            "--brief": "noop",
        }

        def _apply(key: str, value: str) -> None:
            if key == "start":
                cfg.start_time = self._parse_time(value)
            elif key == "end":
                cfg.end_time = self._parse_time(value)
            elif key == "accounts":
                cfg.accounts.extend(_csv(value))
            elif key == "users":
                cfg.users.extend(_csv(value))
            elif key == "clusters":
                cfg.clusters.extend(_csv(value))
            elif key == "jobs":
                # sacct -j accepts "id" or "id.step"; keep the job id part.
                cfg.jobs.extend(item.split(".", 1)[0] for item in _csv(value))
            elif key == "format":
                cfg.format_spec = value
            elif key == "delimiter":
                cfg.mode.delimiter = value

        i = 0
        while i < len(args):
            arg = args[i]
            if arg in flag_opts:
                action = flag_opts[arg]
                if action == "version":
                    cfg.version = True
                elif action == "noheader":
                    cfg.mode.noheader = True
                elif action == "parsable":
                    cfg.mode.parsable = "p"
                elif action == "parsable2":
                    cfg.mode.parsable = "P"
                elif action == "noconvert":
                    cfg.noconvert = True
                elif action == "truncate":
                    cfg.truncate = True
                elif action == "duplicates":
                    cfg.duplicates = True
                i += 1
                continue
            if "=" in arg and arg.split("=", 1)[0] in value_opts:
                name, value = arg.split("=", 1)
                _apply(value_opts[name], value)
                i += 1
                continue
            if arg in value_opts:
                if i + 1 >= len(args):
                    print(f"sacct: error: missing argument for {arg}", file=sys.stderr)
                    self.exit_code = 1
                    raise SystemExit(1)
                _apply(value_opts[arg], args[i + 1])
                i += 2
                continue
            # Attached short-option value, e.g. -S2024-01-01.
            if len(arg) > 2 and arg[:2] in value_opts and arg[1] != "-":
                _apply(value_opts[arg[:2]], arg[2:])
                i += 1
                continue
            print(f"sacct: error: unrecognized arguments: {arg}", file=sys.stderr)
            self.exit_code = 1
            raise SystemExit(1)

        return cfg

    def _parse_time(self, time_str: str) -> datetime:
        """``parse_time(optarg, 1)`` (slurm://src/sacct/options.c#parse_command_line).

        Times without a date or year resolve to the past, on the simulated
        clock. Failure mirrors slurm://src/common/parse_time.c#parse_time: the
        message goes to stderr without a ``sacct:`` prefix and sacct exits 1.
        """
        try:
            return self._parse_time_inner(time_str)
        except ParseTimeError as exc:
            print(exc, file=sys.stderr)
            self.exit_code = 1
            raise SystemExit(1) from None

    def _parse_time_inner(self, time_str: str) -> datetime:
        """Parse one time spec; raises ParseTimeError on anything bogus."""
        return parse_time_spec(time_str, self.time_engine.get_current_time(), past=True)

    def _get_filtered_records(self, config: _Config) -> list[UsageRecord]:
        """Records the query returns, in slurmdbd's order (``emulator/core/jobacct.py``).

        The times each record is reported with -- truncated or not -- are
        kept in ``self.times`` by record identity.
        """
        from emulator.core.scheduler import job_clock_now  # noqa: PLC0415

        self.database.ensure_job_ids()
        records = list(self.database.usage_records)

        # Filter by clusters (use current_cluster if not specified). A
        # nonexistent cluster simply matches no records — real sacct
        # treats -M as a pure filter and exits 0.
        if config.clusters:
            records = [r for r in records if r.cluster in config.clusters]
        else:
            records = [r for r in records if r.cluster == self.database.current_cluster]

        if config.accounts:
            records = [r for r in records if r.account in config.accounts]
        if config.users:
            records = [r for r in records if r.user in config.users]
        if config.jobs:
            records = [r for r in records if str(r.job_id) in config.jobs]
        have = {(r.job_id, r.submit_time) for r in records}
        active = self._active_job_records(config, have)

        # Explicit job ids bypass the default time window — real sacct
        # returns the job regardless of when it ran unless -S/-E are given.
        # Otherwise -S defaults to midnight and -E stays open
        # (slurm://src/common/slurmdb_defs.c#slurmdb_job_cond_def_start_end), on the simulated clock.
        sim_now = self.time_engine.get_current_time()
        job_now = epoch(job_clock_now(self.time_engine))
        now = max(epoch(sim_now), job_now)
        if config.jobs and not (config.start_time or config.end_time):
            usage_start = 0
        else:
            usage_start = epoch(
                config.start_time or sim_now.replace(hour=0, minute=0, second=0, microsecond=0)
            )
        cond = JobCond(
            usage_start=usage_start,
            usage_end=epoch(config.end_time),
            duplicates=config.duplicates,
            truncate=config.truncate,
        )
        candidates: list[tuple[UsageRecord, Optional[Job]]] = [(r, None) for r in records]
        candidates += [(r, self._active_jobs[str(r.job_id)]) for r in active]
        rows = jobacct.query(candidates, cond, now, job_now)
        self.times = {id(record): times for record, _active, times in rows}
        return [record for record, _active, _times in rows]

    def _active_job_records(
        self, config: _Config, have: set[tuple[Optional[int], Optional[datetime]]]
    ) -> list[UsageRecord]:
        """Rows for pending and running jobs, which slurmdbd holds from submission.

        The emulator writes accounting when a job ends; until then the row
        is built from the job: no end time, the requested nodes for a pending
        job and its allocation once it runs.
        """
        from emulator.core.cluster_model import compress_hostlist, job_request  # noqa: PLC0415

        rows = []
        clusters = config.clusters or [self.database.current_cluster]
        for job in self.database.jobs.values():
            job_id = str(job.job_id)
            if job.state not in ("PENDING", "RUNNING", "SUSPENDED") or not job_id.isdigit():
                continue
            if (int(job_id), job.submit_time) in have:
                continue
            if (
                job.cluster not in clusters
                or (config.accounts and job.account not in config.accounts)
                or (config.users and job.user not in config.users)
                or (config.jobs and job_id not in config.jobs)
            ):
                continue
            request = job_request(job)
            running = job.state in ("RUNNING", "SUSPENDED") and bool(job.node_list)
            self._active_jobs[job_id] = job
            rows.append(
                UsageRecord(
                    account=job.account,
                    user=job.user,
                    node_hours=0.0,
                    billing_units=0.0,
                    timestamp=self.time_engine.get_current_time(),
                    period="",
                    cluster=job.cluster,
                    partition=job.partition,
                    job_id=int(job_id),
                    state=job.state,
                    uid=job.uid,
                    gid=job.gid,
                    group_name=job.group_name,
                    job_name=job.name,
                    working_directory=job.working_directory,
                    exit_code=job.exit_code,
                    standard_input=job.standard_input,
                    standard_output=job.standard_output,
                    standard_error=job.standard_error,
                    submit_time=job.submit_time,
                    eligible_time=job.submit_time,
                    start_time=job.start_time if running else None,
                    time_limit=job.time_limit,
                    node_count=len(job.node_list) if running else request.node_count,
                    node_list=compress_hostlist(job.node_list) if running else "None assigned",
                    alloc_cpus=(job.alloc_cpus or request.total_cpus) if running else 0,
                    req_mem_mb=request.mem_per_node,
                    tasks=job.tasks,
                )
            )
        return rows

    def _row(self, record: UsageRecord, config: _Config) -> dict[str, str]:
        state = record.state or "COMPLETED"
        # WEXITSTATUS:WTERMSIG of the stored status (slurm://src/sacct/print.c#PRINT_EXITCODE).
        exit_code = exit_status.colon(state, record.exit_code)
        job_id = str(record.job_id)
        energy = _energy_joules(record)
        fmt = "%Y-%m-%dT%H:%M:%S"
        active = (
            self._active_jobs.get(job_id) if state in ("PENDING", "RUNNING", "SUSPENDED") else None
        )
        start: Optional[datetime]
        end: Optional[datetime]

        if active is not None:
            # Still in the controller: unset times print "Unknown"
            # (slurm://src/common/parse_time.c#slurm_make_time_str); CPU time
            # and MaxRSS only arrive when steps end.
            from emulator.commands.scontrol_info import job_tres  # noqa: PLC0415
            from emulator.core.scheduler import job_clock_now  # noqa: PLC0415

            submit = record.submit_time or record.timestamp
            eligible = record.eligible_time or submit
            start, end = record.start_time, None
            elapsed_secs = (
                max(int((job_clock_now(self.time_engine) - start).total_seconds()), 0)
                if start
                else 0
            )
            nnodes = record.node_count
            node_list = record.node_list
            alloc_cpus = record.alloc_cpus
            req_tres = job_tres(active, allocated=False)
            alloc_tres = job_tres(active, allocated=True) if start else ""
            req_mem_mb = record.req_mem_mb * max(nnodes, 1)
            req_mem = str(req_mem_mb) + "M" if config.noconvert else _convert_exact(req_mem_mb, 2)
            limit = _mins2time_str(record.time_limit) if record.time_limit else "UNLIMITED"
            total_cpu = user_cpu = system_cpu = "00:00:00"
            max_rss = ""
        elif record.start_time is not None and record.end_time is not None:
            # A record of a real run: the values slurmdbd stored for it.
            start, end = record.start_time, record.end_time
            submit = record.submit_time or start
            eligible = record.eligible_time or submit
            elapsed_secs = max(int((end - start).total_seconds()), 0)
            nnodes = record.node_count
            node_list = record.node_list or "None assigned"
            alloc_cpus = record.alloc_cpus
            req_tres = self._explicit_tres(record, allocated=False, noconvert=config.noconvert)
            alloc_tres = self._explicit_tres(record, allocated=True, noconvert=config.noconvert)
            req_mem_mb = record.req_mem_mb * max(nnodes, 1)
            req_mem = str(req_mem_mb) + "M" if config.noconvert else _convert_exact(req_mem_mb, 2)
            # Timelimit through mins2time_str (slurm://src/common/parse_time.c#mins2time_str).
            limit = _mins2time_str(record.time_limit) if record.time_limit else "UNLIMITED"
            total = record.cpu_time_seconds
            system = total * 0.04
            total_cpu = _elapsed_time(total)
            user_cpu = _elapsed_time(total - system)
            system_cpu = _elapsed_time(system)
            max_rss = (
                ""
                if not record.max_rss_bytes
                else (
                    str(record.max_rss_bytes)
                    if config.noconvert
                    else _convert_exact(record.max_rss_bytes, 0)
                )
            )
        else:
            elapsed_secs = int(record.node_hours * 3600)
            end = record.timestamp
            start = submit = eligible = end - timedelta(seconds=elapsed_secs)
            nnodes = 1
            node_list = "node001"
            alloc_cpus = self._cpu_rate(record)
            req_tres = alloc_tres = self._tres_string(record, config.noconvert)
            mem_mb = self._rate(record, "Mem", _NODE_MEM_GB) * 1024
            req_mem = f"{mem_mb}M" if config.noconvert else _convert_exact(mem_mb, 2)
            limit = "UNLIMITED"
            total_cpu = user_cpu = system_cpu = "00:00:00"
            max_rss = ""

        # The times the query reports, truncated or not (emulator/core/jobacct.py);
        # unset ones print "Unknown" (slurm://src/common/parse_time.c#slurm_make_time_str).
        suspended_secs = 0
        times = self.times.get(id(record))
        if times is not None:
            submit = _from_epoch(times.submit) or submit
            eligible = _from_epoch(times.eligible) or eligible
            start, end = _from_epoch(times.start), _from_epoch(times.end)
            elapsed_secs, suspended_secs = times.elapsed, times.suspended

        return {
            "JobID": job_id,
            "JobIDRaw": job_id,
            "ConsumedEnergy": str(energy) if config.noconvert else _convert_num_unit(energy),
            "ConsumedEnergyRaw": str(energy),
            "JobName": record.job_name or f"job_{record.job_id}",
            "Partition": record.partition or "compute",
            "Account": record.account,
            "AllocCPUS": str(alloc_cpus),
            "AllocNodes": str(nnodes if node_list != "None assigned" else 0),
            "AllocTRES": alloc_tres,
            "Cluster": record.cluster,
            "Elapsed": _secs2time_str(elapsed_secs),
            "ElapsedRaw": str(elapsed_secs),
            "Eligible": eligible.strftime(fmt),
            "End": end.strftime(fmt) if end else "Unknown",
            "ExitCode": exit_code,
            "GID": str(record.gid if record.gid is not None else 1000),
            "Group": record.group_name or record.user,
            "MaxRSS": max_rss,
            "NNodes": str(nnodes),
            "NodeList": node_list,
            "ReqMem": req_mem,
            "ReqTRES": req_tres,
            "Start": start.strftime(fmt) if start else "Unknown",
            # slurm://src/sacct/print.c#PRINT_STATE: a cancelled job names who did it.
            "State": f"{state} by {record.requid}"
            if state == "CANCELLED" and record.requid is not None
            else state,
            "Submit": submit.strftime(fmt),
            "Suspended": _secs2time_str(suspended_secs),
            "SystemCPU": system_cpu,
            "Timelimit": limit,
            "TotalCPU": total_cpu,
            "UID": str(record.uid if record.uid is not None else 1000),
            "User": record.user,
            "UserCPU": user_cpu,
            "StdIn": record.standard_input,
            "StdOut": record.standard_output,
            "StdErr": record.standard_error,
            "WorkDir": record.working_directory or f"/home/{record.user}",
        }

    def _explicit_tres(self, record: UsageRecord, allocated: bool, noconvert: bool) -> str:
        """ReqTRES / AllocTRES of a real run, in TRES-id order."""
        nodes = record.node_count
        if allocated and not nodes:
            return ""
        nodes = max(nodes, 1)
        cpus = record.alloc_cpus or _NODE_CPUS * nodes
        mem_mb = record.req_mem_mb * nodes
        mem = f"{mem_mb}M" if noconvert else _convert_exact(mem_mb, 2)
        parts = [f"cpu={cpus}", f"mem={mem}", f"node={nodes}", f"billing={cpus}"]
        if partition_has_gpus(record.partition):
            parts.append(f"gres/gpu={_NODE_GPUS * nodes}")
        return ",".join(parts)

    def _cpu_rate(self, record: UsageRecord) -> int:
        return self._rate(record, "CPU", _NODE_CPUS)

    @staticmethod
    def _rate(record: UsageRecord, key: str, default: int) -> int:
        """Per-hour TRES rate: raw_tres values are <count>-hours totals.

        The standard-node ``default`` applies only when the record carries
        no breakdown for ``key`` (case-insensitive); a zero-node-hour record
        with an explicit breakdown (an energy-only report) renders 0.
        """
        value = None
        for raw_name, raw_value in record.raw_tres.items():
            if raw_name.lower() == key.lower():
                value = raw_value
                break
        if value is None:
            return default
        if record.node_hours <= 0:
            return 0
        return round(value / record.node_hours)

    def _tres_string(self, record: UsageRecord, noconvert: bool) -> str:
        """Standard Slurm TRES string in TRES-id order.

        cpu=1, mem=2, node=4, billing=5, gres/* after — matching
        slurmdb_make_tres_string_from_simple. The emulator-internal
        ``node-hours`` raw_tres key is intentionally not exposed.
        """
        cpus = self._cpu_rate(record)
        mem_gb = self._rate(record, "Mem", _NODE_MEM_GB)
        gpus = self._rate(record, "GRES/gpu", _NODE_GPUS)
        mem = f"{mem_gb * 1024}M" if noconvert else f"{mem_gb}G"
        parts = [f"cpu={cpus}", f"mem={mem}", "node=1", f"billing={cpus}"]
        if gpus:
            parts.append(f"gres/gpu={gpus}")
        return ",".join(parts)


def _from_epoch(value: int) -> Optional[datetime]:
    return datetime.fromtimestamp(value) if value else None  # noqa: DTZ006 - naive local clock


def _energy_joules(record: UsageRecord) -> int:
    for key, value in record.raw_tres.items():
        if key.lower() == "energy":
            return int(value)
    return 0


def _convert_num_unit(number: int, divisor: int = 1000) -> str:
    """Port of slurm://src/common/slurm_protocol_api.c#convert_num_unit2 (no flags).

    Scales aggressively while ``num >= divisor``; integral results print
    without decimals, fractional ones with two.
    """
    if number == 0:
        return "0"
    units = "\0KMGTP?"
    num = float(number)
    order = 0
    while num >= divisor:
        num /= divisor
        order += 1
    unit = units[order] if order < len(units) else "?"
    unit = "" if unit == "\0" else unit
    if float(int(num)) == num:
        return f"{int(num)}{unit}"
    return f"{num:.2f}{unit}"


def _secs2time_str(secs: int) -> str:
    """Port of secs2time_str (slurm://src/common/parse_time.c#secs2time_str)."""
    if secs < 0:
        return "INVALID"
    days, rem = divmod(secs, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    if days:
        return f"{days}-{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _convert_exact(number: float, unit: int) -> str:
    """convert_num_unit with sacct's default CONVERT_NUM_UNIT_EXACT.

    sacct sets ``params.convert_flags = CONVERT_NUM_UNIT_EXACT``
    (slurm://src/sacct/options.c#CONVERT_NUM_UNIT_EXACT): divide by 1024 only
    while the value stays a multiple of 512, so 262144M prints ``256G`` and
    4000M stays ``4000M`` (slurm://src/common/slurm_protocol_api.c#convert_num_unit2).
    ``unit`` is the starting unit index (0 = bytes, 2 = MB).
    """
    units = ["", "K", "M", "G", "T", "P"]
    if int(number) == 0:
        return "0"
    num = float(number)
    while num >= 1024 and int(num) % 512 == 0 and unit < len(units) - 1:
        num /= 1024
        unit += 1
    if float(int(num)) == num:
        return f"{int(num)}{units[unit]}"
    return f"{num:.2f}{units[unit]}"


def _mins2time_str(minutes: int) -> str:
    """Port of mins2time_str (slurm://src/common/parse_time.c#mins2time_str)."""
    days, rem = divmod(int(minutes), 1440)
    hours, mins = divmod(rem, 60)
    if days:
        return f"{days}-{hours:02d}:{mins:02d}:00"
    return f"{hours:02d}:{mins:02d}:00"


def _elapsed_time(seconds: float) -> str:
    """Port of sacct's _elapsed_time for TotalCPU/UserCPU/SystemCPU (slurm://src/sacct/print.c#_elapsed_time)."""
    secs = int(seconds)
    subsec = round((seconds - secs) * 1_000_000) // 1000
    days, rem = divmod(secs, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, sec = divmod(rem, 60)
    if days:
        return f"{days}-{hours:02d}:{minutes:02d}:{sec:02d}"
    if hours:
        return f"{hours:02d}:{minutes:02d}:{sec:02d}"
    if subsec:
        return f"{minutes:02d}:{sec:02d}.{subsec:03d}"
    return f"00:{minutes:02d}:{sec:02d}"
