"""sacct command emulator for usage reporting.

Output formatting and exit codes mirror real Slurm 26.11:

* default fields are ``JobID,JobName,Partition,Account,AllocCPUS,
  State,ExitCode`` (src/sacct/sacct.h:66) with the widths from the
  field table in src/sacct/sacct.c:43-169 (negative = left-aligned);
* header + dashed underline, ``-p``/``-P``/``-n`` parsable/noheader
  modes, and ``value[:width-1]+'+'`` truncation come from the shared
  print_fields module (src/common/print_fields.c semantics);
* ``Elapsed`` renders ``[D-]HH:MM:SS`` (``secs2time_str``,
  src/common/parse_time.c:849-874);
* an invalid time spec prints ``Invalid time specification (pos=N):
  <str>`` to stderr — no ``sacct:`` prefix, parse_time.c:626-631 — and
  exits 1; an unknown format field prints ``sacct: error: Invalid field
  requested: "X"`` and exits 1 (options.c:1215-1216);
* without ``-S``/``-E`` the window is Midnight → Now on the simulated
  clock (slurmdb_job_cond_def_start_end, slurmdb_defs.c:350-395);
* one job row per usage record: numeric JobID from the database,
  standard TRES string (``cpu=...,mem=...G,node=1,billing=...``) in
  TRES-id order — the emulator-internal ``node-hours`` key is not
  exposed.

Documented simplifications: ``-X``/``--allocations`` is a no-op (the
emulator has no job steps, and real ``-X`` only filters step rows);
``-a``/``--allusers`` is a no-op (no UID model); jobs are single-node
(``node001``, partition ``compute``, matching the sinfo emulator).
"""

import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from emulator import __version__
from emulator.commands.print_fields import (
    FieldSpec,
    OutputMode,
    UnknownFieldError,
    parse_format_spec,
    render_table,
    resolve_format,
)
from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine

# Subset of the real field table (src/sacct/sacct.c:43-169), in table
# order so prefix matching resolves like options.c:1204-1208 (first
# match wins, no minimum prefix length).
_REGISTRY: list[FieldSpec] = [
    FieldSpec("Account", 10),  # sacct.c:44
    FieldSpec("AllocCPUS", 10, truncate=False),  # sacct.c:46
    FieldSpec("AllocNodes", 10),  # sacct.c:47
    FieldSpec("AllocTRES", 10),  # sacct.c:48
    FieldSpec("Cluster", 10),  # sacct.c:58
    FieldSpec("Elapsed", 10),  # sacct.c:69
    FieldSpec("ElapsedRaw", 10, truncate=False),  # sacct.c:70
    FieldSpec("End", 19),  # sacct.c:72
    FieldSpec("ExitCode", 8),  # sacct.c:74
    FieldSpec("JobID", -12),  # sacct.c:80
    FieldSpec("JobIDRaw", -12),  # sacct.c:81
    FieldSpec("JobName", 10),  # sacct.c:82
    FieldSpec("NNodes", 8, truncate=False),  # sacct.c:105
    FieldSpec("NodeList", 15),  # sacct.c:106
    FieldSpec("Partition", 10),  # sacct.c:110
    FieldSpec("ReqTRES", 10),  # sacct.c:126
    FieldSpec("Start", 19),  # sacct.c:133
    FieldSpec("State", 10),  # sacct.c:134
    FieldSpec("Submit", 19),  # sacct.c:138
    FieldSpec("Timelimit", 10),  # sacct.c:143
    FieldSpec("User", 9),  # sacct.c:163
]

_DEFAULT_FORMAT = "JobID,JobName,Partition,Account,AllocCPUS,State,ExitCode"  # sacct.h:66

# Standard node config used by the usage simulator
# (usage_simulator.py:156-165): fallback rates when a record carries no
# raw_tres breakdown.
_NODE_CPUS = 64
_NODE_MEM_GB = 512
_NODE_GPUS = 4

_FAILED_STATES = ("FAILED", "OUT_OF_MEMORY", "TIMEOUT")


@dataclass
class _Config:
    accounts: list[str] = field(default_factory=list)
    users: list[str] = field(default_factory=list)
    clusters: list[str] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    format_spec: str = _DEFAULT_FORMAT
    mode: OutputMode = field(default_factory=OutputMode)
    noconvert: bool = False
    version: bool = False


class SacctEmulator:
    """Emulates sacct commands for usage reporting."""

    def __init__(self, database: SlurmDatabase, time_engine: TimeEngine):
        self.database = database
        self.time_engine = time_engine
        # Mirrors sacct's exit handling: 0 unless an error path ran.
        self.exit_code = 0

    def handle_command(self, args: list[str]) -> str:
        """Process sacct command and return output."""
        self.exit_code = 0
        config = self._parse_args(args)

        if config.version:
            return f"slurm-emulator {__version__}"

        try:
            fields = resolve_format(parse_format_spec(config.format_spec), _REGISTRY)
        except UnknownFieldError as e:
            # options.c:1215-1216 via error(): "sacct: error: ..." on stderr.
            print(f'sacct: error: Invalid field requested: "{e.token}"', file=sys.stderr)
            self.exit_code = 1
            raise SystemExit(1) from None

        records = self._get_filtered_records(config)
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
            "-o": "format",
            "--format": "format",
            "--fields": "format",
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
            "--truncate": "noop",
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
            elif key == "format":
                cfg.format_spec = value

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
        """Parse a sacct time spec on the simulated clock.

        Supports the common parse_time() forms: ISO dates/datetimes,
        ``HH:MM[:SS]`` (today), ``now[{+|-}count[unit]]``, ``today``,
        ``midnight``. Failure mirrors parse_time.c:626-631: the message
        goes to stderr without a ``sacct:`` prefix and the process
        exits 1.
        """
        try:
            return self._parse_time_inner(time_str)
        except (ValueError, IndexError):
            print(f"Invalid time specification (pos=0): {time_str}", file=sys.stderr)
            self.exit_code = 1
            raise SystemExit(1) from None

    def _parse_time_inner(self, time_str: str) -> datetime:
        """Parse one time spec; raises ValueError on anything bogus."""
        now = self.time_engine.get_current_time()
        text = time_str.strip()
        lowered = text.lower()

        if lowered in {"today", "midnight"}:
            return now.replace(hour=0, minute=0, second=0, microsecond=0)
        if lowered.startswith("now"):
            rest = lowered[3:]
            if not rest:
                return now
            match = re.fullmatch(r"([+-])(\d+)([a-z]*)", rest)
            if match is None:
                raise ValueError(rest)
            sign = 1 if match.group(1) == "+" else -1
            count = int(match.group(2))
            unit = match.group(3)
            seconds_per = {
                "": 60,  # bare count = minutes, like parse_time()
                "seconds": 1,
                "minutes": 60,
                "hours": 3600,
                "days": 86400,
                "weeks": 604800,
            }
            for name, secs in seconds_per.items():
                if name.startswith(unit) and (name or not unit):
                    return now + timedelta(seconds=sign * count * secs)
            raise ValueError(unit)
        if "T" in text:
            return datetime.fromisoformat(text)
        if "-" in text:
            try:
                return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return datetime.strptime(text, "%Y-%m-%d")
        if ":" in text:
            parts = [int(p) for p in text.split(":")]
            hour, minute = parts[0], parts[1]
            second = parts[2] if len(parts) > 2 else 0
            return now.replace(hour=hour, minute=minute, second=second, microsecond=0)
        raise ValueError(text)

    def _get_filtered_records(self, config: _Config) -> list[UsageRecord]:
        """Get usage records based on filters."""
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

        # Default window: Midnight -> Now on the simulated clock
        # (slurmdb_job_cond_def_start_end, slurmdb_defs.c:371-394).
        now = self.time_engine.get_current_time()
        start = config.start_time or now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = config.end_time or now
        return [r for r in records if start <= r.timestamp <= end]

    def _row(self, record: UsageRecord, config: _Config) -> dict[str, str]:
        elapsed_secs = int(record.node_hours * 3600)
        end = record.timestamp
        start = end - timedelta(seconds=elapsed_secs)
        state = record.state or "COMPLETED"
        exit_code = "1:0" if state.startswith(_FAILED_STATES) else "0:0"
        tres = self._tres_string(record, config.noconvert)
        job_id = str(record.job_id)

        return {
            "JobID": job_id,
            "JobIDRaw": job_id,
            "JobName": f"job_{record.job_id}",
            "Partition": "compute",
            "Account": record.account,
            "AllocCPUS": str(self._cpu_rate(record)),
            "AllocNodes": "1",
            "AllocTRES": tres,
            "Cluster": record.cluster,
            "Elapsed": _secs2time_str(elapsed_secs),
            "ElapsedRaw": str(elapsed_secs),
            "End": end.strftime("%Y-%m-%dT%H:%M:%S"),
            "ExitCode": exit_code,
            "NNodes": "1",
            "NodeList": "node001",
            "ReqTRES": tres,
            "Start": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "State": state,
            "Submit": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "Timelimit": "UNLIMITED",
            "User": record.user,
        }

    def _cpu_rate(self, record: UsageRecord) -> int:
        return self._rate(record, "CPU", _NODE_CPUS)

    @staticmethod
    def _rate(record: UsageRecord, key: str, default: int) -> int:
        """Per-hour TRES rate: raw_tres values are <count>-hours totals."""
        value = record.raw_tres.get(key)
        if value is None or record.node_hours <= 0:
            return default
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


def _secs2time_str(secs: int) -> str:
    """Port of secs2time_str (src/common/parse_time.c:849-874)."""
    if secs < 0:
        return "INVALID"
    days, rem = divmod(secs, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    if days:
        return f"{days}-{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
