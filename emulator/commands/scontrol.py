"""scontrol emulator: reservations, partitions and jobs.

Covers what a site agent needs to manage maintenance windows:

* ``scontrol create reservation key=value ...`` -- options parsed like
  slurm://src/scontrol/create_res.c#_parse_res_options (case-insensitive
  prefixes with the real minimum lengths, ``+=``/``-=`` on lists, Flags
  through slurm://src/common/proc_args.c#parse_resv_flags), checked by
  slurm://src/common/slurm_protocol_defs.c#validate_resv_create_desc@25.05+
  (inline in scontrol_create_res on 24.11), then
  created as slurmctld would (``emulator/core/reservations.py``). Prints
  ``Reservation created: <name>``
  (slurm://src/scontrol/create_res.c#scontrol_create_res); a refusal prints
  ``Error creating the reservation: <strerror>`` and exits 1.
* ``scontrol update ReservationName=<name> key=value ...`` -- prints
  ``Reservation updated.`` (slurm://src/scontrol/create_res.c#scontrol_update_res);
  a refusal prints ``slurm_update error: <strerror>``
  (slurm://src/scontrol/scontrol.c#_update_it).
* ``scontrol delete ReservationName=<name>`` (or ``delete reservation <name>``)
  -- silent on success; ``delete_reservation <arg>: <strerror>`` otherwise
  (slurm://src/scontrol/scontrol.c#_delete_it).
* ``scontrol show reservation [<name>]`` (also ``show res``,
  ``show ReservationName=<name>``) -- the record format of
  slurm://src/api/reservation_info.c#slurm_sprint_reservation_info, with
  ``-o``/``--oneliner`` for one line per record and ``--json`` for the
  OPENAPI_RESERVATION_RESP dump (slurm://src/scontrol/info_res.c#scontrol_print_res).
  The ``AllowedPartitions= QOS=`` line exists from 25.11
  (slurm://src/api/reservation_info.c#"AllowedPartitions=%s QOS=%s"@25.11+).

Errors from option parsing go to stderr as ``scontrol: error: ...`` with exit
code 1, as scontrol's ``error()`` does.

Intentional deviations:

* ``scontrol show partition [<name>]`` and ``scontrol show job [<id>]`` --
  ``emulator/commands/scontrol_info.py``; ``write batch_script <id> [-]``
  and ``ping``.
* Only those entities are emulated; other
  entities print ``scontrol: unsupported``.
* CoreCnt, TRES, TRESPerNode, Licenses, BurstBuffer, Features, QOS,
  AllowedPartitions and MaxStartDelay are refused as unsupported instead of
  being reserved (slurm://src/scontrol/create_res.c#_parse_res_options accepts
  them).
* ``--json`` output carries a reduced ``meta`` block.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from emulator.api.slurmrestd.schemas import reservation_to_dict, uint_no_val
from emulator.commands import scontrol_info
from emulator.commands.slurm_time import ParseTimeError, parse_time
from emulator.core import reservations as resv_core
from emulator.core.cluster_model import NODE_CPUS, compress_hostlist, sync_reservations
from emulator.core.database import Reservation, SlurmDatabase
from emulator.core.reservations import (
    ReservationDesc,
    ReservationDescError,
    ReservationError,
    strerror,
)
from emulator.core.scheduler import advance_job_states, job_clock_now
from emulator.core.time_engine import TimeEngine
from emulator.slurm_version import at_least, current

# (name, minimum prefix, flag, NO_ flag) in parse order
# (slurm://src/common/proc_args.c#parse_resv_flags). ``None`` NO_ flag = the
# ``-`` form is not accepted (``+`` is); "new_only" flags only without +/-.
_FLAG_TABLE = (
    ("Maintenance", 3, "MAINT", "NO_MAINT"),
    ("Overlap", 1, "OVERLAP", None),
    ("Flex", 1, "FLEX", "NO_FLEX"),
    ("Ignore_Jobs", 1, "IGNORE_JOBS", "NO_IGNORE_JOBS"),
    ("Hourly", 1, "HOURLY", "NO_HOURLY"),
    ("Daily", 1, "DAILY", "NO_DAILY"),
    ("Weekday", 1, "WEEKDAY", "NO_WEEKDAY"),
    ("Weekend", 1, "WEEKEND", "NO_WEEKEND"),
    ("Weekly", 1, "WEEKLY", "NO_WEEKLY"),
    ("Any_Nodes", 1, "ANY_NODES", "NO_ANY_NODES"),
    ("License_Only", 1, "ANY_NODES", "NO_ANY_NODES"),
    ("Static_Alloc", 1, "STATIC", "NO_STATIC"),
    ("Part_Nodes", 2, "PART_NODES", "NO_PART_NODES"),
    ("magnetic", 3, "MAGNETIC", "NO_MAGNETIC"),
    ("promiscuous", 2, "MAGNETIC", "NO_MAGNETIC"),
    ("PURGE_COMP", 2, "PURGE_COMP", "NO_PURGE_COMP"),
    ("Time_Float", 1, "TIME_FLOAT", "new_only"),
    ("Replace", 1, "REPLACE", None),
    ("Replace_Down", 8, "REPLACE_DOWN", None),
    ("NO_HOLD_JOBS_AFTER_END", 1, "NO_HOLD_JOBS_AFTER_END", None),
    ("User_Delete", 1, "USER_DELETE", "NO_USER_DELETE"),
    ("Force_Start", 1, "FORCE_START", "new_only"),
)

_UNSUPPORTED_TAGS = (
    ("AllowedPartitions", 2),
    ("QOS", 1),
    ("BurstBuffer", 2),
    ("MaxStartDelay", 2),
    ("CoreCnt", 5),
    ("CoreCount", 5),
    ("CPUCnt", 5),
    ("CPUCount", 5),
    ("Features", 2),
    ("Licenses", 2),
    ("TRES", 1),
    ("TRESPerNode", 5),
)


def _tag_is(tag: str, name: str, minimum: int) -> bool:
    """``xstrncasecmp(tag, name, MAX(taglen, minimum)) == 0``."""
    return len(tag) >= minimum and name.lower().startswith(tag.lower())


class _ParseError(Exception):
    pass


def parse_resv_flags(text: str, msg: str) -> list[str]:
    """slurm://src/common/proc_args.c#parse_resv_flags -> RESERVATION_FLAGS names."""
    out: list[str] = []
    op = ""
    pos = 0
    while pos < len(text):
        if text[pos] in "+-":
            op = text[pos]
            pos += 1
        end = pos
        while end < len(text) and text[end] not in ",=":
            end += 1
        token = text[pos:end]
        for name, minimum, flag, no_flag in _FLAG_TABLE:
            if not _tag_is(token, name, minimum):
                continue
            if flag == "FORCE_START" and not at_least("25.05"):
                continue  # slurm://src/common/proc_args.c#"Force_Start"@25.05+
            if no_flag is None and op == "-":
                continue
            if no_flag == "new_only" and op:
                continue
            out.append(no_flag if op == "-" and no_flag else flag)
            break
        else:
            raise _ParseError(f"Error parsing flags {text}.  {msg}")
        pos = end
        if pos < len(text) and text[pos] == ",":
            pos += 1
        elif pos < len(text):
            raise _ParseError(f"Error parsing flags {text}.  {msg}")
    return out


def _plus_minus(sign: str, value: str) -> str:
    """slurm://src/scontrol/common.c#scontrol_process_plus_minus: prefix every item."""
    return ",".join(f"{sign}{item}" for item in value.split(",") if item)


class ScontrolEmulator:
    """``scontrol`` reservation subcommands over the shared JSON state."""

    def __init__(self, database: SlurmDatabase, time_engine: TimeEngine):
        self.database = database
        self.time_engine = time_engine
        self.exit_code = 0

    # --- entry ---

    def handle_command(self, args: list[str]) -> str:
        """Run one scontrol command line; stdout is returned, stderr printed."""
        self.exit_code = 0
        one_liner = False
        as_json = False
        words: list[str] = []
        for arg in args:
            if arg in ("-o", "--oneliner"):
                one_liner = True
            elif arg == "--json":
                as_json = True
            elif arg in ("-d", "--details", "-Q", "--quiet", "-a", "--all"):
                continue
            else:
                words.append(arg)
        if not words:
            return self._fail("scontrol: error: no command given")
        command, rest = words[0].lower(), words[1:]
        if command == "create":
            return self._create(rest)
        if command == "update":
            return self._update(rest)
        if command == "delete":
            return self._delete(rest)
        if command == "show":
            return self._show(rest, one_liner, as_json)
        if command == "write":
            return self._write(rest)
        if command == "ping":
            return self._ping()
        return self._fail(f"scontrol: unsupported command: {words[0]}")

    def _fail(self, message: str, stdout: str = "") -> str:
        self.exit_code = 1
        if message:
            print(message, file=sys.stderr)
        return stdout

    def _now(self) -> datetime:
        return job_clock_now(self.time_engine)

    def _refresh(self) -> datetime:
        """Reload the shared state and bring jobs and env reservations up to date.

        Every scontrol call is an RPC to slurmctld, so it sees what the REST
        plane or another process wrote since this emulator was created.
        """
        self.database.load_state()
        self.time_engine._load_state()  # the clock may have moved since, too
        changed = advance_job_states(self.database, self.time_engine)
        now = self._now()
        if sync_reservations(self.database, now) or changed:
            self.database.save_state()
        return now

    # --- option parsing (create_res.c#_parse_res_options) ---

    def _parse_options(self, args: list[str], msg: str, now: datetime) -> ReservationDesc:
        desc = ReservationDesc()
        for arg in args:
            if "=" not in arg:
                if arg[:3].lower() == "res":
                    continue
                if _tag_is(arg, "Skip", 2):
                    desc.add_flag("SKIP")
                    continue
                raise _ParseError(
                    f"Misformatted parameter '{arg}', most options have a parameter "
                    f"after '='.  {msg}"
                )
            tag, val = arg.split("=", 1)
            sign = ""
            if tag.endswith(("+", "-")):
                tag, sign = tag[:-1], tag[-1]
            self._apply_option(desc, tag, sign, val, arg, msg, now)
        return desc

    def _apply_option(
        self,
        desc: ReservationDesc,
        tag: str,
        sign: str,
        val: str,
        arg: str,
        msg: str,
        now: datetime,
    ) -> None:
        def once(current: Optional[str]) -> None:
            if current is not None:
                raise _ParseError(f"Parameter {arg} specified more than once")

        def listed() -> str:
            return _plus_minus(sign, val) if sign else val

        if _tag_is(tag, "Accounts", 1):
            once(desc.accounts)
            desc.accounts = listed()
        elif _tag_is(tag, "AllowedPartitions", 2):
            raise _ParseError(f"{tag} is not supported by slurm-emulator.  {msg}")
        elif _tag_is(tag, "Comment", 3):
            once(desc.comment)
            desc.comment = val
        elif _tag_is(tag, "Flags", 2):
            for flag in parse_resv_flags(listed(), msg):
                desc.add_flag(flag)
        elif _tag_is(tag, "Groups", 1):
            once(desc.groups)
            desc.groups = listed()
        elif _tag_is(tag, "Users", 1):
            once(desc.users)
            desc.users = listed()
        elif _tag_is(tag, "ReservationName", 1):
            desc.name = val
        elif _tag_is(tag, "StartTime", 2):
            desc.start_time = self._time(val, f"Invalid start time {arg}.  {msg}", now)
        elif _tag_is(tag, "EndTime", 1):
            desc.end_time = self._time(val, f"Invalid end time {arg}.  {msg}", now)
        elif _tag_is(tag, "Duration", 1):
            minutes = resv_core.time_str2mins(val)
            if minutes is None:
                raise _ParseError(f"Invalid duration {arg}.  {msg}")
            desc.duration = minutes
            if sign:
                desc.add_flag("DURATION_PLUS" if sign == "+" else "DURATION_MINUS")
                desc.extra["duration_op"] = sign
        elif _tag_is(tag, "NodeCnt", 5) or _tag_is(tag, "NodeCount", 5):
            if "," in val:
                text = "Using a comma separated array for NodeCnt is no longer valid."
                raise _ParseError(text)
            if not val.strip().isdigit():
                raise _ParseError(f'"{val}" is not a valid node count')
            desc.node_cnt = int(val)
        elif _tag_is(tag, "Nodes", 5):
            # Nodes+=/-= prefix every hostlist token
            # (slurm://src/scontrol/create_res.c#_parse_res_options).
            desc.node_list = (
                ",".join(f"{sign}{t}" for t in resv_core._hostlist_tokens(val) if t)
                if sign
                else val
            )
        elif _tag_is(tag, "PartitionName", 1):
            desc.partition = val
        elif any(_tag_is(tag, name, minimum) for name, minimum in _UNSUPPORTED_TAGS):
            raise _ParseError(f"{tag} is not supported by slurm-emulator.  {msg}")
        elif tag[:3].lower() == "res":
            return
        else:
            raise _ParseError(f"Unknown parameter {arg}.  {msg}")
        handled_sign = (
            _tag_is(tag, "Accounts", 1)
            or _tag_is(tag, "Groups", 1)
            or (_tag_is(tag, "Users", 1) or _tag_is(tag, "Flags", 2) or _tag_is(tag, "Duration", 1))
            or _tag_is(tag, "Nodes", 5)
        )
        if sign and not handled_sign:
            raise _ParseError(f"The +=/-= notation is not supported when updating {tag}.  {msg}")

    @staticmethod
    def _time(value: str, error: str, now: datetime) -> datetime:
        """parse_time(val, 0) (slurm://src/common/parse_time.c#parse_time) on the job clock.

        parse_time() prints its own line before scontrol's error; an empty
        value is time 0, which the controller then refuses as a past time.
        """
        try:
            parsed = parse_time(value, now)
        except ParseTimeError as exc:
            print(exc, file=sys.stderr)
            raise _ParseError(error) from exc
        return parsed if parsed is not None else datetime.fromtimestamp(0)  # noqa: DTZ006

    # --- create / update / delete ---

    def _create(self, args: list[str]) -> str:
        # The entity is the first argument naming NodeName / PartitionName /
        # ReservationName (slurm://src/scontrol/scontrol.c#_create_it).
        for arg in args:
            tag = arg.split("=", 1)[0]
            if _tag_is(tag, "NodeName", 3) or _tag_is(tag, "PartitionName", 3):
                return self._fail("scontrol: unsupported: only reservations can be created")
            if _tag_is(tag, "ReservationName", 3):
                break
        else:
            return self._fail(
                f"scontrol: error: Invalid creation entity: {args[0] if args else ''}"
            )
        now = self._refresh()
        try:
            desc = self._parse_options(args, "No reservation created.", now)
            resv_core.validate_create_desc(desc)
        except (_ParseError, ReservationDescError) as exc:
            return self._fail(f"scontrol: error: {exc}")
        try:
            resv = resv_core.create_reservation(self.database, desc, now)
        except ReservationError as exc:
            note = ""
            if exc.errno == resv_core.ESLURM_NODES_BUSY and not desc.node_list:
                note = (
                    "Note, unless nodes are directly requested a reservation must exist "
                    "in a single partition.\nIf no partition is requested the default "
                    "partition is assumed.\n"
                )
            return self._fail(f"Error creating the reservation: {strerror(exc.errno)}", note)
        self.database.save_state()
        return f"Reservation created: {resv.name}\n"

    def _update(self, args: list[str]) -> str:
        if not any(_tag_is(a.split("=", 1)[0].rstrip("+-"), "ReservationName", 3) for a in args):
            return self._fail(
                "No valid entity in update command\n"
                'Input line must include "NodeName", "PartitionName", "Reservation", '
                '"JobId", or "SlurmctldDebug"'
            )
        now = self._refresh()
        try:
            desc = self._parse_options(args, "No reservation update.", now)
        except _ParseError as exc:
            return self._fail(f"scontrol: error: {exc}")
        if desc.name is None:
            return self._fail("scontrol: error: Reservation must be given.  No reservation update.")
        try:
            resv_core.update_reservation(self.database, desc, now)
        except ReservationError as exc:
            return self._fail(f"slurm_update error: {strerror(exc.errno)}")
        self.database.save_state()
        return "Reservation updated.\n"

    def _delete(self, args: list[str]) -> str:
        usage = "scontrol: error: Proper format is 'delete <ENTITY>=<ID>' or 'delete <ENTITY> <ID>'"
        if len(args) == 1 and "=" in args[0]:
            tag, val = args[0].split("=", 1)
        elif len(args) == 2:
            tag, val = args
        else:
            return self._fail(usage)
        if not _tag_is(tag, "ReservationName", 3):
            if _tag_is(tag, "NodeName", 3) or _tag_is(tag, "PartitionName", 3):
                return self._fail("scontrol: unsupported: only reservations can be deleted")
            return self._fail(f"Invalid deletion entity: {args[0]}")
        self._refresh()
        try:
            resv_core.delete_reservation(self.database, val)
        except ReservationError as exc:
            return self._fail(f"delete_reservation {args[0]}: {strerror(exc.errno)}")
        self.database.save_state()
        return ""

    # --- show ---

    def _show(self, args: list[str], one_liner: bool, as_json: bool) -> str:
        if not args:
            return self._fail("scontrol: error: too few arguments for keyword:show")
        if len(args) > 2:
            return self._fail("scontrol: error: too many arguments for keyword:show")
        tag, _, val = args[0].partition("=")
        name: Optional[str] = val or (args[1] if len(args) == 2 else None)
        # Entity prefixes and minimum lengths of slurm://src/scontrol/scontrol.c#_show_it.
        if _tag_is(tag, "jobs", 1) or _tag_is(tag, "jobid", 1):
            return self._show_job(name, one_liner)
        if _tag_is(tag, "partitions", 2) or _tag_is(tag, "partitionname", 2):
            return self._show_partition(name, one_liner)
        if not (_tag_is(tag, "reservations", 1) or _tag_is(tag, "reservationname", 1)):
            return self._fail(f"scontrol: unsupported: show {args[0]}")
        now = self._refresh()
        # slurmctld's list order: oldest record first.
        records = list(self.database.reservations.values())
        if name:
            records = [r for r in records if r.name == name][:1]
        if as_json:
            return self._json(records, now)
        out = "".join(sprint_reservation(r, now, one_liner) for r in records)
        if not records:
            if name:
                self.exit_code = 1
                return f"Reservation {name} not found\n"
            return "No reservations in the system\n"
        return out

    def _show_partition(self, name: Optional[str], one_liner: bool) -> str:
        """slurm://src/scontrol/info_part.c#scontrol_print_part."""
        names = scontrol_info.partitions()
        default = names[0] if names else None
        if name:
            names = [n for n in names if n == name][:1]
        out = "".join(scontrol_info.sprint_partition(n, n == default, one_liner) for n in names)
        if not names:
            if name:
                self.exit_code = 1
                return f"Partition {name} not found\n"
            return "No partitions in the system\n"
        return out

    def _show_job(self, job_id: Optional[str], one_liner: bool) -> str:
        """slurm://src/scontrol/info_job.c#scontrol_print_job."""
        if job_id and not all(c.isdigit() or c in "_+" for c in job_id):
            return self._fail("scontrol_print_job error: Invalid job id specified")
        self._refresh()
        now = job_clock_now(self.time_engine)
        jobs = sorted(self.database.jobs.values(), key=lambda j: int(j.job_id or 0))
        if job_id:
            wanted = job_id.split("_")[0].split("+")[0]
            jobs = [j for j in jobs if str(j.job_id) == wanted]
            if not jobs:
                # slurm_load_job() refuses an id the controller does not hold.
                return self._fail("slurm_load_jobs error: Invalid job id specified")
        if not jobs:
            return "No jobs in the system\n"
        return "".join(scontrol_info.sprint_job(j, now, one_liner) for j in jobs)

    def _write(self, args: list[str]) -> str:
        """``write batch_script <job> [file|-]`` (slurm://src/scontrol/info_job.c#scontrol_batch_script)."""
        if not args:
            return self._fail("too few arguments for keyword:write")
        if not _tag_is(args[0], "batch_script", max(len(args[0]), 5)):
            return self._fail(f"scontrol: unsupported: write {args[0]}")
        if len(args) > 3:
            return self._fail("too many arguments for keyword:write")
        if len(args) < 2:
            return ""
        self.database.load_state()
        job = self.database.jobs.get(args[1].split(".")[0])
        target = args[2] if len(args) > 2 else f"slurm-{args[1]}.sh"
        if job is None or not job.script:
            # scontrol prints the failure but keeps exit code 0
            # (slurm://src/scontrol/scontrol.c#"batch_script").
            print("job script retrieval failed: Invalid job id specified", file=sys.stderr)
            return ""
        if target == "-":
            return job.script
        Path(target).write_text(job.script, encoding="utf-8")
        return f"batch script for job {args[1]} written to {target}\n"

    def _ping(self) -> str:
        """slurm://src/scontrol/scontrol.c#_print_ping (one primary controller)."""
        return "Slurmctld(primary) at localhost is UP\n"

    def _json(self, records: list[Reservation], now: datetime) -> str:
        """``scontrol --json show reservation`` (OPENAPI_RESERVATION_RESP via DATA_DUMP_CLI)."""
        release = current()
        body = {
            "reservations": [reservation_to_dict(r) for r in records],
            "last_update": uint_no_val(int(now.timestamp())),
            "meta": {
                "plugin": {
                    "type": "",
                    "name": "",
                    "data_parser": f"data_parser/{release.api_version}",
                },
                "command": ["show", "reservation"],
                "slurm": {
                    "version": release.version_parts,
                    "release": release.release,
                    "cluster": self.database.current_cluster,
                },
            },
            "errors": [],
            "warnings": [],
        }
        return json.dumps(body, indent=2) + "\n"


def _time_str(value: Optional[datetime]) -> str:
    """slurm_make_time_str() default ``%FT%T`` format; 0 and INFINITE print ``Unknown``."""
    if not value or value == resv_core.INFINITE_TIME:
        return "Unknown"
    return value.strftime("%Y-%m-%dT%H:%M:%S")


def secs2time_str(seconds: int) -> str:
    """slurm://src/common/parse_time.c#secs2time_str."""
    if seconds < 0:
        return "INVALID"
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}-{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def sprint_reservation(resv: Reservation, now: datetime, one_liner: bool = False) -> str:
    """slurm://src/api/reservation_info.c#slurm_sprint_reservation_info.

    Unset strings print as ``(null)``, as ``xstrfmtcat("%s", NULL)`` does.
    """

    def s(value: str) -> str:
        return value or "(null)"

    sep = " " if one_liner else "\n   "
    if resv.start_time and resv.end_time and resv.end_time >= resv.start_time:
        duration = secs2time_str(int((resv.end_time - resv.start_time).total_seconds()))
    else:
        duration = "N/A"
    flags = ",".join(resv_core.order_flags(resv.flags, resv_core.FLAG_STRING_ORDER))
    nodes = len(resv.nodes)
    lines = [
        f"ReservationName={resv.name} StartTime={_time_str(resv.start_time)} "
        f"EndTime={_time_str(resv.end_time)} Duration={duration}",
        f"Nodes={s(compress_hostlist(resv.nodes))} NodeCnt={nodes} CoreCnt={nodes * NODE_CPUS} "
        f"Features=(null) PartitionName={s(resv.partition)} Flags={flags}",
        f"TRES={f'cpu={nodes * NODE_CPUS}' if nodes else '(null)'}",
    ]
    if at_least("25.11"):
        lines.append("AllowedPartitions=(null) QOS=(null)")
    active = bool(resv.start_time and resv.end_time and resv.start_time <= now <= resv.end_time)
    lines.append(
        f"Users={s(resv.users)} Groups={s(resv.groups)} Accounts={s(resv.accounts)} "
        f"Licenses=(null) State={'ACTIVE' if active else 'INACTIVE'} BurstBuffer=(null)"
    )
    lines.append("MaxStartDelay=(null)")
    if resv.comment:
        lines.append(f"Comment={resv.comment}")
    return sep.join(lines) + ("\n" if one_liner else "\n\n")
