"""scancel emulator.

Follows slurm://src/scancel/scancel.c#_proc_cluster without ``--ctld``: the
active jobs are loaded, job ids are checked first (an unknown or finished id
prints ``scancel: error: Kill job error on job id N: Invalid job id
specified`` and makes the exit code 1, slurm://src/scancel/scancel.c#_verify_job_ids),
the filters (``-A``, ``-u``, ``-n``, ``-p``, ``-q``, ``-R``, ``-t``, ``-w``)
must all match, and each remaining job is signalled. Only the owner or root
may signal a job (slurm://src/slurmctld/job_mgr.c#"Verify that the user can kill the requested job");
anyone else gets ``Access/permission denied``. SIGKILL -- the default --
cancels a job and records the caller as ``requid``; other signals (``-s``)
go to processes the emulator does not run. Without any job id or filter
scancel refuses with ``No job identification provided``
(slurm://src/scancel/opt.c#"No job identification provided").

Success is silent; ``-v`` adds the ``Terminating job N`` /
``Signal N to job N`` lines (slurm://src/scancel/scancel.c#_log_signal_job_msg).
"""

from __future__ import annotations

import sys
from typing import Callable, Optional

from emulator.core import nss
from emulator.core.cluster_model import expand_hostlist
from emulator.core.database import Job, SlurmDatabase
from emulator.core.scheduler import advance_job_states, job_clock_now
from emulator.core.time_engine import TimeEngine

_SIGNALS = {
    "HUP": 1,
    "INT": 2,
    "QUIT": 3,
    "ABRT": 6,
    "KILL": 9,
    "USR1": 10,
    "USR2": 12,
    "ALRM": 14,
    "TERM": 15,
    "CONT": 18,
    "STOP": 19,
}
_ACTIVE = ("PENDING", "RUNNING", "SUSPENDED")
_STATE_NAMES = {"PD": "PENDING", "R": "RUNNING", "S": "SUSPENDED"}

# (short, long) options that take a value.
_VALUE_OPTS = {
    "-A": "account",
    "--account": "account",
    "-M": "clusters",
    "--clusters": "clusters",
    "--cluster": "clusters",
    "-n": "name",
    "--name": "name",
    "--jobname": "name",
    "-p": "partition",
    "--partition": "partition",
    "-q": "qos",
    "--qos": "qos",
    "-R": "reservation",
    "--reservation": "reservation",
    "-s": "signal",
    "--signal": "signal",
    "-t": "state",
    "--state": "state",
    "-u": "user",
    "--user": "user",
    "-w": "nodelist",
    "--nodelist": "nodelist",
    "--wckey": "wckey",
}
_FLAG_OPTS = {
    "-b",
    "--batch",
    "-c",
    "--cron",
    "-f",
    "--full",
    "-H",
    "--hurry",
    "-i",
    "--interactive",
    "-Q",
    "--quiet",
    "-v",
    "--verbose",
    "--me",
    "--ctld",
    "--sibling",
}


def _signal_number(text: str) -> Optional[int]:
    value = text.strip().upper().removeprefix("SIG")
    if value.isdigit():
        return int(value)
    return _SIGNALS.get(value)


class ScancelEmulator:
    def __init__(self, database: SlurmDatabase, time_engine: TimeEngine):
        self.database = database
        self.time_engine = time_engine
        self.exit_code = 0
        self.caller = "root"

    def _error(self, message: str) -> None:
        print(f"scancel: error: {message}", file=sys.stderr)

    def _parse(self, args: list[str]) -> Optional[tuple[dict[str, str], set[str], list[str]]]:
        values: dict[str, str] = {}
        flags: set[str] = set()
        ids: list[str] = []
        i = 0
        while i < len(args):
            arg = args[i]
            key, eq, inline = arg.partition("=")
            if arg.startswith("--") and eq and key in _VALUE_OPTS:
                values[_VALUE_OPTS[key]] = inline
            elif arg in _VALUE_OPTS:
                if i + 1 >= len(args):
                    self._error(f"option requires an argument -- '{arg.lstrip('-')}'")
                    return None
                values[_VALUE_OPTS[arg]] = args[i + 1]
                i += 1
            elif len(arg) > 2 and arg[:2] in _VALUE_OPTS and not arg.startswith("--"):
                values[_VALUE_OPTS[arg[:2]]] = arg[2:]
            elif arg in _FLAG_OPTS:
                flags.add(arg)
            elif arg.startswith("-"):
                self._error(f"unrecognized option '{arg}'")
                return None
            else:
                ids.append(arg)
            i += 1
        return values, flags, ids

    def handle_command(self, args: list[str]) -> str:
        self.exit_code = 0
        parsed = self._parse(args)
        if parsed is None:
            self.exit_code = 1
            return ""
        values, flags, ids = parsed
        if "--me" in flags:
            values["user"] = self.caller
        signal = 9
        if "signal" in values:
            number = _signal_number(values["signal"])
            if number is None:
                self._error(f"Invalid job signal: {values['signal']}")
                self.exit_code = 1
                return ""
            signal = number
        if not ids and not (set(values) - {"signal", "clusters"}):
            self._error("No job identification provided")
            self.exit_code = 1
            return ""

        self.database.load_state()
        if advance_job_states(self.database, self.time_engine):
            self.database.save_state()
        active = [j for j in self.database.jobs.values() if j.state in _ACTIVE]
        targets = self._verify_ids(ids, active)
        matches = self._filter(values)
        selected = [j for j in active if (not ids or j in targets) and matches(j)]
        verbose = "-v" in flags or "--verbose" in flags
        for job in sorted(selected, key=lambda j: int(j.job_id or 0)):
            if self.caller not in ("root", job.user):
                self._error(f"Kill job error on job id {job.job_id}: Access/permission denied")
                self.exit_code = 1
                continue
            if verbose:
                verb = f"Signal {signal} to job" if "signal" in values else "Terminating job"
                print(f"scancel: {verb} {job.job_id}", file=sys.stderr)
            if signal == 9:
                self._cancel(job)
        self.database.save_state()
        return ""

    def _verify_ids(self, ids: list[str], active: list[Job]) -> list[Job]:
        found: list[Job] = []
        by_id = {str(j.job_id): j for j in active}
        for raw in ids:
            job_id = raw.split(".")[0].split("_")[0]
            job = by_id.get(job_id)
            if job is None:
                self._error(f"Kill job error on job id {raw}: Invalid job id specified")
                self.exit_code = 1
                continue
            found.append(job)
        return found

    def _filter(self, values: dict[str, str]) -> Callable[[Job], bool]:
        def csv(key: str) -> Optional[set[str]]:
            return {v for v in values[key].split(",") if v} if key in values else None

        accounts = {a.lower() for a in csv("account") or set()} or None
        users = csv("user")
        names = csv("name")
        partitions = csv("partition")
        qos = csv("qos")
        reservation = values.get("reservation")
        state = values.get("state")
        nodes = set(expand_hostlist(values["nodelist"])) if "nodelist" in values else None
        clusters = csv("clusters")
        if state is not None:
            state = _STATE_NAMES.get(state.upper(), state.upper())

        def matches(job: Job) -> bool:
            uid = str(job.uid) if job.uid is not None else None
            return (
                (accounts is None or (job.account or "").lower() in accounts)
                and (users is None or job.user in users or (uid is not None and uid in users))
                and (names is None or job.name in names)
                and (partitions is None or job.partition in partitions)
                and (qos is None or (job.qos or "normal") in qos)
                and (reservation is None or job.reservation == reservation)
                and (state is None or job.state == state)
                and (nodes is None or bool(nodes.intersection(job.node_list)))
                and (clusters is None or "all" in clusters or job.cluster in clusters)
            )

        return matches

    def _cancel(self, job: Job) -> None:
        job.state = "CANCELLED"
        job.state_reason, job.state_description = "None", ""
        identity = nss.lookup(self.caller)
        job.requid = 0 if self.caller == "root" else (identity.uid if identity else 1000)
        job.end_time = job_clock_now(self.time_engine)
        advance_job_states(self.database, self.time_engine)
