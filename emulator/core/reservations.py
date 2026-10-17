"""Reservation writes: create, update and delete, as slurmctld does them.

Shared by the slurmrestd plane (``POST /reservation``, ``POST /reservations/``,
``DELETE /reservation/{name}``) and ``scontrol create|update|delete
reservation``, so both refuse the same requests with the same errno. The
checks follow, in order:

* the client-side request check every creator runs first
  (slurm://src/common/slurm_protocol_defs.c#validate_resv_create_desc@25.05+;
  inline in slurm://src/scontrol/create_res.c#scontrol_create_res@24.11):
  start time required, end time or duration required, they must agree,
  a partition alone means its whole node set, users/groups/accounts
  required -- refused with a message, no errno;
* ``create_resv`` (slurm://src/slurmctld/reservation.c#create_resv): start
  or end more than ``MAX_RESV_DELAY`` (600 s) in the past ->
  ``ESLURM_INVALID_TIME_VALUE``; unknown partition ->
  ``ESLURM_INVALID_PARTITION_NAME``; users and groups together ->
  ``ESLURM_RESERVATION_USER_GROUP``; no users/groups/accounts ->
  ``ESLURM_RESERVATION_EMPTY``; unknown account (only with
  ``AccountingStorageEnforce=associations``,
  slurm://src/slurmctld/reservation.c#_is_account_valid) ->
  ``ESLURM_INVALID_ACCOUNT``; unknown or empty node list ->
  ``ESLURM_INVALID_NODE_NAME``; nodes already in another reservation for an
  overlapping time, unless either side is MAINT or OVERLAP
  (slurm://src/slurmctld/reservation.c#_resv_overlap) ->
  ``ESLURM_RESERVATION_OVERLAP``; a running job on the nodes that ends after
  the reservation starts, unless IGNORE_JOBS
  (slurm://src/slurmctld/reservation.c#_job_overlap) -> ``ESLURM_NODES_BUSY``;
  duplicate name -> ``ESLURM_RESERVATION_NAME_DUP``. A NodeCnt request picks
  free nodes of the partition (default partition when none is named), lowest
  index first, and with IGNORE_JOBS falls back to busy ones
  (slurm://src/slurmctld/reservation.c#_pick_node_cnt). No name generates
  ``<first account|user|group|"resv">_<n>``
  (slurm://src/slurmctld/reservation.c#_generate_resv_name);
* ``update_resv`` (slurm://src/slurmctld/reservation.c#update_resv): unknown
  name -> ``ESLURM_RESERVATION_INVALID``; moving the start of a started
  reservation -> ``ESLURM_RSV_ALREADY_STARTED``; start/end more than 60 s in
  the past or start >= end -> ``ESLURM_INVALID_TIME_VALUE``; then the same
  overlap and running-job checks as create. ``+name``/``-name`` items add to
  or remove from the users/accounts lists
  (slurm://src/slurmctld/reservation.c#_update_account_list); ``NO_<flag>``
  clears a flag;
* ``delete_resv`` (slurm://src/slurmctld/reservation.c#delete_resv): unknown
  name -> ``ESLURM_RESERVATION_INVALID``; a job that has not finished still
  using it -> ``ESLURM_RESERVATION_BUSY``
  (slurm://src/slurmctld/reservation.c#_is_resv_used).

Records made here carry ``origin="api"`` and survive the
``SLURM_EMULATOR_RESERVATIONS`` sync.

Intentional deviations:

* No recurrence: HOURLY/DAILY/WEEKDAY/WEEKEND/WEEKLY are stored and shown but
  the window never repeats; TIME_FLOAT does not float; FLEX/MAGNETIC do not
  change which jobs may use the reservation.
* No core, TRES, license, burst-buffer, feature, QOS or AllowedPartitions
  reservations: whole nodes only, users/groups/accounts as the access list.
* Users are checked against NSS only in NSS mode
  (slurm://src/slurmctld/reservation.c#_build_uid_list refuses an unknown
  user with ``ESLURM_USER_ID_MISSING``); without it every name is accepted,
  as every user is uid 1000 there.
* A group list is stored as given; members are not resolved.
"""

from __future__ import annotations

import copy
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from emulator.core import cluster_model, nss
from emulator.core.accounting import ESLURM_INVALID_ACCOUNT, associations_enforced
from emulator.core.database import Reservation, SlurmDatabase
from emulator.core.scheduler import expected_end
from emulator.slurm_version import at_least

# Error numbers from slurm://slurm/slurm_errno.h (slurmctld block from 2000).
ESLURM_INVALID_PARTITION_NAME = 2000
ESLURM_USER_ID_MISSING = 2010
ESLURM_NODES_BUSY = 2016
ESLURM_INVALID_NODE_NAME = 2018
ESLURM_RESERVATION_INVALID = 2053
ESLURM_INVALID_TIME_VALUE = 2054
ESLURM_RESERVATION_BUSY = 2055
ESLURM_RESERVATION_OVERLAP = 2058
ESLURM_RESERVATION_EMPTY = 2080
ESLURM_RESERVATION_NAME_DUP = 2082
ESLURM_RSV_ALREADY_STARTED = 2104
ESLURM_RESERVATION_USER_GROUP = 2129

# slurm_strerror() texts (slurm://src/common/slurm_errno.c#ESLURM_RESERVATION_INVALID).
STRERROR = {
    ESLURM_INVALID_PARTITION_NAME: "Invalid partition name specified",
    ESLURM_USER_ID_MISSING: "Invalid user id",
    ESLURM_NODES_BUSY: "Requested nodes are busy",
    ESLURM_INVALID_NODE_NAME: "Invalid node name specified",
    ESLURM_RESERVATION_INVALID: "Requested reservation is invalid",
    ESLURM_INVALID_TIME_VALUE: "Invalid time specified",
    ESLURM_RESERVATION_BUSY: "Requested reservation is in use",
    ESLURM_RESERVATION_OVERLAP: "Requested reservation overlaps with another reservation",
    ESLURM_RESERVATION_EMPTY: "Reservation request lacks users, groups or accounts",
    ESLURM_RESERVATION_NAME_DUP: "Duplicate reservation name",
    ESLURM_RSV_ALREADY_STARTED: "Reservation already started",
    ESLURM_RESERVATION_USER_GROUP: (
        "Reservations can't have users and groups specified, only one or the other"
    ),
    ESLURM_INVALID_ACCOUNT: "Invalid account or account/partition combination specified",
}

# slurm://src/slurmctld/reservation.c#MAX_RESV_DELAY
MAX_RESV_DELAY = 600
YEAR = timedelta(days=365)
# Duration value meaning INFINITE (slurm://slurm/slurm.h#INFINITE).
DURATION_INFINITE = -1

# RESERVATION_FLAGS names in data_parser order (the REST flag list order,
# slurm://src/plugins/data_parser/v0.0.45/parsers.c#RESERVATION_FLAGS@26.05+).
FLAG_NAMES = (
    "MAINT", "NO_MAINT", "DAILY", "NO_DAILY", "WEEKLY", "NO_WEEKLY",
    "IGNORE_JOBS", "NO_IGNORE_JOBS", "ANY_NODES", "NO_ANY_NODES", "STATIC",
    "NO_STATIC", "PART_NODES", "NO_PART_NODES", "OVERLAP", "SPEC_NODES",
    "TIME_FLOAT", "REPLACE", "ALL_NODES", "PURGE_COMP", "WEEKDAY", "NO_WEEKDAY",
    "WEEKEND", "NO_WEEKEND", "FLEX", "NO_FLEX", "DURATION_PLUS", "DURATION_MINUS",
    "NO_HOLD_JOBS_AFTER_END", "REPLACE_DOWN", "NO_PURGE_COMP", "MAGNETIC",
    "NO_MAGNETIC", "SKIP", "HOURLY", "NO_HOURLY", "USER_DELETE", "FORCE_START",
    "NO_USER_DELETE", "REOCCURRING", "TRES_PER_NODE",
)  # fmt: skip

# Order scontrol prints them in (slurm://src/common/slurm_protocol_defs.c#reservation_flags_string).
FLAG_STRING_ORDER = (
    "MAINT", "NO_MAINT", "FLEX", "OVERLAP", "IGNORE_JOBS", "HOURLY", "NO_HOURLY",
    "DAILY", "NO_DAILY", "WEEKDAY", "WEEKEND", "WEEKLY", "NO_WEEKLY", "SPEC_NODES",
    "ALL_NODES", "ANY_NODES", "NO_ANY_NODES", "STATIC", "NO_STATIC", "PART_NODES",
    "NO_PART_NODES", "TIME_FLOAT", "REPLACE", "REPLACE_DOWN", "PURGE_COMP",
    "NO_HOLD_JOBS_AFTER_END", "MAGNETIC", "NO_MAGNETIC", "USER_DELETE",
    "NO_USER_DELETE", "FORCE_START",
)  # fmt: skip

# Flags create_resv keeps from the request; the rest are dropped
# (slurm://src/slurmctld/reservation.c#create_resv).
_CREATE_FLAGS = {
    "MAINT", "FLEX", "OVERLAP", "IGNORE_JOBS", "HOURLY", "DAILY", "WEEKDAY",
    "WEEKEND", "WEEKLY", "FORCE_START", "STATIC", "ANY_NODES", "PART_NODES",
    "TIME_FLOAT", "PURGE_COMP", "REPLACE", "REPLACE_DOWN", "NO_HOLD_JOBS_AFTER_END",
    "MAGNETIC", "USER_DELETE", "TRES_PER_NODE",
}  # fmt: skip
# Request-only flags that never stick to the record.
_TRANSIENT_FLAGS = {"DURATION_PLUS", "DURATION_MINUS", "SKIP", "REOCCURRING", "TRES_PER_NODE"}


class ReservationError(Exception):
    """slurmctld refused the request with ``errno`` (and maybe a message)."""

    def __init__(self, errno: int, message: str = ""):
        super().__init__(message or strerror(errno))
        self.errno = errno
        self.message = message


class ReservationDescError(Exception):
    """validate_resv_create_desc() refused the request (message, no errno)."""


def strerror(errno: int) -> str:
    return STRERROR.get(errno, f"Unknown error {errno}")


@dataclass
class ReservationDesc:
    """A create/update request (subset of slurm://slurm/slurm.h#resv_desc_msg_t).

    ``None`` = not given (NO_VAL / NULL). ``flags`` are RESERVATION_FLAGS
    names; ``duration`` is minutes or ``DURATION_INFINITE``.
    """

    name: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration: Optional[int] = None
    node_list: Optional[str] = None
    node_cnt: Optional[int] = None
    partition: Optional[str] = None
    flags: Optional[list[str]] = None
    users: Optional[str] = None
    accounts: Optional[str] = None
    groups: Optional[str] = None
    comment: Optional[str] = None
    extra: dict = field(default_factory=dict)

    def has_flag(self, flag: str) -> bool:
        return bool(self.flags) and flag in self.flags  # type: ignore[operator]

    def add_flag(self, flag: str) -> None:
        if self.flags is None:
            self.flags = []
        if flag not in self.flags:
            self.flags.append(flag)


def _filled(value: Optional[str]) -> bool:
    return bool(value)


def validate_create_desc(desc: ReservationDesc) -> None:
    """Refuse a create request the way every Slurm client checks it first.

    slurm://src/common/slurm_protocol_defs.c#validate_resv_create_desc@25.05+
    (slurm://src/scontrol/create_res.c#scontrol_create_res@24.11 before). The
    access-list message names AllowedPartitions and QOS from 25.11
    (slurm://src/common/slurm_protocol_defs.c#"Either Users/Groups, AllowedPartitions, QOS and/or Accounts"@25.11+);
    24.11 has no FORCE_START check. Mutates ``desc`` like the C code: a
    partition with ``Nodes=ALL`` or with no node specification at all
    reserves the whole partition (PART_NODES + ``ALL``).
    """
    message = _create_desc_problem(desc)
    if message:
        raise ReservationDescError(message)


_NOT_CREATED = "  No reservation created."


def _create_desc_problem(desc: ReservationDesc) -> str:
    if desc.start_time is None:
        return "A start time must be given." + _NOT_CREATED
    if desc.end_time is None and desc.duration is None:
        return "An end time or duration must be given." + _NOT_CREATED
    if (
        desc.end_time is not None
        and desc.duration is not None
        and desc.duration != DURATION_INFINITE
        and desc.start_time + timedelta(minutes=desc.duration) != desc.end_time
    ):
        return "StartTime + Duration does not equal EndTime." + _NOT_CREATED
    if desc.end_time is not None and desc.start_time > desc.end_time:
        return "Start time cannot be after end time." + _NOT_CREATED
    if desc.partition and desc.node_list and desc.node_list.upper() == "ALL":
        desc.add_flag("PART_NODES")
    if desc.has_flag("PART_NODES") and (
        not desc.partition or (desc.node_list or "").upper() != "ALL"
    ):
        return "PART_NODES flag requires specifying a Partition and ALL nodes." + _NOT_CREATED
    recurring = ("HOURLY", "DAILY", "WEEKDAY", "WEEKEND", "WEEKLY")
    if (
        at_least("25.05")
        and desc.has_flag("FORCE_START")
        and not any(desc.has_flag(f) for f in recurring)
    ):
        # The only message with a single space before "No reservation created."
        return "FORCE_START flag requires a reoccuring reservation. No reservation created."
    if not desc.node_cnt and not _filled(desc.node_list):
        if desc.partition is None:
            return "CoreCnt, Nodes, NodeCnt, TRES or Watts must be specified." + _NOT_CREATED
        desc.add_flag("PART_NODES")
        desc.node_list = "ALL"
    if not (_filled(desc.users) or _filled(desc.groups) or _filled(desc.accounts)):
        who = "Users/Groups, AllowedPartitions, QOS" if at_least("25.11") else "Users/Groups"
        return f"Either {who} and/or Accounts must be specified." + _NOT_CREATED
    if desc.users and desc.groups:
        return (
            "Users and Groups are mutually exclusive.  You can have one or the other, "
            "but not both." + _NOT_CREATED
        )
    return ""


# --- helpers ---


def order_flags(flags: list[str], order: tuple[str, ...] = FLAG_NAMES) -> list[str]:
    rank = {name: i for i, name in enumerate(order)}
    return sorted(dict.fromkeys(flags), key=lambda f: rank.get(f, len(rank)))


def find(db: SlurmDatabase, name: Optional[str]) -> Optional[Reservation]:
    """Exact-name lookup (slurm://src/slurmctld/reservation.c#_find_resv_name)."""
    return db.reservations.get(name) if name else None


def _overlaps(start: datetime, end: datetime, other: Reservation) -> bool:
    if other.start_time is None or other.end_time is None:
        return False
    return start < other.end_time and other.start_time < end


def _resv_overlap(
    db: SlurmDatabase,
    flags: list[str],
    nodes: list[str],
    start: datetime,
    end: datetime,
    skip: Optional[str] = None,
) -> bool:
    """slurm://src/slurmctld/reservation.c#_resv_overlap."""
    if "MAINT" in flags or "OVERLAP" in flags or not nodes:
        return False
    wanted = set(nodes)
    for other in db.reservations.values():
        if other.name == skip or "MAINT" in other.flags or "OVERLAP" in other.flags:
            continue
        if wanted.intersection(other.nodes) and _overlaps(start, end, other):
            return True
    return False


def _busy_nodes(db: SlurmDatabase, start: datetime, resv_name: Optional[str] = None) -> set[str]:
    """Nodes of running jobs that end after ``start`` (slurm://src/slurmctld/reservation.c#_job_overlap).

    Jobs already running inside ``resv_name`` do not count.
    """
    busy: set[str] = set()
    for job in db.jobs.values():
        if job.state != "RUNNING" or not job.node_list:
            continue
        if resv_name is not None and job.reservation == resv_name:
            continue
        end = expected_end(job)
        if end is None or end > start:
            busy.update(job.node_list)
    return busy


def _resolve_nodes(node_list: str, partition: str, part_nodes: bool) -> tuple[list[str], str]:
    """(node names, extra flag) for a ``Nodes=`` value; raises on unknown nodes."""
    if node_list.upper() == "ALL":
        if partition and part_nodes:
            return cluster_model.partition_nodes(partition), ""
        return cluster_model.all_nodes(), "ALL_NODES"
    known = set(cluster_model.all_nodes())
    names = cluster_model.expand_hostlist(node_list)
    if not names or any(n not in known for n in names):
        # node_name2bitmap() fails on an unknown name.
        raise ReservationError(ESLURM_INVALID_NODE_NAME)
    order = {n: i for i, n in enumerate(cluster_model.all_nodes())}
    return sorted(dict.fromkeys(names), key=order.__getitem__), ""


def _default_partition() -> str:
    return next(iter(cluster_model.partition_ranges()), "")


def _pick_node_cnt(
    db: SlurmDatabase,
    count: int,
    partition: str,
    flags: list[str],
    start: datetime,
    end: datetime,
    skip: Optional[str] = None,
) -> list[str]:
    """slurm://src/slurmctld/reservation.c#_pick_node_cnt (whole nodes, lowest index first)."""
    overrides = cluster_model.node_state_overrides()
    pool = [n for n in cluster_model.partition_nodes(partition) if n not in overrides]
    if "MAINT" not in flags and "OVERLAP" not in flags:
        taken = {
            n
            for r in db.reservations.values()
            if r.name != skip and "MAINT" not in r.flags and "OVERLAP" not in r.flags
            and _overlaps(start, end, r)
            for n in r.nodes
        }  # fmt: skip
        pool = [n for n in pool if n not in taken]
    count = min(count, len(pool))  # "Reducing requested node count."
    busy = _busy_nodes(db, start, skip)
    free = [n for n in pool if n not in busy]
    if len(free) >= count:
        return free[:count]
    if "IGNORE_JOBS" in flags:
        return (free + [n for n in pool if n in busy])[:count]
    raise ReservationError(ESLURM_NODES_BUSY)


def _split_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _apply_list(current: str, value: str) -> str:
    """Replace a CSV list, or add/remove ``+x``/``-x`` items."""
    items = _split_list(value)
    if items and all(item[0] in "+-" for item in items):
        result = _split_list(current)
        for item in items:
            name = item[1:]
            if item[0] == "+" and name not in result:
                result.append(name)
            elif item[0] == "-" and name in result:
                result.remove(name)
        return ",".join(result)
    return ",".join(items)


def _check_access(db: SlurmDatabase, users: str, accounts: str) -> None:
    for account in _split_list(accounts):
        name = account.lstrip("-")
        if associations_enforced() and db.get_account(name) is None:
            raise ReservationError(ESLURM_INVALID_ACCOUNT)
    if nss.enabled():
        for user in _split_list(users):
            if nss.resolve(user.lstrip("-")) is None:
                raise ReservationError(ESLURM_USER_ID_MISSING)


def _generate_name(db: SlurmDatabase, desc: ReservationDesc) -> str:
    """slurm://src/slurmctld/reservation.c#_generate_resv_name."""
    key = desc.accounts or desc.users or desc.groups or "resv"
    key = key.lstrip("-").split(",", 1)[0]
    suffix = 1
    while f"{key}_{suffix}" in db.reservations:
        suffix += 1
    return f"{key}_{suffix}"


# --- create / update / delete ---


def create_reservation(db: SlurmDatabase, desc: ReservationDesc, now: datetime) -> Reservation:
    """slurm://src/slurmctld/reservation.c#create_resv. Stores and returns the record."""
    flags = [f for f in (desc.flags or []) if f in _CREATE_FLAGS and f not in _TRANSIENT_FLAGS]
    slack = timedelta(seconds=MAX_RESV_DELAY)

    start = desc.start_time
    if start is None:
        start = now.replace(microsecond=0)
    elif start < now - slack and "TIME_FLOAT" not in flags:
        raise ReservationError(ESLURM_INVALID_TIME_VALUE)
    if "TIME_FLOAT" in flags and start < now:
        start = now.replace(microsecond=0)

    if desc.end_time is not None:
        if desc.end_time < now - slack:
            raise ReservationError(ESLURM_INVALID_TIME_VALUE)
        end = desc.end_time
    elif desc.duration == DURATION_INFINITE:
        end = start + YEAR
    elif desc.duration:
        end = start + timedelta(minutes=desc.duration)
    else:
        end = start + YEAR

    partition = desc.partition or ""
    if partition and partition not in cluster_model.partition_ranges():
        raise ReservationError(ESLURM_INVALID_PARTITION_NAME)
    if "PART_NODES" in flags and not partition:
        raise ReservationError(ESLURM_INVALID_PARTITION_NAME)
    if "PART_NODES" in flags and (desc.node_list or "").upper() != "ALL":
        raise ReservationError(ESLURM_INVALID_NODE_NAME)

    if desc.users and desc.groups:
        raise ReservationError(ESLURM_RESERVATION_USER_GROUP)
    if not (desc.accounts or desc.users or desc.groups):
        raise ReservationError(ESLURM_RESERVATION_EMPTY)
    _check_access(db, desc.users or "", desc.accounts or "")

    if desc.node_list:
        flags.append("SPEC_NODES")
        nodes, extra = _resolve_nodes(desc.node_list, partition, "PART_NODES" in flags)
        if extra:
            flags = [f for f in flags if f != "PART_NODES"] + [extra]
        elif desc.node_list.upper() != "ALL":
            flags = [f for f in flags if f != "PART_NODES"]
        if not nodes:
            raise ReservationError(ESLURM_INVALID_NODE_NAME)
        if _resv_overlap(db, flags, nodes, start, end):
            raise ReservationError(ESLURM_RESERVATION_OVERLAP)
        if "IGNORE_JOBS" not in flags and set(nodes) & _busy_nodes(db, start):
            raise ReservationError(ESLURM_NODES_BUSY)
    elif "ANY_NODES" not in flags:
        flags = [f for f in flags if f != "PART_NODES"]
        if not desc.node_cnt:
            raise ReservationError(ESLURM_INVALID_NODE_NAME)
        # The default partition is assumed and recorded
        # (slurm://src/slurmctld/reservation.c#_validate_and_set_partition).
        partition = partition or _default_partition()
        nodes = _pick_node_cnt(db, desc.node_cnt, partition, flags, start, end)
    else:
        nodes = []

    if desc.name:
        if desc.name in db.reservations:
            raise ReservationError(ESLURM_RESERVATION_NAME_DUP)
        name = desc.name
    else:
        name = _generate_name(db, desc)

    resv = Reservation(
        name=name,
        nodes=nodes,
        partition=partition,
        start_time=start,
        end_time=end,
        flags=order_flags(flags),
        users=",".join(_split_list(desc.users or "")),
        accounts=",".join(_split_list(desc.accounts or "")),
        origin="api",
        groups=",".join(_split_list(desc.groups or "")),
        comment=desc.comment or "",
        duration=None if desc.end_time is not None else desc.duration,
    )
    db.reservations[name] = resv
    return resv


def _update_flags(current: list[str], requested: list[str]) -> list[str]:
    flags = list(current)
    for flag in requested:
        if flag in _TRANSIENT_FLAGS or flag in ("TIME_FLOAT", "FORCE_START"):
            continue
        if flag.startswith("NO_") and flag != "NO_HOLD_JOBS_AFTER_END":
            base = "IGNORE_JOBS" if flag == "NO_IGNORE_JOBS" else flag[3:]
            flags = [f for f in flags if f != base]
        elif flag not in flags:
            flags.append(flag)
    return order_flags(flags)


def update_reservation(db: SlurmDatabase, desc: ReservationDesc, now: datetime) -> Reservation:
    """slurm://src/slurmctld/reservation.c#update_resv. Applies all or nothing."""
    current = find(db, desc.name)
    if current is None:
        raise ReservationError(ESLURM_RESERVATION_INVALID)
    resv = copy.deepcopy(current)
    slack = timedelta(seconds=60)

    if desc.flags is not None:
        resv.flags = _update_flags(resv.flags, desc.flags)
    if desc.partition is not None:
        if desc.partition == "":
            raise ReservationError(
                ESLURM_INVALID_PARTITION_NAME, "Reservation partition cannot be removed"
            )
        if desc.partition not in cluster_model.partition_ranges():
            raise ReservationError(ESLURM_INVALID_PARTITION_NAME)
        resv.partition = desc.partition
    if desc.accounts is not None:
        resv.accounts = _apply_list(resv.accounts, desc.accounts)
    if desc.groups is not None:
        resv.groups = _apply_list(resv.groups, desc.groups)
    if desc.users is not None:
        resv.users = _apply_list(resv.users, desc.users)
    if resv.users and resv.groups:
        raise ReservationError(ESLURM_RESERVATION_USER_GROUP)
    if desc.accounts is not None or desc.users is not None:
        _check_access(db, desc.users or "", desc.accounts or "")
    if not (resv.users or resv.accounts or resv.groups):
        raise ReservationError(ESLURM_RESERVATION_EMPTY)

    if desc.start_time is not None:
        if resv.start_time is not None and resv.start_time <= now:
            raise ReservationError(ESLURM_RSV_ALREADY_STARTED)
        if desc.start_time < now - slack:
            raise ReservationError(ESLURM_INVALID_TIME_VALUE)
        resv.start_time = desc.start_time
        if resv.duration is not None:
            resv.end_time = desc.start_time + timedelta(minutes=resv.duration)
    if desc.end_time is not None:
        if desc.end_time < now - slack:
            raise ReservationError(ESLURM_INVALID_TIME_VALUE)
        resv.end_time = desc.end_time
        resv.duration = None
    if desc.duration is not None and resv.start_time is not None:
        if desc.duration == DURATION_INFINITE:
            minutes = int(YEAR.total_seconds() // 60)
        else:
            base = resv.duration
            if base is None and resv.end_time is not None:
                base = int((resv.end_time - resv.start_time).total_seconds() // 60)
            if desc.extra.get("duration_op") == "+":
                minutes = (base or 0) + desc.duration
            elif desc.extra.get("duration_op") == "-":
                minutes = max((base or 0) - desc.duration, 0)
            else:
                minutes = desc.duration
        resv.duration = minutes
        resv.end_time = max(resv.start_time + timedelta(minutes=minutes), now)
    if resv.start_time and resv.end_time and resv.start_time >= resv.end_time:
        raise ReservationError(ESLURM_INVALID_TIME_VALUE)

    if desc.node_list is not None:
        nodes, extra = _resolve_nodes(desc.node_list, resv.partition, "PART_NODES" in resv.flags)
        resv.nodes = nodes
        resv.flags = [f for f in resv.flags if f != "ALL_NODES"]
        if extra:
            resv.flags = order_flags([*resv.flags, extra])
    elif desc.node_cnt:
        start = resv.start_time or now
        end = resv.end_time or start + YEAR
        partition = resv.partition or _default_partition()
        resv.nodes = _pick_node_cnt(db, desc.node_cnt, partition, resv.flags, start, end, resv.name)

    start = resv.start_time or now
    end = resv.end_time or start + YEAR
    if _resv_overlap(db, resv.flags, resv.nodes, start, end, skip=resv.name):
        raise ReservationError(ESLURM_RESERVATION_OVERLAP)
    if "IGNORE_JOBS" not in resv.flags and set(resv.nodes) & _busy_nodes(db, start, resv.name):
        raise ReservationError(ESLURM_NODES_BUSY)
    if desc.comment is not None:
        resv.comment = desc.comment

    db.reservations[resv.name] = resv
    return resv


def delete_reservation(db: SlurmDatabase, name: str) -> None:
    """slurm://src/slurmctld/reservation.c#delete_resv."""
    resv = find(db, name)
    if resv is None:
        raise ReservationError(ESLURM_RESERVATION_INVALID)
    finished = ("COMPLETED", "FAILED", "TIMEOUT", "CANCELLED")
    if any(j.reservation == name and j.state not in finished for j in db.jobs.values()):
        raise ReservationError(ESLURM_RESERVATION_BUSY)
    del db.reservations[name]


# --- request field parsers shared by REST and scontrol ---

_TIMESPEC_RE = re.compile(r"^(\d+-)?\d+(:\d+){0,2}$")


def time_str2mins(text: str) -> Optional[int]:
    """slurm://src/common/parse_time.c#time_str2mins: minutes, ``DURATION_INFINITE`` or None."""
    value = text.strip()
    if value.lower() in ("-1", "infinite", "unlimited"):
        return DURATION_INFINITE
    if not _TIMESPEC_RE.match(value):
        return None
    days = 0
    if "-" in value:
        head, value = value.split("-", 1)
        days = int(head)
        parts = [int(p) for p in value.split(":")]
        hours, minutes, seconds = [*parts, 0, 0, 0][:3]
    else:
        parts = [int(p) for p in value.split(":")]
        if len(parts) == 3:
            hours, minutes, seconds = parts
        else:
            hours, minutes, seconds = 0, parts[0], parts[1] if len(parts) > 1 else 0
    total = days * 86400 + hours * 3600 + minutes * 60 + seconds
    return -(-total // 60)
