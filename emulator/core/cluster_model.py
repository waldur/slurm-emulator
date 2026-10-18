"""Cluster occupancy model: node allocation, node states, reservations.

The scheduler (``emulator/core/scheduler.py``) asks this module where a job
can run; the slurmrestd node and reservation views ask it what each node is
doing. Everything here is derived from the JSON state plus a few env vars,
so the REST, SSH and CLI processes all compute the same picture.

What it models after real Slurm:

* **Allocation** (select/cons_tres): a job gets ``node_count`` nodes of its
  partition, each with enough free CPUs and memory for its per-node share;
  nodes that are DRAIN/DOWN, already full, or in a reservation the job is
  not part of are skipped, lowest node index first. CPUs = ``tasks`` x
  ``cpus_per_task`` when given, else every CPU of the allocated nodes
  (slurm://src/plugins/select/cons_tres/job_test.c#job_test@25.05+). Memory
  without ``memory_per_node`` follows a ``DefMemPerCPU`` of node memory / CPUs
  (slurm://src/common/read_config.c#"DefMemPerCPU").
* **Pending reasons** (``state_reason`` plus ``state_description``,
  slurm://src/common/job_state_reason.c#job_state_reason_string): not enough
  free nodes -> ``Resources``; a reservation the job does not request that
  covers every node (``ALL_NODES``), is the job's partition (``PART_NODES``)
  or is MAINT over the whole partition -> ``ReqNodeNotAvail`` described as
  "ReqNodeNotAvail, Reserved for maintenance"
  (slurm://src/slurmctld/reservation.c#job_test_resv returns
  ``ESLURM_RESERVATION_MAINT``,
  slurm://src/slurmctld/node_scheduler.c#"ReqNodeNotAvail, Reserved for maintenance");
  a requested reservation not active yet, or not open to the job ->
  ``Reservation``; every node of the job's reservation taken by an
  overlapping one -> ``ReqNodeNotAvail`` "Reserved for maintenance" /
  "Reserved for other reservations".
* **Reservations** block their nodes for every job that does not request
  them whenever ``[start, start + duration)`` of the job overlaps the
  reservation, where the duration is the job's time limit
  (slurm://src/slurmctld/reservation.c#job_test_resv,
  slurm://src/slurmctld/reservation.c#_get_job_duration). A job inside a
  reservation runs on its nodes (any node with ``ANY_NODES``, any node and
  any time with ``FLEX``) minus those of other overlapping reservations,
  unless its own is MAINT or (towards non-MAINT ones) OVERLAP. Only users
  and accounts the reservation names may use it
  (slurm://src/slurmctld/reservation.c#_valid_job_access_resv). A reservation
  is purged once it has ended and no job runs in it; pending jobs that asked
  for it are held with ``ReservationDeleted``
  (slurm://src/slurmctld/reservation.c#job_resv_check,
  slurm://src/slurmctld/reservation.c#_foreach_clear_job_resv). Nodes inside an
  active reservation carry the ``RESERVED`` state flag, and ``MAINTENANCE``
  for a MAINT one (slurm://src/plugins/data_parser/v0.0.45/parsers.c#NODE_STATES@26.05+).
* **Node view**: base state IDLE / MIXED / ALLOCATED from allocated CPUs, plus
  DRAIN / DOWN with a reason (slurm://src/plugins/data_parser/v0.0.45/parsers.c#NODE@26.05+);
  ``cpu_load`` is the 5-minute load average x 100 as slurmd reports it
  (slurm://src/slurmd/slurmd/get_mach_stat.c#get_cpu_load).

Intentional deviations (test fixture, not a scheduler):

* No backfill planning and no priorities beyond submit order: every
  scheduling pass walks pending jobs FIFO and starts each one that fits now.
* A node range (``nodes: "2-4"``) allocates the minimum; real Slurm may
  grant up to the maximum when nodes are free.
* A job without ``time_limit`` is checked against a reservation for its
  emulated run length, not the partition MaxTime/a year
  (slurm://src/slurmctld/reservation.c#_get_job_duration), because the
  emulator has no partition time limits and would otherwise never start
  such a job ahead of a maintenance window.
* ``SLURM_EMULATOR_BACKGROUND_LOAD`` occupies whole nodes on behalf of other
  tenants without any job rows -- a real cluster would list those jobs.
  The busy set is deterministic and rotates every 15 minutes of job-clock time.
* GPUs are charged (``gres/gpu`` TRES) only to jobs in partitions whose
  name contains ``gpu`` (``partition_has_gpus``); there is no GRES model.
* CPU load, free memory, CPU time and MaxRSS are synthetic: each job gets a
  deterministic CPU efficiency and memory fraction from its id
  (``synthetic_efficiency``), overridable per job through its environment.
  GPU jobs likewise get a GPU utilisation and GPU memory fraction
  (``synthetic_gpu_use``), what the NVML plugin would sample.
"""

from __future__ import annotations

import math
import os
import random
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from emulator.core.database import Job, Reservation, SlurmDatabase

# Standard node (matches usage_simulator / sacct / schemas).
NODE_CPUS = 64
NODE_MEM_MB = 512 * 1024
NODE_GPUS = 4
GPU_MEM_MB = 96 * 1024
DEF_MEM_PER_CPU_MB = NODE_MEM_MB // NODE_CPUS
# Memory the OS itself holds on an idle node (for ``free_mem``).
OS_MEM_MB = 3 * 1024

NODE_STATES_ENV = "SLURM_EMULATOR_NODE_STATES"
BACKGROUND_LOAD_ENV = "SLURM_EMULATOR_BACKGROUND_LOAD"
RESERVATIONS_ENV = "SLURM_EMULATOR_RESERVATIONS"
RUN_SECONDS_ENV = "SLURM_EMULATOR_RUN_SECONDS"
CPU_EFFICIENCY_ENV = "SLURM_EMULATOR_CPU_EFFICIENCY"
MEM_FRACTION_ENV = "SLURM_EMULATOR_MEM_FRACTION"

BACKGROUND_WINDOW_SECONDS = 900
LOAD_WINDOW_SECONDS = 300

# WAIT_NODE_NOT_AVAIL and the descriptions slurmctld gives it.
NODE_NOT_AVAIL = "ReqNodeNotAvail"
MAINT_DESC = "ReqNodeNotAvail, Reserved for maintenance"
OTHER_RESV_DESC = "ReqNodeNotAvail, Reserved for other reservations"


# --- hostlists ---


_HOST_RE = re.compile(r"^(?P<prefix>.*?)(?P<num>\d+)$")


def expand_hostlist(expr: str) -> list[str]:
    """``node[001-003,007],gpu01`` -> individual names (slurm hostlist syntax)."""
    names: list[str] = []
    for part in re.findall(r"[^,\[]+(?:\[[^\]]*\])?[^,]*", expr or ""):
        part = part.strip()
        if not part:
            continue
        match = re.match(r"^(?P<prefix>[^\[]*)\[(?P<body>[^\]]*)\](?P<suffix>.*)$", part)
        if not match:
            names.append(part)
            continue
        prefix, body, suffix = match["prefix"], match["body"], match["suffix"]
        for chunk in body.split(","):
            if "-" in chunk:
                first, last = chunk.split("-", 1)
                width = len(first)
                for i in range(int(first), int(last) + 1):
                    names.append(f"{prefix}{i:0{width}d}{suffix}")
            elif chunk:
                names.append(f"{prefix}{chunk}{suffix}")
    return names


def compress_hostlist(names: list[str]) -> str:
    """Inverse of ``expand_hostlist``: ``node005,node006,node008`` -> ``node[005-006,008]``.

    Ranged output as ``hostlist_ranged_string`` prints it
    (slurm://src/common/hostlist.c#hostlist_ranged_string); a single host
    stays bare.
    """
    groups: dict[tuple[str, int], list[int]] = {}
    order: list[tuple[str, int]] = []
    plain: list[str] = []
    for name in sorted(set(names), key=_host_sort_key):
        match = _HOST_RE.match(name)
        if not match:
            plain.append(name)
            continue
        key = (match["prefix"], len(match["num"]))
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(int(match["num"]))
    out: list[str] = []
    for prefix, width in order:
        nums = groups[(prefix, width)]
        if len(nums) == 1:
            out.append(f"{prefix}{nums[0]:0{width}d}")
            continue
        ranges: list[str] = []
        start = prev = nums[0]
        for num in [*nums[1:], None]:
            if num is not None and num == prev + 1:
                prev = num
                continue
            ranges.append(
                f"{start:0{width}d}" if start == prev else f"{start:0{width}d}-{prev:0{width}d}"
            )
            if num is not None:
                start = prev = num
        out.append(f"{prefix}[{','.join(ranges)}]")
    return ",".join(out + plain)


def _host_sort_key(name: str) -> tuple[str, int]:
    match = _HOST_RE.match(name)
    return (match["prefix"], int(match["num"])) if match else (name, -1)


# --- topology ---


def partition_ranges() -> dict[str, tuple[int, int]]:
    from emulator.api.slurmrestd.schemas import PARTITION_RANGES  # noqa: PLC0415 - cycle

    return PARTITION_RANGES


def all_nodes() -> list[str]:
    names: set[str] = set()
    for first, last in partition_ranges().values():
        names.update(f"node{i:03d}" for i in range(first, last + 1))
    return sorted(names)


def partition_nodes(partition: str) -> list[str]:
    """Nodes of ``partition``; an unknown partition schedules on every node."""
    rng = partition_ranges().get(partition)
    if rng is None:
        return all_nodes()
    return [f"node{i:03d}" for i in range(rng[0], rng[1] + 1)]


# --- static node state overrides ---


def node_state_overrides(spec: Optional[str] = None) -> dict[str, tuple[str, str]]:
    """``node012=DRAIN:Bad DIMM;node[030-031]=DOWN:Not responding`` -> {node: (state, reason)}."""
    if spec is None:
        spec = os.environ.get(NODE_STATES_ENV, "")
    out: dict[str, tuple[str, str]] = {}
    for entry in spec.split(";"):
        if "=" not in entry:
            continue
        hosts, _, rest = entry.partition("=")
        state, _, reason = rest.partition(":")
        state = state.strip().upper()
        if state not in ("DRAIN", "DOWN"):
            continue
        for host in expand_hostlist(hosts.strip()):
            out[host] = (state, reason.strip())
    return out


# --- background load ---


def background_fractions(spec: Optional[str] = None) -> dict[str, float]:
    """``batch=0.5,gpu=0.75`` -> {partition: fraction of its nodes busy}."""
    if spec is None:
        spec = os.environ.get(BACKGROUND_LOAD_ENV, "")
    out: dict[str, float] = {}
    for entry in spec.split(","):
        if "=" not in entry:
            continue
        name, _, value = entry.partition("=")
        try:
            out[name.strip()] = min(max(float(value), 0.0), 1.0)
        except ValueError:
            continue
    return out


def background_window(at: datetime) -> int:
    return int(at.timestamp()) // BACKGROUND_WINDOW_SECONDS


def next_background_boundary(at: datetime) -> datetime:
    nxt = (background_window(at) + 1) * BACKGROUND_WINDOW_SECONDS
    return datetime.fromtimestamp(nxt)  # noqa: DTZ006 - naive local, like the job clock


# --- reservations ---


_DURATION_RE = re.compile(r"^\+?(?P<num>\d+(?:\.\d+)?)(?P<unit>[smhd]?)$")
_UNIT_SECONDS = {"": 60, "s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(text: str) -> timedelta:
    """``+72h`` / ``12h`` / ``30m`` / ``2d`` / ``90`` (minutes) / ``now``."""
    text = text.strip().lower()
    if text in ("now", "+0", "0"):
        return timedelta(0)
    match = _DURATION_RE.match(text)
    if not match:
        raise ValueError(f"bad duration: {text!r}")
    return timedelta(seconds=float(match["num"]) * _UNIT_SECONDS[match["unit"]])


def parse_reservation_spec(entry: str, anchor: datetime) -> Optional[Reservation]:
    """``name:ALL|partition|hostlist:+offset:duration[:FLAGS[:users=a,b][:accounts=x]]``."""
    fields = [f.strip() for f in entry.split(":")]
    if len(fields) < 4 or not fields[0]:
        return None
    name, where, offset, duration = fields[:4]
    flags = (
        [f.strip().upper() for f in fields[4].split(",") if f.strip()] if len(fields) > 4 else []
    )
    users = accounts = ""
    for extra in fields[5:]:
        key, _, value = extra.partition("=")
        if key.strip().lower() == "users":
            users = value.strip()
        elif key.strip().lower() == "accounts":
            accounts = value.strip()
    try:
        start = anchor + parse_duration(offset)
        end = start + parse_duration(duration)
    except ValueError:
        return None
    partition = ""
    if where.upper() == "ALL":
        nodes = all_nodes()
    elif where in partition_ranges():
        nodes = partition_nodes(where)
        partition = where
    else:
        nodes = [n for n in expand_hostlist(where) if n in set(all_nodes())]
    return Reservation(
        name=name,
        nodes=nodes,
        partition=partition,
        start_time=start,
        end_time=end,
        flags=flags,
        users=users,
        accounts=accounts,
        spec=entry.strip(),
    )


def sync_reservations(database: SlurmDatabase, now: datetime) -> bool:
    """Bring ``database.reservations`` in line with ``SLURM_EMULATOR_RESERVATIONS``.

    Offsets resolve against ``reservation_anchor`` -- set the first time any
    reservation is configured -- so absolute times persist in the state file
    and do not drift on every request. Returns True if anything changed.

    Only reservations of origin ``env`` are touched: ones created through
    slurmrestd or scontrol (``emulator/core/reservations.py``) stay until
    deleted, and an env entry whose name such a reservation already holds
    is skipped, as slurmctld refuses a duplicate name
    (slurm://src/slurmctld/reservation.c#create_resv).
    """
    entries = [e.strip() for e in os.environ.get(RESERVATIONS_ENV, "").split(";") if e.strip()]
    owned = {name for name, r in database.reservations.items() if r.origin == "env"}
    if not entries and not owned:
        return False
    changed = False
    if entries and database.reservation_anchor is None:
        database.reservation_anchor = now.replace(microsecond=0)
        changed = True
    wanted: dict[str, str] = {}
    for entry in entries:
        name = entry.split(":", 1)[0].strip()
        if name:
            wanted[name] = entry
    for name in owned:
        if name not in wanted:
            del database.reservations[name]
            changed = True
    for name, entry in wanted.items():
        existing = database.reservations.get(name)
        if existing is not None and (existing.origin != "env" or existing.spec == entry):
            continue
        anchor = database.reservation_anchor or now
        resv = parse_reservation_spec(entry, anchor)
        # An entry that has already ended was purged; it does not come back.
        if resv is not None and resv.end_time is not None and resv.end_time <= now:
            continue
        if resv is not None:
            database.reservations[name] = resv
            changed = True
    return changed


def reservation_active(resv: Reservation, at: datetime) -> bool:
    return bool(resv.start_time and resv.end_time and resv.start_time <= at < resv.end_time)


def _job_may_use(resv: Reservation, job: Job) -> bool:
    return job.reservation == resv.name


def _access_list(csv: str) -> tuple[set[str], bool]:
    """(names, negated) of a Users= / Accounts= list; ``-`` on the first marks a deny list."""
    items = [i.strip() for i in csv.split(",") if i.strip()]
    negated = bool(items) and items[0].startswith("-")
    return {i.lstrip("-") for i in items}, negated


def _account_chain(database: SlurmDatabase, account: str) -> list[str]:
    """The account and its parents: the job association's lineage."""
    chain: list[str] = []
    name: Optional[str] = account.lower()
    while name and name not in chain:
        chain.append(name)
        record = database.get_account(name)
        name = record.parent.lower() if record and record.parent else None
    return chain


def job_may_access(database: SlurmDatabase, resv: Reservation, job: Job) -> bool:
    """Whether ``job`` may run in ``resv``.

    slurm://src/slurmctld/reservation.c#_valid_job_access_resv: root always
    may and nobody may use a TIME_FLOAT reservation. Otherwise the
    reservation's Users= / Accounts= become allow or deny lists of
    associations (slurm://src/slurmctld/reservation.c#_set_assoc_list): users
    and accounts both allowed = only those user/account pairs; else each list
    on its own, an account also covering its sub-accounts. The job's
    association or a parent must not be denied and, when anything is
    allowed, must be allowed (slurm://src/slurmctld/reservation.c#_check_assoc_access@26.05+).
    A Groups= reservation is open to the members of the group.
    """
    if "TIME_FLOAT" in resv.flags:
        return False
    if job.user == "root" or job.uid == 0:
        return True
    users, users_not = _access_list(resv.users)
    if not users and resv.groups:
        groups, _ = _access_list(resv.groups)
        return (job.group_name or job.user) in groups
    accounts = {a.lower() for a in _access_list(resv.accounts)[0]}
    accounts_not = _access_list(resv.accounts)[1]
    chain = _account_chain(database, job.account or "")
    if users and accounts and not users_not and not accounts_not:
        return job.user in users and (job.account or "").lower() in accounts
    any_allowed = allowed = denied = False
    if users:
        if users_not:
            denied = denied or job.user in users
        else:
            any_allowed = True
            allowed = allowed or job.user in users
    if accounts:
        hit = bool(accounts.intersection(chain))
        if accounts_not:
            denied = denied or hit
        else:
            any_allowed = True
            allowed = allowed or hit
    return not denied and (not any_allowed or allowed)


def purge_ended_reservations(database: SlurmDatabase, now: datetime) -> bool:
    """Drop reservations that have ended with no job running in them.

    slurm://src/slurmctld/reservation.c#job_resv_check; pending jobs that
    asked for one are held (priority 0) with reason ``ReservationDeleted``
    (slurm://src/slurmctld/reservation.c#_foreach_clear_job_resv).
    """
    changed = False
    for name, resv in list(database.reservations.items()):
        if resv.end_time is None or resv.end_time > now:
            continue
        users = [j for j in database.jobs.values() if j.reservation == name]
        if any(j.state == "RUNNING" for j in users) and "FLEX" not in resv.flags:
            continue
        del database.reservations[name]
        for job in users:
            job.reservation = ""
            if job.state == "PENDING" and job.state_reason != "JobHeldAdmin":
                job.state_reason = "ReservationDeleted"
                job.state_description = f"Reservation {name} was deleted"
                job.priority = 0
        changed = True
    return changed


def _blocks_whole_job(resv: Reservation, job: Job) -> bool:
    """``ESLURM_RESERVATION_MAINT`` in slurm://src/slurmctld/reservation.c#job_test_resv."""
    if "ALL_NODES" in resv.flags:
        return True
    if "PART_NODES" in resv.flags and resv.partition == job.partition:
        return True
    return "MAINT" in resv.flags and set(partition_nodes(job.partition)) <= set(resv.nodes)


# --- per-job request and synthetic usage ---


@dataclass
class JobRequest:
    node_count: int
    total_cpus: int
    cpus_per_node: int
    mem_per_node: int


def job_request(job: Job) -> JobRequest:
    """Turn the job_desc fields into a per-node CPU/memory requirement."""
    nodes = max(job.node_count or 1, 1)
    cpt = max(job.cpus_per_task or 1, 1)
    if job.tasks:
        total = job.tasks * cpt
    elif job.tasks_per_node:
        total = job.tasks_per_node * nodes * cpt
    else:
        total = nodes * NODE_CPUS
    nodes = max(nodes, math.ceil(total / NODE_CPUS))
    per_node = min(math.ceil(total / nodes), NODE_CPUS)
    mem = job.memory_per_node or per_node * DEF_MEM_PER_CPU_MB
    return JobRequest(nodes, total, per_node, min(mem, NODE_MEM_MB))


def _split(total: int, parts: int) -> list[int]:
    base, extra = divmod(total, parts)
    return [base + (1 if i < extra else 0) for i in range(parts)]


def _env_fraction(environment: dict[str, str], key: str) -> Optional[float]:
    try:
        return float(environment[key])
    except (KeyError, TypeError, ValueError):
        return None


def _seeded(seed: str) -> random.Random:
    """A generator seeded from ``seed``, so synthetic figures repeat across reads."""
    return random.Random(seed)  # noqa: S311 - synthetic data, not security


def synthetic_efficiency(
    job_id: object, environment: Optional[dict[str, str]] = None
) -> tuple[float, float]:
    """(CPU efficiency in [0.05, 0.99], memory fraction in [0, 1]) for a job.

    Deterministic from the job id: most jobs use 60-98% of their CPUs, about
    a fifth are badly parallelised (10-40%), a few anywhere. The job
    environment's ``SLURM_EMULATOR_CPU_EFFICIENCY`` / ``SLURM_EMULATOR_MEM_FRACTION``
    win when set.
    """
    rng = _seeded(f"slurm-emulator-efficiency:{job_id}")
    pick = rng.random()
    if pick < 0.75:
        cpu = rng.uniform(0.60, 0.98)
    elif pick < 0.95:
        cpu = rng.uniform(0.10, 0.40)
    else:
        cpu = rng.uniform(0.05, 0.99)
    mem = rng.uniform(0.15, 0.85) if rng.random() < 0.85 else rng.uniform(0.85, 0.98)
    env = environment or {}
    cpu_override = _env_fraction(env, CPU_EFFICIENCY_ENV)
    mem_override = _env_fraction(env, MEM_FRACTION_ENV)
    if cpu_override is not None:
        cpu = cpu_override
    if mem_override is not None:
        mem = mem_override
    return min(max(cpu, 0.05), 0.99), min(max(mem, 0.0), 1.0)


def synthetic_gpu_use(job_id: object) -> tuple[float, float]:
    """(GPU utilisation in [0.02, 0.98], GPU memory fraction in [0.05, 0.98]).

    Deterministic from the job id, like ``synthetic_efficiency``: most GPU
    jobs keep their GPUs busy, about a quarter leave them mostly idle
    (input-bound or waiting on the CPU).
    """
    rng = _seeded(f"slurm-emulator-gpu:{job_id}")
    util = rng.uniform(0.55, 0.98) if rng.random() < 0.75 else rng.uniform(0.02, 0.35)
    mem = rng.uniform(0.30, 0.98) if util > 0.35 else rng.uniform(0.05, 0.50)
    return util, mem


def job_efficiency(job: Job) -> tuple[float, float]:
    return synthetic_efficiency(job.job_id, job.environment)


def job_run_seconds(job: Job, default: float) -> float:
    """Emulated run length: the job's ``SLURM_EMULATOR_RUN_SECONDS`` or ``default``."""
    value = _env_fraction(job.environment or {}, RUN_SECONDS_ENV)
    return max(value, 0.0) if value is not None else default


def job_mem_per_node(job: Job, cpus_on_node: int) -> int:
    return min(job.memory_per_node or cpus_on_node * DEF_MEM_PER_CPU_MB, NODE_MEM_MB)


# --- occupancy ---


@dataclass
class NodeUse:
    cpus: int = 0
    mem: int = 0
    background: bool = False
    jobs: list[tuple[Job, int, int]] = field(default_factory=list)


def job_node_shares(job: Job) -> list[tuple[str, int, int]]:
    """(node, cpus, mem MB) the job holds on each of its allocated nodes."""
    nodes = list(job.node_list or [])
    if not nodes:
        return []
    shares = _split(job.alloc_cpus or len(nodes) * NODE_CPUS, len(nodes))
    return [(n, c, job_mem_per_node(job, c)) for n, c in zip(nodes, shares)]


def occupancy(
    database: SlurmDatabase,
    at: datetime,
    overrides: Optional[dict[str, tuple[str, str]]] = None,
) -> dict[str, NodeUse]:
    """What each node holds at ``at``: running jobs, then background tenants."""
    if overrides is None:
        overrides = node_state_overrides()
    usage: dict[str, NodeUse] = {}
    for job in database.jobs.values():
        if job.state != "RUNNING":
            continue
        for node, cpus, mem in job_node_shares(job):
            use = usage.setdefault(node, NodeUse())
            use.cpus += cpus
            use.mem += mem
            use.jobs.append((job, cpus, mem))

    fractions = background_fractions()
    if fractions:
        maint = {
            n
            for r in database.reservations.values()
            if "MAINT" in r.flags and reservation_active(r, at)
            for n in r.nodes
        }
        window = background_window(at)
        for partition, fraction in fractions.items():
            if partition not in partition_ranges():
                continue
            nodes = [n for n in partition_nodes(partition) if n not in overrides and n not in maint]
            want = round(fraction * len(nodes))
            order = list(nodes)
            _seeded(f"slurm-emulator-background:{partition}:{window}").shuffle(order)
            taken = 0
            for node in order:
                if taken >= want:
                    break
                if node in usage:  # an emulator job is there: other tenants go elsewhere
                    continue
                usage[node] = NodeUse(cpus=NODE_CPUS, mem=NODE_MEM_MB, background=True)
                taken += 1
    return usage


@dataclass
class Placement:
    """Where a job starts, or why it waits (``state_reason``, ``state_description``)."""

    nodes: Optional[list[str]]
    reason: str = "None"
    description: str = ""


def _overlapping(resv: Reservation, at: datetime, job_end: datetime) -> bool:
    return bool(
        resv.start_time and resv.end_time and resv.start_time < job_end and resv.end_time > at
    )


def _reservation_pool(
    database: SlurmDatabase, job: Job, at: datetime, job_end: datetime
) -> tuple[Optional[list[str]], Placement]:
    """Nodes a job may use inside its reservation (job_test_resv, job branch)."""
    own = database.reservations.get(job.reservation)
    if own is None or not job_may_access(database, own, job):
        return None, Placement(None, "Reservation")
    part = partition_nodes(job.partition)
    if "FLEX" in own.flags:
        pool = list(part)
    else:
        if own.start_time is None or at < own.start_time:
            return None, Placement(None, "Reservation")
        if not own.nodes and "ANY_NODES" not in own.flags:
            return None, Placement(None, "Reservation")
        if own.end_time is not None and at > own.end_time:
            return None, Placement(None, "Reservation")
        allowed = set(part) if "ANY_NODES" in own.flags else set(own.nodes)
        pool = [n for n in part if n in allowed]
    if "MAINT" in own.flags:
        return pool, Placement(pool)
    overlap = maint = False
    own_nodes = set(own.nodes)
    for other in database.reservations.values():
        if other is own or not other.nodes or not _overlapping(other, at, job_end):
            continue
        steal = set(other.nodes)
        if "OVERLAP" in own.flags and "MAINT" not in other.flags:
            if "FLEX" not in own.flags or not own_nodes or steal <= own_nodes:
                continue
            steal -= own_nodes
        if steal.intersection(pool):
            overlap = True
            maint = maint or "MAINT" in other.flags
            pool = [n for n in pool if n not in steal]
    if overlap and not pool:
        return None, Placement(None, NODE_NOT_AVAIL, MAINT_DESC if maint else OTHER_RESV_DESC)
    return pool, Placement(pool)


def try_allocate(
    database: SlurmDatabase,
    job: Job,
    at: datetime,
    duration_seconds: float,
    usage: Optional[dict[str, NodeUse]] = None,
    overrides: Optional[dict[str, tuple[str, str]]] = None,
    unavailable: Optional[set[str]] = None,
) -> Placement:
    """Nodes ``job`` can start on at ``at``, or why it stays pending.

    ``unavailable`` are nodes this scheduling pass no longer hands out
    (partitions whose first job could not start).
    """
    if overrides is None:
        overrides = node_state_overrides()
    if usage is None:
        usage = occupancy(database, at, overrides)
    req = job_request(job)
    job_end = at + timedelta(seconds=duration_seconds)

    if job.reservation:
        pool, placement = _reservation_pool(database, job, at, job_end)
        if pool is None:
            return placement
    else:
        pool = partition_nodes(job.partition)
        blocked: set[str] = set()
        for resv in database.reservations.values():
            if not _overlapping(resv, at, job_end):
                continue
            if _blocks_whole_job(resv, job):
                return Placement(None, NODE_NOT_AVAIL, MAINT_DESC)
            blocked.update(resv.nodes)
        pool = [n for n in pool if n not in blocked]

    free: list[str] = []
    for node in pool:
        if node in overrides or (unavailable and node in unavailable):
            continue
        use = usage.get(node) or NodeUse()
        if use.background:
            continue
        if NODE_CPUS - use.cpus < req.cpus_per_node or NODE_MEM_MB - use.mem < req.mem_per_node:
            continue
        free.append(node)

    if len(free) >= req.node_count:
        return Placement(free[: req.node_count])
    return Placement(None, "Resources")


def allocate(job: Job, nodes: list[str]) -> None:
    """Record the allocation on the job (node list + CPUs)."""
    req = job_request(job)
    job.node_list = list(nodes)
    job.node_count = len(nodes)
    job.alloc_cpus = req.total_cpus if (job.tasks or job.tasks_per_node) else len(nodes) * NODE_CPUS


# --- node view ---


def _noise(*key: object) -> float:
    return _seeded("slurm-emulator-noise:" + ":".join(map(str, key))).random()


@dataclass
class NodeView:
    state: list[str]
    reason: str = ""
    alloc_cpus: int = 0
    alloc_memory: int = 0
    cpu_load: int = 0
    free_mem: Optional[int] = None
    reservation: str = ""
    gpus_used: int = 0


def node_views(database: SlurmDatabase, at: datetime) -> dict[str, NodeView]:
    """Per-node state, allocation and synthetic load at ``at``."""
    overrides = node_state_overrides()
    usage = occupancy(database, at, overrides)
    active = [r for r in database.reservations.values() if reservation_active(r, at)]
    load_window = int(at.timestamp()) // LOAD_WINDOW_SECONDS
    views: dict[str, NodeView] = {}
    for node in all_nodes():
        use = usage.get(node) or NodeUse()
        if use.cpus <= 0:
            base = "IDLE"
        elif use.cpus >= NODE_CPUS:
            base = "ALLOCATED"
        else:
            base = "MIXED"
        state = [base]
        reason = ""
        override = overrides.get(node)
        if override is not None:
            reason = override[1]
            state = ["DOWN"] if override[0] == "DOWN" else [base, "DRAIN"]
        resv_name = ""
        for resv in active:
            if node in resv.nodes:
                resv_name = resv.name
                state.append("RESERVED")
                if "MAINT" in resv.flags:
                    state.append("MAINTENANCE")
                break

        jitter = 1.0 + (_noise(node, load_window, "jitter") - 0.5) * 0.08
        if use.background:
            load = NODE_CPUS * (0.55 + 0.42 * _noise(node, background_window(at), "bg"))
            used_mem = NODE_MEM_MB * (0.3 + 0.5 * _noise(node, background_window(at), "bgmem"))
        else:
            load = 0.0
            used_mem = 0.0
            for job, cpus, mem in use.jobs:
                cpu_eff, mem_frac = job_efficiency(job)
                load += cpus * cpu_eff
                used_mem += mem * mem_frac
            if not use.jobs:
                load = 0.25 * _noise(node, load_window, "idle")
        used_mem += OS_MEM_MB
        down = override is not None and override[0] == "DOWN"
        views[node] = NodeView(
            state=state,
            reason=reason,
            alloc_cpus=min(use.cpus, NODE_CPUS),
            alloc_memory=min(use.mem, NODE_MEM_MB),
            cpu_load=0 if down else round(load * jitter * 100),
            free_mem=None if down else max(int(NODE_MEM_MB - used_mem), 0),
            reservation=resv_name,
            gpus_used=NODE_GPUS if use.cpus and _has_gpus(node) else 0,
        )
    return views


def partition_has_gpus(partition: str) -> bool:
    """Whether jobs in ``partition`` are charged GPUs.

    Every node advertises ``gpu:4`` (the standard node), but only partitions
    named like ``gpu`` hand them out, so CPU jobs carry no ``gres/gpu`` TRES.
    """
    return "gpu" in (partition or "").lower()


def _has_gpus(node: str) -> bool:
    return any(node in partition_nodes(p) for p in partition_ranges() if partition_has_gpus(p))


def max_rss_bytes(mem_fraction: float, mem_per_node_mb: int) -> int:
    """Peak RSS in bytes, whole KiB as the kernel reports it."""
    return int(mem_fraction * mem_per_node_mb * 1024) * 1024
