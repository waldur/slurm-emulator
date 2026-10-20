"""Plain-dict serializers for v0.0.45 response objects.

Field names and paths are copied verbatim from the authoritative
parser tables in slurm://src/plugins/data_parser/v0.0.45/parsers.c@26.05+
(PARSER_ARRAY tables ACCOUNT, USER, ASSOC, QOS, JOB_INFO, NODE,
PARTITION_INFO; v0.0.45 ships with 26.05 and later, see docs/slurm-parity.md).
``*_NO_VAL`` typed fields render as ``{set, infinite, number}`` exactly
like DUMP_FUNC(UINT64_NO_VAL)
(slurm://src/plugins/data_parser/v0.0.45/parsers.c#UINT64_NO_VAL@26.05+). Deliberately a pragmatic subset: every field
Waldur's parsers touch, plus enough context to look real.
"""

from __future__ import annotations

import os
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, Optional

from emulator.commands.sacct import (
    _NODE_CPUS,
    _NODE_GPUS,
    _NODE_MEM_GB,
    SacctEmulator,
    _convert_exact,
)
from emulator.core import exit_status, job_execution
from emulator.core.assoc_view import effective_assoc
from emulator.core.cluster_model import (
    DEF_MEM_PER_CPU_MB,
    GPU_MEM_MB,
    NodeView,
    compress_hostlist,
    expand_hostlist,
    job_request,
    partition_has_gpus,
    synthetic_gpu_use,
)
from emulator.core.database import (
    QOS,
    Account,
    Association,
    Job,
    Reservation,
    UsageRecord,
    User,
)
from emulator.core.jobacct import JobTimes, epoch, stored_times
from emulator.core.reservations import INFINITE_TIME, YEAR
from emulator.core.time_engine import TimeEngine
from emulator.core.tres import DYNAMIC_TRES_FIRST_ID, STATIC_TRES_IDS, tres_id
from emulator.slurm_version import at_least

# Canonical TRES ids as initialized by slurmdbd (emulator/core/tres.py);
# dynamic ids are resolved against the database's TRES list when one is given.
# gres/gpumem and gres/gpuutil exist where AccountingStorageTRES lists them
# (slurm://src/interfaces/gpu.c#gpu_get_tres_pos), as in the database's list.
_TRES_IDS = {
    **STATIC_TRES_IDS,
    "gres/gpu": DYNAMIC_TRES_FIRST_ID,
    "gres/gpumem": DYNAMIC_TRES_FIRST_ID + 1,
    "gres/gpuutil": DYNAMIC_TRES_FIRST_ID + 2,
}


def uint_no_val(number: Optional[int] = None, infinite: bool = False) -> dict[str, Any]:
    """``{set, infinite, number}`` struct for ``*_NO_VAL`` fields."""
    if infinite:
        return {"set": False, "infinite": True, "number": 0}
    if number is None:
        return {"set": False, "infinite": False, "number": 0}
    return {"set": True, "infinite": False, "number": int(number)}


def tres_entry(
    tres_type: str, count: int, tres_types: Optional[list[str]] = None
) -> dict[str, Any]:
    base_type = tres_type.split("/", 1)[0]
    name = tres_type.split("/", 1)[1] if "/" in tres_type else ""
    if tres_types is not None:
        tid = tres_id(tres_type, tres_types)
    else:
        tid = _TRES_IDS.get(tres_type.lower(), 0)
    return {
        "type": base_type,
        "name": name,
        "id": tid,
        "count": int(count),
    }


def tres_list_from_dict(values: dict[str, int]) -> list[dict[str, Any]]:
    ordered = [t for t in _TRES_IDS if t in values]
    extras = [t for t in sorted(values) if t not in _TRES_IDS]
    return [tres_entry(t, values[t]) for t in ordered + extras]


def tres_list_from_str(tres_str: str) -> list[dict[str, Any]]:
    """Parse ``cpu=10,mem=4G`` style strings into TRES object lists."""
    values: dict[str, int] = {}
    for part in tres_str.split(","):
        if "=" not in part:
            continue
        name, _, raw = part.partition("=")
        digits = "".join(ch for ch in raw.strip() if ch.isdigit())
        if name.strip() and digits:
            values[name.strip().lower()] = int(digits)
    return tres_list_from_dict(values)


def tres_str_from_list(entries: list[dict[str, Any]]) -> dict[str, int]:
    """Inverse of ``tres_list_from_dict`` for parsing request bodies."""
    values: dict[str, int] = {}
    for entry in entries or []:
        tres_type = entry.get("type", "")
        if entry.get("name"):
            tres_type = f"{tres_type}/{entry['name']}"
        if tres_type and "count" in entry:
            values[tres_type] = int(entry["count"])
    return values


def _limits_tres(limits: dict[str, int], prefix: str) -> dict[str, int]:
    """Collect ``Prefix:<tres>`` limit keys; bare ``Prefix`` → billing."""
    out: dict[str, int] = {}
    for key, value in limits.items():
        if key == prefix:
            out["billing"] = int(value)
        elif key.startswith(f"{prefix}:"):
            out[key.split(":", 1)[1].lower()] = int(value)
    return out


def assoc_short(assoc: Association) -> dict[str, Any]:
    """ASSOC_SHORT (slurm://src/plugins/data_parser/v0.0.45/parsers.c#ASSOC_SHORT@26.05+)."""
    return {
        "account": assoc.account,
        "cluster": assoc.cluster,
        "partition": assoc.partition or "",
        "user": assoc.user,
        "id": 0,
    }


def account_to_dict(account: Account, associations: list[Association]) -> dict[str, Any]:
    return {
        "name": account.name,
        "description": account.description,
        "organization": account.organization,
        "flags": [],
        "associations": [assoc_short(a) for a in associations],
        "coordinators": [],
    }


def user_to_dict(
    user: User, associations: list[Association], with_assocs: bool = True
) -> dict[str, Any]:
    """USER (slurm://src/plugins/data_parser/v0.0.45/parsers.c#USER@26.05+).

    slurmdbd derives the default account from the user's associations
    and reads those only when asked to, so without ``with_assocs`` both the
    list and ``default.account`` come back empty
    (slurm://src/plugins/accounting_storage/mysql/as_mysql_user.c#as_mysql_get_users).
    """
    if not with_assocs:
        associations = []
    return {
        "name": user.name,
        "administrator_level": ["None"],
        "default": {"account": user.default_account if with_assocs else "", "wckey": ""},
        "flags": [],
        "associations": [assoc_short(a) for a in associations],
        "coordinators": [],
        "wckeys": [],
    }


def _lineage(assoc: Association, account: Optional[Account]) -> str:
    parent = assoc.parent or (account.parent if account else None)
    segments = [s for s in (parent, assoc.account) if s and s != "root"]
    path = "/" + "/".join(segments) + "/" if segments else "/"
    if assoc.user:
        path += f"0-{assoc.user}/"
    return path


def _no_val(value: Optional[int]) -> dict[str, Any]:
    return uint_no_val(value) if value is not None else uint_no_val(infinite=True)


def assoc_to_dict(
    assoc: Association,
    account: Optional[Account],
    is_default: bool = True,
    accounts: Optional[dict[str, Account]] = None,
) -> dict[str, Any]:
    """ASSOC (slurm://src/plugins/data_parser/v0.0.45/parsers.c#ASSOC@26.05+)."""
    view = effective_assoc(assoc, account, accounts)
    tres = {key: tres_list_from_dict(values) for key, values in view["tres"].items()}
    scalars = view["scalars"]
    shares = view["shares"]
    return {
        "accounting": [],
        "account": assoc.account,
        "user": assoc.user,
        "cluster": assoc.cluster,
        "partition": assoc.partition or "",
        # Account rows name their parent, ``root`` for a top-level account;
        # user rows leave it empty.
        "parent_account": (
            "" if assoc.user or assoc.account == "root" else (assoc.parent or "root")
        ),
        "is_default": is_default,
        "lineage": _lineage(assoc, account),
        "qos": view["qos"],
        "shares_raw": shares,
        "comment": "",
        "default": {"qos": view["def_qos"]},
        "flags": [],
        "max": {
            "jobs": {
                "per": {
                    "count": _no_val(scalars["GrpJobs"]),
                    "accruing": _no_val(scalars["GrpJobsAccrue"]),
                    "submitted": _no_val(scalars["GrpSubmitJobs"]),
                    "wall_clock": _no_val(scalars["MaxWall"]),
                },
                "active": _no_val(scalars["MaxJobs"]),
                "accruing": _no_val(scalars["MaxJobsAccrue"]),
                "total": _no_val(scalars["MaxSubmitJobs"]),
            },
            "tres": {
                "total": tres["GrpTRES"],
                "group": {"minutes": tres["GrpTRESMins"], "active": tres["GrpTRESRunMins"]},
                "minutes": {"total": tres["MaxTRESRunMins"], "per": {"job": tres["MaxTRESMins"]}},
                "per": {"job": tres["MaxTRES"], "node": tres["MaxTRESPerNode"]},
            },
            "per": {"account": {"wall_clock": _no_val(scalars["GrpWall"])}},
        },
        "min": {"priority_threshold": _no_val(scalars["MinPrioThresh"])},
        "priority": _no_val(scalars["Priority"]),
    }


def _parse_slurm_duration_to_minutes(value: str) -> Optional[int]:
    """Convert a SLURM duration to whole minutes.

    Accepts ``minutes``, ``MM:SS``, ``HH:MM:SS`` and ``[days-]HH[:MM[:SS]]``
    (slurm_time_str2mins). Returns None for empty / UNLIMITED / INFINITE.
    Partial minutes from a seconds component round up. Falls back to the
    digits-only reading if the shape is unrecognised.
    """
    v = value.strip()
    if not v or v.upper() in {"UNLIMITED", "INFINITE"}:
        return None
    has_days = "-" in v
    days = 0
    if has_days:
        day_str, _, v = v.partition("-")
        try:
            days = int(day_str)
        except ValueError:
            days = 0
    try:
        nums = [int(p) for p in v.split(":")]
    except ValueError:
        digits = "".join(ch for ch in value if ch.isdigit())
        return int(digits) if digits else None
    if has_days:
        # after a days- prefix the remainder counts hours, then minutes, seconds
        hours = nums[0] if len(nums) > 0 else 0
        minutes = nums[1] if len(nums) > 1 else 0
        seconds = nums[2] if len(nums) > 2 else 0
    elif len(nums) == 1:
        hours, minutes, seconds = 0, nums[0], 0
    elif len(nums) == 2:
        hours, minutes, seconds = 0, nums[0], nums[1]  # MM:SS
    else:
        hours, minutes, seconds = nums[0], nums[1], nums[2]  # HH:MM:SS
    return days * 1440 + hours * 60 + minutes + (seconds + 59) // 60


def qos_to_dict(qos: QOS, qos_id: int) -> dict[str, Any]:
    minutes = _parse_slurm_duration_to_minutes(qos.max_wall) if qos.max_wall else None
    max_wall = uint_no_val(minutes)
    return {
        "name": qos.name,
        "description": qos.name,
        "id": qos_id,
        "flags": [f for f in qos.flags.split(",") if f] if qos.flags else [],
        "priority": uint_no_val(qos.priority if qos.priority >= 0 else None),
        "usage_factor": {"set": True, "infinite": False, "number": 1.0},
        "usage_threshold": {"set": False, "infinite": False, "number": 0.0},
        "limits": {
            # Real v0.0.45 QOS: grace_time is a plain UINT32 (seconds) at
            # limits/grace_time (slurm://src/plugins/data_parser/v0.0.45/parsers.c#QOS@26.05+), not a NO_VAL struct.
            "grace_time": max(qos.grace_time, 0),
            "max": {
                "active_jobs": {"accruing": uint_no_val(), "count": uint_no_val()},
                "tres": {
                    "total": tres_list_from_str(qos.grp_tres),
                    "minutes": {"total": tres_list_from_str(qos.grp_tres_mins), "per": {"job": []}},
                    "per": {
                        "job": tres_list_from_str(qos.max_tres_per_job),
                        "user": tres_list_from_str(qos.max_tres_per_user),
                        "account": [],
                        "node": tres_list_from_str(qos.max_tres_per_node),
                    },
                },
                "wall_clock": {"per": {"job": max_wall, "qos": uint_no_val()}},
                "jobs": {
                    "count": uint_no_val(),
                    "active_jobs": {
                        "per": {
                            "account": uint_no_val(),
                            "user": uint_no_val(qos.max_jobs if qos.max_jobs >= 0 else None),
                        }
                    },
                    "per": {
                        "account": uint_no_val(),
                        "user": uint_no_val(qos.max_submit if qos.max_submit >= 0 else None),
                    },
                },
            },
            "min": {
                "priority_threshold": uint_no_val(),
                "tres": {"per": {"job": tres_list_from_str(qos.min_tres_per_job)}},
            },
        },
    }


def _cpu_time(seconds: float) -> dict[str, int]:
    whole = int(seconds)
    return {"seconds": whole, "microseconds": round((seconds - whole) * 1_000_000) % 1_000_000}


def _exit_code(state: str, return_code: Optional[int]) -> dict[str, Any]:
    """PROCESS_EXIT_CODE of a job or step (``emulator/core/exit_status.py``)."""
    return exit_status.verbose(state, return_code)


def _step_usage(record: UsageRecord, cpu: dict[str, Any]) -> tuple[dict[str, int], dict[str, int]]:
    """(last sample, peak) of the batch task's gathered TRES.

    jobacct keeps a task's latest sample as its total and its highest as its
    max (slurm://src/plugins/jobacct_gather/common/common_jag.c#jag_common_poll_data);
    slurmdbd averages the totals over the tasks
    (slurm://src/plugins/accounting_storage/mysql/as_mysql_job.c#_average_tres_usage).
    ``cpu`` is CPU time in milliseconds (``CPU_TIME_ADJ``), ``mem``/``vmem``
    bytes, ``energy`` joules. A GPU task carries what the NVML plugin reads
    summed over its GPUs (slurm://src/plugins/gpu/nvml/gpu_nvml.c#gpu_p_usage_read):
    ``gres/gpuutil`` percent and ``gres/gpumem`` bytes. The emulator's GPU use
    is a mean (``synthetic_gpu_use``); the peak sample is put 10% above it.
    """
    total = cpu["total"]
    usage = {
        "cpu": total["seconds"] * 1000 + total["microseconds"] // 1000,
        "mem": int(record.max_rss_bytes),
    }
    if record.raw_tres.get("energy"):
        usage["energy"] = int(record.raw_tres["energy"])
    usage.update({"fs/disk": 0, "vmem": int(record.max_rss_bytes), "pages": 0})
    last, peak = dict(usage), dict(usage)
    if partition_has_gpus(record.partition):
        util, mem_fraction = synthetic_gpu_use(record.job_id)
        gpumem = int(mem_fraction * GPU_MEM_MB * _NODE_GPUS) * 1024 * 1024
        last.update({"gres/gpumem": gpumem, "gres/gpuutil": round(util * 100 * _NODE_GPUS)})
        peak.update(
            {
                "gres/gpumem": gpumem,
                "gres/gpuutil": min(round(util * 110 * _NODE_GPUS), 100 * _NODE_GPUS),
            }
        )
    return last, peak


def _record_stdio(record: UsageRecord) -> dict[str, str]:
    """``stdin_expanded``/``stdout_expanded``/``stderr_expanded`` of a stored job.

    slurmdbd's patterns expanded for the batch step
    (slurm://src/common/slurmdb_defs.c#slurmdb_expand_job_stdio_fields@25.05+); an
    empty path stays empty.
    """
    job = SimpleNamespace(
        job_id=record.job_id,
        name=record.job_name or f"job_{record.job_id}",
        user=record.user,
        node_list=expand_hostlist(record.node_list) if record.node_count else [],
        working_directory=record.working_directory or f"/home/{record.user}",
    )
    return {
        f"{key}_expanded": (job_execution.expand_stdio_path(path, job, "") if path else "")
        for key, path in (
            ("stdin", record.standard_input),
            ("stdout", record.standard_output),
            ("stderr", record.standard_error),
        )
    }


def _batch_step(record: UsageRecord, start: int, end: int, cpu: dict[str, Any]) -> dict[str, Any]:
    """The ``batch`` step of a job (STEP, slurm://src/plugins/data_parser/v0.0.45/parsers.c#STEP@26.05+).

    It runs on the first allocated node as one task. ``tres/requested`` holds
    every gathered TRES (``_step_usage``): ``max``/``min`` with the task and
    node that reached it (slurm://src/plugins/data_parser/v0.0.45/parsers.c#STEP_TRES_REQ_MAX@26.05+),
    ``average``/``total`` without -- what sacct prints as MaxRSS
    (slurm://src/sacct/print.c#PRINT_MAXRSS) and TRESUsageIn*.
    ``tres/consumed`` holds the writes (``fs/disk``). The step's consumed
    energy is the job's: the batch step is the only one. A timed-out job's
    batch step is CANCELLED by the time-limit signal.
    """
    first_node = (expand_hostlist(record.node_list) or ["node001"])[0]
    state = record.state or "COMPLETED"
    step_state = "CANCELLED" if state == "TIMEOUT" else state
    last, peak = _step_usage(record, cpu)

    def located(values: dict[str, int]) -> list[dict[str, Any]]:
        return [{**tres_entry(k, v), "task": 0, "node": first_node} for k, v in values.items()]

    def plain(values: dict[str, int]) -> list[dict[str, Any]]:
        return [tres_entry(k, v) for k, v in values.items()]

    writes = {"fs/disk": 0}
    cpus_on_node = min(record.alloc_cpus, _NODE_CPUS) if record.alloc_cpus else _NODE_CPUS
    mem_on_node = record.req_mem_mb or cpus_on_node * (_NODE_MEM_GB * 1024 // _NODE_CPUS)
    energy = record.raw_tres.get("energy")
    return {
        "time": {
            "elapsed": end - start,
            "start": uint_no_val(start),
            "end": uint_no_val(end),
            "suspended": 0,
            "limit": uint_no_val(),
            **cpu,
        },
        # The time-limit SIGTERM ends a timed-out job's batch step.
        "exit_code": _exit_code(
            state,
            -exit_status.TIME_LIMIT_SIGNAL if state == "TIMEOUT" else record.exit_code,
        ),
        "nodes": {"count": 1, "range": first_node, "list": [first_node]},
        "tasks": {"count": 1},
        "kill_request_user": "",
        "state": [step_state],
        "statistics": {
            "CPU": {"actual_frequency": 0},
            "energy": {"consumed": uint_no_val(int(energy) if energy else None)},
        },
        "step": {
            "id": f"{record.job_id}.batch",
            "name": "batch",
            "stderr": record.standard_error,
            "stdin": record.standard_input,
            "stdout": record.standard_output,
            # slurm://src/plugins/data_parser/v0.0.45/parsers.c#STEP_INFO_STDOUT_EXP@26.05+
            **(_record_stdio(record) if at_least("25.05") else {}),
        },
        "tres": {
            "requested": {
                "max": located(peak),
                "min": located(peak),
                "average": plain(last),
                "total": plain(last),
            },
            "consumed": {
                "max": located(writes),
                "min": located(writes),
                "average": plain(writes),
                "total": plain(writes),
            },
            "allocated": tres_list_from_dict({"cpu": cpus_on_node, "mem": mem_on_node, "node": 1}),
        },
    }


def dbd_job_to_dict(
    record: UsageRecord,
    active: Optional[Job] = None,
    times: Optional[JobTimes] = None,
    with_steps: bool = True,
) -> dict[str, Any]:
    """One slurmdb JOB per usage record — agrees with sacct output.

    Records that mirror a real run (``start_time`` set: submitted or seeded
    jobs) render what slurmdbd stores
    (slurm://src/plugins/data_parser/v0.0.45/parsers.c#JOB@26.05+): submit /
    eligible / start / end, ``time/limit`` (minutes, UINT32_NO_VAL),
    ``time/total|user|system`` CPU time, ``allocation_nodes``, the ``nodes``
    hostlist, requested vs allocated TRES and a ``batch`` step carrying MaxRSS.
    Legacy records (injected usage) keep the same math as SacctEmulator._row:
    Elapsed = node_hours in seconds, End = record timestamp, per-hour TRES
    rates from raw_tres, one node.
    """
    rate = SacctEmulator._rate
    state = record.state or "COMPLETED"
    exit_code = _exit_code(state, getattr(record, "exit_code", None))
    gpus = rate(record, "GRES/gpu", _NODE_GPUS)
    steps: list[dict[str, Any]] = []

    if active is not None:
        # A pending or running job: no end yet, elapsed so far, the request
        # until it is allocated (as slurmdbd holds it from job start/submit,
        # slurm://src/plugins/accounting_storage/mysql/as_mysql_job.c#as_mysql_job_start).
        from emulator.core.scheduler import job_clock_now  # noqa: PLC0415

        request = job_request(active)
        submit = int(active.submit_time.timestamp()) if active.submit_time else 0
        eligible = submit
        running = active.state in ("RUNNING", "SUSPENDED") and bool(active.node_list)
        start = int(active.start_time.timestamp()) if running and active.start_time else 0
        end = 0
        elapsed = max(int(job_clock_now(TimeEngine()).timestamp()) - start, 0) if start else 0
        nodes = len(active.node_list) if running else 0
        node_list = record.node_list
        cpus = (active.alloc_cpus or request.total_cpus) if running else 0
        requested = {
            "cpu": request.total_cpus,
            "mem": request.mem_per_node * request.node_count,
            "node": request.node_count,
            "billing": request.total_cpus,
        }
        allocated = (
            {"cpu": cpus, "mem": request.mem_per_node * nodes, "node": nodes, "billing": cpus}
            if running
            else {}
        )
        if partition_has_gpus(record.partition):
            requested["gres/gpu"] = _NODE_GPUS * request.node_count
            if running:
                allocated["gres/gpu"] = _NODE_GPUS * nodes
        zero = {"seconds": 0, "microseconds": 0}
        cpu = {"system": zero, "user": dict(zero), "total": dict(zero)}
        limit = uint_no_val(active.time_limit, infinite=active.time_limit is None)
        tres = {
            "allocated": tres_list_from_dict(allocated),
            "requested": tres_list_from_dict(requested),
        }
        required = {
            "CPUs": request.total_cpus,
            "memory_per_cpu": uint_no_val(),
            "memory_per_node": uint_no_val(request.mem_per_node),
        }
    elif record.start_time is not None and record.end_time is not None:
        start = int(record.start_time.timestamp())
        end = int(record.end_time.timestamp())
        submit = int((record.submit_time or record.start_time).timestamp())
        eligible = int(
            (record.eligible_time or record.submit_time or record.start_time).timestamp()
        )
        elapsed = max(end - start, 0)
        nodes = record.node_count
        node_list = record.node_list or "None assigned"
        cpus = record.alloc_cpus
        mem_total = record.req_mem_mb * max(nodes, 1)
        requested = {
            "cpu": record.alloc_cpus or _NODE_CPUS * max(nodes, 1),
            "mem": mem_total,
            "node": max(nodes, 1),
            "billing": record.alloc_cpus or _NODE_CPUS * max(nodes, 1),
        }
        allocated = {"cpu": cpus, "mem": mem_total if nodes else 0, "node": nodes, "billing": cpus}
        if partition_has_gpus(record.partition):
            requested["gres/gpu"] = _NODE_GPUS * max(nodes, 1)
            if nodes:
                allocated["gres/gpu"] = _NODE_GPUS * nodes
        # Consumed energy (joules) sits in the job's allocated TRES, where
        # sacct's ConsumedEnergyRaw reads it
        # (slurm://src/sacct/print.c#PRINT_CONSUMED_ENERGY_RAW).
        if nodes and record.raw_tres.get("energy"):
            allocated["energy"] = record.raw_tres["energy"]
        total = record.cpu_time_seconds
        system = total * 0.04
        cpu = {
            "system": _cpu_time(system),
            "user": _cpu_time(total - system),
            "total": _cpu_time(total),
        }
        limit = uint_no_val(record.time_limit, infinite=record.time_limit is None)
        if nodes:
            steps.append(_batch_step(record, start, end, cpu))
        tres = {
            "allocated": tres_list_from_dict({k: v for k, v in allocated.items() if v}),
            "requested": tres_list_from_dict(requested),
        }
        required = {
            "CPUs": requested["cpu"],
            "memory_per_cpu": uint_no_val(),
            "memory_per_node": uint_no_val(record.req_mem_mb),
        }
    else:
        elapsed = int(record.node_hours * 3600)
        end = int(record.timestamp.timestamp())
        start = submit = eligible = end - elapsed
        nodes = 1
        node_list = "node001"
        cpus = rate(record, "CPU", _NODE_CPUS)
        mem_mb = rate(record, "Mem", _NODE_MEM_GB) * 1024
        tres_values = {"cpu": cpus, "mem": mem_mb, "node": 1, "billing": cpus}
        if gpus:
            tres_values["gres/gpu"] = gpus
        legacy = tres_list_from_dict(tres_values)
        tres = {"allocated": legacy, "requested": legacy}
        zero = {"seconds": 0, "microseconds": 0}
        cpu = {"system": zero, "user": dict(zero), "total": dict(zero)}
        limit = uint_no_val(infinite=True)
        required = None

    # The times the query reports, truncated or not (emulator/core/jobacct.py).
    if times is None:
        from emulator.core.scheduler import job_clock_now  # noqa: PLC0415

        engine = TimeEngine()
        job_now = epoch(job_clock_now(engine))
        times = stored_times(
            record, active, max(epoch(engine.get_current_time()), job_now), job_now
        )
    submit, eligible = times.submit or submit, times.eligible
    start, end, elapsed = times.start, times.end, times.elapsed
    if not with_steps:
        steps = []
    # A requeued run is stored as JOB_PENDING | JOB_REQUEUE
    # (slurm://src/slurmctld/job_mgr.c#_job_requeue_op), which JOB_STATE dumps
    # as base state plus flag.
    current = ["PENDING", "REQUEUED"] if state == "REQUEUED" else [state]

    job: dict[str, Any] = {
        "account": record.account,
        "allocation_nodes": nodes,
        "array": {"job_id": 0},
        "cluster": record.cluster,
        "derived_exit_code": exit_code,
        "exit_code": exit_code,
        "flags": [],
        # ``group`` is the *group* name (GROUP_ID dump of slurmdb_job_rec_t.gid,
        # slurm://src/plugins/data_parser/v0.0.45/parsers.c#JOB_USER@26.05+ sits
        # next to it for ``user``); without NSS mode it falls back to the user.
        "group": record.group_name or record.user,
        "het": {"job_id": 0},
        "job_id": record.job_id,
        "kill_request_user": "",
        "name": getattr(record, "job_name", "") or f"job_{record.job_id}",
        "nodes": node_list,
        "partition": record.partition or "compute",
        "qos": "normal",
        "state": {"current": current, "reason": "None"},
        "steps": steps,
        "stdout": record.standard_output,
        "stderr": record.standard_error,
        "stdin": record.standard_input,
        # slurm://src/plugins/data_parser/v0.0.45/parsers.c#"stdout_expanded"@26.05+
        **_record_stdio(record),
        "time": {
            "elapsed": elapsed,
            "eligible": eligible,
            "start": start,
            "end": end,
            "submission": submit,
            "suspended": times.suspended,
            "limit": limit,
            **cpu,
        },
        "tres": tres,
        "user": record.user,
        "wckey": {"wckey": "", "flags": []},
        "working_directory": getattr(record, "working_directory", "") or f"/home/{record.user}",
    }
    if required is not None:
        job["required"] = required
    return job


def ctld_job_to_dict(job: Job) -> dict[str, Any]:
    """JOB_INFO subset for /slurm/.../jobs (active job view).

    Field names/shapes follow the v0.0.45 JOB_INFO parser
    (slurm://src/plugins/data_parser/v0.0.45/parsers.c#JOB_INFO@26.05+): ``job_resources`` (with
    ``nodes.count``), ``exit_code``/``derived_exit_code`` (PROCESS_EXIT_CODE),
    ``time_limit``/``priority``/``suspend_time`` as ``*_NO_VAL`` structs, etc.
    """

    def ts(value) -> dict[str, Any]:
        return uint_no_val(int(value.timestamp())) if value else uint_no_val()

    request = job_request(job)
    node_list = list(getattr(job, "node_list", []) or [])
    node_count = len(node_list) or request.node_count
    total_cpus = getattr(job, "alloc_cpus", 0) or request.total_cpus
    if node_list:
        nodes = compress_hostlist(node_list)
    elif job.state == "PENDING" or job.start_time is None:
        nodes = ""  # nothing allocated yet
    else:
        nodes = "node001"  # pre-allocation state files
    time_limit = getattr(job, "time_limit", None)
    # The script's own status when it ran (SLURM_EMULATOR_JOB_EXECUTE),
    # else 1/0 from the state (slurm://slurm/slurm.h#job_info_t.exit_code).
    exit_code = _exit_code(job.state, getattr(job, "exit_code", None))
    # A running job's end_time is its expected end: start + time limit, or
    # a year without one (slurm://src/slurmctld/job_mgr.c#job_end_time_reset).
    end = job.end_time
    if end is None and job.state == "RUNNING" and job.start_time:
        end = job.start_time + (timedelta(minutes=time_limit) if time_limit else YEAR)

    return {
        "job_id": int(job.job_id) if str(job.job_id).isdigit() else 0,
        "name": job.name or f"job_{job.job_id}",
        "account": job.account,
        # user_id/user_name and group_id/group_name are overloads of the same
        # uid/gid fields (slurm://src/plugins/data_parser/v0.0.45/parsers.c#JOB_INFO@26.05+);
        # 1000 / the user's own name stand in when NSS mode is off.
        "user_id": job.uid if job.uid is not None else 1000,
        "user_name": job.user,
        "group_id": job.gid if job.gid is not None else 1000,
        "group_name": job.group_name or job.user,
        "partition": job.partition or "compute",
        "job_state": [job.state],
        # slurm://src/common/job_state_reason.c#job_state_reason_string
        "state_reason": job.state_reason or "None",
        "state_description": job.state_description,
        # JOB_INFO carries exit_code/derived_exit_code as PROCESS_EXIT_CODE.
        "exit_code": exit_code,
        "derived_exit_code": exit_code,
        "cluster": job.cluster,
        "qos": job.qos or "normal",
        "priority": uint_no_val(getattr(job, "priority", 1)),
        "nodes": nodes,
        "node_count": uint_no_val(node_count),
        "cpus": uint_no_val(total_cpus),
        # Unset tasks fall back to the node count, cpus_per_task to 1
        # (slurm://src/slurmctld/job_mgr.c#_pack_default_job_details).
        "tasks": uint_no_val(job.tasks or (job.tasks_per_node or 1) * node_count),
        "cpus_per_task": uint_no_val(job.cpus_per_task or 1),
        "tasks_per_node": uint_no_val(job.tasks_per_node or None),
        "memory_per_node": uint_no_val(request.mem_per_node),
        "memory_per_cpu": uint_no_val(),
        "minimum_cpus_per_node": uint_no_val(request.cpus_per_node),
        "resv_name": getattr(job, "reservation", "") or "",
        "job_resources": {
            "nodes": {"count": node_count, "list": nodes, "allocation": []},
            "cpus": total_cpus,
        },
        # None -> UNLIMITED (infinite), matching a job with no --time.
        "time_limit": uint_no_val(time_limit, infinite=(time_limit is None)),
        "submit_time": ts(job.submit_time),
        # Real slurm reports an estimated start (and end) for pending jobs, so
        # fall back to submit_time rather than leaving these unset.
        "start_time": ts(job.start_time or job.submit_time),
        "end_time": ts(end or job.start_time or job.submit_time),
        # TIMESTAMP_NO_VAL; unset means the job was never suspended.
        "suspend_time": uint_no_val(),
        "standard_input": job.standard_input or "/dev/null",
        "standard_output": job.standard_output or "",
        "standard_error": job.standard_error or "",
        # slurm://src/plugins/data_parser/v0.0.45/parsers.c#JOB_INFO_STDOUT_EXP@26.05+
        **(
            {
                f"{key}_expanded": job_execution.expand_stdio_path(path, job, "") if path else ""
                for key, path in (
                    ("stdin", job.standard_input or "/dev/null"),
                    ("stdout", job.standard_output),
                    ("stderr", job.standard_error),
                )
            }
            if at_least("25.05")
            else {}
        ),
        "current_working_directory": job.working_directory or f"/home/{job.user}",
    }


# Cluster topology. Defaults to debug* node[001-004], compute node[005-100]
# (node specs match the usage simulator's standard node —
# sacct.py:_NODE_CPUS/_NODE_MEM_GB/_NODE_GPUS). Override per instance with the
# SLURM_EMULATOR_PARTITIONS env var so several emulators can look different: it
# accepts either counts (e.g. gpu:8,compute:32 — auto contiguous ranges) or
# explicit node ranges (e.g. debug:1-4,compute:5-100). dispatcher.py's sinfo
# derives its output from PARTITION_RANGES, so both views stay in sync.
_DEFAULT_PARTITIONS = "debug:1-4,compute:5-100"


def _parse_partition_ranges(spec: str) -> dict[str, tuple[int, int]]:
    """Parse ``name:first-last`` or ``name:count`` (counts get contiguous ranges)."""
    ranges: dict[str, tuple[int, int]] = {}
    nxt = 1
    for part in spec.split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        name, _, rng = part.partition(":")
        name, rng = name.strip(), rng.strip()
        if not name:
            continue
        if "-" in rng:
            a, b = rng.split("-", 1)
            first, last = int(a), int(b)
        else:
            first, last = nxt, nxt + int(rng) - 1
        ranges[name] = (first, last)
        nxt = last + 1
    return ranges


def _load_partition_ranges() -> dict[str, tuple[int, int]]:
    spec = os.environ.get("SLURM_EMULATOR_PARTITIONS", "").strip()
    try:
        return _parse_partition_ranges(spec) or _parse_partition_ranges(_DEFAULT_PARTITIONS)
    except (ValueError, IndexError):
        return _parse_partition_ranges(_DEFAULT_PARTITIONS)


PARTITION_RANGES = _load_partition_ranges()


# Per-partition QoS gate: AllowQos / DenyQos (slurm://src/common/part_record.h#allow_qos, slurm://src/common/part_record.h#deny_qos) plus the
# partition's own assigned QOS (qos_char, slurm://src/common/part_record.h#qos_char). Config mirrors the
# topology env var: SLURM_EMULATOR_PARTITION_QOS holds ``name=mode:csv`` entries
# (mode ∈ allow|deny|qos) separated by ``;``; several entries may target the
# same partition (e.g. an allow-list plus an assigned QoS). Empty ⇒ every
# partition permits all QoS, preserving the prior hardcoded-empty behaviour.
_MODE_KEYS = {"allow": "allowed", "deny": "deny", "qos": "assigned"}


def _empty_partition_qos() -> dict[str, str]:
    return {"allowed": "", "deny": "", "assigned": ""}


def _parse_partition_qos(spec: str) -> dict[str, dict[str, str]]:
    """Parse ``name=mode:csv;…`` into ``{name: {allowed, deny, assigned}}``."""
    cfg: dict[str, dict[str, str]] = {}
    for entry in spec.split(";"):
        entry = entry.strip()
        if not entry or "=" not in entry or ":" not in entry:
            continue
        name, _, rest = entry.partition("=")
        mode, _, values = rest.partition(":")
        name, mode, values = name.strip(), mode.strip().lower(), values.strip()
        if not name or mode not in _MODE_KEYS:
            continue
        cfg.setdefault(name, _empty_partition_qos())[_MODE_KEYS[mode]] = values
    return cfg


def _load_partition_qos() -> dict[str, dict[str, str]]:
    return _parse_partition_qos(os.environ.get("SLURM_EMULATOR_PARTITION_QOS", "").strip())


PARTITION_QOS = _load_partition_qos()


def partition_allows_qos(
    partition: str, qos: str, config: Optional[dict[str, dict[str, str]]] = None
) -> bool:
    """Return whether ``qos`` may run in ``partition`` under the AllowQos/DenyQos gate.

    An unconfigured partition (or empty AllowQos and DenyQos) permits all QoS.
    A non-empty AllowQos is exclusive and suppresses DenyQos (slurm.conf.5).
    """
    cfg = PARTITION_QOS if config is None else config
    part = cfg.get(partition)
    if not part:
        return True
    allowed = [q for q in part.get("allowed", "").split(",") if q]
    if allowed:
        return qos in allowed
    deny = [q for q in part.get("deny", "").split(",") if q]
    if deny:
        return qos not in deny
    return True


def set_partition_qos(
    partition: str,
    allowed: Optional[str] = None,
    deny: Optional[str] = None,
    assigned: Optional[str] = None,
) -> None:
    """Set the QoS gate for a partition (test/controller seed for the config)."""
    part = PARTITION_QOS.setdefault(partition, _empty_partition_qos())
    if allowed is not None:
        part["allowed"] = allowed
    if deny is not None:
        part["deny"] = deny
    if assigned is not None:
        part["assigned"] = assigned


def _node_names(partition: str) -> list[str]:
    first, last = PARTITION_RANGES[partition]
    return [f"node{i:03d}" for i in range(first, last + 1)]


# slurm://slurm/slurm.h#MEM_PER_CPU
MEM_PER_CPU = 0x8000000000000000


def _mem_str(megabytes: int) -> str:
    """Memory in a TRES string, units converted exactly (512G, 4000M)."""
    return _convert_exact(megabytes, 2)


def node_to_dict(name: str, now_ts: int, view: Optional[NodeView] = None) -> dict[str, Any]:
    """NODE (slurm://src/plugins/data_parser/v0.0.45/parsers.c#NODE@26.05+).

    ``view`` (``cluster_model.node_views``) supplies the live part: state
    flags and reason, allocated CPUs/memory, ``cpu_load`` (load average x 100,
    slurm://src/slurmd/slurmd/get_mach_stat.c#get_cpu_load), ``free_mem``
    (MB, UINT64_NO_VAL; unset for a DOWN node that does not report) and
    ``tres_used``. Without it the node is idle, as before.
    """
    mem_mb = _NODE_MEM_GB * 1024
    if view is None:
        view = NodeView(state=["IDLE"], free_mem=mem_mb)
    tres_used = ""
    if view.alloc_cpus:
        mem_used = view.alloc_memory
        tres_used = f"cpu={view.alloc_cpus},mem={_mem_str(mem_used)}"
        if view.gpus_used:
            tres_used += f",gres/gpu={view.gpus_used}"
    return {
        "name": name,
        "hostname": name,
        "address": name,
        "state": list(view.state),
        "architecture": "x86_64",
        "operating_system": "Linux",
        "cpus": _NODE_CPUS,
        "effective_cpus": _NODE_CPUS,
        "alloc_cpus": view.alloc_cpus,
        "alloc_idle_cpus": _NODE_CPUS - view.alloc_cpus,
        "alloc_memory": view.alloc_memory,
        "cpu_load": view.cpu_load,
        "free_mem": uint_no_val(view.free_mem),
        "real_memory": mem_mb,
        "sockets": 2,
        "cores": _NODE_CPUS // 2,
        "threads": 1,
        "boards": 1,
        "partitions": [p for p, _ in PARTITION_RANGES.items() if name in _node_names(p)],
        "features": [],
        "active_features": [],
        "gres": f"gpu:{_NODE_GPUS}",
        "gres_used": f"gpu:{view.gpus_used}",
        "boot_time": uint_no_val(now_ts),
        "last_busy": uint_no_val(now_ts),
        "slurmd_start_time": uint_no_val(now_ts),
        # PARSER_ARRAY(NODE) serializes weight as UINT32 (a plain int).
        "weight": 1,
        "tres": f"cpu={_NODE_CPUS},mem={_mem_str(mem_mb)},billing={_NODE_CPUS},"
        f"gres/gpu={_NODE_GPUS}",
        "tres_used": tres_used,
        "reason": view.reason,
        "reservation": view.reservation,
    }


def reservation_to_dict(resv: Reservation) -> dict[str, Any]:
    """RESERVATION_INFO (slurm://src/plugins/data_parser/v0.0.45/parsers.c#RESERVATION_INFO@26.05+).

    ``start_time``/``end_time`` are TIMESTAMP_NO_VAL structs, ``flags`` the
    RESERVATION_FLAGS names (``MAINT`` …), ``node_list`` a ranged hostlist.
    """

    def ts(value) -> dict[str, Any]:
        if value is not None and value == INFINITE_TIME:
            return uint_no_val(infinite=True)
        return uint_no_val(int(value.timestamp())) if value else uint_no_val()

    return {
        "accounts": resv.accounts,
        "burst_buffer": "",
        "core_count": len(resv.nodes) * _NODE_CPUS,
        "core_specializations": [],
        "end_time": ts(resv.end_time),
        "features": "",
        "flags": list(resv.flags),
        "groups": "",
        "licenses": "",
        "max_start_delay": 0,
        "name": resv.name,
        "node_count": len(resv.nodes),
        "node_list": compress_hostlist(resv.nodes),
        "partition": resv.partition,
        "purge_completed": {"time": uint_no_val()},
        "start_time": ts(resv.start_time),
        "tres": f"cpu={len(resv.nodes) * _NODE_CPUS}",
        "users": resv.users,
    }


def partition_to_dict(name: str) -> dict[str, Any]:
    first, last = PARTITION_RANGES[name]
    node_count = last - first + 1
    configured = f"node[{first:03d}-{last:03d}]"
    partition: dict[str, Any] = {
        "name": name,
        "nodes": {"total": node_count, "configured": configured, "allowed_allocation": "ALL"},
        "cpus": {"total": node_count * _NODE_CPUS, "task_binding": 0},
        "defaults": {"time": uint_no_val(infinite=True)},
        "maximums": {
            "nodes": uint_no_val(infinite=True),
            "time": uint_no_val(infinite=True),
            "cpus_per_node": uint_no_val(infinite=True),
        },
        "minimums": {"nodes": 1},
        "partition": {"state": ["UP"]},
    }
    # DefMemPerCPU (cluster-wide, node memory / CPUs) and no MaxMemPer*: the
    # raw def_mem_per_cpu carries the MEM_PER_CPU flag, decoded into
    # partition_memory_per_cpu/_node (slurm://src/plugins/data_parser/v0.0.45/parsers.c#"defaults/partition_memory_per_cpu"@26.05+).
    partition["defaults"]["partition_memory_per_cpu"] = uint_no_val(DEF_MEM_PER_CPU_MB)
    partition["defaults"]["partition_memory_per_node"] = uint_no_val()
    partition["maximums"]["partition_memory_per_cpu"] = uint_no_val()
    partition["maximums"]["partition_memory_per_node"] = uint_no_val(0)
    if at_least("26.05") and not at_least("master"):
        # 26.05 keeps the raw values as removed fields, dumped as 0; 26.11
        # drops them (slurm://src/plugins/data_parser/v0.0.45/parsers.c#"defaults/memory_per_cpu"@26.05).
        partition["defaults"]["memory_per_cpu"] = 0
        partition["maximums"]["memory_per_cpu"] = 0
    elif not at_least("26.05"):
        # Raw UINT64 before 26.05: MEM_PER_CPU | DefMemPerCPU, and 0 = unlimited.
        partition["defaults"]["memory_per_cpu"] = MEM_PER_CPU | DEF_MEM_PER_CPU_MB
        partition["maximums"]["memory_per_cpu"] = 0
    partition.update(
        {
            "priority": {"job_factor": 1, "tier": 1},
            "accounts": {"allowed": "", "deny": ""},
            "groups": {"allowed": ""},
            "qos": dict(PARTITION_QOS.get(name, _empty_partition_qos())),
        }
    )
    return partition


def all_node_names() -> list[str]:
    names: list[str] = []
    for partition in PARTITION_RANGES:
        names.extend(_node_names(partition))
    return sorted(set(names))
