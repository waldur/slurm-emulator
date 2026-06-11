"""Plain-dict serializers for v0.0.46 response objects.

Field names and paths are copied verbatim from the authoritative
parser tables in /Users/ilja/workspace/slurm/src/plugins/data_parser/
v0.0.46/parsers.c — ACCOUNT/USER/ASSOC (:8646-8800), QOS (:9321-9349),
JOB, NODE, PARTITION_INFO. ``*_NO_VAL`` typed fields render as
``{set, infinite, number}`` exactly like DUMP_FUNC(UINT64_NO_VAL)
(parsers.c:3197-3223). Deliberately a pragmatic subset: every field
Waldur's parsers touch, plus enough context to look real.
"""

from __future__ import annotations

from typing import Any, Optional

from emulator.commands.sacct import (
    _FAILED_STATES,
    _NODE_CPUS,
    _NODE_GPUS,
    _NODE_MEM_GB,
    SacctEmulator,
)
from emulator.core.database import QOS, Account, Association, Job, UsageRecord, User

# Canonical TRES ids as initialized by slurmdbd (tres_str.c defaults).
_TRES_IDS = {
    "cpu": 1,
    "mem": 2,
    "energy": 3,
    "node": 4,
    "billing": 5,
    "fs/disk": 6,
    "vmem": 7,
    "pages": 8,
    "gres/gpu": 1001,
}


def uint_no_val(number: Optional[int] = None, infinite: bool = False) -> dict[str, Any]:
    """``{set, infinite, number}`` struct for ``*_NO_VAL`` fields."""
    if infinite:
        return {"set": False, "infinite": True, "number": 0}
    if number is None:
        return {"set": False, "infinite": False, "number": 0}
    return {"set": True, "infinite": False, "number": int(number)}


def tres_entry(tres_type: str, count: int) -> dict[str, Any]:
    base_type = tres_type.split("/", 1)[0]
    name = tres_type.split("/", 1)[1] if "/" in tres_type else ""
    return {
        "type": base_type,
        "name": name,
        "id": _TRES_IDS.get(tres_type, 0),
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
    """ASSOC_SHORT (parsers.c:8646-8652)."""
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


def user_to_dict(user: User, associations: list[Association]) -> dict[str, Any]:
    return {
        "name": user.name,
        "administrator_level": ["None"],
        "default": {"account": user.default_account, "wckey": ""},
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


def assoc_to_dict(assoc: Association, account: Optional[Account]) -> dict[str, Any]:
    # Account-level limits live on the Account record; user-level on the
    # Association. Merge with the association taking precedence.
    limits: dict[str, int] = {}
    if account is not None:
        limits.update(account.limits)
    limits.update(assoc.limits)

    grp_tres_mins = _limits_tres(limits, "GrpTRESMins")
    grp_tres = _limits_tres(limits, "GrpTRES")
    max_tres_mins = _limits_tres(limits, "MaxTRESMins")

    return {
        "account": assoc.account,
        "user": assoc.user,
        "cluster": assoc.cluster,
        "partition": assoc.partition or "",
        "parent_account": assoc.parent or "",
        "is_default": True,
        "lineage": _lineage(assoc, account),
        # account.qos holds a CSV QoS list (sacctmgr "qos=a,b" semantics);
        # the REST payload renders it as a list of names.
        "qos": ([q for q in account.qos.split(",") if q] if account and not assoc.user else []),
        "shares_raw": account.fairshare if account else 1,
        "comment": "",
        "default": {
            "qos": (account.default_qos or account.qos.split(",")[0]) if account else "normal"
        },
        "flags": [],
        "max": {
            "jobs": {"active": uint_no_val(), "total": uint_no_val()},
            "tres": {
                "total": tres_list_from_dict(grp_tres),
                "group": {
                    "minutes": tres_list_from_dict(grp_tres_mins),
                    "active": [],
                },
                "minutes": {"per": {"job": tres_list_from_dict(max_tres_mins)}},
                "per": {"job": [], "node": []},
            },
            "per": {"account": {"wall_clock": uint_no_val()}},
        },
        "min": {"priority_threshold": uint_no_val()},
        "priority": uint_no_val(),
    }


def qos_to_dict(qos: QOS, qos_id: int) -> dict[str, Any]:
    max_wall = uint_no_val()
    if qos.max_wall:
        digits = "".join(ch for ch in qos.max_wall if ch.isdigit())
        if digits:
            max_wall = uint_no_val(int(digits))
    return {
        "name": qos.name,
        "description": qos.name,
        "id": qos_id,
        "flags": [f for f in qos.flags.split(",") if f] if qos.flags else [],
        "priority": uint_no_val(0),
        "usage_factor": {"set": True, "infinite": False, "number": 1.0},
        "usage_threshold": {"set": False, "infinite": False, "number": 0.0},
        "limits": {
            "max": {
                "active_jobs": {"accruing": uint_no_val(), "count": uint_no_val()},
                "tres": {
                    "total": tres_list_from_str(qos.grp_tres),
                    "minutes": {"total": [], "per": {"job": []}},
                    "per": {"job": [], "user": [], "account": [], "node": []},
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


def dbd_job_to_dict(record: UsageRecord) -> dict[str, Any]:
    """One slurmdb JOB per usage record — agrees with sacct output.

    Same math as SacctEmulator._row: Elapsed = node_hours in seconds,
    End = record timestamp, per-hour TRES rates from raw_tres.
    """
    rate = SacctEmulator._rate
    elapsed = int(record.node_hours * 3600)
    end = int(record.timestamp.timestamp())
    start = end - elapsed
    state = record.state or "COMPLETED"
    failed = state.startswith(_FAILED_STATES)

    cpus = rate(record, "CPU", _NODE_CPUS)
    mem_mb = rate(record, "Mem", _NODE_MEM_GB) * 1024
    gpus = rate(record, "GRES/gpu", _NODE_GPUS)
    tres_values = {"cpu": cpus, "mem": mem_mb, "node": 1, "billing": cpus}
    if gpus:
        tres_values["gres/gpu"] = gpus
    tres = tres_list_from_dict(tres_values)

    exit_code = {
        "status": ["ERROR"] if failed else ["SUCCESS"],
        "return_code": uint_no_val(1 if failed else 0),
    }
    return {
        "account": record.account,
        "allocation_nodes": 1,
        "array": {"job_id": 0},
        "cluster": record.cluster,
        "derived_exit_code": exit_code,
        "exit_code": exit_code,
        "flags": [],
        "group": record.user,
        "het": {"job_id": 0},
        "job_id": record.job_id,
        "kill_request_user": "",
        "name": f"job_{record.job_id}",
        "nodes": "node001",
        "partition": "compute",
        "qos": "normal",
        "state": {"current": [state], "reason": "None"},
        "steps": [],
        "time": {
            "elapsed": elapsed,
            "eligible": start,
            "start": start,
            "end": end,
            "submission": start,
            "suspended": 0,
            "limit": uint_no_val(infinite=True),
            "system": {"seconds": 0, "microseconds": 0},
            "user": {"seconds": 0, "microseconds": 0},
            "total": {"seconds": 0, "microseconds": 0},
        },
        "tres": {"allocated": tres, "requested": tres},
        "user": record.user,
        "wckey": {"wckey": "", "flags": []},
        "working_directory": f"/home/{record.user}",
    }


def ctld_job_to_dict(job: Job) -> dict[str, Any]:
    """JOB_INFO subset for /slurm/.../jobs (active job view)."""

    def ts(value) -> dict[str, Any]:
        return uint_no_val(int(value.timestamp())) if value else uint_no_val()

    return {
        "job_id": int(job.job_id) if str(job.job_id).isdigit() else 0,
        "name": f"job_{job.job_id}",
        "account": job.account,
        "user_name": job.user,
        "group_name": job.user,
        "partition": "compute",
        "job_state": [job.state],
        "state_reason": "None",
        "cluster": job.cluster,
        "qos": "normal",
        "nodes": "node001",
        "node_count": uint_no_val(1),
        "cpus": uint_no_val(_NODE_CPUS),
        "submit_time": ts(job.submit_time),
        "start_time": ts(job.start_time),
        "end_time": ts(job.end_time),
        "standard_input": "/dev/null",
        "standard_output": "",
        "standard_error": "",
        "current_working_directory": f"/home/{job.user}",
    }


# Static cluster topology — must stay consistent with the sinfo
# emulation (dispatcher.py:_handle_sinfo): debug* node[001-004],
# compute node[005-100]. Node specs match the usage simulator's
# standard node (sacct.py:_NODE_CPUS/_NODE_MEM_GB/_NODE_GPUS).
PARTITION_RANGES = {"debug": (1, 4), "compute": (5, 100)}


def _node_names(partition: str) -> list[str]:
    first, last = PARTITION_RANGES[partition]
    return [f"node{i:03d}" for i in range(first, last + 1)]


def node_to_dict(name: str, now_ts: int) -> dict[str, Any]:
    mem_mb = _NODE_MEM_GB * 1024
    return {
        "name": name,
        "hostname": name,
        "address": name,
        "state": ["IDLE"],
        "architecture": "x86_64",
        "operating_system": "Linux",
        "cpus": _NODE_CPUS,
        "effective_cpus": _NODE_CPUS,
        "alloc_cpus": 0,
        "alloc_idle_cpus": _NODE_CPUS,
        "alloc_memory": 0,
        "real_memory": mem_mb,
        "sockets": 2,
        "cores": _NODE_CPUS // 2,
        "threads": 1,
        "boards": 1,
        "partitions": [p for p, _ in PARTITION_RANGES.items() if name in _node_names(p)],
        "features": [],
        "active_features": [],
        "gres": f"gpu:{_NODE_GPUS}",
        "gres_used": "gpu:0",
        "boot_time": uint_no_val(now_ts),
        "last_busy": uint_no_val(now_ts),
        "slurmd_start_time": uint_no_val(now_ts),
        "weight": uint_no_val(1),
        "tres": f"cpu={_NODE_CPUS},mem={mem_mb}M,billing={_NODE_CPUS},gres/gpu={_NODE_GPUS}",
        "tres_used": "",
        "reason": "",
    }


def partition_to_dict(name: str) -> dict[str, Any]:
    first, last = PARTITION_RANGES[name]
    node_count = last - first + 1
    configured = f"node[{first:03d}-{last:03d}]"
    return {
        "name": name,
        "nodes": {"total": node_count, "configured": configured, "allowed_allocation": "ALL"},
        "cpus": {"total": node_count * _NODE_CPUS, "task_binding": 0},
        "defaults": {
            "memory_per_cpu": 0,
            "time": uint_no_val(infinite=True),
        },
        "maximums": {
            "nodes": uint_no_val(infinite=True),
            "time": uint_no_val(infinite=True),
            "cpus_per_node": uint_no_val(infinite=True),
            "memory_per_cpu": uint_no_val(),
        },
        "minimums": {"nodes": 1},
        "partition": {"state": ["UP"]},
        "priority": {"job_factor": 1, "tier": 1},
        "accounts": {"allowed": "", "deny": ""},
        "groups": {"allowed": ""},
        "qos": {"allowed": "", "deny": "", "assigned": ""},
    }


def all_node_names() -> list[str]:
    names: list[str] = []
    for partition in PARTITION_RANGES:
        names.extend(_node_names(partition))
    return sorted(set(names))
