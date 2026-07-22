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

import os
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


def _assoc_qos_list(assoc: Association, account: Optional[Account]) -> list[str]:
    if assoc.qos_list:
        return list(assoc.qos_list)
    if account and not assoc.user:
        return [q for q in account.qos.split(",") if q]
    return []


def _assoc_default_qos(assoc: Association, account: Optional[Account]) -> str:
    if assoc.def_qos:
        return assoc.def_qos
    if account:
        return account.default_qos or account.qos.split(",")[0]
    return "normal"


def assoc_to_dict(
    assoc: Association, account: Optional[Account], is_default: bool = True
) -> dict[str, Any]:
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
        "is_default": is_default,
        "lineage": _lineage(assoc, account),
        # A per-association grant (slurmdb_assoc_rec_t.qos_list) wins; otherwise
        # an account-level row renders the account QoS list, and a user row with
        # no grant renders nothing of its own.
        "qos": _assoc_qos_list(assoc, account),
        "shares_raw": account.fairshare if account else 1,
        "comment": "",
        "default": {"qos": _assoc_default_qos(assoc, account)},
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
            # Real v0.0.46 QOS: grace_time is a plain UINT32 (seconds) at
            # limits/grace_time (parsers.c PARSER_ARRAY(QOS)), not a NO_VAL struct.
            "grace_time": max(qos.grace_time, 0),
            "max": {
                "active_jobs": {"accruing": uint_no_val(), "count": uint_no_val()},
                "tres": {
                    "total": tres_list_from_str(qos.grp_tres),
                    "minutes": {"total": [], "per": {"job": []}},
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
    """JOB_INFO subset for /slurm/.../jobs (active job view).

    Field names/shapes follow the v0.0.46 JOB_INFO parser
    (parsers.c PARSER_ARRAY(JOB_INFO)): ``job_resources`` (with
    ``nodes.count``), ``exit_code``/``derived_exit_code`` (PROCESS_EXIT_CODE),
    ``time_limit``/``priority``/``suspend_time`` as ``*_NO_VAL`` structs, etc.
    """

    def ts(value) -> dict[str, Any]:
        return uint_no_val(int(value.timestamp())) if value else uint_no_val()

    node_count = getattr(job, "node_count", 1) or 1
    total_cpus = _NODE_CPUS * node_count
    time_limit = getattr(job, "time_limit", None)
    failed = job.state.startswith(_FAILED_STATES)
    exit_code = {
        "status": ["ERROR"] if failed else ["SUCCESS"],
        "return_code": uint_no_val(1 if failed else 0),
    }

    return {
        "job_id": int(job.job_id) if str(job.job_id).isdigit() else 0,
        "name": job.name or f"job_{job.job_id}",
        "account": job.account,
        "user_name": job.user,
        "group_name": job.user,
        "partition": job.partition or "compute",
        "job_state": [job.state],
        "state_reason": "None",
        "state_description": "",
        # JOB_INFO carries exit_code/derived_exit_code as PROCESS_EXIT_CODE.
        "exit_code": exit_code,
        "derived_exit_code": exit_code,
        "cluster": job.cluster,
        "qos": job.qos or "normal",
        "priority": uint_no_val(getattr(job, "priority", 1)),
        "nodes": "node001",
        "node_count": uint_no_val(node_count),
        "cpus": uint_no_val(total_cpus),
        "job_resources": {
            "nodes": {"count": node_count, "list": "node001", "allocation": []},
            "cpus": total_cpus,
        },
        # None -> UNLIMITED (infinite), matching a job with no --time.
        "time_limit": uint_no_val(time_limit, infinite=(time_limit is None)),
        "submit_time": ts(job.submit_time),
        # Real slurm reports an estimated start (and end) for pending jobs, so
        # fall back to submit_time rather than leaving these unset.
        "start_time": ts(job.start_time or job.submit_time),
        "end_time": ts(job.end_time or job.start_time or job.submit_time),
        # TIMESTAMP_NO_VAL; unset means the job was never suspended.
        "suspend_time": uint_no_val(),
        "standard_input": job.standard_input or "/dev/null",
        "standard_output": job.standard_output or "",
        "standard_error": job.standard_error or "",
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


# Per-partition QoS gate: AllowQos / DenyQos (part_record.h:65/79) plus the
# partition's own assigned QOS (qos_char, part_record.h:106). Config mirrors the
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
        # PARSER_ARRAY(NODE) serializes weight as UINT32 (a plain int).
        "weight": 1,
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
        "qos": dict(PARTITION_QOS.get(name, _empty_partition_qos())),
    }


def all_node_names() -> list[str]:
    names: list[str] = []
    for partition in PARTITION_RANGES:
        names.extend(_node_names(partition))
    return sorted(set(names))
