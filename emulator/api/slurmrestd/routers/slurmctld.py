"""/slurm/v0.0.45 endpoints (openapi/slurmctld plugin emulation).

Read paths plus POST /job/submit and DELETE /job/{job_id} (the scancel
equivalent). Nodes/partitions are served from the static topology in
schemas.py — the same picture sinfo paints — with each node's live state
(allocations, DRAIN/DOWN overrides, background load, reservations) from
``emulator/core/cluster_model.py``. Reservations come from
``SLURM_EMULATOR_RESERVATIONS`` and, from 25.05, from ``POST /reservation``,
``POST /reservations/`` and ``DELETE /reservation/{name}``
(``emulator/core/reservations.py`` holds the slurmctld side); licenses are an
empty-list stub.

Intentional deviation: ``POST /reservation(s)`` ignores ``core_count``,
``tres``, ``licenses``, ``burst_buffer``, ``features`` and
``max_start_delay`` instead of reserving cores or licenses
(slurm://src/slurmctld/reservation.c#create_resv supports them).
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, Request
from starlette.concurrency import run_in_threadpool

from emulator.api.slurmrestd.auth import slurmrestd_auth
from emulator.api.slurmrestd.envelope import (
    ESLURM_INVALID_JOB_ID,
    ESLURM_RESERVATION_INVALID,
    ESLURM_REST_INVALID_QUERY,
    ESLURM_REST_UNKNOWN_URL,
    ESLURM_REST_UNKNOWN_URL_METHOD,
    ESLURM_USER_ID_UNKNOWN,
    SLURMCTLD_PLUGIN,
    SlurmrestdRejectError,
    found_nothing_warning,
    make_response,
    slurm_error,
    slurm_release,
    strerror,
    validate_version,
)
from emulator.api.slurmrestd.schemas import (
    PARTITION_RANGES,
    all_node_names,
    ctld_job_to_dict,
    node_to_dict,
    partition_to_dict,
    reservation_to_dict,
    uint_no_val,
)
from emulator.api.slurmrestd.state import StateDep
from emulator.commands.slurm_time import ParseTimeError, parse_time_spec
from emulator.core.accounting import admit_job
from emulator.core.cluster_model import (
    expand_hostlist,
    job_may_access,
    node_views,
    sync_reservations,
)
from emulator.core.database import Job, SlurmDatabase
from emulator.core.reservations import (
    DURATION_INFINITE,
    ESLURM_NODES_BUSY,
    ESLURM_RESERVATION_ACCESS,
    ReservationDesc,
    ReservationDescError,
    ReservationError,
    create_reservation,
    delete_reservation,
    order_flags,
    update_reservation,
    validate_create_desc,
)
from emulator.core.reservations import FLAG_NAMES as RESV_FLAG_NAMES
from emulator.core.reservations import find as find_reservation
from emulator.core.scheduler import advance_job_states, job_clock_now
from emulator.slurm_version import at_least

router = APIRouter(
    prefix="/slurm/{version}",
    dependencies=[Depends(slurmrestd_auth), Depends(validate_version)],
)


def _respond(request, state, payload=None, errors=None, warnings=None):
    return make_response(request, SLURMCTLD_PLUGIN, state.cluster, payload, errors, warnings)


def _float_no_val(number: float) -> dict[str, Any]:
    return {"set": True, "infinite": False, "number": number}


async def _json_body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


def _submit_int(value: object, default: Optional[int]) -> Optional[int]:
    """Read an int from a submit field that may be plain, a NO_VAL struct, or a string."""
    if isinstance(value, dict):
        if not value.get("set"):
            return default
        number = value.get("number", default)
        return int(number) if isinstance(number, (int, float, str)) else default
    if isinstance(value, (int, float, str)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    return default


def _submit_nodes(job_desc: dict[str, Any]) -> int:
    """Minimum node count from ``nodes`` ("4", "2-4", "2-4:2") or ``minimum_nodes``.

    JOB_DESC_MSG_NODES parses the ``nodes`` range into min/max
    (slurm://src/plugins/data_parser/v0.0.45/parsers.c#JOB_DESC_MSG_NODES@26.05+);
    the emulator allocates the minimum (see cluster_model deviations).
    """
    value = job_desc.get("nodes")
    if isinstance(value, str) and value.strip():
        head = value.split(":", 1)[0].split("-", 1)[0].strip()
        if head.isdigit():
            return max(int(head), 1)
    count = _submit_int(value, None)
    if count:
        return max(count, 1)
    minimum = _submit_int(job_desc.get("minimum_nodes"), None)
    return max(minimum or 1, 1)


def _submit_count(value: object) -> int:
    """Optional positive count (tasks, cpus_per_task, memory_per_node …); 0 = unset."""
    number = _submit_int(value, None)
    return number if number and number > 0 else 0


def _submit_exclusive(value: object) -> bool:
    """``shared`` (JOB_SHARED flags): ``none`` / ``topo`` take whole nodes."""
    flags = [value] if isinstance(value, str) else value if isinstance(value, list) else []
    return any(str(f).lower() in ("none", "topo") for f in flags)


def _env_to_dict(value: object) -> dict[str, str]:
    """Job ``environment`` is a ``KEY=VALUE`` list in data_parser >=0.0.39; accept dicts too."""
    if isinstance(value, dict):
        return {str(k): str(v) for k, v in value.items()}
    if isinstance(value, list):
        out: dict[str, str] = {}
        for item in value:
            if isinstance(item, str) and "=" in item:
                key, _, val = item.partition("=")
                out[key] = val
        return out
    return {}


# --- ping / diag / conf ---


def controller_ping_entry() -> dict[str, Any]:
    """One CONTROLLER_PING row in the active target's dialect.

    - 24.11 to 25.11 (slurm://src/plugins/data_parser/v0.0.44/parsers.c#CONTROLLER_PING@25.11+):
      ``hostname``, deprecated ``pinged`` ("UP") and ``mode`` ("primary"),
      ``responding``, ``latency``, ``primary``.
    - 26.05+ (slurm://src/plugins/data_parser/v0.0.45/parsers.c#CONTROLLER_PING@26.05+):
      the deprecated aliases are gone and ``status`` (ERROR string) is added.
    """
    entry: dict[str, Any] = {"hostname": "localhost"}
    if not at_least("26.05"):
        entry["pinged"] = "UP"
    entry["responding"] = True
    entry["latency"] = 123
    if not at_least("26.05"):
        entry["mode"] = "primary"
    entry["primary"] = "primary"
    if at_least("26.05"):
        entry["status"] = "No error"
    return entry


@router.get("/ping/")
async def ping(
    request: Request,
    state: StateDep,
):
    return _respond(request, state, {"pings": [controller_ping_entry()]})


@router.get("/diag/")
async def diag(
    request: Request,
    state: StateDep,
):
    db = state.database
    jobs = [j for j in db.jobs.values() if j.cluster == db.current_cluster]
    statistics = {
        "server_thread_count": 1,
        "req_time": state.now_ts(),
        "req_time_start": state.now_ts(),
        "jobs_submitted": len(db.usage_records) + len(jobs),
        "jobs_started": len(jobs),
        "jobs_running": sum(1 for j in jobs if j.state == "RUNNING"),
        "jobs_pending": sum(1 for j in jobs if j.state == "PENDING"),
        "jobs_completed": len(db.usage_records),
        "jobs_canceled": sum(1 for j in jobs if j.state == "CANCELLED"),
        "jobs_failed": 0,
        "agent_count": 0,
        "schedule_cycle_last": 0,
    }
    return _respond(request, state, {"statistics": statistics})


@router.get("/conf")
async def conf(
    request: Request,
    state: StateDep,
):
    config = {
        "cluster_name": state.cluster,
        "slurm_version": slurm_release(),
        "accounting_storage_type": "accounting_storage/slurmdbd",
        "scheduler_type": "sched/backfill",
        "select_type": "select/cons_tres",
        "slurm_user_name": "slurm",
        "slurmctld_host": ["localhost"],
        "priority_type": "priority/multifactor",
    }
    return _respond(request, state, {"config": config})


# --- jobs ---


def _cluster_jobs(db: SlurmDatabase) -> list:
    return [j for j in db.jobs.values() if j.cluster == db.current_cluster]


@router.post("/job/submit")
async def submit_job(
    request: Request,
    state: StateDep,
):
    """Job submission endpoint (openapi_job_submit).

    Accepts ``{"job": {...}}`` with the batch ``script`` inside the job body
    (data_parser >= 0.0.41, incl. v0.0.45) as well as the pre-0.0.41 sibling
    ``script``. The response mirrors OPENAPI_JOB_SUBMIT_RESPONSE
    (slurm://src/plugins/data_parser/v0.0.45/parsers.c#OPENAPI_JOB_SUBMIT_RESPONSE@26.05+): top-level ``job_id`` / ``step_id`` /
    ``job_submit_user_msg``.
    """
    body = await _json_body(request)
    job_desc = body.get("job")
    if isinstance(job_desc, list):
        job_desc = job_desc[0] if job_desc else {}
    if not isinstance(job_desc, dict):
        job_desc = {}
    script = body.get("script") or job_desc.get("script") or ""

    db = state.database
    user = str(job_desc.get("user_name") or getattr(request.state, "slurm_user", "root") or "root")
    partition = job_desc.get("partition") or "compute"
    # Admission as in slurmctld: the USER_ID parser must resolve the user
    # (NSS mode, slurm://src/plugins/data_parser/v0.0.45/parsers.c#USER_ID@26.05+),
    # then _job_create needs an association for the account
    # (slurm://src/slurmctld/job_mgr.c#_job_create →
    # slurm://src/common/assoc_mgr.c#assoc_mgr_fill_in_assoc). Both errnos
    # are in the slurmctld range → 422 (slurm://src/common/http.c#http_status_from_error@25.11+).
    # NSS lookups may block on the directory, so keep them off the event loop.
    admission = await run_in_threadpool(
        admit_job, db, user, job_desc.get("account") or "", partition
    )
    if admission.error is not None:
        source = (
            "job_desc_msg_t.user_id"
            if admission.error == ESLURM_USER_ID_UNKNOWN
            else "slurm_submit_batch_job()"
        )
        return _respond(
            request, state, errors=[slurm_error(admission.message, admission.error, source)]
        )
    identity = admission.identity
    account = admission.account

    # An unknown reservation, or one the job may not use, is refused at submit
    # (slurm://src/slurmctld/reservation.c#validate_job_resv,
    # slurm://src/slurmctld/reservation.c#_valid_job_access_resv).
    reservation = str(job_desc.get("reservation") or "")
    sync_reservations(db, job_clock_now(state.time_engine))
    if reservation:
        resv = db.reservations.get(reservation)
        probe = Job(
            job_id="0",
            account=account,
            user=user,
            state="PENDING",
            uid=identity.uid if identity else None,
            group_name=identity.group if identity else "",
        )
        errno = (
            ESLURM_RESERVATION_INVALID
            if resv is None
            else (None if job_may_access(db, resv, probe) else ESLURM_RESERVATION_ACCESS)
        )
        if errno is not None:
            return _respond(
                request,
                state,
                errors=[slurm_error(strerror(errno), errno, "slurm_submit_batch_job()")],
            )

    jid = db.allocate_job_id()
    job = Job(
        job_id=str(jid),
        account=account,
        user=user,
        state="PENDING",
        submit_time=job_clock_now(state.time_engine),
        cluster=db.current_cluster,
        name=job_desc.get("name") or f"job_{jid}",
        partition=partition,
        qos=job_desc.get("qos") or "normal",
        working_directory=job_desc.get("current_working_directory")
        or (identity.home if identity and identity.home else f"/home/{user}"),
        script=script,
        standard_output=job_desc.get("standard_output") or "",
        standard_error=job_desc.get("standard_error") or "",
        standard_input=job_desc.get("standard_input") or "/dev/null",
        # Resource request (slurm://src/plugins/data_parser/v0.0.45/parsers.c#JOB_DESC_MSG@26.05+):
        # nodes / minimum_nodes, tasks, tasks_per_node, cpus_per_task,
        # memory_per_node (MB) and time_limit (minutes), each plain or NO_VAL.
        node_count=_submit_nodes(job_desc),
        tasks=_submit_count(job_desc.get("tasks")),
        tasks_per_node=_submit_count(job_desc.get("tasks_per_node")),
        cpus_per_task=_submit_count(job_desc.get("cpus_per_task")),
        memory_per_node=_submit_count(job_desc.get("memory_per_node")),
        reservation=reservation,
        exclusive=_submit_exclusive(job_desc.get("shared")),
        priority=_submit_int(job_desc.get("priority"), 1) or 1,
        time_limit=_submit_count(job_desc.get("time_limit")) or None,
        environment=_env_to_dict(job_desc.get("environment")),
        constraints=job_desc.get("constraints") or "",
        uid=identity.uid if identity else None,
        gid=identity.gid if identity else None,
        group_name=identity.group if identity else "",
    )
    db.add_job(job)
    state.commit()

    # STEP_ID serializes to a string; a batch submission's step is "batch"
    # (STEP_NAMES), matching real slurm's job-submit response.
    return _respond(
        request,
        state,
        {"job_id": jid, "step_id": "batch", "job_submit_user_msg": ""},
    )


@router.get("/jobs/")
async def get_jobs(
    request: Request,
    state: StateDep,
):
    if advance_job_states(state.database, state.time_engine):
        state.commit()
    payload = [ctld_job_to_dict(j) for j in _cluster_jobs(state.database)]
    return _respond(
        request,
        state,
        {
            "jobs": payload,
            "last_update": uint_no_val(state.now_ts()),
            "last_backfill": uint_no_val(state.now_ts()),
        },
    )


@router.get("/jobs/state/")
async def get_jobs_state(
    request: Request,
    state: StateDep,
):
    if advance_job_states(state.database, state.time_engine):
        state.commit()
    payload = [
        {
            "job_id": int(j.job_id) if str(j.job_id).isdigit() else 0,
            "state": [j.state],
        }
        for j in _cluster_jobs(state.database)
    ]
    return _respond(request, state, {"jobs": payload})


@router.get("/job/{job_id}")
async def get_job(
    job_id: str,
    request: Request,
    state: StateDep,
):
    if advance_job_states(state.database, state.time_engine):
        state.commit()
    job = state.database.get_job(job_id)
    if job is None:
        return _respond(
            request,
            state,
            {"jobs": []},
            errors=[
                slurm_error(f"Failure query job {job_id}", ESLURM_INVALID_JOB_ID, request.url.path)
            ],
        )
    return _respond(
        request,
        state,
        {"jobs": [ctld_job_to_dict(job)], "last_update": uint_no_val(state.now_ts())},
    )


@router.delete("/job/{job_id}")
async def cancel_job(
    job_id: str,
    request: Request,
    state: StateDep,
    signal: Optional[str] = None,  # noqa: ARG001 - accepted, not modeled
):
    job = state.database.get_job(job_id)
    if job is None:
        return _respond(
            request,
            state,
            errors=[
                slurm_error(
                    f"Failure cancelling job {job_id}", ESLURM_INVALID_JOB_ID, request.url.path
                )
            ],
        )
    if job.state in ("RUNNING", "PENDING"):
        job.state = "CANCELLED"
        # On the job clock, like start/submit, so the run length is right;
        # the job's nodes are free again and it lands in accounting.
        job.end_time = job_clock_now(state.time_engine)
        advance_job_states(state.database, state.time_engine)
    state.commit()
    status = [
        {
            "job_id": uint_no_val(int(job_id) if job_id.isdigit() else None),
            "step_id": job_id,
            "error": "No error",
            "error_code": 0,
            "error_message": "",
            "federation": {"sibling": ""},
        }
    ]
    return _respond(request, state, {"status": status})


# --- nodes / partitions ---


def _node_views(state) -> dict:
    """Bring jobs up to date, then derive each node's live state on the job clock."""
    if advance_job_states(state.database, state.time_engine):
        state.commit()
    return node_views(state.database, job_clock_now(state.time_engine))


@router.get("/nodes/")
async def get_nodes(
    request: Request,
    state: StateDep,
):
    now = state.now_ts()
    views = _node_views(state)
    payload = [node_to_dict(name, now, views.get(name)) for name in all_node_names()]
    return _respond(request, state, {"nodes": payload, "last_update": uint_no_val(now)})


@router.get("/node/{node_name}")
async def get_node(
    node_name: str,
    request: Request,
    state: StateDep,
):
    now = state.now_ts()
    if node_name not in all_node_names():
        return _respond(
            request,
            state,
            {"nodes": [], "last_update": uint_no_val(now)},
            warnings=[found_nothing_warning("slurm_load_node()", request)],
        )
    return _respond(
        request,
        state,
        {
            "nodes": [node_to_dict(node_name, now, _node_views(state).get(node_name))],
            "last_update": uint_no_val(now),
        },
    )


@router.get("/partitions/")
async def get_partitions(
    request: Request,
    state: StateDep,
):
    payload = [partition_to_dict(name) for name in PARTITION_RANGES]
    return _respond(
        request,
        state,
        {"partitions": payload, "last_update": uint_no_val(state.now_ts())},
    )


@router.get("/partition/{partition_name}")
async def get_partition(
    partition_name: str,
    request: Request,
    state: StateDep,
):
    if partition_name not in PARTITION_RANGES:
        return _respond(
            request,
            state,
            {"partitions": [], "last_update": uint_no_val(state.now_ts())},
            warnings=[found_nothing_warning("slurm_load_partitions()", request)],
        )
    return _respond(
        request,
        state,
        {
            "partitions": [partition_to_dict(partition_name)],
            "last_update": uint_no_val(state.now_ts()),
        },
    )


# --- shares / reservations / licenses ---


@router.get("/shares")
async def get_shares(
    request: Request,
    state: StateDep,
):
    db = state.database
    cluster = db.current_cluster
    total_seconds = sum(int(r.node_hours * 3600) for r in db.usage_records if r.cluster == cluster)

    def share_obj(name, parent, fairshare, usage_seconds, obj_type):
        normalized = usage_seconds / total_seconds if total_seconds > 0 else 0.0
        return {
            "id": 0,
            "cluster": cluster,
            "name": name,
            "parent": parent,
            "partition": "",
            "shares": uint_no_val(fairshare),
            "shares_normalized": _float_no_val(1.0),
            "usage": usage_seconds,
            "usage_normalized": _float_no_val(normalized),
            "effective_usage": _float_no_val(normalized),
            "fairshare": {"factor": _float_no_val(0.5), "level": _float_no_val(0.0)},
            "type": [obj_type],
        }

    shares: list[dict[str, Any]] = []
    for account in db.list_accounts():
        records = [
            r for r in db.usage_records if r.account == account.name and r.cluster == cluster
        ]
        account_seconds = int(sum(r.node_hours for r in records) * 3600)
        shares.append(
            share_obj(
                account.name,
                account.parent or "root",
                account.fairshare,
                account_seconds,
                "account",
            )
        )
        for user in db.list_account_users(account.name):
            user_seconds = int(sum(r.node_hours for r in records if r.user == user) * 3600)
            shares.append(share_obj(user, account.name, 1, user_seconds, "user"))

    return _respond(request, state, {"shares": {"shares": shares}})


def _reservations(state) -> list:
    if sync_reservations(state.database, job_clock_now(state.time_engine)):
        state.commit()
    return sorted(state.database.reservations.values(), key=lambda r: (r.start_time, r.name))


@router.get("/reservations/")
async def get_reservations(
    request: Request,
    state: StateDep,
):
    payload = [reservation_to_dict(r) for r in _reservations(state)]
    return _respond(
        request,
        state,
        {"reservations": payload, "last_update": uint_no_val(state.now_ts())},
    )


@router.get("/reservation/{reservation_name}")
async def get_reservation(
    reservation_name: str,
    request: Request,
    state: StateDep,
):
    """slurm://src/slurmrestd/plugins/openapi/slurmctld/reservations.c#"Unable to find reservation".

    The name matches case-insensitively. With no reservation in the system
    the handler reports "Unable to query reservations" without an error
    number (HTTP 200); an unknown name is ``ESLURM_REST_INVALID_QUERY``.
    Neither dumps a ``reservations`` list.
    """
    records = _reservations(state)
    if not records:
        return _respond(
            request,
            state,
            errors=[
                {
                    "description": "Unable to query reservations",
                    "error_number": 0,
                    "error": strerror(0),
                    "source": "slurm_load_reservations()",
                }
            ],
        )
    found = next((r for r in records if r.name.lower() == reservation_name.lower()), None)
    if found is None:
        return _respond(
            request,
            state,
            errors=[
                slurm_error(
                    f"Unable to find reservation {reservation_name}",
                    ESLURM_REST_INVALID_QUERY,
                    # __func__ of the handler that reports it.
                    "_get_single_reservation" if at_least("25.05") else "op_handler_reservation",
                )
            ],
        )
    return _respond(
        request,
        state,
        {"reservations": [reservation_to_dict(found)], "last_update": uint_no_val(state.now_ts())},
    )


# --- reservation writes (25.05+) ---


class _DescParseError(ValueError):
    """A RESERVATION_DESC_MSG field the data_parser would refuse."""


def _desc_int(value: object) -> Optional[int]:
    """UINT32_NO_VAL: plain number, numeric string or ``{set, infinite, number}``."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise _DescParseError(value)
    if isinstance(value, dict):
        if value.get("infinite"):
            return DURATION_INFINITE
        if not value.get("set", True):
            return None
        value = value.get("number")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value)
    raise _DescParseError(value)


def _desc_time(value: object, now: datetime) -> Optional[datetime]:
    """TIMESTAMP_NO_VAL: UNIX time, ``{set, number}`` or a parse_time() string.

    slurm://src/plugins/data_parser/v0.0.45/parsers.c#_parse_timestamp@26.05+
    """
    if value is None:
        return None
    if isinstance(value, str) and not value.strip().lstrip("-").isdigit():
        # parse_time(str, 0); time 0 ("nothing specified") is a failure too.
        try:
            return parse_time_spec(value, now)
        except ParseTimeError as exc:
            raise _DescParseError(value) from exc
    number = _desc_int(value)
    if number is None or number == DURATION_INFINITE:
        return None
    return datetime.fromtimestamp(number)  # noqa: DTZ006 - naive local, like the job clock


def _desc_csv(value: object) -> Optional[str]:
    """CSV_STRING / HOSTLIST_STRING: a comma string or a list of strings."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, list) and all(isinstance(v, str) for v in value):
        return ",".join(value)
    raise _DescParseError(value)


def _desc_str(value: object) -> Optional[str]:
    if value is None or isinstance(value, str):
        return value
    raise _DescParseError(value)


def _desc_flags(value: object) -> Optional[list[str]]:
    """RESERVATION_FLAGS names (slurm://src/plugins/data_parser/v0.0.45/parsers.c#RESERVATION_FLAGS@26.05+)."""
    if value is None:
        return None
    items = value.split(",") if isinstance(value, str) else value
    if not isinstance(items, list):
        raise _DescParseError(value)
    flags = []
    for item in items:
        name = str(item).strip().upper()
        if name not in RESV_FLAG_NAMES:
            raise _DescParseError(item)
        flags.append(name)
    return flags or None


def _parse_resv_desc(raw: object, now: datetime) -> ReservationDesc:
    """RESERVATION_DESC_MSG (slurm://src/plugins/data_parser/v0.0.45/parsers.c#RESERVATION_DESC_MSG@26.05+).

    Core/TRES/license/burst-buffer/feature fields are accepted and ignored
    (whole-node reservations only, see emulator/core/reservations.py).
    """
    if not isinstance(raw, dict):
        raise _DescParseError(raw)
    purge = raw.get("purge_completed")
    if purge is not None and not isinstance(purge, dict):
        raise _DescParseError(purge)
    return ReservationDesc(
        name=_desc_str(raw.get("name")) or None,
        start_time=_desc_time(raw.get("start_time"), now),
        end_time=_desc_time(raw.get("end_time"), now),
        duration=_desc_int(raw.get("duration")),
        node_list=_desc_csv(raw.get("node_list")) or None,
        node_cnt=_desc_int(raw.get("node_count")),
        partition=_desc_str(raw.get("partition")),
        flags=_desc_flags(raw.get("flags")),
        users=_desc_csv(raw.get("users")) or None,
        accounts=_desc_csv(raw.get("accounts")) or None,
        groups=_desc_csv(raw.get("groups")) or None,
        comment=_desc_str(raw.get("comment")),
    )


def _parse_resv_list(raw: object) -> Optional[list]:
    """RESERVATION_MOD_REQ: ``{"reservations": [...]}``; ``None`` when the key is missing."""
    if not isinstance(raw, dict):
        raise _DescParseError(raw)
    items = raw.get("reservations")
    if items is not None and not isinstance(items, list):
        raise _DescParseError(items)
    return items


def _ts_no_val(value: Optional[datetime]) -> dict[str, Any]:
    return uint_no_val(int(value.timestamp())) if value else uint_no_val()


def _desc_to_dict(desc: ReservationDesc) -> dict[str, Any]:
    """Dump of the request as OPENAPI_RESERVATION_MOD_RESP echoes it.

    The handler dumps its own (parsed) resv_desc_msg_t, not the stored
    record (slurm://src/slurmrestd/plugins/openapi/slurmctld/reservations.c#_mod_reservation@25.05+):
    CSV_STRING / HOSTLIST_STRING as lists, NO_VAL numbers and timestamps as
    ``{set, infinite, number}``, unset flags as ``[]``.
    """

    def csv(value: Optional[str]) -> list[str]:
        return [v for v in (value or "").split(",") if v]

    def no_val(value: Optional[int]) -> dict[str, Any]:
        if value == DURATION_INFINITE:
            return uint_no_val(infinite=True)
        return uint_no_val(value)

    node_list = desc.node_list or ""
    hosts = ["ALL"] if node_list.upper() == "ALL" else expand_hostlist(node_list)
    return {
        "accounts": csv(desc.accounts),
        "burst_buffer": "",
        "comment": desc.comment or "",
        "core_count": uint_no_val(),
        "duration": no_val(desc.duration),
        "end_time": _ts_no_val(desc.end_time),
        "features": "",
        "flags": order_flags(list(desc.flags or [])),
        "groups": csv(desc.groups),
        "licenses": [],
        "max_start_delay": uint_no_val(),
        "name": desc.name or "",
        "node_count": no_val(desc.node_cnt),
        "node_list": hosts,
        "partition": desc.partition or "",
        "purge_completed": {"time": uint_no_val()},
        "start_time": _ts_no_val(desc.start_time),
        "tres": [],
        "users": csv(desc.users),
    }


def _mod_one(db: SlurmDatabase, desc: ReservationDesc, now: datetime) -> Optional[dict[str, Any]]:
    """Create or update one reservation; the error entry on failure.

    Exists -> update, else validate + create
    (slurm://src/slurmrestd/plugins/openapi/slurmctld/reservations.c#_create_or_update_each_resv@25.05+).
    """
    name = desc.name or ""
    if find_reservation(db, desc.name) is not None:
        try:
            update_reservation(db, desc, now)
        except ReservationError as exc:
            return slurm_error(
                f"Error updating reservation {name}", exc.errno, "slurm_update_reservation"
            )
        return None
    try:
        validate_create_desc(desc)
    except ReservationDescError as exc:
        return slurm_error(str(exc), ESLURM_RESERVATION_INVALID, "validate_resv_create_desc")
    try:
        create_reservation(db, desc, now)
    except ReservationError as exc:
        # slurm://src/slurmrestd/plugins/openapi/slurmctld/reservations.c#_create_resv@25.05+
        if exc.errno == ESLURM_NODES_BUSY and not desc.node_list:
            text = (
                f"Error creating reservation {name}. Note, unless nodes are directly "
                "requested a reservation must exist in a single partition. If no "
                "partition is requested the default partition is assumed."
            )
        else:
            text = f"Error creating reservation  {name}"
        return slurm_error(text, exc.errno, "slurm_create_reservation")
    return None


async def _raw_body(request: Request) -> object:
    body = await request.body()
    if not body.strip():
        return None
    try:
        return json.loads(body)
    except ValueError:
        return _BAD_BODY


_BAD_BODY = object()


def _resv_write_state(state) -> datetime:
    """Bring jobs and env reservations up to date; return the job-clock now."""
    db = state.database
    if advance_job_states(db, state.time_engine):
        state.commit()
    now = job_clock_now(state.time_engine)
    sync_reservations(db, now)
    return now


@router.post("/reservation")
async def post_reservation(request: Request, state: StateDep):
    """Create or update one reservation (25.05+).

    slurm://src/slurmrestd/plugins/openapi/slurmctld/api.c#"create or update a reservation"@25.05+,
    slurm://src/slurmrestd/plugins/openapi/slurmctld/reservations.c#_mod_reservation@25.05+.
    Earlier releases have no such path.
    """
    if not at_least("25.05"):
        raise SlurmrestdRejectError(ESLURM_REST_UNKNOWN_URL)
    raw = await _raw_body(request)
    if raw is None:
        return _respond(
            request,
            state,
            errors=[
                slurm_error(
                    "unexpected empty query for reservation creation",
                    ESLURM_REST_INVALID_QUERY,
                    "_parse_resv_desc",
                )
            ],
        )
    now = _resv_write_state(state)
    try:
        desc = _parse_resv_desc(raw, now)  # a non-JSON body is not a dict either
    except _DescParseError:
        return _respond(
            request,
            state,
            errors=[
                slurm_error(
                    "Rejecting request. Failure parsing parameters",
                    ESLURM_REST_INVALID_QUERY,
                    "_parse_resv_desc",
                )
            ],
        )
    if not desc.name:
        return _respond(
            request,
            state,
            errors=[
                slurm_error(
                    "Reservation must be given.", ESLURM_RESERVATION_INVALID, "_mod_reservation"
                )
            ],
        )
    error = _mod_one(state.database, desc, now)
    if error is not None:
        return _respond(request, state, errors=[error])
    state.commit()
    return _respond(request, state, {"reservations": [_desc_to_dict(desc)]})


@router.post("/reservations/")
async def post_reservations(request: Request, state: StateDep):
    """Create or update several reservations (25.05+).

    slurm://src/slurmrestd/plugins/openapi/slurmctld/reservations.c#_mod_reservations@25.05+:
    every description needs a name before anything is applied; then each is
    created or updated in turn, the successful ones stay, and any error
    drops the echo from the response.
    """
    if not at_least("25.05"):
        raise SlurmrestdRejectError(ESLURM_REST_UNKNOWN_URL_METHOD)
    raw = await _raw_body(request)
    empty = "No reservation descriptions specified in reservations array"
    if raw is None:
        return _respond(
            request,
            state,
            errors=[
                slurm_error(
                    "unexpected empty query for reservation creation",
                    ESLURM_REST_INVALID_QUERY,
                    "_parse_resv_desc_list",
                )
            ],
        )
    now = _resv_write_state(state)
    try:
        items = _parse_resv_list(raw)
        descs = [_parse_resv_desc(item, now) for item in items or []]
    except _DescParseError:
        return _respond(
            request,
            state,
            errors=[
                slurm_error(
                    "Rejecting request. Failure parsing parameters",
                    ESLURM_REST_INVALID_QUERY,
                    "_parse_resv_desc_list",
                )
            ],
        )
    if items is None:
        return _respond(
            request,
            state,
            errors=[slurm_error(empty, ESLURM_REST_INVALID_QUERY, "_parse_resv_desc_list")],
        )
    warnings = [] if descs else [{"description": empty, "source": "_parse_resv_desc_list"}]
    if any(not d.name for d in descs):
        return _respond(
            request,
            state,
            errors=[
                slurm_error(
                    "Reservation must be given", ESLURM_RESERVATION_INVALID, "_check_resv_name"
                )
            ],
        )
    errors = [e for e in (_mod_one(state.database, d, now) for d in descs) if e is not None]
    if len(errors) < len(descs):
        state.commit()
    if errors:
        return _respond(request, state, errors=errors, warnings=warnings)
    return _respond(
        request, state, {"reservations": [_desc_to_dict(d) for d in descs]}, warnings=warnings
    )


@router.delete("/reservation/{reservation_name}")
async def delete_reservation_route(reservation_name: str, request: Request, state: StateDep):
    """Delete a reservation (25.05+).

    slurm://src/slurmrestd/plugins/openapi/slurmctld/reservations.c#_delete_resv@25.05+;
    the response is the bare OPENAPI_RESP envelope.
    """
    if not at_least("25.05"):
        raise SlurmrestdRejectError(ESLURM_REST_UNKNOWN_URL_METHOD)
    _resv_write_state(state)
    try:
        delete_reservation(state.database, reservation_name)
    except ReservationError as exc:
        return _respond(
            request,
            state,
            errors=[
                slurm_error(
                    f"Error deleting reservation {reservation_name}",
                    exc.errno,
                    "slurm_delete_reservation",
                )
            ],
        )
    state.commit()
    return _respond(request, state)


@router.get("/licenses/")
async def get_licenses(
    request: Request,
    state: StateDep,
):
    return _respond(
        request,
        state,
        {"licenses": [], "last_update": uint_no_val(state.now_ts())},
    )
