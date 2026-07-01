"""/slurm/v0.0.46 endpoints (openapi/slurmctld plugin emulation).

Read paths plus DELETE /job/{job_id} (the scancel equivalent).
Nodes/partitions are served from the static topology in schemas.py —
the same picture sinfo paints. Reservations and licenses are
empty-list stubs. Job submission endpoints are intentionally not
registered, so they 404 with the real plain-text rejection.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, Request

from emulator.api.slurmrestd.auth import slurmrestd_auth
from emulator.api.slurmrestd.envelope import (
    ESLURM_INVALID_JOB_ID,
    SLURMCTLD_PLUGIN,
    found_nothing_warning,
    make_response,
    slurm_error,
    validate_version,
)
from emulator.api.slurmrestd.schemas import (
    PARTITION_RANGES,
    all_node_names,
    ctld_job_to_dict,
    node_to_dict,
    partition_to_dict,
    uint_no_val,
)
from emulator.api.slurmrestd.state import StateDep
from emulator.core.database import Job, SlurmDatabase
from emulator.core.scheduler import advance_job_states, job_clock_now

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
    if value is None:
        return default
    if isinstance(value, dict):
        return int(value.get("number", default)) if value.get("set") else default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _env_to_dict(value: object) -> dict[str, str]:
    """FireCREST sends ``environment`` as a ``KEY=VALUE`` list (>=0.0.39); accept dicts too."""
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


@router.get("/ping/")
async def ping(
    request: Request,
    state: StateDep,
):
    pings = [
        {
            "hostname": "localhost",
            "responding": True,
            # FireCREST's scheduler health check reads ping["pinged"] == "UP";
            # real slurm exposes the same state under "responding".
            "pinged": "UP",
            "latency": 123,
            "primary": "primary",
            "status": "No error",
        }
    ]
    return _respond(request, state, {"pings": pings})


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
        "slurm_version": "26.11.0",
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

    FireCREST posts ``{"job": {...}}`` with the batch ``script`` inside the
    job body for data_parser >= 0.0.41 (v0.0.46 qualifies); it also tolerates
    the pre-0.0.41 sibling ``script``. We honour both. The response mirrors
    OPENAPI_JOB_SUBMIT_RESPONSE (parsers.c:12959): top-level ``job_id`` /
    ``step_id`` / ``job_submit_user_msg`` — FireCREST reads only ``job_id``.
    """
    body = await _json_body(request)
    job_desc = body.get("job")
    if isinstance(job_desc, list):
        job_desc = job_desc[0] if job_desc else {}
    if not isinstance(job_desc, dict):
        job_desc = {}
    script = body.get("script") or job_desc.get("script") or ""

    db = state.database
    user = job_desc.get("user_name") or getattr(request.state, "slurm_user", "root")
    user_rec = db.get_user(user)
    # Real jobs always carry an account (the user's default association).
    # Fall back to the user's default, then "root", so the account is never
    # empty — FireCREST's UI builds job-detail URLs from it.
    account = job_desc.get("account") or (user_rec.default_account if user_rec else "") or "root"

    jid = db.allocate_job_id()
    job = Job(
        job_id=str(jid),
        account=account,
        user=user,
        state="PENDING",
        submit_time=job_clock_now(state.time_engine),
        cluster=db.current_cluster,
        name=job_desc.get("name") or f"job_{jid}",
        partition=job_desc.get("partition") or "compute",
        qos=job_desc.get("qos") or "normal",
        working_directory=job_desc.get("current_working_directory") or f"/home/{user}",
        script=script,
        standard_output=job_desc.get("standard_output") or "",
        standard_error=job_desc.get("standard_error") or "",
        standard_input=job_desc.get("standard_input") or "/dev/null",
        node_count=_submit_int(job_desc.get("nodes"), 1) or 1,
        priority=_submit_int(job_desc.get("priority"), 1) or 1,
        time_limit=_submit_int(job_desc.get("time_limit"), None),
        environment=_env_to_dict(job_desc.get("environment")),
        constraints=job_desc.get("constraints") or "",
    )
    db.add_job(job)
    state.commit()

    return _respond(
        request,
        state,
        {"job_id": jid, "step_id": "BATCH", "job_submit_user_msg": ""},
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
        job.end_time = state.time_engine.get_current_time()
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


@router.get("/nodes/")
async def get_nodes(
    request: Request,
    state: StateDep,
):
    now = state.now_ts()
    payload = [node_to_dict(name, now) for name in all_node_names()]
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
        {"nodes": [node_to_dict(node_name, now)], "last_update": uint_no_val(now)},
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


@router.get("/reservations/")
async def get_reservations(
    request: Request,
    state: StateDep,
):
    return _respond(
        request,
        state,
        {"reservations": [], "last_update": uint_no_val(state.now_ts())},
    )


@router.get("/reservation/{reservation_name}")
async def get_reservation(
    reservation_name: str,  # noqa: ARG001 - any name yields the empty stub
    request: Request,
    state: StateDep,
):
    return _respond(
        request,
        state,
        {"reservations": [], "last_update": uint_no_val(state.now_ts())},
        warnings=[found_nothing_warning("slurm_load_reservations()", request)],
    )


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
