"""/slurmdb/v0.0.46 endpoints (openapi/slurmdbd plugin emulation).

Endpoint set mirrors src/slurmrestd/plugins/openapi/slurmdbd/api.c.
GET handlers never write state; POST/DELETE handlers call
``state.commit()`` once at the end, like the sacctmgr emulator.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, Request

from emulator.api.slurmrestd.auth import slurmrestd_auth
from emulator.api.slurmrestd.envelope import (
    ESLURM_REST_INVALID_QUERY,
    SLURMDBD_PLUGIN,
    found_nothing_warning,
    make_response,
    slurm_error,
    validate_version,
)
from emulator.api.slurmrestd.schemas import (
    account_to_dict,
    assoc_to_dict,
    dbd_job_to_dict,
    qos_to_dict,
    tres_entry,
    tres_str_from_list,
    user_to_dict,
)
from emulator.api.slurmrestd.state import RequestState, StateDep
from emulator.commands.sacct import SacctEmulator
from emulator.commands.sacct import _Config as SacctConfig
from emulator.core.database import QOS, Association

router = APIRouter(
    prefix="/slurmdb/{version}",
    dependencies=[Depends(slurmrestd_auth), Depends(validate_version)],
)


def _respond(request, state, payload=None, errors=None, warnings=None):
    return make_response(request, SLURMDBD_PLUGIN, state.cluster, payload, errors, warnings)


async def _json_body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception:
        return {}
    return body if isinstance(body, dict) else {}


def _bad_request(request, state, description: str):
    return _respond(
        request,
        state,
        errors=[slurm_error(description, ESLURM_REST_INVALID_QUERY, request.url.path)],
    )


def _account_assocs(state: RequestState, name: str) -> list[Association]:
    return [a for a in state.database.associations.values() if a.account == name]


def _user_assocs(state: RequestState, name: str) -> list[Association]:
    return [a for a in state.database.associations.values() if a.user == name]


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
    statistics = {
        "time_start": state.now_ts(),
        "rollups": [],
        "RPCs": [],
        "users": [],
    }
    return _respond(request, state, {"statistics": statistics})


def _config_dump(state: RequestState) -> dict[str, Any]:
    db = state.database
    return {
        "clusters": [_cluster_to_dict(c) for c in db.list_clusters()],
        "tres": [tres_entry(t.lower(), 0) for t in db.tres_types],
        "accounts": [
            account_to_dict(a, _account_assocs(state, a.name)) for a in db.list_accounts()
        ],
        "users": [user_to_dict(u, _user_assocs(state, u.name)) for u in db.users.values()],
        "qos": [qos_to_dict(q, i + 1) for i, q in enumerate(db.qos_list.values())],
        "associations": [
            assoc_to_dict(a, db.get_account(a.account)) for a in db.associations.values()
        ],
    }


@router.get("/config")
@router.get("/conf")
async def get_config(
    request: Request,
    state: StateDep,
):
    return _respond(request, state, _config_dump(state))


@router.post("/config")
async def post_config(
    request: Request,
    state: StateDep,
):
    body = await _json_body(request)
    for entry in body.get("accounts", []):
        _upsert_account(state, entry)
    for entry in body.get("users", []):
        _upsert_user(state, entry)
    for entry in body.get("associations", []):
        _upsert_association(state, entry)
    for entry in body.get("qos", []):
        _upsert_qos(state, entry)
    state.commit()
    return _respond(request, state)


# --- TRES ---


@router.get("/tres/")
async def get_tres(
    request: Request,
    state: StateDep,
):
    tres = [tres_entry(t.lower(), 0) for t in state.database.tres_types]
    return _respond(request, state, {"TRES": tres})


@router.post("/tres/")
async def post_tres(
    request: Request,
    state: StateDep,
):
    # Real slurmdbd only lets the daemon itself define TRES; accept and
    # ignore, like the live API does for already-known types.
    await _json_body(request)
    return _respond(request, state)


# --- clusters ---


def _cluster_to_dict(cluster) -> dict[str, Any]:
    return {
        "name": cluster.name,
        "controller": {"host": cluster.control_host, "port": cluster.control_port},
        "rpc_version": cluster.rpc_version,
        "flags": [],
        "nodes": cluster.nodes,
        "tres": [],
        "associations": {"root": {"account": "root", "cluster": cluster.name, "user": "", "id": 0}},
    }


@router.get("/clusters/")
async def get_clusters(
    request: Request,
    state: StateDep,
):
    clusters = [_cluster_to_dict(c) for c in state.database.list_clusters()]
    warnings = []
    if not clusters:
        warnings.append(found_nothing_warning("slurmdb_clusters_get()", request))
    return _respond(request, state, {"clusters": clusters}, warnings=warnings)


@router.post("/clusters/")
async def post_clusters(
    request: Request,
    state: StateDep,
):
    body = await _json_body(request)
    for entry in body.get("clusters", []):
        name = entry.get("name")
        if not name:
            return _bad_request(request, state, "No cluster name specified")
        if not state.database.get_cluster(name):
            controller = entry.get("controller", {})
            state.database.add_cluster(
                name,
                control_host=controller.get("host", "localhost"),
                control_port=controller.get("port", 6817),
            )
    state.commit()
    return _respond(request, state)


@router.get("/cluster/{cluster_name}")
async def get_cluster(
    cluster_name: str,
    request: Request,
    state: StateDep,
):
    cluster = state.database.get_cluster(cluster_name)
    if cluster is None:
        return _respond(
            request,
            state,
            {"clusters": []},
            warnings=[found_nothing_warning("slurmdb_clusters_get()", request)],
        )
    return _respond(request, state, {"clusters": [_cluster_to_dict(cluster)]})


@router.delete("/cluster/{cluster_name}")
async def delete_cluster(
    cluster_name: str,
    request: Request,
    state: StateDep,
):
    try:
        state.database.delete_cluster(cluster_name)
    except ValueError as e:
        return _bad_request(request, state, str(e))
    state.commit()
    return _respond(request, state, {"deleted_clusters": [cluster_name]})


# --- accounts ---


def _upsert_account(state: RequestState, entry: dict[str, Any]) -> Optional[str]:
    """Create or update one account from a request-body dict.

    Returns the account name, or None when the entry is invalid.
    """
    name = entry.get("name")
    if not name:
        return None
    existing = state.database.get_account(name)
    if existing is None:
        state.database.add_account(
            name,
            entry.get("description", name),
            entry.get("organization", name),
            parent=entry.get("parent_account") or None,
        )
    else:
        if "description" in entry:
            existing.description = entry["description"]
        if "organization" in entry:
            existing.organization = entry["organization"]
        if "parent_account" in entry:
            state.database.set_account_parent(name, entry["parent_account"] or None)
    return name


@router.get("/accounts/")
async def get_accounts(
    request: Request,
    state: StateDep,
    description: Optional[str] = None,
):
    accounts = state.database.list_accounts()
    if description is not None:
        accounts = [a for a in accounts if a.description == description]
    payload = [account_to_dict(a, _account_assocs(state, a.name)) for a in accounts]
    warnings = []
    if not payload:
        warnings.append(found_nothing_warning("slurmdb_accounts_get()", request))
    return _respond(request, state, {"accounts": payload}, warnings=warnings)


@router.post("/accounts/")
async def post_accounts(
    request: Request,
    state: StateDep,
):
    body = await _json_body(request)
    entries = body.get("accounts", [])
    if not entries:
        return _bad_request(request, state, "No accounts specified")
    for entry in entries:
        if _upsert_account(state, entry) is None:
            return _bad_request(request, state, "Account name is required")
    state.commit()
    return _respond(request, state)


@router.get("/account/{account_name}")
async def get_account(
    account_name: str,
    request: Request,
    state: StateDep,
):
    account = state.database.get_account(account_name)
    if account is None:
        return _respond(
            request,
            state,
            {"accounts": []},
            warnings=[found_nothing_warning("slurmdb_accounts_get()", request)],
        )
    payload = [account_to_dict(account, _account_assocs(state, account_name))]
    return _respond(request, state, {"accounts": payload})


@router.delete("/account/{account_name}")
async def delete_account(
    account_name: str,
    request: Request,
    state: StateDep,
):
    if state.database.get_account(account_name) is None:
        return _respond(
            request,
            state,
            {"removed_accounts": []},
            warnings=[found_nothing_warning("slurmdb_accounts_get()", request)],
        )
    state.database.delete_account(account_name)
    # Cascade: drop every association referencing the account, matching
    # sacctmgr remove account semantics.
    state.database.associations = {
        k: a for k, a in state.database.associations.items() if a.account != account_name
    }
    state.commit()
    return _respond(request, state, {"removed_accounts": [account_name]})


@router.post("/accounts_association/")
async def post_accounts_association(
    request: Request,
    state: StateDep,
):
    body = await _json_body(request)
    added: list[str] = []
    for entry in body.get("accounts", []):
        name = _upsert_account(state, entry)
        if name is None:
            return _bad_request(request, state, "Account name is required")
        added.append(name)
        for assoc_entry in entry.get("associations", []):
            _upsert_association(state, {**assoc_entry, "account": name})
    state.commit()
    return _respond(request, state, {"added_accounts": added})


# --- users ---


def _upsert_user(state: RequestState, entry: dict[str, Any]) -> Optional[str]:
    name = entry.get("name")
    if not name:
        return None
    default_account = entry.get("default", {}).get("account", "")
    existing = state.database.get_user(name)
    if existing is None:
        state.database.add_user(name, default_account)
    elif default_account:
        existing.default_account = default_account
    for assoc_entry in entry.get("associations", []):
        _upsert_association(state, {**assoc_entry, "user": name})
    return name


@router.get("/users/")
async def get_users(
    request: Request,
    state: StateDep,
    default_account: Optional[str] = None,
):
    users = list(state.database.users.values())
    if default_account is not None:
        users = [u for u in users if u.default_account == default_account]
    payload = [user_to_dict(u, _user_assocs(state, u.name)) for u in users]
    warnings = []
    if not payload:
        warnings.append(found_nothing_warning("slurmdb_users_get()", request))
    return _respond(request, state, {"users": payload}, warnings=warnings)


@router.post("/users/")
async def post_users(
    request: Request,
    state: StateDep,
):
    body = await _json_body(request)
    entries = body.get("users", [])
    if not entries:
        return _bad_request(request, state, "No users specified")
    for entry in entries:
        if _upsert_user(state, entry) is None:
            return _bad_request(request, state, "User name is required")
    state.commit()
    return _respond(request, state)


@router.get("/user/{name}")
async def get_user(
    name: str,
    request: Request,
    state: StateDep,
):
    user = state.database.get_user(name)
    if user is None:
        return _respond(
            request,
            state,
            {"users": []},
            warnings=[found_nothing_warning("slurmdb_users_get()", request)],
        )
    return _respond(request, state, {"users": [user_to_dict(user, _user_assocs(state, name))]})


@router.delete("/user/{name}")
async def delete_user(
    name: str,
    request: Request,
    state: StateDep,
):
    if state.database.get_user(name) is None:
        return _respond(
            request,
            state,
            {"removed_users": []},
            warnings=[found_nothing_warning("slurmdb_users_get()", request)],
        )
    del state.database.users[name]
    state.database.associations = {
        k: a for k, a in state.database.associations.items() if a.user != name
    }
    state.commit()
    return _respond(request, state, {"removed_users": [name]})


@router.post("/users_association/")
async def post_users_association(
    request: Request,
    state: StateDep,
):
    body = await _json_body(request)
    added: list[str] = []
    for entry in body.get("users", []):
        name = _upsert_user(state, entry)
        if name is None:
            return _bad_request(request, state, "User name is required")
        added.append(name)
    state.commit()
    return _respond(request, state, {"added_users": added})


# --- associations ---


def _limits_from_assoc_body(entry: dict[str, Any]) -> dict[str, int]:
    """Map the v0.0.46 ``max`` subtree onto emulator limit keys."""
    limits: dict[str, int] = {}
    max_tree = entry.get("max", {})
    tres_tree = max_tree.get("tres", {})
    for tres_type, value in tres_str_from_list(
        tres_tree.get("group", {}).get("minutes", [])
    ).items():
        limits[f"GrpTRESMins:{tres_type}"] = value
    for tres_type, value in tres_str_from_list(tres_tree.get("total", [])).items():
        limits[f"GrpTRES:{tres_type}"] = value
    for tres_type, value in tres_str_from_list(
        tres_tree.get("minutes", {}).get("per", {}).get("job", [])
    ).items():
        limits[f"MaxTRESMins:{tres_type}"] = value
    return limits


def _upsert_association(state: RequestState, entry: dict[str, Any]) -> bool:
    account = entry.get("account")
    if not account:
        return False
    user = entry.get("user", "")
    cluster = entry.get("cluster") or state.cluster
    partition = entry.get("partition") or None
    limits = _limits_from_assoc_body(entry)

    if state.database.get_account(account) is None:
        state.database.add_account(account, account, account)
    if user and state.database.get_user(user) is None:
        state.database.add_user(user, account)

    if user:
        existing = state.database.get_association(
            user, account, cluster=cluster, partition=partition
        )
        if existing is not None:
            # Re-POST updates the row; absent limits stay untouched.
            existing.limits.update(limits)
        else:
            state.database.add_association(
                user, account, limits=limits, cluster=cluster, partition=partition
            )
    else:
        # Account-level association: created by add_account; apply
        # parent/limits updates on the existing row.
        if entry.get("parent_account"):
            state.database.set_account_parent(account, entry["parent_account"], cluster=cluster)
        if limits:
            account_obj = state.database.get_account(account)
            if account_obj is not None:
                account_obj.limits.update(limits)
    return True


def _filter_associations(
    state: RequestState,
    account: Optional[str],
    user: Optional[str],
    cluster: Optional[str],
    partition: Optional[str],
) -> list[Association]:
    assocs = list(state.database.associations.values())
    if account is not None:
        assocs = [a for a in assocs if a.account == account]
    if user is not None:
        assocs = [a for a in assocs if a.user == user]
    if cluster is not None:
        assocs = [a for a in assocs if a.cluster == cluster]
    if partition is not None:
        assocs = [a for a in assocs if (a.partition or "") == partition]
    return assocs


def _removed_assoc_string(assoc: Association) -> str:
    """Removal strings as printed by real slurmdbd.

    Format from as_mysql_assoc.c:1404-1409.
    """
    if not assoc.user:
        return f"C = {assoc.cluster:<10} A = {assoc.account:<20}"
    base = f"C = {assoc.cluster:<10} A = {assoc.account:<20} U = {assoc.user:<9}"
    if assoc.partition:
        return f"{base} P = {assoc.partition}"
    return base


@router.get("/associations/")
@router.get("/association/")
async def get_associations(
    request: Request,
    state: StateDep,
    account: Optional[str] = None,
    user: Optional[str] = None,
    cluster: Optional[str] = None,
    partition: Optional[str] = None,
):
    assocs = _filter_associations(state, account, user, cluster, partition)
    payload = [assoc_to_dict(a, state.database.get_account(a.account)) for a in assocs]
    warnings = []
    if not payload:
        warnings.append(found_nothing_warning("slurmdb_associations_get()", request))
    return _respond(request, state, {"associations": payload}, warnings=warnings)


@router.post("/associations/")
@router.post("/association/")
async def post_associations(
    request: Request,
    state: StateDep,
):
    body = await _json_body(request)
    entries = body.get("associations", [])
    if not entries:
        return _bad_request(request, state, "No associations specified")
    for entry in entries:
        if not _upsert_association(state, entry):
            return _bad_request(request, state, "Association account is required")
    state.commit()
    return _respond(request, state)


@router.delete("/associations/")
@router.delete("/association/")
async def delete_associations(
    request: Request,
    state: StateDep,
    account: Optional[str] = None,
    user: Optional[str] = None,
    cluster: Optional[str] = None,
    partition: Optional[str] = None,
):
    if account is None and user is None:
        return _bad_request(request, state, "No association condition specified")
    matched = _filter_associations(state, account, user, cluster, partition)
    if not matched:
        return _respond(
            request,
            state,
            {"removed_associations": []},
            warnings=[found_nothing_warning("slurmdb_associations_get()", request)],
        )
    removed = [_removed_assoc_string(a) for a in matched]
    for assoc in matched:
        state.database.delete_association(
            assoc.user, assoc.account, cluster=assoc.cluster, partition=assoc.partition
        )
    state.commit()
    return _respond(request, state, {"removed_associations": removed})


# --- QOS ---


def _tres_dict_to_str(values: dict[str, int]) -> str:
    return ",".join(f"{k}={v}" for k, v in values.items())


def _upsert_qos(state: RequestState, entry: dict[str, Any]) -> Optional[str]:
    name = entry.get("name")
    if not name:
        return None
    limits = entry.get("limits", {})
    max_tree = limits.get("max", {})
    jobs_tree = max_tree.get("jobs", {})

    def no_val_number(node: object, default: int = -1) -> int:
        if isinstance(node, dict):
            return int(node["number"]) if node.get("set") else default  # type: ignore[index]
        if isinstance(node, (int, float)):
            return int(node)
        return default

    max_wall = no_val_number(max_tree.get("wall_clock", {}).get("per", {}).get("job"))
    existing = state.database.qos_list.get(name)
    qos = existing or QOS(name=name)
    flags = entry.get("flags")
    if flags is not None:
        qos.flags = ",".join(flags) if isinstance(flags, list) else str(flags)
    grp_tres = tres_str_from_list(max_tree.get("tres", {}).get("total", []))
    if grp_tres:
        qos.grp_tres = _tres_dict_to_str(grp_tres)
    qos.max_jobs = no_val_number(
        jobs_tree.get("active_jobs", {}).get("per", {}).get("user"), qos.max_jobs
    )
    qos.max_submit = no_val_number(jobs_tree.get("per", {}).get("user"), qos.max_submit)
    if max_wall >= 0:
        qos.max_wall = str(max_wall)
    min_tres = tres_str_from_list(
        limits.get("min", {}).get("tres", {}).get("per", {}).get("job", [])
    )
    if min_tres:
        qos.min_tres_per_job = _tres_dict_to_str(min_tres)
    state.database.qos_list[name] = qos
    return name


def _qos_payload(state: RequestState, names: Optional[list[str]] = None) -> list[dict[str, Any]]:
    out = []
    for index, (name, qos) in enumerate(state.database.qos_list.items()):
        if names is not None and name not in names:
            continue
        out.append(qos_to_dict(qos, index + 1))
    return out


@router.get("/qos/")
async def get_qos_list(
    request: Request,
    state: StateDep,
    name: Optional[str] = None,
):
    names = name.split(",") if name else None
    payload = _qos_payload(state, names)
    warnings = []
    if not payload:
        warnings.append(found_nothing_warning("slurmdb_qos_get()", request))
    return _respond(request, state, {"qos": payload}, warnings=warnings)


@router.post("/qos/")
async def post_qos(
    request: Request,
    state: StateDep,
):
    body = await _json_body(request)
    entries = body.get("qos", [])
    if not entries:
        return _bad_request(request, state, "No QOS specified")
    for entry in entries:
        if _upsert_qos(state, entry) is None:
            return _bad_request(request, state, "QOS name is required")
    state.commit()
    return _respond(request, state)


@router.get("/qos/{qos_name}")
async def get_qos(
    qos_name: str,
    request: Request,
    state: StateDep,
):
    payload = _qos_payload(state, [qos_name])
    warnings = []
    if not payload:
        warnings.append(found_nothing_warning("slurmdb_qos_get()", request))
    return _respond(request, state, {"qos": payload}, warnings=warnings)


@router.delete("/qos/{qos_name}")
async def delete_qos(
    qos_name: str,
    request: Request,
    state: StateDep,
):
    if qos_name not in state.database.qos_list:
        return _respond(
            request,
            state,
            {"removed_qos": []},
            warnings=[found_nothing_warning("slurmdb_qos_get()", request)],
        )
    del state.database.qos_list[qos_name]
    state.commit()
    return _respond(request, state, {"removed_qos": [qos_name]})


# --- jobs (accounting view, one job per usage record) ---


def _parse_query_time(value: str, sacct: SacctEmulator):
    if value.isdigit():
        # Naive local time, consistent with the emulator's naive clock.
        return datetime.fromtimestamp(int(value))  # noqa: DTZ006
    return sacct._parse_time_inner(value)


@router.get("/jobs/")
async def get_jobs(
    request: Request,
    state: StateDep,
    account: Optional[str] = None,
    users: Optional[str] = None,
    cluster: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
):
    sacct = SacctEmulator(state.database, state.time_engine)
    cfg = SacctConfig()
    if account:
        cfg.accounts = account.split(",")
    if users:
        cfg.users = users.split(",")
    if cluster:
        cfg.clusters = cluster.split(",")
    try:
        if start_time:
            cfg.start_time = _parse_query_time(start_time, sacct)
        if end_time:
            cfg.end_time = _parse_query_time(end_time, sacct)
    except (ValueError, IndexError):
        return _bad_request(request, state, "Invalid time specification")

    records = sacct._get_filtered_records(cfg)
    payload = [dbd_job_to_dict(r) for r in records]
    warnings = []
    if not payload:
        warnings.append(found_nothing_warning("slurmdb_jobs_get()", request))
    return _respond(request, state, {"jobs": payload}, warnings=warnings)


@router.get("/job/{job_id}")
async def get_job(
    job_id: str,
    request: Request,
    state: StateDep,
):
    state.database.ensure_job_ids()
    matched = [r for r in state.database.usage_records if str(r.job_id) == job_id]
    payload = [dbd_job_to_dict(r) for r in matched]
    warnings = []
    if not payload:
        warnings.append(found_nothing_warning("slurmdb_jobs_get()", request))
    return _respond(request, state, {"jobs": payload}, warnings=warnings)
