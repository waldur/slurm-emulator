"""/slurmdb/v0.0.45 endpoints (openapi/slurmdbd plugin emulation).

Endpoint set mirrors slurm://src/slurmrestd/plugins/openapi/slurmdbd/api.c.
GET handlers never write state; POST/DELETE handlers call
``state.commit()`` once at the end, like the sacctmgr emulator.
"""

from __future__ import annotations

import copy
import json
from datetime import datetime
from typing import Any, Optional, Union

from fastapi import APIRouter, Depends, Request, Response

from emulator.api.slurmrestd.auth import slurmrestd_auth
from emulator.api.slurmrestd.envelope import (
    ESLURM_DATA_CONV_FAILED,
    ESLURM_DATA_PATH_NOT_FOUND,
    ESLURM_INVALID_QOS,
    ESLURM_NO_REMOVE_DEFAULT_ACCOUNT,
    ESLURM_NO_REMOVE_DEFAULT_QOS,
    ESLURM_REST_FAIL_PARSING,
    ESLURM_REST_INVALID_QUERY,
    SLURMDBD_PLUGIN,
    SlurmrestdRejectError,
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
    tres_list_from_str,
    tres_str_from_list,
    user_to_dict,
)
from emulator.api.slurmrestd.state import RequestState, StateDep
from emulator.api.slurmrestd.unknown_fields import unknown_field_warnings
from emulator.commands.sacct import SacctEmulator
from emulator.commands.sacct import _Config as SacctConfig
from emulator.commands.slurm_time import ParseTimeError, parse_time_spec
from emulator.core.assoc_view import effective_assoc
from emulator.core.database import QOS, Association, fold_account
from emulator.core.scheduler import advance_job_states
from emulator.core.tres import DYNAMIC_TRES_FIRST_ID
from emulator.slurm_version import at_least

router = APIRouter(
    prefix="/slurmdb/{version}",
    dependencies=[Depends(slurmrestd_auth), Depends(validate_version)],
)


def _respond(request, state, payload=None, errors=None, warnings=None, rc=None):
    return make_response(request, SLURMDBD_PLUGIN, state.cluster, payload, errors, warnings, rc)


async def _json_body(request: Request) -> dict[str, Any]:
    """The request body, ``{}`` when empty.

    Invalid JSON is refused as ``Failure during parsing`` before any handler runs.
    """
    data = await request.body()
    if not data.strip():
        return {}
    try:
        body = json.loads(data)
    except ValueError:
        raise SlurmrestdRejectError(ESLURM_REST_FAIL_PARSING) from None
    return body if isinstance(body, dict) else {}


def _bad_request(request, state, description: str):
    return _respond(
        request,
        state,
        errors=[slurm_error(description, ESLURM_REST_INVALID_QUERY, request.url.path)],
    )


def _not_modified() -> Response:
    """Slurmdbd answered SLURM_NO_CHANGE_IN_DATA: nothing was added, changed or removed.

    slurmrestd sends a bare 304 with no body
    (slurm://src/slurmrestd/operations.c#SLURM_NO_CHANGE_IN_DATA).
    """
    return Response(status_code=304)


def _missing_field(request, state, field: str, source: str):
    """A required request field is absent (``add_parse_req`` in the parser array).

    slurm://src/plugins/data_parser/v0.0.45/parsing.c#"Missing required field"@26.05+
    """
    return _respond(
        request,
        state,
        errors=[
            slurm_error(
                f"Missing required field '{field}' in dictionary",
                ESLURM_DATA_PATH_NOT_FOUND,
                source,
            )
        ],
        rc=ESLURM_REST_INVALID_QUERY,
    )


def _query_list(value: Optional[str]) -> Optional[list[str]]:
    """An ASSOC_CONDITION query parameter: CSV, ``None`` when not given.

    The value arrives URL-decoded, so ``user=a%2Cb`` is the list ``[a, b]``
    (a literal comma never gets here: slurmrestd refuses the query string).
    """
    if value is None:
        return None
    return [item for item in value.split(",") if item]


def _with_assocs(request: Request) -> tuple[bool, list[dict[str, Any]]]:
    """``with_assocs`` of a users query, and the warning slurmrestd gives for it.

    Query values are strings, so any value is reported as not being a boolean
    (BOOL16 parser) and then read as one; the bare flag reads true quietly.
    slurm://src/plugins/data_parser/v0.0.45/parsers.c#USER_CONDITION@26.05+
    """
    raw = request.query_params.get("with_assocs")
    if raw is None:
        return False, []
    if raw == "":
        return True, []
    warning = {
        "description": (
            "Expected OpenAPI type=boolean (Slurm type=boolean) but got OpenAPI "
            f'type=string (Slurm type=string): "{raw}"'
        ),
        "source": "#/with_assocs/",
    }
    return raw.strip().lower() not in ("false", "no", "0", ""), [warning]


def _nothing_found(function: str, caller: str) -> dict[str, Any]:
    """The empty-result warning.

    Its source is the handler function that dumped the result
    (slurm://src/slurmrestd/plugins/openapi/slurmdbd/api.c#db_query_list_funcname).
    """
    return {"description": f"{function}() found nothing", "source": caller}


def _users_nothing_found() -> dict[str, Any]:
    # slurm://src/slurmrestd/plugins/openapi/slurmdbd/users.c#_dump_users
    return _nothing_found("slurmdb_users_get", "_dump_users")


def _account_assocs(state: RequestState, name: str) -> list[Association]:
    name = fold_account(name)
    return [a for a in state.database.associations.values() if a.account == name]


def _csv_list(value: Union[str, list, None]) -> list[str]:
    """CSV_STRING_LIST fields accept either a JSON list or a CSV string."""
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    return []


# ASSOC_REC_SET carries sacctmgr-style flat names with CSV TRES strings
# (slurm://src/plugins/data_parser/v0.0.45/parsers.c#ASSOC_REC_SET@26.05+),
# unlike the nested ``max`` subtree of plain ASSOC objects.
_REC_SET_TRES = {
    "grptres": "GrpTRES",
    "grptresmins": "GrpTRESMins",
    "grptresrunmins": "GrpTRESRunMins",
    "maxtresperjob": "MaxTRES",
    "maxtrespernode": "MaxTRESPerNode",
    "maxtresminsperjob": "MaxTRESMins",
    "maxtresrunmins": "MaxTRESRunMins",
}
_REC_SET_SCALARS = {
    "grpjobs": "GrpJobs",
    "grpjobsaccrue": "GrpJobsAccrue",
    "grpsubmitjobs": "GrpSubmitJobs",
    "grpwall": "GrpWall",
    "maxjobs": "MaxJobs",
    "maxjobsaccrue": "MaxJobsAccrue",
    "maxsubmitjobs": "MaxSubmitJobs",
    "maxwalldurationperjob": "MaxWall",
    "priority": "Priority",
    "minpriothresh": "MinPrioThresh",
}


def _limits_from_rec_set(rec_set: dict[str, Any]) -> dict[str, int]:
    """Map flat ASSOC_REC_SET limit fields onto emulator limit keys."""
    limits: dict[str, int] = {}
    for field, prefix in _REC_SET_TRES.items():
        value = rec_set.get(field)
        if isinstance(value, str) and value:
            for tres_type, count in tres_str_from_list(tres_list_from_str(value)).items():
                limits[f"{prefix}:{tres_type}"] = count
    for field, key in _REC_SET_SCALARS.items():
        value = rec_set.get(field)
        if isinstance(value, dict):
            value = value.get("number") if value.get("set") else None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            limits[key] = int(value)
    return limits


def _rec_set_type_warning(rec_set: dict[str, Any], source: str) -> Optional[dict[str, str]]:
    """``qoslevel`` must be a JSON list (QOS_STRING_ID_LIST); a string is not parsed.

    slurmrestd then reports the type and runs nothing: 200, no payload.
    slurm://src/plugins/data_parser/v0.0.45/parsers.c#ASSOC_REC_SET@26.05+
    """
    value = rec_set.get("qoslevel")
    if value is None or isinstance(value, list):
        return None
    shown = json.dumps(value) if isinstance(value, str) else str(value)
    kind = "string" if isinstance(value, str) else "object"
    return {
        "description": (
            "Expected OpenAPI type=array (Slurm type=list) but got OpenAPI "
            f"type={kind} (Slurm type={kind}): {shown}"
        ),
        "source": f"{source}qoslevel/",
    }


def _ensure_account_association(
    state: RequestState, account: str, cluster: str, parent: Optional[str]
) -> None:
    """Make sure the account-level association row exists on the cluster."""
    key = state.database._association_key("", account, cluster)
    assoc = state.database.associations.get(key)
    if assoc is None:
        state.database.associations[key] = Association(
            account=account, user="", cluster=cluster, parent=parent
        )
    elif parent:
        assoc.parent = parent


def _user_assocs(state: RequestState, name: str) -> list[Association]:
    return [a for a in state.database.associations.values() if a.user == name]


def _assoc_is_default(state: RequestState, assoc: Association) -> bool:
    """Whether this row is the user's default-account association (ASSOC.is_default).

    Account-level rows are never a default (``is_def`` is a user-row flag); a
    row whose user has no record or no default account counts as the default,
    as the only association of such a user is in slurmdbd.
    """
    if not assoc.user:
        return False
    user = state.database.get_user(assoc.user)
    if user is None or not user.default_account:
        return True
    return user.default_account == assoc.account


# --- ping / diag / conf ---


@router.get("/ping/")
async def ping(
    request: Request,
    state: StateDep,
):
    # SLURMDBD_PING never had the deprecated aliases; ``status`` arrives with
    # v0.0.45 (slurm://src/plugins/data_parser/v0.0.45/parsers.c#SLURMDBD_PING@26.05+,
    # slurm://src/plugins/data_parser/v0.0.44/parsers.c#SLURMDBD_PING@25.11+).
    entry: dict[str, Any] = {
        "hostname": "localhost",
        "responding": True,
        "latency": 123,
        "primary": "primary",
    }
    if at_least("26.05"):
        entry["status"] = "No error"
    return _respond(request, state, {"pings": [entry]})


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
        "tres": [tres_entry(t.lower(), 0, db.tres_types) for t in db.tres_types],
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
        _apply_association(state, entry)
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
    tres = [tres_entry(t.lower(), 0, state.database.tres_types) for t in state.database.tres_types]
    # slurmdbd keeps a deleted placeholder row at TRES_STATIC_CNT (1000) so
    # dynamic TRES ids start at 1001
    # (slurm://src/plugins/accounting_storage/mysql/accounting_storage_mysql.c#dynamic_offset).
    # slurmrestd lists deleted TRES (slurm://src/slurmrestd/plugins/openapi/slurmdbd/tres.c#with_deleted),
    # sacctmgr does not unless asked, so only this listing shows it.
    placeholder = {
        "type": "dynamic_offset",
        "name": "",
        "id": DYNAMIC_TRES_FIRST_ID - 1,
        "count": 0,
    }
    tres.sort(key=lambda entry: entry["id"])
    position = next((i for i, e in enumerate(tres) if e["id"] >= DYNAMIC_TRES_FIRST_ID), len(tres))
    tres.insert(position, placeholder)
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
        warnings.append(_nothing_found("slurmdb_clusters_get", "_dump_clusters"))
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
            warnings=[_nothing_found("slurmdb_clusters_get", "_dump_clusters")],
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
    name = fold_account(name)
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
        warnings.append(_nothing_found("slurmdb_accounts_get", "_dump_accounts"))
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
            warnings=[_nothing_found("slurmdb_accounts_get", "_dump_accounts")],
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
        return _not_modified()
    account_name = fold_account(account_name)
    matched = [a for a in state.database.associations.values() if a.account == account_name]
    if _takes_default_away(state, matched):
        # slurm://src/slurmrestd/plugins/openapi/slurmdbd/accounts.c#_delete_account@26.05+
        return _respond(
            request,
            state,
            errors=[
                slurm_error(
                    "slurmdb_accounts_remove failed",
                    ESLURM_NO_REMOVE_DEFAULT_ACCOUNT,
                    "_delete_account",
                )
            ],
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
    """``sacctmgr add account`` equivalent.

    Body OPENAPI_ACCOUNTS_ADD_COND_RESP: ``association_condition``
    (ACCOUNTS_ADD_COND: accounts/clusters CSV lists + one ASSOC_REC_SET)
    and ``account`` (ACCOUNT_SHORT)
    (slurm://src/plugins/data_parser/v0.0.45/parsers.c#ACCOUNTS_ADD_COND@26.05+).
    Accounts that already exist are left alone; when every one does,
    slurmdbd reports no change and slurmrestd answers 304.
    """
    body = await _json_body(request)
    condition = body.get("association_condition")
    if not isinstance(condition, dict):
        return _missing_field(request, state, "association_condition", "#/association_condition/")
    accounts = _csv_list(condition.get("accounts"))
    if not accounts:
        return _missing_field(request, state, "accounts", "#/association_condition/accounts/")
    warnings = unknown_field_warnings(condition, "ACCOUNTS_ADD_COND", "#/association_condition/")
    rec_set = condition.get("association") or {}
    warnings += unknown_field_warnings(
        rec_set, "ASSOC_REC_SET", "#/association_condition/association/"
    )
    account_short = body.get("account") or {}
    warnings += unknown_field_warnings(account_short, "ACCOUNT_SHORT", "#/account/")
    clusters = _csv_list(condition.get("clusters")) or [state.cluster]
    parent = rec_set.get("parent") or None
    limits = _limits_from_rec_set(rec_set)
    added: list[str] = []
    for name in accounts:
        if state.database.get_account(name) is not None:
            continue
        entry: dict[str, Any] = {"name": name}
        if "description" in account_short:
            entry["description"] = account_short["description"]
        if "organization" in account_short:
            entry["organization"] = account_short["organization"]
        if parent:
            entry["parent_account"] = parent
        _upsert_account(state, entry)
        added.append(fold_account(name))
        account_obj = state.database.get_account(name)
        if account_obj is not None and limits:
            account_obj.limits.update(limits)
        if account_obj is not None and rec_set.get("fairshare") is not None:
            account_obj.fairshare = int(rec_set["fairshare"])
        for cluster in clusters:
            _ensure_account_association(state, name, cluster, parent)
    if not added:
        return _not_modified()
    state.commit()
    return _respond(request, state, {"added_accounts": added}, warnings=warnings)


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
        _apply_association(state, {**assoc_entry, "user": name})
    return name


@router.get("/users/")
async def get_users(
    request: Request,
    state: StateDep,
    default_account: Optional[str] = None,
):
    with_assocs, warnings = _with_assocs(request)
    users = list(state.database.users.values())
    if default_account is not None:
        wanted = _query_list(default_account) or []
        # The default account is known only when associations are read.
        users = [u for u in users if with_assocs and u.default_account in wanted]
    payload = [user_to_dict(u, _user_assocs(state, u.name), with_assocs) for u in users]
    if not payload:
        warnings.append(_users_nothing_found())
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
    warnings: list[dict[str, Any]] = []
    for index, entry in enumerate(entries):
        warnings += unknown_field_warnings(entry, "USER", f"#/users[{index}]/")
        if _upsert_user(state, entry) is None:
            return _bad_request(request, state, "User name is required")
    state.commit()
    return _respond(request, state, warnings=warnings)


@router.get("/user/{name}")
async def get_user(
    name: str,
    request: Request,
    state: StateDep,
):
    with_assocs, warnings = _with_assocs(request)
    user = state.database.get_user(name)
    if user is None:
        return _respond(request, state, {"users": []}, warnings=[*warnings, _users_nothing_found()])
    payload = [user_to_dict(user, _user_assocs(state, name), with_assocs)]
    return _respond(request, state, {"users": payload}, warnings=warnings)


@router.delete("/user/{name}")
async def delete_user(
    name: str,
    request: Request,
    state: StateDep,
):
    if state.database.get_user(name) is None:
        return _not_modified()
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
    """``sacctmgr add user`` equivalent.

    Body OPENAPI_USERS_ADD_COND_RESP: ``association_condition``
    (USERS_ADD_COND: users/accounts/clusters/partitions CSV lists + one
    ASSOC_REC_SET) and ``user`` (USER_SHORT: flat ``defaultaccount``,
    ``adminlevel``…); both are required, and so is ``users``
    (slurm://src/plugins/data_parser/v0.0.45/parsers.c#OPENAPI_USERS_ADD_COND_RESP@26.05+).
    One association per user x account x cluster (x partition) is added;
    existing ones and accounts that do not exist are skipped, and when
    nothing is added slurmrestd answers 304.
    """
    body = await _json_body(request)
    condition = body.get("association_condition")
    if not isinstance(condition, dict):
        return _missing_field(request, state, "association_condition", "#/association_condition/")
    users = _csv_list(condition.get("users"))
    if "users" not in condition:
        return _missing_field(request, state, "users", "#/association_condition/users/")
    if "user" not in body:
        return _missing_field(request, state, "user", "#/user/")
    warnings = unknown_field_warnings(condition, "USERS_ADD_COND", "#/association_condition/")
    rec_set = condition.get("association") or {}
    warnings += unknown_field_warnings(
        rec_set, "ASSOC_REC_SET", "#/association_condition/association/"
    )
    user_short = body.get("user") or {}
    warnings += unknown_field_warnings(user_short, "USER_SHORT", "#/user/")
    type_warning = _rec_set_type_warning(rec_set, "#/association_condition/association/")
    if type_warning is not None:
        return _respond(request, state, warnings=[*warnings, type_warning])

    accounts = [a for a in _csv_list(condition.get("accounts")) if state.database.get_account(a)]
    clusters = _csv_list(condition.get("clusters")) or [state.cluster]
    partitions: list[Optional[str]] = list(_csv_list(condition.get("partitions"))) or [None]
    default_account = user_short.get("defaultaccount") or ""
    limits = _limits_from_rec_set(rec_set)
    qos_level = [str(q) for q in rec_set.get("qoslevel") or []]
    default_qos = str(rec_set.get("defaultqos") or "")
    fairshare = rec_set.get("fairshare")
    added: list[str] = []
    for username in users:
        new_rows = 0
        for account in accounts:
            for cluster in clusters:
                for partition in partitions:
                    if state.database.get_association(
                        username, account, cluster=cluster, partition=partition
                    ):
                        continue
                    state.database.add_association(
                        username,
                        account,
                        limits=dict(limits),
                        cluster=cluster,
                        partition=partition,
                        qos_list=list(qos_level) or None,
                        def_qos=default_qos,
                    )
                    row = state.database.get_association(
                        username, account, cluster=cluster, partition=partition
                    )
                    if row is not None and isinstance(fairshare, int):
                        row.fairshare = fairshare
                    new_rows += 1
        if not new_rows:
            continue
        # sacctmgr defaults a new user's DefaultAccount to the first account it
        # is being added to (user_functions.c).
        effective_default = default_account or accounts[0]
        existing = state.database.get_user(username)
        if existing is None:
            state.database.add_user(username, effective_default)
        elif default_account:
            existing.default_account = default_account
        added.append(username)
    if not added:
        return _not_modified()
    state.commit()
    return _respond(request, state, {"added_users": added}, warnings=warnings)


# --- associations ---


# ASSOC field paths of the limits a POST can carry, and the emulator limit key
# each lands in (slurm://src/plugins/data_parser/v0.0.45/parsers.c#ASSOC@26.05+).
_ASSOC_TRES_PATHS = {
    ("max", "tres", "total"): "GrpTRES",
    ("max", "tres", "group", "minutes"): "GrpTRESMins",
    ("max", "tres", "group", "active"): "GrpTRESRunMins",
    ("max", "tres", "minutes", "total"): "MaxTRESRunMins",
    ("max", "tres", "minutes", "per", "job"): "MaxTRESMins",
    ("max", "tres", "per", "job"): "MaxTRES",
    ("max", "tres", "per", "node"): "MaxTRESPerNode",
}
_ASSOC_SCALAR_PATHS = {
    ("max", "jobs", "per", "count"): "GrpJobs",
    ("max", "jobs", "per", "accruing"): "GrpJobsAccrue",
    ("max", "jobs", "per", "submitted"): "GrpSubmitJobs",
    ("max", "jobs", "per", "wall_clock"): "MaxWall",
    ("max", "jobs", "active"): "MaxJobs",
    ("max", "jobs", "accruing"): "MaxJobsAccrue",
    ("max", "jobs", "total"): "MaxSubmitJobs",
    ("max", "per", "account", "wall_clock"): "GrpWall",
    ("min", "priority_threshold"): "MinPrioThresh",
    ("priority",): "Priority",
}
_MISSING = object()


def _dig(entry: dict[str, Any], path: tuple[str, ...]) -> Any:  # noqa: ANN401
    node: Any = entry
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return _MISSING
        node = node[key]
    return node


def _apply_assoc_limits(limits: dict[str, int], entry: dict[str, Any]) -> None:
    """Write the request's limits onto a row's own limits, as slurmrestd's update does.

    slurmrestd turns the request into the association's whole desired state
    and sends slurmdbd the difference: a TRES list the request leaves out is
    diffed against an empty list, so every TRES in it is removed
    (slurm://src/slurmrestd/plugins/openapi/slurmdbd/associations.c#_diff_tres@26.05+).
    Scalars the request leaves out stay NO_VAL, which slurmdbd reads as
    "unchanged" (slurm://src/slurmrestd/plugins/openapi/slurmdbd/associations.c#_diff_assoc@26.05+).
    """
    for path, prefix in _ASSOC_TRES_PATHS.items():
        for key in [k for k in limits if k == prefix or k.startswith(f"{prefix}:")]:
            del limits[key]
        value = _dig(entry, path)
        if isinstance(value, list):
            for tres_type, count in tres_str_from_list(value).items():
                if count >= 0:
                    limits[f"{prefix}:{tres_type}"] = count
    for path, key in _ASSOC_SCALAR_PATHS.items():
        value = _dig(entry, path)
        if value is _MISSING:
            continue
        if isinstance(value, dict):
            if value.get("infinite"):
                limits.pop(key, None)
            elif value.get("set"):
                limits[key] = int(value.get("number", 0))
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            limits[key] = int(value)


def _shares(value: Any) -> Optional[int]:  # noqa: ANN401
    if isinstance(value, dict):
        return int(value["number"]) if value.get("set") and "number" in value else None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)
    return None


def _apply_association(state: RequestState, entry: dict[str, Any]) -> None:
    """Add or update one association from a POST /associations/ entry.

    A row the request names but that does not exist yet is added without a
    user record, as slurmdbd does; one under an account that does not exist
    is skipped. On an existing row the QoS list the request leaves out is
    the row's current, possibly inherited, list -- written back as the row's
    own (``_diff_assoc`` keeps it), so a later change to the account's list
    no longer reaches the row.
    """
    account_name = fold_account(entry.get("account") or "")
    account = state.database.get_account(account_name)
    if account is None:
        return
    user = entry.get("user") or ""
    cluster = entry.get("cluster") or state.cluster
    partition = entry.get("partition") or None
    qos_body = entry.get("qos")
    default_qos = _dig(entry, ("default", "qos"))
    shares = _shares(entry.get("shares_raw"))

    if not user:
        _apply_assoc_limits(account.limits, entry)
        if isinstance(qos_body, list):
            account.qos = ",".join(str(q) for q in qos_body)
        elif not account.qos:
            account.qos = ",".join(
                effective_assoc(
                    state.database.get_association("", account_name, cluster=cluster)
                    or Association(account=account_name, user="", cluster=cluster),
                    account,
                    state.database.accounts,
                )["qos"]
            )
        if isinstance(default_qos, str):
            account.default_qos = default_qos
        if shares is not None:
            account.fairshare = shares
        parent = entry.get("parent_account")
        if parent and fold_account(parent) != account.parent:
            state.database.set_account_parent(account_name, parent, cluster=cluster)
        return

    row = state.database.get_association(user, account_name, cluster=cluster, partition=partition)
    if row is None:
        state.database.add_association(user, account_name, cluster=cluster, partition=partition)
        row = state.database.get_association(
            user, account_name, cluster=cluster, partition=partition
        )
        if row is None:
            return
        if isinstance(qos_body, list):
            row.qos_list = [str(q) for q in qos_body]
    elif isinstance(qos_body, list):
        row.qos_list = [str(q) for q in qos_body]
    else:
        row.qos_list = list(effective_assoc(row, account, state.database.accounts)["qos"])
    _apply_assoc_limits(row.limits, entry)
    if isinstance(default_qos, str):
        row.def_qos = default_qos
    if shares is not None:
        row.fairshare = shares


def _snapshot_account(state: RequestState, account_name: str):
    account = state.database.get_account(account_name)
    rows = {
        key: copy.deepcopy(row)
        for key, row in state.database.associations.items()
        if row.account == account_name
    }
    return copy.deepcopy(account), rows


def _restore_account(state: RequestState, account_name: str, snapshot) -> None:
    account, rows = snapshot
    if account is not None:
        state.database.accounts[account_name] = account
    state.database.associations = {
        key: row for key, row in state.database.associations.items() if row.account != account_name
    }
    state.database.associations.update(rows)


def _removed_assoc_string(assoc: Association) -> str:
    """Removal strings as printed by real slurmdbd.

    Format from slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_process_remove_assoc_results.
    """
    if not assoc.user:
        return f"C = {assoc.cluster:<10} A = {assoc.account:<10}"
    base = f"C = {assoc.cluster:<10} A = {assoc.account:<10} U = {assoc.user:<9}"
    if assoc.partition:
        return f"{base} P = {assoc.partition}"
    return base


def _filter_associations(
    state: RequestState,
    account: Optional[str],
    user: Optional[str],
    cluster: Optional[str],
    partition: Optional[str],
) -> list[Association]:
    """ASSOC_CONDITION filtering; each parameter is a CSV list.

    A ``user`` parameter that is present but empty selects every user row and
    no account row: slurmdbd reads an empty user list as "all users"
    (slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_setup_assoc_cond_limits).
    """
    assocs = list(state.database.associations.values())
    accounts = _query_list(account)
    if accounts is not None:
        folded = {fold_account(a) for a in accounts}
        assocs = [a for a in assocs if a.account in folded]
    users = _query_list(user)
    if users is not None:
        assocs = [a for a in assocs if a.user in users] if users else [a for a in assocs if a.user]
    clusters = _query_list(cluster)
    if clusters:
        assocs = [a for a in assocs if a.cluster in clusters]
    partitions = _query_list(partition)
    if partitions:
        assocs = [a for a in assocs if (a.partition or "") in partitions]
    return assocs


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
    payload = [
        assoc_to_dict(
            a,
            state.database.get_account(a.account),
            _assoc_is_default(state, a),
            state.database.accounts,
        )
        for a in assocs
    ]
    warnings = []
    if not payload:
        warnings.append(_nothing_found("slurmdb_associations_get", "_dump_assoc_cond"))
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
    warnings: list[dict[str, Any]] = []
    for index, entry in enumerate(entries):
        source = f"#/associations[{index}]/"
        if "user" not in entry:
            return _missing_field(request, state, "user", f"{source}user/")
        warnings += unknown_field_warnings(entry, "ASSOC", source)
        # QOS names are resolved while parsing (QOS_STRING_ID_LIST / QOS_ID).
        for name in entry.get("qos") or []:
            if not state.database.qos_known(str(name)):
                return _respond(
                    request,
                    state,
                    errors=[
                        slurm_error(
                            f"Unable to resolve QOS: {name}", ESLURM_INVALID_QOS, f"{source}qos/"
                        )
                    ],
                )
        default_qos = _dig(entry, ("default", "qos"))
        if (
            isinstance(default_qos, str)
            and default_qos
            and not state.database.qos_known(default_qos)
        ):
            return _respond(
                request,
                state,
                errors=[
                    slurm_error(
                        f"Unable to resolve QOS: {default_qos}",
                        ESLURM_INVALID_QOS,
                        f"{source}default/qos/",
                    )
                ],
            )
    for entry in entries:
        account_name = fold_account(entry.get("account") or "")
        snapshot = _snapshot_account(state, account_name)
        _apply_association(state, entry)
        account = state.database.get_account(account_name)
        # slurmdbd checks every association under the account for
        # DefaultQOS-in-list after the change and rolls back on a violation
        # (slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_foreach_check_default_qos);
        # slurmrestd reports only that the modify failed.
        if account is not None and state.database.default_qos_violations(account):
            _restore_account(state, account_name, snapshot)
            return _respond(
                request,
                state,
                errors=[
                    slurm_error(
                        "slurmdb_associations_modify() failed",
                        ESLURM_NO_REMOVE_DEFAULT_QOS,
                        "_foreach_update_assoc",
                    )
                ],
                warnings=warnings,
            )
    state.commit()
    return _respond(request, state, warnings=warnings)


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
        return _not_modified()
    removed = [_removed_assoc_string(a) for a in matched]
    # slurmdbd refuses the whole request when it would take a user's default
    # association away while other associations remain
    # (slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#as_mysql_remove_assocs);
    # slurmrestd reports it twice, from the call and from the handler
    # (slurm://src/slurmrestd/plugins/openapi/slurmdbd/associations.c#_delete_assoc@26.05+).
    if _takes_default_away(state, matched):
        return _respond(
            request,
            state,
            errors=[
                slurm_error(
                    "slurmdb_associations_remove failed",
                    ESLURM_NO_REMOVE_DEFAULT_ACCOUNT,
                    "_delete_assoc",
                ),
                slurm_error(
                    "remove associations failed",
                    ESLURM_NO_REMOVE_DEFAULT_ACCOUNT,
                    "_delete_assoc",
                ),
            ],
        )
    for assoc in matched:
        state.database.delete_association(
            assoc.user, assoc.account, cluster=assoc.cluster, partition=assoc.partition
        )
    # A user whose last association went is gone too (sacctmgr remove user semantics).
    for name in {a.user for a in matched if a.user}:
        if not state.database.user_association_rows(name):
            state.database.users.pop(name, None)
    state.commit()
    return _respond(request, state, {"removed_associations": removed})


def _takes_default_away(state: RequestState, matched: list[Association]) -> bool:
    """Whether removing ``matched`` leaves a user without their default association but with others."""
    surviving = [a for a in state.database.associations.values() if a not in matched]
    for assoc in matched:
        if not assoc.user:
            continue
        user_rec = state.database.get_user(assoc.user)
        if user_rec is None or assoc.account != fold_account(user_rec.default_account):
            continue
        if any(a.user == assoc.user and a.account != assoc.account for a in surviving):
            return True
    return False


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
    # GrpTRESMins of a QOS is limits/max/tres/minutes/total
    # (slurm://src/plugins/data_parser/v0.0.45/parsers.c#QOS@26.05+); the
    # association path max/tres/group/minutes is an unknown field here. A QOS
    # update changes only the TRES it names (no diff as for associations).
    grp_minutes = tres_str_from_list(
        (max_tree.get("tres", {}).get("minutes") or {}).get("total", [])
    )
    if grp_minutes:
        merged = tres_str_from_list(tres_list_from_str(qos.grp_tres_mins or ""))
        merged.update(grp_minutes)
        qos.grp_tres_mins = _tres_dict_to_str(merged)
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
    # slurmrestd reads the QOS_CONDITION of this listing from the path
    # parameters, not the query, so ``?name=`` filters nothing and every QOS
    # comes back (slurm://src/slurmrestd/plugins/openapi/slurmdbd/qos.c#op_handler_multi_qos).
    del name
    return _respond(request, state, {"qos": _qos_payload(state)})


@router.post("/qos/")
async def post_qos(
    request: Request,
    state: StateDep,
):
    body = await _json_body(request)
    entries = body.get("qos", [])
    if not entries:
        return _bad_request(request, state, "No QOS specified")
    warnings: list[dict[str, Any]] = []
    for index, entry in enumerate(entries):
        warnings += unknown_field_warnings(entry, "QOS", f"#/qos[{index}]/")
        if _upsert_qos(state, entry) is None:
            return _bad_request(request, state, "QOS name is required")
    state.commit()
    return _respond(request, state, warnings=warnings)


@router.get("/qos/{qos_name}")
async def get_qos(
    qos_name: str,
    request: Request,
    state: StateDep,
):
    payload = _qos_payload(state, [qos_name])
    warnings = []
    if not payload:
        warnings.append(_nothing_found("slurmdb_qos_get", "_op_handler_qos"))
    return _respond(request, state, {"qos": payload}, warnings=warnings)


@router.delete("/qos/{qos_name}")
async def delete_qos(
    qos_name: str,
    request: Request,
    state: StateDep,
):
    if qos_name not in state.database.qos_list:
        return _not_modified()
    del state.database.qos_list[qos_name]
    state.commit()
    return _respond(request, state, {"removed_qos": [qos_name]})


# --- jobs (accounting view, one job per usage record) ---


def _parse_query_time(value: str, now: datetime) -> datetime:
    if value.strip().lstrip("-").isdigit():
        # Naive local time, consistent with the emulator's naive clock.
        return datetime.fromtimestamp(int(value))  # noqa: DTZ006
    return parse_time_spec(value, now)


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
    if advance_job_states(state.database, state.time_engine):
        state.commit()
    sacct = SacctEmulator(state.database, state.time_engine)
    cfg = SacctConfig()
    if account:
        cfg.accounts = account.split(",")
    if users:
        cfg.users = users.split(",")
    if cluster:
        cfg.clusters = cluster.split(",")
    # JOB_CONDITION parses start_time/end_time as TIMESTAMP: parse_time(str, 0)
    # (slurm://src/plugins/data_parser/v0.0.45/parsers.c#_parse_timestamp@26.05+).
    # A failure is the parser's ESLURM_DATA_CONV_FAILED, then the handler's
    # rejection, whose 9000 the handler returns and so sets the status
    # (slurm://src/slurmrestd/plugins/openapi/slurmdbd/jobs.c#op_handler_jobs).
    for field, value in (("start_time", start_time), ("end_time", end_time)):
        if not value:
            continue
        try:
            parsed = _parse_query_time(value, state.time_engine.get_current_time())
        except ParseTimeError:
            return _respond(
                request,
                state,
                errors=[
                    slurm_error(
                        f"Parsing of {value} for timestamp failed",
                        ESLURM_DATA_CONV_FAILED,
                        f"#/{field}",
                    ),
                    slurm_error(
                        "Rejecting request. Failure parsing query parameters",
                        ESLURM_REST_INVALID_QUERY,
                        "op_handler_jobs",
                    ),
                ],
                rc=ESLURM_REST_INVALID_QUERY,
            )
        if field == "start_time":
            cfg.start_time = parsed
        else:
            cfg.end_time = parsed

    records = sacct._get_filtered_records(cfg)
    payload = [dbd_job_to_dict(r, sacct._active_jobs.get(str(r.job_id))) for r in records]
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
    if advance_job_states(state.database, state.time_engine):
        state.commit()
    state.database.ensure_job_ids()
    matched = [r for r in state.database.usage_records if str(r.job_id) == job_id]
    payload = [dbd_job_to_dict(r) for r in matched]
    if not payload:
        # slurmdbd holds the job from submission on.
        sacct = SacctEmulator(state.database, state.time_engine)
        cfg = SacctConfig()
        cfg.jobs = [job_id]
        cfg.clusters = [state.database.current_cluster]
        payload = [
            dbd_job_to_dict(r, sacct._active_jobs.get(str(r.job_id)))
            for r in sacct._active_job_records(cfg, set())
        ]
    warnings = []
    if not payload:
        warnings.append(found_nothing_warning("slurmdb_jobs_get()", request))
    return _respond(request, state, {"jobs": payload}, warnings=warnings)
