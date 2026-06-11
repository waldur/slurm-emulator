"""Response envelope and error helpers mirroring slurmrestd v0.0.46.

Every JSON response carries the openapi_resp envelope — payload key
first, then ``meta``/``errors``/``warnings`` (parsers.c:12898-12904).
Auth failures and unknown URLs are rejected with *plain-text* bodies
and ``Connection: Close``, exactly like ``_operations_router_reject``
(src/slurmrestd/operations.c:222).
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import Request
from fastapi.responses import JSONResponse, PlainTextResponse

SLURM_RELEASE = "26.11.0"
SLURM_VERSION = {"major": "26", "micro": "0", "minor": "11"}
API_VERSION = "v0.0.46"
DATA_PARSER = f"data_parser/{API_VERSION}"

SLURMDBD_PLUGIN = ("openapi/slurmdbd", "Slurm OpenAPI slurmdbd")
SLURMCTLD_PLUGIN = ("openapi/slurmctld", "Slurm OpenAPI slurmctld")

# Error numbers from slurm/slurm_errno.h (REST block at :412-421,
# slurmctld block at :99-116).
ESLURM_INVALID_JOB_ID = 2017
ESLURM_REST_INVALID_QUERY = 9000
ESLURM_REST_FAIL_PARSING = 9001
ESLURM_REST_EMPTY_RESULT = 9003
ESLURM_REST_UNKNOWN_URL = 9006
ESLURM_REST_UNKNOWN_URL_METHOD = 9007
ESLURM_REST_AUTH_FAIL = 9008
ESLURM_REST_BAD_REQUEST = 9009

# slurm_strerror() texts (src/common/slurm_errno.c:1380-1397).
_STRERROR = {
    ESLURM_INVALID_JOB_ID: "Invalid job id specified",
    ESLURM_REST_INVALID_QUERY: "Query empty or incorrect type",
    ESLURM_REST_FAIL_PARSING: "Unable to parse request",
    ESLURM_REST_EMPTY_RESULT: "Nothing found with query",
    ESLURM_REST_UNKNOWN_URL: (
        "Unable to find requested URL endpoint. Please query the "
        "'/openapi/v3' endpoint or visit "
        "'https://slurm.schedmd.com/rest_api.html' for the OpenAPI "
        "specification which includes a list of all possible slurmrestd "
        "endpoints."
    ),
    ESLURM_REST_UNKNOWN_URL_METHOD: "Unknown HTTP method requested on known URL endpoint",
    ESLURM_REST_AUTH_FAIL: "Authentication failure",
    ESLURM_REST_BAD_REQUEST: "Request failed to be processed",
}

# http_status_from_error() subset (src/slurmrestd/operations.c).
_HTTP_STATUS = {
    ESLURM_INVALID_JOB_ID: 404,
    ESLURM_REST_INVALID_QUERY: 400,
    ESLURM_REST_FAIL_PARSING: 400,
    ESLURM_REST_BAD_REQUEST: 400,
    ESLURM_REST_UNKNOWN_URL: 404,
    ESLURM_REST_UNKNOWN_URL_METHOD: 405,
    ESLURM_REST_AUTH_FAIL: 401,
}


class SlurmrestdRejectError(Exception):
    """Routing/auth-level rejection: plain-text body, no envelope."""

    def __init__(self, error_number: int):
        super().__init__(strerror(error_number))
        self.error_number = error_number


def strerror(error_number: int) -> str:
    return _STRERROR.get(error_number, f"Unknown error {error_number}")


def http_status_for(error_number: int) -> int:
    if error_number in _HTTP_STATUS:
        return _HTTP_STATUS[error_number]
    if 2000 <= error_number <= 7999:
        # slurmctld/slurmdbd validation errors → Unprocessable Content.
        return 422
    return 500


def validate_version(version: str) -> None:
    """Reject any URL version this build does not serve.

    Real slurmrestd never registers paths for unloaded data_parser
    plugins, so e.g. ``/slurmdb/v0.0.45/...`` is an unknown URL.
    """
    if version != API_VERSION:
        raise SlurmrestdRejectError(ESLURM_REST_UNKNOWN_URL)


def build_meta(request: Request, plugin: tuple[str, str], cluster: str) -> dict[str, Any]:
    user = getattr(request.state, "slurm_user", "root")
    client = request.client
    source = f"[{client.host}]:{client.port}" if client else "[unknown]:0"
    return {
        "plugin": {
            "type": plugin[0],
            "name": plugin[1],
            "data_parser": DATA_PARSER,
            "accounting_storage": "accounting_storage/slurmdbd",
        },
        "client": {"source": source, "user": user, "group": user},
        "command": [],
        "slurm": {
            "version": dict(SLURM_VERSION),
            "release": SLURM_RELEASE,
            "cluster": cluster,
        },
    }


def slurm_error(description: str, error_number: int, source: str) -> dict[str, Any]:
    return {
        "description": description,
        "error_number": error_number,
        "error": strerror(error_number),
        "source": source,
    }


def found_nothing_warning(function: str, request: Request) -> dict[str, Any]:
    """Real empty-GET warning (plugins/openapi/slurmdbd/api.c:718-760)."""
    return {"description": f"{function} found nothing", "source": request.url.path}


def make_response(
    request: Request,
    plugin: tuple[str, str],
    cluster: str,
    payload: Optional[dict[str, Any]] = None,
    errors: Optional[list[dict[str, Any]]] = None,
    warnings: Optional[list[dict[str, Any]]] = None,
) -> JSONResponse:
    body: dict[str, Any] = dict(payload or {})
    body["meta"] = build_meta(request, plugin, cluster)
    body["errors"] = list(errors or [])
    body["warnings"] = list(warnings or [])
    status = 200 if not errors else http_status_for(errors[0]["error_number"])
    return JSONResponse(status_code=status, content=body)


def reject_response(error_number: int) -> PlainTextResponse:
    return PlainTextResponse(
        strerror(error_number),
        status_code=http_status_for(error_number),
        headers={"Connection": "Close"},
    )
