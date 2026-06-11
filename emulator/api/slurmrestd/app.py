"""slurmrestd emulator FastAPI application (default port 6820).

Run with ``slurm-emulator`` console script ``slurmrestd-emulator`` or
``uvicorn emulator.api.slurmrestd.app:app --port 6820``. Rejection
behavior matches real slurmrestd: missing/invalid auth → 401 plain
text, unknown URL or unsupported API version → 404 plain text, both
with ``Connection: Close``; everything else returns the JSON envelope.
"""

from __future__ import annotations

from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from emulator import __version__
from emulator.api.slurmrestd.envelope import (
    API_VERSION,
    ESLURM_REST_UNKNOWN_URL,
    ESLURM_REST_UNKNOWN_URL_METHOD,
    SlurmrestdRejectError,
    reject_response,
)
from emulator.api.slurmrestd.routers import slurmctld, slurmdb

DEFAULT_PORT = 6820


def _build_openapi_spec(app: FastAPI) -> dict[str, Any]:
    """FastAPI auto-spec dressed up as the real self-description.

    Real slurmrestd generates its spec at runtime (no v0.0.46 spec
    file ships in the 26.11 tree), so we do the same rather than
    vendoring the 15k-line v0.0.45 document.
    """
    spec = app.openapi()
    spec["info"]["title"] = "Slurm REST API"
    spec["info"]["version"] = API_VERSION
    spec["paths"] = {
        path.replace("{version}", API_VERSION): operations
        for path, operations in spec.get("paths", {}).items()
    }
    return spec


def create_app() -> FastAPI:
    app = FastAPI(
        title="Slurm REST API",
        version=f"{API_VERSION} (slurm-emulator {__version__})",
        openapi_url=None,  # served manually at the real slurmrestd paths
        docs_url=None,
        redoc_url=None,
    )
    app.include_router(slurmdb.router)
    app.include_router(slurmctld.router)

    @app.exception_handler(SlurmrestdRejectError)
    async def handle_reject(_request: Request, exc: SlurmrestdRejectError) -> PlainTextResponse:
        return reject_response(exc.error_number)

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_exception(
        _request: Request, exc: StarletteHTTPException
    ) -> PlainTextResponse:
        # Unknown paths and unsupported methods reject exactly like
        # _operations_router_reject (operations.c:222).
        if exc.status_code == 404:
            return reject_response(ESLURM_REST_UNKNOWN_URL)
        if exc.status_code == 405:
            return reject_response(ESLURM_REST_UNKNOWN_URL_METHOD)
        return PlainTextResponse(str(exc.detail), status_code=exc.status_code)

    # Real slurmrestd self-description paths (src/slurmrestd/openapi.c:312-330).
    @app.get("/openapi.json", include_in_schema=False)
    @app.get("/openapi", include_in_schema=False)
    @app.get("/openapi/v3", include_in_schema=False)
    async def openapi_spec() -> JSONResponse:
        return JSONResponse(_build_openapi_spec(app))

    return app


app = create_app()


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=DEFAULT_PORT)


if __name__ == "__main__":
    main()
