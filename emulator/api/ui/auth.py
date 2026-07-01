"""HTTP Basic auth for the web dashboard.

Credentials come from ``SLURM_EMULATOR_UI_USER`` / ``SLURM_EMULATOR_UI_PASSWORD``.
When unset they default to ``admin`` / ``admin`` and a warning is printed at
startup — fine for a local dev/testing emulator, but put it behind TLS and set
real credentials if you ever expose it. Comparison uses ``secrets.compare_digest``
for constant-time (timing-attack-safe) matching.
"""

from __future__ import annotations

import os
import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

DEFAULT_UI_USER = "admin"
DEFAULT_UI_PASSWORD = "admin"  # noqa: S105 - dev default, overridable via env var

_security = HTTPBasic()


def _credentials() -> tuple[str, str]:
    return (
        os.environ.get("SLURM_EMULATOR_UI_USER", DEFAULT_UI_USER),
        os.environ.get("SLURM_EMULATOR_UI_PASSWORD", DEFAULT_UI_PASSWORD),
    )


def warn_if_default_credentials() -> None:
    """Print a loud warning when the dashboard is using default credentials."""
    if "SLURM_EMULATOR_UI_USER" not in os.environ or "SLURM_EMULATOR_UI_PASSWORD" not in os.environ:
        print(
            "⚠️  Web UI is using default credentials "
            f"({DEFAULT_UI_USER}/{DEFAULT_UI_PASSWORD}). "
            "Set SLURM_EMULATOR_UI_USER / SLURM_EMULATOR_UI_PASSWORD to change them."
        )


def require_ui_user(
    credentials: Annotated[HTTPBasicCredentials, Depends(_security)],
) -> str:
    """FastAPI dependency enforcing HTTP Basic auth for all UI routes."""
    expected_user, expected_password = _credentials()
    # compare_digest on both fields; encode to bytes so unicode input can't raise.
    correct_user = secrets.compare_digest(
        credentials.username.encode("utf-8"), expected_user.encode("utf-8")
    )
    correct_password = secrets.compare_digest(
        credentials.password.encode("utf-8"), expected_password.encode("utf-8")
    )
    if not (correct_user and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username
