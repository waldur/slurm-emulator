"""slurmrestd authentication emulation (rest_auth/jwt semantics).

Token sources match src/slurmrestd/plugins/auth/jwt/jwt.c:
``X-SLURM-USER-TOKEN`` header or ``Authorization: Bearer``, with the
optional ``X-SLURM-USER-NAME`` hint. By default any non-empty token is
accepted; setting ``SLURM_EMULATOR_JWT_KEY`` switches on real HS256
verification (signature + ``exp``), with the ``sun`` claim naming the
user as in real auth/jwt. Implemented with the stdlib only — no PyJWT
dependency.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any, Optional

from fastapi import Request

from emulator.api.slurmrestd.envelope import ESLURM_REST_AUTH_FAIL, SlurmrestdRejectError

DEFAULT_JWT_KEY = "slurm-emulator-dev-key"


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def _signature(signing_input: bytes, key: str) -> bytes:
    return hmac.new(key.encode("utf-8"), signing_input, hashlib.sha256).digest()


def encode_jwt_hs256(
    username: str,
    lifespan: int = 1800,
    key: Optional[str] = None,
    now: Optional[int] = None,
) -> str:
    """Mint an HS256 JWT like ``scontrol token`` (claims iat/exp/sun)."""
    issued_at = int(now if now is not None else time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    claims = {"iat": issued_at, "exp": issued_at + lifespan, "sun": username}
    signing_input = (
        f"{_b64url_encode(json.dumps(header).encode())}"
        f".{_b64url_encode(json.dumps(claims).encode())}"
    ).encode("ascii")
    signature = _signature(signing_input, key or DEFAULT_JWT_KEY)
    return f"{signing_input.decode('ascii')}.{_b64url_encode(signature)}"


def decode_jwt_hs256(token: str, key: str, now: Optional[int] = None) -> dict[str, Any]:
    """Verify signature and expiry; raises ValueError on any failure."""
    try:
        header_b64, claims_b64, signature_b64 = token.split(".")
        signing_input = f"{header_b64}.{claims_b64}".encode("ascii")
        signature = _b64url_decode(signature_b64)
        claims = json.loads(_b64url_decode(claims_b64))
    except Exception as e:
        raise ValueError(f"Malformed JWT: {e}") from e

    if not hmac.compare_digest(signature, _signature(signing_input, key)):
        msg = "Bad JWT signature"
        raise ValueError(msg)
    expires = claims.get("exp")
    if expires is not None and int(now if now is not None else time.time()) >= int(expires):
        msg = "Expired JWT"
        raise ValueError(msg)
    return claims


def slurmrestd_auth(request: Request) -> None:
    """FastAPI dependency enforcing slurmrestd token auth."""
    token = request.headers.get("X-SLURM-USER-TOKEN")
    if not token:
        authorization = request.headers.get("Authorization", "")
        if authorization.startswith("Bearer "):
            token = authorization[len("Bearer ") :].strip()
    if not token:
        raise SlurmrestdRejectError(ESLURM_REST_AUTH_FAIL)

    username = request.headers.get("X-SLURM-USER-NAME", "root")
    key = os.environ.get("SLURM_EMULATOR_JWT_KEY")
    if key:
        try:
            claims = decode_jwt_hs256(token, key)
        except ValueError as e:
            raise SlurmrestdRejectError(ESLURM_REST_AUTH_FAIL) from e
        username = claims.get("sun", username)

    request.state.slurm_user = username
