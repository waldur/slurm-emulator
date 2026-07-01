"""Real-client conformance: drive FireCREST's own SlurmRestClient.

This is the highest-fidelity check — it exercises FireCREST v2's actual
request-building and response-parsing code against a live emulator, so a
green run means the emulator conforms to what FireCREST really expects.

Requires a FireCREST checkout, pointed at by ``FIRECREST_SRC`` (the repo
root; its ``src/`` is added to sys.path), plus FireCREST's client runtime
deps in the environment (aiohttp, python-jose, packaging, fastapi). The
whole module is skipped when either is missing, so CI stays green without
FireCREST installed.

    FIRECREST_SRC=/path/to/firecrest-v2 uv run --extra dev \
        pytest tests/firecrest/test_real_client.py
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from emulator.core.database import SlurmDatabase

FIRECREST_SRC = os.environ.get("FIRECREST_SRC")
if not FIRECREST_SRC:
    pytest.skip("set FIRECREST_SRC to a firecrest-v2 checkout", allow_module_level=True)

_src = Path(FIRECREST_SRC) / "src"
if _src.is_dir() and str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

try:  # FireCREST client + its runtime deps must import
    from lib.scheduler_clients.slurm.models import SlurmJobDescription
    from lib.scheduler_clients.slurm.slurm_rest_client import SlurmRestClient
except Exception as exc:  # any import failure => skip, not fail
    pytest.skip(f"FireCREST client not importable: {exc}", allow_module_level=True)

USERNAME = "fireuser"
USERNAME_CLAIM = "username"


def _make_jwt(claims: dict) -> str:
    """A well-formed (unsigned) JWT — FireCREST decodes claims unverified."""

    def seg(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{seg({'alg': 'HS256', 'typ': 'JWT'})}.{seg(claims)}.sig"


JWT = _make_jwt({USERNAME_CLAIM: USERNAME, "sub": USERNAME})


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def emulator(tmp_path):
    """Live slurmrestd emulator on an ephemeral port, jobs completing fast."""
    port = _free_port()
    env = {
        **os.environ,
        "SLURM_EMULATOR_STATE_FILE": str(tmp_path / "db.json"),
        "SLURM_EMULATOR_TIME_FILE": str(tmp_path / "time.json"),
        "SLURM_EMULATOR_JOB_CLOCK": "wall",
        "SLURM_EMULATOR_JOB_RUN_DELAY": "0",
        "SLURM_EMULATOR_JOB_RUN_DURATION": "0",
    }
    env.pop("SLURM_EMULATOR_JWT_KEY", None)

    # Seed a user + account + association so submit gets a default account and
    # get_accounts returns something.
    os.environ["SLURM_EMULATOR_STATE_FILE"] = env["SLURM_EMULATOR_STATE_FILE"]
    db = SlurmDatabase()
    db.load_state()
    db.add_account("proj1", "Project 1", "org")
    db.add_user(USERNAME, default_account="proj1")
    db.add_association(USERNAME, "proj1")
    db.save_state()

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "emulator.api.slurmrestd.app:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        env=env,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        _wait_ready(f"{base}/slurm/v0.0.46/ping/")
        yield base
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def _wait_ready(url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    req = urllib.request.Request(url, headers={"X-SLURM-USER-TOKEN": "x"})  # noqa: S310
    while time.time() < deadline:
        try:
            urllib.request.urlopen(req, timeout=2)  # noqa: S310
            return
        except (urllib.error.URLError, ConnectionError, OSError):
            time.sleep(0.3)
    raise RuntimeError(f"emulator did not become ready at {url}")


def _client(base: str) -> SlurmRestClient:
    return SlurmRestClient(
        api_url=base, api_version="0.0.46", timeout=30, username_claim=USERNAME_CLAIM
    )


def test_submit_poll_cancel_and_queries(emulator):
    async def flow():
        c = _client(emulator)
        try:
            desc = SlurmJobDescription(
                name="conformance",
                partition="compute",
                current_working_directory="/home/fireuser",
                script="#!/bin/bash\necho hi",
            )
            job_id = await c.submit_job(desc, username=USERNAME, jwt_token=JWT)
            assert job_id is not None

            jobs = await c.get_job(job_id, username=USERNAME, jwt_token=JWT)
            assert jobs, "get_job returned nothing"
            job = jobs[0]
            assert str(job.job_id) == str(job_id)
            assert job.name == "conformance"
            assert job.status.state  # parsed a state string

            listing = await c.get_jobs(username=USERNAME, jwt_token=JWT, allusers=True)
            assert any(str(j.job_id) == str(job_id) for j in listing)

            assert await c.cancel_job(job_id, username=USERNAME, jwt_token=JWT) is True

            nodes = await c.get_nodes(username=USERNAME, jwt_token=JWT)
            assert nodes
            partitions = await c.get_partitions(show_hidden=False, username=USERNAME, jwt_token=JWT)
            assert {p.partition for p in partitions} == {"debug", "compute"}
            pings = await c.ping(username=USERNAME, jwt_token=JWT)
            assert pings
            accounts = await c.get_accounts(username=USERNAME, jwt_token=JWT)
            assert any(a.name == "proj1" for a in accounts)
        finally:
            await SlurmRestClient.close_aiohttp_client()

    asyncio.run(flow())
