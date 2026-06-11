"""Shared fixtures for slurmrestd emulator tests."""

import pytest
from fastapi.testclient import TestClient

from emulator.api.slurmrestd.app import create_app


@pytest.fixture
def state_env(tmp_path, monkeypatch):
    """Isolate the JSON state files per test via the env overrides."""
    monkeypatch.setenv("SLURM_EMULATOR_STATE_FILE", str(tmp_path / "db.json"))
    monkeypatch.setenv("SLURM_EMULATOR_TIME_FILE", str(tmp_path / "time.json"))
    monkeypatch.delenv("SLURM_EMULATOR_JWT_KEY", raising=False)
    return tmp_path


@pytest.fixture
def restd(state_env):  # noqa: ARG001 - fixture sets the state env vars
    """TestClient for the slurmrestd app with isolated state."""
    return TestClient(create_app())


@pytest.fixture
def auth_headers():
    return {"X-SLURM-USER-TOKEN": "any-token"}
