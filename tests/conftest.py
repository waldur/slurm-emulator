"""Shared fixtures for slurmrestd emulator tests."""

import pytest
from fastapi.testclient import TestClient

from emulator import slurm_version
from emulator.api.slurmrestd.app import create_app


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "slurm_version(*specs): parity expectation holds only for these Slurm versions "
        '("25.11", "25.11+"); skipped when SLURM_EMULATOR_SLURM_VERSION targets another one',
    )


def pytest_runtest_setup(item):
    marker = item.get_closest_marker("slurm_version")
    if marker and not slurm_version.matches(*marker.args):
        pytest.skip(f"target Slurm {slurm_version.get_target_version()} not in {marker.args}")


@pytest.fixture
def slurm_target_version():
    """The Slurm version the emulator currently emulates (docs/slurm-parity.md)."""
    return slurm_version.get_target_version()


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
