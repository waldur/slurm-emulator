"""Tests for the web dashboard (emulator/api/ui)."""

import pytest
from fastapi.testclient import TestClient

from emulator.api.emulator_server import create_app

UI_USER = "tester"
UI_PASSWORD = "s3cret"  # noqa: S105 - test fixture credential
AUTH = (UI_USER, UI_PASSWORD)


@pytest.fixture
def ui(state_env, monkeypatch):  # noqa: ARG001 - state_env sets state file env vars
    monkeypatch.setenv("SLURM_EMULATOR_UI_USER", UI_USER)
    monkeypatch.setenv("SLURM_EMULATOR_UI_PASSWORD", UI_PASSWORD)
    return TestClient(create_app())


def test_dashboard_requires_auth(ui):
    assert ui.get("/ui/").status_code == 401


def test_dashboard_rejects_wrong_password(ui):
    assert ui.get("/ui/", auth=(UI_USER, "wrong")).status_code == 401


def test_dashboard_loads_with_auth(ui):
    resp = ui.get("/ui/", auth=AUTH)
    assert resp.status_code == 200
    assert "SLURM Emulator" in resp.text
    assert 'hx-get="/ui/status"' in resp.text


def test_create_account_then_shows_in_status(ui):
    resp = ui.post("/ui/accounts", auth=AUTH, data={"name": "proj-a", "allocation": 1000})
    assert resp.status_code == 200
    assert "proj-a" in resp.text  # status partial returned by the action

    status = ui.get("/ui/status", auth=AUTH)
    assert "proj-a" in status.text
    assert "1000" in status.text


def test_time_advance_changes_period(ui):
    before = ui.get("/ui/status", auth=AUTH).text
    ui.post("/ui/time/advance", auth=AUTH, data={"months": 6})
    after = ui.get("/ui/status", auth=AUTH).text
    assert before != after  # time/period line changed


def test_inject_usage_reflected_in_status(ui):
    ui.post("/ui/accounts", auth=AUTH, data={"name": "proj-b", "allocation": 500})
    ui.post(
        "/ui/usage/inject",
        auth=AUTH,
        data={"account": "proj-b", "user": "alice", "node_hours": 42},
    )
    status = ui.get("/ui/status", auth=AUTH)
    assert "42" in status.text


def test_status_shows_users_and_parent_columns(ui):
    ui.post("/ui/accounts", auth=AUTH, data={"name": "proj-d", "allocation": 100})
    ui.post(
        "/ui/usage/inject",
        auth=AUTH,
        data={"account": "proj-d", "user": "bob", "node_hours": 10},
    )
    status = ui.get("/ui/status", auth=AUTH)
    assert "Users" in status.text
    assert "Parent" in status.text
    assert "bob" in status.text  # user chip


def test_associations_view(ui):
    ui.post(
        "/ui/usage/inject",
        auth=AUTH,
        data={"account": "proj-e", "user": "carol", "node_hours": 5},
    )
    assoc = ui.get("/ui/associations", auth=AUTH)
    assert assoc.status_code == 200
    assert "proj-e" in assoc.text
    assert "carol" in assoc.text


def test_run_registry_scenario(ui):
    # A non-sequence registry scenario is now runnable headlessly.
    resp = ui.post("/ui/scenario/run", auth=AUTH, data={"name": "qos_thresholds"})
    assert resp.status_code == 200
    assert "completed" in resp.text
    assert "actions executed" in resp.text


def test_run_unknown_scenario_reports_error(ui):
    resp = ui.post("/ui/scenario/run", auth=AUTH, data={"name": "does_not_exist"})
    assert resp.status_code == 200
    assert "not found" in resp.text


def test_config_view_shows_partitions_and_tres(ui):
    cfg = ui.get("/ui/config", auth=AUTH)
    assert cfg.status_code == 200
    # Static topology shared with slurmrestd emulation.
    assert "compute" in cfg.text
    assert "debug" in cfg.text
    # TRES billing weights.
    assert "CPU" in cfg.text
    assert "billing unit" in cfg.text


def test_json_parity_endpoints(ui):
    # POST /api/accounts and POST /api/time/set are unauthenticated JSON parity routes.
    assert ui.post("/api/accounts", json={"name": "proj-c"}).status_code == 200
    assert ui.post("/api/accounts", json={"name": "proj-c"}).status_code == 400  # duplicate

    resp = ui.post("/api/time/set", json={"date": "2025-05-20"})
    assert resp.status_code == 200
    assert resp.json()["new_period"] == "2025-Q2"
