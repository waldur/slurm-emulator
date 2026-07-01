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


def test_edit_account_updates_allocation_and_parent(ui):
    ui.post("/ui/accounts", auth=AUTH, data={"name": "proj-x", "allocation": 100})
    resp = ui.post(
        "/ui/accounts/edit",
        auth=AUTH,
        data={"name": "proj-x", "allocation": "750", "parent": "root", "description": ""},
    )
    assert resp.status_code == 200
    assert "750" in resp.text  # updated allocation shown in status partial

    status = ui.get("/ui/status", auth=AUTH)
    assert "750" in status.text


def test_edit_account_blank_fields_keep_values(ui):
    ui.post("/ui/accounts", auth=AUTH, data={"name": "proj-y", "allocation": 500})
    # Blank allocation must not wipe the existing value.
    ui.post("/ui/accounts/edit", auth=AUTH, data={"name": "proj-y", "allocation": ""})
    status = ui.get("/ui/status", auth=AUTH)
    assert "500" in status.text


def test_control_form_account_field_is_dropdown(ui):
    ui.post("/ui/accounts", auth=AUTH, data={"name": "proj-drop", "allocation": 10})
    form = ui.get("/ui/control/usage", auth=AUTH)
    assert form.status_code == 200
    assert '<select name="account"' in form.text  # dropdown, not free text
    assert "proj-drop" in form.text


def test_control_form_needs_account_notice_when_empty(ui):
    form = ui.get("/ui/control/qos", auth=AUTH)
    assert "No accounts exist yet" in form.text


def test_account_associations_modal(ui):
    ui.post(
        "/ui/usage/inject",
        auth=AUTH,
        data={"account": "proj-assoc", "user": "frank", "node_hours": 3},
    )
    modal = ui.get("/ui/associations/proj-assoc", auth=AUTH)
    assert modal.status_code == 200
    assert "Associations · proj-assoc" in modal.text
    assert "frank" in modal.text


def test_associations_add_and_remove_user(ui):
    ui.post("/ui/accounts", auth=AUTH, data={"name": "proj-assoc2", "allocation": 100})
    # Add a user to the account via the modal endpoint.
    added = ui.post("/ui/associations/proj-assoc2/add", auth=AUTH, data={"user": "grace"})
    assert added.status_code == 200
    assert "grace" in added.text
    assert "Associations · proj-assoc2" in added.text

    # Remove that user again.
    removed = ui.post("/ui/associations/proj-assoc2/remove", auth=AUTH, data={"user": "grace"})
    assert removed.status_code == 200
    assert "grace" not in removed.text


def test_inject_account_level_usage_without_user(ui):
    ui.post("/ui/accounts", auth=AUTH, data={"name": "proj-agg", "allocation": 100})
    # No user field → account-level aggregate usage.
    resp = ui.post("/ui/usage/inject", auth=AUTH, data={"account": "proj-agg", "node_hours": 25})
    assert resp.status_code == 200
    assert "25" in resp.text  # usage recorded on the account
    assert "aggregate" in resp.text  # aggregate user chip


def test_inline_qos_set_from_status(ui):
    ui.post("/ui/accounts", auth=AUTH, data={"name": "proj-q", "allocation": 100})
    resp = ui.post("/ui/qos/set", auth=AUTH, data={"account": "proj-q", "qos": "blocked"})
    assert resp.status_code == 200
    assert "qos-blocked" in resp.text  # status partial reflects new QoS

    # Status table renders a QoS dropdown limited to available levels.
    status = ui.get("/ui/status", auth=AUTH)
    assert 'class="qos-select' in status.text
    assert 'hx-post="/ui/qos/set"' in status.text


def test_scenario_form_loads_steps_preview(ui):
    form = ui.get("/ui/control/scenario", auth=AUTH)
    assert form.status_code == 200
    assert 'hx-get="/ui/scenario/steps"' in form.text
    assert 'id="scenario-steps"' in form.text


def test_scenario_steps_preview(ui):
    seq = ui.get("/ui/scenario/steps", auth=AUTH, params={"name": "sequence"})
    assert seq.status_code == 200
    assert "periodic-limits sequence" in seq.text  # description
    assert "Q1" in seq.text  # step names present
    # A registry scenario enumerates its real steps.
    reg = ui.get("/ui/scenario/steps", auth=AUTH, params={"name": "traditional_max_tres_mins"})
    assert reg.status_code == 200
    assert "setup_traditional" in reg.text


def test_registry_scenario_log_covers_all_actions(ui):
    resp = ui.post("/ui/scenario/run", auth=AUTH, data={"name": "traditional_max_tres_mins"})
    assert resp.status_code == 200
    assert "actions executed" in resp.text
    # Each action prints a "🔧" line, so the log length matches the action count.
    assert resp.text.count("🔧") >= 5


def test_scenario_run_returns_console_log(ui):
    resp = ui.post("/ui/scenario/run", auth=AUTH, data={"name": "sequence"})
    assert resp.status_code == 200
    assert "completed" in resp.text
    assert 'class="console-log"' in resp.text  # captured stdout is shown


def test_bootstrapped_qos_classes_available_and_assignable(ui):
    cfg = ui.get("/ui/config", auth=AUTH)
    assert "DenyOnLimit" in cfg.text  # seeded QoS class flags present
    assert "long" in cfg.text
    # A bootstrapped class (not an operational level) is assignable to an account.
    ui.post("/ui/accounts", auth=AUTH, data={"name": "proj-h", "allocation": 100})
    resp = ui.post("/ui/qos/set", auth=AUTH, data={"account": "proj-h", "qos": "high"})
    assert resp.status_code == 200
    assert "qos-high" in resp.text


def test_scenario_builder_opens_with_action_row(ui):
    b = ui.get("/ui/scenario/build", auth=AUTH)
    assert b.status_code == 200
    assert "Build scenario" in b.text
    assert 'name="row-0-type"' in b.text  # at least one action row
    assert "Create account" in b.text  # action type options


def test_scenario_builder_add_and_remove_rows(ui):
    added = ui.post(
        "/ui/scenario/build/rows",
        auth=AUTH,
        data={"op": "add", "row-0-type": "account_create"},
    )
    assert 'name="row-1-type"' in added.text  # a second row appeared

    removed = ui.post(
        "/ui/scenario/build/rows",
        auth=AUTH,
        data={
            "op": "remove",
            "idx": "1",
            "row-0-type": "account_create",
            "row-1-type": "usage_inject",
        },
    )
    assert 'name="row-1-type"' not in removed.text  # back to one row


def test_scenario_builder_run_executes_actions(ui):
    # Build: create account → inject usage → advance time, then run.
    data = {
        "row-0-type": "account_create",
        "row-0-name": "built-acct",
        "row-0-allocation": "1000",
        "row-1-type": "usage_inject",
        "row-1-account": "built-acct",
        "row-1-user": "dave",
        "row-1-amount": "250",
        "row-2-type": "time_advance",
        "row-2-amount": "3",
        "row-2-unit": "months",
    }
    run = ui.post("/ui/scenario/build/run", auth=AUTH, data=data)
    assert run.status_code == 200
    assert "completed" in run.text
    assert 'class="console-log"' in run.text
    # The actions actually mutated live state.
    status = ui.get("/ui/status", auth=AUTH)
    assert "built-acct" in status.text
    assert "dave" in status.text


def test_scenario_builder_prefills_from_scenario(ui):
    b = ui.get("/ui/scenario/build", auth=AUTH, params={"name": "traditional_max_tres_mins"})
    assert b.status_code == 200
    # Its account_create/usage_inject actions become editable rows.
    assert "traditional_account" in b.text


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
