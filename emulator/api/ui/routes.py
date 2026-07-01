"""HTMX-driven dashboard routes for the SLURM Emulator.

All routes are gated by HTTP Basic auth (:func:`require_ui_user`) and reuse the
manager methods already exposed by the JSON API, so no business logic is
duplicated. Action routes mutate shared state, persist it, then re-render the
status partial for HTMX to swap in.
"""

from __future__ import annotations

import contextlib
import io
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Optional

from fastapi import APIRouter, Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.datastructures import FormData

from emulator import __version__
from emulator.api.ui.auth import require_ui_user, warn_if_default_credentials
from emulator.scenarios.scenario_registry import (
    ActionType,
    ScenarioAction,
    ScenarioDefinition,
    ScenarioStep,
    ScenarioType,
)
from emulator.scenarios.sequence_scenario import SequenceScenario

if TYPE_CHECKING:
    from emulator.api.emulator_server import EmulatorServer

_UI_DIR = Path(__file__).parent
_templates = Jinja2Templates(directory=str(_UI_DIR / "templates"))

# The built-in sequence scenario isn't a registry ScenarioDefinition, so its
# description and high-level steps are described here for the UI preview.
_SEQUENCE_DESC = (
    "Full periodic-limits sequence: Q1 setup, 3-month usage simulation, "
    "Q2 carryover with decay, threshold → slowdown/blocked transitions, "
    "admin allocation increase, hard-limit test, and Q3 15-day decay."
)
_SEQUENCE_STEPS = [
    {
        "name": "Initial Q1 2024 setup",
        "description": "1000Nh quarterly allocation with 20% grace; set fairshare, GrpTRESMins and QoS threshold.",
        "actions": [],
    },
    {
        "name": "Q1 usage simulation",
        "description": "Inject ~500Nh over three months across two users.",
        "actions": [],
    },
    {
        "name": "Q2 transition with carryover",
        "description": "Apply 15-day decay carryover to compute the new total allocation.",
        "actions": [],
    },
    {
        "name": "Q2 heavy usage — threshold testing",
        "description": "Push usage past the threshold → QoS normal → slowdown.",
        "actions": [],
    },
    {
        "name": "Admin allocation increase",
        "description": "Raise the allocation → QoS slowdown → normal.",
        "actions": [],
    },
    {
        "name": "Hard-limit testing",
        "description": "Exceed the grace limit → QoS → blocked.",
        "actions": [],
    },
    {
        "name": "Q3 transition with 15-day decay",
        "description": "Decay previous usage, restore QoS, reset raw usage.",
        "actions": [],
    },
]


# --- Scenario builder ------------------------------------------------------
# Action types the headless runner executes, with their editable fields.
_ACTION_LABELS = {
    "account_create": "Create account",
    "account_delete": "Delete account",
    "usage_inject": "Inject usage",
    "time_advance": "Advance time",
    "time_set": "Set date",
    "qos_set": "Set QoS",
    "qos_check": "Check QoS",
    "limits_calculate": "Calculate limits",
}
_ACTION_FIELDS: dict[str, list[dict[str, str]]] = {
    "account_create": [
        {"key": "name", "kind": "text", "label": "Account name", "ph": "e.g. physics"},
        {"key": "allocation", "kind": "number", "label": "Allocation (Nh)", "ph": "1000"},
        {"key": "description", "kind": "text", "label": "Description", "ph": "optional"},
    ],
    "account_delete": [
        {"key": "account", "kind": "account", "label": "Account to delete", "ph": "account"}
    ],
    "usage_inject": [
        {"key": "account", "kind": "account", "label": "Account", "ph": "account"},
        {"key": "user", "kind": "text", "label": "User", "ph": "e.g. alice"},
        {"key": "amount", "kind": "number", "label": "Usage (node-hours)", "ph": "e.g. 500"},
    ],
    "time_advance": [
        {"key": "amount", "kind": "number", "label": "Amount", "ph": "e.g. 3"},
        {"key": "unit", "kind": "unit", "label": "Unit", "ph": ""},
    ],
    "time_set": [{"key": "time", "kind": "date", "label": "Jump to date", "ph": ""}],
    "qos_set": [
        {"key": "account", "kind": "account", "label": "Account", "ph": "account"},
        {"key": "qos", "kind": "qos", "label": "Set QoS to", "ph": ""},
    ],
    "qos_check": [
        {"key": "account", "kind": "account", "label": "Account to check", "ph": "account"}
    ],
    "limits_calculate": [
        {"key": "account", "kind": "account", "label": "Account", "ph": "account"}
    ],
}


def _blank_action() -> dict[str, Any]:
    return {"type": "account_create", "params": {}}


def _action_to_builder(action: ScenarioAction) -> dict[str, Any]:
    """Flatten a ScenarioAction into a builder row (string params)."""
    return {
        "type": action.type.value,
        "params": {k: str(v) for k, v in action.parameters.items()},
    }


def _parse_builder_actions(form: FormData) -> list[dict[str, Any]]:
    """Group ordered ``row-{i}-{field}`` form fields into an ordered action list."""
    rows: dict[int, dict[str, str]] = {}
    for key, value in form.multi_items():
        if not key.startswith("row-"):
            continue
        parts = key.split("-", 2)
        if len(parts) != 3 or not parts[1].isdigit():
            continue
        rows.setdefault(int(parts[1]), {})[parts[2]] = str(value)
    actions: list[dict[str, Any]] = []
    for i in sorted(rows):
        row = rows[i]
        action_type = row.get("type", "")
        if action_type not in _ACTION_FIELDS:
            continue
        params = {f["key"]: (row.get(f["key"]) or "").strip() for f in _ACTION_FIELDS[action_type]}
        actions.append({"type": action_type, "params": params})
    return actions


def _to_int(value: str, default: int) -> int:
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def _to_float(value: str, default: float) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _build_scenario(actions: list[dict[str, Any]]) -> ScenarioDefinition:
    """Turn builder rows into a runnable single-step ScenarioDefinition."""
    built: list[ScenarioAction] = []
    for action in actions:
        action_type = action["type"]
        p = action["params"]
        if action_type == "time_advance":
            unit = p.get("unit") or "months"
            params: dict[str, Any] = {"amount": _to_int(p.get("amount", ""), 0), "unit": unit}
            desc = f"Advance time {params['amount']} {unit}"
        elif action_type == "time_set":
            params = {"time": p.get("time", "")}
            desc = f"Set date to {p.get('time', '')}"
        elif action_type == "account_create":
            params = {
                "name": p.get("name", ""),
                "description": p.get("description") or "Built via UI",
                "allocation": _to_int(p.get("allocation", ""), 1000),
            }
            desc = f"Create account {p.get('name', '')} @ {params['allocation']}Nh"
        elif action_type == "account_delete":
            params = {"account": p.get("account", "")}
            desc = f"Delete account {p.get('account', '')}"
        elif action_type == "usage_inject":
            params = {
                "account": p.get("account") or "default_account",
                "user": p.get("user") or "aggregate",
                "amount": _to_float(p.get("amount", ""), 0.0),
            }
            desc = f"Inject {params['amount']}Nh for {params['user']} in {params['account']}"
        elif action_type == "qos_set":
            params = {"account": p.get("account", ""), "qos": p.get("qos", "")}
            desc = f"Set QoS {p.get('qos', '')} on {p.get('account', '')}"
        else:  # qos_check, limits_calculate
            params = {"account": p.get("account") or "default_account"}
            desc = f"{_ACTION_LABELS.get(action_type, action_type)} for {params['account']}"
        built.append(
            ScenarioAction(type=ActionType(action_type), description=desc, parameters=params)
        )

    step = ScenarioStep(name="custom", description="Custom scenario", actions=built)
    return ScenarioDefinition(
        name="__custom__",
        title="Custom scenario",
        description="Built in the scenario editor",
        scenario_type=ScenarioType.PERIODIC_LIMITS,
        steps=[step],
    )


def _builder_context(
    server: EmulatorServer, request: Request, actions: list[dict[str, Any]]
) -> dict[str, Any]:
    return {
        "request": request,
        "actions": actions,
        "action_labels": _ACTION_LABELS,
        "action_fields": _ACTION_FIELDS,
        "qos_options": _qos_options(server),
        "units": ["days", "months", "quarters"],
        "accounts": sorted(a.name for a in server.database.list_accounts() if a.name != "root"),
        "scenarios": ["sequence", *[s.name for s in server.scenario_registry.list_scenarios()]],
    }


def _account_rows(server: EmulatorServer, cluster: str) -> list[dict[str, Any]]:
    """Build per-account status rows, mirroring GET /api/status plus thresholds."""
    rows: list[dict[str, Any]] = []
    period = server.time_engine.get_current_quarter()

    for account in server.database.list_accounts():
        if account.name == "root":
            continue

        usage = server.database.get_total_usage(account.name, period, cluster=cluster)

        # Threshold check can divide by a zero allocation — degrade gracefully.
        threshold_status = "unknown"
        percentage = 0.0
        try:
            check = server.limits_calculator.check_usage_thresholds(account.name, cluster=cluster)
            threshold_status = check["threshold_status"]
            percentage = check["percentage_used"]
        except Exception:
            if account.allocation:
                percentage = (usage / account.allocation) * 100

        rows.append(
            {
                "name": account.name,
                "allocation": account.allocation,
                "usage": round(usage, 2),
                "percentage": round(percentage, 1),
                "qos": account.qos,
                "fairshare": account.fairshare,
                "threshold_status": threshold_status,
                "limits": account.limits,
                "parent": account.parent or "—",
                "users": server.database.list_account_users(account.name, cluster=cluster),
            }
        )
    return rows


def _associations_context(
    server: EmulatorServer, request: Request, account: Optional[str] = None
) -> dict[str, Any]:
    cl = server.database.current_cluster
    rows = [
        {
            "account": a.account,
            "user": a.user or "(account)",
            "user_raw": a.user,  # empty string for the account-level row
            "partition": a.partition or "—",
            "parent": a.parent or "—",
            "limits": ", ".join(f"{k}={v}" for k, v in a.limits.items()) or "—",
        }
        for a in server.database.associations.values()
        if a.cluster == cl and (account is None or a.account == account)
    ]
    rows.sort(key=lambda r: (r["account"], r["user"]))
    return {"request": request, "associations": rows}


def _status_context(
    server: EmulatorServer, request: Request, cluster: Optional[str] = None
) -> dict[str, Any]:
    cl = cluster or server.database.current_cluster
    return {
        "request": request,
        "current_time": server.time_engine.get_current_time(),
        "current_period": server.time_engine.get_current_quarter(),
        "cluster": cl,
        "clusters": [c.name for c in server.database.list_clusters()],
        "accounts": _account_rows(server, cl),
        # QoS values valid on this cluster (what set_account_qos accepts):
        # defined QoS classes first, then any operational level not among them.
        "qos_options": _qos_options(server),
    }


def _qos_options(server: EmulatorServer) -> list[str]:
    options = list(server.database.qos_list.keys())
    for level in server.qos_manager.qos_levels:
        if level not in options:
            options.append(level)
    return options


def _run_scenario_headless(server: EmulatorServer, scenario: ScenarioDefinition) -> dict[str, Any]:
    """Execute a registry scenario against the shared live state.

    Non-interactive port of the CLI's scenario runner (see
    ``emulator/cli/cmd_cli.py:_execute_scenario_action``). Bookkeeping-only
    action types (checkpoint/validate/config-reload/cleanup) are skipped.
    """
    actions_run = 0
    print(f"🎬 {scenario.title}")
    print("=" * 60)
    if scenario.description:
        print(scenario.description)

    for i, step in enumerate(scenario.steps, 1):
        print(f"\n📍 Step {i}: {step.name}")
        if step.description:
            print(f"   {step.description}")
        if step.time_point:
            server.time_engine.set_time(step.time_point)
            print(f"   ⏰ Time set to {step.time_point}")

        for action in step.actions:
            params = action.parameters
            print(f"   🔧 {action.description}")
            if action.type == ActionType.TIME_SET:
                server.time_engine.set_time(datetime.fromisoformat(params["time"]))
            elif action.type == ActionType.TIME_ADVANCE:
                unit = params["unit"]
                if unit in ("days", "months", "quarters"):
                    server.time_engine.advance_time(**{unit: params["amount"]})
            elif action.type == ActionType.USAGE_INJECT:
                server.usage_simulator.inject_usage(
                    params.get("account", "default_account"), params["user"], params["amount"]
                )
            elif action.type == ActionType.ACCOUNT_CREATE:
                name = params["name"]
                if server.database.get_account(name):
                    server.database.delete_account(name)
                server.database.add_account(
                    name, params.get("description", "Test Account"), "emulator"
                )
                server.database.set_account_allocation(name, params.get("allocation", 1000))
                print(f"      ✅ account {name} @ {params.get('allocation', 1000)}Nh")
            elif action.type == ActionType.ACCOUNT_DELETE and server.database.get_account(
                params["account"]
            ):
                server.database.delete_account(params["account"])
            elif action.type == ActionType.QOS_SET:
                server.qos_manager.set_account_qos(params["account"], params["qos"])
            elif action.type == ActionType.QOS_CHECK:
                account = params.get("account", "default_account")
                settings = server.limits_calculator.calculate_periodic_settings(account)
                usage = server.database.get_total_usage(
                    account, server.time_engine.get_current_quarter()
                )
                server.qos_manager.check_and_update_qos(
                    account, usage, settings["qos_threshold"], settings["grace_limit"]
                )
                print(
                    f"      📊 usage {usage:.0f}Nh vs threshold "
                    f"{settings['qos_threshold']:.0f}Nh → QoS {server.qos_manager.get_account_qos(account)}"
                )
            elif action.type == ActionType.LIMITS_CALCULATE:
                account = params.get("account", "default_account")
                settings = server.limits_calculator.calculate_periodic_settings(account)
                print(
                    f"      📊 fairshare {settings['fairshare']}, "
                    f"allocation {settings['total_allocation']:.0f}Nh"
                )
            # Remaining types (checkpoint, validate, config_reload, cleanup) are
            # bookkeeping-only and intentionally skipped in headless execution.
            if action.expected_outcome:
                print(f"      → {action.expected_outcome}")
            actions_run += 1

    print(f"\n✅ {scenario.title} completed — {len(scenario.steps)} steps, {actions_run} actions")

    return {"steps": len(scenario.steps), "actions": actions_run}


def _config_context(server: EmulatorServer, request: Request) -> dict[str, Any]:
    """Cluster configuration: clusters, partitions, node spec, QoS, TRES weights."""
    # Partition/node topology and node spec are shared with the slurmrestd emulation.
    from emulator.api.slurmrestd.schemas import (  # noqa: PLC0415
        _NODE_CPUS,
        _NODE_GPUS,
        _NODE_MEM_GB,
        PARTITION_RANGES,
    )

    clusters = [
        {
            "name": c.name,
            "control": f"{c.control_host}:{c.control_port}",
            "classification": c.classification.value,
            "rpc_version": c.rpc_version,
        }
        for c in server.database.list_clusters()
    ]

    partitions = [
        {"name": name, "nodes": last - first + 1, "range": f"node[{first:03d}-{last:03d}]"}
        for name, (first, last) in PARTITION_RANGES.items()
    ]

    qos = [
        {
            "name": q.name,
            "flags": q.flags or "—",
            "grp_tres": q.grp_tres or "—",
            "max_jobs": "—" if q.max_jobs < 0 else q.max_jobs,
            "max_submit": "—" if q.max_submit < 0 else q.max_submit,
            "max_wall": q.max_wall or "—",
        }
        for q in server.database.qos_list.values()
    ]
    qos.sort(key=lambda q: q["name"])

    return {
        "request": request,
        "clusters": clusters,
        "partitions": partitions,
        "node_spec": {"cpus": _NODE_CPUS, "mem_gb": _NODE_MEM_GB, "gpus": _NODE_GPUS},
        "qos": qos,
        "billing_weights": server.usage_simulator.billing_weights,
    }


def _jobs_context(server: EmulatorServer, request: Request) -> dict[str, Any]:
    jobs = server.database.list_jobs()
    # Most recent first (jobs without a submit_time sort last); cap for readability.
    jobs = sorted(
        jobs,
        key=lambda j: j.submit_time.isoformat() if j.submit_time else "",
        reverse=True,
    )[:50]
    return {"request": request, "jobs": jobs}


def mount_ui(app: FastAPI, server: EmulatorServer) -> None:
    """Attach the dashboard (static assets + routes) to an existing FastAPI app."""
    warn_if_default_credentials()

    app.mount(
        "/ui/static",
        StaticFiles(directory=str(_UI_DIR / "static")),
        name="ui-static",
    )

    router = APIRouter(prefix="/ui", dependencies=[Depends(require_ui_user)])

    def status_partial(request: Request, cluster: Optional[str] = None) -> HTMLResponse:
        return _templates.TemplateResponse(
            "_status.html", _status_context(server, request, cluster)
        )

    @router.get("/", response_class=HTMLResponse)
    async def dashboard(request: Request):
        ctx = _status_context(server, request)
        ctx["version"] = __version__
        ctx["scenarios"] = [s.get_summary() for s in server.scenario_registry.list_scenarios()]
        return _templates.TemplateResponse("index.html", ctx)

    @router.get("/status", response_class=HTMLResponse)
    async def status(request: Request, cluster: Optional[str] = None):
        return status_partial(request, cluster)

    @router.get("/jobs", response_class=HTMLResponse)
    async def jobs(request: Request):
        return _templates.TemplateResponse("_jobs.html", _jobs_context(server, request))

    @router.get("/associations", response_class=HTMLResponse)
    async def associations(request: Request):
        return _templates.TemplateResponse(
            "_associations.html", _associations_context(server, request)
        )

    @router.get("/config", response_class=HTMLResponse)
    async def config(request: Request):
        return _templates.TemplateResponse("_config.html", _config_context(server, request))

    @router.post("/time/advance", response_class=HTMLResponse)
    async def time_advance(
        request: Request,
        days: Annotated[int, Form()] = 0,
        months: Annotated[int, Form()] = 0,
        quarters: Annotated[int, Form()] = 0,
    ):
        server.time_engine.advance_time(days=days, months=months, quarters=quarters)
        return status_partial(request)

    @router.post("/time/set", response_class=HTMLResponse)
    async def time_set(request: Request, date: Annotated[str, Form()]):
        server.time_engine.set_time(datetime.fromisoformat(date))
        return status_partial(request)

    @router.post("/usage/inject", response_class=HTMLResponse)
    async def usage_inject(
        request: Request,
        account: Annotated[str, Form()],
        node_hours: Annotated[float, Form()],
        user: Annotated[str, Form()] = "",
    ):
        # Blank user → account-level (aggregate) usage.
        server.usage_simulator.inject_usage(account, user.strip() or "aggregate", node_hours)
        server.database.save_state()
        return status_partial(request)

    @router.post("/accounts", response_class=HTMLResponse)
    async def create_account(
        request: Request,
        name: Annotated[str, Form()],
        description: Annotated[str, Form()] = "Created via web UI",
        allocation: Annotated[int, Form()] = 0,
    ):
        if not server.database.get_account(name):
            server.database.add_account(name, description, "emulator")
        account_obj = server.database.get_account(name)
        if allocation:
            account_obj.allocation = allocation
        server.database.save_state()
        return status_partial(request)

    @router.post("/accounts/edit", response_class=HTMLResponse)
    async def edit_account(
        request: Request,
        name: Annotated[str, Form()],
        allocation: Annotated[Optional[str], Form()] = None,
        parent: Annotated[Optional[str], Form()] = None,
        description: Annotated[Optional[str], Form()] = None,
    ):
        # Blank fields are left unchanged; the account is created if missing.
        name = name.strip()
        if not server.database.get_account(name):
            server.database.add_account(name, description or "Created via web UI", "emulator")
        account_obj = server.database.get_account(name)
        if description:
            account_obj.description = description
        if allocation:
            with contextlib.suppress(ValueError):
                server.database.set_account_allocation(name, int(allocation))
        if parent:
            server.database.set_account_parent(name, parent.strip())
        server.database.save_state()
        return status_partial(request)

    @router.post("/apply-settings", response_class=HTMLResponse)
    async def apply_settings(
        request: Request,
        account: Annotated[str, Form()],
        fairshare: Annotated[Optional[int], Form()] = None,
        grp_tres_mins_billing: Annotated[Optional[int], Form()] = None,
    ):
        # Imported lazily to avoid a circular import (emulator_server imports this module).
        from emulator.api.emulator_server import apply_settings_to_account  # noqa: PLC0415

        grp_tres_mins = (
            {"billing": grp_tres_mins_billing} if grp_tres_mins_billing is not None else None
        )
        apply_settings_to_account(server, account, fairshare=fairshare, grp_tres_mins=grp_tres_mins)
        return status_partial(request)

    @router.post("/qos/downscale", response_class=HTMLResponse)
    async def qos_downscale(
        request: Request,
        account: Annotated[str, Form()],
        qos: Annotated[str, Form()] = "slowdown",
    ):
        server.qos_manager.set_account_qos(account, qos)
        server.database.save_state()
        return status_partial(request)

    @router.post("/qos/restore", response_class=HTMLResponse)
    async def qos_restore(request: Request, account: Annotated[str, Form()]):
        server.qos_manager.restore_qos_for_new_period(account)
        server.database.save_state()
        return status_partial(request)

    @router.post("/qos/set", response_class=HTMLResponse)
    async def qos_set(
        request: Request,
        account: Annotated[str, Form()],
        qos: Annotated[str, Form()],
    ):
        # set_account_qos rejects values outside the cluster's QoS levels.
        server.qos_manager.set_account_qos(account, qos)
        server.database.save_state()
        return status_partial(request)

    @router.post("/scenario/run", response_class=HTMLResponse)
    async def scenario_run(request: Request, name: Annotated[str, Form()] = "sequence"):
        result: dict[str, Any] = {"name": name}
        definition = server.scenario_registry.get_scenario(name) if name != "sequence" else None
        if name != "sequence" and definition is None:
            result["ok"] = False
            result["error"] = f"Scenario '{name}' not found"
        else:
            # Scenarios print a rich step-by-step log to stdout — capture it for the UI.
            buffer = io.StringIO()
            try:
                with contextlib.redirect_stdout(buffer):
                    if definition is None:  # sequence uses the dedicated runner
                        scenario = SequenceScenario(server.time_engine, server.database)
                        result["outcome"] = scenario.run_complete_scenario(interactive=False)
                    else:
                        result["title"] = definition.title
                        outcome = _run_scenario_headless(server, definition)
                        result["summary_line"] = (
                            f"{outcome['steps']} steps, {outcome['actions']} actions executed"
                        )
                server.database.save_state()
                result["ok"] = True
            except Exception as e:
                result["ok"] = False
                result["error"] = str(e)
            result["log"] = buffer.getvalue().strip()
        return _templates.TemplateResponse("_result.html", {"request": request, "result": result})

    @router.get("/scenario/steps", response_class=HTMLResponse)
    async def scenario_steps(request: Request, name: str = "sequence"):
        # Preview the planned steps of a scenario before running it.
        if name == "sequence":
            ctx = {"request": request, "description": _SEQUENCE_DESC, "steps": _SEQUENCE_STEPS}
        else:
            definition = server.scenario_registry.get_scenario(name)
            if definition is None:
                ctx = {"request": request, "description": "", "steps": []}
            else:
                ctx = {
                    "request": request,
                    "description": definition.description,
                    "steps": [
                        {
                            "name": step.name,
                            "description": step.description,
                            "actions": [a.description for a in step.actions],
                        }
                        for step in definition.steps
                    ],
                }
        return _templates.TemplateResponse("_scenario_steps.html", ctx)

    @router.get("/scenario/build", response_class=HTMLResponse)
    async def scenario_build(request: Request, name: str = ""):
        # Prefill from an existing scenario's actions when a name is given.
        actions: list[dict[str, Any]] = []
        if name and name != "sequence":
            definition = server.scenario_registry.get_scenario(name)
            if definition is not None:
                actions = [
                    _action_to_builder(act)
                    for step in definition.steps
                    for act in step.actions
                    if act.type.value in _ACTION_FIELDS
                ]
        if not actions:
            actions = [_blank_action()]
        return _templates.TemplateResponse(
            "_scenario_builder.html", _builder_context(server, request, actions)
        )

    @router.post("/scenario/build/rows", response_class=HTMLResponse)
    async def scenario_build_rows(request: Request):
        form = await request.form()
        op = str(form.get("op", ""))
        idx = _to_int(str(form.get("idx", "-1")), -1)
        actions = _parse_builder_actions(form)
        if op == "add":
            actions.append(_blank_action())
        elif op == "remove" and 0 <= idx < len(actions):
            actions.pop(idx)
        elif op == "up" and 0 < idx < len(actions):
            actions[idx - 1], actions[idx] = actions[idx], actions[idx - 1]
        elif op == "down" and 0 <= idx < len(actions) - 1:
            actions[idx + 1], actions[idx] = actions[idx], actions[idx + 1]
        # Any other op (e.g. a type change) just re-renders with the parsed state.
        if not actions:
            actions = [_blank_action()]
        return _templates.TemplateResponse(
            "_builder_rows.html", _builder_context(server, request, actions)
        )

    @router.post("/scenario/build/run", response_class=HTMLResponse)
    async def scenario_build_run(request: Request):
        form = await request.form()
        actions = _parse_builder_actions(form)
        result: dict[str, Any] = {"name": "custom scenario"}
        buffer = io.StringIO()
        try:
            with contextlib.redirect_stdout(buffer):
                _run_scenario_headless(server, _build_scenario(actions))
            server.database.save_state()
            result["ok"] = True
            result["summary_line"] = f"{len(actions)} actions executed"
        except Exception as e:  # surface any build/run failure to the UI
            result["ok"] = False
            result["error"] = str(e)
        result["log"] = buffer.getvalue().strip()
        return _templates.TemplateResponse("_result.html", {"request": request, "result": result})

    @router.get("/control/{action}", response_class=HTMLResponse)
    async def control_form(request: Request, action: str):
        # Serve a control's form fresh so account dropdowns reflect current state.
        accounts = sorted(a.name for a in server.database.list_accounts() if a.name != "root")
        scenarios = [{"name": "sequence", "description": _SEQUENCE_DESC}]
        scenarios += [
            {"name": s.name, "description": s.description}
            for s in server.scenario_registry.list_scenarios()
            if s.name != "sequence"
        ]
        return _templates.TemplateResponse(
            "_control.html",
            {"request": request, "action": action, "accounts": accounts, "scenarios": scenarios},
        )

    def assoc_modal(request: Request, account: str) -> HTMLResponse:
        ctx = _associations_context(server, request, account=account)
        ctx["account"] = account
        return _templates.TemplateResponse("_assoc_modal.html", ctx)

    @router.get("/associations/{account}", response_class=HTMLResponse)
    async def account_associations(request: Request, account: str):
        return assoc_modal(request, account)

    @router.post("/associations/{account}/add", response_class=HTMLResponse)
    async def add_account_user(request: Request, account: str, user: Annotated[str, Form()]):
        user = user.strip()
        if user:
            if not server.database.get_user(user):
                server.database.add_user(user, account)
            if not server.database.get_association(user, account):
                server.database.add_association(user, account)
            server.database.save_state()
        return assoc_modal(request, account)

    @router.post("/associations/{account}/remove", response_class=HTMLResponse)
    async def remove_account_user(request: Request, account: str, user: Annotated[str, Form()]):
        if user:
            server.database.delete_user_associations(user, account)
            server.database.save_state()
        return assoc_modal(request, account)

    app.include_router(router)
