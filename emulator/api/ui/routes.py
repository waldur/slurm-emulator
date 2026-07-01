"""HTMX-driven dashboard routes for the SLURM Emulator.

All routes are gated by HTTP Basic auth (:func:`require_ui_user`) and reuse the
manager methods already exposed by the JSON API, so no business logic is
duplicated. Action routes mutate shared state, persist it, then re-render the
status partial for HTMX to swap in.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Optional

from fastapi import APIRouter, Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from emulator import __version__
from emulator.api.ui.auth import require_ui_user, warn_if_default_credentials
from emulator.scenarios.scenario_registry import ActionType
from emulator.scenarios.sequence_scenario import SequenceScenario

if TYPE_CHECKING:
    from emulator.api.emulator_server import EmulatorServer
    from emulator.scenarios.scenario_registry import ScenarioDefinition

_UI_DIR = Path(__file__).parent
_templates = Jinja2Templates(directory=str(_UI_DIR / "templates"))


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


def _associations_context(server: EmulatorServer, request: Request) -> dict[str, Any]:
    cl = server.database.current_cluster
    rows = [
        {
            "account": a.account,
            "user": a.user or "(account)",
            "partition": a.partition or "—",
            "parent": a.parent or "—",
            "limits": ", ".join(f"{k}={v}" for k, v in a.limits.items()) or "—",
        }
        for a in server.database.associations.values()
        if a.cluster == cl
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
    }


def _run_scenario_headless(server: EmulatorServer, scenario: ScenarioDefinition) -> dict[str, Any]:
    """Execute a registry scenario against the shared live state.

    Non-interactive port of the CLI's scenario runner (see
    ``emulator/cli/cmd_cli.py:_execute_scenario_action``). Bookkeeping-only
    action types (checkpoint/validate/config-reload/cleanup) are skipped.
    """
    actions_run = 0
    for step in scenario.steps:
        if step.time_point:
            server.time_engine.set_time(step.time_point)

        for action in step.actions:
            params = action.parameters
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
            elif action.type == ActionType.LIMITS_CALCULATE:
                server.limits_calculator.calculate_periodic_settings(
                    params.get("account", "default_account")
                )
            # Remaining types (checkpoint, validate, config_reload, cleanup) are
            # bookkeeping-only and intentionally skipped in headless execution.
            actions_run += 1

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
        user: Annotated[str, Form()],
        node_hours: Annotated[float, Form()],
    ):
        server.usage_simulator.inject_usage(account, user, node_hours)
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

    @router.post("/scenario/run", response_class=HTMLResponse)
    async def scenario_run(request: Request, name: Annotated[str, Form()] = "sequence"):
        result: dict[str, Any] = {"name": name}
        definition = server.scenario_registry.get_scenario(name) if name != "sequence" else None
        if name != "sequence" and definition is None:
            result["ok"] = False
            result["error"] = f"Scenario '{name}' not found"
        else:
            try:
                if definition is None:  # sequence uses the dedicated runner
                    scenario = SequenceScenario(server.time_engine, server.database)
                    result["outcome"] = scenario.run_complete_scenario(interactive=False)
                else:
                    result["title"] = definition.title
                    result["outcome"] = _run_scenario_headless(server, definition)
                server.database.save_state()
                result["ok"] = True
            except Exception as e:
                result["ok"] = False
                result["error"] = str(e)
        return _templates.TemplateResponse("_result.html", {"request": request, "result": result})

    app.include_router(router)
