# ruff: noqa: PTH110, PTH118, PTH123
"""Drive waldur-site-agent's SLURM clients through one scenario and record what they return.

usage: run_scenario.py --mode cli|rest --bin DIR [--rest-url URL] --out FILE

``--bin`` holds wrappers for sacctmgr/sacct/scancel/scontrol/sinfo/id plus two
helpers the agent never calls: ``submit USER ACCOUNT PARTITION SECONDS`` (prints
a job id) and ``jobstate JOBID``. Each step records the client's return value
(normalised) or its exception, and the commands/requests it issued. Raw
backend output is logged by the wrappers (CLI) or by an httpx hook (REST).
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import subprocess
import sys
import time
from typing import Any

import httpx

try:
    from waldur_site_agent.backend.structures import MaintenanceReservation
except ImportError:  # site agent without maintenance reservations
    MaintenanceReservation = None
from waldur_site_agent_slurm.client import SlurmClient
from waldur_site_agent_slurm.rest_client import SlurmRestClient

TRES = {
    "cpu": {
        "limit": 10,
        "measured_unit": "k-Hours",
        "unit_factor": 60000,
        "accounting_type": "usage",
    },
    "mem": {
        "limit": 10,
        "measured_unit": "gb-Hours",
        "unit_factor": 61440,
        "accounting_type": "usage",
    },
    "gres/gpu": {
        "limit": 10,
        "measured_unit": "GPU-Hours",
        "unit_factor": 60,
        "accounting_type": "usage",
    },
}
CLUSTER = "demo"


def normalise(value: Any) -> Any:  # noqa: ANN401
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return normalise(dataclasses.asdict(value))
    if isinstance(value, dt.datetime):
        return value.astimezone(dt.timezone.utc).isoformat()
    if isinstance(value, dict):
        return {str(k): normalise(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple, set)):
        items = [normalise(v) for v in value]
        return items if not isinstance(value, set) else sorted(items, key=json.dumps)
    if type(value).__name__ == "SlurmReportLine":
        return {
            "account": value.account,
            "user": value.user,
            "duration": getattr(value, "duration", None),
            "tres_usage": normalise(value.tres_usage),
        }
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


class Runner:
    def __init__(self, client: Any, bin_dir: str, log_raw: list) -> None:  # noqa: ANN401
        self.client = client
        self.bin = bin_dir
        self.steps: list[dict] = []
        self.raw = log_raw
        self.jobs: dict[str, str] = {}

    def step(self, label: str, method: str, *args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
        self.client.clear_executed_commands()
        self._pull_cli_log()
        raw_start = len(self.raw)
        entry: dict[str, Any] = {"step": label, "method": method}
        try:
            result = getattr(self.client, method)(*args, **kwargs)
            entry["result"] = normalise(result)
        except Exception as exc:
            entry["error"] = f"{type(exc).__name__}: {str(exc).strip()[:600]}"
            result = None
        entry["calls"] = list(self.client.executed_commands)
        self._pull_cli_log()
        entry["raw"] = self.raw[raw_start:]
        self.steps.append(entry)
        print(("ERR " if "error" in entry else "ok  ") + label, file=sys.stderr, flush=True)
        return result

    def _pull_cli_log(self) -> None:
        """Append what the CLI wrappers logged since the last look (helpers excluded)."""
        path = os.environ["PARITY_RAW_LOG"]
        if not os.path.exists(path):
            return
        with open(path) as f:
            f.seek(getattr(self, "_log_pos", 0))
            for line in f:
                record = json.loads(line)
                if record["argv"][0] not in ("submit", "jobstate"):
                    self.raw.append(record)
            self._log_pos = f.tell()

    def helper(self, *argv: str) -> str:
        return subprocess.check_output(
            [os.path.join(self.bin, argv[0]), *argv[1:]], text=True
        ).strip()

    def submit(self, key: str, user: str, account: str, partition: str, seconds: int) -> None:
        try:
            self.jobs[key] = self.helper("submit", user, account, partition, str(seconds))
        except subprocess.CalledProcessError as exc:
            self.jobs[key] = ""
            self.steps.append({"step": f"(helper) submit {key}", "error": f"submit failed: {exc}"})

    def wait(self, key: str, states: set[str], timeout: float = 120) -> str:
        if not self.jobs.get(key):
            return ""
        deadline = time.time() + timeout
        state = ""
        while time.time() < deadline:
            state = self.helper("jobstate", self.jobs[key])
            if state in states:
                return state
            time.sleep(2)
        return state


def scenario(r: Runner) -> None:
    c = r.client
    r.step("version", "get_version")
    r.step("validate binary", "validate_slurm_binary")
    r.step("list tres", "list_tres")
    r.step("list clusters", "list_clusters")
    r.step("list accounts (initial)", "list_resources")

    r.step("create org account", "create_resource", "org_a", "Org A", "org_a")
    r.step("create alloc_a under org_a", "create_resource", "alloc_a", "Alloc A", "org_a", "org_a")
    r.step("create alloc_b under org_a", "create_resource", "alloc_b", "Alloc B", "org_a", "org_a")
    r.step("create alloc_a again", "create_resource", "alloc_a", "Alloc A", "org_a", "org_a")
    r.step("get alloc_a", "get_resource", "alloc_a")
    r.step("get missing account", "get_resource", "nope")
    r.step("list accounts", "list_resources")
    r.step("parent of alloc_a", "get_account_parent", "alloc_a")
    r.step("move alloc_b to root", "set_account_parent", "alloc_b", "root")
    r.step("parent of alloc_b", "get_account_parent", "alloc_b")
    r.step("move alloc_b back", "set_account_parent", "alloc_b", "org_a")
    r.step("parent of missing account", "get_account_parent", "nope")
    r.step("alloc_a has users (none)", "account_has_users", "alloc_a")

    r.step("add alice to alloc_a (default)", "create_association", "alice", "alloc_a", "alloc_a")
    r.step("add bob to alloc_a", "create_association", "bob", "alloc_a")
    r.step("add alice to alloc_a again", "create_association", "alice", "alloc_a")
    r.step(
        "add alice to alloc_b / debug",
        "create_association_with_partition",
        "alice",
        "alloc_b",
        "debug",
    )
    r.step(
        "add carol to alloc_b / debug,gpu",
        "create_association_with_partitions",
        "carol",
        "alloc_b",
        ["debug", "gpu"],
        "alloc_b",
    )
    r.step("association alice/alloc_a", "get_association", "alice", "alloc_a")
    r.step("association nobody/alloc_a", "get_association", "nobody", "alloc_a")
    r.step("users of alloc_a", "list_resource_users", "alloc_a")
    r.step("users of alloc_b", "list_resource_users", "alloc_b")
    r.step("alloc_a has users", "account_has_users", "alloc_a")
    r.step("accounts of alice", "list_user_accounts", "alice")
    r.step("accounts of nobody", "list_user_accounts", "nobody")
    r.step("default account of alice", "get_user_default_account", "alice")
    r.step("default account of nobody", "get_user_default_account", "nobody")
    r.step("set alice default to alloc_b", "set_user_default_account", "alice", "alloc_b")
    r.step("default account of alice (after)", "get_user_default_account", "alice")

    r.step(
        "set alloc_a limits",
        "set_resource_limits",
        "alloc_a",
        {"cpu": 1000, "mem": 2000, "gres/gpu": 10},
    )
    r.step("alloc_a limits", "get_resource_limits", "alloc_a")
    r.step(
        "set alice limits in alloc_a", "set_resource_user_limits", "alloc_a", "alice", {"cpu": 500}
    )
    r.step("user limits in alloc_a", "get_resource_user_limits", "alloc_a")
    r.step(
        "set GrpTRES on alloc_b",
        "set_account_limits",
        "alloc_b",
        "GrpTRES",
        {"cpu": 64, "gres/gpu": 2},
    )
    r.step("set MaxTRESPerJob on alloc_b", "set_account_limits", "alloc_b", "MaxTRES", {"cpu": 32})
    r.step("alloc_b limits", "get_account_limits", "alloc_b")
    r.step("set fairshare", "set_account_fairshare", "alloc_a", 10)
    r.step("fairshare", "get_account_fairshare", "alloc_a")
    r.step("set GrpSubmitJobs", "set_account_grp_submit_jobs", "alloc_a", 50)
    r.step("alloc_a limits (after submit cap)", "get_account_limits", "alloc_a")

    r.step("qos exists (no)", "qos_exists", "q_slow")
    r.step(
        "create qos",
        "create_qos",
        "q_slow",
        flags=["DenyOnLimit"],
        grp_tres="cpu=128,gres/gpu=4",
        max_jobs=5,
        max_submit=10,
        max_wall="1-00:00:00",
        min_tres_per_job="cpu=1",
    )
    r.step("qos exists (yes)", "qos_exists", "q_slow")
    r.step("create qos again", "create_qos", "q_slow")
    r.step("set qos GrpTRESMins", "set_qos_grp_tres_mins", "q_slow", {"cpu": 6000, "gres/gpu": 60})
    r.step("qos GrpTRESMins", "get_qos_grp_tres_mins", "q_slow")
    r.step("qos GrpTRESMins of missing qos", "get_qos_grp_tres_mins", "nope")
    r.step("alloc_a qos (initial)", "get_current_account_qos", "alloc_a")
    r.step("alloc_a default qos (initial)", "get_current_account_default_qos", "alloc_a")
    r.step("add q_slow to alloc_a", "add_account_qos", "alloc_a", "q_slow")
    r.step("alloc_a qos", "get_current_account_qos", "alloc_a")
    r.step("set alloc_a default qos", "set_account_default_qos", "alloc_a", "q_slow")
    r.step("alloc_a default qos", "get_current_account_default_qos", "alloc_a")
    r.step("set alloc_b qos list", "set_account_qos_list", "alloc_b", ["normal", "q_slow"])
    r.step("set alloc_b qos + default", "set_account_qos", "alloc_b", "q_slow", "q_slow")
    r.step("alloc_b qos", "get_current_account_qos", "alloc_b")
    r.step("alloc_b default qos", "get_current_account_default_qos", "alloc_b")
    r.step(
        "add bob to alloc_b with qos",
        "create_association_with_qos",
        "bob",
        "alloc_b",
        ["normal", "q_slow"],
        "normal",
        ["debug"],
    )
    r.step("users of alloc_b (after qos assoc)", "list_resource_users", "alloc_b")
    r.step("reset q_slow raw usage", "reset_qos_raw_usage", "q_slow")
    r.step("reset alloc_a raw usage", "reset_raw_usage", "alloc_a")

    # Jobs: two short ones that finish, one long one that keeps running.
    r.submit("short1", "alice", "alloc_a", "debug", 3)
    r.submit("short2", "bob", "alloc_a", "debug", 3)
    r.submit("long", "bob", "alloc_a", "debug", 600)
    r.wait("short1", {"COMPLETED"})
    r.wait("short2", {"COMPLETED"})
    r.wait("long", {"RUNNING"})
    time.sleep(6)  # accounting catches up
    r.step("active jobs of bob in alloc_a", "list_active_user_jobs", "alloc_a", "bob")
    r.step("active jobs of alice in alloc_a", "list_active_user_jobs", "alloc_a", "alice")
    r.step("usage report alloc_a", "get_usage_report", ["alloc_a"])
    now = dt.datetime.now(dt.timezone.utc)
    r.step(
        "historical usage this month",
        "get_historical_usage_report",
        ["alloc_a"],
        now.year,
        now.month,
    )
    r.step("usage report of unused account", "get_usage_report", ["alloc_b"])
    r.step("user alice exists", "check_user_exists", "alice")
    r.step("user ghost exists", "check_user_exists", "ghost")
    r.step("cancel bob's jobs in alloc_a", "cancel_active_user_jobs", "alloc_a", "bob")
    r.wait("long", {"CANCELLED", "CANCELLED by 0"}, timeout=60)
    r.step("active jobs of bob after cancel", "list_active_user_jobs", "alloc_a", "bob")

    # Maintenance reservations.
    if MaintenanceReservation is not None:
        r.step("partition nodes", "list_partition_nodes")
        r.step("reservations (none)", "list_reservations")
        start = (now + dt.timedelta(days=2)).replace(minute=0, second=0, microsecond=0)
        maint = MaintenanceReservation(
            name="waldur-maint-t1",
            start=start,
            end=start + dt.timedelta(hours=2),
            nodes="node[001-002]",
            flags=["MAINT"],
            users=["root"],
        )
        r.step("create node reservation", "create_reservation", maint)
        part = MaintenanceReservation(
            name="waldur-maint-t2",
            start=start + dt.timedelta(days=1),
            end=start + dt.timedelta(days=1, hours=3),
            partition="gpu",
            flags=["MAINT"],
            users=["root"],
        )
        r.step("create partition reservation", "create_reservation", part)
        allnodes = MaintenanceReservation(
            name="waldur-maint-t3",
            start=start + dt.timedelta(days=3),
            end=start + dt.timedelta(days=3, hours=1),
            nodes="ALL",
            flags=["MAINT"],
            users=["root"],
        )
        r.step("create all-nodes reservation", "create_reservation", allnodes)
        r.step("create duplicate reservation", "create_reservation", maint)
        r.step("reservations", "list_reservations")
        moved = dataclasses.replace(maint, end=maint.end + dt.timedelta(hours=1))
        r.step("extend node reservation", "update_reservation", moved, {"end"})
        renodes = dataclasses.replace(maint, nodes="node[001-003]", end=moved.end)
        r.step("change reservation nodes", "update_reservation", renodes, {"nodes"})
        r.step("reservations (after update)", "list_reservations")
        r.step("delete node reservation", "delete_reservation", "waldur-maint-t1")
        r.step("delete missing reservation", "delete_reservation", "waldur-maint-t1")
        r.step("delete partition reservation", "delete_reservation", "waldur-maint-t2")
        r.step("delete all-nodes reservation", "delete_reservation", "waldur-maint-t3")
        r.step("reservations (end)", "list_reservations")

    # Teardown.
    r.step("remove bob from alloc_a", "delete_association", "bob", "alloc_a")
    r.step("remove bob from alloc_a again", "delete_association", "bob", "alloc_a")
    r.step("users of alloc_a (after)", "list_resource_users", "alloc_a")
    r.step("remove all users from alloc_b", "delete_all_users_from_account", "alloc_b")
    r.step("users of alloc_b (after)", "list_resource_users", "alloc_b")
    r.step("delete user carol", "delete_user", "carol")
    r.step("delete alloc_b", "delete_resource", "alloc_b")
    r.step("delete alloc_b again", "delete_resource", "alloc_b")
    r.step("delete qos", "delete_qos", "q_slow")
    r.step("list accounts (end)", "list_resources")
    del c


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=("cli", "rest"), required=True)
    p.add_argument("--bin", required=True)
    p.add_argument("--rest-url")
    p.add_argument("--api-version", default="v0.0.45")
    p.add_argument("--out", required=True)
    a = p.parse_args()
    raw: list = []
    os.environ["PARITY_RAW_LOG"] = a.out + ".cli.jsonl"
    if a.mode == "cli":
        client = SlurmClient(TRES, slurm_bin_path=a.bin, cluster_name=CLUSTER)
    else:
        os.environ.setdefault("SLURM_JWT", "parity")
        client = SlurmRestClient(
            TRES,
            {
                "url": a.rest_url,
                "api_version": a.api_version,
                "token_env": "SLURM_JWT",
                "username": "root",
            },
            cluster_name=CLUSTER,
            slurm_bin_path=a.bin,
        )

        def log_response(response: httpx.Response) -> None:
            response.read()
            try:
                body = response.json()
            except ValueError:
                body = response.text[:500]
            raw.append(
                {
                    "request": f"{response.request.method} {response.request.url.path}",
                    "query": str(response.request.url.query, "utf-8")
                    if response.request.url.query
                    else "",
                    "body": json.loads(response.request.content)
                    if response.request.content
                    else None,
                    "status": response.status_code,
                    "response": body,
                }
            )

        client._http.event_hooks["response"] = [log_response]
    # Expose the PATH so ``id`` (not a SLURM command) resolves to our wrapper.
    os.environ["PATH"] = a.bin + os.pathsep + os.environ["PATH"]
    runner = Runner(client, a.bin, raw)
    scenario(runner)
    with open(a.out, "w") as f:
        json.dump({"mode": a.mode, "steps": runner.steps, "jobs": runner.jobs}, f, indent=1)


if __name__ == "__main__":
    main()
