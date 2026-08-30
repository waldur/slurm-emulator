"""Energy TRES: power model, record plumbing, sacct fields, API seeding, scenario.

References: the ``energy`` TRES is joules in real slurmdbd
(slurm://src/common/slurmdb_defs.h#TRES_ENERGY); ``sacct ConsumedEnergyRaw`` reads
it from ``tres_alloc_str`` (slurm://src/sacct/print.c#PRINT_CONSUMED_ENERGY_RAW)
and ``ConsumedEnergy`` scales it with
slurm://src/common/slurm_protocol_api.c#convert_num_unit2.
"""

from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from emulator.api.emulator_server import EmulatorServer
from emulator.commands.sacct import SacctEmulator, _convert_num_unit
from emulator.commands.sreport import SreportEmulator
from emulator.core import energy
from emulator.core.database import Job, SlurmDatabase, UsageRecord
from emulator.core.scheduler import _ensure_usage_record
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import UsageSimulator
from emulator.scenarios.scenario_registry import (
    REGULAR_ACCESS_ENERGY_JOULES,
    REGULAR_ACCESS_ENERGY_USERS,
    ActionType,
    ScenarioRegistry,
)

NOW = datetime(2024, 3, 15, 12, 0, 0)


@pytest.fixture(autouse=True)
def _clean_power_env(monkeypatch):
    for name in (
        "SLURM_EMULATOR_NODE_POWER_W",
        "SLURM_EMULATOR_GPU_POWER_W",
        "SLURM_EMULATOR_PARTITION_POWER_W",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def env(tmp_path):
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    te = TimeEngine(start_time=NOW)
    te.set_time(NOW)
    return db, te, UsageSimulator(te, db)


class TestPowerModel:
    def test_defaults(self):
        # 1 Nh at 500 W = 1.8 MJ; GPUs add 300 W each (4 GPU-hours here).
        assert energy.energy_joules(1.0) == 1_800_000
        assert energy.energy_joules(1.0, gpu_hours=4) == 1_800_000 + 4 * 3600 * 300

    def test_env_overrides(self, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_NODE_POWER_W", "1000")
        monkeypatch.setenv("SLURM_EMULATOR_GPU_POWER_W", "100")
        assert energy.energy_joules(2.0) == 7_200_000
        assert energy.energy_joules(1.0, gpu_hours=1) == 3_600_000 + 360_000

    def test_partition_table(self, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_PARTITION_POWER_W", "gpu=900, debug=250,broken")
        assert energy.node_power_w("gpu") == 900
        assert energy.node_power_w("debug") == 250
        assert energy.node_power_w("compute") == energy.DEFAULT_NODE_POWER_W
        assert energy.energy_joules(1.0, partition="gpu") == 900 * 3600

    def test_garbage_env_falls_back(self, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_NODE_POWER_W", "lots")
        assert energy.node_power_w() == energy.DEFAULT_NODE_POWER_W


class TestUsageRecords:
    def test_inject_usage_carries_model_energy_and_partition(self, env):
        db, _, sim = env
        sim.inject_usage("proj", "alice", 2.0)
        rec = db.usage_records[-1]
        assert rec.partition == "compute"
        # 2 Nh * 500 W + 8 GPU-hours * 300 W
        assert rec.raw_tres["energy"] == 2 * 3600 * 500 + 8 * 3600 * 300
        assert "energy" in db.tres_types

    def test_partition_selects_power(self, env, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_PARTITION_POWER_W", "gpu=900")
        db, _, sim = env
        sim.inject_usage("proj", "alice", 1.0, partition="gpu")
        assert db.usage_records[-1].partition == "gpu"
        assert db.usage_records[-1].raw_tres["energy"] == 900 * 3600 + 4 * 3600 * 300

    def test_explicit_energy_bypasses_model(self, env):
        db, _, sim = env
        sim.inject_usage("proj", "alice", 1.0, energy_joules=123)
        assert db.usage_records[-1].raw_tres["energy"] == 123

    def test_state_round_trip(self, env):
        db, _te, sim = env
        sim.inject_usage("proj", "alice", 1.0, partition="gpu", energy_joules=77)
        db.save_state()
        db2 = SlurmDatabase()
        db2.state_file = db.state_file
        db2.load_state()
        rec = db2.usage_records[-1]
        assert (rec.partition, rec.raw_tres["energy"]) == ("gpu", 77)

    def test_old_state_without_partition_loads(self, env):
        db, _, _ = env
        rec = db._deserialize_usage_record(
            {
                "account": "a",
                "user": "u",
                "node_hours": 1.0,
                "billing_units": 1.0,
                "timestamp": NOW.isoformat(),
                "period": "2024-Q1",
                "raw_tres": {},
                "cluster": "default",
            }
        )
        assert rec.partition == "compute"

    def test_completed_job_record_uses_power_model(self, env):
        db, _, _ = env
        start = NOW - timedelta(hours=2)
        db.jobs["7"] = Job(
            job_id="7",
            account="proj",
            user="alice",
            state="COMPLETED",
            start_time=start,
            end_time=start + timedelta(hours=1),
            partition="gpu",
            node_count=2,
        )
        assert _ensure_usage_record(db, db.jobs["7"], NOW)
        rec = db.usage_records[-1]
        assert rec.node_hours == 2.0
        assert rec.partition == "gpu"
        assert rec.raw_tres["energy"] == 2 * 3600 * 500  # no GRES on submitted jobs


class TestSacctEnergyFields:
    def test_consumed_energy_raw_and_scaled(self, env):
        db, te, sim = env
        sim.inject_usage("proj", "alice", 1.0, energy_joules=7_200_000_000)
        sacct = SacctEmulator(db, te)
        out = sacct.handle_command(["--format=ConsumedEnergyRaw,ConsumedEnergy", "-n", "-P"])
        assert out == "7200000000|7.20G"
        out = sacct.handle_command(
            ["--format=ConsumedEnergyRaw,ConsumedEnergy", "-n", "-P", "--noconvert"]
        )
        assert out == "7200000000|7200000000"

    def test_convert_num_unit_matches_c(self):
        assert _convert_num_unit(0) == "0"
        assert _convert_num_unit(999) == "999"
        assert _convert_num_unit(1000) == "1K"
        assert _convert_num_unit(1_800_000) == "1.80M"
        assert _convert_num_unit(2_000_000_000) == "2G"

    def test_record_without_energy_prints_zero(self, env):
        db, te, _ = env
        db.add_usage_record(
            UsageRecord(
                account="proj",
                user="alice",
                node_hours=1.0,
                billing_units=1.0,
                timestamp=NOW,
                period="2024-Q1",
            )
        )
        out = SacctEmulator(db, te).handle_command(
            ["--format=ConsumedEnergyRaw,ConsumedEnergy", "-n", "-P"]
        )
        assert out == "0|0"

    def test_column_widths(self, env):
        db, te, sim = env
        sim.inject_usage("proj", "alice", 1.0, energy_joules=5)
        out = SacctEmulator(db, te).handle_command(["--format=ConsumedEnergy,ConsumedEnergyRaw"])
        lines = out.splitlines()
        assert lines[0] == "ConsumedEnergy ConsumedEnergyRaw "
        assert lines[1] == "-------------- ----------------- "
        assert lines[2] == "             5                 5 "

    def test_tres_string_still_omits_energy(self, env):
        db, te, sim = env
        sim.inject_usage("proj", "alice", 2.0)
        out = SacctEmulator(db, te).handle_command(["--format=ReqTRES", "-n", "-P"])
        assert out == "cpu=64,mem=512G,node=1,billing=64,gres/gpu=4"


def _sreport_energy(server, account, month_start, month_end):
    sreport = SreportEmulator(server.database, server.time_engine)
    out = sreport.handle_command(
        [
            "cluster",
            "AccountUtilizationByUser",
            f"start={month_start}",
            f"end={month_end}",
            "-T",
            "energy",
            "-t",
            "Seconds",
            "-P",
            "-n",
            f"accounts={account}",
        ]
    )
    rows = [line.split("|") for line in out.splitlines()]
    return {(r[1], r[2]): int(r[5]) for r in rows}


class TestSubmitReportSeeding:
    @pytest.fixture
    def server(self, state_env):
        server = EmulatorServer()
        server.time_engine.set_time(NOW)
        server.database.add_account("proj", "Project", "org")
        return server

    def test_aggregate_energy_is_reported_exactly(self, server):
        client = TestClient(server.app)
        resp = client.post(
            "/api/submit-report",
            json={
                "resource_id": "proj",
                "usage": {"billing": 120, "energy": 7_200_000_000},
                "billing_period": "2024-03",
                "date": "2024-03-10T00:00:00",
            },
        )
        assert resp.status_code == 200, resp.text
        totals = _sreport_energy(server, "proj", "2024-03-01", "2024-04-01")
        assert totals[("proj", "")] == 7_200_000_000
        assert totals[("proj", "aggregate_user")] == 7_200_000_000

    def test_per_user_energy(self, server):
        client = TestClient(server.app)
        resp = client.post(
            "/api/submit-report",
            json={
                "resource_id": "proj",
                "usage": {"billing": 30},
                "users": {
                    "alice": {"billing": 20, "energy": 1_000},
                    "bob": {"billing": 10, "GRES/gpu": 4, "energy": 500},
                    "carol": {"energy": 250},
                },
                "billing_period": "2024-03",
                "date": "2024-03-10T00:00:00",
                "partition": "gpu",
            },
        )
        assert resp.status_code == 200, resp.text
        totals = _sreport_energy(server, "proj", "2024-03-01", "2024-04-01")
        assert totals == {
            ("proj", ""): 1_750,
            ("proj", "alice"): 1_000,
            ("proj", "bob"): 500,
            ("proj", "carol"): 250,
        }
        assert {r.partition for r in server.database.usage_records} == {"gpu"}

    def test_report_without_energy_uses_power_model(self, server):
        client = TestClient(server.app)
        client.post(
            "/api/submit-report",
            json={
                "resource_id": "proj",
                "usage": {"billing": 2},
                "billing_period": "2024-03",
                "date": "2024-03-10T00:00:00",
            },
        )
        totals = _sreport_energy(server, "proj", "2024-03-01", "2024-04-01")
        assert totals[("proj", "")] == energy.energy_joules(2.0, gpu_hours=8)


class TestScenario:
    def test_registered_and_seeds_known_totals(self, env):
        db, te, sim = env
        registry = ScenarioRegistry()
        scenario = registry.get_scenario("regular_access_energy")
        assert scenario is not None
        assert scenario.get_total_actions() == 1 + len(REGULAR_ACCESS_ENERGY_USERS) + 1

        db.add_account("regular_access", "RA", "org")
        for step in scenario.steps:
            if step.time_point:
                te.set_time(step.time_point)
            for action in step.actions:
                if action.type == ActionType.USAGE_INJECT:
                    sim.inject_usage(
                        action.parameters["account"],
                        action.parameters["user"],
                        action.parameters["amount"],
                        partition=action.parameters.get("partition"),
                        energy_joules=action.parameters.get("energy"),
                    )
                    assert "energy=" in action.get_cli_command()

        sreport = SreportEmulator(db, te)
        out = sreport.handle_command(
            [
                "cluster",
                "AccountUtilizationByUser",
                "start=2024-03-01",
                "end=2024-04-01",
                "-T",
                "energy",
                "-t",
                "Seconds",
                "-P",
                "-n",
                "accounts=regular_access",
            ]
        )
        rows = {line.split("|")[2]: int(line.split("|")[5]) for line in out.splitlines()}
        assert rows[""] == REGULAR_ACCESS_ENERGY_JOULES
        for user, _nh, _part, joules in REGULAR_ACCESS_ENERGY_USERS:
            assert rows[user] == joules
        assert sum(v for k, v in rows.items() if k) == rows[""]
