"""Job AdminComment and per-job injection: storage, write paths, REST, scenario.

References: ``admin_comment`` is a free-form job field in slurmdbd
(slurm://slurm/slurmdb.h#slurmdb_job_rec_t.admin_comment) and slurmctld
(slurm://slurm/slurm.h#job_info_t.admin_comment). It is written by
``scontrol update`` while the job is active — super users only, refused
once finished (slurm://src/slurmctld/job_mgr.c#_update_job) — or by
``sacctmgr modify job`` afterwards
(slurm://src/sacctmgr/job_functions.c#sacctmgr_modify_job), whose value goes
through slurm://src/sacctmgr/common.c#strip_quotes. slurmrestd dumps it as
``comment.administrator`` (slurm://src/plugins/data_parser/v0.0.45/parsers.c#JOB@26.05+)
and accepts it on submit only from a super user
(slurm://src/slurmctld/job_mgr.c#validate_job_create_req).
"""

import json
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from emulator.api.emulator_server import EmulatorServer
from emulator.api.slurmrestd.schemas import ctld_job_to_dict, dbd_job_to_dict
from emulator.api.ssh import server as ssh_server
from emulator.api.ui.routes import _run_scenario_headless
from emulator.cli.cmd_cli import SlurmEmulatorCmd
from emulator.commands.dispatcher import SlurmEmulator
from emulator.commands.sacct import SacctEmulator, _elapsed_time
from emulator.commands.sacctmgr import SacctmgrEmulator, _strip_quotes
from emulator.core.database import Job, SlurmDatabase, UsageRecord
from emulator.core.scheduler import _ensure_usage_record
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import UsageSimulator
from emulator.scenarios.scenario_registry import (
    EESSI_ACCOUNT,
    EESSI_SOFTWARE_JOBS,
    ActionType,
    ScenarioRegistry,
    eessi_admin_comment,
)
from emulator.slurm_version import current

V = current().api_version
NOW = datetime(2024, 4, 2, 9, 0, 0)
DOC = '{"eessi":{"version":"2023.06","cpu_target":"x86_64/amd/zen3","modules":["R/4.3.2-gfbf-2023a"]}}'
SITE_AGENT_ARGS = [
    "--format=JobID,Account,User,AllocTRES,Elapsed,ConsumedEnergyRaw,AdminComment",
    "--parsable2",
    "--noheader",
    "--allocations",
    "--allusers",
    "--starttime=2024-03-01",
    "--endtime=2024-04-01",
]


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


def _sacct(db, te, *extra):
    return SacctEmulator(db, te).handle_command([*SITE_AGENT_ARGS, *extra])


class TestStorage:
    def test_persists_and_old_state_loads(self, env, tmp_path):
        db, _te, sim = env
        sim.inject_job("proj", "alice", 1.0, admin_comment=DOC, end_time=datetime(2024, 3, 5))
        db.save_state()
        state = json.loads(db.state_file.read_text())
        assert state["usage_records"][0]["admin_comment"] == DOC

        # A pre-AdminComment state file has neither key.
        del state["usage_records"][0]["admin_comment"]
        state["jobs"] = {"7": {"job_id": "7", "account": "proj", "user": "a", "state": "PENDING"}}
        db.state_file.write_text(json.dumps(state))
        fresh = SlurmDatabase()
        fresh.state_file = db.state_file
        fresh.load_state()
        assert fresh.usage_records[0].admin_comment == ""
        assert fresh.jobs["7"].admin_comment == ""

    def test_completed_job_carries_comment_into_accounting(self, env):
        db, te, _sim = env
        job = Job(
            job_id="42",
            account="proj",
            user="alice",
            state="COMPLETED",
            start_time=datetime(2024, 4, 2, 8),
            end_time=datetime(2024, 4, 2, 9),
            admin_comment=DOC,
        )
        assert _ensure_usage_record(db, job, te.get_current_time())
        assert db.usage_records[0].admin_comment == DOC


class TestInjectJob:
    def test_one_row_per_job_with_exact_alloc_and_elapsed(self, env):
        db, te, sim = env
        ids = sim.inject_jobs(
            [
                {
                    "account": "proj",
                    "user": "alice",
                    "cpus": 16,
                    "gpus": 2,
                    "elapsed_hours": 2.5,
                    "partition": "gpu",
                    "energy": 123_456,
                    "admin_comment": DOC,
                    "end": "2024-03-05T10:00:00",
                },
                {"account": "proj", "user": "bob", "cpus": 4, "elapsed_hours": 0.5},
            ]
        )
        assert ids == [1, 2]
        # Second job ends "now" (2024-04-02), outside the March window.
        assert _sacct(db, te).splitlines() == [
            f"1|proj|alice|cpu=16,mem=128G,node=1,billing=16,gres/gpu=2|02:30:00|123456|{DOC}"
        ]
        out = SacctEmulator(db, te).handle_command(
            ["-n", "-P", "-o", "JobID,Partition,AllocTRES,Elapsed,End,AdminComment", "-j", "2"]
        )
        assert out == "2|compute|cpu=4,mem=32G,node=1,billing=4|00:30:00|2024-04-02T09:00:00|"

    def test_energy_defaults_to_node_share_plus_gpus(self, env):
        db, _te, sim = env
        sim.inject_job("proj", "alice", 2.0, cpus=32, gpus=1)
        # Half a 500 W node for 2 h + one 300 W GPU for 2 h.
        assert db.usage_records[0].raw_tres["energy"] == 1_800_000 + 2_160_000

    def test_rejects_counts_that_do_not_round_trip(self, env):
        _db, _te, sim = env
        with pytest.raises(ValueError, match="round-trip"):
            sim.inject_job("proj", "alice", 0.25, cpus=7)

    def test_batch_is_validated_before_any_write(self, env):
        db, _te, sim = env
        good = {"account": "proj", "user": "alice", "elapsed_hours": 1.0}
        with pytest.raises(ValueError, match="cpus must be"):
            sim.inject_jobs([good, {**good, "cpus": 0}])
        with pytest.raises(TypeError):
            sim.inject_jobs([good, {**good, "bogus": 1}])
        assert db.usage_records == []

    def test_creates_account_user_and_association(self, env):
        db, _te, sim = env
        sim.inject_job("newproj", "dora", 1.0)
        assert db.get_account("newproj") is not None
        assert db.get_association("dora", "newproj", cluster=db.current_cluster) is not None


class TestControlApi:
    @pytest.fixture
    def client(self, state_env):
        server = EmulatorServer()
        server.time_engine.set_time(NOW)
        return server, TestClient(server.app)

    def test_inject_jobs_returns_ids(self, client):
        server, http = client
        body = {
            "jobs": [
                {
                    "account": "proj",
                    "user": "alice",
                    "cpus": 8,
                    "elapsed_hours": 3,
                    "admin_comment": DOC,
                    "end": "2024-03-10T12:00:00",
                },
                {
                    "account": "proj",
                    "user": "bob",
                    "elapsed_hours": 1,
                    "end": "2024-03-11T12:00:00",
                },
            ]
        }
        resp = http.post("/api/inject-jobs", json=body)
        assert resp.status_code == 200
        assert resp.json() == {"status": "success", "job_ids": [1, 2]}
        rows = _sacct(server.database, server.time_engine).splitlines()
        assert rows == [
            f"1|proj|alice|cpu=8,mem=64G,node=1,billing=8|03:00:00|675000|{DOC}",
            "2|proj|bob|cpu=1,mem=8G,node=1,billing=1|01:00:00|28125|",
        ]

    def test_bad_job_is_400_and_writes_nothing(self, client):
        server, http = client
        resp = http.post(
            "/api/inject-jobs",
            json={"jobs": [{"account": "p", "user": "u", "cpus": 7, "elapsed_hours": 0.25}]},
        )
        assert resp.status_code == 400
        assert server.database.usage_records == []


class TestSacctmgrModifyJob:
    @pytest.fixture
    def seeded(self, env):
        db, te, sim = env
        sim.inject_job("proj", "alice", 2.0, end_time=datetime(2024, 3, 5, 12))
        sim.inject_job("proj", "bob", 1.0, end_time=datetime(2024, 3, 6, 12))
        return db, te, SacctmgrEmulator(db, te)

    def test_sets_comment_and_lists_modified_jobs(self, seeded):
        db, _te, mgr = seeded
        out = mgr.handle_command(
            ["-i", "modify", "job", "where", "jobid=2", "set", "AdminComment=EESSI1:abc"]
        )
        assert (out, mgr.exit_code) == (
            " Modified jobs...\n  2 submitted at 2024-03-06T11:00:00",
            0,
        )
        assert [r.admin_comment for r in db.usage_records] == ["", "EESSI1:abc"]

    def test_quotes_become_backticks(self, seeded):
        db, te, mgr = seeded
        mgr.handle_command(["-i", "modify", "job", "1", "set", 'AdminComment={"a":"b"}'])
        assert db.usage_records[0].admin_comment == "{`a`:`b`}"
        assert "|{`a`:`b`}" in _sacct(db, te)

    def test_strip_quotes_port(self):
        assert _strip_quotes('"quoted value" tail') == "quoted value"
        assert _strip_quotes("  it's  ") == "it`s"
        assert _strip_quotes('{"k":1}') == "{`k`:1}"

    def test_filters_by_user(self, seeded):
        db, _te, mgr = seeded
        mgr.handle_command(["-i", "modify", "job", "where", "users=alice", "set", "AdminComment=x"])
        assert [r.admin_comment for r in db.usage_records] == ["x", ""]

    def test_no_match(self, seeded, capsys):
        _db, _te, mgr = seeded
        out = mgr.handle_command(["-i", "modify", "job", "jobid=99", "set", "AdminComment=x"])
        assert out == " Error with request: Data has not changed since time specified"
        assert mgr.exit_code == 1
        assert capsys.readouterr().out == " Modified jobs...\n"

    def test_nothing_to_set(self, seeded):
        _db, _te, mgr = seeded
        out = mgr.handle_command(["modify", "job", "where", "jobid=1"])
        assert (out, mgr.exit_code) == (" You didn't give me anything to set", 1)

    def test_unknown_condition(self, seeded):
        _db, _te, mgr = seeded
        out = mgr.handle_command(["modify", "job", "where", "Qos=x", "set", "AdminComment=y"])
        assert out.startswith(" Unknown condition: Qos=x")
        assert mgr.exit_code == 1


class TestScontrolUpdate:
    @pytest.fixture
    def emu(self, state_env, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "none")
        emu = SlurmEmulator()
        emu.database.add_job(Job(job_id="5", account="proj", user="alice", state="RUNNING"))
        emu.database.add_job(Job(job_id="6", account="proj", user="alice", state="COMPLETED"))
        return emu

    def test_super_user_sets_running_job(self, emu):
        assert ssh_server._scontrol(emu, ["update", "job=5", f"AdminComment={DOC}"], "root") == (
            "",
            "",
            0,
        )
        assert emu.database.get_job("5").admin_comment == DOC
        show, _, _ = ssh_server._scontrol(emu, ["show", "job", "5"])
        assert f"   AdminComment={DOC}\n" in show

    def test_regular_user_denied(self, emu):
        out = ssh_server._scontrol(emu, ["update", "JobId=5", "AdminComment=x"], "alice")
        assert out == ("", "Access/permission denied for job 5\n", 1)

    def test_finished_job_refused_before_permission_check(self, emu):
        out = ssh_server._scontrol(emu, ["update", "job=6", "AdminComment=x"], "alice")
        assert out == ("", "Job has already finished for job 6\n", 1)

    def test_unknown_job_and_field(self, emu):
        assert ssh_server._scontrol(emu, ["update", "job=77", "AdminComment=x"], "root")[1] == (
            "Invalid job id specified for job 77\n"
        )
        _out, err, code = ssh_server._scontrol(emu, ["update", "job=5", "Comment=x"], "root")
        assert (err, code) == (
            "Update of this parameter is not supported: Comment=x\nRequest aborted\n",
            1,
        )


class TestSlurmrestd:
    def test_dbd_job_comment_block(self):
        record = UsageRecord(
            account="proj",
            user="alice",
            node_hours=1.0,
            billing_units=1.0,
            timestamp=NOW,
            period="2024-Q2",
            job_id=1,
            admin_comment=DOC,
        )
        assert dbd_job_to_dict(record)["comment"] == {
            "administrator": DOC,
            "job": "",
            "system": "",
        }

    def test_ctld_job_admin_comment(self):
        job = Job(job_id="3", account="proj", user="alice", state="RUNNING", admin_comment=DOC)
        assert ctld_job_to_dict(job)["admin_comment"] == DOC

    def test_submit_with_admin_comment(self, restd, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "none")
        body = {"job": {"script": "#!/bin/bash\ntrue", "admin_comment": DOC, "user_name": "alice"}}
        as_root = {"X-SLURM-USER-TOKEN": "t", "X-SLURM-USER-NAME": "root"}
        resp = restd.post(f"/slurm/{V}/job/submit", json=body, headers=as_root)
        assert resp.status_code == 200
        job_id = resp.json()["job_id"]
        job = restd.get(f"/slurm/{V}/job/{job_id}", headers=as_root).json()["jobs"][0]
        assert job["admin_comment"] == DOC

    def test_submit_admin_comment_needs_super_user(self, restd, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "none")
        body = {"job": {"script": "#!/bin/bash\ntrue", "admin_comment": "x"}}
        headers = {"X-SLURM-USER-TOKEN": "t", "X-SLURM-USER-NAME": "alice"}
        resp = restd.post(f"/slurm/{V}/job/submit", json=body, headers=headers)
        assert resp.status_code == 422
        assert resp.json()["errors"][0]["error_number"] == 2002


class TestScenario:
    def test_registered_with_one_action_per_job(self):
        scenario = ScenarioRegistry().get_scenario("eessi_software_usage")
        assert scenario is not None
        actions = [a for step in scenario.steps for a in step.actions]
        injects = [a for a in actions if a.type == ActionType.JOB_INJECT]
        assert len(injects) == len(EESSI_SOFTWARE_JOBS)
        assert injects[0].get_cli_command().startswith("# POST /api/inject-jobs ")

    def test_comment_format(self):
        assert eessi_admin_comment(None) == ""
        with_host = json.loads(
            eessi_admin_comment(["R/4.3.2-gfbf-2023a"], host_cpu="x86_64/amd/zen4")
        )
        assert with_host["eessi"]["host_cpu"] == "x86_64/amd/zen4"
        assert any(job.host_cpu for job in EESSI_SOFTWARE_JOBS)
        assert {job.state for job in EESSI_SOFTWARE_JOBS} >= {
            "COMPLETED",
            "TIMEOUT",
            "FAILED",
            "OUT_OF_MEMORY",
            "CANCELLED",
        }
        doc = json.loads(eessi_admin_comment(["GROMACS/2024.1-foss-2023b"]))
        assert doc == {
            "eessi": {
                "version": "2023.06",
                "cpu_target": "x86_64/amd/zen3",
                "modules": ["GROMACS/2024.1-foss-2023b"],
            }
        }

    def _check_rows(self, db, te):
        out = SacctEmulator(db, te).handle_command(
            [
                "--format=JobID,Account,User,AllocTRES,Elapsed,ConsumedEnergyRaw,"
                "State,TotalCPU,AdminComment",
                *SITE_AGENT_ARGS[1:],
                f"--accounts={EESSI_ACCOUNT}",
            ]
        )
        rows = [r.split("|") for r in out.splitlines()]
        assert len(rows) == len(EESSI_SOFTWARE_JOBS)
        for row, job in zip(rows, EESSI_SOFTWARE_JOBS):
            assert row[1:3] == [EESSI_ACCOUNT, job.user]
            assert f"cpu={job.cpus}," in row[3]
            assert (f"gres/gpu={job.gpus}" in row[3]) == bool(job.gpus)
            elapsed = int(job.elapsed_hours * 3600)
            assert row[4] == f"{elapsed // 3600:02d}:{elapsed // 60 % 60:02d}:00"
            assert int(row[5]) == job.energy
            assert row[6].split(" by ")[0] == job.state
            assert row[8] == job.admin_comment
        by_state = {row[6]: row[7] for row in rows}
        # 64 CPUs x 12 h x 0.97 = 2,681,856 s; 32 x 0.25 h x 0.4 = 11,520 s.
        assert by_state["TIMEOUT"] == "31-00:57:36"
        assert by_state["FAILED"] == "03:12:00"
        assert by_state["CANCELLED by 1000"] == "01:48:00"
        # The module-less job carries an explicit 1234.567 s.
        assert rows[3][7] == "20:34.567"

    def test_ui_runner(self, state_env):
        server = EmulatorServer()
        scenario = server.scenario_registry.get_scenario("eessi_software_usage")
        _run_scenario_headless(server, scenario)
        self._check_rows(server.database, server.time_engine)

    def test_cli_runner(self, state_env):
        cli = SlurmEmulatorCmd()
        cli.do_scenario_run("eessi_software_usage")
        self._check_rows(cli.database, cli.time_engine)
        # Re-running cleans the account first instead of duplicating jobs.
        cli.do_scenario_run("eessi_software_usage")
        self._check_rows(cli.database, cli.time_engine)


class TestTotalCpuAndState:
    """TotalCPU and State columns of injected jobs.

    ``TotalCPU`` via sacct's slurm://src/sacct/print.c#_elapsed_time,
    ``State`` via slurm://src/sacct/print.c#PRINT_STATE and the base-state
    names of slurm://src/common/slurm_protocol_defs.c#job_state_string.
    """

    @pytest.mark.parametrize(
        ("seconds", "expected"),
        [
            (0, "00:00:00"),
            (59, "00:00:59"),
            (754, "00:12:34"),
            (754.5, "12:34.500"),
            (754.0004, "00:12:34"),  # under a millisecond: no fraction
            (3599.999, "59:59.999"),
            (3600, "01:00:00"),
            (5025.25, "01:23:45"),  # from an hour on, fractions are dropped
            (86399, "23:59:59"),
            (86400, "1-00:00:00"),
            (2_681_856, "31-00:57:36"),
        ],
    )
    def test_elapsed_time_port(self, seconds, expected):
        assert _elapsed_time(seconds) == expected

    def _row(self, db, te, fields):
        return SacctEmulator(db, te).handle_command(["-n", "-P", "-o", fields, "-j", "1"])

    def test_default_is_fully_busy(self, env):
        db, te, sim = env
        sim.inject_job("proj", "alice", 2.0, cpus=4)
        assert self._row(db, te, "TotalCPU,State") == "08:00:00|COMPLETED"

    def test_efficiency_and_explicit_seconds(self, env):
        db, te, sim = env
        sim.inject_job("proj", "alice", 2.0, cpus=4, cpu_efficiency=0.25)
        assert db.usage_records[0].total_cpu_seconds == 7200
        sim.inject_job("proj", "alice", 1.0, cpus=4, total_cpu_seconds=61.25)
        out = SacctEmulator(db, te).handle_command(["-n", "-P", "-o", "TotalCPU", "-j", "2"])
        assert out == "01:01.250"

    def test_efficiency_validation(self, env):
        _db, _te, sim = env
        with pytest.raises(ValueError, match="not both"):
            sim.inject_job("p", "u", 1.0, cpu_efficiency=0.5, total_cpu_seconds=10)
        with pytest.raises(ValueError, match="between 0 and 1"):
            sim.inject_job("p", "u", 1.0, cpu_efficiency=1.5)
        with pytest.raises(ValueError, match="negative"):
            sim.inject_job("p", "u", 1.0, total_cpu_seconds=-1)

    @pytest.mark.parametrize(
        "state",
        [
            "COMPLETED",
            "FAILED",
            "TIMEOUT",
            "OUT_OF_MEMORY",
            "NODE_FAIL",
            "PREEMPTED",
            "BOOT_FAIL",
            "DEADLINE",
        ],
    )
    def test_finished_states_print_verbatim(self, env, state):
        db, te, sim = env
        sim.inject_job("proj", "alice", 1.0, state=state.lower())
        assert self._row(db, te, "State") == state

    def test_cancelled_names_the_requester(self, env):
        db, te, sim = env
        sim.inject_job("proj", "alice", 1.0, state="CANCELLED")
        assert self._row(db, te, "State") == "CANCELLED by 1000"

    def test_unknown_or_active_state_refused(self, env):
        db, _te, sim = env
        for bad in ("RUNNING", "CANCELLED by 0", "bogus"):
            with pytest.raises(ValueError, match="state must be one of"):
                sim.inject_jobs([{"account": "p", "user": "u", "elapsed_hours": 1, "state": bad}])
        assert db.usage_records == []

    def test_prefix_t_still_timelimit(self, env):
        db, te, sim = env
        sim.inject_job("proj", "alice", 1.0)
        assert self._row(db, te, "T") == "UNLIMITED"
        assert self._row(db, te, "To") == "01:00:00"

    def test_old_state_file_loads_without_total_cpu(self, env):
        db, _te, sim = env
        sim.inject_job("proj", "alice", 1.0, cpu_efficiency=0.5)
        db.save_state()
        state = json.loads(db.state_file.read_text())
        del state["usage_records"][0]["total_cpu_seconds"]
        db.state_file.write_text(json.dumps(state))
        fresh = SlurmDatabase()
        fresh.state_file = db.state_file
        fresh.load_state()
        assert fresh.usage_records[0].total_cpu_seconds is None

    def test_slurmdbd_job_cpu_times(self):
        record = UsageRecord(
            account="proj",
            user="alice",
            node_hours=1.0,
            billing_units=1.0,
            timestamp=NOW,
            period="2024-Q2",
            job_id=1,
            total_cpu_seconds=61.25,
        )
        times = dbd_job_to_dict(record)["time"]
        assert times["total"] == {"seconds": 61, "microseconds": 250_000}
        assert times["user"] == times["total"]
        assert times["system"] == {"seconds": 0, "microseconds": 0}

    def test_control_api_accepts_new_fields(self, state_env):
        server = EmulatorServer()
        http = TestClient(server.app)
        job = {"account": "p", "user": "u", "elapsed_hours": 1, "cpus": 2}
        ok = http.post(
            "/api/inject-jobs",
            json={"jobs": [{**job, "state": "TIMEOUT", "cpu_efficiency": 0.5}]},
        )
        assert ok.status_code == 200
        assert server.database.usage_records[0].total_cpu_seconds == 3600
        bad = http.post(
            "/api/inject-jobs",
            json={"jobs": [{**job, "cpu_efficiency": 0.5, "total_cpu_seconds": 5}]},
        )
        assert bad.status_code == 400
