"""Node allocation, queueing, TIMEOUT, node states, reservations and accounting.

Covers ``emulator/core/cluster_model.py`` and the scheduler/REST/sacct code
that uses it:

* submit honours the job_desc resource request
  (slurm://src/plugins/data_parser/v0.0.45/parsers.c#JOB_DESC_MSG@26.05+);
* jobs get concrete nodes and wait with ``Resources`` when none are free
  (slurm://src/common/job_state_reason.c#job_state_reason_string);
* a job outliving its limit ends TIMEOUT (slurm://src/slurmctld/job_mgr.c#_job_timed_out);
* nodes report IDLE/MIXED/ALLOCATED, DRAIN/DOWN, ``cpu_load`` x100
  (slurm://src/slurmd/slurmd/get_mach_stat.c#get_cpu_load);
* reservations render as RESERVATION_INFO and block overlapping jobs
  (slurm://src/plugins/data_parser/v0.0.45/parsers.c#RESERVATION_INFO@26.05+,
  slurm://src/slurmctld/reservation.c#job_test_resv);
* accounting keeps submit/limit/nodes/CPU time and a batch step with MaxRSS
  (slurm://src/plugins/data_parser/v0.0.45/parsers.c#STEP_TRES_REQ_MAX@26.05+,
  slurm://src/sacct/print.c#PRINT_MAXRSS).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from emulator.commands.sacct import SacctEmulator
from emulator.core import cluster_model
from emulator.core.database import Job, SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine
from emulator.slurm_version import current

V = current().api_version
T0 = datetime(2026, 10, 17, 9, 0, 0)


@pytest.fixture
def sim(state_env, monkeypatch):
    """Simulated job clock at T0: 60 s to start, 1 h runs, no admission checks."""
    monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DELAY", "60")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DURATION", "3600")
    monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "none")
    for name in (
        cluster_model.NODE_STATES_ENV,
        cluster_model.BACKGROUND_LOAD_ENV,
        cluster_model.RESERVATIONS_ENV,
        "SLURM_EMULATOR_JOB_EXECUTE",
        "SLURM_EMULATOR_TIME_FOLLOW_WALL",
    ):
        monkeypatch.delenv(name, raising=False)
    set_time(T0)
    return state_env


def set_time(when: datetime) -> None:
    TimeEngine().set_time(when)


def submit(restd, headers, **job):
    body = restd.post(f"/slurm/{V}/job/submit", headers=headers, json={"job": job}).json()
    assert body["errors"] == [], body["errors"]
    return body["job_id"]


def ctld_jobs(restd, headers):
    jobs = restd.get(f"/slurm/{V}/jobs/", headers=headers).json()["jobs"]
    return {j["job_id"]: j for j in jobs}


def nodes_by_name(restd, headers):
    return {n["name"]: n for n in restd.get(f"/slurm/{V}/nodes/", headers=headers).json()["nodes"]}


# --- pure helpers ---


class TestHostlist:
    def test_compress_and_expand_round_trip(self):
        names = ["node005", "node006", "node007", "node010", "node012"]
        assert cluster_model.compress_hostlist(names) == "node[005-007,010,012]"
        assert cluster_model.expand_hostlist("node[005-007,010,012]") == names

    def test_single_host_stays_bare(self):
        assert cluster_model.compress_hostlist(["node001"]) == "node001"
        assert cluster_model.expand_hostlist("node001,node[003-004]") == [
            "node001",
            "node003",
            "node004",
        ]


class TestJobRequest:
    def test_tasks_times_cpus_per_task(self):
        job = Job(
            job_id="1",
            account="a",
            user="u",
            state="PENDING",
            node_count=2,
            tasks=8,
            cpus_per_task=4,
            memory_per_node=16000,
        )
        req = cluster_model.job_request(job)
        assert (req.node_count, req.total_cpus, req.cpus_per_node, req.mem_per_node) == (
            2,
            32,
            16,
            16000,
        )

    def test_one_task_per_node_by_default(self):
        # num_tasks falls back to the node count, cpus_per_task to 1
        # (slurm://src/slurmctld/job_mgr.c#_pack_default_job_details).
        req = cluster_model.job_request(
            Job(job_id="1", account="a", user="u", state="PENDING", node_count=3)
        )
        assert (req.total_cpus, req.cpus_per_node) == (3, 1)
        assert req.mem_per_node == cluster_model.DEF_MEM_PER_CPU_MB

    def test_exclusive_takes_whole_nodes(self):
        req = cluster_model.job_request(
            Job(job_id="1", account="a", user="u", state="PENDING", node_count=3, exclusive=True)
        )
        assert req.total_cpus == 3 * cluster_model.NODE_CPUS
        assert req.mem_per_node == cluster_model.NODE_MEM_MB

    def test_tasks_beyond_nodes_add_nodes(self):
        req = cluster_model.job_request(
            Job(job_id="1", account="a", user="u", state="PENDING", node_count=1, tasks=100)
        )
        assert req.node_count == 2


class TestSyntheticEfficiency:
    def test_deterministic_and_in_range(self):
        values = [cluster_model.synthetic_efficiency(i) for i in range(400)]
        assert values == [cluster_model.synthetic_efficiency(i) for i in range(400)]
        assert all(0.05 <= cpu <= 0.99 and 0.0 <= mem <= 1.0 for cpu, mem in values)
        good = sum(1 for cpu, _ in values if cpu >= 0.6)
        poor = sum(1 for cpu, _ in values if 0.1 <= cpu <= 0.4)
        assert good > 0.6 * len(values)
        assert poor > 0.1 * len(values)

    def test_environment_overrides(self):
        env = {"SLURM_EMULATOR_CPU_EFFICIENCY": "0.3", "SLURM_EMULATOR_MEM_FRACTION": "0.5"}
        assert cluster_model.synthetic_efficiency(7, env) == (0.3, 0.5)


class TestStateFileCompat:
    def test_old_job_without_new_fields_loads(self, state_env):
        old = {
            "jobs": {
                "5": {
                    "job_id": "5",
                    "account": "a",
                    "user": "u",
                    "state": "RUNNING",
                    "submit_time": "2024-01-01T00:00:00",
                    "node_count": 1,
                }
            },
            "usage_records": [
                {
                    "account": "a",
                    "user": "u",
                    "node_hours": 1.0,
                    "billing_units": 1.0,
                    "timestamp": "2024-01-01T01:00:00",
                    "period": "2024-Q1",
                    "job_id": 4,
                }
            ],
        }
        (state_env / "db.json").write_text(json.dumps(old))
        db = SlurmDatabase()
        db.load_state()
        assert db.jobs["5"].node_list == [] and db.jobs["5"].state_reason == "None"
        record = db.usage_records[0]
        assert record.start_time is None and record.node_count == 0
        assert db.reservations == {} and db.sched_cursor is None


# --- submit + allocation + queueing ---


class TestSubmitAndAllocate:
    def test_submit_reads_resource_request(self, restd, auth_headers, sim):
        jid = submit(
            restd,
            auth_headers,
            name="r",
            partition="debug",
            nodes="2-4",
            tasks=8,
            cpus_per_task=4,
            memory_per_node={"set": True, "infinite": False, "number": 16000},
            time_limit={"set": True, "infinite": False, "number": 90},
        )
        job = SlurmDatabase()
        job.load_state()
        stored = job.jobs[str(jid)]
        assert (stored.node_count, stored.tasks, stored.cpus_per_task) == (2, 8, 4)
        assert (stored.memory_per_node, stored.time_limit) == (16000, 90)

    def test_minimum_nodes_and_plain_int_nodes(self, restd, auth_headers, sim):
        a = submit(restd, auth_headers, name="a", partition="compute", minimum_nodes=3)
        b = submit(restd, auth_headers, name="b", partition="compute", nodes=2)
        db = SlurmDatabase()
        db.load_state()
        assert db.jobs[str(a)].node_count == 3
        assert db.jobs[str(b)].node_count == 2

    def test_running_jobs_get_distinct_nodes(self, restd, auth_headers, sim):
        a = submit(restd, auth_headers, name="a", partition="compute", nodes="4", shared=["none"])
        b = submit(restd, auth_headers, name="b", partition="compute", nodes="2", shared=["none"])
        set_time(T0 + timedelta(minutes=2))
        jobs = ctld_jobs(restd, auth_headers)
        assert jobs[a]["nodes"] == "node[005-008]"
        assert jobs[b]["nodes"] == "node[009-010]"
        assert jobs[a]["job_resources"]["nodes"] == {
            "count": 4,
            "list": "node[005-008]",
            "allocation": [],
        }
        assert jobs[a]["cpus"]["number"] == 4 * cluster_model.NODE_CPUS

    def test_cpus_from_tasks_and_shared_node(self, restd, auth_headers, sim):
        jid = submit(restd, auth_headers, name="s", partition="debug", tasks=2, cpus_per_task=8)
        set_time(T0 + timedelta(minutes=2))
        job = ctld_jobs(restd, auth_headers)[jid]
        assert job["cpus"]["number"] == 16
        node = nodes_by_name(restd, auth_headers)["node001"]
        assert node["state"] == ["MIXED"]
        assert node["alloc_cpus"] == 16
        assert node["alloc_idle_cpus"] == cluster_model.NODE_CPUS - 16

    def test_queue_waits_for_resources_then_starts(self, restd, auth_headers, sim):
        first = submit(
            restd, auth_headers, name="big", partition="debug", nodes="4", shared=["none"]
        )
        second = submit(
            restd, auth_headers, name="next", partition="debug", nodes="2", shared=["none"]
        )
        set_time(T0 + timedelta(minutes=5))
        jobs = ctld_jobs(restd, auth_headers)
        assert jobs[first]["job_state"] == ["RUNNING"]
        assert jobs[second]["job_state"] == ["PENDING"]
        assert jobs[second]["state_reason"] == "Resources"
        assert jobs[second]["nodes"] == ""

        set_time(T0 + timedelta(hours=1, minutes=5))
        jobs = ctld_jobs(restd, auth_headers)
        assert jobs[first]["job_state"] == ["COMPLETED"]
        assert jobs[second]["job_state"] == ["RUNNING"]
        assert jobs[second]["state_reason"] == "None"
        # It started the moment the first job freed its nodes.
        assert jobs[second]["start_time"]["number"] == int(
            (T0 + timedelta(seconds=60 + 3600)).timestamp()
        )

    def test_drained_and_down_nodes_are_skipped(self, restd, auth_headers, sim, monkeypatch):
        monkeypatch.setenv(
            cluster_model.NODE_STATES_ENV, "node001=DRAIN:Bad DIMM;node002=DOWN:Not responding"
        )
        jid = submit(restd, auth_headers, name="d", partition="debug", nodes="2", shared=["none"])
        set_time(T0 + timedelta(minutes=2))
        assert ctld_jobs(restd, auth_headers)[jid]["nodes"] == "node[003-004]"
        more = submit(restd, auth_headers, name="e", partition="debug", nodes="1", shared=["none"])
        set_time(T0 + timedelta(minutes=4))
        assert ctld_jobs(restd, auth_headers)[more]["state_reason"] == "Resources"


class TestTimeout:
    def test_run_past_limit_ends_timeout_at_limit(self, restd, auth_headers, sim):
        jid = submit(
            restd,
            auth_headers,
            name="t",
            partition="debug",
            time_limit=30,
            environment=["SLURM_EMULATOR_RUN_SECONDS=7200"],
        )
        set_time(T0 + timedelta(hours=3))
        job = ctld_jobs(restd, auth_headers)[jid]
        assert job["job_state"] == ["TIMEOUT"]
        assert job["end_time"]["number"] - job["start_time"]["number"] == 30 * 60

    def test_run_seconds_per_job(self, restd, auth_headers, sim):
        jid = submit(
            restd,
            auth_headers,
            name="q",
            partition="debug",
            environment=["SLURM_EMULATOR_RUN_SECONDS=120"],
        )
        set_time(T0 + timedelta(minutes=5))
        job = ctld_jobs(restd, auth_headers)[jid]
        assert job["job_state"] == ["COMPLETED"]
        assert job["end_time"]["number"] - job["start_time"]["number"] == 120

    def test_running_job_reports_expected_end(self, restd, auth_headers, sim):
        jid = submit(restd, auth_headers, name="x", partition="debug", time_limit=120)
        set_time(T0 + timedelta(minutes=2))
        job = ctld_jobs(restd, auth_headers)[jid]
        assert job["end_time"]["number"] - job["start_time"]["number"] == 120 * 60


# --- nodes ---


class TestNodes:
    def test_idle_node_shape(self, restd, auth_headers, sim):
        node = nodes_by_name(restd, auth_headers)["node004"]
        assert node["state"] == ["IDLE"]
        assert node["alloc_cpus"] == 0 and node["alloc_memory"] == 0
        assert isinstance(node["cpu_load"], int) and node["cpu_load"] < 50
        assert node["free_mem"]["set"] is True
        assert 0 < node["free_mem"]["number"] <= cluster_model.NODE_MEM_MB
        assert node["tres_used"] == ""

    def test_allocated_node_load_follows_efficiency(self, restd, auth_headers, sim):
        jid = submit(
            restd,
            auth_headers,
            name="l",
            partition="debug",
            shared=["none"],
            environment=["SLURM_EMULATOR_CPU_EFFICIENCY=0.5", "SLURM_EMULATOR_MEM_FRACTION=0.25"],
        )
        set_time(T0 + timedelta(minutes=2))
        node_name = ctld_jobs(restd, auth_headers)[jid]["nodes"]
        node = nodes_by_name(restd, auth_headers)[node_name]
        assert node["state"] == ["ALLOCATED"]
        assert node["alloc_cpus"] == cluster_model.NODE_CPUS
        # 64 CPUs x 50% = load 32 -> 3200, +-4% jitter
        assert 3000 <= node["cpu_load"] <= 3400
        assert node["tres_used"].startswith("cpu=64,mem=512G")
        used = cluster_model.NODE_MEM_MB - node["free_mem"]["number"]
        assert abs(used - (0.25 * cluster_model.NODE_MEM_MB + cluster_model.OS_MEM_MB)) < 2

    def test_overrides_drain_and_down(self, restd, auth_headers, sim, monkeypatch):
        monkeypatch.setenv(
            cluster_model.NODE_STATES_ENV, "node012=DRAIN:Bad DIMM;node030=DOWN:Not responding"
        )
        nodes = nodes_by_name(restd, auth_headers)
        assert nodes["node012"]["state"] == ["IDLE", "DRAIN"]
        assert nodes["node012"]["reason"] == "Bad DIMM"
        assert nodes["node030"]["state"] == ["DOWN"]
        assert nodes["node030"]["reason"] == "Not responding"
        assert nodes["node030"]["free_mem"]["set"] is False

    def test_background_load_fraction_and_rotation(self, restd, auth_headers, sim, monkeypatch):
        monkeypatch.setenv(cluster_model.BACKGROUND_LOAD_ENV, "compute=0.5")
        nodes = nodes_by_name(restd, auth_headers)
        busy = {n for n, v in nodes.items() if v["state"] == ["ALLOCATED"]}
        assert len(busy) == 48  # half of node[005-100]
        assert all(nodes[n]["cpu_load"] > 3000 for n in busy)
        assert not busy & {"node001", "node002", "node003", "node004"}
        # Same window -> same set; 15 minutes later -> another set.
        again = {
            n for n, v in nodes_by_name(restd, auth_headers).items() if v["state"] == ["ALLOCATED"]
        }
        assert again == busy
        set_time(T0 + timedelta(minutes=15))
        later = {
            n for n, v in nodes_by_name(restd, auth_headers).items() if v["state"] == ["ALLOCATED"]
        }
        assert later != busy and len(later) == 48

    def test_background_nodes_are_not_allocated(self, restd, auth_headers, sim, monkeypatch):
        monkeypatch.setenv(cluster_model.BACKGROUND_LOAD_ENV, "debug=0.75")
        jid = submit(restd, auth_headers, name="b", partition="debug", nodes="2")
        set_time(T0 + timedelta(minutes=2))
        assert ctld_jobs(restd, auth_headers)[jid]["state_reason"] == "Resources"

    def test_unset_env_keeps_idle_cluster(self, restd, auth_headers, sim):
        states = {tuple(n["state"]) for n in nodes_by_name(restd, auth_headers).values()}
        assert states == {("IDLE",)}


# --- reservations ---


RESV = "maint:ALL:+72h:12h:MAINT:users=root;train:debug:+2h:4h::users=alice,bob"


class TestReservations:
    def test_empty_without_config(self, restd, auth_headers, sim):
        body = restd.get(f"/slurm/{V}/reservations/", headers=auth_headers).json()
        assert body["reservations"] == []

    def test_shape_and_fixed_times(self, restd, auth_headers, sim, monkeypatch):
        monkeypatch.setenv(cluster_model.RESERVATIONS_ENV, RESV)
        body = restd.get(f"/slurm/{V}/reservations/", headers=auth_headers).json()
        by_name = {r["name"]: r for r in body["reservations"]}
        maint = by_name["maint"]
        assert maint["flags"] == ["MAINT"]
        assert maint["node_list"] == "node[001-100]" and maint["node_count"] == 100
        assert maint["partition"] == "" and maint["users"] == "root"
        assert maint["start_time"] == {
            "set": True,
            "infinite": False,
            "number": int((T0 + timedelta(hours=72)).timestamp()),
        }
        assert maint["end_time"]["number"] - maint["start_time"]["number"] == 12 * 3600
        train = by_name["train"]
        assert (train["partition"], train["node_list"], train["users"]) == (
            "debug",
            "node[001-004]",
            "alice,bob",
        )
        # Resolved once: a later request does not move the window.
        set_time(T0 + timedelta(hours=10))
        again = restd.get(f"/slurm/{V}/reservation/maint", headers=auth_headers).json()
        assert again["reservations"][0]["start_time"] == maint["start_time"]
        # Names match case-insensitively; an unknown one is an error
        # (slurm://src/slurmrestd/plugins/openapi/slurmctld/reservations.c#"Unable to find reservation").
        upper = restd.get(f"/slurm/{V}/reservation/MAINT", headers=auth_headers).json()
        assert upper["reservations"][0]["name"] == "maint"
        missing = restd.get(f"/slurm/{V}/reservation/nope", headers=auth_headers)
        assert missing.status_code == 422 and "reservations" not in missing.json()
        assert missing.json()["errors"][0]["description"] == "Unable to find reservation nope"

    def test_single_reservation_with_none_in_the_system(self, restd, auth_headers, sim):
        resp = restd.get(f"/slurm/{V}/reservation/nope", headers=auth_headers)
        body = resp.json()
        assert resp.status_code == 200 and "reservations" not in body
        assert body["errors"] == [
            {
                "description": "Unable to query reservations",
                "error_number": 0,
                "error": "No error",
                "source": "slurm_load_reservations()",
            }
        ]

    def test_maint_blocks_jobs_whose_limit_overlaps(self, restd, auth_headers, sim, monkeypatch):
        monkeypatch.setenv(cluster_model.RESERVATIONS_ENV, "maint:ALL:+3h:2h:MAINT")
        long_job = submit(restd, auth_headers, name="long", partition="compute", time_limit=240)
        short_job = submit(restd, auth_headers, name="short", partition="compute", time_limit=60)
        set_time(T0 + timedelta(minutes=2))
        jobs = ctld_jobs(restd, auth_headers)
        assert jobs[short_job]["job_state"] == ["RUNNING"]
        assert jobs[long_job]["job_state"] == ["PENDING"]
        assert jobs[long_job]["state_reason"] == "ReqNodeNotAvail"
        assert jobs[long_job]["state_description"] == cluster_model.MAINT_DESC
        # After the window it runs.
        set_time(T0 + timedelta(hours=5, minutes=1))
        assert ctld_jobs(restd, auth_headers)[long_job]["job_state"] == ["RUNNING"]

    def test_nodes_in_active_maint_are_flagged(self, restd, auth_headers, sim, monkeypatch):
        monkeypatch.setenv(cluster_model.RESERVATIONS_ENV, "maint:node[001-002]:now:1h:MAINT")
        nodes = nodes_by_name(restd, auth_headers)
        assert nodes["node001"]["state"] == ["IDLE", "RESERVED", "MAINTENANCE"]
        assert nodes["node001"]["reservation"] == "maint"
        assert nodes["node003"]["state"] == ["IDLE"]

    def test_job_requesting_reservation_runs_inside_it(self, restd, auth_headers, sim, monkeypatch):
        monkeypatch.setenv(cluster_model.RESERVATIONS_ENV, "train:debug:+1h:4h")
        jid = submit(restd, auth_headers, name="in", partition="debug", reservation="train")
        set_time(T0 + timedelta(minutes=5))
        job = ctld_jobs(restd, auth_headers)[jid]
        assert job["state_reason"] == "Reservation" and job["resv_name"] == "train"
        set_time(T0 + timedelta(hours=1, minutes=1))
        assert ctld_jobs(restd, auth_headers)[jid]["job_state"] == ["RUNNING"]

    def test_unknown_reservation_refused(self, restd, auth_headers, sim):
        body = restd.post(
            f"/slurm/{V}/job/submit",
            headers=auth_headers,
            json={"job": {"name": "x", "reservation": "nope"}},
        ).json()
        assert body["errors"][0]["error_number"] == 2053
        assert body["errors"][0]["description"] == "Requested reservation is invalid"


# --- accounting ---


class TestAccounting:
    def _run_one(self, restd, auth_headers):
        jid = submit(
            restd,
            auth_headers,
            name="acct",
            partition="compute",
            nodes="2",
            time_limit=90,
            memory_per_node=200000,
            shared=["none"],
            environment=["SLURM_EMULATOR_CPU_EFFICIENCY=0.5", "SLURM_EMULATOR_MEM_FRACTION=0.25"],
        )
        set_time(T0 + timedelta(hours=2))
        restd.get(f"/slurm/{V}/jobs/", headers=auth_headers)
        jobs = restd.get(f"/slurmdb/{V}/job/{jid}", headers=auth_headers).json()["jobs"]
        return jid, jobs[0]

    def test_slurmdb_job_carries_real_values(self, restd, auth_headers, sim):
        jid, job = self._run_one(restd, auth_headers)
        t = job["time"]
        assert t["submission"] == int(T0.timestamp())
        assert t["start"] - t["submission"] == 60
        assert t["end"] - t["start"] == 3600 == t["elapsed"]
        assert t["limit"] == {"set": True, "infinite": False, "number": 90}
        cpu_seconds = 3600 * 128 * 0.5
        assert t["total"]["seconds"] == int(cpu_seconds)
        assert abs(t["user"]["seconds"] + t["system"]["seconds"] - cpu_seconds) <= 1
        assert job["allocation_nodes"] == 2
        assert job["nodes"] == "node[005-006]"
        requested = {e["type"]: e["count"] for e in job["tres"]["requested"]}
        assert requested == {"cpu": 128, "mem": 400000, "node": 2, "billing": 128}
        assert job["required"]["memory_per_node"]["number"] == 200000
        step = job["steps"][0]
        assert step["step"]["id"] == f"{jid}.batch" and step["step"]["name"] == "batch"
        requested = step["tres"]["requested"]
        peaks = {e["type"]: e for e in requested["max"]}
        assert peaks["mem"]["count"] == int(0.25 * 200000 * 1024) * 1024
        assert (peaks["mem"]["task"], peaks["mem"]["node"]) == (0, "node005")
        # Every gathered TRES is in every list, in TRES id order.
        assert [e["type"] for e in requested["max"]] == [
            "cpu",
            "mem",
            "energy",
            "fs",
            "vmem",
            "pages",
        ]
        assert [e["type"] for e in requested["average"]] == [e["type"] for e in requested["max"]]
        # CPU time in ms: 2 whole nodes x 50% for the 1 h run.
        assert peaks["cpu"]["count"] == 2 * cluster_model.NODE_CPUS * 3600 * 1000 // 2
        assert step["statistics"]["energy"]["consumed"]["set"] is True
        assert step["tres"]["consumed"]["total"] == [
            {"type": "fs", "name": "disk", "id": 6, "count": 0}
        ]
        assert step["nodes"]["list"] == ["node005"]

    def test_sacct_columns_agree(self, restd, auth_headers, sim):
        jid, job = self._run_one(restd, auth_headers)
        db = SlurmDatabase()
        db.load_state()
        out = SacctEmulator(db, TimeEngine()).handle_command(
            [
                "-j",
                str(jid),
                "-P",
                "-o",
                "Submit,Start,Timelimit,NNodes,NodeList,AllocCPUS,ReqMem,TotalCPU,MaxRSS,Elapsed",
            ]
        )
        row = dict(zip(out.splitlines()[0].split("|"), out.splitlines()[1].split("|")))
        assert row["Submit"] == "2026-10-17T09:00:00"
        assert row["Start"] == "2026-10-17T09:01:00"
        assert row["Timelimit"] == "01:30:00"
        assert (row["NNodes"], row["NodeList"], row["AllocCPUS"]) == ("2", "node[005-006]", "128")
        assert row["ReqMem"] == "400000M"  # not a multiple of 512 -> stays in MB
        assert row["TotalCPU"] == "2-16:00:00"  # 230400 s
        # 51200000K is a multiple of 512 -> one more step (CONVERT_NUM_UNIT_EXACT)
        assert row["MaxRSS"] == "50000M"
        assert row["Elapsed"] == "01:00:00"

    def test_cancelled_pending_job_has_no_nodes(self, restd, auth_headers, sim):
        jid = submit(restd, auth_headers, name="c", partition="debug")
        restd.delete(f"/slurm/{V}/job/{jid}", headers=auth_headers)
        job = restd.get(f"/slurmdb/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
        assert job["state"]["current"] == ["CANCELLED"]
        assert job["allocation_nodes"] == 0 and job["nodes"] == "None assigned"
        assert job["steps"] == []

    def test_legacy_record_renders_as_before(self, restd, auth_headers, sim):
        db = SlurmDatabase()
        db.load_state()
        db.add_usage_record(
            UsageRecord(
                account="p",
                user="u",
                node_hours=2.0,
                billing_units=2.0,
                timestamp=T0,
                period="2026-Q4",
            )
        )
        db.save_state()
        job = restd.get(f"/slurmdb/{V}/jobs/", headers=auth_headers).json()["jobs"][0]
        assert job["nodes"] == "node001" and job["steps"] == []
        assert job["time"]["submission"] == job["time"]["start"]
        assert job["time"]["limit"]["infinite"] is True


class TestFollowWall:
    def test_clock_tracks_wall_time(self, state_env, monkeypatch):
        TimeEngine().set_time(datetime(2024, 1, 1))
        monkeypatch.setenv("SLURM_EMULATOR_TIME_FOLLOW_WALL", "1")
        assert abs((TimeEngine().get_current_time() - datetime.now()).total_seconds()) < 5
