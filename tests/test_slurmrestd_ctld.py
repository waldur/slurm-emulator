"""slurmctld endpoints: nodes, partitions, jobs, cancel, stubs."""

from datetime import datetime

import pytest

from emulator.core.database import Job, SlurmDatabase
from emulator.slurm_version import current

V = current().api_version


@pytest.fixture
def still_running(monkeypatch):
    """Seeded jobs keep running: their emulated run outlasts the test."""
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DURATION", str(10**10))


def _seed_job(state="RUNNING"):
    database = SlurmDatabase()
    database.load_state()
    database.add_account("proj1", "Project 1", "org1")
    database.add_job(
        Job(
            job_id="42",
            account="proj1",
            user="alice",
            state=state,
            submit_time=datetime(2024, 3, 15, 10, 0, 0),
            start_time=datetime(2024, 3, 15, 10, 5, 0),
        )
    )
    database.save_state()
    return database


class TestNodesPartitions:
    def test_nodes_match_sinfo_topology(self, restd, auth_headers):
        body = restd.get(f"/slurm/{V}/nodes/", headers=auth_headers).json()
        nodes = body["nodes"]
        assert len(nodes) == 100
        names = {n["name"] for n in nodes}
        assert {"node001", "node004", "node005", "node100"} <= names
        node001 = next(n for n in nodes if n["name"] == "node001")
        assert node001["partitions"] == ["debug"]
        assert node001["state"] == ["IDLE"]
        assert body["last_update"]["set"] is True

    def test_single_node(self, restd, auth_headers):
        node = restd.get(f"/slurm/{V}/node/node007", headers=auth_headers).json()["nodes"][0]
        assert node["partitions"] == ["compute"]

    def test_unknown_node_warns(self, restd, auth_headers):
        body = restd.get(f"/slurm/{V}/node/node999", headers=auth_headers).json()
        assert body["nodes"] == []
        assert body["warnings"]

    def test_partitions(self, restd, auth_headers):
        partitions = restd.get(f"/slurm/{V}/partitions/", headers=auth_headers).json()["partitions"]
        by_name = {p["name"]: p for p in partitions}
        assert set(by_name) == {"debug", "compute"}
        assert by_name["debug"]["nodes"]["total"] == 4
        assert by_name["compute"]["nodes"]["total"] == 96
        assert by_name["compute"]["nodes"]["configured"] == "node[005-100]"
        assert by_name["debug"]["partition"]["state"] == ["UP"]


class TestCtldJobs:
    def test_list_jobs(self, restd, auth_headers, state_env, monkeypatch):
        # Deterministic clock: reads advance jobs on the simulated clock
        # (default 2024-01-01), so a job seeded RUNNING at 2024-03-15 stays
        # RUNNING and this exercises serialization, not the lifecycle.
        monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
        _seed_job()
        body = restd.get(f"/slurm/{V}/jobs/", headers=auth_headers).json()
        assert len(body["jobs"]) == 1
        job = body["jobs"][0]
        assert job["job_id"] == 42
        assert job["job_state"] == ["RUNNING"]
        assert job["user_name"] == "alice"
        assert "last_backfill" in body

    def test_jobs_state(self, restd, auth_headers, state_env, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
        _seed_job()
        jobs = restd.get(f"/slurm/{V}/jobs/state/", headers=auth_headers).json()["jobs"]
        assert jobs == [{"job_id": 42, "state": ["RUNNING"]}]

    def test_unknown_job_404_with_envelope(self, restd, auth_headers):
        response = restd.get(f"/slurm/{V}/job/9999", headers=auth_headers)
        assert response.status_code == 404
        body = response.json()
        assert body["errors"][0]["error_number"] == 2017  # ESLURM_INVALID_JOB_ID
        assert body["errors"][0]["error"] == "Invalid job id specified"

    def test_cancel_job_persists(self, restd, auth_headers, state_env, still_running):
        # Success sends no per-job entry (slurm://src/slurmctld/job_mgr.c#_add_signal_job_resp).
        _seed_job()
        response = restd.delete(f"/slurm/{V}/job/42", headers=auth_headers)
        assert response.status_code == 200
        assert response.json()["status"] == []

        database = SlurmDatabase()
        database.load_state()
        job = database.get_job("42")
        assert job.state == "CANCELLED"
        assert job.end_time is not None
        assert job.requid == 0  # root

    def test_cancel_verbose_reports_success(self, restd, auth_headers, state_env, still_running):
        _seed_job()
        status = restd.delete(
            f"/slurm/{V}/job/42", headers=auth_headers, params={"flags": "VERBOSE"}
        ).json()["status"]
        assert status == [
            {
                "error": {"string": "No error", "code": 0, "message": ""},
                "step_id": "42",
                "job_id": {"set": True, "infinite": False, "number": 42},
                "federation": {"sibling": ""},
            }
        ]

    def test_cancel_unknown_job(self, restd, auth_headers):
        response = restd.delete(f"/slurm/{V}/job/9999", headers=auth_headers)
        assert response.status_code == 200
        entry = response.json()["status"][0]
        assert entry["error"] == {
            "string": "Invalid job id specified",
            "code": 2017,
            "message": "Invalid job id specified",
        }
        assert entry["job_id"]["set"] is False

    def test_cancel_finished_job_is_already_done(self, restd, auth_headers, state_env, still_running):
        _seed_job()
        restd.delete(f"/slurm/{V}/job/42", headers=auth_headers)
        entry = restd.delete(f"/slurm/{V}/job/42", headers=auth_headers).json()["status"][0]
        assert entry["error"]["code"] == 2021
        assert entry["error"]["string"] == "Job/step already completing or completed"

    def test_cancel_other_users_job_is_denied(self, restd, auth_headers, state_env, still_running):
        _seed_job()
        headers = {**auth_headers, "X-SLURM-USER-NAME": "mallory"}
        entry = restd.delete(f"/slurm/{V}/job/42", headers=headers).json()["status"][0]
        assert entry["error"]["code"] == 2002
        database = SlurmDatabase()
        database.load_state()
        assert database.get_job("42").state != "CANCELLED"

    def test_other_signals_do_not_cancel(self, restd, auth_headers, state_env, still_running):
        _seed_job()
        restd.delete(f"/slurm/{V}/job/42", headers=auth_headers, params={"signal": "SIGUSR1"})
        database = SlurmDatabase()
        database.load_state()
        assert database.get_job("42").state != "CANCELLED"


class TestCancelCatchesUp:
    def test_job_that_should_have_ended_is_already_done(self, restd, auth_headers, state_env):
        # Started in 2024 with the default 8 s run: by now it has completed, so
        # the cancel finds it done instead of recording it CANCELLED.
        _seed_job()
        entry = restd.delete(f"/slurm/{V}/job/42", headers=auth_headers).json()["status"][0]
        assert entry["error"]["code"] == 2021
        database = SlurmDatabase()
        database.load_state()
        assert database.get_job("42").state == "COMPLETED"


class TestStubs:
    def test_reservations_empty(self, restd, auth_headers):
        body = restd.get(f"/slurm/{V}/reservations/", headers=auth_headers).json()
        assert body["reservations"] == []
        assert "last_update" in body

    def test_licenses_empty(self, restd, auth_headers):
        body = restd.get(f"/slurm/{V}/licenses/", headers=auth_headers).json()
        assert body["licenses"] == []

    def test_diag(self, restd, auth_headers):
        statistics = restd.get(f"/slurm/{V}/diag/", headers=auth_headers).json()["statistics"]
        assert statistics["server_thread_count"] == 1

    def test_conf(self, restd, auth_headers):
        config = restd.get(f"/slurm/{V}/conf", headers=auth_headers).json()["config"]
        assert config["slurm_version"] == current().release
        assert config["cluster_name"] == "default"

    def test_shares(self, restd, auth_headers, state_env):
        database = SlurmDatabase()
        database.load_state()
        database.add_account("proj1", "Project 1", "org1")
        database.add_association("alice", "proj1")
        database.save_state()

        shares = restd.get(f"/slurm/{V}/shares", headers=auth_headers).json()["shares"]["shares"]
        names = {(s["name"], tuple(s["type"])) for s in shares}
        assert ("proj1", ("account",)) in names
        assert ("alice", ("user",)) in names
