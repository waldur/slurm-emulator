"""slurmctld endpoints: nodes, partitions, jobs, cancel, stubs."""

from datetime import datetime

from emulator.core.database import Job, SlurmDatabase


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
        body = restd.get("/slurm/v0.0.46/nodes/", headers=auth_headers).json()
        nodes = body["nodes"]
        assert len(nodes) == 100
        names = {n["name"] for n in nodes}
        assert {"node001", "node004", "node005", "node100"} <= names
        node001 = next(n for n in nodes if n["name"] == "node001")
        assert node001["partitions"] == ["debug"]
        assert node001["state"] == ["IDLE"]
        assert body["last_update"]["set"] is True

    def test_single_node(self, restd, auth_headers):
        node = restd.get("/slurm/v0.0.46/node/node007", headers=auth_headers).json()["nodes"][0]
        assert node["partitions"] == ["compute"]

    def test_unknown_node_warns(self, restd, auth_headers):
        body = restd.get("/slurm/v0.0.46/node/node999", headers=auth_headers).json()
        assert body["nodes"] == []
        assert body["warnings"]

    def test_partitions(self, restd, auth_headers):
        partitions = restd.get("/slurm/v0.0.46/partitions/", headers=auth_headers).json()[
            "partitions"
        ]
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
        body = restd.get("/slurm/v0.0.46/jobs/", headers=auth_headers).json()
        assert len(body["jobs"]) == 1
        job = body["jobs"][0]
        assert job["job_id"] == 42
        assert job["job_state"] == ["RUNNING"]
        assert job["user_name"] == "alice"
        assert "last_backfill" in body

    def test_jobs_state(self, restd, auth_headers, state_env, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
        _seed_job()
        jobs = restd.get("/slurm/v0.0.46/jobs/state/", headers=auth_headers).json()["jobs"]
        assert jobs == [{"job_id": 42, "state": ["RUNNING"]}]

    def test_unknown_job_404_with_envelope(self, restd, auth_headers):
        response = restd.get("/slurm/v0.0.46/job/9999", headers=auth_headers)
        assert response.status_code == 404
        body = response.json()
        assert body["errors"][0]["error_number"] == 2017  # ESLURM_INVALID_JOB_ID
        assert body["errors"][0]["error"] == "Invalid job id specified"

    def test_cancel_job_persists(self, restd, auth_headers, state_env):
        _seed_job()
        response = restd.delete("/slurm/v0.0.46/job/42", headers=auth_headers)
        assert response.status_code == 200
        status = response.json()["status"][0]
        assert status["job_id"]["number"] == 42
        assert status["error_code"] == 0

        database = SlurmDatabase()
        database.load_state()
        job = database.get_job("42")
        assert job.state == "CANCELLED"
        assert job.end_time is not None

    def test_cancel_unknown_job(self, restd, auth_headers):
        response = restd.delete("/slurm/v0.0.46/job/9999", headers=auth_headers)
        assert response.status_code == 404
        assert response.json()["errors"][0]["error_number"] == 2017


class TestStubs:
    def test_reservations_empty(self, restd, auth_headers):
        body = restd.get("/slurm/v0.0.46/reservations/", headers=auth_headers).json()
        assert body["reservations"] == []
        assert "last_update" in body

    def test_licenses_empty(self, restd, auth_headers):
        body = restd.get("/slurm/v0.0.46/licenses/", headers=auth_headers).json()
        assert body["licenses"] == []

    def test_diag(self, restd, auth_headers):
        statistics = restd.get("/slurm/v0.0.46/diag/", headers=auth_headers).json()["statistics"]
        assert statistics["server_thread_count"] == 1

    def test_conf(self, restd, auth_headers):
        config = restd.get("/slurm/v0.0.46/conf", headers=auth_headers).json()["config"]
        assert config["slurm_version"] == "26.11.0"
        assert config["cluster_name"] == "default"

    def test_shares(self, restd, auth_headers, state_env):
        database = SlurmDatabase()
        database.load_state()
        database.add_account("proj1", "Project 1", "org1")
        database.add_association("alice", "proj1")
        database.save_state()

        shares = restd.get("/slurm/v0.0.46/shares", headers=auth_headers).json()["shares"]["shares"]
        names = {(s["name"], tuple(s["type"])) for s in shares}
        assert ("proj1", ("account",)) in names
        assert ("alice", ("user",)) in names
