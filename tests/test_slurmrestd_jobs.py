"""slurmdb jobs endpoint: UsageRecords serialized in sacct-equivalent form."""

from datetime import datetime

from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine


def _seed_records():
    """Persist two usage records inside the default sacct time window."""
    time_engine = TimeEngine()
    time_engine.set_time(datetime(2024, 3, 15, 18, 0, 0))

    database = SlurmDatabase()
    database.load_state()
    database.add_account("proj1", "Project 1", "org1")
    database.add_usage_record(
        UsageRecord(
            account="proj1",
            user="alice",
            node_hours=2.0,
            billing_units=2.0,
            timestamp=datetime(2024, 3, 15, 12, 0, 0),
            period="2024-Q1",
            raw_tres={"CPU": 16, "Mem": 64},
        )
    )
    database.add_usage_record(
        UsageRecord(
            account="proj1",
            user="bob",
            node_hours=1.0,
            billing_units=1.0,
            timestamp=datetime(2024, 3, 15, 13, 0, 0),
            period="2024-Q1",
        )
    )
    database.save_state()
    return database


class TestSlurmdbJobs:
    def test_one_job_per_usage_record(self, restd, auth_headers, state_env):
        _seed_records()
        jobs = restd.get("/slurmdb/v0.0.46/jobs/", headers=auth_headers).json()["jobs"]
        assert len(jobs) == 2
        assert {j["user"] for j in jobs} == {"alice", "bob"}
        assert all(j["account"] == "proj1" for j in jobs)

    def test_fields_match_sacct_math(self, restd, auth_headers, state_env):
        _seed_records()
        jobs = restd.get(
            "/slurmdb/v0.0.46/jobs/", headers=auth_headers, params={"users": "alice"}
        ).json()["jobs"]
        job = jobs[0]
        # 2 node-hours → 7200s elapsed, ending at the record timestamp.
        assert job["time"]["elapsed"] == 7200
        assert job["time"]["end"] - job["time"]["start"] == 7200
        # raw_tres CPU=16 over 2h → 8 cpu/hour, like sacct's AllocCPUS.
        requested = {e["type"]: e["count"] for e in job["tres"]["requested"]}
        assert requested["cpu"] == 8
        assert requested["node"] == 1
        assert requested["billing"] == 8
        assert job["state"]["current"] == ["COMPLETED"]
        assert job["exit_code"]["status"] == ["SUCCESS"]
        assert job["nodes"] == "node001"
        assert job["partition"] == "compute"

    def test_account_filter(self, restd, auth_headers, state_env):
        _seed_records()
        jobs = restd.get(
            "/slurmdb/v0.0.46/jobs/", headers=auth_headers, params={"account": "other"}
        ).json()
        assert jobs["jobs"] == []
        assert any("found nothing" in w["description"] for w in jobs["warnings"])

    def test_time_window_filter(self, restd, auth_headers, state_env):
        _seed_records()
        jobs = restd.get(
            "/slurmdb/v0.0.46/jobs/",
            headers=auth_headers,
            params={"start_time": "2024-03-15T12:30:00", "end_time": "2024-03-15T14:00:00"},
        ).json()["jobs"]
        assert len(jobs) == 1
        assert jobs[0]["user"] == "bob"

    def test_unix_timestamp_window(self, restd, auth_headers, state_env):
        _seed_records()
        start = int(datetime(2024, 3, 15, 0, 0, 0).timestamp())
        end = int(datetime(2024, 3, 16, 0, 0, 0).timestamp())
        jobs = restd.get(
            "/slurmdb/v0.0.46/jobs/",
            headers=auth_headers,
            params={"start_time": str(start), "end_time": str(end)},
        ).json()["jobs"]
        assert len(jobs) == 2

    def test_bad_time_spec(self, restd, auth_headers, state_env):
        _seed_records()
        response = restd.get(
            "/slurmdb/v0.0.46/jobs/", headers=auth_headers, params={"start_time": "bogus"}
        )
        assert response.status_code == 400
        assert response.json()["errors"][0]["error_number"] == 9000

    def test_single_job_lookup(self, restd, auth_headers, state_env):
        database = _seed_records()
        job_id = database.usage_records[0].job_id
        jobs = restd.get(f"/slurmdb/v0.0.46/job/{job_id}", headers=auth_headers).json()["jobs"]
        assert len(jobs) == 1
        assert jobs[0]["job_id"] == job_id
