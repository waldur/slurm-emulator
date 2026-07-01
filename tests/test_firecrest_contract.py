"""FireCREST v2 conformance contract tests.

These assert the slurmrestd emulator answers exactly the requests
FireCREST's ``SlurmRestClient`` makes, in the shapes its ``SlurmJob`` /
association models parse. They mirror
``firecrest-v2/src/lib/scheduler_clients/slurm/slurm_rest_client.py``:

* submit_job   -> POST /slurm/{v}/job/submit, reads ``job_id``
* get_job      -> GET /slurmdb/{v}/job/{id} AND /slurm/{v}/job/{id}, merged
* get_jobs     -> GET /slurmdb/{v}/jobs AND /slurm/{v}/jobs (?account=)
* cancel_job   -> DELETE /slurm/{v}/job/{id} (200 == ok)
* nodes        -> GET /slurm/{v}/nodes
* reservations -> GET /slurm/{v}/reservations
* partitions   -> GET /slurm/{v}/partitions
* accounts     -> GET /slurmdb/{v}/associations?user={name}
* ping         -> GET /slurm/{v}/ping

No FireCREST runtime is required, so this runs in CI on every commit. The
higher-fidelity variant that imports FireCREST's real client lives in
tests/firecrest/test_real_client.py.
"""

from __future__ import annotations

import pytest

from emulator.core.database import SlurmDatabase

V = "v0.0.46"


def _no_val(field: dict) -> bool:
    """A slurm *_NO_VAL struct: {set, infinite, number}."""
    return isinstance(field, dict) and {"set", "infinite", "number"} <= set(field)


@pytest.fixture
def instant_jobs(monkeypatch):
    """Jobs complete immediately so submit->accounting is observable."""
    monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "wall")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DELAY", "0")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DURATION", "0")


@pytest.fixture
def pending_jobs(monkeypatch):
    """Jobs stay PENDING so the pre-run view is observable."""
    monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "wall")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DELAY", "9999")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DURATION", "9999")


def _submit(restd, auth_headers, **job):
    body = {"job": job or {"name": "t"}}
    r = restd.post(f"/slurm/{V}/job/submit", headers=auth_headers, json=body)
    assert r.status_code == 200, r.text
    return r.json()


class TestSubmit:
    def test_submit_returns_job_id(self, restd, auth_headers, pending_jobs):
        payload = _submit(
            restd,
            auth_headers,
            name="hello",
            partition="compute",
            current_working_directory="/home/alice",
            environment=["F7T_version=v2.0.0", "FOO=bar"],
            script="#!/bin/bash\necho hi",
        )
        # FireCREST reads the top-level job_id from the submit response.
        assert isinstance(payload["job_id"], int)
        assert "step_id" in payload
        assert payload["errors"] == []

    def test_submit_script_sibling_dialect(self, restd, auth_headers, pending_jobs):
        # Pre-0.0.41 dialect: script as a sibling of "job".
        r = restd.post(
            f"/slurm/{V}/job/submit",
            headers=auth_headers,
            json={"job": {"name": "x"}, "script": "#!/bin/bash\ntrue"},
        )
        assert r.status_code == 200
        assert isinstance(r.json()["job_id"], int)


class TestJobViewShapes:
    def test_ctld_job_fields(self, restd, auth_headers, pending_jobs):
        jid = _submit(
            restd,
            auth_headers,
            name="job1",
            partition="compute",
            current_working_directory="/home/bob",
        )["job_id"]
        job = restd.get(f"/slurm/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]

        assert job["job_id"] == jid
        assert isinstance(job["job_state"], list)  # scalar-or-list; emulator uses list
        assert job["job_state"] == ["PENDING"]
        assert job["name"] == "job1"
        assert job["partition"] == "compute"
        assert job["current_working_directory"] == "/home/bob"
        # FireCREST SlurmJob reads job_resources.nodes.count
        assert job["job_resources"]["nodes"]["count"] == 1
        # NO_VAL structs FireCREST decodes via slurm_int_to_int
        assert _no_val(job["time_limit"])
        assert _no_val(job["priority"])
        assert _no_val(job["start_time"])
        assert _no_val(job["end_time"])
        # SlurmJob builds its required `status` from exit_code and its required
        # `time` from start_time/end_time/suspend_time — all must be present even
        # for a PENDING job (start/end fall back to submit_time).
        assert _no_val(job["suspend_time"])
        assert job["exit_code"]["status"] == ["SUCCESS"]
        assert _no_val(job["exit_code"]["return_code"])
        assert job["start_time"]["set"] is True  # pending -> falls back to submit_time
        assert job["end_time"]["set"] is True

    def test_single_job_missing_is_empty_not_error_body(self, restd, auth_headers):
        # FireCREST treats a 404 single-job as "no job" — the ctld path
        # returns 404 with an empty jobs list, the dbd path 200 + warning.
        ctld = restd.get(f"/slurm/{V}/job/999999", headers=auth_headers)
        assert ctld.status_code == 404
        assert ctld.json()["jobs"] == []
        dbd = restd.get(f"/slurmdb/{V}/job/999999", headers=auth_headers)
        assert dbd.status_code == 200
        assert dbd.json()["jobs"] == []


class TestLifecycleAndAccounting:
    def test_submit_poll_to_completed_and_accounting(self, restd, auth_headers, instant_jobs):
        jid = _submit(restd, auth_headers, name="quick")["job_id"]

        ctld = restd.get(f"/slurm/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
        assert ctld["job_state"] == ["COMPLETED"]

        # Once complete it must appear in the accounting (slurmdb) plane —
        # FireCREST's get_job merges both, preferring PENDING from /slurm.
        dbd = restd.get(f"/slurmdb/{V}/job/{jid}", headers=auth_headers).json()["jobs"]
        assert len(dbd) == 1
        assert dbd[0]["state"]["current"] == ["COMPLETED"]
        assert dbd[0]["job_id"] == jid

        listing = restd.get(f"/slurmdb/{V}/jobs/", headers=auth_headers).json()["jobs"]
        assert any(j["job_id"] == jid for j in listing)

    def test_cancel(self, restd, auth_headers, pending_jobs):
        jid = _submit(restd, auth_headers, name="cancelme")["job_id"]
        r = restd.delete(f"/slurm/{V}/job/{jid}", headers=auth_headers)
        assert r.status_code == 200
        job = restd.get(f"/slurm/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
        assert job["job_state"] == ["CANCELLED"]


class TestClusterViews:
    def test_nodes(self, restd, auth_headers):
        nodes = restd.get(f"/slurm/{V}/nodes/", headers=auth_headers).json()["nodes"]
        assert nodes
        assert all("name" in n and isinstance(n["state"], list) for n in nodes)

    def test_partitions(self, restd, auth_headers):
        parts = restd.get(f"/slurm/{V}/partitions/", headers=auth_headers).json()["partitions"]
        assert {p["name"] for p in parts} == {"debug", "compute"}

    def test_reservations_present_as_list(self, restd, auth_headers):
        body = restd.get(f"/slurm/{V}/reservations/", headers=auth_headers).json()
        assert body["reservations"] == []

    def test_ping(self, restd, auth_headers):
        pings = restd.get(f"/slurm/{V}/ping/", headers=auth_headers).json()["pings"]
        assert pings
        assert pings[0]["responding"] is True


class TestAccountsViaAssociations:
    def test_associations_by_user_carry_account_and_is_default(
        self, restd, auth_headers, state_env
    ):
        # FireCREST maps GET /slurmdb/{v}/associations?user=X to accounts,
        # using association.account and is_default.
        db = SlurmDatabase()
        db.load_state()
        db.add_account("proj1", "P1", "org")
        db.add_account("proj2", "P2", "org")
        db.add_user("carol", default_account="proj2")
        db.add_association("carol", "proj1")
        db.add_association("carol", "proj2")
        db.save_state()

        assocs = restd.get(
            f"/slurmdb/{V}/associations/", headers=auth_headers, params={"user": "carol"}
        ).json()["associations"]
        by_account = {a["account"]: a for a in assocs if a["user"] == "carol"}
        assert set(by_account) == {"proj1", "proj2"}
        assert by_account["proj2"]["is_default"] is True
        assert by_account["proj1"]["is_default"] is False
