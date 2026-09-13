"""``AccountingStorageEnforce=associations`` on job submission.

Real slurmctld resolves the (user, account, partition) association before
accepting a job (slurm://src/slurmctld/job_mgr.c#_job_create →
slurm://src/common/assoc_mgr.c#assoc_mgr_fill_in_assoc) and, with
associations enforced, refuses a missing one with ``ESLURM_INVALID_ACCOUNT``
(slurm://src/common/slurm_errno.c#ESLURM_INVALID_ACCOUNT); the setting is
parsed by slurm://src/common/read_config.c#_validate_accounting_storage_enforce.
"""

import pytest

from emulator.api.slurmrestd.envelope import ESLURM_INVALID_ACCOUNT
from emulator.api.ssh import server
from emulator.commands.dispatcher import SlurmEmulator
from emulator.core import accounting
from emulator.core.database import SlurmDatabase
from emulator.slurm_version import current

V = current().api_version
SCRIPT = "#!/bin/bash\nhostname"


@pytest.fixture
def enforced(monkeypatch):
    monkeypatch.setenv(accounting.ENV_VAR, "associations")


@pytest.fixture
def permissive(monkeypatch):
    monkeypatch.delenv(accounting.ENV_VAR, raising=False)


def _seed(state_env):
    """proj1 with alice (default), proj2 with alice on partition gpu only; bob has nothing."""
    db = SlurmDatabase()
    db.load_state()
    db.add_account("proj1", "Project 1", "org")
    db.add_account("proj2", "Project 2", "org")
    db.add_user("alice", default_account="proj1")
    db.add_association("alice", "proj1")
    db.add_association("alice", "proj2", partition="gpu")
    db.add_user("bob")
    db.save_state()
    return db


def _submit(restd, auth_headers, user, **fields):
    body = {"job": {"user_name": user, "script": SCRIPT, **fields}}
    return restd.post(f"/slurm/{V}/job/submit", json=body, headers=auth_headers)


class TestSetting:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("", False),
            ("none", False),
            ("associations", True),
            ("1", True),
            ("limits", True),
            ("qos", True),
            ("safe", True),
            ("wckeys", True),
            ("limits,qos", True),
            ("Associations", True),
        ],
    )
    def test_tokens_implying_associations(self, monkeypatch, value, expected):
        monkeypatch.setenv(accounting.ENV_VAR, value)
        assert accounting.associations_enforced() is expected

    def test_unset_is_off_like_slurm_conf(self, permissive):  # noqa: ARG002
        assert accounting.associations_enforced() is False


class TestResolveJobAccount:
    def test_enforced(self, state_env, enforced):  # noqa: ARG002
        db = _seed(state_env)
        assert accounting.resolve_job_account(db, "alice", "proj1", "compute") == "proj1"
        assert accounting.resolve_job_account(db, "alice", "", "compute") == "proj1"
        assert accounting.resolve_job_account(db, "alice", "Proj1", "compute") == "proj1"
        # partition-bound row only matches its partition
        assert accounting.resolve_job_account(db, "alice", "proj2", "gpu") == "proj2"
        assert accounting.resolve_job_account(db, "alice", "proj2", "compute") is None
        assert accounting.resolve_job_account(db, "bob", "proj1", "compute") is None
        assert accounting.resolve_job_account(db, "bob", "", "compute") is None
        assert accounting.resolve_job_account(db, "ghost", "", "compute") is None

    def test_default_without_recorded_default_account(self, state_env, enforced):  # noqa: ARG002
        db = _seed(state_env)
        db.add_user("carol")
        db.add_association("carol", "proj2")
        assert accounting.resolve_job_account(db, "carol", "", "compute") == "proj2"

    def test_permissive_legacy_chain(self, state_env, permissive):  # noqa: ARG002
        db = _seed(state_env)
        assert accounting.resolve_job_account(db, "alice", "", "compute") == "proj1"
        assert accounting.resolve_job_account(db, "bob", "", "compute") == "root"
        assert accounting.resolve_job_account(db, "bob", "nope", "compute") == "nope"
        assert accounting.resolve_job_account(db, "ghost", "", "compute") == "root"


class TestRestSubmit:
    def test_member_is_accepted(self, restd, auth_headers, state_env, enforced):  # noqa: ARG002
        _seed(state_env)
        resp = _submit(restd, auth_headers, "alice", account="proj1")
        assert resp.status_code == 200
        jid = resp.json()["job_id"]
        job = restd.get(f"/slurm/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
        assert job["account"] == "proj1"

    def test_default_account_when_none_requested(self, restd, auth_headers, state_env, enforced):  # noqa: ARG002
        _seed(state_env)
        jid = _submit(restd, auth_headers, "alice").json()["job_id"]
        job = restd.get(f"/slurm/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
        assert job["account"] == "proj1"

    @pytest.mark.parametrize(
        ("user", "fields"),
        [
            ("bob", {"account": "proj1"}),  # never a member
            ("bob", {}),  # no association at all
            ("alice", {"account": "proj2", "partition": "compute"}),  # wrong partition
            ("alice", {"account": "nope"}),  # unknown account
        ],
    )
    def test_no_association_is_refused(
        self, restd, auth_headers, state_env, enforced, user, fields
    ):  # noqa: ARG002
        _seed(state_env)
        resp = _submit(restd, auth_headers, user, **fields)
        # slurmctld-range errno → 422 (slurm://src/common/http.c#http_status_from_error@25.11+)
        assert resp.status_code == 422
        err = resp.json()["errors"][0]
        assert err["error_number"] == ESLURM_INVALID_ACCOUNT
        assert err["error"] == "Invalid account or account/partition combination specified"
        assert err["description"].startswith("Invalid account or account/partition combination")
        assert restd.get(f"/slurm/{V}/jobs/", headers=auth_headers).json()["jobs"] == []

    def test_removed_member_can_no_longer_submit(self, restd, auth_headers, state_env, enforced):  # noqa: ARG002
        _seed(state_env)
        assert _submit(restd, auth_headers, "alice", account="proj1").status_code == 200
        restd.delete(
            f"/slurmdb/{V}/associations/",
            params={"user": "alice", "account": "proj1"},
            headers=auth_headers,
        )
        assert _submit(restd, auth_headers, "alice", account="proj1").status_code == 422

    def test_permissive_keeps_legacy_fallback(self, restd, auth_headers, state_env, permissive):  # noqa: ARG002
        _seed(state_env)
        jid = _submit(restd, auth_headers, "bob").json()["job_id"]
        job = restd.get(f"/slurm/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
        assert job["account"] == "root"

    def test_associations_filterable_by_user(self, restd, auth_headers, state_env):
        _seed(state_env)
        body = restd.get(
            f"/slurmdb/{V}/associations/", params={"user": "alice"}, headers=auth_headers
        ).json()
        assert {(a["account"], a["user"]) for a in body["associations"]} == {
            ("proj1", "alice"),
            ("proj2", "alice"),
        }
        body = restd.get(
            f"/slurmdb/{V}/associations/", params={"user": "bob"}, headers=auth_headers
        ).json()
        assert body["associations"] == []
        assert body["warnings"]


class TestSshSbatch:
    def test_member_and_non_member(self, state_env, enforced):  # noqa: ARG002
        _seed(state_env)
        emu = SlurmEmulator()
        out, err, code = server._sbatch(emu, "alice", ["-A", "proj1", "job.sh"])  # noqa: SLF001
        assert (err, code) == ("", 0)
        assert out.startswith("Submitted batch job ")
        out, err, code = server._sbatch(emu, "bob", ["-A", "proj1", "job.sh"])  # noqa: SLF001
        assert (out, code) == ("", 1)
        assert err == (
            "sbatch: error: Batch job submission failed: "
            "Invalid account or account/partition combination specified\n"
        )
        assert len(emu.database.jobs) == 1

    def test_partition_bound_association(self, state_env, enforced):  # noqa: ARG002
        _seed(state_env)
        emu = SlurmEmulator()
        _, _, code = server._sbatch(emu, "alice", ["-A", "proj2", "-p", "gpu", "job.sh"])  # noqa: SLF001
        assert code == 0
        _, _, code = server._sbatch(emu, "alice", ["-A", "proj2", "job.sh"])  # noqa: SLF001
        assert code == 1

    def test_permissive(self, state_env, permissive):  # noqa: ARG002
        _seed(state_env)
        emu = SlurmEmulator()
        out, _, code = server._sbatch(emu, "bob", ["job.sh"])  # noqa: SLF001
        assert code == 0
        assert emu.database.get_job(out.split()[-1]).account == "root"
