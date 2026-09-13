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
from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core import accounting
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine
from emulator.slurm_version import current

V = current().api_version
SCRIPT = "#!/bin/bash\nhostname"


@pytest.fixture
def enforced(monkeypatch):
    monkeypatch.setenv(accounting.ENV_VAR, "associations")


@pytest.fixture
def permissive(monkeypatch):
    monkeypatch.setenv(accounting.ENV_VAR, "none")


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

    def test_unset_means_associations(self, monkeypatch):
        monkeypatch.delenv(accounting.ENV_VAR, raising=False)
        assert accounting.DEFAULT == "associations"
        assert accounting.associations_enforced() is True


class TestRootSeed:
    """slurmdbd always has user root under account root
    (slurm://src/plugins/accounting_storage/mysql/accounting_storage_mysql.c#_as_mysql_acct_check_tables,
    slurm://src/plugins/accounting_storage/mysql/as_mysql_cluster.c#as_mysql_add_clusters)."""

    def test_root_user_and_association_on_every_cluster(self, state_env):
        db = SlurmDatabase()
        assert db.get_user("root").default_account == "root"
        assert db.get_association("root", "root", cluster="default") is not None
        db.add_cluster("second")
        assert db.get_association("root", "root", cluster="second") is not None

    def test_root_submits_under_enforcement(self, restd, auth_headers, enforced):
        # No user_name and no token user → slurm_user "root" → root's own association.
        body = {"job": {"script": SCRIPT}}
        resp = restd.post(f"/slurm/{V}/job/submit", json=body, headers=auth_headers)
        assert resp.status_code == 200
        jid = resp.json()["job_id"]
        job = restd.get(f"/slurm/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
        assert (job["user_name"], job["account"]) == ("root", "root")


class TestResolveJobAccount:
    def test_enforced(self, state_env, enforced):
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

    def test_default_without_recorded_default_account(self, state_env, enforced):
        db = _seed(state_env)
        db.add_user("carol")
        db.add_association("carol", "proj2")
        assert accounting.resolve_job_account(db, "carol", "", "compute") == "proj2"

    def test_identity_precedes_association(self, state_env, enforced, monkeypatch):
        # slurmctld order: USER_ID parse error before the association check.
        db = _seed(state_env)
        monkeypatch.setenv("SLURM_EMULATOR_NSS", "1")
        monkeypatch.setattr(accounting.nss, "resolve", lambda _name: None)
        admission = accounting.admit_job(db, "bob", "proj1", "compute")
        assert admission.error == accounting.ESLURM_USER_ID_UNKNOWN
        assert admission.message == "Unable to resolve user: bob"

    def test_permissive_legacy_chain(self, state_env, permissive):
        db = _seed(state_env)
        assert accounting.resolve_job_account(db, "alice", "", "compute") == "proj1"
        assert accounting.resolve_job_account(db, "bob", "", "compute") == "root"
        assert accounting.resolve_job_account(db, "bob", "nope", "compute") == "nope"
        assert accounting.resolve_job_account(db, "ghost", "", "compute") == "root"


class TestRestSubmit:
    def test_member_is_accepted(self, restd, auth_headers, state_env, enforced):
        _seed(state_env)
        resp = _submit(restd, auth_headers, "alice", account="proj1")
        assert resp.status_code == 200
        jid = resp.json()["job_id"]
        job = restd.get(f"/slurm/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
        assert job["account"] == "proj1"

    def test_default_account_when_none_requested(self, restd, auth_headers, state_env, enforced):
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
    ):
        _seed(state_env)
        resp = _submit(restd, auth_headers, user, **fields)
        # slurmctld-range errno → 422 (slurm://src/common/http.c#http_status_from_error@25.11+)
        assert resp.status_code == 422
        err = resp.json()["errors"][0]
        assert err["error_number"] == ESLURM_INVALID_ACCOUNT
        assert err["error"] == "Invalid account or account/partition combination specified"
        assert err["description"].startswith("Invalid account or account/partition combination")
        assert restd.get(f"/slurm/{V}/jobs/", headers=auth_headers).json()["jobs"] == []

    def test_removed_member_can_no_longer_submit(self, restd, auth_headers, state_env, enforced):
        _seed(state_env)
        assert _submit(restd, auth_headers, "alice", account="proj1").status_code == 200
        # proj1 is alice's default: slurmdbd refuses to drop it while proj2 remains
        # (slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#as_mysql_remove_assocs),
        # so membership is withdrawn the way an agent does it — every association goes.
        refused = restd.delete(
            f"/slurmdb/{V}/associations/",
            params={"user": "alice", "account": "proj1"},
            headers=auth_headers,
        )
        assert refused.status_code == 422
        assert _submit(restd, auth_headers, "alice", account="proj1").status_code == 200
        for acct in ("proj2", "proj1"):
            resp = restd.delete(
                f"/slurmdb/{V}/associations/",
                params={"user": "alice", "account": acct},
                headers=auth_headers,
            )
            assert resp.status_code == 200
        assert _submit(restd, auth_headers, "alice", account="proj1").status_code == 422
        assert _submit(restd, auth_headers, "alice").status_code == 422

    def test_permissive_keeps_legacy_fallback(self, restd, auth_headers, state_env, permissive):
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
    def test_member_and_non_member(self, state_env, enforced):
        _seed(state_env)
        emu = SlurmEmulator()
        out, err, code = server._sbatch(emu, "alice", ["-A", "proj1", "job.sh"])
        assert (err, code) == ("", 0)
        assert out.startswith("Submitted batch job ")
        out, err, code = server._sbatch(emu, "bob", ["-A", "proj1", "job.sh"])
        assert (out, code) == ("", 1)
        assert err == (
            "sbatch: error: Batch job submission failed: "
            "Invalid account or account/partition combination specified\n"
        )
        assert len(emu.database.jobs) == 1

    def test_partition_bound_association(self, state_env, enforced):
        _seed(state_env)
        emu = SlurmEmulator()
        _, _, code = server._sbatch(emu, "alice", ["-A", "proj2", "-p", "gpu", "job.sh"])
        assert code == 0
        _, _, code = server._sbatch(emu, "alice", ["-A", "proj2", "job.sh"])
        assert code == 1

    def test_permissive(self, state_env, permissive):
        _seed(state_env)
        emu = SlurmEmulator()
        out, _, code = server._sbatch(emu, "bob", ["job.sh"])
        assert code == 0
        assert emu.database.get_job(out.split()[-1]).account == "root"


class TestClusterName:
    """``SLURM_EMULATOR_CLUSTER_NAME`` is slurm.conf's ClusterName
    (slurm://src/common/read_config.c#"ClusterName"): the cluster everything is filed under."""

    @pytest.fixture
    def linux(self, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_CLUSTER_NAME", "linux")

    def test_sets_and_creates_current_cluster(self, linux, state_env):
        db = SlurmDatabase()
        assert db.current_cluster == "linux"
        assert db.get_cluster("linux") is not None
        assert db.get_association("root", "root", cluster="linux") is not None
        # A saved state file pointing elsewhere does not override the setting.
        db.current_cluster = "default"
        db.save_state()
        fresh = SlurmDatabase()
        fresh.load_state()
        assert fresh.current_cluster == "linux"

    def test_agent_rows_under_the_cluster_name_admit_submits(
        self, linux, restd, auth_headers, enforced
    ):
        restd.post(
            f"/slurmdb/{V}/accounts/",
            json={"accounts": [{"name": "proj1", "description": "P", "organization": "o"}]},
            headers=auth_headers,
        )
        restd.post(
            f"/slurmdb/{V}/associations/",
            json={"associations": [{"account": "proj1", "user": "alice", "cluster": "linux"}]},
            headers=auth_headers,
        )
        conf = restd.get(f"/slurm/{V}/conf", headers=auth_headers).json()["config"]
        assert conf["cluster_name"] == "linux"
        resp = _submit(restd, auth_headers, "alice", account="proj1")
        assert resp.status_code == 200
        jid = resp.json()["job_id"]
        job = restd.get(f"/slurm/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
        assert job["cluster"] == "linux"

    def test_rows_on_another_cluster_do_not_count(self, state_env, enforced):
        db = _seed(state_env)  # alice's rows live on "default"
        db.add_cluster("linux")
        db.current_cluster = "linux"
        assert accounting.resolve_job_account(db, "alice", "proj1", "compute") is None


class TestDefaultAssociationInvariant:
    """slurmdbd keeps "the default account has a row" true
    (slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#as_mysql_remove_assocs,
    slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_make_sure_user_has_default_internal)."""

    @pytest.fixture
    def em(self, tmp_path):
        db = SlurmDatabase()
        db.state_file = tmp_path / "state.json"
        em = SacctmgrEmulator(db, TimeEngine())
        for acct in ("proj-a", "proj-b"):
            em.handle_command(["add", "account", acct, "description=x", "organization=o"])
        em.handle_command(["add", "user", "alice", "account=proj-a"])
        em.handle_command(["add", "user", "alice", "account=proj-b"])
        return em

    def test_first_association_becomes_default(self, em):
        assert em.database.get_user("alice").default_account == "proj-a"

    def test_removing_default_is_refused_while_others_remain(self, em, capsys):
        out = em.handle_command(["remove", "user", "where", "name=alice", "account=proj-a"])
        assert em.exit_code == 1
        assert (
            out.splitlines()[0]
            == " Error with request: You can not remove the default account of a user"
        )
        assert "A = proj-a" in out
        assert out.splitlines()[-1] == " Changes Discarded"
        assert em.database.get_association("alice", "proj-a") is not None

    def test_last_association_takes_the_user_along(self, em):
        em.handle_command(["remove", "user", "where", "name=alice", "account=proj-b"])
        assert em.exit_code == 0
        assert em.database.get_user("alice") is not None
        em.handle_command(["remove", "user", "where", "name=alice", "account=proj-a"])
        assert em.exit_code == 0
        assert em.database.get_user("alice") is None
        assert em.database.user_association_rows("alice") == []

    def test_remove_user_by_name_deletes_everything(self, em):
        out = em.handle_command(["remove", "user", "where", "name=alice"])
        assert em.exit_code == 0
        assert out == " Deleting users...\n  alice"
        assert em.database.get_user("alice") is None
        assert em.database.user_association_rows("alice") == []

    def test_rest_delete_refuses_default_then_allows_last(self, restd, auth_headers, state_env):
        db = SlurmDatabase()
        db.load_state()
        db.add_account("proj-a", "A", "o")
        db.add_account("proj-b", "B", "o")
        db.add_user("alice")
        db.add_association("alice", "proj-a")
        db.add_association("alice", "proj-b")
        db.save_state()
        resp = restd.delete(
            f"/slurmdb/{V}/associations/",
            params={"user": "alice", "account": "proj-a"},
            headers=auth_headers,
        )
        assert resp.status_code == 422
        err = resp.json()["errors"][0]
        assert err["error_number"] == 7009
        assert err["description"] == "You can not remove the default account of a user"
        for acct in ("proj-b", "proj-a"):
            resp = restd.delete(
                f"/slurmdb/{V}/associations/",
                params={"user": "alice", "account": acct},
                headers=auth_headers,
            )
            assert resp.status_code == 200
        users = restd.get(f"/slurmdb/{V}/user/alice", headers=auth_headers).json()["users"]
        assert users == []
