"""Tests for multi-cluster support."""

import pytest

from emulator.commands.dispatcher import SlurmEmulator
from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import (
    ClusterClassification,
    Job,
    SlurmDatabase,
)
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import UsageSimulator


class TestClusterCRUD:
    """Test cluster create/read/update/delete operations."""

    def test_default_cluster_exists(self):
        db = SlurmDatabase()
        assert db.get_cluster("default") is not None
        assert db.current_cluster == "default"

    def test_add_cluster(self):
        db = SlurmDatabase()
        db.add_cluster("test-cluster", control_host="10.0.0.1", control_port=6818)
        cluster = db.get_cluster("test-cluster")
        assert cluster is not None
        assert cluster.name == "test-cluster"
        assert cluster.control_host == "10.0.0.1"
        assert cluster.control_port == 6818

    def test_list_clusters(self):
        db = SlurmDatabase()
        db.add_cluster("cluster-a")
        db.add_cluster("cluster-b")
        clusters = db.list_clusters()
        names = [c.name for c in clusters]
        assert "default" in names
        assert "cluster-a" in names
        assert "cluster-b" in names

    def test_delete_cluster(self):
        db = SlurmDatabase()
        db.add_cluster("to-delete")
        db.delete_cluster("to-delete")
        assert db.get_cluster("to-delete") is None

    def test_cannot_delete_default_cluster(self):
        db = SlurmDatabase()
        db.delete_cluster("default")
        assert db.get_cluster("default") is not None

    def test_set_current_cluster(self):
        db = SlurmDatabase()
        db.add_cluster("other")
        assert db.set_current_cluster("other") is True
        assert db.current_cluster == "other"

    def test_set_current_cluster_nonexistent(self):
        db = SlurmDatabase()
        assert db.set_current_cluster("nonexistent") is False
        assert db.current_cluster == "default"

    def test_delete_cluster_resets_current(self):
        db = SlurmDatabase()
        db.add_cluster("temp")
        db.set_current_cluster("temp")
        db.delete_cluster("temp")
        assert db.current_cluster == "default"

    def test_cluster_id_auto_increment(self):
        db = SlurmDatabase()
        # default cluster gets id=1
        assert db.get_cluster("default").id == 1
        db.add_cluster("a")
        db.add_cluster("b")
        assert db.get_cluster("a").id == 2
        assert db.get_cluster("b").id == 3

    def test_cluster_soft_delete_filtered_from_list(self):
        db = SlurmDatabase()
        db.add_cluster("soft")
        db.delete_cluster("soft")
        # get_cluster returns None for soft-deleted
        assert db.get_cluster("soft") is None
        # list_clusters excludes soft-deleted
        names = [c.name for c in db.list_clusters()]
        assert "soft" not in names
        # But it still exists in the dict internally
        assert "soft" in db.clusters

    def test_cluster_delete_blocked_by_running_jobs(self):
        db = SlurmDatabase()
        db.add_cluster("busy")
        db.add_job(Job(job_id="j1", account="acc", user="u1", state="RUNNING", cluster="busy"))

        with pytest.raises(ValueError, match="running/pending"):
            db.delete_cluster("busy")

        # Cluster still accessible
        assert db.get_cluster("busy") is not None

    def test_cluster_delete_ok_with_completed_jobs(self):
        db = SlurmDatabase()
        db.add_cluster("done")
        db.add_job(Job(job_id="j1", account="acc", user="u1", state="COMPLETED", cluster="done"))
        # Should not raise
        db.delete_cluster("done")
        assert db.get_cluster("done") is None


class TestGlobalAccounts:
    """Test that accounts are global entities, not per-cluster."""

    def test_account_is_global(self):
        db = SlurmDatabase()
        db.add_cluster("cluster-a")

        db.add_account("myacc", "Desc", "org")
        # Accessible regardless of cluster context
        assert db.get_account("myacc") is not None
        db.set_current_cluster("cluster-a")
        assert db.get_account("myacc") is not None

    def test_same_account_name_cannot_be_added_twice(self):
        db = SlurmDatabase()
        db.add_account("dup", "First", "org")
        db.add_account("dup", "Second", "org")
        # Second add overwrites
        assert db.get_account("dup").description == "Second"

    def test_list_accounts_returns_all(self):
        db = SlurmDatabase()
        db.add_cluster("cluster-a")
        db.add_account("acc1", "Acc1", "org")
        db.set_current_cluster("cluster-a")
        db.add_account("acc2", "Acc2", "org")

        all_accounts = db.list_accounts()
        names = [a.name for a in all_accounts]
        # Both visible regardless of current cluster
        assert "acc1" in names
        assert "acc2" in names

    def test_delete_cluster_does_not_delete_accounts(self):
        db = SlurmDatabase()
        db.add_cluster("temp")
        db.add_account("survivor", "Desc", "org")
        db.delete_cluster("temp")
        assert db.get_account("survivor") is not None

    def test_account_creation_ignores_cluster_context(self):
        db = SlurmDatabase()
        db.add_cluster("other")
        db.set_current_cluster("other")
        db.add_account("test", "Test", "org")
        # Account is global, not scoped to "other"
        db.set_current_cluster("default")
        assert db.get_account("test") is not None


class TestPerClusterUsageIsolation:
    """Test that usage records are isolated per cluster."""

    def test_usage_records_filtered_by_cluster(self):
        db = SlurmDatabase()
        te = TimeEngine()
        db.add_cluster("cluster-a")

        sim_default = UsageSimulator(te, db)
        db.set_current_cluster("default")
        db.add_account("acc", "Acc", "org")
        sim_default.inject_usage("acc", "user1", 100.0, cluster="default")

        db.set_current_cluster("cluster-a")
        sim_default.inject_usage("acc", "user1", 200.0, cluster="cluster-a")

        default_usage = db.get_total_usage("acc", cluster="default")
        cluster_a_usage = db.get_total_usage("acc", cluster="cluster-a")

        assert default_usage == 100.0
        assert cluster_a_usage == 200.0

    def test_association_isolation(self):
        db = SlurmDatabase()
        db.add_cluster("cluster-a")

        db.add_user("user1")
        db.add_association("user1", "root", cluster="default")
        db.add_association("user1", "root", cluster="cluster-a")

        assert db.get_association("user1", "root", cluster="default") is not None
        assert db.get_association("user1", "root", cluster="cluster-a") is not None

        db.delete_association("user1", "root", cluster="default")
        assert db.get_association("user1", "root", cluster="default") is None
        assert db.get_association("user1", "root", cluster="cluster-a") is not None

    def test_jobs_filtered_by_cluster(self):
        db = SlurmDatabase()
        db.add_cluster("cluster-a")

        db.add_job(Job(job_id="1", account="acc", user="u1", state="RUNNING", cluster="default"))
        db.add_job(Job(job_id="2", account="acc", user="u1", state="RUNNING", cluster="cluster-a"))

        default_jobs = db.list_jobs(cluster="default")
        cluster_a_jobs = db.list_jobs(cluster="cluster-a")

        assert len(default_jobs) == 1
        assert default_jobs[0].job_id == "1"
        assert len(cluster_a_jobs) == 1
        assert cluster_a_jobs[0].job_id == "2"


class TestClusterFlagParsing:
    """Test -M flag extraction in dispatcher."""

    def test_extract_cluster_flag_dash_m(self):
        emulator = SlurmEmulator()
        args, cluster = emulator.extract_cluster_flag(["-M", "test-cluster", "list", "accounts"])
        assert cluster == "test-cluster"
        assert args == ["list", "accounts"]

    def test_extract_cluster_flag_dash_m_no_space(self):
        emulator = SlurmEmulator()
        args, cluster = emulator.extract_cluster_flag(["-Mtest-cluster", "list", "accounts"])
        assert cluster == "test-cluster"
        assert args == ["list", "accounts"]

    def test_extract_cluster_flag_clusters_eq(self):
        emulator = SlurmEmulator()
        args, cluster = emulator.extract_cluster_flag(
            ["--clusters=test-cluster", "list", "accounts"]
        )
        assert cluster == "test-cluster"
        assert args == ["list", "accounts"]

    def test_extract_cluster_flag_none(self):
        emulator = SlurmEmulator()
        args, cluster = emulator.extract_cluster_flag(["list", "accounts"])
        assert cluster is None
        assert args == ["list", "accounts"]

    def test_sacct_still_supports_dash_m(self):
        """sacct should support -M flag for cluster filtering."""
        emulator = SlurmEmulator()
        emulator.database.add_cluster("test-cluster")
        # sacct with -M should work (no error about nonexistent cluster)
        output = emulator.execute_command("sacct", ["-M", "test-cluster"])
        assert "does not exist" not in output

    def test_sacctmgr_ignores_dash_m(self):
        """sacctmgr should NOT extract -M flag — it passes through as raw args."""
        emulator = SlurmEmulator()
        emulator.database.add_cluster("test-cluster")
        # -M is not extracted for sacctmgr, so it flows as args to sacctmgr handler
        # sacctmgr will treat "-M" as an unknown command
        output = emulator.execute_command("sacctmgr", ["-M", "test-cluster", "list", "accounts"])
        # It won't be intercepted as a cluster flag
        assert "does not exist" not in output

    def test_execute_command_nonexistent_cluster_sacct(self):
        emulator = SlurmEmulator()
        output = emulator.execute_command("sacct", ["-M", "nope"])
        assert "does not exist" in output


class TestSacctmgrClusterCommands:
    """Test sacctmgr add/list/remove cluster commands."""

    def test_add_cluster(self):
        db = SlurmDatabase()
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)

        output = sacctmgr.handle_command(["add", "cluster", "prod-cluster"])
        assert "Adding Cluster" in output
        assert "prod-cluster" in output
        assert db.get_cluster("prod-cluster") is not None

    def test_add_cluster_with_params(self):
        db = SlurmDatabase()
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)

        output = sacctmgr.handle_command(
            ["add", "cluster", "prod", "control_host=10.0.0.1", "control_port=6818"]
        )
        cluster = db.get_cluster("prod")
        assert cluster is not None
        assert cluster.control_host == "10.0.0.1"
        assert cluster.control_port == 6818

    def test_add_duplicate_cluster(self):
        db = SlurmDatabase()
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)

        sacctmgr.handle_command(["add", "cluster", "test"])
        output = sacctmgr.handle_command(["add", "cluster", "test"])
        assert "already exists" in output

    def test_list_clusters(self):
        db = SlurmDatabase()
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)

        db.add_cluster("prod")
        db.add_cluster("dev")
        output = sacctmgr.handle_command(["list", "clusters"])
        assert "default" in output
        assert "prod" in output
        assert "dev" in output
        assert "RPC" in output  # New column

    def test_remove_cluster(self):
        db = SlurmDatabase()
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)

        db.add_cluster("to-remove")
        output = sacctmgr.handle_command(["remove", "cluster", "where", "name=to-remove"])
        assert "Deleting" in output
        assert db.get_cluster("to-remove") is None

    def test_remove_default_cluster_fails(self):
        db = SlurmDatabase()
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)

        output = sacctmgr.handle_command(["remove", "cluster", "where", "name=default"])
        assert "Cannot delete" in output

    def test_list_accounts_no_cluster_column(self):
        db = SlurmDatabase()
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)

        output = sacctmgr.handle_command(["list", "accounts"])
        assert "Cluster" not in output
        assert "Account|Descr|Org|" in output

    def test_remove_cluster_blocked_by_running_jobs(self):
        db = SlurmDatabase()
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)

        db.add_cluster("busy")
        db.add_job(Job(job_id="j1", account="a", user="u", state="RUNNING", cluster="busy"))

        output = sacctmgr.handle_command(["remove", "cluster", "where", "name=busy"])
        assert "error" in output
        assert "running/pending" in output
        # Cluster should still exist
        assert db.get_cluster("busy") is not None


class TestRootAssociationAutoCreated:
    """Test that root association is auto-created when a cluster is added."""

    def test_root_association_on_default_cluster(self):
        db = SlurmDatabase()
        assoc = db.get_association("", "root", cluster="default")
        assert assoc is not None
        assert assoc.account == "root"

    def test_root_association_auto_created_on_cluster_add(self):
        db = SlurmDatabase()
        db.add_cluster("prod")
        assoc = db.get_association("", "root", cluster="prod")
        assert assoc is not None
        assert assoc.account == "root"
        assert assoc.cluster == "prod"

    def test_root_account_exists_globally_after_cluster_add(self):
        db = SlurmDatabase()
        db.add_cluster("new-cluster")
        assert db.get_account("root") is not None


class TestSacctmgrAccountWithCluster:
    """Test sacctmgr add account with cluster= parameter."""

    def test_add_account_with_cluster_creates_association(self):
        db = SlurmDatabase()
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)

        db.add_cluster("prod")
        output = sacctmgr.handle_command(["add", "account", "myacc", "cluster=prod"])
        assert "Adding Account" in output
        # Account exists globally
        assert db.get_account("myacc") is not None
        # Association exists on prod cluster
        assoc = db.get_association("", "myacc", cluster="prod")
        assert assoc is not None

    def test_add_existing_account_with_cluster_creates_association(self):
        db = SlurmDatabase()
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)

        db.add_cluster("prod")
        # Create account first
        sacctmgr.handle_command(["add", "account", "myacc"])
        # Now add to a cluster — should create association, not error
        output = sacctmgr.handle_command(["add", "account", "myacc", "cluster=prod"])
        assert "error" not in output.lower() or "already exists" not in output
        assoc = db.get_association("", "myacc", cluster="prod")
        assert assoc is not None


class TestClassificationEnum:
    """Test cluster classification enum validation."""

    def test_classification_enum_values(self):
        assert ClusterClassification.NONE.value == ""
        assert ClusterClassification.CAPABILITY.value == "capability"
        assert ClusterClassification.CAPACITY.value == "capacity"
        assert ClusterClassification.CAPAPACITY.value == "capapacity"

    def test_add_cluster_with_valid_classification(self):
        db = SlurmDatabase()
        db.add_cluster("gpu", classification="capability")
        cluster = db.get_cluster("gpu")
        assert cluster.classification == ClusterClassification.CAPABILITY

    def test_add_cluster_with_invalid_classification_defaults_to_none(self):
        db = SlurmDatabase()
        db.add_cluster("bad", classification="invalid_value")
        cluster = db.get_cluster("bad")
        assert cluster.classification == ClusterClassification.NONE

    def test_sacctmgr_validates_classification(self):
        db = SlurmDatabase()
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)

        output = sacctmgr.handle_command(["add", "cluster", "bad", "classification=invalid"])
        assert "Invalid classification" in output

    def test_sacctmgr_accepts_valid_classification(self):
        db = SlurmDatabase()
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)

        output = sacctmgr.handle_command(["add", "cluster", "gpu", "classification=capability"])
        assert "Adding Cluster" in output
        cluster = db.get_cluster("gpu")
        assert cluster.classification == ClusterClassification.CAPABILITY


class TestBackwardCompatibleStateLoading:
    """Test that old state files without cluster data load correctly."""

    def test_load_old_format_state(self, tmp_path):
        import json

        # Create old format state file (pre-cluster, plain name keys)
        old_state = {
            "accounts": {
                "root": {
                    "name": "root",
                    "description": "Root account",
                    "organization": "system",
                    "parent": None,
                    "fairshare": 1,
                    "qos": "normal",
                    "limits": {},
                    "last_period": None,
                    "allocation": 1000,
                },
                "test": {
                    "name": "test",
                    "description": "Test account",
                    "organization": "emulator",
                    "parent": None,
                    "fairshare": 1,
                    "qos": "normal",
                    "limits": {},
                    "last_period": None,
                    "allocation": 2000,
                },
            },
            "users": {"user1": {"name": "user1", "default_account": "test"}},
            "associations": {
                "user1:test": {"account": "test", "user": "user1", "limits": {}},
            },
            "usage_records": [
                {
                    "account": "test",
                    "user": "user1",
                    "node_hours": 100.0,
                    "billing_units": 100.0,
                    "timestamp": "2024-01-15T00:00:00",
                    "period": "2024-Q1",
                    "raw_tres": {},
                }
            ],
            "jobs": {},
        }

        state_file = tmp_path / "old_state.json"
        with state_file.open("w") as f:
            json.dump(old_state, f)

        db = SlurmDatabase()
        db.state_file = state_file
        db.load_state()

        # Verify migration — accounts are global now
        assert db.get_cluster("default") is not None
        assert db.get_account("test") is not None

        # Verify associations migrated
        assert db.get_association("user1", "test", cluster="default") is not None

        # Verify usage records migrated
        records = db.get_usage_records(account="test", cluster="default")
        assert len(records) == 1
        assert records[0].cluster == "default"

    def test_load_name_at_cluster_format(self, tmp_path):
        """Test loading state with name@cluster keys (old multi-cluster format)."""
        import json

        state = {
            "clusters": {
                "default": {
                    "name": "default",
                    "control_host": "localhost",
                    "control_port": 6817,
                    "classification": "",
                },
            },
            "accounts": {
                "root@default": {
                    "name": "root",
                    "description": "Root",
                    "organization": "system",
                    "parent": None,
                    "fairshare": 1,
                    "qos": "normal",
                    "limits": {},
                    "last_period": None,
                    "allocation": 1000,
                    "cluster": "default",
                },
                "test@default": {
                    "name": "test",
                    "description": "Test",
                    "organization": "org",
                    "parent": None,
                    "fairshare": 1,
                    "qos": "normal",
                    "limits": {},
                    "last_period": None,
                    "allocation": 500,
                    "cluster": "default",
                },
            },
            "users": {},
            "associations": {},
            "usage_records": [],
            "jobs": {},
        }

        state_file = tmp_path / "state.json"
        with state_file.open("w") as f:
            json.dump(state, f)

        db = SlurmDatabase()
        db.state_file = state_file
        db.load_state()

        # Accounts loaded by name, cluster field stripped
        assert db.get_account("root") is not None
        assert db.get_account("test") is not None
        # Keys should be plain names
        assert "root" in db.accounts
        assert "test" in db.accounts

    def test_delete_cluster_preserves_accounts(self):
        """Deleting a cluster should NOT delete global accounts."""
        db = SlurmDatabase()
        db.add_cluster("temp")
        db.add_account("acc", "Acc", "org")
        db.add_user("user1")
        db.add_association("user1", "acc", cluster="temp")

        te = TimeEngine()
        sim = UsageSimulator(te, db)
        sim.inject_usage("acc", "user1", 50.0, cluster="temp")

        db.delete_cluster("temp")

        # Account survives cluster deletion (global)
        assert db.get_account("acc") is not None
        # But cluster-scoped data is cleaned
        assert db.get_association("user1", "acc", cluster="temp") is None
        records = db.get_usage_records(account="acc", cluster="temp")
        assert len(records) == 0
