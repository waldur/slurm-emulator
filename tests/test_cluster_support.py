"""Tests for multi-cluster support."""

from emulator.commands.dispatcher import SlurmEmulator
from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
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


class TestPerClusterAccountIsolation:
    """Test that accounts are isolated per cluster."""

    def test_same_account_name_different_clusters(self):
        db = SlurmDatabase()
        db.add_cluster("cluster-a")
        db.add_cluster("cluster-b")

        db.add_account("shared-name", "Desc A", "org-a", cluster="cluster-a")
        db.add_account("shared-name", "Desc B", "org-b", cluster="cluster-b")

        acc_a = db.get_account("shared-name", cluster="cluster-a")
        acc_b = db.get_account("shared-name", cluster="cluster-b")

        assert acc_a is not None
        assert acc_b is not None
        assert acc_a.description == "Desc A"
        assert acc_b.description == "Desc B"
        assert acc_a.cluster == "cluster-a"
        assert acc_b.cluster == "cluster-b"

    def test_list_accounts_filtered_by_cluster(self):
        db = SlurmDatabase()
        db.add_cluster("cluster-a")

        db.add_account("acc1", "Acc1", "org", cluster="default")
        db.add_account("acc2", "Acc2", "org", cluster="cluster-a")

        default_accounts = db.list_accounts(cluster="default")
        cluster_a_accounts = db.list_accounts(cluster="cluster-a")

        default_names = [a.name for a in default_accounts]
        cluster_a_names = [a.name for a in cluster_a_accounts]

        assert "acc1" in default_names
        assert "acc2" not in default_names
        assert "acc2" in cluster_a_names
        assert "acc1" not in cluster_a_names

    def test_delete_account_cluster_scoped(self):
        db = SlurmDatabase()
        db.add_cluster("cluster-a")

        db.add_account("myacc", "Desc", "org", cluster="default")
        db.add_account("myacc", "Desc", "org", cluster="cluster-a")

        db.delete_account("myacc", cluster="default")

        assert db.get_account("myacc", cluster="default") is None
        assert db.get_account("myacc", cluster="cluster-a") is not None

    def test_current_cluster_default_used(self):
        db = SlurmDatabase()
        db.add_cluster("other")
        db.set_current_cluster("other")

        db.add_account("test", "Test", "org")  # No explicit cluster -> uses current
        assert db.get_account("test", cluster="other") is not None
        assert db.get_account("test", cluster="default") is None


class TestPerClusterUsageIsolation:
    """Test that usage records are isolated per cluster."""

    def test_usage_records_filtered_by_cluster(self):
        db = SlurmDatabase()
        te = TimeEngine()
        db.add_cluster("cluster-a")

        sim_default = UsageSimulator(te, db)
        db.set_current_cluster("default")
        db.add_account("acc", "Acc", "org", cluster="default")
        sim_default.inject_usage("acc", "user1", 100.0, cluster="default")

        db.set_current_cluster("cluster-a")
        db.add_account("acc", "Acc", "org", cluster="cluster-a")
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
        from emulator.core.database import Job

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

    def test_execute_command_with_cluster(self):
        emulator = SlurmEmulator()
        emulator.database.add_cluster("test-cluster")
        emulator.database.add_account("myacc", "Test", "org", cluster="test-cluster")

        output = emulator.execute_command("sacctmgr", ["-M", "test-cluster", "list", "accounts"])
        assert "myacc" in output

    def test_execute_command_nonexistent_cluster(self):
        emulator = SlurmEmulator()
        output = emulator.execute_command("sacctmgr", ["-M", "nope", "list", "accounts"])
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

    def test_list_accounts_shows_cluster(self):
        db = SlurmDatabase()
        te = TimeEngine()
        sacctmgr = SacctmgrEmulator(db, te)

        output = sacctmgr.handle_command(["list", "accounts"])
        assert "Cluster" in output


class TestBackwardCompatibleStateLoading:
    """Test that old state files without cluster data load correctly."""

    def test_load_old_format_state(self, tmp_path):
        import json

        # Create old format state file
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

        # Verify migration
        assert db.get_cluster("default") is not None
        assert db.get_account("test", cluster="default") is not None
        assert db.get_account("test", cluster="default").cluster == "default"

        # Verify associations migrated
        assert db.get_association("user1", "test", cluster="default") is not None

        # Verify usage records migrated
        records = db.get_usage_records(account="test", cluster="default")
        assert len(records) == 1
        assert records[0].cluster == "default"

    def test_delete_cluster_cleans_data(self):
        db = SlurmDatabase()
        db.add_cluster("temp")
        db.add_account("acc", "Acc", "org", cluster="temp")
        db.add_user("user1")
        db.add_association("user1", "acc", cluster="temp")

        te = TimeEngine()
        sim = UsageSimulator(te, db)
        sim.inject_usage("acc", "user1", 50.0, cluster="temp")

        db.delete_cluster("temp")

        assert db.get_account("acc", cluster="temp") is None
        assert db.get_association("user1", "acc", cluster="temp") is None
        records = db.get_usage_records(account="acc", cluster="temp")
        assert len(records) == 0
