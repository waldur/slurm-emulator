"""Tests for sshare command emulation."""

from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.commands.sshare import SshareEmulator
from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine


class TestSshareEmulator:
    def setup_method(self):
        self.database = SlurmDatabase()
        self.time_engine = TimeEngine()
        self.sacctmgr = SacctmgrEmulator(self.database, self.time_engine)
        self.sshare = SshareEmulator(self.database, self.time_engine)
        self.database.add_account("acct1", "Account 1", "Org")

    def test_grptresraw_aggregates_usage_for_account(self):
        self.database.add_usage_record(
            UsageRecord(
                account="acct1",
                user="user1",
                node_hours=10.0,
                billing_units=20.0,
                timestamp=self.time_engine.get_current_time(),
                period=self.time_engine.get_current_quarter(),
                raw_tres={"CPU": 640, "Mem": 5120, "GRES/gpu": 40, "Energy": 100},
            )
        )
        self.database.add_usage_record(
            UsageRecord(
                account="acct1",
                user="user2",
                node_hours=2.0,
                billing_units=4.0,
                timestamp=self.time_engine.get_current_time(),
                period=self.time_engine.get_current_quarter(),
                raw_tres={"CPU": 128, "Mem": 1024},
            )
        )

        output = self.sshare.handle_command(
            ["--accounts=acct1", "--format=Account,GrpTRESRaw"]
        )

        assert output == "acct1|billing=24,node=12,cpu=768,mem=6144,gres/gpu=40"
        assert "Energy" not in output

    def test_grptresraw_filters_current_cluster(self):
        self.database.add_cluster("prod")
        self.database.add_usage_record(
            UsageRecord(
                account="acct1",
                user="user1",
                node_hours=10.0,
                billing_units=10.0,
                timestamp=self.time_engine.get_current_time(),
                period=self.time_engine.get_current_quarter(),
                raw_tres={"CPU": 640},
                cluster="default",
            )
        )
        self.database.add_usage_record(
            UsageRecord(
                account="acct1",
                user="user1",
                node_hours=3.0,
                billing_units=9.0,
                timestamp=self.time_engine.get_current_time(),
                period=self.time_engine.get_current_quarter(),
                raw_tres={"CPU": 192},
                cluster="prod",
            )
        )
        self.database.current_cluster = "prod"

        output = self.sshare.handle_command(
            ["--accounts=acct1", "--format=Account,GrpTRESRaw"]
        )

        assert output == "acct1|billing=9,node=3,cpu=192"

    def test_grptresmins_returns_configured_limits(self):
        self.sacctmgr.handle_command(
            ["modify", "account", "acct1", "set", "GrpTRESMins=billing=72000,node=1200"]
        )

        output = self.sshare.handle_command(
            ["--accounts=acct1", "--format=Account,GrpTRESMins"]
        )

        assert output == "acct1|billing=72000,node=1200"

    def test_grptresmin_alias_returns_configured_limits(self):
        self.sacctmgr.handle_command(
            ["modify", "account", "acct1", "set", "GrpTRESMins=cpu=600000,mem=614400"]
        )

        output = self.sshare.handle_command(
            ["--accounts=acct1", "--format=Account,GrpTRESMin"]
        )

        assert output == "acct1|cpu=600000,mem=614400"
