"""Tests for sshare command emulation aligned with real Slurm semantics."""

from __future__ import annotations

import pytest

from emulator.commands.dispatcher import SlurmEmulator
from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.commands.sshare import SshareEmulator
from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine


def _add_usage(
    db: SlurmDatabase,
    te: TimeEngine,
    *,
    account: str,
    user: str,
    node_hours: float,
    billing_units: float | None = None,
    raw_tres: dict | None = None,
    cluster: str = "default",
) -> None:
    db.add_usage_record(
        UsageRecord(
            account=account,
            user=user,
            node_hours=node_hours,
            billing_units=node_hours if billing_units is None else billing_units,
            timestamp=te.get_current_time(),
            period=te.get_current_quarter(),
            raw_tres=raw_tres or {},
            cluster=cluster,
        )
    )


class TestSshareDefaultsAndFlags:
    def setup_method(self):
        self.db = SlurmDatabase()
        self.te = TimeEngine()
        self.sshare = SshareEmulator(self.db, self.te)
        self.db.add_account("acct1", "Account 1", "Org")
        self.db.add_user("user1", default_account="acct1")
        self.db.add_association("user1", "acct1")

    def test_default_format_has_real_slurm_columns(self):
        output = self.sshare.handle_command([])
        header = output.splitlines()[0]
        # Real-Slurm default (non-fair-tree, non-long, non-partition).
        for col in (
            "Account",
            "User",
            "RawShares",
            "NormShares",
            "RawUsage",
            "EffectvUsage",
            "FairShare",
        ):
            assert col in header

    def test_noheader_suppresses_header(self):
        output = self.sshare.handle_command(["--noheader"])
        assert "Account" not in output.splitlines()[0]

    def test_parsable_modes_differ_by_trailing_pipe(self):
        # Real Slurm: -p appends "|" after the last cell, -P does not.
        # Use a format whose final cell is non-empty so the comparison
        # isn't masked by an empty trailing column.
        p_out = self.sshare.handle_command(["-p", "-n", "-o", "Account,RawShares"])
        big_p_out = self.sshare.handle_command(["-P", "-n", "-o", "Account,RawShares"])
        for line in p_out.splitlines():
            assert line.endswith("|")
        for line in big_p_out.splitlines():
            assert not line.endswith("|")

    def test_long_flag_adds_extra_columns(self):
        output = self.sshare.handle_command(["-l"])
        header = output.splitlines()[0]
        for col in ("NormUsage", "GrpTRESMins", "TRESRunMins"):
            assert col in header

    def test_partition_flag_inserts_partition_column(self):
        output = self.sshare.handle_command(["-m"])
        header = output.splitlines()[0]
        assert "Partition" in header


class TestSshareGrpTRESRawUnits:
    """GrpTRESRaw is rendered in TRES-minutes (real Slurm: usage/60)."""

    def setup_method(self):
        self.db = SlurmDatabase()
        self.te = TimeEngine()
        self.sshare = SshareEmulator(self.db, self.te)
        self.db.add_account("acct1", "Account 1", "Org")

    def test_grptresraw_in_minutes(self):
        # 10 node-hours and 20 billing-hours: parent row should show
        # node=600, billing=1200, cpu=38400 (640*60), mem=307200 (5120*60),
        # gres/gpu=2400 (40*60). Energy stays in the canonical set as 6000.
        _add_usage(
            self.db,
            self.te,
            account="acct1",
            user="user1",
            node_hours=10.0,
            billing_units=20.0,
            raw_tres={"CPU": 640, "Mem": 5120, "GRES/gpu": 40, "Energy": 100},
        )
        self.db.add_association("user1", "acct1")

        output = self.sshare.handle_command(
            ["--accounts=acct1", "-o", "Account,GrpTRESRaw", "-P", "-n"]
        )
        parent = output.splitlines()[0]
        _, tres = parent.split("|")
        values = dict(item.split("=") for item in tres.split(","))

        assert values["billing"] == "1200"
        assert values["node"] == "600"
        assert values["cpu"] == str(640 * 60)
        assert values["mem"] == str(5120 * 60)
        assert values["gres/gpu"] == str(40 * 60)
        assert values["energy"] == str(100 * 60)
        # Real Slurm keeps zeros in the array (TRES_STR_FLAG_REMOVE only
        # filters INFINITE64 entries).
        for zero_key in ("fs/disk", "vmem", "pages"):
            assert values[zero_key] == "0"

    def test_grptresraw_filters_current_cluster(self):
        self.db.add_cluster("prod")
        _add_usage(
            self.db,
            self.te,
            account="acct1",
            user="user1",
            node_hours=10.0,
            raw_tres={"CPU": 640},
            cluster="default",
        )
        _add_usage(
            self.db,
            self.te,
            account="acct1",
            user="user1",
            node_hours=3.0,
            billing_units=9.0,
            raw_tres={"CPU": 192},
            cluster="prod",
        )
        self.db.current_cluster = "prod"
        self.db.add_association("user1", "acct1", cluster="prod")

        output = self.sshare.handle_command(
            ["--accounts=acct1", "-o", "Account,GrpTRESRaw", "-P", "-n"]
        )
        parent_tres = dict(
            item.split("=") for item in output.splitlines()[0].split("|")[1].split(",")
        )
        assert parent_tres["billing"] == str(9 * 60)
        assert parent_tres["node"] == str(3 * 60)
        assert parent_tres["cpu"] == str(192 * 60)


class TestSshareAssociations:
    """One row per association: parent account row + per-user child rows."""

    def setup_method(self):
        self.db = SlurmDatabase()
        self.te = TimeEngine()
        self.sshare = SshareEmulator(self.db, self.te)
        self.db.add_account("acct1", "Account 1", "Org")
        self.db.add_account("acct2", "Account 2", "Org")
        for user in ("u1", "u2"):
            self.db.add_user(user, default_account="acct1")
            self.db.add_association(user, "acct1")
        _add_usage(self.db, self.te, account="acct1", user="u1", node_hours=2.0)
        _add_usage(self.db, self.te, account="acct1", user="u2", node_hours=8.0)

    def test_emits_parent_and_per_user_rows(self):
        output = self.sshare.handle_command(
            ["--accounts=acct1", "-o", "Account,User,RawUsage", "-P", "-n"]
        )
        lines = output.splitlines()
        assert len(lines) == 3  # parent + u1 + u2

        parent = lines[0].split("|")
        assert parent[0] == "acct1"
        assert parent[1] == ""  # parent has no User
        assert parent[2] == str((2 + 8) * 3600)

        child_rows = [line.split("|") for line in lines[1:]]
        users = {row[1] for row in child_rows}
        assert users == {"u1", "u2"}
        for row in child_rows:
            assert row[0].strip() == "acct1"  # leading-space tree indent

    def test_users_only_filters_out_parent_row(self):
        output = self.sshare.handle_command(
            ["--accounts=acct1", "-o", "Account,User", "-U", "-P", "-n"]
        )
        lines = output.splitlines()
        assert len(lines) == 2
        for line in lines:
            assert line.split("|")[1] in {"u1", "u2"}

    def test_user_filter_limits_child_rows(self):
        output = self.sshare.handle_command(
            ["--accounts=acct1", "-u", "u1", "-o", "Account,User", "-P", "-n"]
        )
        lines = output.splitlines()
        users = {line.split("|")[1] for line in lines}
        assert users == {"", "u1"}


class TestSshareGrpTRESMins:
    def setup_method(self):
        self.db = SlurmDatabase()
        self.te = TimeEngine()
        self.sacctmgr = SacctmgrEmulator(self.db, self.te)
        self.sshare = SshareEmulator(self.db, self.te)
        self.db.add_account("acct1", "Account 1", "Org")
        self.db.add_user("user1", default_account="acct1")
        self.db.add_association("user1", "acct1")

    def test_parent_row_carries_limits_child_row_is_blank(self):
        self.sacctmgr.handle_command(
            ["modify", "account", "acct1", "set", "GrpTRESMins=billing=72000,node=1200"]
        )

        output = self.sshare.handle_command(
            ["--accounts=acct1", "-o", "Account,User,GrpTRESMins", "-P", "-n"]
        )
        parent, child = output.splitlines()
        parent_tres = dict(item.split("=") for item in parent.split("|")[2].split(","))
        assert parent_tres["billing"] == "72000"
        assert parent_tres["node"] == "1200"
        # Child row's GrpTRESMins is blank (real Slurm fills tres_grp_mins
        # only on association rows; account-level limits live on parent).
        assert child.split("|")[2] == ""


class TestSshareUnknownField:
    def setup_method(self):
        self.db = SlurmDatabase()
        self.te = TimeEngine()
        self.sshare = SshareEmulator(self.db, self.te)
        self.db.add_account("acct1", "Account 1", "Org")

    def test_unknown_field_exits_with_error(self, capsys):
        with pytest.raises(SystemExit) as excinfo:
            self.sshare.handle_command(["-o", "Account,Bogus"])
        assert excinfo.value.code == 1
        captured = capsys.readouterr()
        assert "Invalid field requested" in captured.err
        assert '"Bogus"' in captured.err


class TestSshareFieldPrefixMatching:
    def setup_method(self):
        self.db = SlurmDatabase()
        self.te = TimeEngine()
        self.sshare = SshareEmulator(self.db, self.te)
        self.db.add_account("acct1", "Account 1", "Org")

    def test_prefix_resolves_to_canonical_name(self):
        output = self.sshare.handle_command(["-o", "A,Use,GrpTRESR"])
        header = output.splitlines()[0]
        assert "Account" in header
        assert "User" in header
        assert "GrpTRESRaw" in header


class TestSshareMultiCluster:
    def setup_method(self):
        self.db = SlurmDatabase()
        self.te = TimeEngine()
        self.sshare = SshareEmulator(self.db, self.te)
        self.db.add_cluster("c1")
        self.db.add_cluster("c2")
        self.db.add_account("acct1", "Account 1", "Org")

    def test_multi_cluster_emits_banner_per_cluster(self):
        output = self.sshare.handle_command(["-M", "c1,c2", "-A", "acct1", "-P", "-n"])
        assert "CLUSTER: c1" in output
        assert "CLUSTER: c2" in output
        # Blank line separator between blocks.
        assert "\n\nCLUSTER: c2" in output


class TestSshareAccumulatingFlags:
    def setup_method(self):
        self.db = SlurmDatabase()
        self.te = TimeEngine()
        self.sshare = SshareEmulator(self.db, self.te)
        self.db.add_account("acct1", "Account 1", "Org")
        self.db.add_account("acct2", "Account 2", "Org")

    def test_repeated_accounts_flag_accumulates(self):
        output = self.sshare.handle_command(
            ["-A", "acct1", "-A", "acct2", "-o", "Account", "-P", "-n"]
        )
        accounts = {line.strip() for line in output.splitlines()}
        assert {"acct1", "acct2"}.issubset(accounts)

    def test_repeated_users_flag_accumulates(self):
        for user in ("u1", "u2"):
            self.db.add_user(user, default_account="acct1")
            self.db.add_association(user, "acct1")
            _add_usage(self.db, self.te, account="acct1", user=user, node_hours=1.0)
        output = self.sshare.handle_command(
            ["-A", "acct1", "-u", "u1", "-u", "u2", "-o", "Account,User", "-P", "-n"]
        )
        users = {line.split("|")[1] for line in output.splitlines()}
        assert users == {"", "u1", "u2"}


class TestSshareDispatcherIntegration:
    """End-to-end via SlurmEmulator.execute_command."""

    def test_execute_command_routes_to_sshare(self):
        emulator = SlurmEmulator()
        output = emulator.execute_command("sshare", ["-P", "-n"])
        # No exception and at least the header is suppressed.
        assert "Account" not in output.splitlines()[0] if output else True
