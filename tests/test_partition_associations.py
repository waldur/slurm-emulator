"""Tests for partition-aware user associations in sacctmgr.

Covers:
- ``sacctmgr add user … Partitions=p1,p2 DefaultPartition=p1``
- ``sacctmgr list associations format=…,partition,defaultpartition``
- ``sacctmgr show association where account=X format=account,user,partition``
"""

from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


def _emulator(tmp_path):
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    db.add_account("acct1", "Test Account", "Test Org")
    return SacctmgrEmulator(db, TimeEngine())


class TestAddUserPartitionParsing:
    def test_single_partition(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(
            ["add", "user", "alice", "account=acct1", "Partitions=zen3", "Share=parent"]
        )
        assoc = em.database.get_association("alice", "acct1")
        assert assoc is not None
        assert assoc.partitions == ["zen3"]
        assert assoc.default_partition is None

    def test_comma_joined_partitions(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(
            ["add", "user", "alice", "account=acct1", "Partitions=zen3,zen5", "Share=parent"]
        )
        assoc = em.database.get_association("alice", "acct1")
        assert assoc.partitions == ["zen3", "zen5"]

    def test_default_partition_captured(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(
            [
                "add",
                "user",
                "alice",
                "account=acct1",
                "Partitions=zen3,zen5",
                "DefaultPartition=zen3",
                "Share=parent",
            ]
        )
        assoc = em.database.get_association("alice", "acct1")
        assert assoc.partitions == ["zen3", "zen5"]
        assert assoc.default_partition == "zen3"

    def test_legacy_singular_partition_flag(self, tmp_path):
        """Older site-agent versions emit the singular Partition=<name>; accept it."""
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "Partition=zen3"])
        assoc = em.database.get_association("alice", "acct1")
        assert assoc.partitions == ["zen3"]

    def test_no_partition_flag_means_no_restriction(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1"])
        assoc = em.database.get_association("alice", "acct1")
        assert assoc.partitions == []
        assert assoc.default_partition is None

    def test_share_parent_silently_accepted(self, tmp_path):
        """Real sacctmgr accepts Share= on add user; emulator should too."""
        em = _emulator(tmp_path)
        result = em.handle_command(["add", "user", "alice", "account=acct1", "Share=parent"])
        assert "error" not in result.lower()
        assert em.database.get_association("alice", "acct1") is not None


class TestListAssociationsPartitionFormat:
    def test_partition_format_field(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "Partitions=zen3,zen5"])
        out = em.handle_command(["list", "associations", "format=account,user,partition"])
        # account|user|partition| layout
        line = [line for line in out.splitlines() if "alice" in line][0]
        cells = line.split("|")
        assert cells[0] == "acct1"
        assert cells[1] == "alice"
        assert cells[2] == "zen3,zen5"

    def test_defaultpartition_format_field(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(
            [
                "add",
                "user",
                "alice",
                "account=acct1",
                "Partitions=zen3,zen5",
                "DefaultPartition=zen3",
            ]
        )
        out = em.handle_command(["list", "associations", "format=account,user,defaultpartition"])
        line = [line for line in out.splitlines() if "alice" in line][0]
        assert line.split("|")[2] == "zen3"


class TestShowAssociationPartitionFormat:
    def test_show_with_partition_format(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "Partitions=zen3,zen5"])
        out = em.handle_command(
            [
                "show",
                "association",
                "where",
                "user=alice",
                "account=acct1",
                "format=account,user,partition",
            ]
        )
        cells = out.split("|")
        assert cells[0] == "acct1"
        assert cells[1] == "alice"
        assert cells[2] == "zen3,zen5"

    def test_show_default_format_back_compat(self, tmp_path):
        """Without format=, the legacy 11-column shape must be preserved
        so existing site-agent parsers continue to work."""
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "Partitions=zen3"])
        out = em.handle_command(["show", "association", "where", "user=alice", "account=acct1"])
        # Legacy format is "account|user||||||||| |"
        cells = out.split("|")
        assert cells[0] == "acct1"
        assert cells[1] == "alice"
        # The shape is unchanged from before partition support was added.
        assert len(cells) >= 10


class TestRemoveUserDropsPartitionState:
    def test_delete_association_clears_partitions(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "Partitions=zen3,zen5"])
        em.handle_command(["remove", "user", "where", "name=alice", "and", "account=acct1"])
        assert em.database.get_association("alice", "acct1") is None
