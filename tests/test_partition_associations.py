"""Tests for partition-aware user associations in sacctmgr.

Validates emulator parity with real Slurm
(/Users/ilja/workspace/slurm):

- ``sacctmgr add user … Partitions=p1,p2`` creates one association
  per partition (see ``_add_assoc_cond_partition`` in
  ``src/plugins/accounting_storage/mysql/as_mysql_assoc.c``).
- ``Partition=`` (singular) reaches the same handler via real
  Slurm's xstrncasecmp prefix match.
- ``DefaultPartition=`` is NOT a real ``sacctmgr add user`` option —
  real Slurm prints "Unknown option: …" and sets exit_code=1.
- ``format=partition`` is the only association partition format token
  in real Slurm (``common.c`` minimum prefix ``Part``). Plural and
  Default* variants are unknown and exit with "Unknown field 'X'".
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
    def test_single_partition_creates_one_row(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(
            ["add", "user", "alice", "account=acct1", "Partitions=zen3", "Share=parent"]
        )
        rows = em.database.list_user_associations("alice", "acct1")
        assert len(rows) == 1
        assert rows[0].partition == "zen3"

    def test_comma_joined_partitions_create_one_row_each(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(
            ["add", "user", "alice", "account=acct1", "Partitions=zen3,zen5", "Share=parent"]
        )
        rows = em.database.list_user_associations("alice", "acct1")
        partitions = sorted(r.partition for r in rows)
        assert partitions == ["zen3", "zen5"]
        # No non-partition base row — matches as_mysql_assoc.c:2869-2875
        # where _add_assoc_cond_user_internal is bypassed when
        # partition_list is non-empty.
        assert em.database.get_association("alice", "acct1", partition=None) is None

    def test_legacy_singular_partition_flag(self, tmp_path):
        """Real Slurm accepts both ``Partition=`` and ``Partitions=`` via prefix match."""
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "Partition=zen3"])
        rows = em.database.list_user_associations("alice", "acct1")
        assert len(rows) == 1
        assert rows[0].partition == "zen3"

    def test_no_partition_flag_creates_base_row(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1"])
        rows = em.database.list_user_associations("alice", "acct1")
        assert len(rows) == 1
        assert rows[0].partition is None

    def test_share_parent_silently_accepted(self, tmp_path):
        """Real sacctmgr accepts ``Share=parent`` on add user; emulator does too."""
        em = _emulator(tmp_path)
        result = em.handle_command(["add", "user", "alice", "account=acct1", "Share=parent"])
        assert "error" not in result.lower()
        assert "unknown option" not in result.lower()
        assert em.database.get_association("alice", "acct1") is not None

    def test_default_partition_is_rejected(self, tmp_path):
        """Real sacctmgr has no DefaultPartition= on add user — it errors out."""
        em = _emulator(tmp_path)
        result = em.handle_command(
            ["add", "user", "alice", "account=acct1", "DefaultPartition=zen3"]
        )
        assert "Unknown option" in result
        assert "DefaultPartition=zen3" in result
        # Real Slurm aborts the add when an unknown option appears —
        # the emulator must not silently persist a partial association.
        assert em.database.get_association("alice", "acct1") is None


class TestListAssociationsPartitionFormat:
    def test_partition_format_field(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "Partitions=zen3,zen5"])
        out = em.handle_command(["list", "associations", "format=account,user,partition"])
        # One data line per (account, user, partition) row.
        alice_lines = [line for line in out.splitlines() if "alice" in line]
        assert len(alice_lines) == 2
        partitions = sorted(line.split("|")[2] for line in alice_lines)
        assert partitions == ["zen3", "zen5"]

    def test_plural_partitions_format_rejected(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "Partitions=zen3"])
        out = em.handle_command(["list", "associations", "format=account,user,partitions"])
        assert "Unknown field 'partitions'" in out

    def test_defaultpartition_format_rejected(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "Partitions=zen3"])
        out = em.handle_command(["list", "associations", "format=account,user,defaultpartition"])
        assert "Unknown field 'defaultpartition'" in out


class TestShowAssociationPartitionFormat:
    def test_show_with_partition_format_returns_row_per_partition(self, tmp_path):
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
        lines = [line for line in out.splitlines() if line]
        assert len(lines) == 2
        partitions = sorted(line.split("|")[2] for line in lines)
        assert partitions == ["zen3", "zen5"]

    def test_show_default_format_back_compat(self, tmp_path):
        """Without format=, the legacy 11-column shape must be preserved.

        Existing site-agent parsers depend on this exact column layout.
        """
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "Partitions=zen3"])
        out = em.handle_command(["show", "association", "where", "user=alice", "account=acct1"])
        cells = out.split("|")
        assert cells[0] == "acct1"
        assert cells[1] == "alice"
        assert len(cells) >= 10

    def test_show_plural_format_rejected(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "Partitions=zen3"])
        out = em.handle_command(
            [
                "show",
                "association",
                "where",
                "user=alice",
                "account=acct1",
                "format=account,user,partitions",
            ]
        )
        assert "Unknown field 'partitions'" in out


class TestRemoveUserDropsAllPartitionRows:
    def test_remove_user_deletes_every_partition_row(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "Partitions=zen3,zen5"])
        assert len(em.database.list_user_associations("alice", "acct1")) == 2
        em.handle_command(["remove", "user", "where", "name=alice", "and", "account=acct1"])
        assert em.database.list_user_associations("alice", "acct1") == []
