"""Tests for per-association QoS grants and account QoS-list mutation in sacctmgr.

Validates emulator parity with real Slurm (see docs/slurm-parity.md):

- An association (slurmdb_assoc_rec_t, slurm://slurm/slurmdb.h#slurmdb_assoc_rec_t) carries a
  ``qos_list`` (QosLevel — the set a user may request) and ``def_qos_id``
  (DefaultQOS). ``sacctmgr add/modify user … QosLevel=… DefaultQOS=…`` sets
  them, per partition-scoped row.
- ``QosLevel`` / account ``qos`` accept the ``=`` (replace), ``+=`` (add),
  ``-=`` (remove) list operators (sacctmgr common ``_set_assoc_rec`` /
  ``slurm_addto_char_list``).
- ``sacctmgr modify account … set qos+=/qos-=/defaultqos=`` mutate the
  account QoS list and default without clobbering the whole list — the
  mechanism the site-agent needs so a pause/downscale QoS swap does not
  wipe a multi-QoS grant.

These exercise storage/round-trip only; the emulator does not gate job
submission on QoS (out of scope — real Slurm owns that decision).
"""

from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


def _emulator(tmp_path):
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    db.add_account("acct1", "Test Account", "Test Org")
    return SacctmgrEmulator(db, TimeEngine())


class TestAddUserQosGrant:
    def test_qoslevel_sets_association_qos_list(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "QosLevel=normal,boost"])
        rows = em.database.list_user_associations("alice", "acct1")
        assert len(rows) == 1
        assert rows[0].qos_list == ["normal", "boost"]

    def test_defaultqos_sets_association_def_qos(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(
            ["add", "user", "alice", "account=acct1", "QosLevel=normal,boost", "DefaultQOS=boost"]
        )
        rows = em.database.list_user_associations("alice", "acct1")
        assert rows[0].def_qos == "boost"

    def test_qos_grant_applied_to_each_partition_row(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(
            [
                "add",
                "user",
                "alice",
                "account=acct1",
                "Partitions=cpu,gpu",
                "QosLevel=normal,boost",
                "DefaultQOS=normal",
            ]
        )
        rows = em.database.list_user_associations("alice", "acct1")
        assert len(rows) == 2
        for row in rows:
            assert row.qos_list == ["normal", "boost"]
            assert row.def_qos == "normal"

    def test_qoslevel_renders_in_association_format(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(
            ["add", "user", "alice", "account=acct1", "QosLevel=normal,boost", "DefaultQOS=boost"]
        )
        out = em.handle_command(["list", "associations", "format=user,qos,defaultqos", "-n", "-P"])
        alice = [ln for ln in out.splitlines() if ln.startswith("alice")]
        assert len(alice) == 1
        _user, qos, defqos = alice[0].split("|")
        assert sorted(qos.split(",")) == ["boost", "normal"]
        assert defqos == "boost"

    def test_no_qoslevel_falls_back_to_account_qos(self, tmp_path):
        """An association with no explicit grant inherits the account QOS column."""
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1"])
        rows = em.database.list_user_associations("alice", "acct1")
        assert rows[0].qos_list == []
        out = em.handle_command(["list", "associations", "format=user,qos", "-n", "-P"])
        alice = [ln for ln in out.splitlines() if ln.startswith("alice")]
        assert alice[0].split("|")[1] == "normal"  # account default qos


class TestModifyUserQosGrant:
    def test_modify_sets_qoslevel(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1"])
        em.handle_command(
            ["modify", "user", "alice", "set", "QosLevel=normal,boost", "where", "account=acct1"]
        )
        rows = em.database.list_user_associations("alice", "acct1")
        assert rows[0].qos_list == ["normal", "boost"]

    def test_modify_qoslevel_add_operator(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "QosLevel=normal"])
        em.handle_command(
            ["modify", "user", "alice", "set", "QosLevel+=boost", "where", "account=acct1"]
        )
        rows = em.database.list_user_associations("alice", "acct1")
        assert rows[0].qos_list == ["normal", "boost"]

    def test_modify_qoslevel_remove_operator(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "QosLevel=normal,boost"])
        em.handle_command(
            ["modify", "user", "alice", "set", "QosLevel-=boost", "where", "account=acct1"]
        )
        rows = em.database.list_user_associations("alice", "acct1")
        assert rows[0].qos_list == ["normal"]

    def test_modify_sets_default_qos(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "user", "alice", "account=acct1", "QosLevel=normal,boost"])
        em.handle_command(
            ["modify", "user", "alice", "set", "DefaultQOS=boost", "where", "account=acct1"]
        )
        rows = em.database.list_user_associations("alice", "acct1")
        assert rows[0].def_qos == "boost"


class TestModifyAccountQosList:
    def test_qos_replace_still_overwrites(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["modify", "account", "acct1", "set", "qos=high"])
        assert em.database.get_account("acct1").qos == "high"

    def test_qos_add_operator_appends(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["modify", "account", "acct1", "set", "qos+=boost"])
        qos = set(em.database.get_account("acct1").qos.split(","))
        assert {"normal", "boost"} <= qos

    def test_canonical_qoslevel_name_on_account(self, tmp_path):
        # Real account modify routes through QosLevel (min prefix 1), so the
        # canonical name and its +=/-= operators must also apply.
        em = _emulator(tmp_path)
        em.handle_command(["modify", "account", "acct1", "set", "QosLevel+=boost"])
        assert "boost" in em.database.get_account("acct1").qos.split(",")

    def test_qos_remove_operator_drops_one(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["modify", "account", "acct1", "set", "qos=normal,boost,limited"])
        em.handle_command(["modify", "account", "acct1", "set", "qos-=limited"])
        qos = em.database.get_account("acct1").qos.split(",")
        assert "limited" not in qos
        assert sorted(qos) == ["boost", "normal"]

    def test_defaultqos_sets_account_default(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["modify", "account", "acct1", "set", "defaultqos=boost"])
        assert em.database.get_account("acct1").default_qos == "boost"

    def test_add_then_remove_operational_qos_preserves_grant(self, tmp_path):
        """Pause/downscale decoupling.

        Adding then removing an operational QoS with += / -= must leave the
        original granted list intact.
        """
        em = _emulator(tmp_path)
        em.handle_command(["modify", "account", "acct1", "set", "qos=normal,boost"])
        em.handle_command(["modify", "account", "acct1", "set", "qos+=paused"])
        assert "paused" in em.database.get_account("acct1").qos.split(",")
        em.handle_command(["modify", "account", "acct1", "set", "qos-=paused"])
        assert sorted(em.database.get_account("acct1").qos.split(",")) == ["boost", "normal"]


class TestQosGrantRoundTrip:
    def test_association_qos_survives_save_load(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(
            ["add", "user", "alice", "account=acct1", "QosLevel=normal,boost", "DefaultQOS=boost"]
        )
        em.database.save_state()
        reloaded = SlurmDatabase()
        reloaded.state_file = em.database.state_file
        reloaded.load_state()
        rows = reloaded.list_user_associations("alice", "acct1")
        assert rows[0].qos_list == ["normal", "boost"]
        assert rows[0].def_qos == "boost"
