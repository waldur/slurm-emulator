"""Tests for account GrpSubmitJobs — the orthogonal pause lever.

The Waldur site agent pauses a QoS-enforcing allocation with
``sacctmgr modify account <a> set GrpSubmitJobs=0`` (block new submissions
without touching the QoS grant) and restores it with ``GrpSubmitJobs=-1``
(clear the limit, matching real sacctmgr where a negative value removes it).
"""

from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


def _emulator(tmp_path):
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    db.add_account("acct1", "Test Account", "Test Org")
    db.add_user("alice", "acct1")
    db.add_association("alice", "acct1")
    return SacctmgrEmulator(db, TimeEngine())


class TestGrpSubmitJobs:
    def test_set_blocks_submission(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["modify", "account", "acct1", "set", "GrpSubmitJobs=0"])
        assert em.database.get_account("acct1").limits["GrpSubmitJobs"] == 0

    def test_negative_clears_limit(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["modify", "account", "acct1", "set", "GrpSubmitJobs=0"])
        em.handle_command(["modify", "account", "acct1", "set", "GrpSubmitJobs=-1"])
        assert "GrpSubmitJobs" not in em.database.get_account("acct1").limits

    def test_rendered_in_association_format(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["modify", "account", "acct1", "set", "GrpSubmitJobs=0"])
        out = em.handle_command(
            ["list", "associations", "format=account,grpsubmitjobs", "-n", "-P"]
        )
        rows = [ln for ln in out.splitlines() if ln.startswith("acct1")]
        assert rows
        assert any(ln.split("|")[1] == "0" for ln in rows)

    def test_survives_save_load(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["modify", "account", "acct1", "set", "GrpSubmitJobs=0"])
        em.database.save_state()
        reloaded = SlurmDatabase()
        reloaded.state_file = em.database.state_file
        reloaded.load_state()
        assert reloaded.get_account("acct1").limits["GrpSubmitJobs"] == 0

    def test_empty_value_parses_as_zero(self, tmp_path):
        # Real sacctmgr get_uint: strtol("") == 0 (a valid cap), not a crash.
        em = _emulator(tmp_path)
        em.handle_command(["modify", "account", "acct1", "set", "GrpSubmitJobs="])
        assert em.database.get_account("acct1").limits["GrpSubmitJobs"] == 0

    def test_non_numeric_value_is_graceful_error(self, tmp_path):
        # Real sacctmgr prints " error: Invalid value ..." and exits 1 rather
        # than crashing; the emulator must not raise an uncaught ValueError.
        em = _emulator(tmp_path)
        out = em.handle_command(["modify", "account", "acct1", "set", "GrpSubmitJobs=abc"])
        assert "Invalid value" in out
        assert em.exit_code == 1
        assert "GrpSubmitJobs" not in em.database.get_account("acct1").limits
