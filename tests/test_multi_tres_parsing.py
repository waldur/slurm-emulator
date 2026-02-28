"""Tests for comma-separated multi-TRES parsing in sacctmgr modify."""

from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


class TestMultiTresParsing:
    """Test that sacctmgr modify parses comma-separated TRES specs."""

    def setup_method(self):
        self.db = SlurmDatabase()
        self.time_engine = TimeEngine()
        self.sacctmgr = SacctmgrEmulator(self.db, self.time_engine)
        self.db.add_account("acct1", "Test Account", "Org")

    def _modify(self, *set_args: str) -> str:
        return self.sacctmgr.handle_command(["modify", "account", "acct1", "set", *set_args])

    def _limits(self) -> dict:
        return self.db.get_account("acct1").limits

    # --- GrpTRESMins ---

    def test_grptresmin_single_tres(self):
        self._modify("GrpTRESMins=billing=72000")
        assert self._limits()["GrpTRESMins:billing"] == 72000

    def test_grptresmin_multi_tres(self):
        self._modify("GrpTRESMins=cpu=600000,ram=614400")
        limits = self._limits()
        assert limits["GrpTRESMins:cpu"] == 600000
        assert limits["GrpTRESMins:ram"] == 614400

    def test_grptresmin_three_tres(self):
        self._modify("GrpTRESMins=cpu=100,mem=200,billing=300")
        limits = self._limits()
        assert limits["GrpTRESMins:cpu"] == 100
        assert limits["GrpTRESMins:mem"] == 200
        assert limits["GrpTRESMins:billing"] == 300

    def test_grptresmin_plain_numeric(self):
        self._modify("GrpTRESMins=5000")
        assert self._limits()["GrpTRESMins"] == 5000

    # --- MaxTRESMins ---

    def test_maxtresmin_single_tres(self):
        self._modify("MaxTRESMins=billing=72000")
        assert self._limits()["MaxTRESMins:billing"] == 72000

    def test_maxtresmin_multi_tres(self):
        self._modify("MaxTRESMins=cpu=600000,ram=614400")
        limits = self._limits()
        assert limits["MaxTRESMins:cpu"] == 600000
        assert limits["MaxTRESMins:ram"] == 614400

    def test_maxtresmin_plain_numeric(self):
        self._modify("MaxTRESMins=5000")
        assert self._limits()["MaxTRESMins"] == 5000

    # --- GrpTRES (concurrent limits) ---

    def test_grptres_single_tres(self):
        self._modify("GrpTRES=cpu=10")
        assert self._limits()["GrpTRES:cpu"] == 10

    def test_grptres_multi_tres(self):
        self._modify("GrpTRES=cpu=10,node=5")
        limits = self._limits()
        assert limits["GrpTRES:cpu"] == 10
        assert limits["GrpTRES:node"] == 5

    def test_grptres_plain_numeric(self):
        self._modify("GrpTRES=42")
        assert self._limits()["GrpTRES"] == 42

    # --- Multiple set args in one call ---

    def test_mixed_tres_types_in_one_modify(self):
        self._modify("GrpTRESMins=billing=72000", "GrpTRES=cpu=10,node=5")
        limits = self._limits()
        assert limits["GrpTRESMins:billing"] == 72000
        assert limits["GrpTRES:cpu"] == 10
        assert limits["GrpTRES:node"] == 5

    # --- Overwrite behavior ---

    def test_multi_tres_overwrites_previous(self):
        self._modify("GrpTRESMins=cpu=100")
        assert self._limits()["GrpTRESMins:cpu"] == 100

        self._modify("GrpTRESMins=cpu=200,ram=300")
        limits = self._limits()
        assert limits["GrpTRESMins:cpu"] == 200
        assert limits["GrpTRESMins:ram"] == 300

    # --- Output message ---

    def test_modify_output_mentions_tres(self):
        result = self._modify("GrpTRESMins=cpu=600000,ram=614400")
        assert "GrpTRESMins=" in result
        assert "acct1" in result
