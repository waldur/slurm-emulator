"""Tests for comma-separated multi-TRES parsing in sacctmgr modify.

TRES names must be ones slurmdbd knows and a bare number is read as a TRES
id (slurm://src/common/slurmdb_defs.c#slurmdb_format_tres_str); either
failing is ``no TRES known by ...`` plus ``Unknown option``, exit 1, as real
26.05 prints it.
"""

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
        self._modify("GrpTRESMins=cpu=600000,mem=614400")
        limits = self._limits()
        assert limits["GrpTRESMins:cpu"] == 600000
        assert limits["GrpTRESMins:mem"] == 614400

    def test_grptresmin_three_tres(self):
        self._modify("GrpTRESMins=cpu=100,mem=200,billing=300")
        limits = self._limits()
        assert limits["GrpTRESMins:cpu"] == 100
        assert limits["GrpTRESMins:mem"] == 200
        assert limits["GrpTRESMins:billing"] == 300

    def test_grptresmin_plain_numeric_is_a_tres_id(self):
        out = self._modify("GrpTRESMins=5000")
        assert out == (
            "sacctmgr: error: slurmdb_format_tres_str: no TRES known by id 5000\n"
            " Unknown option: GrpTRESMins=5000\n Use keyword 'where' to modify condition"
        )
        assert self.sacctmgr.exit_code == 1
        assert "GrpTRESMins" not in self._limits()

    def test_unknown_tres_type_is_refused(self):
        out = self._modify("GrpTRESMins=ram=1")
        assert out.startswith(
            "sacctmgr: error: slurmdb_format_tres_str: no TRES known by type ram\n"
        )
        assert self.sacctmgr.exit_code == 1

    # --- MaxTRESMins ---

    def test_maxtresmin_single_tres(self):
        self._modify("MaxTRESMins=billing=72000")
        assert self._limits()["MaxTRESMins:billing"] == 72000

    def test_maxtresmin_multi_tres(self):
        self._modify("MaxTRESMins=cpu=600000,mem=614400")
        limits = self._limits()
        assert limits["MaxTRESMins:cpu"] == 600000
        assert limits["MaxTRESMins:mem"] == 614400

    def test_maxtresmin_plain_numeric_is_a_tres_id(self):
        self._modify("MaxTRESMins=5000")
        assert self.sacctmgr.exit_code == 1
        assert "MaxTRESMins" not in self._limits()

    # --- GrpTRES (concurrent limits) ---

    def test_grptres_single_tres(self):
        self._modify("GrpTRES=cpu=10")
        assert self._limits()["GrpTRES:cpu"] == 10

    def test_grptres_multi_tres(self):
        self._modify("GrpTRES=cpu=10,node=5")
        limits = self._limits()
        assert limits["GrpTRES:cpu"] == 10
        assert limits["GrpTRES:node"] == 5

    def test_grptres_plain_numeric_is_a_tres_id(self):
        self._modify("GrpTRES=42")
        assert self.sacctmgr.exit_code == 1
        assert "GrpTRES" not in self._limits()

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

        self._modify("GrpTRESMins=cpu=200,mem=300")
        limits = self._limits()
        assert limits["GrpTRESMins:cpu"] == 200
        assert limits["GrpTRESMins:mem"] == 300

    # --- Output message ---

    def test_modify_output_lists_the_account_row(self):
        # slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_process_modify_assoc_results
        result = self._modify("GrpTRESMins=cpu=600000,mem=614400")
        assert result == " Modified account associations...\n  C = default    A = acct1 of root"
