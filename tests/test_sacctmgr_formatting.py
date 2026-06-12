"""Byte-level output and exit-code parity tests for the sacctmgr emulator.

Expected shapes come from real Slurm 26.11:
- header/dash rows and column padding: src/common/print_fields.c:66-176
  (every column is followed by a space, including the last);
- default field sets: src/sacctmgr/*_functions.c;
- field headers and widths: src/sacctmgr/common.c:219-891.
"""

import pytest

from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


@pytest.fixture
def em(tmp_path):
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    emulator = SacctmgrEmulator(db, TimeEngine())
    emulator.handle_command(["add", "account", "proj-a", "description=Alpha", "organization=org1"])
    emulator.handle_command(["add", "user", "alice", "account=proj-a"])
    return emulator


class TestDefaultFixedWidthMode:
    def test_list_account_header_bytes(self, em):
        out = em.handle_command(["list", "account"])
        lines = out.splitlines()
        # Account 10 | Descr 20 | Org 20, right-aligned, trailing space
        # after every column (print_fields.c:90-91).
        assert lines[0] == "   Account                Descr                  Org "
        assert lines[1] == "---------- -------------------- -------------------- "

    def test_list_account_row_alignment(self, em):
        out = em.handle_command(["list", "account"])
        assert "    proj-a                Alpha                 org1 " in out.splitlines()

    def test_list_user_header(self, em):
        out = em.handle_command(["list", "user"])
        lines = out.splitlines()
        # User 10 | "Def Acct" 10 | Admin 9 (user_functions.c:968).
        assert lines[0] == "      User   Def Acct     Admin "
        assert lines[1] == "---------- ---------- --------- "

    def test_list_cluster_default_has_no_classification(self, em):
        out = em.handle_command(["list", "cluster"])
        header = out.splitlines()[0]
        for name in ("Cluster", "ControlHost", "ControlPort", "RPC", "Share", "Def QOS"):
            assert name in header
        assert "Class" not in header

    def test_assoc_default_is_twenty_columns(self, em):
        out = em.handle_command(["list", "assoc", "-n", "-P"])
        for line in out.splitlines():
            assert len(line.split("|")) == 20

    def test_truncation_appends_plus(self, em):
        em.handle_command(["add", "account", "very-long-account-name"])
        out = em.handle_command(["list", "account"])
        # Account column is 10 wide: value[:9] + '+' (print_fields.c:147-160).
        assert "very-long+ " in out

    def test_header_only_when_no_rows(self, em):
        out = em.handle_command(["list", "qos"])
        # Header + dash row, no data rows; print_fields_header runs before
        # the (empty) list iteration in real sacctmgr.
        assert len(out.splitlines()) == 2

    def test_format_width_override(self, em):
        out = em.handle_command(["list", "account", "format=Account%5"])
        lines = out.splitlines()
        assert lines[0] == "Accou "
        assert lines[1] == "----- "


class TestParsableModes:
    def test_parsable_trailing_pipe(self, em):
        out = em.handle_command(["-p", "list", "account"])
        lines = out.splitlines()
        assert lines[0] == "Account|Descr|Org|"
        assert "proj-a|Alpha|org1|" in lines

    def test_parsable2_no_trailing_pipe(self, em):
        out = em.handle_command(["-P", "list", "account"])
        lines = out.splitlines()
        assert lines[0] == "Account|Descr|Org"
        assert "proj-a|Alpha|org1" in lines

    def test_noheader(self, em):
        out = em.handle_command(["-n", "-P", "list", "account"])
        assert "Account|Descr|Org" not in out
        assert "proj-a|Alpha|org1" in out.splitlines()

    def test_combined_short_cluster(self, em):
        combined = em.handle_command(["-nP", "list", "account"])
        separate = em.handle_command(["-n", "-P", "list", "account"])
        assert combined == separate

    def test_long_flags_equal_short(self, em):
        long_form = em.handle_command(["--noheader", "--parsable2", "list", "account"])
        short_form = em.handle_command(["-n", "-P", "list", "account"])
        assert long_form == short_form

    def test_parsable_values_not_truncated(self, em):
        em.handle_command(["add", "account", "very-long-account-name"])
        out = em.handle_command(["-n", "-P", "list", "account"])
        assert any(line.startswith("very-long-account-name|") for line in out.splitlines())


class TestFieldResolution:
    def test_prefix_match_case_insensitive(self, em):
        out = em.handle_command(["-n", "-P", "list", "account", "format=acc,des"])
        assert "proj-a|Alpha" in out.splitlines()

    def test_unknown_field_exits_one(self, em):
        out = em.handle_command(["list", "account", "format=bogus"])
        assert out == "Unknown field 'bogus'"
        assert em.exit_code == 1

    def test_def_qos_header_name(self, em):
        out = em.handle_command(["list", "cluster", "format=DefaultQOS", "-P"])
        # common.c:326-329: printed header is "Def QOS".
        assert out.splitlines()[0] == "Def QOS"


class TestExitCodesAndMessages:
    def test_nothing_modified_exits_one(self, em):
        # Real sacctmgr: the message goes to stdout (printf) but the modify
        # branch returns SLURM_ERROR and _modify_it() sets exit_code=1
        # (account_functions.c:727-729, sacctmgr.c:982-984).
        out = em.handle_command(["modify", "account", "where", "name=ghost", "set", "parent=root"])
        assert out == "  Nothing modified"
        assert em.exit_code == 1
        assert em.stdout_error is True

    def test_readd_account_reports_no_change(self, em):
        out = em.handle_command(["add", "account", "proj-a"])
        assert out == " Data has not changed since time specified"
        assert em.exit_code == 0

    def test_add_account_to_missing_cluster_exits_one(self, em):
        out = em.handle_command(["add", "account", "proj-b", "cluster=ghost"])
        assert "Cluster ghost does not exist" in out
        assert out.startswith(" error: ")
        assert em.exit_code == 1

    def test_missing_parent_exits_one(self, em):
        out = em.handle_command(["modify", "account", "where", "name=proj-a", "set", "parent=zz"])
        assert out == " Parent Account zz doesn't exist."
        assert em.exit_code == 1

    def test_error_prefix_is_real_style(self, em):
        out = em.handle_command(["remove", "account", "where", "name=ghost"])
        assert out.startswith(" error: ")
        assert em.exit_code == 1

    def test_immediate_is_accepted_noop(self, em):
        out = em.handle_command(["--immediate", "add", "account", "proj-i"])
        assert "Adding Account(s)" in out
        assert em.exit_code == 0

    def test_dash_m_is_tolerated(self, em):
        # Intentional deviation: consumers pass -M; it is ignored.
        out = em.handle_command(["-M", "default", "list", "account", "-n", "-P"])
        assert "proj-a|Alpha|org1" in out.splitlines()
        assert em.exit_code == 0
