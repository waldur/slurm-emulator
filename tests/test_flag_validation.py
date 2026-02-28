"""Tests for per-command SLURM flag validation."""

import pytest
from emulator.commands.dispatcher import SlurmEmulator


class TestValidateFlags:
    """Test SlurmEmulator.validate_flags()."""

    def setup_method(self):
        self.emulator = SlurmEmulator()

    # --- sacctmgr accepts all three flags ---

    def test_sacctmgr_accepts_parsable2(self):
        self.emulator.validate_flags("sacctmgr", ["--parsable2", "list", "accounts"])

    def test_sacctmgr_accepts_noheader(self):
        self.emulator.validate_flags("sacctmgr", ["--noheader", "list", "accounts"])

    def test_sacctmgr_accepts_immediate(self):
        self.emulator.validate_flags("sacctmgr", ["--immediate", "add", "account", "x"])

    def test_sacctmgr_accepts_all_flags_together(self):
        self.emulator.validate_flags(
            "sacctmgr", ["--parsable2", "--noheader", "--immediate", "list", "accounts"]
        )

    # --- sacct accepts --parsable2 and --noheader, rejects --immediate ---

    def test_sacct_accepts_parsable2(self):
        self.emulator.validate_flags("sacct", ["--parsable2"])

    def test_sacct_accepts_noheader(self):
        self.emulator.validate_flags("sacct", ["--noheader"])

    def test_sacct_rejects_immediate(self):
        with pytest.raises(SystemExit, match="unrecognized arguments: --immediate"):
            self.emulator.validate_flags("sacct", ["--immediate"])

    def test_sacct_rejects_immediate_among_valid(self):
        with pytest.raises(SystemExit, match="--immediate"):
            self.emulator.validate_flags("sacct", ["--parsable2", "--immediate"])

    # --- scancel rejects all formatting flags ---

    def test_scancel_rejects_parsable2(self):
        with pytest.raises(SystemExit, match="unrecognized arguments: --parsable2"):
            self.emulator.validate_flags("scancel", ["--parsable2"])

    def test_scancel_rejects_noheader(self):
        with pytest.raises(SystemExit, match="--noheader"):
            self.emulator.validate_flags("scancel", ["--noheader"])

    def test_scancel_rejects_immediate(self):
        with pytest.raises(SystemExit, match="--immediate"):
            self.emulator.validate_flags("scancel", ["--immediate"])

    def test_scancel_rejects_multiple_flags(self):
        with pytest.raises(SystemExit, match="--parsable2 --noheader"):
            self.emulator.validate_flags("scancel", ["--parsable2", "--noheader"])

    # --- sinfo accepts -V only (not in _SLURM_FLAGS so it passes through) ---

    def test_sinfo_no_slurm_flags_passes(self):
        self.emulator.validate_flags("sinfo", ["-V"])

    # --- Non-SLURM flags are ignored by validation ---

    def test_non_slurm_flags_pass_through(self):
        self.emulator.validate_flags("sacct", ["--format=JobID", "--accounts=test"])

    def test_empty_args(self):
        self.emulator.validate_flags("sacctmgr", [])

    # --- Unknown command gets empty valid set ---

    def test_unknown_command_rejects_slurm_flags(self):
        with pytest.raises(SystemExit, match="--parsable2"):
            self.emulator.validate_flags("unknown_cmd", ["--parsable2"])

    def test_unknown_command_allows_non_slurm_flags(self):
        self.emulator.validate_flags("unknown_cmd", ["--some-other-flag"])

    # --- Error message format ---

    def test_error_message_includes_command_name(self):
        with pytest.raises(SystemExit, match="scancel: error:"):
            self.emulator.validate_flags("scancel", ["--immediate"])
