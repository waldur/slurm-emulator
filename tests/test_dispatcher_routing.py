"""Dispatcher-level stdout/stderr routing and exit-code propagation.

Real sacctmgr writes errors to stderr and exits 1; normal output goes
to stdout with exit 0. The emulator returns one message per command, so
sacctmgr_main routes by the recorded exit code.
"""

import sys

import pytest

from emulator.commands import dispatcher


@pytest.fixture
def fresh_emulator(tmp_path, monkeypatch):
    em = dispatcher.SlurmEmulator()
    em.database.state_file = tmp_path / "state.json"
    monkeypatch.setattr(dispatcher, "_emulator", em)
    return em


def _run_main(monkeypatch, main, argv):
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as exc:
        main()
    return exc.value.code or 0


class TestSacctmgrRouting:
    def test_error_goes_to_stderr_exit_one(self, fresh_emulator, monkeypatch, capsys):
        code = _run_main(
            monkeypatch,
            dispatcher.sacctmgr_main,
            ["sacctmgr", "remove", "account", "where"],
        )
        captured = capsys.readouterr()
        assert code == 1
        assert " error: " in captured.err
        assert captured.out == ""

    def test_success_goes_to_stdout_exit_zero(self, fresh_emulator, monkeypatch, capsys):
        code = _run_main(
            monkeypatch,
            dispatcher.sacctmgr_main,
            ["sacctmgr", "list", "account", "-n", "-P"],
        )
        captured = capsys.readouterr()
        assert code == 0
        assert captured.err == ""

    def test_nothing_modified_stdout_exit_one(self, fresh_emulator, monkeypatch, capsys):
        # Real sacctmgr prints "Nothing modified" with printf (stdout) but
        # exits 1 (_modify_it() sets exit_code on SLURM_ERROR).
        code = _run_main(
            monkeypatch,
            dispatcher.sacctmgr_main,
            ["sacctmgr", "modify", "account", "where", "name=ghost-xyz", "set", "parent=root"],
        )
        captured = capsys.readouterr()
        assert code == 1
        assert "Nothing modified" in captured.out
        assert captured.err == ""


class TestSacctRouting:
    def test_exit_code_propagated(self, fresh_emulator, monkeypatch, capsys):
        code = _run_main(monkeypatch, dispatcher.sacct_main, ["sacct", "-o", "Bogus"])
        assert code == 1
        assert "Invalid field requested" in capsys.readouterr().err

    def test_success_exit_zero(self, fresh_emulator, monkeypatch, capsys):
        code = _run_main(monkeypatch, dispatcher.sacct_main, ["sacct", "-n", "-P"])
        assert code == 0
