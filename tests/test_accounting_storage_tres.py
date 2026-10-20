"""Extra tracked TRES (``SLURM_EMULATOR_ACCOUNTING_STORAGE_TRES``).

A real cluster tracks custom TRES by listing them in slurm.conf's
AccountingStorageTRES; sacctmgr refuses limits on any other name with
``no TRES known by type`` (slurm://src/sacctmgr/qos_functions.c#_set_rec).
"""

from __future__ import annotations

import pytest

from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


def _sacctmgr(*args: str) -> tuple[str, int]:
    db = SlurmDatabase()
    db.load_state()
    emulator = SacctmgrEmulator(db, TimeEngine())
    try:
        out = emulator.handle_command(list(args))
    except SystemExit:
        return "", 1
    return out, emulator.exit_code


@pytest.fixture
def account(state_env, monkeypatch):
    monkeypatch.delenv("SLURM_EMULATOR_ACCOUNTING_STORAGE_TRES", raising=False)
    monkeypatch.delenv("SLURM_EMULATOR_GPU_TYPES", raising=False)
    assert _sacctmgr("-i", "add", "account", "acc1")[1] == 0


def test_unlisted_tres_is_refused(account, capsys):
    out, code = _sacctmgr("-i", "modify", "account", "acc1", "set", "GrpTRESMins=cpu=1,ram=2")
    captured = capsys.readouterr()
    assert code == 1
    assert "no TRES known by type ram" in out + captured.out + captured.err


def test_listed_tres_takes_limits_and_the_next_ids(account, monkeypatch):
    monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_STORAGE_TRES", "ram, gpu,storage,ram")
    _out, code = _sacctmgr("-i", "modify", "account", "acc1", "set", "GrpTRESMins=cpu=1,ram=2")
    assert code == 0
    out, _code = _sacctmgr(
        "-n", "-P", "show", "association", "where", "account=acc1", "format=GrpTRESMins"
    )
    assert "ram=2" in out
    tres, _code = _sacctmgr("-n", "-P", "show", "tres", "format=Type,Name,ID")
    rows = [line.split("|") for line in tres.splitlines()]
    assert rows[-3:] == [["ram", "", "1004"], ["gpu", "", "1005"], ["storage", "", "1006"]]


def test_rest_lists_them_too(account, restd, auth_headers, monkeypatch):
    from emulator.slurm_version import current  # noqa: PLC0415

    monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_STORAGE_TRES", "node_hours")
    body = restd.get(f"/slurmdb/{current().api_version}/tres/", headers=auth_headers).json()
    assert ["node_hours", "", 1004] in [[t["type"], t["name"], t["id"]] for t in body["TRES"]]
