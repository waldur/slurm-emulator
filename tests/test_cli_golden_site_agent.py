"""sacctmgr/sinfo command lines the waldur-site-agent runs, replayed against what real Slurm 26.05 printed.

``tests/data/slurm_26.05_site_agent_cli.json`` holds the argv the SLURM plugin
sends and the stdout, stderr and exit status of real Slurm 26.05 commands
(slurmctld + slurmdbd, see its ``source``), recorded in order on a fresh
cluster by ``scripts/real_slurmrestd/record_cli.py``. Each case runs the
emulator's console entry point as a fresh process would (a new emulator per
command, state shared through the state file) and must match exactly; only
the release in ``slurm <release>`` is the emulator's own.

slurm://src/sacctmgr/account_functions.c#sacctmgr_add_account,
slurm://src/sacctmgr/user_functions.c#sacctmgr_add_user,
slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_modify_child_assocs
"""

from __future__ import annotations

import contextlib
import io
import json
import sys
from pathlib import Path

import pytest

from emulator.commands import dispatcher
from emulator.slurm_version import at_least, current

GOLDEN = json.loads(
    (Path(__file__).parent / "data" / "slurm_26.05_site_agent_cli.json").read_text()
)
REAL_RELEASE = "slurm 26.05.6-0pre1"

pytestmark = pytest.mark.skipif(
    not at_least("26.05") or at_least("master"), reason="recorded on Slurm 26.05"
)


@pytest.fixture
def demo(state_env, monkeypatch):  # noqa: ARG001 - fixture sets the state env vars
    monkeypatch.setenv("SLURM_EMULATOR_CLUSTER_NAME", "demo")
    monkeypatch.setenv("SLURM_EMULATOR_PARTITIONS", "debug:1-2,gpu:3-4")
    monkeypatch.setenv("USER", "root")
    monkeypatch.delenv("SLURM_EMULATOR_NSS", raising=False)
    monkeypatch.setattr(dispatcher, "_emulator", None)


def _run(argv: list[str]) -> dict:
    out, err = io.StringIO(), io.StringIO()
    code = 0
    saved = sys.argv
    sys.argv = list(argv)
    dispatcher._emulator = None
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                getattr(dispatcher, f"{argv[0]}_main")()
            except SystemExit as e:
                code = e.code or 0
    finally:
        sys.argv = saved
    return {"stdout": out.getvalue(), "stderr": err.getvalue(), "exit": code}


def _expected(case: dict) -> dict:
    expect = dict(case["expect"])
    expect["stdout"] = expect["stdout"].replace(REAL_RELEASE, f"slurm {current().release}")
    return expect


def test_site_agent_commands_match_real_slurm(demo):
    mismatches = []
    for case in GOLDEN["cases"]:
        got = _run(case["argv"])
        if got != _expected(case):
            mismatches.append({"case": case["name"], "real": _expected(case), "emulator": got})
    assert mismatches == [], json.dumps(mismatches, indent=1)
