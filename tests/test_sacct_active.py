"""Pending and running jobs in accounting (slurmdbd holds them from submission)."""

from __future__ import annotations

from datetime import timedelta

from emulator.commands.sacct import SacctEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine
from tests.test_cluster_model import T0, V, set_time, submit
from tests.test_cluster_model import sim as sim  # noqa: F401 - fixture

FIELDS = "JobID,State,Start,End,Elapsed,NNodes,AllocNodes,NodeList,AllocCPUS,ReqTRES,AllocTRES"


def sacct(*args):
    db = SlurmDatabase()
    db.load_state()
    out = SacctEmulator(db, TimeEngine()).handle_command(list(args))
    return [dict(zip(FIELDS.split(","), line.split("|"))) for line in out.splitlines()]


def test_running_and_pending_rows(restd, auth_headers, sim):
    running = submit(
        restd, auth_headers, name="r", partition="debug", nodes="4", shared=["none"], time_limit=600
    )
    pending = submit(restd, auth_headers, name="p", partition="debug", nodes="2", time_limit=60)
    set_time(T0 + timedelta(minutes=11))
    rows = {r["JobID"]: r for r in sacct("-n", "-P", "-o", FIELDS)}
    run = rows[str(running)]
    assert run["State"] == "RUNNING"
    assert run["Start"] == "2026-10-17T09:01:00" and run["End"] == "Unknown"
    assert run["Elapsed"] == "00:10:00"
    assert (run["NNodes"], run["AllocNodes"], run["NodeList"]) == ("4", "4", "node[001-004]")
    assert run["AllocCPUS"] == "256" and run["AllocTRES"].startswith("cpu=256,")
    pend = rows[str(pending)]
    assert pend["State"] == "PENDING"
    assert (pend["Start"], pend["End"], pend["Elapsed"]) == ("Unknown", "Unknown", "00:00:00")
    assert (pend["NNodes"], pend["AllocNodes"], pend["NodeList"]) == ("2", "0", "None assigned")
    assert pend["AllocCPUS"] == "0" and pend["AllocTRES"] == ""
    assert pend["ReqTRES"] == "cpu=2,mem=16G,node=2,billing=2"


def test_window_in_the_past_leaves_them_out(restd, auth_headers, sim):
    submit(restd, auth_headers, name="r", partition="debug", time_limit=600)
    set_time(T0 + timedelta(minutes=5))
    assert sacct("-n", "-P", "-o", FIELDS, "-S", "2026-10-16", "-E", "2026-10-16T12:00:00") == []


def test_slurmdb_rest_carries_running_job(restd, auth_headers, sim):
    jid = submit(restd, auth_headers, name="r", partition="debug", time_limit=600)
    set_time(T0 + timedelta(minutes=5))
    job = restd.get(f"/slurmdb/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
    assert job["state"]["current"] == ["RUNNING"]
    assert job["time"]["end"] == 0 and job["time"]["elapsed"] == 4 * 60
    assert job["nodes"] == "node001" and job["steps"] == []


def test_default_stdout_and_expanded_paths(restd, auth_headers, sim):
    import pytest  # noqa: PLC0415

    from emulator.slurm_version import at_least  # noqa: PLC0415

    jid = submit(
        restd,
        auth_headers,
        name="d",
        partition="debug",
        time_limit=10,
        current_working_directory="/home/root",
    )
    job = restd.get(f"/slurm/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
    if not at_least("25.05"):
        pytest.skip("no *_expanded fields before 25.05")
    # From 25.11 slurmctld stores the default path itself.
    assert job["standard_output"] == ("slurm-%j.out" if at_least("25.11") else "")
    expected = f"/home/root/slurm-{jid}.out" if at_least("25.11") else ""
    assert job["stdout_expanded"] == expected
    assert job["stdin_expanded"] == "/dev/null"
    assert job["stderr_expanded"] == ""
