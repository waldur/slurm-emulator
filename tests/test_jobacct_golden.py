"""Job accounting reads against a real Slurm 25.11.7 run (``tests/data/slurm_25.11.7_job_accounting.json``).

The data was recorded with ``scripts/agent_parity/usage/`` on the accounting
cluster of ``scripts/agent_parity/cluster``: six jobs submitted at T0 (two
that finish, one running, one suspended twice and resumed once, one requeued
while running, one held), then sacct and ``GET /slurmdb/v0.0.43/jobs`` asked
about them with and without truncation and duplicates
(slurm://src/plugins/accounting_storage/mysql/as_mysql_jobacct_process.c#_cluster_get_jobs,
slurm://src/slurmrestd/plugins/openapi/slurmdbd/jobs.c#op_handler_jobs).
The test replays the same run on the simulated clock and requires the same
records, states and times, as offsets from T0.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from emulator.commands.sacct import SacctEmulator
from emulator.commands.scontrol import ScontrolEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine
from tests.test_cluster_model import V, set_time, submit

GOLDEN = json.loads(
    (Path(__file__).parent / "data" / "slurm_25.11.7_job_accounting.json").read_text()
)
T0 = datetime(2026, 10, 20, 11, 0, 0)
FIELDS = "JobID,JobName,State,Submit,Eligible,Start,End,Elapsed,ElapsedRaw,Suspended"
# name, run seconds, extra job fields -- as in scripts/agent_parity/usage/scenario.sh
JOBS = [
    ("rocket", 20, {}),
    ("finished", 10, {}),
    ("running", 3000, {}),
    ("suspended", 3000, {}),
    ("requeued", 3000, {}),
    ("pending", 30, {"hold": True}),
]


def at(offset: int) -> datetime:
    return T0 + timedelta(seconds=offset)


def scontrol(*args: str) -> None:
    emulator = ScontrolEmulator(SlurmDatabase(), TimeEngine())
    emulator.handle_command(list(args))
    assert emulator.exit_code == 0


@pytest.fixture
def scenario(restd, auth_headers, state_env, monkeypatch):
    """The recorded run, replayed up to the moment the queries were made."""
    monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DELAY", "1")
    monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "none")
    for name in ("SLURM_EMULATOR_JOB_EXECUTE", "SLURM_EMULATOR_TIME_FOLLOW_WALL"):
        monkeypatch.delenv(name, raising=False)
    set_time(T0)
    ids = {}
    for name, seconds, extra in JOBS:
        ids[name] = submit(
            restd,
            auth_headers,
            name=name,
            account="acc1",
            partition="debug",
            time_limit=60,
            environment=[f"SLURM_EMULATOR_RUN_SECONDS={seconds}"],
            **extra,
        )
    offsets = GOLDEN["t0_offsets"]
    for event, command, job in (
        ("SUSPEND1", "suspend", "suspended"),
        ("RESUME1", "resume", "suspended"),
        ("REQUEUE", "requeue", "requeued"),
        ("SUSPEND2", "suspend", "suspended"),
    ):
        set_time(at(offsets[event]))
        scontrol(command, str(ids[job]))
    return ids


def _sacct(ids: dict[str, int], query: dict) -> list[dict]:
    args = ["-X", "-P", "-n", "--noconvert", "--allusers", "-A", "acc1"]
    args += ["-S", at(query["start"]).isoformat(), "-E", at(3600).isoformat()]
    args += [*query["options"], f"--format={FIELDS}"]
    db = SlurmDatabase()
    db.load_state()
    out = SacctEmulator(db, TimeEngine()).handle_command(args)
    by_id = {str(job_id): name for name, job_id in ids.items()}
    rows = []
    for line in out.splitlines():
        row = dict(zip(FIELDS.split(","), line.split("|")))
        row["JobID"] = by_id[row["JobID"]]
        for field in ("Submit", "Eligible", "Start", "End"):
            if row[field] != "Unknown":
                row[field] = int((datetime.fromisoformat(row[field]) - T0).total_seconds())
        rows.append(row)
    return rows


def _golden_sacct(name: str) -> list[dict]:
    names = {str(i + 1): job[0] for i, job in enumerate(JOBS)}
    return [{**row, "JobID": names[row["JobID"]]} for row in GOLDEN["sacct"][name]["rows"]]


@pytest.mark.parametrize("query", list(GOLDEN["sacct"]))
def test_sacct_matches_real_slurm(scenario, query):
    set_time(at(GOLDEN["sacct_now"]))
    assert _sacct(scenario, GOLDEN["sacct"][query]) == _golden_sacct(query)


@pytest.mark.parametrize("flags", list(GOLDEN["rest"]))
def test_slurmdb_jobs_match_real_slurm(scenario, restd, auth_headers, flags):
    set_time(at(GOLDEN["rest_now"]))
    t0 = int(T0.timestamp())
    url = f"/slurmdb/{V}/jobs/?account=acc1&start_time={t0 - 60}&end_time={t0 + 3600}"
    body = restd.get(url + (f"&{flags}" if flags else ""), headers=auth_headers).json()
    assert body["errors"] == []
    names = {job_id: name for name, job_id in scenario.items()}
    real_names = {i + 1: job[0] for i, job in enumerate(JOBS)}
    got = []
    for job in body["jobs"]:
        t = job["time"]
        row = {"job": names[job["job_id"]], "state": job["state"]["current"]}
        row.update(
            {k: (t[k] - t0 if t[k] else 0) for k in ("submission", "eligible", "start", "end")}
        )
        row.update(elapsed=t["elapsed"], suspended=t["suspended"])
        got.append(row)
    expected = [
        {
            "job": real_names[row["job_id"]],
            "state": row["state"],
            **{k: row[k] for k in ("submission", "eligible", "start", "end")},
            "elapsed": row["elapsed"],
            "suspended": row["suspended"],
        }
        for row in GOLDEN["rest"][flags]
    ]
    assert got == expected
