"""Job accounting reads against a real Slurm 25.11.7 run (``tests/data/slurm_25.11.7_job_accounting.json``).

The data was recorded with ``scripts/agent_parity/usage/`` on the accounting
cluster of ``scripts/agent_parity/cluster``: seven jobs submitted at T0 (two
that finish, one running, one suspended twice and resumed once, one requeued
while running, one held, one suspended and resumed that then finishes), then
sacct and slurmrestd asked about them with and without truncation and
duplicates, by window and by job id
(slurm://src/plugins/accounting_storage/mysql/as_mysql_jobacct_process.c#_cluster_get_jobs,
slurm://src/slurmrestd/plugins/openapi/slurmdbd/jobs.c#op_handler_jobs).
The test replays the same run on the simulated clock and requires the same
records, states and times, as offsets from T0.

Running and suspended jobs have no batch step in the emulator yet, so steps
are only compared for records of finished and requeued runs.
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
# name, work seconds, extra job fields -- as in scripts/agent_parity/usage/scenario.sh.
# "resumed" sleeps 20 s of wall time with 10 s suspended: 10 s of work.
JOBS = [
    ("rocket", 20, {}),
    ("finished", 10, {}),
    ("running", 3000, {}),
    ("suspended", 3000, {}),
    ("requeued", 3000, {}),
    ("pending", 30, {"hold": True}),
    ("resumed", 10, {}),
]
EVENTS = [
    ("SUSPEND_RESUMED", "suspend", "resumed"),
    ("SUSPEND1", "suspend", "suspended"),
    ("RESUME_RESUMED", "resume", "resumed"),
    ("RESUME1", "resume", "suspended"),
    ("REQUEUE", "requeue", "requeued"),
    ("SUSPEND2", "suspend", "suspended"),
]
# Job ids on the real cluster were 1..7 in JOBS order.
REAL_NAMES = {i + 1: job[0] for i, job in enumerate(JOBS)}
_FIRST = next(row for row in GOLDEN["sacct_by_id"] if row["JobName"] == "finished")
START_DELAY = _FIRST["Start"] - _FIRST["Submit"]
_CONTROLLER = ("PENDING", "RUNNING", "SUSPENDED")


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
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DELAY", str(START_DELAY))
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
    for event, command, job in EVENTS:
        set_time(at(GOLDEN["t0_offsets"][event]))
        scontrol(command, str(ids[job]))
    return ids


def _sacct_rows(ids: dict[str, int], args: list[str]) -> list[dict]:
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


def _golden_sacct(rows: list[dict]) -> list[dict]:
    return [{**row, "JobID": REAL_NAMES[int(row["JobID"])]} for row in rows]


@pytest.mark.parametrize("query", list(GOLDEN["sacct"]))
def test_sacct_matches_real_slurm(scenario, query):
    set_time(at(GOLDEN["sacct_now"]))
    spec = GOLDEN["sacct"][query]
    args = ["-X", "-P", "-n", "--noconvert", "--allusers", "-A", "acc1"]
    args += ["-S", at(spec["start"]).isoformat(), "-E", at(3600).isoformat()]
    args += [*spec["options"], f"--format={FIELDS}"]
    assert _sacct_rows(scenario, args) == _golden_sacct(spec["rows"])


def test_sacct_by_job_id_matches_real_slurm(scenario):
    """``sacct -D -j <all>`` without a window: -S is 0 and -E now; a held job's
    Eligible prints Unknown (slurm://src/common/slurmdb_defs.c#slurmdb_job_cond_def_start_end)."""
    set_time(at(GOLDEN["sacct_now"]))
    ids = ",".join(str(scenario[name]) for name, _seconds, _extra in JOBS)
    args = ["-X", "-P", "-n", "--noconvert", "-D", f"-j{ids}", f"--format={FIELDS}"]
    assert _sacct_rows(scenario, args) == _golden_sacct(GOLDEN["sacct_by_id"])


def _rest_rows(body: dict, names: dict[int, str], t0: int) -> list[dict]:
    rows = []
    for job in body["jobs"]:
        t = job["time"]
        state = job["state"]["current"]
        row = {"job": names[job["job_id"]], "state": state}
        row.update(
            {k: (t[k] - t0 if t[k] else 0) for k in ("submission", "eligible", "start", "end")}
        )
        row.update(elapsed=t["elapsed"], suspended=t["suspended"])
        if state[0] not in _CONTROLLER or "REQUEUED" in state:
            row["steps"] = [
                {
                    "name": step["step"]["name"],
                    "start": step["time"]["start"]["number"] - t0,
                    "end": step["time"]["end"]["number"] - t0,
                    "elapsed": step["time"]["elapsed"],
                    "suspended": step["time"]["suspended"],
                }
                for step in job["steps"]
            ]
        rows.append(row)
    return rows


def _golden_rest(rows: list[dict]) -> list[dict]:
    expected = []
    for row in rows:
        out = {"job": REAL_NAMES[row["job_id"]], "state": row["state"]}
        out.update({k: row[k] for k in ("submission", "eligible", "start", "end")})
        out.update(elapsed=row["elapsed"], suspended=row["suspended"])
        if row["state"][0] not in _CONTROLLER or "REQUEUED" in row["state"]:
            out["steps"] = row["steps"]
        expected.append(out)
    return expected


@pytest.mark.parametrize("query", list(GOLDEN["rest"]))
def test_slurmdb_jobs_match_real_slurm(scenario, restd, auth_headers, query):
    set_time(at(GOLDEN["rest_now"]))
    t0 = int(T0.timestamp())
    start = -60
    flags = query
    if query.startswith("start=+"):
        prefix, _, flags = query.partition("&")
        start = int(prefix.removeprefix("start=+"))
    url = f"/slurmdb/{V}/jobs/?account=acc1&start_time={t0 + start}&end_time={t0 + 3600}"
    body = restd.get(url + (f"&{flags}" if flags else ""), headers=auth_headers).json()
    assert body["errors"] == []
    names = {job_id: name for name, job_id in scenario.items()}
    assert _rest_rows(body, names, t0) == _golden_rest(GOLDEN["rest"][query])


@pytest.mark.parametrize("real_id", list(GOLDEN["rest_by_id"]))
def test_slurmdb_job_by_id_matches_real_slurm(scenario, restd, auth_headers, real_id):
    """``GET /slurmdb/{v}/job/{id}`` returns every record of the job, untruncated
    (slurm://src/slurmrestd/plugins/openapi/slurmdbd/jobs.c#op_handler_job)."""
    set_time(at(GOLDEN["rest_now"]))
    t0 = int(T0.timestamp())
    job_id = scenario[REAL_NAMES[int(real_id)]]
    body = restd.get(f"/slurmdb/{V}/job/{job_id}", headers=auth_headers).json()
    names = {jid: name for name, jid in scenario.items()}
    assert _rest_rows(body, names, t0) == _golden_rest(GOLDEN["rest_by_id"][real_id])


def _sacct_lines(*args: str) -> list[str]:
    db = SlurmDatabase()
    db.load_state()
    return SacctEmulator(db, TimeEngine()).handle_command(list(args)).splitlines()


def test_job_id_query_compares_submit_time(scenario):
    """With -j the window tests submit time, so a run not yet eligible still shows.

    Seen on the same 25.11.7 cluster: ``sacct -D --truncate -j <requeued>`` with
    the window ending at +60 s returned the REQUEUED run and the new record,
    eligible only at +151 s, with 0 s
    (slurm://src/plugins/accounting_storage/mysql/as_mysql_jobacct_process.c#setup_job_cond_limits).
    """
    set_time(at(60))
    run = next(r for r in GOLDEN["sacct_by_id"] if r["State"] == "REQUEUED")
    window = ["-S", at(0).isoformat(), "-E", at(60).isoformat()]
    lines = _sacct_lines(
        "-X",
        "-P",
        "-n",
        "-D",
        "--truncate",
        "-j",
        str(scenario["requeued"]),
        *window,
        "--format=State,ElapsedRaw",
    )
    assert lines == [f"REQUEUED|{run['ElapsedRaw']}", "PENDING|0"]


def test_without_end_time_a_requeued_job_shows_its_last_run(scenario):
    """-E defaults to now, so a pending record not yet eligible is left out.

    Duplicates are removed after the window is applied, so sacct then shows
    the REQUEUED run instead -- as the 25.11.7 cluster did right after a
    requeue (slurm://src/common/slurmdb_defs.c#slurmdb_job_cond_def_start_end).
    """
    set_time(at(60))
    names = {str(job_id): name for name, job_id in scenario.items()}
    lines = _sacct_lines(
        "-X", "-P", "-n", "-A", "acc1", "-S", at(-60).isoformat(), "-o", "JobID,State"
    )
    got = [(names[line.split("|")[0]], line.split("|")[1]) for line in lines]
    assert got == [
        ("rocket", "COMPLETED"),
        ("finished", "COMPLETED"),
        ("running", "RUNNING"),
        ("suspended", "SUSPENDED"),
        ("requeued", "REQUEUED"),
        ("resumed", "COMPLETED"),
    ]


def test_scontrol_job_ops_need_an_operator(state_env, restd, auth_headers, monkeypatch, capsys):
    """Suspend/resume are operator-only, even on one's own job; requeue is also
    the owner's. The 25.11.7 cluster answered ``Access/permission denied for
    job <id>`` and exit 1 to bob suspending his job and alice requeueing it
    (slurm://src/slurmctld/job_mgr.c#_job_suspend_op, slurm://src/slurmctld/job_mgr.c#_job_requeue_op).
    """
    monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
    monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "none")
    set_time(T0)
    job_id = submit(
        restd,
        {**auth_headers, "X-SLURM-USER-NAME": "bob"},
        name="j",
        account="acc1",
        partition="debug",
        time_limit=60,
    )
    set_time(at(120))

    def run(caller: str, command: str) -> int:
        emulator = ScontrolEmulator(SlurmDatabase(), TimeEngine())
        emulator.caller = caller
        emulator.handle_command([command, str(job_id)])
        return emulator.exit_code

    assert run("bob", "suspend") == 1
    assert run("alice", "requeue") == 1
    assert capsys.readouterr().err.splitlines() == [
        f"Access/permission denied for job {job_id}",
        f"Access/permission denied for job {job_id}",
    ]
    assert run("bob", "requeue") == 0


def test_scontrol_show_job_leaves_suspended_time_out(state_env, restd, auth_headers, monkeypatch):
    """Seen on the 25.11.7 cluster for a 5-minute job suspended 4 s after it
    started: RunTime=00:00:04 while suspended, and after a 10 s suspension its
    EndTime moved 10 s on (slurm://src/scontrol/info_job.c#_sprint_job_info,
    slurm://src/slurmctld/job_mgr.c#_job_suspend_op)."""
    monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DELAY", "1")
    monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "none")
    set_time(T0)
    job_id = str(submit(restd, auth_headers, name="j", partition="debug", time_limit=5))

    def show() -> dict[str, str]:
        emulator = ScontrolEmulator(SlurmDatabase(), TimeEngine())
        out = emulator.handle_command(["show", "job", job_id])
        return dict(word.split("=", 1) for word in out.split() if "=" in word)

    set_time(at(5))
    scontrol("suspend", job_id)
    set_time(at(8))
    assert show()["RunTime"] == "00:00:04"
    planned_end = at(1 + 300).strftime("%Y-%m-%dT%H:%M:%S")
    assert show()["EndTime"] == planned_end
    set_time(at(15))
    scontrol("resume", job_id)
    assert show()["EndTime"] == at(1 + 300 + 10).strftime("%Y-%m-%dT%H:%M:%S")


def test_record_without_submit_time_stands_for_its_job(state_env, restd, auth_headers, monkeypatch):
    """A record saved before records carried a submit time still counts as the
    job's accounting, so the finished job is not written a second time."""
    from emulator.core.scheduler import advance_job_states  # noqa: PLC0415

    monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
    monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "none")
    set_time(T0)
    job_id = submit(
        restd,
        auth_headers,
        name="j",
        partition="debug",
        time_limit=5,
        environment=["SLURM_EMULATOR_RUN_SECONDS=10"],
    )
    set_time(at(120))
    db = SlurmDatabase()
    db.load_state()
    advance_job_states(db, TimeEngine())
    (record,) = [r for r in db.usage_records if r.job_id == job_id]
    record.submit_time = None  # as an older state file stores it
    assert not advance_job_states(db, TimeEngine())
    assert [r.job_id for r in db.usage_records].count(job_id) == 1
