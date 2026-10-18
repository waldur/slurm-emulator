"""scancel: slurm://src/scancel/scancel.c#_proc_cluster (without --ctld)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from emulator.commands.scancel import ScancelEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine
from tests.test_cluster_model import T0, ctld_jobs, set_time, submit
from tests.test_cluster_model import sim as sim  # noqa: F401 - fixture


def run(capsys, caller, *args):
    db = SlurmDatabase()
    db.load_state()
    emu = ScancelEmulator(db, TimeEngine())
    emu.caller = caller
    out = emu.handle_command(list(args))
    return out, capsys.readouterr().err, emu.exit_code


@pytest.fixture
def jobs(restd, auth_headers, sim):
    def job(user, account, **extra):
        headers = {**auth_headers, "X-SLURM-USER-NAME": user}
        return submit(restd, headers, name="j", partition="compute", account=account,
                      time_limit=600, **extra)

    ids = {
        "alice_a": job("alice", "proj_a"),
        "alice_b": job("alice", "proj_b"),
        "bob_a": job("bob", "proj_a"),
    }
    set_time(T0 + timedelta(minutes=2))
    return ids


def states(restd, auth_headers):
    return {jid: j["job_state"][0] for jid, j in ctld_jobs(restd, auth_headers).items()}


def test_site_agent_cancels_an_account(restd, auth_headers, jobs, capsys):
    # waldur_site_agent_slurm: scancel --cluster=<c> -A <account> -f
    out, err, code = run(capsys, "root", "--cluster=default", "-A", "proj_a", "-f")
    assert (out, err, code) == ("", "", 0)
    now = states(restd, auth_headers)
    assert now[jobs["alice_a"]] == now[jobs["bob_a"]] == "CANCELLED"
    assert now[jobs["alice_b"]] == "RUNNING"


def test_user_and_account_filters_combine(restd, auth_headers, jobs, capsys):
    run(capsys, "root", "-u", "alice", "-A", "proj_a", "-f")
    now = states(restd, auth_headers)
    assert now[jobs["alice_a"]] == "CANCELLED"
    assert now[jobs["bob_a"]] == now[jobs["alice_b"]] == "RUNNING"


def test_cancelled_by_caller_in_accounting(restd, auth_headers, jobs, capsys):
    run(capsys, "alice", str(jobs["alice_a"]))
    db = SlurmDatabase()
    db.load_state()
    assert db.get_job(str(jobs["alice_a"])).requid == 1000


def test_other_users_job_is_denied(restd, auth_headers, jobs, capsys):
    _, err, code = run(capsys, "alice", str(jobs["bob_a"]))
    assert code == 1
    assert err == f"scancel: error: Kill job error on job id {jobs['bob_a']}: " \
        "Access/permission denied\n"
    assert states(restd, auth_headers)[jobs["bob_a"]] == "RUNNING"


def test_unknown_job_id(restd, auth_headers, jobs, capsys):
    assert run(capsys, "root", "999") == (
        "",
        "scancel: error: Kill job error on job id 999: Invalid job id specified\n",
        1,
    )


def test_no_job_identification(capsys, sim):
    assert run(capsys, "root") == ("", "scancel: error: No job identification provided\n", 1)


def test_signal_other_than_kill_leaves_job(restd, auth_headers, jobs, capsys):
    run(capsys, "root", "-s", "USR1", str(jobs["alice_a"]))
    assert states(restd, auth_headers)[jobs["alice_a"]] == "RUNNING"


def test_sacct_names_who_cancelled(restd, auth_headers, jobs, capsys):
    from emulator.commands.sacct import SacctEmulator  # noqa: PLC0415

    run(capsys, "alice", str(jobs["alice_a"]))
    db = SlurmDatabase()
    db.load_state()
    out = SacctEmulator(db, TimeEngine()).handle_command(
        ["-j", str(jobs["alice_a"]), "-n", "-P", "-o", "State"]
    )
    # slurm://src/sacct/print.c#PRINT_STATE
    assert out.splitlines()[0] == "CANCELLED by 1000"
