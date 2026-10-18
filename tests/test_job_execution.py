"""Opt-in batch script execution (SLURM_EMULATOR_JOB_EXECUTE).

A failing script ends the job FAILED with the script's exit status
(slurm://src/slurmctld/job_mgr.c#job_complete); StdOut/StdErr patterns are
expanded as slurm_get_job_stdout() does (slurm://src/api/job_info.c#slurm_get_job_stdout).
"""

from datetime import datetime, timedelta

import pytest

from emulator.api.slurmrestd.schemas import dbd_job_to_dict
from emulator.core import job_execution
from emulator.core.database import Job, SlurmDatabase
from emulator.core.scheduler import advance_job_states
from emulator.core.time_engine import TimeEngine


@pytest.fixture
def fs_root(tmp_path, monkeypatch):
    monkeypatch.setenv("SLURM_EMULATOR_FS_ROOT", str(tmp_path))
    monkeypatch.setenv("SLURM_EMULATOR_STATE_FILE", str(tmp_path / "state" / "db.json"))
    monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "wall")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DELAY", "0")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DURATION", "0")
    (tmp_path / "home" / "alice").mkdir(parents=True)
    return tmp_path


def _job(script, **kwargs):
    return Job(
        job_id="42",
        account="alloc",
        user="alice",
        state="PENDING",
        submit_time=datetime.now() - timedelta(seconds=5),
        name="sim",
        working_directory="/home/alice",
        script=script,
        **kwargs,
    )


def _advance(job):
    database = SlurmDatabase()
    database.jobs = {job.job_id: job}
    database.usage_records = []
    advance_job_states(database, TimeEngine())
    return database


def test_disabled_by_default(fs_root, monkeypatch):
    monkeypatch.delenv(job_execution.EXECUTE_ENV, raising=False)
    job = _job("echo hello")
    _advance(job)
    assert job.state == "COMPLETED"
    assert job.exit_code is None
    assert not (fs_root / "home/alice/slurm-42.out").exists()


def test_successful_script_writes_output(fs_root, monkeypatch):
    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    job = _job(
        "#!/bin/bash\nmodule load GROMACS/2024.1\nsrun -n 4 echo hello $SLURM_JOB_ID\n",
        standard_output="/home/alice/%x-%j.out",
        standard_error="/home/alice/%x-%j.err",
    )
    database = _advance(job)
    assert job.state == "COMPLETED"
    assert job.exit_code == 0
    assert (fs_root / "home/alice/sim-42.out").read_text() == "hello 42\n"
    assert "module load GROMACS/2024.1" in (fs_root / "home/alice/sim-42.err").read_text()
    assert database.usage_records[0].state == "COMPLETED"


def test_failing_script_marks_job_failed(fs_root, monkeypatch):
    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    job = _job("#!/bin/bash\nno_such_binary\n", standard_output="out-%j.txt")
    database = _advance(job)
    assert job.state == "FAILED"
    assert job.exit_code == 127
    # No --error: stderr goes to the stdout file, relative to the workdir.
    assert "no_such_binary" in (fs_root / "home/alice/out-42.txt").read_text()
    assert database.usage_records[0].state == "FAILED"


def test_script_runs_once(fs_root, monkeypatch):
    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    job = _job("echo once", standard_output="once.out")
    database = _advance(job)
    advance_job_states(database, TimeEngine())
    assert (fs_root / "home/alice/once.out").read_text() == "once\n"


def test_second_process_reuses_the_first_runs_exit_code(fs_root, monkeypatch):
    """Another process seeing the same job finish must not run it again."""
    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    first = _job("echo run; exit 5", standard_output="race.out")
    _advance(first)
    # Same job as another process's in-memory copy sees it: still RUNNING.
    second = _job("echo run; exit 5", standard_output="race.out")
    _advance(second)
    assert (fs_root / "home/alice/race.out").read_text() == "run\n"
    assert first.exit_code == second.exit_code == 5
    assert second.state == "FAILED"


def test_stdio_pattern_expansion():
    job = _job("", standard_output="")
    assert job_execution.stdout_path(job) == "/home/alice/slurm-42.out"
    assert job_execution.expand_stdio_path("logs/%u_%x_%%.log", job, "") == (
        "/home/alice/logs/alice_sim_%.log"
    )


@pytest.mark.usefixtures("fs_root")
def test_accounting_record_keeps_name_and_exit_code(monkeypatch):
    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    job = _job("exit 3", standard_output="x.out")
    database = _advance(job)
    rendered = dbd_job_to_dict(database.usage_records[0])
    assert rendered["name"] == "sim"
    assert rendered["exit_code"]["return_code"]["number"] == 3


def test_sacct_serves_firecrest_job_metadata(fs_root, monkeypatch):
    """FireCREST's SacctJobMetadataCommand: -j, --parsable2, --delimiter, StdOut & co.

    sacct prints a job's StdOut unexpanded (slurm://src/sacct/print.c#print_fields)
    and splits fields on --delimiter (slurm://src/sacct/options.c#parse_command_line).
    """
    from emulator.commands.sacct import SacctEmulator

    monkeypatch.delenv(job_execution.EXECUTE_ENV, raising=False)
    running = _job("sleep 1", standard_output="/home/alice/%x-%j.out")
    running.job_id = "43"
    running.submit_time = datetime.now()  # still pending: no accounting yet
    finished = _job("true", standard_output="/home/alice/%x-%j.out")
    database = _advance(finished)
    database.jobs[running.job_id] = running

    sacct = SacctEmulator(database, TimeEngine())
    args = [
        "--jobs=42,43",
        "--parsable2",
        "--delimiter=|~f7t~|",
        "--noheader",
        "--format=JobID,JobName,StdIn,StdOut,StdErr,WorkDir",
    ]
    lines = sorted(sacct.handle_command(args).splitlines())
    assert lines == [
        "42|~f7t~|sim|~f7t~|/dev/null|~f7t~|/home/alice/%x-%j.out|~f7t~||~f7t~|/home/alice",
        "43|~f7t~|sim|~f7t~|/dev/null|~f7t~|/home/alice/%x-%j.out|~f7t~||~f7t~|/home/alice",
    ]


def test_home_working_directory_is_provisioned(fs_root, monkeypatch):
    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    job = _job("echo hi", standard_output="hi.out")
    job.user = "bob"
    job.working_directory = "/home/bob"
    _advance(job)
    assert job.state == "COMPLETED"
    assert (fs_root / "home/bob/hi.out").read_text() == "hi\n"


def test_srun_stand_in_runs_the_command_and_reports_a_missing_one(fs_root, monkeypatch):
    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    job = _job("srun --mpi=pmix -n 4 -N 2 gmx_mpi mdrun -s topol.tpr", standard_output="m.out")
    _advance(job)
    assert job.state == "FAILED"
    assert job.exit_code == 127
    assert "gmx_mpi" in (fs_root / "home/alice/m.out").read_text()
