"""Opt-in batch script execution (SLURM_EMULATOR_JOB_EXECUTE).

A failing script ends the job FAILED with the script's exit status
(slurm://src/slurmctld/job_mgr.c#job_complete); StdOut/StdErr patterns are
expanded as slurm_get_job_stdout() does (slurm://src/api/job_info.c#slurm_get_job_stdout).
"""

import os
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

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


def test_execution_cap_names_itself_not_a_time_limit(fs_root, monkeypatch):
    """A script stopped by the emulator's cap fails 124 and says which cap stopped it.

    Real Slurm runs the script up to the job's time limit, ends the job
    TIMEOUT (slurm://src/slurmctld/job_mgr.c#_job_timed_out) and slurmstepd
    logs slurm://src/slurmd/slurmstepd/req.c#"*** %s ON %s CANCELLED AT %s DUE TO TIME LIMIT ***".
    The cap is the emulator's own bound, so its message must not read as one.
    """
    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    monkeypatch.setenv(job_execution.TIMEOUT_ENV, "0.5")
    job = _job("exec sleep 5", standard_output="cap.out", time_limit=120)
    _advance(job)
    assert job.state == "FAILED"
    assert job.exit_code == 124
    out = (fs_root / "home/alice/cap.out").read_text()
    assert "SLURM_EMULATOR_JOB_EXECUTE_TIMEOUT=0.5" in out
    assert "TIME LIMIT" not in out


def _running(pid: int) -> bool:
    """Alive and not a zombie waiting to be reaped."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    stat = Path(f"/proc/{pid}/stat")
    if stat.exists():
        return stat.read_text().rsplit(")", 1)[-1].split()[0] != "Z"
    state = subprocess.run(
        ["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True, check=False
    ).stdout.strip()
    return bool(state) and not state.startswith("Z")


def test_execution_cap_ends_what_the_script_started(fs_root, monkeypatch):
    """slurmstepd ends the job's whole cgroup; the cap ends the script's process group."""
    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    monkeypatch.setenv(job_execution.TIMEOUT_ENV, "0.5")
    job = _job("sleep 30 & echo $! > child.pid; wait", standard_output="bg.out", time_limit=120)
    _advance(job)
    assert job.exit_code == 124
    pid = int((fs_root / "home/alice/child.pid").read_text())
    deadline = time.monotonic() + 5
    while _running(pid) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not _running(pid)


@pytest.mark.usefixtures("fs_root")
def test_job_reaching_its_time_limit_stays_timeout_when_scripts_run(monkeypatch):
    """slurm://src/slurmctld/job_mgr.c#_job_timed_out: TIMEOUT whatever the script returned."""
    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    job = _job("exit 3", standard_output="tl.out", time_limit=1)
    job.environment = {"SLURM_EMULATOR_RUN_SECONDS": "120"}
    job.submit_time = datetime.now() - timedelta(minutes=5)
    _advance(job)
    assert job.state == "TIMEOUT"
    assert job.exit_code == -15


def test_srun_stand_in_runs_the_command_and_reports_a_missing_one(fs_root, monkeypatch):
    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    job = _job("srun --mpi=pmix -n 4 -N 2 gmx_mpi mdrun -s topol.tpr", standard_output="m.out")
    _advance(job)
    assert job.state == "FAILED"
    assert job.exit_code == 127
    assert "gmx_mpi" in (fs_root / "home/alice/m.out").read_text()


def _script_env(fs_root, out: str) -> tuple[list[str], dict[str, str]]:
    lines = (fs_root / "home/alice" / out).read_text().splitlines()
    return lines, dict(line.split("=", 1) for line in lines if "=" in line)


def test_batch_script_gets_the_batch_job_environment(fs_root, monkeypatch):
    """The variables Slurm sets for a batch job, from the job record.

    slurmctld's slurm://src/slurmctld/job_scheduler.c#_set_job_env, slurmstepd's
    slurm://src/common/env.c#env_array_for_batch_job and, for the batch task,
    slurm://src/slurmd/slurmstepd/task.c#exec_task + slurm://src/common/env.c#setup_env;
    sbatch's own exports (slurm://src/sbatch/opt.c#set_envs). Tasks are laid out in
    blocks (slurm://src/common/slurm_step_layout.c#_task_layout_block).
    """
    import socket  # noqa: PLC0415

    from emulator.commands.scontrol_info import mem_str, sprint_job  # noqa: PLC0415
    from emulator.slurm_version import at_least, matches  # noqa: PLC0415

    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    job = _job(
        'hostname; echo "HOST=$HOSTNAME"; env',
        standard_output="env.out",
        partition="compute",
        tasks=4,
        cpus_per_task=2,
        memory_per_node=4096,
        node_count=2,
        time_limit=30,
    )
    _advance(job)
    assert job.state == "COMPLETED"
    lines, env = _script_env(fs_root, "env.out")
    first = job.node_list[0]
    assert lines[0] == first  # `hostname`
    assert env["HOST"] == env["HOSTNAME"] == env["SLURMD_NODENAME"] == first
    start = int(job.start_time.timestamp())
    expected = {
        "SLURM_CLUSTER_NAME": "default",
        "SLURM_JOB_ID": "42",
        "SLURM_JOBID": "42",
        "SLURM_JOB_NAME": "sim",
        "SLURM_JOB_NODELIST": "node[005-006]",
        "SLURM_NODELIST": "node[005-006]",
        "SLURM_JOB_NUM_NODES": "2",
        "SLURM_NNODES": "2",
        "SLURM_JOB_CPUS_PER_NODE": "4(x2)",
        "SLURM_CPUS_ON_NODE": "4",
        "SLURM_NTASKS": "4",
        "SLURM_NPROCS": "4",
        "SLURM_TASKS_PER_NODE": "2(x2)",
        "SLURM_CPUS_PER_TASK": "2",
        "SLURM_TRES_PER_TASK": "cpu=2",
        "SLURM_MEM_PER_NODE": "4096",
        "SLURM_JOB_PARTITION": "compute",
        "SLURM_JOB_ACCOUNT": "alloc",
        "SLURM_JOB_QOS": "normal",
        "ENVIRONMENT": "BATCH",
        "SLURM_TOPOLOGY_ADDR": first,
        "SLURM_TOPOLOGY_ADDR_PATTERN": "node",
        "SLURM_NODEID": "0",
        "SLURM_PROCID": "0",
        "SLURM_LOCALID": "0",
        "SLURM_GTIDS": "0",
        "SLURM_JOB_UID": "1000",
        "SLURM_JOB_GID": "1000",
        "SLURM_JOB_USER": "alice",
        "SLURM_OOM_KILL_STEP": "0",
        "SLURM_JOB_START_TIME": str(start),
        "SLURM_JOB_END_TIME": str(start + 30 * 60),
        "SLURM_SUBMIT_DIR": "/home/alice",
        "SLURM_SUBMIT_HOST": socket.gethostname(),
        "SLURM_PRIO_PROCESS": "0",
    }
    assert {k: env.get(k) for k in expected} == expected
    assert env["SLURM_TASK_PID"].isdigit()
    assert "SLURM_MEM_PER_CPU" not in env
    assert "SLURM_JOB_GPUS" not in env
    # slurm://src/common/slurm_protocol_defs.c#get_job_oversubscribe_value@26.05+
    if at_least("26.05"):
        assert env["SLURM_JOB_OVERSUBSCRIBE"] == "NO"
        assert env["SLURM_JOB_EXCLUSIVE"] == "NO"
    else:
        assert "SLURM_JOB_EXCLUSIVE" not in env
    # Only the 26.05 releases print the batch step's id (signed): slurm://src/common/env.c#"SLURM_STEP_ID"
    if matches("26.05"):
        assert env["SLURM_STEP_ID"] == env["SLURM_STEPID"] == "-5"
    else:
        assert "SLURM_STEP_ID" not in env

    # Same job as scontrol show job reports it.
    record = dict(
        item.split("=", 1) for item in sprint_job(job, datetime.now(), True).split() if "=" in item
    )
    assert record["NodeList"] == env["SLURM_JOB_NODELIST"]
    assert record["NumNodes"] == env["SLURM_JOB_NUM_NODES"]
    assert record["NumCPUs"] == str(job.alloc_cpus) == "8"
    assert record["NumTasks"] == env["SLURM_NTASKS"]
    assert record["CPUs/Task"] == env["SLURM_CPUS_PER_TASK"]
    assert record["MinMemoryNode"] == mem_str(int(env["SLURM_MEM_PER_NODE"]))
    assert record["Partition"] == env["SLURM_JOB_PARTITION"]
    assert record["Account"] == env["SLURM_JOB_ACCOUNT"]
    assert record["QOS"] == env["SLURM_JOB_QOS"]
    # Both from get_job_oversubscribe_value from 26.05
    # (slurm://src/common/slurm_protocol_defs.c#get_job_oversubscribe_value@26.05+).
    if at_least("26.05"):
        assert record["OverSubscribe"] == env["SLURM_JOB_OVERSUBSCRIBE"]
    # sbatch's external-launcher settings (slurm://src/common/env.c#set_env_from_opts).
    assert env["OMPI_MCA_plm_slurm_args"] == env["PRTE_MCA_plm_slurm_args"] == "--external-launcher"
    assert env["HYDRA_BOOTSTRAP"] == env["I_MPI_HYDRA_BOOTSTRAP"] == "slurm"


def test_batch_script_sees_its_gpus(fs_root, monkeypatch):
    """slurm://src/plugins/gres/common/gres_common.c#gres_common_gpu_set_env, global
    indices without device confinement (slurm://src/interfaces/gres.c#gres_use_local_device_index)."""
    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    monkeypatch.setenv("SLURM_EMULATOR_GPU_TYPES", "debug")
    job = _job("env", standard_output="gpu.out", partition="debug", tres_per_node="gres/gpu:2")
    _advance(job)
    _lines, env = _script_env(fs_root, "gpu.out")
    for name in ("SLURM_JOB_GPUS", "CUDA_VISIBLE_DEVICES", "ROCR_VISIBLE_DEVICES"):
        assert env[name] == "0,1"
    assert env["ZE_AFFINITY_MASK"] == env["GPU_DEVICE_ORDINAL"] == "0,1"
    assert env["SLURM_GPUS_ON_NODE"] == "2"


def test_defaults_and_user_environment_wins(fs_root, monkeypatch):
    """No --ntasks: no SLURM_NTASKS, one task per allocated CPU; no --mem: DefMemPerCPU
    (slurm://src/common/env.c#env_array_for_batch_job). The job's own environment wins."""
    monkeypatch.setenv(job_execution.EXECUTE_ENV, "1")
    job = _job("env", standard_output="d.out", partition="compute")
    job.environment = {"SLURM_JOB_QOS": "mine", "HOSTNAME": "mine"}
    _advance(job)
    _lines, env = _script_env(fs_root, "d.out")
    assert "SLURM_NTASKS" not in env
    assert "SLURM_CPUS_PER_TASK" not in env
    assert env["SLURM_TASKS_PER_NODE"] == env["SLURM_JOB_CPUS_PER_NODE"] == "1"
    assert env["SLURM_MEM_PER_CPU"] == "8192"
    assert env["SLURM_JOB_QOS"] == env["HOSTNAME"] == "mine"


def test_gpus_requested_for_the_job_are_exported(monkeypatch):
    """sbatch exports --gpus as SLURM_GPUS (slurm://src/common/env.c#set_env_from_opts)."""
    job = _job("env", partition="debug", tres_per_job="gres/gpu:a100:2")
    assert job_execution._submit_environment(job)["SLURM_GPUS"] == "a100:2"
    assert "SLURM_GPUS" not in job_execution._submit_environment(_job("env"))


def test_task_count_follows_the_node_count_without_a_node_list():
    """ntasks_per_node x nodes (slurm://src/common/env.c#env_array_for_batch_job)."""
    job = _job("env", node_count=3, tasks_per_node=4)
    env = job_execution.batch_environment(job)
    assert env["SLURM_JOB_NUM_NODES"] == env["SLURM_NNODES"] == "3"
    assert env["SLURM_NTASKS"] == env["SLURM_NPROCS"] == "12"


def test_null_fields_of_a_stored_job_still_give_strings():
    job = _job("env")
    job.partition = job.cluster = None
    env = {**job_execution._submit_environment(job), **job_execution.batch_environment(job)}
    assert all(isinstance(value, str) for value in env.values())
    assert env["SLURM_JOB_PARTITION"] == ""
