"""Opt-in execution of submitted batch scripts.

By default the emulator only *models* a job: it advances PENDING -> RUNNING
-> COMPLETED on a clock and never runs the script, so a client reading the
job's output files finds nothing. With ``SLURM_EMULATOR_JOB_EXECUTE=1`` the
script is run once, when the job reaches the end of its emulated run time:

* in the job's working directory, under ``bash``, with the environment
  Slurm gives a batch job (``batch_environment``) and a short prelude that
  turns cluster-only commands into harmless stand-ins (``module``/``spack``
  echo to stderr, ``srun`` drops its options and their values and runs the
  command, ``hostname`` prints the job's first node);
* stdout/stderr go to the job's ``standard_output``/``standard_error`` with
  Slurm's filename patterns expanded (default ``slurm-%j.out``), so a
  FireCREST/portal client can tail them;
* a non-zero exit status turns the job FAILED with that exit code, as
  slurmctld records it (``job_ptr->exit_code``,
  slurm://src/slurmctld/job_mgr.c#job_complete).

Deliberate deviations from real Slurm (this is a test fixture -- enable it
only where running the submitted scripts is acceptable):

* the script runs inside the emulator (container), not on a compute node,
  and as the emulator's own OS user unless NSS mode resolves the job's user;
* it is stopped after ``SLURM_EMULATOR_JOB_EXECUTE_TIMEOUT`` seconds
  (default 60), whatever the job's time limit, because it runs inside the
  request that first sees the job end. Everything the script started goes
  with it (its process group, as slurmstepd ends the job's whole cgroup).
  The job then ends FAILED with exit code 124 and a stderr line naming the
  cap. Real Slurm lets the script run
  until its time limit, ends the job TIMEOUT
  (slurm://src/slurmctld/job_mgr.c#_job_timed_out) and slurmstepd logs
  slurm://src/slurmd/slurmstepd/req.c#"*** %s ON %s CANCELLED AT %s DUE TO TIME LIMIT ***";
  the emulator does not print that line, so a capped script is not mistaken
  for one that hit its own limit;
* only ``hostname`` and ``$HOSTNAME`` name the job's first node:
  ``gethostname()``/``uname -n`` still return the container's name, which
  would take a UTS namespace per job;
* variables in the job's own environment win over the ones Slurm sets, where
  slurmstepd overwrites them (``env_array_overwrite_fmt`` in
  slurm://src/common/env.c#env_array_for_batch_job);
* what ``sbatch`` exports at submit (``SLURM_SUBMIT_DIR``/``_HOST``,
  ``SLURM_PRIO_PROCESS``, ``SLURM_CPUS_PER_TASK`` & co.,
  slurm://src/sbatch/opt.c#set_envs) is set for every job, also one submitted
  over REST, where slurmrestd leaves the environment to the client;
  ``SLURM_SUBMIT_DIR`` is the job's working directory, as the record keeps
  no other: it differs from the directory sbatch ran in only with ``--chdir``;
* not set: ``SLURM_CONF`` (there is no slurm.conf,
  slurm://src/common/env.c#"SLURM_CONF"), ``SLURM_JOB_SLUID`` (no SLUIDs,
  slurm://src/common/env.c#"SLURM_JOB_SLUID"@26.05+), ``SLURM_UMASK`` (the
  submitting sbatch's umask, slurm://src/sbatch/sbatch.c#_set_umask_env) and
  ``SLURM_GPUS_PER_NODE`` (``--gpus-per-node`` and ``--gres`` both end up in
  the record's ``tres_per_node``); GPU indices are the global ones, as on
  nodes without device confinement.
"""

from __future__ import annotations

import contextlib
import logging
import os
import signal
import socket
import subprocess
import tempfile
import time
from datetime import timedelta
from pathlib import Path

from emulator import slurm_version
from emulator.core import cluster_model, nss

logger = logging.getLogger(__name__)

EXECUTE_ENV = "SLURM_EMULATOR_JOB_EXECUTE"
TIMEOUT_ENV = "SLURM_EMULATOR_JOB_EXECUTE_TIMEOUT"

# Stand-ins for commands that only exist on a real cluster node.
PRELUDE = """\
module() { echo "module $*" >&2; }
spack() { echo "spack $*" >&2; }
srun() {
  while [ $# -gt 0 ]; do
    case "$1" in
      -n|-N|-c|-t|-p|-A|-J|-o|-e|-w|-x|-G|--ntasks|--nodes|--cpus-per-task|--time|\
      --partition|--account|--job-name|--output|--error|--nodelist|--exclude|--gpus|\
      --mem|--mpi) shift 2 ;;
      -*) shift ;;
      *) break ;;
    esac
  done
  "$@"
}
hostname() {
  case "${1:-}" in
    ""|-s|--short|-f|--fqdn|--long) echo "${SLURMD_NODENAME:-$(command hostname)}" ;;
    *) command hostname "$@" ;;
  esac
}
SLURM_TASK_PID=${SLURM_TASK_PID:-$$}
export SLURM_TASK_PID
export -f module spack srun hostname
"""


def execution_enabled() -> bool:
    return os.environ.get(EXECUTE_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def _timeout() -> float:
    try:
        return float(os.environ.get(TIMEOUT_ENV, "60"))
    except ValueError:
        return 60.0


def _cap_message() -> str:
    return (
        f"slurm-emulator: script stopped after {TIMEOUT_ENV}={_timeout():g} seconds, "
        "the emulator's execution cap (not the job's time limit)\n"
    )


def _fs_root() -> Path:
    return Path(os.environ.get("SLURM_EMULATOR_FS_ROOT", "/tmp/slurm_emulator_fs"))


def host_path(path: str) -> Path:
    """Where a cluster path lives on this host (``SLURM_EMULATOR_FS_ROOT``)."""
    return _fs_root() / path.lstrip("/")


def working_directory(job) -> str:
    return job.working_directory or f"/home/{job.user}"


# slurm://src/common/print_fields.c#_is_wildcard (outside srun every one expands).
_NUMERIC_WILDCARDS = set("AabJjrsnt")
_WILDCARDS = _NUMERIC_WILDCARDS | set("NSux")
# A job that is not an array task carries NO_VAL as its task id.
_NO_VAL = 0xFFFFFFFE


def _wildcard(char: str, job, padding: int) -> str:
    """slurm://src/common/print_fields.c#_expand_wildcard for the batch step."""
    job_id = int(job.job_id) if str(job.job_id).isdigit() else 0
    numbers = {
        "A": job_id,  # not an array: the job id
        "J": job_id,  # the batch step prints no ".step"
        "j": job_id,
        "a": _NO_VAL,
        "b": _NO_VAL % 10,
        "r": 0,
        "n": 0,
        "t": 0,
    }
    if char in numbers:
        return f"{numbers[char]:0{padding}d}"
    nodes = getattr(job, "node_list", None) or []
    return {
        "s": "batch",
        "N": nodes[0] if nodes else "(null)",
        "S": "",
        "u": job.user or "",
        "x": job.name or "",
    }[char]


def expand_stdio_path(pattern: str, job, default: str) -> str:
    r"""Expand a stdin/stdout/stderr filename pattern for a job's batch step.

    A port of slurm://src/common/print_fields.c#expand_stdio_fields: a
    relative path is under the working directory, ``%%`` is ``%``, a number
    after ``%`` zero-pads a numeric wildcard (at most 10), an unknown
    ``%c`` stays as written, a lone ``%`` at the end is dropped, and a ``\``
    anywhere turns expansion off. The
    same expansion gives ``scontrol show job``'s StdOut/StdErr
    (slurm://src/api/job_info.c#slurm_get_job_stdout) and the REST
    ``stdout_expanded`` fields.
    """
    path = pattern or default
    out = ""
    if not path.startswith("/"):
        workdir = working_directory(job)
        out = workdir if workdir.endswith("/") else f"{workdir}/"
    if "\\" in path:
        return out + path.replace("\\", "")
    i = 0
    while i < len(path):
        char = path[i]
        if char != "%":
            out += char
            i += 1
            continue
        if i + 1 >= len(path):
            break
        i += 1
        if path[i] == "%":
            out += "%"
            i += 1
            continue
        start = i
        while i < len(path) and path[i].isdigit():
            i += 1
        padding = min(int(path[start:i]), 10) if i > start else 0
        if i >= len(path) or path[i] not in _WILDCARDS:
            out += "%" + path[start : i + 1]
            i += 1
            continue
        out += _wildcard(path[i], job, padding)
        i += 1
    return out


def stdout_path(job) -> str:
    return expand_stdio_path(job.standard_output, job, "slurm-%j.out")


def stderr_path(job) -> str:
    # Without --error Slurm writes stderr into the stdout file.
    return expand_stdio_path(job.standard_error or job.standard_output, job, "slurm-%j.out")


def _compressed(values: list[int]) -> str:
    """``4(x2),2``: runs of equal counts, as slurm://src/common/env.c#uint16_array_to_str prints them."""
    runs: list[list[int]] = []
    for value in values:
        if runs and runs[-1][0] == value:
            runs[-1][1] += 1
        else:
            runs.append([value, 1])
    return ",".join(f"{v}(x{n})" if n > 1 else str(v) for v, n in runs)


def _tasks_per_node(num_tasks: int, cpus: list[int]) -> list[int]:
    """Block layout without pack_nodes (slurm://src/common/slurm_step_layout.c#_task_layout_block).

    ``cpus`` are tasks' worth of CPUs per node; once every node is full the
    rest are spread one per node again.
    """
    counts = [0] * len(cpus)
    placed, row, oversubscribe = 0, 0, False
    while placed < num_tasks:
        room = False
        for i, cpu in enumerate(cpus):
            if placed >= num_tasks:
                break
            if row < cpu or oversubscribe:
                counts[i] += 1
                placed += 1
                room = room or row + 1 < cpu
        oversubscribe = oversubscribe or not room
        row += 1
    return counts


def _submit_environment(job) -> dict[str, str]:
    """What sbatch exports into the job's environment for the options the job record keeps.

    slurm://src/common/env.c#set_submit_dir_env@26.05+ (before: slurm://src/sbatch/sbatch.c#_set_submit_dir_env@24.11,25.05,25.11),
    slurm://src/sbatch/opt.c#set_envs (cpus per task, tasks, tasks per node),
    slurm://src/common/env.c#set_env_from_opts with the ``cpu=N`` that
    slurm://src/common/slurm_opt.c#_set_tres_per_task_from_sibling_opt makes of
    ``-c N``, ``SLURM_GPUS`` for ``--gpus``, and the external-launcher
    settings it always adds for an mpirun that starts srun; ``SLURM_PRIO_PROCESS``
    stays unless PropagatePrioProcess
    (slurm://src/slurmd/slurmstepd/mgr.c#_set_prio_process). ``SLURM_NTASKS``
    is slurmstepd's (``batch_environment``).
    """
    env = {
        "SLURM_SUBMIT_DIR": working_directory(job),
        "SLURM_SUBMIT_HOST": socket.gethostname(),
        "SLURM_PRIO_PROCESS": "0",
        "OMPI_MCA_plm_slurm_args": "--external-launcher",
        "PRTE_MCA_plm_slurm_args": "--external-launcher",
        "HYDRA_BOOTSTRAP": "slurm",
        "HYDRA_LAUNCHER_EXTRA_ARGS": "--external-launcher",
        "I_MPI_HYDRA_BOOTSTRAP": "slurm",
        "I_MPI_HYDRA_BOOTSTRAP_EXEC_EXTRA_ARGS": "--external-launcher",
    }
    if job.cpus_per_task:
        env["SLURM_CPUS_PER_TASK"] = str(job.cpus_per_task)
        env["SLURM_TRES_PER_TASK"] = f"cpu={job.cpus_per_task}"
    if job.tasks_per_node:
        env["SLURM_NTASKS_PER_NODE"] = str(job.tasks_per_node)
    # --gpus is the job's tres_per_job, "gres/gpu[:type]:N"; sbatch exports
    # what was given, "[type:]N".
    if (job.tres_per_job or "").startswith("gres/gpu:"):
        env["SLURM_GPUS"] = job.tres_per_job[len("gres/gpu:") :]
    return env


def batch_environment(job) -> dict[str, str]:
    """The variables Slurm sets for a batch job, from the job record.

    slurmctld adds the job name, OverSubscribe/Exclusive (26.05+) and the
    tasks per node to the launch environment
    (slurm://src/slurmctld/job_scheduler.c#_set_job_env,
    slurm://src/slurmctld/job_scheduler.c#get_tasks_per_node@26.05+); slurmstepd
    adds the allocation (slurm://src/common/env.c#env_array_for_batch_job), the
    GPUs (slurm://src/plugins/gres/common/gres_common.c#gres_common_gpu_set_env,
    global indices without device confinement,
    slurm://src/interfaces/gres.c#gres_use_local_device_index) and the node's
    topology address (slurm://src/slurmd/slurmstepd/slurmstepd.c#"SLURM_TOPOLOGY_ADDR",
    slurm://src/plugins/topology/common/common_topo.c#common_topo_get_node_addr),
    then the batch task's own (slurm://src/slurmd/slurmstepd/task.c#exec_task,
    slurm://src/common/env.c#setup_env). ``SLURM_TASK_PID`` is the script's
    pid, set by the prelude.
    """
    job_id = str(job.job_id)
    uid = job.uid if job.uid is not None else 1000
    gid = job.gid if job.gid is not None else 1000
    shares = cluster_model.job_node_shares(job)
    num_nodes = str(len(shares) or job.node_count or 1)
    # A state file can hold nulls; the environment takes strings only.
    env = {
        "SLURM_CLUSTER_NAME": job.cluster or "",
        "SLURM_JOB_ID": job_id,
        "SLURM_JOBID": job_id,
        "SLURM_JOB_NUM_NODES": num_nodes,
        "SLURM_NNODES": num_nodes,
        "SLURM_JOB_PARTITION": job.partition or "",
        "SLURM_JOB_ACCOUNT": job.account or "",
        "SLURM_JOB_QOS": job.qos or "normal",
        "ENVIRONMENT": "BATCH",
        "SLURM_NODEID": "0",
        "SLURM_PROCID": "0",
        "SLURM_LOCALID": "0",
        "SLURM_GTIDS": "0",
        "SLURM_OOM_KILL_STEP": "0",
        "SLURM_JOB_UID": str(uid),
        "SLURM_JOB_GID": str(gid),
        "SLURM_JOB_USER": job.user or "",
    }
    if job.name:
        env["SLURM_JOB_NAME"] = job.name
    if job.reservation:
        env["SLURM_JOB_RESERVATION"] = job.reservation
    # Without --mem slurmctld stores DefMemPerCPU as a per-CPU request.
    if job.memory_per_node:
        env["SLURM_MEM_PER_NODE"] = str(job.memory_per_node)
    else:
        env["SLURM_MEM_PER_CPU"] = str(cluster_model.DEF_MEM_PER_CPU_MB)
    if slurm_version.at_least("26.05"):
        # The emulator's partitions are OverSubscribe=NO (max_share 1)
        # (slurm://src/common/slurm_protocol_defs.c#get_job_oversubscribe_value@26.05+,
        # slurm://src/common/slurm_protocol_defs.c#get_job_exclusive_display_value@26.05+).
        env["SLURM_JOB_OVERSUBSCRIBE"] = "NO"
        env["SLURM_JOB_EXCLUSIVE"] = "NODE" if job.exclusive else "NO"
    if slurm_version.matches("26.05"):
        # 26.05 prints the batch step's id SLURM_BATCH_SCRIPT signed; older
        # releases and master skip special steps (slurm://src/common/env.c#"SLURM_STEP_ID").
        env["SLURM_STEP_ID"] = env["SLURM_STEPID"] = "-5"
    if job.start_time:
        # The credential's start and end: start + time limit, a year without one
        # (slurm://src/interfaces/cred.c#job_end_time,
        # slurm://src/slurmctld/job_mgr.c#job_end_time_reset). At launch, so
        # without the suspended time scheduler.expected_end adds later.
        limit = job.time_limit * 60 if job.time_limit else 365 * 86400
        env["SLURM_JOB_START_TIME"] = str(int(job.start_time.timestamp()))
        env["SLURM_JOB_END_TIME"] = str(
            int((job.start_time + timedelta(seconds=limit)).timestamp())
        )

    # Only an explicit task count, or sbatch's SLURM_NTASKS_PER_NODE x nodes.
    num_tasks = job.tasks or job.tasks_per_node * int(num_nodes)
    if num_tasks:
        env["SLURM_NTASKS"] = env["SLURM_NPROCS"] = str(num_tasks)
    if not shares:
        return env
    nodes = [node for node, _cpus, _mem in shares]
    cpus = [cpus for _node, cpus, _mem in shares]
    first = nodes[0]
    cpt = max(job.cpus_per_task or 1, 1)
    # Without one, SLURM_TASKS_PER_NODE fills every allocated CPU.
    num_tasks = num_tasks or sum(c // cpt for c in cpus)
    hostlist = cluster_model.compress_hostlist(nodes)
    env.update(
        {
            "SLURM_JOB_NODELIST": hostlist,
            "SLURM_NODELIST": hostlist,
            "SLURM_JOB_CPUS_PER_NODE": _compressed(cpus),
            "SLURM_TASKS_PER_NODE": _compressed(
                _tasks_per_node(num_tasks, [max(c // cpt, 1) for c in cpus])
            ),
            "SLURM_CPUS_ON_NODE": str(cpus[0]),
            "HOSTNAME": first,
            "SLURMD_NODENAME": first,
            "SLURM_TOPOLOGY_ADDR": first,
            "SLURM_TOPOLOGY_ADDR_PATTERN": "node",
        }
    )
    gpus = job.gpu_indices.get(first) or []
    if gpus:
        listed = ",".join(str(i) for i in gpus)
        env["SLURM_JOB_GPUS"] = listed
        env["SLURM_GPUS_ON_NODE"] = str(len(gpus))
        for name in (
            "CUDA_VISIBLE_DEVICES",
            "ROCR_VISIBLE_DEVICES",
            "ZE_AFFINITY_MASK",
            "GPU_DEVICE_ORDINAL",
        ):
            env[name] = listed
    return env


def _environment(job) -> dict[str, str]:
    env = {
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "HOME": f"/home/{job.user}",
        "USER": job.user or "root",
        "LOGNAME": job.user or "root",
        **_submit_environment(job),
        **batch_environment(job),
    }
    environment = job.environment
    if isinstance(environment, dict):
        env.update({str(k): str(v) for k, v in environment.items()})
    elif isinstance(environment, list):
        env.update(dict(item.split("=", 1) for item in environment if "=" in item))
    return env


def _run_as(job) -> dict:
    if not nss.enabled() or os.geteuid() != 0:
        return {}
    identity = nss.resolve(job.user)
    if identity is None:
        return {}
    return {"user": identity.uid, "group": identity.gid}


def _marker_dir() -> Path:
    state_file = os.environ.get("SLURM_EMULATOR_STATE_FILE")
    base = Path(state_file).parent if state_file else Path(tempfile.gettempdir())
    path = base / "executed-jobs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def execute_once(job) -> int:
    """Run the script exactly once across the emulator's processes.

    State advances lazily in whichever process reads it first (REST, SSH,
    control API), so two of them can see the same job finish. An
    exclusively-created marker elects one runner; the others wait for the
    exit code it records.
    """
    marker = _marker_dir() / str(job.job_id)
    result = marker.with_suffix(".rc")
    try:
        fd = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        deadline = time.monotonic() + _timeout() + 10
        while time.monotonic() < deadline:
            with contextlib.suppress(OSError, ValueError):
                return int(result.read_text())
            time.sleep(0.2)
        return 1
    os.close(fd)
    exit_code = run_job_script(job)
    tmp = result.with_suffix(".tmp")
    tmp.write_text(str(exit_code))
    tmp.replace(result)
    return exit_code


def run_job_script(job) -> int:
    """Run the job's script, write its output files, return the exit code."""
    cwd = host_path(working_directory(job))
    out_path = host_path(stdout_path(job))
    err_path = host_path(stderr_path(job))
    if not cwd.is_dir() and working_directory(job) == f"/home/{job.user}":
        # Cluster homes exist before the first job; the SSH plane makes them
        # only on first login, so provision it here (pam_mkhomedir-style).
        cwd.mkdir(parents=True, exist_ok=True)
    if not cwd.is_dir():
        logger.info("Job %s: working directory %s does not exist", job.job_id, cwd)
        return 1
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        separate_err = err_path if err_path != out_path else Path(os.devnull)
        with out_path.open("ab") as out, separate_err.open("ab") as err:
            # Its own session, so the cap ends what the script started too.
            process = subprocess.Popen(
                ["bash", "-c", PRELUDE + (job.script or "")],
                cwd=cwd,
                env=_environment(job),
                stdout=out,
                stderr=err if err_path != out_path else subprocess.STDOUT,
                start_new_session=True,
                **_run_as(job),
            )
            try:
                return process.wait(timeout=_timeout())
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError, PermissionError):
                    os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                raise
    except subprocess.TimeoutExpired:
        with err_path.open("ab") as err:
            err.write(_cap_message().encode())
        return 124
    except OSError as e:
        logger.warning("Job %s: cannot run script: %s", job.job_id, e)
        return 1
