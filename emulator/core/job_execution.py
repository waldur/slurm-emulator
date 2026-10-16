"""Opt-in execution of submitted batch scripts.

By default the emulator only *models* a job: it advances PENDING -> RUNNING
-> COMPLETED on a clock and never runs the script, so a client reading the
job's output files finds nothing. With ``SLURM_EMULATOR_JOB_EXECUTE=1`` the
script is run once, when the job reaches the end of its emulated run time:

* in the job's working directory, with ``SLURM_JOB_ID`` & co. set, under
  ``bash`` with a short prelude that turns cluster-only commands into
  harmless stand-ins (``module``/``spack`` echo to stderr, ``srun`` drops its
  options and their values and runs the command);
* stdout/stderr go to the job's ``standard_output``/``standard_error`` with
  Slurm's filename patterns expanded (default ``slurm-%j.out``), so a
  FireCREST/portal client can tail them;
* a non-zero exit status turns the job FAILED with that exit code, as
  slurmctld records it (``job_ptr->exit_code``,
  slurm://src/slurmctld/job_mgr.c#job_complete).

Deliberate deviations from real Slurm: the script runs inside the emulator
(container), not on a compute node; it runs as the emulator's own OS user
unless NSS mode resolves the job's user; and it is bounded by
``SLURM_EMULATOR_JOB_EXECUTE_TIMEOUT`` seconds (default 60) rather than the
job's time limit. This is a test fixture -- enable it only where running the
submitted scripts is acceptable.
"""

from __future__ import annotations

import contextlib
import logging
import os
import subprocess
import tempfile
import time
from pathlib import Path

from emulator.core import nss

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
export -f module spack srun
"""


def execution_enabled() -> bool:
    return os.environ.get(EXECUTE_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def _timeout() -> float:
    try:
        return float(os.environ.get(TIMEOUT_ENV, "60"))
    except ValueError:
        return 60.0


def _fs_root() -> Path:
    return Path(os.environ.get("SLURM_EMULATOR_FS_ROOT", "/tmp/slurm_emulator_fs"))


def host_path(path: str) -> Path:
    """Where a cluster path lives on this host (``SLURM_EMULATOR_FS_ROOT``)."""
    return _fs_root() / path.lstrip("/")


def working_directory(job) -> str:
    return job.working_directory or f"/home/{job.user}"


def expand_stdio_path(pattern: str, job, default: str) -> str:
    """Expand Slurm's filename pattern for a job's stdout/stderr.

    %j/%A job id, %x job name, %u user, %N first node, %% a literal %;
    relative paths are under the working directory. The same expansion
    ``scontrol show job`` applies to StdOut/StdErr
    (slurm://src/api/job_info.c#slurm_get_job_stdout).
    """
    pattern = pattern or default
    replacements = {
        "%%": "\0",
        "%j": str(job.job_id),
        "%A": str(job.job_id),
        "%x": job.name or "",
        "%u": job.user or "",
        "%N": "node001",
    }
    for key, value in replacements.items():
        pattern = pattern.replace(key, value)
    pattern = pattern.replace("\0", "%")
    if not pattern.startswith("/"):
        pattern = f"{working_directory(job).rstrip('/')}/{pattern}"
    return pattern


def stdout_path(job) -> str:
    return expand_stdio_path(job.standard_output, job, "slurm-%j.out")


def stderr_path(job) -> str:
    # Without --error Slurm writes stderr into the stdout file.
    return expand_stdio_path(job.standard_error or job.standard_output, job, "slurm-%j.out")


def _environment(job) -> dict[str, str]:
    env = {
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "HOME": f"/home/{job.user}",
        "USER": job.user or "root",
        "LOGNAME": job.user or "root",
        "SLURM_JOB_ID": str(job.job_id),
        "SLURM_JOBID": str(job.job_id),
        "SLURM_JOB_NAME": job.name or "",
        "SLURM_JOB_ACCOUNT": job.account or "",
        "SLURM_JOB_PARTITION": job.partition or "",
        "SLURM_JOB_NUM_NODES": str(job.node_count or 1),
        "SLURM_SUBMIT_DIR": working_directory(job),
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
            completed = subprocess.run(
                ["bash", "-c", PRELUDE + (job.script or "")],
                cwd=cwd,
                env=_environment(job),
                stdout=out,
                stderr=err if err_path != out_path else subprocess.STDOUT,
                timeout=_timeout(),
                check=False,
                **_run_as(job),
            )
        return completed.returncode
    except subprocess.TimeoutExpired:
        with err_path.open("ab") as err:
            err.write(b"slurmstepd: error: *** JOB CANCELLED DUE TO TIME LIMIT ***\n")
        return 124
    except OSError as e:
        logger.warning("Job %s: cannot run script: %s", job.job_id, e)
        return 1
