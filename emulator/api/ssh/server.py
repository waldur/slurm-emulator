"""Thin SSH front-end exposing the emulator's filesystem + Slurm CLI.

FireCREST reaches a cluster over SSH for *all* filesystem operations, for
job stdout/stderr/script retrieval (``get_job_metadata``) and for
submit-by-path — even when the scheduler itself is driven over slurmrestd.
``HPCCluster.ssh`` is a required config field on every FireCREST cluster,
so the emulator has to answer SSH for FireCREST to boot and serve those
endpoints.

This is NOT a real sshd. For each SSH ``exec`` request we either:

* dispatch a Slurm command (``sacct``/``sacctmgr``/``sshare``/``sinfo``/
  ``scancel``/``sbatch``/``squeue``/``scontrol``/``id``) to the emulator's
  command layer, sharing the same JSON state as the REST plane; or
* run the command line for real via ``bash -c`` in a per-user sandbox home
  — real GNU coreutils produce exactly the output FireCREST parses. On macOS
  (BSD coreutils), Homebrew's GNU tools are auto-detected and preferred so
  GNU-style flags still work (see ``_command_env`` / ``_gnu_gnubin_dirs``).

Security: shell commands run as the emulator's own OS user, confined only
by the sandbox working directory. This is a dev/test tool — do not expose
it to untrusted clients. In the docker-compose e2e setup the emulator and
FireCREST share a home volume so absolute paths (e.g. ``/home/user``)
resolve identically on both sides.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import io
import os
import platform
import shlex
import subprocess
from pathlib import Path
from typing import Optional

from emulator.commands.dispatcher import SlurmEmulator
from emulator.core.database import Job
from emulator.core.scheduler import advance_job_states, job_clock_now

try:
    import asyncssh
except ImportError:  # pragma: no cover - optional dependency
    asyncssh = None  # type: ignore[assignment]

# Slurm binaries handled by the emulator rather than the shell.
_SLURM_BINS = {
    "sacct",
    "sacctmgr",
    "sshare",
    "sinfo",
    "scancel",
    "id",
    "sbatch",
    "squeue",
    "scontrol",
    "srun",
}

_SHELL_TIMEOUT = int(os.environ.get("SLURM_EMULATOR_SSH_TIMEOUT", "30"))


def _fs_root() -> Path:
    root = Path(os.environ.get("SLURM_EMULATOR_FS_ROOT", "/tmp/slurm_emulator_fs"))
    root.mkdir(parents=True, exist_ok=True)
    return root


def _user_home(user: str) -> Path:
    home = _fs_root() / "home" / (user or "root")
    home.mkdir(parents=True, exist_ok=True)
    return home


# --- GNU/BSD coreutils portability ---
#
# FireCREST builds GNU-style command lines (stat -c, ls --full-time, date -d,
# GNU tar/sed flags, cksum/sha*sum). Linux has these natively. macOS ships BSD
# variants that reject those flags, so on Darwin we prefer Homebrew's GNU tools
# by prepending their `gnubin` dirs to PATH — transparent, and covers commands
# inside pipes. No effect on Linux.
_GNU_BREW_PKGS = ("coreutils", "gnu-tar", "findutils", "gnu-sed", "grep")
_BREW_PREFIXES = ("/opt/homebrew", "/usr/local")  # Apple Silicon, Intel


@functools.lru_cache(maxsize=1)
def _gnu_gnubin_dirs() -> tuple[str, ...]:
    """Existing Homebrew GNU-tool ``gnubin`` dirs on macOS; empty elsewhere."""
    if platform.system() != "Darwin":
        return ()
    dirs = [
        str(gnubin)
        for prefix in _BREW_PREFIXES
        for pkg in _GNU_BREW_PKGS
        if (gnubin := Path(prefix) / "opt" / pkg / "libexec" / "gnubin").is_dir()
    ]
    return tuple(dirs)


def gnu_coreutils_available() -> bool:
    """True on Linux (native GNU) or macOS with Homebrew GNU tools installed."""
    return platform.system() != "Darwin" or bool(_gnu_gnubin_dirs())


def _command_env(user: str) -> dict[str, str]:
    home = _user_home(user)
    env = {**os.environ, "HOME": str(home), "USER": user or "root"}
    gnubin = _gnu_gnubin_dirs()
    if gnubin:
        env["PATH"] = os.pathsep.join((*gnubin, env.get("PATH", "")))
    return env


# --- Slurm command dispatch (shared state with the REST plane) ---


def _new_emulator() -> SlurmEmulator:
    return SlurmEmulator()


def _advance(emu: SlurmEmulator) -> None:
    if advance_job_states(emu.database, emu.time_engine):
        emu.database.save_state()


def _run_slurm(user: str, argv: list[str]) -> tuple[str, str, int]:
    """Dispatch a Slurm command, never letting it crash the SSH session.

    The emulator's command layer prints errors to stderr and raises
    SystemExit; we capture that stream and turn the exit into a real
    (stdout, stderr, code) triple for the SSH channel.
    """
    name = argv[0]
    args = argv[1:]
    err = io.StringIO()
    try:
        with contextlib.redirect_stderr(err):
            emu = _new_emulator()
            if name in {"sacct", "sacctmgr", "sshare", "sinfo", "scancel", "id"}:
                if name in {"sacct", "sshare"}:
                    _advance(emu)
                out = emu.execute_command(name, args)
                code = getattr(getattr(emu, name, None), "exit_code", 0) or 0
                return _nl(out), err.getvalue(), code
            if name == "sbatch":
                return _sbatch(emu, user, args)
            if name == "squeue":
                _advance(emu)
                return _nl(_squeue(emu)), err.getvalue(), 0
            if name == "scontrol":
                _advance(emu)
                out, serr, code = _scontrol(emu, args)
                return out, serr or err.getvalue(), code
            if name == "srun":
                return "", "srun: unsupported in emulator\n", 1
    except SystemExit as exc:
        code = exc.code if isinstance(exc.code, int) else 1
        return "", err.getvalue(), code
    except Exception as exc:  # surface as command failure, not a dropped session
        return "", err.getvalue() + f"{name}: {exc}\n", 1
    return "", f"{name}: unsupported\n", 1


def _nl(text: str) -> str:
    if text and not text.endswith("\n"):
        return text + "\n"
    return text


def _flag_value(args: list[str], short: str, long: str) -> Optional[str]:
    for i, arg in enumerate(args):
        if arg in (short, long) and i + 1 < len(args):
            return args[i + 1]
        if arg.startswith(f"{long}="):
            return arg.split("=", 1)[1]
        if short and arg.startswith(short) and len(arg) > len(short) and not arg.startswith("--"):
            return arg[len(short) :]
    return None


def _sbatch(emu: SlurmEmulator, user: str, args: list[str]) -> tuple[str, str, int]:
    name = _flag_value(args, "-J", "--job-name") or "batch"
    partition = _flag_value(args, "-p", "--partition") or "compute"
    account = _flag_value(args, "-A", "--account") or ""
    script_path = next((a for a in args if not a.startswith("-")), "")

    jid = emu.database.allocate_job_id()
    emu.database.add_job(
        Job(
            job_id=str(jid),
            account=account,
            user=user,
            state="PENDING",
            submit_time=job_clock_now(emu.time_engine),
            cluster=emu.database.current_cluster,
            name=name,
            partition=partition,
            working_directory=str(_user_home(user)),
            script=script_path,
        )
    )
    emu.database.save_state()
    return f"Submitted batch job {jid}\n", "", 0


def _squeue(emu) -> str:
    st_map = {"PENDING": "PD", "RUNNING": "R", "COMPLETED": "CD", "CANCELLED": "CA", "FAILED": "F"}
    header = "JOBID PARTITION NAME USER ST TIME NODES NODELIST(REASON)"
    rows = [header]
    for job in emu.database.jobs.values():
        if job.cluster != emu.database.current_cluster:
            continue
        st = st_map.get(job.state, job.state[:2])
        rows.append(
            f"{job.job_id} {job.partition} {job.name} {job.user} {st} 0:00 {job.node_count} node001"
        )
    return "\n".join(rows)


def _scontrol(emu, args: list[str]) -> tuple[str, str, int]:
    if len(args) >= 2 and args[0] == "show" and args[1] == "job":
        job_id = args[2] if len(args) > 2 else ""
        job = emu.database.get_job(job_id)
        if job is None:
            return "", "slurm_load_jobs error: Invalid job id specified\n", 1
        wd = job.working_directory or f"/home/{job.user}"
        out = (
            f"JobId={job.job_id} JobName={job.name}\n"
            f"   UserId={job.user}(1000) GroupId={job.user}(1000)\n"
            f"   Account={job.account} QOS={job.qos} Partition={job.partition}\n"
            f"   JobState={job.state} Reason=None Dependency=(null)\n"
            f"   StdOut={job.standard_output or f'{wd}/slurm-{job.job_id}.out'}\n"
            f"   StdErr={job.standard_error or f'{wd}/slurm-{job.job_id}.err'}\n"
            f"   Command={job.script}\n"
            f"   WorkDir={wd}\n"
        )
        return out, "", 0
    return "", "scontrol: unsupported subcommand\n", 1


# --- Shell (filesystem) dispatch ---


def _run_shell(user: str, command: str) -> tuple[str, str, int]:
    home = _user_home(user)
    env = _command_env(user)
    try:
        proc = subprocess.run(
            ["/bin/bash", "-c", command],
            cwd=str(home),
            capture_output=True,
            text=True,
            timeout=_SHELL_TIMEOUT,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return "", "command timed out\n", 124
    return proc.stdout, proc.stderr, proc.returncode


def _dispatch(user: str, command: str) -> tuple[str, str, int]:
    stripped = command.strip()
    if not stripped:
        return "", "", 0
    first = stripped.split(None, 1)[0]
    base = Path(first).name
    if base in _SLURM_BINS:
        try:
            argv = shlex.split(command)
        except ValueError:
            return "", "parse error\n", 2
        argv[0] = base
        return _run_slurm(user, argv)
    return _run_shell(user, command)


# --- asyncssh wiring ---


async def _handle_process(process) -> None:  # pragma: no cover - needs a live socket
    command = process.command
    user = process.get_extra_info("username") or "root"
    if not command:
        process.stdout.write("slurm-emulator ssh: interactive shell not supported\n")
        process.exit(0)
        return
    loop = asyncio.get_event_loop()
    stdout, stderr, code = await loop.run_in_executor(None, _dispatch, user, command)
    if stdout:
        process.stdout.write(stdout)
    if stderr:
        process.stderr.write(stderr)
    process.exit(code)


def _server_factory():  # pragma: no cover - needs asyncssh
    class _EmulatorSSHServer(asyncssh.SSHServer):
        def connection_made(self, conn) -> None:
            self._conn = conn

        def begin_auth(self, username: str) -> bool:
            # False => no authentication required. FireCREST still offers a
            # key/password; the server simply accepts the connection. Set
            # SLURM_EMULATOR_SSH_ACCEPT_ALL=0 is a no-op here (dev default).
            return False

    return _EmulatorSSHServer()


def _host_key():  # pragma: no cover - needs asyncssh
    path = os.environ.get("SLURM_EMULATOR_SSH_HOST_KEY")
    if path and Path(path).exists():
        return path
    key = asyncssh.generate_private_key("ssh-rsa")
    if path:
        key.write_private_key(path)
        return path
    return key


async def _serve(host: str, port: int) -> None:  # pragma: no cover - needs a live socket
    await asyncssh.listen(
        host,
        port,
        server_factory=_server_factory,
        server_host_keys=[_host_key()],
        process_factory=_handle_process,
    )
    print(f"slurm-ssh-emulator listening on {host}:{port}")
    await asyncio.Future()  # run forever


def main() -> None:
    """Console-script entry point for ``slurm-ssh-emulator``."""
    if asyncssh is None:
        msg = (
            "asyncssh is required for the SSH plane. Install with: "
            "uv sync --extra ssh  (or pip install 'slurm-emulator[ssh]')"
        )
        raise SystemExit(msg)
    host = os.environ.get("SLURM_EMULATOR_SSH_HOST", "0.0.0.0")
    port = int(os.environ.get("SLURM_EMULATOR_SSH_PORT", "2222"))
    if not gnu_coreutils_available():
        print(
            "warning: GNU coreutils not found on macOS — FireCREST sends GNU-style "
            "commands (stat -c, ls --full-time, tar ...) that BSD tools reject. "
            "Install with: brew install coreutils gnu-tar findutils gnu-sed grep"
        )
    elif _gnu_gnubin_dirs():
        print(f"using GNU tools from: {os.pathsep.join(_gnu_gnubin_dirs())}")
    try:
        asyncio.run(_serve(host, port))
    except (OSError, KeyboardInterrupt) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
