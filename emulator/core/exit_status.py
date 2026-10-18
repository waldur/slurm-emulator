"""Job exit codes as Slurm records and prints them.

Slurm stores a job's exit code as the batch script's wait(2) status
(slurm://slurm/slurm.h#job_info_t.exit_code): ``WEXITSTATUS`` when the script
exited, ``WTERMSIG`` when a signal killed it. The emulator keeps the code as
Python's ``returncode`` -- the exit status, or minus the signal number -- and
converts here:

* sacct and scontrol print ``<exit>:<signal>``
  (slurm://src/sacct/print.c#PRINT_EXITCODE, slurm://src/scontrol/info_job.c#_sprint_job_info);
* slurmrestd dumps ``{status, return_code, signal}``
  (slurm://src/plugins/data_parser/v0.0.45/parsers.c#"DUMP_FUNC(PROCESS_EXIT_CODE)"@26.05+);
* a signalled script fails the job with reason ``RaisedSignal`` and the
  description ``RaisedSignal:<n>(<strsignal>)``, a non-zero exit with
  ``NonZeroExitCode``, a time limit with ``TimeLimit``
  (slurm://src/slurmctld/job_mgr.c#_job_complete).

A job that hits its time limit is logged to the accounting database when the
limit fires, before the script is signalled, so slurmdbd keeps ``0:0``; the
controller then records the batch script's SIGTERM, ``0:15``
(slurm://src/slurmctld/job_mgr.c#_job_timed_out).
"""

from __future__ import annotations

from typing import Any, Optional

# Signal the batch script gets at its time limit (slurmstepd: SIGTERM, then
# SIGKILL after KillWait).
TIME_LIMIT_SIGNAL = 15  # SIGTERM

_FAILED_STATES = ("FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "BOOT_FAIL", "DEADLINE")

# strsignal() texts (glibc) for the signals a batch script plausibly dies of.
_STRSIGNAL = {
    1: "Hangup",
    2: "Interrupt",
    3: "Quit",
    4: "Illegal instruction",
    6: "Aborted",
    7: "Bus error",
    8: "Floating point exception",
    9: "Killed",
    10: "User defined signal 1",
    11: "Segmentation fault",
    12: "User defined signal 2",
    13: "Broken pipe",
    14: "Alarm clock",
    15: "Terminated",
    24: "CPU time limit exceeded",
    25: "File size limit exceeded",
}


def effective(state: str, code: Optional[int]) -> int:
    """The job's code; unknown (the script never ran) follows the state."""
    if code is not None:
        return code
    return 1 if state.startswith(_FAILED_STATES) else 0


def split(code: int) -> tuple[int, int]:
    """(exit status, signal) of a Python-style return code."""
    if code < 0:
        return 0, -code
    return code & 0xFF, 0


def colon(state: str, code: Optional[int]) -> str:
    """``<exit>:<signal>``, as sacct's ExitCode and scontrol's ExitCode print it."""
    exit_status, sig = split(effective(state, code))
    return f"{exit_status}:{sig}"


# slurm://src/common/proc_args.c#signals_mapping (Linux numbering); other
# signals dump as their number (slurm://src/common/proc_args.c#sig_num2name).
_SIGNAL_NAMES = {
    1: "HUP",
    2: "INT",
    3: "QUIT",
    6: "ABRT",
    9: "KILL",
    14: "ALRM",
    15: "TERM",
    17: "CHLD",
    10: "USR1",
    12: "USR2",
    13: "PIPE",
    23: "URG",
    18: "CONT",
    19: "STOP",
    20: "TSTP",
    21: "TTIN",
    22: "TTOU",
    24: "XCPU",
}


def signal_name(sig: int) -> str:
    return _SIGNAL_NAMES.get(sig, str(sig))


def strsignal(sig: int) -> str:
    return _STRSIGNAL.get(sig, f"Unknown signal {sig}")


def verbose(state: str, code: Optional[int]) -> dict[str, Any]:
    """PROCESS_EXIT_CODE_VERBOSE for slurmrestd."""
    from emulator.api.slurmrestd.schemas import uint_no_val  # noqa: PLC0415 - cycle

    exit_status, sig = split(effective(state, code))
    if sig:
        return {
            "status": ["SIGNALED"],
            "return_code": uint_no_val(),
            "signal": {"id": uint_no_val(sig), "name": signal_name(sig)},
        }
    return {
        "status": ["ERROR"] if exit_status else ["SUCCESS"],
        "return_code": uint_no_val(exit_status),
        "signal": {"id": uint_no_val(), "name": ""},
    }


def failure_reason(state: str, code: Optional[int]) -> tuple[str, str]:
    """(state_reason, state_description) of a finished job."""
    if state == "TIMEOUT":
        return "TimeLimit", ""
    if code is None or state != "FAILED":
        return "None", ""
    exit_status, sig = split(code)
    if sig:
        return "RaisedSignal", f"RaisedSignal:{sig}({strsignal(sig)})"
    if exit_status:
        return "NonZeroExitCode", ""
    return "None", ""
