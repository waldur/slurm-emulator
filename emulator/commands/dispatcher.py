"""Command dispatcher for SLURM emulator."""

import sys
from pathlib import Path
from typing import ClassVar

from emulator import __version__
from emulator.commands.sacct import SacctEmulator
from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


class SlurmEmulator:
    """Main SLURM emulator class."""

    def __init__(self):
        self.time_engine = TimeEngine()
        self.database = SlurmDatabase()
        self.sacctmgr = SacctmgrEmulator(self.database, self.time_engine)
        self.sacct = SacctEmulator(self.database, self.time_engine)

        # Load existing state
        self.database.load_state()

    # Flags that are only valid for specific commands
    _VALID_FLAGS: ClassVar[dict[str, set[str]]] = {
        "sacctmgr": {"--parsable2", "--noheader", "--immediate"},
        "sacct": {"--parsable2", "--noheader"},
        "scancel": set(),
        "id": set(),
        "sinfo": {"-V"},
    }

    # The set of SLURM formatting/control flags that we validate
    _SLURM_FLAGS = frozenset({"--parsable2", "--noheader", "--immediate"})

    def validate_flags(self, command_name: str, args: list[str]) -> None:
        """Validate flags for the given command.

        Raises SystemExit for flags not supported by the command, matching
        real SLURM behavior.
        """
        valid = self._VALID_FLAGS.get(command_name, set())
        invalid = [a for a in args if a in self._SLURM_FLAGS and a not in valid]
        if invalid:
            raise SystemExit(f"{command_name}: error: unrecognized arguments: {' '.join(invalid)}")

    def execute_command(self, command_name: str, args: list[str]) -> str:
        """Execute a SLURM command and return output."""
        if command_name == "sacctmgr":
            return self.sacctmgr.handle_command(args)
        if command_name == "sacct":
            return self.sacct.handle_command(args)
        if command_name == "sinfo":
            return self._handle_sinfo(args)
        if command_name == "scancel":
            return self._handle_scancel(args)
        if command_name == "id":
            return self._handle_id(args)
        return f"slurm-emulator: Unknown command: {command_name}"

    def _handle_sinfo(self, args: list[str]) -> str:
        """Handle sinfo command."""
        if args and args[0] == "-V":
            return f"slurm-emulator {__version__}"

        # Return basic cluster info
        return """PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
debug*       up   infinite      4   idle node[001-004]
compute      up   infinite     96   idle node[005-100]"""

    def _handle_scancel(self, args: list[str]) -> str:
        """Handle scancel command."""
        # Parse arguments
        account = None
        user = None
        force = False

        for arg in args:
            if arg.startswith("-A="):
                account = arg.split("=", 1)[1]
            elif arg.startswith("-u="):
                user = arg.split("=", 1)[1]
            elif arg == "-f":
                force = True

        if account:
            jobs_cancelled = 0
            jobs = self.database.list_jobs(account=account, user=user)
            for job in jobs:
                if job.state in ["RUNNING", "PENDING"]:
                    job.state = "CANCELLED"
                    jobs_cancelled += 1

            if jobs_cancelled > 0:
                return f"scancel: Cancelled {jobs_cancelled} job(s)"
            return "scancel: No jobs found to cancel"
        return "scancel: No account specified"

    def _handle_id(self, args: list[str]) -> str:
        """Handle id command for user validation."""
        if not args:
            return "id: missing operand"

        if args[0] == "-u":
            if len(args) < 2:
                return "id: missing username"
            username = args[1]
        else:
            username = args[0]

        # Check if user exists in our database
        user = self.database.get_user(username)
        if user:
            return "1000"  # Return fake UID
        return f"id: {username}: no such user"


# Global emulator instance
_emulator = None


def get_emulator():
    """Get global emulator instance."""
    global _emulator
    if _emulator is None:
        _emulator = SlurmEmulator()
    return _emulator


def sacctmgr_main():
    """Entry point for sacctmgr command."""
    emulator = get_emulator()
    args = sys.argv[1:]

    # Validate flags (all three are valid for sacctmgr)
    emulator.validate_flags("sacctmgr", args)

    # Filter out formatting/control flags handled by the emulator internally
    filtered_args = [a for a in args if a not in ("--parsable2", "--noheader", "--immediate")]

    try:
        output = emulator.execute_command("sacctmgr", filtered_args)
        print(output)
    except Exception as e:
        print(f"sacctmgr: error: {e}", file=sys.stderr)
        sys.exit(1)


def sacct_main():
    """Entry point for sacct command."""
    emulator = get_emulator()
    args = sys.argv[1:]

    # Validate flags (--immediate is NOT valid for sacct)
    try:
        emulator.validate_flags("sacct", args)
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

    # Filter out formatting flags handled by the emulator internally
    filtered_args = [a for a in args if a not in ("--parsable2", "--noheader")]

    try:
        output = emulator.execute_command("sacct", filtered_args)
        print(output)
    except Exception as e:
        print(f"sacct: error: {e}", file=sys.stderr)
        sys.exit(1)


def sinfo_main():
    """Entry point for sinfo command."""
    emulator = get_emulator()
    args = sys.argv[1:]

    try:
        output = emulator.execute_command("sinfo", args)
        print(output)
    except Exception as e:
        print(f"sinfo: error: {e}", file=sys.stderr)
        sys.exit(1)


def scancel_main():
    """Entry point for scancel command."""
    emulator = get_emulator()
    args = sys.argv[1:]

    # Validate flags (--immediate, --parsable2, --noheader are NOT valid for scancel)
    try:
        emulator.validate_flags("scancel", args)
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

    try:
        output = emulator.execute_command("scancel", args)
        print(output)
    except Exception as e:
        print(f"scancel: error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    # Determine which command was called based on argv[0]
    command_name = Path(sys.argv[0]).name

    if command_name == "sacctmgr":
        sacctmgr_main()
    elif command_name == "sacct":
        sacct_main()
    elif command_name == "sinfo":
        sinfo_main()
    elif command_name == "scancel":
        scancel_main()
    else:
        emulator = get_emulator()
        args = sys.argv[1:]
        if args:
            output = emulator.execute_command(args[0], args[1:])
            print(output)
        else:
            print("Usage: slurm-emulator <command> [args...]")
