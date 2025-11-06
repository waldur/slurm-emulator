"""Main CLI interface for SLURM emulator with time manipulation."""

import atexit
import contextlib
import os
import readline
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from emulator.commands.dispatcher import SlurmEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.slurm_config import SlurmConfigParser
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import UsageSimulator
from emulator.periodic_limits.calculator import PeriodicLimitsCalculator
from emulator.periodic_limits.qos_manager import QoSManager
from emulator.scenarios.scenario_registry import ActionType, ScenarioRegistry, ScenarioType
from emulator.scenarios.sequence_scenario import SequenceScenario


class EmulatorCLI:
    """Interactive CLI for SLURM emulator."""

    def __init__(self, slurm_config_path: Optional[str] = None):
        self.time_engine = TimeEngine()
        self.database = SlurmDatabase()

        # Load SLURM configuration if provided
        self.slurm_config = None
        if slurm_config_path:
            try:
                self.slurm_config = SlurmConfigParser(slurm_config_path)
                self.slurm_config.print_config_summary()

                # Validate configuration
                warnings = self.slurm_config.validate_configuration()
                if warnings:
                    print("\n⚠️  Configuration Warnings:")
                    for warning in warnings:
                        print(f"   - {warning}")
            except Exception as e:
                print(f"❌ Error loading SLURM configuration: {e}")
                print("   Using default configuration values")
                self.slurm_config = None

        # Initialize components with configuration
        self.usage_simulator = UsageSimulator(self.time_engine, self.database)
        if self.slurm_config:
            self.usage_simulator.billing_weights = self.slurm_config.get_tres_billing_weights()

        self.limits_calculator = PeriodicLimitsCalculator(
            self.database, self.time_engine, self.slurm_config
        )
        self.qos_manager = QoSManager(self.database, self.time_engine)

        # Create SLURM emulator with shared components
        self.slurm_emulator = SlurmEmulator()
        # Override the emulator's components to use ours
        self.slurm_emulator.database = self.database
        self.slurm_emulator.time_engine = self.time_engine
        self.slurm_emulator.sacctmgr.database = self.database
        self.slurm_emulator.sacct.database = self.database
        self.scenario_registry = ScenarioRegistry()

        # Load existing state
        self.database.load_state()

        # State management
        self.checkpoints: dict[str, Any] = {}

        # Auto-completion setup
        self.autocomplete_enabled = False
        self._completion_cache: list[str] = []
        self._last_completion_line = ""
        self._setup_autocomplete()

    def run(self) -> None:
        """Run the interactive CLI."""
        print("🎮 SLURM Emulator - Time Travel Edition")

        if self.autocomplete_enabled:
            print("💡 Auto-completion: Use [TAB] to complete commands, [↑][↓] for history")
        else:
            print("💡 Auto-completion: Use 'complete <command>' for completion help")

        print("Type 'help' for commands, 'exit' to quit")
        print(f"Current time: {self.time_engine.get_current_time()}")
        print(f"Current period: {self.time_engine.get_current_quarter()}")

        # Quick start tips
        if not self.database.list_accounts() or len(self.database.list_accounts()) <= 1:
            print("\n🚀 Quick start:")
            print('   account create test "Test Account" 1000')
            print("   usage inject user1 200 test")
            print("   scenario list")
            print("   scenario describe qos_thresholds")

        while True:
            try:
                command = input("\nslurm-emulator> ").strip()
                if not command:
                    continue

                if command.lower() in ["exit", "quit"]:
                    print("👋 Goodbye!")
                    break
                if command.lower() == "help":
                    self._show_help()
                else:
                    self._execute_command(command)

            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except EOFError:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")

    def _setup_autocomplete(self) -> None:
        """Setup readline auto-completion."""
        try:
            # Check if we're in a proper terminal
            if not sys.stdin.isatty():
                return  # No auto-completion for non-terminal input

            # Configure readline for better compatibility
            readline.parse_and_bind("tab: complete")
            readline.parse_and_bind("set completion-ignore-case on")
            readline.parse_and_bind("set show-all-if-ambiguous on")
            readline.parse_and_bind("set completion-map-case on")

            # Set delimiter characters (don't break on = or -)
            readline.set_completer_delims(" \t\n;")

            # Set the completer function
            readline.set_completer(self._completer)

            # Setup history file
            history_file = Path("~/.slurm_emulator_history").expanduser()
            with contextlib.suppress(FileNotFoundError, PermissionError, OSError):
                readline.read_history_file(history_file)

            # Save history on exit (with error handling)
            def safe_write_history():
                with contextlib.suppress(PermissionError, OSError):
                    readline.write_history_file(history_file)

            atexit.register(safe_write_history)

            # Limit history size
            readline.set_history_length(1000)

            # Enable auto-completion feedback
            self.autocomplete_enabled = True
            print("⌨️  Auto-completion enabled (use TAB for completion)")

        except (ImportError, OSError):
            # readline not available or terminal issues
            self.autocomplete_enabled = False
            print("⚠️  Auto-completion not available (readline unavailable)")
            print("💡 Commands still work normally, just type them out fully")

    def _completer(self, text: str, state: int) -> Optional[str]:
        """Auto-completion function."""
        try:
            line = readline.get_line_buffer()
            parts = line.split()

            # Cache completions for this line
            if not hasattr(self, "_completion_cache") or self._last_completion_line != line:
                self._last_completion_line = line

                # Determine what we're completing
                if not parts or (len(parts) == 1 and not line.endswith(" ")):
                    # Completing main command
                    commands = self._get_main_commands()
                    self._completion_cache = [cmd for cmd in commands if cmd.startswith(text)]
                else:
                    # Completing subcommand or parameter
                    main_cmd = parts[0]
                    if len(parts) == 1 and line.endswith(" "):
                        # Just completed main command, show subcommands
                        self._completion_cache = self._get_subcommands(main_cmd)
                    # Complete parameters
                    elif line.endswith(" "):
                        # Starting new parameter
                        self._completion_cache = self._get_parameters(main_cmd, parts[1:], "")
                    else:
                        # Completing current parameter
                        self._completion_cache = self._get_parameters(main_cmd, parts[1:-1], text)

            # Return match if available
            if state < len(self._completion_cache):
                return self._completion_cache[state]
            return None

        except Exception as e:
            # Debug completion errors
            if hasattr(self, "autocomplete_enabled") and os.getenv("SLURM_EMULATOR_DEBUG"):
                print(f"\n❌ Completion error: {e}")
                print(f"   Line: '{line}'")
                print(f"   Text: '{text}'")
                print(f"   State: {state}")
            return None

    def _get_main_commands(self) -> list[str]:
        """Get list of main commands."""
        return [
            "time",
            "usage",
            "scenario",
            "checkpoint",
            "status",
            "limits",
            "qos",
            "account",
            "config",
            "cleanup",
            "complete",
            "sacctmgr",
            "sacct",
            "sinfo",
            "help",
            "exit",
        ]

    def _get_subcommands(self, main_cmd: str) -> list[str]:
        """Get subcommands for a main command."""
        subcommands = {
            "time": ["advance", "set"],
            "usage": ["inject", "show", "pattern"],
            "scenario": ["run", "list", "describe", "steps", "validate"],
            "checkpoint": ["create", "restore", "list"],
            "limits": ["calculate", "show", "apply"],
            "qos": ["show", "set", "check"],
            "account": ["create", "list", "show", "delete"],
            "config": ["show", "validate", "reload"],
            "cleanup": ["all", "scenario", "account"],
            "sacctmgr": ["add", "modify", "remove", "list", "show"],
            "sacct": ["--accounts", "--users", "--format", "--starttime", "--endtime"],
        }
        return subcommands.get(main_cmd, [])

    def _get_parameters(self, main_cmd: str, parts: list[str], text: str) -> list[str]:
        """Get parameter completions."""
        if main_cmd == "scenario" and len(parts) >= 1:
            if parts[0] in ["run", "describe", "steps", "validate"]:
                # Complete scenario names
                scenarios = list(self.scenario_registry.scenarios.keys())
                if text:
                    return [s for s in scenarios if s.startswith(text)]
                return scenarios
        elif main_cmd == "account" and len(parts) >= 1:
            if parts[0] in ["show", "delete"]:
                # Complete account names
                accounts = [acc.name for acc in self.database.list_accounts()]
                return [acc for acc in accounts if acc.startswith(text)]
        elif main_cmd == "usage" and len(parts) >= 3:
            if parts[0] == "inject":
                # Complete account names for usage injection
                accounts = [acc.name for acc in self.database.list_accounts()]
                return [acc for acc in accounts if acc.startswith(text)]
        elif main_cmd == "checkpoint" and len(parts) >= 1:
            if parts[0] == "restore":
                # Complete checkpoint names
                checkpoint_names = list(self.checkpoints.keys())
                return [name for name in checkpoint_names if name.startswith(text)]
        elif main_cmd == "config" and len(parts) >= 1:
            if parts[0] == "reload":
                # Complete file paths
                return self._complete_filepath(text)
        elif main_cmd == "time" and len(parts) >= 2:
            if parts[0] == "advance" and len(parts) == 2:
                # Complete time units
                units = ["days", "months", "quarters"]
                return [unit for unit in units if unit.startswith(text)]
        elif main_cmd == "sacctmgr":
            return self._complete_sacctmgr_command(parts, text)
        elif main_cmd == "sacct":
            return self._complete_sacct_command(parts, text)
        elif main_cmd == "qos" and len(parts) >= 1:
            if parts[0] in ["show", "check"]:
                # Complete account names
                accounts = [acc.name for acc in self.database.list_accounts()]
                return [acc for acc in accounts if acc.startswith(text)]
            if parts[0] == "set" and len(parts) >= 2:
                if len(parts) == 2:
                    # Complete account names
                    accounts = [acc.name for acc in self.database.list_accounts()]
                    return [acc for acc in accounts if acc.startswith(text)]
                if len(parts) == 3:
                    # Complete QoS levels
                    qos_levels = self.qos_manager.list_qos_levels()
                    return [qos for qos in qos_levels if qos.startswith(text)]
        elif main_cmd == "limits" and len(parts) >= 1:
            if parts[0] in ["calculate", "show", "apply"]:
                # Complete account names
                accounts = [acc.name for acc in self.database.list_accounts()]
                return [acc for acc in accounts if acc.startswith(text)]
        elif main_cmd == "cleanup" and len(parts) >= 1:
            if parts[0] == "scenario":
                # Complete scenario names
                scenarios = list(self.scenario_registry.scenarios.keys())
                return [s for s in scenarios if s.startswith(text)]
            if parts[0] == "account":
                # Complete account names
                accounts = [acc.name for acc in self.database.list_accounts()]
                return [acc for acc in accounts if acc.startswith(text)]

        return []

    def _complete_filepath(self, text: str) -> list[str]:
        """Complete file paths."""
        try:
            if text:
                path_matches = list(Path().glob(text + "*"))
                matches = [str(p) for p in path_matches]
            else:
                path_matches = list(Path().glob("*"))
                matches = [str(p) for p in path_matches]
            return matches
        except Exception:
            return []

    def _complete_sacctmgr_command(self, parts: list[str], text: str) -> list[str]:
        """Complete sacctmgr commands and parameters."""
        if not parts:
            # Complete main sacctmgr commands
            commands = ["add", "modify", "remove", "delete", "list", "show", "-V", "--help"]
            return [cmd for cmd in commands if cmd.startswith(text)]

        if parts[0] in ["add", "modify", "remove", "delete", "list", "show"]:
            if len(parts) == 1:
                # Complete entity types
                entities = ["account", "user", "association", "tres", "qos"]
                return [entity for entity in entities if entity.startswith(text)]
            if len(parts) >= 2:
                # Complete based on entity type and command
                entity = parts[1]
                return self._complete_sacctmgr_entity_params(parts[0], entity, parts[2:], text)

        return []

    def _complete_sacctmgr_entity_params(
        self, command: str, entity: str, remaining_parts: list[str], text: str
    ) -> list[str]:
        """Complete parameters for specific sacctmgr entity commands."""
        if command == "add" and entity == "account":
            if len(remaining_parts) == 0:
                # Complete account name with existing accounts as suggestions
                accounts = [acc.name for acc in self.database.list_accounts()]
                if text:
                    return [acc for acc in accounts if acc.startswith(text)]
                return ["new-account", "test-account", *accounts]
            if len(remaining_parts) >= 1:
                # Complete account creation parameters
                params = ["description=", "organization=", "parent="]
                return [param for param in params if param.startswith(text)]

        elif command == "add" and entity == "user":
            if len(remaining_parts) == 0:
                # Complete username
                users = [user.name for user in self.database.users.values()]
                if text:
                    return [user for user in users if user.startswith(text)]
                return ["new-user", "testuser", *users]
            if len(remaining_parts) >= 1:
                # Complete user creation parameters
                params = ["account=", "DefaultAccount="]
                if any(part.startswith("account=") for part in remaining_parts):
                    # Complete account names for account= parameter
                    accounts = [acc.name for acc in self.database.list_accounts()]
                    return [acc for acc in accounts if acc.startswith(text)]
                return [param for param in params if param.startswith(text)]

        elif command == "modify" and entity == "account":
            if len(remaining_parts) == 0:
                # Complete account names
                accounts = [acc.name for acc in self.database.list_accounts()]
                return [acc for acc in accounts if acc.startswith(text)]
            if "set" in remaining_parts:
                # Complete modification parameters
                set_index = remaining_parts.index("set")
                if len(remaining_parts) > set_index:
                    params = [
                        "fairshare=",
                        "qos=",
                        "GrpTRESMins=",
                        "MaxTRESMins=",
                        "RawUsage=",
                        "parent=",
                        "description=",
                    ]

                    # Smart completion for specific parameters
                    if text.startswith("qos="):
                        qos_levels = self.qos_manager.list_qos_levels()
                        qos_text = text[4:]  # Remove 'qos=' prefix
                        return [f"qos={qos}" for qos in qos_levels if qos.startswith(qos_text)]
                    if text.startswith("GrpTRESMins="):
                        # Complete TRES types
                        tres_types = ["billing=", "CPU=", "Mem=", "GRES/gpu="]
                        tres_text = text[12:]  # Remove 'GrpTRESMins=' prefix
                        return [
                            f"GrpTRESMins={tres}"
                            for tres in tres_types
                            if tres.startswith(tres_text)
                        ]
                    if text.startswith("MaxTRESMins="):
                        # Complete TRES types
                        tres_types = ["billing=", "CPU=", "Mem=", "GRES/gpu="]
                        tres_text = text[12:]  # Remove 'MaxTRESMins=' prefix
                        return [
                            f"MaxTRESMins={tres}"
                            for tres in tres_types
                            if tres.startswith(tres_text)
                        ]
                    if text.startswith("RawUsage="):
                        return ["RawUsage=0"]
                    return [param for param in params if param.startswith(text)]
            else:
                # Complete 'set' keyword
                return ["set"] if "set".startswith(text) else []

        elif command in ["list", "show"] and entity == "account":
            if len(remaining_parts) == 0 or (
                len(remaining_parts) == 1 and not any("=" in part for part in remaining_parts)
            ):
                # Complete account names or format parameters
                if text.startswith("format="):
                    format_options = ["account", "description", "organization", "fairshare", "qos"]
                    return [f"format={opt}" for opt in format_options if opt.startswith(text[7:])]
                accounts = [acc.name for acc in self.database.list_accounts()]
                return [acc for acc in accounts if acc.startswith(text)] + ["format="]

        elif command == "show" and entity == "association":
            # Complete association parameters
            if "where" not in remaining_parts:
                return ["where"] if "where".startswith(text) else []
            where_index = remaining_parts.index("where")
            if len(remaining_parts) > where_index:
                params = ["user=", "account="]
                if text.startswith("user="):
                    # Complete usernames
                    users = [user.name for user in self.database.users.values()]
                    user_text = text[5:]
                    return [f"user={user}" for user in users if user.startswith(user_text)]
                if text.startswith("account="):
                    # Complete account names
                    accounts = [acc.name for acc in self.database.list_accounts()]
                    acc_text = text[8:]
                    return [f"account={acc}" for acc in accounts if acc.startswith(acc_text)]
                return [param for param in params if param.startswith(text)]

        return []

    def _complete_sacct_command(self, parts: list[str], text: str) -> list[str]:
        """Complete sacct commands and parameters."""
        # Common sacct flags and parameters
        flags = [
            "--accounts=",
            "--users=",
            "--format=",
            "--starttime=",
            "--endtime=",
            "--allocations",
            "--allusers",
            "--noconvert",
            "--truncate",
            "-V",
            "--account=",
            "--user=",
            "-a",
        ]

        # If text starts with a flag, provide specific completions
        if text.startswith("--accounts="):
            accounts = [acc.name for acc in self.database.list_accounts()]
            acc_text = text[11:]  # Remove '--accounts=' prefix
            return [f"--accounts={acc}" for acc in accounts if acc.startswith(acc_text)]
        if text.startswith("--account="):
            accounts = [acc.name for acc in self.database.list_accounts()]
            acc_text = text[10:]  # Remove '--account=' prefix
            return [f"--account={acc}" for acc in accounts if acc.startswith(acc_text)]
        if text.startswith(("--users=", "--user=")):
            users = [user.name for user in self.database.users.values()]
            prefix_len = 8 if text.startswith("--users=") else 7
            user_text = text[prefix_len:]
            prefix = text[:prefix_len]
            return [f"{prefix}{user}" for user in users if user.startswith(user_text)]
        if text.startswith("--format="):
            format_options = [
                "Account",
                "User",
                "JobID",
                "JobName",
                "Partition",
                "State",
                "Elapsed",
                "Timelimit",
                "NodeList",
                "ReqTRES",
            ]
            format_text = text[9:]  # Remove '--format=' prefix
            return [f"--format={opt}" for opt in format_options if opt.startswith(format_text)]
        if text.startswith(("--starttime=", "--endtime=")):
            # Provide common time formats
            current_year = self.time_engine.get_current_time().year
            time_examples = [
                f"{current_year}-01-01",
                f"{current_year}-04-01",
                f"{current_year}-07-01",
                f"{current_year}-01-01T00:00:00",
                "now",
                "today",
                "yesterday",
            ]
            prefix_len = 12 if text.startswith("--starttime=") else 10
            time_text = text[prefix_len:]
            prefix = text[:prefix_len]
            return [f"{prefix}{time}" for time in time_examples if time.startswith(time_text)]
        # Complete flag names
        return [flag for flag in flags if flag.startswith(text)]

    def _execute_command(self, command: str) -> None:
        """Execute a CLI command."""
        parts = command.split()
        if not parts:
            return

        cmd = parts[0].lower()
        args = parts[1:]

        if cmd == "time":
            self._handle_time_commands(args)
        elif cmd == "usage":
            self._handle_usage_commands(args)
        elif cmd == "scenario":
            self._handle_scenario_commands(args)
        elif cmd == "checkpoint":
            self._handle_checkpoint_commands(args)
        elif cmd == "status":
            self._show_status(args)
        elif cmd == "limits":
            self._handle_limits_commands(args)
        elif cmd == "qos":
            self._handle_qos_commands(args)
        elif cmd == "account":
            self._handle_account_commands(args)
        elif cmd == "config":
            self._handle_config_commands(args)
        elif cmd == "complete":
            self._handle_manual_completion(args)
        elif cmd == "cleanup":
            self._handle_cleanup_commands(args)
        elif cmd in ["sacctmgr", "sacct", "sinfo", "scancel"]:
            # Direct SLURM command execution
            output = self.slurm_emulator.execute_command(cmd, args)
            print(output)
        else:
            print(f"❌ Unknown command: {cmd}")
            print(
                "Type 'help' for available commands or 'complete <partial_command>' for completion"
            )

    def _handle_time_commands(self, args: list[str]) -> None:
        """Handle time manipulation commands."""
        if not args:
            current = self.time_engine.get_current_time()
            period = self.time_engine.get_current_quarter()
            print(f"⏰ Current time: {current}")
            print(f"📅 Current period: {period}")
            return

        subcommand = args[0].lower()

        if subcommand == "advance":
            if len(args) < 3:
                print("Usage: time advance <amount> <unit>")
                print("Units: days, months, quarters")
                return

            try:
                amount = int(args[1])
                unit = args[2].lower()

                if unit in ["day", "days"]:
                    self.time_engine.advance_time(days=amount)
                elif unit in ["month", "months"]:
                    self.time_engine.advance_time(months=amount)
                elif unit in ["quarter", "quarters"]:
                    self.time_engine.advance_time(quarters=amount)
                else:
                    print("❌ Invalid unit. Use: days, months, quarters")
                    return

                print(f"⏭️  Advanced {amount} {unit}")
                print(f"⏰ New time: {self.time_engine.get_current_time()}")
                print(f"📅 New period: {self.time_engine.get_current_quarter()}")

            except ValueError:
                print("❌ Invalid amount. Must be a number.")

        elif subcommand == "set":
            if len(args) < 2:
                print("Usage: time set YYYY-MM-DD [HH:MM:SS]")
                return

            try:
                if len(args) == 2:
                    # Date only
                    target_time = datetime.fromisoformat(args[1])
                else:
                    # Date and time
                    target_time = datetime.fromisoformat(f"{args[1]} {args[2]}")

                old_period = self.time_engine.get_current_quarter()
                self.time_engine.set_time(target_time)
                new_period = self.time_engine.get_current_quarter()

                print(f"🎯 Time set to {target_time}")
                print(f"📅 Period: {old_period} → {new_period}")

                if old_period != new_period:
                    print("🔄 Period transition detected!")

            except ValueError:
                print("❌ Invalid date format. Use YYYY-MM-DD [HH:MM:SS]")

        else:
            print("❌ Unknown time subcommand")
            print("Available: advance, set")

    def _handle_usage_commands(self, args: list[str]) -> None:
        """Handle usage simulation commands."""
        if not args:
            print("Usage commands: inject, show, pattern")
            return

        subcommand = args[0].lower()

        if subcommand == "inject":
            if len(args) < 3:
                print("Usage: usage inject <user> <amount> [account]")
                return

            user = args[1]
            try:
                amount = float(args[2])
                account = args[3] if len(args) > 3 else "default_account"

                self.usage_simulator.inject_usage(account, user, amount)

                # Show updated usage
                total = self.database.get_total_usage(
                    account, self.time_engine.get_current_quarter()
                )
                print(f"📊 Total usage for {account}: {total}Nh")

            except ValueError:
                print("❌ Invalid amount. Must be a number.")

        elif subcommand == "show":
            account = args[1] if len(args) > 1 else "default_account"
            period = args[2] if len(args) > 2 else self.time_engine.get_current_quarter()

            usage = self.database.get_total_usage(account, period)
            print(f"📊 {account} usage in {period}: {usage}Nh")

            # Show breakdown by user
            records = self.database.get_usage_records(account=account, period=period)
            user_usage: dict[str, float] = {}
            for record in records:
                user_usage[record.user] = user_usage.get(record.user, 0) + record.node_hours

            if user_usage:
                print("   User breakdown:")
                for user, user_total in user_usage.items():
                    print(f"   - {user}: {user_total}Nh")

        elif subcommand == "pattern":
            print("Usage pattern simulation not yet implemented")

        else:
            print("❌ Unknown usage subcommand")
            print("Available: inject, show, pattern")

    def _handle_scenario_commands(self, args: list[str]) -> None:
        """Handle scenario execution commands."""
        if not args:
            print("Scenario commands: run, list, describe, steps, validate, search")
            return

        subcommand = args[0].lower()

        if subcommand == "list":
            self._list_scenarios(args[1:])

        elif subcommand == "describe":
            if len(args) < 2:
                print("Usage: scenario describe <scenario_name>")
                return
            self._describe_scenario(args[1])

        elif subcommand == "steps":
            if len(args) < 2:
                print("Usage: scenario steps <scenario_name>")
                return
            self._show_scenario_steps(args[1])

        elif subcommand == "run":
            if len(args) < 2:
                print("Usage: scenario run <scenario_name> [--interactive] [--step-by-step]")
                return
            scenario_name = args[1]
            interactive = "--interactive" in args
            step_by_step = "--step-by-step" in args

            if scenario_name == "sequence":
                # Use legacy sequence scenario
                scenario = SequenceScenario(self.time_engine, self.database)
                result = scenario.run_complete_scenario(interactive or step_by_step)

                if result["status"] == "completed":
                    print(f"\n✅ Scenario '{scenario_name}' completed successfully!")
                    print(f"📊 Summary: {result['summary']}")
                else:
                    print(
                        f"\n❌ Scenario '{scenario_name}' failed: {result.get('error', 'Unknown error')}"
                    )
            else:
                self._run_registry_scenario(scenario_name, interactive, step_by_step)

        elif subcommand == "validate":
            if len(args) < 2:
                print("Usage: scenario validate <scenario_name>")
                return
            self._validate_scenario(args[1])

        elif subcommand == "search":
            if len(args) < 2:
                print("Usage: scenario search <query>")
                return
            self._search_scenarios(" ".join(args[1:]))

        else:
            print("❌ Unknown scenario subcommand")
            print("Available: run, list, describe, steps, validate, search")

    def _handle_checkpoint_commands(self, args: list[str]) -> None:
        """Handle checkpoint management commands."""
        if not args:
            print("Checkpoint commands: create, restore, list")
            return

        subcommand = args[0].lower()

        if subcommand == "create":
            if len(args) < 2:
                print("Usage: checkpoint create <name>")
                return

            name = args[1]
            self.checkpoints[name] = {
                "time": self.time_engine.get_current_time(),
                "created_at": datetime.now(),
                "period": self.time_engine.get_current_quarter(),
            }

            # Save database state
            self.database.save_state()

            print(f"💾 Checkpoint '{name}' created")

        elif subcommand == "restore":
            if len(args) < 2:
                print("Usage: checkpoint restore <name>")
                return

            name = args[1]
            if name not in self.checkpoints:
                print(f"❌ Checkpoint '{name}' not found")
                return

            checkpoint = self.checkpoints[name]
            self.time_engine.set_time(checkpoint["time"])

            print(f"🔄 Restored to checkpoint '{name}'")
            print(f"⏰ Time: {checkpoint['time']}")
            print(f"📅 Period: {checkpoint['period']}")

        elif subcommand == "list":
            if not self.checkpoints:
                print("📋 No checkpoints created yet")
                return

            print("📋 Available Checkpoints:")
            for i, (name, info) in enumerate(self.checkpoints.items()):
                print(f"  {i + 1}. {name} - {info['time']} ({info['period']})")

        else:
            print("❌ Unknown checkpoint subcommand")
            print("Available: create, restore, list")

    def _handle_limits_commands(self, args: list[str]) -> None:
        """Handle limits calculation commands."""
        if not args:
            print("Limits commands: calculate, show, apply")
            return

        subcommand = args[0].lower()
        account = args[1] if len(args) > 1 else "default_account"

        if subcommand == "calculate":
            try:
                settings = self.limits_calculator.calculate_periodic_settings(account)

                print(f"📊 Periodic Limits for {account}:")
                print(f"   Period: {settings['period']}")
                print(f"   Base allocation: {settings['base_allocation']}Nh")
                print(f"   Total allocation: {settings['total_allocation']:.1f}Nh")
                print(f"   Fairshare: {settings['fairshare']}")
                print(f"   QoS threshold: {settings['qos_threshold']:.1f}Nh")
                print(f"   Grace limit: {settings['grace_limit']:.1f}Nh")
                print(f"   Billing minutes: {settings['billing_minutes']}")

                # Show carryover details if applicable
                carryover = settings["carryover_details"]
                if carryover["unused_allocation"] > 0:
                    print("\n🎁 Carryover Details:")
                    print(f"   Previous usage: {carryover['previous_usage']}Nh")
                    print(f"   Decay factor: {carryover['decay_factor']:.4f}")
                    print(f"   Carryover amount: {carryover['unused_allocation']:.1f}Nh")

            except ValueError as e:
                print(f"❌ {e}")

        elif subcommand == "show":
            # Show current limits from account
            account_obj = self.database.get_account(account)
            if not account_obj:
                print(f"❌ Account {account} not found")
                return

            print(f"📊 Current Limits for {account}:")
            print(f"   Fairshare: {account_obj.fairshare}")
            print(f"   QoS: {account_obj.qos}")
            print(f"   Limits: {account_obj.limits}")

        elif subcommand == "apply":
            try:
                result = self.limits_calculator.apply_period_transition(account)
                print(f"✅ Applied periodic settings to {account}")
                print(f"📊 Settings: {result['settings_applied']}")
            except ValueError as e:
                print(f"❌ {e}")

        else:
            print("❌ Unknown limits subcommand")
            print("Available: calculate, show, apply")

    def _handle_qos_commands(self, args: list[str]) -> None:
        """Handle QoS management commands."""
        if not args:
            print("QoS commands: show, set, check")
            return

        subcommand = args[0].lower()

        if subcommand == "show":
            account = args[1] if len(args) > 1 else "default_account"
            qos = self.qos_manager.get_account_qos(account)
            qos_info = self.qos_manager.get_qos_info(qos)

            print(f"🎛️  QoS for {account}: {qos}")
            if qos_info:
                print(f"   Description: {qos_info.get('description', 'N/A')}")
                print(f"   Priority Weight: {qos_info.get('priority_weight', 'N/A')}")

        elif subcommand == "set":
            if len(args) < 3:
                print("Usage: qos set <account> <qos>")
                return

            account = args[1]
            qos = args[2]

            success = self.qos_manager.set_account_qos(account, qos)
            if success:
                print(f"✅ QoS set to {qos} for {account}")
            else:
                print("❌ Failed to set QoS")

        elif subcommand == "check":
            account = args[1] if len(args) > 1 else "default_account"

            try:
                # Get current settings for threshold calculations
                settings = self.limits_calculator.calculate_periodic_settings(account)
                current_usage = self.database.get_total_usage(
                    account, self.time_engine.get_current_quarter()
                )

                # Check and automatically update QoS
                qos_result = self.qos_manager.check_and_update_qos(
                    account, current_usage, settings["qos_threshold"], settings["grace_limit"]
                )

                print(f"🔍 Threshold Check for {account}:")
                print(f"   Current usage: {current_usage}Nh")
                print(f"   QoS threshold: {settings['qos_threshold']:.1f}Nh")
                print(f"   Grace limit: {settings['grace_limit']:.1f}Nh")
                print(f"   Current QoS: {qos_result['current_qos']}")
                print(f"   New QoS: {qos_result['new_qos']}")
                print(f"   Status: {qos_result['threshold_status']}")
                print(
                    f"   Percentage used: {(current_usage / settings['total_allocation']) * 100:.1f}%"
                )

                if qos_result["action_taken"]:
                    print(f"   ✅ Action taken: {qos_result['action_taken']}")
                else:
                    print("   ℹ️  No QoS change needed")

            except ValueError as e:
                print(f"❌ {e}")

        else:
            print("❌ Unknown QoS subcommand")
            print("Available: show, set, check")

    def _handle_account_commands(self, args: list[str]) -> None:
        """Handle account management commands."""
        if not args:
            print("Account commands: create, list, show, delete")
            return

        subcommand = args[0].lower()

        if subcommand == "create":
            if len(args) < 2:
                print("Usage: account create <name> [description] [allocation]")
                return

            name = args[1]
            description = f"Account {name}"
            allocation = 1000

            # Parse optional parameters
            for _i, arg in enumerate(args[2:], 2):
                if arg.isdigit():
                    # This is the allocation number
                    allocation = int(arg)
                else:
                    # This is the description
                    description = arg.strip('"')

            self.database.add_account(name, description, "emulator")
            self.database.set_account_allocation(name, allocation)

            print(f"✅ Created account {name} with {allocation}Nh allocation")

        elif subcommand == "list":
            accounts = self.database.list_accounts()
            print("📋 Accounts:")
            for account in accounts:
                print(f"  - {account.name}: {account.description} ({account.allocation}Nh)")

        elif subcommand == "show":
            if len(args) < 2:
                print("Usage: account show <name>")
                return

            name = args[1]
            account_opt = self.database.get_account(name)

            if account_opt is None:
                print(f"❌ Account {name} not found")
                return

            account = account_opt  # Now we know it's not None
            print(f"📊 Account: {account.name}")
            print(f"   Description: {account.description}")
            print(f"   Organization: {account.organization}")
            print(f"   Allocation: {account.allocation}Nh")
            print(f"   Fairshare: {account.fairshare}")
            print(f"   QoS: {account.qos}")
            print(f"   Last period: {account.last_period}")

            # Show usage summary
            summary = self.usage_simulator.get_current_usage_summary(name)
            print("\n📊 Usage Summary:")
            print(f"   Current period: {summary['current_period']}")
            print(f"   Period usage: {summary['period_usage']}Nh")
            print(f"   Remaining: {summary['remaining']}Nh")
            print(f"   Percentage used: {summary['percentage_used']:.1f}%")

        elif subcommand == "delete":
            if len(args) < 2:
                print("Usage: account delete <name>")
                return

            name = args[1]
            self.database.delete_account(name)
            print(f"✅ Deleted account {name}")

        else:
            print("❌ Unknown account subcommand")
            print("Available: create, list, show, delete")

    def _handle_config_commands(self, args: list[str]) -> None:
        """Handle configuration commands."""
        if not args:
            args = ["show"]

        subcommand = args[0].lower()

        if subcommand == "show":
            if self.slurm_config:
                self.slurm_config.print_config_summary()

                # Show emulator-specific config
                emulator_config = self.slurm_config.get_emulator_config()
                print("\n🤖 Emulator Configuration:")
                print(f"   Manual Usage Reset: {emulator_config['manual_usage_reset']}")
                print(f"   TRES Billing Support: {emulator_config['supports_tres_billing']}")
                print(f"   Priority Flags: {', '.join(emulator_config['priority_flags'])}")
            else:
                print("📊 Using Default Configuration:")
                print("   No slurm.conf file loaded")
                print("   Decay Half-Life: 15.0 days")
                print("   Usage Reset: Manual")
                print("   QoS Weight: 500000")
                print("   Fairshare Weight: 259200")
                print("   TRES Billing Weights:")
                print("      CPU: 0.015625")
                print("      Mem: 0.001953125")
                print("      GRES/gpu: 0.25")

        elif subcommand == "validate":
            if self.slurm_config:
                warnings = self.slurm_config.validate_configuration()
                if warnings:
                    print("⚠️  Configuration Warnings:")
                    for warning in warnings:
                        print(f"   - {warning}")
                else:
                    print("✅ Configuration is valid!")
            else:
                print("❌ No configuration loaded to validate")

        elif subcommand == "reload":
            if len(args) < 2:
                print("Usage: config reload <path-to-slurm.conf>")
                return

            config_path = args[1]
            try:
                new_config = SlurmConfigParser(config_path)
                self.slurm_config = new_config

                # Update components with new config
                self.usage_simulator.billing_weights = new_config.get_tres_billing_weights()
                self.limits_calculator = PeriodicLimitsCalculator(
                    self.database, self.time_engine, new_config
                )

                print(f"✅ Reloaded configuration from {config_path}")
                new_config.print_config_summary()

            except Exception as e:
                print(f"❌ Failed to reload configuration: {e}")

        else:
            print("❌ Unknown config subcommand")
            print("Available: show, validate, reload")

    def _show_status(self, args: list[str]) -> None:
        """Show overall emulator status."""
        print("📊 SLURM Emulator Status")
        print("=" * 40)
        print(f"⏰ Current time: {self.time_engine.get_current_time()}")
        print(f"📅 Current period: {self.time_engine.get_current_quarter()}")

        # Show accounts summary
        accounts = self.database.list_accounts()
        print(f"\n📋 Accounts: {len(accounts)}")
        for account in accounts:
            if account.name == "root":
                continue
            usage = self.database.get_total_usage(
                account.name, self.time_engine.get_current_quarter()
            )
            print(f"   - {account.name}: {usage}/{account.allocation}Nh ({account.qos})")

        # Show users summary
        users = list(self.database.users.values())
        print(f"\n👥 Users: {len(users)}")

        # Show checkpoints
        print(f"\n💾 Checkpoints: {len(self.checkpoints)}")
        for name in list(self.checkpoints.keys())[-3:]:  # Show last 3
            print(f"   - {name}: {self.checkpoints[name]['time']}")

    def _list_scenarios(self, args: list[str]) -> None:
        """List available scenarios."""
        scenario_type = args[0] if args else None

        if scenario_type:
            try:
                type_filter = ScenarioType(scenario_type)
                scenarios = self.scenario_registry.list_by_type(type_filter)
                print(f"📋 {scenario_type.title().replace('_', ' ')} Scenarios:")
            except ValueError:
                print(f"❌ Unknown scenario type: {scenario_type}")
                print(
                    "Available types: periodic_limits, decay_testing, qos_management, usage_patterns, configuration"
                )
                return
        else:
            scenarios = self.scenario_registry.list_scenarios()
            print("📋 Available Scenarios:")

        if not scenarios:
            print("   No scenarios found")
            return

        for scenario in scenarios:
            complexity_emoji = {"basic": "🟢", "intermediate": "🟡", "advanced": "🔴"}.get(
                scenario.complexity, "⚪"
            )
            print(f"\n  {complexity_emoji} {scenario.name}: {scenario.title}")
            print(f"     {scenario.description}")
            print(
                f"     Duration: {scenario.duration_estimate} | Complexity: {scenario.complexity}"
            )
            print(f"     Steps: {len(scenario.steps)} | Type: {scenario.scenario_type.value}")

    def _describe_scenario(self, scenario_name: str) -> None:
        """Show detailed description of a scenario."""
        scenario = self.scenario_registry.get_scenario(scenario_name)
        if not scenario:
            print(f"❌ Scenario '{scenario_name}' not found")
            return

        complexity_emoji = {"basic": "🟢", "intermediate": "🟡", "advanced": "🔴"}.get(
            scenario.complexity, "⚪"
        )

        print(f"\n📖 Scenario: {scenario.title}")
        print("=" * (len(scenario.title) + 12))
        print(f"🏷️  Name: {scenario.name}")
        print(f"{complexity_emoji} Complexity: {scenario.complexity}")
        print(f"⏱️  Duration: {scenario.duration_estimate}")
        print(f"🔧 Type: {scenario.scenario_type.value}")

        if scenario.recommended_config:
            print(f"⚙️  Recommended Config: {scenario.recommended_config}")

        print("\n📝 Description:")
        print(f"   {scenario.description}")

        if scenario.learning_objectives:
            print("\n🎯 Learning Objectives:")
            for obj in scenario.learning_objectives:
                print(f"   • {obj}")

        if scenario.key_concepts:
            print("\n🔑 Key Concepts:")
            for concept in scenario.key_concepts:
                print(f"   • {concept}")

        if scenario.prerequisites:
            print("\n⚠️  Prerequisites:")
            for prereq in scenario.prerequisites:
                print(f"   • {prereq}")

        print("\n📊 Structure:")
        print(f"   Steps: {len(scenario.steps)}")
        print(f"   Total Actions: {scenario.get_total_actions()}")

        print("\n💡 Usage:")
        print(f"   scenario run {scenario.name}                    # Run automatically")
        print(f"   scenario run {scenario.name} --interactive      # Run with prompts")
        print(f"   scenario run {scenario.name} --step-by-step     # Run with detailed steps")
        print(f"   scenario steps {scenario.name}                 # Show step breakdown")

    def _show_scenario_steps(self, scenario_name: str) -> None:
        """Show detailed step breakdown of a scenario."""
        scenario = self.scenario_registry.get_scenario(scenario_name)
        if not scenario:
            print(f"❌ Scenario '{scenario_name}' not found")
            return

        print(f"\n📋 Steps for '{scenario.title}':")
        print("=" * 50)

        for i, step in enumerate(scenario.steps, 1):
            print(f"\n📍 Step {i}: {step.name}")
            print(f"   📝 {step.description}")
            if step.time_point:
                print(f"   ⏰ Time: {step.time_point}")

            if step.actions:
                print(f"   🔧 Actions ({len(step.actions)}):")
                for j, action in enumerate(step.actions, 1):
                    cli_cmd = action.get_cli_command()
                    print(f"      {j}. {action.description}")
                    print(f"         Command: {cli_cmd}")
                    if action.expected_outcome:
                        print(f"         Expected: {action.expected_outcome}")

            if step.expected_state:
                print("   ✅ Expected State:")
                for key, value in step.expected_state.items():
                    print(f"      {key}: {value}")

    def _validate_scenario(self, scenario_name: str) -> None:
        """Validate scenario definition."""
        scenario = self.scenario_registry.get_scenario(scenario_name)
        if not scenario:
            print(f"❌ Scenario '{scenario_name}' not found")
            return

        print(f"🔍 Validating scenario: {scenario.title}")

        issues = []

        # Check basic structure
        if not scenario.steps:
            issues.append("No steps defined")
        if not scenario.description:
            issues.append("No description provided")
        if not scenario.key_concepts:
            issues.append("No key concepts listed")

        # Check steps
        for i, step in enumerate(scenario.steps):
            if not step.actions:
                issues.append(f"Step {i + 1} has no actions")
            if not step.description:
                issues.append(f"Step {i + 1} has no description")

        # Check configuration requirements
        if scenario.recommended_config and not Path(scenario.recommended_config).exists():
            issues.append(f"Recommended config file not found: {scenario.recommended_config}")

        if issues:
            print("⚠️  Issues found:")
            for issue in issues:
                print(f"   - {issue}")
        else:
            print("✅ Scenario validation passed!")

    def _search_scenarios(self, query: str) -> None:
        """Search scenarios by query."""
        results = self.scenario_registry.search_scenarios(query)

        if not results:
            print(f"🔍 No scenarios found matching '{query}'")
            return

        print(f"🔍 Search results for '{query}' ({len(results)} found):")

        for scenario in results:
            complexity_emoji = {"basic": "🟢", "intermediate": "🟡", "advanced": "🔴"}.get(
                scenario.complexity, "⚪"
            )
            print(f"\n  {complexity_emoji} {scenario.name}: {scenario.title}")
            print(f"     {scenario.description}")

    def _run_registry_scenario(
        self, scenario_name: str, interactive: bool, step_by_step: bool
    ) -> None:
        """Run a scenario from the registry."""
        scenario = self.scenario_registry.get_scenario(scenario_name)
        if not scenario:
            print(f"❌ Scenario '{scenario_name}' not found")
            return

        print(f"🎬 Starting scenario: {scenario.title}")
        print(f"📝 {scenario.description}")

        # Clean state for scenarios to ensure consistent results
        self._clean_scenario_state(scenario_name)

        if interactive:
            response = input(f"\nProceed with {len(scenario.steps)} steps? [Y/n]: ")
            if response.lower() in ["n", "no"]:
                print("❌ Scenario cancelled")
                return

        try:
            for i, step in enumerate(scenario.steps, 1):
                if step_by_step or interactive:
                    print(f"\n📍 Step {i}: {step.name}")
                    print(f"   📝 {step.description}")
                    if step.time_point:
                        print(f"   ⏰ Target time: {step.time_point}")

                    if interactive:
                        input("⏸️  Press Enter to execute this step...")

                # Set time if specified
                if step.time_point:
                    self.time_engine.set_time(step.time_point)
                    if step_by_step:
                        print(f"   ⏰ Time set to: {step.time_point}")

                # Execute actions
                for j, action in enumerate(step.actions, 1):
                    if step_by_step:
                        print(f"   🔧 Action {j}: {action.description}")
                        cli_cmd = action.get_cli_command()
                        print(f"      Command: {cli_cmd}")

                    self._execute_scenario_action(action)

                    if step_by_step and action.expected_outcome:
                        print(f"      Expected: {action.expected_outcome}")

                if step_by_step:
                    print(f"   ✅ Step {i} completed")

            print(f"\n✅ Scenario '{scenario.title}' completed successfully!")

        except Exception as e:
            print(f"\n❌ Scenario failed at step: {e}")

    def _execute_scenario_action(self, action) -> None:
        """Execute a scenario action."""
        if action.type == ActionType.TIME_SET:
            time_str = action.parameters["time"]
            target_time = datetime.fromisoformat(time_str)
            self.time_engine.set_time(target_time)

        elif action.type == ActionType.TIME_ADVANCE:
            amount = action.parameters["amount"]
            unit = action.parameters["unit"]
            if unit == "days":
                self.time_engine.advance_time(days=amount)
            elif unit == "months":
                self.time_engine.advance_time(months=amount)
            elif unit == "quarters":
                self.time_engine.advance_time(quarters=amount)

        elif action.type == ActionType.USAGE_INJECT:
            user = action.parameters["user"]
            amount = action.parameters["amount"]
            account = action.parameters.get("account", "default_account")
            self.usage_simulator.inject_usage(account, user, amount)

        elif action.type == ActionType.ACCOUNT_CREATE:
            name = action.parameters["name"]
            desc = action.parameters.get("description", "Test Account")
            allocation = action.parameters.get("allocation", 1000)

            # Clean up existing account first to ensure clean state
            if self.database.get_account(name):
                self.database.delete_account(name)

            self.database.add_account(name, desc, "emulator")
            self.database.set_account_allocation(name, allocation)

        elif action.type == ActionType.ACCOUNT_DELETE:
            name = action.parameters["account"]
            if self.database.get_account(name):
                self.database.delete_account(name)

        elif action.type == ActionType.CLEANUP:
            # Handle cleanup actions
            account = action.parameters.get("account")
            if account and self.database.get_account(account):
                self.database.delete_account(account)

        elif action.type == ActionType.LIMITS_CALCULATE:
            account = action.parameters.get("account", "default_account")
            try:
                # For decay scenarios, force carryover calculation
                config_override = {}
                if "decay" in action.description.lower():
                    # Set the account to have a previous period to trigger carryover
                    account_obj = self.database.get_account(account)
                    if account_obj:
                        # Set last period to previous quarter to trigger carryover calculation
                        current_period = self.time_engine.get_current_quarter()
                        from_period = self.limits_calculator._get_previous_quarter(current_period)
                        account_obj.last_period = from_period

                    config_override = {
                        "force_carryover_calculation": True,
                        "carryover_enabled": True,
                        "grace_ratio": 0.2,
                        "half_life_days": self.limits_calculator.half_life_days,
                    }

                settings = self.limits_calculator.calculate_periodic_settings(
                    account, config_override
                )

                # Show detailed results for decay scenarios
                if "decay" in action.description.lower():
                    carryover = settings["carryover_details"]
                    half_life = self.limits_calculator.half_life_days
                    print(f"      📊 Decay Analysis (Half-life: {half_life} days):")
                    print(f"         Previous usage: {carryover['previous_usage']}Nh")
                    print(f"         Days elapsed: {carryover['days_elapsed']}")
                    print(f"         Decay factor: {carryover['decay_factor']:.6f}")
                    print(
                        f"         Effective previous: {carryover['effective_previous_usage']:.1f}Nh"
                    )
                    print(f"         Unused (after decay): {carryover['unused_allocation']:.1f}Nh")
                    print(
                        f"         New total allocation: {carryover['new_total_allocation']:.1f}Nh"
                    )

                    # Calculate what the expected values should be for comparison
                    expected_decay = 2 ** (-90 / half_life)
                    expected_effective = carryover["previous_usage"] * expected_decay
                    expected_carryover = max(0, carryover["base_allocation"] - expected_effective)

                    print("      🎯 Expected vs Actual:")
                    print(f"         Expected decay factor: {expected_decay:.6f}")
                    print(f"         Expected effective usage: {expected_effective:.1f}Nh")
                    print(f"         Expected carryover: {expected_carryover:.1f}Nh")
                else:
                    print(
                        f"      Result: Fairshare: {settings['fairshare']}, Allocation: {settings['total_allocation']:.1f}Nh"
                    )

                    # Show carryover details if available
                    if settings["carryover_details"]["unused_allocation"] > 0:
                        carryover = settings["carryover_details"]
                        print(
                            f"      Carryover: {carryover['unused_allocation']:.1f}Nh (from {carryover['previous_usage']}Nh previous)"
                        )

            except ValueError:
                pass

        elif action.type == ActionType.QOS_CHECK:
            account = action.parameters.get("account", "default_account")
            try:
                # Get current settings for threshold values
                settings = self.limits_calculator.calculate_periodic_settings(account)
                current_usage = self.database.get_total_usage(
                    account, self.time_engine.get_current_quarter()
                )

                # Check and update QoS based on thresholds
                qos_result = self.qos_manager.check_and_update_qos(
                    account, current_usage, settings["qos_threshold"], settings["grace_limit"]
                )

                final_qos = self.qos_manager.get_account_qos(account)
                print(f"      Result: QoS: {final_qos}, Status: {qos_result['threshold_status']}")

                if qos_result["action_taken"]:
                    print(f"      Action: {qos_result['action_taken']}")

            except ValueError:
                pass

        elif action.type == ActionType.CONFIG_RELOAD:
            config_path = action.parameters["config_path"]
            try:
                new_config = SlurmConfigParser(config_path)
                self.slurm_config = new_config
                self.usage_simulator.billing_weights = new_config.get_tres_billing_weights()
                self.limits_calculator = PeriodicLimitsCalculator(
                    self.database, self.time_engine, new_config
                )
            except Exception as e:
                print(f"      Error: Failed to reload config: {e}")

    def _clean_scenario_state(self, scenario_name: str) -> None:
        """Clean state for scenario to ensure consistent results."""
        print("🧹 Cleaning scenario state for consistent results...")

        # Get scenario-specific accounts to clean
        scenario_accounts = self._get_scenario_accounts(scenario_name)

        # Clean up all scenario-related accounts and data
        for account in scenario_accounts:
            self._clean_account_completely(account)

        # For comprehensive cleanup, also clean any orphaned data
        self._clean_orphaned_data()

        # Save cleaned state
        self.database.save_state()
        print(f"✅ Cleaned {len(scenario_accounts)} scenario accounts")

    def _get_scenario_accounts(self, scenario_name: str) -> list[str]:
        """Get list of accounts used by a specific scenario."""
        scenario_accounts = {
            "qos_thresholds": ["qos_test"],
            "carryover_test": ["carryover_light", "carryover_heavy"],
            "decay_comparison": ["decay_test_15", "decay_test_7"],
            "config_comparison": ["config_standard", "config_custom"],
            "sequence": ["slurm_account_123"],
        }

        # Return accounts for this scenario, or empty list for unknown scenarios
        return scenario_accounts.get(scenario_name, [])

    def _clean_account_completely(self, account_name: str) -> None:
        """Completely clean an account and all its data."""
        # Remove account
        if self.database.get_account(account_name):
            self.database.delete_account(account_name)

        # Remove all usage records for this account
        self.database.usage_records = [
            record for record in self.database.usage_records if record.account != account_name
        ]

        # Remove all associations for this account
        keys_to_remove = [key for key in self.database.associations if f":{account_name}" in key]
        for key in keys_to_remove:
            del self.database.associations[key]

        # Remove any jobs for this account
        job_ids_to_remove = [
            job_id for job_id, job in self.database.jobs.items() if job.account == account_name
        ]
        for job_id in job_ids_to_remove:
            del self.database.jobs[job_id]

    def _clean_orphaned_data(self) -> None:
        """Clean up any orphaned data from deleted accounts."""
        # Get existing account names
        existing_accounts = set(self.database.accounts.keys())

        # Clean usage records for non-existent accounts
        self.database.usage_records = [
            record for record in self.database.usage_records if record.account in existing_accounts
        ]

        # Clean associations for non-existent accounts
        keys_to_remove = [
            key for key in self.database.associations if key.split(":")[1] not in existing_accounts
        ]
        for key in keys_to_remove:
            del self.database.associations[key]

        # Clean jobs for non-existent accounts
        job_ids_to_remove = [
            job_id
            for job_id, job in self.database.jobs.items()
            if job.account not in existing_accounts
        ]
        for job_id in job_ids_to_remove:
            del self.database.jobs[job_id]

    def _handle_cleanup_commands(self, args: list[str]) -> None:
        """Handle cleanup commands."""
        if not args:
            print("Cleanup commands: all, scenario <name>, account <name>")
            return

        subcommand = args[0].lower()

        if subcommand == "all":
            # Clean everything except root account
            print("🧹 Cleaning all accounts and data except 'root'...")

            accounts_to_keep = ["root"]
            accounts_to_remove = [
                acc.name
                for acc in self.database.list_accounts()
                if acc.name not in accounts_to_keep
            ]

            for account in accounts_to_remove:
                self._clean_account_completely(account)

            # Reset time to default
            self.time_engine.set_time(datetime(2024, 1, 1))

            self.database.save_state()
            print(f"✅ Cleaned {len(accounts_to_remove)} accounts and reset time to 2024-01-01")

        elif subcommand == "scenario":
            if len(args) < 2:
                print("Usage: cleanup scenario <scenario_name>")
                return

            scenario_name = args[1]
            scenario_accounts = self._get_scenario_accounts(scenario_name)

            if scenario_accounts:
                for account in scenario_accounts:
                    self._clean_account_completely(account)
                self.database.save_state()
                print(
                    f"✅ Cleaned scenario '{scenario_name}' accounts: {', '.join(scenario_accounts)}"
                )
            else:
                print(f"❌ Unknown scenario: {scenario_name}")

        elif subcommand == "account":
            if len(args) < 2:
                print("Usage: cleanup account <account_name>")
                return

            account_name = args[1]
            if self.database.get_account(account_name):
                self._clean_account_completely(account_name)
                self.database.save_state()
                print(f"✅ Cleaned account '{account_name}' completely")
            else:
                print(f"❌ Account '{account_name}' not found")

        else:
            print("❌ Unknown cleanup subcommand")
            print("Available: all, scenario <name>, account <name>")

    def _handle_manual_completion(self, args: list[str]) -> None:
        """Handle manual completion command for terminals without TAB support."""
        if not args:
            print("Usage: complete <partial_command>")
            print("Example: complete scenario r")
            print("Example: complete sacctmgr modify account test set")
            return

        # Reconstruct the command line
        command_line = " ".join(args)
        parts = command_line.split()

        if not parts:
            matches = self._get_main_commands()
        elif len(parts) == 1:
            # Complete main command or show subcommands
            main_commands = self._get_main_commands()
            main_matches = [cmd for cmd in main_commands if cmd.startswith(parts[0])]

            if len(main_matches) == 1:
                # Show subcommands for this main command
                matches = self._get_subcommands(main_matches[0])
                print(f"📋 Subcommands for '{main_matches[0]}':")
            else:
                # Show matching main commands
                matches = main_matches
                print(f"📋 Main commands starting with '{parts[0]}':")
        else:
            # Complete parameters
            main_cmd = parts[0]
            if main_cmd in self._get_main_commands():
                # For manual completion, complete with empty text to show all options
                matches = self._get_parameters(main_cmd, parts[1:], "")
                print(f"📋 Completions for '{command_line}':")
            else:
                matches = []

        if matches:
            for match in matches:
                print(f"  {match}")
        else:
            print("  No completions available")

        # Show example usage
        if len(parts) >= 1:
            main_cmd = parts[0]
            if main_cmd == "scenario":
                print("\n💡 Try: scenario list, scenario describe <name>, scenario run <name>")
            elif main_cmd == "sacctmgr":
                print(
                    "\n💡 Try: sacctmgr add account <name>, sacctmgr modify account <name> set <param>=<value>"
                )
            elif main_cmd == "account":
                print("\n💡 Try: account create <name>, account show <name>")
            elif main_cmd == "usage":
                print("\n💡 Try: usage inject <user> <amount> <account>")

    def _show_help(self) -> None:
        """Show help message."""
        print("""
📖 SLURM Emulator Commands:

⏰ Time Management:
  time                          - Show current time and period
  time advance <N> days/months/quarters - Advance time by amount
  time set YYYY-MM-DD [HH:MM:SS]       - Set specific date/time

💾 Usage Simulation:
  usage inject <user> <amount> [account] - Inject node-hour usage
  usage show [account] [period]          - Show usage summary

🎬 Scenarios:
  scenario list [type]                  - List scenarios (optionally by type)
  scenario describe <name>              - Show detailed scenario description
  scenario steps <name>                 - Show step-by-step breakdown
  scenario run <name> [--interactive]   - Run scenario
  scenario run <name> --step-by-step    - Run with detailed step output
  scenario validate <name>              - Validate scenario definition
  scenario search <query>               - Search scenarios by keyword

💾 Checkpoints:
  checkpoint create <name>              - Save current state
  checkpoint restore <name>             - Restore saved state
  checkpoint list                       - List all checkpoints

📊 Limits & QoS:
  limits calculate [account]            - Calculate periodic limits
  limits show [account]                 - Show current limits
  limits apply [account]                - Apply period transition
  qos show [account]                    - Show QoS status
  qos set <account> <qos>               - Set QoS level
  qos check [account]                   - Check usage thresholds

🏢 Account Management:
  account create <name> [desc] [alloc]  - Create account
  account list                          - List all accounts
  account show <name>                   - Show account details
  account delete <name>                 - Delete account

⚙️  Configuration:
  config show                           - Show current configuration
  config validate                       - Validate loaded configuration
  config reload <path>                  - Reload configuration from file

🔧 SLURM Commands:
  sacctmgr <args>                       - Run sacctmgr command
  sacct <args>                          - Run sacct command
  sinfo <args>                          - Run sinfo command

📊 General:
  status                                - Show emulator status
  cleanup all                           - Clean all accounts and reset state
  cleanup scenario <name>               - Clean specific scenario accounts
  cleanup account <name>                - Clean specific account completely
  help                                  - Show this help
  exit                                  - Quit emulator
  complete <partial_command>            - Manual completion (if TAB not working)

⌨️  Auto-Completion:
  [TAB]                                 - Complete commands and parameters
  [TAB][TAB]                            - Show all available options
  [↑][↓]                                - Navigate command history
  [Ctrl+R]                              - Search command history
  complete <cmd>                        - Manual completion fallback

💡 Example Session:
  time set 2024-01-01
  account create test-account "Test" 1000
  usage inject user1 200 test-account
  time advance 2 months
  usage inject user1 400 test-account
  limits calculate test-account
  scenario run sequence --interactive

💡 Auto-Completion Examples:
  scenario [TAB]                        # Shows: run, list, describe, steps
  scenario describe [TAB]               # Shows scenario names
  sacctmgr modify account [TAB]         # Shows account names
  sacctmgr modify account test set [TAB] # Shows: fairshare=, qos=, etc.
  qos set test [TAB]                    # Shows QoS levels
  time advance 2 [TAB]                  # Shows: days, months, quarters
        """)


def main():
    """Main entry point for CLI."""
    # Import here to avoid circular imports
    from emulator.cli.cmd_cli import main as cmd_main  # noqa: PLC0415

    # Use CMD-based CLI
    cmd_main()


if __name__ == "__main__":
    main()
