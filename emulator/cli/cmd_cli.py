"""CMD-based CLI interface for SLURM emulator."""

import argparse
import builtins
import cmd
import contextlib
import os
import readline
import shlex
import sys
from datetime import datetime
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


class SlurmEmulatorCmd(cmd.Cmd):
    """CMD-based interactive CLI for SLURM emulator."""

    intro = """
🎮 SLURM Emulator - Time Travel Edition (CMD Interface)
Type 'help' or '?' for commands. TAB for auto-completion.
Type 'help <command>' for detailed help on specific commands.
"""

    prompt = "slurm-emulator> "

    def __init__(self, slurm_config_path: Optional[str] = None):
        super().__init__()

        # Store config path for later
        self._config_path = slurm_config_path

        self.time_engine = TimeEngine()
        self.database = SlurmDatabase()

        # Load SLURM configuration if provided
        self.slurm_config = None
        if self._config_path:
            try:
                self.slurm_config = SlurmConfigParser(self._config_path)
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
        self.slurm_emulator.database = self.database
        self.slurm_emulator.time_engine = self.time_engine
        self.slurm_emulator.sacctmgr.database = self.database
        self.slurm_emulator.sacct.database = self.database
        self.scenario_registry = ScenarioRegistry()

        # Load existing state
        self.database.load_state()

        # State management
        self.checkpoints: dict[str, Any] = {}

        # Show initial status
        print(f"Current time: {self.time_engine.get_current_time()}")
        print(f"Current period: {self.time_engine.get_current_quarter()}")

        # Quick start tips
        if not self.database.list_accounts() or len(self.database.list_accounts()) <= 1:
            print("\n🚀 Quick start:")
            print('   account_create test "Test Account" 1000')
            print("   usage_inject user1 200 test")
            print("   scenario_list")
            print("   scenario_describe qos_thresholds")

    def cmdloop(self, intro=None):
        """Override cmdloop to ensure completion is properly set up."""
        # Setup completion after cmd module initialization
        self._setup_completion()

        # Call parent cmdloop
        super().cmdloop(intro)

    def _setup_completion(self):
        """Setup completion for cmd module."""
        try:
            # Configure readline for better completion
            readline.parse_and_bind("tab: complete")
            readline.parse_and_bind("set completion-ignore-case on")
            readline.parse_and_bind("set show-all-if-ambiguous on")

            # Critical fix: cmd module doesn't always set completer automatically
            # We need to manually set it up
            current_completer = readline.get_completer()
            if not current_completer:
                readline.set_completer(self.complete)
                readline.set_completer_delims(" \t\n`~!@#$%^&*()=+[{]}\\|;:'\",<>?")

            # Verify completion is working
            final_completer = readline.get_completer()
            if final_completer and sys.stdin.isatty():
                print("⌨️  TAB auto-completion enabled")
            elif final_completer:
                print("⌨️  Completion configured (use in interactive terminal)")
            else:
                print("⚠️  Could not configure TAB completion")

        except ImportError:
            print("⚠️  TAB completion not available (readline missing)")
        except Exception as e:
            print(f"⚠️  Completion setup issue: {e}")

    def onecmd(self, line):
        """Override to add debug for completion issues."""
        if os.getenv("SLURM_EMULATOR_DEBUG"):
            print(f"DEBUG: Command line: '{line}'")
        return super().onecmd(line)

    def completedefault(self, text, line, begidx, endidx):
        """Default completion fallback."""
        # This is called when no specific complete_* method exists
        return []

    # ============================================================================
    # Time Management Commands
    # ============================================================================

    def do_time_show(self, arg):
        """Show current emulator time and period."""
        current = self.time_engine.get_current_time()
        period = self.time_engine.get_current_quarter()
        print(f"⏰ Current time: {current}")
        print(f"📅 Current period: {period}")

    def do_time_advance(self, arg):
        """Advance time by specified amount.

        Usage: time_advance <amount> <unit>
        Units: days, months, quarters
        Example: time_advance 2 months
        """
        args = shlex.split(arg)
        if len(args) != 2:
            print("Usage: time_advance <amount> <unit>")
            print("Units: days, months, quarters")
            return

        try:
            amount = int(args[0])
            unit = args[1].lower()

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

    def do_time_set(self, arg):
        """Set specific date and time.

        Usage: time_set YYYY-MM-DD [HH:MM:SS]
        Example: time_set 2024-05-20
        Example: time_set 2024-05-20 14:30:00
        """
        if not arg:
            print("Usage: time_set YYYY-MM-DD [HH:MM:SS]")
            return

        try:
            target_time = datetime.fromisoformat(arg)
            old_period = self.time_engine.get_current_quarter()
            self.time_engine.set_time(target_time)
            new_period = self.time_engine.get_current_quarter()

            print(f"🎯 Time set to {target_time}")
            print(f"📅 Period: {old_period} → {new_period}")

            if old_period != new_period:
                print("🔄 Period transition detected!")

        except ValueError:
            print("❌ Invalid date format. Use YYYY-MM-DD [HH:MM:SS]")

    def complete_time_advance(self, text, line, begidx, endidx):
        """Auto-complete time advance command."""
        debug_mode = os.getenv("SLURM_EMULATOR_DEBUG")  # Only show when debug enabled

        if debug_mode:
            print("\n🔧 COMPLETION CALLED: complete_time_advance")
            print(f"   text: '{text}'")
            print(f"   line: '{line}'")
            print(f"   begidx: {begidx}, endidx: {endidx}")
            print(f"   Interactive: {sys.stdin.isatty()}")

        parts = line.split()
        if len(parts) >= 3:  # "time_advance" "2" "units"
            # Completing units
            units = ["days", "months", "quarters"]
            matches = [unit for unit in units if unit.startswith(text)]
            if debug_mode:
                print(f"   Available units: {units}")
                print(f"   Matching units: {matches}")
            return matches
        if len(parts) == 2 and line.endswith(" "):
            # Just finished typing the number, show all units
            units = ["days", "months", "quarters"]
            if debug_mode:
                print(f"   Showing all units: {units}")
            return units

        if debug_mode:
            print("   No completion context matched")
        return []

    # ============================================================================
    # Usage Simulation Commands
    # ============================================================================

    def do_usage_inject(self, arg):
        """Inject node-hour usage for a user.

        Usage: usage_inject <user> <amount> [account]
        Example: usage_inject user1 200 test-account
        """
        args = shlex.split(arg)
        if len(args) < 2:
            print("Usage: usage_inject <user> <amount> [account]")
            return

        user = args[0]
        try:
            amount = float(args[1])
            account = args[2] if len(args) > 2 else "default_account"

            self.usage_simulator.inject_usage(account, user, amount)

            # Show updated usage
            total = self.database.get_total_usage(account, self.time_engine.get_current_quarter())
            print(f"📊 Total usage for {account}: {total}Nh")

        except ValueError:
            print("❌ Invalid amount. Must be a number.")

    def do_usage_show(self, arg):
        """Show usage summary for account.

        Usage: usage_show [account] [period]
        Example: usage_show test-account
        Example: usage_show test-account 2024-Q1.
        """
        args = shlex.split(arg)
        account = args[0] if args else "default_account"
        period = args[1] if len(args) > 1 else self.time_engine.get_current_quarter()

        usage = self.database.get_total_usage(account, period)
        print(f"📊 {account} usage in {period}: {usage}Nh")

        # Show breakdown by user
        records = self.database.get_usage_records(account=account, period=period)
        user_usage = {}
        for record in records:
            user_usage[record.user] = user_usage.get(record.user, 0) + record.node_hours

        if user_usage:
            print("   User breakdown:")
            for user, user_total in user_usage.items():
                print(f"   - {user}: {user_total}Nh")

    def complete_usage_inject(self, text, line, begidx, endidx):
        """Auto-complete usage inject command."""
        parts = line.split()
        if len(parts) >= 4:  # "usage_inject" "user" "amount" "account"
            # Completing account name
            accounts = [acc.name for acc in self.database.list_accounts()]
            return [acc for acc in accounts if acc.startswith(text)]
        if len(parts) == 3 and line.endswith(" "):
            # Just finished amount, show account options
            return [acc.name for acc in self.database.list_accounts()]
        return []

    def complete_usage_show(self, text, line, begidx, endidx):
        """Auto-complete usage show command."""
        args = shlex.split(line)
        if len(args) == 2 and not line.endswith(" "):
            # Completing account name
            accounts = [acc.name for acc in self.database.list_accounts()]
            return [acc for acc in accounts if acc.startswith(text)]
        return []

    # ============================================================================
    # Account Management Commands
    # ============================================================================

    def do_account_create(self, arg):
        """Create a new account.

        Usage: account_create <name> [description] [allocation]
        Example: account_create test-account "Test Account" 1000.
        """
        args = shlex.split(arg)
        if not args:
            print("Usage: account_create <name> [description] [allocation]")
            return

        name = args[0]
        description = f"Account {name}"
        allocation = 1000

        # Parse optional parameters
        for _i, arg_val in enumerate(args[1:], 1):
            if arg_val.isdigit():
                allocation = int(arg_val)
            else:
                description = arg_val

        # Clean up existing account first to ensure clean state
        if self.database.get_account(name):
            self.database.delete_account(name)

        self.database.add_account(name, description, "emulator")
        self.database.set_account_allocation(name, allocation)

        print(f"✅ Created account {name} with {allocation}Nh allocation")

    def do_account_list(self, arg):
        """List all accounts."""
        accounts = self.database.list_accounts()
        print("📋 Accounts:")
        for account in accounts:
            usage = self.database.get_total_usage(
                account.name, self.time_engine.get_current_quarter()
            )
            print(
                f"  - {account.name}: {account.description} ({usage}/{account.allocation}Nh) [{account.qos}]"
            )

    def do_account_show(self, arg):
        """Show detailed account information.

        Usage: account_show <name>
        Example: account_show test-account.
        """
        if not arg:
            print("Usage: account_show <name>")
            return

        name = arg.strip()
        account = self.database.get_account(name)

        if not account:
            print(f"❌ Account {name} not found")
            return

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

    def do_account_delete(self, arg):
        """Delete an account.

        Usage: account_delete <name>
        Example: account_delete test-account.
        """
        if not arg:
            print("Usage: account_delete <name>")
            return

        name = arg.strip()
        if self.database.get_account(name):
            self._clean_account_completely(name)
            print(f"✅ Deleted account {name}")
        else:
            print(f"❌ Account {name} not found")

    def complete_account_show(self, text, line, begidx, endidx):
        """Auto-complete account show command."""
        accounts = [acc.name for acc in self.database.list_accounts()]
        return [acc for acc in accounts if acc.startswith(text)]

    def complete_account_delete(self, text, line, begidx, endidx):
        """Auto-complete account delete command."""
        accounts = [acc.name for acc in self.database.list_accounts()]
        return [acc for acc in accounts if acc.startswith(text)]

    # ============================================================================
    # Scenario Management Commands
    # ============================================================================

    def do_scenario_list(self, arg):
        """List available scenarios.

        Usage: scenario_list [type]
        Types: periodic_limits, decay_testing, qos_management, configuration
        Example: scenario_list qos_management.
        """
        scenario_type = arg.strip() if arg else None

        if scenario_type:
            try:
                type_filter = ScenarioType(scenario_type)
                scenarios = self.scenario_registry.list_by_type(type_filter)
                print(f"📋 {scenario_type.title().replace('_', ' ')} Scenarios:")
            except ValueError:
                print(f"❌ Unknown scenario type: {scenario_type}")
                print(
                    "Available types: periodic_limits, decay_testing, qos_management, configuration"
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

    def do_scenario_describe(self, arg):
        """Show detailed description of a scenario.

        Usage: scenario_describe <name>
        Example: scenario_describe qos_thresholds.
        """
        if not arg:
            print("Usage: scenario_describe <name>")
            return

        scenario_name = arg.strip()
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

        print("\n💡 Usage:")
        print(f"   scenario_run {scenario.name}")
        print(f"   scenario_run {scenario.name} --interactive")
        print(f"   scenario_run {scenario.name} --step-by-step")
        print(f"   scenario_steps {scenario.name}")

    def do_scenario_steps(self, arg):
        """Show step-by-step breakdown of a scenario.

        Usage: scenario_steps <name>
        Example: scenario_steps qos_thresholds.
        """
        if not arg:
            print("Usage: scenario_steps <name>")
            return

        scenario_name = arg.strip()
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

    def do_scenario_run(self, arg):
        """Run a scenario.

        Usage: scenario_run <name> [--interactive] [--step-by-step]
        Example: scenario_run qos_thresholds
        Example: scenario_run sequence --interactive
        Example: scenario_run decay_comparison --step-by-step.
        """
        if not arg:
            print("Usage: scenario_run <name> [--interactive] [--step-by-step]")
            return

        args = shlex.split(arg)
        scenario_name = args[0]
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

    def do_scenario_search(self, arg):
        """Search scenarios by keyword.

        Usage: scenario_search <query>
        Example: scenario_search decay
        Example: scenario_search qos.
        """
        if not arg:
            print("Usage: scenario_search <query>")
            return

        query = arg.strip()
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

    def complete_scenario_describe(self, text, line, begidx, endidx):
        """Auto-complete scenario describe."""
        scenarios = list(self.scenario_registry.scenarios.keys())
        return [s for s in scenarios if s.startswith(text)]

    def complete_scenario_steps(self, text, line, begidx, endidx):
        """Auto-complete scenario steps."""
        scenarios = list(self.scenario_registry.scenarios.keys())
        return [s for s in scenarios if s.startswith(text)]

    def complete_scenario_run(self, text, line, begidx, endidx):
        """Auto-complete scenario run."""
        scenarios = list(self.scenario_registry.scenarios.keys())
        return [s for s in scenarios if s.startswith(text)]

    # ============================================================================
    # QoS Management Commands
    # ============================================================================

    def do_qos_show(self, arg):
        """Show QoS for account.

        Usage: qos_show [account]
        Example: qos_show test-account.
        """
        account = arg.strip() if arg else "default_account"
        qos = self.qos_manager.get_account_qos(account)
        qos_info = self.qos_manager.get_qos_info(qos)

        print(f"🎛️  QoS for {account}: {qos}")
        if qos_info:
            print(f"   Description: {qos_info.get('description', 'N/A')}")
            print(f"   Priority Weight: {qos_info.get('priority_weight', 'N/A')}")

    def do_qos_set(self, arg):
        """Set QoS for account.

        Usage: qos_set <account> <qos>
        Example: qos_set test-account slowdown.
        """
        args = shlex.split(arg)
        if len(args) != 2:
            print("Usage: qos_set <account> <qos>")
            return

        account = args[0]
        qos = args[1]

        success = self.qos_manager.set_account_qos(account, qos)
        if success:
            print(f"✅ QoS set to {qos} for {account}")
        else:
            print("❌ Failed to set QoS")

    def do_qos_check(self, arg):
        """Check usage thresholds and update QoS automatically.

        Usage: qos_check [account>
        Example: qos_check test-account.
        """
        account = arg.strip() if arg else "default_account"

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

    def complete_qos_show(self, text, line, begidx, endidx):
        """Auto-complete QoS show."""
        accounts = [acc.name for acc in self.database.list_accounts()]
        return [acc for acc in accounts if acc.startswith(text)]

    def complete_qos_check(self, text, line, begidx, endidx):
        """Auto-complete QoS check."""
        accounts = [acc.name for acc in self.database.list_accounts()]
        return [acc for acc in accounts if acc.startswith(text)]

    def complete_qos_set(self, text, line, begidx, endidx):
        """Auto-complete QoS set."""
        args = shlex.split(line)
        if len(args) == 2 and not line.endswith(" "):
            # Complete account name
            accounts = [acc.name for acc in self.database.list_accounts()]
            return [acc for acc in accounts if acc.startswith(text)]
        if len(args) == 3 and not line.endswith(" "):
            # Complete QoS level
            qos_levels = self.qos_manager.list_qos_levels()
            return [qos for qos in qos_levels if qos.startswith(text)]
        return []

    # ============================================================================
    # Limits Calculation Commands
    # ============================================================================

    def do_limits_calculate(self, arg):
        """Calculate periodic limits for account.

        Usage: limits_calculate [account]
        Example: limits_calculate test-account.
        """
        account = arg.strip() if arg else "default_account"

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

    def complete_limits_calculate(self, text, line, begidx, endidx):
        """Auto-complete limits calculate."""
        accounts = [acc.name for acc in self.database.list_accounts()]
        return [acc for acc in accounts if acc.startswith(text)]

    # ============================================================================
    # Cleanup Commands
    # ============================================================================

    def do_cleanup_all(self, arg):
        """Clean all accounts and reset state."""
        print("🧹 Cleaning all accounts and data except 'root'...")

        accounts_to_keep = ["root"]
        accounts_to_remove = [
            acc.name for acc in self.database.list_accounts() if acc.name not in accounts_to_keep
        ]

        for account in accounts_to_remove:
            self._clean_account_completely(account)

        # Reset time to default
        self.time_engine.set_time(datetime(2024, 1, 1))

        self.database.save_state()
        print(f"✅ Cleaned {len(accounts_to_remove)} accounts and reset time to 2024-01-01")

    def do_cleanup_scenario(self, arg):
        """Clean specific scenario accounts.

        Usage: cleanup_scenario <scenario_name>
        Example: cleanup_scenario qos_thresholds.
        """
        if not arg:
            print("Usage: cleanup_scenario <scenario_name>")
            return

        scenario_name = arg.strip()
        self._clean_scenario_state(scenario_name)

    def do_cleanup_account(self, arg):
        """Clean specific account completely.

        Usage: cleanup_account <account_name>
        Example: cleanup_account test-account.
        """
        if not arg:
            print("Usage: cleanup_account <account_name>")
            return

        account_name = arg.strip()
        if self.database.get_account(account_name):
            self._clean_account_completely(account_name)
            self.database.save_state()
            print(f"✅ Cleaned account '{account_name}' completely")
        else:
            print(f"❌ Account '{account_name}' not found")

    def complete_cleanup_scenario(self, text, line, begidx, endidx):
        """Auto-complete cleanup scenario."""
        scenarios = list(self.scenario_registry.scenarios.keys())
        return [s for s in scenarios if s.startswith(text)]

    def complete_cleanup_account(self, text, line, begidx, endidx):
        """Auto-complete cleanup account."""
        accounts = [acc.name for acc in self.database.list_accounts()]
        return [acc for acc in accounts if acc.startswith(text)]

    # ============================================================================
    # Configuration Commands
    # ============================================================================

    def do_config_show(self, arg):
        """Show current configuration."""
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

    def do_config_reload(self, arg):
        """Reload configuration from file.

        Usage: config_reload <path>
        Example: config_reload examples/slurm.conf.
        """
        if not arg:
            print("Usage: config_reload <path>")
            return

        config_path = arg.strip()
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

    # ============================================================================
    # SLURM Commands (Pass-through)
    # ============================================================================

    def do_sacctmgr(self, arg):
        """Run sacctmgr command.

        Usage: sacctmgr <args>
        Example: sacctmgr list accounts
        Example: sacctmgr modify account test-account set fairshare=333.
        """
        args = shlex.split(arg)
        output = self.slurm_emulator.execute_command("sacctmgr", args)
        print(output)

    def do_sacct(self, arg):
        """Run sacct command.

        Usage: sacct <args>
        Example: sacct --accounts=test-account --format=Account,User,Elapsed.
        """
        args = shlex.split(arg)
        output = self.slurm_emulator.execute_command("sacct", args)
        print(output)

    def do_sinfo(self, arg):
        """Run sinfo command.

        Usage: sinfo <args>
        Example: sinfo -V.
        """
        args = shlex.split(arg)
        output = self.slurm_emulator.execute_command("sinfo", args)
        print(output)

    # ============================================================================
    # Status and Help Commands
    # ============================================================================

    def do_status(self, arg):
        """Show emulator status."""
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

        # Show configuration
        print(f"\n⚙️  Configuration: {'Loaded' if self.slurm_config else 'Default'}")

        # Show completion status
        try:
            import readline  # noqa: PLC0415

            completer = readline.get_completer()
            print("\n⌨️  TAB Completion:")
            print(f"   Completer: {type(completer).__name__ if completer else 'None'}")
            print(
                f"   Methods: {len([attr for attr in dir(self) if attr.startswith('complete_')])}"
            )
        except ImportError:
            print("\n❌ TAB Completion: readline not available")

    def do_test_completion(self, arg):
        """Test completion functionality manually.

        Usage: test_completion.

        This command tests various completion scenarios to verify TAB completion is working.
        """
        print("🧪 Testing Completion Methods")
        print("=" * 40)

        # Test cases with proper line formats
        test_cases = [
            {
                "description": "Time advance units",
                "method": "complete_time_advance",
                "text": "",
                "line": "time_advance 2 ",
                "expected": "days, months, quarters",
            },
            {
                "description": "Time advance partial unit",
                "method": "complete_time_advance",
                "text": "m",
                "line": "time_advance 2 m",
                "expected": "months",
            },
            {
                "description": "Scenario names",
                "method": "complete_scenario_run",
                "text": "",
                "line": "scenario_run ",
                "expected": "scenario names",
            },
            {
                "description": "Scenario partial name",
                "method": "complete_scenario_run",
                "text": "qos",
                "line": "scenario_run qos",
                "expected": "qos_thresholds",
            },
            {
                "description": "Account names",
                "method": "complete_account_show",
                "text": "",
                "line": "account_show ",
                "expected": "account names",
            },
            {
                "description": "QoS levels",
                "method": "complete_qos_set",
                "text": "s",
                "line": "qos_set test s",
                "expected": "slowdown",
            },
        ]

        for i, case in enumerate(test_cases, 1):
            print(f"\n{i}. {case['description']}:")
            print(f"   Command: {case['line']}")
            print(f"   Completing: '{case['text']}'")

            try:
                method = getattr(self, case["method"])
                result = method(case["text"], case["line"], 0, len(case["text"]))

                print(f"   Result: {result}")
                print(f"   Expected: {case['expected']}")

                if result:
                    print("   ✅ Working!")
                else:
                    print("   ⚠️  No matches")

            except Exception as e:
                print(f"   ❌ Error: {e}")

        print("\n💡 Interactive Usage:")
        print("   In a real terminal, type these commands and press TAB:")
        for case in test_cases[:3]:
            print(f"   {case['line']}[TAB]")

        print("\n🎯 TAB Completion Status:")
        try:
            import readline  # noqa: PLC0415,F401

            print("   ✅ readline available")
            print("   ✅ 15 completion methods implemented")
            print("   ✅ cmd module handles TAB completion automatically")
            if sys.stdin.isatty():
                print("   ✅ Interactive terminal detected")
                print("   → TAB completion should work!")
            else:
                print("   ❌ Non-interactive terminal")
                print("   → TAB completion disabled for security")
        except ImportError:
            print("   ❌ readline not available - TAB completion won't work")

    def do_debug_tab(self, arg):
        """Debug TAB completion behavior.

        This command helps diagnose why TAB completion might not work in your terminal.
        """
        print("🔧 TAB Completion Debug")
        print("=" * 30)

        try:
            import readline  # noqa: PLC0415

            print("📚 Readline Information:")
            print("   Module loaded: ✅")
            print(f"   Interactive terminal: {sys.stdin.isatty()}")

            try:
                completer = readline.get_completer()
                print(f"   Current completer: {type(completer).__name__ if completer else 'None'}")

                if completer:
                    print("   ✅ Completer is set")

                    # Test the completer directly
                    print("\n🔧 Testing completer directly:")
                    try:
                        test_result = completer("tim", 0)
                        print(f"   completer('tim', 0) = {test_result}")
                        test_result2 = completer("time_", 0)
                        print(f"   completer('time_', 0) = {test_result2}")
                    except Exception as e:
                        print(f"   Direct completer test error: {e}")
                else:
                    print("   ❌ No completer set!")
                    print("   This means cmd module completion isn't working")

            except Exception as e:
                print(f"   Completer check error: {e}")

            # Check completion settings
            try:
                delims = readline.get_completer_delims()
                print(f"   Completion delims: '{delims}'")
            except Exception as e:
                print(f"   Delimiter error: {e}")

            # Test manual completion triggering
            print("\n🔧 Manual Completion Test:")
            print("   Testing if completion methods can be called...")

            try:
                # Test our completion methods manually
                result1 = self.complete_time_advance("m", "time_advance 2 m", 0, 1)
                print(f"   complete_time_advance('m', 'time_advance 2 m') = {result1}")

                result2 = self.complete_scenario_run("q", "scenario_run q", 0, 1)
                print(f"   complete_scenario_run('q', 'scenario_run q') = {result2}")

            except Exception as e:
                print(f"   Manual method test error: {e}")

        except ImportError:
            print("❌ readline module not available")
            print("   This is why TAB completion isn't working!")

        # Show completion methods available
        print("\n🎮 CMD Completion Methods:")
        methods = [attr for attr in dir(self) if attr.startswith("complete_")]
        print(f"   Found {len(methods)} methods:")
        for method in methods[:5]:  # Show first 5
            print(f"     - {method}")
        if len(methods) > 5:
            print(f"     ... and {len(methods) - 5} more")

        print("\n🔍 CMD Module Integration Check:")
        print(f"   cmd.Cmd class: {cmd.Cmd}")
        print(f"   Our class: {type(self)}")
        print(f"   Inherits from cmd.Cmd: {isinstance(self, cmd.Cmd)}")

        # Check if cmd module should handle completion
        print(f"   cmdloop method: {hasattr(self, 'cmdloop')}")
        print(f"   complete method: {hasattr(self, 'complete')}")
        print(f"   parseline method: {hasattr(self, 'parseline')}")

        print("\n💡 If TAB still doesn't work:")
        print("   1. Try the debug_tab_completion.py script")
        print("   2. Check your terminal settings")
        print("   3. Try a different terminal (iTerm2, etc.)")
        print("   4. Use test_completion command as fallback")

    def do_debug_readline(self, arg):
        """Debug readline configuration directly."""
        try:
            import readline  # noqa: PLC0415

            print("🔧 Advanced Readline Debug")
            print("=" * 35)

            # Check if readline is properly initialized
            print("📋 Readline State:")
            try:
                # Get current line buffer (should be empty when not completing)
                buffer = (
                    readline.get_line_buffer() if hasattr(readline, "get_line_buffer") else "N/A"
                )
                print(f"   Line buffer: '{buffer}'")
            except Exception:
                print("   Line buffer: Not available")

            # Check completion state
            with contextlib.suppress(builtins.BaseException):
                print(
                    f"   Completion type: {readline.get_completion_type() if hasattr(readline, 'get_completion_type') else 'N/A'}"
                )

            # Test if we can set our own completer
            print("\n🔧 Testing Custom Completer:")

            def debug_completer(text, state):
                print(f"   🎯 Custom completer called: text='{text}', state={state}")
                options = ["debug1", "debug2", "debug3"]
                matches = [opt for opt in options if opt.startswith(text)]
                return matches[state] if state < len(matches) else None

            old_completer = readline.get_completer()
            readline.set_completer(debug_completer)

            print("   Custom completer set")
            print("   Try typing: debug[TAB] in the next prompt")
            print("   Type 'back' to return to normal mode")

            # Mini loop to test custom completer
            try:
                while True:
                    user_input = input("debug-readline> ")
                    if user_input.lower() == "back":
                        break
                    print(f"   You entered: '{user_input}'")
            except (EOFError, KeyboardInterrupt):
                pass

            # Restore original completer
            readline.set_completer(old_completer)
            print("   Restored original completer")

        except ImportError:
            print("❌ readline not available for advanced debugging")

    def do_EOF(self, arg):  # noqa: N802
        """Handle Ctrl+D (EOF) gracefully."""
        print("\n👋 Goodbye!")
        return True

    def do_exit(self, arg):
        """Exit the emulator."""
        print("👋 Goodbye!")
        return True

    def do_quit(self, arg):
        """Quit the emulator."""
        print("👋 Goodbye!")
        return True

    # ============================================================================
    # Helper Methods (from original CLI)
    # ============================================================================

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
            try:
                response = input(f"\nProceed with {len(scenario.steps)} steps? [Y/n]: ")
                if response.lower() in ["n", "no"]:
                    print("❌ Scenario cancelled")
                    return
            except (EOFError, KeyboardInterrupt):
                print("\n❌ Scenario cancelled")
                return

        try:
            for i, step in enumerate(scenario.steps, 1):
                if step_by_step or interactive:
                    print(f"\n📍 Step {i}: {step.name}")
                    print(f"   📝 {step.description}")
                    if step.time_point:
                        print(f"   ⏰ Target time: {step.time_point}")

                    if interactive:
                        try:
                            input("⏸️  Press Enter to execute this step...")
                        except (EOFError, KeyboardInterrupt):
                            print("\n❌ Scenario cancelled")
                            return

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
            print(f"\n❌ Scenario failed: {e}")

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


def main():
    """Main entry point for CMD-based CLI."""
    parser = argparse.ArgumentParser(
        description="SLURM Emulator - Time Travel Edition (CMD Interface)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  slurm-emulator                                    # Run with defaults
  slurm-emulator --config examples/slurm.conf      # Load SLURM configuration
  slurm-emulator -c /etc/slurm/slurm.conf          # Use system config

CMD Interface Features:
  - Built-in TAB auto-completion
  - Built-in help system (help <command>)
  - Better command parsing with quoted arguments
  - History navigation with arrow keys
        """,
    )

    parser.add_argument(
        "-c", "--config", metavar="FILE", help="Path to slurm.conf configuration file"
    )

    parser.add_argument(
        "--validate-only", action="store_true", help="Only validate configuration and exit"
    )

    args = parser.parse_args()

    if args.validate_only and args.config:
        # Just validate configuration and exit
        try:
            config = SlurmConfigParser(args.config)
            config.print_config_summary()
            warnings = config.validate_configuration()
            if warnings:
                print("\n⚠️  Configuration Warnings:")
                for warning in warnings:
                    print(f"   - {warning}")
                sys.exit(1)
            else:
                print("\n✅ Configuration is valid!")
                sys.exit(0)
        except Exception as e:
            print(f"❌ Configuration validation failed: {e}")
            sys.exit(1)
    elif args.validate_only:
        print("❌ --validate-only requires --config to be specified")
        sys.exit(1)

    # Run the CMD-based CLI
    cli = SlurmEmulatorCmd(args.config)
    try:
        cli.cmdloop()
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")


if __name__ == "__main__":
    main()
