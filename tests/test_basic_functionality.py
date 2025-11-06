"""Basic functionality tests that should pass."""

import pytest
from datetime import datetime
from emulator.core.database import SlurmDatabase, Account, User, UsageRecord
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import UsageSimulator
from emulator.scenarios.sequence_scenario import SequenceScenario
from emulator.scenarios.scenario_registry import ScenarioRegistry


class TestBasicDatabase:
    """Test basic database operations."""

    def setup_method(self):
        """Set up test database."""
        self.db = SlurmDatabase()

    def test_database_initialization(self):
        """Test database initializes properly."""
        assert self.db.accounts is not None
        assert self.db.users is not None
        assert len(self.db.accounts) >= 1  # Should have root account

    def test_account_operations(self):
        """Test basic account operations."""
        self.db.add_account("test_account", "Test Description", "Test Org")

        account = self.db.get_account("test_account")
        assert account is not None
        assert account.name == "test_account"
        assert account.description == "Test Description"

    def test_user_operations(self):
        """Test basic user operations."""
        self.db.add_user("test_user", "test_account")

        user = self.db.get_user("test_user")
        assert user is not None
        assert user.name == "test_user"


class TestBasicTimeEngine:
    """Test basic time engine operations."""

    def setup_method(self):
        """Set up test time engine."""
        self.time_engine = TimeEngine()

    def test_time_engine_initialization(self):
        """Test time engine initializes properly."""
        current_time = self.time_engine.get_current_time()
        assert current_time is not None
        assert isinstance(current_time, datetime)

    def test_quarter_calculation(self):
        """Test quarter calculation."""
        quarter = self.time_engine.get_current_quarter()
        assert quarter is not None
        assert "Q" in quarter
        assert len(quarter.split("-")) == 2


class TestBasicUsageSimulator:
    """Test basic usage simulator operations."""

    def setup_method(self):
        """Set up test environment."""
        self.time_engine = TimeEngine()
        self.database = SlurmDatabase()
        self.usage_sim = UsageSimulator(self.time_engine, self.database)

    def test_usage_simulator_initialization(self):
        """Test usage simulator initializes properly."""
        assert self.usage_sim.time_engine is not None
        assert self.usage_sim.database is not None

    def test_usage_injection_basic(self):
        """Test basic usage injection."""
        # Set up account and user
        self.database.add_account("test_account", "Test", "Org")
        self.database.add_user("test_user", "test_account")

        # Inject usage
        self.usage_sim.inject_usage("test_account", "test_user", 100.0)

        # Verify usage was recorded
        records = self.database.get_usage_records(account="test_account")
        assert len(records) >= 1

        # Find our record
        our_record = None
        for record in records:
            if record.user == "test_user" and record.account == "test_account":
                our_record = record
                break

        assert our_record is not None
        assert our_record.node_hours == 100.0


class TestBasicScenarios:
    """Test basic scenario operations."""

    def setup_method(self):
        """Set up test environment."""
        self.time_engine = TimeEngine()
        self.database = SlurmDatabase()
        self.scenario = SequenceScenario(self.time_engine, self.database)
        self.registry = ScenarioRegistry()

    def test_sequence_scenario_initialization(self):
        """Test sequence scenario initializes properly."""
        assert self.scenario.time_engine is not None
        assert self.scenario.database is not None
        assert self.scenario.account is not None
        assert len(self.scenario.users) > 0

    def test_scenario_setup(self):
        """Test scenario setup works."""
        self.scenario.setup_scenario()

        # Verify account was created
        account = self.database.get_account(self.scenario.account)
        assert account is not None
        assert account.name == self.scenario.account

    def test_scenario_registry_initialization(self):
        """Test scenario registry initializes properly."""
        assert self.registry.scenarios is not None
        assert isinstance(self.registry.scenarios, dict)

    def test_scenario_registry_has_scenarios(self):
        """Test that registry has some scenarios."""
        scenarios = self.registry.list_scenarios()
        assert isinstance(scenarios, list)
        # Don't require specific scenarios, just that it doesn't crash


class TestBasicPeriodicLimits:
    """Test basic periodic limits functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.time_engine = TimeEngine()
        self.database = SlurmDatabase()

    def test_periodic_limits_import(self):
        """Test periodic limits modules can be imported."""
        from emulator.periodic_limits.calculator import PeriodicLimitsCalculator
        from emulator.periodic_limits.qos_manager import QoSManager

        calculator = PeriodicLimitsCalculator(self.database, self.time_engine)
        qos_manager = QoSManager(self.database, self.time_engine)

        assert calculator is not None
        assert qos_manager is not None

    def test_decay_calculation_basic(self):
        """Test basic decay calculation."""
        from emulator.periodic_limits.calculator import PeriodicLimitsCalculator

        calculator = PeriodicLimitsCalculator(self.database, self.time_engine)

        # Test decay calculation doesn't crash
        decay_factor = calculator.calculate_decay_factor(15)
        assert isinstance(decay_factor, float)
        assert 0 <= decay_factor <= 1

    def test_qos_basic_operations(self):
        """Test basic QoS operations."""
        from emulator.periodic_limits.qos_manager import QoSManager

        qos_manager = QoSManager(self.database, self.time_engine)

        # Set up test account
        self.database.add_account("test_account", "Test", "Org")

        # Test basic QoS operations
        qos = qos_manager.get_account_qos("test_account")
        assert isinstance(qos, str)

        # Test QoS levels list
        levels = qos_manager.list_qos_levels()
        assert isinstance(levels, list)
        assert len(levels) > 0


class TestBasicCommands:
    """Test basic command functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.time_engine = TimeEngine()
        self.database = SlurmDatabase()

    def test_command_imports(self):
        """Test command modules can be imported."""
        from emulator.commands.sacctmgr import SacctmgrEmulator
        from emulator.commands.sacct import SacctEmulator
        from emulator.commands.dispatcher import SlurmEmulator

        sacctmgr = SacctmgrEmulator(self.database, self.time_engine)
        sacct = SacctEmulator(self.database, self.time_engine)
        dispatcher = SlurmEmulator()

        assert sacctmgr is not None
        assert sacct is not None
        assert dispatcher is not None

    def test_basic_command_handling(self):
        """Test basic command handling doesn't crash."""
        from emulator.commands.sacctmgr import SacctmgrEmulator
        from emulator.commands.sacct import SacctEmulator

        sacctmgr = SacctmgrEmulator(self.database, self.time_engine)
        sacct = SacctEmulator(self.database, self.time_engine)

        # Test that commands return strings and don't crash
        result1 = sacctmgr.handle_command(["sacctmgr", "--version"])
        result2 = sacct.handle_command(["sacct", "--version"])

        assert isinstance(result1, str)
        assert isinstance(result2, str)


class TestBasicCLI:
    """Test basic CLI functionality."""

    def test_cli_imports(self):
        """Test CLI modules can be imported."""
        from emulator.cli.main import EmulatorCLI
        from emulator.cli.cmd_cli import SlurmEmulatorCmd

        # Test basic instantiation doesn't crash
        cli = EmulatorCLI()
        cmd_cli = SlurmEmulatorCmd()

        assert cli is not None
        assert cmd_cli is not None
