"""Test that all main modules can be imported successfully."""


def test_core_modules_import():
    """Test that core modules import without errors."""
    from emulator.core.database import SlurmDatabase
    from emulator.core.time_engine import TimeEngine
    from emulator.core.usage_simulator import UsageSimulator

    # Test instantiation
    time_engine = TimeEngine()
    database = SlurmDatabase()
    usage_simulator = UsageSimulator(time_engine, database)

    assert time_engine is not None
    assert database is not None
    assert usage_simulator is not None


def test_periodic_limits_modules_import():
    """Test that periodic limits modules import without errors."""
    from emulator.core.database import SlurmDatabase
    from emulator.core.time_engine import TimeEngine
    from emulator.periodic_limits.calculator import PeriodicLimitsCalculator
    from emulator.periodic_limits.qos_manager import QoSManager

    time_engine = TimeEngine()
    database = SlurmDatabase()

    calculator = PeriodicLimitsCalculator(database, time_engine)
    qos_manager = QoSManager(database, time_engine)

    assert calculator is not None
    assert qos_manager is not None


def test_command_modules_import():
    """Test that command modules import without errors."""
    from emulator.core.database import SlurmDatabase
    from emulator.core.time_engine import TimeEngine
    from emulator.commands.sacctmgr import SacctmgrEmulator
    from emulator.commands.sacct import SacctEmulator
    from emulator.commands.dispatcher import SlurmEmulator

    time_engine = TimeEngine()
    database = SlurmDatabase()

    sacctmgr = SacctmgrEmulator(database, time_engine)
    sacct = SacctEmulator(database, time_engine)
    dispatcher = SlurmEmulator()

    assert sacctmgr is not None
    assert sacct is not None
    assert dispatcher is not None


def test_scenario_modules_import():
    """Test that scenario modules import without errors."""
    from emulator.core.database import SlurmDatabase
    from emulator.core.time_engine import TimeEngine
    from emulator.scenarios.sequence_scenario import SequenceScenario
    from emulator.scenarios.scenario_registry import ScenarioRegistry

    time_engine = TimeEngine()
    database = SlurmDatabase()

    scenario = SequenceScenario(time_engine, database)
    registry = ScenarioRegistry()

    assert scenario is not None
    assert registry is not None


def test_cli_modules_import():
    """Test that CLI modules import without errors."""
    from emulator.cli.main import EmulatorCLI
    from emulator.cli.cmd_cli import SlurmEmulatorCmd

    cli = EmulatorCLI()
    cmd_cli = SlurmEmulatorCmd()

    assert cli is not None
    assert cmd_cli is not None
