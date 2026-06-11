"""Tests for TRES string formatting improvements."""

from emulator.commands.sacct import SacctEmulator
from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import UsageSimulator


class TestTresFormatting:
    """Test TRES string formatting for site agent compatibility."""

    def setup_method(self):
        """Set up test environment."""
        self.database = SlurmDatabase()
        self.time_engine = TimeEngine()
        self.usage_sim = UsageSimulator(self.time_engine, self.database)
        self.sacct = SacctEmulator(self.database, self.time_engine)

        # Set up test account and user
        self.database.add_account("test_account", "Test Account", "Test Org")
        self.database.add_user("test_user", "test_account")

    def test_raw_tres_includes_node_hours(self):
        """Test that raw TRES includes node-hours component."""
        # Convert node hours to raw TRES
        raw_tres = self.usage_sim._convert_to_raw_tres(100.0)

        # Verify node-hours component is included
        assert "node-hours" in raw_tres
        assert raw_tres["node-hours"] == 100

        # Verify other components are still present
        assert "CPU" in raw_tres
        assert "Mem" in raw_tres
        assert "GRES/gpu" in raw_tres

        # Verify standard node ratios
        assert raw_tres["CPU"] == 100 * 64  # 64 CPUs per node
        assert raw_tres["Mem"] == 100 * 512  # 512GB per node
        assert raw_tres["GRES/gpu"] == 100 * 4  # 4 GPUs per node

    def test_tres_string_format_with_node_hours(self):
        """ReqTRES is a standard per-job TRES string in TRES-id order.

        Real sacct never prints the emulator-internal node-hours key;
        rates derive from the record's raw_tres totals / node_hours.
        """
        usage_record = UsageRecord(
            account="test_account",
            user="test_user",
            node_hours=50.0,
            billing_units=50.0,
            timestamp=self.time_engine.get_current_time(),
            period=self.time_engine.get_current_quarter(),
        )
        usage_record.raw_tres = {"node-hours": 50, "CPU": 3200, "Mem": 25600, "GRES/gpu": 200}
        self.database.usage_records.append(usage_record)

        result = self.sacct.handle_command(
            ["--format=JobID,ReqTRES", "--accounts=test_account", "-n", "-P"]
        )

        lines = result.strip().split("\n")
        assert len(lines) == 1
        job_id, tres_field = lines[0].split("|")
        assert job_id == "1"  # numeric JobID assigned by the database
        # 3200 CPU-hours / 50 node-hours = 64 cpus; 25600/50 = 512G; 200/50 = 4
        assert tres_field == "cpu=64,mem=512G,node=1,billing=64,gres/gpu=4"
        assert "node-hours" not in result

    def test_tres_string_standard_node_when_no_usage(self):
        """A zero-elapsed job still requested resources: the TRES string
        falls back to the standard node config (64 CPU / 512G / 4 GPU)."""
        usage_record = UsageRecord(
            account="test_account",
            user="test_user",
            node_hours=0.0,
            billing_units=0.0,
            timestamp=self.time_engine.get_current_time(),
            period=self.time_engine.get_current_quarter(),
        )
        usage_record.raw_tres = {}
        self.database.usage_records.append(usage_record)

        result = self.sacct.handle_command(
            ["--format=JobID,ReqTRES", "--accounts=test_account", "-n", "-P"]
        )

        lines = result.strip().split("\n")
        assert len(lines) == 1
        tres_field = lines[0].split("|")[1]
        assert tres_field == "cpu=64,mem=512G,node=1,billing=64,gres/gpu=4"

    def test_tres_string_with_mixed_tres_types(self):
        """Test TRES string with various TRES types."""
        # Inject usage to create a record with mixed TRES
        self.usage_sim.inject_usage("test_account", "test_user", 25.0)

        # Get the created record and modify it
        records = self.database.get_usage_records(account="test_account")
        assert len(records) >= 1

        record = records[-1]  # Get the most recent record

        # Add additional TRES types (modify the existing record)
        record.raw_tres["Energy"] = 1000
        record.raw_tres["GRES/gpu"] = 100

        # Format output
        result = self.sacct.handle_command(
            ["--format=JobID,ReqTRES", "--accounts=test_account", "-n", "-P"]
        )

        lines = result.strip().split("\n")
        assert len(lines) >= 1
        tres_field = lines[0].split("|")[1]

        # The internal node-hours key is never exposed.
        assert "node-hours" not in tres_field
        # 100 GPU-hours over 25 node-hours = 4 GPUs.
        assert "gres/gpu=4" in tres_field
        assert tres_field.startswith("cpu=64,mem=512G,node=1,billing=64")

    def test_usage_simulator_tres_consistency(self):
        """Test that usage simulator generates consistent TRES data."""
        # Test multiple node hour values
        test_values = [1.0, 10.5, 100.0, 0.25]

        for node_hours in test_values:
            raw_tres = self.usage_sim._convert_to_raw_tres(node_hours)

            # Verify node-hours component matches input
            assert raw_tres["node-hours"] == int(node_hours)

            # Verify ratios are consistent
            assert raw_tres["CPU"] == int(node_hours * 64)
            assert raw_tres["Mem"] == int(node_hours * 512)
            assert raw_tres["GRES/gpu"] == int(node_hours * 4)

            # Verify all values are integers
            for key, value in raw_tres.items():
                assert isinstance(value, int), f"{key} should be int, got {type(value)}"
