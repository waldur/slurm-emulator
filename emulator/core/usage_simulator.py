"""Usage simulation for injecting node-hour consumption."""

from datetime import datetime, timedelta
from typing import Optional

from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine


class UsageSimulator:
    """Simulates user usage patterns and injection."""

    def __init__(self, time_engine: TimeEngine, database: SlurmDatabase):
        self.time_engine = time_engine
        self.database = database
        self.billing_weights = {
            "CPU": 0.015625,  # 64 CPUs = 1 billing unit
            "Mem": 0.001953125,  # 512 GB = 1 billing unit
            "GRES/gpu": 0.25,  # 4 GPUs = 1 billing unit
        }

    def inject_usage(
        self, account: str, user: str, node_hours: float, at_time: Optional[datetime] = None
    ) -> None:
        """Inject specific usage amount at given time."""
        if at_time is None:
            at_time = self.time_engine.get_current_time()

        # Ensure user and account exist
        if not self.database.get_user(user):
            self.database.add_user(user, account)

        if not self.database.get_account(account):
            self.database.add_account(account, f"Account {account}", "emulator")

        # Ensure association exists
        if not self.database.get_association(user, account):
            self.database.add_association(user, account)

        # Create usage record
        usage_record = UsageRecord(
            account=account,
            user=user,
            node_hours=node_hours,
            billing_units=node_hours,  # 1:1 for node-hour billing
            timestamp=at_time,
            period=self.time_engine.get_current_quarter(),
            raw_tres=self._convert_to_raw_tres(node_hours),
        )

        self.database.add_usage_record(usage_record)
        print(f"💾 Injected {node_hours}Nh usage for {user} in {account} at {at_time}")

        # Save state after injection
        self.database.save_state()

    def inject_usage_pattern(self, account: str, user: str, pattern_config: dict) -> None:
        """Inject usage following a pattern over time."""
        pattern_type = pattern_config.get("type", "steady")

        if pattern_type == "steady":
            self._steady_pattern(account, user, pattern_config)
        elif pattern_type == "bursty":
            self._bursty_pattern(account, user, pattern_config)
        elif pattern_type == "end_of_period":
            self._end_of_period_pattern(account, user, pattern_config)
        else:
            raise ValueError(f"Unknown pattern type: {pattern_type}")

    def simulate_sequence_scenario(self) -> None:
        """Run the exact scenario from SLURM_PERIODIC_LIMITS_SEQUENCE.md."""
        account = "slurm_account_123"
        user1, user2 = "user1", "user2"

        print("🎬 Starting sequence scenario simulation")

        # Q1: 500Nh over 3 months (167Nh per month roughly)
        print("\n📅 Q1 2024: Simulating 500Nh usage over 3 months")

        self.time_engine.set_time(datetime(2024, 1, 31))
        self.inject_usage(account, user1, 100)
        self.inject_usage(account, user2, 67)

        self.time_engine.set_time(datetime(2024, 2, 29))
        self.inject_usage(account, user1, 100)
        self.inject_usage(account, user2, 67)

        self.time_engine.set_time(datetime(2024, 3, 31))
        self.inject_usage(account, user1, 100)
        self.inject_usage(account, user2, 66)  # Total: 500Nh

        q1_total = self.database.get_total_usage(account, "2024-Q1")
        print(f"✅ Q1 total usage: {q1_total}Nh")

        # Q2 Transition - advance to Q2
        print("\n📅 Q2 2024: Transition with carryover")
        self.time_engine.set_time(datetime(2024, 4, 1))

        # Q2 Month 1: 500Nh
        self.time_engine.set_time(datetime(2024, 4, 30))
        self.inject_usage(account, user1, 300)
        self.inject_usage(account, user2, 200)

        # Q2 Month 2: 500Nh more (reaching 1500Nh total for Q2)
        self.time_engine.set_time(datetime(2024, 5, 20))
        self.inject_usage(account, user1, 300)
        self.inject_usage(account, user2, 200)

        q2_partial = self.database.get_total_usage(account, "2024-Q2")
        print(f"📊 Q2 usage so far: {q2_partial}Nh (should trigger threshold)")

        # Additional usage: 200Nh more (total 1700Nh in Q2)
        self.time_engine.advance_time(days=5)
        self.inject_usage(account, user1, 200)

        # Final push: 250Nh more (total 2000Nh - hitting hard limit)
        self.time_engine.advance_time(days=10)
        self.inject_usage(account, user1, 250)

        q2_total = self.database.get_total_usage(account, "2024-Q2")
        print(f"🚨 Q2 final usage: {q2_total}Nh (should hit hard limit)")

        # Q3 Transition with decay
        print("\n📅 Q3 2024: Transition with 15-day decay")
        self.time_engine.set_time(datetime(2024, 7, 1))

        print("✅ Sequence scenario simulation completed!")

    def get_current_usage_summary(self, account: str) -> dict:
        """Get current usage summary for account."""
        current_period = self.time_engine.get_current_quarter()
        period_usage = self.database.get_total_usage(account, current_period)
        total_usage = self.database.get_total_usage(account)

        account_obj = self.database.get_account(account)
        allocation = account_obj.allocation if account_obj else 1000

        return {
            "account": account,
            "current_period": current_period,
            "period_usage": period_usage,
            "total_usage": total_usage,
            "allocation": allocation,
            "remaining": max(0, allocation - period_usage),
            "percentage_used": (period_usage / allocation) * 100 if allocation > 0 else 0,
        }

    def _convert_to_raw_tres(self, node_hours: float) -> dict[str, int]:
        """Convert billing units to raw TRES based on standard node config."""
        # Standard node: 64 CPUs, 512GB RAM, 4 GPUs
        # Include "node" component for site agent compatibility
        return {
            "node-hours": int(node_hours),  # Direct node-hours mapping for site agent
            "CPU": int(node_hours * 64),
            "Mem": int(node_hours * 512),  # GB
            "GRES/gpu": int(node_hours * 4),
        }

    def _steady_pattern(self, account: str, user: str, config: dict) -> None:
        """Generate steady usage pattern over time period."""
        total_usage = config["total_usage"]
        days = config.get("days", 30)
        daily_usage = total_usage / days

        start_time = self.time_engine.get_current_time()

        for day in range(days):
            usage_time = start_time + timedelta(days=day)
            self.inject_usage(account, user, daily_usage, usage_time)

    def _bursty_pattern(self, account: str, user: str, config: dict) -> None:
        """Generate bursty usage pattern with irregular spikes."""
        burst_times = config["burst_times"]  # List of (day, usage) tuples
        start_time = self.time_engine.get_current_time()

        for day, usage in burst_times:
            usage_time = start_time + timedelta(days=day)
            self.inject_usage(account, user, usage, usage_time)

    def _end_of_period_pattern(self, account: str, user: str, config: dict) -> None:
        """Generate usage pattern concentrated at end of period."""
        total_usage = config["total_usage"]
        period_days = config.get("period_days", 90)  # Quarter
        concentration_days = config.get("concentration_days", 7)  # Last week

        # 80% of usage in last concentration_days
        concentrated_usage = total_usage * 0.8
        regular_usage = total_usage * 0.2

        start_time = self.time_engine.get_current_time()

        # Regular usage throughout period
        daily_regular = regular_usage / (period_days - concentration_days)
        for day in range(period_days - concentration_days):
            usage_time = start_time + timedelta(days=day)
            self.inject_usage(account, user, daily_regular, usage_time)

        # Concentrated usage at end
        daily_concentrated = concentrated_usage / concentration_days
        for day in range(period_days - concentration_days, period_days):
            usage_time = start_time + timedelta(days=day)
            self.inject_usage(account, user, daily_concentrated, usage_time)
