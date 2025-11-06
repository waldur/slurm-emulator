"""Scenarios demonstrating different SLURM limit configurations."""

from datetime import datetime

from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import UsageSimulator
from emulator.periodic_limits.calculator import PeriodicLimitsCalculator
from emulator.periodic_limits.qos_manager import QoSManager


class TraditionalMaxTRESMinsScenario:
    """Example 1 from configuration plan: Traditional MaxTRESMins Setup."""

    def __init__(self, time_engine: TimeEngine, database: SlurmDatabase):
        self.time_engine = time_engine
        self.database = database
        self.usage_simulator = UsageSimulator(time_engine, database)
        self.limits_calculator = PeriodicLimitsCalculator(database, time_engine)
        self.qos_manager = QoSManager(database, time_engine)

        # Scenario configuration
        self.account = "traditional_account"
        self.users = ["researcher1", "researcher2", "student1"]

    def setup_scenario(self) -> None:
        """Set up traditional MaxTRESMins configuration."""
        print("🏛️  Setting up Traditional MaxTRESMins Scenario...")
        print("   Using raw TRES values with per-user time limits")

        # Create account and users
        self.database.add_account(
            self.account, "Traditional HPC allocation with MaxTRESMins", "research_group"
        )

        for user in self.users:
            self.database.add_user(user, self.account)
            self.database.add_association(user, self.account)

        # Set time to start of quarter
        self.time_engine.set_time(datetime(2024, 1, 1))

        account_obj = self.database.get_account(self.account)
        if account_obj is not None:
            # Apply traditional MaxTRESMins limits (raw TRES, no billing units)
            account_obj.limits["MaxTRESMins:CPU"] = 43200  # 30 days * 24 hours * 60 mins
            account_obj.limits["MaxTRESMins:mem"] = 2160000  # 1000GB * 30 days * 24 * 60 / 1024
            account_obj.limits["MaxTRESMins:gres/gpu"] = 2880  # 2 GPUs * 30 days * 24 * 60

            # Set fairshare
            account_obj.fairshare = 100

            self.database.save_state()
            print(f"✅ Account '{self.account}' configured with MaxTRESMins limits")
            print(f"   CPU: {account_obj.limits['MaxTRESMins:CPU']} minutes")
            print(f"   Memory: {account_obj.limits['MaxTRESMins:mem']} MB-minutes")
            print(f"   GPU: {account_obj.limits['MaxTRESMins:gres/gpu']} GPU-minutes")
        else:
            print(f"❌ Error: Account '{self.account}' not found")

    def run_usage_pattern(self) -> None:
        """Simulate typical usage under MaxTRESMins limits."""
        print("\n📊 Simulating usage under MaxTRESMins limits...")

        # Week 1: Light usage
        self.usage_simulator.inject_usage(
            self.account, "researcher1", 50, datetime(2024, 1, 7)
        )  # 50 node-hours
        self.usage_simulator.inject_usage(self.account, "researcher2", 30, datetime(2024, 1, 7))

        print("   Week 1: Light usage (researcher1: 50Nh, researcher2: 30Nh)")

        # Week 2: Heavy usage by one user
        self.usage_simulator.inject_usage(self.account, "researcher1", 200, datetime(2024, 1, 14))

        print("   Week 2: Heavy usage (researcher1: +200Nh)")

        # Check individual user limits
        current_quarter = self.time_engine.get_current_quarter()
        user1_records = [
            r
            for r in self.database.get_usage_records(account=self.account, period=current_quarter)
            if r.user == "researcher1"
        ]
        user2_records = [
            r
            for r in self.database.get_usage_records(account=self.account, period=current_quarter)
            if r.user == "researcher2"
        ]

        total_usage_r1 = sum(r.node_hours for r in user1_records)
        total_usage_r2 = sum(r.node_hours for r in user2_records)

        print(
            f"   Current usage - researcher1: {total_usage_r1}Nh, researcher2: {total_usage_r2}Nh"
        )

        # Week 3: Student starts using allocation
        self.usage_simulator.inject_usage(self.account, "student1", 100, datetime(2024, 1, 21))

        print("   Week 3: Student joins (student1: 100Nh)")

        self._check_limit_enforcement()

    def _check_limit_enforcement(self) -> None:
        """Check how MaxTRESMins limits would be enforced."""
        print("\n🔍 Checking MaxTRESMins limit enforcement...")

        account_obj = self.database.get_account(self.account)
        if account_obj is None:
            print(f"❌ Error: Account '{self.account}' not found")
            return

        current_quarter = self.time_engine.get_current_quarter()

        for user in self.users:
            user_records = [
                r
                for r in self.database.get_usage_records(
                    account=self.account, period=current_quarter
                )
                if r.user == user
            ]
            user_usage = sum(r.node_hours for r in user_records)
            # Convert node-hours to CPU-minutes (assuming 1 node = 1 CPU)
            cpu_minutes_used = user_usage * 60
            max_cpu_minutes = account_obj.limits.get("MaxTRESMins:CPU", 0)

            if cpu_minutes_used > max_cpu_minutes:
                print(
                    f"   ❌ {user}: {cpu_minutes_used} > {max_cpu_minutes} CPU-minutes (EXCEEDED)"
                )
            else:
                remaining = max_cpu_minutes - cpu_minutes_used
                print(
                    f"   ✅ {user}: {cpu_minutes_used}/{max_cpu_minutes} CPU-minutes ({remaining} remaining)"
                )


class ModernBillingUnitsScenario:
    """Example 2 from configuration plan: Modern Billing Units with GrpTRESMins."""

    def __init__(self, time_engine: TimeEngine, database: SlurmDatabase):
        self.time_engine = time_engine
        self.database = database
        self.usage_simulator = UsageSimulator(time_engine, database)
        self.limits_calculator = PeriodicLimitsCalculator(database, time_engine)
        self.qos_manager = QoSManager(database, time_engine)

        # Modern billing configuration
        self.usage_simulator.billing_weights = {
            "CPU": 0.015625,  # 64 CPUs = 1 billing unit
            "Mem": 0.001953125,  # 512 GB = 1 billing unit
            "GRES/gpu": 0.25,  # 4 GPUs = 1 billing unit
        }

        self.account = "modern_billing_account"
        self.users = ["data_scientist1", "ml_engineer1", "postdoc1"]

    def setup_scenario(self) -> None:
        """Set up modern billing units configuration."""
        print("🚀 Setting up Modern Billing Units Scenario...")
        print("   Using billing units with GrpTRESMins group limits")

        # Create account and users
        self.database.add_account(
            self.account, "Modern cloud-style allocation with billing units", "ai_research_lab"
        )

        for user in self.users:
            self.database.add_user(user, self.account)
            self.database.add_association(user, self.account)

        # Set time to start of quarter
        self.time_engine.set_time(datetime(2024, 1, 1))

        account_obj = self.database.get_account(self.account)
        if account_obj is not None:
            # Apply GrpTRESMins with billing units
            # 1000 billing units * 60 minutes = 60000 billing-minutes per quarter
            account_obj.limits["GrpTRESMins:billing"] = 60000

            # Set fairshare for group sharing
            account_obj.fairshare = 500

            # Configure QoS thresholds (manually set in account metadata)
            account_obj.limits["qos_threshold"] = 48000  # 80% threshold

            self.database.save_state()
            print(f"✅ Account '{self.account}' configured with billing-based GrpTRESMins")
            print(f"   Group limit: {account_obj.limits['GrpTRESMins:billing']} billing-minutes")
        else:
            print(f"❌ Error: Account '{self.account}' not found")
        print("   Billing weights: 64 CPU = 512GB = 4 GPU = 1 billing unit")

    def run_mixed_workloads(self) -> None:
        """Simulate mixed CPU/GPU workloads with billing units."""
        print("\n🔬 Simulating mixed workloads with billing unit conversion...")

        # Week 1: CPU-intensive work
        print("   Week 1: CPU-intensive analysis")
        self.usage_simulator.inject_usage(
            self.account,
            "data_scientist1",
            100,  # 100 node-hours
            datetime(2024, 1, 7),
        )

        # Week 2: GPU machine learning
        print("   Week 2: GPU machine learning training")
        # Simulate GPU usage: 50 node-hours on GPU nodes
        # With 4 GPUs per node, this equals 200 GPU-hours = 50 billing units
        self.usage_simulator.inject_usage(
            self.account,
            "ml_engineer1",
            200,  # GPU-equivalent
            datetime(2024, 1, 14),
        )

        # Week 3: Memory-intensive processing
        print("   Week 3: High-memory data processing")
        self.usage_simulator.inject_usage(self.account, "postdoc1", 80, datetime(2024, 1, 21))

        self._check_billing_consumption()

        # Week 4: Approaching limits
        print("   Week 4: Heavy usage pushing toward limits")
        self.usage_simulator.inject_usage(
            self.account, "data_scientist1", 150, datetime(2024, 1, 28)
        )

        self._check_threshold_breach()

    def _check_billing_consumption(self) -> None:
        """Check billing unit consumption across the group."""
        print("\n💰 Checking billing unit consumption...")

        account_obj = self.database.get_account(self.account)
        if account_obj is None:
            print(f"❌ Error: Account '{self.account}' not found")
            return

        current_quarter = self.time_engine.get_current_quarter()

        total_usage = self.database.get_total_usage(self.account, current_quarter)
        billing_minutes_used = total_usage * 60  # Convert to minutes

        limit = account_obj.limits.get("GrpTRESMins:billing", 0)
        remaining = limit - billing_minutes_used
        percentage = (billing_minutes_used / limit) * 100 if limit > 0 else 0

        print(f"   Group usage: {billing_minutes_used}/{limit} billing-minutes ({percentage:.1f}%)")
        print(f"   Remaining: {remaining} billing-minutes")

        for user in self.users:
            user_records = [
                r
                for r in self.database.get_usage_records(
                    account=self.account, period=current_quarter
                )
                if r.user == user
            ]
            user_usage = sum(r.node_hours for r in user_records)
            user_billing_mins = user_usage * 60
            print(f"   {user}: {user_billing_mins} billing-minutes ({user_usage} node-hours)")

    def _check_threshold_breach(self) -> None:
        """Check if usage threshold is breached."""
        threshold_status = self.limits_calculator.check_usage_thresholds(self.account)

        # Check the various threshold conditions
        if threshold_status.get("over_grace_limit", False):
            print(
                f"   ❌ Grace limit exceeded: {threshold_status.get('threshold_status', 'Over limit')}"
            )
            print(
                f"   Recommended action: {threshold_status.get('recommended_action', 'Block access')}"
            )
        elif threshold_status.get("over_qos_threshold", False):
            print(
                f"   ⚠️  QoS threshold exceeded: {threshold_status.get('threshold_status', 'Over threshold')}"
            )
            print(
                f"   Recommended action: {threshold_status.get('recommended_action', 'Apply slowdown')}"
            )
        else:
            print("   ✅ Within threshold limits")


class ConcurrentResourceLimitsScenario:
    """Example 3 from configuration plan: Concurrent Resource Limits with GrpTRES."""

    def __init__(self, time_engine: TimeEngine, database: SlurmDatabase):
        self.time_engine = time_engine
        self.database = database
        self.usage_simulator = UsageSimulator(time_engine, database)
        self.limits_calculator = PeriodicLimitsCalculator(database, time_engine)
        self.qos_manager = QoSManager(database, time_engine)

        self.account = "concurrent_limits_account"
        self.users = ["simulation_user1", "simulation_user2", "analysis_user1"]

    def setup_scenario(self) -> None:
        """Set up concurrent resource limits configuration."""
        print("⚡ Setting up Concurrent Resource Limits Scenario...")
        print("   Using GrpTRES for simultaneous resource constraints")

        # Create account and users
        self.database.add_account(
            self.account, "Account with concurrent resource limits", "simulation_group"
        )

        for user in self.users:
            self.database.add_user(user, self.account)
            self.database.add_association(user, self.account)

        # Set time to start of quarter
        self.time_engine.set_time(datetime(2024, 1, 1))

        account_obj = self.database.get_account(self.account)
        if account_obj is not None:
            # Apply GrpTRES concurrent limits (not time-based)
            account_obj.limits["GrpTRES:node"] = 10  # Max 10 nodes simultaneously
            account_obj.limits["GrpTRES:CPU"] = 640  # Max 640 CPUs simultaneously
            account_obj.limits["GrpTRES:gres/gpu"] = 8  # Max 8 GPUs simultaneously

            # Also set time-based limits for total consumption
            account_obj.limits["GrpTRESMins:billing"] = 72000  # Total time budget

            # Set fairshare
            account_obj.fairshare = 200

            self.database.save_state()
            print(f"✅ Account '{self.account}' configured with concurrent GrpTRES limits")
            print(
                f"   Concurrent limits: {account_obj.limits['GrpTRES:node']} nodes, "
                f"{account_obj.limits['GrpTRES:CPU']} CPUs, {account_obj.limits['GrpTRES:gres/gpu']} GPUs"
            )
            print(f"   Time budget: {account_obj.limits['GrpTRESMins:billing']} billing-minutes")
        else:
            print(f"❌ Error: Account '{self.account}' not found")

    def simulate_concurrent_jobs(self) -> None:
        """Simulate jobs running concurrently against GrpTRES limits."""
        print("\n🏗️  Simulating concurrent job scheduling...")

        # Job 1: Large simulation (would use 8 nodes = 512 CPUs)
        print("   Job 1 request: 8 nodes × 64 CPUs = 512 CPUs")
        if self._check_concurrent_limit("CPU", 512):
            print("   ✅ Job 1: Approved (within 640 CPU limit)")
            current_cpu_usage = 512
        else:
            print("   ❌ Job 1: Rejected (exceeds CPU limit)")
            current_cpu_usage = 0

        # Job 2: GPU job (would use 4 GPUs)
        print("   Job 2 request: 4 GPUs")
        if self._check_concurrent_limit("gres/gpu", 4):
            print("   ✅ Job 2: Approved (within 8 GPU limit)")
            current_gpu_usage = 4
        else:
            print("   ❌ Job 2: Rejected (exceeds GPU limit)")
            current_gpu_usage = 0

        # Job 3: Additional CPU job (would use 2 nodes = 128 CPUs)
        remaining_cpus = 640 - current_cpu_usage
        print(f"   Job 3 request: 2 nodes × 64 CPUs = 128 CPUs (available: {remaining_cpus})")
        if self._check_concurrent_limit("CPU", current_cpu_usage + 128):
            print("   ✅ Job 3: Approved")
        else:
            print("   ❌ Job 3: Rejected (would exceed concurrent CPU limit)")

        # Show resource utilization
        self._show_resource_utilization(current_cpu_usage, current_gpu_usage)

    def _check_concurrent_limit(self, resource_type: str, requested_amount: int) -> bool:
        """Check if concurrent resource request fits within GrpTRES limits."""
        account_obj = self.database.get_account(self.account)
        if account_obj is None:
            print(f"❌ Error: Account '{self.account}' not found")
            return False

        limit_key = f"GrpTRES:{resource_type}"
        limit = account_obj.limits.get(limit_key, 0)
        return requested_amount <= limit

    def _show_resource_utilization(self, cpu_usage: int, gpu_usage: int) -> None:
        """Show current resource utilization vs limits."""
        print("\n📊 Current concurrent resource utilization:")

        account_obj = self.database.get_account(self.account)
        if account_obj is None:
            print(f"❌ Error: Account '{self.account}' not found")
            return

        cpu_limit = account_obj.limits.get("GrpTRES:CPU", 0)
        gpu_limit = account_obj.limits.get("GrpTRES:gres/gpu", 0)
        node_limit = account_obj.limits.get("GrpTRES:node", 0)

        nodes_used = cpu_usage // 64  # Assuming 64 CPUs per node

        print(f"   CPUs: {cpu_usage}/{cpu_limit} ({(cpu_usage / cpu_limit) * 100:.1f}%)")
        print(f"   GPUs: {gpu_usage}/{gpu_limit} ({(gpu_usage / gpu_limit) * 100:.1f}%)")
        print(f"   Nodes: {nodes_used}/{node_limit} ({(nodes_used / node_limit) * 100:.1f}%)")


class MixedLimitsConfigurationScenario:
    """Advanced scenario combining all limit types."""

    def __init__(self, time_engine: TimeEngine, database: SlurmDatabase):
        self.time_engine = time_engine
        self.database = database
        self.usage_simulator = UsageSimulator(time_engine, database)
        self.limits_calculator = PeriodicLimitsCalculator(database, time_engine)
        self.qos_manager = QoSManager(database, time_engine)

        self.account = "mixed_limits_account"
        self.users = ["power_user1", "regular_user1", "regular_user2", "guest_user1"]

    def setup_scenario(self) -> None:
        """Set up comprehensive mixed limits configuration."""
        print("🎯 Setting up Mixed Limits Configuration Scenario...")
        print("   Combining GrpTRES, GrpTRESMins, and MaxTRESMins")

        # Create account and users
        self.database.add_account(
            self.account, "Advanced account with mixed limit types", "multi_tier_group"
        )

        for user in self.users:
            self.database.add_user(user, self.account)
            self.database.add_association(user, self.account)

        # Set time to start of quarter
        self.time_engine.set_time(datetime(2024, 1, 1))

        account_obj = self.database.get_account(self.account)
        if account_obj is not None:
            # Group concurrent limits (GrpTRES)
            account_obj.limits["GrpTRES:node"] = 20
            account_obj.limits["GrpTRES:CPU"] = 1280
            account_obj.limits["GrpTRES:gres/gpu"] = 16

            # Group time-based limits (GrpTRESMins)
            account_obj.limits["GrpTRESMins:billing"] = 120000  # Large group allocation

            # Individual user time limits (MaxTRESMins)
            account_obj.limits["MaxTRESMins:CPU"] = 86400  # 60 days worth for power users
            account_obj.limits["MaxTRESMins:gres/gpu"] = 14400  # 10 days worth of GPU time

            # Set fairshare
            account_obj.fairshare = 750

            # Configure progressive QoS thresholds (manually set in account metadata)
            account_obj.limits["qos_threshold"] = 96000  # 80% threshold

            self.database.save_state()
            print(f"✅ Account '{self.account}' configured with mixed limit types")
            print("   Concurrent: 20 nodes, 1280 CPUs, 16 GPUs")
            print("   Group time: 120000 billing-minutes")
            print("   User time: 86400 CPU-minutes, 14400 GPU-minutes per user")
        else:
            print(f"❌ Error: Account '{self.account}' not found")

    def run_comprehensive_scenario(self) -> None:
        """Run scenario testing all limit types."""
        print("\n🎭 Running comprehensive multi-limit scenario...")

        # Month 1: Normal operation
        print("\n📅 Month 1: Normal operations")
        self.usage_simulator.inject_usage(self.account, "power_user1", 500, datetime(2024, 1, 31))
        self.usage_simulator.inject_usage(self.account, "regular_user1", 200, datetime(2024, 1, 31))
        self.usage_simulator.inject_usage(self.account, "regular_user2", 150, datetime(2024, 1, 31))

        self._check_all_limits("Month 1")

        # Month 2: Heavy usage
        print("\n📅 Month 2: Heavy usage period")
        self.usage_simulator.inject_usage(self.account, "power_user1", 800, datetime(2024, 2, 29))
        self.usage_simulator.inject_usage(self.account, "regular_user1", 400, datetime(2024, 2, 29))
        self.usage_simulator.inject_usage(self.account, "guest_user1", 100, datetime(2024, 2, 29))

        self._check_all_limits("Month 2")

        # Month 3: Approaching limits
        print("\n📅 Month 3: Approaching various limits")
        self.usage_simulator.inject_usage(self.account, "power_user1", 600, datetime(2024, 3, 31))

        self._check_all_limits("Month 3")
        self._simulate_limit_conflicts()

    def _check_all_limits(self, period: str) -> None:
        """Check status against all limit types."""
        print(f"\n🔍 Limit status check - {period}:")

        account_obj = self.database.get_account(self.account)
        if account_obj is None:
            print(f"❌ Error: Account '{self.account}' not found")
            return

        current_quarter = self.time_engine.get_current_quarter()

        # Check group time limits (GrpTRESMins)
        total_usage = self.database.get_total_usage(self.account, current_quarter)
        billing_minutes_used = total_usage * 60
        group_limit = account_obj.limits.get("GrpTRESMins:billing", 0)
        group_percentage = (billing_minutes_used / group_limit) * 100 if group_limit > 0 else 0

        print(
            f"   Group time usage: {billing_minutes_used}/{group_limit} "
            f"billing-minutes ({group_percentage:.1f}%)"
        )

        # Check individual user limits (MaxTRESMins)
        cpu_limit = account_obj.limits.get("MaxTRESMins:CPU", 0)
        gpu_limit = account_obj.limits.get("MaxTRESMins:gres/gpu", 0)

        for user in self.users:
            user_records = [
                r
                for r in self.database.get_usage_records(
                    account=self.account, period=current_quarter
                )
                if r.user == user
            ]
            user_usage = sum(r.node_hours for r in user_records)
            user_cpu_minutes = user_usage * 60  # Simplified: assume 1 node = 1 CPU

            cpu_percent = (user_cpu_minutes / cpu_limit) * 100 if cpu_limit > 0 else 0

            if user_cpu_minutes > cpu_limit:
                status = "❌ EXCEEDED"
            elif cpu_percent > 80:
                status = "⚠️  HIGH"
            else:
                status = "✅ OK"

            print(
                f"   {user}: {user_cpu_minutes}/{cpu_limit} CPU-minutes "
                f"({cpu_percent:.1f}%) {status}"
            )

    def _simulate_limit_conflicts(self) -> None:
        """Simulate scenarios where different limits conflict."""
        print("\n⚔️  Simulating limit conflict scenarios...")

        print("   Scenario: User has individual capacity but group is near limit")
        print("   - Individual user still has MaxTRESMins capacity")
        print("   - But group GrpTRESMins is at 95% usage")
        print("   - Concurrent GrpTRES allows the job")
        print("   - Result: Job runs but group hits time limit faster")

        print("\n   Scenario: Group has time budget but concurrent limit hit")
        print("   - Group GrpTRESMins has remaining capacity")
        print("   - But all 20 nodes (GrpTRES) are in use")
        print("   - Result: Job queued until nodes become available")

        # Check threshold status
        threshold_status = self.limits_calculator.check_usage_thresholds(self.account)
        if threshold_status.get("over_grace_limit", False) or threshold_status.get(
            "over_qos_threshold", False
        ):
            print(
                f"\n   ⚠️  QoS threshold triggered: {threshold_status.get('recommended_action', 'Check limits')}"
            )
        else:
            print("\n   ✅ All thresholds within normal range")
