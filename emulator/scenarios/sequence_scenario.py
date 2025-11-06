"""Implementation of the SLURM_PERIODIC_LIMITS_SEQUENCE.md scenario."""

from datetime import datetime
from typing import Any

from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import UsageSimulator
from emulator.periodic_limits.calculator import PeriodicLimitsCalculator
from emulator.periodic_limits.qos_manager import QoSManager


class SequenceScenario:
    """Implements the complete sequence scenario from the markdown file."""

    def __init__(self, time_engine: TimeEngine, database: SlurmDatabase):
        self.time_engine = time_engine
        self.database = database
        self.usage_simulator = UsageSimulator(time_engine, database)
        self.limits_calculator = PeriodicLimitsCalculator(database, time_engine)
        self.qos_manager = QoSManager(database, time_engine)

        # Scenario configuration
        self.account = "slurm_account_123"
        self.users = ["user1", "user2"]
        self.base_allocation = 1000  # node-hours per quarter
        self.grace_ratio = 0.2  # 20% overconsumption allowed

        self.steps: list[str] = []
        self.checkpoints: dict[str, Any] = {}

    def setup_scenario(self) -> None:
        """Set up initial scenario state."""
        print("🎬 Setting up sequence scenario...")

        # Create account and users
        self.database.add_account(self.account, "Test account for sequence scenario", "emulator")

        for user in self.users:
            self.database.add_user(user, self.account)
            self.database.add_association(user, self.account)

        # Set allocation
        self.database.set_account_allocation(self.account, self.base_allocation)

        # Reset to start time
        self.time_engine.set_time(datetime(2024, 1, 1))

        print(
            f"✅ Scenario setup complete: {self.account} with {self.base_allocation}Nh allocation"
        )

    def run_complete_scenario(self, interactive: bool = False) -> dict:
        """Run the complete sequence scenario."""
        print("\n🎬 Starting SLURM Periodic Limits Sequence Scenario")
        print("=" * 60)

        self.setup_scenario()
        results = []

        try:
            # Step 1: Initial Q1 setup
            result = self._step_1_initial_setup(interactive)
            results.append(result)

            # Step 2-4: Q1 usage (3 months)
            result = self._step_2_q1_usage(interactive)
            results.append(result)

            # Step 5: Q2 transition with carryover
            result = self._step_5_q2_transition(interactive)
            results.append(result)

            # Step 6: Q2 heavy usage reaching threshold
            result = self._step_6_q2_heavy_usage(interactive)
            results.append(result)

            # Step 7: Allocation increase
            result = self._step_7_allocation_increase(interactive)
            results.append(result)

            # Step 8: Hard limit test
            result = self._step_8_hard_limit_test(interactive)
            results.append(result)

            # Step 9: Q3 transition with decay
            result = self._step_9_q3_transition_with_decay(interactive)
            results.append(result)

            print("\n✅ Sequence scenario completed successfully!")

            return {
                "scenario": "sequence_scenario",
                "status": "completed",
                "steps": results,
                "final_time": self.time_engine.get_current_time(),
                "summary": self._generate_summary(results),
            }

        except Exception as e:
            print(f"\n❌ Scenario failed: {e}")
            return {
                "scenario": "sequence_scenario",
                "status": "failed",
                "error": str(e),
                "steps": results,
            }

    def _step_1_initial_setup(self, interactive: bool) -> dict:
        """Step 1: Initial Quarter 1 setup."""
        if interactive:
            input("\n⏸️  Press Enter to execute Step 1: Initial Q1 setup...")

        print("\n📍 Step 1: Initial Q1 2024 Setup")
        print("   Setting up 1000Nh quarterly allocation with 20% grace period")

        # Set time to Q1 start
        self.time_engine.set_time(datetime(2024, 1, 1))

        # Apply initial periodic settings
        settings = self.limits_calculator.calculate_periodic_settings(
            self.account,
            {"grace_ratio": self.grace_ratio, "carryover_enabled": True, "half_life_days": 15},
        )

        # Apply settings to account
        account_obj = self.database.get_account(self.account)
        if account_obj is not None:
            account_obj.fairshare = settings["fairshare"]
            account_obj.limits["GrpTRESMins:billing"] = settings["billing_minutes"]
            account_obj.qos = "normal"
            account_obj.last_period = settings["period"]

        print(f"   ⚖️  Set fairshare to {settings['fairshare']}")
        print(f"   🚫 Set GrpTRESMins to {settings['billing_minutes']} billing-minutes")
        print(f"   🎯 QoS threshold set to {settings['qos_threshold']:.1f}Nh")

        self._create_checkpoint("initial_setup")

        return {
            "step": 1,
            "name": "initial_setup",
            "time": self.time_engine.get_current_time(),
            "settings": settings,
            "status": "completed",
        }

    def _step_2_q1_usage(self, interactive: bool) -> dict:
        """Steps 2-4: Q1 usage over 3 months (500Nh total)."""
        if interactive:
            input("\n⏸️  Press Enter to execute Steps 2-4: Q1 usage simulation...")

        print("\n📍 Steps 2-4: Q1 Usage Simulation")
        print("   Simulating 500Nh usage over 3 months")

        # Month 1: 167Nh
        self.time_engine.set_time(datetime(2024, 1, 31))
        self.usage_simulator.inject_usage(self.account, "user1", 100)
        self.usage_simulator.inject_usage(self.account, "user2", 67)

        month1_usage = self.database.get_total_usage(self.account, "2024-Q1")
        print(f"   📊 Month 1: {month1_usage}Nh")

        # Month 2: 167Nh more
        self.time_engine.set_time(datetime(2024, 2, 29))
        self.usage_simulator.inject_usage(self.account, "user1", 100)
        self.usage_simulator.inject_usage(self.account, "user2", 67)

        month2_usage = self.database.get_total_usage(self.account, "2024-Q1")
        print(f"   📊 Month 2: {month2_usage}Nh total")

        # Month 3: 166Nh more (total 500Nh)
        self.time_engine.set_time(datetime(2024, 3, 31))
        self.usage_simulator.inject_usage(self.account, "user1", 100)
        self.usage_simulator.inject_usage(self.account, "user2", 66)

        q1_total = self.database.get_total_usage(self.account, "2024-Q1")
        print(f"   📊 Q1 final: {q1_total}Nh")

        # Check QoS status
        qos_status = self.qos_manager.check_and_update_qos(self.account, q1_total, 1000, 1200)
        print(f"   ✅ QoS status: {qos_status['current_qos']}")

        self._create_checkpoint("q1_usage_complete")

        return {
            "step": "2-4",
            "name": "q1_usage",
            "time": self.time_engine.get_current_time(),
            "q1_usage": q1_total,
            "qos_status": qos_status,
            "status": "completed",
        }

    def _step_5_q2_transition(self, interactive: bool) -> dict:
        """Step 5: Q2 transition with carryover calculation."""
        if interactive:
            input("\n⏸️  Press Enter to execute Step 5: Q2 transition...")

        print("\n📍 Step 5: Q2 Transition with Carryover")

        # Advance to Q2
        self.time_engine.set_time(datetime(2024, 4, 1))

        # Calculate carryover
        total_allocation, carryover_details = self.limits_calculator.calculate_carryover(
            self.account, "2024-Q1", "2024-Q2"
        )

        print(f"   📊 Previous Q1 usage: {carryover_details['previous_usage']}Nh")
        print(f"   💰 Base allocation: {carryover_details['base_allocation']}Nh")
        print(f"   📉 Decay factor: {carryover_details['decay_factor']:.4f}")
        print(f"   🎁 Carryover: {carryover_details['unused_allocation']:.1f}Nh")
        print(f"   🎯 New total allocation: {total_allocation:.1f}Nh")

        # Apply new settings
        settings = self.limits_calculator.calculate_periodic_settings(self.account)
        account_obj = self.database.get_account(self.account)
        if account_obj is not None:
            account_obj.limits["GrpTRESMins:billing"] = settings["billing_minutes"]
            account_obj.last_period = "2024-Q2"

        print(f"   🚫 Updated GrpTRESMins to {settings['billing_minutes']} billing-minutes")
        print(f"   🎯 New QoS threshold: {settings['qos_threshold']:.1f}Nh")

        self._create_checkpoint("q2_transition")

        return {
            "step": 5,
            "name": "q2_transition",
            "time": self.time_engine.get_current_time(),
            "carryover_details": carryover_details,
            "new_settings": settings,
            "status": "completed",
        }

    def _step_6_q2_heavy_usage(self, interactive: bool) -> dict:
        """Step 6: Q2 heavy usage reaching threshold."""
        if interactive:
            input("\n⏸️  Press Enter to execute Step 6: Q2 heavy usage...")

        print("\n📍 Step 6: Q2 Heavy Usage - Threshold Testing")

        # Q2 Month 1: 500Nh
        self.time_engine.set_time(datetime(2024, 4, 30))
        self.usage_simulator.inject_usage(self.account, "user1", 300)
        self.usage_simulator.inject_usage(self.account, "user2", 200)

        month1_usage = self.database.get_total_usage(self.account, "2024-Q2")
        print(f"   📊 Q2 Month 1: {month1_usage}Nh")

        # Q2 Month 2: 500Nh more (1000Nh total, reaching threshold)
        self.time_engine.set_time(datetime(2024, 5, 20))
        self.usage_simulator.inject_usage(self.account, "user1", 300)
        self.usage_simulator.inject_usage(self.account, "user2", 200)

        # Check threshold with carryover considered
        current_settings = self.limits_calculator.calculate_periodic_settings(self.account)
        q2_usage = self.database.get_total_usage(self.account, "2024-Q2")

        print(f"   📊 Q2 Usage so far: {q2_usage}Nh")
        print(f"   🎯 Current threshold: {current_settings['qos_threshold']:.1f}Nh")

        # Check and update QoS
        qos_status = self.qos_manager.check_and_update_qos(
            self.account,
            q2_usage,
            current_settings["qos_threshold"],
            current_settings["grace_limit"],
        )

        if qos_status["action_taken"]:
            print(f"   🔴 {qos_status['action_taken']}")

        # Additional usage: 200Nh more
        self.time_engine.advance_time(days=5)
        self.usage_simulator.inject_usage(self.account, "user1", 200)

        final_q2_usage = self.database.get_total_usage(self.account, "2024-Q2")
        print(f"   📊 Q2 Updated usage: {final_q2_usage}Nh")

        self._create_checkpoint("q2_heavy_usage")

        return {
            "step": 6,
            "name": "q2_heavy_usage",
            "time": self.time_engine.get_current_time(),
            "q2_usage": final_q2_usage,
            "qos_status": qos_status,
            "threshold_exceeded": final_q2_usage >= current_settings["qos_threshold"],
            "status": "completed",
        }

    def _step_7_allocation_increase(self, interactive: bool) -> dict:
        """Step 7: Manual allocation increase by admin."""
        if interactive:
            input("\n⏸️  Press Enter to execute Step 7: Allocation increase...")

        print("\n📍 Step 7: Admin Allocation Increase")
        print("   Simulating admin increasing allocation from 1000Nh to 1250Nh")

        # Update base allocation
        old_allocation = self.database.get_account_allocation(self.account)
        self.database.set_account_allocation(self.account, 1250)

        # Recalculate settings with new allocation
        settings = self.limits_calculator.calculate_periodic_settings(self.account)

        # Apply new settings
        account_obj = self.database.get_account(self.account)
        if account_obj is not None:
            account_obj.fairshare = settings["fairshare"]
            account_obj.limits["GrpTRESMins:billing"] = settings["billing_minutes"]

        print(f"   💰 Allocation: {old_allocation}Nh → {settings['total_allocation']:.1f}Nh")
        print(f"   ⚖️  Fairshare: {settings['fairshare']}")
        print(f"   🚫 GrpTRESMins: {settings['billing_minutes']} billing-minutes")
        print(f"   🎯 New threshold: {settings['qos_threshold']:.1f}Nh")

        # Check if QoS should be restored
        current_usage = self.database.get_total_usage(self.account, "2024-Q2")
        qos_status = self.qos_manager.check_and_update_qos(
            self.account, current_usage, settings["qos_threshold"], settings["grace_limit"]
        )

        if qos_status["action_taken"]:
            print(f"   ✅ {qos_status['action_taken']}")

        self._create_checkpoint("allocation_increased")

        return {
            "step": 7,
            "name": "allocation_increase",
            "time": self.time_engine.get_current_time(),
            "old_allocation": old_allocation,
            "new_allocation": 1250,
            "new_settings": settings,
            "qos_status": qos_status,
            "status": "completed",
        }

    def _step_8_hard_limit_test(self, interactive: bool) -> dict:
        """Step 8: Push to hard limit (2000Nh total)."""
        if interactive:
            input("\n⏸️  Press Enter to execute Step 8: Hard limit test...")

        print("\n📍 Step 8: Hard Limit Testing")
        print("   Pushing usage to hard limit")

        # Add more usage to approach hard limit
        self.time_engine.advance_time(days=10)
        self.usage_simulator.inject_usage(self.account, "user1", 250)

        current_usage = self.database.get_total_usage(self.account, "2024-Q2")
        print(f"   📊 Current usage: {current_usage}Nh")

        # Final push to hit hard limit
        self.time_engine.advance_time(days=5)
        self.usage_simulator.inject_usage(self.account, "user1", 250)

        final_usage = self.database.get_total_usage(self.account, "2024-Q2")

        # Check against hard limit
        current_settings = self.limits_calculator.calculate_periodic_settings(self.account)
        qos_status = self.qos_manager.check_and_update_qos(
            self.account,
            final_usage,
            current_settings["qos_threshold"],
            current_settings["grace_limit"],
        )

        print(f"   📊 Final Q2 usage: {final_usage}Nh")
        print(f"   🚫 Grace limit: {current_settings['grace_limit']:.1f}Nh")
        print(f"   🚨 Hard limit exceeded: {final_usage >= current_settings['grace_limit']}")

        if qos_status["action_taken"]:
            print(f"   🔴 {qos_status['action_taken']}")

        self._create_checkpoint("hard_limit_reached")

        return {
            "step": 8,
            "name": "hard_limit_test",
            "time": self.time_engine.get_current_time(),
            "final_usage": final_usage,
            "grace_limit": current_settings["grace_limit"],
            "hard_limit_exceeded": final_usage >= current_settings["grace_limit"],
            "qos_status": qos_status,
            "status": "completed",
        }

    def _step_9_q3_transition_with_decay(self, interactive: bool) -> dict:
        """Step 9: Q3 transition with 15-day decay."""
        if interactive:
            input("\n⏸️  Press Enter to execute Step 9: Q3 transition with decay...")

        print("\n📍 Step 9: Q3 Transition with 15-day Decay")

        # Advance to Q3
        self.time_engine.set_time(datetime(2024, 7, 1))

        # Reset allocation to base (admin increase was temporary for Q2)
        self.database.set_account_allocation(self.account, 1000)

        # Calculate carryover with decay
        total_allocation, carryover_details = self.limits_calculator.calculate_carryover(
            self.account, "2024-Q2", "2024-Q3"
        )

        print(f"   📊 Q2 final usage: {carryover_details['previous_usage']}Nh")
        print(f"   ⏱️  Days elapsed: {carryover_details['days_elapsed']}")
        print(f"   📉 Decay factor (15-day half-life): {carryover_details['decay_factor']:.4f}")
        print(
            f"   🔄 Effective previous usage: {carryover_details['effective_previous_usage']:.1f}Nh"
        )
        print(f"   🎁 Carryover: {carryover_details['unused_allocation']:.1f}Nh")
        print(f"   🎯 Q3 total allocation: {total_allocation:.1f}Nh")

        # Apply Q3 settings
        settings = self.limits_calculator.calculate_periodic_settings(self.account)
        account_obj = self.database.get_account(self.account)
        if account_obj is not None:
            account_obj.fairshare = settings["fairshare"]
            account_obj.limits["GrpTRESMins:billing"] = settings["billing_minutes"]
            account_obj.last_period = "2024-Q3"

        # Reset QoS and raw usage for new period
        self.qos_manager.restore_qos_for_new_period(self.account)
        self.database.reset_raw_usage(self.account)

        print(f"   ⚖️  Reset fairshare to {settings['fairshare']}")
        print(f"   🚫 Reset GrpTRESMins to {settings['billing_minutes']} billing-minutes")
        print("   ✅ QoS restored to normal")
        print("   🔄 Raw usage reset for clean start")

        self._create_checkpoint("q3_transition_complete")

        return {
            "step": 9,
            "name": "q3_transition_with_decay",
            "time": self.time_engine.get_current_time(),
            "carryover_details": carryover_details,
            "q3_settings": settings,
            "status": "completed",
        }

    def _create_checkpoint(self, name: str) -> None:
        """Create a checkpoint for state restoration."""
        self.checkpoints[name] = {
            "time": self.time_engine.get_current_time(),
            "created_at": datetime.now(),
        }
        print(f"   💾 Checkpoint '{name}' created")

    def _generate_summary(self, results: list[dict]) -> dict:
        """Generate summary of scenario execution."""
        return {
            "total_steps": len(results),
            "completed_steps": len([r for r in results if r.get("status") == "completed"]),
            "q1_usage": next((r.get("q1_usage") for r in results if "q1_usage" in r), 0),
            "q2_usage": next((r.get("final_usage") for r in results if "final_usage" in r), 0),
            "hard_limit_exceeded": any(r.get("hard_limit_exceeded", False) for r in results),
            "checkpoints_created": list(self.checkpoints.keys()),
        }

    def get_scenario_definition(self) -> dict:
        """Get the scenario definition for external execution."""
        return {
            "name": "SLURM Periodic Limits Sequence",
            "description": "Complete sequence scenario from SLURM_PERIODIC_LIMITS_SEQUENCE.md",
            "account": self.account,
            "users": self.users,
            "base_allocation": self.base_allocation,
            "grace_ratio": self.grace_ratio,
            "steps": [
                {"name": "initial_setup", "description": "Q1 2024 setup with 1000Nh allocation"},
                {"name": "q1_usage", "description": "Q1 usage simulation (500Nh over 3 months)"},
                {
                    "name": "q2_transition",
                    "description": "Q2 transition with carryover calculation",
                },
                {"name": "q2_heavy_usage", "description": "Q2 heavy usage reaching threshold"},
                {
                    "name": "allocation_increase",
                    "description": "Admin increases allocation to 1250Nh",
                },
                {"name": "hard_limit_test", "description": "Push to hard limit (2000Nh total)"},
                {
                    "name": "q3_transition_with_decay",
                    "description": "Q3 transition with 15-day decay",
                },
            ],
        }
