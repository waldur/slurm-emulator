"""Periodic limits calculations including decay and carryover logic."""

from typing import Any, Optional

from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import UsageSimulator


class PeriodicLimitsCalculator:
    """Calculates periodic limits with decay and carryover logic."""

    def __init__(self, database: SlurmDatabase, time_engine: TimeEngine, slurm_config=None):
        self.database = database
        self.time_engine = time_engine
        self.slurm_config = slurm_config

        # Use SLURM config if available, otherwise defaults
        if slurm_config:
            self.billing_weights = slurm_config.get_tres_billing_weights()
            self.half_life_days = slurm_config.get_decay_half_life_days()
            self.manual_usage_reset = slurm_config.is_manual_usage_reset()
            self.qos_weight = slurm_config.get_qos_weight()
            self.fairshare_weight = slurm_config.get_fairshare_weight()
        else:
            self.billing_weights = {
                "CPU": 0.015625,  # 64 CPUs = 1 billing unit
                "Mem": 0.001953125,  # 512 GB = 1 billing unit (per GB)
                "GRES/gpu": 0.25,  # 4 GPUs = 1 billing unit
            }
            self.half_life_days = 15.0
            self.manual_usage_reset = True
            self.qos_weight = 500000
            self.fairshare_weight = 259200

    def calculate_decay_factor(self, days_elapsed: int, half_life: Optional[float] = None) -> float:
        """Calculate decay factor using half-life formula."""
        if half_life is None:
            half_life = self.half_life_days
        return 2 ** (-days_elapsed / half_life)

    def calculate_fairshare(self, allocation: int, num_accounts: int = 3) -> int:
        """Calculate fairshare value based on allocation."""
        # Simple fairshare calculation: allocation / number of sibling accounts
        return max(1, allocation // num_accounts)

    def calculate_billing_minutes(self, node_hours: float) -> int:
        """Convert node-hours to billing minutes."""
        return int(node_hours * 60)

    def calculate_tres_billing_units(self, tres_usage: dict[str, int]) -> float:
        """Convert raw TRES usage to billing units."""
        billing_units = 0.0

        for tres_type, usage in tres_usage.items():
            if tres_type in self.billing_weights:
                weight = self.billing_weights[tres_type]
                billing_units += usage * weight

        return billing_units

    def calculate_carryover(
        self, account: str, from_period: str, to_period: str
    ) -> tuple[float, dict]:
        """Calculate carryover allocation with decay for period transition."""
        # Get previous period usage
        previous_usage = self.database.get_period_usage(account, from_period)
        base_allocation = self.database.get_account_allocation(account)

        # Calculate days elapsed between periods (use 90 days for quarterly transitions)
        if from_period and to_period:
            # For quarterly transitions, use 90 days (standard quarter duration)
            days_elapsed = 90
        else:
            days_elapsed = 90  # Default to quarter transition (90 days)

        # Calculate decay factor
        decay_factor = self.calculate_decay_factor(days_elapsed)

        # Calculate effective previous usage after decay
        effective_previous_usage = previous_usage * decay_factor

        # Calculate unused allocation (what wasn't consumed, after decay)
        unused_allocation = max(0, base_allocation - effective_previous_usage)

        # New total allocation = base + carryover
        new_total_allocation = base_allocation + unused_allocation

        calculation_details = {
            "previous_usage": previous_usage,
            "base_allocation": base_allocation,
            "days_elapsed": days_elapsed,
            "decay_factor": decay_factor,
            "effective_previous_usage": effective_previous_usage,
            "unused_allocation": unused_allocation,
            "new_total_allocation": new_total_allocation,
        }

        return new_total_allocation, calculation_details

    def calculate_qos_threshold(self, allocation: float, grace_ratio: float = 0.2) -> float:
        """Calculate QoS threshold (when to trigger slowdown) - should be at allocation limit."""
        return allocation  # Slowdown at 100% of allocation

    def calculate_periodic_settings(
        self, account: str, config: Optional[dict[Any, Any]] = None
    ) -> dict:
        """Calculate all periodic settings for an account."""
        if config is None:
            config = {
                "grace_ratio": 0.2,
                "carryover_enabled": True,
                "half_life_days": 15,
                "limit_type": "GrpTRESMins",
            }

        account_obj = self.database.get_account(account)
        if not account_obj:
            raise ValueError(f"Account {account} not found")

        current_period = self.time_engine.get_current_quarter()
        base_allocation = account_obj.allocation

        # Check if this is a period transition or force calculation for testing
        last_period = account_obj.last_period
        force_carryover = config and config.get("force_carryover_calculation", False)

        if (
            last_period and last_period != current_period and config.get("carryover_enabled")
        ) or force_carryover:
            # Calculate carryover - use previous quarter if no last_period set
            from_period = last_period or self._get_previous_quarter(current_period)
            total_allocation, carryover_details = self.calculate_carryover(
                account, from_period, current_period
            )
        else:
            # No carryover, use base allocation
            total_allocation = base_allocation
            carryover_details = {
                "previous_usage": 0,
                "base_allocation": base_allocation,
                "days_elapsed": 0,
                "decay_factor": 1.0,
                "effective_previous_usage": 0,
                "unused_allocation": 0,
                "new_total_allocation": base_allocation,
            }

        # Calculate other settings
        fairshare = self.calculate_fairshare(int(total_allocation))
        billing_minutes = self.calculate_billing_minutes(total_allocation)
        qos_threshold = self.calculate_qos_threshold(
            total_allocation, config.get("grace_ratio", 0.2)
        )

        # Calculate grace limit (hard limit)
        grace_limit = total_allocation * (1.0 + config.get("grace_ratio", 0.2))
        grace_billing_minutes = self.calculate_billing_minutes(grace_limit)

        settings = {
            "account": account,
            "period": current_period,
            "base_allocation": base_allocation,
            "total_allocation": total_allocation,
            "fairshare": fairshare,
            "qos_threshold": qos_threshold,
            "grace_limit": grace_limit,
            "billing_minutes": billing_minutes,
            "grace_billing_minutes": grace_billing_minutes,
            "limit_type": config.get("limit_type", "GrpTRESMins"),
            "carryover_details": carryover_details,
            "timestamp": self.time_engine.get_current_time(),
        }

        return {
            "period": current_period,
            "base_allocation": base_allocation,
            "total_allocation": total_allocation,
            "fairshare": fairshare,
            "qos_threshold": qos_threshold,
            "grace_limit": grace_limit,
            "billing_minutes": billing_minutes,
            "carryover_details": carryover_details,
        }

    def check_usage_thresholds(
        self, account: str, settings: Optional[dict[Any, Any]] = None
    ) -> dict:
        """Check current usage against thresholds and return status."""
        if settings is None:
            settings = self.calculate_periodic_settings(account)

        current_period = self.time_engine.get_current_quarter()
        current_usage = self.database.get_total_usage(account, current_period)

        qos_threshold = settings["qos_threshold"]
        grace_limit = settings["grace_limit"]

        status = {
            "account": account,
            "current_usage": current_usage,
            "qos_threshold": qos_threshold,
            "grace_limit": grace_limit,
            "percentage_used": (current_usage / settings["total_allocation"]) * 100,
            "threshold_status": "normal",
            "recommended_action": None,
        }

        if current_usage >= grace_limit:
            status["threshold_status"] = "blocked"
            status["recommended_action"] = "block_jobs"
        elif current_usage >= qos_threshold:
            status["threshold_status"] = "slowdown"
            status["recommended_action"] = "set_qos_slowdown"
        else:
            status["threshold_status"] = "normal"
            status["recommended_action"] = "set_qos_normal"

        return status

    def apply_period_transition(
        self, account: str, config: Optional[dict[Any, Any]] = None
    ) -> dict:
        """Apply period transition for account."""
        settings = self.calculate_periodic_settings(account, config)

        account_obj = self.database.get_account(account)
        if account_obj:
            # Update account with new period
            account_obj.last_period = settings["period"]

            # Update fairshare
            account_obj.fairshare = settings["fairshare"]

            # Update limits
            limit_key = f"{settings['limit_type']}:billing"
            account_obj.limits[limit_key] = settings["billing_minutes"]

            # Reset QoS to normal for new period
            account_obj.qos = "normal"

        self.database.save_state()

        return {
            "account": account,
            "action": "period_transition",
            "settings_applied": settings,
            "success": True,
        }

    def simulate_usage_scenario(self, account: str, scenario_config: dict) -> dict:
        """Simulate a usage scenario and calculate resulting settings."""
        # Save current state
        original_time = self.time_engine.get_current_time()

        results = []

        try:
            for step in scenario_config.get("steps", []):
                step_type = step.get("type")

                if step_type == "advance_time":
                    days = step.get("days", 0)
                    months = step.get("months", 0)
                    self.time_engine.advance_time(days=days, months=months)

                elif step_type == "inject_usage":
                    user = step.get("user", "testuser")
                    usage = step.get("usage", 0)

                    # Ensure user exists
                    if not self.database.get_user(user):
                        self.database.add_user(user, account)
                    if not self.database.get_association(user, account):
                        self.database.add_association(user, account)

                    # Inject usage
                    simulator = UsageSimulator(self.time_engine, self.database)
                    simulator.inject_usage(account, user, usage)

                elif step_type == "check_thresholds":
                    threshold_status = self.check_usage_thresholds(account)
                    results.append(
                        {
                            "step": step.get("name", "threshold_check"),
                            "type": "threshold_check",
                            "time": self.time_engine.get_current_time(),
                            "status": threshold_status,
                        }
                    )

                elif step_type == "period_transition":
                    transition_result = self.apply_period_transition(account)
                    results.append(
                        {
                            "step": step.get("name", "period_transition"),
                            "type": "period_transition",
                            "time": self.time_engine.get_current_time(),
                            "result": transition_result,
                        }
                    )

            return {
                "scenario": scenario_config.get("name", "unnamed"),
                "results": results,
                "final_time": self.time_engine.get_current_time(),
                "success": True,
            }

        finally:
            # Restore original time if requested
            if scenario_config.get("restore_time", False):
                self.time_engine.set_time(original_time)

    def _get_previous_quarter(self, current_quarter: str) -> str:
        """Get the previous quarter for a given quarter."""
        # Parse "2024-Q2" format
        year_str, q_str = current_quarter.split("-Q")
        year = int(year_str)
        quarter = int(q_str)

        if quarter == 1:
            # Q1 -> previous year Q4
            prev_quarter = 4
            prev_year = year - 1
        else:
            # Q2->Q1, Q3->Q2, Q4->Q3
            prev_quarter = quarter - 1
            prev_year = year

        return f"{prev_year}-Q{prev_quarter}"
