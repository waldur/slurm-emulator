"""QoS management for threshold-based switching."""

from typing import Any

from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


class QoSManager:
    """Manages QoS transitions based on usage thresholds."""

    def __init__(self, database: SlurmDatabase, time_engine: TimeEngine):
        self.database = database
        self.time_engine = time_engine
        self.qos_levels = {
            "normal": {"priority_weight": 1000, "description": "Normal priority jobs"},
            "slowdown": {
                "priority_weight": 500000,  # High weight = lower priority
                "description": "Reduced priority for over-threshold usage",
            },
            "blocked": {
                "priority_weight": 1000000,
                "max_submit_jobs": 0,
                "description": "Jobs blocked due to hard limit exceeded",
            },
        }

    def get_account_qos(self, account: str) -> str:
        """Get current QoS for account."""
        account_obj = self.database.get_account(account)
        return account_obj.qos if account_obj else "normal"

    def set_account_qos(self, account: str, qos: str) -> bool:
        """Set QoS for account."""
        if qos not in self.qos_levels:
            return False

        account_obj = self.database.get_account(account)
        if account_obj:
            old_qos = account_obj.qos
            account_obj.qos = qos
            self.database.save_state()

            print(f"🎛️  QoS changed for {account}: {old_qos} → {qos}")
            return True

        return False

    def check_and_update_qos(
        self, account: str, current_usage: float, qos_threshold: float, grace_limit: float
    ) -> dict:
        """Check usage against thresholds and update QoS if needed."""
        current_qos = self.get_account_qos(account)
        new_qos = self._determine_qos_level(current_usage, qos_threshold, grace_limit)

        action_taken = None
        if current_qos != new_qos:
            success = self.set_account_qos(account, new_qos)
            action_taken = f"qos_change:{current_qos}→{new_qos}" if success else "qos_change_failed"

        return {
            "account": account,
            "current_usage": current_usage,
            "qos_threshold": qos_threshold,
            "grace_limit": grace_limit,
            "current_qos": current_qos,
            "new_qos": new_qos,
            "action_taken": action_taken,
            "threshold_status": self._get_threshold_status(
                current_usage, qos_threshold, grace_limit
            ),
        }

    def _determine_qos_level(self, usage: float, qos_threshold: float, grace_limit: float) -> str:
        """Determine appropriate QoS level based on usage."""
        if usage >= grace_limit:
            return "blocked"
        if usage >= qos_threshold:
            return "slowdown"
        return "normal"

    def _get_threshold_status(self, usage: float, qos_threshold: float, grace_limit: float) -> str:
        """Get human-readable threshold status."""
        if usage >= grace_limit:
            return "hard_limit_exceeded"
        if usage >= qos_threshold:
            return "soft_limit_exceeded"
        if usage >= qos_threshold * 0.9:
            return "approaching_threshold"
        return "normal"

    def get_qos_info(self, qos: str) -> dict:
        """Get information about a QoS level."""
        return self.qos_levels.get(qos, {})

    def list_qos_levels(self) -> list[str]:
        """List all available QoS levels."""
        return list(self.qos_levels.keys())

    def simulate_qos_impact(
        self, account: str, projected_usage: float, qos_threshold: float, grace_limit: float
    ) -> dict:
        """Simulate QoS impact for projected usage without applying changes."""
        current_qos = self.get_account_qos(account)
        projected_qos = self._determine_qos_level(projected_usage, qos_threshold, grace_limit)

        # Get account users for affected_users list
        affected_users = self.database.list_account_users(account)

        # Determine impact description and type
        if current_qos == projected_qos:
            impact_description = f"No QoS change needed - remains at {current_qos}"
            impact_type = "no_change"
        elif projected_qos == "blocked":
            impact_description = "Account will be blocked due to usage exceeding grace limit"
            impact_type = "restriction"
        elif projected_qos == "slowdown":
            impact_description = "Account will be slowed down due to usage exceeding threshold"
            impact_type = "restriction"
        else:
            impact_description = f"Account QoS improved from {current_qos} to {projected_qos}"
            impact_type = "improvement"

        return {
            "account": account,
            "impact_description": impact_description,
            "impact_type": impact_type,
            "affected_users": affected_users,
            "current_qos": current_qos,
            "projected_qos": projected_qos,
            "projected_usage": projected_usage,
            "qos_change_needed": current_qos != projected_qos,
            "impact_severity": self._calculate_impact_severity(current_qos, projected_qos),
        }

    def _calculate_impact_severity(self, current_qos: str, projected_qos: str) -> str:
        """Calculate severity of QoS impact."""
        severity_map = {"normal": 0, "slowdown": 1, "blocked": 2}

        current_level = severity_map.get(current_qos, 0)
        projected_level = severity_map.get(projected_qos, 0)

        if projected_level > current_level:
            if projected_qos == "blocked":
                return "critical"
            if projected_qos == "slowdown":
                return "warning"
        elif projected_level < current_level:
            return "improvement"

        return "none"

    def generate_qos_report(self, accounts: list[str]) -> dict[str, Any]:
        """Generate QoS status report for multiple accounts."""
        report = {
            "timestamp": self.time_engine.get_current_time(),
            "period": self.time_engine.get_current_quarter(),
            "accounts": {},
            "summary": {"normal": 0, "slowdown": 0, "blocked": 0},
        }

        current_period = str(report["period"])

        for account in accounts:
            qos = self.get_account_qos(account)
            current_usage = self.database.get_total_usage(account, current_period)

            account_info = {"qos": qos, "usage": current_usage, "qos_info": self.get_qos_info(qos)}

            accounts_dict = report["accounts"]
            if isinstance(accounts_dict, dict):
                accounts_dict[account] = account_info
            summary_dict = report["summary"]
            if isinstance(summary_dict, dict):
                summary_dict[qos] = summary_dict.get(qos, 0) + 1

        return report

    def restore_qos_for_new_period(self, account: str) -> bool:
        """Restore QoS to normal for new period."""
        return self.set_account_qos(account, "normal")
