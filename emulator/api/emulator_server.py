"""REST API server for waldur-site-agent integration."""

from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from emulator import __version__
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import UsageSimulator
from emulator.periodic_limits.calculator import PeriodicLimitsCalculator
from emulator.periodic_limits.qos_manager import QoSManager


# Pydantic models for API requests/responses
class PeriodicSettingsRequest(BaseModel):
    resource_id: str
    fairshare: Optional[int] = None
    grp_tres_mins: Optional[dict[str, int]] = None
    max_tres_mins: Optional[dict[str, int]] = None
    grp_tres: Optional[dict[str, int]] = None
    qos_threshold: Optional[dict[str, float]] = None
    qos_default: Optional[str] = "normal"
    qos_slowdown: Optional[str] = "slowdown"
    billing_weights: Optional[dict[str, float]] = None
    reset_raw_usage: Optional[bool] = False


class ResourceActionRequest(BaseModel):
    resource_id: str
    action: str
    qos: Optional[str] = None
    reason: Optional[str] = None


class UsageReportRequest(BaseModel):
    resource_id: str
    usage: dict[str, float]
    billing_period: str
    date: str
    users: Optional[dict[str, dict[str, float]]] = None
    raw_tres_usage: Optional[dict[str, int]] = None


class EmulatorServer:
    """FastAPI server for waldur-site-agent integration."""

    def __init__(self):
        self.app = FastAPI(title="SLURM Emulator API", version=__version__)

        # Initialize emulator components
        self.time_engine = TimeEngine()
        self.database = SlurmDatabase()
        self.usage_simulator = UsageSimulator(self.time_engine, self.database)
        self.limits_calculator = PeriodicLimitsCalculator(self.database, self.time_engine)
        self.qos_manager = QoSManager(self.database, self.time_engine)

        # Load existing state
        self.database.load_state()

        # Setup routes
        self._setup_routes()

    def _setup_routes(self):
        """Setup API routes."""

        @self.app.get("/")
        async def root():
            return {
                "message": "SLURM Emulator API",
                "version": __version__,
                "current_time": self.time_engine.get_current_time(),
                "current_period": self.time_engine.get_current_quarter(),
            }

        @self.app.post("/api/apply-periodic-settings")
        async def apply_periodic_settings(request: PeriodicSettingsRequest):
            """Apply periodic settings to account (from Waldur Mastermind)."""
            try:
                resource_id = request.resource_id

                # Ensure account exists
                if not self.database.get_account(resource_id):
                    self.database.add_account(resource_id, f"Account {resource_id}", "emulator")

                account_obj = self.database.get_account(resource_id)

                # Apply fairshare
                if request.fairshare is not None:
                    account_obj.fairshare = request.fairshare

                # Apply GrpTRESMins limits
                if request.grp_tres_mins:
                    for tres_type, value in request.grp_tres_mins.items():
                        account_obj.limits[f"GrpTRESMins:{tres_type}"] = value

                # Apply MaxTRESMins limits
                if request.max_tres_mins:
                    for tres_type, value in request.max_tres_mins.items():
                        account_obj.limits[f"MaxTRESMins:{tres_type}"] = value

                # Apply GrpTRES limits (concurrent resource limits)
                if request.grp_tres:
                    for tres_type, value in request.grp_tres.items():
                        account_obj.limits[f"GrpTRES:{tres_type}"] = value

                # Reset raw usage if requested
                if request.reset_raw_usage:
                    self.database.reset_raw_usage(resource_id)

                # Update billing weights if provided
                if request.billing_weights:
                    self.usage_simulator.billing_weights.update(request.billing_weights)

                self.database.save_state()

                print(f"🔧 Applied periodic settings to {resource_id}")
                if request.fairshare:
                    print(f"   ⚖️  Fairshare: {request.fairshare}")
                if request.grp_tres_mins:
                    print(f"   🚫 GrpTRESMins: {request.grp_tres_mins}")
                if request.max_tres_mins:
                    print(f"   ⏱️  MaxTRESMins: {request.max_tres_mins}")
                if request.grp_tres:
                    print(f"   🔗 GrpTRES: {request.grp_tres}")
                if request.reset_raw_usage:
                    print("   🔄 Raw usage reset")

                return {
                    "status": "success",
                    "resource_id": resource_id,
                    "message": "Periodic settings applied successfully",
                    "timestamp": self.time_engine.get_current_time(),
                }

            except Exception as e:
                print(f"❌ Error applying periodic settings: {e}")
                raise HTTPException(status_code=500, detail=str(e)) from e

        @self.app.post("/api/downscale-resource")
        async def downscale_resource(request: ResourceActionRequest):
            """Downscale resource (QoS slowdown)."""
            try:
                resource_id = request.resource_id

                if request.action == "set_qos" and request.qos:
                    success = self.qos_manager.set_account_qos(resource_id, request.qos)

                    if success:
                        print(f"🔴 Downscaled {resource_id}: QoS → {request.qos}")
                        if request.reason:
                            print(f"   Reason: {request.reason}")

                        return {
                            "status": "success",
                            "resource_id": resource_id,
                            "action": request.action,
                            "qos": request.qos,
                            "message": f"QoS set to {request.qos}",
                            "timestamp": self.time_engine.get_current_time(),
                        }
                    self._raise_qos_error("Failed to set QoS")
                self._raise_qos_error("Invalid action or missing QoS")

            except Exception as e:
                print(f"❌ Error downscaling resource: {e}")
                raise HTTPException(status_code=500, detail=str(e)) from e

        @self.app.post("/api/restore-resource")
        async def restore_resource(request: ResourceActionRequest):
            """Restore resource (QoS normal)."""
            try:
                resource_id = request.resource_id

                success = self.qos_manager.restore_qos_for_new_period(resource_id)

                if success:
                    print(f"✅ Restored {resource_id}: QoS → normal")

                    return {
                        "status": "success",
                        "resource_id": resource_id,
                        "action": "restore_qos",
                        "qos": "normal",
                        "message": "QoS restored to normal",
                        "timestamp": self.time_engine.get_current_time(),
                    }
                self._raise_qos_error("Failed to restore QoS")

            except Exception as e:
                print(f"❌ Error restoring resource: {e}")
                raise HTTPException(status_code=500, detail=str(e)) from e

        @self.app.post("/api/submit-report")
        async def submit_report(request: UsageReportRequest):
            """Submit usage report (from site agent to Waldur)."""
            try:
                resource_id = request.resource_id

                # Parse billing period
                billing_period = request.billing_period
                period = self._parse_billing_period(billing_period)

                # Inject usage for each user
                if request.users:
                    for user, user_usage in request.users.items():
                        for tres_type, usage_value in user_usage.items():
                            # Convert to node-hours if needed
                            if tres_type == "billing":
                                node_hours = usage_value
                            else:
                                # Convert from raw TRES to billing units
                                weight = self.usage_simulator.billing_weights.get(tres_type, 1.0)
                                node_hours = usage_value * weight

                            self.usage_simulator.inject_usage(
                                resource_id,
                                user,
                                node_hours,
                                datetime.fromisoformat(request.date.replace("Z", "+00:00")),
                            )
                else:
                    # Use aggregate usage data
                    for tres_type, usage_value in request.usage.items():
                        if tres_type == "billing":
                            node_hours = usage_value
                        else:
                            weight = self.usage_simulator.billing_weights.get(tres_type, 1.0)
                            node_hours = usage_value * weight

                        self.usage_simulator.inject_usage(
                            resource_id,
                            "aggregate_user",
                            node_hours,
                            datetime.fromisoformat(request.date.replace("Z", "+00:00")),
                        )

                print(f"📊 Received usage report for {resource_id}")
                print(f"   Period: {billing_period}")
                print(f"   Usage: {request.usage}")

                # Check thresholds after usage update
                threshold_status = self.limits_calculator.check_usage_thresholds(resource_id)

                if (
                    threshold_status["recommended_action"]
                    and threshold_status["recommended_action"] != "set_qos_normal"
                ):
                    print(f"   ⚠️  Threshold check: {threshold_status['threshold_status']}")

                return {
                    "status": "success",
                    "resource_id": resource_id,
                    "message": "Usage report submitted successfully",
                    "threshold_status": threshold_status,
                    "timestamp": self.time_engine.get_current_time(),
                }

            except Exception as e:
                print(f"❌ Error submitting report: {e}")
                raise HTTPException(status_code=500, detail=str(e)) from e

        @self.app.get("/api/status")
        async def get_status():
            """Get emulator status."""
            accounts = self.database.list_accounts()
            account_status = {}

            for account in accounts:
                if account.name == "root":
                    continue

                usage = self.database.get_total_usage(
                    account.name, self.time_engine.get_current_quarter()
                )

                account_status[account.name] = {
                    "allocation": account.allocation,
                    "usage": usage,
                    "qos": account.qos,
                    "fairshare": account.fairshare,
                    "limits": account.limits,
                }

            return {
                "status": "running",
                "current_time": self.time_engine.get_current_time(),
                "current_period": self.time_engine.get_current_quarter(),
                "accounts": account_status,
            }

        @self.app.post("/api/time/advance")
        async def advance_time(days: int = 0, months: int = 0, quarters: int = 0):
            """Advance emulator time."""
            old_time = self.time_engine.get_current_time()
            old_period = self.time_engine.get_current_quarter()

            self.time_engine.advance_time(days=days, months=months, quarters=quarters)

            new_time = self.time_engine.get_current_time()
            new_period = self.time_engine.get_current_quarter()

            print(f"⏭️  Time advanced: {old_time} → {new_time}")
            if old_period != new_period:
                print(f"📅 Period transition: {old_period} → {new_period}")

            return {
                "status": "success",
                "old_time": old_time,
                "new_time": new_time,
                "old_period": old_period,
                "new_period": new_period,
            }

    def _parse_billing_period(self, billing_period: str) -> str:
        """Parse billing period string to quarter format."""
        # Handle various formats: "2024-01", "2024-01-01", etc.
        if len(billing_period) == 7:  # "2024-01"
            year, month = billing_period.split("-")
            quarter = (int(month) - 1) // 3 + 1
            return f"{year}-Q{quarter}"
        if len(billing_period) >= 10:  # "2024-01-01T..."
            date_part = billing_period[:10]
            year, month, _ = date_part.split("-")
            quarter = (int(month) - 1) // 3 + 1
            return f"{year}-Q{quarter}"
        return billing_period

    def _raise_qos_error(self, message: str) -> None:
        """Raise QoS-related HTTP exception."""
        raise HTTPException(status_code=400, detail=message)


def create_app() -> FastAPI:
    """Create FastAPI application."""
    server = EmulatorServer()
    return server.app


# For running with uvicorn
app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("emulator_server:app", host="0.0.0.0", port=8080, reload=True)
