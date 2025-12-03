"""sacct command emulator for usage reporting."""

from datetime import datetime
from typing import Any, Optional, Union

from emulator import __version__
from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine


class SacctEmulator:
    """Emulates sacct commands for usage reporting."""

    def __init__(self, database: SlurmDatabase, time_engine: TimeEngine):
        self.database = database
        self.time_engine = time_engine

    def handle_command(self, args: list[str]) -> str:
        """Process sacct command and return output."""
        # Parse command line arguments
        config = self._parse_args(args)

        if config.get("version"):
            return f"slurm-emulator {__version__}"

        # Get usage records based on filters
        records = self._get_filtered_records(config)

        # Format output based on requested format
        return self._format_output(records, config)

    def _parse_args(self, args: list[str]) -> dict[str, Any]:
        """Parse sacct command line arguments."""
        config: dict[str, Union[bool, str, list[Any], Optional[datetime]]] = {
            "accounts": [],
            "users": [],
            "start_time": None,
            "end_time": None,
            "format": "Account,ReqTRES,Elapsed,User",
            "allocations": False,
            "allusers": False,
            "noconvert": False,
            "truncate": False,
            "version": False,
        }

        i = 0
        while i < len(args):
            arg = args[i]

            if arg == "-V":
                config["version"] = True
            elif arg == "--noconvert":
                config["noconvert"] = True
            elif arg == "--truncate":
                config["truncate"] = True
            elif arg == "--allocations":
                config["allocations"] = True
            elif arg == "--allusers":
                config["allusers"] = True
            elif arg.startswith("--starttime="):
                config["start_time"] = self._parse_time(arg.split("=", 1)[1])
            elif arg.startswith("--endtime="):
                config["end_time"] = self._parse_time(arg.split("=", 1)[1])
            elif arg.startswith("--accounts="):
                accounts_str = arg.split("=", 1)[1]
                config["accounts"] = [a.strip() for a in accounts_str.split(",")]
            elif arg.startswith("--users="):
                users_str = arg.split("=", 1)[1]
                config["users"] = [u.strip() for u in users_str.split(",")]
            elif arg.startswith("--format="):
                config["format"] = arg.split("=", 1)[1]
            elif arg == "-a":
                # All jobs (including completed)
                pass
            elif arg.startswith("--account="):
                accounts = config["accounts"]
                if isinstance(accounts, list):
                    accounts.append(arg.split("=", 1)[1])
            elif arg.startswith("--user="):
                users = config["users"]
                if isinstance(users, list):
                    users.append(arg.split("=", 1)[1])

            i += 1

        return config

    def _parse_time(self, time_str: str) -> datetime:
        """Parse time string in various formats."""
        # Handle YYYY-MM-DD format
        if "T" not in time_str and ":" not in time_str:
            return datetime.strptime(time_str, "%Y-%m-%d")
        # Handle YYYY-MM-DDTHH:MM:SS format
        if "T" in time_str:
            return datetime.fromisoformat(time_str)
        # Try other common formats
        try:
            return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return datetime.strptime(time_str, "%Y-%m-%d")

    def _get_filtered_records(self, config: dict[str, Any]) -> list[UsageRecord]:
        """Get usage records based on filters."""
        records = self.database.usage_records.copy()

        # Filter by accounts
        if config["accounts"]:
            records = [r for r in records if r.account in config["accounts"]]

        # Filter by users
        if config["users"]:
            records = [r for r in records if r.user in config["users"]]

        # Filter by time range
        if config["start_time"]:
            records = [r for r in records if r.timestamp >= config["start_time"]]
        if config["end_time"]:
            records = [r for r in records if r.timestamp <= config["end_time"]]

        # If no time range specified, use current month
        if not config["start_time"] and not config["end_time"]:
            month_start, month_end = self.time_engine.format_current_month()
            start_dt = datetime.fromisoformat(month_start)
            end_dt = datetime.fromisoformat(month_end)
            records = [r for r in records if start_dt <= r.timestamp <= end_dt]

        return records

    def _format_output(self, records: list[UsageRecord], config: dict[str, Any]) -> str:
        """Format usage records as sacct output."""
        format_fields = [f.strip() for f in config["format"].split(",")]

        lines = []

        # Group records by account and user for summary
        if config["allocations"]:
            # Allocation mode - summarize by account/user
            summary = {}
            for record in records:
                key = (record.account, record.user)
                if key not in summary:
                    summary[key] = {
                        "account": record.account,
                        "user": record.user,
                        "total_node_hours": 0.0,
                        "raw_tres": {"CPU": 0, "Mem": 0, "GRES/gpu": 0},
                        "elapsed_time": "00:00:00",
                    }
                record_data = summary[key]
                if isinstance(record_data, dict) and "total_node_hours" in record_data:
                    current_hours = record_data["total_node_hours"]
                    if isinstance(current_hours, (int, float)):
                        record_data["total_node_hours"] = current_hours + record.node_hours

                # Sum raw TRES
                for tres_type, value in record.raw_tres.items():
                    raw_tres = record_data.get("raw_tres", {})
                    if isinstance(raw_tres, dict) and tres_type in raw_tres:
                        raw_tres[tres_type] += value

            # Format summary records
            for data in summary.values():
                line_data: list[str] = []
                for field in format_fields:
                    if field == "Account":
                        line_data.append(str(data.get("account", "")))
                    elif field == "User":
                        line_data.append(str(data.get("user", "")))
                    elif field == "ReqTRES":
                        # Format as TRES string - ensure non-empty for site agent parsing
                        tres_parts = []
                        raw_tres = data.get("raw_tres", {})
                        hours_raw = data.get("total_node_hours", 0)
                        hours = float(hours_raw) if isinstance(hours_raw, (int, float, str)) else 0

                        # Always include node-hours component first for site agent compatibility
                        if hours > 0:
                            tres_parts.append(f"node-hours={int(hours)}")

                        # Add other TRES components
                        if isinstance(raw_tres, dict):
                            for tres_type, value in raw_tres.items():
                                if (
                                    value > 0 and tres_type != "node-hours"
                                ):  # Skip node-hours, already added
                                    if tres_type == "GRES/gpu":
                                        tres_parts.append(f"gres/gpu={value}")
                                    else:
                                        tres_parts.append(f"{tres_type.lower()}={value}")

                        line_data.append(",".join(tres_parts))
                    elif field == "Elapsed":
                        # Convert node-hours to elapsed time representation
                        hours_value = data.get("total_node_hours", 0)
                        if isinstance(hours_value, (int, float)):
                            total_hours = float(hours_value)
                        else:
                            total_hours = 0.0
                        hours = int(total_hours)
                        minutes = int((total_hours - hours) * 60)
                        line_data.append(f"{hours:02d}:{minutes:02d}:00")
                    else:
                        line_data.append("")

                lines.append("|".join(line_data))
        else:
            # Job mode - individual records (simulated as jobs)
            for i, record in enumerate(records):
                job_line_data: list[str] = []
                for field in format_fields:
                    if field == "JobID":
                        job_line_data.append(f"job_{i + 1}")
                    elif field == "JobName":
                        job_line_data.append(f"emulated_job_{i + 1}")
                    elif field == "Account":
                        job_line_data.append(record.account)
                    elif field == "User":
                        job_line_data.append(record.user)
                    elif field == "State":
                        job_line_data.append("COMPLETED")
                    elif field == "ReqTRES":
                        # Format as TRES string - ensure non-empty for site agent parsing
                        tres_parts = []
                        raw_tres = record.raw_tres
                        hours = record.node_hours

                        # Always include node-hours component first for site agent compatibility
                        if hours > 0:
                            tres_parts.append(f"node-hours={int(hours)}")

                        # Add other TRES components
                        if isinstance(raw_tres, dict):
                            for tres_type, value in raw_tres.items():
                                if (
                                    value > 0 and tres_type != "node-hours"
                                ):  # Skip node-hours, already added
                                    if tres_type == "GRES/gpu":
                                        tres_parts.append(f"gres/gpu={value}")
                                    else:
                                        tres_parts.append(f"{tres_type.lower()}={value}")

                        job_line_data.append(",".join(tres_parts))
                    elif field == "Elapsed":
                        hours = int(record.node_hours)
                        minutes = int((record.node_hours - hours) * 60)
                        job_line_data.append(f"{hours:02d}:{minutes:02d}:00")
                    elif field == "Timelimit":
                        job_line_data.append("UNLIMITED")
                    elif field == "NodeList":
                        node_count = max(1, int(record.node_hours))
                        job_line_data.append(f"node[001-{node_count:03d}]")
                    else:
                        job_line_data.append("")

                lines.append("|".join(job_line_data))

        return "\n".join(lines)

    def generate_realistic_usage_report(
        self, accounts: list[str], start_time: str, end_time: str
    ) -> str:
        """Generate realistic usage report for testing."""
        # This method can be used to generate test data
        lines = []

        for account in accounts:
            # Get users for this account
            users = self.database.list_account_users(account)
            if not users:
                users = ["testuser1", "testuser2"]  # Default test users

            for user in users:
                # Get actual usage records
                records = self.database.get_usage_records(account=account, user=user)

                if records:
                    # Use real data
                    total_tres = {"CPU": 0, "Mem": 0, "GRES/gpu": 0}
                    total_hours = 0.0

                    for record in records:
                        total_hours += record.node_hours
                        for tres_type, value in record.raw_tres.items():
                            if tres_type in total_tres:
                                total_tres[tres_type] += value

                    # Format TRES string
                    tres_parts = []
                    for tres_type, value in total_tres.items():
                        if value > 0:
                            if tres_type == "GRES/gpu":
                                tres_parts.append(f"gres/gpu={value}")
                            else:
                                tres_parts.append(f"{tres_type.lower()}={value}")

                    tres_str = ",".join(tres_parts)
                    elapsed = f"{int(total_hours):02d}:{int((total_hours % 1) * 60):02d}:00"

                    lines.append(f"{account}|{tres_str}|{elapsed}|{user}")

        return "\n".join(lines)
