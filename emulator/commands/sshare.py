"""sshare command emulator for account usage and limits."""

from typing import Any

from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine


class SshareEmulator:
    """Emulates the subset of sshare used by Waldur Site Agent."""

    def __init__(self, database: SlurmDatabase, time_engine: TimeEngine):
        self.database = database
        self.time_engine = time_engine

    def handle_command(self, args: list[str]) -> str:
        config = self._parse_args(args)
        accounts = config["accounts"]
        if not accounts:
            accounts = [account.name for account in self.database.list_accounts()]

        lines = []
        for account in accounts:
            if not self.database.get_account(account):
                continue
            lines.append(self._format_account(account, config["format"]))
        return "\n".join(lines)

    def _parse_args(self, args: list[str]) -> dict[str, Any]:
        config: dict[str, Any] = {
            "accounts": [],
            "format": ["Account", "GrpTRESRaw"],
        }

        i = 0
        while i < len(args):
            arg = args[i]
            if arg.startswith("--accounts="):
                config["accounts"] = self._split_csv(arg.split("=", 1)[1])
            elif arg == "--accounts" and i + 1 < len(args):
                config["accounts"] = self._split_csv(args[i + 1])
                i += 1
            elif arg.startswith("--format="):
                config["format"] = self._split_csv(arg.split("=", 1)[1])
            elif arg == "--format" and i + 1 < len(args):
                config["format"] = self._split_csv(args[i + 1])
                i += 1
            i += 1

        return config

    @staticmethod
    def _split_csv(value: str) -> list[str]:
        return [item.strip() for item in value.split(",") if item.strip()]

    def _format_account(self, account: str, format_fields: list[str]) -> str:
        cells = []
        for field in format_fields:
            normalized = field.lower()
            if normalized == "account":
                cells.append(account)
            elif normalized == "grptresraw":
                cells.append(self._format_tres(self._get_raw_usage(account)))
            elif normalized in {"grptresmins", "grptresmin"}:
                cells.append(self._format_tres(self._get_grp_tres_mins(account)))
            else:
                cells.append("")
        return "|".join(cells)

    def _get_current_cluster_records(self, account: str) -> list[UsageRecord]:
        return [
            record
            for record in self.database.usage_records
            if record.account == account and record.cluster == self.database.current_cluster
        ]

    def _get_raw_usage(self, account: str) -> dict[str, int]:
        usage = {"billing": 0, "node": 0, "cpu": 0, "mem": 0, "gres/gpu": 0}

        for record in self._get_current_cluster_records(account):
            usage["billing"] += int(record.billing_units)
            usage["node"] += int(record.node_hours)

            for tres_type, value in record.raw_tres.items():
                normalized = self._normalize_tres_name(tres_type)
                if normalized in usage:
                    usage[normalized] += int(value)

        return {key: value for key, value in usage.items() if value}

    def _get_grp_tres_mins(self, account: str) -> dict[str, int]:
        account_obj = self.database.get_account(account)
        if account_obj is None:
            return {}

        limits = {}
        for key, value in account_obj.limits.items():
            if key == "GrpTRESMins":
                limits["billing"] = int(value)
            elif key.startswith("GrpTRESMins:"):
                tres_name = self._normalize_tres_name(key.split(":", 1)[1])
                limits[tres_name] = int(value)
        return limits

    @staticmethod
    def _normalize_tres_name(tres_type: str) -> str:
        normalized = tres_type.lower()
        if normalized == "cpu":
            return "cpu"
        if normalized in {"mem", "ram"}:
            return "mem"
        if normalized in {"gres/gpu", "gpu"}:
            return "gres/gpu"
        if normalized in {"node", "node-hours", "node_hours"}:
            return "node"
        if normalized == "billing":
            return "billing"
        return normalized

    @staticmethod
    def _format_tres(values: dict[str, int]) -> str:
        order = ["billing", "node", "cpu", "mem", "gres/gpu"]
        parts = [f"{key}={values[key]}" for key in order if key in values]
        extra_keys = sorted(key for key in values if key not in order)
        parts.extend(f"{key}={values[key]}" for key in extra_keys)
        return ",".join(parts)
