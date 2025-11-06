"""In-memory database for SLURM emulator state."""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


@dataclass
class Account:
    """SLURM account representation."""

    name: str
    description: str
    organization: str
    parent: Optional[str] = None
    fairshare: int = 1
    qos: str = "normal"
    limits: dict[str, int] = field(default_factory=dict)
    last_period: Optional[str] = None
    allocation: int = 1000  # Base allocation in node-hours


@dataclass
class User:
    """SLURM user representation."""

    name: str
    default_account: str = ""


@dataclass
class Association:
    """SLURM association between user and account."""

    account: str
    user: str
    limits: dict[str, int] = field(default_factory=dict)


@dataclass
class UsageRecord:
    """Usage record for a user in an account."""

    account: str
    user: str
    node_hours: float
    billing_units: float
    timestamp: datetime
    period: str
    raw_tres: dict[str, int] = field(default_factory=dict)


@dataclass
class Job:
    """SLURM job representation."""

    job_id: str
    account: str
    user: str
    state: str
    node_hours: float = 0.0
    submit_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class SlurmDatabase:
    """In-memory database for SLURM emulator."""

    def __init__(self) -> None:
        self.accounts: dict[str, Account] = {}
        self.users: dict[str, User] = {}
        self.associations: dict[str, Association] = {}  # key: "user:account"
        self.usage_records: list[UsageRecord] = []
        self.jobs: dict[str, Job] = {}
        self.tres_types = ["CPU", "Mem", "GRES/gpu", "billing"]
        self.state_file = Path("/tmp/slurm_emulator_db.json")

        # Create default account
        self.add_account("root", "Root account", "system")

    def add_account(
        self, name: str, description: str, organization: str, parent: Optional[str] = None
    ) -> None:
        """Add account to database."""
        self.accounts[name] = Account(
            name=name, description=description, organization=organization, parent=parent
        )

    def get_account(self, name: str) -> Optional[Account]:
        """Get account by name."""
        return self.accounts.get(name)

    def list_accounts(self) -> list[Account]:
        """List all accounts."""
        return list(self.accounts.values())

    def delete_account(self, name: str) -> None:
        """Delete account."""
        if name in self.accounts:
            del self.accounts[name]

    def add_user(self, name: str, default_account: str = "") -> None:
        """Add user to database."""
        self.users[name] = User(name=name, default_account=default_account)

    def get_user(self, name: str) -> Optional[User]:
        """Get user by name."""
        return self.users.get(name)

    def add_association(
        self, user: str, account: str, limits: Optional[dict[str, int]] = None
    ) -> None:
        """Add user-account association."""
        key = f"{user}:{account}"
        self.associations[key] = Association(account=account, user=user, limits=limits or {})

    def get_association(self, user: str, account: str) -> Optional[Association]:
        """Get association between user and account."""
        key = f"{user}:{account}"
        return self.associations.get(key)

    def list_account_users(self, account: str) -> list[str]:
        """List users associated with account."""
        users = []
        for assoc in self.associations.values():
            if assoc.account == account and assoc.user:
                users.append(assoc.user)
        return users

    def delete_association(self, user: str, account: str) -> None:
        """Delete user-account association."""
        key = f"{user}:{account}"
        if key in self.associations:
            del self.associations[key]

    def add_usage_record(self, record: UsageRecord) -> None:
        """Add usage record."""
        self.usage_records.append(record)

    def get_usage_records(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        period: Optional[str] = None,
    ) -> list[UsageRecord]:
        """Get usage records with optional filtering."""
        records = self.usage_records

        if account:
            records = [r for r in records if r.account == account]
        if user:
            records = [r for r in records if r.user == user]
        if period:
            records = [r for r in records if r.period == period]

        return records

    def get_total_usage(self, account: str, period: Optional[str] = None) -> float:
        """Get total usage for account in period."""
        records = self.get_usage_records(account=account, period=period)
        return sum(r.node_hours for r in records)

    def get_period_usage(self, account: str, period: str) -> float:
        """Get usage for specific period."""
        return self.get_total_usage(account, period)

    def get_account_allocation(self, account: str) -> int:
        """Get base allocation for account."""
        account_obj = self.get_account(account)
        return account_obj.allocation if account_obj else 1000

    def set_account_allocation(self, account: str, allocation: int) -> None:
        """Set base allocation for account."""
        account_obj = self.get_account(account)
        if account_obj:
            account_obj.allocation = allocation

    def reset_raw_usage(self, account: str) -> None:
        """Reset raw usage for account (simulates sacctmgr RawUsage=0)."""
        # In real SLURM this affects fairshare calculations
        # For emulator, we'll mark this in account metadata
        account_obj = self.get_account(account)
        if account_obj:
            account_obj.limits["raw_usage_reset"] = 1

    def add_job(self, job: Job) -> None:
        """Add job to database."""
        self.jobs[job.job_id] = job

    def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID."""
        return self.jobs.get(job_id)

    def list_jobs(self, account: Optional[str] = None, user: Optional[str] = None) -> list[Job]:
        """List jobs with optional filtering."""
        jobs = list(self.jobs.values())

        if account:
            jobs = [j for j in jobs if j.account == account]
        if user:
            jobs = [j for j in jobs if j.user == user]

        return jobs

    def save_state(self) -> None:
        """Save database state to file."""
        state = {
            "accounts": {name: asdict(acc) for name, acc in self.accounts.items()},
            "users": {name: asdict(user) for name, user in self.users.items()},
            "associations": {key: asdict(assoc) for key, assoc in self.associations.items()},
            "usage_records": [self._serialize_usage_record(r) for r in self.usage_records],
            "jobs": {jid: asdict(job) for jid, job in self.jobs.items()},
        }

        try:
            with self.state_file.open("w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            print(f"Warning: Failed to save database state: {e}")

    def load_state(self) -> None:
        """Load database state from file."""
        try:
            if self.state_file.exists():
                with self.state_file.open() as f:
                    state = json.load(f)

                # Load accounts
                self.accounts = {}
                for name, data in state.get("accounts", {}).items():
                    self.accounts[name] = Account(**data)

                # Load users
                self.users = {}
                for name, data in state.get("users", {}).items():
                    self.users[name] = User(**data)

                # Load associations
                self.associations = {}
                for key, data in state.get("associations", {}).items():
                    self.associations[key] = Association(**data)

                # Load usage records
                self.usage_records = []
                for data in state.get("usage_records", []):
                    self.usage_records.append(self._deserialize_usage_record(data))

                # Load jobs
                self.jobs = {}
                for jid, data in state.get("jobs", {}).items():
                    # Handle datetime fields
                    for field in ["submit_time", "start_time", "end_time"]:
                        if data.get(field):
                            data[field] = datetime.fromisoformat(data[field])
                    self.jobs[jid] = Job(**data)

        except Exception as e:
            print(f"Warning: Failed to load database state: {e}")

    def _serialize_usage_record(self, record: UsageRecord) -> dict[str, Any]:
        """Serialize usage record for JSON storage."""
        data = asdict(record)
        data["timestamp"] = record.timestamp.isoformat()
        return data

    def _deserialize_usage_record(self, data: dict[str, Any]) -> UsageRecord:
        """Deserialize usage record from JSON storage."""
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        return UsageRecord(**data)
