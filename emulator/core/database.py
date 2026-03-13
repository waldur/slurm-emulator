"""In-memory database for SLURM emulator state."""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


@dataclass
class Cluster:
    """SLURM cluster representation."""

    name: str
    control_host: str = "localhost"
    control_port: int = 6817
    classification: str = ""


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
    cluster: str = "default"


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
    cluster: str = "default"


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
    cluster: str = "default"


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
    cluster: str = "default"


class SlurmDatabase:
    """In-memory database for SLURM emulator."""

    def __init__(self) -> None:
        self.clusters: dict[str, Cluster] = {"default": Cluster(name="default")}
        self.current_cluster: str = "default"
        self.accounts: dict[str, Account] = {}
        self.users: dict[str, User] = {}
        self.associations: dict[str, Association] = {}  # key: "user:account:cluster"
        self.usage_records: list[UsageRecord] = []
        self.jobs: dict[str, Job] = {}
        self.tres_types = ["CPU", "Mem", "GRES/gpu", "billing"]
        self.state_file = Path("/tmp/slurm_emulator_db.json")

        # Create default account
        self.add_account("root", "Root account", "system")

    # --- Cluster CRUD ---

    def add_cluster(
        self,
        name: str,
        control_host: str = "localhost",
        control_port: int = 6817,
        classification: str = "",
    ) -> None:
        """Add cluster to database."""
        self.clusters[name] = Cluster(
            name=name,
            control_host=control_host,
            control_port=control_port,
            classification=classification,
        )

    def get_cluster(self, name: str) -> Optional[Cluster]:
        """Get cluster by name."""
        return self.clusters.get(name)

    def list_clusters(self) -> list[Cluster]:
        """List all clusters."""
        return list(self.clusters.values())

    def delete_cluster(self, name: str) -> None:
        """Delete cluster and all its per-cluster data."""
        if name == "default":
            return  # Never delete default cluster
        if name in self.clusters:
            del self.clusters[name]
            # Clean up per-cluster data
            self.accounts = {k: v for k, v in self.accounts.items() if v.cluster != name}
            self.associations = {k: v for k, v in self.associations.items() if v.cluster != name}
            self.usage_records = [r for r in self.usage_records if r.cluster != name]
            self.jobs = {k: v for k, v in self.jobs.items() if v.cluster != name}
            if self.current_cluster == name:
                self.current_cluster = "default"

    def set_current_cluster(self, name: str) -> bool:
        """Set the current cluster context. Returns True if successful."""
        if name in self.clusters:
            self.current_cluster = name
            return True
        return False

    # --- Account methods (cluster-aware) ---

    def _account_key(self, name: str, cluster: Optional[str] = None) -> str:
        """Generate unique key for account within a cluster."""
        cl = cluster or self.current_cluster
        return f"{name}@{cl}"

    def add_account(
        self,
        name: str,
        description: str,
        organization: str,
        parent: Optional[str] = None,
        cluster: Optional[str] = None,
    ) -> None:
        """Add account to database."""
        cl = cluster or self.current_cluster
        key = self._account_key(name, cl)
        self.accounts[key] = Account(
            name=name,
            description=description,
            organization=organization,
            parent=parent,
            cluster=cl,
        )

    def get_account(self, name: str, cluster: Optional[str] = None) -> Optional[Account]:
        """Get account by name in the given cluster."""
        cl = cluster or self.current_cluster
        return self.accounts.get(self._account_key(name, cl))

    def list_accounts(self, cluster: Optional[str] = None) -> list[Account]:
        """List all accounts, optionally filtered by cluster."""
        cl = cluster or self.current_cluster
        return [acc for acc in self.accounts.values() if acc.cluster == cl]

    def delete_account(self, name: str, cluster: Optional[str] = None) -> None:
        """Delete account."""
        cl = cluster or self.current_cluster
        key = self._account_key(name, cl)
        if key in self.accounts:
            del self.accounts[key]

    # --- User methods (global, unchanged) ---

    def add_user(self, name: str, default_account: str = "") -> None:
        """Add user to database."""
        self.users[name] = User(name=name, default_account=default_account)

    def get_user(self, name: str) -> Optional[User]:
        """Get user by name."""
        return self.users.get(name)

    # --- Association methods (cluster-aware) ---

    def _association_key(self, user: str, account: str, cluster: Optional[str] = None) -> str:
        """Generate unique key for association."""
        cl = cluster or self.current_cluster
        return f"{user}:{account}:{cl}"

    def add_association(
        self,
        user: str,
        account: str,
        limits: Optional[dict[str, int]] = None,
        cluster: Optional[str] = None,
    ) -> None:
        """Add user-account association."""
        cl = cluster or self.current_cluster
        key = self._association_key(user, account, cl)
        self.associations[key] = Association(
            account=account, user=user, limits=limits or {}, cluster=cl
        )

    def get_association(
        self, user: str, account: str, cluster: Optional[str] = None
    ) -> Optional[Association]:
        """Get association between user and account."""
        cl = cluster or self.current_cluster
        key = self._association_key(user, account, cl)
        return self.associations.get(key)

    def list_account_users(self, account: str, cluster: Optional[str] = None) -> list[str]:
        """List users associated with account."""
        cl = cluster or self.current_cluster
        users = []
        for assoc in self.associations.values():
            if assoc.account == account and assoc.user and assoc.cluster == cl:
                users.append(assoc.user)
        return users

    def delete_association(self, user: str, account: str, cluster: Optional[str] = None) -> None:
        """Delete user-account association."""
        cl = cluster or self.current_cluster
        key = self._association_key(user, account, cl)
        if key in self.associations:
            del self.associations[key]

    # --- Usage record methods (cluster-aware) ---

    def add_usage_record(self, record: UsageRecord) -> None:
        """Add usage record."""
        self.usage_records.append(record)

    def get_usage_records(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        period: Optional[str] = None,
        cluster: Optional[str] = None,
    ) -> list[UsageRecord]:
        """Get usage records with optional filtering."""
        cl = cluster or self.current_cluster
        records = [r for r in self.usage_records if r.cluster == cl]

        if account:
            records = [r for r in records if r.account == account]
        if user:
            records = [r for r in records if r.user == user]
        if period:
            records = [r for r in records if r.period == period]

        return records

    def get_total_usage(
        self, account: str, period: Optional[str] = None, cluster: Optional[str] = None
    ) -> float:
        """Get total usage for account in period."""
        records = self.get_usage_records(account=account, period=period, cluster=cluster)
        return sum(r.node_hours for r in records)

    def get_period_usage(self, account: str, period: str, cluster: Optional[str] = None) -> float:
        """Get usage for specific period."""
        return self.get_total_usage(account, period, cluster=cluster)

    def get_account_allocation(self, account: str, cluster: Optional[str] = None) -> int:
        """Get base allocation for account."""
        account_obj = self.get_account(account, cluster=cluster)
        return account_obj.allocation if account_obj else 1000

    def set_account_allocation(
        self, account: str, allocation: int, cluster: Optional[str] = None
    ) -> None:
        """Set base allocation for account."""
        account_obj = self.get_account(account, cluster=cluster)
        if account_obj:
            account_obj.allocation = allocation

    def reset_raw_usage(self, account: str, cluster: Optional[str] = None) -> None:
        """Reset raw usage for account (simulates sacctmgr RawUsage=0)."""
        account_obj = self.get_account(account, cluster=cluster)
        if account_obj:
            account_obj.limits["raw_usage_reset"] = 1

    # --- Job methods (cluster-aware) ---

    def add_job(self, job: Job) -> None:
        """Add job to database."""
        self.jobs[job.job_id] = job

    def get_job(self, job_id: str) -> Optional[Job]:
        """Get job by ID."""
        return self.jobs.get(job_id)

    def list_jobs(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        cluster: Optional[str] = None,
    ) -> list[Job]:
        """List jobs with optional filtering."""
        cl = cluster or self.current_cluster
        jobs = [j for j in self.jobs.values() if j.cluster == cl]

        if account:
            jobs = [j for j in jobs if j.account == account]
        if user:
            jobs = [j for j in jobs if j.user == user]

        return jobs

    # --- State persistence ---

    def save_state(self) -> None:
        """Save database state to file."""
        state = {
            "clusters": {name: asdict(cl) for name, cl in self.clusters.items()},
            "current_cluster": self.current_cluster,
            "accounts": {key: asdict(acc) for key, acc in self.accounts.items()},
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
        """Load database state from file with backward-compatible migration."""
        try:
            if self.state_file.exists():
                with self.state_file.open() as f:
                    state = json.load(f)

                # Load clusters (new field, migrate if absent)
                if "clusters" in state:
                    self.clusters = {}
                    for name, data in state["clusters"].items():
                        self.clusters[name] = Cluster(**data)
                else:
                    self.clusters = {"default": Cluster(name="default")}

                self.current_cluster = state.get("current_cluster", "default")

                # Load accounts (migrate old format without cluster field)
                self.accounts = {}
                for key, data in state.get("accounts", {}).items():
                    data.setdefault("cluster", "default")
                    acc = Account(**data)
                    # Migrate old keys (plain name) to new format (name@cluster)
                    if "@" not in key:
                        key = f"{acc.name}@{acc.cluster}"
                    self.accounts[key] = acc

                # Load users
                self.users = {}
                for name, data in state.get("users", {}).items():
                    self.users[name] = User(**data)

                # Load associations (migrate old format)
                self.associations = {}
                for key, data in state.get("associations", {}).items():
                    data.setdefault("cluster", "default")
                    assoc = Association(**data)
                    # Migrate old keys ("user:account") to new format ("user:account:cluster")
                    parts = key.split(":")
                    if len(parts) == 2:
                        key = f"{parts[0]}:{parts[1]}:{assoc.cluster}"
                    self.associations[key] = assoc

                # Load usage records
                self.usage_records = []
                for data in state.get("usage_records", []):
                    data.setdefault("cluster", "default")
                    self.usage_records.append(self._deserialize_usage_record(data))

                # Load jobs
                self.jobs = {}
                for jid, data in state.get("jobs", {}).items():
                    data.setdefault("cluster", "default")
                    # Handle datetime fields
                    for dt_field in ["submit_time", "start_time", "end_time"]:
                        if data.get(dt_field):
                            data[dt_field] = datetime.fromisoformat(data[dt_field])
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
