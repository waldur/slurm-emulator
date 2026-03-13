"""In-memory database for SLURM emulator state."""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional


class ClusterClassification(str, Enum):
    """SLURM cluster classification types."""

    NONE = ""
    CAPABILITY = "capability"
    CAPACITY = "capacity"
    CAPAPACITY = "capapacity"


@dataclass
class Cluster:
    """SLURM cluster representation."""

    name: str
    control_host: str = "localhost"
    control_port: int = 6817
    classification: ClusterClassification = ClusterClassification.NONE
    deleted: bool = False
    id: int = 0
    rpc_version: int = 9600
    flags: int = 0
    nodes: str = ""
    tres_str: str = ""


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
        self._next_cluster_id: int = 1
        self.clusters: dict[str, Cluster] = {
            "default": Cluster(name="default", id=self._allocate_cluster_id())
        }
        self.current_cluster: str = "default"
        self.accounts: dict[str, Account] = {}
        self.users: dict[str, User] = {}
        self.associations: dict[str, Association] = {}  # key: "user:account:cluster"
        self.usage_records: list[UsageRecord] = []
        self.jobs: dict[str, Job] = {}
        self.tres_types = ["CPU", "Mem", "GRES/gpu", "billing"]
        self.state_file = Path("/tmp/slurm_emulator_db.json")

        # Create global root account and root association for default cluster
        self.add_account("root", "Root account", "system")
        root_key = self._association_key("", "root", "default")
        self.associations[root_key] = Association(account="root", user="", cluster="default")

    def _allocate_cluster_id(self) -> int:
        """Allocate the next cluster ID."""
        cid = self._next_cluster_id
        self._next_cluster_id += 1
        return cid

    # --- Cluster CRUD ---

    def add_cluster(
        self,
        name: str,
        control_host: str = "localhost",
        control_port: int = 6817,
        classification: str = "",
    ) -> None:
        """Add cluster to database."""
        if isinstance(classification, str):
            try:
                cls_enum = ClusterClassification(classification)
            except ValueError:
                cls_enum = ClusterClassification.NONE
        else:
            cls_enum = classification
        self.clusters[name] = Cluster(
            name=name,
            control_host=control_host,
            control_port=control_port,
            classification=cls_enum,
            id=self._allocate_cluster_id(),
        )
        # Ensure root account exists globally
        if "root" not in self.accounts:
            self.add_account("root", "Root account", "system")
        # Create root association for the new cluster
        root_key = self._association_key("", "root", name)
        self.associations[root_key] = Association(account="root", user="", cluster=name)

    def get_cluster(self, name: str) -> Optional[Cluster]:
        """Get cluster by name (excludes soft-deleted)."""
        c = self.clusters.get(name)
        return c if c and not c.deleted else None

    def list_clusters(self) -> list[Cluster]:
        """List all non-deleted clusters."""
        return [c for c in self.clusters.values() if not c.deleted]

    def delete_cluster(self, name: str) -> None:
        """Soft-delete cluster and clean its per-cluster data.

        Raises ValueError if there are running/pending jobs on the cluster.
        Accounts are global and NOT deleted.
        """
        if name == "default":
            return  # Never delete default cluster
        cluster = self.clusters.get(name)
        if cluster is None or cluster.deleted:
            return
        # Check for running/pending jobs
        active_jobs = [
            j for j in self.jobs.values() if j.cluster == name and j.state in ("RUNNING", "PENDING")
        ]
        if active_jobs:
            raise ValueError(
                f"Cannot delete cluster '{name}': {len(active_jobs)} running/pending job(s) exist"
            )
        # Soft-delete
        cluster.deleted = True
        # Clean up per-cluster data (but NOT accounts — they are global)
        self.associations = {k: v for k, v in self.associations.items() if v.cluster != name}
        self.usage_records = [r for r in self.usage_records if r.cluster != name]
        self.jobs = {k: v for k, v in self.jobs.items() if v.cluster != name}
        if self.current_cluster == name:
            self.current_cluster = "default"

    def set_current_cluster(self, name: str) -> bool:
        """Set the current cluster context. Returns True if successful."""
        c = self.clusters.get(name)
        if c and not c.deleted:
            self.current_cluster = name
            return True
        return False

    # --- Account methods (global) ---

    def add_account(
        self,
        name: str,
        description: str,
        organization: str,
        parent: Optional[str] = None,
    ) -> None:
        """Add account to database (global, not per-cluster)."""
        self.accounts[name] = Account(
            name=name,
            description=description,
            organization=organization,
            parent=parent,
        )

    def get_account(self, name: str) -> Optional[Account]:
        """Get account by name (global)."""
        return self.accounts.get(name)

    def list_accounts(self) -> list[Account]:
        """List all accounts (global)."""
        return list(self.accounts.values())

    def delete_account(self, name: str) -> None:
        """Delete account (global)."""
        if name in self.accounts:
            del self.accounts[name]

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
        account_obj = self.get_account(account)
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

        def _serialize_cluster(cl: Cluster) -> dict:
            d = asdict(cl)
            # Serialize enum to string value
            if isinstance(d.get("classification"), ClusterClassification):
                d["classification"] = d["classification"].value
            else:
                d["classification"] = str(d.get("classification", ""))
            return d

        state = {
            "_next_cluster_id": self._next_cluster_id,
            "clusters": {name: _serialize_cluster(cl) for name, cl in self.clusters.items()},
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

                self._next_cluster_id = state.get("_next_cluster_id", 1)

                # Load clusters (new field, migrate if absent)
                if "clusters" in state:
                    self.clusters = {}
                    for name, data in state["clusters"].items():
                        data.setdefault("deleted", False)
                        data.setdefault("id", 0)
                        data.setdefault("rpc_version", 9600)
                        data.setdefault("flags", 0)
                        data.setdefault("nodes", "")
                        data.setdefault("tres_str", "")
                        # Convert classification string to enum
                        cls_val = data.get("classification", "")
                        try:
                            data["classification"] = ClusterClassification(cls_val)
                        except ValueError:
                            data["classification"] = ClusterClassification.NONE
                        self.clusters[name] = Cluster(**data)
                else:
                    self.clusters = {
                        "default": Cluster(name="default", id=self._allocate_cluster_id())
                    }

                self.current_cluster = state.get("current_cluster", "default")

                # Load accounts — handle 3 formats:
                # (a) pre-cluster: plain name keys, no cluster field
                # (b) name@cluster keys with cluster field (old multi-cluster)
                # (c) new: plain name keys without cluster field
                self.accounts = {}
                for key, data in state.get("accounts", {}).items():
                    # Strip cluster field if present (accounts are now global)
                    data.pop("cluster", None)
                    # For name@cluster keys, extract the name
                    if "@" in key:
                        name = key.split("@", 1)[0]
                    else:
                        name = key
                    # Avoid duplicates — first one wins
                    if name not in self.accounts:
                        self.accounts[name] = Account(**data)

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
