"""In-memory database for SLURM emulator state."""

import fcntl
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional, overload


@overload
def fold_account(name: str) -> str: ...


@overload
def fold_account(name: None) -> None: ...


def fold_account(name: Optional[str]) -> Optional[str]:
    """Canonicalise an account name to lower case, as real Slurm does.

    sacctmgr parses every account name and account condition through
    ``slurm_addto_char_list`` (account_functions.c:113 for the
    ``name=``/``account=`` condition, :204 for the added account), which
    is ``slurm_addto_char_list_with_case(..., true)`` and calls
    ``xstrtolower(name)`` before storing it (slurm_protocol_defs.c:523-525,
    537-539). So ``sacctmgr add account 2026_00A`` is stored and reported
    as ``2026_00a`` and a mixed-case query still matches. Every account-name
    key and lookup in this emulator routes through here to reproduce that.
    ``None`` and ``""`` pass through unchanged (blank parent / no filter).
    """
    return name.lower() if name else name


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
    default_qos: str = ""
    limits: dict[str, int] = field(default_factory=dict)
    last_period: Optional[str] = None
    allocation: int = 1000  # Base allocation in node-hours

    def __post_init__(self) -> None:
        """Fold the (case-insensitive) account and parent names to lower case.

        Canonicalise on construction so every code path — direct
        construction, state reload, command handlers — sees the same stored
        form real Slurm keeps.
        """
        self.name = fold_account(self.name)
        self.parent = fold_account(self.parent)


@dataclass
class User:
    """SLURM user representation."""

    name: str
    default_account: str = ""


@dataclass
class QOS:
    """SLURM Quality of Service."""

    name: str
    flags: str = ""
    grp_tres: str = ""
    max_jobs: int = -1
    max_submit: int = -1
    max_wall: str = ""
    min_tres_per_job: str = ""


@dataclass
class Association:
    """SLURM association between user and account.

    Mirrors slurmdb_assoc_rec_t: an association is keyed by
    (cluster, account, user, partition). ``Partitions=p1,p2`` on
    ``sacctmgr add user`` produces one Association per partition,
    each with a single ``partition`` string. ``partition=None`` is
    the "non-partition" association.
    """

    account: str
    user: str
    limits: dict[str, int] = field(default_factory=dict)
    cluster: str = "default"
    partition: Optional[str] = None
    # parent_acct is stored on the account-level association (user == "").
    # User associations leave it unset, matching real Slurm where
    # ``assoc->parent_acct`` is NULL for user rows (see
    # as_mysql_assoc.c:2116-2126) so ParentName prints blank for them.
    parent: Optional[str] = None

    def __post_init__(self) -> None:
        """Fold the account and parent account names to lower case.

        The account (and parent account) an association points at are
        case-insensitive; the user name is not, so only the account names
        are folded — a mixed-case account then matches its stored row.
        """
        self.account = fold_account(self.account)
        self.parent = fold_account(self.parent)


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
    # Numeric job id (real sacct JobIDs are numeric); assigned by the
    # database when None so direct construction stays backward compatible.
    job_id: Optional[int] = None
    state: str = "COMPLETED"


@dataclass
class Job:
    """SLURM job representation.

    The trailing fields carry what a slurmrestd ``POST /job/submit`` body
    provides (name, partition, script, working directory, …) so a job
    submitted over REST round-trips through the active-job view. They all
    default, so older state files and direct constructions stay valid.
    """

    job_id: str
    account: str
    user: str
    state: str
    node_hours: float = 0.0
    submit_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    cluster: str = "default"
    name: str = ""
    partition: str = "compute"
    qos: str = "normal"
    working_directory: str = ""
    script: str = ""
    standard_output: str = ""
    standard_error: str = ""
    standard_input: str = "/dev/null"
    node_count: int = 1
    priority: int = 1
    # Wall-clock minutes; ``None`` renders as UNLIMITED / infinite.
    time_limit: Optional[int] = None
    environment: dict[str, str] = field(default_factory=dict)
    constraints: str = ""


class SlurmDatabase:
    """In-memory database for SLURM emulator."""

    def __init__(self) -> None:
        self._next_cluster_id: int = 1
        self._next_job_id: int = 1
        self.clusters: dict[str, Cluster] = {
            "default": Cluster(name="default", id=self._allocate_cluster_id())
        }
        self.current_cluster: str = "default"
        self.accounts: dict[str, Account] = {}
        self.users: dict[str, User] = {}
        self.associations: dict[str, Association] = {}  # key: "user:account:cluster"
        self.usage_records: list[UsageRecord] = []
        self.jobs: dict[str, Job] = {}
        self.qos_list: dict[str, QOS] = {}
        self.tres_types = ["CPU", "Mem", "GRES/gpu", "billing"]
        self.state_file = Path(
            os.environ.get("SLURM_EMULATOR_STATE_FILE", "/tmp/slurm_emulator_db.json")
        )

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
        """Add account to database.

        The account record is global, but — as in real Slurm — every account
        also has an account-level association (``user == ""``) on a cluster that
        carries its ``parent_acct``. We create that row on the current cluster so
        ``show assoc`` / ``show account withassoc`` can report the parent.
        """
        name = fold_account(name)
        self.accounts[name] = Account(
            name=name,
            description=description,
            organization=organization,
            parent=parent,
        )
        key = self._association_key("", name, self.current_cluster)
        self.associations[key] = Association(
            account=name, user="", cluster=self.current_cluster, parent=parent
        )

    def set_account_parent(
        self, name: str, parent: Optional[str], cluster: Optional[str] = None
    ) -> None:
        """Reparent an account: update the record and its account-level association."""
        cl = cluster or self.current_cluster
        name = fold_account(name)
        parent = fold_account(parent)
        account = self.accounts.get(name)
        if account is not None:
            account.parent = parent
        key = self._association_key("", name, cl)
        assoc = self.associations.get(key)
        if assoc is not None:
            assoc.parent = parent

    def get_account(self, name: str) -> Optional[Account]:
        """Get account by name (global, case-insensitive)."""
        return self.accounts.get(fold_account(name))

    def list_accounts(self) -> list[Account]:
        """List all accounts (global)."""
        return list(self.accounts.values())

    def delete_account(self, name: str) -> None:
        """Delete account (global, case-insensitive)."""
        name = fold_account(name)
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

    def _association_key(
        self,
        user: str,
        account: str,
        cluster: Optional[str] = None,
        partition: Optional[str] = None,
    ) -> str:
        """Generate unique key for association.

        The account component is folded to lower case so account-name case
        never splits a row across two keys — the user name is left as-is.
        """
        cl = cluster or self.current_cluster
        return f"{user}:{fold_account(account)}:{cl}:{partition or ''}"

    def add_association(
        self,
        user: str,
        account: str,
        limits: Optional[dict[str, int]] = None,
        cluster: Optional[str] = None,
        partition: Optional[str] = None,
    ) -> None:
        """Add user-account association.

        ``Partitions=p1,p2`` on ``sacctmgr add user`` is represented by
        calling this method once per partition. Callers that want
        multiple partition-scoped rows must iterate themselves —
        matches real Slurm where each (user, account, cluster, partition)
        tuple is a distinct slurmdb_assoc_rec_t.
        """
        cl = cluster or self.current_cluster
        key = self._association_key(user, account, cl, partition)
        self.associations[key] = Association(
            account=account,
            user=user,
            limits=limits or {},
            cluster=cl,
            partition=partition,
        )

    def get_association(
        self,
        user: str,
        account: str,
        cluster: Optional[str] = None,
        partition: Optional[str] = None,
    ) -> Optional[Association]:
        """Get association between user and account.

        ``partition=None`` returns the non-partition association
        (matching real Slurm semantics where unbound rows have a NULL
        partition). To find partition-scoped rows, pass ``partition=``
        explicitly or use ``list_user_associations``.
        """
        cl = cluster or self.current_cluster
        key = self._association_key(user, account, cl, partition)
        return self.associations.get(key)

    def list_user_associations(
        self, user: str, account: str, cluster: Optional[str] = None
    ) -> list[Association]:
        """Return every Association row matching (user, account, cluster).

        With multiple ``Partitions=`` this returns one entry per partition.
        """
        cl = cluster or self.current_cluster
        account = fold_account(account)
        return [
            a
            for a in self.associations.values()
            if a.user == user and a.account == account and a.cluster == cl
        ]

    def list_account_users(self, account: str, cluster: Optional[str] = None) -> list[str]:
        """List users associated with account (deduplicated across partitions)."""
        cl = cluster or self.current_cluster
        account = fold_account(account)
        users: list[str] = []
        seen: set[str] = set()
        for assoc in self.associations.values():
            if (
                assoc.account == account
                and assoc.user
                and assoc.cluster == cl
                and assoc.user not in seen
            ):
                seen.add(assoc.user)
                users.append(assoc.user)
        return users

    def delete_association(
        self,
        user: str,
        account: str,
        cluster: Optional[str] = None,
        partition: Optional[str] = None,
    ) -> None:
        """Delete a single user-account-partition association row."""
        cl = cluster or self.current_cluster
        key = self._association_key(user, account, cl, partition)
        if key in self.associations:
            del self.associations[key]

    def delete_user_associations(
        self, user: str, account: str, cluster: Optional[str] = None
    ) -> int:
        """Delete every row matching (user, account, cluster), all partitions.

        Returns the number of rows removed. Mirrors
        ``sacctmgr remove user where name=X and account=Y`` which wipes
        all partition-scoped associations for that pair.
        """
        cl = cluster or self.current_cluster
        account = fold_account(account)
        keys = [
            k
            for k, a in self.associations.items()
            if a.user == user and a.account == account and a.cluster == cl
        ]
        for k in keys:
            del self.associations[k]
        return len(keys)

    # --- Usage record methods (cluster-aware) ---

    def add_usage_record(self, record: UsageRecord) -> None:
        """Add usage record."""
        if record.job_id is None:
            record.job_id = self._next_job_id
            self._next_job_id += 1
        self.usage_records.append(record)

    def ensure_job_ids(self) -> None:
        """Assign job ids to records appended without one.

        Tests (and older state files) append to ``usage_records``
        directly; ids are handed out in list order so they are
        deterministic and stable across calls.
        """
        max_assigned = max(
            (r.job_id for r in self.usage_records if r.job_id is not None),
            default=0,
        )
        self._next_job_id = max(self._next_job_id, max_assigned + 1)
        for record in self.usage_records:
            if record.job_id is None:
                record.job_id = self._next_job_id
                self._next_job_id += 1

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

    def allocate_job_id(self) -> int:
        """Allocate a fresh numeric job id shared with usage records.

        Active jobs and completed usage records draw from the same
        ``_next_job_id`` counter so a submitted job keeps its id when it
        rolls over into accounting. Bumps past any id already present.
        """
        self.ensure_job_ids()
        existing = [int(j.job_id) for j in self.jobs.values() if str(j.job_id).isdigit()]
        if existing:
            self._next_job_id = max(self._next_job_id, max(existing) + 1)
        jid = self._next_job_id
        self._next_job_id += 1
        return jid

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

    def bootstrap_default_qos(self) -> bool:
        """Seed a standard set of usable QoS classes when none are defined.

        Idempotent — does nothing if any QoS already exists. Includes the
        periodic-limits operational classes (``normal``/``slowdown``/``blocked``)
        plus common HPC classes so accounts can be assigned meaningful QoS out
        of the box. Returns True if it seeded anything.
        """
        if self.qos_list:
            return False
        defaults = [
            QOS(name="normal", max_wall="7-00:00:00"),
            QOS(name="high", max_jobs=20, max_wall="1-00:00:00"),
            QOS(name="low", max_wall="14-00:00:00"),
            QOS(name="long", max_jobs=10, max_submit=20, max_wall="30-00:00:00"),
            QOS(name="gpu", grp_tres="gres/gpu=8", max_wall="2-00:00:00"),
            QOS(name="debug", max_jobs=2, max_submit=4, max_wall="00:30:00"),
            QOS(name="slowdown", flags="DenyOnLimit", max_wall="7-00:00:00"),
            QOS(name="blocked", flags="DenyOnLimit", max_jobs=0, max_submit=0),
        ]
        for qos in defaults:
            self.qos_list[qos.name] = qos
        return True

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

        def _serialize_job(job: Job) -> dict:
            d = asdict(job)
            for dt_field in ("submit_time", "start_time", "end_time"):
                if d.get(dt_field) is not None:
                    d[dt_field] = d[dt_field].isoformat()
            return d

        state = {
            "_next_cluster_id": self._next_cluster_id,
            "_next_job_id": self._next_job_id,
            "clusters": {name: _serialize_cluster(cl) for name, cl in self.clusters.items()},
            "current_cluster": self.current_cluster,
            "accounts": {key: asdict(acc) for key, acc in self.accounts.items()},
            "users": {name: asdict(user) for name, user in self.users.items()},
            "associations": {key: asdict(assoc) for key, assoc in self.associations.items()},
            "usage_records": [self._serialize_usage_record(r) for r in self.usage_records],
            "jobs": {jid: _serialize_job(job) for jid, job in self.jobs.items()},
            "qos": {name: asdict(qos) for name, qos in self.qos_list.items()},
        }

        try:
            # Lock-then-truncate so concurrent CLI/API processes never
            # produce torn JSON: open("w") would truncate before the
            # lock is held, letting a LOCK_SH reader observe an empty
            # file (or a second writer leave trailing bytes). Whole-file
            # last-writer-wins semantics are unchanged.
            with self.state_file.open("a+") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                f.seek(0)
                f.truncate()
                json.dump(state, f, indent=2)
        except Exception as e:
            print(f"Warning: Failed to save database state: {e}")

    def load_state(self) -> None:
        """Load database state from file with backward-compatible migration."""
        try:
            if self.state_file.exists():
                with self.state_file.open() as f:
                    fcntl.flock(f, fcntl.LOCK_SH)
                    state = json.load(f)

                self._next_cluster_id = state.get("_next_cluster_id", 1)
                self._next_job_id = state.get("_next_job_id", 1)

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
                    # Account names are case-insensitive: key by the folded
                    # name so lookups match regardless of the stored case.
                    name = fold_account(name)
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
                    # Strip legacy fields from the prior shape of this branch
                    # (partitions: list + default_partition) and any older
                    # files that never had partition support. Expand
                    # non-empty partition lists into one row per partition.
                    legacy_partitions = data.pop("partitions", None)
                    data.pop("default_partition", None)
                    data.setdefault("partition", None)

                    cluster = data["cluster"]
                    if legacy_partitions:
                        for part in legacy_partitions:
                            row = dict(data)
                            row["partition"] = part
                            assoc = Association(**row)
                            new_key = self._association_key(
                                assoc.user, assoc.account, cluster, part
                            )
                            self.associations[new_key] = assoc
                        continue

                    assoc = Association(**data)
                    # Migrate older keys ("user:account" or "user:account:cluster")
                    # to the new "user:account:cluster:partition" form.
                    parts = key.split(":")
                    if len(parts) == 2:
                        key = self._association_key(
                            parts[0], parts[1], assoc.cluster, assoc.partition
                        )
                    elif len(parts) == 3:
                        key = self._association_key(parts[0], parts[1], parts[2], assoc.partition)
                    self.associations[key] = assoc

                # Load usage records
                self.usage_records = []
                for data in state.get("usage_records", []):
                    data.setdefault("cluster", "default")
                    self.usage_records.append(self._deserialize_usage_record(data))
                # Migrate pre-job_id state files.
                self.ensure_job_ids()

                # Load jobs
                self.jobs = {}
                for jid, data in state.get("jobs", {}).items():
                    data.setdefault("cluster", "default")
                    # Handle datetime fields
                    for dt_field in ["submit_time", "start_time", "end_time"]:
                        if data.get(dt_field):
                            data[dt_field] = datetime.fromisoformat(data[dt_field])
                    self.jobs[jid] = Job(**data)

                # Load QOS entries (absent in pre-0.7 state files)
                self.qos_list = {}
                for name, data in state.get("qos", {}).items():
                    self.qos_list[name] = QOS(**data)

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
        data.setdefault("job_id", None)
        data.setdefault("state", "COMPLETED")
        return UsageRecord(**data)
