"""Usage simulation for injecting node-hour consumption."""

import inspect
from datetime import datetime, timedelta
from typing import Any, Optional

from emulator.core.database import FINISHED_JOB_STATES, SlurmDatabase, UsageRecord
from emulator.core.energy import DEFAULT_PARTITION, energy_joules
from emulator.core.time_engine import TimeEngine

# Standard node: 64 CPUs, 512 GB RAM, 4 GPUs (the same rates sacct and
# slurmrestd fall back to when a record carries no breakdown).
NODE_CPUS = 64
NODE_MEM_GB = 512
NODE_GPUS = 4


def standard_node_raw_tres(
    node_hours: float,
    partition: str = DEFAULT_PARTITION,
    energy: Optional[int] = None,
) -> dict[str, int]:
    """Raw TRES (``<count>-hours``; joules for ``energy``) of a standard-node job.

    Shared by injected usage and by jobs the scheduler completes so that
    every record path yields the same CPU/Mem/GPU breakdown and the same
    power-model energy for the same node-hours.
    """
    gpu_hours = int(node_hours * NODE_GPUS)
    return {
        "node-hours": int(node_hours),  # Direct node-hours mapping for site agent
        "CPU": int(node_hours * NODE_CPUS),
        "Mem": int(node_hours * NODE_MEM_GB),  # GB
        "GRES/gpu": gpu_hours,
        # Joules rather than hours; see the power model in energy.py.
        "energy": energy_joules(node_hours, gpu_hours, partition)
        if energy is None
        else int(energy),
    }


def job_raw_tres(
    elapsed_hours: float,
    cpus: int = 1,
    gpus: int = 0,
    mem_gb: Optional[int] = None,
    partition: str = DEFAULT_PARTITION,
    energy: Optional[int] = None,
) -> dict[str, int]:
    """Raw TRES of one job with an explicit CPU/GPU allocation.

    Records keep ``<count>-hours`` totals and sacct divides the per-job
    count back out, so ``count * elapsed_hours`` must survive integer
    rounding — true for any whole count once ``elapsed_hours >= 1``;
    shorter jobs need a whole product and raise ``ValueError`` otherwise.
    ``mem_gb`` defaults to the standard node's memory per CPU; without
    ``energy`` the power model charges the job's share of a node
    (``cpus / NODE_CPUS``) plus its GPUs.
    """
    if elapsed_hours <= 0:
        msg = "elapsed_hours must be positive"
        raise ValueError(msg)
    if cpus < 1 or gpus < 0:
        msg = "cpus must be >= 1 and gpus >= 0"
        raise ValueError(msg)
    mem = mem_gb if mem_gb is not None else cpus * NODE_MEM_GB // NODE_CPUS
    raw: dict[str, int] = {}
    for key, count in (("CPU", cpus), ("Mem", mem), ("GRES/gpu", gpus)):
        total = round(count * elapsed_hours)
        if round(total / elapsed_hours) != count:
            msg = (
                f"{key}={count} over {elapsed_hours}h does not round-trip; "
                "choose a whole count x hours product"
            )
            raise ValueError(msg)
        raw[key] = total
    raw["energy"] = (
        energy_joules(elapsed_hours * cpus / NODE_CPUS, gpus * elapsed_hours, partition)
        if energy is None
        else int(energy)
    )
    return raw


def job_total_cpu(
    cpus: int,
    elapsed_hours: float,
    cpu_efficiency: Optional[float] = None,
    total_cpu_seconds: Optional[float] = None,
) -> Optional[float]:
    """CPU time a job consumed, from an efficiency or an explicit figure.

    ``cpu_efficiency`` (0..1) scales ``cpus x elapsed``; the two inputs
    are mutually exclusive, and neither leaves the record at "fully busy".
    """
    if cpu_efficiency is not None and total_cpu_seconds is not None:
        msg = "give cpu_efficiency or total_cpu_seconds, not both"
        raise ValueError(msg)
    if cpu_efficiency is not None:
        if not 0 <= cpu_efficiency <= 1:
            msg = "cpu_efficiency must be between 0 and 1"
            raise ValueError(msg)
        return cpus * elapsed_hours * 3600 * cpu_efficiency
    if total_cpu_seconds is not None and total_cpu_seconds < 0:
        msg = "total_cpu_seconds must not be negative"
        raise ValueError(msg)
    return total_cpu_seconds


def job_state(state: str) -> str:
    """Upper-cased final state; anything but a finished base state is refused."""
    upper = state.upper()
    if upper not in FINISHED_JOB_STATES:
        msg = f"state must be one of {', '.join(FINISHED_JOB_STATES)}"
        raise ValueError(msg)
    return upper


class UsageSimulator:
    """Simulates user usage patterns and injection."""

    def __init__(self, time_engine: TimeEngine, database: SlurmDatabase):
        self.time_engine = time_engine
        self.database = database
        self.billing_weights = {
            "CPU": 0.015625,  # 64 CPUs = 1 billing unit
            "Mem": 0.001953125,  # 512 GB = 1 billing unit
            "GRES/gpu": 0.25,  # 4 GPUs = 1 billing unit
        }

    def inject_usage(
        self,
        account: str,
        user: str,
        node_hours: float,
        at_time: Optional[datetime] = None,
        cluster: Optional[str] = None,
        partition: Optional[str] = None,
        energy_joules: Optional[int] = None,
    ) -> None:
        """Inject specific usage amount at given time.

        ``energy_joules`` seeds an exact ``energy`` TRES value; when omitted
        the power model in :mod:`emulator.core.energy` derives it from the
        node-hours, the GPU-hours and the partition.
        """
        if at_time is None:
            at_time = self.time_engine.get_current_time()

        cl = cluster or self.database.current_cluster
        part = partition or DEFAULT_PARTITION

        # Ensure user and account exist
        if not self.database.get_user(user):
            self.database.add_user(user, account)

        if not self.database.get_account(account):
            self.database.add_account(account, f"Account {account}", "emulator")

        # Ensure association exists
        if not self.database.get_association(user, account, cluster=cl):
            self.database.add_association(user, account, cluster=cl)

        # Create usage record
        usage_record = UsageRecord(
            account=account,
            user=user,
            node_hours=node_hours,
            billing_units=node_hours,  # 1:1 for node-hour billing
            timestamp=at_time,
            period=self.time_engine.get_current_quarter(),
            raw_tres=self._convert_to_raw_tres(node_hours, part, energy_joules),
            cluster=cl,
            partition=part,
        )

        self.database.add_usage_record(usage_record)
        print(f"💾 Injected {node_hours}Nh usage for {user} in {account} at {at_time}")

        # Save state after injection
        self.database.save_state()

    def inject_job(
        self,
        account: str,
        user: str,
        elapsed_hours: float,
        cpus: int = 1,
        gpus: int = 0,
        mem_gb: Optional[int] = None,
        partition: Optional[str] = None,
        energy: Optional[int] = None,
        admin_comment: str = "",
        end_time: Optional[datetime] = None,
        cluster: Optional[str] = None,
        state: str = "COMPLETED",
        cpu_efficiency: Optional[float] = None,
        total_cpu_seconds: Optional[float] = None,
    ) -> int:
        """Record one finished job and return its job id.

        Unlike :meth:`inject_usage`, which books whole standard nodes, this
        describes a single job the way ``sacct`` shows it: ``AllocTRES`` is
        ``cpu=<cpus>`` (``gres/gpu=<gpus>``), ``Elapsed`` is
        ``elapsed_hours``, ``End`` is ``end_time`` (default: the simulated
        now) and ``AdminComment`` is ``admin_comment`` verbatim. ``State``
        is any finished base state (:data:`FINISHED_JOB_STATES`);
        ``TotalCPU`` comes from ``cpu_efficiency`` or ``total_cpu_seconds``
        (:func:`job_total_cpu`). See :func:`job_raw_tres` for the rounding
        rule and the energy default. Does not save state;
        :meth:`inject_jobs` does.
        """
        part = partition or DEFAULT_PARTITION
        raw_tres = job_raw_tres(elapsed_hours, cpus, gpus, mem_gb, part, energy)
        final_state = job_state(state)
        total_cpu = job_total_cpu(cpus, elapsed_hours, cpu_efficiency, total_cpu_seconds)
        end = end_time or self.time_engine.get_current_time()
        cl = cluster or self.database.current_cluster

        if not self.database.get_account(account):
            self.database.add_account(account, f"Account {account}", "emulator")
        if not self.database.get_user(user):
            self.database.add_user(user, account)
        if not self.database.get_association(user, account, cluster=cl):
            self.database.add_association(user, account, cluster=cl)

        record = UsageRecord(
            account=account,
            user=user,
            node_hours=elapsed_hours,
            billing_units=elapsed_hours,
            timestamp=end,
            period=f"{end.year}-Q{(end.month - 1) // 3 + 1}",
            raw_tres=raw_tres,
            cluster=cl,
            partition=part,
            state=final_state,
            admin_comment=admin_comment,
            total_cpu_seconds=total_cpu,
        )
        self.database.add_usage_record(record)
        return int(record.job_id or 0)  # add_usage_record always assigns one

    def inject_jobs(self, jobs: list[dict[str, Any]]) -> list[int]:
        """Inject several jobs from plain dicts (control API, scenarios).

        Keys are :meth:`inject_job`'s keyword arguments, with ``end`` as an
        ISO 8601 string instead of ``end_time``. Every job is validated
        before any is recorded, so a bad entry leaves the state untouched;
        the state is saved once at the end.
        """
        prepared = []
        for spec in jobs:
            kwargs = dict(spec)
            end = kwargs.pop("end", None)
            if end is not None:
                kwargs["end_time"] = datetime.fromisoformat(str(end))
            # Missing/unknown keys raise TypeError here, before any write.
            inspect.signature(self.inject_job).bind(**kwargs)
            job_raw_tres(
                kwargs.get("elapsed_hours", 0),
                kwargs.get("cpus", 1),
                kwargs.get("gpus", 0),
                kwargs.get("mem_gb"),
                energy=kwargs.get("energy"),
            )
            job_state(kwargs.get("state", "COMPLETED"))
            job_total_cpu(
                kwargs.get("cpus", 1),
                kwargs["elapsed_hours"],
                kwargs.get("cpu_efficiency"),
                kwargs.get("total_cpu_seconds"),
            )
            prepared.append(kwargs)
        job_ids = [self.inject_job(**kwargs) for kwargs in prepared]
        self.database.save_state()
        print(f"💾 Injected {len(job_ids)} job(s): {', '.join(map(str, job_ids))}")
        return job_ids

    def inject_usage_pattern(self, account: str, user: str, pattern_config: dict) -> None:
        """Inject usage following a pattern over time."""
        pattern_type = pattern_config.get("type", "steady")

        if pattern_type == "steady":
            self._steady_pattern(account, user, pattern_config)
        elif pattern_type == "bursty":
            self._bursty_pattern(account, user, pattern_config)
        elif pattern_type == "end_of_period":
            self._end_of_period_pattern(account, user, pattern_config)
        else:
            raise ValueError(f"Unknown pattern type: {pattern_type}")

    def simulate_sequence_scenario(self) -> None:
        """Run the exact scenario from SLURM_PERIODIC_LIMITS_SEQUENCE.md."""
        account = "slurm_account_123"
        user1, user2 = "user1", "user2"

        print("🎬 Starting sequence scenario simulation")

        # Q1: 500Nh over 3 months (167Nh per month roughly)
        print("\n📅 Q1 2024: Simulating 500Nh usage over 3 months")

        self.time_engine.set_time(datetime(2024, 1, 31))
        self.inject_usage(account, user1, 100)
        self.inject_usage(account, user2, 67)

        self.time_engine.set_time(datetime(2024, 2, 29))
        self.inject_usage(account, user1, 100)
        self.inject_usage(account, user2, 67)

        self.time_engine.set_time(datetime(2024, 3, 31))
        self.inject_usage(account, user1, 100)
        self.inject_usage(account, user2, 66)  # Total: 500Nh

        q1_total = self.database.get_total_usage(account, "2024-Q1")
        print(f"✅ Q1 total usage: {q1_total}Nh")

        # Q2 Transition - advance to Q2
        print("\n📅 Q2 2024: Transition with carryover")
        self.time_engine.set_time(datetime(2024, 4, 1))

        # Q2 Month 1: 500Nh
        self.time_engine.set_time(datetime(2024, 4, 30))
        self.inject_usage(account, user1, 300)
        self.inject_usage(account, user2, 200)

        # Q2 Month 2: 500Nh more (reaching 1500Nh total for Q2)
        self.time_engine.set_time(datetime(2024, 5, 20))
        self.inject_usage(account, user1, 300)
        self.inject_usage(account, user2, 200)

        q2_partial = self.database.get_total_usage(account, "2024-Q2")
        print(f"📊 Q2 usage so far: {q2_partial}Nh (should trigger threshold)")

        # Additional usage: 200Nh more (total 1700Nh in Q2)
        self.time_engine.advance_time(days=5)
        self.inject_usage(account, user1, 200)

        # Final push: 250Nh more (total 2000Nh - hitting hard limit)
        self.time_engine.advance_time(days=10)
        self.inject_usage(account, user1, 250)

        q2_total = self.database.get_total_usage(account, "2024-Q2")
        print(f"🚨 Q2 final usage: {q2_total}Nh (should hit hard limit)")

        # Q3 Transition with decay
        print("\n📅 Q3 2024: Transition with 15-day decay")
        self.time_engine.set_time(datetime(2024, 7, 1))

        print("✅ Sequence scenario simulation completed!")

    def get_current_usage_summary(self, account: str) -> dict:
        """Get current usage summary for account."""
        current_period = self.time_engine.get_current_quarter()
        period_usage = self.database.get_total_usage(account, current_period)
        total_usage = self.database.get_total_usage(account)

        account_obj = self.database.get_account(account)
        allocation = account_obj.allocation if account_obj else 1000

        return {
            "account": account,
            "current_period": current_period,
            "period_usage": period_usage,
            "total_usage": total_usage,
            "allocation": allocation,
            "remaining": max(0, allocation - period_usage),
            "percentage_used": (period_usage / allocation) * 100 if allocation > 0 else 0,
        }

    def _convert_to_raw_tres(
        self,
        node_hours: float,
        partition: str = DEFAULT_PARTITION,
        energy: Optional[int] = None,
    ) -> dict[str, int]:
        return standard_node_raw_tres(node_hours, partition, energy)

    def _steady_pattern(self, account: str, user: str, config: dict) -> None:
        """Generate steady usage pattern over time period."""
        total_usage = config["total_usage"]
        days = config.get("days", 30)
        daily_usage = total_usage / days

        start_time = self.time_engine.get_current_time()

        for day in range(days):
            usage_time = start_time + timedelta(days=day)
            self.inject_usage(account, user, daily_usage, usage_time)

    def _bursty_pattern(self, account: str, user: str, config: dict) -> None:
        """Generate bursty usage pattern with irregular spikes."""
        burst_times = config["burst_times"]  # List of (day, usage) tuples
        start_time = self.time_engine.get_current_time()

        for day, usage in burst_times:
            usage_time = start_time + timedelta(days=day)
            self.inject_usage(account, user, usage, usage_time)

    def _end_of_period_pattern(self, account: str, user: str, config: dict) -> None:
        """Generate usage pattern concentrated at end of period."""
        total_usage = config["total_usage"]
        period_days = config.get("period_days", 90)  # Quarter
        concentration_days = config.get("concentration_days", 7)  # Last week

        # 80% of usage in last concentration_days
        concentrated_usage = total_usage * 0.8
        regular_usage = total_usage * 0.2

        start_time = self.time_engine.get_current_time()

        # Regular usage throughout period
        daily_regular = regular_usage / (period_days - concentration_days)
        for day in range(period_days - concentration_days):
            usage_time = start_time + timedelta(days=day)
            self.inject_usage(account, user, daily_regular, usage_time)

        # Concentrated usage at end
        daily_concentrated = concentrated_usage / concentration_days
        for day in range(period_days - concentration_days, period_days):
            usage_time = start_time + timedelta(days=day)
            self.inject_usage(account, user, daily_concentrated, usage_time)
