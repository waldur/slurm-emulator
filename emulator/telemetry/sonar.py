"""Sonar-format telemetry for the emulated cluster.

Sonar (https://github.com/NordicHPC/sonar) runs on every node of a real
cluster and publishes JSON messages to Kafka, one topic per kind:
``<cluster>.sample`` (node + per-process + per-GPU state, every few minutes),
``<cluster>.sysinfo`` (hardware, every few hours), ``<cluster>.job`` (Slurm job records)
and ``<cluster>.cluster`` (partitions and node states). slurm-monitor's
listener stores them in TimescaleDB and its REST API serves per-job time
series from there.

The emulator has no nodes or GPUs, so this module synthesises what Sonar
would sample, as pure functions of a job's accounting row, the node and the
timestamp -- the same inputs always give the same message. The values agree
with what the accounting view already reports:

* a job's CPU use and memory follow ``cluster_model.synthetic_efficiency``
  (CPU efficiency x allocated CPUs, memory fraction x memory per node);
  its GPUs follow ``cluster_model.synthetic_gpu_use`` (utilisation, memory
  fraction of ``GPU_MEM_MB`` per card), the means behind sacct's TotalCPU,
  MaxRSS and the ``gres/gpuutil`` / ``gres/gpumem`` step TRES;
* each sample wobbles around that mean with deterministic zero-mean noise
  (seeded by job, node, card and timestamp) and ramps up over the first
  minutes of the job; memory never exceeds the accounted peak.

Message shapes follow Sonar's ``types.spec.yaml`` 0.18.4 (envelope
``{"meta": ..., "data": {"type", "attributes"}}``), with one deviation the
consumer needs: the job topic's data type is ``"job"`` (slurm-monitor's
importer dispatches on ``parse_<type>`` and has no ``parse_jobs``), where the
spec documents ``"jobs"``.

Intentional simplifications: ``system.cpus`` (CPU seconds per core since
boot; slurm-monitor stores it but reads nothing from it) grows at a fixed
per-core rate instead of following the load, no disk statistics, one
application process per node, and every GPU job holds all four cards of each of its nodes, as
the accounting view charges ``gres/gpu`` (``partition_has_gpus``).
"""

from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from emulator.core import cluster_model
from emulator.core.cluster_model import (
    GPU_MEM_MB,
    NODE_CPUS,
    NODE_GPUS,
    NODE_MEM_MB,
    OS_MEM_MB,
    compress_hostlist,
    expand_hostlist,
    partition_has_gpus,
)
from emulator.core.database import Job, UsageRecord

SONAR_VERSION = "0.18.4"
# No "format": slurm-monitor 5fa6a4c builds its Meta from exactly producer +
# version and rejects any other key, so a message carrying it is skipped.
META = {"producer": "sonar", "version": SONAR_VERSION}
TOPIC_KINDS = ("sample", "sysinfo", "job", "cluster")

GPU_MODEL = "NVIDIA GH200 480GB"
GPU_ARCHITECTURE = "Hopper"
GPU_MEM_KIB = GPU_MEM_MB * 1024
GPU_IDLE_W = 90
GPU_POWER_LIMIT_W = 700
GPU_MAX_CE_CLOCK = 1980
GPU_MAX_MEMORY_CLOCK = 2619

# Samples ramp from ~5% to the job's mean over this long (or a tenth of a
# shorter job).
RAMP_SECONDS = 600.0
# Slow component of the per-sample wobble.
WOBBLE_PERIOD_SECONDS = 47 * 60.0
# Nodes reboot every so often; ``boot`` is the start of the current window.
BOOT_WINDOW_SECONDS = 30 * 86400

_UUID_NAMESPACE = uuid.UUID("6f1c2d4e-9a7b-4c1e-8f3a-5b2d7e9c0a11")


def topic(cluster: str, kind: str) -> str:
    return f"{cluster}.{kind}"


def envelope(kind: str, attributes: dict[str, Any]) -> dict[str, Any]:
    return {"meta": dict(META), "data": {"type": kind, "attributes": attributes}}


def utc(value: datetime) -> datetime:
    """Aware UTC; a naive value is local time, as the emulator stores it."""
    return value.astimezone(timezone.utc)


def iso(value: datetime) -> str:
    return utc(value).isoformat(timespec="seconds")


# --- deterministic noise ---


def _u(*key: object) -> float:
    """Uniform [0, 1) from ``key``, the same in every process and run."""
    return random.Random("sonar:" + ":".join(map(str, key))).random()  # noqa: S311 - noise, not crypto


def _wobble(at: datetime, *key: object) -> float:
    """Zero-mean noise in [-1, 1]: per-sample jitter plus a slow wave."""
    ts = utc(at).timestamp()
    phase = 2 * math.pi * _u(*key, "phase")
    slow = math.sin(2 * math.pi * ts / WOBBLE_PERIOD_SECONDS + phase)
    return 0.6 * (2 * _u(*key, int(ts)) - 1) + 0.4 * slow


def _clamp(value: float, low: float, high: float) -> float:
    return min(max(value, low), high)


# --- topology ---


def node_has_gpus(node: str) -> bool:
    return cluster_model._has_gpus(node)


def gpu_uuid(cluster: str, node: str, index: int) -> str:
    return "GPU-" + str(uuid.uuid5(_UUID_NAMESPACE, f"{cluster}/{node}/{index}"))


def boot_time(node: str, at: datetime) -> datetime:
    offset = int(_u(node, "boot") * 86400)
    ts = int(utc(at).timestamp()) - offset
    start = (ts // BOOT_WINDOW_SECONDS) * BOOT_WINDOW_SECONDS + offset
    return datetime.fromtimestamp(start, tz=timezone.utc)


# --- jobs ---


@dataclass(frozen=True)
class JobRun:
    """What the telemetry needs of one job: where and when it ran."""

    job_id: int
    user: str
    account: str
    partition: str
    job_name: str
    state: str
    nodes: tuple[str, ...]
    alloc_cpus: int
    mem_per_node_mb: int
    start_time: datetime
    end_time: Optional[datetime] = None  # None while running
    submit_time: Optional[datetime] = None
    time_limit: Optional[int] = None  # minutes
    exit_code: Optional[int] = None
    environment: dict[str, str] = field(default_factory=dict, hash=False, compare=False)
    # GPUs the run was allocated per node; None for records from before
    # records stored their TRES, which hold every GPU of a GPU partition's node.
    allocated_gpus_per_node: Optional[int] = field(default=None, hash=False, compare=False)

    @property
    def gpus_per_node(self) -> int:
        if self.allocated_gpus_per_node is not None:
            return self.allocated_gpus_per_node
        return NODE_GPUS if partition_has_gpus(self.partition) else 0

    def cpus_on(self, node: str) -> int:
        if node not in self.nodes:
            return 0
        shares = cluster_model._split(
            self.alloc_cpus or len(self.nodes) * NODE_CPUS, len(self.nodes)
        )
        return shares[self.nodes.index(node)]

    def active_at(self, at: datetime) -> bool:
        return self.start_time <= at and (self.end_time is None or at < self.end_time)

    def duration(self, now: datetime) -> float:
        return ((self.end_time or now) - self.start_time).total_seconds()


def run_from_record(record: UsageRecord) -> Optional[JobRun]:
    """A finished job from its accounting row (needs times and nodes)."""
    if record.job_id is None or not record.start_time or not record.end_time:
        return None
    nodes = tuple(expand_hostlist(record.node_list))
    if not nodes:
        return None
    cpus = record.alloc_cpus or len(nodes) * NODE_CPUS
    first_share = cluster_model._split(cpus, len(nodes))[0]
    mem = record.req_mem_mb or first_share * cluster_model.DEF_MEM_PER_CPU_MB
    return JobRun(
        job_id=int(record.job_id),
        user=record.user,
        account=record.account,
        partition=record.partition,
        job_name=record.job_name or f"job_{record.job_id}",
        state=record.state,
        nodes=nodes,
        alloc_cpus=cpus,
        mem_per_node_mb=min(mem, NODE_MEM_MB),
        start_time=utc(record.start_time),
        end_time=utc(record.end_time),
        submit_time=utc(record.submit_time) if record.submit_time else None,
        time_limit=record.time_limit,
        exit_code=record.exit_code if record.exit_code is not None else 0,
        allocated_gpus_per_node=record.tres_alloc.get("gres/gpu", 0) // max(len(nodes), 1)
        if record.tres_req
        else None,
    )


def run_from_job(job: Job) -> Optional[JobRun]:
    """A running job from the controller's view (needs a start and nodes)."""
    if not job.start_time or not job.node_list or not str(job.job_id).isdigit():
        return None
    nodes = tuple(job.node_list)
    cpus = job.alloc_cpus or len(nodes) * NODE_CPUS
    first_share = cluster_model._split(cpus, len(nodes))[0]
    return JobRun(
        job_id=int(job.job_id),
        user=job.user,
        account=job.account,
        partition=job.partition,
        job_name=job.name or f"job_{job.job_id}",
        state="RUNNING",
        nodes=nodes,
        alloc_cpus=cpus,
        mem_per_node_mb=cluster_model.job_mem_per_node(job, first_share),
        start_time=utc(job.start_time),
        submit_time=utc(job.submit_time) if job.submit_time else None,
        time_limit=job.time_limit,
        environment=dict(job.environment or {}),
        allocated_gpus_per_node=max((len(i) for i in job.gpu_indices.values()), default=0),
    )


def application_command(job_name: str) -> str:
    name = (job_name or "").lower()
    if name.startswith(("gromacs", "md-")):
        return "gmx_mpi"
    if name.startswith("wrf"):
        return "wrf.exe"
    if name.startswith(("multiqc", "qc")):
        return "multiqc"
    return "python3"


def _ramp(run: JobRun, at: datetime) -> float:
    elapsed = (at - run.start_time).total_seconds()
    window = min(RAMP_SECONDS, max(run.duration(at) * 0.1, 1.0))
    return _clamp(elapsed / window, 0.05, 1.0)


@dataclass
class GpuUse:
    index: int
    util: float  # percent
    memory_kib: int


@dataclass
class NodeUse:
    """One job's synthetic use of one node at one instant."""

    cpu_util: float  # percent of one core
    cpu_avg: float
    cpu_time: int  # seconds
    resident_kib: int
    gpus: list[GpuUse]


def job_node_use(run: JobRun, node: str, at: datetime, gpu_indices: list[int]) -> NodeUse:
    cpus = run.cpus_on(node)
    cpu_eff, mem_frac = cluster_model.synthetic_efficiency(run.job_id, run.environment)
    ramp = _ramp(run, at)
    elapsed = max((at - run.start_time).total_seconds(), 0.0)
    cpu_util = _clamp(
        cpus * cpu_eff * 100 * ramp * (1 + 0.08 * _wobble(at, run.job_id, node, "cpu")),
        0.0,
        cpus * 100.0,
    )
    resident = mem_frac * run.mem_per_node_mb * 1024 * ramp * (1 - 0.05 * _u(run.job_id, node, at))
    gpus: list[GpuUse] = []
    if gpu_indices:
        util, gpu_mem_frac = cluster_model.synthetic_gpu_use(run.job_id)
        offsets = [0.12 * (_u(run.job_id, node, i, "card") - 0.5) for i in gpu_indices]
        mean_offset = sum(offsets) / len(offsets)
        for index, offset in zip(gpu_indices, offsets):
            noise = 0.10 * _wobble(at, run.job_id, node, index, "gpu")
            # a few points of absolute jitter, so a mostly idle card still wobbles
            jitter = 3.0 * ramp * _wobble(at, run.job_id, node, index, "gpu-abs")
            gpus.append(
                GpuUse(
                    index=index,
                    util=_clamp(
                        util * 100 * ramp * (1 + offset - mean_offset + noise) + jitter, 0.0, 100.0
                    ),
                    memory_kib=int(
                        gpu_mem_frac
                        * GPU_MEM_KIB
                        * ramp
                        * (1 - 0.04 * _u(run.job_id, node, index, at))
                    ),
                )
            )
    return NodeUse(
        cpu_util=cpu_util,
        cpu_avg=cpus * cpu_eff * 100,
        cpu_time=int(cpus * cpu_eff * elapsed),
        resident_kib=int(resident),
        gpus=gpus,
    )


def _pid_base(run: JobRun, node: str) -> int:
    return 20000 + int(_u(run.job_id, node, "pid") * 1_000_000)


def job_processes(run: JobRun, node: str, at: datetime, use: NodeUse, cluster: str) -> list[dict]:
    """Sonar ``SampleProcess`` records of ``run`` on ``node``.

    The batch host (first node) runs ``slurm_script`` -> ``srun`` -> the
    application; the other nodes only the application rank.
    """
    base = _pid_base(run, node)
    elapsed = max((at - run.start_time).total_seconds(), 0.0)
    processes: list[dict] = []
    app_rss = use.resident_kib
    app_ppid = base - 2  # slurmstepd, not part of the job's processes
    if node == run.nodes[0]:
        script_rss, srun_rss = 3 * 1024, 12 * 1024
        app_rss = max(app_rss - script_rss - srun_rss, 0)
        processes.append(
            {
                "resident_memory": script_rss,
                "virtual_memory": 9 * 1024,
                "cmd": "slurm_script",
                "pid": base,
                "ppid": base - 2,
                "num_threads": 0,
                "cpu_avg": 0.0,
                "cpu_util": 0.0,
                "cpu_time": 0,
                "data_read": 64,
                "data_written": 4,
                "data_cancelled": 0,
                "rolledup": 0,
                "in_container": False,
            }
        )
        processes.append(
            {
                "resident_memory": srun_rss,
                "virtual_memory": 410 * 1024,
                "cmd": "srun",
                "pid": base + 1,
                "ppid": base,
                "num_threads": 4,
                "cpu_avg": 0.1,
                "cpu_util": round(0.1 + 0.1 * _u(run.job_id, node, at, "srun"), 2),
                "cpu_time": int(elapsed * 0.001),
                "data_read": 128,
                "data_written": 16,
                "data_cancelled": 0,
                "rolledup": 0,
                "in_container": False,
            }
        )
        app_ppid = base + 1
    cpu_eff, _ = cluster_model.synthetic_efficiency(run.job_id, run.environment)
    app: dict[str, Any] = {
        "resident_memory": app_rss,
        # A little above RSS, but within the request: slurm-monitor's job report
        # warns when the summed virtual memory exceeds the requested memory.
        "virtual_memory": min(
            int(app_rss * 1.05), max(run.mem_per_node_mb * 1024 - 512 * 1024, app_rss)
        ),
        "cmd": application_command(run.job_name),
        "pid": base + 7,
        "ppid": app_ppid,
        "num_threads": max(run.cpus_on(node) - 1, 0),
        "cpu_avg": round(use.cpu_avg, 2),
        "cpu_util": round(use.cpu_util, 2),
        "cpu_time": use.cpu_time,
        "data_read": int(elapsed * 400 * cpu_eff),
        "data_written": int(elapsed * 120 * cpu_eff),
        "data_cancelled": 0,
        "rolledup": 0,
        "in_container": False,
    }
    if use.gpus:
        app["gpus"] = [
            {
                "index": gpu.index,
                "uuid": gpu_uuid(cluster, node, gpu.index),
                "gpu_util": round(gpu.util, 1),
                "gpu_memory": gpu.memory_kib,
                "gpu_memory_util": round(100.0 * gpu.memory_kib / GPU_MEM_KIB, 1),
            }
            for gpu in use.gpus
        ]
    processes.append(app)
    return processes


def gpu_card_sample(
    cluster: str, node: str, index: int, at: datetime, util: float, memory_kib: int
) -> dict[str, Any]:
    """Sonar ``SampleGpu`` for one card; ``util`` in percent."""
    busy = util > 0 or memory_kib > 0
    noise = _wobble(at, node, index, "power")
    power = GPU_IDLE_W + util / 100 * (GPU_POWER_LIMIT_W - GPU_IDLE_W)
    return {
        "index": index,
        "uuid": gpu_uuid(cluster, node, index),
        "failing": 0,
        "fan": 0,  # liquid-cooled
        "compute_mode": "Default",
        "performance_state": 0 if busy else 8,
        "memory": memory_kib,
        "ce_util": round(util),
        "memory_util": round(100.0 * memory_kib / GPU_MEM_KIB),
        "temperature": round(31 + util * 0.38 + 2 * noise),
        "power": round(_clamp(power * (1 + 0.04 * noise), 0, GPU_POWER_LIMIT_W)),
        "power_limit": GPU_POWER_LIMIT_W,
        "ce_clock": round(GPU_MAX_CE_CLOCK - 60 * _u(node, index, at)) if busy else 345,
        "memory_clock": GPU_MAX_MEMORY_CLOCK,
    }


def sample_attributes(
    cluster: str,
    node: str,
    at: datetime,
    runs: list[JobRun],
    background: bool = False,
) -> dict[str, Any]:
    """Sonar ``SampleAttributes`` for ``node`` at ``at``.

    ``runs`` are the jobs on the node (those not active at ``at`` are
    skipped); ``background`` marks a node busy with other tenants' work,
    which loads the node and its GPUs without listing processes.
    """
    active = sorted(
        (r for r in runs if node in r.nodes and r.active_at(at)), key=lambda r: r.job_id
    )
    has_gpus = node_has_gpus(node)
    free_cards = list(range(NODE_GPUS)) if has_gpus else []
    cards: dict[int, tuple[float, int]] = {}
    jobs: list[dict] = []
    load = 0.25 * _u(node, at, "idle")
    used_kib = OS_MEM_MB * 1024
    threads = 0
    for run in active:
        claim = free_cards[: run.gpus_per_node] if run.gpus_per_node else []
        free_cards = [c for c in free_cards if c not in claim]
        use = job_node_use(run, node, at, claim)
        for gpu in use.gpus:
            cards[gpu.index] = (gpu.util, gpu.memory_kib)
        processes = job_processes(run, node, at, use, cluster)
        load += use.cpu_util / 100
        used_kib += sum(p["resident_memory"] for p in processes)
        threads += sum(p["num_threads"] + 1 for p in processes)
        jobs.append({"job": run.job_id, "user": run.user, "epoch": 0, "processes": processes})
    if background and not active:
        load = NODE_CPUS * (0.55 + 0.4 * _u(node, at, "bgload"))
        used_kib += int(NODE_MEM_MB * 1024 * (0.3 + 0.5 * _u(node, at, "bgmem")))
        threads += 3 * NODE_CPUS
        for index in free_cards:
            util = 100 * _clamp(0.6 + 0.15 * _wobble(at, node, index, "bg"), 0, 1)
            cards[index] = (util, int(GPU_MEM_KIB * (0.4 + 0.3 * _u(node, index, "bgmem"))))
    boot = boot_time(node, at)
    since_boot = (at - boot).total_seconds()
    system: dict[str, Any] = {
        "boot": iso(boot),
        "cpus": [int(since_boot * (0.35 + 0.3 * _u(node, i, "core"))) for i in range(NODE_CPUS)],
        "used_memory": min(used_kib, NODE_MEM_MB * 1024),
        "load1": round(load, 2),
        "load5": round(load * (0.97 + 0.03 * _u(node, at, "l5")), 2),
        "load15": round(load * (0.94 + 0.06 * _u(node, at, "l15")), 2),
        "runnable_entities": math.ceil(load) + 1,
        "existing_entities": 850 + threads,
    }
    if has_gpus:
        system["gpus"] = [
            gpu_card_sample(cluster, node, i, at, *cards.get(i, (0.0, 0))) for i in range(NODE_GPUS)
        ]
    attributes: dict[str, Any] = {
        "time": iso(at),
        "cluster": cluster,
        "node": node,
        "system": system,
    }
    if jobs:
        attributes["jobs"] = jobs
    return attributes


def sample_message(
    cluster: str, node: str, at: datetime, runs: list[JobRun], background: bool = False
) -> dict[str, Any]:
    return envelope("sample", sample_attributes(cluster, node, at, runs, background))


def sysinfo_message(cluster: str, node: str, at: datetime) -> dict[str, Any]:
    if node_has_gpus(node):
        cpu = {
            "architecture": "aarch64",
            "os_release": "5.14.0-503.15.1.el9_5.aarch64+64k",
            "cpu_model": "Neoverse-V2",
            "sockets": 1,
            "cores_per_socket": NODE_CPUS,
            "numa_nodes": 1,
            "distances": [[10]],
        }
    else:
        cpu = {
            "architecture": "x86_64",
            "os_release": "5.14.0-503.15.1.el9_5.x86_64",
            "cpu_model": "AMD EPYC 9354 32-Core Processor",
            "sockets": 2,
            "cores_per_socket": NODE_CPUS // 2,
            "numa_nodes": 2,
            "distances": [[10, 32], [32, 10]],
        }
    attributes: dict[str, Any] = {
        "time": iso(at),
        "cluster": cluster,
        "node": node,
        "os_name": "Linux",
        "threads_per_core": 1,
        "memory": NODE_MEM_MB * 1024,
        **cpu,
    }
    if node_has_gpus(node):
        attributes["cards"] = [
            {
                "index": i,
                "uuid": gpu_uuid(cluster, node, i),
                "address": f"0000000{i}:01:00.0",
                "manufacturer": "NVIDIA",
                "model": GPU_MODEL,
                "architecture": GPU_ARCHITECTURE,
                "driver": "565.57.01",
                "firmware": "12.7",
                "memory": GPU_MEM_KIB,
                "power_limit": GPU_POWER_LIMIT_W,
                "max_power_limit": 900,
                "min_power_limit": 200,
                "max_ce_clock": GPU_MAX_CE_CLOCK,
                "max_memory_clock": GPU_MAX_MEMORY_CLOCK,
            }
            for i in range(NODE_GPUS)
        ]
    return envelope("sysinfo", attributes)


def _mem_tres(mb: int) -> str:
    return f"{mb // 1024}G" if mb % 1024 == 0 else f"{mb}M"


def tres_string(run: JobRun) -> str:
    """``billing=..,cpu=..,gres/gpu=..,mem=..,node=..`` as squeue/sacct print it."""
    parts = [f"billing={run.alloc_cpus}", f"cpu={run.alloc_cpus}"]
    if run.gpus_per_node:
        parts.append(f"gres/gpu={run.gpus_per_node * len(run.nodes)}")
    parts.append(f"mem={_mem_tres(run.mem_per_node_mb * len(run.nodes))}")
    parts.append(f"node={len(run.nodes)}")
    return ",".join(parts)


def slurm_job_record(run: JobRun) -> dict[str, Any]:
    """Sonar ``SlurmJob`` for ``run`` (finished or running)."""
    record: dict[str, Any] = {
        "job_id": run.job_id,
        "job_step": "",
        "job_name": run.job_name,
        "job_state": run.state,
        "user_name": run.user,
        "account": run.account,
        "partition": run.partition,
        "reservation": "",
        "nodes": [compress_hostlist(list(run.nodes))],
        "priority": 1,
        "distribution": "",
        "requested_resources": tres_string(run),
        "allocated_resources": tres_string(run),
        "requested_cpus": run.alloc_cpus,
        "requested_node_count": len(run.nodes),
        "requested_memory_per_node": run.mem_per_node_mb * 1024,
        # Deprecated in the spec (use requested_cpus), but slurm-monitor's
        # JobResponse still requires it.
        "minimum_cpus_per_node": run.cpus_on(run.nodes[0]),
        "submit_time": iso(run.submit_time or run.start_time),
        "time_limit": run.time_limit or 0,
        "start_time": iso(run.start_time),
        "suspend_time": 0,
    }
    if run.end_time is not None:
        record["end_time"] = iso(run.end_time)
        record["exit_code"] = run.exit_code or 0
    return record


def job_message(cluster: str, at: datetime, runs: list[JobRun]) -> dict[str, Any]:
    return envelope(
        "job",
        {"time": iso(at), "cluster": cluster, "slurm_jobs": [slurm_job_record(r) for r in runs]},
    )


def cluster_message(
    cluster: str, at: datetime, node_states: dict[str, list[str]]
) -> dict[str, Any]:
    """Sonar ``ClusterAttributes``: partitions and nodes grouped by state."""
    partitions = [
        {"name": name, "nodes": [compress_hostlist(cluster_model.partition_nodes(name))]}
        for name in cluster_model.partition_ranges()
    ]
    groups: dict[tuple[str, ...], list[str]] = {}
    for node, states in sorted(node_states.items()):
        groups.setdefault(tuple(states), []).append(node)
    nodes = [
        {"names": [compress_hostlist(names)], "states": list(states)}
        for states, names in groups.items()
    ]
    return envelope(
        "cluster",
        {
            "time": iso(at),
            "cluster": cluster,
            "slurm": True,
            "partitions": partitions,
            "nodes": nodes,
        },
    )


def sample_times(run: JobRun, now: datetime, spacing: float = 300.0) -> list[datetime]:
    """Instants to sample ``run`` at over its lifetime (up to ``now``).

    ``spacing`` for long jobs; a short job gets a dozen samples instead, at
    least 5 s apart, so even a 40-second demo job has a curve.
    """
    end = run.end_time or now
    length = (end - run.start_time).total_seconds()
    if length <= 0:
        return []
    step = timedelta(seconds=max(5.0, min(spacing, length / 12)))
    times: list[datetime] = []
    at = (run.start_time + step / 2).replace(microsecond=0)
    while at < end:
        times.append(at)
        at += step
    return times
