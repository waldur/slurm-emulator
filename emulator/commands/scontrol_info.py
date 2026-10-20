"""``scontrol show partition`` and ``scontrol show job`` records.

* Partitions print as slurm://src/api/partition_info.c#slurm_sprint_partition_info
  does, from the emulator's partition model: every partition is UP, the first
  one is the default, no time or node limits (``part_record_create`` defaults,
  slurm://src/common/part_record.c#_init_part_record), ``DefMemPerCPU`` the
  cluster's node memory / CPUs, AllowQos/DenyQos/QoS from
  ``SLURM_EMULATOR_PARTITION_QOS``.
* Jobs print as slurm://src/scontrol/info_job.c#_sprint_job_info without
  ``-d`` details: ``NumTasks`` falls back to the node count and ``CPUs/Task``
  to 1 as slurmctld packs them (slurm://src/slurmctld/job_mgr.c#_pack_default_job_details),
  ``Reason`` is the state description when there is one (blanks turned into
  ``_``), ``ExitCode`` the wait status split into exit:signal, and the batch
  job's StdErr/StdIn/StdOut have their patterns expanded
  (slurm://src/api/job_info.c#slurm_get_job_stdout). The OverSubscribe line
  gains ``LicensesAlloc`` in 25.05 and ``Exclusive`` in 26.05, ``SubmitLine``
  appears from 25.11
  (slurm://src/scontrol/info_job.c#"OverSubscribe=%s Exclusive=%s Contiguous=%d Licenses=%s LicensesAlloc=%s Network=%s"@26.05+).

Intentional deviations: jobs stay in the controller's list after they end
(no ``MinJobAge`` purge), ``AllocNode:Sid`` is ``localhost:0``, and
``Priority`` is the submitted priority, not a multifactor result.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from emulator.commands.sacct import _convert_exact
from emulator.core import cluster_model, exit_status, job_execution
from emulator.core.cluster_model import (
    DEF_MEM_PER_CPU_MB,
    NODE_CPUS,
    NODE_GPUS,
    NODE_MEM_MB,
    compress_hostlist,
    job_request,
    partition_nodes,
    partition_ranges,
)
from emulator.core.database import Job
from emulator.core.scheduler import expected_end
from emulator.slurm_version import at_least


def mem_str(megabytes: int) -> str:
    """A memory count in a TRES string (``TRES_STR_CONVERT_UNITS``: 512G, 2T, 4000M)."""
    return _convert_exact(megabytes, 2)


def _null(value: Optional[str]) -> str:
    return value if value else "(null)"


def _time_str(value: Optional[datetime]) -> str:
    """slurm://src/common/parse_time.c#slurm_make_time_str: 0 prints ``Unknown``."""
    return value.strftime("%Y-%m-%dT%H:%M:%S") if value else "Unknown"


def secs2time_str(seconds: float) -> str:
    """slurm://src/common/parse_time.c#secs2time_str."""
    seconds = int(seconds)
    if seconds < 0:
        return "INVALID"
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}-{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def mins2time_str(minutes: Optional[int]) -> str:
    """slurm://src/common/parse_time.c#mins2time_str; no limit is ``UNLIMITED``."""
    if minutes is None:
        return "UNLIMITED"
    return secs2time_str(minutes * 60)


# --- partitions ---


def _partition_qos(name: str) -> tuple[str, str]:
    from emulator.api.slurmrestd.schemas import PARTITION_QOS  # noqa: PLC0415 - cycle

    cfg = PARTITION_QOS.get(name) or {}
    allowed, deny, assigned = cfg.get("allowed", ""), cfg.get("deny", ""), cfg.get("assigned", "")
    gate = f"AllowQos={allowed}" if allowed or not deny else f"DenyQos={deny}"
    if not allowed and not deny:
        gate = "AllowQos=ALL"
    return gate, assigned or "N/A"


def partition_tres(name: str) -> str:
    """The partition's TRES (cpu, mem, node, billing, gres), as ``tres_fmt_str``."""
    nodes = len(partition_nodes(name))
    tres = (
        f"cpu={nodes * NODE_CPUS},mem={mem_str(nodes * NODE_MEM_MB)},node={nodes},"
        f"billing={nodes * NODE_CPUS}"
    )
    if NODE_GPUS:
        tres += f",gres/gpu={nodes * NODE_GPUS}"
    return tres


def sprint_partition(name: str, default: bool, one_liner: bool = False) -> str:
    """slurm://src/api/partition_info.c#slurm_sprint_partition_info."""
    sep = " " if one_liner else "\n   "
    nodes = partition_nodes(name)
    gate, qos = _partition_qos(name)
    lines = [
        f"PartitionName={name}",
        f"AllowGroups=ALL AllowAccounts=ALL {gate}",
        f"AllocNodes=ALL Default={'YES' if default else 'NO'} QoS={qos}",
        "DefaultTime=NONE DisableRootJobs=NO Exclusive=NO GraceTime=0 Hidden=NO",
        "MaxNodes=UNLIMITED MaxTime=UNLIMITED MinNodes=1 LLN=NO MaxCPUsPerNode=UNLIMITED "
        "MaxCPUsPerSocket=UNLIMITED",
        f"Nodes={compress_hostlist(nodes)}",
        "PriorityJobFactor=1 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO",
        "OverTimeLimit=NONE PreemptMode=OFF",
        f"State=UP TotalCPUs={len(nodes) * NODE_CPUS} TotalNodes={len(nodes)} "
        "SelectTypeParameters=NONE",
        "JobDefaults=(null)",
        f"DefMemPerCPU={DEF_MEM_PER_CPU_MB} MaxMemPerNode=UNLIMITED",
        f"TRES={partition_tres(name)}",
    ]
    return sep.join(lines) + ("\n" if one_liner else "\n\n")


def partitions() -> list[str]:
    return list(partition_ranges())


# --- jobs ---


def tres_string(counts: dict[str, int]) -> str:
    """``cpu=2,mem=16G,node=1,billing=2,gres/gpu=1,...`` in TRES-id order, as slurmctld prints it."""
    ids = gres_tres_order()
    names = sorted(counts, key=lambda name: (ids.get(name, len(ids)), name))
    return ",".join(
        f"{name}={mem_str(counts[name]) if name == 'mem' else counts[name]}" for name in names
    )


def gres_tres_order() -> dict[str, int]:
    from emulator.api.slurmrestd.schemas import _tres_ids  # noqa: PLC0415 - cycle

    return _tres_ids()


def job_tres(job: Job, allocated: bool) -> str:
    """ReqTRES / AllocTRES of a job in the controller (``cluster_model.job_tres_counts``)."""
    return tres_string(cluster_model.job_tres_counts(job, allocated))


def _num_tasks(job: Job) -> int:
    if job.tasks:
        return job.tasks
    if job.tasks_per_node:
        return job.tasks_per_node * job_request(job).node_count
    return len(job.node_list) or job_request(job).node_count


def _run_time(job: Job, now: datetime) -> float:
    """RunTime: time run so far, without the time spent suspended.

    slurm://src/scontrol/info_job.c#_sprint_job_info
    """
    if job.state == "PENDING" or job.start_time is None:
        return 0.0
    active = job.state in ("RUNNING", "SUSPENDED")
    end = now if active or job.end_time is None else job.end_time
    clock = int(now.timestamp())
    suspended = sum((e or clock) - s for s, e in job.suspend_intervals)
    return max((end - job.start_time).total_seconds() - suspended, 0.0)


def _end_time(job: Job) -> Optional[datetime]:
    if job.end_time:
        return job.end_time
    if job.state in ("RUNNING", "SUSPENDED") and job.time_limit:
        return expected_end(job)
    return None


def sprint_job(job: Job, now: datetime, one_liner: bool = False) -> str:
    """slurm://src/scontrol/info_job.c#_sprint_job_info (no ``-d``)."""
    sep = " " if one_liner else "\n   "
    req = job_request(job)
    uid = job.uid if job.uid is not None else 1000
    gid = job.gid if job.gid is not None else 1000
    reason = (job.state_description or job.state_reason or "None").replace(" ", "_")
    exit_code = exit_status.colon(job.state, job.exit_code)
    started = job.start_time if job.state != "PENDING" else None
    end = _end_time(job) if job.state != "PENDING" else None
    end_str = "Unknown" if job.time_limit is None and end is None else _time_str(end)
    if job.memory_per_node:
        min_memory = f"MinMemoryNode={mem_str(job.memory_per_node)}"
    else:
        min_memory = f"MinMemoryCPU={mem_str(DEF_MEM_PER_CPU_MB)}"
    allocated = bool(job.node_list) and job.state != "PENDING"
    num_nodes = len(job.node_list) if allocated else req.node_count
    num_cpus = (job.alloc_cpus or req.total_cpus) if allocated else req.total_cpus

    lines = [
        f"JobId={job.job_id} JobName={job.name or 'job_' + str(job.job_id)}",
        f"UserId={job.user}({uid}) GroupId={job.group_name or job.user}({gid}) MCS_label=N/A",
        f"Priority={job.priority} Nice=0 Account={job.account} QOS={job.qos or 'normal'}",
        f"JobState={job.state} Reason={reason} Dependency=(null)",
        f"Requeue=1 Restarts=0 BatchFlag=1 Reboot=0 ExitCode={exit_code}",
        f"RunTime={secs2time_str(_run_time(job, now))} "
        f"TimeLimit={mins2time_str(job.time_limit)} TimeMin=N/A",
        f"SubmitTime={_time_str(job.submit_time)} EligibleTime={_time_str(job.submit_time)}",
        f"AccrueTime={_time_str(job.submit_time)}",
        f"StartTime={_time_str(started)} EndTime={end_str} Deadline=N/A",
        f"SuspendTime=None SecsPreSuspend=0 LastSchedEval={_time_str(started or job.submit_time)} "
        "Scheduler=Main",
        f"Partition={job.partition} AllocNode:Sid=localhost:0",
        "ReqNodeList=(null) ExcNodeList=(null)",
        f"NodeList={_null(compress_hostlist(job.node_list) if allocated else '')}",
    ]
    if allocated:
        lines.append(f"BatchHost={job.node_list[0]}")
    lines += [
        f"NumNodes={num_nodes} NumCPUs={num_cpus} NumTasks={_num_tasks(job)} "
        f"CPUs/Task={job.cpus_per_task or 1} ReqB:S:C:T=0:0:*:*",
        f"ReqTRES={job_tres(job, allocated=False)}",
        f"AllocTRES={job_tres(job, allocated=True) if allocated else '(null)'}",
        f"Socks/Node=* NtasksPerN:B:S:C={job.tasks_per_node or 0}:0:*:* CoreSpec=*",
        f"MinCPUsNode={req.cpus_per_node if job.tasks_per_node else job.cpus_per_task or 1} "
        f"{min_memory} MinTmpDiskNode=0",
        "Features=(null) DelayBoot=00:00:00",
    ]
    if job.reservation:
        lines.append(f"Reservation={job.reservation}")
    oversubscribe = "NO" if job.exclusive else "OK"
    if at_least("26.05"):
        lines.append(
            f"OverSubscribe={oversubscribe} Exclusive={'NODE' if job.exclusive else 'NO'} "
            "Contiguous=0 Licenses=(null) LicensesAlloc=(null) Network=(null)"
        )
    elif at_least("25.05"):
        lines.append(
            f"OverSubscribe={oversubscribe} Contiguous=0 Licenses=(null) LicensesAlloc=(null) "
            "Network=(null)"
        )
    else:
        lines.append(f"OverSubscribe={oversubscribe} Contiguous=0 Licenses=(null) Network=(null)")
    # sbatch records the script's path; a script sent inline (REST) has none.
    command = job.script if job.script and "\n" not in job.script else ""
    lines.append(f"Command={_null(command)}")
    if at_least("25.11"):
        lines.append("SubmitLine=(null)")
    lines += [
        f"WorkDir={job_execution.working_directory(job)}",
        f"StdErr={job_execution.stderr_path(job)}",
        f"StdIn={job.standard_input or '/dev/null'}",
        f"StdOut={job_execution.stdout_path(job)}",
    ]
    # The GRES request as given (slurm://src/scontrol/info_job.c#_sprint_job_info).
    if job.tres_per_job:
        lines.append(f"TresPerJob={job.tres_per_job}")
    if job.tres_per_node:
        lines.append(f"TresPerNode={job.tres_per_node}")
    return sep.join(lines) + ("\n" if one_liner else "\n\n")
