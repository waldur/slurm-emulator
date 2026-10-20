"""GPUs (GRES) and hardware threads of the emulated nodes, and GPU requests.

Every node is the standard node (``emulator/core/cluster_model.py``); this
module says which of them carry GPUs, of which type, and how many hardware
threads a core has -- what slurm.conf's ``Gres=``/``ThreadsPerCore=`` and
gres.conf's ``Type=`` say on a real cluster:

* ``SLURM_EMULATOR_GPU_TYPES`` -- ``gpu=h200,debug``: the partitions whose
  nodes have ``NODE_GPUS`` GPUs, with their type (none after a bare name).
  Unset, partitions named like ``gpu`` have untyped GPUs, as before.
  A configured type is also tracked in accounting, as
  ``AccountingStorageTRES=gres/gpu,gres/gpu:<type>`` would
  (slurm://src/stepmgr/gres_stepmgr.c#_set_type_tres_cnt).
* ``SLURM_EMULATOR_THREADS_PER_CORE`` -- ``gpu=2``: threads per core of a
  partition's nodes (1 when not listed). ``SelectTypeParameters=CR_Core_Memory``
  hands out whole cores, so a job asking for 1 CPU there gets 2.

A job asks for GPUs with ``--gres=gpu[:type]:N`` (``tres_per_node``) or
``--gpus=[type:]N`` (``tres_per_job``), as job_desc carries them
(slurm://slurm/slurm.h#job_desc_msg_t.tres_per_node).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Optional

NODE_GPUS = 4

GPU_TYPES_ENV = "SLURM_EMULATOR_GPU_TYPES"
THREADS_PER_CORE_ENV = "SLURM_EMULATOR_THREADS_PER_CORE"
# Further TRES the cluster tracks, as slurm.conf's AccountingStorageTRES lists
# them (slurm://src/plugins/accounting_storage/mysql/accounting_storage_mysql.c#acct_storage_p_add_tres);
# without them sacctmgr refuses limits on those names.
ACCOUNTING_STORAGE_TRES_ENV = "SLURM_EMULATOR_ACCOUNTING_STORAGE_TRES"


def _entries(env: str) -> Optional[dict[str, str]]:
    raw = os.environ.get(env, "").strip()
    if not raw:
        return None
    entries: dict[str, str] = {}
    for item in raw.split(","):
        name, _, value = item.strip().partition("=")
        if name:
            entries[name] = value.strip()
    return entries


def partition_has_gpus(partition: str) -> bool:
    """Whether the nodes of ``partition`` carry GPUs."""
    configured = _entries(GPU_TYPES_ENV)
    if configured is None:
        return "gpu" in (partition or "").lower()
    return partition in configured


def partition_gpu_type(partition: str) -> str:
    """The GPU type of ``partition``'s nodes; "" for untyped GPUs or none."""
    return (_entries(GPU_TYPES_ENV) or {}).get(partition, "")


def gpu_types() -> list[str]:
    """Configured GPU types, in configuration order, each once."""
    types: list[str] = []
    for value in (_entries(GPU_TYPES_ENV) or {}).values():
        if value and value not in types:
            types.append(value)
    return types


def gpu_tres_names() -> list[str]:
    """The GPU TRES accounting tracks, in the order slurmdbd creates them.

    ``gres/gpu`` and its typed counterparts first, then what the GPU plugin
    adds for it (slurm://src/interfaces/gpu.c#gpu_get_tres_pos) -- the order
    a 25.11 cluster gave them ids 1001.. in.

    Intentional deviation: the ids follow the current configuration, so
    adding a GPU type to an existing state moves ``gres/gpumem`` and
    ``gres/gpuutil`` up by one; slurmdbd keeps existing ids and appends new
    TRES at the next free one. Configure GPU types before creating limits
    by id.
    """
    return ["gres/gpu", *(f"gres/gpu:{t}" for t in gpu_types()), "gres/gpumem", "gres/gpuutil"]


def extra_tres_names() -> list[str]:
    """``SLURM_EMULATOR_ACCOUNTING_STORAGE_TRES`` -- e.g. ``ram,gpu,storage`` -- in order, each once.

    Names the cluster tracks anyway -- the static TRES slurmdbd always has
    and the GPU TRES -- are skipped, case-insensitively, so a list copied
    from a real slurm.conf (``cpu,mem,node,billing,gres/gpu,...``) only adds
    what is new.

    Intentional deviations: real slurmctld accepts only known TRES kinds
    here (``gres/*``, ``license/*``, ``bb/*``, ``fs/lustre``, ``ic/ofed``,
    and the static ones); the emulator takes any name, so test
    configurations with ``ram``-style names keep working. Like the GPU
    TRES, the ids follow the current list: adding a GPU type or changing
    the list moves them, where slurmdbd keeps ids and appends new TRES.
    """
    from emulator.core.tres import STATIC_TRES_IDS  # noqa: PLC0415

    known = set(STATIC_TRES_IDS) | {name.lower() for name in gpu_tres_names()}
    names: list[str] = []
    for item in os.environ.get(ACCOUNTING_STORAGE_TRES_ENV, "").split(","):
        name = item.strip().lower()
        if name and name not in known:
            known.add(name)
            names.append(name)
    return names


def dynamic_tres_names() -> list[str]:
    """Every dynamic TRES (ids from 1001): the GPU TRES, then the extra ones."""
    return [*gpu_tres_names(), *extra_tres_names()]


def partition_threads_per_core(partition: str) -> int:
    value = (_entries(THREADS_PER_CORE_ENV) or {}).get(partition, "")
    return int(value) if value.isdigit() and int(value) > 0 else 1


@dataclass(frozen=True)
class GpuRequest:
    """``count`` GPUs per node (``per_job``: in all), of ``gpu_type`` or any."""

    count: int
    gpu_type: str = ""
    per_job: bool = False


_GRES_RE = re.compile(r"^(?:gres/|gres:)?gpu(?::(?P<type>[^:=\d][^:=]*))?(?:[:=](?P<count>\d+))?$")


def parse_gpu_request(tres: str, per_job: bool = False) -> Optional[GpuRequest]:
    """``gres/gpu:2``, ``gres/gpu:h200:2``, ``gpu`` (one GPU) -> a GPU request.

    A GRES without a count asks for one, as a 25.11 cluster recorded
    ``--gres=gpu`` and ``--gres=gpu:h200`` with gres/gpu=1
    (slurm://src/interfaces/gres.c#gres_job_state_validate).
    """
    for item in (tres or "").split(","):
        match = _GRES_RE.match(item.strip())
        if not match:
            continue
        count = int(match["count"]) if match["count"] is not None else 1
        if count > 0:
            return GpuRequest(count, match["type"] or "", per_job)
    return None
