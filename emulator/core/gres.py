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
    """
    return ["gres/gpu", *(f"gres/gpu:{t}" for t in gpu_types()), "gres/gpumem", "gres/gpuutil"]


def partition_threads_per_core(partition: str) -> int:
    value = (_entries(THREADS_PER_CORE_ENV) or {}).get(partition, "")
    return int(value) if value.isdigit() and int(value) > 0 else 1


@dataclass(frozen=True)
class GpuRequest:
    """``count`` GPUs per node (``per_job``: in all), of ``gpu_type`` or any."""

    count: int
    gpu_type: str = ""
    per_job: bool = False


_GRES_RE = re.compile(r"^(?:gres/|gres:)?gpu(?::(?P<type>[^:=]+))?[:=](?P<count>\d+)$")


def parse_gpu_request(tres: str, per_job: bool = False) -> Optional[GpuRequest]:
    """``gres/gpu:2``, ``gres/gpu:h200:2`` (also ``gpu:2``) -> a GPU request."""
    for item in (tres or "").split(","):
        match = _GRES_RE.match(item.strip())
        if match and int(match["count"]) > 0:
            return GpuRequest(int(match["count"]), match["type"] or "", per_job)
    return None
