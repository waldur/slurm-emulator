"""Per-node power model that turns node-hours into consumed energy (joules).

Real Slurm gets ``energy`` from ``acct_gather_energy`` plugins on the
compute nodes and stores it as the ``energy`` TRES (``TRES_ENERGY``,
slurm://src/common/slurmdb_defs.h#TRES_ENERGY) — for a job in ``tres_alloc_str``
(read back by ``sacct ConsumedEnergyRaw``,
slurm://src/sacct/print.c#PRINT_CONSUMED_ENERGY_RAW) and, rolled up per
association, in the assoc usage tables that ``sreport … -T energy``
reports. The emulator has no nodes, so it derives the figure from a
constant power model instead:

    joules = node_hours * 3600 * node_watts(partition)
           + gpu_hours  * 3600 * gpu_watts

Configuration (environment):

* ``SLURM_EMULATOR_NODE_POWER_W`` — watts per node, default 500;
* ``SLURM_EMULATOR_PARTITION_POWER_W`` — per-partition overrides,
  ``compute=400,gpu=900``; partitions not listed fall back to the
  default above;
* ``SLURM_EMULATOR_GPU_POWER_W`` — additional watts per allocated GPU,
  default 300, applied only when the record carries ``GRES/gpu`` hours.

An explicit energy value (``POST /api/submit-report`` with ``energy``)
bypasses the model so scenarios can seed exact monthly totals.
"""

from __future__ import annotations

import os

DEFAULT_NODE_POWER_W = 500.0
DEFAULT_GPU_POWER_W = 300.0
DEFAULT_PARTITION = "compute"


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def partition_power_table() -> dict[str, float]:
    """Parse ``SLURM_EMULATOR_PARTITION_POWER_W`` (``name=watts,...``)."""
    table: dict[str, float] = {}
    for item in os.environ.get("SLURM_EMULATOR_PARTITION_POWER_W", "").split(","):
        if "=" not in item:
            continue
        name, _, value = item.partition("=")
        try:
            table[name.strip()] = float(value)
        except ValueError:
            continue
    return table


def node_power_w(partition: str | None = None) -> float:
    """Watts per node for ``partition`` (default node power when unlisted)."""
    default = _env_float("SLURM_EMULATOR_NODE_POWER_W", DEFAULT_NODE_POWER_W)
    if partition:
        return partition_power_table().get(partition, default)
    return default


def gpu_power_w() -> float:
    return _env_float("SLURM_EMULATOR_GPU_POWER_W", DEFAULT_GPU_POWER_W)


def energy_joules(node_hours: float, gpu_hours: float = 0.0, partition: str | None = None) -> int:
    """Energy for ``node_hours`` (+ ``gpu_hours`` GPU-hours) in whole joules."""
    joules = node_hours * 3600.0 * node_power_w(partition)
    if gpu_hours > 0:
        joules += gpu_hours * 3600.0 * gpu_power_w()
    return round(joules)
