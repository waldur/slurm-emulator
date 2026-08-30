"""TRES ids shared by the sacctmgr and slurmrestd planes.

Real slurmdbd assigns the static TRES fixed ids (``tres_types_t``:
cpu=1, mem=2, energy=3, node=4, billing=5, fs/disk=6, vmem=7, pages=8;
slurm://src/common/slurmdb_defs.h#TRES_ENERGY) and every dynamic TRES
(``gres/*``, ``license/*``, …) an id from ``TRES_STATIC_CNT`` + 1 = 1001
upwards in creation order. ``sacctmgr show tres`` and
``GET /slurmdb/v0.0.4x/tres/`` must print the same ids for the same
database, so both derive them here.
"""

from __future__ import annotations

STATIC_TRES_IDS: dict[str, int] = {
    "cpu": 1,
    "mem": 2,
    "energy": 3,
    "node": 4,
    "billing": 5,
    "fs/disk": 6,
    "vmem": 7,
    "pages": 8,
}

DYNAMIC_TRES_FIRST_ID = 1001


def tres_id(name: str, tres_types: list[str]) -> int:
    """Id of ``name`` (case-insensitive) given the database's TRES list."""
    lower = name.lower()
    if lower in STATIC_TRES_IDS:
        return STATIC_TRES_IDS[lower]
    dynamic = [t.lower() for t in tres_types if t.lower() not in STATIC_TRES_IDS]
    if lower in dynamic:
        return DYNAMIC_TRES_FIRST_ID + dynamic.index(lower)
    return 0
