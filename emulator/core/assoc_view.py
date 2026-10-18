"""What an association row says once slurmdbd has filled in inherited values.

Shared by the slurmrestd and sacctmgr planes so both show the same row.
slurmdbd returns a row's own Grp* limits and shares, and fills every Max*
limit, the QoS list and the default QoS it does not set itself from the
rows above it -- the user row's account, that account's parent, up to root
(slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_cluster_get_assocs).
The account-level row keeps its values on the Account record (where
sacctmgr writes them), a user row on its Association.
"""

from __future__ import annotations

from typing import Any, Optional

from emulator.core.database import Account, Association

# slurm://slurm/slurmdb.h#SLURMDB_FS_USE_PARENT: Fairshare=parent.
FS_USE_PARENT = 0x7FFFFFFF


def limits_tres(limits: dict[str, int], prefix: str) -> dict[str, int]:
    """Collect ``Prefix:<tres>`` limit keys; a bare ``Prefix`` is billing."""
    out: dict[str, int] = {}
    for key, value in limits.items():
        if key == prefix:
            out["billing"] = int(value)
        elif key.startswith(f"{prefix}:"):
            out[key.split(":", 1)[1].lower()] = int(value)
    return out


def row_values(
    assoc: Association, account: Optional[Account]
) -> tuple[dict[str, int], list[str], str]:
    """The limits, QoS list and default QoS stored on this row itself."""
    if not assoc.user:
        if account is None:
            return {}, [], ""
        return account.limits, [q for q in account.qos.split(",") if q], account.default_qos
    return assoc.limits, list(assoc.qos_list), assoc.def_qos


def ancestors(
    assoc: Association, account: Optional[Account], accounts: Optional[dict[str, Account]]
) -> list[Account]:
    """Account rows above this row, nearest first (the user row's own account first)."""
    chain: list[Account] = []
    current = account if assoc.user else None
    if not assoc.user and account is not None and account.parent and accounts is not None:
        current = accounts.get(account.parent)
    while current is not None and current not in chain:
        chain.append(current)
        if not current.parent or accounts is None:
            break
        current = accounts.get(current.parent)
    return chain


INHERITED_TRES = ("MaxTRES", "MaxTRESPerNode", "MaxTRESMins", "MaxTRESRunMins")
INHERITED_SCALARS = (
    "MaxJobs",
    "MaxJobsAccrue",
    "MaxSubmitJobs",
    "MaxWall",
    "Priority",
    "MinPrioThresh",
)
OWN_TRES = ("GrpTRES", "GrpTRESMins", "GrpTRESRunMins")
OWN_SCALARS = ("GrpJobs", "GrpJobsAccrue", "GrpSubmitJobs", "GrpWall")


def effective_assoc(
    assoc: Association, account: Optional[Account], accounts: Optional[dict[str, Account]] = None
) -> dict[str, Any]:
    """The row as slurmdbd returns it: own values, inherited ones filled in.

    TRES limits combine per TRES, the nearest value winning
    (``slurmdb_combine_tres_strings``).
    """
    limits, qos_list, def_qos = row_values(assoc, account)
    chain = ancestors(assoc, account, accounts)
    tres = {prefix: limits_tres(limits, prefix) for prefix in (*OWN_TRES, *INHERITED_TRES)}
    scalars: dict[str, Optional[int]] = {
        key: limits.get(key) for key in (*OWN_SCALARS, *INHERITED_SCALARS)
    }
    for parent in chain:
        for prefix in INHERITED_TRES:
            for tres_type, count in limits_tres(parent.limits, prefix).items():
                tres[prefix].setdefault(tres_type, count)
        for key in INHERITED_SCALARS:
            if scalars[key] is None and key in parent.limits:
                scalars[key] = parent.limits[key]
        if not qos_list:
            qos_list = [q for q in parent.qos.split(",") if q]
        if not def_qos:
            def_qos = parent.default_qos
    shares = assoc.fairshare if assoc.user else (account.fairshare if account else 1)
    return {"tres": tres, "scalars": scalars, "qos": qos_list, "def_qos": def_qos, "shares": shares}


_MEM_LIKE = ("mem", "gres/gpumem")
_UNITS = "MGTPE"


def _mem_units(count: int) -> str:
    """convert_num_unit(UNIT_MEGA, CONVERT_NUM_UNIT_EXACT): step up while exact."""
    unit = 0
    while count >= 1024 and count % 1024 == 0 and unit < len(_UNITS) - 1:
        count //= 1024
        unit += 1
    return f"{count}{_UNITS[unit]}"


def format_tres(values: dict[str, int]) -> str:
    """A TRES string as sacctmgr prints it.

    Names in alphabetical order (the string is built as a char list and
    collated), memory-like counts with binary units
    (slurm://src/common/slurmdb_defs.c#slurmdb_make_tres_string_from_simple).
    """
    parts = []
    for name in sorted(values):
        count = int(values[name])
        if name in _MEM_LIKE or name.startswith("bb/"):
            parts.append(f"{name}={_mem_units(count)}")
        else:
            parts.append(f"{name}={count}")
    return ",".join(parts)
