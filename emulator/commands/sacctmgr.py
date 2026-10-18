"""sacctmgr command emulator.

Output formatting and exit codes mirror real Slurm 26.05:

* list/show output defaults to fixed-width columns with a dashed
  underline (``slurm://src/common/print_fields.c``); ``-p``/``--parsable``
  pipe-separates with a trailing ``|``, ``-P``/``--parsable2`` without,
  ``-n``/``--noheader`` drops the header;
* field names, printed headers, and column widths come from the
  ``sacctmgr_process_format_list`` chain in ``src/sacctmgr/common.c``
  (line refs below); default field sets per entity come from each
  ``sacctmgr/*_functions.c``;
* errors print with a leading-space `` error: ...`` prefix and exit 1
  (the dispatcher routes failing output to stderr); ``Nothing
  modified`` goes to stdout but exits 1 (slurm://src/sacctmgr/account_functions.c#sacctmgr_modify_account
  returns SLURM_ERROR, which slurm://src/sacctmgr/sacctmgr.c#_modify_it
  turns into the global ``exit_code``);
* re-adding an existing account reports ``SLURM_NO_CHANGE_IN_DATA``:
  `` Data has not changed since time specified`` on stdout, exit 0
  (slurm://src/sacctmgr/account_functions.c#sacctmgr_add_account, slurm://src/common/slurm_errno.c#SLURM_NO_CHANGE_IN_DATA).

Intentional deviations: no interactive commit prompt (``-i`` is an
accepted no-op — the emulator is headless), and a leading ``-M
<cluster>`` is tolerated and ignored for waldur-site-agent
compatibility (real sacctmgr has no ``-M``). A user has one default
account, not one per cluster: ``modify user … set DefaultAccount=``
checks every cluster as real sacctmgr does, but where slurmdbd flags the
user's association on each of those clusters as the default (``is_def``,
slurm://src/plugins/accounting_storage/mysql/as_mysql_user.c#as_mysql_modify_users)
the emulator moves that single default. The ``default`` cluster the
emulator always seeds is left out of "every cluster" when
``SLURM_EMULATOR_CLUSTER_NAME`` names another one — a real database has
no such placeholder.
"""

import dataclasses
from typing import Optional

from emulator.commands.print_fields import (
    FieldSpec,
    OutputMode,
    UnknownFieldError,
    extract_output_flags,
    keyword_match,
    parse_format_spec,
    render_table,
    resolve_format,
)
from emulator.core import nss
from emulator.core.assoc_view import FS_USE_PARENT, effective_assoc, format_tres
from emulator.core.database import (
    QOS,
    Association,
    ClusterClassification,
    SlurmDatabase,
    fold_account,
)
from emulator.core.time_engine import TimeEngine
from emulator.core.tres import STATIC_TRES_IDS, tres_id
from emulator.slurm_version import current

# Field registry mirroring the prefix-match chain in
# slurm://src/sacctmgr/common.c#_get_print_field. Order matters: tokens resolve to the
# first entry they prefix-match (e.g. "Cl" must hit Clusters, not
# Classification, because Classification needs 3 chars). Alias entries
# (e.g. Acct) carry the canonical printed header so they share a column.
_REGISTRY: list[FieldSpec] = [
    FieldSpec("Account", 10, min_prefix=3),
    FieldSpec("Acct", 10, header="Account", min_prefix=4),
    FieldSpec("AdminLevel", 9, header="Admin", min_prefix=2),
    FieldSpec("Classification", 9, header="Class", min_prefix=3),
    FieldSpec("Clusters", 10, header="Cluster", min_prefix=2),
    FieldSpec("ControlHost", 15, min_prefix=8),
    FieldSpec("ControlPort", 12, min_prefix=8),
    FieldSpec("DefaultAccount", 10, header="Def Acct", min_prefix=8),
    FieldSpec("DefaultQOS", 9, header="Def QOS", min_prefix=8),
    FieldSpec("Description", 20, header="Descr", min_prefix=3),
    FieldSpec("Flags", 20, min_prefix=2),
    FieldSpec("GraceTime", 10, min_prefix=3),
    FieldSpec("GrpCPUs", 8, min_prefix=6),
    FieldSpec("GrpCPUMins", 11, min_prefix=7),
    FieldSpec("GrpTRES", 13, min_prefix=7),
    FieldSpec("GrpTRESMins", 13, min_prefix=7),
    FieldSpec("GrpTRESRunMins", 13, min_prefix=8),
    FieldSpec("GrpJobs", 7, min_prefix=4),
    FieldSpec("GrpMemory", 7, header="GrpMem", min_prefix=4),
    FieldSpec("GrpNodes", 8, min_prefix=4),
    FieldSpec("GrpSubmitJobs", 9, header="GrpSubmit", min_prefix=4),
    FieldSpec("GrpWall", 11, min_prefix=4),
    FieldSpec("ID", 6, min_prefix=2),
    FieldSpec("MaxCPUMinsPerJob", 11, header="MaxCPUMins", min_prefix=7),
    FieldSpec("MaxCPUsPerJob", 8, header="MaxCPUs", min_prefix=6),
    FieldSpec("MaxTRES", 13, min_prefix=7),
    FieldSpec("MaxTRESPerJob", 13, header="MaxTRES", min_prefix=11),
    FieldSpec("MaxTRESPerNode", 14, min_prefix=11),
    FieldSpec("MaxTRESPN", 14, header="MaxTRESPerNode", min_prefix=9),
    FieldSpec("MaxTRESMinsPerJob", 13, header="MaxTRESMins", min_prefix=8),
    FieldSpec("MaxTRESMinsPJ", 13, header="MaxTRESMins", min_prefix=13),
    FieldSpec("MaxTRESRunMinsPerAccount", 16, header="MaxTRESRunMinsPA", min_prefix=18),
    FieldSpec("MaxTRESRunMinsPerAcct", 16, header="MaxTRESRunMinsPA", min_prefix=18),
    FieldSpec("MaxTRESRunMinsPA", 16, min_prefix=15),
    FieldSpec("MaxTRESRunMinsPerUser", 16, header="MaxTRESRunMinsPU", min_prefix=8),
    FieldSpec("MaxTRESRunMinsPU", 16, min_prefix=8),
    FieldSpec("MaxTRESPerAccount", 13, header="MaxTRESPA", min_prefix=11),
    FieldSpec("MaxTRESPerAcct", 13, header="MaxTRESPA", min_prefix=11),
    FieldSpec("MaxTRESPA", 13, min_prefix=9),
    FieldSpec("MaxTRESPerUser", 13, header="MaxTRESPU", min_prefix=11),
    FieldSpec("MaxTRESPU", 13, min_prefix=9),
    FieldSpec("MaxJobs", 7, min_prefix=4),
    FieldSpec("MaxJobsPerAccount", 9, header="MaxJobsPA", min_prefix=8),
    FieldSpec("MaxJobsPerAcct", 9, header="MaxJobsPA", min_prefix=8),
    FieldSpec("MaxJobsPA", 9, min_prefix=8),
    FieldSpec("MaxJobsPerUser", 9, header="MaxJobsPU", min_prefix=8),
    FieldSpec("MaxJobsPU", 9, min_prefix=8),
    FieldSpec("MaxNodesPerJob", 8, header="MaxNodes", min_prefix=4),
    FieldSpec("MaxSubmitJobs", 9, header="MaxSubmit", min_prefix=4),
    FieldSpec("MaxSubmitJobsPerAccount", 11, header="MaxSubmitPA", min_prefix=11),
    FieldSpec("MaxSubmitJobsPerAcct", 11, header="MaxSubmitPA", min_prefix=11),
    FieldSpec("MaxSubmitPA", 11, min_prefix=10),
    FieldSpec("MaxSubmitJobsPerUser", 11, header="MaxSubmitPU", min_prefix=11),
    FieldSpec("MaxSubmitPU", 11, min_prefix=10),
    FieldSpec("MaxWallDurationPerJob", 11, header="MaxWall", min_prefix=4),
    FieldSpec("MinTRESPerJob", 13, header="MinTRES", min_prefix=7),
    FieldSpec("Name", 10, min_prefix=2),
    FieldSpec("Organization", 20, header="Org", min_prefix=1),
    FieldSpec("ParentName", 10, min_prefix=7),
    FieldSpec("Partition", 10, min_prefix=4),
    FieldSpec("PreemptMode", 11, min_prefix=8),
    FieldSpec("Preempt", 10, min_prefix=7),
    FieldSpec("PreemptExemptTime", 19, min_prefix=8),
    FieldSpec("Priority", 10, min_prefix=3),
    FieldSpec("QOSLevel", 20, header="QOS", min_prefix=3),
    FieldSpec("RPC", 5, min_prefix=1),
    FieldSpec("Share", 9, min_prefix=1),
    FieldSpec("FairShare", 9, header="Share", min_prefix=2),
    FieldSpec("Type", 8, min_prefix=2),
    FieldSpec("UsageFactor", 11, min_prefix=6),
    FieldSpec("UsageThreshold", 10, header="UsageThres", min_prefix=6),
    FieldSpec("User", 10, min_prefix=1),
]

# Default format strings, verbatim from real sacctmgr.
_ACCOUNT_DEFAULT = "Acc,Des,O"  # slurm://src/sacctmgr/account_functions.c#sacctmgr_list_account
_ACCOUNT_WITHASSOC = (  # slurm://src/sacctmgr/account_functions.c#sacctmgr_list_account
    "Cl,ParentN,U,Share,Priority,GrpJ,GrpN,GrpCPUs,GrpMEM,GrpS,GrpWall,"
    "GrpCPUMins,MaxJ,MaxN,MaxCPUs,MaxS,MaxW,MaxCPUMins,QOS,DefaultQOS"
)
_USER_DEFAULT = "U,DefaultA,Ad"  # slurm://src/sacctmgr/user_functions.c#sacctmgr_list_user
_ASSOC_DEFAULT = (  # slurm://src/sacctmgr/association_functions.c#sacctmgr_list_assoc
    "Cluster,Account,User,Part,Share,Priority,GrpJ,GrpTRES,GrpS,GrpWall,"
    "GrpTRESMins,MaxJ,MaxTRES,MaxTRESPerN,MaxS,MaxW,MaxTRESMins,QOS,"
    "DefaultQOS,GrpTRESRunMins"
)
_CLUSTER_DEFAULT = (  # slurm://src/sacctmgr/cluster_functions.c#sacctmgr_list_cluster
    "Cl,Controlh,Controlp,RPC,Fa,GrpJ,GrpTRES,GrpS,MaxJ,MaxTRES,MaxS,MaxW,QOS,DefaultQOS"
)
_QOS_DEFAULT = (  # slurm://src/sacctmgr/qos_functions.c#sacctmgr_list_qos
    "Name,Prio,GraceT,Preempt,PreemptE,PreemptM,Flags%40,UsageThres,"
    "UsageFactor,GrpTRES,GrpTRESMins,GrpTRESRunMins,GrpJ,GrpS,GrpW,"
    "MaxTRES,MaxTRESPerN,MaxTRESMins,MaxW,MaxTRESPerUser,MaxJobsPerUser,"
    "MaxSubmitJobsPerUser,MaxTRESPerAcct,MaxTRESRunMinsPerAcct%22,"
    "MaxTRESRunMinsPerUser%22,MaxJobsPerAcct,MaxSubmitJobsPerAcct,MinTRES"
)
_TRES_DEFAULT = "Type,Name%15,ID"  # slurm://src/sacctmgr/tres_function.c#sacctmgr_list_tres


def _split_list_operator(key: str) -> tuple[str, str]:
    """Split a set-clause key into (base_key, operator).

    Real sacctmgr list-valued fields (QosLevel, account ``qos``) accept the
    ``=`` (replace), ``+=`` (add) and ``-=`` (remove) operators handled by
    ``slurm_addto_char_list_with_case`` (slurm://src/common/slurm_protocol_defs.c#slurm_addto_char_list_with_case) via the
    trailing ``+``/``-`` on the token. Returns e.g. ("qos", "+") for
    ``qos+``. A plain key returns ("qos", "").
    """
    if key.endswith("+"):
        return key[:-1], "+"
    if key.endswith("-"):
        return key[:-1], "-"
    return key, ""


# TRES ids live in emulator/core/tres.py (shared with slurmrestd).
_STATIC_TRES_IDS = STATIC_TRES_IDS


class UnknownTresError(ValueError):
    """A TRES name that ``slurmdb_format_tres_str`` cannot resolve to an id."""


def _tres_sort_key(name: str, known: list[str]) -> tuple[int, int]:
    """Order TRES the way ``TRES_STR_FLAG_SORT_ID`` does: by TRES id."""
    if name in _STATIC_TRES_IDS:
        return (0, _STATIC_TRES_IDS[name])
    lowered = [k.lower() for k in known]
    return (1, lowered.index(name) if name in lowered else len(lowered))


def _combine_tres_string(current: str, incoming: str, known_tres: list[str]) -> str:
    """Merge a ``GrpTRESMins``-style TRES string the way sacctmgr + slurmdbd do.

    Real ``sacctmgr modify qos … set GrpTRESMins=cpu=1`` does **not** replace
    the stored list. The client resolves names to TRES ids with
    ``slurmdb_format_tres_str`` (slurm://src/sacctmgr/qos_functions.c#_set_rec →
    ``sacctmgr_set_tres_rec_field``) — an unknown name is
    ``error: no TRES known by type <name>``, exit 1, nothing modified — and
    only concatenates the values from that one command line
    (``TRES_STR_FLAG_REPLACE``). slurmdbd then folds the new string onto the
    stored row: slurm://src/plugins/accounting_storage/mysql/accounting_storage_mysql.c#mod_tres_str starts from the
    *new* string and appends the old one with ``TRES_STR_FLAG_REMOVE``;
    ``slurmdb_tres_list_from_string`` keeps the first occurrence (the new
    count) and ``slurmdb_make_tres_string`` drops any TRES whose count is
    ``INFINITE64`` (``-1``) under REMOVE.

    Net effect: named TRES are updated, unnamed TRES survive, ``-1`` removes.
    Names are stored canonical lower-case (ids on the real side) and the
    result is ordered by TRES id (``TRES_STR_FLAG_SORT_ID``), which is also
    what ``sacctmgr_print_tres`` renders — plain integers for every TRES
    except ``mem``-like ones (``convert_num_unit`` UNIT_MEGA, not modelled).
    """
    lowered_known = {k.lower() for k in known_tres}
    merged: dict[str, str] = {}
    for part in current.split(","):
        name, sep, count = part.partition("=")
        if sep and name.strip():
            merged[name.strip().lower()] = count.strip()
    for part in incoming.split(","):
        name, sep, count = part.partition("=")
        if not sep or not name.strip():
            continue
        name = name.strip().lower()
        if name not in lowered_known:
            raise UnknownTresError(name)
        count = count.strip()
        if count == "-1":
            merged.pop(name, None)
        else:
            merged[name] = count
    ordered = sorted(merged, key=lambda n: _tres_sort_key(n, known_tres))
    return ",".join(f"{n}={merged[n]}" for n in ordered)


def _parse_raw_usage(value: str) -> float:
    """Parse a ``RawUsage=`` value the way slurm://src/sacctmgr/qos_functions.c#_set_rec does.

    ``get_double`` maps a negative number to INFINITE, which ``_set_rec``
    then rejects — so anything that is not a non-negative number is
    ``Bad RawUsage value``.
    """
    usage = float(value)
    if usage < 0:
        raise ValueError(value)
    return usage


def _set_qos_field(qos: QOS, key: str, value: str, known_tres: list[str]) -> bool:
    """Apply a single ``key=value`` QoS attribute; return whether it was known.

    Shared by ``add qos`` (which errors on an unknown key) and ``modify qos``
    (which ignores one), so both accept the same field set. Keys are the
    lower-cased sacctmgr option names (slurm://src/sacctmgr/qos_functions.c#_set_rec).
    """
    if key == "flags":
        qos.flags = value
    elif key == "grptres":
        qos.grp_tres = value
    elif key == "grptresmins":
        qos.grp_tres_mins = _combine_tres_string(qos.grp_tres_mins, value, known_tres)
    elif key == "rawusage":
        # Parsed by _set_rec (so a bad value fails the command) but *not*
        # stored through the QoS record: sacctmgr_modify_qos routes it to
        # sacctmgr_update_qos_usage instead, and sacctmgr_add_qos ignores
        # ``qos->usage`` entirely. Callers handle the field themselves.
        _parse_raw_usage(value)
    elif key == "maxjobs":
        qos.max_jobs = int(value)
    elif key == "maxsubmit":
        qos.max_submit = int(value)
    elif key == "maxwall":
        qos.max_wall = value
    elif key == "mintresperjob":
        qos.min_tres_per_job = value
    elif key in {"maxtres", "maxtresperjob"}:
        qos.max_tres_per_job = value
    elif key in {"maxtrespernode", "maxtrespn"}:
        qos.max_tres_per_node = value
    elif key in {"maxtresperuser", "maxtrespu"}:
        qos.max_tres_per_user = value
    elif key == "priority":
        qos.priority = int(value)
    elif key == "gracetime":
        qos.grace_time = int(value)
    else:
        return False
    return True


def _apply_list_operator(current: list[str], operator: str, value: str) -> list[str]:
    """Apply a list operator to a CSV grant, preserving order and dropping dups.

    ``operator`` is "" (replace), "+" (add missing), or "-" (remove present).
    ``value`` is a comma-separated list. Mirrors real sacctmgr where ``=``
    replaces the whole list and ``+=``/``-=`` add/remove members.
    """
    incoming = [v for v in value.split(",") if v]
    if operator == "":
        result: list[str] = []
        for v in incoming:
            if v not in result:
                result.append(v)
        return result
    result = list(current)
    if operator == "+":
        for v in incoming:
            if v not in result:
                result.append(v)
    elif operator == "-":
        result = [v for v in result if v not in incoming]
    return result


# slurmdb_qos_flags_map in its order
# (slurm://src/common/slurmdb_defs.c#_str_2_qos_flags); "Deleted" and
# "PartQOS" are internal and refused.
_QOS_FLAGS = (
    "DenyOnLimit",
    "EnforceUsageThreshold",
    "PartitionMinNodes",
    "PartitionMaxNodes",
    "PartitionTimeLimit",
    "RequiresReservation",
    "OverPartQOS",
    "NoReserve",
    "NoDecay",
    "Relative",
    "UsageFactorSafe",
)


def _qos_flags(value: str) -> Optional[str]:
    """Canonical flag list for ``Flags=``, None when a token is not a flag.

    Each comma token is a case-insensitive prefix of a flag name, first match
    wins (slurm://src/common/slurmdb_defs.c#str_2_qos_flags); ``-1`` clears.
    """
    if value.strip() == "-1":
        return ""
    names: list[str] = []
    for token in value.split(","):
        if not token:
            continue
        if token.lower() in ("deleted", "partqos"):
            return None
        match = next((f for f in _QOS_FLAGS if f.lower().startswith(token.lower())), None)
        if match is None:
            return None
        if match not in names:
            names.append(match)
    return ",".join(names) if names else None


def _strip_quotes(value: str, make_lower: bool) -> str:
    """slurm://src/sacctmgr/common.c#strip_quotes.

    Drops one leading quote and everything from its partner on, skips
    leading and trailing spaces, turns other quotes into backticks and, when
    asked, lower-cases.
    """
    quote = value[:1] if value[:1] in ('"', "'") else ""
    body = value[1:] if quote else value
    if quote and quote in body:
        body = body[: body.index(quote)]
    body = body.replace('"', "`").replace("'", "`").strip()
    return body.lower() if make_lower else body


def _add_assoc_line(cluster: str, account: str, user: str = "", partition: str = "") -> str:
    """An added association as slurmdbd reports it.

    (slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_add_assoc_internal).
    """
    line = f"  C = {cluster:<10} A = {account:<20}"
    if user:
        line += f" U = {user:<9}"
        if partition:
            line += f" P = {partition}"
    return line


def _modified_user_line(assoc: Association) -> str:
    """A modified user association (``_process_modify_assoc_results`` /.

    ``_modify_child_assocs``: ``C = %-10s A = %-20s U = %-9s``).
    """
    line = f"C = {assoc.cluster:<10} A = {assoc.account:<20} U = {assoc.user:<9}"
    if assoc.partition:
        line += f" P = {assoc.partition}"
    return line


def _removed_user_line(assoc: Association) -> str:
    """A removed user association.

    (slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_process_remove_assoc_results).
    """
    line = f"C = {assoc.cluster:<10} A = {assoc.account:<10} U = {assoc.user:<9}"
    if assoc.partition:
        line += f" P = {assoc.partition}"
    return line


def _parent_name(assoc: Association) -> str:
    """parent_acct of a row: set on account rows only, ``root`` for a top-level account.

    (slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_cluster_get_assocs).
    """
    if assoc.user or assoc.account == "root":
        return ""
    return assoc.parent or "root"


def _account_row_line(cluster: str, account: str, parent: Optional[str]) -> str:
    """An account row in modify/remove results: ``C = %-10s A = %s of %s``."""
    if parent:
        return f"C = {cluster:<10} A = {account} of {parent}"
    return f"C = {cluster:<10} A = {account}"


@dataclasses.dataclass
class _AssocRec:
    """The association settings of one sacctmgr command line.

    What slurm://src/sacctmgr/association_functions.c#sacctmgr_set_assoc_rec
    fills in: TRES limits keep their per-TRES counts (``-1`` removes),
    scalars their value (``-1`` clears), QoS the list operator and names.
    """

    shares: Optional[int] = None
    tres: dict[str, dict[str, int]] = dataclasses.field(default_factory=dict)
    scalars: dict[str, int] = dataclasses.field(default_factory=dict)
    parent: Optional[str] = None
    qos: Optional[tuple[str, list[str]]] = None
    def_qos: Optional[str] = None
    comment: Optional[str] = None

    def is_set(self) -> bool:
        return any(
            (
                self.shares is not None,
                self.tres,
                self.scalars,
                self.parent is not None,
                self.qos is not None,
                self.def_qos is not None,
                self.comment is not None,
            )
        )


# sacctmgr_set_assoc_rec's keyword chain, in order: (name, min prefix, kind, key).
_ASSOC_REC_KEYS: tuple[tuple[str, int, str, str], ...] = (
    ("Comment", 2, "comment", ""),
    ("DefaultQOS", 8, "defqos", ""),
    ("FairShare", 1, "shares", ""),
    ("Shares", 1, "shares", ""),
    ("GrpJobs", 4, "scalar", "GrpJobs"),
    ("GrpJobsAccrue", 8, "scalar", "GrpJobsAccrue"),
    ("GrpSubmitJobs", 4, "scalar", "GrpSubmitJobs"),
    ("GrpTRES", 7, "tres", "GrpTRES"),
    ("GrpTRESMins", 8, "tres", "GrpTRESMins"),
    ("GrpTRESRunMins", 8, "tres", "GrpTRESRunMins"),
    ("GrpWall", 4, "scalar", "GrpWall"),
    ("MaxJobs", 4, "scalar", "MaxJobs"),
    ("MaxJobsAccrue", 8, "scalar", "MaxJobsAccrue"),
    ("MinPrioThresh", 4, "scalar", "MinPrioThresh"),
    ("MaxSubmitJobs", 4, "scalar", "MaxSubmitJobs"),
    ("MaxTRES", 7, "tres", "MaxTRES"),
    ("MaxTRESPJ", 9, "tres", "MaxTRES"),
    ("MaxTRESPerJob", 11, "tres", "MaxTRES"),
    ("MaxTRESPerNode", 11, "tres", "MaxTRESPerNode"),
    ("MaxTRESPN", 9, "tres", "MaxTRESPerNode"),
    ("MaxTRESMinsPerJob", 8, "tres", "MaxTRESMins"),
    ("MaxTRESMinsPJ", 13, "tres", "MaxTRESMins"),
    ("MaxTRESRunMins", 8, "tres", "MaxTRESRunMins"),
    ("MaxWallDurationPerJob", 4, "scalar", "MaxWall"),
    ("Parent", 2, "parent", ""),
    ("Priority", 2, "scalar", "Priority"),
    ("QosLevel", 1, "qos", ""),
)


def _get_uint(value: str, name: str) -> int:
    """get_uint: empty is 0, a negative number clears (-1), anything else must be a number."""
    if not value:
        return 0
    if value.lstrip("-").isdigit():
        number = int(value)
        return -1 if number < 0 else number
    raise InvalidValueError(name, value)


def _parse_assoc_rec(rec: _AssocRec, arg: str, known_tres: list[str]) -> bool:
    """Apply one ``key=value`` to ``rec``; False when no keyword matches.

    Raises :class:`UnknownTresError` for a TRES name slurmdbd does not know
    and :class:`InvalidValueError` for a number field get_uint refuses.
    """
    key, sep, value = arg.partition("=")
    if not sep:
        return False
    base, operator = _split_list_operator(key)
    for name, min_len, kind, target in _ASSOC_REC_KEYS:
        if not keyword_match(base, name, min_len):
            continue
        if kind == "comment":
            rec.comment = _strip_quotes(value, True)
        elif kind == "defqos":
            rec.def_qos = "" if value.strip() == "-1" else value
        elif kind == "shares":
            if value.lower().startswith("parent"):
                rec.shares = FS_USE_PARENT
            else:
                rec.shares = _get_uint(value, "FairShare")
        elif kind == "scalar":
            rec.scalars[target] = _get_uint(value, name)
        elif kind == "tres":
            counts = rec.tres.setdefault(target, {})
            for part in value.split(","):
                tres_name, eq, count = part.partition("=")
                if not eq:
                    # A bare number is read as a TRES id with no count
                    # (slurm://src/common/slurmdb_defs.c#slurmdb_format_tres_str).
                    if part.strip():
                        raise UnknownTresError(f"id {part.strip()}")
                    continue
                tres_name = tres_name.strip().lower()
                if tres_name not in {t.lower() for t in known_tres}:
                    raise UnknownTresError(f"type {tres_name}")
                counts[tres_name] = int(count) if count.strip().lstrip("-").isdigit() else -1
        elif kind == "parent":
            rec.parent = fold_account(_strip_quotes(value, True))
        elif kind == "qos":
            names = [q for q in value.split(",") if q]
            rec.qos = (operator, names)
        return True
    return False


def _apply_tres(limits: dict[str, int], prefix: str, counts: dict[str, int]) -> None:
    """Fold named TRES onto a row's list: named ones updated, ``-1`` removed.

    (slurm://src/plugins/accounting_storage/mysql/accounting_storage_mysql.c#mod_tres_str).
    """
    for tres_name, count in counts.items():
        key = f"{prefix}:{tres_name}"
        if count < 0:
            limits.pop(key, None)
        else:
            limits[key] = count


def _print_assoc_limits(rec: _AssocRec) -> list[str]:
    """slurm://src/sacctmgr/common.c#sacctmgr_print_assoc_limits, for what the emulator models."""
    lines: list[str] = []
    if rec.shares is not None:
        lines.append(
            "  Fairshare     = parent"
            if rec.shares == FS_USE_PARENT
            else f"  Fairshare     = {rec.shares}"
        )

    def scalar(key: str, label: str) -> None:
        if key in rec.scalars:
            value = rec.scalars[key]
            lines.append(f"  {label}= {'NONE' if value < 0 else value}")

    def tres(prefix: str, label: str) -> None:
        if prefix in rec.tres:
            counts = rec.tres[prefix]
            shown = format_tres({k: v for k, v in counts.items() if v >= 0})
            removed = [f"{k}=NONE" for k in sorted(k for k, v in counts.items() if v < 0)]
            lines.append(f"  {label}= " + ",".join(x for x in [shown, *removed] if x))

    scalar("GrpJobs", "GrpJobs       ")
    scalar("GrpJobsAccrue", "GrpJobsAccrue            ")
    scalar("GrpSubmitJobs", "GrpSubmitJobs ")
    tres("GrpTRES", "GrpTRES       ")
    tres("GrpTRESMins", "GrpTRESMins   ")
    tres("GrpTRESRunMins", "GrpTRESRunMins")
    scalar("GrpWall", "GrpWall       ")
    scalar("MaxJobs", "MaxJobs       ")
    scalar("MaxJobsAccrue", "MaxJobsPrioAcc")
    scalar("MaxSubmitJobs", "MaxSubmitJobs ")
    tres("MaxTRES", "MaxTRES       ")
    tres("MaxTRESPerNode", "MaxTRESPerNode")
    tres("MaxTRESMins", "MaxTRESMins   ")
    tres("MaxTRESRunMins", "MaxTRESRUNMins")
    scalar("MaxWall", "MaxWall       ")
    scalar("MinPrioThresh", "MinPrioThresh ")
    if rec.parent is not None:
        lines.append(f"  Parent        = {rec.parent}")
    scalar("Priority", "Priority      ")
    if rec.qos is not None:
        operator, names = rec.qos
        lines.append("  QOS           = " + ",".join(f"{operator}{n}" for n in names))
    if rec.def_qos is not None:
        lines.append(f"  DefQOS        = {rec.def_qos}")
    if rec.comment is not None:
        lines.append(f"  Comment       = {rec.comment}")
    return lines


class InvalidValueError(ValueError):
    """A number field with a value get_uint refuses (slurm://src/sacctmgr/common.c#get_uint)."""

    def __init__(self, name: str, value: str):
        super().__init__(f"Invalid value for {name} ({value})")
        self.name = name


class UnknownConditionError(ValueError):
    """A ``where`` token no sacctmgr condition parser accepts."""


@dataclasses.dataclass
class _Conditions:
    """The ``where`` lists the emulator models from a user_cond / assoc_cond."""

    users: list[str] = dataclasses.field(default_factory=list)
    accounts: list[str] = dataclasses.field(default_factory=list)
    clusters: list[str] = dataclasses.field(default_factory=list)
    partitions: list[str] = dataclasses.field(default_factory=list)
    default_accounts: list[str] = dataclasses.field(default_factory=list)


def _addto_list(target: list[str], value: str, fold: bool = False) -> None:
    """Append a comma list without duplicates, like ``slurm_addto_char_list``."""
    for item in value.split(","):
        item = fold_account(item) if fold else item
        if item and item not in target:
            target.append(item)


# Bare words slurm://src/sacctmgr/user_functions.c#_set_cond consumes as
# flags (keyword, minimum prefix) before a bare word means a user name.
_USER_COND_FLAGS = (
    ("Set", 3),
    ("WithAssoc", 5),
    ("WithCoordinators", 5),
    ("WithDeleted", 5),
    ("WithRawQOSLevel", 5),
    ("WOPLimits", 4),
    ("where", 5),
)

# slurm://src/sacctmgr/association_functions.c#sacctmgr_set_assoc_cond in its
# order: (keyword, minimum prefix, _Conditions list, or None if not modelled).
_ASSOC_COND_KEYS = (
    ("Accounts", 2, "accounts"),
    ("Acct", 4, "accounts"),
    ("Ids", 1, None),
    ("Associations", 2, None),
    ("Clusters", 1, "clusters"),
    ("DefaultQOS", 8, None),
    ("Partitions", 3, "partitions"),
    ("Parents", 4, None),
    ("QosLevel", 1, None),
    ("Users", 1, "users"),
)


def _set_assoc_cond(cond: _Conditions, key: str, value: str) -> bool:
    """Apply one condition like slurm://src/sacctmgr/association_functions.c#sacctmgr_set_assoc_cond.

    Returns False when no keyword matches (the caller's "Unknown
    condition"). Ids, DefaultQOS, Parents and QosLevel are accepted but do
    not filter.
    """
    for name, min_len, attr in _ASSOC_COND_KEYS:
        if keyword_match(key, name, min_len):
            if attr:
                _addto_list(getattr(cond, attr), value, fold=attr == "accounts")
            return True
    return False


def _parse_user_cond(args: list[str]) -> _Conditions:
    """Parse ``where`` tokens like slurm://src/sacctmgr/user_functions.c#_set_cond.

    A bare word that is not a flag is a user name (the ``!end ||`` branch);
    ``Names``/``Users`` and ``Clusters`` match from one character,
    ``DefaultAccount`` from eight (singular only — ``DefaultAccounts`` is
    not a prefix of it); everything else goes to sacctmgr_set_assoc_cond,
    and a token nothing matches raises :class:`UnknownConditionError`.
    AdminLevel, DefaultWCKey and WCKeys are accepted but do not filter.
    """
    cond = _Conditions()
    for arg in args:
        key, sep, value = arg.partition("=")
        # parse_option_end: the keyword stops before a "+=" / "-=".
        key, _ = _split_list_operator(key)
        if not sep:
            if not any(keyword_match(arg, name, n) for name, n in _USER_COND_FLAGS):
                _addto_list(cond.users, arg)
        elif keyword_match(key, "Names", 1) or keyword_match(key, "Users", 1):
            _addto_list(cond.users, value)
        elif keyword_match(key, "AdminLevel", 2):
            continue
        elif keyword_match(key, "Clusters", 1):
            _addto_list(cond.clusters, value)
        elif keyword_match(key, "DefaultAccount", 8):
            _addto_list(cond.default_accounts, value, fold=True)
        elif any(
            keyword_match(key, name, n)
            for name, n in (("DefaultWCKey", 8), ("Format", 1), ("WCKeys", 1))
        ):
            continue
        elif not _set_assoc_cond(cond, key, value):
            raise UnknownConditionError(
                f" Unknown condition: {arg}\n Use keyword 'set' to modify value"
            )
    return cond


def _parse_assoc_cond(args: list[str]) -> _Conditions:
    """Parse ``where`` tokens like slurm://src/sacctmgr/association_functions.c#_set_cond.

    Bare words are flags (OnlyDefaults, Tree, With*, WOPInfo, WOPLimits) or
    association ids, neither modelled; ``Format`` is consumed; the rest goes
    to sacctmgr_set_assoc_cond — ``Users`` and ``Clusters`` from one
    character, ``Accounts`` from two (or ``Acct`` from four),
    ``Partitions`` from three. A token nothing matches raises
    :class:`UnknownConditionError` with this parser's shorter message.
    """
    cond = _Conditions()
    for arg in args:
        key, sep, value = arg.partition("=")
        key, _ = _split_list_operator(key)
        if not sep or keyword_match(key, "Format", 1):
            continue
        if not _set_assoc_cond(cond, key, value):
            raise UnknownConditionError(f" Unknown condition: {arg}")
    return cond


class SacctmgrEmulator:
    """Emulates sacctmgr commands for account management."""

    def __init__(self, database: SlurmDatabase, time_engine: TimeEngine):
        self.database = database
        self.time_engine = time_engine
        # Mirrors sacctmgr's global ``exit_code`` (slurm://src/sacctmgr/sacctmgr.c#exit_code): reset to 0 at
        # the start of each command, set to 1 by any error path. The dispatcher
        # propagates it to the process exit status and routes failing output
        # to stderr.
        self.exit_code = 0
        # True when a failing command's message belongs on stdout, not
        # stderr (real sacctmgr prints "Nothing modified" with printf but
        # still exits 1).
        self.stdout_error = False
        self._mode = OutputMode()
        self._immediate = False

    def _fail(self, message: str) -> str:
        """Record a non-zero exit (matching real sacctmgr) and return ``message``."""
        self.exit_code = 1
        return message

    def _nothing_modified(self) -> str:
        """No-op modify: message on stdout, process exit code 1.

        The modify branch prints "  Nothing modified" with printf and
        returns SLURM_ERROR (slurm://src/sacctmgr/account_functions.c#sacctmgr_modify_account), and
        _modify_it() turns any non-SUCCESS error_code into the global
        exit_code=1 (slurm://src/sacctmgr/sacctmgr.c#_modify_it) — so the process exits 1 even
        though the message goes to stdout.
        """
        self.exit_code = 1
        self.stdout_error = True
        return "  Nothing modified"

    def handle_command(self, args: list[str]) -> str:
        """Process sacctmgr command and return output."""
        self.exit_code = 0
        self.stdout_error = False
        # -i/--immediate: the emulator is headless and never shows real
        # sacctmgr's commit prompt, so the flag only matters where real
        # sacctmgr asks a question — the unknown-uid check on ``add user``
        # (slurm://src/sacctmgr/common.c#commit_check returns 1 at once
        # when rollback is off).
        if any(a in ("-V", "--version") for a in args):
            # slurm://src/sacctmgr/sacctmgr.c#print_slurm_version: "slurm <release>".
            return f"slurm {current().release}"
        self._mode, self._immediate, args = extract_output_flags(args, shorts="npPi")
        args = self._strip_cluster_flag(args)
        try:
            return self._dispatch(args)
        except UnknownFieldError as e:
            # slurm://src/sacctmgr/common.c#"Unknown field": bare "Unknown field '%s'" on stderr, exit 1.
            return self._fail(f"Unknown field '{e.token}'")
        except UnknownConditionError as e:
            return self._fail(str(e))

    def _dispatch(self, args: list[str]) -> str:
        if not args:
            return self._show_help()

        command = args[0].lower()

        if command == "add":
            return self._handle_add(args[1:])
        if command == "modify":
            return self._handle_modify(args[1:])
        if command in {"remove", "delete"}:
            return self._handle_remove(args[1:])
        if command == "list":
            return self._handle_list(args[1:])
        if command == "show":
            return self._handle_show(args[1:])
        return self._fail(f" error: Unknown command: {command}")

    @staticmethod
    def _strip_cluster_flag(args: list[str]) -> list[str]:
        """Tolerate and drop ``-M <name>`` / ``--cluster(s)=<name>``.

        Real sacctmgr has no ``-M`` (it talks to one slurmdbd), but
        existing emulator consumers pass it; ignoring it keeps them
        working.
        """
        out: list[str] = []
        i = 0
        while i < len(args):
            arg = args[i]
            if arg == "-M" and i + 1 < len(args):
                i += 2
                continue
            if arg.startswith(("--cluster=", "--clusters=")):
                i += 1
                continue
            out.append(arg)
            i += 1
        return out

    def _resolve(self, default_spec: str, args: list[str]) -> list[FieldSpec]:
        """Resolve ``format=`` (or the default spec) to field specs.

        Raises :class:`UnknownFieldError` for bogus tokens;
        ``handle_command`` turns that into ``Unknown field '%s'`` + exit 1.
        """
        spec = default_spec
        for arg in args:
            if arg.lower().startswith("format="):
                spec = arg.split("=", 1)[1]
        return resolve_format(parse_format_spec(spec), _REGISTRY)

    def _handle_add(self, args: list[str]) -> str:
        """Handle add commands."""
        if not args:
            return self._fail(" error: No entity specified for add")

        entity = args[0].lower()

        if entity == "account":
            return self._add_account(args[1:])
        if entity == "user":
            return self._add_user(args[1:])
        if entity == "cluster":
            return self._add_cluster(args[1:])
        if entity == "qos":
            return self._add_qos(args[1:])
        return self._fail(f" error: Unknown entity for add: {entity}")

    def _handle_modify(self, args: list[str]) -> str:
        """Handle modify commands."""
        if not args:
            return self._fail(" error: No entity specified for modify")

        entity = args[0].lower()

        if entity == "account":
            return self._modify_account(args[1:])
        if entity == "user":
            return self._modify_user(args[1:])
        if entity == "qos":
            return self._modify_qos(args[1:])
        return self._fail(f" error: Unknown entity for modify: {entity}")

    def _handle_remove(self, args: list[str]) -> str:
        """Handle remove/delete commands."""
        if not args:
            return self._fail(" error: No entity specified for remove")

        entity = args[0].lower()

        if entity == "account":
            return self._remove_account(args[1:])
        if entity == "user":
            return self._remove_user(args[1:])
        if entity == "cluster":
            return self._remove_cluster(args[1:])
        if entity in {"qos", "qoss"}:
            return self._remove_qos(args[1:])
        return self._fail(f" error: Unknown entity for remove: {entity}")

    def _handle_list(self, args: list[str]) -> str:
        """Handle list commands (real ``show`` is an alias for ``list``)."""
        if not args:
            return self._fail(" error: No entity specified for list")

        entity = args[0].lower()

        if entity in {"account", "accounts", "acct"}:
            return self._list_accounts(args[1:])
        if entity in {"user", "users"}:
            return self._list_users(args[1:])
        if entity in {"association", "associations", "assoc"}:
            return self._list_associations(args[1:])
        if entity == "tres":
            return self._list_tres(args[1:])
        if entity in {"cluster", "clusters"}:
            return self._list_clusters(args[1:])
        if entity in {"qos", "qoss"}:
            return self._list_qos(args[1:])
        return self._fail(f" error: Unknown entity for list: {entity}")

    def _handle_show(self, args: list[str]) -> str:
        """Handle show commands."""
        if not args:
            return self._fail(" error: No entity specified for show")

        entity = args[0].lower()

        if entity in {"account", "accounts", "acct"}:
            return self._show_account(args[1:])
        # Real sacctmgr prefix-matches the entity: "assoc" is accepted for
        # "association" (xstrncasecmp, common.c).
        if entity in {"association", "associations", "assoc"}:
            return self._list_associations(args[1:])
        if entity in {"qos", "qoss"}:
            return self._list_qos(args[1:])
        if entity in {"cluster", "clusters"}:
            return self._list_clusters(args[1:])
        if entity in {"user", "users"}:
            return self._list_users(args[1:])
        if entity == "tres":
            return self._list_tres(args[1:])
        return self._fail(f" error: Unknown entity for show: {entity}")

    def _unknown_option(self, arg: str) -> str:
        return self._fail(f" Unknown option: {arg}\n Use keyword 'where' to modify condition")

    def _parse_rec(self, rec: _AssocRec, arg: str) -> Optional[str]:
        """Feed ``arg`` to the association record; "" when taken, a failure message when not."""
        try:
            if _parse_assoc_rec(rec, arg, self.database.tres_types):
                return ""
        except UnknownTresError as e:
            return self._fail(
                f"sacctmgr: error: slurmdb_format_tres_str: no TRES known by {e}\n"
                f" Unknown option: {arg}\n Use keyword 'where' to modify condition"
            )
        except InvalidValueError as e:
            # get_uint logs the error and sacctmgr_set_assoc_rec then reports
            # the option as unknown.
            return self._fail(
                f"sacctmgr: error: {e}\n Unknown option: {arg}\n Use keyword 'where' to modify condition"
            )
        return None

    def _account_row(self, account: str, cluster: str) -> Optional[Association]:
        return self.database.associations.get(self.database._association_key("", account, cluster))

    def _add_account(self, args: list[str]) -> str:
        """``add account`` (slurm://src/sacctmgr/account_functions.c#sacctmgr_add_account).

        slurmdbd answers with what it did
        (slurm://src/plugins/accounting_storage/mysql/as_mysql_acct.c#as_mysql_add_accts_cond):
        `` Adding Account(s)`` and the account settings for new account
        records, `` Associations =`` and a line per new account-level
        association, ``Already existing account <a> on cluster <c>`` for one
        that is there. sacctmgr adds `` Settings`` and the association
        settings of the command line when any were given. Nothing new at all
        is SLURM_NO_CHANGE_IN_DATA: the message on stdout, exit 1.
        Descriptions and organizations are lower-cased by strip_quotes.
        """
        names: list[str] = []
        clusters: list[str] = []
        description: Optional[str] = None
        organization: Optional[str] = None
        rec = _AssocRec()
        limit_set = False
        for arg in args:
            key, sep, value = arg.partition("=")
            base, _ = _split_list_operator(key)
            if not sep and (keyword_match(arg, "Where", 5) or keyword_match(arg, "Set", 3)):
                continue
            if (
                not sep
                or keyword_match(base, "Accounts", 1)
                or keyword_match(base, "Names", 1)
                or keyword_match(base, "Acct", 4)
            ):
                _addto_list(names, value if sep else arg, fold=True)
            elif keyword_match(base, "Clusters", 1):
                _addto_list(clusters, value)
            elif keyword_match(base, "Description", 1):
                description = _strip_quotes(value, True)
                limit_set = True
            elif keyword_match(base, "Flags", 2):
                limit_set = True
            elif keyword_match(base, "Organization", 1):
                organization = _strip_quotes(value, True)
                limit_set = True
            elif keyword_match(base, "RawUsage", 7):
                continue
            else:
                failure = self._parse_rec(rec, arg)
                if failure is None:
                    return self._unknown_option(arg)
                if failure:
                    return failure
                limit_set = True
        if not names:
            return self._fail(" Need name of account to add.")

        parent = rec.parent or "root"
        clusters = clusters or self._all_clusters()
        for cluster in clusters:
            if self.database.get_cluster(cluster) is None:
                return self._fail(
                    f" error: Cluster '{cluster}' has not been added yet, please contact your"
                    " admin before adding accounts to it"
                )
            if self._account_row(parent, cluster) is None:
                return self._fail(f" error: No parent account '{parent}' on cluster '{cluster}'")

        out: list[str] = []
        new_accounts = [n for n in names if self.database.get_account(n) is None]
        if new_accounts:
            out.append(" Adding Account(s)")
            out += [f"  {n}" for n in new_accounts]
            out += [
                " Settings",
                f"  Description     = {description or 'Account Name'}",
                f"  Organization    = {organization or 'Parent/Account Name'}",
            ]
        assoc_lines: list[str] = []
        added = False
        for cluster in clusters:
            for name in names:
                if self._account_row(name, cluster) is not None:
                    assoc_lines.append(f"Already existing account {name} on cluster {cluster}")
                    continue
                if not added:
                    assoc_lines.append(" Associations =")
                added = True
                assoc_lines.append(_add_assoc_line(cluster, name))
                if self.database.get_account(name) is None:
                    self.database.add_account(
                        name,
                        description or name,
                        organization or (parent if parent != "root" else name),
                        rec.parent,
                    )
                    # add_account seeds a row on the current cluster; keep only
                    # the clusters the command names.
                    if self.database.current_cluster not in clusters:
                        self.database.associations.pop(
                            self.database._association_key("", name, self.database.current_cluster),
                            None,
                        )
                    account = self.database.get_account(name)
                    if account is not None:
                        self._apply_rec_to_account(account, rec)
                self.database.associations[self.database._association_key("", name, cluster)] = (
                    Association(account=name, user="", cluster=cluster, parent=rec.parent)
                )
        if not added and not new_accounts:
            # SLURM_NO_CHANGE_IN_DATA: printf(" %s", ret_str), exit 1.
            self.exit_code = 1
            self.stdout_error = True
            return " " + "\n".join(assoc_lines)
        self.database.save_state()
        out += assoc_lines
        if limit_set:
            out.append(" Settings")
            out += _print_assoc_limits(rec)
        return "\n".join(out)

    def _apply_rec_to_account(self, account, rec: _AssocRec) -> None:
        """Store an association record on an account-level row (the Account record)."""
        if rec.shares is not None:
            account.fairshare = rec.shares
        for prefix, counts in rec.tres.items():
            _apply_tres(account.limits, prefix, counts)
        for key, value in rec.scalars.items():
            if value < 0:
                account.limits.pop(key, None)
            else:
                account.limits[key] = value
        if rec.qos is not None:
            operator, names = rec.qos
            current = [q for q in account.qos.split(",") if q]
            account.qos = ",".join(_apply_list_operator(current, operator, ",".join(names)))
        if rec.def_qos is not None:
            account.default_qos = rec.def_qos

    def _apply_rec_to_user_row(self, row: Association, rec: _AssocRec) -> None:
        if rec.shares is not None:
            row.fairshare = rec.shares
        for prefix, counts in rec.tres.items():
            _apply_tres(row.limits, prefix, counts)
        for key, value in rec.scalars.items():
            if value < 0:
                row.limits.pop(key, None)
            else:
                row.limits[key] = value
        if rec.qos is not None:
            operator, names = rec.qos
            row.qos_list = _apply_list_operator(row.qos_list, operator, ",".join(names))
        if rec.def_qos is not None:
            row.def_qos = rec.def_qos

    def _add_user(self, args: list[str]) -> str:
        """``add user`` (slurm://src/sacctmgr/user_functions.c#sacctmgr_add_user).

        slurmdbd reports `` Adding User(s)`` and the user settings for new
        user records (slurm://src/plugins/accounting_storage/mysql/as_mysql_user.c#as_mysql_add_users_cond),
        then `` Associations =`` and a line per new association; sacctmgr
        adds `` Non Default Settings`` with the association settings given.
        When nothing is new: ``Request didn't affect anything`` and the
        ``Nothing added.`` advice on stdout, exit 1. A new user without
        DefaultAccount gets the first account as default, as slurmdbd marks
        the first association is_def.
        """
        users: list[str] = []
        accounts: list[str] = []
        clusters: list[str] = []
        partitions: list[str] = []
        default_account = ""
        rec = _AssocRec()
        for arg in args:
            key, sep, value = arg.partition("=")
            base, _ = _split_list_operator(key)
            if not sep and (keyword_match(arg, "Where", 5) or keyword_match(arg, "Set", 3)):
                continue
            if not sep or keyword_match(base, "Names", 1) or keyword_match(base, "Users", 1):
                _addto_list(users, value if sep else arg)
            elif keyword_match(base, "Accounts", 2) or keyword_match(base, "Acct", 4):
                _addto_list(accounts, value, fold=True)
            elif keyword_match(base, "AdminLevel", 2):
                continue
            elif keyword_match(base, "Clusters", 1):
                _addto_list(clusters, value)
            elif keyword_match(base, "DefaultAccount", 8):
                default_account = fold_account(_strip_quotes(value, True))
            elif keyword_match(base, "DefaultWCKey", 8) or keyword_match(base, "WCKeys", 1):
                continue
            elif keyword_match(base, "Partitions", 2):
                _addto_list(partitions, value)
            else:
                failure = self._parse_rec(rec, arg)
                if failure is None:
                    return self._unknown_option(arg)
                if failure:
                    return failure
        if not users:
            return self._fail(" Need name of user to add.")
        if not accounts:
            return self._fail(" Need name of account or wckey to add.")

        # NSS mode: real sacctmgr warns "There is no uid for user 'x'" and
        # asks to continue (slurm://src/sacctmgr/user_functions.c#_check_uid);
        # headless, the prompt times out as "no" (exit 1) unless -i/--immediate
        # skips it (slurm://src/sacctmgr/common.c#commit_check).
        for username in users:
            if nss.enabled() and nss.resolve(username) is None and not self._immediate:
                return self._fail(f" There is no uid for user '{username}'")

        clusters = clusters or self._all_clusters()
        new_users = [u for u in users if self.database.get_user(u) is None]
        lines: list[str] = []
        for cluster in clusters:
            for account in accounts:
                if self._account_row(account, cluster) is None:
                    continue
                for username in users:
                    for partition in partitions or [""]:
                        if self.database.get_association(
                            username, account, cluster=cluster, partition=partition or None
                        ):
                            continue
                        self.database.add_association(
                            username, account, cluster=cluster, partition=partition or None
                        )
                        row = self.database.get_association(
                            username, account, cluster=cluster, partition=partition or None
                        )
                        if row is not None:
                            self._apply_rec_to_user_row(row, rec)
                        lines.append(_add_assoc_line(cluster, account, username, partition))
        if not lines:
            self.exit_code = 1
            self.stdout_error = True
            return (
                "Request didn't affect anything\nNothing added.\nMake sure the accounts "
                "requested here have been added to the system/cluster. (sacctmgr add account...)"
            )
        out: list[str] = []
        if new_users:
            out.append(" Adding User(s)")
            out += [f"  {u}" for u in new_users]
            out.append(" Settings")
            if default_account:
                out.append(f"  Default Account = {default_account}")
        for username in new_users:
            self.database.add_user(username, default_account or accounts[0])
        for username in users:
            user = self.database.get_user(username)
            if user is not None and not user.default_account:
                user.default_account = default_account or accounts[0]
        self.database.save_state()
        out.append(" Associations =")
        out += lines
        if rec.is_set():
            out.append(" Non Default Settings")
            out += _print_assoc_limits(rec)
        return "\n".join(out)

    @staticmethod
    def _extract_account_filter(cond_args: list[str]) -> Optional[str]:
        """Resolve the target account name from a modify/where clause.

        Real sacctmgr (account ``_set_cond``) accepts the account either
        positionally or via ``name=`` / ``account=`` (the ``where`` keyword is
        optional). Returns the account name, or None if none was given.
        """
        for arg in cond_args:
            low = arg.lower()
            if low in {"where", "set"}:
                continue
            if low.startswith(("name=", "account=")):
                return arg.split("=", 1)[1]
            if "=" not in arg:
                return arg
        return None

    @staticmethod
    def _split_cond_set(args: list[str]) -> tuple[list[str], list[str]]:
        """Split a modify command into condition and set tokens.

        Tokens before any keyword and after ``where`` are conditions, those
        after ``set`` are settings; the two may come in either order.
        """
        cond: list[str] = []
        recs: list[str] = []
        target = cond
        for arg in args:
            if "=" not in arg:
                if keyword_match(arg, "Where", 5):
                    target = cond
                    continue
                if keyword_match(arg, "Set", 3):
                    target = recs
                    continue
            target.append(arg)
        return cond, recs

    def _modify_account(self, args: list[str]) -> str:
        """``modify account`` (slurm://src/sacctmgr/account_functions.c#sacctmgr_modify_account).

        Description/Organization change the account records (`` Modified
        accounts...``); everything else the account-level associations
        (`` Modified account associations...``), listed as slurmdbd returns
        them: each changed account row as ``C = <c> A = <a> of <old parent>``
        (slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_process_modify_assoc_results),
        then the rows below it that take the new value
        (:meth:`_modified_children`). ``RawUsage=0`` resets usage and prints
        nothing. No matching row is ``  Nothing modified``, exit 1.
        """
        cond_args, set_args = self._split_cond_set(args)
        names: list[str] = []
        clusters: list[str] = []
        for arg in cond_args:
            key, sep, value = arg.partition("=")
            base, _ = _split_list_operator(key)
            if (
                not sep
                or keyword_match(base, "Accounts", 1)
                or keyword_match(base, "Names", 1)
                or keyword_match(base, "Acct", 4)
            ):
                _addto_list(names, value if sep else arg, fold=True)
            elif keyword_match(base, "Clusters", 1):
                _addto_list(clusters, value)

        description: Optional[str] = None
        organization: Optional[str] = None
        raw_usage: Optional[str] = None
        rec = _AssocRec()
        for arg in set_args:
            key, sep, value = arg.partition("=")
            base, _ = _split_list_operator(key)
            if keyword_match(base, "Description", 1):
                description = _strip_quotes(value, True)
            elif keyword_match(base, "Organization", 1):
                organization = _strip_quotes(value, True)
            elif keyword_match(base, "RawUsage", 7):
                raw_usage = value
            else:
                failure = self._parse_rec(rec, arg)
                if failure is None:
                    return self._unknown_option(arg)
                if failure:
                    return failure
        if description is None and organization is None and raw_usage is None and not rec.is_set():
            return self._fail(" You didn't give me anything to set")

        accounts = [a for a in (self.database.get_account(n) for n in names) if a is not None]
        if raw_usage is not None:
            # Special case: reset raw usage only, nothing printed on success.
            for account in accounts:
                if raw_usage == "0":
                    self.database.reset_raw_usage(account.name)
            self.database.save_state()
            return ""

        out: list[str] = []
        if description is not None or organization is not None:
            out.append(" Modified accounts...")
            if not accounts:
                out.append(self._nothing_modified())
                return "\n".join(out)
            for account in accounts:
                if description is not None:
                    account.description = description
                if organization is not None:
                    account.organization = organization
                out.append(f"  {account.name}")
            if not rec.is_set():
                self.database.save_state()
                return "\n".join(out)

        clusters = clusters or self._all_clusters()
        if rec.parent is not None and self.database.get_account(rec.parent) is None:
            return self._fail(f" Parent Account {rec.parent} doesn't exist.")
        lines: list[str] = []
        snapshots = []
        for account in accounts:
            rows = [c for c in clusters if self._account_row(account.name, c) is not None]
            if not rows:
                continue
            old_parent = account.parent or "root"
            moved = rec.parent is not None and rec.parent != old_parent
            if rec.parent is not None and not moved and not self._rec_without_parent(rec):
                continue  # "Trying to move association to the same parent? Nothing to do."
            snapshots.append((account, dataclasses.replace(account, limits=dict(account.limits))))
            self._apply_rec_to_account(account, rec)
            if moved:
                for cluster in rows:
                    self.database.set_account_parent(account.name, rec.parent, cluster=cluster)
            for cluster in rows:
                lines.append("  " + _account_row_line(cluster, account.name, old_parent))
                lines += self._modified_children(account.name, cluster, rec, moved)

        if not lines:
            out.append(" Modified account associations...")
            out.append(self._nothing_modified())
            return "\n".join(out)

        violations = []
        for account, _ in snapshots:
            violations.extend(self.database.default_qos_violations(account))
        if violations:
            for account, before in snapshots:
                self.database.accounts[account.name] = before
            return self._fail(self._default_qos_error(violations))

        self.database.save_state()
        out.append(" Modified account associations...")
        out += lines
        return "\n".join(out)

    @staticmethod
    def _rec_without_parent(rec: _AssocRec) -> bool:
        return dataclasses.replace(rec, parent=None).is_set()

    def _modified_children(
        self, account: str, cluster: str, rec: _AssocRec, moved: bool
    ) -> list[str]:
        """Rows below ``account`` that take the change, as slurmdbd lists them.

        slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_modify_child_assocs
        walks the account's user rows and direct sub-accounts in lineage
        order (user rows first, by name) and reports one that has no value
        of its own for an inherited setting being changed -- DefaultQOS,
        the Max* scalars, the QoS list -- or any row when a Max* TRES limit
        or the parent changes; a reported sub-account is walked in turn.
        """
        inherited_scalars = {
            "MaxJobs",
            "MaxJobsAccrue",
            "MinPrioThresh",
            "MaxSubmitJobs",
            "MaxWall",
            "Priority",
        }
        max_tres = {"MaxTRES", "MaxTRESPerNode", "MaxTRESMins", "MaxTRESRunMins"}

        def modified(limits: dict[str, int], qos: list[str], def_qos: str) -> bool:
            if moved:
                return True
            if rec.def_qos is not None and not def_qos:
                return True
            if any(k in inherited_scalars and k not in limits for k in rec.scalars):
                return True
            if max_tres & set(rec.tres):
                return True
            return rec.qos is not None and not qos

        lines: list[str] = []
        user_rows = sorted(
            (
                a
                for a in self.database.associations.values()
                if a.account == account and a.user and a.cluster == cluster
            ),
            key=lambda a: (a.user, a.partition or ""),
        )
        for row in user_rows:
            if modified(row.limits, row.qos_list, row.def_qos):
                lines.append("  " + _modified_user_line(row))
        subs = sorted(
            (
                a
                for a in self.database.accounts.values()
                if a.parent == account and a.name != account
            ),
            key=lambda a: a.name,
        )
        for sub in subs:
            if self._account_row(sub.name, cluster) is None:
                continue
            sub_qos = [q for q in sub.qos.split(",") if q]
            if modified(sub.limits, sub_qos, sub.default_qos):
                lines.append(f"  C = {cluster:<10} A = {sub.name:<20}")
                lines += self._modified_children(sub.name, cluster, rec, moved)
        return lines

    def _modify_user(self, args: list[str]) -> str:
        """Modify user (slurm://src/sacctmgr/user_functions.c#sacctmgr_modify_user).

        Tokens outside ``set`` form the user_cond (:func:`_parse_user_cond`),
        so ``set`` and ``where`` may come in either order and a bare word is
        a user name (``modify user <name> set …``).

        DefaultAccount changes the user record. slurmdbd returns every user
        the condition matches, changed or not
        (slurm://src/plugins/accounting_storage/mysql/as_mysql_user.c#as_mysql_modify_users),
        listed under `` Modified users...``; when none matches the block
        says ``  Nothing modified`` and the process exits 1. Each matched
        user must already be associated with the new account on every
        cluster of the condition — all clusters when it names none
        (slurm://src/sacctmgr/user_functions.c#_check_and_set_cluster_list)
        — else slurm://src/sacctmgr/user_functions.c#_check_default_assocs
        lists each missing (user, cluster) and exits 1. See the module
        docstring for the single-default deviation.

        QosLevel (=/+=/-=) and DefaultQOS go to the matching user
        association rows (slurmdb_assoc_rec_t.qos_list / def_qos_id) under
        `` Modified user associations...``.
        """
        set_index = next((i for i, a in enumerate(args) if a.lower() == "set"), -1)
        if set_index == -1:
            return self._fail(" error: No 'set' clause found")
        where_index = next((i for i, a in enumerate(args) if a.lower() == "where"), -1)

        if where_index != -1 and where_index < set_index:
            where_args = args[where_index + 1 : set_index]
            set_args = args[set_index + 1 :]
            pre_args = args[:where_index]
        elif where_index != -1:
            set_args = args[set_index + 1 : where_index]
            where_args = args[where_index + 1 :]
            pre_args = args[:set_index]
        else:
            set_args = args[set_index + 1 :]
            where_args = []
            pre_args = args[:set_index]

        cond = _parse_user_cond(pre_args + where_args)
        if not cond.users and not cond.default_accounts:
            return self._fail(" error: No user name specified")

        # DefaultAccount is a user-record change (SA_SET_USER), handled
        # before the association settings.
        new_default = ""
        assoc_set_args: list[str] = []
        for arg in set_args:
            key, sep, value = arg.partition("=")
            if sep and keyword_match(key, "DefaultAccount", 8):
                # _set_rec: strip_quotes(value, NULL, make_lower=1).
                new_default = fold_account(value.strip("\"'"))
            else:
                assoc_set_args.append(arg)

        user_output = ""
        if new_default:
            users = self._users_matching(cond)
            if not users:
                user_output = " Modified users...\n" + self._nothing_modified()
            else:
                refusal = self._default_account_refusal(users, new_default, cond.clusters)
                if refusal:
                    return refusal
                for name in users:
                    self.database.users[name].default_account = new_default
                self.database.save_state()
                user_output = " Modified users...\n" + "\n".join(f"  {n}" for n in users)
            if not any("=" in a for a in assoc_set_args):
                return user_output
        set_args = assoc_set_args

        def after_user_output(out: str) -> str:
            return f"{user_output}\n{out}" if user_output else out

        if not cond.users:
            # A user-only condition (e.g. DefaultAccount=) selects no rows.
            return after_user_output(self._fail(" There was a problem with your 'where' options."))
        rows = self._matching_associations(cond)
        if not rows:
            return after_user_output(self._nothing_modified())

        rec = _AssocRec()
        for arg in set_args:
            if "=" not in arg:
                continue
            failure = self._parse_rec(rec, arg)
            if failure is None:
                return after_user_output(self._unknown_option(arg))
            if failure:
                return after_user_output(failure)
        if not rec.is_set():
            return after_user_output(self._nothing_modified())

        snapshot = [(row, dataclasses.replace(row, limits=dict(row.limits))) for row in rows]
        for row in rows:
            self._apply_rec_to_user_row(row, rec)

        violations: list[str] = []
        for account_name in sorted({row.account for row in rows}):
            account_obj = self.database.get_account(account_name)
            if account_obj is not None:
                violations.extend(self.database.default_qos_violations(account_obj))
        if violations:
            for row, before in snapshot:
                row.limits, row.qos_list, row.def_qos, row.fairshare = (
                    before.limits,
                    before.qos_list,
                    before.def_qos,
                    before.fairshare,
                )
            return after_user_output(self._fail(self._default_qos_error(violations)))

        self.database.save_state()
        # slurmdbd lists every matched row
        # (slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_process_modify_assoc_results).
        ordered = sorted(rows, key=self._lineage_key)
        lines = "\n".join(f"  {_modified_user_line(row)}" for row in ordered)
        return after_user_output(f" Modified user associations...\n{lines}")

    def _lineage_key(self, assoc: Association) -> tuple:
        """Slurmdbd's ``order by lineage``: the account path, then ``0-<user>``, then partition."""
        path: list[str] = []
        account = self.database.get_account(assoc.account)
        seen: set[str] = set()
        while account is not None and account.name not in seen and account.name != "root":
            seen.add(account.name)
            path.insert(0, account.name)
            account = self.database.get_account(account.parent) if account.parent else None
        return (
            assoc.cluster,
            tuple(path),
            f"0-{assoc.user}" if assoc.user else "",
            assoc.partition or "",
        )

    def _users_matching(self, cond: _Conditions) -> list[str]:
        """Users a modify user_cond selects, in name order (user_table's key).

        Named users that exist; without names, those whose default account
        is in the DefaultAccount condition
        (slurm://src/plugins/accounting_storage/mysql/as_mysql_user.c#_get_other_user_names_to_mod).
        """
        if cond.users:
            return sorted(n for n in cond.users if self.database.get_user(n))
        return sorted(
            u.name
            for u in self.database.users.values()
            if fold_account(u.default_account) in cond.default_accounts
        )

    def _all_clusters(self) -> list[str]:
        """The cluster list sacctmgr fills in when ``where`` names none.

        slurm://src/sacctmgr/user_functions.c#_check_and_set_cluster_list
        takes every cluster in the database, in name order (cluster_table's
        key) — minus the emulator's placeholder ``default`` cluster when a
        ClusterName is configured (see the module docstring).
        """
        names = sorted(c.name for c in self.database.list_clusters())
        if self.database.current_cluster != "default":
            names = [n for n in names if n != "default"]
        return names

    def _default_account_refusal(self, users: list[str], account: str, clusters: list[str]) -> str:
        """Sacctmgr's check that ``account`` can become the users' default.

        slurm://src/sacctmgr/user_functions.c#_check_default_assocs: every
        (user, cluster) pair without an association to ``account`` — any
        partition — is listed and the command exits 1, message on stdout.
        Returns "" when every pair is associated.
        """
        missing = [
            f"  U = {user} C = {cluster}"
            for user in users
            for cluster in clusters or self._all_clusters()
            if not self.database.list_user_associations(user, account, cluster)
        ]
        if not missing:
            return ""
        self.exit_code = 1
        self.stdout_error = True
        return (
            " Modified users...\n"
            " Can't modify because these users aren't associated with new "
            f"default account '{account}'...\n" + "\n".join(missing)
        )

    def _matching_associations(self, cond: _Conditions) -> list[Association]:
        """Association rows an assoc_cond selects; no ``cluster=`` means every cluster."""
        return [
            a
            for a in self.database.associations.values()
            if (not cond.users or a.user in cond.users)
            and (not cond.accounts or a.account in cond.accounts)
            and (not cond.clusters or a.cluster in cond.clusters)
            and (not cond.partitions or a.partition in cond.partitions)
        ]

    @staticmethod
    def _default_qos_error(violations: list[str]) -> str:
        """Format slurmdbd's ESLURM_NO_REMOVE_DEFAULT_QOS message as sacctmgr prints it."""
        return (
            " error: \n These associations don't have access to their default qos.\n"
            " Please give them access before they the default can be set to this.\n"
            + "\n".join(violations)
            + "\n\n Error with request: This request would make it so some associations"
            " would not have access to their default qos.\n"
        )

    def _subtree_rows(self, accounts: list[str], clusters: list[str]) -> list[Association]:
        """Rows of ``accounts`` and of every account below them on ``clusters``, lineage order."""
        names = set(accounts)
        grew = True
        while grew:
            children = {a.name for a in self.database.accounts.values() if a.parent in names}
            grew = not children <= names
            names |= children
        rows = [
            a
            for a in self.database.associations.values()
            if a.account in names and a.cluster in clusters
        ]
        return sorted(rows, key=self._lineage_key)

    def _removal_line(self, row: Association) -> str:
        if row.user:
            return _removed_user_line(row)
        account = self.database.get_account(row.account)
        return _account_row_line(
            row.cluster, row.account, (account.parent if account else None) or "root"
        )

    def _default_refusals(self, rows: list[Association]) -> list[Association]:
        """Removed rows that are a user's default while the user keeps others.

        slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#as_mysql_remove_assocs
        """
        removing = set(map(id, rows))
        refused = []
        for row in rows:
            if not row.user:
                continue
            user = self.database.get_user(row.user)
            if user is None or row.account != fold_account(user.default_account):
                continue
            if any(
                a.user == row.user and a.account != row.account and id(a) not in removing
                for a in self.database.associations.values()
            ):
                refused.append(row)
        return refused

    def _drop_rows(self, rows: list[Association]) -> None:
        removing = set(map(id, rows))
        self.database.associations = {
            k: a for k, a in self.database.associations.items() if id(a) not in removing
        }

    def _remove_account(self, args: list[str]) -> str:
        """``remove account`` (slurm://src/sacctmgr/account_functions.c#sacctmgr_delete_account).

        With only account names the account records go with all their
        associations (`` Deleting accounts...``); with an association
        condition such as ``cluster=`` only the matching associations and
        those below them go, and the account record stays (`` Deleting
        account associations...``). Nothing matched is `` Nothing deleted``
        on stdout, exit 1.
        """
        names: list[str] = []
        clusters: list[str] = []
        for arg in args:
            key, sep, value = arg.partition("=")
            base, _ = _split_list_operator(key)
            if not sep and (keyword_match(arg, "Where", 5) or keyword_match(arg, "Set", 3)):
                continue
            if (
                not sep
                or keyword_match(base, "Accounts", 1)
                or keyword_match(base, "Names", 1)
                or keyword_match(base, "Acct", 4)
            ):
                _addto_list(names, value if sep else arg, fold=True)
            elif keyword_match(base, "Clusters", 1):
                _addto_list(clusters, value)
        if not names and not clusters:
            return self._fail(" No conditions given to remove, not executing.")
        if "root" in names:
            return self._fail(
                " You are not allowed to remove the root account.\n Use remove cluster instead."
            )
        existing = [n for n in names if self.database.get_account(n) is not None]
        rows = self._subtree_rows(existing, clusters or self._all_clusters())
        if (clusters and not rows) or (not clusters and not existing):
            self.exit_code = 1
            self.stdout_error = True
            return " Nothing deleted"
        refused = self._default_refusals(rows)
        if refused:
            listed = "\n".join(f"  {self._removal_line(r)}" for r in refused)
            return self._fail(
                " Error with request: You can not remove the default account of a user\n"
                f"{listed}\n"
                " Please either remove the accounts listed above from list and resubmit,\n"
                " or change these users' default accounts to remove the account(s).\n"
                " Changes Discarded"
            )
        if clusters:
            lines = [f"  {self._removal_line(r)}" for r in rows]
            self._drop_rows(rows)
            self.database.save_state()
            return " Deleting account associations...\n" + "\n".join(lines)
        self._drop_rows(
            self._subtree_rows(existing, [c.name for c in self.database.list_clusters()])
        )
        for name in existing:
            self.database.delete_account(name)
        self.database.save_state()
        return " Deleting accounts...\n" + "\n".join(f"  {n}" for n in existing)

    def _remove_user(self, args: list[str]) -> str:
        """``remove user`` (slurm://src/sacctmgr/user_functions.c#sacctmgr_delete_user).

        With only user names the users go with every association
        (`` Deleting users...``); with an association condition (account,
        cluster, partition) the matching associations go (`` Deleting user
        associations...``), followed by the users left with none
        (`` Deleting users (No Associations)...``). Nothing matched is
        `` Nothing deleted`` on stdout, exit 1; taking a user's default
        association while others remain is refused.
        """
        try:
            cond = _parse_user_cond(args)
        except UnknownConditionError as e:
            return self._fail(str(e))
        if not (cond.users or cond.accounts or cond.clusters or cond.partitions):
            return self._fail(" No conditions given to remove, not executing.")
        if not (cond.accounts or cond.clusters or cond.partitions):
            users = [u for u in cond.users if self.database.get_user(u) is not None]
            if not users:
                self.exit_code = 1
                self.stdout_error = True
                return " Nothing deleted"
            for user in users:
                self.database.remove_user(user)
            self.database.save_state()
            return " Deleting users...\n" + "\n".join(f"  {u}" for u in users)

        rows = [
            a
            for a in self._matching_associations(cond)
            if a.user and (not cond.users or a.user in cond.users)
        ]
        if not rows:
            self.exit_code = 1
            self.stdout_error = True
            return " Nothing deleted"
        refused = self._default_refusals(rows)
        if refused:
            row = refused[0]
            return self._fail(
                " Error with request: You can not remove the default account of a user\n"
                f"  C = {row.cluster:<10} A = {row.account:<20} U = {row.user:<9}\n"
                " You must change the default account of these users or remove the users "
                "completely from the affected clusters to allow these changes.\n"
                " Changes Discarded"
            )
        rows = sorted(rows, key=self._lineage_key)
        lines = [" Deleting user associations..."]
        lines += [f"  {_removed_user_line(r)}" for r in rows]
        self._drop_rows(rows)
        orphans = sorted(
            {r.user for r in rows} - {a.user for a in self.database.associations.values()}
        )
        if orphans:
            lines.append(" Deleting users (No Associations)...")
            lines += [f"  {u}" for u in orphans]
            for user in orphans:
                self.database.users.pop(user, None)
        self.database.save_state()
        return "\n".join(lines)

    def _add_cluster(self, args: list[str]) -> str:
        """Add cluster command."""
        if not args:
            return self._fail(" error: No cluster name specified")

        cluster_name = args[0]

        # Parse optional parameters
        control_host = "localhost"
        control_port = 6817
        classification = ""

        for arg in args[1:]:
            if arg.startswith("control_host="):
                control_host = arg.split("=", 1)[1]
            elif arg.startswith("control_port="):
                control_port = int(arg.split("=", 1)[1])
            elif arg.startswith("classification="):
                classification = arg.split("=", 1)[1]

        # Validate classification value
        valid_values = [e.value for e in ClusterClassification]
        if classification and classification not in valid_values:
            return self._fail(
                f" error: Invalid classification '{classification}'. "
                f"Valid values: {', '.join(v for v in valid_values if v)}"
            )

        if self.database.get_cluster(cluster_name):
            return self._fail(f" error: Cluster {cluster_name} already exists")

        self.database.add_cluster(cluster_name, control_host, control_port, classification)
        self.database.save_state()

        return f" Adding Cluster(s)\n  Name          = {cluster_name}\n  Control Host  = {control_host}\n  Control Port  = {control_port}"

    # --- QOS operations ---

    def _add_qos(self, args: list[str]) -> str:
        """``add qos`` (slurm://src/sacctmgr/qos_functions.c#sacctmgr_add_qos).

        New QoS are listed under `` Adding QOS(s)`` with `` Settings``, the
        description (the name unless given) and the limits that were set;
        when every name exists already it is `` Nothing new added.``, exit 1.
        ``Flags=`` must be flag names (a prefix of each, comma separated):
        anything else prints the valid set on stdout and exits 1.
        """
        names: list[str] = []
        description = ""
        qos = QOS(name="")
        set_lines: list[str] = []
        for arg in args:
            key, sep, value = arg.partition("=")
            if not sep:
                if keyword_match(arg, "Where", 5) or keyword_match(arg, "Set", 3):
                    continue
                _addto_list(names, arg)
                continue
            lower = key.lower()
            if keyword_match(key, "Names", 1):
                _addto_list(names, value)
                continue
            if keyword_match(key, "Description", 1):
                description = _strip_quotes(value, True)
                continue
            if keyword_match(key, "Flags", 2):
                flags = _qos_flags(value)
                if flags is None:
                    self.exit_code = 1
                    self.stdout_error = True
                    return (
                        f" Unknown QOS flag used in:\n  '{value}'\n Valid QOS flags are\n"
                        f"  '{','.join(_QOS_FLAGS)}'"
                    )
                qos.flags = flags
                set_lines.append(f"  Flags                    = {flags}")
                continue
            try:
                known = _set_qos_field(qos, lower, value, self.database.tres_types)
            except UnknownTresError as e:
                return self._fail(
                    f"sacctmgr: error: slurmdb_format_tres_str: no TRES known by type {e}"
                )
            except ValueError:
                return self._fail(f" Bad RawUsage value: {value}")
            if not known:
                return self._unknown_option(arg)
        if not names:
            return self._fail(" Need name of qos to add.")
        new = [n for n in names if n not in self.database.qos_list]
        if not new:
            self.exit_code = 1
            self.stdout_error = True
            return " Nothing new added."
        for name in new:
            self.database.qos_list[name] = dataclasses.replace(qos, name=name)
        self.database.save_state()
        out = [" Adding QOS(s)", *(f"  {n}" for n in new), " Settings"]
        out.append(f"  Description    = {description or new[-1]}")
        if qos.grp_tres:
            out.append(f"  GrpTRES                  = {qos.grp_tres}")
        if qos.grp_tres_mins:
            out.append(f"  GrpTRESMins              = {qos.grp_tres_mins}")
        out += set_lines
        return "\n".join(out)

    def _remove_qos(self, args: list[str]) -> str:
        """``remove qos`` (slurm://src/sacctmgr/qos_functions.c#sacctmgr_delete_qos).

        A QoS that is some association's own DefaultQOS is not removed:
        slurmdbd lists those associations and sacctmgr discards the change.
        Nothing matched is ``  Nothing deleted`` on stdout, exit 1.
        """
        names: list[str] = []
        for arg in args:
            key, sep, value = arg.partition("=")
            if not sep:
                if keyword_match(arg, "Where", 5) or keyword_match(arg, "Set", 3):
                    continue
                _addto_list(names, arg)
            elif keyword_match(key, "Names", 1):
                _addto_list(names, value)
        if not names:
            return self._fail(" No conditions given to remove, not executing.")
        existing = [n for n in names if n in self.database.qos_list]
        if not existing:
            self.exit_code = 1
            self.stdout_error = True
            return "  Nothing deleted"
        defaults: list[str] = []
        for row in sorted(self.database.associations.values(), key=self._lineage_key):
            account = self.database.get_account(row.account)
            own = row.def_qos if row.user else (account.default_qos if account else "")
            if own in existing:
                line = f"  DefQOS = {own:<10} C = {row.cluster:<10} A = {row.account:<20}"
                if row.user:
                    line += f" U = {row.user:<9}"
                defaults.append(line)
        if defaults:
            # slurm://src/plugins/accounting_storage/mysql/as_mysql_qos.c#as_mysql_remove_qos
            return self._fail(
                " Associations listed below have these as their Default QOS.\n"
                + "\n".join(defaults)
                + "\n Please either remove the qos' listed above from list and resubmit,\n"
                " or change the default qos to remove the qos.\n"
                " Changes Discarded"
            )
        for name in existing:
            del self.database.qos_list[name]
            for account in self.database.accounts.values():
                account.qos = ",".join(q for q in account.qos.split(",") if q and q != name)
            for row in self.database.associations.values():
                row.qos_list = [q for q in row.qos_list if q != name]
        self.database.save_state()
        return " Deleting QOS(s)...\n" + "\n".join(f"  {n}" for n in existing)

    def _modify_qos(self, args: list[str]) -> str:
        """Modify QOS command.

        Parses: modify qos <name> set key=value [key=value ...]
        """
        if not args:
            return self._fail(" error: No qos name specified")

        qos_name = args[0]
        qos = self.database.qos_list.get(qos_name)

        set_index = -1
        for i, arg in enumerate(args):
            if arg.lower() == "set":
                set_index = i
                break

        if set_index == -1:
            return self._fail(" error: No 'set' clause found")

        # Real sacctmgr parses every set-clause first (_set_rec) and bails
        # out before any RPC if one of them set exit_code, so a bad field
        # leaves the QoS untouched. Work on a copy and commit at the end.
        pending = dataclasses.replace(qos) if qos is not None else QOS(name=qos_name)
        raw_usage: Optional[float] = None
        for arg in args[set_index + 1 :]:
            if "=" not in arg:
                return self._fail(f" Unknown option: {arg}")
            key, value = arg.split("=", 1)
            key = key.lower()
            try:
                if key == "rawusage":
                    raw_usage = _parse_raw_usage(value)
                else:
                    _set_qos_field(pending, key, value, self.database.tres_types)
            except UnknownTresError as e:
                return self._fail(
                    f"sacctmgr: error: slurmdb_format_tres_str: no TRES known by type {e}"
                )
            except ValueError:
                # slurm://src/sacctmgr/qos_functions.c#_set_rec: exit_code=1, " Bad RawUsage value: …"
                return self._fail(f" Bad RawUsage value: {value}")

        if raw_usage is not None:
            # sacctmgr_modify_qos: ``if (qos->usage)`` short-circuits into
            # sacctmgr_update_qos_usage (common.c) and returns — the other
            # set-fields in the same invocation are never sent, nothing is
            # printed on success except the local-cluster notice, and a QoS
            # the cond does not match is ``error("Failed to find QOS %s")``.
            # commit_check() is answered by -i; the interactive 30 s prompt
            # of a non-immediate run is not modelled.
            if qos is None:
                return self._fail(f"sacctmgr: error: Failed to find QOS {qos_name}")
            qos.usage_raw = raw_usage
            self.database.save_state()
            # The local cluster is slurm.conf's ClusterName.
            return (
                f"No cluster specified, resetting on local cluster {self.database.current_cluster}"
            )

        if qos is None:
            # Same SLURM_NO_CHANGE_IN_DATA shape as accounts: stdout, exit 1.
            return self._nothing_modified()

        self.database.qos_list[qos_name] = pending
        self.database.save_state()
        return f" Modified qos...\n  {qos_name}"

    def _remove_cluster(self, args: list[str]) -> str:
        """Remove cluster command."""
        if "where" not in args:
            return self._fail(" error: No where clause found")

        where_index = args.index("where")
        cluster_name = ""
        for arg in args[where_index + 1 :]:
            if arg.startswith("name="):
                cluster_name = arg.split("=", 1)[1]

        if not cluster_name:
            return self._fail(" error: No cluster name specified in where clause")

        if cluster_name == "default":
            return self._fail(" error: Cannot delete the default cluster")

        if not self.database.get_cluster(cluster_name):
            return self._fail(f" error: Cluster {cluster_name} does not exist")

        try:
            self.database.delete_cluster(cluster_name)
        except ValueError as e:
            return self._fail(f" error: {e}")

        self.database.save_state()

        return f" Deleting cluster(s)...\n  {cluster_name}"

    # --- List/show rendering ---

    def _list_qos(self, args: list[str]) -> str:
        """List QOS (real default format from slurm://src/sacctmgr/qos_functions.c#sacctmgr_list_qos)."""
        fields = self._resolve(_QOS_DEFAULT, args)

        positional = [a for a in args if "=" not in a and a.lower() != "where"]
        qos_items = [
            q for name, q in self.database.qos_list.items() if not positional or name in positional
        ]

        def _row(q: QOS) -> dict[str, str]:
            return {
                "Name": q.name,
                # Real defaults for a fresh QOS row.
                "Priority": "0",
                "GraceTime": "00:00:00",
                "PreemptMode": "cluster",
                "UsageFactor": "1.000000",
                "Flags": q.flags,
                "GrpTRES": q.grp_tres,
                "GrpTRESMins": q.grp_tres_mins,
                "MaxJobs": str(q.max_jobs) if q.max_jobs else "",
                "MaxSubmit": str(q.max_submit) if q.max_submit else "",
                "MaxWall": q.max_wall,
                "MinTRES": q.min_tres_per_job,
            }

        return render_table(fields, [_row(q) for q in qos_items], self._mode)

    def _list_clusters(self, args: list[str]) -> str:
        """List clusters (real default format from slurm://src/sacctmgr/cluster_functions.c#sacctmgr_list_cluster)."""
        fields = self._resolve(_CLUSTER_DEFAULT, args)

        rows = []
        visible = set(self._all_clusters())
        for cluster in self.database.list_clusters():
            if cluster.name not in visible:
                continue
            rows.append(
                {
                    "Cluster": cluster.name,
                    "ControlHost": cluster.control_host,
                    "ControlPort": str(cluster.control_port),
                    "RPC": str(cluster.rpc_version),
                    "Class": cluster.classification.value if cluster.classification else "",
                }
            )
        return render_table(fields, rows, self._mode)

    def _list_accounts(self, args: list[str]) -> str:
        """``list account`` (slurm://src/sacctmgr/account_functions.c#sacctmgr_list_account).

        One row per account in name order; association columns stay blank
        unless ``WithAssoc`` loads the associations, which gives one row per
        association (lineage order) on the clusters of the condition.
        Accounts filter by ``Names``/``Accounts`` or a bare word.
        """
        names: list[str] = []
        clusters: list[str] = []
        with_assoc = False
        for arg in args:
            key, sep, value = arg.partition("=")
            base, _ = _split_list_operator(key)
            if not sep:
                if keyword_match(arg, "WithAssoc", 5):
                    with_assoc = True
                elif not any(
                    keyword_match(arg, n, m)
                    for n, m in (
                        ("Where", 5),
                        ("WithCoordinators", 5),
                        ("WithDeleted", 5),
                        ("WOPLimits", 4),
                        ("WithRawQOS", 5),
                    )
                ):
                    _addto_list(names, arg, fold=True)
            elif (
                keyword_match(base, "Accounts", 1)
                or keyword_match(base, "Names", 1)
                or keyword_match(base, "Acct", 4)
            ):
                _addto_list(names, value, fold=True)
            elif keyword_match(base, "Clusters", 1):
                _addto_list(clusters, value)
        default_spec = (
            f"{_ACCOUNT_DEFAULT},{_ACCOUNT_WITHASSOC}" if with_assoc else _ACCOUNT_DEFAULT
        )
        fields = self._resolve(default_spec, args)
        accounts = sorted(
            (a for a in self.database.list_accounts() if not names or a.name in names),
            key=lambda a: a.name,
        )
        rows: list[dict[str, str]] = []
        for account in accounts:
            base_row = {
                "Account": account.name,
                "Descr": account.description,
                "Org": account.organization,
            }
            if not with_assoc:
                rows.append(base_row)
                continue
            for assoc in sorted(
                (
                    a
                    for a in self.database.associations.values()
                    if a.account == account.name and a.cluster in (clusters or self._all_clusters())
                ),
                key=self._lineage_key,
            ):
                rows.append({**self._assoc_row(assoc), **base_row})
        return render_table(fields, rows, self._mode)

    def _list_users(self, args: list[str]) -> str:
        """List users (real default ``U,DefaultA,Ad``).

        Filters by the user names and DefaultAccount of the user_cond
        (:func:`_parse_user_cond`, ``where`` optional); the association
        conditions only matter with WithAssoc, which is not modelled.
        """
        cond = _parse_user_cond(args)
        fields = self._resolve(_USER_DEFAULT, args)

        rows = [
            {
                "User": u.name,
                "Def Acct": u.default_account,
                # Real sacctmgr prints "None" for regular users.
                "Admin": "None",
            }
            for u in self.database.users.values()
            if (not cond.users or u.name in cond.users)
            and (
                not cond.default_accounts
                or fold_account(u.default_account) in cond.default_accounts
            )
        ]
        return render_table(fields, rows, self._mode)

    def _list_associations(self, args: list[str]) -> str:
        """List/show associations (real default from slurm://src/sacctmgr/association_functions.c#sacctmgr_list_assoc).

        ``where`` is optional; conditions parse like :func:`_parse_assoc_cond`.
        Account conditions fold to lower case, as slurm_addto_char_list
        does, so ``account=2026_00A`` finds the stored ``2026_00a``.
        """
        cond = _parse_assoc_cond(args)
        fields = self._resolve(_ASSOC_DEFAULT, args)
        associations = sorted(self._matching_associations(cond), key=self._lineage_key)
        return render_table(fields, [self._assoc_row(a) for a in associations], self._mode)

    def _assoc_row(self, assoc: Association) -> dict[str, str]:
        """An association's columns, with inherited values filled in as slurmdbd returns them.

        TRES limits print as sacctmgr formats them (:func:`format_tres`);
        Share prints ``parent`` for Fairshare=parent.
        """
        account_obj = self.database.get_account(assoc.account)
        view = effective_assoc(assoc, account_obj, self.database.accounts)
        tres = view["tres"]
        scalars = view["scalars"]

        def number(key: str) -> str:
            value = scalars.get(key)
            return "" if value is None else str(value)

        def wall(key: str) -> str:
            value = scalars.get(key)
            if value is None:
                return ""
            days, rest = divmod(int(value), 1440)
            hours, minutes = divmod(rest, 60)
            return (
                f"{days}-{hours:02d}:{minutes:02d}:00" if days else f"{hours:02d}:{minutes:02d}:00"
            )

        shares = view["shares"]
        return {
            "Cluster": assoc.cluster,
            "Account": assoc.account,
            "User": assoc.user,
            "Partition": assoc.partition or "",
            # parent_acct lives on the account-level row (empty User);
            # user rows print blank (slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_cluster_get_assocs).
            "ParentName": _parent_name(assoc),
            "Share": "parent" if shares == FS_USE_PARENT else str(shares),
            "Priority": number("Priority"),
            "GrpJobs": number("GrpJobs"),
            "GrpSubmit": number("GrpSubmitJobs"),
            "GrpWall": wall("GrpWall"),
            "GrpTRES": format_tres(tres["GrpTRES"]),
            "GrpTRESMins": format_tres(tres["GrpTRESMins"]),
            "GrpTRESRunMins": format_tres(tres["GrpTRESRunMins"]),
            "MaxJobs": number("MaxJobs"),
            "MaxSubmit": number("MaxSubmitJobs"),
            "MaxWall": wall("MaxWall"),
            "MaxTRES": format_tres(tres["MaxTRES"]),
            "MaxTRESPerNode": format_tres(tres["MaxTRESPerNode"]),
            "MaxTRESMins": format_tres(tres["MaxTRESMins"]),
            "QOS": ",".join(view["qos"]),
            "Def QOS": view["def_qos"],
        }

    def _list_tres(self, args: list[str]) -> str:
        """List TRES types (real default ``Type,Name%15,ID``); ids from core/tres.py."""
        fields = self._resolve(_TRES_DEFAULT, args)

        rows = []
        for tres_type in self.database.tres_types:
            if "/" in tres_type:
                type_part, name_part = tres_type.split("/", 1)
            else:
                type_part, name_part = tres_type, ""
            # slurmdbd stores TRES types and names in lower case
            # (slurm://src/sacctmgr/tres_function.c#sacctmgr_list_tres).
            rows.append(
                {
                    "Type": type_part.lower(),
                    "Name": name_part.lower(),
                    "ID": str(tres_id(tres_type, self.database.tres_types)),
                }
            )
        return render_table(fields, rows, self._mode)

    def _show_account(self, args: list[str]) -> str:
        """Show account command.

        Mirrors ``sacctmgr show account`` (slurm://src/sacctmgr/account_functions.c#sacctmgr_list_account):

        - Without ``WithAssoc`` the account's associations are not loaded, so
          association fields such as ``ParentName`` print blank (the ``default:``
          branch passes NULL). One row per account.
        - With ``WithAssoc`` one row per association is emitted; the account-level
          row (empty User) carries ParentName, user rows leave it blank.
        - ``format=`` selects/orders columns; the default field set matches
          real sacctmgr (``Acc,Des,O`` plus the WithAssoc block).
        """
        positional: list[str] = []
        with_assoc = False
        for arg in args:
            low = arg.lower()
            if low.startswith("format="):
                continue
            if low in {"withassoc", "withassociations"}:
                with_assoc = True
            elif low == "where":
                continue
            elif low.startswith("name="):
                positional.append(arg.split("=", 1)[1])
            elif "=" not in arg:
                positional.append(arg)

        default_spec = _ACCOUNT_DEFAULT
        if with_assoc:
            default_spec = f"{_ACCOUNT_DEFAULT},{_ACCOUNT_WITHASSOC}"
        fields = self._resolve(default_spec, args)

        accounts = [
            a for a in self.database.list_accounts() if not positional or a.name in positional
        ]

        rows: list[dict[str, str]] = []
        for account in accounts:
            base = {
                "Account": account.name,
                "Descr": account.description,
                "Org": account.organization,
            }
            if not with_assoc:
                # No association loaded → ParentName (and other assoc
                # fields) stay blank.
                rows.append(base)
                continue
            assocs = [
                a
                for a in self.database.associations.values()
                if a.account == account.name and a.cluster == self.database.current_cluster
            ]
            for assoc in assocs:
                row = dict(base)
                row.update(
                    {
                        "Cluster": assoc.cluster,
                        "User": assoc.user,
                        "ParentName": (assoc.parent or "") if assoc.user == "" else "",
                        "QOS": account.qos,
                        "Share": str(account.fairshare),
                    }
                )
                rows.append(row)
        return render_table(fields, rows, self._mode)

    def _show_help(self) -> str:
        """Show help message."""
        return """sacctmgr: Emulated SLURM account manager

Usage: sacctmgr [OPTIONS] COMMAND [ENTITY] [ARGS...]

Commands:
  add account <name> [description="desc"] [organization="org"] [parent=<parent>]
  add user <username> [account=<account>] [DefaultAccount=<account>]
  modify account <name> set <key=value> [<key=value>...]
  modify user <username> where account=<account> set <key=value>
  remove account where name=<name>
  remove user where [name=<username>] [account=<account>]
  list accounts
  list users
  list associations [format=<fields>] [where account=<account>]
  show account <name>
  show association where user=<user> account=<account>

Examples:
  sacctmgr add account test-account description="Test Account"
  sacctmgr modify account test-account set fairshare=333
  sacctmgr modify account test-account set GrpTRESMins=billing=72000
  sacctmgr modify account test-account set qos=slowdown
"""
