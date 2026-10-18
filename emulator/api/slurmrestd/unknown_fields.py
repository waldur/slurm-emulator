"""Warnings data_parser gives for request fields its parser does not know.

slurm://src/plugins/data_parser/v0.0.45/parsing.c#_foreach_parse_marray@26.05+:
a key path is known only when it is a whole field path of the parser
(``limits/max/tres/minutes/total``), not a prefix of one. An unknown dict is
walked a level further, so the warning names the first non-dict value under
it: ``{"default": {"account": "x"}}`` against USER_SHORT warns about
``account`` at ``#/user/default/account/``.

Field paths below are copied from the parser arrays in
slurm://src/plugins/data_parser/v0.0.45/parsers.c#QOS@26.05+ (and the
neighbouring USER, USER_SHORT, ASSOC, ASSOC_REC_SET, USERS_ADD_COND,
ACCOUNTS_ADD_COND, ACCOUNT_SHORT arrays).
"""

from __future__ import annotations

from typing import Any

from emulator.api.slurmrestd.resv_parser import data_type

FIELDS: dict[str, frozenset[str]] = {
    "QOS": frozenset(
        {
            "description",
            "flags",
            "id",
            "limits/grace_time",
            "limits/max/active_jobs/accruing",
            "limits/max/active_jobs/count",
            "limits/max/jobs/count",
            "limits/max/tres/total",
            "limits/max/tres/minutes/total",
            "limits/max/tres/minutes/per/qos",
            "name",
            "limits/max/wall_clock/per/qos",
            "limits/factor",
            "limits/max/jobs/active_jobs/per/account",
            "limits/max/jobs/active_jobs/per/user",
            "limits/max/accruing/per/account",
            "limits/max/accruing/per/user",
            "limits/max/jobs/per/account",
            "limits/max/jobs/per/user",
            "limits/max/tres/minutes/per/job",
            "limits/max/tres/per/account",
            "limits/max/tres/per/job",
            "limits/max/tres/per/node",
            "limits/max/tres/per/user",
            "limits/max/tres/minutes/per/account",
            "limits/max/tres/minutes/per/user",
            "limits/max/wall_clock/per/job",
            "limits/min/priority_threshold",
            "limits/min/tres/per/job",
            "preempt/list",
            "preempt/mode",
            "preempt/exempt_time",
            "priority",
            "usage_factor",
            "usage_threshold",
        }
    ),
    "USER": frozenset(
        {
            "administrator_level",
            "associations",
            "coordinators",
            "default/qos",
            "default/account",
            "default/wckey",
            "flags",
            "name",
            "old_name",
            "wckeys",
        }
    ),
    "USER_SHORT": frozenset({"adminlevel", "defaultqos", "defaultaccount", "defaultwckey"}),
    "ASSOC": frozenset(
        {
            "accounting",
            "account",
            "cluster",
            "comment",
            "default/qos",
            "flags",
            "max/jobs/per/count",
            "max/jobs/per/accruing",
            "max/jobs/per/submitted",
            "max/tres/total",
            "max/tres/group/minutes",
            "max/tres/group/active",
            "max/tres/minutes/total",
            "max/per/account/wall_clock",
            "id",
            "is_default",
            "lineage",
            "max/jobs/active",
            "max/jobs/accruing",
            "max/jobs/total",
            "max/tres/minutes/per/job",
            "max/tres/per/job",
            "max/tres/per/node",
            "max/jobs/per/wall_clock",
            "min/priority_threshold",
            "parent_account",
            "partition",
            "priority",
            "qos",
            "shares_raw",
            "user",
        }
    ),
    "ASSOC_REC_SET": frozenset(
        {
            "comment",
            "defaultqos",
            "grpjobs",
            "grpjobsaccrue",
            "grpsubmitjobs",
            "grptres",
            "grptresmins",
            "grptresrunmins",
            "grpwall",
            "maxjobs",
            "maxjobsaccrue",
            "maxsubmitjobs",
            "maxtresminsperjob",
            "maxtresrunmins",
            "maxtresperjob",
            "maxtrespernode",
            "maxwalldurationperjob",
            "minpriothresh",
            "parent",
            "priority",
            "qoslevel",
            "fairshare",
        }
    ),
    "USERS_ADD_COND": frozenset(
        {"accounts", "association", "clusters", "partitions", "users", "wckeys"}
    ),
    "ACCOUNTS_ADD_COND": frozenset({"accounts", "association", "clusters"}),
    "ACCOUNT_SHORT": frozenset({"description", "organization"}),
}


def unknown_field_warnings(obj: Any, parser: str, source: str) -> list[dict[str, str]]:  # noqa: ANN401
    """Warnings for every unknown field of ``obj`` parsed as ``parser``.

    ``source`` is the JSON path of ``obj`` itself, e.g. ``#/qos[0]/``.
    """
    if not isinstance(obj, dict):
        return []
    known = FIELDS[parser]
    warnings: list[dict[str, str]] = []

    def walk(node: dict[str, Any], prefix: str, path: str) -> None:
        for key, value in node.items():
            field = f"{prefix}{key}"
            if field in known:
                continue
            if isinstance(value, dict):
                walk(value, f"{field}/", f"{path}{key}/")
                continue
            warnings.append(
                {
                    "description": (
                        f'Ignoring unknown field "{key}" of type {data_type(value)} '
                        f"in DATA_PARSER_{parser}"
                    ),
                    "source": f"{path}{key}/",
                }
            )

    walk(obj, "", source)
    return warnings
