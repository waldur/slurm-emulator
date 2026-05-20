"""sshare command emulator.

Models real Slurm's ``sshare`` (see ``src/sshare/sshare.c`` and
``src/sshare/process.c``):

* one row per association: the parent account row aggregates child
  usage; each user under the account gets its own row;
* ``GrpTRESRaw`` is expressed in TRES-minutes (real Slurm divides
  ``usage_tres_raw`` by 60; our ``UsageRecord`` values are in
  ``<count>-hours`` so we multiply by 60 to reach the same units);
* field-name parsing is case-insensitive prefix match and unknown
  fields raise ``SystemExit`` with ``Invalid field requested``;
* parsable modes ``-p``/``-P`` and ``-n``/``--noheader`` are honoured
  here (not stripped by the dispatcher);
* multi-cluster mode (``-M a,b``) prints ``CLUSTER: <name>`` banners.

Intentional simplifications (documented in the MR description):
fairshare math is stubbed (``RawShares = Account.fairshare`` on parent
rows; ``NormShares`` evenly split between siblings; ``FairShare`` and
``LevelFS`` are placeholders), no UID-based default user filter, no
``--json`` / ``--yaml`` / ``--helpformat``, ``ID`` is always 0.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Optional

from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine

# Canonical TRES order roughly mirrors what slurmdbd advertises via
# ``tres_names`` in a typical cluster, so the comma-separated output
# from ``GrpTRESRaw`` / ``GrpTRESMins`` / ``TRESRunMins`` is stable.
_CANONICAL_TRES: tuple[str, ...] = (
    "cpu",
    "mem",
    "energy",
    "node",
    "billing",
    "fs/disk",
    "vmem",
    "pages",
    "gres/gpu",
)


@dataclass
class _PrintField:
    """Mirror of real Slurm's ``print_field_t`` for one column."""

    name: str
    width: int
    right_align: bool


# Widths copied from /Users/ilja/workspace/slurm/src/sshare/process.c:51-68.
# A negative width in real Slurm means left-aligned; we store the
# absolute width and a separate alignment bit.
_FIELDS: dict[str, _PrintField] = {
    "Account": _PrintField("Account", 20, right_align=False),
    "Cluster": _PrintField("Cluster", 10, right_align=True),
    "EffectvUsage": _PrintField("EffectvUsage", 13, right_align=True),
    "FairShare": _PrintField("FairShare", 10, right_align=True),
    "GrpTRESMins": _PrintField("GrpTRESMins", 30, right_align=True),
    "GrpTRESRaw": _PrintField("GrpTRESRaw", 30, right_align=True),
    "ID": _PrintField("ID", 6, right_align=True),
    "LevelFS": _PrintField("LevelFS", 10, right_align=True),
    "NormShares": _PrintField("NormShares", 11, right_align=True),
    "NormUsage": _PrintField("NormUsage", 11, right_align=True),
    "Partition": _PrintField("Partition", 12, right_align=True),
    "RawShares": _PrintField("RawShares", 10, right_align=True),
    "RawUsage": _PrintField("RawUsage", 11, right_align=True),
    "TRESRunMins": _PrintField("TRESRunMins", 30, right_align=True),
    "User": _PrintField("User", 10, right_align=True),
}


@dataclass
class _Config:
    accounts: list[str] = field(default_factory=list)
    users: list[str] = field(default_factory=list)
    clusters: list[str] = field(default_factory=list)
    format_spec: list[tuple[str, Optional[int]]] = field(default_factory=list)
    long: bool = False
    partition: bool = False
    users_only: bool = False
    all_users: bool = False
    noheader: bool = False
    parsable: Optional[str] = None  # None | "p" | "P"


@dataclass
class _Row:
    account: str
    user: str  # "" for parent account row
    partition: str = ""
    siblings: int = 1
    records: list[UsageRecord] = field(default_factory=list)


class SshareEmulator:
    """Emulates ``sshare``."""

    def __init__(self, database: SlurmDatabase, time_engine: TimeEngine):
        self.database = database
        self.time_engine = time_engine

    def handle_command(self, args: list[str]) -> str:
        cfg = self._parse_args(args)
        fields = self._resolve_format(cfg)

        clusters = cfg.clusters or [self.database.current_cluster]
        blocks: list[str] = []
        saved = self.database.current_cluster
        multi = len(clusters) > 1
        try:
            for name in clusters:
                if name not in self.database.clusters:
                    blocks.append(f"slurm-emulator: error: Cluster '{name}' does not exist")
                    continue
                self.database.current_cluster = name
                table = self._render_cluster(cfg, fields)
                if multi:
                    blocks.append(f"CLUSTER: {name}\n{table}")
                else:
                    blocks.append(table)
        finally:
            self.database.current_cluster = saved

        return "\n\n".join(blocks) if multi else "\n".join(blocks)

    def _parse_args(self, args: list[str]) -> _Config:
        cfg = _Config()
        i = 0
        while i < len(args):
            arg = args[i]
            nxt = args[i + 1] if i + 1 < len(args) else None

            if arg in ("-A", "--accounts"):
                cfg.accounts.extend(_split_csv(_require_value(nxt, "--accounts")))
                i += 2
                continue
            if arg.startswith("--accounts="):
                cfg.accounts.extend(_split_csv(arg.split("=", 1)[1]))
                i += 1
                continue
            if arg in ("-u", "--users"):
                cfg.users.extend(_split_csv(_require_value(nxt, "--users")))
                i += 2
                continue
            if arg.startswith("--users="):
                cfg.users.extend(_split_csv(arg.split("=", 1)[1]))
                i += 1
                continue
            if arg in ("-M", "--clusters", "--cluster"):
                cfg.clusters.extend(_split_csv(_require_value(nxt, "--clusters")))
                i += 2
                continue
            if arg.startswith(("--clusters=", "--cluster=")):
                cfg.clusters.extend(_split_csv(arg.split("=", 1)[1]))
                i += 1
                continue
            if arg in ("-o", "--format"):
                cfg.format_spec.extend(_parse_format(_require_value(nxt, "--format")))
                i += 2
                continue
            if arg.startswith("--format="):
                cfg.format_spec.extend(_parse_format(arg.split("=", 1)[1]))
                i += 1
                continue
            if arg in ("-l", "--long"):
                cfg.long = True
            elif arg in ("-m", "--partition"):
                cfg.partition = True
            elif arg in ("-U", "--Users"):
                cfg.users_only = True
            elif arg in ("-a", "--all"):
                cfg.all_users = True
            elif arg in ("-n", "-h", "--noheader"):
                cfg.noheader = True
            elif arg in ("-p", "--parsable"):
                cfg.parsable = "p"
            elif arg in ("-P", "--parsable2"):
                cfg.parsable = "P"
            else:
                msg = f"sshare: error: unrecognized arguments: {arg}"
                raise SystemExit(msg)
            i += 1
        return cfg

    def _resolve_format(self, cfg: _Config) -> list[_PrintField]:
        spec = cfg.format_spec or _default_format(cfg.long, cfg.partition)
        resolved: list[_PrintField] = []
        for name, width_override in spec:
            match = _match_field(name)
            if match is None:
                print(
                    f'sshare: error: Invalid field requested: "{name}"',
                    file=sys.stderr,
                )
                raise SystemExit(1)
            if width_override is not None:
                match = _PrintField(match.name, width_override, match.right_align)
            resolved.append(match)
        return resolved

    def _render_cluster(self, cfg: _Config, fields: list[_PrintField]) -> str:
        rows = self._build_rows(cfg)
        return _render(rows, fields, cfg, self.database)

    def _build_rows(self, cfg: _Config) -> list[_Row]:
        cluster = self.database.current_cluster
        if cfg.accounts:
            account_names = [a for a in cfg.accounts if self.database.get_account(a)]
        else:
            account_names = [a.name for a in self.database.list_accounts()]

        rows: list[_Row] = []
        for account in account_names:
            account_records = [
                r
                for r in self.database.usage_records
                if r.account == account and r.cluster == cluster
            ]
            users = self.database.list_account_users(account)
            if cfg.users:
                users = [u for u in users if u in cfg.users]
            siblings = max(1, len(users))

            if not cfg.users_only:
                rows.append(
                    _Row(
                        account=account,
                        user="",
                        siblings=siblings,
                        records=account_records,
                    )
                )

            for user in users:
                user_records = [r for r in account_records if r.user == user]
                if cfg.partition:
                    assocs = self.database.list_user_associations(user, account)
                    if not assocs:
                        rows.append(
                            _Row(
                                account=account,
                                user=user,
                                siblings=siblings,
                                records=user_records,
                            )
                        )
                    else:
                        for assoc in assocs:
                            rows.append(
                                _Row(
                                    account=account,
                                    user=user,
                                    partition=assoc.partition or "",
                                    siblings=siblings,
                                    records=user_records,
                                )
                            )
                else:
                    rows.append(
                        _Row(
                            account=account,
                            user=user,
                            siblings=siblings,
                            records=user_records,
                        )
                    )

            # Real sshare narrows to the caller's UID when neither -a
            # nor -u is supplied. The emulator has no UID model so we
            # keep all rows regardless; -a is accepted for parity.
            _ = cfg.all_users

        return rows


def _require_value(value: Optional[str], flag: str) -> str:
    if value is None:
        msg = f"sshare: error: missing argument for {flag}"
        raise SystemExit(msg)
    return value


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_format(value: str) -> list[tuple[str, Optional[int]]]:
    items: list[tuple[str, Optional[int]]] = []
    for raw in value.split(","):
        token = raw.strip()
        if not token:
            continue
        width: Optional[int] = None
        if "%" in token:
            head, _, tail = token.partition("%")
            token = head
            try:
                width = int(tail)
            except ValueError:
                width = None
        items.append((token, width))
    return items


def _match_field(name: str) -> Optional[_PrintField]:
    """Case-insensitive prefix match against the known field names.

    Mirrors real Slurm's ``xstrncasecmp(fields[i].name, object,
    strlen(object))`` lookup in ``process.c``.
    """
    if not name:
        return None
    needle = name.casefold()
    for field_name, descriptor in _FIELDS.items():
        if field_name.casefold().startswith(needle):
            return descriptor
    return None


def _default_format(long_flag: bool, partition: bool) -> list[tuple[str, Optional[int]]]:
    """Real-Slurm default (non-fair-tree path), process.c:138-164."""
    base: list[str] = ["Account", "User"]
    if partition:
        base.append("Partition")
    base.extend(["RawShares", "NormShares", "RawUsage"])
    if long_flag:
        base.append("NormUsage")
    base.extend(["EffectvUsage", "FairShare"])
    if long_flag:
        base.extend(["GrpTRESMins", "TRESRunMins"])
    return [(name, None) for name in base]


def _aggregate_raw_tres_minutes(records: list[UsageRecord]) -> dict[str, int]:
    """Sum ``UsageRecord`` raw TRES and convert to TRES-minutes.

    Real Slurm's ``GrpTRESRaw`` is ``usage_tres_raw[i] / 60`` where
    ``usage_tres_raw`` is in TRES-seconds. Our records carry
    ``node_hours`` / ``billing_units`` (hours) and ``raw_tres`` values
    in ``<count>-hours``, so the conversion factor here is *x60*, not
    /60.
    """
    totals: dict[str, int] = dict.fromkeys(_CANONICAL_TRES, 0)
    for record in records:
        totals["node"] += int(record.node_hours)
        totals["billing"] += int(record.billing_units)
        for raw_name, value in record.raw_tres.items():
            key = _normalize_tres_name(raw_name)
            if key not in totals:
                totals[key] = 0
            totals[key] += int(value)
    return {k: v * 60 for k, v in totals.items()}


def _normalize_tres_name(name: str) -> str:
    lower = name.lower()
    if lower in {"mem", "ram"}:
        return "mem"
    if lower in {"gres/gpu", "gpu"}:
        return "gres/gpu"
    if lower in {"node-hours", "node_hours", "node"}:
        return "node"
    if lower in {"fs/disk", "disk"}:
        return "fs/disk"
    return lower


def _format_tres(values: dict[str, int]) -> str:
    """Stable comma-separated ``tres=value`` rendering.

    ``slurmdb_make_tres_string_from_arrays`` only drops entries whose
    value is ``INFINITE64`` (``slurmdb_defs.c:3851``), so we keep zeros.
    """
    ordered = [f"{k}={values[k]}" for k in _CANONICAL_TRES if k in values]
    extras = [f"{k}={values[k]}" for k in sorted(values) if k not in _CANONICAL_TRES]
    return ",".join(ordered + extras)


def _render(
    rows: list[_Row],
    fields: list[_PrintField],
    cfg: _Config,
    database: SlurmDatabase,
) -> str:
    cluster_total_raw_seconds = sum(
        int(r.node_hours * 3600)
        for r in database.usage_records
        if r.cluster == database.current_cluster
    )

    cell_grid = [
        [_cell_for(row, fld, database, cluster_total_raw_seconds) for fld in fields] for row in rows
    ]

    lines: list[str] = []
    if not cfg.noheader:
        lines.append(_render_header(fields, cfg.parsable))
        if cfg.parsable is None:
            lines.append(" ".join("-" * f.width for f in fields).rstrip())

    for cells in cell_grid:
        lines.append(_render_row(cells, fields, cfg.parsable))

    return "\n".join(lines)


def _render_header(fields: list[_PrintField], parsable: Optional[str]) -> str:
    if parsable is None:
        return " ".join(_align(f.name, f) for f in fields).rstrip()
    line = "|".join(f.name for f in fields)
    return line + "|" if parsable == "p" else line


def _render_row(cells: list[str], fields: list[_PrintField], parsable: Optional[str]) -> str:
    if parsable is None:
        return " ".join(_align(cell, fld) for cell, fld in zip(cells, fields)).rstrip()
    line = "|".join(cells)
    return line + "|" if parsable == "p" else line


def _align(text: str, fld: _PrintField) -> str:
    if len(text) > fld.width:
        return text[: fld.width]
    return text.rjust(fld.width) if fld.right_align else text.ljust(fld.width)


def _cell_for(
    row: _Row,
    fld: _PrintField,
    database: SlurmDatabase,
    cluster_total_raw_seconds: int,
) -> str:
    name = fld.name
    is_parent = row.user == ""

    if name == "Account":
        return row.account if is_parent else f" {row.account}"
    if name == "User":
        return "" if is_parent else row.user
    if name == "Cluster":
        return database.current_cluster
    if name == "Partition":
        return row.partition
    if name == "ID":
        return "0"
    if name == "RawShares":
        account = database.get_account(row.account)
        shares = account.fairshare if account is not None else 1
        return str(shares if is_parent else 1)
    if name == "NormShares":
        return _fmt_float(1.0 if is_parent else 1.0 / max(1, row.siblings))
    if name == "RawUsage":
        return str(int(sum(r.node_hours for r in row.records) * 3600))
    if name == "NormUsage":
        row_seconds = int(sum(r.node_hours for r in row.records) * 3600)
        if cluster_total_raw_seconds <= 0:
            return _fmt_float(0.0)
        return _fmt_float(row_seconds / cluster_total_raw_seconds)
    if name == "EffectvUsage":
        # Simplified — real Slurm augments by sibling effective usage.
        row_seconds = int(sum(r.node_hours for r in row.records) * 3600)
        if cluster_total_raw_seconds <= 0:
            return _fmt_float(0.0)
        return _fmt_float(row_seconds / cluster_total_raw_seconds)
    if name == "FairShare":
        return "" if is_parent else _fmt_float(0.5)
    if name == "LevelFS":
        return ""
    if name == "GrpTRESMins":
        if not is_parent:
            return ""
        account = database.get_account(row.account)
        if account is None:
            return _format_tres(dict.fromkeys(_CANONICAL_TRES, 0))
        return _format_tres(_grp_tres_mins_from_limits(account.limits))
    if name == "GrpTRESRaw":
        return _format_tres(_aggregate_raw_tres_minutes(row.records))
    if name == "TRESRunMins":
        return _format_tres(dict.fromkeys(_CANONICAL_TRES, 0))
    return ""


def _grp_tres_mins_from_limits(limits: dict[str, int]) -> dict[str, int]:
    out: dict[str, int] = dict.fromkeys(_CANONICAL_TRES, 0)
    for key, value in limits.items():
        if key == "GrpTRESMins":
            out["billing"] = int(value)
        elif key.startswith("GrpTRESMins:"):
            tres_name = _normalize_tres_name(key.split(":", 1)[1])
            if tres_name not in out:
                out[tres_name] = 0
            out[tres_name] = int(value)
    return out


def _fmt_float(value: float) -> str:
    return f"{value:.6f}"
