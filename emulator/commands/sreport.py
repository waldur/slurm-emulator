"""sreport command emulator — ``cluster AccountUtilizationByUser`` only.

The report aggregates ``UsageRecord`` rows per account and per user over a
time window on the simulated clock, the way slurmdbd's association
rollup feeds ``sreport cluster AccountUtilizationByUser``
(slurm://src/sreport/cluster_reports.c#cluster_account_by_user). Its
purpose is to give the site agent an *aggregate* source for TRES such as
``energy`` that real sites report from ``sreport … -T energy`` rather
than from job rows.

Behaviour mirrored from real Slurm 26.05 (same in every tracked version):

* option parsing (``-a -M -n -p -P -t -T -V``,
  slurm://src/sreport/sreport.c#main); ``-t`` accepts ``Seconds``,
  ``Minutes`` (default), ``Hours``, ``Percent``, ``SecPer``, ``MinPer``,
  ``HourPer`` — anything else prints ``unknown time format X`` to stderr
  and the report continues in the previous format
  (slurm://src/sreport/sreport.c#_set_time_format). There is **no**
  ``Joules`` format: the ``energy`` TRES stores joules in the same
  ``alloc_secs`` slot the time formats divide, so joules are what
  ``-t Seconds`` prints (``-t Minutes`` prints joules/60);
* ``-T``/``--tres=`` is a comma list resolved against the cluster's TRES
  (slurm://src/sreport/sreport.c#_build_tres_list): unknown names are
  silently dropped, ``ALL`` selects everything, and an empty result is
  ``sreport: fatal: No valid TRES given`` (exit 1). Without ``-T`` the
  report is CPU only and the default format gains an ``Energy`` column
  (``Cluster,Ac,Login,Proper,Used,Energy``); with ``-T`` it is
  ``Cluster,Ac,Login,Proper,TresName,Used``;
* conditions (``start= end= accounts= users= clusters= format= tree
  all_clusters``, a bare word is a user list) come from
  slurm://src/sreport/cluster_reports.c#_set_assoc_cond, and an unknown
  one prints `` Unknown condition: X`` / ``Use keyword set to modify
  value`` to stderr, sets exit 1 and *keeps going*;
* the window is normalised by
  slurm://src/common/slurmdb_defs.c#slurmdb_report_set_start_end_time:
  default end = today 00:00, default start = yesterday 00:00, end rounded
  up to the hour, start truncated to the hour, at least one hour wide;
  ``end`` is exclusive;
* usage values are ``alloc_secs`` (TRES-seconds; joules for ``energy``)
  rendered by slurm://src/sreport/common.c#sreport_get_time_str, the
  ``Used``/``Energy`` widths grow with the largest value
  (slurm://src/sreport/common.c#sreport_set_usage_col_width), field
  names/widths come from
  slurm://src/sreport/cluster_reports.c#_setup_print_fields_list;
* the header block (dash rule, ``Cluster/Account/User Utilization
  <start> - <end-1s> (N secs)``, ``Usage reported in TRES Minutes``,
  dash rule) is printed unless ``-n`` — also in parsable modes;
* one row per association and requested TRES, associations without
  usage in the window are not listed at all
  (slurm://src/api/cluster_report_functions.c#_process_assoc_type), the
  account row carries the account's own plus its sub-accounts' usage
  and precedes its user rows.

Intentional deviations: only ``cluster AccountUtilizationByUser`` of the
reports in slurm://src/sreport/sreport.c#_cluster_rep is emulated (the
others exit 1 with ``sreport: error: … is not emulated``); there is no
interactive mode (slurm://src/sreport/sreport.c#_get_command), so no
arguments is an error; ``-h`` prints a one-line usage instead of
slurm://src/sreport/sreport.c#_usage; ``-s`` is accepted and ignored;
``Proper Name`` is always empty (no passwd database behind ``getpwnam``
in slurm://src/sreport/cluster_reports.c#_cluster_account_by_user_tres_report);
a ``users=`` filter drops the account total rows (the ``user_list``
condition in slurm://src/sreport/cluster_reports.c#_set_assoc_cond
returns only the matching user associations); the cluster-wide total
used by the percentage formats is the sum of all usage on the cluster in
the window, not the cluster's capacity.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from typing import Optional

from emulator import __version__
from emulator.commands.print_fields import (
    FieldSpec,
    OutputMode,
    UnknownFieldError,
    parse_format_spec,
    render_table,
    resolve_format,
)
from emulator.commands.slurm_time import parse_time_spec
from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine

# Field table subset of slurm://src/sreport/cluster_reports.c#_setup_print_fields_list,
# in chain order (first prefix match wins).
_REGISTRY: list[FieldSpec] = [
    FieldSpec("Accounts", 15, header="Account", min_prefix=2),
    FieldSpec("Cluster", 9, min_prefix=2),
    FieldSpec("Login", 9),
    FieldSpec("Proper", 15, header="Proper Name", min_prefix=2),
    FieldSpec("TresName", 14, header="TRES Name", min_prefix=5),
    FieldSpec("Used", 10),
    FieldSpec("Energy", 10),
]

_DEFAULT_FORMAT_TRES = "Cluster,Ac,Login,Proper,TresName,Used"
_DEFAULT_FORMAT_CPU = "Cluster,Ac,Login,Proper,Used,Energy"

# slurm://src/sreport/sreport.c#_set_time_format — (name, min prefix, label)
_TIME_FORMATS: list[tuple[str, int, str]] = [
    ("SecPer", 6, "Seconds/Percentage of Total"),
    ("MinPer", 6, "Minutes/Percentage of Total"),
    ("HourPer", 6, "Hours/Percentage of Total"),
    ("Seconds", 1, "Seconds"),
    ("Minutes", 1, "Minutes"),
    ("Hours", 1, "Hours"),
    ("Percent", 1, "Percentage of Total"),
]
_PER_FORMATS = {"SecPer", "MinPer", "HourPer"}

# Standard node used by the usage simulator; fallback rates for records
# that carry no raw_tres breakdown (same as sacct).
_NODE_CPUS = 64
_NODE_MEM_GB = 512
_NODE_GPUS = 4

# Real cluster reports other than the emulated one
# (slurm://src/sreport/sreport.c#_cluster_rep).
_OTHER_CLUSTER_REPORTS = (
    "AccountUtilizationByQOS",
    "UserUtilizationByAccount",
    "UserUtilizationByWckey",
    "Utilization",
    "WCKeyUtilizationByUser",
)


@dataclass
class _Config:
    all_clusters: bool = False
    clusters: list[str] = field(default_factory=list)
    accounts: list[str] = field(default_factory=list)
    users: list[str] = field(default_factory=list)
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    format_spec: str = ""
    tres_spec: Optional[str] = None
    time_format: str = "Minutes"
    tree: bool = False
    mode: OutputMode = field(default_factory=OutputMode)
    version: bool = False


@dataclass
class _Assoc:
    """One report association: an account total (user "") or a user."""

    cluster: str
    account: str
    user: str
    alloc_secs: dict[str, int]  # per canonical TRES name


class SreportEmulator:
    """Emulates ``sreport cluster AccountUtilizationByUser``."""

    def __init__(self, database: SlurmDatabase, time_engine: TimeEngine):
        self.database = database
        self.time_engine = time_engine
        # Mirrors sreport.c's global exit_code (=1 on any error at any time).
        self.exit_code = 0

    # ------------------------------------------------------------------ entry

    def handle_command(self, args: list[str]) -> str:
        self.exit_code = 0
        cfg, positional = self._parse_options(args)
        if cfg.version:
            return f"slurm-emulator {__version__}"

        if not positional:
            self._error("sreport: error: no report given (interactive mode is not emulated)")
            raise SystemExit(1)
        if not _prefix(positional[0], "cluster", 2):
            self._error(f"sreport: error: only cluster reports are emulated, got {positional[0]}")
            raise SystemExit(1)
        if len(positional) < 2:
            self._error("Not valid report \nValid cluster reports are, ")
            raise SystemExit(1)
        report = positional[1]
        if not _prefix(report, "AccountUtilizationByUser", 21):
            if any(_prefix(report, name, 2) for name in _OTHER_CLUSTER_REPORTS):
                self._error(f"sreport: error: cluster {report} is not emulated by slurm-emulator")
            else:
                self._error(
                    f"Not valid report {report}\n"
                    'Valid cluster reports are, "AccountUtilizationByUser", '
                    '"AccountUtilizationByQOS", "UserUtilizationByAccount", '
                    '"UserUtilizationByWckey", "Utilization", and "WCKeyUtilizationByUser"'
                )
            raise SystemExit(1)

        self._parse_conditions(positional[2:], cfg)
        start, end = self._window(cfg)
        tres_list = self._build_tres_list(cfg.tres_spec, cfg.time_format)

        spec = cfg.format_spec or (_DEFAULT_FORMAT_TRES if cfg.tres_spec else _DEFAULT_FORMAT_CPU)
        try:
            fields = resolve_format(parse_format_spec(spec), _REGISTRY)
        except UnknownFieldError as e:
            self._error(f"sreport: error: Unknown field '{e.token}'")
            raise SystemExit(1) from None
        fields = self._apply_widths(fields, cfg)

        clusters = self._select_clusters(cfg)
        assocs, cluster_totals = self._aggregate(cfg, clusters, start, end)
        fields = self._set_usage_column_width(fields, assocs, tres_list, cfg)

        rows = []
        for assoc in assocs:
            total = cluster_totals[assoc.cluster]
            for tres in tres_list:
                rows.append(self._row(assoc, tres, total, cfg))

        lines: list[str] = []
        if not cfg.mode.noheader:
            lines.extend(self._header_block(start, end, cfg))
        lines.append(render_table(fields, rows, cfg.mode))
        return "\n".join(lines)

    # --------------------------------------------------------------- parsing

    def _parse_options(self, args: list[str]) -> tuple[_Config, list[str]]:
        """getopt_long("aM:hnpPQs:t:T:vV") with the long names from sreport.c.

        Short options cluster like getopt: ``-nP``, ``-an``, ``-nTenergy``
        (a value option consumes the rest of the token or the next argument).
        """
        cfg = _Config()
        positional: list[str] = []
        long_value = {"--cluster": "M", "--tres": "T"}
        long_flag = {
            "--all_clusters": "a",
            "--noheader": "n",
            "--parsable": "p",
            "--parsable2": "P",
            "--version": "V",
            "--help": "h",
            "--usage": "h",
            "--quiet": "Q",
            "--verbose": "v",
            "--federation": "",
            "--local": "",
        }
        value_shorts = "MTts"
        flag_shorts = "ahnpPQvV"

        def apply_flag(opt: str) -> None:
            if opt == "a":
                cfg.all_clusters = True
            elif opt == "n":
                cfg.mode.noheader = True
            elif opt == "p":
                cfg.mode.parsable = "p"
            elif opt == "P":
                cfg.mode.parsable = "P"
            elif opt == "V":
                cfg.version = True
            elif opt == "h":
                # Real _usage() prints the full option list; the emulator
                # prints a one-line reminder of the emulated subset.
                print(
                    "Usage: sreport [-a] [-M cluster] [-n] [-p|-P] [-t FORMAT] [-T TRES] "
                    "cluster AccountUtilizationByUser [start=…] [end=…] [accounts=…] [users=…]"
                )
                raise SystemExit(0)

        def apply_value(opt: str, value: str) -> None:
            if opt == "M":
                cfg.clusters = [c for c in value.split(",") if c]
            elif opt == "T":
                cfg.tres_spec = value
            elif opt == "t":
                self._set_time_format(cfg, value)
            # "s" (sort) is accepted and ignored: rows are already in the
            # hierarchical order sreport sorts into.

        i = 0
        while i < len(args):
            arg = args[i]
            if arg == "--":
                positional.extend(args[i + 1 :])
                break
            if arg.startswith("--"):
                name, eq, value = arg.partition("=")
                if name in long_value:
                    if not eq:
                        if i + 1 >= len(args):
                            self._bad_option(f"option '{name}' requires an argument")
                        value = args[i + 1]
                        i += 1
                    apply_value(long_value[name], value)
                elif name in long_flag:
                    apply_flag(long_flag[name])
                else:
                    self._bad_option(f"unrecognized option '{arg}'")
                i += 1
                continue
            if arg.startswith("-") and len(arg) > 1:
                j = 1
                while j < len(arg):
                    opt = arg[j]
                    if opt in value_shorts:
                        rest = arg[j + 1 :]
                        if not rest:
                            if i + 1 >= len(args):
                                self._bad_option(f"option requires an argument -- '{opt}'")
                            rest = args[i + 1]
                            i += 1
                        apply_value(opt, rest)
                        break
                    if opt in flag_shorts:
                        apply_flag(opt)
                        j += 1
                        continue
                    self._bad_option(f"invalid option -- '{opt}'")
                i += 1
                continue
            positional.append(arg)
            i += 1
        return cfg, positional

    def _bad_option(self, message: str) -> None:
        self._error(f"sreport: {message}")
        self._error('Try "sreport --help" for more information')
        raise SystemExit(1)

    def _set_time_format(self, cfg: _Config, value: str) -> None:
        for name, min_prefix, _label in _TIME_FORMATS:
            if _prefix(value, name, min_prefix):
                cfg.time_format = name
                return
        # Real sreport prints this without a newline and ignores the
        # error (main() discards _set_time_format's return value).
        sys.stderr.write(f"unknown time format {value}")

    def _parse_conditions(self, args: list[str], cfg: _Config) -> None:
        """slurm://src/sreport/cluster_reports.c#_set_assoc_cond."""
        for arg in args:
            if "=" in arg:
                key, value = arg.split("=", 1)
                has_value = True
            else:
                key, value = arg, ""
                has_value = False
            if not has_value and _prefix(key, "all_clusters", 1):
                cfg.all_clusters = True
            elif not has_value and _prefix(key, "Tree", 4):
                cfg.tree = True
            elif not has_value or _prefix(key, "Users", 1):
                cfg.users.extend(_csv(value or key))
            elif _prefix(key, "Accounts", 2) or _prefix(key, "Acct", 4):
                cfg.accounts.extend(_csv(value))
            elif _prefix(key, "Clusters", 1):
                cfg.clusters.extend(_csv(value))
            elif _prefix(key, "End", 1):
                # parse_time("") is 0 = "nothing specified" -> default window.
                cfg.end = self._parse_time(value) if value else None
            elif _prefix(key, "Format", 1):
                cfg.format_spec = value
            elif _prefix(key, "Start", 1):
                cfg.start = self._parse_time(value) if value else None
            else:
                self._error(f" Unknown condition: {arg}\nUse keyword set to modify value")
                self.exit_code = 1

    def _parse_time(self, text: str) -> datetime:
        try:
            return parse_time_spec(text, self.time_engine.get_current_time())
        except (ValueError, IndexError):
            # slurm://src/common/parse_time.c#parse_time — no sreport: prefix
            self._error(f"Invalid time specification (pos=0): {text}")
            self.exit_code = 1
            raise SystemExit(1) from None

    def _window(self, cfg: _Config) -> tuple[datetime, datetime]:
        """slurm://src/common/slurmdb_defs.c#slurmdb_report_set_start_end_time."""
        now = self.time_engine.get_current_time()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = cfg.end if cfg.end is not None else midnight
        if end.second != 0 or end.microsecond != 0:
            end = end.replace(second=0, microsecond=0) + timedelta(minutes=1)
        if end.minute != 0:
            end = end.replace(minute=0) + timedelta(hours=1)
        start = cfg.start if cfg.start is not None else midnight - timedelta(days=1)
        start = start.replace(minute=0, second=0, microsecond=0)
        if end - start < timedelta(hours=1):
            end = start + timedelta(hours=1)
        return start, end

    def _build_tres_list(self, tres_spec: Optional[str], time_format: str) -> list[str]:
        """slurm://src/sreport/sreport.c#_build_tres_list — canonical names."""
        known = [_canonical(t) for t in self.database.tres_types]
        if tres_spec is None:
            return ["cpu"]
        selected: list[str] = []
        for tok in tres_spec.split(","):
            if tok.upper() == "ALL":
                selected = list(known)
                break
            canon = _canonical(tok)
            if canon in known and canon not in selected:
                selected.append(canon)
        if "node" in selected and time_format in (_PER_FORMATS | {"Percent"}):
            self._fatal(
                "TRES node usage is no longer reported in percent format reports.  "
                "Please use TRES CPU instead."
            )
        if not selected:
            self._fatal("No valid TRES given")
        return selected

    def _fatal(self, message: str) -> None:
        self._error(f"sreport: fatal: {message}")
        self.exit_code = 1
        raise SystemExit(1)

    # ----------------------------------------------------------- aggregation

    def _select_clusters(self, cfg: _Config) -> list[str]:
        if cfg.all_clusters:
            return sorted(self.database.clusters)
        if cfg.clusters:
            return sorted(cfg.clusters)
        return [self.database.current_cluster]

    def _aggregate(
        self, cfg: _Config, clusters: list[str], start: datetime, end: datetime
    ) -> tuple[list[_Assoc], dict[str, dict[str, int]]]:
        self.database.ensure_job_ids()
        records = [
            r
            for r in self.database.usage_records
            if r.cluster in clusters and start <= r.timestamp < end
        ]
        wanted = self._account_scope(cfg.accounts)
        assocs: list[_Assoc] = []
        totals: dict[str, dict[str, int]] = {}
        for cluster in clusters:
            cluster_records = [r for r in records if r.cluster == cluster]
            totals[cluster] = _sum_usage(cluster_records)
            for account in self._hierarchical_accounts():
                if wanted is not None and account not in wanted:
                    continue
                subtree = self._subtree(account)
                own = [r for r in cluster_records if r.account in subtree]
                if not own:
                    continue
                if not cfg.users:
                    assocs.append(_Assoc(cluster, account, "", _sum_usage(own)))
                direct = [r for r in own if r.account == account]
                for user in sorted({r.user for r in direct}):
                    if cfg.users and user not in cfg.users:
                        continue
                    assocs.append(
                        _Assoc(
                            cluster,
                            account,
                            user,
                            _sum_usage([r for r in direct if r.user == user]),
                        )
                    )
        return assocs, totals

    def _account_scope(self, requested: list[str]) -> Optional[set[str]]:
        """Requested accounts plus their sub-accounts (ASSOC_COND_FLAG_SUB_ACCTS)."""
        if not requested:
            return None
        scope: set[str] = set()
        for name in requested:
            scope |= self._subtree(name.lower())
        return scope

    def _children(self) -> dict[Optional[str], list[str]]:
        children: dict[Optional[str], list[str]] = {}
        for acct in self.database.accounts.values():
            parent = acct.parent if acct.name != "root" else None
            children.setdefault(parent, []).append(acct.name)
        for names in children.values():
            names.sort()
        return children

    def _subtree(self, account: str) -> set[str]:
        children = self._children()
        out: set[str] = set()
        stack = [account]
        while stack:
            name = stack.pop()
            if name in out:
                continue
            out.add(name)
            stack.extend(children.get(name, []))
        return out

    def _hierarchical_accounts(self) -> list[str]:
        """Depth-first, parents before children, siblings alphabetical."""
        children = self._children()
        order: list[str] = []
        seen: set[str] = set()

        def walk(name: str) -> None:
            if name in seen:
                return
            seen.add(name)
            order.append(name)
            for child in children.get(name, []):
                walk(child)

        for root in children.get(None, []):
            walk(root)
        # Orphans (parent not in the database) still get listed.
        for name in sorted(self.database.accounts):
            walk(name)
        return order

    # ------------------------------------------------------------- rendering

    def _row(
        self, assoc: _Assoc, tres: str, cluster_total: dict[str, int], cfg: _Config
    ) -> dict[str, str]:
        account = assoc.account
        if cfg.tree:
            account = f" {account}" if assoc.user else account
        return {
            "Cluster": assoc.cluster,
            "Account": account,
            "Login": assoc.user,
            "Proper Name": "",
            "TRES Name": tres,
            "Used": self._time_str(assoc.alloc_secs.get(tres, 0), cluster_total.get(tres, 0), cfg),
            "Energy": self._time_str(
                assoc.alloc_secs.get("energy", 0), cluster_total.get("energy", 0), cfg
            ),
        }

    @staticmethod
    def _time_str(value: int, total: int, cfg: _Config) -> str:
        """slurm://src/sreport/common.c#sreport_get_time_str."""
        total = total or 1
        percent = value / total * 100
        fmt = cfg.time_format
        if fmt == "Seconds":
            return str(value)
        if fmt == "Minutes":
            return f"{value / 60:.0f}"
        if fmt == "Hours":
            return f"{value / 3600:.0f}"
        if fmt == "Percent":
            return f"{percent:.2f}%"
        if fmt == "SecPer":
            return f"{value}({percent:.2f}%)"
        if fmt == "MinPer":
            return f"{value / 60:.0f}({percent:.2f}%)"
        if fmt == "HourPer":
            return f"{value / 3600:.0f}({percent:.2f}%)"
        return f"{value / 60:.0f}"

    @staticmethod
    def _apply_widths(fields: list[FieldSpec], cfg: _Config) -> list[FieldSpec]:
        """Widths that depend on the time format / tree flag."""
        out = []
        for f in fields:
            if f.name == "Used" and cfg.time_format in _PER_FORMATS and f.width == 10:
                f = replace(f, width=18)
            if f.name == "Accounts" and cfg.tree and f.width == 15:
                f = replace(f, width=-20)
            out.append(f)
        return out

    @staticmethod
    def _set_usage_column_width(
        fields: list[FieldSpec], assocs: list[_Assoc], tres_list: list[str], cfg: _Config
    ) -> list[FieldSpec]:
        """slurm://src/sreport/common.c#sreport_set_usage_column_width."""
        has_energy = any(f.name == "Energy" for f in fields)
        max_usage = 0
        max_energy = 0
        for assoc in assocs:
            if has_energy:
                max_usage = max(max_usage, assoc.alloc_secs.get("cpu", 0))
                max_energy = max(max_energy, assoc.alloc_secs.get("energy", 0))
            else:
                for tres in tres_list:
                    max_usage = max(max_usage, assoc.alloc_secs.get(tres, 0))

        def width(number: int) -> int:
            length = 8
            order = 100_000_000
            while order < 100_000_000 * 100_000_000_000_000_000:
                if number < order:
                    break
                length += 1
                order *= 10
            if cfg.time_format in _PER_FORMATS:
                length += 9
            return length

        out = []
        for f in fields:
            if f.name == "Used":
                f = replace(f, width=width(max_usage))
            elif f.name == "Energy":
                f = replace(f, width=width(max_energy))
            out.append(f)
        return out

    def _header_block(self, start: datetime, end: datetime, cfg: _Config) -> list[str]:
        rule = "-" * 80
        secs = int((end - start).total_seconds())
        label = next(label for name, _p, label in _TIME_FORMATS if name == cfg.time_format)
        usage = (
            f"Usage reported in {label}"
            if cfg.time_format == "Percent"
            else f"Usage reported in {'TRES' if cfg.tres_spec is not None else 'CPU'} {label}"
        )
        start_s = start.strftime("%Y-%m-%dT%H:%M:%S")
        end_s = (end - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S")
        return [
            rule,
            f"Cluster/Account/User Utilization {start_s} - {end_s} ({secs} secs)",
            usage,
            rule,
        ]

    @staticmethod
    def _error(message: str) -> None:
        print(message, file=sys.stderr)


# ------------------------------------------------------------------ helpers


def _prefix(token: str, name: str, min_len: int) -> bool:
    """``!xstrncasecmp(token, name, MAX(strlen(token), min_len))``."""
    n = max(len(token), min_len)
    return token[:n].lower() == name[:n].lower()


def _csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _canonical(name: str) -> str:
    lower = name.lower()
    if lower in {"mem", "ram"}:
        return "mem"
    if lower in {"gres/gpu", "gpu"}:
        return "gres/gpu"
    if lower in {"node-hours", "node_hours", "node"}:
        return "node"
    return lower


def _record_alloc_secs(record: UsageRecord) -> dict[str, int]:
    """TRES-seconds for one record (joules for ``energy``)."""
    raw = {_canonical(k): v for k, v in record.raw_tres.items()}
    hours = record.node_hours

    def rate(key: str, per_node: int) -> float:
        return raw[key] if key in raw else hours * per_node

    return {
        "cpu": int(rate("cpu", _NODE_CPUS) * 3600),
        "mem": int(rate("mem", _NODE_MEM_GB) * 1024 * 3600),  # MB-seconds
        "energy": int(raw.get("energy", 0)),
        "node": int(hours * 3600),
        "billing": int(record.billing_units * 3600),
        "gres/gpu": int(rate("gres/gpu", _NODE_GPUS) * 3600),
    }


def _sum_usage(records: list[UsageRecord]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for record in records:
        for key, value in _record_alloc_secs(record).items():
            totals[key] = totals.get(key, 0) + value
    return totals
