"""sacctmgr command emulator.

Output formatting and exit codes mirror real Slurm 26.11:

* list/show output defaults to fixed-width columns with a dashed
  underline (``src/common/print_fields.c``); ``-p``/``--parsable``
  pipe-separates with a trailing ``|``, ``-P``/``--parsable2`` without,
  ``-n``/``--noheader`` drops the header;
* field names, printed headers, and column widths come from the
  ``sacctmgr_process_format_list`` chain in ``src/sacctmgr/common.c``
  (line refs below); default field sets per entity come from each
  ``sacctmgr/*_functions.c``;
* errors print with a leading-space `` error: ...`` prefix and exit 1
  (the dispatcher routes failing output to stderr); ``Nothing
  modified`` goes to stdout and exits 0 (account_functions.c:727-729 —
  only the local rc is set, the global ``exit_code`` stays 0);
* re-adding an existing account reports ``SLURM_NO_CHANGE_IN_DATA``:
  `` Data has not changed since time specified`` on stdout, exit 0
  (account_functions.c:342-343, slurm_errno.c:205-207).

Intentional deviations: no interactive commit prompt (``-i`` is an
accepted no-op — the emulator is headless), and a leading ``-M
<cluster>`` is tolerated and ignored for waldur-site-agent
compatibility (real sacctmgr has no ``-M``).
"""

from typing import Optional

from emulator import __version__
from emulator.commands.print_fields import (
    FieldSpec,
    OutputMode,
    UnknownFieldError,
    extract_output_flags,
    parse_format_spec,
    render_table,
    resolve_format,
)
from emulator.core.database import (
    QOS,
    Association,
    ClusterClassification,
    SlurmDatabase,
    fold_account,
)
from emulator.core.time_engine import TimeEngine

# Field registry mirroring the prefix-match chain in
# src/sacctmgr/common.c:219-891. Order matters: tokens resolve to the
# first entry they prefix-match (e.g. "Cl" must hit Clusters, not
# Classification, because Classification needs 3 chars). Alias entries
# (e.g. Acct) carry the canonical printed header so they share a column.
_REGISTRY: list[FieldSpec] = [
    FieldSpec("Account", 10, min_prefix=3),  # common.c:219
    FieldSpec("Acct", 10, header="Account", min_prefix=4),
    FieldSpec("AdminLevel", 9, header="Admin", min_prefix=2),  # common.c:243
    FieldSpec("Classification", 9, header="Class", min_prefix=3),  # common.c:263
    FieldSpec("Clusters", 10, header="Cluster", min_prefix=2),  # common.c:274
    FieldSpec("ControlHost", 15, min_prefix=8),  # common.c:289
    FieldSpec("ControlPort", 12, min_prefix=8),  # common.c:294
    FieldSpec("DefaultAccount", 10, header="Def Acct", min_prefix=8),  # common.c:320
    FieldSpec("DefaultQOS", 9, header="Def QOS", min_prefix=8),  # common.c:326
    FieldSpec("Description", 20, header="Descr", min_prefix=3),  # common.c:336
    FieldSpec("Flags", 20, min_prefix=2),  # common.c:381
    FieldSpec("GraceTime", 10, min_prefix=3),  # common.c:386
    FieldSpec("GrpCPUs", 8, min_prefix=6),  # common.c:391
    FieldSpec("GrpCPUMins", 11, min_prefix=7),  # common.c:396
    FieldSpec("GrpTRES", 13, min_prefix=7),  # common.c:406
    FieldSpec("GrpTRESMins", 13, min_prefix=7),  # common.c:411
    FieldSpec("GrpTRESRunMins", 13, min_prefix=8),  # common.c:416
    FieldSpec("GrpJobs", 7, min_prefix=4),  # common.c:422
    FieldSpec("GrpMemory", 7, header="GrpMem", min_prefix=4),  # common.c:433
    FieldSpec("GrpNodes", 8, min_prefix=4),  # common.c:438
    FieldSpec("GrpSubmitJobs", 9, header="GrpSubmit", min_prefix=4),  # common.c:443
    FieldSpec("GrpWall", 11, min_prefix=4),  # common.c:448
    FieldSpec("ID", 6, min_prefix=2),  # common.c:453
    FieldSpec("MaxCPUMinsPerJob", 11, header="MaxCPUMins", min_prefix=7),  # common.c:483
    FieldSpec("MaxCPUsPerJob", 8, header="MaxCPUs", min_prefix=6),  # common.c:497
    FieldSpec("MaxTRES", 13, min_prefix=7),  # common.c:511
    FieldSpec("MaxTRESPerJob", 13, header="MaxTRES", min_prefix=11),
    FieldSpec("MaxTRESPerNode", 14, min_prefix=11),  # common.c:521
    FieldSpec("MaxTRESPN", 14, header="MaxTRESPerNode", min_prefix=9),
    FieldSpec("MaxTRESMinsPerJob", 13, header="MaxTRESMins", min_prefix=11),  # common.c:529
    FieldSpec("MaxTRESRunMinsPerAccount", 16, header="MaxTRESRunMinsPA", min_prefix=16),
    FieldSpec("MaxTRESRunMinsPerAcct", 16, header="MaxTRESRunMinsPA", min_prefix=16),
    FieldSpec("MaxTRESRunMinsPA", 16, min_prefix=16),  # common.c:537
    FieldSpec("MaxTRESRunMinsPerUser", 16, header="MaxTRESRunMinsPU", min_prefix=16),
    FieldSpec("MaxTRESRunMinsPU", 16, min_prefix=16),  # common.c:547
    FieldSpec("MaxTRESPerAccount", 13, header="MaxTRESPA", min_prefix=11),  # common.c:555
    FieldSpec("MaxTRESPerAcct", 13, header="MaxTRESPA", min_prefix=11),
    FieldSpec("MaxTRESPA", 13, min_prefix=9),
    FieldSpec("MaxTRESPerUser", 13, header="MaxTRESPU", min_prefix=11),  # common.c:565
    FieldSpec("MaxTRESPU", 13, min_prefix=9),
    FieldSpec("MaxJobs", 7, min_prefix=4),  # common.c:573
    FieldSpec("MaxJobsPerAccount", 9, header="MaxJobsPA", min_prefix=8),  # common.c:602
    FieldSpec("MaxJobsPerAcct", 9, header="MaxJobsPA", min_prefix=8),
    FieldSpec("MaxJobsPA", 9, min_prefix=8),
    FieldSpec("MaxJobsPerUser", 9, header="MaxJobsPU", min_prefix=8),  # common.c:612
    FieldSpec("MaxJobsPU", 9, min_prefix=8),
    FieldSpec("MaxNodesPerJob", 8, header="MaxNodes", min_prefix=4),  # common.c:620
    FieldSpec("MaxSubmitJobs", 9, header="MaxSubmit", min_prefix=4),  # common.c:640
    FieldSpec("MaxSubmitJobsPerAccount", 11, header="MaxSubmitPA", min_prefix=11),
    FieldSpec("MaxSubmitJobsPerAcct", 11, header="MaxSubmitPA", min_prefix=11),
    FieldSpec("MaxSubmitPA", 11, min_prefix=10),  # common.c:646
    FieldSpec("MaxSubmitJobsPerUser", 11, header="MaxSubmitPU", min_prefix=11),
    FieldSpec("MaxSubmitPU", 11, min_prefix=10),  # common.c:658
    FieldSpec("MaxWallDurationPerJob", 11, header="MaxWall", min_prefix=4),  # common.c:668
    FieldSpec("MinTRESPerJob", 13, header="MinTRES", min_prefix=7),  # common.c:680
    FieldSpec("Name", 10, min_prefix=2),  # common.c:686
    FieldSpec("Organization", 20, header="Org", min_prefix=1),  # common.c:706
    FieldSpec("ParentName", 10, min_prefix=7),  # common.c:716
    FieldSpec("Partition", 10, min_prefix=4),  # common.c:721
    FieldSpec("PreemptMode", 11, min_prefix=8),  # common.c:726
    FieldSpec("Preempt", 10, min_prefix=7),  # common.c:732
    FieldSpec("PreemptExemptTime", 19, min_prefix=8),  # common.c:737
    FieldSpec("Priority", 10, min_prefix=3),  # common.c:743
    FieldSpec("QOSLevel", 20, header="QOS", min_prefix=3),  # common.c:753
    FieldSpec("RPC", 5, min_prefix=1),  # common.c:769
    FieldSpec("Share", 9, min_prefix=1),  # common.c:779
    FieldSpec("FairShare", 9, header="Share", min_prefix=2),
    FieldSpec("Type", 8, min_prefix=2),  # common.c:831
    FieldSpec("UsageFactor", 11, min_prefix=6),  # common.c:838
    FieldSpec("UsageThreshold", 10, header="UsageThres", min_prefix=6),  # common.c:843
    FieldSpec("User", 10, min_prefix=1),  # common.c:867
]

# Default format strings, verbatim from real sacctmgr.
_ACCOUNT_DEFAULT = "Acc,Des,O"  # account_functions.c:400
_ACCOUNT_WITHASSOC = (  # account_functions.c:402-408
    "Cl,ParentN,U,Share,Priority,GrpJ,GrpN,GrpCPUs,GrpMEM,GrpS,GrpWall,"
    "GrpCPUMins,MaxJ,MaxN,MaxCPUs,MaxS,MaxW,MaxCPUMins,QOS,DefaultQOS"
)
_USER_DEFAULT = "U,DefaultA,Ad"  # user_functions.c:968
_ASSOC_DEFAULT = (  # association_functions.c:793-801
    "Cluster,Account,User,Part,Share,Priority,GrpJ,GrpTRES,GrpS,GrpWall,"
    "GrpTRESMins,MaxJ,MaxTRES,MaxTRESPerN,MaxS,MaxW,MaxTRESMins,QOS,"
    "DefaultQOS,GrpTRESRunMins"
)
_CLUSTER_DEFAULT = (  # cluster_functions.c:482-489
    "Cl,Controlh,Controlp,RPC,Fa,GrpJ,GrpTRES,GrpS,MaxJ,MaxTRES,MaxS,MaxW,QOS,DefaultQOS"
)
_QOS_DEFAULT = (  # qos_functions.c:1178-1193
    "Name,Prio,GraceT,Preempt,PreemptE,PreemptM,Flags%40,UsageThres,"
    "UsageFactor,GrpTRES,GrpTRESMins,GrpTRESRunMins,GrpJ,GrpS,GrpW,"
    "MaxTRES,MaxTRESPerN,MaxTRESMins,MaxW,MaxTRESPerUser,MaxJobsPerUser,"
    "MaxSubmitJobsPerUser,MaxTRESPerAcct,MaxTRESRunMinsPerAcct%22,"
    "MaxTRESRunMinsPerUser%22,MaxJobsPerAcct,MaxSubmitJobsPerAcct,MinTRES"
)
_TRES_DEFAULT = "Type,Name%15,ID"  # tres_function.c:152


class SacctmgrEmulator:
    """Emulates sacctmgr commands for account management."""

    def __init__(self, database: SlurmDatabase, time_engine: TimeEngine):
        self.database = database
        self.time_engine = time_engine
        # Mirrors sacctmgr's global ``exit_code`` (sacctmgr.c:61): reset to 0 at
        # the start of each command, set to 1 by any error path. The dispatcher
        # propagates it to the process exit status and routes failing output
        # to stderr.
        self.exit_code = 0
        # True when a failing command's message belongs on stdout, not
        # stderr (real sacctmgr prints "Nothing modified" with printf but
        # still exits 1).
        self.stdout_error = False
        self._mode = OutputMode()

    def _fail(self, message: str) -> str:
        """Record a non-zero exit (matching real sacctmgr) and return ``message``."""
        self.exit_code = 1
        return message

    def _nothing_modified(self) -> str:
        """No-op modify: message on stdout, process exit code 1.

        The modify branch prints "  Nothing modified" with printf and
        returns SLURM_ERROR (account_functions.c:727-729), and
        _modify_it() turns any non-SUCCESS error_code into the global
        exit_code=1 (sacctmgr.c:982-984) — so the process exits 1 even
        though the message goes to stdout.
        """
        self.exit_code = 1
        self.stdout_error = True
        return "  Nothing modified"

    def handle_command(self, args: list[str]) -> str:
        """Process sacctmgr command and return output."""
        self.exit_code = 0
        self.stdout_error = False
        # -i/--immediate is accepted but has no effect: the emulator is
        # headless and never shows real sacctmgr's commit prompt.
        self._mode, _immediate, args = extract_output_flags(args, shorts="npPi")
        args = self._strip_cluster_flag(args)
        try:
            return self._dispatch(args)
        except UnknownFieldError as e:
            # common.c:882-885: bare "Unknown field '%s'" on stderr, exit 1.
            return self._fail(f"Unknown field '{e.token}'")

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
        if command == "-V":
            return f"slurm-emulator {__version__}"
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
            return self._show_association(args[1:])
        if entity in {"qos", "qoss"}:
            return self._list_qos(args[1:])
        if entity in {"cluster", "clusters"}:
            return self._list_clusters(args[1:])
        if entity in {"user", "users"}:
            return self._list_users(args[1:])
        if entity == "tres":
            return self._list_tres(args[1:])
        return self._fail(f" error: Unknown entity for show: {entity}")

    def _add_account(self, args: list[str]) -> str:
        """Add account command."""
        if not args:
            return self._fail(" error: No account name specified")

        account_name = args[0]

        # Parse additional parameters
        description = ""
        organization = "emulator"
        parent = None
        target_cluster = None

        for arg in args[1:]:
            if arg.startswith("description="):
                description = arg.split("=", 1)[1].strip('"')
            elif arg.startswith("organization="):
                organization = arg.split("=", 1)[1].strip('"')
            elif arg.startswith("parent="):
                # Real sacctmgr runs the value through strip_quotes()
                # (association_functions.c:512), same as description/organization.
                parent = arg.split("=", 1)[1].strip("\"'")
            elif arg.startswith("cluster="):
                target_cluster = arg.split("=", 1)[1]

        # Check if account already exists
        existing = self.database.get_account(account_name)
        if existing:
            # Account exists globally — but if cluster= specified, just create association
            if target_cluster:
                if not self.database.get_cluster(target_cluster):
                    return self._fail(f" error: Cluster {target_cluster} does not exist")
                assoc_key = self.database._association_key("", account_name, target_cluster)
                if assoc_key not in self.database.associations:
                    self.database.associations[assoc_key] = Association(
                        account=account_name,
                        user="",
                        cluster=target_cluster,
                        parent=existing.parent,
                    )
                self.database.save_state()
                return f" Adding Account(s)\n  {account_name}\n Settings\n  Cluster    = {target_cluster}"
            # Re-adding an existing account is NOT an error in real sacctmgr:
            # SLURM_NO_CHANGE_IN_DATA prints slurm_strerror(rc) to stdout and
            # exits 0 (account_functions.c:342-343, slurm_errno.c:205-207).
            return " Data has not changed since time specified"

        # Add global account (also creates the account-level association on the
        # current cluster carrying parent_acct).
        self.database.add_account(account_name, description, organization, parent)

        # If cluster= specified, also create the account-level association there.
        if target_cluster:
            if not self.database.get_cluster(target_cluster):
                return self._fail(f" error: Cluster {target_cluster} does not exist")
            assoc_key = self.database._association_key("", account_name, target_cluster)
            self.database.associations[assoc_key] = Association(
                account=account_name, user="", cluster=target_cluster, parent=parent
            )

        self.database.save_state()

        return f" Adding Account(s)\n  {account_name}\n Settings\n  Parent     = {parent or 'root'}\n  Description = {description}"

    def _add_user(self, args: list[str]) -> str:
        """Add user command."""
        if not args:
            return self._fail(" error: No user name specified")

        username = args[0]
        account = ""
        default_account = ""
        target_cluster = None
        partitions: list[str] = []

        # Parse parameters
        for arg in args[1:]:
            lowered = arg.lower()
            if arg.startswith("account="):
                account = arg.split("=", 1)[1]
            elif arg.startswith("DefaultAccount="):
                default_account = arg.split("=", 1)[1]
            elif arg.startswith("cluster="):
                target_cluster = arg.split("=", 1)[1]
            elif lowered.startswith("partitions="):
                # Comma-joined list of partition names. Real Slurm's
                # prefix-match also lets ``Partition=`` (singular) reach
                # the same handler — covered below.
                value = arg.split("=", 1)[1]
                partitions = [p for p in value.split(",") if p]
            elif lowered.startswith("partition="):
                value = arg.split("=", 1)[1]
                if value:
                    partitions = [value]
            elif lowered.startswith("defaultpartition="):
                # Real sacctmgr does NOT recognise DefaultPartition on
                # add user — neither user_functions.c nor
                # sacctmgr_set_assoc_rec accepts it, so it falls through
                # to the "Unknown option" branch with exit_code=1. The
                # emulator must do the same so callers that rely on the
                # rejection see it here too.
                return self._fail(f" Unknown option: {arg}")
            # Other association attributes (Share, FairShare, Priority,
            # GrpJobs, MaxTRES, …) are silently accepted: real sacctmgr
            # supports them and the emulator does not model them yet.

        # Add user if doesn't exist
        if not self.database.get_user(username):
            self.database.add_user(username, default_account)

        # Add association if account specified
        if account:
            if not self.database.get_account(account):
                return self._fail(f" error: Account {account} does not exist")
            # One association row per partition (matches
            # _add_assoc_cond_partition in as_mysql_assoc.c — no base
            # row is created when partitions are given).
            if partitions:
                for part in partitions:
                    self.database.add_association(
                        username,
                        account,
                        cluster=target_cluster,
                        partition=part,
                    )
            else:
                self.database.add_association(
                    username,
                    account,
                    cluster=target_cluster,
                )

        self.database.save_state()
        return f" Adding User(s)\n  {username}\n Settings\n  Account     = {account}\n  DefaultAccount = {default_account}"

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

    def _modify_account(self, args: list[str]) -> str:
        """Modify account command."""
        if not args:
            return self._fail(" error: No account name specified")

        # Look for 'set' keyword
        set_index = -1
        for i, arg in enumerate(args):
            if arg.lower() == "set":
                set_index = i
                break

        if set_index == -1:
            return self._fail(" error: No 'set' clause found")

        account_name = self._extract_account_filter(args[:set_index])
        account = self.database.get_account(account_name) if account_name else None

        # Parse the set clause up front so reparenting can be handled specially.
        set_pairs: list[tuple[str, str]] = []
        for arg in args[set_index + 1 :]:
            if "=" in arg:
                key, value = arg.split("=", 1)
                set_pairs.append((key.lower(), value))

        # ``set parent=`` reparents the account-level association. Match real
        # sacctmgr semantics (account_functions.c:715-748): a condition that
        # matches no account, or a no-op change, prints "  Nothing modified"
        # to stdout but exits 1 — the branch returns SLURM_ERROR and
        # _modify_it() sets the global exit_code (sacctmgr.c:982-984); a
        # missing parent account is its own error with exit 1; a real
        # change prints "Modified account associations...".
        parent_value = next((v for k, v in set_pairs if k == "parent"), None)
        if parent_value is not None:
            # Real sacctmgr strips quotes from the parent value (strip_quotes,
            # association_functions.c:512) before resolving the account.
            parent_value = parent_value.strip("\"'")
            # Parent is an account name — fold so a case-only "change" is
            # correctly seen as a no-op against the stored lower-cased parent.
            parent_value = fold_account(parent_value)
            if not account:
                return self._nothing_modified()
            if self.database.get_account(parent_value) is None:
                return self._fail(f" Parent Account {parent_value} doesn't exist.")
            if account.parent == parent_value:
                return self._nothing_modified()
            self.database.set_account_parent(account.name, parent_value)
            self.database.save_state()
            return f" Modified account associations...\n  {account.name}"

        if not account:
            return self._nothing_modified()

        # Process set parameters
        modifications = []
        for arg in args[set_index + 1 :]:
            if "=" in arg:
                key, value = arg.split("=", 1)
                key = key.lower()

                if key == "fairshare":
                    account.fairshare = int(value)
                    modifications.append(f"fairshare={value}")
                elif key == "qos":
                    account.qos = value
                    modifications.append(f"qos={value}")
                elif key.startswith("grptresmin"):
                    # Handle GrpTRESMins=billing=72000 or GrpTRESMins=cpu=600000,ram=614400
                    tres_spec = value
                    if "=" in tres_spec:
                        for tres_item in tres_spec.split(","):
                            if "=" in tres_item:
                                tres_type, tres_value = tres_item.split("=", 1)
                                account.limits[f"GrpTRESMins:{tres_type}"] = int(tres_value)
                    else:
                        account.limits["GrpTRESMins"] = int(tres_spec)
                    modifications.append(f"GrpTRESMins={value}")
                elif key.startswith("maxtresmin"):
                    # Handle MaxTRESMins=billing=72000 or MaxTRESMins=cpu=600000,ram=614400
                    tres_spec = value
                    if "=" in tres_spec:
                        for tres_item in tres_spec.split(","):
                            if "=" in tres_item:
                                tres_type, tres_value = tres_item.split("=", 1)
                                account.limits[f"MaxTRESMins:{tres_type}"] = int(tres_value)
                    else:
                        account.limits["MaxTRESMins"] = int(tres_spec)
                    modifications.append(f"MaxTRESMins={value}")
                elif key.startswith("grptres") and not key.startswith("grptresmin"):
                    # Handle GrpTRES=CPU=10 or GrpTRES=cpu=10,node=5 (concurrent limits)
                    tres_spec = value
                    if "=" in tres_spec:
                        for tres_item in tres_spec.split(","):
                            if "=" in tres_item:
                                tres_type, tres_value = tres_item.split("=", 1)
                                account.limits[f"GrpTRES:{tres_type}"] = int(tres_value)
                    else:
                        account.limits["GrpTRES"] = int(tres_spec)
                    modifications.append(f"GrpTRES={value}")
                elif key == "rawusage":
                    # Handle raw usage reset
                    if value == "0":
                        self.database.reset_raw_usage(account.name)
                        modifications.append("RawUsage=0")

        self.database.save_state()

        return f" Modified account...\n  {account.name}\n Settings\n  " + "\n  ".join(modifications)

    def _modify_user(self, args: list[str]) -> str:
        """Modify user command."""
        # Parse user modification - typically for per-user limits
        if "where" not in args:
            return self._fail(" error: No where clause found")

        where_index = args.index("where")
        set_index = -1

        for i, arg in enumerate(args):
            if arg.lower() == "set":
                set_index = i
                break

        if set_index == -1:
            return self._fail(" error: No 'set' clause found")

        # Parse where clause for account
        account = ""
        for arg in args[where_index + 1 : set_index]:
            if arg.startswith("account="):
                account = arg.split("=", 1)[1]

        if not account:
            return self._fail(" error: No account specified in where clause")

        return f" Modified user associations for account {account}"

    def _remove_account(self, args: list[str]) -> str:
        """Remove account command."""
        if "where" not in args:
            return self._fail(" error: No where clause found")

        where_index = args.index("where")

        # Parse where clause
        account_name = ""
        for arg in args[where_index + 1 :]:
            if arg.startswith("name="):
                account_name = arg.split("=", 1)[1]

        if not account_name:
            return self._fail(" error: No account name specified in where clause")

        if not self.database.get_account(account_name):
            return self._fail(f" error: Account {account_name} does not exist")

        self.database.delete_account(account_name)
        self.database.save_state()

        return f" Deleting account(s)...\n  {account_name}"

    def _remove_user(self, args: list[str]) -> str:
        """Remove user command."""
        if "where" not in args:
            return self._fail(" error: No where clause found")

        where_index = args.index("where")

        # Parse where clause
        account = ""
        username = ""

        for arg in args[where_index + 1 :]:
            if arg.startswith("account="):
                account = arg.split("=", 1)[1]
            elif arg.startswith("name="):
                username = arg.split("=", 1)[1]

        if account and username:
            # Remove every association row for this (user, account),
            # including every partition-scoped row — mirrors real
            # sacctmgr remove user where name=… and account=… .
            self.database.delete_user_associations(username, account)
            result = f" Deleting user association...\n  User: {username}\n  Account: {account}"
        elif account:
            # Remove all users from account
            users = self.database.list_account_users(account)
            for user in users:
                self.database.delete_user_associations(user, account)
            result = f" Deleting {len(users)} user association(s) from account {account}"
        else:
            return self._fail(" error: Insufficient parameters in where clause")

        self.database.save_state()
        return result

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
        """Add QOS command.

        Parses: add qos <name> [set] [flags=X] [key=value ...]
        The ``set`` keyword is optional and simply skipped, matching real
        sacctmgr behaviour.  Each key=value pair must be a **separate**
        argv element (just like real sacctmgr).
        """
        if not args:
            return self._fail(" error: No qos name specified")

        qos_name = args[0]

        if qos_name in self.database.qos_list:
            return self._fail(f" error: QOS {qos_name} already exists")

        qos = QOS(name=qos_name)

        for arg in args[1:]:
            lower = arg.lower()
            if lower == "set":
                continue
            if "=" not in arg:
                return self._fail(
                    f" Unknown option: {arg}\n Use keyword 'where' to modify condition"
                )
            key, value = arg.split("=", 1)
            key = key.lower()
            if key == "flags":
                qos.flags = value
            elif key == "grptres":
                qos.grp_tres = value
            elif key == "maxjobs":
                qos.max_jobs = int(value)
            elif key == "maxsubmit":
                qos.max_submit = int(value)
            elif key == "maxwall":
                qos.max_wall = value
            elif key == "mintresperjob":
                qos.min_tres_per_job = value
            else:
                return self._fail(
                    f" Unknown option: {arg}\n Use keyword 'where' to modify condition"
                )

        self.database.qos_list[qos_name] = qos
        return f" Adding QOS(s)\n  Name          = {qos_name}"

    def _modify_qos(self, args: list[str]) -> str:
        """Modify QOS command.

        Parses: modify qos <name> set key=value [key=value ...]
        """
        if not args:
            return self._fail(" error: No qos name specified")

        qos_name = args[0]
        qos = self.database.qos_list.get(qos_name)
        if qos is None:
            # Same SLURM_NO_CHANGE_IN_DATA shape as accounts: stdout, exit 1.
            return self._nothing_modified()

        set_index = -1
        for i, arg in enumerate(args):
            if arg.lower() == "set":
                set_index = i
                break

        if set_index == -1:
            return self._fail(" error: No 'set' clause found")

        for arg in args[set_index + 1 :]:
            if "=" not in arg:
                return self._fail(f" Unknown option: {arg}")
            key, value = arg.split("=", 1)
            key = key.lower()
            if key == "flags":
                qos.flags = value
            elif key == "grptres":
                qos.grp_tres = value
            elif key == "maxjobs":
                qos.max_jobs = int(value)
            elif key == "maxsubmit":
                qos.max_submit = int(value)
            elif key == "maxwall":
                qos.max_wall = value
            elif key == "mintresperjob":
                qos.min_tres_per_job = value

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
        """List QOS (real default format from qos_functions.c:1178-1193)."""
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
                "MaxJobs": str(q.max_jobs) if q.max_jobs else "",
                "MaxSubmit": str(q.max_submit) if q.max_submit else "",
                "MaxWall": q.max_wall,
                "MinTRES": q.min_tres_per_job,
            }

        return render_table(fields, [_row(q) for q in qos_items], self._mode)

    def _list_clusters(self, args: list[str]) -> str:
        """List clusters (real default format from cluster_functions.c:482-489)."""
        fields = self._resolve(_CLUSTER_DEFAULT, args)

        rows = []
        for cluster in self.database.list_clusters():
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
        """List accounts (real default ``Acc,Des,O``)."""
        fields = self._resolve(_ACCOUNT_DEFAULT, args)

        rows = [
            {
                "Account": a.name,
                "Descr": a.description,
                "Org": a.organization,
            }
            for a in self.database.list_accounts()
        ]
        return render_table(fields, rows, self._mode)

    def _list_users(self, args: list[str]) -> str:
        """List users (real default ``U,DefaultA,Ad``)."""
        fields = self._resolve(_USER_DEFAULT, args)

        rows = [
            {
                "User": u.name,
                "Def Acct": u.default_account,
                # Real sacctmgr prints "None" for regular users.
                "Admin": "None",
            }
            for u in self.database.users.values()
        ]
        return render_table(fields, rows, self._mode)

    def _list_associations(self, args: list[str]) -> str:
        """List associations (real default from association_functions.c:793-801)."""
        fields = self._resolve(_ASSOC_DEFAULT, args)

        account_filter = None
        user_filter = None
        for arg in args:
            if arg.startswith("account="):
                # Case-insensitive account filter (see _show_association).
                account_filter = fold_account(arg.split("=", 1)[1])
            elif arg.startswith("user="):
                user_filter = arg.split("=", 1)[1]

        associations = list(self.database.associations.values())
        if account_filter:
            associations = [a for a in associations if a.account == account_filter]
        if user_filter:
            associations = [a for a in associations if a.user == user_filter]

        return render_table(fields, [self._assoc_row(a) for a in associations], self._mode)

    def _assoc_row(self, assoc: Association) -> dict[str, str]:
        account_obj = self.database.get_account(assoc.account)
        limits = ",".join(f"{k}={v}" for k, v in assoc.limits.items()) if assoc.limits else ""
        return {
            "Cluster": assoc.cluster,
            "Account": assoc.account,
            "User": assoc.user,
            "Partition": assoc.partition or "",
            # parent_acct lives on the account-level row (empty User);
            # user rows print blank (as_mysql_assoc.c:2116-2126).
            "ParentName": (assoc.parent or "") if assoc.user == "" else "",
            "QOS": account_obj.qos if account_obj else "",
            "MaxTRESMins": limits,
        }

    def _list_tres(self, args: list[str]) -> str:
        """List TRES types (real default ``Type,Name%15,ID``)."""
        fields = self._resolve(_TRES_DEFAULT, args)

        rows = []
        for idx, tres_type in enumerate(self.database.tres_types, start=1):
            if "/" in tres_type:
                type_part, name_part = tres_type.split("/", 1)
            else:
                type_part, name_part = tres_type, ""
            rows.append({"Type": type_part, "Name": name_part, "ID": str(idx)})
        return render_table(fields, rows, self._mode)

    def _show_account(self, args: list[str]) -> str:
        """Show account command.

        Mirrors ``sacctmgr show account`` (account_functions.c:436-572):

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

    def _show_association(self, args: list[str]) -> str:
        """Show association command."""
        # Parse where clause and optional format=. The ``where`` keyword is
        # optional in real sacctmgr — ``user=``/``account=`` conditions are
        # accepted whether or not it is present.
        user = ""
        account = ""
        for arg in args:
            if arg.startswith("user="):
                user = arg.split("=", 1)[1]
            elif arg.startswith("account="):
                # Account filters are case-insensitive (folded to match the
                # stored lower-cased rows), so ``account=2026_00A`` finds
                # ``2026_00a`` — the mismatch real Slurm papers over.
                account = fold_account(arg.split("=", 1)[1])

        fields = self._resolve(_ASSOC_DEFAULT, args)

        if user and account:
            associations = self.database.list_user_associations(user, account)
        else:
            associations = [
                a
                for a in self.database.associations.values()
                if not account or a.account == account
            ]

        return render_table(fields, [self._assoc_row(a) for a in associations], self._mode)

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
