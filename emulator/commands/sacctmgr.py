"""sacctmgr command emulator."""

from emulator import __version__
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


class SacctmgrEmulator:
    """Emulates sacctmgr commands for account management."""

    def __init__(self, database: SlurmDatabase, time_engine: TimeEngine):
        self.database = database
        self.time_engine = time_engine

    def handle_command(self, args: list[str]) -> str:
        """Process sacctmgr command and return output."""
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
        return f"sacctmgr: error: Unknown command: {command}"

    def _handle_add(self, args: list[str]) -> str:
        """Handle add commands."""
        if not args:
            return "sacctmgr: error: No entity specified for add"

        entity = args[0].lower()

        if entity == "account":
            return self._add_account(args[1:])
        if entity == "user":
            return self._add_user(args[1:])
        return f"sacctmgr: error: Unknown entity for add: {entity}"

    def _handle_modify(self, args: list[str]) -> str:
        """Handle modify commands."""
        if not args:
            return "sacctmgr: error: No entity specified for modify"

        entity = args[0].lower()

        if entity == "account":
            return self._modify_account(args[1:])
        if entity == "user":
            return self._modify_user(args[1:])
        return f"sacctmgr: error: Unknown entity for modify: {entity}"

    def _handle_remove(self, args: list[str]) -> str:
        """Handle remove/delete commands."""
        if not args:
            return "sacctmgr: error: No entity specified for remove"

        entity = args[0].lower()

        if entity == "account":
            return self._remove_account(args[1:])
        if entity == "user":
            return self._remove_user(args[1:])
        return f"sacctmgr: error: Unknown entity for remove: {entity}"

    def _handle_list(self, args: list[str]) -> str:
        """Handle list commands."""
        if not args:
            return "sacctmgr: error: No entity specified for list"

        entity = args[0].lower()

        if entity in {"account", "accounts"}:
            return self._list_accounts(args[1:])
        if entity in {"user", "users"}:
            return self._list_users(args[1:])
        if entity in {"association", "associations"}:
            return self._list_associations(args[1:])
        if entity == "tres":
            return self._list_tres()
        return f"sacctmgr: error: Unknown entity for list: {entity}"

    def _handle_show(self, args: list[str]) -> str:
        """Handle show commands."""
        if not args:
            return "sacctmgr: error: No entity specified for show"

        entity = args[0].lower()

        if entity == "account":
            return self._show_account(args[1:])
        if entity == "association":
            return self._show_association(args[1:])
        return f"sacctmgr: error: Unknown entity for show: {entity}"

    def _add_account(self, args: list[str]) -> str:
        """Add account command."""
        if not args:
            return "sacctmgr: error: No account name specified"

        account_name = args[0]

        # Parse additional parameters
        description = ""
        organization = "emulator"
        parent = None

        for arg in args[1:]:
            if arg.startswith("description="):
                description = arg.split("=", 1)[1].strip('"')
            elif arg.startswith("organization="):
                organization = arg.split("=", 1)[1].strip('"')
            elif arg.startswith("parent="):
                parent = arg.split("=", 1)[1]

        # Check if account already exists
        if self.database.get_account(account_name):
            return f"sacctmgr: error: Account {account_name} already exists"

        # Add account
        self.database.add_account(account_name, description, organization, parent)
        self.database.save_state()

        return f" Adding Account(s)\n  {account_name}\n Settings\n  Parent     = {parent or 'root'}\n  Description = {description}"

    def _add_user(self, args: list[str]) -> str:
        """Add user command."""
        if not args:
            return "sacctmgr: error: No user name specified"

        username = args[0]
        account = ""
        default_account = ""

        # Parse parameters
        for arg in args[1:]:
            if arg.startswith("account="):
                account = arg.split("=", 1)[1]
            elif arg.startswith("DefaultAccount="):
                default_account = arg.split("=", 1)[1]

        # Add user if doesn't exist
        if not self.database.get_user(username):
            self.database.add_user(username, default_account)

        # Add association if account specified
        if account:
            if not self.database.get_account(account):
                return f"sacctmgr: error: Account {account} does not exist"
            self.database.add_association(username, account)

        self.database.save_state()
        return f" Adding User(s)\n  {username}\n Settings\n  Account     = {account}\n  DefaultAccount = {default_account}"

    def _modify_account(self, args: list[str]) -> str:
        """Modify account command."""
        if not args:
            return "sacctmgr: error: No account name specified"

        account_name = args[0]
        account = self.database.get_account(account_name)

        if not account:
            return f"sacctmgr: error: Account {account_name} does not exist"

        # Look for 'set' keyword
        set_index = -1
        for i, arg in enumerate(args):
            if arg.lower() == "set":
                set_index = i
                break

        if set_index == -1:
            return "sacctmgr: error: No 'set' clause found"

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
                    # Handle GrpTRESMins=billing=72000 or GrpTRESMins=CPU=1000
                    tres_spec = value
                    if "=" in tres_spec:
                        tres_type, tres_value = tres_spec.split("=", 1)
                        account.limits[f"GrpTRESMins:{tres_type}"] = int(tres_value)
                    else:
                        account.limits["GrpTRESMins"] = int(tres_spec)
                    modifications.append(f"GrpTRESMins={value}")
                elif key.startswith("maxtresmin"):
                    # Handle MaxTRESMins
                    tres_spec = value
                    if "=" in tres_spec:
                        tres_type, tres_value = tres_spec.split("=", 1)
                        account.limits[f"MaxTRESMins:{tres_type}"] = int(tres_value)
                    else:
                        account.limits["MaxTRESMins"] = int(tres_spec)
                    modifications.append(f"MaxTRESMins={value}")
                elif key.startswith("grptres") and not key.startswith("grptresmin"):
                    # Handle GrpTRES=CPU=10 or GrpTRES=node=5 (concurrent limits)
                    tres_spec = value
                    if "=" in tres_spec:
                        tres_type, tres_value = tres_spec.split("=", 1)
                        account.limits[f"GrpTRES:{tres_type}"] = int(tres_value)
                    else:
                        account.limits["GrpTRES"] = int(tres_spec)
                    modifications.append(f"GrpTRES={value}")
                elif key == "rawusage":
                    # Handle raw usage reset
                    if value == "0":
                        self.database.reset_raw_usage(account_name)
                        modifications.append("RawUsage=0")

        self.database.save_state()

        return f" Modified account...\n  {account_name}\n Settings\n  " + "\n  ".join(modifications)

    def _modify_user(self, args: list[str]) -> str:
        """Modify user command."""
        # Parse user modification - typically for per-user limits
        if "where" not in args:
            return "sacctmgr: error: No where clause found"

        where_index = args.index("where")
        set_index = -1

        for i, arg in enumerate(args):
            if arg.lower() == "set":
                set_index = i
                break

        if set_index == -1:
            return "sacctmgr: error: No 'set' clause found"

        # Parse where clause for account
        account = ""
        for arg in args[where_index + 1 : set_index]:
            if arg.startswith("account="):
                account = arg.split("=", 1)[1]

        if not account:
            return "sacctmgr: error: No account specified in where clause"

        return f" Modified user associations for account {account}"

    def _remove_account(self, args: list[str]) -> str:
        """Remove account command."""
        if "where" not in args:
            return "sacctmgr: error: No where clause found"

        where_index = args.index("where")

        # Parse where clause
        account_name = ""
        for arg in args[where_index + 1 :]:
            if arg.startswith("name="):
                account_name = arg.split("=", 1)[1]

        if not account_name:
            return "sacctmgr: error: No account name specified in where clause"

        if not self.database.get_account(account_name):
            return f"sacctmgr: error: Account {account_name} does not exist"

        self.database.delete_account(account_name)
        self.database.save_state()

        return f" Deleting account(s)...\n  {account_name}"

    def _remove_user(self, args: list[str]) -> str:
        """Remove user command."""
        if "where" not in args:
            return "sacctmgr: error: No where clause found"

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
            # Remove specific association
            self.database.delete_association(username, account)
            result = f" Deleting user association...\n  User: {username}\n  Account: {account}"
        elif account:
            # Remove all users from account
            users = self.database.list_account_users(account)
            for user in users:
                self.database.delete_association(user, account)
            result = f" Deleting {len(users)} user association(s) from account {account}"
        else:
            return "sacctmgr: error: Insufficient parameters in where clause"

        self.database.save_state()
        return result

    def _list_accounts(self, args: list[str]) -> str:
        """List accounts command."""
        accounts = self.database.list_accounts()

        if not accounts:
            return "Account|Descr|Org|"

        lines = ["Account|Descr|Org|"]
        for account in accounts:
            lines.append(f"{account.name}|{account.description}|{account.organization}|")

        return "\n".join(lines)

    def _list_users(self, args: list[str]) -> str:
        """List users command."""
        users = list(self.database.users.values())

        if not users:
            return "User|DefaultAccount|"

        lines = ["User|DefaultAccount|"]
        for user in users:
            lines.append(f"{user.name}|{user.default_account}|")

        return "\n".join(lines)

    def _list_associations(self, args: list[str]) -> str:
        """List associations command."""
        # Parse format and where clauses
        format_fields = ["account", "user"]
        account_filter = None

        for i, arg in enumerate(args):
            if arg.startswith("format="):
                format_spec = arg.split("=", 1)[1]
                format_fields = [f.strip().lower() for f in format_spec.split(",")]
            elif arg.startswith("where"):
                # Look for account filter
                for j in range(i + 1, len(args)):
                    if args[j].startswith("account="):
                        account_filter = args[j].split("=", 1)[1]

        associations = list(self.database.associations.values())

        if account_filter:
            associations = [a for a in associations if a.account == account_filter]

        # Build header
        header = "|".join([f.title() for f in format_fields]) + "|"
        lines = [header]

        # Build data lines
        for assoc in associations:
            row_data = []
            for field in format_fields:
                if field == "account":
                    row_data.append(assoc.account)
                elif field == "user":
                    row_data.append(assoc.user)
                elif field == "qos":
                    account_obj = self.database.get_account(assoc.account)
                    row_data.append(account_obj.qos if account_obj else "")
                elif field in {"maxtresmin", "maxtressmins"}:
                    # Format user limits
                    limits_str = ",".join([f"{k}={v}" for k, v in assoc.limits.items()])
                    row_data.append(limits_str)
                else:
                    row_data.append("")

            lines.append("|".join(row_data) + "|")

        return "\n".join(lines)

    def _list_tres(self) -> str:
        """List TRES types."""
        lines = ["Type|Name|"]
        for tres_type in self.database.tres_types:
            if "/" in tres_type:
                type_part, name_part = tres_type.split("/", 1)
                lines.append(f"{type_part}|{name_part}|")
            else:
                lines.append(f"{tres_type}||")

        return "\n".join(lines)

    def _show_account(self, args: list[str]) -> str:
        """Show account command."""
        if not args:
            return "sacctmgr: error: No account name specified"

        account_name = args[0]
        account = self.database.get_account(account_name)

        if not account:
            return ""  # No output for non-existent accounts

        return f"{account.name}|{account.description}|{account.organization}|"

    def _show_association(self, args: list[str]) -> str:
        """Show association command."""
        # Parse where clause
        user = ""
        account = ""

        if "where" in args:
            where_index = args.index("where")
            for arg in args[where_index + 1 :]:
                if arg.startswith("user="):
                    user = arg.split("=", 1)[1]
                elif arg.startswith("account="):
                    account = arg.split("=", 1)[1]

        if user and account:
            assoc = self.database.get_association(user, account)
            if assoc:
                return f"{assoc.account}|{assoc.user}||||||||| |"
            return ""
        # List all associations for account
        associations = [
            a for a in self.database.associations.values() if not account or a.account == account
        ]

        lines = []
        for assoc in associations:
            lines.append(f"{assoc.account}|{assoc.user}||||||||| |")

        return "\n".join(lines)

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
