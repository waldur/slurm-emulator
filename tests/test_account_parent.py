"""Tests for account parent / hierarchy parity with real Slurm.

Validates emulator parity with real Slurm (/Users/ilja/workspace/slurm):

- ``ParentName`` is an *association* field (``PRINT_PNAME`` →
  ``assoc->parent_acct``, association_functions.c:732-734). On
  ``sacctmgr show account`` the associations are only loaded with
  ``WithAssoc``; without it the ``default:`` branch prints NULL, so
  ParentName is blank (account_functions.c:460-571).
- For an association, ``parent_acct`` is populated only on the
  account-level row (empty ``User``); user rows leave it NULL and print
  blank (as_mysql_assoc.c:2116-2126).
- ``modify account ... set parent=`` reparents the account-level
  association. A no-op change or a condition matching no account prints
  "Nothing modified" and exits 1; a missing parent account is its own
  error; a real change prints "Modified account associations..."
  (account_functions.c:715-748, exit mapping in sacctmgr.c:982-984).
"""

from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


def _emulator(tmp_path):
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    em = SacctmgrEmulator(db, TimeEngine())
    # root -> c-org -> p-proj, plus a user under the project account.
    em.handle_command(["add", "account", "c-org", "parent=root"])
    em.handle_command(["add", "account", "p-proj", "parent=c-org"])
    em.handle_command(["add", "user", "alice", "account=p-proj"])
    return em


class TestShowAccountParentName:
    """``show account format=Account,ParentName`` — parity with real Slurm."""

    def test_parentname_is_blank_without_withassoc(self, tmp_path):
        # Real Slurm: associations not loaded → ParentName prints blank.
        em = _emulator(tmp_path)
        out = em.handle_command(
            ["show", "account", "p-proj", "format=Account,ParentName", "-n", "-P"]
        )
        # ParentName column is empty (emulator uses a trailing-``|`` row shape).
        assert out == "p-proj||"
        # The decisive parity property: the parent is NOT recoverable here.
        assert "c-org" not in out

    def test_parentname_populated_with_withassoc(self, tmp_path):
        # Real Slurm: WithAssoc loads the account-level association → parent shown.
        em = _emulator(tmp_path)
        out = em.handle_command(
            ["show", "account", "p-proj", "withassoc", "format=Account,ParentName"]
        )
        rows = [r for r in out.splitlines() if r.startswith("p-proj|")]
        # Account-level row carries the parent.
        assert "p-proj|c-org|" in rows


class TestShowAssociationParentName:
    """``show assoc ... format=Account,ParentName,User`` — parity with real Slurm."""

    def test_account_level_row_carries_parent(self, tmp_path):
        em = _emulator(tmp_path)
        out = em.handle_command(
            ["show", "assoc", "account=p-proj", "format=Account,ParentName,User", "-n", "-P"]
        )
        # The account-level row (empty User) reports the parent.
        assert "p-proj|c-org||" in out.splitlines()

    def test_user_row_parentname_is_blank(self, tmp_path):
        em = _emulator(tmp_path)
        out = em.handle_command(
            ["show", "assoc", "account=p-proj", "format=Account,ParentName,User", "-n", "-P"]
        )
        # The user row prints a blank ParentName (assoc->parent_acct is NULL).
        assert "p-proj||alice|" in out.splitlines()

    def test_where_keyword_is_optional(self, tmp_path):
        em = _emulator(tmp_path)
        bare = em.handle_command(
            ["show", "assoc", "account=p-proj", "format=Account,ParentName,User", "-n", "-P"]
        )
        with_where = em.handle_command(
            [
                "show",
                "assoc",
                "where",
                "account=p-proj",
                "format=Account,ParentName,User",
                "-n",
                "-P",
            ]
        )
        assert sorted(bare.splitlines()) == sorted(with_where.splitlines())


class TestModifyAccountParent:
    """``modify account ... set parent=`` — reparent semantics and exit codes."""

    def test_reparent_to_new_parent_succeeds(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "account", "c-new", "parent=root"])
        out = em.handle_command(
            ["modify", "account", "where", "name=p-proj", "set", "parent=c-new"]
        )
        assert "Modified account associations" in out
        assert em.exit_code == 0
        assert em.database.get_account("p-proj").parent == "c-new"
        # The change is visible through the association too.
        assoc = em.handle_command(
            ["show", "assoc", "account=p-proj", "format=Account,ParentName,User", "-n", "-P"]
        )
        assert "p-proj|c-new||" in assoc.splitlines()

    def test_reparent_to_same_parent_is_nothing_modified(self, tmp_path):
        em = _emulator(tmp_path)
        out = em.handle_command(
            ["modify", "account", "where", "name=p-proj", "set", "parent=c-org"]
        )
        assert "Nothing modified" in out
        assert em.exit_code == 1

    def test_reparent_to_missing_parent_errors(self, tmp_path):
        em = _emulator(tmp_path)
        out = em.handle_command(
            ["modify", "account", "where", "name=p-proj", "set", "parent=c-ghost"]
        )
        assert "doesn't exist" in out
        assert em.exit_code == 1
        # Parent unchanged.
        assert em.database.get_account("p-proj").parent == "c-org"

    def test_modify_missing_account_is_nothing_modified(self, tmp_path):
        em = _emulator(tmp_path)
        out = em.handle_command(
            ["modify", "account", "where", "name=p-ghost", "set", "parent=c-org"]
        )
        assert "Nothing modified" in out
        assert em.exit_code == 1

    def test_where_name_filter_form_is_parsed(self, tmp_path):
        # The agent uses ``where name=<acct>`` — real Slurm parses ``name=`` as
        # the account-name condition (account_functions.c:103-116).
        em = _emulator(tmp_path)
        em.handle_command(["add", "account", "c-new", "parent=root"])
        em.handle_command(["modify", "account", "where", "name=p-proj", "set", "parent=c-new"])
        assert em.database.get_account("p-proj").parent == "c-new"


class TestAddAccountCreatesParentAssociation:
    def test_add_account_creates_account_level_association_with_parent(self, tmp_path):
        em = _emulator(tmp_path)
        key = em.database._association_key("", "p-proj", "default")
        assoc = em.database.associations.get(key)
        assert assoc is not None
        assert assoc.user == ""
        assert assoc.parent == "c-org"
