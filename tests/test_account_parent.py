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
  "  Nothing modified" to stdout but exits 1 — the branch returns
  SLURM_ERROR (account_functions.c:727-729) and ``_modify_it()`` sets
  the global ``exit_code = 1`` on any non-SUCCESS error_code
  (sacctmgr.c:982-984); a missing parent account is its own error with
  exit 1; a real change prints "Modified account associations...".
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
        # ParentName column is empty (parsable2: no trailing pipe).
        assert out == "p-proj|"
        # The decisive parity property: the parent is NOT recoverable here.
        assert "c-org" not in out

    def test_parentname_populated_with_withassoc(self, tmp_path):
        # Real Slurm: WithAssoc loads the account-level association → parent shown.
        em = _emulator(tmp_path)
        out = em.handle_command(
            ["show", "account", "p-proj", "withassoc", "format=Account,ParentName", "-n", "-P"]
        )
        rows = [r for r in out.splitlines() if r.startswith("p-proj|")]
        # Account-level row carries the parent.
        assert "p-proj|c-org" in rows


class TestShowAssociationParentName:
    """``show assoc ... format=Account,ParentName,User`` — parity with real Slurm."""

    def test_account_level_row_carries_parent(self, tmp_path):
        em = _emulator(tmp_path)
        out = em.handle_command(
            ["show", "assoc", "account=p-proj", "format=Account,ParentName,User", "-n", "-P"]
        )
        # The account-level row (empty User) reports the parent.
        assert "p-proj|c-org|" in out.splitlines()

    def test_user_row_parentname_is_blank(self, tmp_path):
        em = _emulator(tmp_path)
        out = em.handle_command(
            ["show", "assoc", "account=p-proj", "format=Account,ParentName,User", "-n", "-P"]
        )
        # The user row prints a blank ParentName (assoc->parent_acct is NULL).
        assert "p-proj||alice" in out.splitlines()

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
        assert "p-proj|c-new|" in assoc.splitlines()

    def test_reparent_to_same_parent_is_nothing_modified(self, tmp_path):
        em = _emulator(tmp_path)
        out = em.handle_command(
            ["modify", "account", "where", "name=p-proj", "set", "parent=c-org"]
        )
        assert out == "  Nothing modified"
        # Real sacctmgr exits 1 here: the branch returns SLURM_ERROR
        # (account_functions.c:727-729) and _modify_it() sets the global
        # exit_code on any non-SUCCESS error_code (sacctmgr.c:982-984).
        # The message itself still goes to stdout.
        assert em.exit_code == 1
        assert em.stdout_error is True

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
        assert out == "  Nothing modified"
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


class TestParentValueQuoteStripping:
    """``parent=`` values are quote-stripped, matching real Slurm.

    Real sacctmgr runs the value through ``strip_quotes`` before storing it
    (association_functions.c:512), exactly as it does for description and
    organization (account_functions.c:224,243). The Waldur site agent quotes
    the value (``parent="acct"``) and passes it to subprocess without a shell,
    so the binary receives literal quotes; the emulator must strip them or the
    stored/returned parent carries quotes and parent comparisons break.
    """

    def test_add_account_strips_quotes_from_parent(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "account", "c-quoted", 'parent="root"'])
        # Stored parent has no quotes.
        assert em.database.get_account("c-quoted").parent == "root"
        # And it prints clean (not "root") in the association.
        out = em.handle_command(
            ["show", "assoc", "account=c-quoted", "format=Account,ParentName,User", "-n", "-P"]
        )
        assert "c-quoted|root|" in out.splitlines()

    def test_modify_account_strips_quotes_from_parent(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "account", "c-new", "parent=root"])
        out = em.handle_command(
            ["modify", "account", "where", "name=p-proj", "set", 'parent="c-new"']
        )
        assert "Modified account associations" in out
        assert em.exit_code == 0
        assert em.database.get_account("p-proj").parent == "c-new"


class TestAddExistingAccountIsNotAnError:
    """Re-adding an existing account must not set a non-zero exit code.

    Real sacctmgr reports SLURM_NO_CHANGE_IN_DATA and exits 0 in this case
    (account_functions.c:341-343). Returning exit 1 here breaks idempotent
    callers (e.g. the Waldur site agent's account provisioning), which is the
    regression that the 0.5.2 exit-code work introduced.
    """

    def test_readding_account_keeps_exit_code_zero(self, tmp_path):
        em = _emulator(tmp_path)
        out = em.handle_command(["add", "account", "c-org", "parent=root"])
        # Exact SLURM_NO_CHANGE_IN_DATA shape: printf(" %s", slurm_strerror(rc))
        # to stdout (account_functions.c:342-343, slurm_errno.c:205-207).
        assert out == " Data has not changed since time specified"
        assert em.exit_code == 0

    def test_modify_parent_still_reports_failure(self, tmp_path):
        # Guard: the fix must not flatten the exit code for genuine failures.
        em = _emulator(tmp_path)
        em.handle_command(["modify", "account", "where", "name=p-proj", "set", "parent=c-ghost"])
        assert em.exit_code == 1


class TestAccountNameCaseInsensitivity:
    """Account names are case-insensitive and stored lower-cased.

    Real Slurm folds every account name and condition to lower case:
    sacctmgr adds them to the acct list via ``slurm_addto_char_list``
    (account_functions.c:113,204), which normalises with ``xstrtolower``
    (slurm_protocol_defs.c:523-525,537-539). So ``add account 2026_00A``
    is stored and reported as ``2026_00a``, and ``show assoc
    account=2026_00A`` still finds it. The emulator must reproduce this so
    the Waldur site agent's ``get_account_parent`` — which reads the
    account back and compares names — is exercised against the same
    mixed-case mismatch that bit it in production.
    """

    def test_add_account_is_stored_lower_cased(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "account", "2026_00A", "parent=c-org"])
        # Looked up by any case, the stored name is folded.
        account = em.database.get_account("2026_00A")
        assert account is not None
        assert account.name == "2026_00a"
        assert em.database.get_account("2026_00a") is account

    def test_show_assoc_matches_mixed_case_query_and_reports_lower(self, tmp_path):
        # The exact production scenario: the account exists as 2026_00a, the
        # agent queries with the Waldur-supplied mixed case, and the row must
        # come back — reported in Slurm's stored (lower) form.
        em = _emulator(tmp_path)
        em.handle_command(["add", "account", "2026_00A", "parent=c-org"])
        out = em.handle_command(
            ["show", "assoc", "account=2026_00A", "format=Account,ParentName,User", "-n", "-P"]
        )
        # Account column is the folded name; the query still matched.
        assert "2026_00a|c-org|" in out.splitlines()

    def test_reparent_by_mixed_case_name(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "account", "2026_00A", "parent=c-org"])
        em.handle_command(["add", "account", "c-new", "parent=root"])
        out = em.handle_command(
            ["modify", "account", "where", "name=2026_00A", "set", "parent=c-new"]
        )
        assert "Modified account associations" in out
        assert em.exit_code == 0
        assert em.database.get_account("2026_00a").parent == "c-new"

    def test_reparent_to_same_parent_mixed_case_is_noop(self, tmp_path):
        # Parent 'C-ORG' differs from stored 'c-org' only in case: real Slurm
        # folds it and sees no change, printing "Nothing modified" / exit 1.
        # This is the precise loop the agent hit — a spurious reparent that
        # errored because the parent was already correct.
        em = _emulator(tmp_path)
        em.handle_command(["add", "account", "2026_00A", "parent=c-org"])
        out = em.handle_command(
            ["modify", "account", "where", "name=2026_00A", "set", "parent=C-ORG"]
        )
        assert out == "  Nothing modified"
        assert em.exit_code == 1

    def test_add_user_under_mixed_case_account(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "account", "2026_00A", "parent=c-org"])
        em.handle_command(["add", "user", "bob", "account=2026_00A"])
        # The user association resolves regardless of the case queried.
        assert "bob" in em.database.list_account_users("2026_00a")
        assert "bob" in em.database.list_account_users("2026_00A")

    def test_readding_with_different_case_is_not_a_new_account(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "account", "2026_00a", "parent=c-org"])
        out = em.handle_command(["add", "account", "2026_00A", "parent=c-org"])
        # Case-insensitive uniqueness: the second add is a no-op, not a dup.
        assert out == " Data has not changed since time specified"
        assert em.exit_code == 0
        assert sum(1 for a in em.database.list_accounts() if a.name == "2026_00a") == 1
