"""``sacctmgr modify user … set DefaultAccount=`` and ``where`` filters on show.

Real behaviour: user-record changes print `` Modified users...`` and every
user the condition matches, changed or not
(slurm://src/sacctmgr/user_functions.c#sacctmgr_modify_user,
slurm://src/plugins/accounting_storage/mysql/as_mysql_user.c#as_mysql_modify_users);
no match prints ``  Nothing modified`` and exits 1
(slurm://src/sacctmgr/sacctmgr.c#_modify_it). A default account the user is
not associated with on every cluster of the condition — all clusters when it
names none (slurm://src/sacctmgr/user_functions.c#_check_and_set_cluster_list)
— is refused by slurm://src/sacctmgr/user_functions.c#_check_default_assocs
with exit 1. The ``where`` conditions of ``show user`` come from
slurm://src/sacctmgr/user_functions.c#_set_cond, those of ``show association``
from slurm://src/sacctmgr/association_functions.c#_set_cond and
slurm://src/sacctmgr/association_functions.c#sacctmgr_set_assoc_cond.
"""

import pytest

from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


def _refusal(account, *pairs):
    return (
        " Modified users...\n"
        f" Can't modify because these users aren't associated with new default account '{account}'...\n"
        + "\n".join(f"  U = {u} C = {c}" for u, c in pairs)
    )


def _populate(em):
    for acct in ("acca", "accb", "accc"):
        em.handle_command(["add", "account", acct, "description=x", "organization=o"])
    em.handle_command(["add", "user", "twoacc", "account=acca", "DefaultAccount=acca"])
    em.handle_command(["add", "user", "twoacc", "account=accb"])
    em.handle_command(["add", "user", "other", "account=accc", "DefaultAccount=accc"])
    return em


@pytest.fixture
def em(tmp_path):
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    return _populate(SacctmgrEmulator(db, TimeEngine()))


def _users(em, *where):
    return em.handle_command(["-P", "-n", "show", "user", *where, "format=user"]).splitlines()


class TestModifyDefaultAccount:
    @pytest.mark.parametrize(
        "args",
        [
            ["modify", "user", "where", "name=twoacc", "set", "DefaultAccount=accb"],
            ["modify", "user", "twoacc", "set", "DefaultAccount=accb"],
            ["modify", "user", "twoacc", "set", "defaultaccount=accb"],
            ["modify", "user", "set", "DefaultAcc=accb", "where", "name=twoacc"],
            ["modify", "user", "where", "n=twoacc", "set", "DefaultAccount=ACCB"],
            ["modify", "user", "where", "users=twoacc", "set", "DefaultAccount='accb'"],
        ],
    )
    def test_sets_default_account(self, em, args):
        out = em.handle_command(args)
        assert em.exit_code == 0
        assert out == " Modified users...\n  twoacc"
        assert em.database.get_user("twoacc").default_account == "accb"
        assert (
            em.handle_command(
                ["-P", "-n", "show", "user", "where", "name=twoacc", "format=user,defaultaccount"]
            )
            == "twoacc|accb"
        )

    def test_persists_to_state_file(self, em):
        em.handle_command(["modify", "user", "twoacc", "set", "DefaultAccount=accb"])
        fresh = SlurmDatabase()
        fresh.state_file = em.database.state_file
        fresh.load_state()
        assert fresh.get_user("twoacc").default_account == "accb"

    def test_refuses_account_without_association(self, em):
        out = em.handle_command(["modify", "user", "twoacc", "set", "DefaultAccount=accc"])
        assert em.exit_code == 1
        assert em.stdout_error
        assert out == _refusal("accc", ("twoacc", "default"))
        assert em.database.get_user("twoacc").default_account == "acca"

    def test_refusal_prints_the_lowercased_account(self, em):
        """_set_rec reads the value with strip_quotes(…, make_lower=1)."""
        out = em.handle_command(["modify", "user", "twoacc", "set", "DefaultAccount=ACCC"])
        assert out == _refusal("accc", ("twoacc", "default"))

    def test_current_default_is_listed_as_modified(self, em):
        """Every matching user comes back from slurmdbd, changed or not."""
        out = em.handle_command(["modify", "user", "twoacc", "set", "DefaultAccount=acca"])
        assert (out, em.exit_code) == (" Modified users...\n  twoacc", 0)

    def test_unknown_user_is_nothing_modified(self, em):
        out = em.handle_command(["modify", "user", "ghost", "set", "DefaultAccount=acca"])
        assert (out, em.exit_code) == (" Modified users...\n  Nothing modified", 1)
        assert em.stdout_error

    def test_unblocks_removal_of_the_old_default(self, em):
        em.handle_command(["remove", "user", "where", "name=twoacc", "account=acca"])
        assert em.exit_code == 1  # acca is still the default
        em.handle_command(["modify", "user", "where", "name=twoacc", "set", "DefaultAccount=accb"])
        assert em.exit_code == 0
        em.handle_command(["remove", "user", "where", "name=twoacc", "account=acca"])
        assert em.exit_code == 0
        assert em.database.get_association("twoacc", "acca") is None
        assert em.database.get_user("twoacc").default_account == "accb"

    def test_combined_with_association_settings(self, em):
        em.handle_command(["add", "qos", "high"])
        out = em.handle_command(
            ["modify", "user", "twoacc", "set", "DefaultAccount=accb", "QosLevel+=high"]
        )
        assert em.exit_code == 0
        assert out.startswith(
            " Modified users...\n  twoacc\n Modified user associations...\n  twoacc"
        )
        assert em.database.get_user("twoacc").default_account == "accb"
        assert "high" in em.database.get_association("twoacc", "accb").qos_list

    def test_several_users(self, em):
        em.handle_command(["add", "user", "other", "account=accb"])
        out = em.handle_command(
            ["modify", "user", "where", "name=twoacc,other", "set", "DefaultAccount=accb"]
        )
        assert (out, em.exit_code) == (" Modified users...\n  other\n  twoacc", 0)
        assert _users(em, "defaultaccount=accb") == ["twoacc", "other"]

    def test_selects_users_by_current_default(self, em):
        out = em.handle_command(
            ["modify", "user", "where", "defaultaccount=acca", "set", "DefaultAccount=accb"]
        )
        assert (out, em.exit_code) == (" Modified users...\n  twoacc", 0)


class TestDefaultAccountClusters:
    """No ``cluster=`` checks every cluster; ``cluster=`` narrows the check."""

    @pytest.fixture
    def two_clusters(self, em):
        em.handle_command(["add", "cluster", "second"])
        return em

    def test_refuses_when_missing_on_another_cluster(self, two_clusters):
        em = two_clusters
        out = em.handle_command(["modify", "user", "twoacc", "set", "DefaultAccount=accb"])
        assert (out, em.exit_code) == (_refusal("accb", ("twoacc", "second")), 1)
        assert em.database.get_user("twoacc").default_account == "acca"

    def test_lists_every_missing_user_and_cluster(self, two_clusters):
        em = two_clusters
        out = em.handle_command(
            ["modify", "user", "where", "name=twoacc,other", "set", "DefaultAccount=accb"]
        )
        assert out == _refusal(
            "accb", ("other", "default"), ("other", "second"), ("twoacc", "second")
        )

    def test_association_on_every_cluster_allows_it(self, two_clusters):
        em = two_clusters
        em.handle_command(["add", "user", "twoacc", "account=accb", "cluster=second"])
        out = em.handle_command(["modify", "user", "twoacc", "set", "DefaultAccount=accb"])
        assert (out, em.exit_code) == (" Modified users...\n  twoacc", 0)

    @pytest.mark.parametrize("key", ["cluster", "clusters", "c"])
    def test_cluster_condition_narrows_the_check(self, two_clusters, key):
        em = two_clusters
        out = em.handle_command(
            [
                "modify",
                "user",
                "where",
                "name=twoacc",
                f"{key}=default",
                "set",
                "DefaultAccount=accb",
            ]
        )
        assert (out, em.exit_code) == (" Modified users...\n  twoacc", 0)
        out = em.handle_command(
            [
                "modify",
                "user",
                "where",
                "name=twoacc",
                f"{key}=second",
                "set",
                "DefaultAccount=acca",
            ]
        )
        assert (out, em.exit_code) == (_refusal("acca", ("twoacc", "second")), 1)

    def test_configured_cluster_name_skips_the_placeholder(self, tmp_path, monkeypatch):
        """With a ClusterName the seeded ``default`` cluster is not a real one."""
        monkeypatch.setenv("SLURM_EMULATOR_CLUSTER_NAME", "linux")
        db = SlurmDatabase()
        db.state_file = tmp_path / "state.json"
        em = _populate(SacctmgrEmulator(db, TimeEngine()))
        assert em.database.get_association("twoacc", "accb", cluster="linux") is not None
        out = em.handle_command(
            ["modify", "user", "where", "name=twoacc", "set", "DefaultAccount=accb"]
        )
        assert (out, em.exit_code) == (" Modified users...\n  twoacc", 0)


class TestShowUserFilters:
    @pytest.mark.parametrize(
        "where",
        [
            ["where", "name=twoacc"],
            ["name=twoacc"],
            ["twoacc"],
            ["where", "twoacc"],
            ["n=twoacc"],
            ["na=twoacc"],
            ["Names=twoacc"],
            ["u=twoacc"],
            ["use=twoacc"],
            ["users=twoacc"],
        ],
    )
    def test_user_name_forms(self, em, where):
        assert _users(em, *where) == ["twoacc"]

    def test_name_lists(self, em):
        assert sorted(_users(em, "name=other,twoacc")) == ["other", "twoacc"]
        assert sorted(_users(em, "twoacc,other")) == ["other", "twoacc"]

    @pytest.mark.parametrize("key", ["defaulta", "defaultaccount", "DefaultAcc"])
    def test_default_account(self, em, key):
        assert _users(em, "where", f"{key}=accc") == ["other"]

    @pytest.mark.parametrize("key", ["defaultaccounts", "default", "namesx"])
    def test_unknown_condition(self, em, key):
        out = em.handle_command(["show", "user", "where", f"{key}=accc"])
        assert em.exit_code == 1
        assert out == f" Unknown condition: {key}=accc\n Use keyword 'set' to modify value"

    def test_flags_are_not_user_names(self, em):
        assert {"root", "twoacc", "other"} <= set(_users(em, "withdeleted"))

    def test_unfiltered_lists_all(self, em):
        assert {"root", "twoacc", "other"} <= set(_users(em))


class TestShowAssociationFilters:
    @pytest.mark.parametrize("key", ["user", "users", "u", "Use"])
    def test_user_keys(self, em, key):
        out = em.handle_command(
            ["-P", "-n", "show", "association", "where", f"{key}=twoacc", "format=account"]
        )
        assert sorted(out.splitlines()) == ["acca", "accb"]

    @pytest.mark.parametrize("key", ["account", "accounts", "ac", "acct", "Acc"])
    def test_account_keys(self, em, key):
        out = em.handle_command(["-P", "-n", "show", "assoc", f"{key}=ACCB", "format=account,user"])
        assert sorted(out.splitlines()) == ["accb|", "accb|twoacc"]

    @pytest.mark.parametrize("key", ["a", "acc_", "pa", "partitionsx"])
    def test_unknown_condition(self, em, key):
        out = em.handle_command(["show", "assoc", f"{key}=x"])
        assert (out, em.exit_code) == (f" Unknown condition: {key}=x", 1)

    def test_cluster_and_partition(self, em):
        em.handle_command(["add", "cluster", "second"])
        em.handle_command(["add", "user", "other", "account=accb", "cluster=second"])
        em.handle_command(["add", "user", "part", "account=accb", "Partitions=zen3,zen5"])
        out = em.handle_command(
            ["-P", "-n", "show", "assoc", "c=second", "u=other", "format=cluster,user"]
        )
        assert out == "second|other"
        out = em.handle_command(
            ["-P", "-n", "list", "assoc", "where", "par=zen5", "format=user,partition"]
        )
        assert out == "part|zen5"

    def test_list_and_show_agree(self, em):
        args = ["-P", "-n", "where", "user=twoacc", "account=accb", "format=user,account"]
        show = em.handle_command([*args[:2], "show", "association", *args[2:]])
        listed = em.handle_command([*args[:2], "list", "associations", *args[2:]])
        assert show == listed == "twoacc|accb"


def test_site_agent_command_shapes(em):
    """The exact invocations the site agent issues."""
    em.handle_command(["modify", "user", "where", "name=twoacc", "set", "DefaultAccount=accb"])
    assert em.exit_code == 0
    out = em.handle_command(
        ["-P", "-n", "show", "association", "where", "user=twoacc", "format=user,account"]
    )
    assert sorted(out.splitlines()) == ["twoacc|acca", "twoacc|accb"]
    out = em.handle_command(
        ["-P", "-n", "show", "user", "where", "name=twoacc", "format=user,defaultaccount"]
    )
    assert out == "twoacc|accb"
    # ``accounts=`` (plural) is how the agent reads limits back.
    out = em.handle_command(
        ["-P", "-n", "show", "association", "format=account,user", "where", "accounts=accb"]
    )
    assert sorted(out.splitlines()) == ["accb|", "accb|twoacc"]
