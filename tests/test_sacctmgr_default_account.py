"""``sacctmgr modify user … set DefaultAccount=`` and ``where`` filters on show.

Real behaviour: user-record changes print `` Modified users...`` and the user
(slurm://src/sacctmgr/user_functions.c#sacctmgr_modify_user); a default account
the user is not associated with is refused by
slurm://src/sacctmgr/user_functions.c#_check_default_assocs with exit 1; the
``where`` conditions of ``show user`` / ``show association`` come from
slurm://src/sacctmgr/user_functions.c#_set_cond and
slurm://src/sacctmgr/association_functions.c#_set_cond.
"""

import pytest

from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


@pytest.fixture
def em(tmp_path):
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    em = SacctmgrEmulator(db, TimeEngine())
    for acct in ("acca", "accb", "accc"):
        em.handle_command(["add", "account", acct, "description=x", "organization=o"])
    em.handle_command(["add", "user", "twoacc", "account=acca", "DefaultAccount=acca"])
    em.handle_command(["add", "user", "twoacc", "account=accb"])
    em.handle_command(["add", "user", "other", "account=accc"])
    return em


class TestModifyDefaultAccount:
    @pytest.mark.parametrize(
        "args",
        [
            ["modify", "user", "where", "name=twoacc", "set", "DefaultAccount=accb"],
            ["modify", "user", "twoacc", "set", "DefaultAccount=accb"],
            ["modify", "user", "twoacc", "set", "defaultaccount=accb"],
            ["modify", "user", "set", "DefaultAcc=accb", "where", "name=twoacc"],
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
        assert out == (
            " Modified users...\n"
            " Can't modify because these users aren't associated with new default account 'accc'...\n"
            "  U = twoacc C = default"
        )
        assert em.database.get_user("twoacc").default_account == "acca"

    def test_same_default_is_nothing_modified(self, em):
        out = em.handle_command(["modify", "user", "twoacc", "set", "DefaultAccount=acca"])
        assert (out, em.exit_code) == ("  Nothing modified", 1)

    def test_unknown_user_is_nothing_modified(self, em):
        out = em.handle_command(["modify", "user", "ghost", "set", "DefaultAccount=acca"])
        assert (out, em.exit_code) == ("  Nothing modified", 1)

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


class TestShowFilters:
    def test_show_user_where_name(self, em):
        out = em.handle_command(["-P", "-n", "show", "user", "where", "name=twoacc", "format=user"])
        assert out == "twoacc"
        out = em.handle_command(["-P", "-n", "show", "user", "name=other,twoacc", "format=user"])
        assert sorted(out.splitlines()) == ["other", "twoacc"]
        out = em.handle_command(
            ["-P", "-n", "show", "user", "where", "defaultaccount=accc", "format=user"]
        )
        assert out == "other"

    def test_show_user_unfiltered_lists_all(self, em):
        out = em.handle_command(["-P", "-n", "show", "user", "format=user"])
        assert {"root", "twoacc", "other"} <= set(out.splitlines())

    def test_show_association_where_user(self, em):
        out = em.handle_command(
            ["-P", "-n", "show", "association", "where", "user=twoacc", "format=account"]
        )
        assert sorted(out.splitlines()) == ["acca", "accb"]
        out = em.handle_command(["-P", "-n", "show", "assoc", "user=other", "format=account,user"])
        assert out == "accc|other"

    def test_site_agent_command_shapes(self, em):
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

    def test_show_association_where_account(self, em):
        out = em.handle_command(
            ["-P", "-n", "show", "association", "where", "account=accb", "format=account,user"]
        )
        assert sorted(out.splitlines()) == ["accb|", "accb|twoacc"]
