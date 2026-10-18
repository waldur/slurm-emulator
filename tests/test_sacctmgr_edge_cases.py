"""sacctmgr edge cases found reviewing the site-agent parity work.

- A modification that fails slurmdbd's DefaultQOS check changes nothing,
  parent moves included: slurmdbd rolls the transaction back
  (slurm://src/sacctmgr/account_functions.c#sacctmgr_modify_account).
- Adding an account on several clusters reports only rows that existed before
  the command (slurm://src/sacctmgr/account_functions.c#sacctmgr_add_account).
- ``RawUsage`` can only be reset to 0
  (slurm://src/sacctmgr/account_functions.c#sacctmgr_modify_account).
- Without ``DefaultAccount=`` a new user's default is the first association
  actually added
  (slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_make_sure_user_has_default_internal).
- ``-V`` prints ``slurm <release>``
  (slurm://src/sacct/options.c#print_slurm_version,
  slurm://src/sreport/sreport.c#print_slurm_version).
"""

import pytest

from emulator.commands.sacct import SacctEmulator
from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.commands.sreport import SreportEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine
from emulator.slurm_version import current


@pytest.fixture
def db(tmp_path):
    database = SlurmDatabase()
    database.state_file = tmp_path / "state.json"
    return database


@pytest.fixture
def em(db):
    return SacctmgrEmulator(db, TimeEngine())


def _run(em, *args):
    out = em.handle_command(list(args))
    return out, em.exit_code


def test_failed_default_qos_check_undoes_the_parent_move(em, db):
    for name in ("a", "b"):
        _run(em, "add", "account", name)
    _run(em, "add", "qos", "q1")

    _, code = _run(em, "modify", "account", "a", "set", "parent=b", "DefaultQOS=q1")

    assert code == 1
    assert db.get_account("a").parent in (None, "root")
    row = db.associations[db._association_key("", "a", db.current_cluster)]
    assert row.parent in (None, "root")


def test_new_account_on_two_clusters_is_not_reported_as_existing(em):
    _run(em, "add", "cluster", "alpha")

    out, code = _run(em, "add", "account", "newacc", f"cluster=alpha,{em.database.current_cluster}")

    assert code == 0
    assert "Already existing" not in out


def test_raw_usage_other_than_zero_is_refused(em):
    _run(em, "add", "account", "a")

    out, code = _run(em, "modify", "account", "a", "set", "RawUsage=5")

    assert (out, code) == ("sacctmgr: error: Raw usage can only be set to 0 (zero)", 1)
    assert _run(em, "modify", "account", "a", "set", "RawUsage=0") == ("", 0)


def test_default_account_is_the_first_association_added(em, db):
    _run(em, "add", "account", "a1")

    _, code = _run(em, "add", "user", "u", "account=missing,a1")

    assert code == 0
    assert db.get_user("u").default_account == "a1"


def test_sacct_and_sreport_print_the_slurm_release(db):
    expected = f"slurm {current().release}"
    assert SacctEmulator(db, TimeEngine()).handle_command(["-V"]) == expected
    assert SreportEmulator(db, TimeEngine()).handle_command(["-V"]) == expected
