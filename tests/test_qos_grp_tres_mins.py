"""QoS GrpTRESMins and RawUsage round-trips (waldur/slurm-emulator#1).

Consumer: ``waldur/waldur-site-agent!541`` (``apply_limits_to_qos``) issues
``modify qos <name> set GrpTRESMins=…``, ``show qos <name>
format=Name,GrpTRESMins`` and ``modify qos <name> set RawUsage=0``.

Parity references (real Slurm source):
- ``GrpTRESMins`` merge, not replace: qos_functions.c ``_set_rec`` +
  as_mysql_qos.c ``_setup_qos_limits`` (``TRES_STR_FLAG_REMOVE``), where a
  ``-1`` count drops the TRES (slurmdb_defs.c ``slurmdb_tres_list_from_string``).
- ``RawUsage`` is a QoS option: qos_functions.c ``_set_rec`` "RawUsage"
  (``get_double``; bad value → exit 1, " Bad RawUsage value: …").
"""

import pytest

from emulator.api.slurmrestd import schemas
from emulator.commands.sacctmgr import SacctmgrEmulator, _combine_tres_string
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


@pytest.fixture
def em(tmp_path):
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    emulator = SacctmgrEmulator(db, TimeEngine())
    emulator.handle_command(
        ["add", "qos", "ehpc-dev-2026d07-114", "set", "flags=DenyOnLimit,NoDecay"]
    )
    return emulator


QOS = "ehpc-dev-2026d07-114"


class TestCombineTresString:
    def test_new_over_empty(self):
        assert _combine_tres_string("", "cpu=100,gres/gpu=200") == "cpu=100,gres/gpu=200"

    def test_named_tres_updated_unnamed_kept(self):
        assert _combine_tres_string("cpu=100,gres/gpu=200", "cpu=1") == "cpu=1,gres/gpu=200"

    def test_minus_one_removes(self):
        assert _combine_tres_string("cpu=100,gres/gpu=200", "gres/gpu=-1") == "cpu=100"

    def test_minus_one_on_absent_is_noop(self):
        assert _combine_tres_string("cpu=100", "mem=-1") == "cpu=100"


class TestGrpTRESMinsRoundTrip:
    def test_modify_persists_and_show_round_trips(self, em):
        out = em.handle_command(
            ["modify", "qos", QOS, "set", "GrpTRESMins=billing=14428800,gres/gpu=216000"]
        )
        assert em.exit_code == 0
        assert out == f" Modified qos...\n  {QOS}"
        assert em.database.qos_list[QOS].grp_tres_mins == "billing=14428800,gres/gpu=216000"

        out = em.handle_command(["-P", "show", "qos", QOS, "format=Name,GrpTRESMins"])
        assert out.splitlines() == [
            "Name|GrpTRESMins",
            f"{QOS}|billing=14428800,gres/gpu=216000",
        ]

    def test_show_table_mode(self, em):
        em.handle_command(["modify", "qos", QOS, "set", "GrpTRESMins=cpu=100"])
        out = em.handle_command(["show", "qos", QOS, "format=Name%22,GrpTRESMins"])
        lines = out.splitlines()
        assert lines[0].split() == ["Name", "GrpTRESMins"]
        assert lines[2].split() == [QOS, "cpu=100"]

    def test_partial_update_merges(self, em):
        em.handle_command(["modify", "qos", QOS, "set", "GrpTRESMins=billing=100,gres/gpu=200"])
        em.handle_command(["modify", "qos", QOS, "set", "GrpTRESMins=billing=300"])
        assert em.database.qos_list[QOS].grp_tres_mins == "billing=300,gres/gpu=200"

    def test_minus_one_removes_tres(self, em):
        em.handle_command(["modify", "qos", QOS, "set", "GrpTRESMins=billing=100,gres/gpu=200"])
        em.handle_command(["modify", "qos", QOS, "set", "GrpTRESMins=gres/gpu=-1"])
        assert em.database.qos_list[QOS].grp_tres_mins == "billing=100"

    def test_add_qos_accepts_grp_tres_mins(self, em):
        em.handle_command(["add", "qos", "other", "set", "GrpTRESMins=cpu=5"])
        assert em.exit_code == 0
        assert em.database.qos_list["other"].grp_tres_mins == "cpu=5"

    def test_unknown_qos_nothing_modified(self, em):
        out = em.handle_command(["modify", "qos", "missing", "set", "GrpTRESMins=cpu=1"])
        assert out == "  Nothing modified"
        assert em.exit_code == 1
        assert "missing" not in em.database.qos_list

    def test_persists_across_reload(self, em, tmp_path):
        em.handle_command(["modify", "qos", QOS, "set", "GrpTRESMins=cpu=7", "RawUsage=42"])
        db = SlurmDatabase()
        db.state_file = tmp_path / "state.json"
        db.load_state()
        assert db.qos_list[QOS].grp_tres_mins == "cpu=7"
        assert db.qos_list[QOS].usage_raw == 42.0

    def test_slurmrestd_qos_exposes_minutes_total(self, em):
        em.handle_command(["modify", "qos", QOS, "set", "GrpTRESMins=cpu=100,gres/gpu=200"])
        rendered = schemas.qos_to_dict(em.database.qos_list[QOS], 1)
        minutes = rendered["limits"]["max"]["tres"]["minutes"]["total"]
        assert {(t["type"], t.get("name", ""), t["count"]) for t in minutes} == {
            ("cpu", "", 100),
            ("gres", "gpu", 200),
        }


class TestRawUsage:
    def test_reset_to_zero(self, em):
        em.database.qos_list[QOS].usage_raw = 1234.5
        out = em.handle_command(["modify", "qos", QOS, "set", "RawUsage=0"])
        assert em.exit_code == 0
        assert out == f" Modified qos...\n  {QOS}"
        assert em.database.qos_list[QOS].usage_raw == 0.0

    def test_bad_value_exits_one(self, em):
        out = em.handle_command(["modify", "qos", QOS, "set", "RawUsage=abc"])
        assert em.exit_code == 1
        assert out == " Bad RawUsage value: abc"

    def test_does_not_touch_grp_tres_mins(self, em):
        em.handle_command(["modify", "qos", QOS, "set", "GrpTRESMins=cpu=100"])
        em.handle_command(["modify", "qos", QOS, "set", "RawUsage=0"])
        assert em.database.qos_list[QOS].grp_tres_mins == "cpu=100"
