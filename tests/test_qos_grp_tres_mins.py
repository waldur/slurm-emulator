"""QoS GrpTRESMins and RawUsage round-trips (waldur/slurm-emulator#1).

Consumer: ``waldur/waldur-site-agent!541`` (``apply_limits_to_qos``) issues
``modify qos <name> set GrpTRESMins=…``, ``show qos <name>
format=Name,GrpTRESMins`` and ``modify qos <name> set RawUsage=0``.

Parity references (real Slurm 26.11 source):
- ``GrpTRESMins`` merge, not replace: qos_functions.c ``_set_rec`` →
  ``sacctmgr_set_tres_rec_field`` (client side, ``TRES_STR_FLAG_REPLACE``),
  then accounting_storage_mysql.c ``mod_tres_str`` (new string first, old
  appended, ``TRES_STR_FLAG_REMOVE``): first occurrence wins, ``-1`` drops
  the TRES. Output sorted by TRES id (``TRES_STR_FLAG_SORT_ID``).
- Unknown TRES name: slurmdb_defs.c ``slurmdb_format_tres_str`` →
  ``error("no TRES known by type %s")``, exit 1, nothing sent.
- Any bad set-field aborts the whole modify before the RPC
  (``sacctmgr_modify_qos``: ``if (exit_code) return SLURM_ERROR``).
- ``RawUsage`` is a QoS option (qos_functions.c ``_set_rec`` "RawUsage",
  ``get_double``; negative → INFINITE → rejected) but takes a separate path:
  ``sacctmgr_modify_qos`` short-circuits into ``sacctmgr_update_qos_usage``
  (common.c), which prints only "No cluster specified, resetting on local
  cluster <c>" and never " Modified qos...". ``sacctmgr_add_qos`` ignores it.
"""

import pytest

from emulator.api.slurmrestd import schemas
from emulator.commands.sacctmgr import (
    SacctmgrEmulator,
    UnknownTresError,
    _combine_tres_string,
)
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine

QOS = "ehpc-dev-2026d07-114"
KNOWN = ["CPU", "Mem", "GRES/gpu", "billing"]


@pytest.fixture
def em(tmp_path):
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    emulator = SacctmgrEmulator(db, TimeEngine())
    emulator.handle_command(["add", "qos", QOS, "set", "flags=DenyOnLimit,NoDecay"])
    return emulator


class TestCombineTresString:
    def test_new_over_empty(self):
        assert _combine_tres_string("", "cpu=100,gres/gpu=200", KNOWN) == "cpu=100,gres/gpu=200"

    def test_named_tres_updated_unnamed_kept(self):
        assert _combine_tres_string("cpu=100,gres/gpu=200", "cpu=1", KNOWN) == "cpu=1,gres/gpu=200"

    def test_minus_one_removes(self):
        assert _combine_tres_string("cpu=100,gres/gpu=200", "gres/gpu=-1", KNOWN) == "cpu=100"

    def test_minus_one_on_absent_is_noop(self):
        assert _combine_tres_string("cpu=100", "mem=-1", KNOWN) == "cpu=100"

    def test_sorted_by_tres_id_and_lowercased(self):
        # billing (id 5) sorts after cpu (1) and mem (2); gres/* is dynamic and
        # sorts last. Names are canonical lower-case like the real id→name print.
        out = _combine_tres_string("", "GRES/gpu=3,billing=5,CPU=1,Mem=2", KNOWN)
        assert out == "cpu=1,mem=2,billing=5,gres/gpu=3"

    def test_static_tres_always_accepted(self):
        assert _combine_tres_string("", "node=4,vmem=1", KNOWN) == "node=4,vmem=1"

    def test_unknown_tres_raises(self):
        with pytest.raises(UnknownTresError):
            _combine_tres_string("", "foo=1", KNOWN)


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

    def test_show_parsable_trailing_pipe(self, em):
        em.handle_command(["modify", "qos", QOS, "set", "GrpTRESMins=cpu=100"])
        out = em.handle_command(["-p", "show", "qos", QOS, "format=Name,GrpTRESMins"])
        assert out.splitlines()[1] == f"{QOS}|cpu=100|"

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

    def test_unknown_tres_exits_one_and_modifies_nothing(self, em):
        em.handle_command(["modify", "qos", QOS, "set", "GrpTRESMins=cpu=100"])
        out = em.handle_command(["modify", "qos", QOS, "set", "GrpTRESMins=foo=1"])
        assert em.exit_code == 1
        assert out == "sacctmgr: error: slurmdb_format_tres_str: no TRES known by type foo"
        assert em.database.qos_list[QOS].grp_tres_mins == "cpu=100"

    def test_bad_field_aborts_whole_modify(self, em):
        # sacctmgr_modify_qos checks exit_code after parsing every set-field
        # and returns before the RPC, so MaxJobs must not have been applied.
        out = em.handle_command(["modify", "qos", QOS, "set", "MaxJobs=7", "GrpTRESMins=nope=1"])
        assert em.exit_code == 1
        assert "no TRES known by type nope" in out
        assert em.database.qos_list[QOS].max_jobs == -1

    def test_add_qos_accepts_grp_tres_mins(self, em):
        em.handle_command(["add", "qos", "other", "set", "GrpTRESMins=cpu=5"])
        assert em.exit_code == 0
        assert em.database.qos_list["other"].grp_tres_mins == "cpu=5"

    def test_no_set_clause_is_real_message(self, em):
        out = em.handle_command(["modify", "qos", QOS])
        assert em.exit_code == 1
        assert out == " You didn't give me anything to set"

    def test_unknown_qos_nothing_modified(self, em):
        out = em.handle_command(["modify", "qos", "missing", "set", "GrpTRESMins=cpu=1"])
        assert out == "  Nothing modified"
        assert em.exit_code == 1
        assert "missing" not in em.database.qos_list

    def test_persists_across_reload(self, em, tmp_path):
        em.handle_command(["modify", "qos", QOS, "set", "GrpTRESMins=cpu=7"])
        em.handle_command(["modify", "qos", QOS, "set", "RawUsage=42"])
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


class TestSlurmrestdQosPost:
    def test_post_qos_minutes_total_round_trips(self, restd, auth_headers):
        body = {
            "qos": [
                {
                    "name": QOS,
                    "limits": {
                        "max": {
                            "tres": {
                                "minutes": {
                                    "total": [
                                        {"type": "billing", "name": "", "count": 100},
                                        {"type": "gres", "name": "gpu", "count": 200},
                                    ]
                                },
                                # association-style path: not a QOS field, ignored
                                "group": {"minutes": [{"type": "cpu", "name": "", "count": 999}]},
                            }
                        }
                    },
                }
            ]
        }
        r = restd.post("/slurmdb/v0.0.46/qos/", json=body, headers=auth_headers)
        assert r.status_code == 200, r.text
        r = restd.get(f"/slurmdb/v0.0.46/qos/{QOS}", headers=auth_headers)
        minutes = r.json()["qos"][0]["limits"]["max"]["tres"]["minutes"]["total"]
        assert {(t["type"], t.get("name", ""), t["count"]) for t in minutes} == {
            ("billing", "", 100),
            ("gres", "gpu", 200),
        }


class TestRawUsage:
    def test_reset_to_zero_takes_usage_path(self, em):
        em.database.qos_list[QOS].usage_raw = 1234.5
        out = em.handle_command(["-i", "modify", "qos", QOS, "set", "RawUsage=0"])
        assert em.exit_code == 0
        # sacctmgr_update_qos_usage, not the " Modified qos..." branch.
        assert out == "No cluster specified, resetting on local cluster default"
        assert em.database.qos_list[QOS].usage_raw == 0.0

    def test_other_fields_in_same_command_are_ignored(self, em):
        # ``if (qos->usage)`` returns after the usage update; the qos_modify
        # RPC carrying MaxJobs is never sent.
        em.handle_command(["modify", "qos", QOS, "set", "RawUsage=0", "MaxJobs=9"])
        assert em.exit_code == 0
        assert em.database.qos_list[QOS].max_jobs == -1

    def test_bad_value_exits_one(self, em):
        out = em.handle_command(["modify", "qos", QOS, "set", "RawUsage=abc"])
        assert em.exit_code == 1
        assert out == " Bad RawUsage value: abc"

    def test_negative_is_bad_value(self, em):
        # get_double turns a negative into INFINITE, which _set_rec rejects.
        out = em.handle_command(["modify", "qos", QOS, "set", "RawUsage=-1"])
        assert em.exit_code == 1
        assert out == " Bad RawUsage value: -1"

    def test_missing_qos_is_failed_to_find(self, em):
        out = em.handle_command(["modify", "qos", "ghost", "set", "RawUsage=0"])
        assert em.exit_code == 1
        assert out == "sacctmgr: error: Failed to find QOS ghost"

    def test_add_qos_parses_but_ignores_raw_usage(self, em):
        em.handle_command(["add", "qos", "fresh", "set", "RawUsage=5"])
        assert em.exit_code == 0
        assert em.database.qos_list["fresh"].usage_raw == 0.0
        em.handle_command(["add", "qos", "broken", "set", "RawUsage=x"])
        assert em.exit_code == 1
        assert "broken" not in em.database.qos_list

    def test_does_not_touch_grp_tres_mins(self, em):
        em.handle_command(["modify", "qos", QOS, "set", "GrpTRESMins=cpu=100"])
        em.handle_command(["modify", "qos", QOS, "set", "RawUsage=0"])
        assert em.database.qos_list[QOS].grp_tres_mins == "cpu=100"
