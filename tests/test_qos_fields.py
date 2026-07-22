"""Tests for the expanded QoS record and MaxWall duration parsing.

Validates emulator parity with real Slurm (/Users/ilja/workspace/slurm):

- slurmdb_qos_rec_t (slurm/slurmdb.h:1076) carries priority, grace_time and
  per-job/node/user TRES limits (max_tres_pj/pn/pu). The emulator now stores
  and round-trips these alongside the original 7 fields.
- MaxWall accepts SLURM duration syntax (minutes, ``[days-]HH:MM:SS``,
  ``MM:SS``). Rendering converts to whole minutes instead of extracting the
  digits (which turned ``7-00:00:00`` into 70000000).
"""

from emulator.api.slurmrestd import schemas
from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


def _emulator(tmp_path):
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    return SacctmgrEmulator(db, TimeEngine())


class TestDurationParsing:
    def test_plain_minutes(self):
        assert schemas._parse_slurm_duration_to_minutes("2880") == 2880

    def test_days_hms(self):
        assert schemas._parse_slurm_duration_to_minutes("7-00:00:00") == 10080

    def test_one_day(self):
        assert schemas._parse_slurm_duration_to_minutes("1-00:00:00") == 1440

    def test_hms(self):
        assert schemas._parse_slurm_duration_to_minutes("00:30:00") == 30

    def test_days_with_hours(self):
        assert schemas._parse_slurm_duration_to_minutes("2-12:00:00") == 3600

    def test_empty_and_unlimited(self):
        assert schemas._parse_slurm_duration_to_minutes("") is None
        assert schemas._parse_slurm_duration_to_minutes("UNLIMITED") is None


class TestQosFieldParsing:
    def test_add_qos_stores_new_fields(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(
            [
                "add",
                "qos",
                "boost",
                "set",
                "Priority=100",
                "GraceTime=300",
                "MaxTRESPerJob=cpu=256",
                "MaxTRESPerNode=cpu=64",
                "MaxTRESPerUser=cpu=512",
            ]
        )
        qos = em.database.qos_list["boost"]
        assert qos.priority == 100
        assert qos.grace_time == 300
        assert qos.max_tres_per_job == "cpu=256"
        assert qos.max_tres_per_node == "cpu=64"
        assert qos.max_tres_per_user == "cpu=512"

    def test_modify_qos_updates_new_fields(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "qos", "boost"])
        em.handle_command(["modify", "qos", "boost", "set", "Priority=50", "MaxTRESPerJob=cpu=8"])
        qos = em.database.qos_list["boost"]
        assert qos.priority == 50
        assert qos.max_tres_per_job == "cpu=8"

    def test_maxtres_alias_maps_to_per_job(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "qos", "boost", "set", "MaxTRES=cpu=16"])
        assert em.database.qos_list["boost"].max_tres_per_job == "cpu=16"


class TestQosRendering:
    def test_max_wall_duration_not_mangled(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "qos", "long", "set", "MaxWall=7-00:00:00"])
        rendered = schemas.qos_to_dict(em.database.qos_list["long"], 1)
        wall = rendered["limits"]["max"]["wall_clock"]["per"]["job"]
        assert wall == {"set": True, "infinite": False, "number": 10080}

    def test_priority_rendered(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "qos", "boost", "set", "Priority=100"])
        rendered = schemas.qos_to_dict(em.database.qos_list["boost"], 1)
        assert rendered["priority"] == {"set": True, "infinite": False, "number": 100}

    def test_grace_time_rendered(self, tmp_path):
        # Real v0.0.46 QOS: grace_time is a plain UINT32 (seconds) under
        # limits/grace_time, not a top-level NO_VAL struct (parsers.c:9321).
        em = _emulator(tmp_path)
        em.handle_command(["add", "qos", "boost", "set", "GraceTime=120"])
        rendered = schemas.qos_to_dict(em.database.qos_list["boost"], 1)
        assert rendered["limits"]["grace_time"] == 120
        assert "grace_time" not in rendered  # not at the top level

    def test_grace_time_defaults_to_zero(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "qos", "boost"])
        rendered = schemas.qos_to_dict(em.database.qos_list["boost"], 1)
        assert rendered["limits"]["grace_time"] == 0

    def test_max_tres_per_job_node_user_rendered(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(
            [
                "add",
                "qos",
                "boost",
                "set",
                "MaxTRESPerJob=cpu=256",
                "MaxTRESPerNode=cpu=64",
                "MaxTRESPerUser=cpu=512",
            ]
        )
        per = schemas.qos_to_dict(em.database.qos_list["boost"], 1)["limits"]["max"]["tres"]["per"]
        assert per["job"] == [{"type": "cpu", "name": "", "id": 1, "count": 256}]
        assert per["node"] == [{"type": "cpu", "name": "", "id": 1, "count": 64}]
        assert per["user"] == [{"type": "cpu", "name": "", "id": 1, "count": 512}]


class TestQosRoundTrip:
    def test_new_fields_survive_save_load(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(
            ["add", "qos", "boost", "set", "Priority=100", "GraceTime=300", "MaxTRESPerJob=cpu=256"]
        )
        em.database.save_state()
        reloaded = SlurmDatabase()
        reloaded.state_file = em.database.state_file
        reloaded.load_state()
        qos = reloaded.qos_list["boost"]
        assert qos.priority == 100
        assert qos.grace_time == 300
        assert qos.max_tres_per_job == "cpu=256"
