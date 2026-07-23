"""QoS commands must persist to the state file like every other mutation.

``add qos`` / ``modify qos`` previously updated the in-memory ``qos_list`` but
never called ``save_state()``, so a QoS created by one ``sacctmgr`` process was
invisible to the next (e.g. a ``show qos`` reader). This regressed callers that
create a QoS and then read it back in a fresh process.
"""

from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


def _emulator(tmp_path):
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    return SacctmgrEmulator(db, TimeEngine())


def _reload(state_file):
    db = SlurmDatabase()
    db.state_file = state_file
    db.load_state()
    return db


class TestQoSPersistence:
    def test_add_qos_persists(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "qos", "gpu", "set", "GrpTRES=cpu=64", "MaxWall=00:30:00"])
        reloaded = _reload(em.database.state_file)
        assert "gpu" in reloaded.qos_list
        assert reloaded.qos_list["gpu"].grp_tres == "cpu=64"

    def test_modify_qos_persists(self, tmp_path):
        em = _emulator(tmp_path)
        em.handle_command(["add", "qos", "gpu"])
        em.handle_command(["modify", "qos", "gpu", "set", "MaxWall=1-00:00:00"])
        reloaded = _reload(em.database.state_file)
        assert reloaded.qos_list["gpu"].max_wall == "1-00:00:00"
