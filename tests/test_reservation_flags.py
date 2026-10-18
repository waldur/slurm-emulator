"""Reservation flag rules and generated names.

* slurm://src/common/proc_args.c#parse_resv_flags: ``+Replace`` is allowed,
  ``-Replace`` is not; Force_Start exists from 25.05.
* slurm://src/slurmctld/reservation.c#update_resv: REPLACE only on NodeCnt
  reservations without MAINT/STATIC; TIME_FLOAT cannot be set later.
* slurm://src/slurmctld/reservation.c#_generate_resv_name: the suffix is the
  controller's reservation counter.
"""

from __future__ import annotations

import pytest

from emulator.commands.scontrol import _ParseError, parse_resv_flags
from emulator.core import reservations
from emulator.core.database import SlurmDatabase
from emulator.core.reservations import ReservationDesc, ReservationError
from emulator.slurm_version import at_least
from tests.test_cluster_model import T0
from tests.test_cluster_model import sim as sim  # noqa: F401 - fixture


def test_plus_replace_parses():
    assert parse_resv_flags("+Replace", "x") == ["REPLACE"]
    with pytest.raises(_ParseError):
        parse_resv_flags("-Replace", "x")


def test_force_start_from_25_05():
    if at_least("25.05"):
        assert parse_resv_flags("Force_Start", "x") == ["FORCE_START"]
    else:
        with pytest.raises(_ParseError):
            parse_resv_flags("Force_Start", "x")


def _create(db, **extra):
    desc = ReservationDesc(start_time=T0, duration=60, users="root", **extra)
    return reservations.create_reservation(db, desc, T0)


def test_replace_refused_on_node_list_reservation(state_env):
    db = SlurmDatabase()
    _create(db, name="r", node_list="node001")
    with pytest.raises(ReservationError) as info:
        reservations.update_reservation(db, ReservationDesc(name="r", flags=["REPLACE"]), T0)
    assert info.value.errno == reservations.ESLURM_NOT_SUPPORTED


def test_replace_refused_with_maint(state_env):
    db = SlurmDatabase()
    _create(db, name="r", node_cnt=1, flags=["MAINT"])
    with pytest.raises(ReservationError) as info:
        reservations.update_reservation(db, ReservationDesc(name="r", flags=["REPLACE"]), T0)
    assert info.value.errno == reservations.ESLURM_NOT_SUPPORTED


def test_time_float_cannot_be_set_later(state_env):
    db = SlurmDatabase()
    _create(db, name="r", node_list="node001")
    with pytest.raises(ReservationError) as info:
        reservations.update_reservation(db, ReservationDesc(name="r", flags=["TIME_FLOAT"]), T0)
    assert info.value.errno == reservations.ESLURM_INVALID_TIME_VALUE


def test_generated_names_follow_the_global_counter(state_env):
    db = SlurmDatabase()
    _create(db, name="named", node_list="node001")
    generated = _create(db, node_list="node002")
    # The named reservation drew id 1, so the next generated name is root_2.
    assert generated.name == "root_2" and generated.resv_id == 2


def test_overlapping_reservations_flag_nodes(state_env):
    from emulator.core import cluster_model  # noqa: PLC0415
    from emulator.core.database import Reservation  # noqa: PLC0415

    db = SlurmDatabase()
    end = T0.replace(hour=12)
    db.reservations["maint"] = Reservation(
        name="maint", nodes=["node001"], start_time=T0, end_time=end, flags=["MAINT"]
    )
    db.reservations["other"] = Reservation(
        name="other", nodes=["node001"], start_time=T0, end_time=end, flags=["OVERLAP"]
    )
    view = cluster_model.node_views(db, T0.replace(hour=10))["node001"]
    assert view.state == ["IDLE", "RESERVED", "MAINTENANCE"]
    assert view.reservation == "other"


def test_nodes_plus_minus_on_update(sim, capsys):
    from emulator.commands.scontrol import ScontrolEmulator  # noqa: PLC0415
    from emulator.core.time_engine import TimeEngine  # noqa: PLC0415

    db = SlurmDatabase()
    _create(db, name="r", node_list="node[001-002]")
    db.save_state()
    scontrol = ScontrolEmulator(db, TimeEngine())
    scontrol.handle_command(["update", "ReservationName=r", "Nodes+=node[003-004],node006"])
    assert capsys.readouterr().err == "" and scontrol.exit_code == 0
    assert scontrol.database.reservations["r"].nodes == [
        "node001", "node002", "node003", "node004", "node006",
    ]
    scontrol.handle_command(["update", "ReservationName=r", "Nodes-=node001"])
    assert scontrol.database.reservations["r"].nodes[0] == "node002"
    scontrol.handle_command(["update", "ReservationName=r", "Nodes+=bogus01"])
    assert scontrol.exit_code == 1
    assert "Invalid node name specified" in capsys.readouterr().err


def test_impossible_request_is_refused_at_submit(restd, auth_headers):
    from tests.test_cluster_model import V  # noqa: PLC0415

    for job in (
        {"partition": "debug", "nodes": "5"},  # debug has 4 nodes
        {"partition": "debug", "memory_per_node": 10**7},
        {"partition": "debug", "cpus_per_task": 65},
        {"partition": "debug", "nodes": "1", "tasks": 65},
    ):
        body = restd.post(
            f"/slurm/{V}/job/submit", headers=auth_headers, json={"job": {"name": "x", **job}}
        ).json()
        assert body["errors"][0]["error_number"] == 2014, job
        assert body["errors"][0]["description"] == "Requested node configuration is not available"
