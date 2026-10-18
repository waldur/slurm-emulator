"""Reservation writes over slurmrestd, scontrol (CLI dispatcher + SSH) and the state file.

* ``POST /reservation`` / ``POST /reservations/`` / ``DELETE /reservation/{name}``
  exist from 25.05 (slurm://src/slurmrestd/plugins/openapi/slurmctld/reservations.c#_mod_reservation@25.05+,
  slurm://src/slurmrestd/plugins/openapi/slurmctld/reservations.c#_mod_reservations@25.05+,
  slurm://src/slurmrestd/plugins/openapi/slurmctld/reservations.c#_delete_resv@25.05+);
  the echo is the parsed RESERVATION_DESC_MSG
  (slurm://src/plugins/data_parser/v0.0.45/parsers.c#RESERVATION_DESC_MSG@26.05+).
* Request checks: slurm://src/common/slurm_protocol_defs.c#validate_resv_create_desc@25.05+,
  then slurm://src/slurmctld/reservation.c#create_resv (``_job_overlap`` ->
  ESLURM_NODES_BUSY unless IGNORE_JOBS, ``_resv_overlap`` ->
  ESLURM_RESERVATION_OVERLAP), slurm://src/slurmctld/reservation.c#update_resv,
  slurm://src/slurmctld/reservation.c#delete_resv.
* scontrol: slurm://src/scontrol/create_res.c#_parse_res_options,
  slurm://src/scontrol/create_res.c#scontrol_create_res,
  slurm://src/scontrol/scontrol.c#_delete_it,
  slurm://src/api/reservation_info.c#slurm_sprint_reservation_info.

Version gating of the REST paths is covered per dialect in
``tests/test_slurmrestd_dialects.py``.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta

import pytest

from emulator.commands import dispatcher
from emulator.commands.scontrol import ScontrolEmulator, _ParseError, parse_resv_flags
from emulator.core import cluster_model, reservations
from emulator.core.database import Reservation, SlurmDatabase
from emulator.core.time_engine import TimeEngine
from emulator.slurm_version import at_least, current

# The REST write paths exist from 25.05; scontrol writes in every tracked version.
REST_WRITES = pytest.mark.slurm_version("25.05+")

V = current().api_version
T0 = datetime(2026, 10, 17, 9, 0, 0)
TS0 = int(T0.timestamp())


@pytest.fixture
def sim(state_env, monkeypatch):
    """Simulated job clock at T0: 60 s to start, 1 h runs, no admission checks."""
    monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DELAY", "60")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DURATION", "3600")
    monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "none")
    for name in (
        cluster_model.NODE_STATES_ENV,
        cluster_model.BACKGROUND_LOAD_ENV,
        cluster_model.RESERVATIONS_ENV,
        "SLURM_EMULATOR_JOB_EXECUTE",
        "SLURM_EMULATOR_TIME_FOLLOW_WALL",
        "SLURM_EMULATOR_NSS",
    ):
        monkeypatch.delenv(name, raising=False)
    set_time(T0)
    return state_env


def set_time(when: datetime) -> None:
    TimeEngine().set_time(when)


def submit(restd, headers, **job):
    body = restd.post(f"/slurm/{V}/job/submit", headers=headers, json={"job": job}).json()
    assert body["errors"] == [], body["errors"]
    return body["job_id"]


def ctld_jobs(restd, headers):
    jobs = restd.get(f"/slurm/{V}/jobs/", headers=headers).json()["jobs"]
    return {j["job_id"]: j for j in jobs}


def post_resv(restd, headers, **desc):
    return restd.post(f"/slurm/{V}/reservation", headers=headers, json=desc)


def maint(name="waldur-maint-1", start=TS0 + 3600, **extra):
    desc = {
        "name": name,
        "start_time": start,
        "duration": 120,
        "node_list": "node[001-002]",
        "flags": ["MAINT"],
        "users": ["root"],
    }
    desc.update(extra)
    return desc


def stored() -> dict:
    db = SlurmDatabase()
    db.load_state()
    return db.reservations


# --- slurmrestd ---


@REST_WRITES
class TestRestCreate:
    def test_create_echoes_request_and_lists(self, restd, auth_headers, sim):
        resp = post_resv(restd, auth_headers, **maint(comment="Waldur announcement 7"))
        body = resp.json()
        assert resp.status_code == 200 and body["errors"] == []
        echo = body["reservations"][0]
        assert echo["name"] == "waldur-maint-1"
        assert echo["node_list"] == ["node001", "node002"]
        assert echo["users"] == ["root"] and echo["accounts"] == []
        assert echo["flags"] == ["MAINT"]
        assert echo["duration"] == {"set": True, "infinite": False, "number": 120}
        assert echo["start_time"] == {"set": True, "infinite": False, "number": TS0 + 3600}
        assert echo["end_time"] == {"set": False, "infinite": False, "number": 0}
        assert echo["comment"] == "Waldur announcement 7"
        listed = restd.get(f"/slurm/{V}/reservations/", headers=auth_headers).json()
        resv = listed["reservations"][0]
        assert resv["name"] == "waldur-maint-1"
        assert resv["node_list"] == "node[001-002]" and resv["node_count"] == 2
        assert resv["flags"] == ["MAINT", "SPEC_NODES"]
        assert resv["end_time"]["number"] - resv["start_time"]["number"] == 7200
        single = restd.get(f"/slurm/{V}/reservation/waldur-maint-1", headers=auth_headers).json()
        assert single["reservations"][0]["name"] == "waldur-maint-1"

    def test_persisted_with_api_origin(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint())
        resv = stored()["waldur-maint-1"]
        assert resv.origin == "api" and resv.duration == 120 and resv.spec == ""

    def test_string_times_and_partition_only(self, restd, auth_headers, sim):
        body = post_resv(
            restd,
            auth_headers,
            name="part",
            start_time="now",
            end_time="now+60minutes",
            partition="debug",
            accounts="root",
        ).json()
        assert body["errors"] == []
        # validate_resv_create_desc turns a bare partition into PART_NODES + ALL.
        assert body["reservations"][0]["node_list"] == ["ALL"]
        assert body["reservations"][0]["flags"] == ["PART_NODES"]
        resv = stored()["part"]
        assert resv.nodes == cluster_model.partition_nodes("debug")
        assert resv.flags == ["PART_NODES", "SPEC_NODES"]
        assert (resv.start_time, resv.end_time) == (T0, T0 + timedelta(minutes=60))

    def test_bare_now_offset_is_seconds(self, restd, auth_headers, sim):
        # slurm://src/common/parse_time.c#_get_delta multiplies only when a unit follows.
        post_resv(
            restd,
            auth_headers,
            name="s",
            start_time="now",
            end_time="now+90",
            partition="debug",
            accounts="root",
        )
        assert stored()["s"].end_time == T0 + timedelta(seconds=90)

    def test_all_nodes_flag(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint(node_list="ALL"))
        resv = stored()["waldur-maint-1"]
        assert resv.nodes == cluster_model.all_nodes()
        assert resv.flags == ["MAINT", "SPEC_NODES", "ALL_NODES"]

    def test_node_count_picks_free_nodes_of_partition(self, restd, auth_headers, sim):
        jid = submit(restd, auth_headers, name="busy", partition="debug", time_limit=600)
        set_time(T0 + timedelta(minutes=2))
        assert ctld_jobs(restd, auth_headers)[jid]["nodes"] == "node001"
        body = post_resv(
            restd,
            auth_headers,
            name="cnt",
            start_time="now",
            duration=60,
            node_count=2,
            partition="debug",
            users="root",
        ).json()
        assert body["errors"] == []
        assert stored()["cnt"].nodes == ["node002", "node003"]

    def test_node_count_defaults_to_first_partition(self, restd, auth_headers, sim):
        post_resv(
            restd, auth_headers, name="cnt", start_time=TS0, duration=60, node_count=1, users="root"
        )
        first = next(iter(cluster_model.partition_ranges()))
        assert stored()["cnt"].partition == first


@REST_WRITES
class TestRestErrors:
    @pytest.mark.parametrize(
        ("change", "message"),
        [
            ({"start_time": None}, "A start time must be given.  No reservation created."),
            ({"duration": None}, "An end time or duration must be given.  No reservation created."),
            (
                {"users": None},
                (
                    "Either Users/Groups, AllowedPartitions, QOS and/or Accounts must "
                    "be specified.  No reservation created."
                )
                if at_least("25.11")
                else "Either Users/Groups and/or Accounts must be specified.  No reservation created.",
            ),
            (
                {"node_list": None},
                "CoreCnt, Nodes, NodeCnt, TRES or Watts must be specified.  "
                "No reservation created.",
            ),
            (
                {"end_time": TS0 + 60},
                "StartTime + Duration does not equal EndTime.  No reservation created.",
            ),
        ],
    )
    def test_validate_resv_create_desc(self, restd, auth_headers, sim, change, message):
        desc = maint()
        desc.update(change)
        resp = post_resv(restd, auth_headers, **{k: v for k, v in desc.items() if v is not None})
        err = resp.json()["errors"][0]
        assert resp.status_code == 422
        assert (err["error_number"], err["source"], err["description"]) == (
            2053,
            "validate_resv_create_desc",
            message,
        )
        assert "reservations" not in resp.json()

    @pytest.mark.parametrize(
        ("change", "errno"),
        [
            ({"start_time": TS0 - 3600}, 2054),  # ESLURM_INVALID_TIME_VALUE
            ({"partition": "nope", "node_list": None}, 2000),  # ESLURM_INVALID_PARTITION_NAME
            ({"node_list": "node[001-002],bogus01"}, 2018),  # ESLURM_INVALID_NODE_NAME
        ],
    )
    def test_create_resv_errors(self, restd, auth_headers, sim, change, errno):
        desc = maint()
        desc.update(change)
        resp = post_resv(restd, auth_headers, **{k: v for k, v in desc.items() if v is not None})
        err = resp.json()["errors"][0]
        assert resp.status_code == 422
        assert err["error_number"] == errno and err["source"] == "slurm_create_reservation"
        assert err["description"] == "Error creating reservation  waldur-maint-1"
        assert err["error"] == reservations.strerror(errno)

    def test_unknown_account_refused_when_enforcing(self, restd, auth_headers, sim, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "associations")
        err = post_resv(restd, auth_headers, **maint(users=None, accounts="ghost")).json()[
            "errors"
        ][0]
        assert err["error_number"] == 2045
        assert (
            post_resv(restd, auth_headers, **maint(users=None, accounts="root")).json()["errors"]
            == []
        )

    def test_name_required(self, restd, auth_headers, sim):
        err = post_resv(restd, auth_headers, **maint(name=None)).json()["errors"][0]
        assert (err["error_number"], err["description"], err["source"]) == (
            2053,
            "Reservation must be given.",
            "_mod_reservation",
        )

    def test_empty_body(self, restd, auth_headers, sim):
        resp = restd.post(f"/slurm/{V}/reservation", headers=auth_headers)
        err = resp.json()["errors"][0]
        assert resp.status_code == 400
        assert err["description"] == "unexpected empty query for reservation creation"

    def test_unknown_flag_fails_parsing(self, restd, auth_headers, sim):
        resp = post_resv(restd, auth_headers, **maint(flags=["MAINT", "BOGUS"]))
        assert resp.status_code == 400
        assert resp.json()["errors"][0]["description"] == (
            "Rejecting request. Failure parsing parameters"
        )


@REST_WRITES
class TestRunningJobs:
    """slurm://src/slurmctld/reservation.c#_job_overlap against the emulator's allocations."""

    def _running_job(self, restd, auth_headers, time_limit=240):
        jid = submit(restd, auth_headers, name="run", partition="debug", time_limit=time_limit)
        set_time(T0 + timedelta(minutes=2))
        assert ctld_jobs(restd, auth_headers)[jid]["nodes"] == "node001"
        return jid

    def test_maint_now_on_busy_node_refused(self, restd, auth_headers, sim):
        self._running_job(restd, auth_headers)
        err = post_resv(restd, auth_headers, **maint(start="now")).json()["errors"][0]
        assert err["error_number"] == 2016 and err["error"] == "Requested nodes are busy"

    def test_ignore_jobs_allows_it(self, restd, auth_headers, sim):
        self._running_job(restd, auth_headers)
        body = post_resv(restd, auth_headers, **maint(start="now", flags=["MAINT", "IGNORE_JOBS"]))
        assert body.json()["errors"] == []
        assert stored()["waldur-maint-1"].flags == ["MAINT", "IGNORE_JOBS", "SPEC_NODES"]

    def test_window_after_job_end_is_fine(self, restd, auth_headers, sim):
        self._running_job(restd, auth_headers, time_limit=30)
        assert post_resv(restd, auth_headers, **maint()).json()["errors"] == []

    def test_window_before_job_end_is_busy(self, restd, auth_headers, sim):
        self._running_job(restd, auth_headers, time_limit=240)
        assert post_resv(restd, auth_headers, **maint()).json()["errors"][0]["error_number"] == 2016

    def test_node_count_without_partition_note(self, restd, auth_headers, sim):
        for _ in range(4):
            submit(restd, auth_headers, name="fill", partition="debug", time_limit=600)
        set_time(T0 + timedelta(minutes=2))
        err = post_resv(
            restd,
            auth_headers,
            name="cnt",
            start_time="now",
            duration=60,
            node_count=1,
            partition="debug",
            users="root",
        ).json()["errors"][0]
        assert err["error_number"] == 2016
        assert err["description"].startswith("Error creating reservation cnt. Note, unless nodes")


@REST_WRITES
class TestOverlap:
    def test_plain_reservations_cannot_share_nodes(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint(name="a", flags=[]))
        err = post_resv(restd, auth_headers, **maint(name="b", flags=[])).json()["errors"][0]
        assert err["error_number"] == 2058

    def test_maint_may_overlap(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint(name="a", flags=[]))
        assert post_resv(restd, auth_headers, **maint(name="b")).json()["errors"] == []

    def test_disjoint_times_do_not_overlap(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint(name="a", flags=[]))
        later = maint(name="b", flags=[], start=TS0 + 6 * 3600)
        assert post_resv(restd, auth_headers, **later).json()["errors"] == []


@REST_WRITES
class TestRestUpdate:
    def test_post_existing_updates(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint())
        body = post_resv(
            restd, auth_headers, name="waldur-maint-1", end_time=TS0 + 5 * 3600, users="+alice"
        ).json()
        assert body["errors"] == []
        assert body["reservations"][0]["users"] == ["+alice"]
        resv = stored()["waldur-maint-1"]
        assert resv.end_time == T0 + timedelta(hours=5) and resv.users == "root,alice"
        assert resv.duration is None

    def test_moving_start_keeps_duration(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint())
        post_resv(restd, auth_headers, name="waldur-maint-1", start_time=TS0 + 7200)
        resv = stored()["waldur-maint-1"]
        assert (resv.start_time, resv.end_time) == (
            T0 + timedelta(hours=2),
            T0 + timedelta(hours=4),
        )

    def test_started_reservation_cannot_move(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint(start=TS0))
        err = post_resv(restd, auth_headers, name="waldur-maint-1", start_time=TS0 + 7200).json()[
            "errors"
        ][0]
        assert (err["error_number"], err["source"], err["description"]) == (
            2104,
            "slurm_update_reservation",
            "Error updating reservation waldur-maint-1",
        )

    def test_end_before_start(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint())
        err = post_resv(restd, auth_headers, name="waldur-maint-1", end_time=TS0 + 1800).json()[
            "errors"
        ][0]
        assert err["error_number"] == 2054

    def test_no_flag_removal(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint(flags=["MAINT", "IGNORE_JOBS"]))
        post_resv(restd, auth_headers, name="waldur-maint-1", flags=["NO_IGNORE_JOBS"])
        assert stored()["waldur-maint-1"].flags == ["MAINT", "SPEC_NODES"]


@REST_WRITES
class TestRestBulk:
    def test_creates_several(self, restd, auth_headers, sim):
        resp = restd.post(
            f"/slurm/{V}/reservations/",
            headers=auth_headers,
            json={
                "reservations": [maint(name="a"), maint(name="b", node_list="node003")],
            },
        )
        assert resp.json()["errors"] == []
        assert [r["name"] for r in resp.json()["reservations"]] == ["a", "b"]
        assert set(stored()) == {"a", "b"}

    def test_partial_failure_keeps_successes(self, restd, auth_headers, sim):
        resp = restd.post(
            f"/slurm/{V}/reservations/",
            headers=auth_headers,
            json={
                "reservations": [maint(name="a"), maint(name="b", start_time=None)],
            },
        )
        body = resp.json()
        assert resp.status_code == 422 and "reservations" not in body
        assert body["errors"][0]["source"] == "validate_resv_create_desc"
        assert set(stored()) == {"a"}

    def test_every_entry_needs_a_name(self, restd, auth_headers, sim):
        body = restd.post(
            f"/slurm/{V}/reservations/",
            headers=auth_headers,
            json={
                "reservations": [maint(name="a"), maint(name=None)],
            },
        ).json()
        assert body["errors"][0]["description"] == "Reservation must be given"
        assert stored() == {}

    def test_missing_and_empty_array(self, restd, auth_headers, sim):
        missing = restd.post(f"/slurm/{V}/reservations/", headers=auth_headers, json={}).json()
        assert missing["errors"][0]["description"] == (
            "No reservation descriptions specified in reservations array"
        )
        empty = restd.post(
            f"/slurm/{V}/reservations/", headers=auth_headers, json={"reservations": []}
        ).json()
        assert empty["errors"] == [] and empty["reservations"] == []
        assert empty["warnings"][0]["source"] == "_parse_resv_desc_list"


@REST_WRITES
class TestRestDelete:
    def test_delete(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint())
        resp = restd.delete(f"/slurm/{V}/reservation/waldur-maint-1", headers=auth_headers)
        body = resp.json()
        assert resp.status_code == 200 and body["errors"] == []
        assert set(body) == {"meta", "errors", "warnings"}
        assert stored() == {}

    def test_unknown(self, restd, auth_headers, sim):
        resp = restd.delete(f"/slurm/{V}/reservation/nope", headers=auth_headers)
        err = resp.json()["errors"][0]
        assert resp.status_code == 422
        assert (err["error_number"], err["description"], err["source"]) == (
            2053,
            "Error deleting reservation nope",
            "slurm_delete_reservation",
        )

    def test_in_use(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint(flags=[]))
        submit(restd, auth_headers, name="in", partition="debug", reservation="waldur-maint-1")
        err = restd.delete(f"/slurm/{V}/reservation/waldur-maint-1", headers=auth_headers).json()[
            "errors"
        ][0]
        assert err["error_number"] == 2055 and err["error"] == "Requested reservation is in use"


@REST_WRITES
class TestSchedulingWithApiReservations:
    def test_maint_blocks_overlapping_job(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint(node_list="ALL", start=TS0 + 3 * 3600))
        long_job = submit(restd, auth_headers, name="long", partition="compute", time_limit=240)
        short_job = submit(restd, auth_headers, name="short", partition="compute", time_limit=60)
        set_time(T0 + timedelta(minutes=2))
        jobs = ctld_jobs(restd, auth_headers)
        assert jobs[short_job]["job_state"] == ["RUNNING"]
        assert jobs[long_job]["state_reason"] == "ReqNodeNotAvail"
        assert jobs[long_job]["state_description"] == cluster_model.MAINT_DESC

    def test_active_maint_flags_nodes(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint(start=TS0))
        nodes = {
            n["name"]: n
            for n in restd.get(f"/slurm/{V}/nodes/", headers=auth_headers).json()["nodes"]
        }
        assert nodes["node001"]["state"] == ["IDLE", "RESERVED", "MAINTENANCE"]
        assert nodes["node001"]["reservation"] == "waldur-maint-1"

    def test_job_may_request_api_reservation(self, restd, auth_headers, sim):
        post_resv(restd, auth_headers, **maint(flags=[], start=TS0, node_list="node[001-002]"))
        jid = submit(
            restd, auth_headers, name="in", partition="debug", reservation="waldur-maint-1"
        )
        set_time(T0 + timedelta(minutes=2))
        job = ctld_jobs(restd, auth_headers)[jid]
        assert job["job_state"] == ["RUNNING"] and job["resv_name"] == "waldur-maint-1"


@REST_WRITES
class TestEnvSyncOwnership:
    def test_env_sync_keeps_api_reservations(self, restd, auth_headers, sim, monkeypatch):
        post_resv(restd, auth_headers, **maint())
        monkeypatch.setenv(cluster_model.RESERVATIONS_ENV, "envmaint:node010:+1h:1h:MAINT")
        names = [
            r["name"]
            for r in restd.get(f"/slurm/{V}/reservations/", headers=auth_headers).json()[
                "reservations"
            ]
        ]
        assert set(names) == {"waldur-maint-1", "envmaint"}
        monkeypatch.delenv(cluster_model.RESERVATIONS_ENV)
        names = [
            r["name"]
            for r in restd.get(f"/slurm/{V}/reservations/", headers=auth_headers).json()[
                "reservations"
            ]
        ]
        assert names == ["waldur-maint-1"]

    def test_env_entry_does_not_replace_api_one(self, sim, monkeypatch):
        db = SlurmDatabase()
        db.reservations["dup"] = Reservation(
            name="dup",
            nodes=["node001"],
            origin="api",
            start_time=T0,
            end_time=T0 + timedelta(hours=1),
        )
        monkeypatch.setenv(cluster_model.RESERVATIONS_ENV, "dup:ALL:+1h:1h:MAINT")
        cluster_model.sync_reservations(db, T0)
        assert db.reservations["dup"].nodes == ["node001"]

    def test_old_state_file_reservations_are_env_owned(self, sim, monkeypatch):
        old = {
            "reservations": {
                "maint": {
                    "name": "maint",
                    "nodes": ["node001"],
                    "partition": "",
                    "start_time": T0.isoformat(),
                    "end_time": (T0 + timedelta(hours=1)).isoformat(),
                    "flags": ["MAINT"],
                    "users": "",
                    "accounts": "",
                    "spec": "maint:node001:+0:1h:MAINT",
                }
            }
        }
        (sim / "db.json").write_text(json.dumps(old))
        db = SlurmDatabase()
        db.load_state()
        assert db.reservations["maint"].origin == "env"
        cluster_model.sync_reservations(db, T0)  # env var unset: env-owned record goes
        assert db.reservations == {}


# --- scontrol ---


@pytest.fixture
def scontrol(sim):  # noqa: ARG001 - sim isolates the state and clock
    db = SlurmDatabase()
    db.load_state()
    return ScontrolEmulator(db, TimeEngine())


def run(scontrol, capsys, *args):
    out = scontrol.handle_command(list(args))
    return out, capsys.readouterr().err, scontrol.exit_code


CREATE = (
    "create",
    "reservation",
    "ReservationName=waldur-maint-1",
    "StartTime=2026-10-17T10:00:00",
    "Duration=2:00:00",
    "Nodes=node[001-002]",
    "Flags=MAINT,IGNORE_JOBS",
    "Users=root",
)


class TestScontrol:
    def test_create_show_update_delete(self, scontrol, capsys):
        assert run(scontrol, capsys, *CREATE) == ("Reservation created: waldur-maint-1\n", "", 0)
        out, _, code = run(scontrol, capsys, "show", "reservation", "waldur-maint-1")
        allowed = "   AllowedPartitions=(null) QOS=(null)\n" if at_least("25.11") else ""
        assert code == 0
        assert out == (
            "ReservationName=waldur-maint-1 StartTime=2026-10-17T10:00:00 "
            "EndTime=2026-10-17T12:00:00 Duration=02:00:00\n"
            "   Nodes=node[001-002] NodeCnt=2 CoreCnt=128 Features=(null) PartitionName=(null) "
            "Flags=MAINT,IGNORE_JOBS,SPEC_NODES\n"
            "   TRES=cpu=128\n"
            f"{allowed}"
            "   Users=root Groups=(null) Accounts=(null) Licenses=(null) State=INACTIVE "
            "BurstBuffer=(null)\n"
            "   MaxStartDelay=(null)\n\n"
        )
        assert run(
            scontrol,
            capsys,
            "update",
            "ReservationName=waldur-maint-1",
            "EndTime=2026-10-17T13:30:00",
        ) == ("Reservation updated.\n", "", 0)
        oneline, _, _ = run(scontrol, capsys, "-o", "show", "res")
        assert oneline.count("\n") == 1 and "Duration=03:30:00 Nodes=node[001-002]" in oneline
        assert run(scontrol, capsys, "delete", "ReservationName=waldur-maint-1") == ("", "", 0)
        assert run(scontrol, capsys, "show", "reservation") == (
            "No reservations in the system\n",
            "",
            0,
        )

    def test_persists_for_rest(self, scontrol, capsys, restd, auth_headers):
        run(scontrol, capsys, *CREATE)
        names = [
            r["name"]
            for r in restd.get(f"/slurm/{V}/reservations/", headers=auth_headers).json()[
                "reservations"
            ]
        ]
        assert names == ["waldur-maint-1"]
        assert stored()["waldur-maint-1"].origin == "api"

    def test_generated_name_and_partition(self, scontrol, capsys):
        out, _, code = run(
            scontrol,
            capsys,
            "create",
            "res",
            "StartTime=now",
            "Duration=60",
            "PartitionName=debug",
            "Accounts=root",
        )
        assert (out, code) == ("Reservation created: root_1\n", 0)
        assert scontrol.database.reservations["root_1"].flags == ["PART_NODES", "SPEC_NODES"]

    def test_now_plus_and_state_active(self, scontrol, capsys):
        run(
            scontrol,
            capsys,
            "create",
            "res",
            "ReservationName=r",
            "StartTime=now",
            "EndTime=now+1hour",
            "Nodes=node005",
            "Users=root",
        )
        out, _, _ = run(scontrol, capsys, "show", "ReservationName=r")
        assert "StartTime=2026-10-17T09:00:00 EndTime=2026-10-17T10:00:00" in out
        assert "State=ACTIVE" in out

    def test_json(self, scontrol, capsys):
        run(scontrol, capsys, *CREATE)
        out, _, code = run(scontrol, capsys, "--json", "show", "reservation")
        body = json.loads(out)
        assert code == 0 and body["errors"] == []
        assert body["reservations"][0]["name"] == "waldur-maint-1"
        assert body["reservations"][0]["node_list"] == "node[001-002]"

    @pytest.mark.parametrize(
        ("args", "stderr"),
        [
            (
                ("create", "reservation", "ReservationName=x", "Bogus=1"),
                "scontrol: error: Unknown parameter Bogus=1.  No reservation created.\n",
            ),
            (
                ("create", "reservation", "ReservationName=x", "oops"),
                "scontrol: error: Misformatted parameter 'oops', most options have a parameter "
                "after '='.  No reservation created.\n",
            ),
            (
                (
                    "create",
                    "reservation",
                    "ReservationName=x",
                    "StartTime=now",
                    "Duration=1",
                    "Nodes=ALL",
                    "Users=root",
                    "Flags=WHAT",
                ),
                "scontrol: error: Error parsing flags WHAT.  No reservation created.\n",
            ),
            (
                (
                    "create",
                    "reservation",
                    "ReservationName=x",
                    "StartTime=now",
                    "Nodes=ALL",
                    "Users=root",
                ),
                "scontrol: error: An end time or duration must be given.  "
                "No reservation created.\n",
            ),
            (
                (
                    "create",
                    "reservation",
                    "ReservationName=x",
                    "StartTime=now",
                    "Duration=x",
                    "Nodes=ALL",
                    "Users=root",
                ),
                "scontrol: error: Invalid duration Duration=x.  No reservation created.\n",
            ),
            (("create", "NodeCnt=1"), "scontrol: error: Invalid creation entity: NodeCnt=1\n"),
        ],
    )
    def test_parse_errors(self, scontrol, capsys, args, stderr):
        assert run(scontrol, capsys, *args) == ("", stderr, 1)

    def test_create_refused_by_slurmctld(self, scontrol, capsys):
        run(scontrol, capsys, *CREATE)
        assert run(scontrol, capsys, *CREATE) == (
            "",
            "Error creating the reservation: Duplicate reservation name\n",
            1,
        )

    def test_busy_nodes(self, scontrol, capsys, restd, auth_headers):
        submit(restd, auth_headers, name="run", partition="debug", time_limit=240)
        set_time(T0 + timedelta(minutes=2))
        ctld_jobs(restd, auth_headers)
        args = (
            "create",
            "reservation",
            "ReservationName=m",
            "StartTime=now",
            "Duration=60",
            "Nodes=node001",
            "Flags=MAINT",
            "Users=root",
        )
        assert run(scontrol, capsys, *args) == (
            "",
            "Error creating the reservation: Requested nodes are busy\n",
            1,
        )
        out, _, code = run(scontrol, capsys, *args[:-2], "Flags=MAINT,IGNORE_JOBS", "Users=root")
        assert (out, code) == ("Reservation created: m\n", 0)

    def test_update_and_delete_errors(self, scontrol, capsys):
        assert run(scontrol, capsys, "update", "ReservationName=nope", "Duration=10") == (
            "",
            "slurm_update error: Requested reservation is invalid\n",
            1,
        )
        assert run(scontrol, capsys, "delete", "reservation", "nope") == (
            "",
            "delete_reservation reservation: Requested reservation is invalid\n",
            1,
        )
        assert run(scontrol, capsys, "show", "reservation", "nope") == (
            "Reservation nope not found\n",
            "",
            1,
        )

    def test_plus_equals_lists_and_flags(self, scontrol, capsys):
        run(scontrol, capsys, *CREATE)
        run(
            scontrol,
            capsys,
            "update",
            "ReservationName=waldur-maint-1",
            "Users+=alice",
            "Flags-=IGNORE_JOBS",
        )
        resv = scontrol.database.reservations["waldur-maint-1"]
        assert resv.users == "root,alice" and resv.flags == ["MAINT", "SPEC_NODES"]
        run(scontrol, capsys, "update", "ReservationName=waldur-maint-1", "Duration+=30")
        resv = scontrol.database.reservations["waldur-maint-1"]
        assert resv.end_time - resv.start_time == timedelta(minutes=150)


class TestScontrolEntryPoints:
    def test_dispatcher_main(self, sim, monkeypatch, capsys):
        monkeypatch.setattr(dispatcher, "_emulator", None)
        monkeypatch.setattr(sys, "argv", ["scontrol", *CREATE])
        with pytest.raises(SystemExit) as exc:
            dispatcher.scontrol_main()
        assert exc.value.code == 0
        assert capsys.readouterr().out == "Reservation created: waldur-maint-1\n"
        monkeypatch.setattr(dispatcher, "_emulator", None)
        monkeypatch.setattr(sys, "argv", ["scontrol", "delete", "ReservationName=nope"])
        with pytest.raises(SystemExit) as exc:
            dispatcher.scontrol_main()
        captured = capsys.readouterr()
        assert exc.value.code == 1 and captured.out == ""
        assert (
            captured.err
            == "delete_reservation ReservationName=nope: Requested reservation is invalid\n"
        )

    def test_ssh_plane(self, sim):
        from emulator.api.ssh import server

        out, err, code = server._run_slurm("root", ["scontrol", *CREATE])
        assert (out, err, code) == ("Reservation created: waldur-maint-1\n", "", 0)
        out, err, code = server._run_slurm("root", ["scontrol", "show", "reservation"])
        assert code == 0 and out.startswith("ReservationName=waldur-maint-1 ")
        out, err, code = server._run_slurm("root", ["scontrol", "delete", "ReservationName=x"])
        assert code == 1 and err.endswith("Requested reservation is invalid\n")


class TestParsers:
    def test_flag_prefixes(self):
        assert parse_resv_flags("MAINT,IGNORE_JOBS", "") == ["MAINT", "IGNORE_JOBS"]
        assert parse_resv_flags("maint,ignore", "") == ["MAINT", "IGNORE_JOBS"]
        assert parse_resv_flags("-MAINT,IGNORE_JOBS", "") == ["NO_MAINT", "NO_IGNORE_JOBS"]
        with pytest.raises(_ParseError):
            parse_resv_flags("MA", "")  # "Maintenance" needs 3 characters

    def test_time_str2mins(self):
        assert reservations.time_str2mins("90") == 90
        assert reservations.time_str2mins("2:00:00") == 120
        assert reservations.time_str2mins("1-00:00:00") == 1440
        assert reservations.time_str2mins("1:30") == 2  # minutes:seconds, rounded up
        assert reservations.time_str2mins("UNLIMITED") == reservations.DURATION_INFINITE
        assert reservations.time_str2mins("x") is None
