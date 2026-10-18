"""Pending reasons, reservation access and exit codes as slurmctld sets them.

* ``ReqNodeNotAvail`` / "Reserved for maintenance" only for a reservation over
  all nodes, the job's partition, or MAINT over the whole partition
  (slurm://src/slurmctld/reservation.c#job_test_resv); otherwise reserved
  nodes are just unavailable and the job waits for ``Resources``;
* later jobs of a partition that could not start wait with ``Priority``
  (slurm://src/slurmctld/job_scheduler.c#_schedule);
* reservation access follows Users=/Accounts=
  (slurm://src/slurmctld/reservation.c#_valid_job_access_resv);
* an ended reservation is purged and its pending jobs held
  (slurm://src/slurmctld/reservation.c#_foreach_clear_job_resv);
* exit codes are wait statuses: a signal is ``0:<sig>``, a time limit ``0:15``
  in the controller and ``0:0`` in accounting
  (slurm://src/slurmctld/job_mgr.c#_job_complete, slurm://src/slurmctld/job_mgr.c#_job_timed_out).
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from emulator.commands.sacct import SacctEmulator
from emulator.core import cluster_model, exit_status
from emulator.core.database import Job, Reservation, SlurmDatabase
from emulator.core.time_engine import TimeEngine
from tests.test_cluster_model import T0, V, ctld_jobs, set_time, submit
from tests.test_cluster_model import sim as sim  # noqa: F401 - fixture


def as_user(headers, user):
    return {**headers, "X-SLURM-USER-NAME": user}


class TestMaintenanceReason:
    def test_partial_maint_is_just_unavailable_nodes(self, restd, auth_headers, sim, monkeypatch):
        # node001-002 of debug (node001-004) are reserved; a 4-node job waits for Resources.
        monkeypatch.setenv(cluster_model.RESERVATIONS_ENV, "m:node[001-002]:now:2h:MAINT")
        jid = submit(restd, auth_headers, name="big", partition="debug", nodes="4", time_limit=30,
                     shared=["none"])
        set_time(T0 + timedelta(minutes=2))
        job = ctld_jobs(restd, auth_headers)[jid]
        assert (job["state_reason"], job["state_description"]) == ("Resources", "")

    def test_maint_over_the_partition(self, restd, auth_headers, sim, monkeypatch):
        monkeypatch.setenv(cluster_model.RESERVATIONS_ENV, "m:debug:now:2h:MAINT")
        jid = submit(restd, auth_headers, name="j", partition="debug", time_limit=30)
        set_time(T0 + timedelta(minutes=2))
        job = ctld_jobs(restd, auth_headers)[jid]
        assert job["state_reason"] == "ReqNodeNotAvail"
        assert job["state_description"] == "ReqNodeNotAvail, Reserved for maintenance"

    def test_all_nodes_reservation_blocks_even_without_maint(self, restd, auth_headers, sim):
        db = SlurmDatabase()
        db.load_state()
        db.reservations["all"] = Reservation(
            name="all",
            nodes=cluster_model.all_nodes(),
            start_time=T0,
            end_time=T0 + timedelta(hours=2),
            flags=["ALL_NODES"],
            users="root",
            origin="api",
        )
        db.save_state()
        jid = submit(restd, auth_headers, name="j", partition="compute", time_limit=30)
        set_time(T0 + timedelta(minutes=2))
        assert ctld_jobs(restd, auth_headers)[jid]["state_reason"] == "ReqNodeNotAvail"


class TestPriority:
    def test_later_jobs_of_a_blocked_partition_wait_with_priority(self, restd, auth_headers, sim):
        whole = {"shared": ["none"]}
        first = submit(restd, auth_headers, name="a", partition="debug", nodes="4",
                       time_limit=600, **whole)
        second = submit(restd, auth_headers, name="b", partition="debug", nodes="4",
                        time_limit=60, **whole)
        third = submit(restd, auth_headers, name="c", partition="debug", nodes="1",
                       time_limit=60, **whole)
        other = submit(restd, auth_headers, name="d", partition="compute", time_limit=60)
        set_time(T0 + timedelta(minutes=2))
        jobs = ctld_jobs(restd, auth_headers)
        assert jobs[first]["job_state"] == ["RUNNING"]
        assert jobs[second]["state_reason"] == "Resources"
        assert jobs[third]["state_reason"] == "Priority"
        assert jobs[other]["job_state"] == ["RUNNING"]


class TestReservationAccess:
    @pytest.mark.parametrize(
        ("users", "accounts", "user", "account", "allowed"),
        [
            ("alice", "", "alice", "proj", True),
            ("alice", "", "bob", "proj", False),
            ("-bob", "", "alice", "proj", True),
            ("-bob", "", "bob", "proj", False),
            ("", "proj", "bob", "proj", True),
            ("", "proj", "bob", "sub", True),  # sub-account of proj
            ("", "proj", "bob", "other", False),
            ("", "-proj", "bob", "sub", False),
            ("alice", "proj", "alice", "proj", True),
            ("alice", "proj", "alice", "sub", False),  # the user/account pair only
            ("alice", "proj", "bob", "proj", False),
            ("alice", "", "root", "x", True),
        ],
    )
    def test_users_and_accounts(self, state_env, users, accounts, user, account, allowed):
        db = SlurmDatabase()
        db.add_account("proj", "p", "org")
        db.add_account("sub", "s", "org")
        db.set_account_parent("sub", "proj")
        db.add_account("other", "o", "org")
        resv = Reservation(name="r", users=users, accounts=accounts)
        job = Job(job_id="1", account=account, user=user, state="PENDING")
        assert cluster_model.job_may_access(db, resv, job) is allowed

    def test_time_float_is_closed(self, state_env):
        resv = Reservation(name="r", users="alice", flags=["TIME_FLOAT"])
        job = Job(job_id="1", account="a", user="alice", state="PENDING")
        assert not cluster_model.job_may_access(SlurmDatabase(), resv, job)

    def test_submit_outside_the_reservation_is_refused(self, restd, auth_headers, sim,
                                                       monkeypatch):
        monkeypatch.setenv(cluster_model.RESERVATIONS_ENV, "train:debug:now:4h::users=alice")
        body = restd.post(
            f"/slurm/{V}/job/submit",
            headers=as_user(auth_headers, "bob"),
            json={"job": {"name": "x", "partition": "debug", "reservation": "train"}},
        ).json()
        assert body["errors"][0]["error_number"] == 2052
        assert body["errors"][0]["description"] == "Access denied to requested reservation"

    def test_access_withdrawn_after_submit_keeps_job_waiting(
        self, restd, auth_headers, sim, monkeypatch
    ):
        monkeypatch.setenv(cluster_model.RESERVATIONS_ENV, "train:debug:+1h:4h::users=alice")
        jid = submit(
            restd, as_user(auth_headers, "alice"), name="x", partition="debug", reservation="train"
        )
        monkeypatch.setenv(cluster_model.RESERVATIONS_ENV, "train:debug:+1h:4h::users=carol")
        set_time(T0 + timedelta(hours=1, minutes=5))
        assert ctld_jobs(restd, auth_headers)[jid]["state_reason"] == "Reservation"


class TestEndedReservation:
    def test_pending_jobs_are_held(self, restd, auth_headers, sim, monkeypatch):
        monkeypatch.setenv(cluster_model.RESERVATIONS_ENV, "r:node001:now:1h")
        busy = submit(restd, auth_headers, name="busy", partition="debug", reservation="r",
                      time_limit=120, shared=["none"])
        waiting = submit(restd, auth_headers, name="w", partition="debug", reservation="r",
                         time_limit=30)
        set_time(T0 + timedelta(minutes=2))
        assert ctld_jobs(restd, auth_headers)[busy]["job_state"] == ["RUNNING"]
        # The running job ends at its emulated hour; the reservation then has
        # ended with nothing running in it and is purged.
        set_time(T0 + timedelta(hours=1, minutes=5))
        job = ctld_jobs(restd, auth_headers)[waiting]
        assert job["job_state"] == ["PENDING"]
        assert job["state_reason"] == "ReservationDeleted"
        assert job["state_description"] == "Reservation r was deleted"
        assert job["resv_name"] == ""
        assert restd.get(f"/slurm/{V}/reservations/", headers=auth_headers).json()[
            "reservations"
        ] == []
        set_time(T0 + timedelta(hours=5))
        assert ctld_jobs(restd, auth_headers)[waiting]["job_state"] == ["PENDING"]


class TestExitStatus:
    @pytest.mark.parametrize(
        ("state", "code", "colon", "status"),
        [
            ("COMPLETED", None, "0:0", "SUCCESS"),
            ("FAILED", None, "1:0", "ERROR"),
            ("FAILED", 3, "3:0", "ERROR"),
            ("FAILED", -9, "0:9", "SIGNALED"),
            ("TIMEOUT", None, "0:0", "SUCCESS"),
            ("TIMEOUT", -15, "0:15", "SIGNALED"),
        ],
    )
    def test_renderings(self, state, code, colon, status):
        assert exit_status.colon(state, code) == colon
        assert exit_status.verbose(state, code)["status"] == [status]

    def test_signal_fields(self):
        dumped = exit_status.verbose("FAILED", -9)
        assert dumped["signal"]["name"] == "KILL"
        assert dumped["signal"]["id"]["number"] == 9
        assert exit_status.verbose("COMPLETED", 0)["signal"] == {
            "id": {"set": False, "infinite": False, "number": 0},
            "name": "",
        }
        assert exit_status.signal_name(11) == "11"  # not in Slurm's table

    def test_failure_reasons(self):
        assert exit_status.failure_reason("FAILED", -11) == (
            "RaisedSignal",
            "RaisedSignal:11(Segmentation fault)",
        )
        assert exit_status.failure_reason("FAILED", 2) == ("NonZeroExitCode", "")
        assert exit_status.failure_reason("TIMEOUT", -15) == ("TimeLimit", "")

    def test_timeout_job_in_controller_and_accounting(self, restd, auth_headers, sim):
        jid = submit(restd, auth_headers, name="t", partition="compute", time_limit=10)
        set_time(T0 + timedelta(minutes=30))
        job = ctld_jobs(restd, auth_headers)[jid]
        assert job["job_state"] == ["TIMEOUT"]
        assert job["state_reason"] == "TimeLimit"
        assert job["exit_code"]["status"] == ["SIGNALED"]
        assert job["exit_code"]["signal"]["id"]["number"] == 15
        dbd = restd.get(f"/slurmdb/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
        assert dbd["exit_code"]["status"] == ["SUCCESS"]
        assert dbd["steps"][0]["exit_code"]["signal"]["name"] == "TERM"
        db = SlurmDatabase()
        db.load_state()
        out = SacctEmulator(db, TimeEngine()).handle_command(
            ["-j", str(jid), "-n", "-P", "-o", "State,ExitCode"]
        )
        assert out.splitlines()[0] == "TIMEOUT|0:0"
