"""scontrol show partition / show job / write batch_script / ping.

* slurm://src/api/partition_info.c#slurm_sprint_partition_info
* slurm://src/scontrol/info_job.c#_sprint_job_info, slurm://src/scontrol/info_job.c#scontrol_print_job
* slurm://src/scontrol/info_job.c#scontrol_batch_script
* slurm://src/scontrol/scontrol.c#_print_ping
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from emulator.commands.scontrol import ScontrolEmulator
from emulator.core import cluster_model
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine
from emulator.slurm_version import at_least
from tests.test_cluster_model import T0, set_time, submit
from tests.test_cluster_model import sim as sim  # noqa: F401 - fixture


@pytest.fixture
def scontrol(sim):  # noqa: ARG001 - sim isolates the state and clock
    db = SlurmDatabase()
    db.load_state()
    return ScontrolEmulator(db, TimeEngine())


def run(scontrol, capsys, *args):
    out = scontrol.handle_command(list(args))
    return out, capsys.readouterr().err, scontrol.exit_code


def fields(record: str) -> dict[str, str]:
    """key=value pairs of one record (values without blanks)."""
    out: dict[str, str] = {}
    for token in record.split():
        key, sep, value = token.partition("=")
        if sep:
            out[key] = value
    return out


class TestShowPartition:
    def test_record(self, scontrol, capsys):
        out, err, code = run(scontrol, capsys, "show", "partition", "debug")
        assert (err, code) == ("", 0)
        assert out == (
            "PartitionName=debug\n"
            "   AllowGroups=ALL AllowAccounts=ALL AllowQos=ALL\n"
            "   AllocNodes=ALL Default=YES QoS=N/A\n"
            "   DefaultTime=NONE DisableRootJobs=NO Exclusive=NO GraceTime=0 Hidden=NO\n"
            "   MaxNodes=UNLIMITED MaxTime=UNLIMITED MinNodes=1 LLN=NO "
            "MaxCPUsPerNode=UNLIMITED MaxCPUsPerSocket=UNLIMITED\n"
            "   Nodes=node[001-004]\n"
            "   PriorityJobFactor=1 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO\n"
            "   OverTimeLimit=NONE PreemptMode=OFF\n"
            "   State=UP TotalCPUs=256 TotalNodes=4 SelectTypeParameters=NONE\n"
            "   JobDefaults=(null)\n"
            "   DefMemPerCPU=8192 MaxMemPerNode=UNLIMITED\n"
            "   TRES=cpu=256,mem=2T,node=4,billing=256,gres/gpu=16\n"
            "\n"
        )

    def test_one_liner_for_every_partition(self, scontrol, capsys):
        # The site agent's call (waldur_site_agent_slurm reservations).
        out, _, code = run(scontrol, capsys, "-o", "show", "partition")
        lines = out.splitlines()
        assert code == 0 and len(lines) == 2
        assert fields(lines[0])["PartitionName"] == "debug"
        assert fields(lines[1])["Nodes"] == "node[005-100]"
        assert fields(lines[1])["Default"] == "NO"

    def test_firecrest_partitions_command(self, scontrol, capsys):
        out, _, _ = run(scontrol, capsys, "-a", "show", "-o", "partitions")
        for line in out.splitlines():
            record = fields(line)
            assert {"PartitionName", "State", "TotalCPUs", "TotalNodes", "Hidden"} <= set(record)

    def test_unknown(self, scontrol, capsys):
        out, _, code = run(scontrol, capsys, "show", "partition", "nope")
        assert (out, code) == ("Partition nope not found\n", 1)


class TestShowJob:
    def test_running_job(self, restd, auth_headers, scontrol, capsys):
        jid = submit(restd, auth_headers, name="sim", partition="compute", nodes="2",
                     tasks_per_node=4, cpus_per_task=2, time_limit=90)
        set_time(T0 + timedelta(minutes=11))
        out, err, code = run(scontrol, capsys, "show", "job", str(jid))
        assert (err, code) == ("", 0)
        record = fields(out)
        assert record["JobId"] == str(jid) and record["JobName"] == "sim"
        assert record["JobState"] == "RUNNING" and record["Reason"] == "None"
        assert record["ExitCode"] == "0:0"
        assert record["RunTime"] == "00:10:00" and record["TimeLimit"] == "01:30:00"
        assert record["NodeList"] == "node[005-006]" and record["BatchHost"] == "node005"
        assert (record["NumNodes"], record["NumCPUs"], record["NumTasks"]) == ("2", "16", "8")
        assert record["CPUs/Task"] == "2"
        assert record["ReqTRES"] == "cpu=16,mem=128G,node=2,billing=16"
        assert record["AllocTRES"] == "cpu=16,mem=128G,node=2,billing=16"
        assert record["MinMemoryCPU"] == "8G"
        assert record["StdIn"] == "/dev/null"
        assert record["StdOut"].endswith(f"slurm-{jid}.out")
        assert ("Exclusive" in record) is at_least("26.05")
        assert ("LicensesAlloc" in record) is at_least("25.05")
        assert ("SubmitLine" in record) is at_least("25.11")
        assert out.endswith("\n\n")

    def test_pending_job_and_reason_blanks(self, restd, auth_headers, scontrol, capsys,
                                            monkeypatch):
        monkeypatch.setenv(cluster_model.RESERVATIONS_ENV, "m:debug:now:2h:MAINT")
        jid = submit(restd, auth_headers, name="p", partition="debug", time_limit=30)
        set_time(T0 + timedelta(minutes=2))
        out, _, _ = run(scontrol, capsys, "show", "-o", "job", str(jid))
        record = fields(out)
        assert out.count("\n") == 1
        assert record["JobState"] == "PENDING"
        assert record["Reason"] == "ReqNodeNotAvail,_Reserved_for_maintenance"
        assert (record["StartTime"], record["EndTime"]) == ("Unknown", "Unknown")
        assert record["NodeList"] == "(null)" and record["AllocTRES"] == "(null)"
        assert "BatchHost" not in record

    def test_signalled_job(self, restd, auth_headers, scontrol, capsys):
        jid = submit(restd, auth_headers, name="t", partition="compute", time_limit=10)
        set_time(T0 + timedelta(hours=1))
        record = fields(run(scontrol, capsys, "show", "job", str(jid))[0])
        assert record["JobState"] == "TIMEOUT"
        assert record["Reason"] == "TimeLimit" and record["ExitCode"] == "0:15"

    def test_exclusive_job(self, restd, auth_headers, scontrol, capsys):
        jid = submit(restd, auth_headers, name="x", partition="compute", shared=["none"])
        set_time(T0 + timedelta(minutes=2))
        record = fields(run(scontrol, capsys, "show", "job", str(jid))[0])
        assert record["NumCPUs"] == str(cluster_model.NODE_CPUS)
        assert record["OverSubscribe"] == "NO"

    def test_unknown_and_invalid_ids(self, scontrol, capsys):
        assert run(scontrol, capsys, "show", "job", "999") == (
            "",
            "slurm_load_jobs error: Invalid job id specified\n",
            1,
        )
        assert run(scontrol, capsys, "show", "job", "abc") == (
            "",
            "scontrol_print_job error: Invalid job id specified\n",
            1,
        )

    def test_no_jobs(self, scontrol, capsys):
        assert run(scontrol, capsys, "show", "job") == ("No jobs in the system\n", "", 0)


class TestWriteBatchScript:
    def test_to_stdout(self, restd, auth_headers, scontrol, capsys):
        script = "#!/bin/bash\necho hi\n"
        jid = submit(restd, auth_headers, name="s", partition="debug", script=script)
        assert run(scontrol, capsys, "write", "batch_script", str(jid), "-") == (script, "", 0)

    def test_unknown_job_keeps_exit_code(self, scontrol, capsys):
        out, err, code = run(scontrol, capsys, "write", "batch_script", "999", "-")
        assert (out, code) == ("", 0)
        assert err == "job script retrieval failed: Invalid job id specified\n"


def test_ping(scontrol, capsys):
    assert run(scontrol, capsys, "ping") == ("Slurmctld(primary) at localhost is UP\n", "", 0)
