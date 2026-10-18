"""Synthetic Sonar telemetry (emulator/telemetry/sonar.py + the publisher CLI)."""

from __future__ import annotations

import io
import json
from datetime import datetime, timedelta, timezone
from statistics import mean

import pytest

from emulator.api.slurmrestd import schemas
from emulator.core import cluster_model
from emulator.core.database import Job, SlurmDatabase, UsageRecord
from emulator.telemetry import sonar, sonar_cli

# Field lists of Sonar's types.spec.yaml 0.18.4, the format slurm-monitor's
# importer consumes (it splats these names into its table classes).
SPEC = {
    "MetadataObject": {"producer", "version", "format", "attrs", "token"},
    "SampleAttributes": {"time", "cluster", "node", "system", "jobs", "errors"},
    "SampleSystem": {
        "boot",
        "cpus",
        "gpus",
        "disks",
        "used_memory",
        "load1",
        "load5",
        "load15",
        "runnable_entities",
        "existing_entities",
    },
    "SampleGpu": {
        "index",
        "uuid",
        "failing",
        "fan",
        "compute_mode",
        "performance_state",
        "memory",
        "ce_util",
        "memory_util",
        "temperature",
        "power",
        "power_limit",
        "ce_clock",
        "memory_clock",
    },
    "SampleJob": {"job", "user", "epoch", "processes"},
    "SampleProcess": {
        "resident_memory",
        "virtual_memory",
        "cmd",
        "pid",
        "ppid",
        "in_container",
        "num_threads",
        "cpu_avg",
        "cpu_util",
        "cpu_time",
        "data_read",
        "data_written",
        "data_cancelled",
        "rolledup",
        "gpus",
    },
    "SampleProcessGpu": {"index", "uuid", "gpu_util", "gpu_memory", "gpu_memory_util"},
    "SysinfoAttributes": {
        "time",
        "cluster",
        "node",
        "os_name",
        "os_release",
        "architecture",
        "numa_nodes",
        "sockets",
        "cores_per_socket",
        "threads_per_core",
        "cpu_model",
        "memory",
        "topo_svg",
        "topo_text",
        "cards",
        "distances",
    },
    "SysinfoGpuCard": {
        "index",
        "uuid",
        "address",
        "manufacturer",
        "model",
        "architecture",
        "driver",
        "firmware",
        "memory",
        "power_limit",
        "max_power_limit",
        "min_power_limit",
        "max_ce_clock",
        "max_memory_clock",
    },
    "JobsAttributes": {"time", "cluster", "slurm_jobs"},
    "SlurmJob": {
        "job_id",
        "job_step",
        "job_name",
        "job_state",
        "array_job_id",
        "array_task_id",
        "het_job_id",
        "het_job_offset",
        "user_name",
        "account",
        "submit_time",
        "time_limit",
        "partition",
        "reservation",
        "nodes",
        "priority",
        "distribution",
        "requested_resources",
        "allocated_resources",
        "requested_cpus",
        "requested_memory_per_node",
        "requested_node_count",
        "start_time",
        "suspend_time",
        "end_time",
        "exit_code",
        "sacct",
        # deprecated, not in 0.18.4's list, still in Sonar's example data and
        # required by slurm-monitor's JobResponse
        "minimum_cpus_per_node",
    },
    "ClusterAttributes": {"time", "cluster", "slurm", "partitions", "nodes"},
    "ClusterPartition": {"name", "nodes"},
    "ClusterNodes": {"names", "states"},
}

CLUSTER = "hpc-demo"
START = datetime(2026, 10, 12, 21, 37, 37, tzinfo=timezone.utc)
END = START + timedelta(hours=14)


@pytest.fixture(autouse=True)
def demo_partitions(monkeypatch):
    monkeypatch.setattr(
        schemas, "PARTITION_RANGES", {"debug": (1, 4), "batch": (5, 36), "gpu": (37, 40)}
    )
    monkeypatch.delenv(cluster_model.NODE_STATES_ENV, raising=False)
    monkeypatch.delenv(cluster_model.BACKGROUND_LOAD_ENV, raising=False)
    monkeypatch.delenv(cluster_model.RESERVATIONS_ENV, raising=False)


def gpu_run(**kw) -> sonar.JobRun:
    base = {
        "job_id": 86,
        "user": "alice",
        "account": "alloc_climate",
        "partition": "gpu",
        "job_name": "gromacs-md",
        "state": "COMPLETED",
        "nodes": ("node039", "node040"),
        "alloc_cpus": 128,
        "mem_per_node_mb": 512 * 1024,
        "start_time": START,
        "end_time": END,
        "time_limit": 1440,
        "exit_code": 0,
    }
    base.update(kw)
    return sonar.JobRun(**base)


def cpu_run(**kw) -> sonar.JobRun:
    base = {
        "job_id": 50,
        "user": "bob",
        "partition": "batch",
        "job_name": "wrf-post",
        "nodes": ("node017",),
        "alloc_cpus": 64,
    }
    base.update(kw)
    return gpu_run(**base)


def check_keys(obj: dict, spec: str) -> None:
    assert set(obj) <= SPEC[spec], f"{spec}: unknown fields {set(obj) - SPEC[spec]}"


def check_sample(message: dict) -> None:
    check_keys(message["meta"], "MetadataObject")
    assert message["data"]["type"] == "sample"
    attrs = message["data"]["attributes"]
    check_keys(attrs, "SampleAttributes")
    check_keys(attrs["system"], "SampleSystem")
    for gpu in attrs["system"].get("gpus", []):
        check_keys(gpu, "SampleGpu")
    for job in attrs.get("jobs", []):
        check_keys(job, "SampleJob")
        for process in job["processes"]:
            check_keys(process, "SampleProcess")
            for gpu in process.get("gpus", []):
                check_keys(gpu, "SampleProcessGpu")


def test_envelope_and_topics():
    msg = sonar.envelope("sample", {"x": 1})
    assert msg == {
        "meta": {"producer": "sonar", "version": "0.18.4"},
        "data": {"type": "sample", "attributes": {"x": 1}},
    }
    assert [sonar.topic(CLUSTER, k) for k in sonar.TOPIC_KINDS] == [
        "hpc-demo.sample",
        "hpc-demo.sysinfo",
        "hpc-demo.job",
        "hpc-demo.cluster",
    ]


def test_sample_shape_gpu_job():
    at = START + timedelta(hours=2)
    msg = sonar.sample_message(CLUSTER, "node039", at, [gpu_run()])
    check_sample(msg)
    attrs = msg["data"]["attributes"]
    assert attrs["time"] == "2026-10-12T23:37:37+00:00"
    assert len(attrs["system"]["gpus"]) == 4
    assert len(attrs["system"]["cpus"]) == 64
    (job,) = attrs["jobs"]
    assert job["job"] == 86
    assert job["user"] == "alice"
    assert job["epoch"] == 0
    cmds = [p["cmd"] for p in job["processes"]]
    assert cmds == ["slurm_script", "srun", "gmx_mpi"]
    script, srun, app = job["processes"]
    assert srun["ppid"] == script["pid"]
    assert app["ppid"] == srun["pid"]
    assert [g["index"] for g in app["gpus"]] == [0, 1, 2, 3]
    card_uuids = {g["uuid"] for g in attrs["system"]["gpus"]}
    assert {g["uuid"] for g in app["gpus"]} == card_uuids


def test_second_node_runs_only_the_application():
    msg = sonar.sample_message(CLUSTER, "node040", START + timedelta(hours=1), [gpu_run()])
    (job,) = msg["data"]["attributes"]["jobs"]
    assert [p["cmd"] for p in job["processes"]] == ["gmx_mpi"]


def test_cpu_job_has_no_gpu_entries_and_cpu_node_no_cards():
    msg = sonar.sample_message(CLUSTER, "node017", START + timedelta(hours=1), [cpu_run()])
    check_sample(msg)
    attrs = msg["data"]["attributes"]
    assert "gpus" not in attrs["system"]
    assert all("gpus" not in p for p in attrs["jobs"][0]["processes"])
    assert attrs["jobs"][0]["processes"][-1]["cmd"] == "wrf.exe"


def test_idle_gpu_node_reports_idle_cards():
    msg = sonar.sample_message(CLUSTER, "node038", START, [])
    attrs = msg["data"]["attributes"]
    assert "jobs" not in attrs
    for card in attrs["system"]["gpus"]:
        assert card["ce_util"] == 0
        assert card["memory"] == 0
        assert 80 <= card["power"] <= 100


def test_determinism():
    at = START + timedelta(minutes=37)
    first = sonar.sample_message(CLUSTER, "node039", at, [gpu_run()])
    second = sonar.sample_message(CLUSTER, "node039", at, [gpu_run()])
    assert first == second
    assert sonar.gpu_uuid(CLUSTER, "node039", 2) == sonar.gpu_uuid(CLUSTER, "node039", 2)
    assert sonar.gpu_uuid(CLUSTER, "node039", 2) != sonar.gpu_uuid(CLUSTER, "node040", 2)


def _steady_samples(run: sonar.JobRun, node: str) -> list[dict]:
    times = [t for t in sonar.sample_times(run, END) if t >= run.start_time + timedelta(minutes=15)]
    return [sonar.sample_attributes(CLUSTER, node, t, [run]) for t in times]


def test_means_agree_with_accounting_model():
    run = gpu_run()
    samples = _steady_samples(run, "node039")
    assert len(samples) > 100
    cpu_eff, mem_frac = cluster_model.synthetic_efficiency(86)
    gpu_util, gpu_mem_frac = cluster_model.synthetic_gpu_use(86)

    apps = [s["jobs"][0]["processes"][-1] for s in samples]
    assert mean(a["cpu_util"] for a in apps) == pytest.approx(64 * cpu_eff * 100, rel=0.03)
    assert apps[0]["cpu_avg"] == pytest.approx(64 * cpu_eff * 100, rel=0.01)

    rss = [sum(p["resident_memory"] for p in s["jobs"][0]["processes"]) for s in samples]
    peak_kib = mem_frac * 512 * 1024 * 1024
    assert max(rss) <= peak_kib
    assert mean(rss) == pytest.approx(peak_kib, rel=0.05)

    for index in range(4):
        utils = [a["gpus"][index]["gpu_util"] for a in apps]
        assert mean(utils) == pytest.approx(gpu_util * 100, abs=8)
        assert max(a["gpus"][index]["gpu_memory"] for a in apps) <= gpu_mem_frac * sonar.GPU_MEM_KIB
    all_cards = [a["gpus"][i]["gpu_util"] for a in apps for i in range(4)]
    assert mean(all_cards) == pytest.approx(gpu_util * 100, abs=2)
    # cards differ from each other and wobble over time
    assert len({a["gpus"][0]["gpu_util"] for a in apps}) > 20


def test_ramp_up_at_start():
    run = gpu_run()
    early = sonar.sample_attributes(CLUSTER, "node039", START + timedelta(seconds=30), [run])
    late = sonar.sample_attributes(CLUSTER, "node039", START + timedelta(hours=3), [run])
    early_app = early["jobs"][0]["processes"][-1]
    late_app = late["jobs"][0]["processes"][-1]
    assert early_app["cpu_util"] < late_app["cpu_util"] / 4
    assert early_app["gpus"][0]["gpu_memory"] < late_app["gpus"][0]["gpu_memory"] / 4


def test_power_follows_utilisation():
    run = gpu_run()
    attrs = sonar.sample_attributes(CLUSTER, "node039", START + timedelta(hours=3), [run])
    for card in attrs["system"]["gpus"]:
        expected = 90 + card["ce_util"] / 100 * (700 - 90)
        assert card["power"] == pytest.approx(expected, rel=0.06)
        assert card["power_limit"] == 700


def test_job_outside_its_lifetime_is_not_sampled():
    msg = sonar.sample_message(CLUSTER, "node039", END + timedelta(minutes=1), [gpu_run()])
    assert "jobs" not in msg["data"]["attributes"]


def test_sysinfo_shape():
    msg = sonar.sysinfo_message(CLUSTER, "node037", START)
    attrs = msg["data"]["attributes"]
    assert msg["data"]["type"] == "sysinfo"
    check_keys(attrs, "SysinfoAttributes")
    assert attrs["memory"] == 512 * 1024 * 1024
    assert attrs["sockets"] * attrs["cores_per_socket"] * attrs["threads_per_core"] == 64
    assert len(attrs["cards"]) == 4
    for i, card in enumerate(attrs["cards"]):
        check_keys(card, "SysinfoGpuCard")
        assert card["model"] == "NVIDIA GH200 480GB"
        assert card["architecture"] == "Hopper"
        assert card["memory"] == 96 * 1024 * 1024
        assert card["uuid"] == sonar.gpu_uuid(CLUSTER, "node037", i)
    assert "cards" not in sonar.sysinfo_message(CLUSTER, "node005", START)["data"]["attributes"]


def test_job_record():
    msg = sonar.job_message(CLUSTER, END, [gpu_run()])
    assert msg["data"]["type"] == "job"
    attrs = msg["data"]["attributes"]
    check_keys(attrs, "JobsAttributes")
    (record,) = attrs["slurm_jobs"]
    check_keys(record, "SlurmJob")
    assert record["nodes"] == ["node[039-040]"]
    assert record["job_step"] == ""
    assert record["requested_memory_per_node"] == 512 * 1024 * 1024
    assert record["minimum_cpus_per_node"] == 64
    assert record["allocated_resources"] == "billing=128,cpu=128,gres/gpu=8,mem=1024G,node=2"
    assert record["start_time"] == "2026-10-12T21:37:37+00:00"
    assert record["end_time"] == "2026-10-13T11:37:37+00:00"
    assert "gres/gpu" not in sonar.slurm_job_record(cpu_run())["allocated_resources"]
    running = sonar.slurm_job_record(gpu_run(end_time=None, state="RUNNING"))
    assert "end_time" not in running
    assert "exit_code" not in running


def test_cluster_message():
    states = {n: ["IDLE"] for n in cluster_model.all_nodes()}
    states["node012"] = ["IDLE", "DRAIN"]
    msg = sonar.cluster_message(CLUSTER, START, states)
    attrs = msg["data"]["attributes"]
    check_keys(attrs, "ClusterAttributes")
    assert {p["name"]: p["nodes"] for p in attrs["partitions"]} == {
        "debug": ["node[001-004]"],
        "batch": ["node[005-036]"],
        "gpu": ["node[037-040]"],
    }
    for group in attrs["nodes"]:
        check_keys(group, "ClusterNodes")
    assert {"names": ["node012"], "states": ["IDLE", "DRAIN"]} in attrs["nodes"]


def test_application_command_mapping():
    assert sonar.application_command("gromacs-md") == "gmx_mpi"
    assert sonar.application_command("md-equil") == "gmx_mpi"
    assert sonar.application_command("wrf-post") == "wrf.exe"
    assert sonar.application_command("multiqc-run") == "multiqc"
    assert sonar.application_command("qc-reads") == "multiqc"
    assert sonar.application_command("train") == "python3"


def test_short_job_still_gets_a_curve():
    run = gpu_run(end_time=START + timedelta(seconds=40))
    times = sonar.sample_times(run, END)
    assert len(times) >= 6
    assert all(run.active_at(t) for t in times)
    long_times = sonar.sample_times(gpu_run(), END)
    assert (long_times[1] - long_times[0]) == timedelta(minutes=5)


def test_run_from_record():
    record = UsageRecord(
        account="alloc_climate",
        user="alice",
        node_hours=28.0,
        billing_units=28.0,
        timestamp=END.replace(tzinfo=None),
        period="2026-Q4",
        partition="gpu",
        job_id=86,
        job_name="gromacs-md",
        start_time=START.replace(tzinfo=None),
        end_time=END.replace(tzinfo=None),
        node_list="node[039-040]",
        alloc_cpus=128,
        req_mem_mb=524288,
    )
    run = sonar.run_from_record(record)
    assert run is not None
    assert run.nodes == ("node039", "node040")
    assert run.cpus_on("node040") == 64
    assert run.gpus_per_node == 4
    legacy = UsageRecord("a", "u", 1.0, 1.0, END, "2026-Q4", job_id=1)
    assert sonar.run_from_record(legacy) is None


# --- publisher ---


def _database(now: datetime) -> SlurmDatabase:
    db = SlurmDatabase()
    local = now.astimezone().replace(tzinfo=None)
    db.usage_records.append(
        UsageRecord(
            account="alloc_climate",
            user="alice",
            node_hours=2.0,
            billing_units=2.0,
            timestamp=local - timedelta(hours=1),
            period="2026-Q4",
            partition="gpu",
            job_id=86,
            job_name="gromacs-md",
            start_time=local - timedelta(hours=3),
            end_time=local - timedelta(hours=1),
            node_list="node037",
            alloc_cpus=64,
            req_mem_mb=524288,
        )
    )
    db.jobs["120"] = Job(
        job_id="120",
        account="alloc_climate",
        user="bob",
        state="RUNNING",
        partition="batch",
        name="qc-reads",
        start_time=local - timedelta(minutes=10),
        node_list=["node010"],
        alloc_cpus=16,
        time_limit=60,
    )
    return db


def _publish(db: SlurmDatabase, state: sonar_cli.PublisherState, now: datetime) -> list[tuple]:
    out = io.StringIO()
    sonar_cli.tick(sonar_cli.StreamSink(out), state, db, CLUSTER, now, 7, 300)
    lines = [line.split("\t", 1) for line in out.getvalue().splitlines()]
    return [(t, json.loads(m)) for t, m in lines]


def test_publisher_backfill_then_live(monkeypatch):
    monkeypatch.setenv(cluster_model.NODE_STATES_ENV, "node030=DOWN:Not responding")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    db = _database(now)
    state = sonar_cli.PublisherState()

    first = _publish(db, state, now)
    topics = [t for t, _ in first]
    assert topics.count("hpc-demo.sysinfo") == (7 * 4 + 1) * 40  # every 6 h over 7 days + now
    assert topics.count("hpc-demo.cluster") == 1
    backfill = [
        m
        for t, m in first
        if t == "hpc-demo.sample"
        and any(j["job"] == 86 for j in m["data"]["attributes"].get("jobs", []))
    ]
    assert len(backfill) == 24  # 2 h every 5 min
    job_records = [m for t, m in first if t == "hpc-demo.job"]
    assert [
        r["slurm_jobs"][0]["job_state"] for r in (m["data"]["attributes"] for m in job_records)
    ] == [
        "COMPLETED",
        "RUNNING",
    ]
    live = [m["data"]["attributes"] for t, m in first if t == "hpc-demo.sample"][-39:]
    assert {a["time"] for a in live} == {sonar.iso(now)}
    assert "node030" not in {a["node"] for a in live}  # DOWN: probe silent
    node010 = next(a for a in live if a["node"] == "node010")
    assert node010["jobs"][0]["job"] == 120
    assert node010["jobs"][0]["processes"][-1]["cmd"] == "multiqc"
    assert 86 in state.emitted

    second = _publish(db, state, now + timedelta(minutes=1))
    topics = [t for t, _ in second]
    assert "hpc-demo.sysinfo" not in topics
    assert topics.count("hpc-demo.sample") == 39  # live only, no second backfill


def test_publisher_state_roundtrip(tmp_path):
    state = sonar_cli.PublisherState(emitted={1, 2}, live={"3": ["a", "b"]}, sysinfo_at="x")
    path = tmp_path / "s" / "sonar-emulator.json"
    state.save(path)
    loaded = sonar_cli.PublisherState.load(path)
    assert loaded == state
    assert sonar_cli.PublisherState.load(tmp_path / "missing.json") == sonar_cli.PublisherState()


def test_virtual_memory_stays_within_the_request():
    run = cpu_run(
        alloc_cpus=32,
        mem_per_node_mb=125 * 1024,
        environment={"SLURM_EMULATOR_MEM_FRACTION": "0.98"},
    )
    for at in sonar.sample_times(run, END):
        attrs = sonar.sample_attributes(CLUSTER, "node017", at, [run])
        total = sum(p["virtual_memory"] for p in attrs["jobs"][0]["processes"])
        assert total <= 125 * 1024 * 1024


def test_running_job_is_seen_without_anyone_polling(state_env, monkeypatch):
    """A probe sees a job run although no client has read it from the controller."""
    from emulator.core.time_engine import TimeEngine  # noqa: PLC0415

    monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DELAY", "60")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DURATION", "3600")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_EXECUTE", "1")
    start = datetime(2026, 10, 18, 9, 0, 0)
    TimeEngine().set_time(start)
    db = SlurmDatabase()
    db.add_job(
        Job(
            job_id="7",
            account="a",
            user="alice",
            state="PENDING",
            partition="batch",
            submit_time=start,
            time_limit=120,
        )
    )
    db.save_state()
    TimeEngine().set_time(start + timedelta(minutes=10))

    database = SlurmDatabase()
    database.load_state()
    assert database.jobs["7"].state == "PENDING"  # nothing advanced the file
    sonar_cli._catch_up_running(database)
    assert database.jobs["7"].state == "RUNNING"
    stored = SlurmDatabase()
    stored.load_state()
    assert stored.jobs["7"].state == "PENDING"  # the probe never writes
    assert database.usage_records == []
    import os  # noqa: PLC0415

    assert os.environ["SLURM_EMULATOR_JOB_EXECUTE"] == "1"
