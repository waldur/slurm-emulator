"""GPU and CPU allocation against a real Slurm 25.11.7 run (``tests/data/slurm_25.11.7_gres.json``).

Recorded with ``scripts/agent_parity/usage/gres.sh`` and ``record_gres.py`` on
the accounting cluster with ``scripts/agent_parity/usage/cluster-gres/``:
node001-002 carry four untyped GPUs, node003-004 four ``h200`` with two
threads per core and ``AccountingStorageTRES=gres/gpu,gres/gpu:h200``. Seven
jobs ask for GPUs by ``--gres`` (typed and untyped) and ``--gpus``, for none
on a GPU node, for a whole node with ``--exclusive``, and for plain CPUs. The
emulator, configured the same way, must allocate the same CPUs, memory, GPUs
and device indices and report the same TRES everywhere
(slurm://src/stepmgr/gres_stepmgr.c#_set_type_tres_cnt,
slurm://src/common/slurmdb_defs.c#slurmdb_make_tres_string_from_simple).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from emulator.api.slurmrestd import schemas
from emulator.commands.sacct import SacctEmulator
from emulator.commands.scontrol import ScontrolEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine
from tests.test_cluster_model import V, set_time, submit

GOLDEN = json.loads((Path(__file__).parent / "data" / "slurm_25.11.7_gres.json").read_text())
T0 = datetime(2026, 10, 20, 9, 0, 0)
# The jobs of gres.sh, in submit order (real job ids 1..7), as job_desc fields.
JOBS = [
    (
        "untyped_gpu",
        {"partition": "gpu", "tasks": 1, "cpus_per_task": 1, "tres_per_node": "gres/gpu:1"},
    ),
    (
        "typed_gpu",
        {"partition": "gpu", "tasks": 1, "cpus_per_task": 3, "tres_per_node": "gres/gpu:h200:2"},
    ),
    ("no_gres_gpu_part", {"partition": "gpu", "tasks": 2, "memory_per_node": 2048}),
    ("untyped_node", {"partition": "debug", "tasks": 1, "tres_per_node": "gres/gpu:2"}),
    ("exclusive", {"partition": "gpu", "tasks": 1, "shared": ["none"]}),
    ("debug_cpus", {"partition": "debug", "tasks": 4}),
    ("gpus_opt", {"partition": "gpu", "tasks": 1, "tres_per_job": "gres/gpu:1"}),
]


@pytest.fixture
def jobs(restd, auth_headers, state_env, monkeypatch):
    """The seven jobs submitted on the emulator, running."""
    monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DELAY", "1")
    monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "none")
    monkeypatch.setenv("SLURM_EMULATOR_GPU_TYPES", "debug,gpu=h200")
    monkeypatch.setenv("SLURM_EMULATOR_THREADS_PER_CORE", "gpu=2")
    for name in ("SLURM_EMULATOR_JOB_EXECUTE", "SLURM_EMULATOR_TIME_FOLLOW_WALL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(schemas, "PARTITION_RANGES", {"debug": (1, 2), "gpu": (3, 4)})
    set_time(T0)
    ids = {}
    for name, fields in JOBS:
        ids[name] = submit(
            restd,
            auth_headers,
            name=name,
            account="acc1",
            time_limit=10,
            environment=["SLURM_EMULATOR_RUN_SECONDS=600"],
            **fields,
        )
    set_time(T0 + timedelta(seconds=4))
    return {str(i + 1): ids[name] for i, (name, _f) in enumerate(JOBS)}


def test_sacct_tres_and_cpus_match_real_slurm(jobs):
    fields = "JobID,JobName,ReqCPUS,AllocCPUS,ReqTRES,AllocTRES"
    db = SlurmDatabase()
    db.load_state()
    out = SacctEmulator(db, TimeEngine()).handle_command(
        ["-X", "-P", "-n", "--noconvert", f"--format={fields}"]
    )
    real_id = {str(v): k for k, v in jobs.items()}
    rows = [dict(zip(fields.split(","), line.split("|"))) for line in out.splitlines()]
    for row in rows:
        row["JobID"] = real_id[row["JobID"]]
    assert rows == GOLDEN["sacct"]


@pytest.mark.parametrize("real_id", [str(i) for i in range(1, 8)])
def test_scontrol_show_job_matches_real_slurm(jobs, real_id):
    out = ScontrolEmulator(SlurmDatabase(), TimeEngine()).handle_command(
        ["show", "job", str(jobs[real_id])]
    )
    fields = dict(w.split("=", 1) for w in out.split() if "=" in w)
    got = {k: fields.get(k, "") for k in GOLDEN["show_job"][real_id]}
    assert got == GOLDEN["show_job"][real_id]


def test_slurmdb_tres_match_real_slurm(jobs, restd, auth_headers):
    body = restd.get(
        f"/slurmdb/{V}/jobs/?account=acc1&skip_steps=true", headers=auth_headers
    ).json()
    real_id = {v: k for k, v in jobs.items()}
    got = {
        real_id[job["job_id"]]: {
            kind: [
                [t["type"], t["name"], t["id"], t["count"]]
                for t in job["tres"][kind]
                if t["type"] != "energy"
            ]
            for kind in ("requested", "allocated")
        }
        for job in body["jobs"]
    }
    assert got == GOLDEN["slurmdb_tres"]


def test_slurmctld_jobs_match_real_slurm(jobs, restd, auth_headers):
    body = restd.get(f"/slurm/{V}/jobs/", headers=auth_headers).json()
    real_id = {v: k for k, v in jobs.items()}
    got = {
        real_id[job["job_id"]]: {
            "nodes": job["nodes"],
            "cpus": job["cpus"]["number"],
            "tres_per_node": job["tres_per_node"],
            "tres_per_job": job["tres_per_job"],
            "tres_req_str": job["tres_req_str"],
            "tres_alloc_str": job["tres_alloc_str"],
            "gres_detail": job["gres_detail"],
        }
        for job in body["jobs"]
    }
    assert got == GOLDEN["ctld"]


def test_nodes_match_real_slurm(jobs, restd, auth_headers):
    body = restd.get(f"/slurm/{V}/nodes/", headers=auth_headers).json()
    keys = ("gres", "gres_used", "tres", "tres_used", "threads")
    got = {n["name"]: {k: n[k] for k in keys} for n in body["nodes"]}
    assert got == GOLDEN["nodes"]


def test_tres_ids_match_real_slurm(jobs, restd, auth_headers):
    body = restd.get(f"/slurmdb/{V}/tres/", headers=auth_headers).json()
    got = [[t["type"], t["name"], t["id"]] for t in body["TRES"] if t["type"] != "dynamic_offset"]
    assert got == GOLDEN["tres"]


def test_sbatch_gpu_options_become_job_desc_tres():
    """sbatch prefixes ``--gres`` entries with ``gres/`` and turns ``--gpus`` into
    tres_per_job (slurm://src/common/slurm_opt.c#arg_set_gres,
    slurm://src/common/slurm_opt.c#slurm_opt_create_job_desc)."""
    from emulator.api.ssh.server import _sbatch_gpus  # noqa: PLC0415

    assert _sbatch_gpus(["--gres=gpu:h200:2", "job.sh"]) == {"tres_per_node": "gres/gpu:h200:2"}
    assert _sbatch_gpus(["-G", "1"]) == {"tres_per_job": "gres/gpu:1"}
    assert _sbatch_gpus(["--gpus=h200:1"]) == {"tres_per_job": "gres/gpu:h200:1"}
    assert _sbatch_gpus(["-p", "gpu"]) == {}


@pytest.fixture
def gpu_cluster(restd, auth_headers, state_env, monkeypatch):
    """The cluster-gres/ layout, on the simulated clock."""
    monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
    monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DELAY", "1")
    monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "none")
    monkeypatch.setenv("SLURM_EMULATOR_GPU_TYPES", "debug,gpu=h200")
    monkeypatch.setenv("SLURM_EMULATOR_THREADS_PER_CORE", "gpu=2")
    for name in ("SLURM_EMULATOR_JOB_EXECUTE", "SLURM_EMULATOR_TIME_FOLLOW_WALL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(schemas, "PARTITION_RANGES", {"debug": (1, 2), "gpu": (3, 4)})
    set_time(T0)

    def run(**fields):
        body = restd.post(
            f"/slurm/{V}/job/submit",
            headers=auth_headers,
            json={"job": {"account": "acc1", "time_limit": 60, **fields}},
        ).json()
        return body

    return run


def _ctld(restd, auth_headers, job_id):
    return restd.get(f"/slurm/{V}/job/{job_id}", headers=auth_headers).json()["jobs"][0]


def test_a_gres_without_count_is_one_gpu(gpu_cluster, restd, auth_headers):
    """``--gres=gpu`` and ``--gres=gpu:h200`` ask for one GPU: the 25.11.7 cluster
    recorded ReqTRES gres/gpu=1 and gres/gpu:h200=1,gres/gpu=1 for them
    (slurm://src/interfaces/gres.c#gres_job_state_validate)."""
    bare = gpu_cluster(name="bare", partition="gpu", tasks=1, tres_per_node="gres/gpu")["job_id"]
    typed = gpu_cluster(name="t", partition="gpu", tasks=1, tres_per_node="gres/gpu:h200")["job_id"]
    set_time(T0 + timedelta(seconds=4))
    assert _ctld(restd, auth_headers, bare)["tres_req_str"].endswith(",gres/gpu=1")
    assert _ctld(restd, auth_headers, typed)["tres_req_str"].endswith(",gres/gpu=1,gres/gpu:h200=1")


def test_more_gpus_than_a_node_has_waits_like_real_slurm(gpu_cluster, restd, auth_headers):
    """``-n1 --gpus=8`` on 4-GPU nodes: the 25.11.7 cluster accepted it and kept it
    PENDING with Resources, and the job behind it waited with Priority."""
    big = gpu_cluster(name="big", partition="gpu", tasks=1, tres_per_job="gres/gpu:8")
    behind = gpu_cluster(name="b", partition="gpu", tasks=1, tres_per_node="gres/gpu:1")
    assert big["errors"] == [] and behind["errors"] == []
    set_time(T0 + timedelta(seconds=4))
    job = _ctld(restd, auth_headers, big["job_id"])
    assert (job["job_state"], job["state_reason"]) == (["PENDING"], "Resources")
    assert _ctld(restd, auth_headers, behind["job_id"])["state_reason"] == "Priority"


@pytest.mark.parametrize(
    "fields",
    [
        {"partition": "gpu", "tasks": 1, "tres_per_node": "gres/gpu:5"},
        {"partition": "debug", "tasks": 1, "tres_per_node": "gres/gpu:h200:1"},
    ],
)
def test_impossible_gpu_requests_are_refused_at_submit(gpu_cluster, fields):
    """More GPUs per node than a node has, or a type the partition lacks: the
    25.11.7 cluster answered "Requested node configuration is not available"
    (slurm://src/slurmctld/job_mgr.c#job_allocate)."""
    body = gpu_cluster(name="x", **fields)
    assert body["errors"][0]["error_number"] == 2014


def test_a_gpu_job_needs_as_many_gpu_nodes_as_it_asks_for(gpu_cluster, monkeypatch):
    """Two h200 nodes cannot hold a 3-node h200 job, as two nodes cannot hold a
    3-node partition request."""
    monkeypatch.setattr(schemas, "PARTITION_RANGES", {"mixed": (1, 4), "gpu": (3, 4)})
    body = gpu_cluster(name="x", partition="mixed", nodes="3", tres_per_node="gres/gpu:h200:1")
    assert body["errors"][0]["error_number"] == 2014


def test_hardware_belongs_to_the_node_not_the_first_partition(gpu_cluster, monkeypatch):
    """A node in a CPU and a GPU partition keeps its GPUs, type and threads."""
    from emulator.core import cluster_model  # noqa: PLC0415

    monkeypatch.setattr(schemas, "PARTITION_RANGES", {"all": (1, 4), "gpu": (3, 4)})
    assert cluster_model.node_hardware("node003") == cluster_model.NodeHardware(4, "h200", 2)
    assert cluster_model.node_hardware("node001") == cluster_model.NodeHardware(0, "", 1)


def test_a_short_run_still_counts_its_cpu_and_gpu_hours(gpu_cluster, restd, auth_headers):
    """A 45-minute run with 2 CPUs and 1 GPU is 1.5 CPU-hours and 0.75 GPU-hours."""
    job_id = gpu_cluster(
        name="short",
        partition="gpu",
        tasks=1,
        tres_per_node="gres/gpu:1",
        environment=["SLURM_EMULATOR_RUN_SECONDS=2700"],
    )["job_id"]
    set_time(T0 + timedelta(hours=1))
    restd.get(f"/slurm/{V}/jobs/", headers=auth_headers)
    db = SlurmDatabase()
    db.load_state()
    (record,) = [r for r in db.usage_records if r.job_id == job_id]
    assert record.raw_tres["CPU"] == pytest.approx(1.5)
    assert record.raw_tres["GRES/gpu"] == pytest.approx(0.75)


def test_sshare_sums_fractional_hours_before_rounding():
    from emulator.commands.sshare import _aggregate_raw_tres_minutes  # noqa: PLC0415
    from emulator.core.database import UsageRecord  # noqa: PLC0415

    records = [
        UsageRecord(
            account="a",
            user="u",
            node_hours=0.0,
            billing_units=0.0,
            timestamp=T0,
            period="",
            raw_tres={"CPU": 0.5},
        )
        for _ in range(3)
    ]
    assert _aggregate_raw_tres_minutes(records)["cpu"] == 90


def test_requeue_frees_the_gpu_devices(gpu_cluster, restd, auth_headers):
    job_id = gpu_cluster(name="r", partition="gpu", tasks=1, tres_per_node="gres/gpu:2")["job_id"]
    set_time(T0 + timedelta(seconds=4))
    assert _ctld(restd, auth_headers, job_id)["gres_detail"] == ["gpu:h200:2(IDX:0-1)"]
    emulator = ScontrolEmulator(SlurmDatabase(), TimeEngine())
    emulator.handle_command(["requeue", str(job_id)])
    db = SlurmDatabase()
    db.load_state()
    assert db.jobs[str(job_id)].gpu_indices == {}


def test_sonar_cards_are_the_jobs_devices():
    """Telemetry samples the devices slurmctld gave the job (gres_detail IDX)."""
    from emulator.telemetry.sonar import JobRun  # noqa: PLC0415

    run = JobRun(
        job_id=1,
        user="u",
        account="a",
        partition="gpu",
        job_name="j",
        state="RUNNING",
        nodes=("node003", "node004"),
        alloc_cpus=4,
        mem_per_node_mb=1024,
        start_time=T0,
        gpu_indices={"node003": (2, 3), "node004": (0,)},
    )
    assert run.gpu_cards("node003") == [2, 3]
    assert run.gpu_total == 3


def test_an_empty_sbatch_argument_is_not_a_gres():
    from emulator.api.ssh.server import _sbatch_gpus  # noqa: PLC0415

    assert _sbatch_gpus(["--comment", "", "-p", "gpu", "job.sh"]) == {}
    assert _sbatch_gpus(["--gres", "gpu:1", "job.sh"]) == {"tres_per_node": "gres/gpu:1"}
