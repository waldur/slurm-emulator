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
