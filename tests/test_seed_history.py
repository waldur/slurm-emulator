"""The Waldur example's job-history seeding (examples/firecrest/waldur/seed_history.py).

Seeded records carry what slurmdbd stores per job
(slurm://slurm/slurmdb.h#slurmdb_job_rec_t), so the accounting views render
them like jobs the scheduler finished.
"""

from __future__ import annotations

import importlib.util
import statistics
from pathlib import Path

import pytest

from emulator.api.slurmrestd import schemas
from emulator.core.database import SlurmDatabase

SCRIPT = Path(__file__).resolve().parent.parent / "examples/firecrest/waldur/seed_history.py"


@pytest.fixture
def seed(state_env, monkeypatch):
    monkeypatch.setenv("SLURM_EMULATOR_FS_ROOT", str(state_env / "fs"))
    monkeypatch.setenv("SLURM_EMULATOR_TIME_FOLLOW_WALL", "1")
    # The example's topology (SLURM_EMULATOR_PARTITIONS in its compose file).
    monkeypatch.setattr(
        schemas, "PARTITION_RANGES", {"debug": (1, 4), "batch": (5, 36), "gpu": (37, 40)}
    )
    spec = importlib.util.spec_from_file_location("seed_history", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_seeds_history_once(seed, state_env):
    assert seed.main() == 0
    db = SlurmDatabase()
    db.load_state()
    records = list(db.usage_records)
    assert 70 <= len(records) <= 95
    assert {r.account for r in records} == {"alloc_climate", "alloc_genomics"}
    assert {r.user for r in records if r.account == "alloc_genomics"} == {"alice"}
    states = {r.state for r in records}
    assert {"COMPLETED", "FAILED", "TIMEOUT", "CANCELLED"} <= states
    assert {r.exit_code for r in records if r.state == "FAILED"} <= {1, 127, 139}
    for r in records:
        assert r.submit_time <= r.start_time <= r.end_time
        assert 30 <= r.time_limit <= 2880
        if r.state == "TIMEOUT":
            assert (r.end_time - r.start_time).total_seconds() == r.time_limit * 60
        if r.node_count:
            assert 1 <= r.node_count <= 8 and r.cpu_time_seconds > 0 and r.max_rss_bytes > 0

    def median_wait(partition):
        return statistics.median(
            (r.start_time - r.submit_time).total_seconds()
            for r in records
            if r.partition == partition and r.node_count
        )

    assert median_wait("debug") < median_wait("batch") < median_wait("gpu")

    # Second run: nothing added.
    assert seed.main() == 0
    db.load_state()
    assert len(db.usage_records) == len(records)


def test_writes_sample_files(seed, state_env):
    seed.main()
    home = state_env / "fs" / "home" / "alice"
    rsl = (home / "wrf-baltic" / "rsl.error.0000").read_text()
    assert "Timing for main: time " in rsl and "on domain   1:" in rsl
    xvg = (home / "md" / "energy.xvg").read_text().splitlines()
    assert '@ s0 legend "Potential"' in xvg and '@ s1 legend "Temperature"' in xvg
    assert len([line for line in xvg if not line.startswith(("#", "@"))]) == 100
    pdb = (home / "md" / "protein.pdb").read_text()
    assert pdb.count("\nATOM  ") >= 24 and pdb.rstrip().endswith("END")
    assert "<table>" in (home / "qc" / "multiqc_report.html").read_text()
