"""Reservation writes replayed against what a real slurmrestd 26.05 answered.

``tests/data/slurmrestd_26.05_reservation_requests.json`` holds requests and
the responses of slurmrestd built from SchedMD's 26.05 branch, with
slurmctld running (see its ``source``). The emulator must give the same
status, errors and warnings, in the same order, and the same echo.
Cases run in order on one state, as they did on the real cluster.

slurm://src/slurmrestd/plugins/openapi/slurmctld/reservations.c#_mod_reservation@26.05+,
slurm://src/plugins/data_parser/v0.0.45/parsing.c#_parse_check_openapi@26.05+
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from emulator.slurm_version import at_least
from tests.test_cluster_model import set_time

GOLDEN = json.loads(
    (Path(__file__).parent / "data" / "slurmrestd_26.05_reservation_requests.json").read_text()
)

pytestmark = pytest.mark.skipif(
    not at_least("26.05") or at_least("master"), reason="recorded on slurmrestd 26.05"
)


@pytest.fixture
def cluster(state_env, monkeypatch):
    monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
    monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "none")
    monkeypatch.delenv("SLURM_EMULATOR_RESERVATIONS", raising=False)
    set_time(datetime(2026, 10, 18, 7, 8, 0))


def _post(restd, case):
    headers = {"X-SLURM-USER-TOKEN": "t", "Content-Type": "application/json"}
    if "raw" in case:
        content = case["raw"]
    elif case.get("body") is None:
        content = ""
    else:
        content = json.dumps(case["body"])
    return restd.post(case["path"], headers=headers, content=content)


def _outcome(resp) -> dict:
    try:
        data = resp.json()
    except ValueError:
        return {"status": resp.status_code, "text": resp.text}
    return {
        "status": resp.status_code,
        "errors": data.get("errors"),
        "warnings": data.get("warnings"),
        "payload_keys": sorted(k for k in data if k not in ("meta", "errors", "warnings")),
    }


def test_responses_match_real_slurmrestd(restd, cluster):
    mismatches = []
    for case in GOLDEN["cases"]:
        got = _outcome(_post(restd, case))
        if got != case["expect"]:
            mismatches.append({"case": case["name"], "real": case["expect"], "emulator": got})
    assert mismatches == [], json.dumps(mismatches, indent=1)


def test_echo_matches_real_slurmrestd(restd, cluster):
    resp = _post(
        restd,
        {"path": "/slurm/v0.0.45/reservation", "body": GOLDEN["echo"]["request"]},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["reservations"][0] == GOLDEN["echo"]["response"]
