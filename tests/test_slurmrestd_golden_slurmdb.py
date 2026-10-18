"""slurmdb requests the waldur-site-agent sends, replayed against what a real slurmrestd 26.05 answered.

``tests/data/slurmrestd_26.05_site_agent_slurmdb.json`` holds requests and the
responses of slurmrestd built from SchedMD's 26.05 branch with slurmctld and
slurmdbd running (see its ``source``), recorded by
``scripts/real_slurmrestd/record.py``. Cases run in order on one state, as
they did on the real cluster. For each case the emulator must give the same
status, errors and warnings, the same payload keys, and the same values at
the case's ``pick`` paths.

slurm://src/slurmrestd/plugins/openapi/slurmdbd/associations.c#_diff_assoc@26.05+,
slurm://src/plugins/data_parser/v0.0.45/parsers.c#OPENAPI_USERS_ADD_COND_RESP@26.05+,
slurm://src/plugins/serializer/url-encoded/serializer_url_encoded.c#serialize_p_string_to_data,
slurm://src/slurmrestd/operations.c#SLURM_NO_CHANGE_IN_DATA
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from emulator.api.slurmrestd.app import create_app
from emulator.slurm_version import at_least

ROOT = Path(__file__).parent.parent
GOLDEN = json.loads((ROOT / "tests/data/slurmrestd_26.05_site_agent_slurmdb.json").read_text())

_spec = importlib.util.spec_from_file_location("record", ROOT / "scripts/real_slurmrestd/record.py")
record = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(record)  # type: ignore[union-attr]

pytestmark = pytest.mark.skipif(
    not at_least("26.05") or at_least("master"), reason="recorded on slurmrestd 26.05"
)


@pytest.fixture
def demo(state_env, monkeypatch):  # noqa: ARG001 - fixture sets the state env vars
    monkeypatch.setenv("SLURM_EMULATOR_CLUSTER_NAME", "demo")
    monkeypatch.setenv("SLURM_EMULATOR_PARTITIONS", "debug:1-2,gpu:3-4")
    return TestClient(create_app())


def _send(client, case):
    body = case.get("body")
    return client.request(
        case.get("method", "GET"),
        case["path"],
        headers={"X-SLURM-USER-TOKEN": "t", "Content-Type": "application/json"},
        content=json.dumps(body) if body is not None else None,
    )


def test_site_agent_requests_match_real_slurmrestd(demo):
    mismatches = []
    for case in GOLDEN["cases"]:
        resp = _send(demo, case)
        got = record.outcome(resp.status_code, resp.text, case.get("pick", []))
        if got != case["expect"]:
            mismatches.append({"case": case["name"], "real": case["expect"], "emulator": got})
    assert mismatches == [], json.dumps(mismatches, indent=1)
