"""The slurmrestd plane speaks the dialect of the Slurm version it is launched as.

``SLURM_EMULATOR_SLURM_VERSION`` selects a tracked release; the REST plane then
serves that release's data_parser version under ``/slurm/<api>/`` and
``/slurmdb/<api>/``, reports its release in ``meta.slurm``, and shapes the few
tables that changed across the window (docs/slurm-parity.md):

- URL prefix / ``meta.plugin.data_parser``: META ``API_CURRENT`` per branch.
- ``CONTROLLER_PING``: deprecated ``pinged``/``mode`` through v0.0.44
  (slurm://src/plugins/data_parser/v0.0.44/parsers.c#CONTROLLER_PING@25.11+),
  ``status`` from v0.0.45
  (slurm://src/plugins/data_parser/v0.0.45/parsers.c#CONTROLLER_PING@26.05+).
- ``SLURMDBD_PING``: ``status`` from v0.0.45 only
  (slurm://src/plugins/data_parser/v0.0.45/parsers.c#SLURMDBD_PING@26.05+).
- ``PARTITION_INFO``: ``defaults/memory_per_cpu`` and ``maximums/memory_per_cpu``
  exist through v0.0.45 and are gone on master
  (slurm://src/plugins/data_parser/v0.0.45/parsers.c#"maximums/memory_per_cpu"@26.05).

These tests set the target explicitly per case, so they exercise every
dialect regardless of which version the surrounding suite runs as.
"""

import pytest
from fastapi.testclient import TestClient

from emulator import slurm_version
from emulator.api.slurmrestd.app import create_app
from emulator.slurm_version import RELEASES

VERSIONS = sorted(RELEASES, key=slurm_version._key)


@pytest.fixture(params=VERSIONS)
def target(request, state_env, monkeypatch):  # noqa: ARG001 - state_env isolates state files
    monkeypatch.setenv(slurm_version.ENV_VAR, request.param)
    return RELEASES[request.param]


@pytest.fixture
def client(target):  # noqa: ARG001 - target must be set before the app handles requests
    return TestClient(create_app())


def test_release_table_is_consistent():
    for version, rel in RELEASES.items():
        assert rel.version == version
        assert rel.api_version.startswith("v0.0.")
        major, minor, _ = rel.release.split(".")
        expected = ("26", "11") if version == "master" else tuple(version.split("."))
        assert (major, minor) == expected
    # API versions increase monotonically with the release order.
    apis = [int(RELEASES[v].api_version.rsplit(".", 1)[1]) for v in VERSIONS]
    assert apis == sorted(apis) and len(set(apis)) == len(apis)


def test_unknown_target_version_is_rejected(monkeypatch):
    monkeypatch.setenv(slurm_version.ENV_VAR, "23.11")
    with pytest.raises(ValueError, match="not a tracked Slurm version"):
        slurm_version.current()


def test_url_prefix_and_meta_follow_target(client, target, auth_headers):
    ok = client.get(f"/slurmdb/{target.api_version}/ping/", headers=auth_headers)
    assert ok.status_code == 200
    meta = ok.json()["meta"]
    assert meta["plugin"]["data_parser"] == f"data_parser/{target.api_version}"
    assert meta["slurm"]["release"] == target.release
    assert meta["slurm"]["version"] == target.version_parts

    other = client.get(f"/slurmdb/{target.previous_api_version}/ping/", headers=auth_headers)
    assert other.status_code == 404
    assert "Unable to find requested URL endpoint" in other.text


def test_openapi_spec_advertises_target_version(client, target, auth_headers):
    spec = client.get("/openapi/v3", headers=auth_headers).json()
    assert spec["info"]["version"] == target.api_version
    assert f"/slurmdb/{target.api_version}/accounts/" in spec["paths"]
    assert not any("v0.0." in p and target.api_version not in p for p in spec["paths"])


def test_conf_reports_target_release(client, target, auth_headers):
    config = client.get(f"/slurm/{target.api_version}/conf", headers=auth_headers).json()["config"]
    assert config["slurm_version"] == target.release


def test_controller_ping_dialect(client, target, auth_headers):
    ping = client.get(f"/slurm/{target.api_version}/ping/", headers=auth_headers).json()["pings"][0]
    assert ping["hostname"] == "localhost"
    assert ping["responding"] is True
    assert ping["primary"] == "primary"
    if slurm_version.at_least("26.05"):
        assert set(ping) == {"hostname", "responding", "latency", "primary", "status"}
        assert ping["status"] == "No error"
    else:
        assert set(ping) == {"hostname", "pinged", "responding", "latency", "mode", "primary"}
        assert ping["pinged"] == "UP"
        assert ping["mode"] == "primary"


def test_slurmdbd_ping_dialect(client, target, auth_headers):
    ping = client.get(f"/slurmdb/{target.api_version}/ping/", headers=auth_headers).json()["pings"][
        0
    ]
    expected = {"hostname", "responding", "latency", "primary"}
    if slurm_version.at_least("26.05"):
        expected.add("status")
    assert set(ping) == expected


def test_partition_memory_limits_dialect(client, target, auth_headers):
    part = client.get(f"/slurm/{target.api_version}/partitions/", headers=auth_headers).json()[
        "partitions"
    ][0]
    has_mem = "memory_per_cpu" in part["defaults"]
    assert has_mem == ("memory_per_cpu" in part["maximums"])
    assert has_mem == (not slurm_version.at_least("master"))
