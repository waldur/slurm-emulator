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


def test_node_and_reservation_shapes_in_every_dialect(client, target, auth_headers, monkeypatch):
    """NODE ``cpu_load``/``free_mem``/``alloc_idle_cpus``/``tres_used`` and the
    RESERVATION_INFO fields are the same from v0.0.42 to v0.0.45
    (slurm://src/plugins/data_parser/v0.0.42/parsers.c#RESERVATION_INFO@24.11,
    slurm://src/plugins/data_parser/v0.0.45/parsers.c#RESERVATION_INFO@26.05+;
    ``watts`` is already add_removed in v0.0.42)."""
    monkeypatch.setenv("SLURM_EMULATOR_RESERVATIONS", "maint:ALL:+1h:2h:MAINT")
    node = client.get(f"/slurm/{target.api_version}/nodes/", headers=auth_headers).json()["nodes"][
        0
    ]
    assert {"cpu_load", "free_mem", "alloc_idle_cpus", "tres_used", "reason"} <= set(node)
    assert set(node["free_mem"]) == {"set", "infinite", "number"}
    resv = client.get(f"/slurm/{target.api_version}/reservations/", headers=auth_headers).json()[
        "reservations"
    ][0]
    assert {"name", "node_list", "node_count", "partition", "flags", "users", "accounts"} <= set(
        resv
    )
    assert "watts" not in resv
    assert set(resv["start_time"]) == {"set", "infinite", "number"}


def test_reservation_writes_exist_from_25_05(client, target, auth_headers):
    """``POST /reservation``, ``POST /reservations/`` and ``DELETE /reservation/{name}``
    arrived with 25.05 (slurm://src/slurmrestd/plugins/openapi/slurmctld/api.c#"create or update a reservation"@25.05+);
    24.11 only serves the GETs
    (slurm://src/slurmrestd/plugins/openapi/slurmctld/api.c#"get reservation info"@24.11),
    so the bare path is an unknown URL and the others an unknown method."""
    base = f"/slurm/{target.api_version}"
    desc = {
        "name": "m",
        "start_time": "now",
        "duration": 60,
        "node_list": "node001",
        "flags": ["MAINT"],
        "users": "root",
    }
    created = client.post(f"{base}/reservation", headers=auth_headers, json=desc)
    bulk = client.post(
        f"{base}/reservations/",
        headers=auth_headers,
        json={"reservations": [{**desc, "name": "n", "node_list": "node002"}]},
    )
    deleted = client.delete(f"{base}/reservation/m", headers=auth_headers)
    if slurm_version.at_least("25.05"):
        assert created.status_code == 200 and created.json()["reservations"][0]["name"] == "m"
        assert bulk.status_code == 200 and bulk.json()["errors"] == []
        assert deleted.status_code == 200 and deleted.json()["errors"] == []
    else:
        assert created.status_code == 404
        assert bulk.status_code == 405 and deleted.status_code == 405
        assert (
            client.get(f"{base}/reservations/", headers=auth_headers).json()["reservations"] == []
        )
