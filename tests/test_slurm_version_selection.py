"""SLURM_EMULATOR_SLURM_VERSION: selectable emulated Slurm release."""

import pytest
from fastapi.testclient import TestClient

from emulator.api.slurmrestd.app import create_app
from emulator.commands.sacct import SacctEmulator
from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine
from emulator.slurm_version import ENV_VAR, RELEASES, get_selected_release

RELEASE_KEYS = sorted(RELEASES)


def _client(monkeypatch, release):
    monkeypatch.setenv(ENV_VAR, release)
    return TestClient(create_app())


class TestEnvParsing:
    def test_default_is_26_11(self, monkeypatch):
        monkeypatch.delenv(ENV_VAR, raising=False)
        assert get_selected_release().release == "26.11.0"

    def test_short_and_full_forms_equal(self, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "25.11")
        short = get_selected_release()
        monkeypatch.setenv(ENV_VAR, "25.11.0")
        assert get_selected_release() == short

    def test_unsupported_release_fails_fast(self, state_env, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "23.02")
        with pytest.raises(ValueError, match="not a supported Slurm release"):
            create_app()


class TestVersionReporting:
    @pytest.mark.parametrize("key", RELEASE_KEYS)
    def test_ping_meta_reports_selected_release(self, state_env, monkeypatch, auth_headers, key):
        rel = RELEASES[key]
        client = _client(monkeypatch, key)
        response = client.get(f"/slurm/{rel.api_version}/ping/", headers=auth_headers)
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta["slurm"]["release"] == rel.release
        assert meta["slurm"]["version"] == rel.version
        assert meta["plugin"]["data_parser"] == rel.data_parser

    @pytest.mark.parametrize("key", RELEASE_KEYS)
    def test_conf_reports_selected_release(self, state_env, monkeypatch, auth_headers, key):
        rel = RELEASES[key]
        client = _client(monkeypatch, key)
        response = client.get(f"/slurm/{rel.api_version}/conf", headers=auth_headers)
        assert response.json()["config"]["slurm_version"] == rel.release

    def test_openapi_spec_uses_selected_api_version(self, state_env, monkeypatch, auth_headers):
        client = _client(monkeypatch, "25.05")
        spec = client.get("/openapi/v3", headers=auth_headers).json()
        assert spec["info"]["version"] == "v0.0.44"
        assert "/slurm/v0.0.44/ping/" in spec["paths"]


class TestAcceptedWindow:
    def test_26_11_serves_two_prior_versions(self, state_env, monkeypatch, auth_headers):
        client = _client(monkeypatch, "26.11")
        for version in ("v0.0.44", "v0.0.45", "v0.0.46"):
            response = client.get(f"/slurm/{version}/ping/", headers=auth_headers)
            assert response.status_code == 200
            meta = response.json()["meta"]
            # data_parser echoes the requested plugin; the release does not.
            assert meta["plugin"]["data_parser"] == f"data_parser/{version}"
            assert meta["slurm"]["release"] == "26.11.0"

    def test_24_11_serves_its_own_window(self, state_env, monkeypatch, auth_headers):
        client = _client(monkeypatch, "24.11")
        for version in ("v0.0.41", "v0.0.42", "v0.0.43"):
            response = client.get(f"/slurmdb/{version}/ping/", headers=auth_headers)
            assert response.status_code == 200

    def test_newer_than_release_rejected(self, state_env, monkeypatch, auth_headers):
        client = _client(monkeypatch, "24.11")
        response = client.get("/slurm/v0.0.44/ping/", headers=auth_headers)
        assert response.status_code == 404
        assert response.headers["content-type"].startswith("text/plain")
        assert response.headers["connection"] == "Close"

    def test_below_window_rejected(self, state_env, monkeypatch, auth_headers):
        client = _client(monkeypatch, "26.11")
        response = client.get("/slurm/v0.0.43/ping/", headers=auth_headers)
        assert response.status_code == 404


class TestRpcVersion:
    def test_fresh_cluster_gets_release_rpc_version(self, state_env, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "24.11")
        db = SlurmDatabase()
        assert db.clusters["default"].rpc_version == 8832

    def test_stored_rpc_version_survives_downgrade(self, state_env, monkeypatch):
        db = SlurmDatabase()
        assert db.clusters["default"].rpc_version == 9600
        db.save_state()

        monkeypatch.setenv(ENV_VAR, "24.11")
        db2 = SlurmDatabase()
        db2.load_state()
        assert db2.clusters["default"].rpc_version == 9600


class TestCliVersion:
    def test_sacct_version(self, state_env, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "24.11")
        sacct = SacctEmulator(SlurmDatabase(), TimeEngine())
        assert sacct.handle_command(["--version"]) == "slurm 24.11.0"

    def test_sacctmgr_version(self, state_env, monkeypatch):
        monkeypatch.setenv(ENV_VAR, "25.05")
        sacctmgr = SacctmgrEmulator(SlurmDatabase(), TimeEngine())
        assert sacctmgr.handle_command(["-V"]) == "slurm 25.05.0"
