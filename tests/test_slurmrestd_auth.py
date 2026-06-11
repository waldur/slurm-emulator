"""Auth header handling and optional JWT verification."""

from fastapi.testclient import TestClient

from emulator.api.emulator_server import create_app as create_control_app
from emulator.api.slurmrestd.auth import encode_jwt_hs256


class TestLenientAuth:
    def test_missing_token_rejected(self, restd):
        response = restd.get("/slurmdb/v0.0.46/ping/")
        assert response.status_code == 401
        assert response.text == "Authentication failure"
        assert response.headers["content-type"].startswith("text/plain")
        assert response.headers.get("connection") == "Close"

    def test_any_token_accepted_by_default(self, restd):
        response = restd.get("/slurmdb/v0.0.46/ping/", headers={"X-SLURM-USER-TOKEN": "whatever"})
        assert response.status_code == 200

    def test_bearer_token_accepted(self, restd):
        response = restd.get("/slurmdb/v0.0.46/ping/", headers={"Authorization": "Bearer whatever"})
        assert response.status_code == 200


class TestJwtVerification:
    KEY = "test-signing-key"

    def test_garbage_token_rejected(self, restd, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_JWT_KEY", self.KEY)
        response = restd.get("/slurmdb/v0.0.46/ping/", headers={"X-SLURM-USER-TOKEN": "garbage"})
        assert response.status_code == 401

    def test_valid_token_accepted_and_names_user(self, restd, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_JWT_KEY", self.KEY)
        token = encode_jwt_hs256("bob", lifespan=600, key=self.KEY)
        response = restd.get("/slurmdb/v0.0.46/ping/", headers={"X-SLURM-USER-TOKEN": token})
        assert response.status_code == 200
        assert response.json()["meta"]["client"]["user"] == "bob"

    def test_expired_token_rejected(self, restd, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_JWT_KEY", self.KEY)
        token = encode_jwt_hs256("bob", lifespan=10, key=self.KEY, now=1000)
        response = restd.get("/slurmdb/v0.0.46/ping/", headers={"X-SLURM-USER-TOKEN": token})
        assert response.status_code == 401

    def test_wrong_key_rejected(self, restd, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_JWT_KEY", self.KEY)
        token = encode_jwt_hs256("bob", key="another-key")
        response = restd.get("/slurmdb/v0.0.46/ping/", headers={"X-SLURM-USER-TOKEN": token})
        assert response.status_code == 401


class TestTokenHelper:
    def test_control_api_token_accepted_by_slurmrestd(self, restd, state_env):
        control = TestClient(create_control_app())
        response = control.post("/api/token", json={"username": "alice", "lifespan": 600})
        assert response.status_code == 200
        body = response.json()
        assert body["SLURM_JWT"] == f"SLURM_JWT={body['token']}"

        # Without SLURM_EMULATOR_JWT_KEY the dev key signs the token and
        # the slurmrestd app accepts any token anyway.
        ping = restd.get("/slurmdb/v0.0.46/ping/", headers={"X-SLURM-USER-TOKEN": body["token"]})
        assert ping.status_code == 200

    def test_token_round_trip_with_verification(self, restd, monkeypatch, state_env):
        monkeypatch.setenv("SLURM_EMULATOR_JWT_KEY", "shared-key")
        control = TestClient(create_control_app())
        token = control.post("/api/token", json={"username": "alice"}).json()["token"]
        ping = restd.get("/slurmdb/v0.0.46/ping/", headers={"X-SLURM-USER-TOKEN": token})
        assert ping.status_code == 200
        assert ping.json()["meta"]["client"]["user"] == "alice"
