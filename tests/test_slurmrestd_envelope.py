"""Envelope shape, version handling, and URL rejection behavior."""

from emulator.slurm_version import current

V = current().api_version


class TestEnvelope:
    def test_slurmdb_ping_meta(self, restd, auth_headers):
        response = restd.get(f"/slurmdb/{V}/ping/", headers=auth_headers)
        assert response.status_code == 200
        body = response.json()
        meta = body["meta"]
        assert meta["plugin"]["type"] == "openapi/slurmdbd"
        assert meta["plugin"]["name"] == "Slurm OpenAPI slurmdbd"
        assert meta["plugin"]["data_parser"] == f"data_parser/{V}"
        assert meta["slurm"]["release"] == current().release
        assert meta["slurm"]["version"] == current().version_parts
        assert meta["slurm"]["cluster"] == "default"
        assert body["errors"] == []
        assert isinstance(body["warnings"], list)
        assert body["pings"][0]["responding"] is True

    def test_slurmctld_ping_meta(self, restd, auth_headers):
        response = restd.get(f"/slurm/{V}/ping/", headers=auth_headers)
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta["plugin"]["type"] == "openapi/slurmctld"
        assert meta["plugin"]["name"] == "Slurm OpenAPI slurmctld"

    def test_client_user_from_header(self, restd):
        response = restd.get(
            f"/slurmdb/{V}/ping/",
            headers={"X-SLURM-USER-TOKEN": "t", "X-SLURM-USER-NAME": "alice"},
        )
        assert response.json()["meta"]["client"]["user"] == "alice"


class TestVersionRejection:
    def test_older_version_rejected(self, restd, auth_headers):
        response = restd.get(
            f"/slurmdb/{current().previous_api_version}/accounts/", headers=auth_headers
        )
        assert response.status_code == 404
        assert response.headers["content-type"].startswith("text/plain")
        assert "Unable to find requested URL endpoint" in response.text

    def test_garbage_version_rejected(self, restd, auth_headers):
        response = restd.get("/slurmdb/v9.9.99/ping/", headers=auth_headers)
        assert response.status_code == 404
        assert "Unable to find requested URL endpoint" in response.text

    def test_unknown_path_rejected_plaintext(self, restd, auth_headers):
        response = restd.get(f"/slurm/{V}/does-not-exist/", headers=auth_headers)
        assert response.status_code == 404
        assert response.headers["content-type"].startswith("text/plain")
        assert "Unable to find requested URL endpoint" in response.text
        assert response.headers.get("connection") == "Close"

    def test_job_submit_registered(self, restd, auth_headers, state_env):
        # Job submission is implemented (FireCREST needs POST /job/submit).
        # Response mirrors OPENAPI_JOB_SUBMIT_RESPONSE: top-level job_id.
        response = restd.post(f"/slurm/{V}/job/submit", headers=auth_headers, json={"job": {}})
        assert response.status_code == 200
        body = response.json()
        assert isinstance(body["job_id"], int)
        assert "meta" in body
        assert body["errors"] == []

    def test_unknown_method_on_known_path(self, restd, auth_headers):
        response = restd.post(f"/slurm/{V}/partitions/", headers=auth_headers)
        assert response.status_code == 405
        assert "Unknown HTTP method" in response.text


class TestOpenapiSelfDescription:
    def test_spec_served_on_real_paths(self, restd):
        for path in ("/openapi.json", "/openapi", "/openapi/v3"):
            response = restd.get(path)
            assert response.status_code == 200, path
            spec = response.json()
            assert spec["info"]["title"] == "Slurm REST API"
            assert spec["info"]["version"] == f"{V}"
            assert f"/slurmdb/{V}/accounts/" in spec["paths"]
            assert f"/slurm/{V}/jobs/" in spec["paths"]
