"""Envelope shape, version handling, and URL rejection behavior."""


class TestEnvelope:
    def test_slurmdb_ping_meta(self, restd, auth_headers):
        response = restd.get("/slurmdb/v0.0.46/ping/", headers=auth_headers)
        assert response.status_code == 200
        body = response.json()
        meta = body["meta"]
        assert meta["plugin"]["type"] == "openapi/slurmdbd"
        assert meta["plugin"]["name"] == "Slurm OpenAPI slurmdbd"
        assert meta["plugin"]["data_parser"] == "data_parser/v0.0.46"
        assert meta["slurm"]["release"] == "26.11.0"
        assert meta["slurm"]["version"] == {"major": "26", "micro": "0", "minor": "11"}
        assert meta["slurm"]["cluster"] == "default"
        assert body["errors"] == []
        assert isinstance(body["warnings"], list)
        assert body["pings"][0]["responding"] is True

    def test_slurmctld_ping_meta(self, restd, auth_headers):
        response = restd.get("/slurm/v0.0.46/ping/", headers=auth_headers)
        assert response.status_code == 200
        meta = response.json()["meta"]
        assert meta["plugin"]["type"] == "openapi/slurmctld"
        assert meta["plugin"]["name"] == "Slurm OpenAPI slurmctld"

    def test_client_user_from_header(self, restd):
        response = restd.get(
            "/slurmdb/v0.0.46/ping/",
            headers={"X-SLURM-USER-TOKEN": "t", "X-SLURM-USER-NAME": "alice"},
        )
        assert response.json()["meta"]["client"]["user"] == "alice"


class TestVersionRejection:
    def test_older_version_rejected(self, restd, auth_headers):
        response = restd.get("/slurmdb/v0.0.45/accounts/", headers=auth_headers)
        assert response.status_code == 404
        assert response.headers["content-type"].startswith("text/plain")
        assert "Unable to find requested URL endpoint" in response.text

    def test_garbage_version_rejected(self, restd, auth_headers):
        response = restd.get("/slurmdb/v9.9.99/ping/", headers=auth_headers)
        assert response.status_code == 404
        assert "Unable to find requested URL endpoint" in response.text

    def test_unknown_path_rejected_plaintext(self, restd, auth_headers):
        response = restd.get("/slurm/v0.0.46/does-not-exist/", headers=auth_headers)
        assert response.status_code == 404
        assert response.headers["content-type"].startswith("text/plain")
        assert "Unable to find requested URL endpoint" in response.text
        assert response.headers.get("connection") == "Close"

    def test_job_submit_registered(self, restd, auth_headers, state_env):
        # Job submission is implemented (FireCREST needs POST /job/submit).
        # Response mirrors OPENAPI_JOB_SUBMIT_RESPONSE: top-level job_id.
        response = restd.post("/slurm/v0.0.46/job/submit", headers=auth_headers, json={"job": {}})
        assert response.status_code == 200
        body = response.json()
        assert isinstance(body["job_id"], int)
        assert "meta" in body
        assert body["errors"] == []

    def test_unknown_method_on_known_path(self, restd, auth_headers):
        response = restd.post("/slurm/v0.0.46/partitions/", headers=auth_headers)
        assert response.status_code == 405
        assert "Unknown HTTP method" in response.text


class TestOpenapiSelfDescription:
    def test_spec_served_on_real_paths(self, restd):
        for path in ("/openapi.json", "/openapi", "/openapi/v3"):
            response = restd.get(path)
            assert response.status_code == 200, path
            spec = response.json()
            assert spec["info"]["title"] == "Slurm REST API"
            assert spec["info"]["version"] == "v0.0.46"
            assert "/slurmdb/v0.0.46/accounts/" in spec["paths"]
            assert "/slurm/v0.0.46/jobs/" in spec["paths"]
