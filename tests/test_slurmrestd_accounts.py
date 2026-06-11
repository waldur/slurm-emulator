"""slurmdb account/user/association/qos CRUD round-trips.

Includes cross-surface checks: entities created through the REST API
must be visible to a fresh SlurmDatabase load and to the sacctmgr CLI
emulator (shared JSON state file).
"""

from emulator.commands.dispatcher import SlurmEmulator
from emulator.core.database import SlurmDatabase

ACCOUNT = {"name": "proj1", "description": "Project 1", "organization": "org1"}


class TestAccounts:
    def test_create_and_get(self, restd, auth_headers):
        response = restd.post(
            "/slurmdb/v0.0.46/accounts/", headers=auth_headers, json={"accounts": [ACCOUNT]}
        )
        assert response.status_code == 200
        assert response.json()["errors"] == []

        single = restd.get("/slurmdb/v0.0.46/account/proj1", headers=auth_headers)
        account = single.json()["accounts"][0]
        assert account["name"] == "proj1"
        assert account["description"] == "Project 1"
        assert account["organization"] == "org1"
        assert account["flags"] == []

    def test_visible_to_fresh_database_and_cli(self, restd, auth_headers):
        restd.post("/slurmdb/v0.0.46/accounts/", headers=auth_headers, json={"accounts": [ACCOUNT]})

        database = SlurmDatabase()
        database.load_state()
        assert database.get_account("proj1") is not None

        cli = SlurmEmulator()
        output = cli.execute_command(
            "sacctmgr", ["--parsable2", "--noheader", "--immediate", "list", "account"]
        )
        assert any(line.startswith("proj1|") for line in output.splitlines())

    def test_cli_account_visible_to_rest(self, restd, auth_headers):
        cli = SlurmEmulator()
        cli.execute_command(
            "sacctmgr",
            ["--immediate", "add", "account", "cliacct", "description=x", "organization=y"],
        )
        response = restd.get("/slurmdb/v0.0.46/account/cliacct", headers=auth_headers)
        assert response.json()["accounts"][0]["name"] == "cliacct"

    def test_parent_account(self, restd, auth_headers):
        restd.post(
            "/slurmdb/v0.0.46/accounts/",
            headers=auth_headers,
            json={"accounts": [ACCOUNT, {"name": "child", "parent_account": "proj1"}]},
        )
        assocs = restd.get(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            params={"account": "child"},
        ).json()["associations"]
        assert assocs[0]["parent_account"] == "proj1"

    def test_unknown_account_warns_found_nothing(self, restd, auth_headers):
        response = restd.get("/slurmdb/v0.0.46/account/nope", headers=auth_headers)
        assert response.status_code == 200
        body = response.json()
        assert body["accounts"] == []
        assert any("found nothing" in w["description"] for w in body["warnings"])

    def test_delete_account(self, restd, auth_headers):
        restd.post("/slurmdb/v0.0.46/accounts/", headers=auth_headers, json={"accounts": [ACCOUNT]})
        response = restd.delete("/slurmdb/v0.0.46/account/proj1", headers=auth_headers)
        assert response.json()["removed_accounts"] == ["proj1"]

        database = SlurmDatabase()
        database.load_state()
        assert database.get_account("proj1") is None
        assert not [a for a in database.associations.values() if a.account == "proj1"]

    def test_post_without_accounts_is_error(self, restd, auth_headers):
        response = restd.post("/slurmdb/v0.0.46/accounts/", headers=auth_headers, json={})
        assert response.status_code == 400
        assert response.json()["errors"][0]["error_number"] == 9000


class TestUsers:
    def test_create_user_with_association(self, restd, auth_headers):
        restd.post("/slurmdb/v0.0.46/accounts/", headers=auth_headers, json={"accounts": [ACCOUNT]})
        response = restd.post(
            "/slurmdb/v0.0.46/users/",
            headers=auth_headers,
            json={
                "users": [
                    {
                        "name": "alice",
                        "default": {"account": "proj1"},
                        "associations": [{"account": "proj1"}],
                    }
                ]
            },
        )
        assert response.status_code == 200

        user = restd.get("/slurmdb/v0.0.46/user/alice", headers=auth_headers).json()["users"][0]
        assert user["name"] == "alice"
        assert user["default"]["account"] == "proj1"
        assert any(a["account"] == "proj1" for a in user["associations"])

    def test_delete_user_removes_associations(self, restd, auth_headers):
        restd.post(
            "/slurmdb/v0.0.46/users/",
            headers=auth_headers,
            json={"users": [{"name": "alice", "associations": [{"account": "proj1"}]}]},
        )
        response = restd.delete("/slurmdb/v0.0.46/user/alice", headers=auth_headers)
        assert response.json()["removed_users"] == ["alice"]

        database = SlurmDatabase()
        database.load_state()
        assert database.get_user("alice") is None
        assert not [a for a in database.associations.values() if a.user == "alice"]


class TestAssociations:
    def _setup(self, restd, auth_headers, partition=None):
        entry = {"account": "proj1", "user": "alice"}
        if partition:
            entry["partition"] = partition
        restd.post(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            json={"associations": [entry]},
        )

    def test_create_and_filter(self, restd, auth_headers):
        self._setup(restd, auth_headers)
        assocs = restd.get(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            params={"account": "proj1", "user": "alice"},
        ).json()["associations"]
        assert len(assocs) == 1
        assert assocs[0]["account"] == "proj1"
        assert assocs[0]["user"] == "alice"
        assert assocs[0]["cluster"] == "default"

    def test_partition_scoped(self, restd, auth_headers):
        self._setup(restd, auth_headers, partition="compute")
        assocs = restd.get(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            params={"user": "alice", "partition": "compute"},
        ).json()["associations"]
        assert assocs[0]["partition"] == "compute"

    def test_limits_round_trip(self, restd, auth_headers):
        restd.post(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            json={
                "associations": [
                    {
                        "account": "proj1",
                        "user": "alice",
                        "max": {
                            "tres": {
                                "group": {"minutes": [{"type": "cpu", "name": "", "count": 6000}]}
                            }
                        },
                    }
                ]
            },
        )
        assoc = restd.get(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            params={"user": "alice"},
        ).json()["associations"][0]
        minutes = assoc["max"]["tres"]["group"]["minutes"]
        assert minutes == [{"type": "cpu", "name": "", "id": 1, "count": 6000}]

    def test_repost_without_limits_preserves_them(self, restd, auth_headers):
        self.test_limits_round_trip(restd, auth_headers)
        # Re-POST the same association with no limits subtree.
        restd.post(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            json={"associations": [{"account": "proj1", "user": "alice"}]},
        )
        assoc = restd.get(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            params={"user": "alice"},
        ).json()["associations"][0]
        assert assoc["max"]["tres"]["group"]["minutes"] == [
            {"type": "cpu", "name": "", "id": 1, "count": 6000}
        ]

    def test_delete_returns_real_removal_strings(self, restd, auth_headers):
        self._setup(restd, auth_headers)
        response = restd.delete(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            params={"account": "proj1", "user": "alice"},
        )
        removed = response.json()["removed_associations"]
        assert len(removed) == 1
        assert removed[0].startswith("C = default")
        assert "A = proj1" in removed[0]
        assert "U = alice" in removed[0]

    def test_delete_without_condition_is_error(self, restd, auth_headers):
        response = restd.delete("/slurmdb/v0.0.46/associations/", headers=auth_headers)
        assert response.status_code == 400


class TestQos:
    def test_create_and_get(self, restd, auth_headers):
        response = restd.post(
            "/slurmdb/v0.0.46/qos/",
            headers=auth_headers,
            json={
                "qos": [
                    {
                        "name": "slowdown",
                        "limits": {
                            "max": {
                                "tres": {"total": [{"type": "cpu", "count": 100}]},
                                "wall_clock": {"per": {"job": 60}},
                            }
                        },
                    }
                ]
            },
        )
        assert response.status_code == 200

        qos = restd.get("/slurmdb/v0.0.46/qos/slowdown", headers=auth_headers).json()["qos"][0]
        assert qos["name"] == "slowdown"
        assert qos["limits"]["max"]["tres"]["total"] == [
            {"type": "cpu", "name": "", "id": 1, "count": 100}
        ]
        wall = qos["limits"]["max"]["wall_clock"]["per"]["job"]
        assert wall == {"set": True, "infinite": False, "number": 60}

    def test_delete(self, restd, auth_headers):
        restd.post("/slurmdb/v0.0.46/qos/", headers=auth_headers, json={"qos": [{"name": "tmp"}]})
        response = restd.delete("/slurmdb/v0.0.46/qos/tmp", headers=auth_headers)
        assert response.json()["removed_qos"] == ["tmp"]

    def test_tres_listing(self, restd, auth_headers):
        tres = restd.get("/slurmdb/v0.0.46/tres/", headers=auth_headers).json()["TRES"]
        types = {entry["type"] for entry in tres}
        assert {"cpu", "mem", "billing"} <= types
