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

    def test_account_names_are_case_insensitive(self, restd, auth_headers):
        # Real slurmdbd folds account names to lower case (xstrtolower via
        # slurm_addto_char_list). An account created as MixedCase is stored
        # lower-cased, and an associations query with any case must find it.
        restd.post(
            "/slurmdb/v0.0.46/accounts/",
            headers=auth_headers,
            json={"accounts": [ACCOUNT, {"name": "2026_00A", "parent_account": "proj1"}]},
        )
        stored = restd.get("/slurmdb/v0.0.46/account/2026_00A", headers=auth_headers)
        assert stored.json()["accounts"][0]["name"] == "2026_00a"

        assocs = restd.get(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            params={"account": "2026_00A"},
        ).json()["associations"]
        account_rows = [a for a in assocs if not a.get("user")]
        assert account_rows
        assert account_rows[0]["account"] == "2026_00a"
        assert account_rows[0]["parent_account"] == "proj1"

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


class TestAccountsAssociationRealShape:
    """``association_condition`` request shape, as real slurmrestd parses it.

    POST /accounts_association/ body = OPENAPI_ACCOUNTS_ADD_COND_RESP:
    ``association_condition`` (ACCOUNTS_ADD_COND: accounts/clusters CSV
    lists + one ASSOC_REC_SET) and ``account`` (ACCOUNT_SHORT), per
    data_parser v0.0.46 parsers.c:11038-11046 + 13090-13096.
    """

    def test_create_account_with_parent(self, restd, auth_headers):
        restd.post("/slurmdb/v0.0.46/accounts/", headers=auth_headers, json={"accounts": [ACCOUNT]})
        response = restd.post(
            "/slurmdb/v0.0.46/accounts_association/",
            headers=auth_headers,
            json={
                "association_condition": {
                    "accounts": ["child1"],
                    "clusters": ["default"],
                    "association": {"parent": "proj1"},
                },
                "account": {"description": "Child 1", "organization": "org1"},
            },
        )
        assert response.status_code == 200
        assert response.json()["errors"] == []
        assert response.json()["added_accounts"] == ["child1"]

        account = restd.get("/slurmdb/v0.0.46/account/child1", headers=auth_headers).json()[
            "accounts"
        ][0]
        assert account["description"] == "Child 1"
        assert account["organization"] == "org1"
        assocs = restd.get(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            params={"account": "child1"},
        ).json()["associations"]
        assert assocs[0]["parent_account"] == "proj1"
        assert assocs[0]["cluster"] == "default"

    def test_csv_string_lists_accepted(self, restd, auth_headers):
        # CSV_STRING_LIST fields accept a comma-separated string too.
        restd.post(
            "/slurmdb/v0.0.46/accounts_association/",
            headers=auth_headers,
            json={"association_condition": {"accounts": "acc-a,acc-b"}},
        )
        names = {
            a["name"]
            for a in restd.get("/slurmdb/v0.0.46/accounts/", headers=auth_headers).json()[
                "accounts"
            ]
        }
        assert {"acc-a", "acc-b"} <= names

    def test_rec_set_limits_applied(self, restd, auth_headers):
        restd.post(
            "/slurmdb/v0.0.46/accounts_association/",
            headers=auth_headers,
            json={
                "association_condition": {
                    "accounts": ["limited"],
                    "association": {"grptresmins": "cpu=60000,mem=1024", "fairshare": 42},
                }
            },
        )
        assoc = restd.get(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            params={"account": "limited"},
        ).json()["associations"][0]
        minutes = {
            f"{t['type']}/{t['name']}" if t["name"] else t["type"]: t["count"]
            for t in assoc["max"]["tres"]["group"]["minutes"]
        }
        assert minutes == {"cpu": 60000, "mem": 1024}
        assert assoc["shares_raw"] == 42

    def test_empty_condition_is_error(self, restd, auth_headers):
        response = restd.post(
            "/slurmdb/v0.0.46/accounts_association/",
            headers=auth_headers,
            json={"association_condition": {}},
        )
        assert response.status_code == 400

    def test_legacy_accounts_shape_still_accepted(self, restd, auth_headers):
        restd.post(
            "/slurmdb/v0.0.46/accounts_association/",
            headers=auth_headers,
            json={"accounts": [{"name": "legacy1", "associations": [{"account": "legacy1"}]}]},
        )
        account = restd.get("/slurmdb/v0.0.46/account/legacy1", headers=auth_headers).json()
        assert account["accounts"][0]["name"] == "legacy1"


class TestUsersAssociationRealShape:
    """``association_condition`` request shape for POST /users_association/.

    Body = OPENAPI_USERS_ADD_COND_RESP: ``association_condition``
    (USERS_ADD_COND: users/accounts/clusters/partitions CSV lists + one
    ASSOC_REC_SET) and ``user`` (USER_SHORT), per data_parser v0.0.46
    parsers.c:11061-11069 + 13100-13108.
    """

    def _create_account(self, restd, auth_headers, name="proj1"):
        restd.post(
            "/slurmdb/v0.0.46/accounts/",
            headers=auth_headers,
            json={"accounts": [{"name": name, "description": name, "organization": "org1"}]},
        )

    def test_create_user_with_association_and_default(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        response = restd.post(
            "/slurmdb/v0.0.46/users_association/",
            headers=auth_headers,
            json={
                "association_condition": {
                    "users": ["alice"],
                    "accounts": ["proj1"],
                    "clusters": ["default"],
                    "association": {"fairshare": 2147483647},
                },
                "user": {"default": {"account": "proj1"}},
            },
        )
        assert response.status_code == 200
        assert response.json()["errors"] == []
        assert response.json()["added_users"] == ["alice"]

        user = restd.get("/slurmdb/v0.0.46/user/alice", headers=auth_headers).json()["users"][0]
        assert user["default"]["account"] == "proj1"
        assocs = restd.get(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            params={"account": "proj1", "user": "alice"},
        ).json()["associations"]
        assert len(assocs) == 1
        assert assocs[0]["cluster"] == "default"

    def test_default_account_falls_back_to_first_account(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            "/slurmdb/v0.0.46/users_association/",
            headers=auth_headers,
            json={"association_condition": {"users": ["bob"], "accounts": ["proj1"]}},
        )
        user = restd.get("/slurmdb/v0.0.46/user/bob", headers=auth_headers).json()["users"][0]
        assert user["default"]["account"] == "proj1"

    def test_partition_scoped_associations(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            "/slurmdb/v0.0.46/users_association/",
            headers=auth_headers,
            json={
                "association_condition": {
                    "users": ["carol"],
                    "accounts": ["proj1"],
                    "partitions": ["cn", "gpu"],
                }
            },
        )
        assocs = restd.get(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            params={"account": "proj1", "user": "carol"},
        ).json()["associations"]
        assert sorted(a["partition"] for a in assocs) == ["cn", "gpu"]

    def test_missing_users_is_error(self, restd, auth_headers):
        response = restd.post(
            "/slurmdb/v0.0.46/users_association/",
            headers=auth_headers,
            json={"association_condition": {"accounts": ["proj1"]}},
        )
        assert response.status_code == 400

    def test_legacy_users_shape_still_accepted(self, restd, auth_headers):
        restd.post(
            "/slurmdb/v0.0.46/users_association/",
            headers=auth_headers,
            json={"users": [{"name": "dave", "default": {"account": "proj1"}}]},
        )
        user = restd.get("/slurmdb/v0.0.46/user/dave", headers=auth_headers).json()["users"][0]
        assert user["name"] == "dave"


class TestAccountLevelAssociationWrites:
    """qos / default qos / shares_raw via POST /associations/ (ASSOC fields).

    These are settable on associations in real slurmrestd (data_parser
    v0.0.46 parsers.c:8780-8790: "qos", "default/qos", "shares_raw").
    """

    def _create_account(self, restd, auth_headers, name="proj1"):
        restd.post(
            "/slurmdb/v0.0.46/accounts/",
            headers=auth_headers,
            json={"accounts": [{"name": name, "description": name, "organization": "org1"}]},
        )

    def _account_assoc(self, restd, auth_headers, name="proj1"):
        assocs = restd.get(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            params={"account": name},
        ).json()["associations"]
        return next(a for a in assocs if not a["user"])

    def test_qos_list_roundtrip(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            json={"associations": [{"account": "proj1", "qos": ["fast", "slow"]}]},
        )
        assoc = self._account_assoc(restd, auth_headers)
        assert assoc["qos"] == ["fast", "slow"]

    def test_default_qos_roundtrip(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            json={"associations": [{"account": "proj1", "default": {"qos": "fast"}}]},
        )
        assoc = self._account_assoc(restd, auth_headers)
        assert assoc["default"]["qos"] == "fast"

    def test_shares_raw_roundtrip_plain_and_tristate(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            json={"associations": [{"account": "proj1", "shares_raw": 42}]},
        )
        assert self._account_assoc(restd, auth_headers)["shares_raw"] == 42

        restd.post(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            json={"associations": [{"account": "proj1", "shares_raw": {"set": True, "number": 7}}]},
        )
        assert self._account_assoc(restd, auth_headers)["shares_raw"] == 7

    def test_visible_to_cli(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            "/slurmdb/v0.0.46/associations/",
            headers=auth_headers,
            json={"associations": [{"account": "proj1", "shares_raw": 99}]},
        )
        database = SlurmDatabase()
        database.load_state()
        assert database.get_account("proj1").fairshare == 99
