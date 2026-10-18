"""slurmdb account/user/association/qos CRUD round-trips.

Includes cross-surface checks: entities created through the REST API
must be visible to a fresh SlurmDatabase load and to the sacctmgr CLI
emulator (shared JSON state file).
"""

from emulator.api.slurmrestd.envelope import ESLURM_NO_REMOVE_DEFAULT_QOS, http_status_for
from emulator.commands.dispatcher import SlurmEmulator
from emulator.core.database import SlurmDatabase
from emulator.slurm_version import current

V = current().api_version

ACCOUNT = {"name": "proj1", "description": "Project 1", "organization": "org1"}


class TestAccounts:
    def test_create_and_get(self, restd, auth_headers):
        response = restd.post(
            f"/slurmdb/{V}/accounts/", headers=auth_headers, json={"accounts": [ACCOUNT]}
        )
        assert response.status_code == 200
        assert response.json()["errors"] == []

        single = restd.get(f"/slurmdb/{V}/account/proj1", headers=auth_headers)
        account = single.json()["accounts"][0]
        assert account["name"] == "proj1"
        assert account["description"] == "Project 1"
        assert account["organization"] == "org1"
        assert account["flags"] == []

    def test_visible_to_fresh_database_and_cli(self, restd, auth_headers):
        restd.post(f"/slurmdb/{V}/accounts/", headers=auth_headers, json={"accounts": [ACCOUNT]})

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
        response = restd.get(f"/slurmdb/{V}/account/cliacct", headers=auth_headers)
        assert response.json()["accounts"][0]["name"] == "cliacct"

    def test_parent_account(self, restd, auth_headers):
        restd.post(
            f"/slurmdb/{V}/accounts/",
            headers=auth_headers,
            json={"accounts": [ACCOUNT, {"name": "child", "parent_account": "proj1"}]},
        )
        assocs = restd.get(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            params={"account": "child"},
        ).json()["associations"]
        assert assocs[0]["parent_account"] == "proj1"

    def test_account_names_are_case_insensitive(self, restd, auth_headers):
        # Real slurmdbd folds account names to lower case (xstrtolower via
        # slurm_addto_char_list). An account created as MixedCase is stored
        # lower-cased, and an associations query with any case must find it.
        restd.post(
            f"/slurmdb/{V}/accounts/",
            headers=auth_headers,
            json={"accounts": [ACCOUNT, {"name": "2026_00A", "parent_account": "proj1"}]},
        )
        stored = restd.get(f"/slurmdb/{V}/account/2026_00A", headers=auth_headers)
        assert stored.json()["accounts"][0]["name"] == "2026_00a"

        assocs = restd.get(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            params={"account": "2026_00A"},
        ).json()["associations"]
        account_rows = [a for a in assocs if not a.get("user")]
        assert account_rows
        assert account_rows[0]["account"] == "2026_00a"
        assert account_rows[0]["parent_account"] == "proj1"

    def test_unknown_account_warns_found_nothing(self, restd, auth_headers):
        response = restd.get(f"/slurmdb/{V}/account/nope", headers=auth_headers)
        assert response.status_code == 200
        body = response.json()
        assert body["accounts"] == []
        assert any("found nothing" in w["description"] for w in body["warnings"])

    def test_delete_account(self, restd, auth_headers):
        restd.post(f"/slurmdb/{V}/accounts/", headers=auth_headers, json={"accounts": [ACCOUNT]})
        response = restd.delete(f"/slurmdb/{V}/account/proj1", headers=auth_headers)
        assert response.json()["removed_accounts"] == ["proj1"]

        database = SlurmDatabase()
        database.load_state()
        assert database.get_account("proj1") is None
        assert not [a for a in database.associations.values() if a.account == "proj1"]

    def test_post_without_accounts_is_error(self, restd, auth_headers):
        response = restd.post(f"/slurmdb/{V}/accounts/", headers=auth_headers, json={})
        assert response.status_code == 422  # ESLURM_REST_INVALID_QUERY
        assert response.json()["errors"][0]["error_number"] == 9000


class TestUsers:
    def test_create_user_with_association(self, restd, auth_headers):
        restd.post(f"/slurmdb/{V}/accounts/", headers=auth_headers, json={"accounts": [ACCOUNT]})
        response = restd.post(
            f"/slurmdb/{V}/users/",
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

        user = restd.get(
            f"/slurmdb/{V}/user/alice", headers=auth_headers, params={"with_assocs": ""}
        ).json()["users"][0]
        assert user["name"] == "alice"
        assert user["default"]["account"] == "proj1"
        assert any(a["account"] == "proj1" for a in user["associations"])

    def test_default_account_needs_with_assocs(self, restd, auth_headers):
        """slurmdbd derives the default account from the user's associations and
        reads them only for ``with_assocs``
        (slurm://src/plugins/accounting_storage/mysql/as_mysql_user.c#as_mysql_get_users).
        """
        self.test_create_user_with_association(restd, auth_headers)
        user = restd.get(f"/slurmdb/{V}/user/alice", headers=auth_headers).json()["users"][0]
        assert user["default"]["account"] == ""
        assert user["associations"] == []
        response = restd.get(
            f"/slurmdb/{V}/user/alice", headers=auth_headers, params={"with_assocs": "true"}
        ).json()
        assert response["users"][0]["default"]["account"] == "proj1"
        assert response["warnings"][0]["source"] == "#/with_assocs/"

    def test_delete_user_removes_associations(self, restd, auth_headers):
        restd.post(
            f"/slurmdb/{V}/users/",
            headers=auth_headers,
            json={"users": [{"name": "alice", "associations": [{"account": "proj1"}]}]},
        )
        response = restd.delete(f"/slurmdb/{V}/user/alice", headers=auth_headers)
        assert response.json()["removed_users"] == ["alice"]

        database = SlurmDatabase()
        database.load_state()
        assert database.get_user("alice") is None
        assert not [a for a in database.associations.values() if a.user == "alice"]


class TestAssociations:
    def _setup(self, restd, auth_headers, partition=None):
        restd.post(f"/slurmdb/{V}/accounts/", headers=auth_headers, json={"accounts": [ACCOUNT]})
        entry = {"account": "proj1", "user": "alice"}
        if partition:
            entry["partition"] = partition
        restd.post(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            json={"associations": [entry]},
        )

    def test_create_and_filter(self, restd, auth_headers):
        self._setup(restd, auth_headers)
        assocs = restd.get(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            params={"account": "proj1", "user": "alice"},
        ).json()["associations"]
        assert len(assocs) == 1
        assert assocs[0]["account"] == "proj1"
        assert assocs[0]["user"] == "alice"
        assert assocs[0]["cluster"] == "default"

    def test_one_default_per_user_without_a_user_record(self, restd, auth_headers):
        """A user added only through associations still has a single default.

        slurmdbd gives ``is_def`` to the first association of a user that has
        none (slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_make_sure_user_has_default_internal).
        """
        for name in ("proj1", "proj2"):
            restd.post(
                f"/slurmdb/{V}/accounts/",
                headers=auth_headers,
                json={"accounts": [{**ACCOUNT, "name": name}]},
            )
            restd.post(
                f"/slurmdb/{V}/associations/",
                headers=auth_headers,
                json={"associations": [{"account": name, "user": "alice"}]},
            )
        assocs = restd.get(
            f"/slurmdb/{V}/associations/", headers=auth_headers, params={"user": "alice"}
        ).json()["associations"]
        assert sorted((a["account"], a["is_default"]) for a in assocs) == [
            ("proj1", True),
            ("proj2", False),
        ]

    def test_partition_scoped(self, restd, auth_headers):
        self._setup(restd, auth_headers, partition="compute")
        assocs = restd.get(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            params={"user": "alice", "partition": "compute"},
        ).json()["associations"]
        assert assocs[0]["partition"] == "compute"

    def test_limits_round_trip(self, restd, auth_headers):
        restd.post(f"/slurmdb/{V}/accounts/", headers=auth_headers, json={"accounts": [ACCOUNT]})
        restd.post(
            f"/slurmdb/{V}/associations/",
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
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            params={"user": "alice"},
        ).json()["associations"][0]
        minutes = assoc["max"]["tres"]["group"]["minutes"]
        assert minutes == [{"type": "cpu", "name": "", "id": 1, "count": 6000}]

    def test_repost_without_limits_clears_them(self, restd, auth_headers):
        """A POST is the association's whole TRES state: a list left out is removed.

        slurm://src/slurmrestd/plugins/openapi/slurmdbd/associations.c#_diff_tres@26.05+
        """
        self.test_limits_round_trip(restd, auth_headers)
        # Re-POST the same association with no limits subtree.
        restd.post(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            json={"associations": [{"account": "proj1", "user": "alice"}]},
        )
        assoc = restd.get(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            params={"user": "alice"},
        ).json()["associations"][0]
        assert assoc["max"]["tres"]["group"]["minutes"] == []

    def test_delete_returns_real_removal_strings(self, restd, auth_headers):
        self._setup(restd, auth_headers)
        response = restd.delete(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            params={"account": "proj1", "user": "alice"},
        )
        removed = response.json()["removed_associations"]
        assert len(removed) == 1
        assert removed[0].startswith("C = default")
        assert "A = proj1" in removed[0]
        assert "U = alice" in removed[0]

    def test_delete_without_condition_is_error(self, restd, auth_headers):
        response = restd.delete(f"/slurmdb/{V}/associations/", headers=auth_headers)
        assert response.status_code == 422  # ESLURM_REST_INVALID_QUERY


class TestQos:
    def test_create_and_get(self, restd, auth_headers):
        response = restd.post(
            f"/slurmdb/{V}/qos/",
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

        qos = restd.get(f"/slurmdb/{V}/qos/slowdown", headers=auth_headers).json()["qos"][0]
        assert qos["name"] == "slowdown"
        assert qos["limits"]["max"]["tres"]["total"] == [
            {"type": "cpu", "name": "", "id": 1, "count": 100}
        ]
        wall = qos["limits"]["max"]["wall_clock"]["per"]["job"]
        assert wall == {"set": True, "infinite": False, "number": 60}

    def test_delete(self, restd, auth_headers):
        restd.post(f"/slurmdb/{V}/qos/", headers=auth_headers, json={"qos": [{"name": "tmp"}]})
        response = restd.delete(f"/slurmdb/{V}/qos/tmp", headers=auth_headers)
        assert response.json()["removed_qos"] == ["tmp"]

    def test_tres_listing(self, restd, auth_headers):
        tres = restd.get(f"/slurmdb/{V}/tres/", headers=auth_headers).json()["TRES"]
        types = {entry["type"] for entry in tres}
        assert {"cpu", "mem", "billing"} <= types


class TestAccountsAssociationRealShape:
    """``association_condition`` request shape, as real slurmrestd parses it.

    POST /accounts_association/ body = OPENAPI_ACCOUNTS_ADD_COND_RESP:
    ``association_condition`` (ACCOUNTS_ADD_COND: accounts/clusters CSV
    lists + one ASSOC_REC_SET) and ``account`` (ACCOUNT_SHORT), per
    data_parser {V} slurm://src/plugins/data_parser/{V}/parsers.c#ACCOUNTS_ADD_COND@26.05+.
    """

    def test_create_account_with_parent(self, restd, auth_headers):
        restd.post(f"/slurmdb/{V}/accounts/", headers=auth_headers, json={"accounts": [ACCOUNT]})
        response = restd.post(
            f"/slurmdb/{V}/accounts_association/",
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

        account = restd.get(f"/slurmdb/{V}/account/child1", headers=auth_headers).json()[
            "accounts"
        ][0]
        assert account["description"] == "Child 1"
        assert account["organization"] == "org1"
        assocs = restd.get(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            params={"account": "child1"},
        ).json()["associations"]
        assert assocs[0]["parent_account"] == "proj1"
        assert assocs[0]["cluster"] == "default"

    def test_csv_string_lists_accepted(self, restd, auth_headers):
        # CSV_STRING_LIST fields accept a comma-separated string too.
        restd.post(
            f"/slurmdb/{V}/accounts_association/",
            headers=auth_headers,
            json={"association_condition": {"accounts": "acc-a,acc-b"}},
        )
        names = {
            a["name"]
            for a in restd.get(f"/slurmdb/{V}/accounts/", headers=auth_headers).json()["accounts"]
        }
        assert {"acc-a", "acc-b"} <= names

    def test_rec_set_limits_applied(self, restd, auth_headers):
        restd.post(
            f"/slurmdb/{V}/accounts_association/",
            headers=auth_headers,
            json={
                "association_condition": {
                    "accounts": ["limited"],
                    "association": {"grptresmins": "cpu=60000,mem=1024", "fairshare": 42},
                }
            },
        )
        assoc = restd.get(
            f"/slurmdb/{V}/associations/",
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
            f"/slurmdb/{V}/accounts_association/",
            headers=auth_headers,
            json={"association_condition": {}},
        )
        assert response.status_code == 422  # ESLURM_REST_INVALID_QUERY

    def test_body_without_association_condition_is_refused(self, restd, auth_headers):
        response = restd.post(
            f"/slurmdb/{V}/accounts_association/",
            headers=auth_headers,
            json={"accounts": [{"name": "legacy1"}]},
        )
        assert response.status_code == 422
        assert response.json()["errors"][0]["source"] == "#/association_condition/"

    def test_existing_accounts_are_not_modified(self, restd, auth_headers):
        body = {"association_condition": {"accounts": ["again"]}}
        restd.post(f"/slurmdb/{V}/accounts_association/", headers=auth_headers, json=body)
        response = restd.post(
            f"/slurmdb/{V}/accounts_association/", headers=auth_headers, json=body
        )
        assert response.status_code == 304
        assert response.content == b""


class TestUsersAssociationRealShape:
    """``association_condition`` request shape for POST /users_association/.

    Body = OPENAPI_USERS_ADD_COND_RESP: ``association_condition``
    (USERS_ADD_COND: users/accounts/clusters/partitions CSV lists + one
    ASSOC_REC_SET) and ``user`` (USER_SHORT), per data_parser {V}
    slurm://src/plugins/data_parser/{V}/parsers.c#USERS_ADD_COND@26.05+.
    """

    def _create_account(self, restd, auth_headers, name="proj1"):
        restd.post(
            f"/slurmdb/{V}/accounts/",
            headers=auth_headers,
            json={"accounts": [{"name": name, "description": name, "organization": "org1"}]},
        )

    def test_create_user_with_association_and_default(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        response = restd.post(
            f"/slurmdb/{V}/users_association/",
            headers=auth_headers,
            json={
                "association_condition": {
                    "users": ["alice"],
                    "accounts": ["proj1"],
                    "clusters": ["default"],
                    "association": {"fairshare": 2147483647},
                },
                "user": {"defaultaccount": "proj1"},
            },
        )
        assert response.status_code == 200
        assert response.json()["errors"] == []
        assert response.json()["added_users"] == ["alice"]

        user = restd.get(
            f"/slurmdb/{V}/user/alice", headers=auth_headers, params={"with_assocs": ""}
        ).json()["users"][0]
        assert user["default"]["account"] == "proj1"
        assocs = restd.get(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            params={"account": "proj1", "user": "alice"},
        ).json()["associations"]
        assert len(assocs) == 1
        assert assocs[0]["cluster"] == "default"

    def test_default_account_falls_back_to_first_account(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            f"/slurmdb/{V}/users_association/",
            headers=auth_headers,
            json={"association_condition": {"users": ["bob"], "accounts": ["proj1"]}, "user": {}},
        )
        user = restd.get(
            f"/slurmdb/{V}/user/bob", headers=auth_headers, params={"with_assocs": ""}
        ).json()["users"][0]
        assert user["default"]["account"] == "proj1"

    def test_partition_scoped_associations(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            f"/slurmdb/{V}/users_association/",
            headers=auth_headers,
            json={
                "association_condition": {
                    "users": ["carol"],
                    "accounts": ["proj1"],
                    "partitions": ["cn", "gpu"],
                },
                "user": {},
            },
        )
        assocs = restd.get(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            params={"account": "proj1", "user": "carol"},
        ).json()["associations"]
        assert sorted(a["partition"] for a in assocs) == ["cn", "gpu"]

    def test_missing_users_is_error(self, restd, auth_headers):
        response = restd.post(
            f"/slurmdb/{V}/users_association/",
            headers=auth_headers,
            json={"association_condition": {"accounts": ["proj1"]}},
        )
        assert response.status_code == 422  # ESLURM_REST_INVALID_QUERY

    def test_user_is_required(self, restd, auth_headers):
        """``user`` is add_parse_req in OPENAPI_USERS_ADD_COND_RESP."""
        self._create_account(restd, auth_headers)
        response = restd.post(
            f"/slurmdb/{V}/users_association/",
            headers=auth_headers,
            json={"association_condition": {"users": ["dave"], "accounts": ["proj1"]}},
        )
        assert response.status_code == 422
        assert response.json()["errors"][0]["description"] == (
            "Missing required field 'user' in dictionary"
        )

    def test_nested_default_account_is_ignored(self, restd, auth_headers):
        """USER_SHORT has a flat ``defaultaccount``; ``default.account`` is unknown."""
        self._create_account(restd, auth_headers)
        self._create_account(restd, auth_headers, "proj2")
        response = restd.post(
            f"/slurmdb/{V}/users_association/",
            headers=auth_headers,
            json={
                "association_condition": {"users": ["erin"], "accounts": ["proj1", "proj2"]},
                "user": {"default": {"account": "proj2"}},
            },
        )
        assert response.json()["warnings"] == [
            {
                "description": (
                    'Ignoring unknown field "account" of type string in DATA_PARSER_USER_SHORT'
                ),
                "source": "#/user/default/account/",
            }
        ]
        user = restd.get(
            f"/slurmdb/{V}/user/erin", headers=auth_headers, params={"with_assocs": ""}
        ).json()["users"][0]
        assert user["default"]["account"] == "proj1"

    def test_qoslevel_string_adds_nothing(self, restd, auth_headers):
        """QOS_STRING_ID_LIST needs a JSON list; a string is reported and nothing runs."""
        self._create_account(restd, auth_headers)
        response = restd.post(
            f"/slurmdb/{V}/users_association/",
            headers=auth_headers,
            json={
                "association_condition": {
                    "users": ["fay"],
                    "accounts": ["proj1"],
                    "association": {"qoslevel": "normal"},
                },
                "user": {},
            },
        )
        assert response.status_code == 200
        assert "added_users" not in response.json()
        assert response.json()["warnings"][0]["source"] == (
            "#/association_condition/association/qoslevel/"
        )
        assocs = restd.get(
            f"/slurmdb/{V}/associations/", headers=auth_headers, params={"user": "fay"}
        ).json()["associations"]
        assert assocs == []

    def test_adding_existing_association_is_not_modified(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        body = {"association_condition": {"users": ["gus"], "accounts": ["proj1"]}, "user": {}}
        restd.post(f"/slurmdb/{V}/users_association/", headers=auth_headers, json=body)
        response = restd.post(f"/slurmdb/{V}/users_association/", headers=auth_headers, json=body)
        assert response.status_code == 304


class TestAccountLevelAssociationWrites:
    """qos / default qos / shares_raw via POST /associations/ (ASSOC fields).

    These are settable on associations in real slurmrestd (data_parser
    {V} slurm://src/plugins/data_parser/{V}/parsers.c#"default/qos"@26.05+: "qos", "default/qos", "shares_raw").
    """

    def _create_account(self, restd, auth_headers, name="proj1"):
        restd.post(
            f"/slurmdb/{V}/accounts/",
            headers=auth_headers,
            json={"accounts": [{"name": name, "description": name, "organization": "org1"}]},
        )
        restd.post(
            f"/slurmdb/{V}/qos/",
            headers=auth_headers,
            json={"qos": [{"name": "fast"}, {"name": "slow"}, {"name": "stop"}]},
        )

    def _account_assoc(self, restd, auth_headers, name="proj1"):
        assocs = restd.get(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            params={"account": name},
        ).json()["associations"]
        return next(a for a in assocs if not a["user"])

    def test_qos_list_roundtrip(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            json={"associations": [{"account": "proj1", "user": "", "qos": ["fast", "slow"]}]},
        )
        assoc = self._account_assoc(restd, auth_headers)
        assert assoc["qos"] == ["fast", "slow"]

    def test_default_qos_roundtrip(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            json={
                "associations": [
                    {
                        "account": "proj1",
                        "user": "",
                        "qos": ["normal", "fast"],
                        "default": {"qos": "fast"},
                    }
                ]
            },
        )
        assoc = self._account_assoc(restd, auth_headers)
        assert assoc["default"]["qos"] == "fast"

    def test_default_qos_outside_list_is_rejected(self, restd, auth_headers):
        """Slurmdbd's DefaultQOS check applies to REST updates too.

        slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#_foreach_check_default_qos
        runs after the update is applied and rolls it back when an effective
        default is not in the QoS list.
        """
        self._create_account(restd, auth_headers)
        before = self._account_assoc(restd, auth_headers)
        response = restd.post(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            json={"associations": [{"account": "proj1", "user": "", "default": {"qos": "fast"}}]},
        )
        assert response.status_code == http_status_for(ESLURM_NO_REMOVE_DEFAULT_QOS)
        # slurmrestd reports only that the modify failed; the errno carries why.
        assert response.json()["errors"][0]["description"] == (
            "slurmdb_associations_modify() failed"
        )
        after = self._account_assoc(restd, auth_headers)
        assert after["qos"] == before["qos"]
        assert after["default"]["qos"] == before["default"]["qos"]

    def test_qos_list_swap_without_default_is_rejected_and_rolled_back(self, restd, auth_headers):
        """The site-agent pause swap (``qos=[stop]``) on an account with a default."""
        self._create_account(restd, auth_headers)
        restd.post(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            json={"associations": [{"account": "proj1", "user": "", "default": {"qos": "normal"}}]},
        )
        response = restd.post(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            json={"associations": [{"account": "proj1", "user": "", "qos": ["stop"]}]},
        )
        assert response.status_code == http_status_for(ESLURM_NO_REMOVE_DEFAULT_QOS)
        assoc = self._account_assoc(restd, auth_headers)
        assert assoc["qos"] == ["normal"]
        assert assoc["default"]["qos"] == "normal"

        response = restd.post(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            json={
                "associations": [
                    {"account": "proj1", "user": "", "qos": ["stop"], "default": {"qos": "stop"}}
                ]
            },
        )
        assert response.status_code == 200
        assoc = self._account_assoc(restd, auth_headers)
        assert assoc["qos"] == ["stop"]
        assert assoc["default"]["qos"] == "stop"

    def test_shares_raw_roundtrip_plain_and_tristate(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            json={"associations": [{"account": "proj1", "user": "", "shares_raw": 42}]},
        )
        assert self._account_assoc(restd, auth_headers)["shares_raw"] == 42

        restd.post(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            json={
                "associations": [
                    {"account": "proj1", "user": "", "shares_raw": {"set": True, "number": 7}}
                ]
            },
        )
        assert self._account_assoc(restd, auth_headers)["shares_raw"] == 7

    def test_visible_to_cli(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            json={"associations": [{"account": "proj1", "user": "", "shares_raw": 99}]},
        )
        database = SlurmDatabase()
        database.load_state()
        assert database.get_account("proj1").fairshare == 99
