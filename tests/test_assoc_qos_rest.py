"""Tests for per-association QoS grants over the slurmrestd REST plane.

Validates emulator parity with real Slurm (see docs/slurm-parity.md):

- A user association (slurmdb_assoc_rec_t) carries its own ``qos_list`` and
  ``def_qos_id``. Over data_parser {V} these are the ASSOC ``qos`` list
  and ``default/qos`` fields (slurm://src/plugins/data_parser/{V}/parsers.c#"default/qos"@26.05+). POST /associations/ for a
  user row now stores them on the association, and GET renders them from the
  association (falling back to the account QOS when the row has no grant).
"""

from emulator.api.slurmrestd import schemas
from emulator.core.database import Account, Association
from emulator.slurm_version import current

V = current().api_version


class TestAssocToDictGrant:
    def test_user_assoc_renders_own_qos(self):
        account = Account(name="proj1", description="p", organization="o", qos="normal")
        assoc = Association(
            account="proj1", user="alice", qos_list=["normal", "boost"], def_qos="boost"
        )
        rendered = schemas.assoc_to_dict(assoc, account)
        assert rendered["qos"] == ["normal", "boost"]
        assert rendered["default"]["qos"] == "boost"

    def test_user_assoc_without_grant_falls_back_to_account(self):
        account = Account(name="proj1", description="p", organization="o", qos="normal")
        assoc = Association(account="proj1", user="alice")
        rendered = schemas.assoc_to_dict(assoc, account)
        # No explicit grant: the user row shows no QoS of its own.
        assert rendered["qos"] == []


class TestAssocQosRestRoundTrip:
    def _create_account(self, restd, auth_headers, name="proj1"):
        restd.post(
            f"/slurmdb/{V}/accounts/",
            headers=auth_headers,
            json={"accounts": [{"name": name, "description": name, "organization": "org1"}]},
        )

    def _user_assoc(self, restd, auth_headers, user="alice", name="proj1"):
        assocs = restd.get(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            params={"account": name, "user": user},
        ).json()["associations"]
        return next(a for a in assocs if a["user"] == user)

    def test_user_assoc_qos_list_roundtrip(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            json={
                "associations": [
                    {
                        "account": "proj1",
                        "user": "alice",
                        "qos": ["normal", "boost"],
                        "default": {"qos": "boost"},
                    }
                ]
            },
        )
        assoc = self._user_assoc(restd, auth_headers)
        assert assoc["qos"] == ["normal", "boost"]
        assert assoc["default"]["qos"] == "boost"

    def test_user_assoc_partition_scoped_qos(self, restd, auth_headers):
        self._create_account(restd, auth_headers)
        restd.post(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            json={
                "associations": [
                    {
                        "account": "proj1",
                        "user": "alice",
                        "partition": "gpu",
                        "qos": ["gp_debug"],
                    }
                ]
            },
        )
        assocs = restd.get(
            f"/slurmdb/{V}/associations/",
            headers=auth_headers,
            params={"account": "proj1", "user": "alice", "partition": "gpu"},
        ).json()["associations"]
        assert len(assocs) == 1
        assert assocs[0]["qos"] == ["gp_debug"]
