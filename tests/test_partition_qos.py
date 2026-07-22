"""Tests for partition-level QoS gating (AllowQos / DenyQos / assigned QOS).

Validates emulator parity with real Slurm (/Users/ilja/workspace/slurm):

- A partition gates which QoS a job may request via ``AllowQos`` /
  ``DenyQos`` (part_record.h:65/79; slurm.conf.5 AllowQos/DenyQos). An
  empty ``AllowQos`` means ALL QoS are permitted; if ``AllowQos`` is set,
  ``DenyQos`` is not enforced ("If AllowQos is used then DenyQos will not
  be enforced", slurm.conf.5).
- A partition also owns a single ``QOS=`` (qos_char, part_record.h:106) —
  limit-extension only — surfaced as the partition's assigned QoS.
- The slurmrestd partition view exposes these under
  ``qos: {allowed, deny, assigned}`` (previously hardcoded empty).

Config seeding mirrors the topology (SLURM_EMULATOR_PARTITIONS): the
``SLURM_EMULATOR_PARTITION_QOS`` env var declares the per-partition gate as
``name=mode:csv`` entries (mode ∈ allow|deny|qos) separated by ``;``.
"""


from emulator.api.slurmrestd import schemas


class TestParsePartitionQos:
    def test_allow_list(self):
        cfg = schemas._parse_partition_qos("gpp=allow:gp_debug,gp_ehpc")
        assert cfg["gpp"]["allowed"] == "gp_debug,gp_ehpc"
        assert cfg["gpp"]["deny"] == ""

    def test_deny_list(self):
        cfg = schemas._parse_partition_qos("cpu=deny:long")
        assert cfg["cpu"]["deny"] == "long"
        assert cfg["cpu"]["allowed"] == ""

    def test_assigned_qos(self):
        cfg = schemas._parse_partition_qos("boost=qos:highmem")
        assert cfg["boost"]["assigned"] == "highmem"

    def test_multiple_partitions_and_merge(self):
        cfg = schemas._parse_partition_qos("gpp=allow:a,b;cpu=deny:long;gpp=qos:owned")
        assert set(cfg) == {"gpp", "cpu"}
        assert cfg["gpp"]["allowed"] == "a,b"
        assert cfg["gpp"]["assigned"] == "owned"
        assert cfg["cpu"]["deny"] == "long"

    def test_empty_spec(self):
        assert schemas._parse_partition_qos("") == {}


class TestPartitionAllowsQos:
    def test_no_config_allows_all(self):
        assert schemas.partition_allows_qos("anything", "whatever", {}) is True

    def test_allow_list_gates(self):
        cfg = {"gpp": {"allowed": "gp_debug,gp_ehpc", "deny": "", "assigned": ""}}
        assert schemas.partition_allows_qos("gpp", "gp_debug", cfg) is True
        assert schemas.partition_allows_qos("gpp", "gp_resa", cfg) is False

    def test_deny_list_gates(self):
        cfg = {"cpu": {"allowed": "", "deny": "long", "assigned": ""}}
        assert schemas.partition_allows_qos("cpu", "long", cfg) is False
        assert schemas.partition_allows_qos("cpu", "short", cfg) is True

    def test_allow_takes_precedence_over_deny(self):
        # SLURM: if AllowQos is set, DenyQos is not enforced.
        cfg = {"p": {"allowed": "a", "deny": "a", "assigned": ""}}
        assert schemas.partition_allows_qos("p", "a", cfg) is True
        assert schemas.partition_allows_qos("p", "b", cfg) is False

    def test_unknown_partition_allows_all(self):
        cfg = {"gpp": {"allowed": "a", "deny": "", "assigned": ""}}
        assert schemas.partition_allows_qos("other", "z", cfg) is True


class TestPartitionRendering:
    def test_default_partition_qos_empty(self, monkeypatch):
        monkeypatch.setattr(schemas, "PARTITION_QOS", {})
        # Use a real topology partition name.
        name = next(iter(schemas.PARTITION_RANGES))
        rendered = schemas.partition_to_dict(name)
        assert rendered["qos"] == {"allowed": "", "deny": "", "assigned": ""}

    def test_configured_partition_qos_rendered(self, monkeypatch):
        name = next(iter(schemas.PARTITION_RANGES))
        monkeypatch.setattr(
            schemas,
            "PARTITION_QOS",
            {name: {"allowed": "gp_debug,gp_ehpc", "deny": "", "assigned": "owned"}},
        )
        rendered = schemas.partition_to_dict(name)
        assert rendered["qos"]["allowed"] == "gp_debug,gp_ehpc"
        assert rendered["qos"]["assigned"] == "owned"


class TestPartitionQosRestEndpoint:
    def test_rest_partitions_expose_configured_qos(self, restd, auth_headers, monkeypatch):
        name = next(iter(schemas.PARTITION_RANGES))
        monkeypatch.setattr(
            schemas, "PARTITION_QOS", {name: {"allowed": "a,b", "deny": "", "assigned": ""}}
        )
        partitions = restd.get("/slurm/v0.0.46/partitions/", headers=auth_headers).json()[
            "partitions"
        ]
        by_name = {p["name"]: p for p in partitions}
        assert by_name[name]["qos"]["allowed"] == "a,b"


class TestSetPartitionQos:
    def test_setter_updates_gate_and_rendering(self, monkeypatch):
        monkeypatch.setattr(schemas, "PARTITION_QOS", {})
        name = next(iter(schemas.PARTITION_RANGES))
        schemas.set_partition_qos(name, allowed="a,b")
        assert schemas.partition_allows_qos(name, "a") is True
        assert schemas.partition_allows_qos(name, "z") is False
        assert schemas.partition_to_dict(name)["qos"]["allowed"] == "a,b"
