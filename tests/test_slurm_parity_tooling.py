"""Guards for the Slurm source-parity tooling (docs/slurm-parity.md).

These do not need the source cache: they check the configuration, the
reference grammar and the runtime version switch, so they run everywhere.

slurm-refs: ignore-file — the sample references below are fixtures, not claims.
"""

import sys
from pathlib import Path

import pytest

from emulator import slurm_version

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import check_slurm_refs  # noqa: E402
from slurm_parity_config import load_config  # noqa: E402


@pytest.fixture(scope="module")
def cfg():
    return load_config(REPO / "pyproject.toml")


def test_primary_constant_matches_pyproject(cfg):
    assert slurm_version.PRIMARY_VERSION == cfg.primary
    assert cfg.primary in cfg.versions


def test_resolve_version_specs(cfg):
    assert cfg.resolve_versions(None) == cfg.versions
    assert cfg.resolve_versions("all") == cfg.versions
    assert cfg.resolve_versions("master") == ["master"]
    assert cfg.resolve_versions("25.11+") == ["master", "26.05", "25.11"]
    assert cfg.resolve_versions("24.11,26.05") == ["26.05", "24.11"]
    with pytest.raises(ValueError, match="unknown Slurm version"):
        cfg.resolve_versions("23.11")


@pytest.mark.parametrize(
    ("text", "path", "anchor", "versions"),
    [
        ("slurm://src/sacctmgr/common.c", "src/sacctmgr/common.c", None, None),
        (
            "(slurm://src/sacctmgr/common.c#_get_print_field).",
            "src/sacctmgr/common.c",
            "_get_print_field",
            None,
        ),
        ('slurm://src/sacctmgr/common.c#"Def QOS",', "src/sacctmgr/common.c", '"Def QOS"', None),
        (
            "slurm://slurm/slurmdb.h#slurmdb_qos_rec_t.grace_time",
            "slurm/slurmdb.h",
            "slurmdb_qos_rec_t.grace_time",
            None,
        ),
        (
            "see slurm://src/x/parsers.c#ASSOC_SHORT@master.",
            "src/x/parsers.c",
            "ASSOC_SHORT",
            "master",
        ),
        (
            "slurm://src/slurmrestd/operations.c#f@25.11+ trailing",
            "src/slurmrestd/operations.c",
            "f",
            "25.11+",
        ),
        ("``slurm://src/sshare/process.c``", "src/sshare/process.c", None, None),
    ],
)
def test_reference_grammar(text, path, anchor, versions):
    m = check_slurm_refs.REF_RE.search(text)
    assert m, text
    assert m.group("path") == path
    assert m.group("anchor") == anchor
    assert m.group("versions") == versions


def test_legacy_line_refs_are_rejected():
    f = REPO / "tests" / "_tmp_legacy_ref_probe.py"
    f.write_text("# account_functions.c:727 and /Users/nobody/workspace/slurm\n")
    try:
        _, problems = check_slurm_refs.collect([f], Path("/nonexistent"))
    finally:
        f.unlink()
    assert len(problems) == 2
    assert "legacy line reference" in problems[0]
    assert "hard-coded local" in problems[1]


def test_anchor_matching():
    text = 'int foo(void);\nchar *bar_baz;\nprintf("Def QOS");\n'
    assert check_slurm_refs._anchor_found(text, "foo")
    assert not check_slurm_refs._anchor_found(text, "fo")
    assert check_slurm_refs._anchor_found(text, "bar_baz")
    assert not check_slurm_refs._anchor_found(text, "baz")
    assert check_slurm_refs._anchor_found(text, '"Def QOS"')
    assert not check_slurm_refs._anchor_found(text, '"Def Acct"')


def test_runtime_version_switch(monkeypatch):
    monkeypatch.delenv(slurm_version.ENV_VAR, raising=False)
    assert slurm_version.get_target_version() == slurm_version.PRIMARY_VERSION
    monkeypatch.setenv(slurm_version.ENV_VAR, "24.11")
    assert slurm_version.get_target_version() == "24.11"
    assert not slurm_version.at_least("25.11")
    assert slurm_version.at_least("24.11")
    assert slurm_version.matches("24.11", "25.05")
    assert not slurm_version.matches("25.11+")
    monkeypatch.setenv(slurm_version.ENV_VAR, "master")
    assert slurm_version.at_least("26.05")
    assert slurm_version.matches("25.11+")


@pytest.mark.slurm_version("25.11+")
def test_marker_skips_when_target_is_older():
    assert slurm_version.at_least("25.11")


def test_all_references_in_repo_parse_cleanly(cfg):
    """Every slurm:// ref must resolve to a tracked version spec (no cache needed)."""
    files = check_slurm_refs.iter_files(cfg)
    refs, problems = check_slurm_refs.collect(files, REPO / "scripts" / "check_slurm_refs.py")
    assert problems == []
    assert refs, "expected slurm:// references in the repo"
    for ref in refs:
        cfg.resolve_versions(ref.versions)
