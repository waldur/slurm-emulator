"""Shared configuration for the Slurm source-parity tooling.

Reads ``[tool.slurm-parity]`` from ``pyproject.toml`` (the single source of
truth for which upstream versions the emulator is traced against) and resolves
the on-disk cache layout:

    $SLURM_SRC_CACHE/               (default ~/.cache/slurm-emulator)
        slurm.git/                  bare clone of github.com/SchedMD/slurm
        slurm-26.05/                worktree of origin/slurm-26.05
        slurm-25.11/                worktree of origin/slurm-25.11
        ...
        master/                     worktree of origin/master (next release, pre-release)

Used by ``scripts/slurm_src.py`` (cache management) and
``scripts/check_slurm_refs.py`` (reference validation).
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

try:  # Python >= 3.11
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.9/3.10
    import tomli as tomllib  # type: ignore[no-redef]

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CACHE = Path.home() / ".cache" / "slurm-emulator"


@dataclass
class ParityConfig:
    remote: str
    primary: str
    versions: list[str]
    scan_paths: list[str] = field(default_factory=list)
    cache: Path = DEFAULT_CACHE

    @property
    def bare_repo(self) -> Path:
        return self.cache / "slurm.git"

    def worktree(self, version: str) -> Path:
        return self.cache / self.dirname(version)

    @staticmethod
    def dirname(version: str) -> str:
        """``26.05`` -> ``slurm-26.05``; ``master`` stays ``master``."""
        return version if version == "master" else f"slurm-{version}"

    @staticmethod
    def upstream_branch(version: str) -> str:
        """Upstream maintenance branch for a version (``slurm-XX.YY`` or ``master``)."""
        return version if version == "master" else f"slurm-{version}"

    def resolve_versions(self, spec: str | None) -> list[str]:
        """Expand a ``@`` version spec (``all``, ``26.05,25.11``, ``25.11+``)."""
        if not spec or spec == "all":
            return list(self.versions)
        out: list[str] = []
        for token in spec.split(","):
            token = token.strip()
            if token.endswith("+"):
                base = token[:-1]
                # "25.11+" = 25.11 and every newer release (master counts as newest).
                out.extend(v for v in self.versions if v == "master" or v >= base)
            else:
                out.append(token)
        unknown = [v for v in out if v not in self.versions]
        if unknown:
            raise ValueError(f"unknown Slurm version(s) {unknown}; tracked: {self.versions}")
        # Preserve tracked order, drop duplicates.
        return [v for v in self.versions if v in out]


def load_config(pyproject: Path | None = None) -> ParityConfig:
    path = pyproject or REPO_ROOT / "pyproject.toml"
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    try:
        section = data["tool"]["slurm-parity"]
    except KeyError:
        sys.exit(f"{path}: missing [tool.slurm-parity] section")
    cache = Path(os.environ.get("SLURM_SRC_CACHE") or section.get("cache") or DEFAULT_CACHE)
    cfg = ParityConfig(
        remote=section.get("remote", "https://github.com/SchedMD/slurm.git"),
        primary=section["primary"],
        versions=list(section["versions"]),
        scan_paths=list(section.get("scan_paths", [])),
        cache=cache.expanduser(),
    )
    if cfg.primary not in cfg.versions:
        sys.exit(f"[tool.slurm-parity] primary={cfg.primary!r} is not in versions={cfg.versions}")
    return cfg
