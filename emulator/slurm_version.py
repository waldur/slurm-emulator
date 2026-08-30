"""Which real Slurm release the emulator is currently standing in for.

The emulator is traced against several Slurm versions (see
``docs/slurm-parity.md`` and ``[tool.slurm-parity]`` in ``pyproject.toml``).
Most behaviour is identical across that window; where real Slurm differs
between releases, code branches on :func:`at_least` and cites the version in
its ``slurm://...@X.Y+`` reference.

Selection: ``SLURM_EMULATOR_SLURM_VERSION`` (e.g. ``25.11``), falling back to
:data:`PRIMARY_VERSION`. ``master`` is the not-yet-released next version and
compares newer than every numbered release.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

ENV_VAR = "SLURM_EMULATOR_SLURM_VERSION"

# Keep in sync with [tool.slurm-parity].primary in pyproject.toml
# (tests/test_slurm_parity_tooling.py enforces this).
PRIMARY_VERSION = "26.05"


@dataclass(frozen=True)
class SlurmRelease:
    """What one tracked Slurm version looks like from the outside."""

    version: str  # "26.05" / "master"
    release: str  # meta.slurm.release / slurm_version in /conf: latest tag on the branch
    api_version: str  # data_parser version that release's slurmrestd serves (META API_CURRENT)

    @property
    def version_parts(self) -> dict[str, str]:
        """``meta.slurm.version`` triple, as slurmrestd renders it from SLURM_VERSION_*."""
        major, minor, micro = self.release.split(".", 2)
        return {"major": major, "micro": micro, "minor": minor}

    @property
    def previous_api_version(self) -> str:
        """The data_parser version one older than ours (for "unknown URL" tests)."""
        prefix, _, last = self.api_version.rpartition(".")
        return f"{prefix}.{int(last) - 1}"


# One entry per [tool.slurm-parity] version. ``release`` is the newest release
# tag on the maintenance branch (``master`` is the pre-release of the next
# major); ``api_version`` is META's API_CURRENT for that branch.
RELEASES: dict[str, SlurmRelease] = {
    "24.11": SlurmRelease("24.11", "24.11.7", "v0.0.42"),
    "25.05": SlurmRelease("25.05", "25.05.8", "v0.0.43"),
    "25.11": SlurmRelease("25.11", "25.11.7", "v0.0.44"),
    "26.05": SlurmRelease("26.05", "26.05.3", "v0.0.45"),
    "master": SlurmRelease("master", "26.11.0", "v0.0.46"),
}


def _key(version: str) -> tuple[int, int]:
    if version == "master":
        return (9999, 0)
    major, minor = version.split(".", 1)
    return (int(major), int(minor))


def get_target_version() -> str:
    """Return the Slurm version the emulator currently emulates."""
    value = os.environ.get(ENV_VAR, "").strip() or PRIMARY_VERSION
    if value not in RELEASES:
        raise ValueError(
            f"{ENV_VAR}={value!r} is not a tracked Slurm version; choose one of {sorted(RELEASES)}"
        )
    return value


def current() -> SlurmRelease:
    """Release descriptor for the active target version."""
    return RELEASES[get_target_version()]


def at_least(version: str) -> bool:
    """True when the target version is ``version`` or newer (``master`` is newest)."""
    return _key(get_target_version()) >= _key(version)


def matches(*specs: str) -> bool:
    """True when the target version satisfies any spec (``"25.11"``, ``"25.11+"``)."""
    target = _key(get_target_version())
    for spec in specs:
        if spec.endswith("+"):
            if target >= _key(spec[:-1]):
                return True
        elif target == _key(spec):
            return True
    return False
