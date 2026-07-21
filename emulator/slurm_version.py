"""Selectable emulated Slurm release.

``SLURM_EMULATOR_SLURM_VERSION`` picks which Slurm release the emulator
impersonates (default ``26.05``, the newest release SchedMD has shipped;
Slurm releases twice yearly as X.05/X.11, so 26.11 doesn't exist yet).
Version strings and accepted slurmrestd URL versions follow the selection;
response schemas do not change.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

ENV_VAR = "SLURM_EMULATOR_SLURM_VERSION"
DEFAULT_RELEASE = "26.05"


@dataclass(frozen=True)
class SlurmRelease:
    release: str  # e.g. "26.05.0"
    api_version: str  # newest data_parser, e.g. "v0.0.45"
    accepted_api_versions: tuple[str, ...]  # newest + 2 prior, like real slurmrestd
    rpc_version: int  # slurmdbd cluster protocol number

    @property
    def version(self) -> dict[str, str]:
        major, minor, micro = self.release.split(".")
        return {"major": major, "micro": micro, "minor": minor}

    @property
    def data_parser(self) -> str:
        return f"data_parser/{self.api_version}"


RELEASES: dict[str, SlurmRelease] = {
    "24.11": SlurmRelease("24.11.0", "v0.0.42", ("v0.0.40", "v0.0.41", "v0.0.42"), 10752),
    "25.05": SlurmRelease("25.05.0", "v0.0.43", ("v0.0.41", "v0.0.42", "v0.0.43"), 11008),
    "25.11": SlurmRelease("25.11.0", "v0.0.44", ("v0.0.42", "v0.0.43", "v0.0.44"), 11264),
    "26.05": SlurmRelease("26.05.0", "v0.0.45", ("v0.0.43", "v0.0.44", "v0.0.45"), 11520),
}


def get_selected_release() -> SlurmRelease:
    """Return the release selected via the env var (read on every call).

    Accepts "26.05" or "26.05.0". Raises ValueError for unknown values so
    startup fails fast instead of silently emulating the wrong release.
    """
    raw = os.environ.get(ENV_VAR, DEFAULT_RELEASE).strip()
    key = ".".join(raw.split(".")[:2])
    try:
        return RELEASES[key]
    except KeyError:
        supported = ", ".join(sorted(RELEASES))
        raise ValueError(
            f"{ENV_VAR}={raw!r} is not a supported Slurm release (supported: {supported})"
        ) from None
