"""Opt-in identity resolution through the OS name service switch.

By default the emulator has no uid/gid model: every user is ``1000`` and
its own group. Setting ``SLURM_EMULATOR_NSS=1`` makes the emulator ask
the OS (``getpwnam``/``getgrgid`` — libc NSS, which inside the container
is backed by sssd → LDAP) instead, the same calls real Slurm makes
(slurm://src/common/uid.c#uid_from_string, slurm://src/common/uid.c#gid_to_string).
A client that authenticates as user X then sees X's real uid/gid in
``id``, ``scontrol show job``, ``sacct`` and the slurmrestd job views.

Positive lookups are cached for the life of the process like
slurm://src/common/uid.c#uid_from_string_cached@26.05+; misses are not, so a
user created in the directory after a failed lookup resolves on retry.
Inert when the variable is unset: :func:`lookup` returns ``None`` and no
NSS call is made.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

try:
    import grp
    import pwd
except ImportError:  # pragma: no cover - no passwd database on this platform
    grp = None  # type: ignore[assignment]
    pwd = None  # type: ignore[assignment]

ENV_VAR = "SLURM_EMULATOR_NSS"

_TRUE = {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Identity:
    """One passwd entry plus the resolved primary group."""

    name: str
    uid: int
    gid: int
    group: str
    gecos: str = ""
    home: str = ""
    shell: str = ""
    # Every gid the user belongs to, primary first (``initgroups`` order).
    groups: tuple[int, ...] = ()

    @property
    def proper_name(self) -> str:
        """First comma field of gecos — what sreport prints as ``Proper Name``.

        slurm://src/sreport/cluster_reports.c#_cluster_account_by_user_tres_report
        uses ``strtok(pw_gecos, ",")``.
        """
        return self.gecos.split(",", 1)[0].strip()


_cache: dict[str, Identity] = {}


def enabled() -> bool:
    """True when ``SLURM_EMULATOR_NSS`` asks for real identity resolution."""
    return os.environ.get(ENV_VAR, "").strip().lower() in _TRUE


def clear_cache() -> None:
    _cache.clear()


def group_name(gid: int) -> str:
    """Group name for ``gid``, falling back to the number like ``gid_to_string``."""
    if grp is None:
        return str(gid)
    try:
        return grp.getgrgid(gid).gr_name
    except (KeyError, OverflowError):
        return str(gid)


def resolve(name: str) -> Optional[Identity]:
    """Look ``name`` up in the OS passwd database; ``None`` when unknown.

    Always performs the lookup regardless of :func:`enabled` — callers
    that must stay inert use :func:`lookup`.
    """
    if not name:
        return None
    cached = _cache.get(name)
    if cached is not None:
        return cached
    if pwd is None:
        return None
    try:
        entry = pwd.getpwnam(name)
    except KeyError:
        return None
    try:
        groups: tuple[int, ...] = tuple(os.getgrouplist(name, entry.pw_gid))
    except (AttributeError, OSError):
        groups = (entry.pw_gid,)
    if entry.pw_gid in groups:
        groups = (entry.pw_gid, *(g for g in groups if g != entry.pw_gid))
    identity = Identity(
        name=entry.pw_name,
        uid=entry.pw_uid,
        gid=entry.pw_gid,
        group=group_name(entry.pw_gid),
        gecos=entry.pw_gecos or "",
        home=entry.pw_dir or "",
        shell=entry.pw_shell or "",
        groups=groups,
    )
    _cache[name] = identity
    return identity


def lookup(name: str) -> Optional[Identity]:
    """:func:`resolve` when NSS mode is on; ``None`` (no OS call) otherwise."""
    if not enabled():
        return None
    return resolve(name)
