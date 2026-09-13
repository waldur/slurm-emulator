"""``AccountingStorageEnforce=associations`` for job submission.

Real slurmctld resolves every submission to an association — the
(user, account, partition) row in slurmdbd — before it accepts the job
(slurm://src/slurmctld/job_mgr.c#_job_create →
slurm://src/common/assoc_mgr.c#assoc_mgr_fill_in_assoc). With
``associations`` (or anything implying it: ``limits``, ``qos``, ``safe``,
``wckeys``) in ``AccountingStorageEnforce``, a user with no association for
the requested account is refused with ``ESLURM_INVALID_ACCOUNT``; without
it, slurmctld silently falls back to the user's default account.

slurm.conf leaves ``AccountingStorageEnforce`` unset, but every production
cluster a site agent manages sets ``associations`` — and the case the flag
exists for (a person removed from a project can no longer run jobs against
it) is exactly what integration tests need to observe. The emulator
therefore defaults to ``associations`` (:data:`DEFAULT`) everywhere — library,
image and chart alike — and ``SLURM_EMULATOR_ACCOUNTING_ENFORCE=none``
restores the permissive pre-0.10 behaviour where any user could submit
against any account and fell back to ``root``. The value is parsed like
slurm://src/common/read_config.c#_validate_accounting_storage_enforce.

:func:`admit_job` is the single admission path for ``POST /job/submit`` and
``sbatch``: resolve the user (NSS mode, the USER_ID parser step,
slurm://src/plugins/data_parser/v0.0.45/parsers.c#USER_ID@26.05+), then the
association.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from emulator.core import nss
from emulator.core.database import Association, SlurmDatabase, fold_account

ENV_VAR = "SLURM_EMULATOR_ACCOUNTING_ENFORCE"
DEFAULT = "associations"

# Error numbers from slurm://slurm/slurm_errno.h, re-exported by the
# slurmrestd envelope module.
ESLURM_INVALID_ACCOUNT = 2045  # slurm://src/slurmctld/job_mgr.c#_job_create
ESLURM_USER_ID_UNKNOWN = 2166  # slurm://src/plugins/data_parser/v0.0.45/parsers.c#USER_ID@26.05+

# Tokens that set ACCOUNTING_ENFORCE_ASSOCS, directly or by implication
# (slurm://src/common/read_config.c#_validate_accounting_storage_enforce).
_ASSOC_TOKENS = {"1", "associations", "2", "limits", "safe", "wckeys", "qos", "all"}


def associations_enforced() -> bool:
    """True when submissions must match an association."""
    value = os.environ.get(ENV_VAR)
    if value is None:
        value = DEFAULT
    return any(tok.strip().lower() in _ASSOC_TOKENS for tok in value.split(","))


def user_associations(db: SlurmDatabase, user: str) -> list[Association]:
    """The user's association rows on the current cluster, in a stable order."""
    return sorted(
        (a for a in db.associations.values() if a.user == user and a.cluster == db.current_cluster),
        key=lambda a: (a.account, a.partition or ""),
    )


def resolve_job_account(
    db: SlurmDatabase, user: str, account: str, partition: str
) -> Optional[str]:
    """Account a job by ``user`` lands in, or ``None`` when enforcement refuses it.

    Mirrors ``assoc_mgr_fill_in_assoc``: an explicit account must have a
    row for the user that is either partition-less (``None``) or bound to
    ``partition``;
    no account means the user's default association. Without enforcement the
    legacy fallback chain (requested → default → ``root``) applies unchanged.
    """
    rows = user_associations(db, user)
    user_rec = db.get_user(user)
    default = (user_rec.default_account if user_rec else "") or ""
    if not associations_enforced():
        return account or default or "root"

    wanted = fold_account(account) if account else ""
    if not wanted:
        wanted = fold_account(default) if default else (rows[0].account if rows else "")
    for row in rows:
        if row.account == wanted and row.partition in (None, "", partition):
            return row.account
    return None


@dataclass(frozen=True)
class Admission:
    """Outcome of :func:`admit_job`: either an account (and identity) or an errno."""

    account: str = ""
    identity: Optional[nss.Identity] = None
    error: Optional[int] = None
    message: str = ""

    @property
    def accepted(self) -> bool:
        return self.error is None


def admit_job(db: SlurmDatabase, user: str, account: str, partition: str) -> Admission:
    """Admit ``user``'s submission, in slurmctld order: identity first, then association.

    Blocking (NSS may hit the directory) — async callers run it in a thread.
    """
    identity = nss.lookup(user)
    if nss.enabled() and identity is None:
        return Admission(error=ESLURM_USER_ID_UNKNOWN, message=f"Unable to resolve user: {user}")
    resolved = resolve_job_account(db, user, account, partition)
    if resolved is None:
        return Admission(
            error=ESLURM_INVALID_ACCOUNT,
            message=(
                "Invalid account or account/partition combination specified for user "
                f"{user}, account '{account}', partition '{partition}'"
            ),
        )
    return Admission(account=resolved, identity=identity)
