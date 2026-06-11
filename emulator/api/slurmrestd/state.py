"""Per-request emulator state for the slurmrestd app.

Mirrors the CLI commands' persistence model (dispatcher.py): every
request reloads the JSON state files, mutating handlers call
``commit()`` at the end. Concurrency is whole-file last-writer-wins —
``save_state``/``load_state`` hold an flock so writes are never torn,
but a REST write racing a CLI write can lose the other side's changes.
That matches the existing CLI-vs-control-API behavior and is fine for
a single-tester emulator.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends

from emulator.core.database import SlurmDatabase
from emulator.core.time_engine import TimeEngine


class RequestState:
    """Fresh database/time-engine view loaded for one request."""

    def __init__(self) -> None:
        self.time_engine = TimeEngine()
        self.database = SlurmDatabase()
        self.database.load_state()

    def commit(self) -> None:
        self.database.save_state()

    @property
    def cluster(self) -> str:
        return self.database.current_cluster

    def now_ts(self) -> int:
        """Current simulated time as a UNIX timestamp."""
        return int(self.time_engine.get_current_time().timestamp())


def get_state() -> RequestState:
    """FastAPI dependency: one RequestState per request."""
    return RequestState()


# Annotated dependency for handler signatures.
StateDep = Annotated[RequestState, Depends(get_state)]
