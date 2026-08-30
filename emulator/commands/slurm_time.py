"""Time-spec parsing shared by the sacct and sreport emulators.

Port of the common ``parse_time()`` forms
(slurm://src/common/parse_time.c#parse_time): ISO dates/datetimes,
``HH:MM[:SS]`` (today), ``now[{+|-}count[unit]]``, ``today``,
``midnight`` — all resolved against the *simulated* clock.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta


def parse_time_spec(time_str: str, now: datetime) -> datetime:
    """Parse one time spec relative to ``now``; raises ValueError when bogus."""
    text = time_str.strip()
    lowered = text.lower()

    if lowered in {"today", "midnight"}:
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if lowered.startswith("now"):
        rest = lowered[3:]
        if not rest:
            return now
        match = re.fullmatch(r"([+-])(\d+)([a-z]*)", rest)
        if match is None:
            raise ValueError(rest)
        sign = 1 if match.group(1) == "+" else -1
        count = int(match.group(2))
        unit = match.group(3)
        seconds_per = {
            "": 60,  # bare count = minutes, like parse_time()
            "seconds": 1,
            "minutes": 60,
            "hours": 3600,
            "days": 86400,
            "weeks": 604800,
        }
        for name, secs in seconds_per.items():
            if name.startswith(unit) and (name or not unit):
                return now + timedelta(seconds=sign * count * secs)
        raise ValueError(unit)
    if "T" in text:
        return datetime.fromisoformat(text)
    if "-" in text:
        try:
            return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return datetime.strptime(text, "%Y-%m-%d")
    if ":" in text:
        parts = [int(p) for p in text.split(":")]
        hour, minute = parts[0], parts[1]
        second = parts[2] if len(parts) > 2 else 0
        return now.replace(hour=hour, minute=minute, second=second, microsecond=0)
    raise ValueError(text)
