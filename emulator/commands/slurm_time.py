"""Time-spec parsing shared by sacct, sreport, scontrol and slurmrestd.

A line-by-line port of ``parse_time()`` (slurm://src/common/parse_time.c#parse_time),
resolved against the emulator's clock instead of ``time(NULL)``:

* ``today``, ``tomorrow``, ``midnight``, ``noon``, ``elevenses``, ``fika``,
  ``teatime`` (matched case-insensitively, but a leading ``T`` is skipped as
  the ISO separator first, exactly as Slurm does);
* ``HH:MM[:SS] [AM|PM]``, ``MMDD[YY]``, ``MM/DD[/YY]``, ``MM.DD[.YY]``,
  ``YYYY-MM-DD[THH:MM[:SS]]`` and combinations;
* ``now[{+|-}count[seconds|minutes|hours|days|weeks]]`` -- a bare count is
  **seconds**, and units must be spelled out (``second``/``seconds`` …), as in
  slurm://src/common/parse_time.c#_get_delta;
* ``uts<epoch>``.

A time without a date is the next such time (today or tomorrow), or the last
one when ``past`` is set; a date without a year is the next (or last) such
date. Day overflow normalises like ``mktime()`` (Feb 31 -> Mar 3).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

# slurm://src/common/parse_time.c#un -- longest name first, matched by prefix.
_UNITS = (
    ("seconds", 1),
    ("second", 1),
    ("minutes", 60),
    ("minute", 60),
    ("hours", 3600),
    ("hour", 3600),
    ("days", 86400),
    ("day", 86400),
    ("weeks", 604800),
    ("week", 604800),
)

_NAMED_TIMES = (
    ("midnight", 0),
    ("noon", 12),
    ("elevenses", 11),
    ("fika", 15),
    ("teatime", 16),
)

_MIN_UTS = 1000000


class ParseTimeError(ValueError):
    """Invalid time specification; ``pos`` is what Slurm prints as ``pos=``."""

    def __init__(self, text: str, pos: int):
        super().__init__(f"Invalid time specification (pos={pos}): {text}")
        self.text = text
        self.pos = pos


class _Prob(Exception):  # noqa: N818 - mirrors the C ``goto prob``
    def __init__(self, pos: int):
        super().__init__(pos)
        self.pos = pos


def _at(s: str, i: int) -> str:
    """The C string's char at ``i``: NUL past the end."""
    return s[i] if 0 <= i < len(s) else "\0"


def _digit(c: str) -> bool:
    return "0" <= c <= "9"


def _blank(c: str) -> bool:
    return c in " \t"


def _starts(s: str, i: int, word: str) -> bool:
    return s[i : i + len(word)].lower() == word


def _get_delta(s: str, pos: int) -> tuple[int, int]:
    """slurm://src/common/parse_time.c#_get_delta -> (new pos, seconds)."""
    cnt = 0
    digits = 0
    offset = pos + 1
    while _at(s, offset) not in ("\0", "\n"):
        c = _at(s, offset)
        if c.isspace():
            offset += 1
            continue
        unit = next(((n, m) for n, m in _UNITS if _starts(s, offset, n)), None)
        if unit is not None:
            offset += len(unit[0])
            cnt *= unit[1]
            break
        if _digit(c):
            cnt = cnt * 10 + int(c)
            digits += 1
            offset += 1
            continue
        raise _Prob(offset - 1)
    if not digits:
        raise _Prob(pos)
    return offset - 1, cnt


def _two(s: str, offset: int, required: bool) -> tuple[int, int]:
    """One digit, then an optional (or ``required``) second one."""
    if not _digit(_at(s, offset)):
        raise _Prob(offset)
    value = int(s[offset])
    offset += 1
    if _digit(_at(s, offset)):
        value = value * 10 + int(s[offset])
        offset += 1
    elif required:
        raise _Prob(offset)
    return value, offset


def _get_time(s: str, pos: int) -> tuple[int, int, int, int]:
    """slurm://src/common/parse_time.c#_get_time -> (pos, hour, minute, second)."""
    offset = pos
    if not _digit(_at(s, offset)):
        raise _Prob(offset)
    hr = int(s[offset])
    offset += 1
    if _at(s, offset) != ":":
        if not _digit(_at(s, offset)):
            raise _Prob(offset)
        hr = hr * 10 + int(s[offset])
        offset += 1
    if hr > 23:
        raise _Prob(offset - 2)
    if _at(s, offset) != ":":
        raise _Prob(offset)
    offset += 1
    mins, offset = _two(s, offset, required=True)
    if mins > 59:
        raise _Prob(offset - 2)
    sec = 0
    if _at(s, offset) == ":":
        sec, offset = _two(s, offset + 1, required=True)
        if sec > 59:
            raise _Prob(offset - 2)
    while _at(s, offset).isspace() and _at(s, offset) != "\0":
        offset += 1
    if _starts(s, offset, "pm"):
        hr += 12
        if hr > 23:
            if hr != 24:
                raise _Prob(offset)
            hr = 12
        offset += 2
    elif _starts(s, offset, "am"):
        if hr > 11:
            if hr != 12:
                raise _Prob(offset)
            hr = 0
        offset += 2
    return offset - 1, hr, mins, sec


def _get_date(s: str, pos: int) -> tuple[int, int, int, Optional[int]]:
    """slurm://src/common/parse_time.c#_get_date -> (pos, month 0-11, mday, year).

    ``year`` is the full year, or None when not given (or given as ``00``,
    which Slurm cannot tell apart from "not given").
    """
    offset = pos
    if len(s) >= offset + 7 and _at(s, offset + 4) == "-" and _at(s, offset + 7) == "-":
        for i in range(4):
            if not _digit(_at(s, offset + i)):
                raise _Prob(offset + i)
        yr = int(s[offset : offset + 4])
        offset += 5
        # No digit check on the first month char, as in C.
        mon, offset = ord(_at(s, offset)) - ord("0"), offset + 1
        if _digit(_at(s, offset)):
            mon = mon * 10 + int(s[offset])
            offset += 1
        if not 1 <= mon <= 12:
            raise _Prob(offset - 2)
        offset += 1
        day, offset = _two(s, offset, required=False)
        if not 1 <= day <= 31:
            raise _Prob(offset - 2)
        return offset - 1, mon - 1, day, yr

    mon, offset = ord(_at(s, offset)) - ord("0"), offset + 1
    if _digit(_at(s, offset)):
        mon = mon * 10 + int(s[offset])
        offset += 1
    if not 1 <= mon <= 12:
        raise _Prob(offset - 2)
    if _at(s, offset) in "./" and _at(s, offset) != "\0":
        offset += 1
    day, offset = _two(s, offset, required=False)
    if not 1 <= day <= 31:
        raise _Prob(offset - 2)
    if _at(s, offset) in "./" and _at(s, offset) != "\0":
        offset += 1
    year: Optional[int] = None
    if _digit(_at(s, offset)):
        yr, offset = _two(s, offset, required=True)
        year = 2000 + yr if yr else None
    return offset - 1, mon - 1, day, year


def _mktime(year: int, month: int, mday: int, hour: int, minute: int, second: int) -> datetime:
    """``mktime()`` with its normalisation of out-of-range days."""
    return datetime(year, month + 1, 1, hour, minute, second) + timedelta(days=mday - 1)


def parse_time(text: str, now: datetime, past: bool = False) -> Optional[datetime]:
    """slurm://src/common/parse_time.c#parse_time on the emulator's clock.

    Returns None for "nothing specified" (Slurm's time 0, e.g. an empty
    string); raises ParseTimeError where Slurm prints "Invalid time
    specification".
    """
    try:
        return _parse(text, now.replace(microsecond=0), past)
    except _Prob as prob:
        raise ParseTimeError(text, prob.pos) from None


def _parse(s: str, now: datetime, past: bool) -> Optional[datetime]:
    if _starts(s, 0, "uts"):
        rest = s[3:]
        digits = rest.lstrip(" \t\n").removeprefix("+")
        if not digits.lstrip("-").isdigit() or int(digits) < _MIN_UTS:
            raise _Prob(0)
        rest = digits
        return datetime.fromtimestamp(int(rest))  # noqa: DTZ006 - naive local, like the job clock

    hour = minute = month = mday = -1
    second = 0
    year: Optional[int] = None
    pos = 0
    while _at(s, pos) not in ("\0", "\n"):
        c = _at(s, pos)
        if _blank(c) or c in "-T":
            pos += 1
            continue
        if _starts(s, pos, "today"):
            month, mday, year = now.month - 1, now.day, now.year
            pos += 5
            continue
        if _starts(s, pos, "tomorrow"):
            later = now + timedelta(days=1)
            month, mday, year = later.month - 1, later.day, later.year
            pos += 8
            continue
        named = next(((w, h) for w, h in _NAMED_TIMES if _starts(s, pos, w)), None)
        if named is not None:
            hour, minute, second = named[1], 0, 0
            pos += len(named[0])
            continue
        if _starts(s, pos, "now"):
            delta = 0
            i = pos + 3
            while True:
                ci = _at(s, i)
                if ci in "+-" and ci != "\0":
                    # Slurm adds the absolute index to pos, so a "now" that is
                    # not at the start of the string parses from past it.
                    pos += i
                    pos, delta = _get_delta(s, pos)
                    if ci == "-":
                        delta = -delta
                    break
                if _blank(ci):
                    i += 1
                    continue
                if ci in ("\0", "\n"):
                    pos += i - 1
                    break
                raise _Prob(pos + i)
            later = now + timedelta(seconds=delta)
            month, mday, year = later.month - 1, later.day, later.year
            hour, minute, second = later.hour, later.minute, later.second
            pos += 1
            continue
        if not _digit(c):
            raise _Prob(pos)
        if _at(s, pos + 1) == ":" or _at(s, pos + 2) == ":":
            pos, hour, minute, second = _get_time(s, pos)
        else:
            pos, month, mday, date_year = _get_date(s, pos)
            if date_year is not None:
                year = date_year
        pos += 1

    if hour == -1 and month == -1:
        return None
    if hour == -1:
        hour, minute = 0, 0
    elif month == -1:
        # A time without a date is the soonest such time.
        if past or (hour, minute) > (now.hour, now.minute):
            month, mday, year = now.month - 1, now.day, now.year
        else:
            later = now + timedelta(days=1)
            month, mday, year = later.month - 1, later.day, later.year
    if year is None:
        if past:
            year = now.year - 1 if month > now.month - 1 else now.year
        elif (month, mday, hour, minute) > (now.month - 1, now.day, now.hour, now.minute):
            year = now.year
        else:
            year = now.year + 1
    return _mktime(year, month, mday, hour, minute, second)


def parse_time_spec(time_str: str, now: datetime, past: bool = False) -> datetime:
    """``parse_time`` for callers that need a time: "nothing specified" is an error."""
    result = parse_time(time_str, now, past)
    if result is None:
        raise ParseTimeError(time_str, -1)
    return result
