"""parse_time() port: slurm://src/common/parse_time.c#parse_time."""

from datetime import datetime, timedelta

import pytest

from emulator.commands.slurm_time import ParseTimeError, parse_time, parse_time_spec

NOW = datetime(2026, 10, 17, 9, 30, 15)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("now", NOW),
        ("now+90", NOW + timedelta(seconds=90)),  # bare count = seconds
        ("now-90", NOW - timedelta(seconds=90)),
        ("now+1second", NOW + timedelta(seconds=1)),
        ("now+5minutes", NOW + timedelta(minutes=5)),
        ("now+5minute", NOW + timedelta(minutes=5)),
        ("now+2hours", NOW + timedelta(hours=2)),
        ("NOW+2HOURS", NOW + timedelta(hours=2)),
        ("now+3days", NOW + timedelta(days=3)),
        ("now-1week", NOW - timedelta(weeks=1)),
        ("now + 5 minutes", NOW + timedelta(minutes=5)),
        ("today", datetime(2026, 10, 17)),
        ("tomorrow", datetime(2026, 10, 18)),
        ("midnight", datetime(2026, 10, 18)),  # next midnight
        ("noon", datetime(2026, 10, 17, 12)),
        ("elevenses", datetime(2026, 10, 17, 11)),
        ("fika", datetime(2026, 10, 17, 15)),
        ("teatime", datetime(2026, 10, 17, 16)),
        ("10:00", datetime(2026, 10, 17, 10)),
        ("09:00", datetime(2026, 10, 18, 9)),  # passed today -> tomorrow
        ("9:45:30", datetime(2026, 10, 17, 9, 45, 30)),
        ("3:00pm", datetime(2026, 10, 17, 15)),
        ("12:00 am", datetime(2026, 10, 18, 0)),
        ("2026-12-01", datetime(2026, 12, 1)),
        ("2026-12-01T08:15", datetime(2026, 12, 1, 8, 15)),
        ("2026-12-01T08:15:09", datetime(2026, 12, 1, 8, 15, 9)),
        ("12/01", datetime(2026, 12, 1)),
        ("12.01.27", datetime(2027, 12, 1)),
        ("1201", datetime(2026, 12, 1)),
        ("01/05", datetime(2027, 1, 5)),  # already passed this year -> next
        ("12/01-10:00", datetime(2026, 12, 1, 10)),
        ("2026-02-31", datetime(2026, 3, 3)),  # mktime() normalises
        ("uts1800000000", datetime.fromtimestamp(1800000000)),  # noqa: DTZ006
    ],
)
def test_future_forms(text, expected):
    assert parse_time(text, NOW) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("10:00", datetime(2026, 10, 17, 10)),  # past keeps today
        ("01/05", datetime(2026, 1, 5)),
        ("12/01", datetime(2025, 12, 1)),  # later month -> last year
    ],
)
def test_past_forms(text, expected):
    assert parse_time(text, NOW, past=True) == expected


@pytest.mark.parametrize(
    ("text", "pos"),
    [
        ("now+1h", 4),  # abbreviations are not units
        ("now+5min", 4),
        ("now+", 3),  # no digits
        ("now+1hourx", 9),
        ("nowx", 3),
        ("yesterday", 0),
        ("25:00", 0),
        ("10:61", 3),
        ("13/01", 0),
        ("Today", 1),  # the leading T is skipped as the ISO separator
        ("uts99", 0),
    ],
)
def test_invalid(text, pos):
    with pytest.raises(ParseTimeError) as info:
        parse_time(text, NOW)
    assert info.value.pos == pos
    assert str(info.value) == f"Invalid time specification (pos={pos}): {text}"


def test_nothing_specified_is_time_zero():
    assert parse_time("", NOW) is None
    with pytest.raises(ParseTimeError):
        parse_time_spec("", NOW)
