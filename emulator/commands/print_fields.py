"""Shared fixed-width/parsable field rendering for command emulators.

Mirrors real Slurm's ``src/common/print_fields.c``:

* default mode prints a header row plus a dash row, each column padded
  with ``printf("%*.*s ", len, abs_len, value)`` semantics — note the
  trailing space after *every* column, including the last one;
* string cells longer than the column are truncated to
  ``value[:width-1] + "+"`` (``print_fields.c:147-160``); numeric cells
  are never truncated, they overflow the column like ``printf`` min
  widths do;
* ``-p``/``--parsable`` joins cells with ``|`` and keeps a trailing
  ``|``; ``-P``/``--parsable2`` drops the trailing one; neither prints
  the dash row;
* ``-n``/``--noheader`` suppresses the header (and dash row).

Field name resolution is a case-insensitive prefix match in registry
order, mirroring the ``xstrncasecmp(object, key, MAX(command_len, N))``
chains in ``sacctmgr/common.c`` and ``sacct/options.c``; ``Name%W``
format tokens override the width (a signed ``W`` also flips alignment).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Optional, Union


@dataclass(frozen=True)
class FieldSpec:
    """One output column, mirroring real Slurm's ``print_field_t``."""

    name: str  # canonical key used for prefix matching
    width: int  # signed, C-style: > 0 right-aligned, < 0 left-aligned
    header: str = ""  # printed name when it differs from ``name``
    min_prefix: int = 1  # the N in xstrncasecmp(..., MAX(command_len, N))
    truncate: bool = True  # False for numeric columns (printf min width)

    @property
    def display_name(self) -> str:
        return self.header or self.name

    @property
    def abs_width(self) -> int:
        return abs(self.width)

    @property
    def right_align(self) -> bool:
        return self.width > 0


@dataclass
class OutputMode:
    """Header/parsable switches shared by sacctmgr, sacct, and sshare."""

    noheader: bool = False
    parsable: Optional[str] = None  # None | "p" (trailing |) | "P" (no trailing |)


class UnknownFieldError(Exception):
    """Raised when a ``format=`` token matches no registered field."""

    def __init__(self, token: str):
        super().__init__(token)
        self.token = token


def parse_format_spec(value: str) -> list[tuple[str, Optional[int]]]:
    """Split ``Name%W,Other`` into ``[(token, width_override), ...]``.

    The width is signed: ``Name%-20`` means left-aligned 20, matching
    ``newlen = atoi(tmp_char)`` in real Slurm.
    """
    items: list[tuple[str, Optional[int]]] = []
    for raw in value.split(","):
        token = raw.strip()
        if not token:
            continue
        width: Optional[int] = None
        if "%" in token:
            head, _, tail = token.partition("%")
            token = head
            try:
                width = int(tail)
            except ValueError:
                width = None
        items.append((token, width))
    return items


def resolve_format(
    spec: list[tuple[str, Optional[int]]],
    registry: list[FieldSpec],
) -> list[FieldSpec]:
    """Resolve format tokens against ``registry`` in declaration order.

    A token matches a field when it is a case-insensitive prefix of the
    field name and is at least ``min_prefix`` characters long. Raises
    :class:`UnknownFieldError` for the first token with no match.
    """
    resolved: list[FieldSpec] = []
    for token, width_override in spec:
        match = _match_field(token, registry)
        if match is None:
            raise UnknownFieldError(token)
        if width_override is not None:
            match = replace(match, width=width_override)
        resolved.append(match)
    return resolved


def _match_field(token: str, registry: list[FieldSpec]) -> Optional[FieldSpec]:
    if not token:
        return None
    needle = token.casefold()
    for spec in registry:
        if len(token) >= spec.min_prefix and spec.name.casefold().startswith(needle):
            return spec
    return None


def render_header(fields: list[FieldSpec], mode: OutputMode) -> list[str]:
    """Header lines: ``[]``, one parsable line, or name row + dash row."""
    if mode.noheader:
        return []
    if mode.parsable is not None:
        line = "|".join(f.display_name for f in fields)
        return [line + "|" if mode.parsable == "p" else line]
    name_row = "".join(_pad(f.display_name[: f.abs_width], f) for f in fields)
    dash_row = "".join("-" * f.abs_width + " " for f in fields)
    return [name_row, dash_row]


def render_row(cells: list[str], fields: list[FieldSpec], mode: OutputMode) -> str:
    if mode.parsable is not None:
        line = "|".join(cells)
        return line + "|" if mode.parsable == "p" else line
    return "".join(_pad(_clip(cell, f), f) for cell, f in zip(cells, fields))


def _clip(value: str, f: FieldSpec) -> str:
    if f.truncate and len(value) > f.abs_width:
        return value[: f.abs_width - 1] + "+"
    return value


def _pad(value: str, f: FieldSpec) -> str:
    padded = value.rjust(f.abs_width) if f.right_align else value.ljust(f.abs_width)
    return padded + " "


Row = Union[dict[str, str], list[str]]


def render_table(fields: list[FieldSpec], rows: Sequence[Row], mode: OutputMode) -> str:
    """Render header + rows.

    Dict rows are keyed by the *display* name (so alias specs like
    ``Acct`` share the ``Account`` column); missing keys render blank
    (real Slurm's NO_VAL columns).
    """
    lines = render_header(fields, mode)
    for row in rows:
        if isinstance(row, dict):
            cells = [row.get(f.display_name, "") for f in fields]
        else:
            cells = list(row)
        lines.append(render_row(cells, fields, mode))
    return "\n".join(lines)


def extract_output_flags(
    args: list[str],
    shorts: str = "npP",
) -> tuple[OutputMode, bool, list[str]]:
    """Pull output-mode flags out of ``args``.

    Recognizes ``--parsable``, ``--parsable2``, ``--noheader``,
    ``--immediate`` and combined short clusters built from ``shorts``
    (plus ``i`` when ``--immediate`` is relevant), e.g. ``-nP``.
    Returns ``(mode, immediate, remaining_args)``.
    """
    mode = OutputMode()
    immediate = False
    rest: list[str] = []
    char_map = {"n": "noheader", "p": "p", "P": "P", "i": "immediate"}
    allowed = set(shorts)
    for arg in args:
        if arg == "--parsable":
            mode.parsable = "p"
        elif arg == "--parsable2":
            mode.parsable = "P"
        elif arg == "--noheader":
            mode.noheader = True
        elif arg == "--immediate" and "i" in shorts:
            immediate = True
        elif (
            len(arg) > 1 and arg[0] == "-" and arg[1] != "-" and all(c in allowed for c in arg[1:])
        ):
            for c in arg[1:]:
                action = char_map[c]
                if action == "noheader":
                    mode.noheader = True
                elif action == "immediate":
                    immediate = True
                else:
                    mode.parsable = action
        else:
            rest.append(arg)
    return mode, immediate, rest
