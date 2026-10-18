"""RESERVATION_DESC_MSG / RESERVATION_MOD_REQ parsing as slurmrestd's data_parser does it.

Verified against a real slurmrestd 26.05 (``examples``-independent probe, see
the MR) and the source:

* fields are parsed in the parser array's order
  (slurm://src/plugins/data_parser/v0.0.45/parsers.c#RESERVATION_DESC_MSG@26.05+);
  the first fatal error stops parsing, and the handler then adds
  ``Rejecting request. Failure parsing parameters`` (9000)
  (slurm://src/slurmrestd/plugins/openapi/slurmctld/reservations.c#_parse_resv_desc@26.05+);
* a value of another JSON type than the OpenAPI schema names draws a
  warning first (slurm://src/plugins/data_parser/v0.0.45/parsing.c#_parse_check_openapi@26.05+)
  -- for the list fields (``accounts``, ``groups``, ``licenses``,
  ``node_list``, ``users``) and the plain strings;
* an unknown flag is *not* fatal: it is reported (9207) and parsing goes on
  (slurm://src/plugins/data_parser/v0.0.45/parsing.c#_foreach_flag_parser@26.05+),
  so the request is carried out and still answers with that error;
* scalars convert where the parser converts them (numbers to strings,
  numeric strings to numbers); a string field given a list or object fails
  without an entry of its own (slurm://src/plugins/data_parser/v0.0.45/parsers.c#PARSE_FUNC@26.05+);
* unknown keys are warned about once the fields are parsed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from emulator.commands.slurm_time import ParseTimeError, parse_time_spec
from emulator.core.cluster_model import expand_hostlist
from emulator.core.reservations import DURATION_INFINITE, ReservationDesc, rest_flag_names

ESLURM_DATA_CONV_FAILED = 9202
ESLURM_DATA_FLAGS_INVALID_TYPE = 9206
ESLURM_DATA_FLAGS_INVALID = 9207
ESLURM_DATA_EXPECTED_LIST = 9208
ESLURM_DATA_EXPECTED_DICT = 9209
ESLURM_DATA_PARSE_BAD_INPUT = 9216

STRERROR = {
    ESLURM_DATA_CONV_FAILED: "Unable to convert Data type",
    ESLURM_DATA_FLAGS_INVALID_TYPE: "Data parser expects flags to be a list",
    ESLURM_DATA_FLAGS_INVALID: "Data parser unable to parse invalid flag",
    ESLURM_DATA_EXPECTED_LIST: "Data parser expected a list",
    ESLURM_DATA_EXPECTED_DICT: "Data parser expected a dictionary or object",
    ESLURM_DATA_PARSE_BAD_INPUT: "Request to parse invalid data",
}

# NO_VAL in a 64-bit time_t: an unset start/end echoes as this number.
NO_VAL = 0xFFFFFFFE

_LIST_FIELDS = ("accounts", "groups", "licenses", "node_list", "users")
_STRING_FIELDS = ("burst_buffer", "comment", "features", "name", "partition")
_KNOWN = {
    "accounts", "burst_buffer", "comment", "core_count", "duration", "end_time",
    "features", "flags", "groups", "licenses", "max_start_delay", "name",
    "node_count", "node_list", "partition", "purge_completed", "start_time",
    "tres", "users",
}  # fmt: skip


def data_type(value: object) -> str:
    """slurm://src/common/data.c#data_get_type_string names."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "64 bit integer"
    if isinstance(value, float):
        return "floating point"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "dictionary"
    return "list"


def _openapi_type(value: object) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    return "array"


def dump(value: object) -> str:
    """``%pd``: the value as compact JSON."""
    return json.dumps(value, separators=(",", ":"))


def _as_string(value: object) -> Optional[str]:
    """data_convert_type(..., DATA_TYPE_STRING) for scalars."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, str)):
        return str(value)
    if isinstance(value, float):
        return f"{value:f}"
    return None


class Fatal(Exception):  # noqa: N818 - the parser's stop, not an error class
    """Parsing stopped; ``entry`` is the parser's own error, if it wrote one."""

    def __init__(self, entry: Optional[dict[str, Any]] = None):
        super().__init__(entry)
        self.entry = entry


@dataclass
class Parsed:
    errors: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[dict[str, Any]] = field(default_factory=list)


class DescParser:
    def __init__(self, now: datetime, out: Parsed):
        self.now = now
        self.out = out

    def _error(self, errno: int, text: str, path: str) -> dict[str, Any]:
        return {
            "description": text,
            "error_number": errno,
            "error": STRERROR[errno],
            "source": path,
        }

    def _warn(self, text: str, path: str) -> None:
        self.out.warnings.append({"description": text, "source": path})

    def _check_type(self, value: object, expected: str, path: str) -> None:
        if value is None or _openapi_type(value) == expected:
            return
        slurm = {"array": "list", "string": "string"}[expected]
        self._warn(
            f"Expected OpenAPI type={expected} (Slurm type={slurm}) but got OpenAPI "
            f"type={_openapi_type(value)} (Slurm type={data_type(value)}): {dump(value)}",
            path,
        )

    # --- field parsers ---

    def csv(self, value: object, path: str) -> Optional[str]:
        """CSV_STRING: list, dict (``k=v``) or a scalar."""
        if value is None:
            return None
        if isinstance(value, (list, dict)):
            items = value.items() if isinstance(value, dict) else ((None, v) for v in value)
            parts = []
            for key, item in items:
                text = _as_string(item)
                if text is None:
                    raise Fatal(
                        self._error(
                            ESLURM_DATA_CONV_FAILED,
                            f"unable to convert csv entry {data_type(item)} to string",
                            path,
                        )
                    )
                parts.append(text if key is None else f"{key}={text}")
            return ",".join(parts)
        return _as_string(value)

    def hostlist(self, value: object, path: str) -> Optional[str]:
        """HOSTLIST_STRING: a hostlist string or a list of host strings."""
        if value is None:
            return None
        if isinstance(value, str):
            if not value:
                return None
            if value.count("[") != value.count("]") or not expand_hostlist(value):
                raise Fatal(
                    self._error(
                        ESLURM_DATA_PARSE_BAD_INPUT, f"Invalid hostlist string: {value}", path
                    )
                )
            return value
        if isinstance(value, list):
            hosts = []
            for item in value:
                text = _as_string(item)
                if text is None:
                    raise Fatal(
                        self._error(
                            ESLURM_DATA_PARSE_BAD_INPUT,
                            f"string expected but got {dump(item)}",
                            path,
                        )
                    )
                hosts.append(text)
            return ",".join(hosts) or None
        raise Fatal(
            self._error(ESLURM_DATA_PARSE_BAD_INPUT, f"string expected but got {dump(value)}", path)
        )

    def string(self, value: object, path: str) -> Optional[str]:
        """STRING: a scalar converts; a list or object fails with no entry of its own."""
        if value is None:
            return None
        text = _as_string(value)
        if text is None:
            raise Fatal
        return text

    def uint32(self, value: object, path: str) -> Optional[int]:
        """UINT32_NO_VAL via UINT64_NO_VAL."""
        if value is None:
            return None
        if isinstance(value, (bool, list)):
            raise Fatal(
                self._error(
                    ESLURM_DATA_CONV_FAILED, f"Expected integer but got {dump(value)}", path
                )
            )
        if isinstance(value, dict):
            if value.get("infinite"):
                return DURATION_INFINITE
            if not value.get("set"):
                return None
            value = value.get("number", 0)
        if isinstance(value, str):
            try:
                number = int(value.strip(), 0)
            except ValueError:
                raise Fatal(
                    self._error(
                        ESLURM_DATA_CONV_FAILED, f"Expected integer but got {dump(value)}", path
                    )
                ) from None
        elif isinstance(value, (int, float)):
            number = int(value)
        else:
            raise Fatal(
                self._error(
                    ESLURM_DATA_CONV_FAILED, f"Expected integer but got {dump(value)}", path
                )
            )
        return DURATION_INFINITE if number >= NO_VAL else number

    def timestamp(self, value: object, path: str) -> Optional[datetime]:
        """TIMESTAMP_NO_VAL: a parse_time() string, a number or a NO_VAL struct."""
        if isinstance(value, str) and not value.strip().lstrip("-").isdigit():
            try:
                return parse_time_spec(value, self.now)
            except ParseTimeError:
                raise Fatal(
                    self._error(
                        ESLURM_DATA_CONV_FAILED, f"Parsing of {value} for timestamp failed", path
                    )
                ) from None
        number = self.uint32(value, path)
        if number is None or number == DURATION_INFINITE:
            return None
        return datetime.fromtimestamp(number)  # noqa: DTZ006 - naive local, like the job clock

    def flags(self, value: object, path: str) -> Optional[list[str]]:
        """RESERVATION_FLAGS: a list of names, or one name; unknown names are not fatal."""
        if value is None:
            return None
        if isinstance(value, str):
            items: list[object] = [value]
        elif isinstance(value, list):
            items = value
        else:
            raise Fatal(
                self._error(
                    ESLURM_DATA_FLAGS_INVALID_TYPE,
                    f"Expected a List but found a {data_type(value)}",
                    path,
                )
            )
        names = rest_flag_names()
        out: list[str] = []
        for index, item in enumerate(items):
            text = _as_string(item)
            item_path = f"{path[:-1]}[{index}]/"
            if text is None:
                self._warn(f"Ignoring unexpected field of type {data_type(item)}", item_path)
                continue
            name = text.strip().upper()
            if name in names:
                out.append(name)
            else:
                self.out.errors.append(
                    self._error(ESLURM_DATA_FLAGS_INVALID, f'Unknown flag "{text}"', item_path)
                )
        return out or None

    # --- the message ---

    def desc(self, raw: object, base: str = "#/") -> ReservationDesc:
        if not isinstance(raw, dict):
            raise Fatal(
                self._error(
                    ESLURM_DATA_EXPECTED_DICT,
                    f"Rejecting {data_type(raw)} when dictionary expected",
                    base,
                )
            )

        def path(key: str) -> str:
            return f"{base}{key}/"

        values: dict[str, Any] = {}
        for key in (
            "accounts", "burst_buffer", "comment", "core_count", "duration", "end_time",
            "features", "flags", "groups", "licenses", "max_start_delay", "name",
            "node_count", "node_list", "partition", "purge_completed", "start_time", "users",
        ):  # fmt: skip
            value = raw.get(key)
            if key in _LIST_FIELDS:
                self._check_type(value, "array", path(key))
            elif key in _STRING_FIELDS:
                self._check_type(value, "string", path(key))
            if key == "node_list":
                values[key] = self.hostlist(value, path(key))
            elif key in _LIST_FIELDS:
                values[key] = self.csv(value, path(key))
            elif key in _STRING_FIELDS:
                values[key] = self.string(value, path(key))
            elif key in ("start_time", "end_time"):
                values[key] = self.timestamp(value, path(key))
            elif key == "flags":
                values[key] = self.flags(value, path(key))
            elif key == "purge_completed":
                if isinstance(value, dict):
                    self.uint32(value.get("time"), f"{path(key)}time/")
            else:
                values[key] = self.uint32(value, path(key))
        for key, value in raw.items():
            if key not in _KNOWN:
                self._warn(
                    f'Ignoring unknown field "{key}" of type {data_type(value)} in '
                    "DATA_PARSER_RESERVATION_DESC_MSG",
                    path(key),
                )
        return ReservationDesc(
            name=values["name"] or None,
            start_time=values["start_time"],
            end_time=values["end_time"],
            duration=values["duration"],
            node_list=values["node_list"],
            node_cnt=values["node_count"],
            partition=values["partition"],
            flags=values["flags"],
            users=values["users"] or None,
            accounts=values["accounts"] or None,
            groups=values["groups"] or None,
            comment=values["comment"],
        )

    def mod_req(self, raw: object) -> Optional[list[ReservationDesc]]:
        """RESERVATION_MOD_REQ; None when ``reservations`` is missing."""
        if not isinstance(raw, dict):
            raise Fatal(
                self._error(
                    ESLURM_DATA_EXPECTED_DICT,
                    f"Rejecting {data_type(raw)} when dictionary expected",
                    "#/",
                )
            )
        items = raw.get("reservations")
        if items is None:
            return None
        if isinstance(items, dict):
            raise Fatal(
                self._error(
                    ESLURM_DATA_EXPECTED_LIST,
                    "Expected List but found a dictionary",
                    "#/reservations/",
                )
            )
        if not isinstance(items, list):
            items = [items]  # a scalar is a one-entry list
        return [self.desc(item, f"#/reservations[{i}]/") for i, item in enumerate(items)]
