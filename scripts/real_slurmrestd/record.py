"""Replay a sequence of slurmrestd requests and record what came back, as golden data.

usage: record.py cases.json --url http://host:port --source "<what answered>" > golden.json

Each case is ``{"name", "method", "path", "body"?, "pick"?}``. The recorded
``expect`` holds the status, errors and warnings, the payload keys, and for
every ``pick`` path the values found there. A pick path is dot separated;
``*`` walks every item of a list, e.g. ``associations.*.max.tres.total``.
``tests/test_slurmrestd_golden_slurmdb.py`` replays the same cases against
the emulator and compares with ``outcome()`` from this module's twin there.
"""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def pick(data: Any, path: str) -> Any:  # noqa: ANN401
    parts = path.split(".")

    def walk(node: Any, rest: list[str]) -> Any:  # noqa: ANN401
        if not rest:
            return node
        head, tail = rest[0], rest[1:]
        if head == "*":
            return [walk(item, tail) for item in node] if isinstance(node, list) else None
        if isinstance(node, dict) and head in node:
            return walk(node[head], tail)
        return None

    return walk(data, parts)


def outcome(status: int, text: str, picks: list[str]) -> dict[str, Any]:
    try:
        data = json.loads(text)
    except ValueError:
        return {"status": status, "text": text}
    result: dict[str, Any] = {
        "status": status,
        "errors": data.get("errors"),
        "warnings": data.get("warnings"),
        "payload_keys": sorted(k for k in data if k not in ("meta", "errors", "warnings")),
    }
    if picks:
        result["picks"] = {p: pick(data, p) for p in picks}
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("cases")
    parser.add_argument("--url", required=True)
    parser.add_argument("--source", required=True)
    args = parser.parse_args()
    cases = json.loads(Path(args.cases).read_text())
    for case in cases:
        body = case.get("body")
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(  # noqa: S310 - the URL is the operator's
            args.url + case["path"],
            method=case.get("method", "GET"),
            data=data,
            headers={"Content-Type": "application/json", "X-SLURM-USER-TOKEN": "t"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
                status, text = response.status, response.read().decode()
        except urllib.error.HTTPError as error:
            status, text = error.code, error.read().decode()
        case["expect"] = outcome(status, text, case.get("pick", []))
    print(json.dumps({"source": args.source, "cases": cases}, indent=1))


if __name__ == "__main__":
    main()
