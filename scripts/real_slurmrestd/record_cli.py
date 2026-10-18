"""Run a sequence of Slurm CLI commands in a real cluster container and record what came back.

usage: record_cli.py cases.json --container NAME --source "<what answered>" > golden.json

Each case is ``{"name", "argv"}``; ``argv[0]`` is the command (``sacctmgr``,
``sinfo`` …), run as root with ``docker exec <container> /opt/slurm/bin/...``
exactly as given, so quoting in an argument (``description="Org A"``) reaches
the command as the waldur-site-agent sends it. The recorded ``expect`` holds
stdout, stderr and the exit status.
``tests/test_cli_golden_site_agent.py`` replays the cases against the
emulator's console entry points.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("cases")
    parser.add_argument("--container", required=True)
    parser.add_argument("--source", required=True)
    args = parser.parse_args()
    cases = json.loads(Path(args.cases).read_text())
    for case in cases:
        command, *rest = case["argv"]
        done = subprocess.run(
            ["docker", "exec", args.container, f"/opt/slurm/bin/{command}", *rest],
            capture_output=True,
            text=True,
            check=False,
        )
        case["expect"] = {"stdout": done.stdout, "stderr": done.stderr, "exit": done.returncode}
    print(json.dumps({"source": args.source, "cases": cases}, indent=1))


if __name__ == "__main__":
    main()
