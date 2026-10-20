"""Record what a real Slurm cluster reports for the job-accounting scenario.

Usage (see README.md): run scenario.sh in the cluster, then

    python3 record.py <container> <rest-url> <scenario-output> <golden.json>

Times are stored as seconds after the scenario start (T0), so the emulator
replays the same run on its simulated clock and compares offsets.
"""

from __future__ import annotations

import json
import subprocess
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

SACCT_FIELDS = "JobID,JobName,State,Submit,Eligible,Start,End,Elapsed,ElapsedRaw,Suspended"
DATE_FIELDS = ("Submit", "Eligible", "Start", "End")
# (name, window start offset, extra sacct options)
SACCT_QUERIES = [
    ("S1", -60, []),
    ("S1 --truncate", -60, ["--truncate"]),
    ("S1 --truncate -D", -60, ["--truncate", "-D"]),
    ("S1 -D", -60, ["-D"]),
    ("S2 --truncate", 30, ["--truncate"]),
]
REST_QUERIES = [
    "",
    "skip_steps=true",
    "skip_steps=true&show_duplicates=true",
    "disable_truncate_usage_time=true",
]
WINDOW_END = 3600


def main(container: str, rest_url: str, scenario_out: str, golden: str) -> int:
    lines = Path(scenario_out).read_text().splitlines()
    events = dict(line.split("=", 1) for line in lines if "=" in line and line[0].isupper())
    t0 = int(events.pop("T0"))
    events.pop("JOBS", None)
    data: dict = {
        "t0_offsets": {k: int(v) - t0 for k, v in events.items()},
        "sacct": {},
        "rest": {},
    }

    def stamp(offset: int) -> str:
        return datetime.fromtimestamp(t0 + offset, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    def date_offset(value: str):
        if value == "Unknown":
            return value
        moment = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        return int(moment.timestamp()) - t0

    now = int(subprocess.check_output(["docker", "exec", container, "date", "+%s"], text=True))
    data["sacct_now"] = now - t0
    for name, start, options in SACCT_QUERIES:
        args = [
            "docker",
            "exec",
            container,
            "sacct",
            "-X",
            "-P",
            "-n",
            "--noconvert",
            "--allusers",
            "-A",
            "acc1",
            "-S",
            stamp(start),
            "-E",
            stamp(WINDOW_END),
            *options,
            f"--format={SACCT_FIELDS}",
        ]
        rows = []
        for line in subprocess.check_output(args, text=True).splitlines():
            row = dict(zip(SACCT_FIELDS.split(","), line.split("|")))
            for field in DATE_FIELDS:
                row[field] = date_offset(row[field])
            rows.append(row)
        data["sacct"][name] = {"start": start, "options": options, "rows": rows}

    now = int(subprocess.check_output(["docker", "exec", container, "date", "+%s"], text=True))
    data["rest_now"] = now - t0
    for flags in REST_QUERIES:
        url = f"{rest_url}/slurmdb/v0.0.43/jobs/?account=acc1&start_time={t0 - 60}&end_time={t0 + WINDOW_END}"
        # The cluster's own REST URL, given on the command line.
        body = json.load(urllib.request.urlopen(url + (f"&{flags}" if flags else "")))  # noqa: S310
        rows = []
        for job in body["jobs"]:
            t = job["time"]
            rel = {
                k: (t[k] - t0 if t[k] else 0) for k in ("submission", "eligible", "start", "end")
            }
            rows.append(
                {
                    "job_id": job["job_id"],
                    "state": job["state"]["current"],
                    **rel,
                    "elapsed": t["elapsed"],
                    "suspended": t["suspended"],
                    "steps": len(job["steps"]),
                }
            )
        data["rest"][flags] = rows

    Path(golden).write_text(json.dumps(data, indent=1) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
