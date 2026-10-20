"""Record GPU/CPU allocation of a real cluster after gres.sh (see README.md).

python3 record_gres.py <container> <rest-url> <golden.json>
"""

from __future__ import annotations

import json
import subprocess
import sys
import urllib.request
from pathlib import Path

SACCT_FIELDS = "JobID,JobName,ReqCPUS,AllocCPUS,ReqTRES,AllocTRES"
SHOW_KEYS = ("NumCPUs", "ReqTRES", "AllocTRES", "TresPerNode", "TresPerJob")


def _get(url: str) -> dict:
    # The cluster's own REST URL, given on the command line.
    return json.load(urllib.request.urlopen(url))  # noqa: S310


def main(container: str, rest_url: str, golden: str) -> int:
    def run(*args: str) -> str:
        return subprocess.check_output(["docker", "exec", container, *args], text=True)

    sacct = run("sacct", "-X", "-P", "-n", "--noconvert", f"--format={SACCT_FIELDS}")
    data: dict = {
        "sacct": [
            dict(zip(SACCT_FIELDS.split(","), line.split("|"))) for line in sacct.splitlines()
        ]
    }
    data["show_job"] = {}
    for row in data["sacct"]:
        words = run("scontrol", "show", "job", row["JobID"]).split()
        fields = dict(w.split("=", 1) for w in words if "=" in w)
        data["show_job"][row["JobID"]] = {k: fields.get(k, "") for k in SHOW_KEYS}
    jobs = _get(f"{rest_url}/slurmdb/v0.0.43/jobs/?account=acc1&skip_steps=true")["jobs"]
    data["slurmdb_tres"] = {
        str(job["job_id"]): {
            kind: [
                [t["type"], t["name"], t["id"], t["count"]]
                for t in job["tres"][kind]
                if t["type"] != "energy"
            ]
            for kind in ("requested", "allocated")
        }
        for job in jobs
    }
    ctld = _get(f"{rest_url}/slurm/v0.0.43/jobs/")["jobs"]
    data["ctld"] = {
        str(job["job_id"]): {
            "nodes": job["nodes"],
            "cpus": job["cpus"]["number"],
            "tres_per_node": job["tres_per_node"],
            "tres_per_job": job["tres_per_job"],
            "tres_req_str": job["tres_req_str"],
            "tres_alloc_str": job["tres_alloc_str"],
            "gres_detail": job["gres_detail"],
        }
        for job in ctld
    }
    nodes = _get(f"{rest_url}/slurm/v0.0.43/nodes/")["nodes"]
    keys = ("gres", "gres_used", "tres", "tres_used", "threads")
    data["nodes"] = {n["name"]: {k: n[k] for k in keys} for n in nodes}
    tres = _get(f"{rest_url}/slurmdb/v0.0.43/tres/")["TRES"]
    data["tres"] = [[t["type"], t["name"], t["id"]] for t in tres if t["type"] != "dynamic_offset"]
    Path(golden).write_text(json.dumps(data, indent=1) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))
