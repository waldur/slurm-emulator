# /// script
# requires-python = ">=3.10"
# dependencies = ["requests>=2.31"]
# ///
r"""Submit a realistic job mix through Waldur's HPC workspace REST API.

Unlike seed_history.py, which writes finished jobs straight into accounting,
every job here travels the real path: Waldur -> FireCREST v2 -> slurmrestd
(the emulator). Jobs queue, run on emulator nodes in wall-clock time, end
COMPLETED / FAILED / TIMEOUT / CANCELLED, and the `sonar` service samples
them every minute into slurm-monitor, so Waldur's per-job Performance tab
and PDF show genuine time series.

How long a job runs: Waldur's job spec has an `environment` dict that it
forwards as FireCREST's `env`, which slurmrestd receives as the job
environment. The emulator reads SLURM_EMULATOR_RUN_SECONDS (run length; a
run past the time limit ends TIMEOUT) and SLURM_EMULATOR_CPU_EFFICIENCY /
SLURM_EMULATOR_MEM_FRACTION from there. With SLURM_EMULATOR_JOB_EXECUTE=1
(as in this stack) the batch script really runs when the emulated run ends,
so the scripts below write output and result files and fail for real.

GPU use per job is synthetic and deterministic from the job id
(emulator/core/cluster_model.py#synthetic_gpu_use). The driver logs the
predicted use of every GPU job, and submits the deliberately under-used one
when the next job id predicts a mostly idle card (it waits at most
--idle-gpu-wait minutes for that, then submits anyway).

Usage (as up.sh's preview stack is set up; the Keycloak login runs for each
user and is repeated when Waldur reports the identity-provider session has
expired, and proactively every --relogin-minutes):

    uv run submit_workload.py --waldur-url http://localhost:8000 \\
        --duration 2h --log-dir ./workload

    # behind a reverse proxy whose public URL Keycloak redirects to:
    uv run submit_workload.py --waldur-url http://localhost:20310 \\
        --public-url https://hpc-demo.example.org \\
        --route /auth=http://localhost:20315 --route /=http://localhost:20311

Resumable: the plan is derived from the seed and start time kept in
<log-dir>/state.json, and every submission is appended to
<log-dir>/jobs.jsonl before the next one; a restart with the same --log-dir
skips what was submitted (also checking job names in Waldur, in case the
driver died between submitting and logging). --reset starts a new run.
Stop it with Ctrl-C / SIGTERM; running jobs keep running.
"""

from __future__ import annotations

import argparse
import html
import json
import math
import os
import random
import re
import signal
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlsplit

import requests

ACTIVE = {"PENDING", "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED", "REQUEUED"}
EXPIRED_HINT = "identity provider session"
# Queue at most this many of our jobs per partition; further arrivals wait.
MAX_PENDING = {"gpu": 2, "batch": 3, "debug": 2}

EESSI = "module load EESSI/2023.06"
GROMACS = "module load GROMACS/2024.1-foss-2023b"
WRF = "module load WRF/4.4.1-foss-2022b-dmpar"
PYTHON = "module load Python/3.11.5-GCCcore-13.2.0"
# The project's sample inputs; seed_history.py writes them into alice's home.
# Jobs copy what they need into their own user's home when it is missing.
SHARED_INPUTS = "/home/alice"


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def parse_duration(value: str) -> int:
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*", value)
    if not m:
        raise argparse.ArgumentTypeError(f"bad duration: {value!r} (e.g. 90m, 2h)")
    return int(float(m.group(1)) * {"": 60, "s": 1, "m": 60, "h": 3600, "d": 86400}[m.group(2)])


def predicted_gpu_use(job_id: object) -> float:
    """Mirror of cluster_model.synthetic_gpu_use's utilisation (0..1)."""
    rng = random.Random(f"slurm-emulator-gpu:{job_id}")  # noqa: S311 - mirrors the emulator
    return rng.uniform(0.55, 0.98) if rng.random() < 0.75 else rng.uniform(0.02, 0.35)


# --------------------------------------------------------------------------
# Waldur client with Keycloak login
# --------------------------------------------------------------------------


class Waldur:
    def __init__(self, args: argparse.Namespace):
        self.api = args.waldur_url.rstrip("/") + "/api"
        self.public = (args.public_url or args.waldur_url).rstrip("/")
        self.routes = sorted((tuple(r.split("=", 1)) for r in args.route), key=lambda r: -len(r[0]))
        self.password = args.password
        self.relogin_after = args.relogin_minutes * 60
        self.token_dir = Path(args.token_dir) if args.token_dir else None
        self.tokens: dict[str, str] = {}
        self.logged_in_at: dict[str, float] = {}

    # Browser-like login: Waldur's Keycloak init -> login form -> callback ->
    # token exchange. Logging in also refreshes the OIDC token Waldur keeps
    # for the user and forwards to FireCREST.
    def _go(self, jar: dict, method: str, url: str, **kw) -> requests.Response:
        headers = {}
        if self.routes and url.startswith(self.public):
            path = url[len(self.public) :] or "/"
            origin = next((o for p, o in self.routes if path.startswith(p)), None)
            if origin:
                host = urlsplit(self.public)
                headers = {
                    "Host": host.netloc,
                    "X-Forwarded-Host": host.netloc,
                    "X-Forwarded-Proto": host.scheme,
                }
                url = origin.rstrip("/") + path
        if jar:
            headers["Cookie"] = "; ".join(f"{k}={v}" for k, v in jar.items())
        r = requests.request(method, url, headers=headers, allow_redirects=False, timeout=30, **kw)
        for cookie in r.raw.headers.getlist("Set-Cookie"):
            k, v = cookie.split(";")[0].split("=", 1)
            jar[k] = v
        return r

    def login(self, user: str) -> None:
        jar: dict[str, str] = {}
        r = self._go(jar, "GET", f"{self.public}/api-auth/keycloak/init/")
        r = self._go(jar, "GET", r.headers["Location"])
        form = re.search(r'action="([^"]+)"', r.text)
        if not form:
            raise RuntimeError(f"login {user}: no Keycloak login form ({r.status_code})")
        r = self._go(
            jar,
            "POST",
            html.unescape(form.group(1)),
            data={"username": user, "password": self.password, "credentialId": ""},
        )
        if "Location" not in r.headers:
            raise RuntimeError(f"login {user}: Keycloak refused the credentials ({r.status_code})")
        r = self._go(jar, "GET", r.headers["Location"])
        code = parse_qs(urlsplit(r.headers.get("Location", "")).query).get("code", [None])[0]
        if not code:
            raise RuntimeError(f"login {user}: Waldur callback gave no code ({r.status_code})")
        r = self._go(jar, "POST", f"{self.public}/api-auth/token-exchange/", json={"code": code})
        r.raise_for_status()
        self.tokens[user] = r.json()["token"]
        self.logged_in_at[user] = time.time()
        if self.token_dir:
            path = self.token_dir / f"{user}-token.txt"
            path.write_text(self.tokens[user])
            path.chmod(0o600)
        log(f"logged in as {user}")

    def _token(self, user: str) -> str:
        if user not in self.tokens or time.time() - self.logged_in_at[user] > self.relogin_after:
            self.login(user)
        return self.tokens[user]

    def request(self, user: str, method: str, path: str, **kw) -> requests.Response:
        for attempt in range(4):
            try:
                r = requests.request(
                    method,
                    f"{self.api}{path}",
                    headers={"Authorization": f"Token {self._token(user)}"},
                    timeout=120,
                    **kw,
                )
            except requests.RequestException as e:
                if method != "GET":
                    raise  # never blindly repeat a submit; the caller re-checks
                log(f"{method} {path}: {e}; retrying")
                time.sleep(10 * (attempt + 1))
                continue
            if r.status_code == 401 or (r.status_code in (400, 403) and EXPIRED_HINT in r.text):
                log(f"{user}: session expired ({r.status_code}); logging in again")
                self.login(user)
                continue
            if r.status_code >= 500 and method == "GET":
                log(f"{method} {path}: {r.status_code}; retrying")
                time.sleep(10 * (attempt + 1))
                continue
            return r
        return r

    def get(self, user: str, path: str, **kw) -> list | dict:
        r = self.request(user, "GET", path, **kw)
        r.raise_for_status()
        return r.json()


# --------------------------------------------------------------------------
# The plan
# --------------------------------------------------------------------------


@dataclass
class Entry:
    key: str
    at: int  # seconds after the run's start
    user: str
    account: str
    kind: str
    spec: dict
    run_seconds: int
    expect: str  # COMPLETED / FAILED / TIMEOUT / CANCELLED
    cancel_after: Optional[int] = None  # seconds RUNNING before the user cancels
    prefer_idle_gpu: bool = False
    env: dict = field(default_factory=dict)


def _limit(run_seconds: int, rng: random.Random, slack=(1.3, 1.8)) -> int:
    return max(5, math.ceil(run_seconds * rng.uniform(*slack) / 60 / 5) * 5)


def gromacs(rng, *, nodes=1, idle=False) -> dict:
    steps = rng.choice([250_000, 500_000, 1_000_000])
    ns_day = rng.uniform(85, 160) * nodes * (0.25 if idle else 1)
    system = rng.choice(["lysozyme", "aquaporin", "villin", "membrane-patch"])
    name = f"gromacs-{system}-{'analysis' if idle else 'md'}"
    commands = f"""\
set -euo pipefail
mkdir -p "$HOME/md" && cd "$HOME/md"
for f in protein.pdb energy.xvg; do [ -f "$f" ] || cp "{SHARED_INPUTS}/md/$f" .; done
out="run-$SLURM_JOB_ID"
mkdir -p "$out"
atoms=$(grep -c '^ATOM' protein.pdb)
echo "GROMACS 2024.1 on $SLURM_JOB_NUM_NODES node(s), {4 * nodes} GPUs"
echo "Input protein.pdb: $atoms atoms, solvated system ~{rng.randint(23, 96)}k atoms"
echo "Running {steps} steps of 2 fs ({steps * 2 / 1e6:.1f} ns)"
for pct in 10 20 30 40 50 60 70 80 90 100; do
  echo "step $(({steps} * pct / 100)) ($pct%)  T=300.$((RANDOM % 9)) K  P=1.0$((RANDOM % 9)) bar"
done
{"echo 'NOTE: GPU waits on CPU-side trajectory analysis; consider fewer GPUs' >&2" if idle else ":"}
awk '!/^[@#]/ {{n++; s+=$2}} END {{printf "Average potential energy: %.1f kJ/mol over %d frames\\n", s/n, n}}' energy.xvg | tee "$out/summary.txt"
printf "Performance: %.3f ns/day\\n" {ns_day:.3f} | tee -a "$out/summary.txt"
cp protein.pdb "$out/confout.pdb"
echo "Finished mdrun; results in md/$out"
"""
    return {
        "name": name,
        "partition": "gpu",
        "nodes": nodes,
        "ntasks_per_node": 4,
        "cpus_per_task": 16,  # 4 x 16 = one 64-core gpu node
        "gpus_per_node": 4,
        "memory_mb": rng.choice([128_000, 240_000, 360_000]),
        "modules": [EESSI, GROMACS],
        "commands": commands,
    }


def wrf(rng, *, nodes, fail=False) -> dict:
    hours = rng.choice([24, 48, 72])
    domain = rng.choice(["baltic-3km", "nordic-9km", "gulf-of-finland-1km"])
    case = "wrf-baltic"
    if fail:
        # A real mistake: the run points at a case directory without input.
        case = rng.choice(["wrf-baltic-2025", "wrf-nordic", "wrf-baltc"])
    stage = (
        ":"
        if fail
        else f'mkdir -p "$case_dir"; [ -f "$case_dir/namelist.input" ] || '
        f'cp "{SHARED_INPUTS}/wrf-baltic/namelist.input" "$case_dir/"'
    )
    commands = f"""\
set -uo pipefail
case_dir="$HOME/{case}"
{stage}
echo "WRF 4.4.1 dmpar, domain {domain}, {hours} h forecast, $SLURM_JOB_NUM_NODES nodes"
if [ ! -f "$case_dir/namelist.input" ]; then
  echo "taskid: 0 hostname: $(hostname)" >&2
  echo "-------------- FATAL CALLED ---------------" >&2
  echo "FATAL CALLED FROM FILE:  <stdin>  LINE:     342" >&2
  echo "ERROR OPENING namelist.input: $case_dir/namelist.input not found" >&2
  echo "-------------------------------------------" >&2
  exit 1
fi
cd "$case_dir"
out="run-$SLURM_JOB_ID"
mkdir -p "$out"
grep -E 'run_hours|dx|time_step' namelist.input | sed 's/^ */  /' || true
for h in $(seq 6 6 {hours}); do
  echo "Timing for Writing wrfout_d01 +${{h}}h: $((RANDOM % 9 + 2)).$((RANDOM % 99)) elapsed seconds"
done | tee "$out/rsl.out.0000"
echo "d01 {hours}h forecast written" > "$out/wrfout_d01.summary"
echo "wrf: SUCCESS COMPLETE WRF"
"""
    return {
        "name": f"wrf-{domain}",
        "partition": "batch",
        "nodes": nodes,
        "ntasks_per_node": 64,
        "cpus_per_task": 1,
        "memory_mb": rng.choice([192_000, 256_000, 384_000]),
        "modules": [EESSI, WRF],
        "commands": commands,
    }


def multiqc(rng) -> dict:
    samples = rng.randint(12, 96)
    commands = f"""\
set -euo pipefail
cd "$HOME/qc"
out="qc-$SLURM_JOB_ID"
mkdir -p "$out"
echo "MultiQC on {samples} samples (fastqc + bwa-align)"
title=$(grep -o '<title>[^<]*' multiqc_report.html | sed 's/<title>//')
echo "Template report: $title"
for i in $(seq 1 {samples}); do
  printf "S%03d\\t%d\\t%.1f\\n" "$i" $((RANDOM * 1000 + 2000000)) "$((RANDOM % 15 + 30)).$((RANDOM % 9))"
done > "$out/general_stats.tsv"
echo "Wrote $out/general_stats.tsv ($(wc -l < "$out/general_stats.tsv") samples)"
"""
    return {
        "name": rng.choice(["multiqc-batch", "fastqc-summary", "qc-trimmed-reads"]),
        "partition": "debug",
        "nodes": 1,
        "ntasks_per_node": 1,
        "cpus_per_task": 8,
        "memory_mb": 16_000,
        "modules": [EESSI, PYTHON],
        "commands": commands,
    }


def tiny(rng) -> dict:
    return {
        "name": rng.choice(["hello-mpi", "env-check", "srun-test"]),
        "partition": "debug",
        "nodes": 1,
        "ntasks_per_node": 4,
        "cpus_per_task": 1,
        "memory_mb": 4_000,
        "modules": [],
        "commands": 'echo "Job $SLURM_JOB_ID on $(hostname) as $USER"\n'
        "srun -n 4 echo rank ok\nenv | grep ^SLURM_JOB | sort\n",
    }


def build_plan(seed: int, duration: int, first_batch: bool, mean_gap: float) -> list[Entry]:
    rng = random.Random(seed)  # noqa: S311 - reproducible demo plan
    plan: list[Entry] = []

    def add(at, user, account, kind, spec, run, expect, **kw):
        idx = len(plan) + 1
        spec = dict(spec)
        spec["name"] = f"{spec['name']}-{seed % 1000:03d}{idx:02d}"
        limit = kw.pop("limit", None) or _limit(run, rng)
        spec["time_limit_minutes"] = limit
        env = {"SLURM_EMULATOR_RUN_SECONDS": str(run), **kw.pop("env", {})}
        plan.append(
            Entry(f"e{idx:03d}", int(at), user, account, kind, spec, run, expect, env=env, **kw)
        )

    if first_batch:
        # Front-loaded so that within ~45 min several GPU and CPU jobs have
        # finished with >=10 min of samples, while others still run.
        add(0, "alice", "alloc_climate", "gromacs", gromacs(rng), 14 * 60, "COMPLETED")
        add(30, "alice", "alloc_genomics", "multiqc", multiqc(rng), 3 * 60, "COMPLETED")
        add(60, "bob", "alloc_climate", "wrf", wrf(rng, nodes=4), 16 * 60, "COMPLETED")
        add(120, "bob", "alloc_climate", "gromacs", gromacs(rng), 12 * 60, "COMPLETED")
        add(180, "bob", "alloc_climate", "wrf", wrf(rng, nodes=2, fail=True), 5 * 60, "FAILED")
        add(240, "alice", "alloc_climate", "wrf", wrf(rng, nodes=2), 26 * 60, "TIMEOUT", limit=15)
        add(300, "bob", "alloc_climate", "tiny", tiny(rng), 60, "COMPLETED")
        add(
            420,
            "alice",
            "alloc_climate",
            "wrf-long",
            wrf(rng, nodes=6),
            3 * 3600,
            "CANCELLED",
            limit=240,
            cancel_after=25 * 60,
        )
        add(
            480,
            "alice",
            "alloc_climate",
            "gromacs-idle",
            gromacs(rng, idle=True),
            15 * 60,
            "COMPLETED",
            prefer_idle_gpu=True,
            env={"SLURM_EMULATOR_CPU_EFFICIENCY": "0.35"},
        )
        add(600, "alice", "alloc_genomics", "multiqc", multiqc(rng), 4 * 60, "COMPLETED")
        add(900, "alice", "alloc_climate", "gromacs", gromacs(rng, nodes=2), 22 * 60, "COMPLETED")
        add(1200, "bob", "alloc_climate", "wrf", wrf(rng, nodes=3), 18 * 60, "COMPLETED")
        t = 1500
    else:
        t = 0

    # Poisson arrivals for the rest of the run.
    kinds = [
        ("gromacs", 3),
        ("wrf", 4),
        ("multiqc", 3),
        ("tiny", 1),
        ("wrf-fail", 1),
        ("wrf-timeout", 1),
    ]
    while True:
        t += rng.expovariate(1 / (mean_gap * 60))
        if t >= duration:
            break
        kind = rng.choices([k for k, _ in kinds], weights=[w for _, w in kinds])[0]
        user = rng.choice(["alice", "alice", "bob"])
        if kind == "gromacs":
            nodes = rng.choice([1, 1, 2])
            add(
                t,
                user,
                "alloc_climate",
                kind,
                gromacs(rng, nodes=nodes),
                rng.randint(20, 60) * 60,
                "COMPLETED",
            )
        elif kind == "wrf":
            add(
                t,
                user,
                "alloc_climate",
                kind,
                wrf(rng, nodes=rng.randint(2, 8)),
                rng.randint(15, 45) * 60,
                "COMPLETED",
            )
        elif kind == "wrf-fail":
            add(
                t,
                user,
                "alloc_climate",
                kind,
                wrf(rng, nodes=rng.randint(2, 4), fail=True),
                rng.randint(2, 8) * 60,
                "FAILED",
            )
        elif kind == "wrf-timeout":
            run = rng.randint(30, 45) * 60
            add(
                t,
                user,
                "alloc_climate",
                kind,
                wrf(rng, nodes=rng.randint(2, 4)),
                run,
                "TIMEOUT",
                limit=max(10, run // 60 - rng.randint(8, 15)),
            )
        elif kind == "multiqc":
            add(
                t,
                "alice",
                "alloc_genomics",
                kind,
                multiqc(rng),
                rng.randint(2, 5) * 60,
                "COMPLETED",
            )
        else:
            add(t, user, "alloc_climate", kind, tiny(rng), rng.randint(30, 120), "COMPLETED")
    return plan


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------


class Driver:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.dir = Path(args.log_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.jsonl = self.dir / "jobs.jsonl"
        state_path = self.dir / "state.json"
        if args.reset or not state_path.exists():
            if args.reset and self.jsonl.exists():
                self.jsonl.rename(self.dir / f"jobs-{int(time.time())}.jsonl")
            seed = args.seed if args.seed is not None else random.randrange(10**6)  # noqa: S311
            self.state = {
                "seed": seed,
                "start": time.time(),
                "duration": args.duration,
                "first_batch": not args.no_first_batch,
                "mean_gap": args.mean_gap,
            }
            state_path.write_text(json.dumps(self.state, indent=2))
            log(f"new run: seed {seed}")
        else:
            self.state = json.loads(state_path.read_text())
            log(
                f"resuming run started {time.ctime(self.state['start'])} (seed {self.state['seed']})"
            )
        self.plan = build_plan(
            self.state["seed"],
            self.state["duration"],
            self.state["first_batch"],
            self.state["mean_gap"],
        )
        self.submitted: dict[str, dict] = {}  # key -> submitted event
        self.cancelled: set[str] = set()
        self.states: dict[str, str] = {}
        self.delay: dict[str, int] = {}
        if self.jsonl.exists():
            for line in self.jsonl.read_text().splitlines():
                ev = json.loads(line)
                if ev["event"] == "submitted":
                    self.submitted[ev["key"]] = ev
                elif ev["event"] == "cancelled":
                    self.cancelled.add(ev["key"])
                elif ev["event"] == "state":
                    self.states[ev["job_id"]] = ev["state"]
        self.waldur = Waldur(args)
        self.workspaces: dict[tuple[str, str], str] = {}
        self.jobs: dict[str, dict] = {}  # job_id -> latest Waldur job

    def write(self, **event) -> None:
        event["ts"] = round(time.time(), 1)
        with self.jsonl.open("a") as f:
            f.write(json.dumps(event) + "\n")

    def discover(self) -> None:
        for user in sorted({e.user for e in self.plan}):
            for ws in self.waldur.get(user, "/hpc-workspaces/"):
                self.workspaces[(user, ws["account"])] = ws["resource_uuid"]
        log("workspaces: " + ", ".join(f"{u}/{a}" for u, a in self.workspaces))

    def refresh(self) -> None:
        """Latest state of our jobs, one listing per user and workspace.

        A workspace lists only the caller's own jobs.
        """
        for (user, _account), uuid in sorted(self.workspaces.items()):
            for job in self.waldur.get(
                user, f"/hpc-workspaces/{uuid}/jobs/", params={"time_window": "24h"}
            ):
                self.jobs[str(job["job_id"])] = job
        mine = {ev["job_id"]: k for k, ev in self.submitted.items()}
        for job_id, key in mine.items():
            job = self.jobs.get(job_id)
            if job and self.states.get(job_id) != job["state"]:
                self.states[job_id] = job["state"]
                self.write(
                    event="state",
                    key=key,
                    job_id=job_id,
                    state=job["state"],
                    reason=job.get("state_reason"),
                    nodes=job.get("nodes"),
                    exit_code=job.get("exit_code"),
                )
                log(f"job {job_id} ({key}) -> {job['state']} {job.get('nodes') or ''}")

    def pending_in(self, partition: str) -> int:
        ids = {ev["job_id"] for ev in self.submitted.values()}
        return sum(
            1
            for i in ids
            if self.jobs.get(i, {}).get("partition") == partition
            and self.jobs[i]["state"] == "PENDING"
        )

    def next_job_id(self) -> int:
        return max((int(i) for i in self.jobs if i.isdigit()), default=0) + 1

    def submit(self, entry: Entry, now: float) -> None:
        # Died between submitting and logging last time? The name is unique per run.
        for job in self.jobs.values():
            if (
                job["name"] == entry.spec["name"]
                and (job.get("submit_time") or 0) >= self.state["start"] - 60
            ):
                self._record(entry, str(job["job_id"]), recovered=True)
                return
        partition = entry.spec["partition"]
        if self.pending_in(partition) >= MAX_PENDING.get(partition, 2):
            self.delay[entry.key] = self.delay.get(entry.key, 0) + 120
            log(f"{entry.key}: {partition} queue full, delaying 2 min")
            return
        if entry.prefer_idle_gpu:
            waited = now - (self.state["start"] + entry.at)
            nxt = self.next_job_id()
            if predicted_gpu_use(nxt) > 0.35 and waited < self.args.idle_gpu_wait * 60:
                return  # try again when the next id predicts an idle card
        uuid = self.workspaces.get((entry.user, entry.account))
        if not uuid:
            log(f"{entry.key}: {entry.user} has no workspace for {entry.account}; skipped")
            self._record(entry, None, skipped=True)
            return
        body = {**entry.spec, "environment": entry.env}
        if self.args.dry_run:
            log(f"[dry-run] {entry.user} {entry.spec['name']} {partition} x{entry.spec['nodes']}")
            self._record(entry, f"dry-{entry.key}")
            return
        r = self.waldur.request(entry.user, "POST", f"/hpc-workspaces/{uuid}/jobs/", json=body)
        if r.status_code != 201:
            log(f"{entry.key}: submit failed {r.status_code}: {r.text[:300]}")
            self.delay[entry.key] = self.delay.get(entry.key, 0) + 300
            return
        self._record(entry, str(r.json()["job_id"]), warnings=r.json().get("warnings") or [])

    def _record(self, entry: Entry, job_id: Optional[str], **extra) -> None:
        ev = dict(
            event="submitted",
            key=entry.key,
            job_id=job_id,
            user=entry.user,
            account=entry.account,
            kind=entry.kind,
            name=entry.spec["name"],
            partition=entry.spec["partition"],
            nodes=entry.spec["nodes"],
            gpus=entry.spec.get("gpus_per_node", 0) * entry.spec["nodes"],
            run_seconds=entry.run_seconds,
            time_limit_minutes=entry.spec["time_limit_minutes"],
            expect=entry.expect,
            cancel_after=entry.cancel_after,
            **extra,
        )
        if job_id and entry.spec.get("gpus_per_node") and job_id.isdigit():
            ev["predicted_gpu_util"] = round(predicted_gpu_use(job_id), 2)
        self.write(**ev)
        self.submitted[entry.key] = ev
        if job_id:
            self.jobs.setdefault(
                job_id,
                {
                    "job_id": job_id,
                    "name": entry.spec["name"],
                    "partition": entry.spec["partition"],
                    "state": "PENDING",
                },
            )
        log(
            f"submitted {entry.key} as {entry.user}: job {job_id} {entry.spec['name']} "
            f"{entry.spec['partition']} x{entry.spec['nodes']} run {entry.run_seconds // 60} min "
            f"limit {entry.spec['time_limit_minutes']} min -> {entry.expect}"
            + (f" (gpu use {ev['predicted_gpu_util']})" if "predicted_gpu_util" in ev else "")
        )

    def cancels(self, now: float) -> None:
        for entry in self.plan:
            ev = self.submitted.get(entry.key)
            if (
                not ev
                or not ev.get("job_id")
                or entry.cancel_after is None
                or entry.key in self.cancelled
            ):
                continue
            job = self.jobs.get(ev["job_id"], {})
            if job.get("state") not in ACTIVE:
                self.cancelled.add(entry.key)
                continue
            start = job.get("start_time")
            if job.get("state") == "RUNNING" and start and now - start >= entry.cancel_after:
                uuid = self.workspaces[(entry.user, entry.account)]
                r = self.waldur.request(
                    entry.user, "POST", f"/hpc-workspaces/{uuid}/jobs/{ev['job_id']}/cancel/"
                )
                log(
                    f"cancel job {ev['job_id']} ({entry.key}) after {int(now - start) // 60} min: {r.status_code}"
                )
                if r.status_code < 300:
                    self.cancelled.add(entry.key)
                    self.write(event="cancelled", key=entry.key, job_id=ev["job_id"])

    def done(self) -> bool:
        if any(e.key not in self.submitted for e in self.plan):
            return False
        return all(e.key in self.cancelled for e in self.plan if e.cancel_after is not None)

    def run(self) -> int:
        stop = []
        signal.signal(signal.SIGTERM, lambda *_: stop.append(1))
        self.discover()
        log(
            f"plan: {len(self.plan)} jobs over {self.state['duration'] // 60} min, "
            f"{len(self.submitted)} already submitted"
        )
        while not stop:
            now = time.time()
            try:
                self.refresh()
                self.cancels(now)
                for entry in self.plan:
                    if entry.key in self.submitted:
                        continue
                    if self.state["start"] + entry.at + self.delay.get(entry.key, 0) > now:
                        continue
                    self.submit(entry, now)
            except (requests.RequestException, RuntimeError, KeyError, ValueError) as e:
                log(f"error: {e!r}; retrying next round")
            if self.done():
                log("all jobs submitted and handled; exiting")
                return 0
            for _ in range(self.args.poll):
                if stop:
                    break
                time.sleep(1)
        log("stopped")
        return 0


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--waldur-url",
        default=os.environ.get("WALDUR_URL", "http://localhost:8000"),
        help="Waldur backend (API at <url>/api)",
    )
    p.add_argument("--public-url", help="origin the browser login runs on (default --waldur-url)")
    p.add_argument(
        "--route",
        action="append",
        default=[],
        metavar="PREFIX=ORIGIN",
        help="send public-URL paths with PREFIX to ORIGIN, with X-Forwarded-* headers",
    )
    p.add_argument(
        "--password",
        default=os.environ.get("WORKLOAD_PASSWORD", "password"),
        help="Keycloak password of the users (env WORKLOAD_PASSWORD)",
    )
    p.add_argument(
        "--duration",
        type=parse_duration,
        default=parse_duration("2h"),
        help="span of arrivals, e.g. 90m, 2h (default 2h)",
    )
    p.add_argument("--mean-gap", type=float, default=5.0, help="mean minutes between arrivals (5)")
    p.add_argument("--no-first-batch", action="store_true", help="skip the front-loaded demo batch")
    p.add_argument("--seed", type=int, help="plan seed (default random, kept in state.json)")
    p.add_argument("--log-dir", default="workload", help="state.json + jobs.jsonl (./workload)")
    p.add_argument("--token-dir", help="also write <user>-token.txt there after each login")
    p.add_argument("--relogin-minutes", type=float, default=20, help="log in again this often (20)")
    p.add_argument(
        "--idle-gpu-wait",
        type=float,
        default=20,
        help="minutes the idle-GPU job waits for an id predicting low GPU use (20)",
    )
    p.add_argument("--poll", type=int, default=20, help="seconds between rounds (20)")
    p.add_argument("--reset", action="store_true", help="start a new run in --log-dir")
    p.add_argument(
        "--dry-run", action="store_true", help="log in and print the plan, submit nothing"
    )
    p.add_argument("--plan", action="store_true", help="print the plan and exit (no login)")
    args = p.parse_args()
    if args.plan:
        for e in build_plan(args.seed or 1, args.duration, not args.no_first_batch, args.mean_gap):
            print(
                f"{e.at // 60:4d} min  {e.user:6s} {e.account:15s} {e.spec['partition']:6s} "
                f"x{e.spec['nodes']}  run {e.run_seconds // 60:3d} / limit "
                f"{e.spec['time_limit_minutes']:3d} min  {e.expect:9s} {e.spec['name']}"
            )
        return 0
    return Driver(args).run()


if __name__ == "__main__":
    sys.exit(main())
