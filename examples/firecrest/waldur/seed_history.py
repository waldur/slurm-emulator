"""Seed ~3 weeks of finished jobs and sample output files into the emulator.

Runs inside the slurm container, against the same state file the REST and
SSH planes use (up.sh pipes it in):

    docker compose exec -T slurm python3 - < seed_history.py

Idempotent: if the accounting already holds the seeded jobs it does
nothing. Jobs go straight into accounting as usage records with everything
slurmdbd would keep -- submit/start/end, time limit, node list, CPUs,
requested memory, CPU time and MaxRSS -- so the allocation's charts (usage,
timeline, requested vs used walltime, outcomes per day, efficiency, queue
wait per partition) have history from the first login.

Env (all optional): SEED_DAYS (21), SEED_JOBS (~82), SEED_RANDOM (2026).
"""

from __future__ import annotations

import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

from emulator.core import cluster_model, nss
from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import standard_node_raw_tres

DAYS = int(os.environ.get("SEED_DAYS", "21"))
RNG = random.Random(int(os.environ.get("SEED_RANDOM", "2026")))
MARKER_NAME = "wrf-baltic-3km"

# name: (account, users, partitions, node choices, limits (min), used fraction,
#        cpu efficiency range, memory fraction range, cpus per node or None = whole node,
#        memory per node MB or None, weight)
TEMPLATES = {
    "wrf-baltic-3km": (
        "alloc_climate", ["alice", "alice", "bob"], ["batch"], [4, 6, 8],
        [1440, 2160, 2880], (0.25, 0.8), (0.80, 0.95), (0.35, 0.60), None, None, 18,
    ),
    "wrf-post": (
        "alloc_climate", ["alice", "bob"], ["batch", "batch", "debug"], [1],
        [120, 240], (0.08, 0.5), (0.05, 0.15), (0.05, 0.15), None, None, 20,
    ),
    "gromacs-md": (
        "alloc_climate", ["alice", "bob"], ["gpu"], [1, 2, 4],
        [720, 1440], (0.4, 0.95), (0.85, 0.97), (0.10, 0.25), None, None, 12,
    ),
    "md-equil": (
        "alloc_climate", ["alice"], ["gpu", "debug"], [1],
        [240, 480], (0.2, 0.7), (0.60, 0.90), (0.10, 0.20), None, None, 8,
    ),
    "bwa-align": (
        "alloc_genomics", ["alice"], ["batch"], [1, 2],
        [240, 480, 720], (0.2, 0.7), (0.65, 0.92), (0.30, 0.70), 32, 128000, 14,
    ),
    "multiqc": (
        "alloc_genomics", ["alice"], ["debug"], [1],
        [30], (0.05, 0.3), (0.20, 0.45), (0.20, 0.50), 4, 16000, 10,
    ),
}
DEBUG_LIMIT = 30  # minutes, the debug partition's cap in Waldur
# Submit -> start wait (minutes) per partition: debug minutes, batch tens of
# minutes, gpu hours.
WAITS = {"debug": (0.5, 8), "batch": (8, 75), "gpu": (60, 360)}
FAILURE_EXIT_CODES = [1, 127, 139]


def _already_seeded(db: SlurmDatabase) -> bool:
    return any(
        r.job_name == MARKER_NAME and r.account == "alloc_climate" for r in db.usage_records
    )


def _plan_jobs(now: datetime) -> list[dict]:
    names = [n for n, t in TEMPLATES.items() for _ in range(t[-1])]
    RNG.shuffle(names)
    window_start = now - timedelta(days=DAYS)
    # Two consecutive bad days (a broken module after a system update):
    # WRF segfaults, a missing binary, post-processing errors.
    bad_start = (now - timedelta(days=9)).replace(hour=0, minute=0, second=0, microsecond=0)
    bad_end = bad_start + timedelta(days=2)
    plans = []
    for name in names:
        submit = window_start + timedelta(seconds=RNG.uniform(0, (DAYS - 0.4) * 86400))
        plans.append({"name": name, "submit": submit})
    for i in range(7):  # make the bad days visibly busy
        name = ["wrf-baltic-3km", "wrf-post", "bwa-align"][i % 3]
        plans.append(
            {"name": name, "submit": bad_start + timedelta(hours=RNG.uniform(1, 46))}
        )
    plans.sort(key=lambda p: p["submit"])
    for plan in plans:
        plan["bad_day"] = bad_start <= plan["submit"] < bad_end
    return plans


def _outcome(plan: dict) -> str:
    name = plan["name"]
    if plan["bad_day"] and name in ("wrf-baltic-3km", "wrf-post", "bwa-align"):
        return "FAILED" if RNG.random() < 0.85 else "COMPLETED"
    roll = RNG.random()
    if roll < 0.07:
        return "FAILED"
    if roll < 0.14:
        return "TIMEOUT"
    if roll < 0.20:
        return "CANCELLED"
    return "COMPLETED"


def _allocate(partition: str, count: int, start: datetime, busy: dict, skip: set):
    """Lowest free nodes at ``start``; slide the start until enough are free."""
    nodes = [n for n in cluster_model.partition_nodes(partition) if n not in skip]
    count = min(count, len(nodes))
    while True:
        free = [n for n in nodes if busy.get(n, datetime.min) <= start]
        if len(free) >= count:
            return free[:count], start
        start = min(busy[n] for n in nodes if busy.get(n, datetime.min) > start)


def build_records(db: SlurmDatabase, now: datetime) -> list[UsageRecord]:
    skip = set(cluster_model.node_state_overrides())
    busy: dict[str, datetime] = {}
    records = []
    for plan in _plan_jobs(now):
        name = plan["name"]
        (account, users, partitions, node_choices, limits, used, cpu_eff, mem_frac,
         cpus_per_node, mem_per_node, _weight) = TEMPLATES[name]
        user = RNG.choice(users)
        partition = RNG.choice(partitions)
        nodes_wanted = RNG.choice(node_choices)
        limit = DEBUG_LIMIT if partition == "debug" else RNG.choice(limits)
        submit = plan["submit"].replace(microsecond=0)
        state = _outcome(plan)
        exit_code = 0
        if state == "FAILED":
            exit_code = 139 if name.startswith(("wrf-baltic", "gromacs")) else RNG.choice(
                FAILURE_EXIT_CODES
            )
        wait = timedelta(minutes=RNG.uniform(*WAITS[partition]))

        cancelled_pending = state == "CANCELLED" and RNG.random() < 0.5
        if cancelled_pending:
            end = submit + wait / 2
            start = end
            node_list: list[str] = []
            elapsed = 0.0
        else:
            node_list, start = _allocate(partition, nodes_wanted, submit + wait, busy, skip)
            if state == "TIMEOUT":
                elapsed = limit * 60.0
            elif state == "FAILED":
                elapsed = limit * 60.0 * RNG.uniform(0.005, 0.15)
            elif state == "CANCELLED":
                elapsed = limit * 60.0 * RNG.uniform(0.05, 0.4)
            else:
                elapsed = limit * 60.0 * RNG.uniform(*used)
            end = start + timedelta(seconds=int(elapsed))
        if end > now - timedelta(minutes=10):
            continue  # still running at "now": leave it out of the history
        for node in node_list:
            busy[node] = end

        count = len(node_list)
        whole_node = cpus_per_node is None
        alloc_cpus = count * (cluster_model.NODE_CPUS if whole_node else cpus_per_node)
        req_mem = mem_per_node or (
            cluster_model.NODE_MEM_MB
            if whole_node
            else cpus_per_node * cluster_model.DEF_MEM_PER_CPU_MB
        )
        eff = RNG.uniform(*cpu_eff)
        frac = RNG.uniform(*mem_frac)
        node_hours = elapsed / 3600.0 * count
        workdir = f"/home/{user}/{name.split('-')[0]}"
        records.append(
            UsageRecord(
                account=account,
                user=user,
                node_hours=node_hours,
                billing_units=node_hours,
                timestamp=end,
                period=f"{end.year}-Q{(end.month - 1) // 3 + 1}",
                raw_tres=standard_node_raw_tres(node_hours, partition),
                cluster=db.current_cluster,
                partition=partition,
                job_id=db.allocate_job_id(),
                state=state,
                job_name=name,
                working_directory=workdir,
                exit_code=exit_code,
                standard_output=f"{workdir}/{name}-%j.out",
                standard_error=f"{workdir}/{name}-%j.err",
                standard_input="/dev/null",
                submit_time=submit,
                eligible_time=submit,
                start_time=start,
                end_time=end,
                time_limit=limit,
                node_count=count,
                node_list=cluster_model.compress_hostlist(node_list) or "None assigned",
                alloc_cpus=alloc_cpus,
                req_mem_mb=req_mem,
                cpu_time_seconds=elapsed * alloc_cpus * eff,
                max_rss_bytes=cluster_model.max_rss_bytes(frac, req_mem) if count else 0,
                tasks=0 if whole_node else count,
            )
        )
    return records


# --- sample output files ---


def _wrf_rsl(start: datetime) -> str:
    rng = random.Random(7)
    lines = [
        "taskid: 0 hostname: node005",
        " module_io_quilt_old.F        2931 F",
        "Quilting with   1 groups of   0 I/O tasks.",
        " Ntasks in X           16 , ntasks in Y           32",
        "--- WARNING: traj_opt is zero, but num_traj is not zero; setting num_traj to zero.",
        "--- NOTE: grid_fdda is 0 for domain      1, setting gfdda interval and ending time to 0 for that domain.",
        "  Domain # 1: dx =  3000.000 m",
        "WRF V4.5.2 MODEL",
        " *************************************",
        " Parent domain",
        " ids,ide,jds,jde            1         451           1         601",
        " ims,ime,jms,jme           -4          36          -4          28",
        " ips,ipe,jps,jpe            1          29           1          19",
        " *************************************",
        "DYNAMICS OPTION: Eulerian Mass Coordinate",
        "   alloc_space_field: domain            1 ,            3261534824  bytes allocated",
        "  med_initialdata_input: calling input_input",
        "Timing for processing wrfinput file (stream 0) for domain        1:    4.81327 elapsed seconds",
        "Max map factor in domain 1 =  1.03. Scale the dt in the model accordingly.",
        f"Timing for Writing wrfout_d01_{start:%Y-%m-%d_%H:%M:%S} for domain        1:    2.94181 elapsed seconds",
        "Timing for processing lateral boundary for domain        1:    0.61925 elapsed seconds",
    ]
    dt = 18  # seconds; ~6 x dx[km], the usual WRF time step for 3 km
    steps = int(48 * 3600 / dt)  # two thirds of a 72 h forecast
    t = start
    for step in range(1, steps + 1):
        t = start + timedelta(seconds=step * dt)
        cost = 1.18 + rng.gauss(0, 0.04) + (0.35 if step % 200 == 0 else 0.0)
        lines.append(
            f"Timing for main: time {t:%Y-%m-%d_%H:%M:%S} on domain   1:"
            f" {cost:10.5f} elapsed seconds"
        )
        if (step * dt) % 3600 == 0:
            lines.append(
                f"Timing for Writing wrfout_d01_{t:%Y-%m-%d_%H:%M:%S} for domain        1:"
                f" {2.8 + rng.random():10.5f} elapsed seconds"
            )
            if (step * dt) % (6 * 3600) == 0:
                lines.append(
                    "Timing for processing lateral boundary for domain        1:"
                    f" {0.55 + rng.random() * 0.2:10.5f} elapsed seconds"
                )
    return "\n".join(lines) + "\n"


def _energy_xvg(now: datetime) -> str:
    rng = random.Random(11)
    head = [
        f"# This file was created {now:%a %b %d %H:%M:%S %Y}",
        "# Created by:",
        "#                    :-) GROMACS - gmx energy, 2023.3 (-:",
        "# ",
        "# Executable:   /cvmfs/software.eessi.io/versions/2023.06/software/linux/x86_64/amd/zen3/software/GROMACS/2023.3-foss-2023a/bin/gmx",
        "# Data prefix:  /cvmfs/software.eessi.io/versions/2023.06/software/linux/x86_64/amd/zen3/software/GROMACS/2023.3-foss-2023a",
        "# Working dir:  /home/alice/md",
        "# Command line:",
        "#   gmx energy -f md.edr -o energy.xvg",
        "# gmx energy is part of G R O M A C S:",
        "#",
        "# Good gRace! Old Maple Actually Chews Slate",
        "#",
        '@    title "GROMACS Energies"',
        '@    xaxis  label "Time (ps)"',
        '@    yaxis  label "(kJ/mol), (K)"',
        "@TYPE xy",
        "@ view 0.15, 0.15, 0.75, 0.85",
        "@ legend on",
        "@ legend box on",
        "@ legend loctype view",
        "@ legend 0.78, 0.8",
        "@ legend length 2",
        '@ s0 legend "Potential"',
        '@ s1 legend "Temperature"',
    ]
    rows = []
    for i in range(100):
        time_ps = i * 10.0
        potential = -583950.0 + 1650.0 * 2.718281828 ** (-time_ps / 120.0) + rng.gauss(0, 260)
        temperature = 300.0 + rng.gauss(0, 1.4) + 6.0 * 2.718281828 ** (-time_ps / 40.0)
        rows.append(f"{time_ps:12.6f}  {potential:14.6f}  {temperature:10.6f}")
    return "\n".join(head + rows) + "\n"


def _protein_pdb() -> str:
    # A short helical peptide (Ac-AEAAAKEAAAKA): backbone + CB on an ideal
    # alpha helix (rise 1.5 A, 100 deg per residue).
    import math

    residues = ["ALA", "GLU", "ALA", "ALA", "ALA", "LYS", "GLU", "ALA", "ALA", "ALA", "LYS", "ALA"]
    atoms = [("N", 1.55, -0.95, "N"), ("CA", 2.30, 0.0, "C"), ("C", 1.65, 0.85, "C"),
             ("O", 1.90, 1.75, "O"), ("CB", 3.45, 0.25, "C")]
    lines = [
        "HEADER    DE NOVO PROTEIN                         16-OCT-26   XXXX",
        "TITLE     EQUILIBRATED HELICAL PEPTIDE AEAAAKEAAAKA, FRAME 1000 PS",
        "REMARK    GENERATED BY GROMACS GMX TRJCONV 2023.3",
        "CRYST1   45.000   45.000   45.000  90.00  90.00  90.00 P 1           1",
        "MODEL        1",
    ]
    serial = 1
    for index, resname in enumerate(residues, start=1):
        for name, radius, dz, element in atoms:
            angle = math.radians(100.0 * index + (25.0 if name in ("C", "O") else 0.0))
            x = 22.5 + radius * math.cos(angle)
            y = 22.5 + radius * math.sin(angle)
            z = 10.0 + 1.5 * index + dz
            atom_name = f" {name:<3}" if len(name) < 4 else name
            lines.append(
                f"ATOM  {serial:5d} {atom_name} {resname} A{index:4d}    "
                f"{x:8.3f}{y:8.3f}{z:8.3f}{1.00:6.2f}{0.00:6.2f}          {element:>2}"
            )
            serial += 1
    lines.append(f"TER   {serial:5d}      {residues[-1]} A{len(residues):4d}")
    lines += ["ENDMDL", "END"]
    return "\n".join(lines) + "\n"


def _multiqc_html(now: datetime) -> str:
    samples = [
        ("SRR3192396", 38.2, 41.5, 98.7, 12.1),
        ("SRR3192397", 41.0, 40.9, 98.9, 11.4),
        ("SRR3192398", 36.7, 42.1, 97.8, 14.9),
        ("SRR3192399", 44.3, 41.2, 99.1, 10.2),
        ("SRR3192400", 29.8, 43.0, 96.4, 18.6),
    ]
    rows = "\n".join(
        f"<tr><td>{s}</td><td>{m:.1f}</td><td>{gc:.1f}%</td><td>{al:.1f}%</td><td>{dup:.1f}%</td></tr>"
        for s, m, gc, al, dup in samples
    )
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>MultiQC Report</title>
<style>
body{{font-family:Helvetica,Arial,sans-serif;margin:2em;color:#222}}
h1{{font-weight:300}} table{{border-collapse:collapse}}
th,td{{border:1px solid #ccc;padding:4px 10px;text-align:right}}
th{{background:#f4f4f4}} td:first-child,th:first-child{{text-align:left}}
</style></head><body>
<h1>MultiQC v1.21</h1>
<p>Report generated on {now:%Y-%m-%d, %H:%M %Z}UTC based on data in <code>/home/alice/qc/bwa-align</code>.</p>
<h2>General Statistics</h2>
<table><thead><tr><th>Sample Name</th><th>M Seqs</th><th>% GC</th><th>% Aligned</th><th>% Dups</th></tr></thead>
<tbody>
{rows}
</tbody></table>
<p>Modules: FastQC, Samtools (flagstat), Picard (MarkDuplicates).</p>
</body></html>
"""


def write_files(now: datetime) -> list[Path]:
    root = Path(os.environ.get("SLURM_EMULATOR_FS_ROOT", "/tmp/slurm_emulator_fs"))
    home = root / "home" / "alice"
    forecast = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    files = {
        home / "wrf-baltic" / "rsl.error.0000": _wrf_rsl(forecast),
        home / "md" / "energy.xvg": _energy_xvg(now),
        home / "md" / "protein.pdb": _protein_pdb(),
        home / "qc" / "multiqc_report.html": _multiqc_html(now),
    }
    for path, text in files.items():
        if path.exists():
            continue  # never clobber a file the user may have changed
        path.parent.mkdir(parents=True, exist_ok=True)
        path.parent.chmod(0o755)
        path.write_text(text)
        path.chmod(0o644)
    # In NSS mode the SSH plane hands a home to its user once per identity
    # (a stamp file); drop the stamp so the new files get chowned too, and
    # chown now if we can.
    (home / ".slurm-emulator-owner").unlink(missing_ok=True)
    identity = nss.resolve("alice") if nss.enabled() and os.geteuid() == 0 else None
    if identity is not None:
        for path in [home, *{p.parent for p in files}, *files]:
            os.chown(path, identity.uid, identity.gid)
    return list(files)


def main() -> int:
    db = SlurmDatabase()
    db.load_state()
    now = TimeEngine().get_current_time()
    if abs((now - datetime.now()).total_seconds()) > 86400:
        # History is laid out on the wall clock; the simulated clock must
        # agree or accounting windows miss it (SLURM_EMULATOR_TIME_FOLLOW_WALL=1).
        print(f"note: emulator clock is {now:%Y-%m-%d}; seeding around the wall clock instead")
        now = datetime.now()
    paths = write_files(now)
    print(f"sample files: {', '.join(str(p) for p in paths)}")
    if _already_seeded(db):
        print("job history already seeded; nothing to do")
        return 0
    records = build_records(db, now)
    db.usage_records.extend(records)
    db.save_state()
    by_state: dict[str, int] = {}
    for r in records:
        by_state[r.state] = by_state.get(r.state, 0) + 1
    print(f"seeded {len(records)} finished jobs over {DAYS} days: {by_state}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
