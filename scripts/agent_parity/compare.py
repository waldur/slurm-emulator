"""Side-by-side of the four runs: what the agent's clients returned per step."""

import json
import re
import sys
from pathlib import Path

D = Path(sys.argv[1])
RUNS = ["real-cli", "emu-cli", "real-rest", "emu-rest"]
data = {n: json.loads((D / f"{n}.json").read_text()) for n in RUNS}


def mask(value, jobs):
    text = json.dumps(value, sort_keys=True)
    for key, jid in jobs.items():
        if jid:
            text = re.sub(rf'"{jid}"', f'"<job:{key}>"', text)
    value = json.loads(text)

    def walk(v, key=""):
        if isinstance(v, dict):
            return {k: walk(x, k) for k, x in v.items()}
        if isinstance(v, list):
            return [walk(x, key) for x in v]
        if key in ("tres_usage",) or (
            isinstance(v, (int, float)) and key in ("cpu", "mem", "gres/gpu", "duration")
        ):
            return "<n>" if v else 0
        return v

    return walk(value)


def outcome(step, jobs):
    if step is not None and not re.search(r"job|cancel|usage", step["step"], re.IGNORECASE):
        jobs = {}
    if step is None:
        return "—"
    if "error" in step:
        return "ERROR " + re.sub(r"\s+", " ", step["error"])[:160]
    return json.dumps(mask(step.get("result"), jobs), sort_keys=True)[:400]


by_label = {n: {s["step"]: s for s in d["steps"]} for n, d in data.items()}
labels = [s["step"] for s in data["real-cli"]["steps"]]
for extra in by_label["real-rest"]:
    if extra not in labels:
        labels.append(extra)

rows = []
for label in labels:
    o = {n: outcome(by_label[n].get(label), data[n]["jobs"]) for n in RUNS}
    flags = []
    if o["real-cli"] != o["emu-cli"]:
        flags.append("CLI:emu≠real")
    if o["real-rest"] != o["emu-rest"]:
        flags.append("REST:emu≠real")
    if o["real-cli"] != o["real-rest"]:
        flags.append("agent:cli≠rest")
    rows.append({"step": label, "flags": flags, **o})

(D / "comparison.json").write_text(json.dumps(rows, indent=1))
for r in rows:
    if r["flags"]:
        print(f"## {r['step']}  [{', '.join(r['flags'])}]")
        for n in RUNS:
            print(f"   {n:9} {r[n]}")
print(f"\n{sum(1 for r in rows if r['flags'])}/{len(rows)} steps differ somewhere")
