# Site-agent parity harness

Runs every SLURM operation of the waldur-site-agent SLURM plugin through both
of its clients -- `SlurmClient` (sacctmgr/sacct/scancel/scontrol CLI) and
`SlurmRestClient` (slurmrestd v0.0.45) -- against a real Slurm 26.05 cluster
and against this emulator, then compares the four runs step by step.
It found the REST-client bugs fixed in waldur/waldur-site-agent#76 and the
emulator gaps closed in waldur/slurm-emulator#4.

## Run

```bash
# 1. Real cluster image (once): see scripts/real_slurmrestd/ for the build kit,
#    then the accounting image from cluster/Dockerfile
docker build -t real-slurm-acct:26.05 scripts/agent_parity/cluster
scripts/agent_parity/real-start.sh          # fresh container, REST on 127.0.0.1:26820
scripts/agent_parity/emu-start.sh           # fresh emulator, REST on 127.0.0.1:26821
scripts/agent_parity/make-bins.sh           # work/bin-real, work/bin-emu wrappers

# 2. One run per client and target, from a waldur-site-agent checkout
#    (uv run inside it so its packages import); restart both targets between runs
cd ../waldur-site-agent
H=../slurm-emulator/scripts/agent_parity
PARITY_RAW_LOG=$H/work/real-cli.jsonl uv run python $H/run_scenario.py --mode cli --bin $H/work/bin-real --out $H/work/real-cli.json
PARITY_RAW_LOG=$H/work/emu-cli.jsonl  uv run python $H/run_scenario.py --mode cli --bin $H/work/bin-emu  --out $H/work/emu-cli.json
uv run python $H/run_scenario.py --mode rest --bin $H/work/bin-real --rest-url http://127.0.0.1:26820 --out $H/work/real-rest.json
uv run python $H/run_scenario.py --mode rest --bin $H/work/bin-emu  --rest-url http://127.0.0.1:26821 --out $H/work/emu-rest.json

# 3. Compare
python3 $H/compare.py $H/work > $H/work/comparison.txt
```

`comparison.txt` lists every step whose result differs, flagged
`CLI:emu≠real`, `REST:emu≠real` (emulator gaps) or `agent:cli≠rest`
(the two agent clients disagree on real Slurm). Timing-dependent values
(job ids, durations, reservation times) still differ between runs.

The reservation steps need a site agent with `MaintenanceReservation`;
without it they are skipped. `PARITY_CONTAINER`, `REAL_PORT`, `EMU_PORT`
and `STATE_DIR` override the defaults.

The emulator's own regression tests replay recorded real responses instead
of needing a cluster: `tests/test_slurmrestd_golden_slurmdb.py` and
`tests/test_cli_golden_site_agent.py` (recorded with
`scripts/real_slurmrestd/record.py` and `record_cli.py`).
