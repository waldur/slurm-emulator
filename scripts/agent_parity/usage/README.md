# Recording job-accounting golden data

`tests/test_jobacct_golden.py` replays a run of six jobs on the emulator and
compares what `sacct` and `GET /slurmdb/{v}/jobs` report with what a real
cluster reported for the same run (`tests/data/slurm_<release>_job_accounting.json`):

| Job | What happens |
|---|---|
| rocket, finished | Finish after 20 s and 10 s |
| running | Still running at query time |
| suspended | Suspended at +10 s, resumed at +25 s, suspended again at +40 s |
| requeued | Requeued at +30 s while running, so it has a REQUEUED run and a new pending record |
| pending | Held, so it has no eligible time |

The queries cover sacct with and without `--truncate` and `-D`, a window that
starts after the first jobs ended, and slurmrestd with no flags,
`skip_steps=true`, `skip_steps=true&show_duplicates=true` and
`disable_truncate_usage_time=true`. Times are stored as seconds after the
scenario start.

## Record

Build the accounting cluster from `../cluster/` (see `../README.md`), from
the Slurm release to record, for example the 25.11.7 tag:

```bash
mkdir -p /tmp/real-acct/src && cp ../cluster/Dockerfile /tmp/real-acct/
git -C "$(uv run scripts/slurm_src.py path 25.11)" archive slurm-25-11-7-1 | tar -x -C /tmp/real-acct/src
docker build -t real-slurm-acct:25.11.7 /tmp/real-acct
```

Start it, add the account and users, run the scenario and record straight
after it (the queries assume they run at +60 s):

```bash
docker create --name acct-real --hostname localhost -p 127.0.0.1:26830:6820 \
  real-slurm-acct:25.11.7 /run-config/start.sh
docker cp ../cluster acct-real:/run-config && docker start acct-real
until docker logs acct-real 2>&1 | grep -q READY; do sleep 2; done
docker exec acct-real sacctmgr -i add account acc1
docker exec acct-real sacctmgr -i add user alice,bob account=acc1
docker cp scenario.sh acct-real:/tmp/scenario.sh
docker exec acct-real sh /tmp/scenario.sh > /tmp/scenario.out
python3 record.py acct-real http://127.0.0.1:26830 /tmp/scenario.out \
  ../../../tests/data/slurm_25.11.7_job_accounting.json
```

The 25.11.7 data was recorded with node003/node004 configured with typed
GPUs (`Gres=gpu:h200:4`, `AccountingStorageTRES=gres/gpu,gres/gpu:h200`) and
`ThreadsPerCore=2`. The golden test does not compare TRES yet, because the
emulator has no GRES model.
