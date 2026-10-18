# Recording real slurmrestd responses

`tests/test_slurmrestd_golden_reservations.py` replays requests against the
emulator and compares them with what a real slurmrestd answered
(`tests/data/slurmrestd_<release>_reservation_requests.json`). To record
them again, for example for a new release:

```bash
uv run scripts/slurm_src.py update
mkdir -p /tmp/real-slurm && cp -r scripts/real_slurmrestd/* /tmp/real-slurm/
rsync -a --exclude .git "$(uv run scripts/slurm_src.py path 26.05)/" /tmp/real-slurm/src/
docker build -t real-slurmrestd:26.05 /tmp/real-slurm
docker run -d --name real-slurmrestd real-slurmrestd:26.05
docker cp cases.json real-slurmrestd:/tmp/cases.json   # [{"name", "path", "body"|"raw"}]
docker exec real-slurmrestd python3 /opt/probe/probe.py /tmp/cases.json --unix /run/rr/s.sock
```

The cluster has slurmctld but no slurmdbd, so every response carries
"Slurm accounting storage is disabled" warnings; drop those when writing
the golden file. `probe.py` prints status, errors, warnings and payload keys
per case; requests run in order on one cluster, so later cases see the
reservations earlier ones made.

The image lifts exactly one guard: slurmrestd's refusal to run as root
(`SLURMRESTD_SECURITY=disable_user_check` is compiled out of non-development
builds). Nothing in request parsing or handling is changed.
