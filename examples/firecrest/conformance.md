# FireCREST v2 conformance

This document tracks how the SLURM emulator stands in for a real cluster when
running [eth-cscs/firecrest-v2](https://github.com/eth-cscs/firecrest-v2), and
which harness verifies each piece. "100% conformance" means every FireCREST →
cluster interaction below is either **supported** or explicitly **out of scope**.

FireCREST reaches a cluster over two planes:

- **Scheduler plane** — slurmrestd REST (`connection_mode: rest`), served by
  `slurmrestd-emulator` on `:6820`.
- **SSH plane** — a mandatory transport (`HPCCluster.ssh` is required on every
  cluster even in `rest` mode) used for all filesystem operations, job
  stdout/stderr/script retrieval (`get_job_metadata`) and submit-by-path. Served
  by `slurm-ssh-emulator` on `:2222`.

FireCREST's own infra (Keycloak/OIDC, OpenFGA, MinIO/S3) is **not** the cluster's
job — reuse FireCREST's containers for those.

## Scheduler plane (slurmrestd)

| FireCREST call | slurmrestd request | Status | Harness |
|---|---|---|---|
| `submit_job` | `POST /slurm/{v}/job/submit` → reads `job_id` | ✅ | contract, real-client |
| `get_job` | `GET /slurmdb/{v}/job/{id}` + `GET /slurm/{v}/job/{id}` (merged, PENDING wins) | ✅ | contract, real-client |
| `get_jobs` | `GET /slurmdb/{v}/jobs` + `GET /slurm/{v}/jobs` (`?account=`) | ✅ | contract, real-client |
| `cancel_job` | `DELETE /slurm/{v}/job/{id}` (200 = ok) | ✅ | contract, real-client |
| job lifecycle | PENDING→RUNNING→COMPLETED (poll observes progress) | ✅ (configurable clock) | contract |
| `get_nodes` | `GET /slurm/{v}/nodes` | ✅ | contract, real-client |
| `get_partitions` | `GET /slurm/{v}/partitions` | ✅ | contract, real-client |
| `get_reservations` | `GET /slurm/{v}/reservations` | ✅ (empty list) | contract |
| accounts | `GET /slurmdb/{v}/associations?user={name}` → `account` + `is_default` | ✅ | contract, real-client |
| `ping` | `GET /slurm/{v}/ping` | ✅ | contract, real-client |

Field-shape notes (verified by the contract test):
- `job_state` is a list; `state.current` in accounting is a list.
- `start_time`/`end_time`/`time_limit`/`priority` are `{set, infinite, number}` structs.
- `job_resources.nodes.count` is present (FireCREST's `SlurmJob` reads it).
- API version is `v0.0.46`; set FireCREST `scheduler.api_version: "0.0.46"` so the
  request dialect matches (script inside the job body, `environment` as a list).

## SSH plane (filesystem + job metadata)

| FireCREST need | Over SSH | Status | Notes |
|---|---|---|---|
| Filesystem ops (`ls`, `stat`, `mkdir`, `chmod`, `chown`, `rm`, `ln`, `tar`, `dd`, `head`, `tail`, `file`, `id`, checksum) | real coreutils in a sandbox home | ✅ | GNU coreutils output matches FireCREST parsers; on macOS, Homebrew GNU tools are auto-detected and preferred (`brew install coreutils gnu-tar findutils gnu-sed grep`) |
| Small file up/download | `dd`/`head`/`tail` | ✅ | bounded by FireCREST `max_ops_file_size` |
| `get_job_metadata` (stdout/stderr/script) | `sacct -j` / `scontrol show job` | ✅ | REST client raises NotImplementedError → SSH fallback |
| submit-by-`script_path` | `sbatch <path>` | ✅ | always SSH even in rest mode |

Security: the SSH plane runs commands as the emulator's own OS user, confined only
by the sandbox working directory (`SLURM_EMULATOR_FS_ROOT`). Dev/test only.

## Out of scope (not the emulator's job)

- FireCREST authn/authz: **Keycloak (OIDC)** and **OpenFGA** — run FireCREST's own.
- Large data transfer backends: **S3/MinIO**, streamer, magic-wormhole.
- FireCREST `connection_mode: ssh` / `hybrid` full test matrix (the SSH plane
  supports the commands, but we don't assert that path end-to-end here).
- Job arrays and job steps (not required by FireCREST's core compute flow).

## Configuration knobs

| Env var | Default | Meaning |
|---|---|---|
| `SLURM_EMULATOR_JOB_CLOCK` | `wall` | `wall` = real-time lifecycle; `time` = simulated clock (deterministic tests) |
| `SLURM_EMULATOR_JOB_RUN_DELAY` | `2` | seconds from submit → RUNNING |
| `SLURM_EMULATOR_JOB_RUN_DURATION` | `8` | seconds RUNNING → COMPLETED |
| `SLURM_EMULATOR_SSH_PORT` | `2222` | SSH listen port |
| `SLURM_EMULATOR_SSH_HOST_KEY` | (ephemeral) | path to persist the SSH host key |
| `SLURM_EMULATOR_FS_ROOT` | `/tmp/slurm_emulator_fs` | sandbox filesystem root |
| `SLURM_EMULATOR_STATE_FILE` / `SLURM_EMULATOR_TIME_FILE` | `/tmp/slurm_emulator_*.json` | shared state (REST + SSH + CLI) |

## Harnesses

1. **Contract tests** — `tests/test_firecrest_contract.py`. Boots the ASGI app
   in-process, asserts envelopes/field shapes against FireCREST's request set.
   Runs in CI on every commit; no FireCREST runtime needed.
   `uv run --extra dev pytest tests/test_firecrest_contract.py`
2. **Real-client conformance** — `tests/firecrest/test_real_client.py`. Imports
   FireCREST's own `SlurmRestClient` from a checkout (`FIRECREST_SRC=/path/to/firecrest-v2`)
   and drives it against a live emulator. Skipped when `FIRECREST_SRC` is unset.
3. **Docker-compose e2e** — `examples/firecrest/e2e/`. Swaps FireCREST's real `slurm`
   service for the emulator image (slurmrestd + ssh) and reuses its
   Keycloak/OpenFGA/MinIO. `bash examples/firecrest/e2e/run.sh`.
