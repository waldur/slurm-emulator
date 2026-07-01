# FireCREST-UI stack on the slurm-emulator

Launch the complete [firecrest-ui](https://github.com/eth-cscs/firecrest-ui) →
[firecrest-v2](https://github.com/eth-cscs/firecrest-v2) → cluster stack with the
**slurm-emulator** standing in for a real Slurm cluster. Everything runs in one
`docker compose` project; no HPC system required.

```
firecrest-ui (Remix, :3000)
   │  browser OIDC login ─────────────► keycloak (:8080, realm kcrealm)
   │  bearer-forwards token (server) ─► firecrest API (:8000)
                                            │  slurmrestd REST ─► slurm-emulator :6820
                                            │  SSH filesystem  ─► slurm-emulator :22
```

## Prerequisites

- Docker + Docker Compose v2, `git`, `ssh-keygen`.
- **A hosts entry** so the OIDC issuer URL resolves the same in your browser and
  inside the containers (Keycloak's classic dual-URL problem):

  ```
  sudo sh -c 'echo "127.0.0.1 keycloak" >> /etc/hosts'
  ```

## Quick start

```bash
cd examples/firecrest/ui
./up.sh                 # clones firecrest-v2, writes dev secrets, builds & starts
```

Then open **http://localhost:3000**, click login, and sign in as:

- **username:** `fireuser`  **password:** `password`

## What comes up

| Service | URL | Notes |
|---|---|---|
| firecrest-ui | http://localhost:3000 | the web UI |
| firecrest API | http://localhost:8000 | OpenAPI at `/docs` |
| keycloak | http://keycloak:8080/auth | realm `kcrealm`, admin `admin`/`admin2` |
| slurm-emulator (`cluster-emulator`) | slurmrestd http://localhost:6820, SSH localhost:2222 | `v0.0.46` |
| slurm-emulator #2 (`cluster-emulator-2`) | slurmrestd http://localhost:6821, SSH localhost:2223 | independent container/state |

Two clusters are wired to show FireCREST's multi-cluster support: `cluster-emulator`
(service `slurm`) and `cluster-emulator-2` (service `slurm2`). Each is its own
container with independent state, addressed by name in the API
(`/compute/{system}/...`) and the UI's "HPC Clusters" nav. Drop the `slurm2`
service + its `clusters:` entry in `firecrest/config.yaml` for a single-cluster setup.

They're deliberately given **different topologies** via `SLURM_EMULATOR_PARTITIONS`
so they look distinct: `slurm` = `debug:1-4,compute:5-100` (100 nodes), `slurm2` =
`gpu:8,compute:32` (40 nodes). The value accepts counts (`gpu:8`) or explicit node
ranges (`compute:5-100`).

### Persistence

Each emulator's job/accounting state (`SLURM_EMULATOR_STATE_FILE`) and uploaded
files (`/home`) live on named Docker volumes (`slurm_state`/`slurm_home`,
`slurm2_state`/`slurm2_home`), so **jobs and files survive `docker compose restart`
and container recreation**. `docker compose down -v` wipes them for a clean slate.

## What works

- **Login** via Keycloak (realm `kcrealm`, client `firecrest-web-ui`).
- **Compute**: submit / list / cancel jobs — the API drives the emulator's
  slurmrestd; submitted jobs advance PENDING→RUNNING→COMPLETED (wall clock,
  ~2s→8s) and show up in accounting.
- **Filesystem**: browse/upload/download in `/home/<user>` — the API runs real
  coreutils over the emulator's SSH plane (GNU coreutils in the Linux container).

## What is intentionally omitted

- **MinIO/S3, OpenFGA, PBS, ssh-ca** — not needed to boot or for the core
  compute/filesystem flow. Large-file transfers (which need S3) are out of scope.
- The emulator's SSH plane accepts any key (dev); the generated key in
  `secrets/` just satisfies the API's static-key loader.

## Caveats to verify for your versions

- **UI image / env scheme.** This wiring uses the `OIDC_*` variables from
  firecrest-ui `main`. Some builds (e.g. the firecrest-v2 *demo* image) use
  `KEYCLOAK_*` instead. If login misbehaves, check the pinned
  `${FIRECREST_UI_IMAGE}`'s `env_example` and adjust `docker-compose.yml`. If the
  image can't be pulled, build it from a checkout and set `FIRECREST_UI_IMAGE`
  (see `.env.example`).
- **API is built from source.** There is no reliable public API-only image, so
  the `firecrest` service builds from the cloned firecrest-v2 (`FIRECREST_V2_DIR`).
- **api_version.** The emulator serves `v0.0.46`; `firecrest/config.yaml` sets
  `scheduler.api_version: "0.0.46"` accordingly (upstream demo uses `0.0.42`).

## Files

- `docker-compose.yml` — the stack.
- `firecrest/config.yaml` — API config: Keycloak auth + one cluster → emulator.
- `keycloak/realm-kcrealm.json`, `keycloak/keycloak.env` — realm import (from
  firecrest-v2), clients incl. `firecrest-web-ui`.
- `up.sh` — clone + secrets + `docker compose up`.
- `.env.example` — knobs (host IP, image tags, session secrets).

## Teardown

```bash
docker compose down -v
```
