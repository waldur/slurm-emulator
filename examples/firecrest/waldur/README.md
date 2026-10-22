# Waldur HPC workspace on the slurm-emulator

Validation stack for Waldur's HPC workspace: Waldur users run and follow
Slurm jobs, browse files and look up partitions and software of a SLURM
allocation, from the portal and through the AI assistant, with FireCREST v2
in between and the emulator as the cluster.

```
browser ──► homeport ──► mastermind (dev stack, native)
   │                         │  user's Keycloak token
   │ OIDC login              ▼
   └──────────────► keycloak        firecrest (API) ──slurmrestd──► slurm (emulator)
                    (kcrealm)              └───────────SSH────────►   (/home, sbatch)
```

Mastermind calls FireCREST with the signed-in user's own access token, so
the cluster applies that user's POSIX identity and Slurm associations;
Waldur adds the allocation context (which account a job is charged to, the
published partitions/QoS limits, the software catalog) and confines each
workspace to its allocation's account.

## Run

```bash
# 1. Cluster, FireCREST, Keycloak (+ cluster accounts/associations)
PORT_BASE=8080 ./up.sh        # KC :8080, FireCREST :8081, slurmrestd :8082, SSH :8083
                              # (unset: 8080/8000/6820/2222)

# 2. Waldur dev stack from the workspace hub, then seed it
./scripts/start-dev-stack.sh --skip-preset
cd waldur-mastermind
KC_PORT=8080 FIRECREST_PORT=8081 AI_ASSISTANT_CREDS=/path/to/creds \
  uv run waldur shell < ../slurm-emulator/examples/firecrest/waldur/seed_waldur.py
```

Sign in to homeport with **HPC centre (Keycloak)** as `alice` / `password`
(or `bob`). Open the *Climate allocation* resource: tabs *Jobs*, *Files*,
*Cluster*; or ask the assistant "run GROMACS on 2 nodes in batch for an hour
in my climate allocation".

## What is seeded

| Cluster account | Users (associations) | Waldur |
|---|---|---|
| `alloc_climate` | alice, bob | project *Climate*, resource *Climate allocation* |
| `alloc_genomics` | alice | project *Genomics*, resource *Genomics allocation* |
| `alloc_private` | bob | not in Waldur — its jobs never show in a workspace |

Partitions `debug` (30 min, 2 nodes), `batch` (2 days, 16 nodes, QoS
`normal`/`long`), `gpu` (aarch64 + nvidia/cc90); an EESSI 2023.06 catalog
excerpt (GROMACS, OpenFOAM, Python, PyTorch, WRF) with per-architecture
builds. Jobs go PENDING → RUNNING after 5 s and COMPLETED after 40 s
when nodes are free (a job needing more nodes than are free waits with reason
`Resources`), or TIMEOUT at their time limit.

The cluster is not idle, so the allocation's charts have something to show:

- **History**: `up.sh` runs `seed_history.py` inside the slurm container
  (`docker compose exec -T slurm python3 - < seed_history.py`; idempotent):
  ~80 finished jobs over 21 days in `alloc_climate` (alice, bob) and
  `alloc_genomics` (alice) — WRF, GROMACS, BWA, MultiQC; 1–8 nodes; time
  limits 30 min–48 h; mostly COMPLETED with FAILED (exit 1/127/139, a cluster
  of failures on two consecutive days), TIMEOUT and CANCELLED; queue waits of
  minutes on `debug`, tens of minutes on `batch`, hours on `gpu`; synthetic CPU
  time and MaxRSS. It also writes sample files for the file viewers:
  `/home/alice/wrf-baltic/rsl.error.0000`, `/home/alice/md/energy.xvg`,
  `/home/alice/md/protein.pdb`, `/home/alice/qc/multiqc_report.html`.
- **Nodes** (`docker-compose.yml`): `node012`/`node013` drained, `node030`
  down (`SLURM_EMULATOR_NODE_STATES`); other tenants keep ~40% of `batch` and
  ~60% of `gpu` busy, rotating every 15 min (`SLURM_EMULATOR_BACKGROUND_LOAD`).
- **Reservations** (`SLURM_EMULATOR_RESERVATIONS`): a MAINT window on all
  nodes 3 days after the first start (jobs whose time limit would run into it
  do not start), a course on `debug` after 5 days, a GPU benchmark after 9.
- The accounting clock follows the wall clock (`SLURM_EMULATOR_TIME_FOLLOW_WALL=1`).

A submitted job runs `SLURM_EMULATOR_RUN_SECONDS` from its environment when
set (e.g. to watch a TIMEOUT), and `SLURM_EMULATOR_CPU_EFFICIENCY` /
`SLURM_EMULATOR_MEM_FRACTION` (0–1) fix its synthetic efficiency.

`up.sh` also applies `firecrest-patches/*.patch` to the FireCREST checkout
before building it (each once; a patch that no longer applies stops it).

## Notes

- Keycloak's public hostname is `localhost:<port>`, so no hosts entry is
  needed. FireCREST verifies the signature but not the issuer, so tokens
  issued for `localhost` validate inside the compose network.
- Mastermind only sends tokens to URLs in the `FIRECREST_ALLOWED_API_URLS`
  setting; the seed sets it to this stack's FireCREST.
- FireCREST and slurm-monitor accept only tokens issued for them
  (`aud=firecrest` / `aud=slurm-monitor`; `firecrest-patches/0002`,
  `SLURM_MONITOR_OAUTH_AUDIENCE`), so a user's sign-in token is refused there.
  Waldur exchanges it for one (Keycloak standard token exchange, which needs
  Keycloak 26.2 or later): the seed's allowlist entries end in `|firecrest`
  and `|slurm-monitor`, and `keycloak-setup` creates those clients, an
  optional client scope of the same name with the audience mapper, and turns
  on token exchange for the `waldur` client. This needs mastermind with
  waldur/waldur-mastermind#791.
- Moving an existing stack from Keycloak 26.0.7 to 26.7.5 recreates the
  container. In dev mode Keycloak keeps its data inside the container, so
  alice and bob get new user ids (`sub`). Waldur matches users by `sub`, so
  either reseed Waldur's users, or keep the old image for a stack whose
  Waldur users must survive.
- `SLURM_EMULATOR_JOB_EXECUTE=1` makes the emulator run each batch script
  (inside its container) when the emulated run ends: output files appear in
  `/home/<user>`, and a failing script ends the job FAILED with its exit code.
  `module`/`spack` are stand-ins that only echo, so a job "loads" software
  without it being installed.
