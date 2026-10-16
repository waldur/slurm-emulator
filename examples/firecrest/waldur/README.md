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
builds. Jobs go PENDING → RUNNING after 5 s and COMPLETED after 40 s.

## Notes

- Keycloak's public hostname is `localhost:<port>`, so no hosts entry is
  needed. FireCREST verifies the signature but not the issuer, so tokens
  issued for `localhost` validate inside the compose network.
- Mastermind only sends tokens to URLs in the `FIRECREST_ALLOWED_API_URLS`
  setting; the seed sets it to this stack's FireCREST.
- `SLURM_EMULATOR_JOB_EXECUTE=1` makes the emulator run each batch script
  (inside its container) when the emulated run ends: output files appear in
  `/home/<user>`, and a failing script ends the job FAILED with its exit code.
  `module`/`spack` are stand-ins that only echo, so a job "loads" software
  without it being installed.
