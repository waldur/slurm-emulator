# FireCREST v2 end-to-end with slurm-emulator

Run the full FireCREST v2 stack with the slurm-emulator standing in for the real
Slurm cluster — the emulator answers both the **scheduler plane** (slurmrestd on
`:6820`) and the **SSH filesystem plane** (`:22`). FireCREST's own
Keycloak/OpenFGA/MinIO come from its compose unchanged.

See `../../docs/firecrest-conformance.md` for what is and isn't covered.

## Quick start

```bash
# from this repo root
bash scripts/firecrest_e2e.sh
```

This builds `opennode/slurm-emulator:latest`, clones FireCREST, brings the stack
up with `docker-compose.override.yml`, waits for slurmrestd, and runs a direct
scheduler smoke. Override with `FIRECREST_REF`, `SLURM_SERVICE`, `WORKDIR`.

## Files

- `docker-compose.override.yml` — swaps FireCREST's `slurm` service for the
  emulator image, enables the SSH plane, exposes `6820`/`22`, and replaces the
  `scontrol ping` healthcheck with the REST ping.
- `f7t-api-config.emulator.yaml` — a FireCREST cluster entry pointing
  `scheduler.api_url` at `http://slurm:6820`, `api_version: "0.0.46"`,
  `connection_mode: rest`, and `ssh.host` at the emulator.

## Notes / caveats

- **Service name**: the overlay assumes FireCREST's Slurm service is named
  `slurm`. Confirm in your FireCREST `docker-compose.yml` and set
  `SLURM_SERVICE` / rename the overlay key if it differs.
- **api_version**: keep FireCREST's `scheduler.api_version` at `0.0.46` so the
  submit request dialect matches the emulator (script inside the job body,
  `environment` as a list).
- **Filesystem paths**: the overlay sets `SLURM_EMULATOR_FS_ROOT=/` so absolute
  paths FireCREST sends over SSH (e.g. `/home/<user>`) resolve against the
  emulator container's real filesystem.
- **Auth**: FireCREST forwards the end-user OIDC token as `X-SLURM-USER-TOKEN`;
  the emulator accepts any token by default (set `SLURM_EMULATOR_JWT_KEY` to
  enforce HS256 verification).
- **Out of scope**: large data transfers (S3/streamer/wormhole) and FireCREST's
  own authn/authz remain FireCREST's responsibility.
