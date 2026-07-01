# FireCREST integration (example)

This directory is an **example integration**, not part of the emulator core. The
repository's product is the SLURM emulator (`emulator/`); this shows how a real
client — [FireCREST v2](https://github.com/eth-cscs/firecrest-v2) and
[firecrest-ui](https://github.com/eth-cscs/firecrest-ui) — runs against it
unmodified, because the emulator faithfully speaks `slurmrestd` + a login-node
SSH plane.

Nothing here is imported by the emulator package; it's all deployment configs,
scripts and docs.

## Contents

| Path | What |
|---|---|
| [`ui/`](ui/) | Self-contained docker-compose stack: **firecrest-ui → FireCREST API → emulator** + Keycloak. Run `ui/up.sh`. Two clusters, persistence. |
| [`e2e/`](e2e/) | Overlay onto FireCREST v2's *own* docker-compose, swapping its `slurm` service for the emulator. Run `e2e/run.sh`. |
| [`ui-guide.md`](ui-guide.md) | Walkthrough with screenshots: run & evaluate the stack via the UI. |
| [`conformance.md`](conformance.md) | Field-by-field parity matrix (what FireCREST calls, what the emulator serves). |
| [`img/`](img/) | Screenshots used by the guide. |

## Which one?

- **Evaluate/demo the UI** → `ui/` (start with [`ui-guide.md`](ui-guide.md)).
- **Test the emulator inside FireCREST's upstream compose** (scheduler REST
  plane, CI-style) → `e2e/`.

The emulator's own FireCREST conformance tests live with the rest of the suite
under the repo's `tests/` (`test_firecrest_contract.py`, `firecrest/`).
