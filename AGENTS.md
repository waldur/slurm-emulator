# AGENTS.md

These instructions apply to the whole `slurm-emulator` repository.

## Project Overview

SLURM Emulator is a time-travel enabled emulator for testing SLURM
periodic-limits functionality. It provides time manipulation, usage
injection, decay calculations, and command emulation used by Waldur
Site Agent tests.

## Development Rules

- Use `uv` for Python package management and tool execution.
- Do not use `pip`, Poetry, Conda, or other package managers directly.
- Keep changes small, explicit, and covered by focused tests.
- Do not change package version or release metadata unless explicitly asked.
- Preserve existing command-emulator behavior unless the task requires a
  compatibility fix.
- Every behaviour claim about real Slurm (command output, exit codes, REST
  field shapes) must cite SchedMD source as `slurm://<path>#<symbol>[@versions]`
  in the code comment and in the covering test — never `file.c:<line>`, never a
  local path. Verify with `uv run scripts/check_slurm_refs.py`; refresh the
  cache with `uv run scripts/slurm_src.py update`. Tracked versions are in
  `[tool.slurm-parity]` (`pyproject.toml`); see `docs/slurm-parity.md`.

## Common Commands

```bash
uv sync
uv run pytest
uv run --with ruff ruff format .
uv run --with ruff ruff check emulator/ --fix
uv run --with mypy mypy emulator/
uv run scripts/slurm_src.py update          # refresh the real-Slurm source cache
uv run scripts/check_slurm_refs.py --summary  # verify slurm:// references
```

## Command Emulators

- `sacctmgr` lives in `emulator/commands/sacctmgr.py`.
- `sacct` lives in `emulator/commands/sacct.py`.
- Command dispatch and script entry points live in
  `emulator/commands/dispatcher.py`.
- The slurmrestd REST API emulation (Slurm 26.05, v0.0.45, port 6820)
  lives in `emulator/api/slurmrestd/`; it must stay consistent with the
  command emulators (shared `SlurmDatabase`, sacct job math, sinfo
  topology).

When adding command support, wire it through the dispatcher, add a
`[project.scripts]` entry in `pyproject.toml`, and include unit tests for
flag validation and command output.
