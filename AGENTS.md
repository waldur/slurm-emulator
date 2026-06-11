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

## Common Commands

```bash
uv sync
uv run pytest
uv run --with ruff ruff format .
uv run --with ruff ruff check emulator/ --fix
uv run --with mypy mypy emulator/
```

## Command Emulators

- `sacctmgr` lives in `emulator/commands/sacctmgr.py`.
- `sacct` lives in `emulator/commands/sacct.py`.
- Command dispatch and script entry points live in
  `emulator/commands/dispatcher.py`.
- The slurmrestd REST API emulation (Slurm 26.11, v0.0.46, port 6820)
  lives in `emulator/api/slurmrestd/`; it must stay consistent with the
  command emulators (shared `SlurmDatabase`, sacct job math, sinfo
  topology).

When adding command support, wire it through the dispatcher, add a
`[project.scripts]` entry in `pyproject.toml`, and include unit tests for
flag validation and command output.
