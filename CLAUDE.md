# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SLURM Emulator is a comprehensive time-travel enabled emulator for testing SLURM periodic limits
functionality. It provides time manipulation, usage injection, decay calculations, and complete
scenario validation for the Waldur periodic limits implementation.

## Philosophy

### Core Beliefs

- **Incremental progress over big bangs** - Small changes that compile and pass tests
- **Learning from existing code** - Study and plan before implementing
- **Pragmatic over dogmatic** - Adapt to project reality
- **Clear intent over clever code** - Be boring and obvious

### Simplicity Means

- Single responsibility per function/class
- Avoid premature abstractions
- No clever tricks - choose the boring solution
- If you need to explain it, it's too complex

## Python Package Management with uv

Use uv exclusively for Python package management in this project.

### Package Management Commands

- All Python dependencies **must be installed, synchronized, and locked** using uv
- Never use pip, pip-tools, poetry, or conda directly for dependency management

Use these commands:

- Install dependencies: `uv add <package>`
- Remove dependencies: `uv remove <package>`
- Sync dependencies: `uv sync`

### Running Python Code

- Run a Python script with `uv run <script-name>.py`
- Run Python tools like Pytest with `uv run pytest` or `uv run ruff`
- Launch a Python repl with `uv run python`

### Managing Scripts with PEP 723 Inline Metadata

- Run a Python script with inline metadata (dependencies defined at the top of the file) with: `uv run script.py`
- You can add or remove dependencies manually from the `dependencies =` section at the top of the script, or
- Or using uv CLI:
    - `uv add package-name --script script.py`
    - `uv remove package-name --script script.py`

## Development Commands

### Installation and Setup

- **Install project**: `uv sync`
- **Install in development mode**: `uv sync --dev`
- **Add new dependency**: `uv add <package-name>`
- **Add development dependency**: `uv add --dev <package-name>`

### Running the Emulator

- **Interactive CLI**: `uv run slurm-emulator`
- **API Server**: `uv run uvicorn emulator.api.emulator_server:app --host 0.0.0.0 --port 8080`
- **slurmrestd API**: `uv run slurmrestd-emulator` (Slurm 26.05 REST API v0.0.45 on port 6820)
- **SSH filesystem plane**: `uv run --extra ssh slurm-ssh-emulator` (asyncssh server on port 2222;
  filesystem ops + Slurm CLI dispatch; for running FireCREST v2 against the emulator)
- **Direct commands**: `uv run sacctmgr`, `uv run sacct`, `uv run sinfo`

### Helm Chart

The chart in `charts/slurm-emulator/` deploys the Docker image (control API +
slurmrestd, optional SSH plane) to Kubernetes. Everything is configured through
environment variables — the chart sets no `command`/`args`, so the image's
`scripts/docker-entrypoint.sh` stays in charge of which servers start.

- **Lint**: `helm lint charts/slurm-emulator/`
- **Unit tests**: `helm unittest charts/slurm-emulator/` (needs the
  [helm-unittest](https://github.com/helm-unittest/helm-unittest) plugin)
- **Render a variant**: `helm template se charts/slurm-emulator/ --set persistence.enabled=true`

`templates/` is excluded from the `check-yaml` pre-commit hook — Go templates are
not parseable YAML. CI runs `helm lint` + `helm unittest` plus a `helm template`
render of every optional-feature combination instead.

Adding a new value means touching four places: `values.yaml` (with a comment
explaining *why*, not just what), the template that consumes it, a case in
`tests/`, and the table in `charts/slurm-emulator/README.md`.

Releases: pushing a `X.Y.Z` tag makes CI rewrite the chart's `version` and
`appVersion` to the tag, package it, and push it to the `gh-pages` branch of the
GitHub mirror. The chart therefore always deploys `opennode/slurm-emulator:<tag>`,
which the "Publish docker image" job pushes from the same pipeline.

### Tracing changes against real Slurm

The emulator's behaviour is a set of claims about real Slurm, and every claim
is anchored to SchedMD's source. Full guide: `docs/slurm-parity.md`.

- **Source cache**: `uv run scripts/slurm_src.py update` clones
  https://github.com/SchedMD/slurm once into `~/.cache/slurm-emulator/` (override
  with `SLURM_SRC_CACHE`) and keeps one worktree per tracked version;
  `status` shows freshness, `path 26.05` prints a worktree, `grep PATTERN [paths]`
  searches every version at once. Run `update` before parity work.
- **Tracked versions** live in `[tool.slurm-parity]` in `pyproject.toml`:
  `primary = "26.05"` (what the emulator claims by default) and
  `versions = ["master", "26.05", "25.11", "25.05", "24.11"]` (SchedMD's support
  window plus `master` as an early warning for upstream changes). The slurmrestd
  plane emulates `data_parser/v0.0.45` (26.05); its references carry `@26.05+`.
- **Reference syntax** — required in the comment/docstring next to any behaviour
  in `emulator/commands/`, `emulator/api/slurmrestd/`, `emulator/core/database.py`
  and in the test that covers it:
  `slurm://<path>[#<symbol>][@<versions>]`, e.g.
  `slurm://src/sacctmgr/common.c#_get_print_field`,
  `slurm://slurm/slurmdb.h#slurmdb_qos_rec_t.grace_time`,
  `slurm://src/sacctmgr/common.c#"Def QOS"`,
  `slurm://src/plugins/data_parser/v0.0.45/parsers.c#ASSOC_SHORT@26.05+`,
  `slurm://src/slurmrestd/operations.c#http_status_from_error@25.11+`.
  Anchor on the function/table, not a line. No `@` means "same in every
  tracked version". **Never** write `file.c:<line>` or a local checkout path —
  `scripts/check_slurm_refs.py` rejects both.
- **Checking**: `uv run scripts/check_slurm_refs.py --summary` verifies each
  reference's file and symbol in every version it claims. Runs in pre-commit
  (skips uncached versions) and in CI with `--strict`.
- **Version differences**: if the checker says a symbol is missing in an older
  version, narrow the reference (`@25.11+`); if the emulator must behave
  differently, branch on `emulator.slurm_version.at_least()` (target selected by
  `SLURM_EMULATOR_SLURM_VERSION`, default = primary) and either mark the test
  `@pytest.mark.slurm_version("25.11+")` / `("24.11", "25.05")` or, for REST
  shapes, add a case to `tests/test_slurmrestd_dialects.py` (which runs every
  version). REST tests must never hard-code `v0.0.4X`: use
  `current().api_version` (`V` in the existing test modules). CI runs the whole
  suite once per tracked version.
- **Intentional deviations** from real Slurm go in the module docstring (existing
  examples: `sacctmgr -M` tolerance, no interactive commit prompt, `sacct -X`/`-a`
  no-ops) — with the reference to what real Slurm does instead.
- Changelog entries for parity changes cite the same reference.

### Testing and Quality

- **Run tests**: `uv run pytest`
- **Run tests with coverage**: `uv run pytest --cov=emulator/`
- **Format code**: `uv run --with ruff ruff format .`
- **Lint code**: `uv run --with ruff ruff check emulator/ --fix`
- **Type check**: `uv run --with mypy mypy emulator/`
- **Run all pre-commit checks**: `uv run pre-commit run --all-files`
- **Verify Slurm source references**: `uv run scripts/check_slurm_refs.py --summary`

### Release Management

**Local Development Commands:**
- **Check current version**: `uv run scripts/release.py status`
- **Update version only**: `uv run scripts/release.py version-update X.Y.Z`
- **Run local checks**: `uv run scripts/release.py check` (linting, type checking)
- **Test local build**: `uv run scripts/release.py build`
- **Full release workflow**: `uv run scripts/release.py release X.Y.Z`
- **Skip changelog**: `uv run scripts/release.py release X.Y.Z --skip-changelog`

**Release Flow** (`release X.Y.Z`):
1. Check git status
2. Update version in `pyproject.toml`
3. Generate changelog entry via `scripts/changelog.sh` — skip with `--skip-changelog`
4. Create git tag, commit (`pyproject.toml` + `CHANGELOG.md`), optionally push — skip with `--skip-tag`
5. GitLab CI/CD handles testing, building, and PyPI publishing

**Automated CI/CD (GitLab CI/CD):**
- **Testing**: Runs automatically on every push/MR (Python 3.9–3.13)
- **Publishing**: Triggered by pushing version tags matching `X.Y.Z` pattern
- **PyPI Release**: Automatically builds with `uv build` and publishes with `uv publish` when version tag is pushed
- **Docker Image**: Publishes `opennode/slurm-emulator:latest` to Docker Hub on every commit to `main`,
  and `opennode/slurm-emulator:X.Y.Z` on a version tag
- **Helm Chart**: Publishes `charts/slurm-emulator/` to https://waldur.github.io/slurm-emulator/ on a version tag

The release script handles local version management and creates git tags that trigger automated CI/CD for testing and publishing.

**Version Management:**
- **Single Source**: Version defined in `pyproject.toml` only
- **Automatic Propagation**: All code imports version from `emulator.__init__.py`
- **No Hardcoding**: All version references are automatically updated

**Changelog Generation:**
- **Changelog file**: `CHANGELOG.md` follows [Keep a Changelog](https://keepachangelog.com/) format
- **Collect commit data**: `python3 scripts/generate_changelog_data.py <new_ref> <old_ref>` — outputs categorized JSON
- **Generate entry**: `bash scripts/changelog.sh <version>` — uses `claude --print` with prompt from `scripts/prompts/changelog-prompt.md`, interactive accept/edit/regenerate/quit flow
- **Integrated in release**: Runs automatically between version update and build (skip with `--skip-changelog`)

#### Linting Configuration

The project uses Ruff with a balanced configuration that:
- ✅ **Enforces important code quality**: unused variables, proper imports, exception handling
- ✅ **Maintains style consistency**: magic numbers, function complexity, best practices
- ⚠️ **Allows development pragmatism**: print statements in CLI tool, missing docstrings, type annotations
- 🔧 **Auto-fixes where possible**: formatting, simple style issues

This approach ensures code quality while keeping development velocity for an emulator tool.

### Scenario Execution

- **Complete sequence scenario**: `uv run python -c "from emulator.scenarios.sequence_scenario import SequenceScenario; from emulator.core.time_engine import TimeEngine; from emulator.core.database import SlurmDatabase; s = SequenceScenario(TimeEngine(), SlurmDatabase()); s.run_complete_scenario()"`

## Architecture Overview

### Core Components

1. **Time Engine** (`emulator/core/time_engine.py`)
   - Time manipulation and advancement
   - Period transition detection
   - Quarter calculations with decay

2. **Database** (`emulator/core/database.py`)
   - In-memory state management
   - Account, user, and usage tracking
   - State persistence to JSON files

3. **Usage Simulator** (`emulator/core/usage_simulator.py`)
   - Node-hour usage injection
   - Pattern-based usage generation
   - Billing unit conversion

4. **Periodic Limits Calculator** (`emulator/periodic_limits/calculator.py`)
   - Decay factor calculations (15-day half-life)
   - Carryover logic for period transitions
   - Fairshare and billing minute calculations

5. **QoS Manager** (`emulator/periodic_limits/qos_manager.py`)
   - Threshold-based QoS switching
   - Normal → slowdown → blocked transitions
   - Usage monitoring and recommendations

### Command Emulators

- **sacctmgr** (`emulator/commands/sacctmgr.py`) - Account management
- **sacct** (`emulator/commands/sacct.py`) - Usage reporting
- **sinfo** - Cluster information
- **scancel** - Job cancellation

### Integration Points

- **CLI Interface** (`emulator/cli/main.py`) - Interactive time travel interface
- **API Server** (`emulator/api/emulator_server.py`) - REST API for waldur-site-agent
- **Web UI** (`emulator/api/ui/`) - Lightweight HTMX + Jinja2 dashboard mounted on the API
  server at `http://localhost:8080/ui/`. Shows status (time/period, accounts, usage, QoS,
  jobs) and provides full control (advance/set time, inject usage, create accounts, apply
  periodic settings, downscale/restore QoS, run the sequence scenario). Shares the same
  in-memory managers and JSON state as the CLI/API. Protected by HTTP Basic auth via
  `SLURM_EMULATOR_UI_USER` / `SLURM_EMULATOR_UI_PASSWORD` (default `admin`/`admin` with a
  startup warning; put behind TLS if exposed beyond localhost). Screenshots and a
  feature walkthrough live in `docs/web-ui.md`. Also includes inline QoS editing,
  per-account association add/remove, and a scenario editor (build/adjust steps)
- **slurmrestd Emulation** (`emulator/api/slurmrestd/`) - Slurm 26.05 REST API (v0.0.45) on
  port 6820: `/slurmdb` CRUD + `/slurm` controller read paths, real response envelopes,
  JWT-style auth (`X-SLURM-USER-TOKEN`, optional `SLURM_EMULATOR_JWT_KEY` verification).
  Shares state with the CLI commands via the JSON state files
  (`SLURM_EMULATOR_STATE_FILE` / `SLURM_EMULATOR_TIME_FILE` overrides)
- **Scenario Runner** (`emulator/scenarios/sequence_scenario.py`) - Complete test scenarios

## Key Features

> Note: native emulator commands use underscores (`time_advance`, `usage_inject`,
> ...). The SLURM passthrough commands (`sacctmgr`, `sacct`, `sinfo`, `sshare`)
> take space-separated arguments, as on a real cluster.

### Time Manipulation
```bash
time_advance 2 months        # Jump forward 2 months
time_set 2024-05-20          # Jump to specific date
```

### Usage Injection
```bash
usage_inject user1 200 account   # Add 200 node-hours
usage_show account               # Show current usage
```

### Decay Calculations
- **Formula**: `decay_factor = 2^(-days_elapsed/15)`
- **Example**: After 90 days, 2000Nh usage → 31Nh effective impact
- **Purpose**: Prevents past usage from punishing users forever

### Scenario Validation
- Complete SLURM_PERIODIC_LIMITS_SEQUENCE.md implementation
- Step-by-step interactive execution
- Checkpoint/restore for testing different paths

## Important Reminders

**ALWAYS**:
- Use uv for all Python package management
- Test time manipulation before complex scenarios
- Validate decay calculations with known values
- Cite real Slurm source (`slurm://<path>#<symbol>`) for every command/REST behaviour change and run `scripts/check_slurm_refs.py`
- Use checkpoints for complex testing scenarios
- Check QoS transitions after usage injection

**NEVER**:
- Use pip, poetry, or other package managers
- Assume time advances automatically
- Skip decay factor validation
- Reference Slurm source by line number (`file.c:<line>`) or by a local checkout path
- Ignore QoS threshold calculations
- Commit without testing sequence scenario

## Testing Strategy

### Unit Testing
```bash
uv run pytest tests/test_time_engine.py
uv run pytest tests/test_usage_simulator.py
uv run pytest tests/test_periodic_limits.py
```

### Integration Testing
```bash
uv run pytest tests/test_sequence_scenario.py
uv run pytest tests/test_api_integration.py
```

### Manual Validation
```bash
# Test basic functionality
uv run slurm-emulator
> time_set 2024-01-01
> account_create test "Test Account" 1000
> usage_inject user1 500 test
> time_advance 3 months
> limits_calculate test

# Test complete scenario
> scenario_run sequence --interactive
```

## Troubleshooting

### Common Issues

1. **Time not advancing properly**
   - Check `time` command output
   - Verify period transitions with `time_advance`

2. **Decay calculations incorrect**
   - Validate with: `2^(-90/15) ≈ 0.0156` for quarterly transitions
   - Check carryover logic in limits calculator

3. **QoS not switching**
   - Verify threshold calculations
   - Check usage vs qos_threshold values

4. **State not persisting**
   - Check `/tmp/slurm_emulator_*.json` files
   - Ensure database.save_state() is called

### Debug Commands
```bash
# Check emulator state
uv run python -c "
from emulator.core.database import SlurmDatabase
db = SlurmDatabase()
db.load_state()
print('Accounts:', list(db.accounts.keys()))
print('Usage records:', len(db.usage_records))
"

# Validate time engine
uv run python -c "
from emulator.core.time_engine import TimeEngine
te = TimeEngine()
print('Current time:', te.get_current_time())
print('Current quarter:', te.get_current_quarter())
"
```

## Integration with Waldur

### API Endpoints
- `POST /api/apply-periodic-settings` - From Waldur Mastermind
- `POST /api/submit-report` - From site agent
- `POST /api/downscale-resource` - QoS management
- `GET /api/status` - System status
- `POST /api/token` - Mint a JWT for the slurmrestd API (scontrol token stand-in)
- `POST /api/time/set` - Jump emulator time to a specific date (ISO 8601)
- `POST /api/accounts` - Create an account (sacctmgr add account stand-in)

### slurmrestd Endpoints (port 6820)
- `/slurmdb/v0.0.45/...` - accounts, users, associations, qos, tres, clusters, jobs
  (one job per usage record, matching `sacct` output)
- `/slurm/v0.0.45/...` - jobs (+ `POST /job/submit` = sbatch, `DELETE /job/{id}` = scancel),
  nodes, partitions, shares, ping, diag
- Auth header required: `X-SLURM-USER-TOKEN` (any token accepted unless
  `SLURM_EMULATOR_JWT_KEY` is set)

### Job lifecycle (submitted jobs)
Jobs created via `POST /slurm/v0.0.45/job/submit` (or `sbatch` over SSH) advance
PENDING → RUNNING → COMPLETED lazily on read, and emit a usage record on completion
so they also appear in the accounting (`/slurmdb` / `sacct`) view. Configurable via env:
- `SLURM_EMULATOR_SLURM_VERSION` = which tracked Slurm release the emulator is launched as
  (`24.11`, `25.05`, `25.11`, `26.05` default, `master`): selects the slurmrestd URL prefix
  (`/slurm/v0.0.42/` … `/slurm/v0.0.46/`), `meta.slurm.release` and version-gated response
  shapes; see `emulator/slurm_version.py` `RELEASES` and `docs/slurm-parity.md`
- `SLURM_EMULATOR_JOB_CLOCK` = `wall` (default, real-time) or `time` (simulated clock)
- `SLURM_EMULATOR_JOB_RUN_DELAY` (default 2s to RUNNING), `SLURM_EMULATOR_JOB_RUN_DURATION`
  (default 8s to COMPLETED)

### Running FireCREST v2 against the emulator
The emulator can stand in for a real cluster for eth-cscs/firecrest-v2 (scheduler
plane over slurmrestd + a thin SSH filesystem plane). See
`examples/firecrest/conformance.md` for the coverage matrix, `examples/firecrest/e2e/` for
a docker-compose overlay, and these harnesses:
- `uv run --extra dev pytest tests/test_firecrest_contract.py` — envelope/field contract
- `FIRECREST_SRC=/path/to/firecrest-v2 uv run --extra dev pytest tests/firecrest/` — drives
  FireCREST's own `SlurmRestClient` against the emulator (skipped without `FIRECREST_SRC`)
- `bash examples/firecrest/e2e/run.sh` — full-stack docker-compose smoke

### Configuration
```yaml
# In waldur-site-agent config
emulator_mode: true
emulator_base_url: "http://localhost:8080"
command_prefix: ["uv", "run", "python", "emulator/commands/dispatcher.py"]
```

This emulator enables comprehensive testing of the periodic limits implementation without requiring a full SLURM cluster deployment.
