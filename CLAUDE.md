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
- **Direct commands**: `uv run sacctmgr`, `uv run sacct`, `uv run sinfo`

### Testing and Quality

- **Run tests**: `uv run pytest`
- **Run tests with coverage**: `uv run pytest --cov=emulator/`
- **Format code**: `uv run --with ruff ruff format .`
- **Lint code**: `uv run --with ruff ruff check emulator/ --fix`
- **Type check**: `uv run --with mypy mypy emulator/`
- **Run all pre-commit checks**: `uv run pre-commit run --all-files`

### Release Management

**Local Development Commands:**
- **Check current version**: `uv run scripts/release.py status`
- **Update version only**: `uv run scripts/release.py version-update X.Y.Z`
- **Run local checks**: `uv run scripts/release.py check` (linting, type checking)
- **Test local build**: `uv run scripts/release.py build`
- **Full release workflow**: `uv run scripts/release.py release X.Y.Z`

**Automated CI/CD (GitHub Actions):**
- **Testing**: Runs automatically on every push/PR (multiple Python versions)
- **Publishing**: Triggered by pushing version tags (e.g., `0.1.1`)
- **PyPI Release**: Automatically builds and publishes when version tag is pushed

The release script handles local version management and creates git tags that trigger automated CI/CD for testing and publishing.

**Version Management:**
- **Single Source**: Version defined in `pyproject.toml` only
- **Automatic Propagation**: All code imports version from `emulator.__init__.py`
- **No Hardcoding**: All version references are automatically updated

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
- **Scenario Runner** (`emulator/scenarios/sequence_scenario.py`) - Complete test scenarios

## Key Features

### Time Manipulation
```bash
time advance 2 months        # Jump forward 2 months
time set 2024-05-20         # Jump to specific date
```

### Usage Injection
```bash
usage inject user1 200 account   # Add 200 node-hours
usage show account              # Show current usage
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
- Use checkpoints for complex testing scenarios
- Check QoS transitions after usage injection

**NEVER**:
- Use pip, poetry, or other package managers
- Assume time advances automatically
- Skip decay factor validation
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
> time set 2024-01-01
> account create test "Test Account" 1000
> usage inject user1 500 test
> time advance 3 months
> limits calculate test

# Test complete scenario
> scenario run sequence --interactive
```

## Troubleshooting

### Common Issues

1. **Time not advancing properly**
   - Check `time` command output
   - Verify period transitions with `time advance`

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

### Configuration
```yaml
# In waldur-site-agent config
emulator_mode: true
emulator_base_url: "http://localhost:8080"
command_prefix: ["uv", "run", "python", "emulator/commands/dispatcher.py"]
```

This emulator enables comprehensive testing of the periodic limits implementation without requiring a full SLURM cluster deployment.
