# SLURM Emulator - Time Travel Edition

A comprehensive SLURM command emulator with time manipulation capabilities for testing periodic limits and decay calculations.

## Features

- 🎮 **Interactive CLI** - Full command-line interface with time travel
- ⏰ **Time Manipulation** - Advance time by days, months, or quarters
- 💾 **Usage Injection** - Add specific node-hour usage at any time point
- 🔄 **Decay Calculations** - 15-day half-life fairshare decay simulation
- 🎯 **QoS Management** - Threshold-based QoS switching (normal → slowdown → blocked)
- 📊 **Periodic Limits** - Quarterly allocation with carryover logic
- 🎬 **Scenario Runner** - Complete SLURM_PERIODIC_LIMITS_SEQUENCE.md validation
- 🖥️ **Web Dashboard** - Browser console to view status and drive the emulator ([docs](docs/web-ui.md))
- 🔌 **API Integration** - REST API for waldur-site-agent integration
- 💾 **State Management** - Checkpoint/restore functionality for testing

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/waldur/slurm-emulator.git
cd slurm-emulator

# Install dependencies using uv
uv sync
```

### Interactive CLI (CMD-based)

```bash
# Run with default configuration
uv run slurm-emulator

# Run with SLURM configuration file
uv run slurm-emulator --config examples/slurm.conf

# Advanced features work the same way
uv run slurm-emulator --config examples/custom_slurm.conf

# Validate configuration only
uv run slurm-emulator --validate-only --config /etc/slurm/slurm.conf

🎮 SLURM Emulator - Time Travel Edition (CMD Interface)
Type 'help' or '?' for commands. TAB for auto-completion.
Type 'help <command>' for detailed help on specific commands.

slurm-emulator> help
# Shows all available commands

slurm-emulator> help time_advance
# Shows detailed help for specific command

slurm-emulator> time_advance 2 months
⏭️  Advanced 2 months

slurm-emulator> account_create test "Test Account" 1000
✅ Created account test with 1000Nh allocation

slurm-emulator> account create test-account "Test Account" 1000
✅ Created account test-account with 1000Nh allocation

slurm-emulator> usage inject user1 200 test-account
💾 Injected 200.0Nh usage for user1 in test-account at 2024-01-01 00:00:00

slurm-emulator> time advance 2 months
⏭️  Advanced 2 months
⏰ New time: 2024-03-01 00:00:00

slurm-emulator> usage inject user1 400 test-account
💾 Injected 400.0Nh usage for user1 in test-account at 2024-03-01 00:00:00

slurm-emulator> limits calculate test-account
📊 Periodic Limits for test-account:
   Period: 2024-Q1
   Base allocation: 1000Nh
   Total allocation: 1000.0Nh
   Fairshare: 333
   QoS threshold: 1200.0Nh
   Grace limit: 1200.0Nh
   Billing minutes: 60000
```

### Complete Sequence Scenario

Run the full scenario from SLURM_PERIODIC_LIMITS_SEQUENCE.md:

```bash
slurm-emulator> scenario run sequence --interactive

🎬 Starting SLURM Periodic Limits Sequence Scenario
============================================================

⏸️  Press Enter to execute Step 1: Initial Q1 setup...

📍 Step 1: Initial Q1 2024 Setup
   Setting up 1000Nh quarterly allocation with 20% grace period
   ⚖️  Set fairshare to 333
   🚫 Set GrpTRESMins to 72000 billing-minutes
   🎯 QoS threshold set to 1200.0Nh
   💾 Checkpoint 'initial_setup' created

# ... continues through all 9 steps of the sequence
```

### Direct SLURM Commands

The emulator intercepts and emulates real SLURM commands:

```bash
slurm-emulator> sacctmgr add account test-account description="Test"
 Adding Account(s)
  test-account
 Settings
  Parent     = root
  Description = Test

slurm-emulator> sacctmgr modify account test-account set fairshare=333
 Modified account...
  test-account
 Settings
  fairshare=333

slurm-emulator> sacctmgr modify account test-account set GrpTRESMins=billing=72000
 Modified account...
  test-account
 Settings
  GrpTRESMins=billing=72000

slurm-emulator> sacct --accounts=test-account --starttime=2024-01-01 --endtime=2024-12-31
JobID           JobName  Partition    Account  AllocCPUS      State ExitCode
------------ ---------- ---------- ---------- ---------- ---------- --------
1                 job_1    compute test-acco+         64  COMPLETED      0:0

slurm-emulator> sacct --accounts=test-account -S 2024-01-01 --format=Account,ReqTRES,Elapsed,User --noheader --parsable2
test-account|cpu=64,mem=512G,node=1,billing=64,gres/gpu=4|08:00:00|user1
```

## Web Dashboard

A lightweight, browser-based control console is mounted on the API server at
**`http://localhost:8080/ui/`**. It shows live status (time/period, accounts,
usage, QoS, jobs, cluster config) and provides full control — advance/set time,
create/edit accounts, inject usage, apply periodic settings, inline QoS editing,
add/remove account users, and a **scenario editor** to build/adjust and run
scenarios. It shares the same in-memory state as the CLI and JSON API.

```bash
# Start the server (Basic-auth credentials via env vars)
SLURM_EMULATOR_UI_USER=admin SLURM_EMULATOR_UI_PASSWORD=secret \
  uv run uvicorn emulator.api.emulator_server:app --host 0.0.0.0 --port 8080
# then open http://localhost:8080/ui/
```

All `/ui` routes are protected by HTTP Basic auth
(`SLURM_EMULATOR_UI_USER` / `SLURM_EMULATOR_UI_PASSWORD`, default `admin`/`admin`
with a startup warning). See **[docs/web-ui.md](docs/web-ui.md)** for a full
walkthrough with screenshots.

![SLURM Emulator dashboard](docs/screenshots/01-dashboard.png)

## API Integration

Start the API server for waldur-site-agent integration:

```bash
# From the slurm-emulator directory
uv run uvicorn emulator.api.emulator_server:app --host 0.0.0.0 --port 8080
```

### API Endpoints

- `POST /api/apply-periodic-settings` - Apply periodic limits settings
- `POST /api/downscale-resource` - Set QoS to slowdown
- `POST /api/restore-resource` - Restore QoS to normal
- `POST /api/submit-report` - Submit usage reports
- `GET /api/status` - Get emulator status
- `POST /api/time/advance` - Advance emulator time

### Example API Usage

```bash
# Apply periodic settings (from Waldur Mastermind)
curl -X POST http://localhost:8080/api/apply-periodic-settings \\
  -H "Content-Type: application/json" \\
  -d '{
    "resource_id": "slurm_account_123",
    "fairshare": 333,
    "grp_tres_mins": {"billing": 72000},
    "qos_threshold": {"billing": 1000}
  }'

# Submit usage report (from site agent)
curl -X POST http://localhost:8080/api/submit-report \\
  -H "Content-Type: application/json" \\
  -d '{
    "resource_id": "slurm_account_123",
    "usage": {"billing": 167},
    "billing_period": "2024-01-01",
    "date": "2024-01-31T23:59:59Z",
    "users": {
      "user1": {"billing": 100},
      "user2": {"billing": 67}
    }
  }'

# Advance time for testing
curl -X POST "http://localhost:8080/api/time/advance?months=3"
```

## SLURM REST API Emulation (slurmrestd)

The emulator also serves the Slurm 26.11 REST API (`slurmrestd`,
data parser `v0.0.46`) on port 6820, backed by the same state as the
CLI commands and the control API:

```bash
uv run slurmrestd-emulator
# or: uv run uvicorn emulator.api.slurmrestd.app:app --host 0.0.0.0 --port 6820
```

### Endpoint families

- `/slurmdb/v0.0.46/...` — accounting: `accounts`, `users`,
  `associations`, `qos`, `tres`, `clusters`, `jobs` (one job per usage
  record, matching `sacct` output), `ping`, `diag`, `config`. Write
  support (POST/DELETE) covers everything Waldur drives via `sacctmgr`.
- `/slurm/v0.0.46/...` — controller read paths: `jobs` (+ `DELETE
  /job/{job_id}` as the `scancel` equivalent), `nodes`, `partitions`
  (static topology matching `sinfo`), `shares`, `ping`, `diag`, `conf`;
  `reservations`/`licenses` are empty stubs.
- `/openapi.json`, `/openapi`, `/openapi/v3` — generated self-description.

Responses use the real envelope (`meta`/`errors`/`warnings`, payload
keys and field names from the v0.0.46 data parser). Unsupported URL
versions (e.g. `v0.0.45`), unknown paths, and auth failures reject
with slurmrestd's plain-text errors and exit statuses.

### Authentication

Requests need an `X-SLURM-USER-TOKEN` header (or `Authorization:
Bearer`); `X-SLURM-USER-NAME` optionally names the user. By default
any non-empty token is accepted. Set `SLURM_EMULATOR_JWT_KEY` to
enforce real HS256 verification. Mint tokens via the control API
(`scontrol token` stand-in):

```bash
curl -X POST http://localhost:8080/api/token \
  -H "Content-Type: application/json" -d '{"username": "alice"}'

curl http://localhost:6820/slurmdb/v0.0.46/accounts/ \
  -H "X-SLURM-USER-TOKEN: <token>"
```

### State sharing and concurrency

Both API servers and the CLI commands persist to the same JSON state
files (`/tmp/slurm_emulator_db.json`, `/tmp/slurm_emulator_time.json`;
override with `SLURM_EMULATOR_STATE_FILE` / `SLURM_EMULATOR_TIME_FILE`).
The slurmrestd app reloads state on every request and saves after
writes — file locking prevents torn writes, but concurrent writers are
last-writer-wins. Note the control API on 8080 loads state once at
startup, so it can serve stale reads after REST/CLI writes.

The Docker image runs both servers (ports 8080 and 6820) via
`scripts/docker-entrypoint.sh`.

## Waldur Site Agent Integration

Configure waldur-site-agent to use the emulator:

```yaml
# waldur-site-agent-config.yaml
offerings:
  - name: "SLURM HPC Cluster - Emulator"
    backend_type: "slurm"
    backend_settings:
      # Enable emulator mode
      emulator_mode: true
      emulator_base_url: "http://localhost:8080"

      # Override SLURM commands to use emulator
      command_prefix: ["python", "/path/to/slurm-emulator/emulator/commands/dispatcher.py"]

      # Periodic limits configuration
      periodic_limits:
        enabled: true
        limit_type: "GrpTRESMins"
        tres_billing_enabled: true
        tres_billing_weights:
          CPU: 0.015625
          Mem: 0.001953125G
          "GRES/gpu": 0.25
        fairshare_decay_half_life: 15
        api_endpoints:
          apply_periodic_settings: "http://localhost:8080/api/apply-periodic-settings"
          downscale_resource: "http://localhost:8080/api/downscale-resource"
          restore_resource: "http://localhost:8080/api/restore-resource"
```

## SLURM Configuration Support

The emulator now supports real SLURM configuration files to match actual deployment behavior:

### Loading Configuration

```bash
# Use system SLURM configuration
uv run slurm-emulator --config /etc/slurm/slurm.conf

# Use custom configuration
uv run slurm-emulator --config examples/slurm.conf

# Validate configuration
uv run slurm-emulator --validate-only --config slurm.conf
```

### Supported Configuration Parameters

The emulator parses and applies these SLURM configuration parameters:

**Priority and Decay Settings:**
- `PriorityDecayHalfLife` - Fairshare decay half-life (e.g., "15-00:00:00")
- `PriorityUsageResetPeriod` - Usage reset period ("None" for manual reset)
- `PriorityWeightFairShare` - Fairshare weight for priority calculations
- `PriorityWeightQOS` - QoS weight for priority calculations
- `FairShareDampeningFactor` - Dampening factor for fairshare

**TRES Billing:**
- `TRESBillingWeights` - Billing weights (e.g., "CPU=0.015625,Mem=0.001953125G,GRES/gpu=0.25")

**Priority Flags:**
- `PriorityFlags` - Priority calculation flags (e.g., "MAX_TRES,NO_NORMAL_ASSOC")

### Example Configuration

```bash
# SLURM Configuration
PriorityDecayHalfLife   = 15-00:00:00
PriorityUsageResetPeriod = None # manual reset via sacctmgr RawUsage=0
PriorityWeightFairShare = 259200
PriorityWeightQOS       = 500000
FairShareDampeningFactor = 3
TRESBillingWeights="CPU=0.015625,Mem=0.001953125G,GRES/gpu=0.25"
PriorityFlags=MAX_TRES,NO_NORMAL_ASSOC
```

## Understanding Decay Calculations

The emulator implements SLURM's fairshare decay using the configured half-life:

```python
# Decay formula matches SLURM's implementation
decay_factor = 2 ** (-days_elapsed / half_life_days)

# With default 15-day half-life, after 90 days (1 quarter):
decay_factor = 2 ** (-90 / 15) = 0.0156 (1.56%)

# With 7-day half-life, after 90 days:
decay_factor = 2 ** (-90 / 7) = 0.000135 (0.01%)
```

**Example with 15-day half-life**: User consumes 2000 hours in Q1. After Q1 ends (90 days later):
- Original impact: 2000 hours
- Decayed impact: 2000 × 0.0156 = 31 hours equivalent
- Q2 allocation: 1000 + (1000 - 31) = 1969 hours available

## Key Commands Reference

### Time Manipulation
```bash
time_show                             # Show current time and period
time_advance <amount> <unit>          # Advance time (units: days, months, quarters)
time_set YYYY-MM-DD [HH:MM:SS]       # Set specific date/time

# Examples:
time_advance 2 months
time_advance 30 days
time_set 2024-05-20
```

### Usage Simulation
```bash
usage_inject <user> <amount> [account]  # Inject node-hour usage
usage_show [account] [period]           # Show usage summary with user breakdown

# Examples:
usage_inject user1 200 test-account
usage_show test-account
usage_show test-account 2024-Q1
```

### Account Management
```bash
account_create <name> [description] [allocation]  # Create account
account_list                                      # List all accounts with status
account_show <name>                               # Show detailed account info
account_delete <name>                             # Delete account

# Examples:
account_create test "Test Account" 1000
account_show test
account_list
```

### QoS Management
```bash
qos_show [account]                    # Show QoS status and details
qos_set <account> <qos>              # Set QoS level (normal/slowdown/blocked)
qos_check [account]                  # Check thresholds and auto-update QoS

# Examples:
qos_check test-account
qos_set test-account slowdown
qos_show test-account
```

### Limits Calculation
```bash
limits_calculate [account]           # Calculate and display periodic limits

# Example:
limits_calculate test-account
```

### Scenario Management
```bash
scenario_list [type]                 # List scenarios (optionally filter by type)
scenario_describe <name>             # Show detailed description and learning objectives
scenario_steps <name>                # Show step-by-step command breakdown
scenario_run <name>                  # Run scenario automatically
scenario_run <name> --interactive    # Run with confirmation prompts
scenario_run <name> --step-by-step   # Run with detailed step output
scenario_search <query>              # Search scenarios by keyword

# Examples:
scenario_list qos_management
scenario_describe qos_thresholds
scenario_run qos_thresholds --step-by-step
scenario_search decay
```

### Configuration Management
```bash
config_show                          # Show current SLURM configuration
config_reload <path>                 # Hot-reload configuration file

# Examples:
config_show
config_reload examples/slurm.conf
```

### State Management
```bash
cleanup_all                          # Clean all accounts and reset to fresh state
cleanup_scenario <name>              # Clean specific scenario accounts
cleanup_account <name>               # Clean specific account completely

# Examples:
cleanup_all
cleanup_scenario qos_thresholds
cleanup_account test-account
```

### SLURM Commands
```bash
sacctmgr <args>                      # Run sacctmgr command
sacct <args>                         # Run sacct command
sinfo <args>                         # Run sinfo command

# Examples:
sacctmgr list accounts
sacctmgr modify account test set fairshare=333
sacct --accounts=test --format=Account,User,Elapsed --noheader --parsable2
```

## Testing Scenarios

### Basic Usage Pattern
```bash
# Setup with specific configuration
uv run slurm-emulator --config examples/slurm.conf

# In emulator CLI:
time set 2024-01-01
account create test-account "Test" 1000

# Month 1: Light usage
usage inject user1 100 test-account
time advance 1 months

# Month 2: Heavy usage
usage inject user1 600 test-account
limits calculate test-account
qos check test-account

# Quarter transition
time advance 1 months
limits apply test-account
```

### Configuration Testing
```bash
# Test different decay rates
uv run slurm-emulator --config examples/custom_slurm.conf

# Compare configurations
uv run slurm-emulator --validate-only --config examples/slurm.conf
uv run slurm-emulator --validate-only --config examples/custom_slurm.conf
```

### Decay Validation
```bash
# Q1: Heavy usage
time set 2024-01-01
account create test-account "Test" 1000
usage inject user1 1500 test-account

# Q2: Check decay impact
time set 2024-04-01
limits calculate test-account
# Should show ~23Nh effective previous usage (1500 * 0.0156)
```

### QoS Threshold Testing
```bash
# Setup with 1000Nh allocation (1200Nh threshold with 20% grace)
account create test-account "Test" 1000
qos show test-account  # Should show "normal"

usage inject user1 1100 test-account
qos check test-account  # Should show approaching threshold

usage inject user1 200 test-account  # Total: 1300Nh
qos check test-account  # Should trigger slowdown QoS
```

## Architecture

```
slurm-emulator/
├── emulator/
│   ├── core/
│   │   ├── time_engine.py          # Time manipulation
│   │   ├── database.py             # In-memory state
│   │   ├── slurm_config.py         # SLURM config parsing
│   │   └── usage_simulator.py      # Usage injection
│   ├── commands/
│   │   ├── sacctmgr.py             # sacctmgr emulator
│   │   ├── sacct.py                # sacct emulator
│   │   └── dispatcher.py           # Command routing
│   ├── periodic_limits/
│   │   ├── calculator.py           # Decay & carryover
│   │   └── qos_manager.py          # QoS management
│   ├── scenarios/
│   │   ├── sequence_scenario.py    # Complete scenario
│   │   ├── scenario_registry.py    # Scenario discovery & running
│   │   └── limits_configuration_scenarios.py
│   ├── cli/
│   │   ├── main.py                 # Interactive CLI
│   │   └── cmd_cli.py              # CMD-based CLI
│   └── api/
│       └── emulator_server.py      # REST API
├── scripts/
│   ├── release.py                  # Release management
│   ├── changelog.sh                # Changelog generation
│   ├── generate_changelog_data.py  # Commit data collection
│   └── prompts/
│       └── changelog-prompt.md     # Changelog prompt template
└── tests/                          # Test suites
```

## Development

### Running Tests

```bash
uv run pytest
```

### Releasing

```bash
# Full release: update version, generate changelog, tag, push
uv run scripts/release.py release X.Y.Z

# Skip changelog generation
uv run scripts/release.py release X.Y.Z --skip-changelog
```

Pushing the tag triggers GitHub Actions for testing and PyPI publishing.

### Adding New Scenarios

```python
# Create new scenario class
class CustomScenario:
    def __init__(self, time_engine, database):
        self.time_engine = time_engine
        self.database = database

    def run_scenario(self):
        # Implement scenario steps
        pass
```

### Extending Commands

```python
# Add new SLURM command support
class NewCommandEmulator:
    def handle_command(self, args):
        # Implement command logic
        return "command output"
```

## Troubleshooting

### State Persistence

Emulator state is saved to:
- `/tmp/slurm_emulator_time.json` - Current time
- `/tmp/slurm_emulator_db.json` - Database state

### Common Issues

**"Account not found"**: Create account first with `account create`
**"No usage records"**: Inject usage with `usage inject`
**"Time not advancing"**: Check time with `time` command
**"API connection failed"**: Ensure server is running on port 8080

### Reset Emulator

```bash
rm /tmp/slurm_emulator_*.json
slurm-emulator
# Start fresh
```

## License

MIT License - See LICENSE file for details.
