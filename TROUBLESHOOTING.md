# SLURM Emulator Troubleshooting Guide

## Auto-Completion Issues

### Problem: TAB completion not working

**Symptoms:**
- Pressing TAB doesn't complete commands
- Backspace doesn't work properly
- Terminal feels "broken"

**Causes & Solutions:**

#### 1. Terminal Compatibility
Some terminals don't support readline properly:

```bash
# Test if readline is available
uv run python -c "import readline; print('✅ readline available')"

# Check if terminal is interactive
uv run python -c "import sys; print('Interactive:', sys.stdin.isatty())"
```

**Solution**: Diagnose completion with the built-in test commands, and use
`help` to discover commands and arguments:

```bash
[default] slurm-emulator> test_completion
# Exercises the complete_* methods and reports which are wired up

[default] slurm-emulator> debug_tab
# Shows TAB / readline binding state

[default] slurm-emulator> help
# Lists all commands (native commands use underscores: time_advance, ...)

[default] slurm-emulator> help time_advance
# Detailed help, including accepted arguments, for a specific command
```

#### 2. Environment Variables
Enable debug mode to see what's happening:

```bash
export SLURM_EMULATOR_DEBUG=1
uv run slurm-emulator --config examples/slurm.conf
```

#### 3. Input Method Issues
If you're using input methods (IME) or special keyboard layouts:

```bash
# Try with basic terminal
export TERM=xterm
uv run slurm-emulator
```

#### 4. Python readline Module Issues
Some Python installations have broken readline:

```bash
# Test readline functionality
uv run python -c "
import readline
readline.parse_and_bind('tab: complete')
print('✅ readline basic functionality works')
"
```

### Problem: Commands not executing properly

#### 1. Configuration File Issues
```bash
# Validate configuration first
uv run slurm-emulator --validate-only --config /etc/slurm/slurm.conf

# Check file permissions
ls -la /etc/slurm/slurm.conf
```

#### 2. State File Permissions
```bash
# Check state files
ls -la /tmp/slurm_emulator_*.json

# Remove if corrupted
rm -f /tmp/slurm_emulator_*.json
```

## Common Issues and Solutions

### 1. "Configuration validation failed"

**Error**: `PriorityDecayHalfLife` parsing error

**Solution**: Check time format in slurm.conf:
```bash
# Correct formats:
PriorityDecayHalfLife = 15-00:00:00    # 15 days
PriorityDecayHalfLife = 7-00:00:00     # 7 days
PriorityDecayHalfLife = 00:05:00       # 5 minutes

# Invalid formats:
PriorityDecayHalfLife = 15 days        # Wrong format
PriorityDecayHalfLife = 15d            # Wrong format
```

### 2. "Account not found" errors

**Solution**: Create accounts first:
```bash
[default] slurm-emulator> account_create test-account "Test Account" 1000
[default] slurm-emulator> usage_inject user1 100 test-account  # Now works
```

### 3. Scenario execution failures

**Debug steps:**
```bash
[default] slurm-emulator> scenario_describe qos_thresholds   # Inspect scenario definition
[default] slurm-emulator> scenario_steps qos_thresholds      # See what it will do
[default] slurm-emulator> scenario_run qos_thresholds --step-by-step  # Run with debug
```

### 4. Time manipulation issues

**Problem**: Time not advancing properly

**Solution**: Check current time:
```bash
[default] slurm-emulator> time_show                 # Show current time
[default] slurm-emulator> time_set 2024-01-01       # Reset to known date
[default] slurm-emulator> time_advance 1 months     # Test advancement
```

### 5. Usage injection problems

**Problem**: Usage not appearing in reports

**Solution**: Check period alignment:
```bash
[default] slurm-emulator> time_show                 # Check current period
[default] slurm-emulator> usage_show account        # Check current period usage
[default] slurm-emulator> usage_show account 2024-Q1  # Check specific period
```

### 6. QoS not switching

**Problem**: QoS stays "normal" despite high usage

**Solution**: Check threshold calculations:
```bash
[default] slurm-emulator> limits_calculate account  # See current thresholds
[default] slurm-emulator> qos_check account         # Check threshold status
[default] slurm-emulator> usage_show account        # Verify usage amounts
```

## Alternative Usage Methods

### 1. Command-Line Mode (No Interactive Shell)

If interactive mode has issues, use command-line mode:

```bash
# Run single commands
uv run python -c "
from emulator.commands.dispatcher import get_emulator
emulator = get_emulator()
print(emulator.execute_command('sacctmgr', ['list', 'accounts']))
"

# Run scenarios programmatically
uv run python -c "
from emulator.scenarios.sequence_scenario import SequenceScenario
from emulator.core.time_engine import TimeEngine
from emulator.core.database import SlurmDatabase

time_engine = TimeEngine()
database = SlurmDatabase()
scenario = SequenceScenario(time_engine, database)
result = scenario.run_complete_scenario()
print('Scenario result:', result['status'])
"
```

### 2. API Mode

Use the REST API instead of CLI:

```bash
# Start API server
uv run uvicorn emulator.api.emulator_server:app --host 0.0.0.0 --port 8080 &

# Use curl for testing
curl http://localhost:8080/api/status

# Apply settings
curl -X POST http://localhost:8080/api/apply-periodic-settings \\
  -H "Content-Type: application/json" \\
  -d '{"resource_id": "test", "fairshare": 333}'
```

### 3. Direct Python Usage

Import and use emulator components directly:

```python
from emulator.core.time_engine import TimeEngine
from emulator.core.database import SlurmDatabase
from emulator.core.usage_simulator import UsageSimulator

# Initialize
time_engine = TimeEngine()
database = SlurmDatabase()
usage_sim = UsageSimulator(time_engine, database)

# Create account and inject usage
database.add_account('test', 'Test Account', 'org')
usage_sim.inject_usage('test', 'user1', 200)

# Advance time and check
time_engine.advance_time(months=2)
usage_sim.inject_usage('test', 'user1', 400)

print(f"Total usage: {database.get_total_usage('test')}")
```

## Getting Help

### Debug Information

Enable debug mode:
```bash
export SLURM_EMULATOR_DEBUG=1
uv run slurm-emulator
```

### Checking System Requirements

```bash
# Check Python version
python --version

# Check readline availability
uv run python -c "import readline; print('readline OK')"

# Check terminal type
echo $TERM

# Check if running in proper terminal
uv run python -c "import sys; print('TTY:', sys.stdin.isatty())"
```

### Reset Everything

If things get corrupted:
```bash
# Remove state files
rm -f /tmp/slurm_emulator_*.json

# Remove history
rm -f ~/.slurm_emulator_history

# Start fresh
uv run slurm-emulator
```

### Working Auto-Completion Test

To verify auto-completion is working:

```bash
uv run slurm-emulator

# Should show "⌨️  Completion configured (use in interactive terminal)"
# If not, you'll see "⚠️  Auto-completion not available"

# Exercise the completion methods directly:
[default] slurm-emulator> test_completion
# Reports which complete_* methods are wired up
```

## Platform-Specific Issues

### macOS
- Some terminals (like Terminal.app) have better readline support than others
- iTerm2 usually works better
- Try: `export TERM=xterm-256color`

### Linux
- Most terminals support readline well
- Check your shell: `echo $SHELL`
- Try: bash, zsh, or fish

### Windows/WSL
- Windows Terminal usually works
- PowerShell may have issues
- WSL with bash recommended

## Recovery Commands

If the CLI becomes unresponsive:

1. **Ctrl+C** - Cancel current operation
2. **Ctrl+D** - Exit gracefully
3. **exit** - Normal exit
4. **quit** - Alternative exit

If these don't work, use **Ctrl+Z** to suspend and `kill %1` to terminate.
