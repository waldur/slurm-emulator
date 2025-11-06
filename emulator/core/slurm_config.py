"""SLURM configuration parser and behavior adapter."""

import re
from pathlib import Path
from typing import Any, Callable, Optional


class SlurmConfigParser:
    """Parse and interpret SLURM configuration files."""

    def __init__(self, config_path: Optional[str] = None):
        self.config: dict[str, Any] = {}
        self.raw_config: dict[str, str] = {}

        if config_path:
            self.load_config(config_path)
        else:
            # Set reasonable defaults
            self._set_defaults()

    def load_config(self, config_path: str) -> None:
        """Load configuration from slurm.conf file."""
        config_file = Path(config_path)

        if not config_file.exists():
            raise FileNotFoundError(f"SLURM config file not found: {config_path}")

        print(f"📄 Loading SLURM configuration from {config_path}")

        with config_file.open() as f:
            content = f.read()

        self._parse_config_content(content)
        self._process_config_values()

        print(f"✅ Loaded {len(self.config)} configuration parameters")

    def _parse_config_content(self, content: str) -> None:
        """Parse the raw configuration content."""
        for line_num, line in enumerate(content.split("\n"), 1):
            line = line.strip()

            # Skip comments and empty lines
            if not line or line.startswith("#"):
                continue

            # Handle inline comments
            if "#" in line:
                line = line.split("#")[0].strip()

            # Parse key=value pairs
            if "=" in line:
                try:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()

                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]

                    self.raw_config[key] = value

                except ValueError:
                    print(f"Warning: Could not parse line {line_num}: {line}")

    def _process_config_values(self) -> None:
        """Process and validate configuration values."""
        processors: dict[str, Callable[[str], Any]] = {
            "PriorityDecayHalfLife": self._parse_time_duration,
            "PriorityCalcPeriod": self._parse_time_duration,
            "PriorityMaxAge": self._parse_time_duration,
            "PriorityUsageResetPeriod": self._parse_usage_reset_period,
            "PriorityWeightAge": int,
            "PriorityWeightAssoc": int,
            "PriorityWeightFairShare": int,
            "PriorityWeightJobSize": int,
            "PriorityWeightPartition": int,
            "PriorityWeightQOS": int,
            "FairShareDampeningFactor": int,
            "TRESBillingWeights": self._parse_tres_billing_weights,
            "PriorityFavorSmall": self._parse_boolean,
            "PriorityFlags": self._parse_priority_flags,
            "PriorityType": str,
            "SchedulerType": str,
        }

        for key, raw_value in self.raw_config.items():
            if key in processors:
                try:
                    processed_value = processors[key](raw_value)
                    self.config[key] = processed_value
                except (ValueError, TypeError) as e:
                    print(f"Warning: Could not process {key}={raw_value}: {e}")
                    self.config[key] = raw_value
            else:
                self.config[key] = raw_value

    def _parse_time_duration(self, value: str) -> int:
        """Parse SLURM time duration to minutes (matches SLURM's time_str2mins)."""
        # Handle special values
        if value.lower() in ["none", "infinite", "infinity", "unlimited"]:
            return -1

        return self._time_str2mins(value)

    def _time_str2mins(self, string: str) -> int:
        """Exact implementation of SLURM's time_str2mins function."""
        seconds = self._time_str2secs(string)
        if seconds not in {-1, -2}:  # Not INFINITE and not NO_VAL
            # Round up to next minute
            seconds = ((seconds + 59) // 60) * 60
        return seconds // 60 if seconds > 0 else seconds

    def _time_str2secs(self, string: str) -> int:
        """Exact implementation of SLURM's time_str2secs function."""
        if not string or string == "":
            return -2  # NO_VAL

        if string.upper() in ["-1", "INFINITE", "UNLIMITED"]:
            return -1  # INFINITE

        if not self._is_valid_timespec(string):
            return -2  # NO_VAL

        d = h = m = s = 0

        if "-" in string:
            # days-[hours[:minutes[:seconds]]] format
            parts = string.split("-", 1)
            d = int(parts[0])
            time_part = parts[1] if len(parts) > 1 else "0:0:0"

            time_components = time_part.split(":")
            if len(time_components) >= 1:
                h = int(time_components[0])
            if len(time_components) >= 2:
                m = int(time_components[1])
            if len(time_components) >= 3:
                s = int(time_components[2])

            d *= 86400  # days to seconds
            h *= 3600  # hours to seconds
            m *= 60  # minutes to seconds
        else:
            time_components = string.split(":")
            if len(time_components) == 3:
                # hours:minutes:seconds
                h = int(time_components[0]) * 3600
                m = int(time_components[1]) * 60
                s = int(time_components[2])
            elif len(time_components) == 2:
                m = int(time_components[0]) * 60
                s = int(time_components[1])
            elif len(time_components) == 1:
                # just minutes
                m = int(time_components[0]) * 60

        return d + h + m + s

    def _is_valid_timespec(self, string: str) -> bool:
        """Validate time specification format."""
        # Basic validation - contains only digits, colons, and dashes
        pattern = r"^[\d:-]+$"
        return bool(re.match(pattern, string))

    def _parse_usage_reset_period(self, value: str) -> Optional[int]:
        """Parse usage reset period."""
        if value.lower() == "none":
            return None  # Manual reset only
        return self._parse_time_duration(value)

    def _parse_tres_billing_weights(self, value: str) -> dict[str, float]:
        """Parse TRES billing weights."""
        weights = {}

        for item in value.split(","):
            item = item.strip()
            if "=" in item:
                tres_type, weight_str = item.split("=", 1)
                tres_type = tres_type.strip()
                weight_str = weight_str.strip()

                # Handle memory with units (e.g., "0.001953125G")
                if tres_type == "Mem" and weight_str.endswith("G"):
                    weight = float(weight_str[:-1])  # Remove 'G' suffix
                else:
                    weight = float(weight_str)

                weights[tres_type] = weight

        return weights

    def _parse_boolean(self, value: str) -> bool:
        """Parse boolean values."""
        return value.lower() in ["yes", "true", "1", "on"]

    def _parse_priority_flags(self, value: str) -> list:
        """Parse priority flags."""
        return [flag.strip() for flag in value.split(",")]

    def _set_defaults(self) -> None:
        """Set reasonable default values."""
        self.config = {
            "PriorityDecayHalfLife": 15 * 24 * 60,  # 15 days in minutes
            "PriorityCalcPeriod": 5,  # 5 minutes
            "PriorityMaxAge": 14 * 24 * 60,  # 14 days in minutes
            "PriorityUsageResetPeriod": None,  # Manual reset
            "PriorityWeightAge": 172800,
            "PriorityWeightAssoc": 100000,
            "PriorityWeightFairShare": 259200,
            "PriorityWeightJobSize": 0,
            "PriorityWeightPartition": 172800,
            "PriorityWeightQOS": 500000,
            "FairShareDampeningFactor": 3,
            "TRESBillingWeights": {"CPU": 0.015625, "Mem": 0.001953125, "GRES/gpu": 0.25},
            "PriorityFavorSmall": False,
            "PriorityFlags": ["NO_NORMAL_ASSOC", "MAX_TRES"],
            "PriorityType": "priority/multifactor",
            "SchedulerType": "sched/backfill",
        }

    def get_decay_half_life_days(self) -> float:
        """Get decay half-life in days."""
        minutes = self.config.get("PriorityDecayHalfLife", 15 * 24 * 60)
        return minutes / (24 * 60)

    def get_tres_billing_weights(self) -> dict[str, float]:
        """Get TRES billing weights."""
        return self.config.get("TRESBillingWeights", {})

    def get_qos_weight(self) -> int:
        """Get QoS priority weight."""
        return self.config.get("PriorityWeightQOS", 500000)

    def get_fairshare_weight(self) -> int:
        """Get fairshare priority weight."""
        return self.config.get("PriorityWeightFairShare", 259200)

    def is_manual_usage_reset(self) -> bool:
        """Check if usage reset is manual only."""
        return self.config.get("PriorityUsageResetPeriod") is None

    def get_dampening_factor(self) -> int:
        """Get fairshare dampening factor."""
        return self.config.get("FairShareDampeningFactor", 3)

    def has_priority_flag(self, flag: str) -> bool:
        """Check if specific priority flag is set."""
        flags = self.config.get("PriorityFlags", [])
        return flag in flags

    def supports_tres_billing(self) -> bool:
        """Check if TRES billing is configured."""
        weights = self.get_tres_billing_weights()
        return len(weights) > 0

    def print_config_summary(self) -> None:
        """Print summary of loaded configuration."""
        print("\n📊 SLURM Configuration Summary:")
        print("=" * 50)

        print(f"🕐 Priority Decay Half-Life: {self.get_decay_half_life_days():.1f} days")
        print(f"🔄 Usage Reset: {'Manual' if self.is_manual_usage_reset() else 'Automatic'}")
        print(f"⚖️  Fairshare Weight: {self.get_fairshare_weight():,}")
        print(f"🎛️  QoS Weight: {self.get_qos_weight():,}")
        print(f"📉 Dampening Factor: {self.get_dampening_factor()}")

        print("\n💰 TRES Billing Weights:")
        for tres_type, weight in self.get_tres_billing_weights().items():
            print(f"   {tres_type}: {weight}")

        flags = self.config.get("PriorityFlags", [])
        if flags:
            print(f"\n🏷️  Priority Flags: {', '.join(flags)}")

    def validate_configuration(self) -> list:
        """Validate configuration and return any warnings."""
        warnings = []

        # Check for common configuration issues
        half_life_days = self.get_decay_half_life_days()
        if half_life_days < 1:
            warnings.append(f"Very short decay half-life: {half_life_days:.1f} days")
        elif half_life_days > 365:
            warnings.append(f"Very long decay half-life: {half_life_days:.1f} days")

        # Check TRES billing weights
        weights = self.get_tres_billing_weights()
        if not weights:
            warnings.append("No TRES billing weights configured")
        else:
            # Validate weight ranges
            for tres_type, weight in weights.items():
                if weight <= 0:
                    warnings.append(f"Invalid {tres_type} weight: {weight}")
                elif weight > 1:
                    warnings.append(f"Unusually high {tres_type} weight: {weight}")

        # Check QoS configuration
        qos_weight = self.get_qos_weight()
        fairshare_weight = self.get_fairshare_weight()

        if qos_weight <= fairshare_weight:
            warnings.append(
                f"QoS weight ({qos_weight}) should typically be higher than fairshare weight ({fairshare_weight})"
            )

        return warnings

    def get_emulator_config(self) -> dict[str, Any]:
        """Get configuration formatted for emulator components."""
        return {
            "decay_half_life_days": self.get_decay_half_life_days(),
            "tres_billing_weights": self.get_tres_billing_weights(),
            "manual_usage_reset": self.is_manual_usage_reset(),
            "qos_weight": self.get_qos_weight(),
            "fairshare_weight": self.get_fairshare_weight(),
            "dampening_factor": self.get_dampening_factor(),
            "priority_flags": self.config.get("PriorityFlags", []),
            "supports_tres_billing": self.supports_tres_billing(),
        }
