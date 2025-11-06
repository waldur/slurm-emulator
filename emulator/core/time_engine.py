"""Time manipulation engine for SLURM emulator."""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

from dateutil.relativedelta import relativedelta


class TimeEngine:
    """Handles time manipulation and period transitions."""

    def __init__(self, start_time: Optional[datetime] = None):
        self.current_time = start_time or datetime(2024, 1, 1)
        self.time_callbacks: list[Callable] = []
        self.state_file = Path("/tmp/slurm_emulator_time.json")
        self._load_state()

    def advance_time(self, days: int = 0, months: int = 0, quarters: int = 0) -> None:
        """Advance time by specified amount."""
        if quarters:
            months += quarters * 3
        if months:
            self.current_time = self.current_time + relativedelta(months=months)
        if days:
            self.current_time = self.current_time + timedelta(days=days)

        self._save_state()
        self._trigger_time_callbacks()

    def set_time(self, target_time: datetime) -> None:
        """Jump to specific time."""
        old_quarter = self.get_current_quarter()
        self.current_time = target_time
        new_quarter = self.get_current_quarter()

        self._save_state()

        # Check for period transition
        if old_quarter != new_quarter:
            self._trigger_time_callbacks()

    def get_current_time(self) -> datetime:
        """Get current emulator time."""
        return self.current_time

    def get_current_quarter(self) -> str:
        """Get current quarter info for period calculations."""
        year = self.current_time.year
        quarter = (self.current_time.month - 1) // 3 + 1
        return f"{year}-Q{quarter}"

    def get_quarter_start_end(self, quarter_str: Optional[str] = None) -> tuple[datetime, datetime]:
        """Get start/end dates for quarter."""
        if not quarter_str:
            quarter_str = self.get_current_quarter()

        # Parse "2024-Q2" format
        year_str, q_str = quarter_str.split("-Q")
        year = int(year_str)
        quarter = int(q_str)

        start_month = (quarter - 1) * 3 + 1
        start_date = datetime(year, start_month, 1)
        end_date = start_date + relativedelta(months=3) - timedelta(days=1)

        return start_date, end_date

    def get_days_between_quarters(self, from_quarter: str, to_quarter: str) -> int:
        """Calculate days elapsed between quarter transitions."""
        _, from_end = self.get_quarter_start_end(from_quarter)
        to_start, _ = self.get_quarter_start_end(to_quarter)

        # Use end of from_quarter to start of to_quarter
        return (to_start - from_end).days

    def register_time_callback(self, callback: Callable) -> None:
        """Register function to call when time changes."""
        self.time_callbacks.append(callback)

    def _trigger_time_callbacks(self) -> None:
        """Trigger all registered time callbacks."""
        for callback in self.time_callbacks:
            try:
                callback()
            except Exception as e:
                print(f"Warning: Time callback failed: {e}")

    def _save_state(self) -> None:
        """Save current time state to file."""
        state = {"current_time": self.current_time.isoformat()}
        try:
            with self.state_file.open("w") as f:
                json.dump(state, f)
        except Exception:
            pass  # Ignore save errors

    def _load_state(self) -> None:
        """Load time state from file."""
        try:
            if self.state_file.exists():
                with self.state_file.open() as f:
                    state = json.load(f)
                    self.current_time = datetime.fromisoformat(state["current_time"])
        except Exception:
            pass  # Ignore load errors

    def format_current_month(self) -> tuple[str, str]:
        """Format current month start/end for SLURM commands."""
        month_start = self.current_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        month_end = month_start + relativedelta(months=1) - timedelta(seconds=1)

        return (month_start.strftime("%Y-%m-%dT%H:%M:%S"), month_end.strftime("%Y-%m-%dT%H:%M:%S"))
