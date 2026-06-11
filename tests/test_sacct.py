"""Output and exit-code parity tests for the sacct emulator.

Expected shapes come from real Slurm 26.11: default fields
(src/sacct/sacct.h:66), field widths (src/sacct/sacct.c:43-169),
header/dash/truncation bytes (src/common/print_fields.c:66-176),
elapsed format (secs2time_str, src/common/parse_time.c:849-874), and
error handling (src/sacct/options.c:591-593, 1215-1216).
"""

from datetime import datetime, timedelta

import pytest

from emulator.commands.sacct import SacctEmulator
from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine

NOW = datetime(2024, 5, 20, 12, 0, 0)


@pytest.fixture
def env(tmp_path):
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    te = TimeEngine(start_time=NOW)
    te.set_time(NOW)
    return db, te, SacctEmulator(db, te)


def _record(te, account="proj-a", user="alice", node_hours=1.5, **kwargs):
    return UsageRecord(
        account=account,
        user=user,
        node_hours=node_hours,
        billing_units=node_hours,
        timestamp=kwargs.pop("timestamp", te.get_current_time()),
        period=te.get_current_quarter(),
        **kwargs,
    )


class TestDefaultFormat:
    def test_header_bytes(self, env):
        _, _, sacct = env
        out = sacct.handle_command([])
        lines = out.splitlines()
        # JobID is -12 (left-aligned); everything else right-aligned;
        # trailing space after every column.
        assert lines[0] == (
            "JobID           JobName  Partition    Account  AllocCPUS      State ExitCode "
        )
        assert lines[1] == (
            "------------ ---------- ---------- ---------- ---------- ---------- -------- "
        )

    def test_default_fields_in_order(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te))
        out = sacct.handle_command(["-n", "-P"])
        cells = out.split("|")
        assert cells == ["1", "job_1", "compute", "proj-a", "64", "COMPLETED", "0:0"]

    def test_no_records_prints_header_only(self, env):
        _, _, sacct = env
        out = sacct.handle_command([])
        assert len(out.splitlines()) == 2

    def test_noheader_no_records_is_empty(self, env):
        _, _, sacct = env
        assert sacct.handle_command(["-n"]) == ""


class TestParsableModes:
    def test_p_trailing_pipe_with_header(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te))
        lines = sacct.handle_command(["-p"]).splitlines()
        assert lines[0] == "JobID|JobName|Partition|Account|AllocCPUS|State|ExitCode|"
        assert lines[1].endswith("|")

    def test_long_forms_equal_short(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te))
        assert sacct.handle_command(["--noheader", "--parsable2"]) == sacct.handle_command(
            ["-n", "-P"]
        )

    def test_truncation_only_in_fixed_width(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te, account="very-long-account"))
        fixed = sacct.handle_command(["-n", "-o", "Account"])
        parsable = sacct.handle_command(["-n", "-P", "-o", "Account"])
        assert fixed == "very-long+ "
        assert parsable == "very-long-account"


class TestElapsed:
    def test_elapsed_hh_mm_ss(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te, node_hours=1.5))
        out = sacct.handle_command(["-n", "-P", "-o", "Elapsed"])
        assert out == "01:30:00"

    def test_elapsed_with_days(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te, node_hours=30))
        out = sacct.handle_command(["-n", "-P", "-o", "Elapsed"])
        # secs2time_str: "%ld-%2.2ld:%2.2ld:%2.2ld" when days > 0.
        assert out == "1-06:00:00"


class TestJobIds:
    def test_sequential_numeric_ids(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te))
        db.add_usage_record(_record(te, user="bob"))
        out = sacct.handle_command(["-n", "-P", "-o", "JobID"])
        assert out.splitlines() == ["1", "2"]

    def test_ids_stable_across_calls(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te))
        first = sacct.handle_command(["-n", "-P", "-o", "JobID"])
        second = sacct.handle_command(["-n", "-P", "-o", "JobID"])
        assert first == second == "1"

    def test_ids_survive_state_roundtrip(self, env, tmp_path):
        db, te, _sacct = env
        db.add_usage_record(_record(te))
        db.save_state()

        db2 = SlurmDatabase()
        db2.state_file = db.state_file
        db2.load_state()
        assert db2.usage_records[0].job_id == 1
        db2.add_usage_record(_record(te, user="bob"))
        assert db2.usage_records[1].job_id == 2

    def test_directly_appended_records_get_lazy_ids(self, env):
        db, te, sacct = env
        db.usage_records.append(_record(te))
        out = sacct.handle_command(["-n", "-P", "-o", "JobID"])
        assert out == "1"


class TestStateField:
    def test_state_from_record(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te, state="FAILED"))
        out = sacct.handle_command(["-n", "-P", "-o", "State,ExitCode"])
        assert out == "FAILED|1:0"

    def test_state_defaults_completed(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te))
        out = sacct.handle_command(["-n", "-P", "-o", "State,ExitCode"])
        assert out == "COMPLETED|0:0"


class TestErrors:
    def test_invalid_time_exits_one(self, env, capsys):
        _, _, sacct = env
        with pytest.raises(SystemExit) as exc:
            sacct.handle_command(["--starttime=garbage"])
        assert exc.value.code == 1
        assert sacct.exit_code == 1
        # parse_time.c:626-631: no "sacct:" prefix on this one.
        assert capsys.readouterr().err.startswith("Invalid time specification (pos=")

    def test_invalid_field_exits_one(self, env, capsys):
        _, _, sacct = env
        with pytest.raises(SystemExit) as exc:
            sacct.handle_command(["-o", "JobID,Bogus"])
        assert exc.value.code == 1
        assert 'sacct: error: Invalid field requested: "Bogus"' in capsys.readouterr().err

    def test_unrecognized_argument_exits_one(self, env, capsys):
        _, _, sacct = env
        with pytest.raises(SystemExit):
            sacct.handle_command(["--bogus-flag"])
        assert "unrecognized arguments" in capsys.readouterr().err


class TestFieldResolution:
    def test_prefix_match(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te))
        out = sacct.handle_command(["-n", "-P", "-o", "acc"])
        assert out == "proj-a"

    def test_width_override(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te))
        out = sacct.handle_command(["-n", "-o", "Account%20"])
        assert out == "              proj-a "


class TestTimeWindow:
    def test_default_window_is_midnight_to_now(self, env):
        db, te, sacct = env
        yesterday = NOW - timedelta(days=1)
        db.add_usage_record(_record(te, timestamp=yesterday))
        db.add_usage_record(_record(te, user="bob"))
        out = sacct.handle_command(["-n", "-P", "-o", "User"])
        # slurmdb_job_cond_def_start_end: start = Midnight, end = Now —
        # yesterday's record is outside the default window.
        assert out == "bob"

    def test_explicit_start_includes_history(self, env):
        db, te, sacct = env
        yesterday = NOW - timedelta(days=1)
        db.add_usage_record(_record(te, timestamp=yesterday))
        out = sacct.handle_command(["-n", "-P", "-o", "User", "-S", "2024-05-01"])
        assert out == "alice"

    def test_now_keyword(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te, timestamp=NOW - timedelta(days=1)))
        out = sacct.handle_command(["-n", "-P", "-o", "User", "-S", "now-2days"])
        assert out == "alice"


class TestFilters:
    def test_short_flags(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te))
        db.add_usage_record(_record(te, account="proj-b", user="bob"))
        out = sacct.handle_command(["-n", "-P", "-A", "proj-b", "-o", "User"])
        assert out == "bob"
        out = sacct.handle_command(["-n", "-P", "-u", "alice", "-o", "User"])
        assert out == "alice"

    def test_allocations_and_allusers_are_noops(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te))
        plain = sacct.handle_command(["-n", "-P"])
        flagged = sacct.handle_command(["-n", "-P", "-X", "-a"])
        assert plain == flagged

    def test_nonexistent_cluster_filter_yields_no_rows(self, env):
        db, te, sacct = env
        db.add_usage_record(_record(te))
        out = sacct.handle_command(["-n", "-P", "-M", "ghost"])
        assert out == ""
        assert sacct.exit_code == 0


class TestSiteAgentPath:
    def test_site_agent_invocation_shape(self, env):
        """The site-agent invocation must yield real parsable2 output.

        This is the exact flag combination waldur-site-agent uses.
        """
        db, te, sacct = env
        db.add_usage_record(_record(te, node_hours=2.0))
        out = sacct.handle_command(
            [
                "--allusers",
                "--allocations",
                "-S",
                "2024-05-01",
                "-E",
                "2024-05-31",
                "--accounts=proj-a",
                "--format=Account,ReqTRES,Elapsed,User",
                "--noheader",
                "--parsable2",
            ]
        )
        assert out == "proj-a|cpu=64,mem=512G,node=1,billing=64,gres/gpu=4|02:00:00|alice"
        assert sacct.exit_code == 0
