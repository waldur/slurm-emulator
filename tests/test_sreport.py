"""sreport cluster AccountUtilizationByUser emulation.

Every expectation is anchored to the real client:
slurm://src/sreport/cluster_reports.c#cluster_account_by_user (report
shape), slurm://src/sreport/sreport.c#_build_tres_list (TRES list),
slurm://src/sreport/sreport.c#_set_time_format (unit switch),
slurm://src/sreport/common.c#sreport_get_time_str (unit rendering) and
slurm://src/common/slurmdb_defs.c#slurmdb_report_set_start_end_time
(window normalisation).
"""

import sys
from datetime import datetime

import pytest

from emulator.commands import dispatcher
from emulator.commands.sacct import SacctEmulator
from emulator.commands.sreport import SreportEmulator
from emulator.core.database import SlurmDatabase, UsageRecord
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import UsageSimulator

NOW = datetime(2024, 2, 10, 12, 0, 0)
JAN = ["start=2024-01-01", "end=2024-02-01"]


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.delenv("SLURM_EMULATOR_NODE_POWER_W", raising=False)
    monkeypatch.delenv("SLURM_EMULATOR_GPU_POWER_W", raising=False)
    monkeypatch.delenv("SLURM_EMULATOR_PARTITION_POWER_W", raising=False)
    db = SlurmDatabase()
    db.state_file = tmp_path / "state.json"
    te = TimeEngine(start_time=NOW)
    te.set_time(NOW)
    db.add_account("proj", "Project", "org")
    sim = UsageSimulator(te, db)
    return db, te, sim, SreportEmulator(db, te)


def _seed(sim):
    """Two users in January, one record in February (outside the window)."""
    sim.inject_usage("proj", "alice", 10.0, datetime(2024, 1, 5), energy_joules=1_000_000)
    sim.inject_usage("proj", "bob", 5.0, datetime(2024, 1, 20), energy_joules=250_000)
    sim.inject_usage("proj", "alice", 7.0, datetime(2024, 2, 2), energy_joules=999)


def _rows(output):
    return [line.split("|") for line in output.splitlines() if line]


class TestFlagParsing:
    def test_short_attached_and_long_tres_forms(self, env):
        _, _, sim, sreport = env
        _seed(sim)
        base = ["cluster", "AccountUtilizationByUser", *JAN, "-P", "-n", "accounts=proj"]
        a = sreport.handle_command([*base, "-T", "energy", "-t", "Seconds"])
        b = sreport.handle_command([*base, "-Tenergy", "-tSec"])
        c = sreport.handle_command([*base, "--tres=energy", "-t", "S"])
        assert a == b == c
        assert sreport.exit_code == 0

    def test_report_name_prefix_and_case(self, env):
        _, _, sim, sreport = env
        _seed(sim)
        full = sreport.handle_command(["cluster", "AccountUtilizationByUser", *JAN, "-P", "-n"])
        short = sreport.handle_command(["Cluster", "accountutilizationbyu", *JAN, "-P", "-n"])
        assert full == short

    def test_unknown_time_format_is_reported_and_ignored(self, env, capsys):
        """_set_time_format's error is discarded by main(): no newline, exit 0."""
        _, _, sim, sreport = env
        _seed(sim)
        out = sreport.handle_command(
            [
                "cluster",
                "AccountUtilizationByUser",
                *JAN,
                "-T",
                "energy",
                "-t",
                "Joules",
                "-P",
                "-n",
            ]
        )
        assert capsys.readouterr().err == "unknown time format Joules"
        assert sreport.exit_code == 0
        # Still rendered, in the default Minutes (joules / 60).
        assert _rows(out)[0][-1] == f"{1_250_000 / 60:.0f}"

    def test_unknown_condition_sets_exit_one_but_continues(self, env, capsys):
        _, _, sim, sreport = env
        _seed(sim)
        out = sreport.handle_command(
            ["cluster", "AccountUtilizationByUser", *JAN, "bogus=1", "-P", "-n"]
        )
        assert (
            " Unknown condition: bogus=1\nUse keyword set to modify value"
            in capsys.readouterr().err
        )
        assert sreport.exit_code == 1
        assert _rows(out)

    def test_unknown_tres_is_fatal(self, env, capsys):
        _, _, sim, sreport = env
        _seed(sim)
        with pytest.raises(SystemExit) as exc:
            sreport.handle_command(["cluster", "AccountUtilizationByUser", *JAN, "-T", "watts"])
        assert exc.value.code == 1
        assert capsys.readouterr().err.strip() == "sreport: fatal: No valid TRES given"
        assert sreport.exit_code == 1

    def test_unknown_tres_in_list_is_dropped(self, env):
        _, _, sim, sreport = env
        _seed(sim)
        out = sreport.handle_command(
            ["cluster", "AccountUtilizationByUser", *JAN, "-T", "watts,energy", "-P", "-n"]
        )
        assert {row[4] for row in _rows(out)} == {"energy"}

    def test_other_real_reports_are_rejected(self, env, capsys):
        _, _, _, sreport = env
        with pytest.raises(SystemExit):
            sreport.handle_command(["cluster", "Utilization"])
        assert "is not emulated" in capsys.readouterr().err
        with pytest.raises(SystemExit):
            sreport.handle_command(["cluster", "Nonsense"])
        assert capsys.readouterr().err.startswith("Not valid report Nonsense")

    def test_invalid_time_spec(self, env, capsys):
        _, _, _, sreport = env
        with pytest.raises(SystemExit):
            sreport.handle_command(["cluster", "AccountUtilizationByUser", "start=yesterday-ish"])
        assert capsys.readouterr().err.startswith("Invalid time specification (pos=0)")

    def test_version(self, env):
        _, _, _, sreport = env
        assert sreport.handle_command(["-V"]).startswith("slurm-emulator ")


class TestAggregation:
    def test_energy_rows_per_user_plus_account_total(self, env):
        _, _, sim, sreport = env
        _seed(sim)
        out = sreport.handle_command(
            [
                "cluster",
                "AccountUtilizationByUser",
                *JAN,
                "-T",
                "energy",
                "-t",
                "Seconds",
                "--parsable2",
                "-n",
                "accounts=proj",
            ]
        )
        assert _rows(out) == [
            ["default", "proj", "", "", "energy", "1250000"],
            ["default", "proj", "alice", "", "energy", "1000000"],
            ["default", "proj", "bob", "", "energy", "250000"],
        ]

    def test_month_boundary_on_simulated_clock(self, env):
        _, te, sim, sreport = env
        _seed(sim)
        feb = sreport.handle_command(
            [
                "cluster",
                "AccountUtilizationByUser",
                "start=2024-02-01",
                "end=2024-03-01",
                "-T",
                "energy",
                "-t",
                "Seconds",
                "-P",
                "-n",
                "accounts=proj",
            ]
        )
        assert _rows(feb) == [
            ["default", "proj", "", "", "energy", "999"],
            ["default", "proj", "alice", "", "energy", "999"],
        ]
        # A "previous month" report from a later simulated date sees the same data.
        te.set_time(datetime(2024, 6, 1))
        assert (
            sreport.handle_command(
                [
                    "cluster",
                    "AccountUtilizationByUser",
                    "start=2024-02-01",
                    "end=2024-03-01",
                    "-T",
                    "energy",
                    "-t",
                    "Seconds",
                    "-P",
                    "-n",
                    "accounts=proj",
                ]
            )
            == feb
        )

    def test_default_window_is_yesterday_midnight_to_today_midnight(self, env):
        _, _, sim, sreport = env
        sim.inject_usage("proj", "alice", 1.0, datetime(2024, 2, 9, 8, 0), energy_joules=5)
        sim.inject_usage("proj", "alice", 1.0, datetime(2024, 2, 10, 8, 0), energy_joules=7)
        out = sreport.handle_command(
            ["cluster", "AccountUtilizationByUser", "-T", "energy", "-t", "S"]
        )
        lines = out.splitlines()
        assert lines[1] == (
            "Cluster/Account/User Utilization 2024-02-09T00:00:00 - 2024-02-09T23:59:59 (86400 secs)"
        )
        assert lines[-1].split()[-1] == "5"

    def test_end_rounds_up_and_start_truncates_to_the_hour(self, env):
        _, _, _, sreport = env
        out = sreport.handle_command(
            [
                "cluster",
                "AccountUtilizationByUser",
                "start=2024-01-01T10:20:00",
                "end=2024-01-01T10:30:15",
            ]
        )
        assert "2024-01-01T10:00:00 - 2024-01-01T10:59:59 (3600 secs)" in out

    def test_sub_account_usage_rolls_into_parent_total(self, env):
        db, _, sim, sreport = env
        db.add_account("child", "Child", "org", parent="proj")
        sim.inject_usage("proj", "alice", 1.0, datetime(2024, 1, 5), energy_joules=100)
        sim.inject_usage("child", "carol", 1.0, datetime(2024, 1, 6), energy_joules=40)
        out = sreport.handle_command(
            [
                "cluster",
                "AccountUtilizationByUser",
                *JAN,
                "-T",
                "energy",
                "-t",
                "S",
                "-P",
                "-n",
                "accounts=proj",
            ]
        )
        assert _rows(out) == [
            ["default", "proj", "", "", "energy", "140"],
            ["default", "proj", "alice", "", "energy", "100"],
            ["default", "child", "", "", "energy", "40"],
            ["default", "child", "carol", "", "energy", "40"],
        ]

    def test_users_filter(self, env):
        _, _, sim, sreport = env
        _seed(sim)
        out = sreport.handle_command(
            [
                "cluster",
                "AccountUtilizationByUser",
                *JAN,
                "-T",
                "energy",
                "-t",
                "S",
                "-P",
                "-n",
                "users=bob",
            ]
        )
        assert _rows(out) == [["default", "proj", "bob", "", "energy", "250000"]]

    def test_records_without_energy_report_zero(self, env):
        db, _te, _, sreport = env
        db.add_usage_record(
            UsageRecord(
                account="proj",
                user="dave",
                node_hours=2.0,
                billing_units=2.0,
                timestamp=datetime(2024, 1, 3),
                period="2024-Q1",
            )
        )
        out = sreport.handle_command(
            [
                "cluster",
                "AccountUtilizationByUser",
                *JAN,
                "-T",
                "energy,cpu",
                "-t",
                "Hours",
                "-P",
                "-n",
            ]
        )
        assert ["default", "proj", "dave", "", "energy", "0"] in _rows(out)
        assert ["default", "proj", "dave", "", "cpu", "128"] in _rows(out)
        assert sreport.exit_code == 0

    def test_cluster_filter_and_all_clusters(self, env):
        db, _, sim, sreport = env
        db.add_cluster("prod")
        sim.inject_usage(
            "proj", "alice", 1.0, datetime(2024, 1, 5), cluster="prod", energy_joules=11
        )
        sim.inject_usage("proj", "alice", 1.0, datetime(2024, 1, 5), energy_joules=22)
        args = ["cluster", "AccountUtilizationByUser", *JAN, "-T", "energy", "-t", "S", "-P", "-n"]
        assert [r[0] for r in _rows(sreport.handle_command(args))] == ["default", "default"]
        assert [r[0] for r in _rows(sreport.handle_command([*args, "cluster=prod"]))] == [
            "prod",
            "prod",
        ]
        assert [r[0] for r in _rows(sreport.handle_command([*args, "-a"]))] == ["default"] * 2 + [
            "prod"
        ] * 2


class TestUnits:
    def test_hours_minutes_seconds_for_cpu(self, env):
        _, _, sim, sreport = env
        sim.inject_usage("proj", "alice", 10.0, datetime(2024, 1, 5))  # 640 CPU-hours
        base = ["cluster", "AccountUtilizationByUser", *JAN, "-T", "cpu", "-P", "-n", "users=alice"]
        assert _rows(sreport.handle_command([*base, "-t", "Hours"]))[0][-1] == "640"
        assert _rows(sreport.handle_command([*base, "-t", "Minutes"]))[0][-1] == str(640 * 60)
        assert _rows(sreport.handle_command([*base]))[0][-1] == str(640 * 60)  # default
        assert _rows(sreport.handle_command([*base, "-t", "Seconds"]))[0][-1] == str(640 * 3600)

    def test_energy_seconds_are_joules(self, env):
        _, _, sim, sreport = env
        sim.inject_usage("proj", "alice", 1.0, datetime(2024, 1, 5), energy_joules=7_200_000_000)
        base = [
            "cluster",
            "AccountUtilizationByUser",
            *JAN,
            "-T",
            "energy",
            "-P",
            "-n",
            "users=alice",
        ]
        assert _rows(sreport.handle_command([*base, "-t", "Seconds"]))[0][-1] == "7200000000"
        assert _rows(sreport.handle_command([*base, "-t", "Hours"]))[0][-1] == "2000000"

    def test_percent_formats(self, env):
        _, _, sim, sreport = env
        sim.inject_usage("proj", "alice", 3.0, datetime(2024, 1, 5), energy_joules=300)
        sim.inject_usage("proj", "bob", 1.0, datetime(2024, 1, 5), energy_joules=100)
        base = [
            "cluster",
            "AccountUtilizationByUser",
            *JAN,
            "-T",
            "energy",
            "-P",
            "-n",
            "users=alice",
        ]
        assert _rows(sreport.handle_command([*base, "-t", "Percent"]))[0][-1] == "75.00%"
        assert _rows(sreport.handle_command([*base, "-t", "SecPer"]))[0][-1] == "300(75.00%)"

    def test_cpu_and_gpu_match_sacct_totals(self, env):
        """-T cpu,gres/gpu agrees with the sacct-derived job rows for the window."""
        db, te, sim, sreport = env
        _seed(sim)
        sacct = SacctEmulator(db, te)
        jobs = sacct.handle_command(
            [
                "-S",
                "2024-01-01",
                "-E",
                "2024-02-01",
                "--accounts=proj",
                "--format=User,ReqTRES,ElapsedRaw",
                "-n",
                "-P",
            ]
        )
        expected_cpu = 0
        expected_gpu = 0
        for line in jobs.splitlines():
            _user, tres, elapsed = line.split("|")
            values = dict(item.split("=") for item in tres.split(","))
            expected_cpu += int(values["cpu"]) * int(elapsed)
            expected_gpu += int(values["gres/gpu"]) * int(elapsed)
        rows = _rows(
            sreport.handle_command(
                [
                    "cluster",
                    "AccountUtilizationByUser",
                    *JAN,
                    "-T",
                    "cpu,gres/gpu",
                    "-t",
                    "Seconds",
                    "-P",
                    "-n",
                    "accounts=proj",
                ]
            )
        )
        totals = {row[4]: int(row[5]) for row in rows if row[2] == ""}
        assert totals == {"cpu": expected_cpu, "gres/gpu": expected_gpu}


class TestRendering:
    def test_header_block_and_column_widths(self, env):
        _, _, sim, sreport = env
        _seed(sim)
        out = sreport.handle_command(
            [
                "cluster",
                "AccountUtilizationByUser",
                *JAN,
                "-T",
                "energy",
                "-t",
                "Seconds",
                "accounts=proj",
            ]
        )
        lines = out.splitlines()
        assert lines[0] == "-" * 80
        assert lines[1] == (
            "Cluster/Account/User Utilization 2024-01-01T00:00:00 - 2024-01-31T23:59:59 (2678400 secs)"
        )
        assert lines[2] == "Usage reported in TRES Seconds"
        assert lines[3] == "-" * 80
        # Widths: Cluster 9, Account 15, Login 9, Proper Name 15, TRES Name 14,
        # Used 8 (max value 1,250,000 < 1e8); trailing space after every column.
        assert (
            lines[4]
            == "  Cluster         Account     Login     Proper Name      TRES Name     Used "
        )
        assert (
            lines[5]
            == "--------- --------------- --------- --------------- -------------- -------- "
        )
        assert (
            lines[6]
            == "  default            proj                                   energy  1250000 "
        )

    def test_used_width_grows_with_value(self, env):
        _, _, sim, sreport = env
        sim.inject_usage("proj", "alice", 1.0, datetime(2024, 1, 5), energy_joules=7_200_000_000)
        out = sreport.handle_command(
            ["cluster", "AccountUtilizationByUser", *JAN, "-T", "energy", "-t", "Seconds", "-n"]
        )
        # 7.2e9 >= 1e8 and 1e9 -> width 10
        assert out.splitlines()[0].endswith(" 7200000000 ")

    def test_default_format_without_tres_has_energy_column(self, env):
        _, _, sim, sreport = env
        sim.inject_usage("proj", "alice", 1.0, datetime(2024, 1, 5), energy_joules=6000)
        out = sreport.handle_command(["cluster", "AccountUtilizationByUser", *JAN, "-p"])
        lines = out.splitlines()
        assert lines[2] == "Usage reported in CPU Minutes"
        assert lines[4] == "Cluster|Account|Login|Proper Name|Used|Energy|"
        assert lines[5] == "default|proj|||3840|100|"

    def test_format_condition_and_parsable(self, env):
        _, _, sim, sreport = env
        _seed(sim)
        out = sreport.handle_command(
            [
                "cluster",
                "AccountUtilizationByUser",
                *JAN,
                "-T",
                "energy",
                "-t",
                "S",
                "-P",
                "format=Login,Used",
                "users=bob",
            ]
        )
        assert out.splitlines()[4:] == ["Login|Used", "bob|250000"]

    def test_unknown_format_field(self, env, capsys):
        _, _, _, sreport = env
        with pytest.raises(SystemExit):
            sreport.handle_command(["cluster", "AccountUtilizationByUser", "format=Nope"])
        assert "Unknown field 'Nope'" in capsys.readouterr().err


class TestDispatcher:
    def test_sreport_main_routes_and_exits(self, tmp_path, monkeypatch, capsys):
        em = dispatcher.SlurmEmulator()
        em.database.state_file = tmp_path / "state.json"
        monkeypatch.setattr(dispatcher, "_emulator", em)
        em.time_engine.set_time(NOW)
        UsageSimulator(em.time_engine, em.database).inject_usage(
            "proj", "alice", 1.0, datetime(2024, 1, 5), energy_joules=42
        )
        capsys.readouterr()
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "sreport",
                "cluster",
                "AccountUtilizationByUser",
                *JAN,
                "-T",
                "energy",
                "-t",
                "Seconds",
                "--parsable2",
                "-n",
                "accounts=proj",
            ],
        )
        with pytest.raises(SystemExit) as exc:
            dispatcher.sreport_main()
        assert (exc.value.code or 0) == 0
        assert (
            capsys.readouterr().out == "default|proj|||energy|42\ndefault|proj|alice||energy|42\n"
        )

        monkeypatch.setattr(
            sys, "argv", ["sreport", "cluster", "AccountUtilizationByUser", "-T", "watts"]
        )
        with pytest.raises(SystemExit) as exc:
            dispatcher.sreport_main()
        assert exc.value.code == 1
        assert "sreport: fatal: No valid TRES given" in capsys.readouterr().err

    def test_immediate_flag_rejected(self, tmp_path, monkeypatch, capsys):
        em = dispatcher.SlurmEmulator()
        em.database.state_file = tmp_path / "state.json"
        monkeypatch.setattr(dispatcher, "_emulator", em)
        monkeypatch.setattr(
            sys, "argv", ["sreport", "--immediate", "cluster", "AccountUtilizationByUser"]
        )
        with pytest.raises(SystemExit) as exc:
            dispatcher.sreport_main()
        assert exc.value.code == 1
        assert "unrecognized arguments: --immediate" in capsys.readouterr().err
