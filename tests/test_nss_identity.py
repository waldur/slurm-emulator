"""Opt-in NSS identity mode (``SLURM_EMULATOR_NSS=1``).

CI containers have no sssd/LDAP, so the OS lookup (``emulator.core.nss.resolve``)
is monkeypatched with a small directory. Expectations follow real Slurm:
the USER_ID parser
(slurm://src/plugins/data_parser/v0.0.45/parsers.c#USER_ID@26.05+), the
JOB_INFO uid/gid overloads
(slurm://src/plugins/data_parser/v0.0.45/parsers.c#JOB_INFO@26.05+), the
sacct field table (slurm://src/sacct/sacct.c#fields, slurm://src/sacct/print.c#PRINT_UID),
sacctmgr's uid warning (slurm://src/sacctmgr/user_functions.c#_check_uid,
slurm://src/sacctmgr/common.c#commit_check) and sreport's gecos lookup
(slurm://src/sreport/cluster_reports.c#_cluster_account_by_user_tres_report).
"""

import json
import subprocess
from datetime import datetime

import pytest

from emulator.api.slurmrestd.envelope import ESLURM_USER_ID_UNKNOWN
from emulator.api.ssh import server
from emulator.commands.dispatcher import SlurmEmulator
from emulator.commands.sacct import SacctEmulator
from emulator.commands.sacctmgr import SacctmgrEmulator
from emulator.commands.sreport import SreportEmulator
from emulator.core import nss
from emulator.core.database import Job, SlurmDatabase, UsageRecord
from emulator.core.scheduler import advance_job_states
from emulator.core.time_engine import TimeEngine
from emulator.core.usage_simulator import UsageSimulator
from emulator.slurm_version import current

V = current().api_version
NOW = datetime(2024, 5, 20, 12, 0, 0)

DIRECTORY = {
    "hpc_9001": nss.Identity(
        name="hpc_9001",
        uid=9001,
        gid=9001,
        group="hpc_9001",
        gecos="Test Person,Room 1,,",
        home="/home/hpc_9001",
        groups=(9001, 5000),
        group_names=("hpc_9001", "project-a"),
    ),
}


@pytest.fixture(autouse=True)
def permissive_accounting(monkeypatch):
    """These tests are about identity; admission is covered in test_accounting_enforce."""
    monkeypatch.setenv("SLURM_EMULATOR_ACCOUNTING_ENFORCE", "none")


@pytest.fixture
def nss_on(monkeypatch):
    """Turn NSS mode on with a fake passwd/group database."""
    monkeypatch.setenv(nss.ENV_VAR, "1")
    monkeypatch.setattr(nss, "resolve", lambda name: DIRECTORY.get(name))
    return DIRECTORY


@pytest.fixture
def nss_off(monkeypatch):
    monkeypatch.delenv(nss.ENV_VAR, raising=False)

    def boom(name):  # pragma: no cover - the whole point is that it is never called
        raise AssertionError(f"NSS lookup of {name!r} while disabled")

    monkeypatch.setattr(nss, "resolve", boom)


@pytest.fixture
def emu(state_env):  # state files isolated by the env fixture
    return SlurmEmulator()


def _submit(restd, auth_headers, user, **fields):
    body = {"job": {"user_name": user, "script": "#!/bin/bash\nhostname", **fields}}
    return restd.post(f"/slurm/{V}/job/submit", json=body, headers=auth_headers)


class TestModule:
    def test_enabled_reads_env(self, monkeypatch):
        monkeypatch.delenv(nss.ENV_VAR, raising=False)
        assert nss.enabled() is False
        monkeypatch.setenv(nss.ENV_VAR, "1")
        assert nss.enabled() is True
        monkeypatch.setenv(nss.ENV_VAR, "0")
        assert nss.enabled() is False

    def test_lookup_is_inert_when_off(self, nss_off):
        assert nss.lookup("root") is None

    def test_resolve_uses_the_os_passwd_database(self, monkeypatch):
        # ``root`` exists on every POSIX host the suite runs on; the cache
        # keeps positive hits only (slurm://src/common/uid.c#uid_from_string_cached@26.05+).
        pytest.importorskip("pwd")
        monkeypatch.setattr(nss, "_cache", {})
        root = nss.resolve("root")
        assert root is not None
        assert root.uid == 0
        assert root.groups[0] == root.gid
        assert root.group_names[0] == root.group
        assert root.group_name_of(root.gid) == root.group
        assert nss.resolve("root") is root
        assert nss.resolve("no-such-user-for-the-emulator") is None
        assert "no-such-user-for-the-emulator" not in nss._cache

    def test_misses_are_remembered_briefly(self, monkeypatch):
        pytest.importorskip("pwd")
        monkeypatch.setattr(nss, "_cache", {})
        monkeypatch.setattr(nss, "_misses", {})
        calls = []

        class FakePwd:
            @staticmethod
            def getpwnam(name):
                calls.append(name)
                raise KeyError(name)

        monkeypatch.setattr(nss, "pwd", FakePwd)
        assert nss.resolve("ghost") is None
        assert nss.resolve("ghost") is None
        assert calls == ["ghost"]  # second call served from the negative cache
        monkeypatch.setattr(nss, "NEGATIVE_TTL", 0.0)
        assert nss.resolve("ghost") is None
        assert calls == ["ghost", "ghost"]

    def test_proper_name_is_first_gecos_field(self):
        assert DIRECTORY["hpc_9001"].proper_name == "Test Person"
        assert nss.Identity("x", 1, 1, "x").proper_name == ""


class TestRestSubmit:
    def test_records_uid_gid_group(self, restd, auth_headers, nss_on):
        resp = _submit(restd, auth_headers, "hpc_9001")
        assert resp.status_code == 200
        jid = resp.json()["job_id"]
        job = restd.get(f"/slurm/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
        assert job["user_name"] == "hpc_9001"
        assert job["user_id"] == 9001
        assert job["group_id"] == 9001
        assert job["group_name"] == "hpc_9001"
        assert job["current_working_directory"] == "/home/hpc_9001"

    def test_unresolvable_user_is_rejected(self, restd, auth_headers, nss_on):
        resp = _submit(restd, auth_headers, "ghost")
        # ESLURM_USER_ID_UNKNOWN sits in the slurmctld range → 422
        # (slurm://src/common/http.c#http_status_from_error@25.11+).
        assert resp.status_code == 422
        err = resp.json()["errors"][0]
        assert err["description"] == "Unable to resolve user: ghost"
        assert err["error_number"] == ESLURM_USER_ID_UNKNOWN
        assert err["error"] == "Unable to resolve user ID to user name"
        assert restd.get(f"/slurm/{V}/jobs/", headers=auth_headers).json()["jobs"] == []

    def test_legacy_shape_when_off(self, restd, auth_headers, nss_off):
        resp = _submit(restd, auth_headers, "ghost")
        assert resp.status_code == 200
        jid = resp.json()["job_id"]
        job = restd.get(f"/slurm/{V}/job/{jid}", headers=auth_headers).json()["jobs"][0]
        assert (job["user_id"], job["group_id"], job["group_name"]) == (1000, 1000, "ghost")

    def test_completed_job_carries_identity_into_accounting(
        self, restd, auth_headers, nss_on, monkeypatch
    ):
        monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "wall")
        monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DELAY", "0")
        monkeypatch.setenv("SLURM_EMULATOR_JOB_RUN_DURATION", "0")
        jid = _submit(restd, auth_headers, "hpc_9001").json()["job_id"]
        restd.get(f"/slurm/{V}/jobs/", headers=auth_headers)  # lazily completes the job
        jobs = restd.get(f"/slurmdb/{V}/jobs/", headers=auth_headers).json()["jobs"]
        row = next(j for j in jobs if j["job_id"] == jid)
        assert row["user"] == "hpc_9001"
        assert row["group"] == "hpc_9001"


class TestSshPlane:
    def test_sbatch_and_scontrol_show_real_ids(self, emu, nss_on):
        out, err, code = server._sbatch(emu, "hpc_9001", ["job.sh"])
        assert (err, code) == ("", 0)
        jid = out.split()[-1]
        job = emu.database.get_job(jid)
        assert (job.uid, job.gid, job.group_name) == (9001, 9001, "hpc_9001")
        show, _, _ = server._scontrol(emu, ["show", "job", jid])
        assert "   UserId=hpc_9001(9001) GroupId=hpc_9001(9001)\n" in show

    def test_sbatch_rejects_unknown_user(self, emu, nss_on):
        out, err, code = server._sbatch(emu, "ghost", ["job.sh"])
        assert (out, code) == ("", 1)
        assert err == "sbatch: error: Unable to resolve user: ghost\n"
        assert emu.database.jobs == {}

    def test_scontrol_legacy_ids_when_off(self, emu, nss_off):
        out, _, _ = server._sbatch(emu, "alice", ["job.sh"])
        jid = out.split()[-1]
        show, _, _ = server._scontrol(emu, ["show", "job", jid])
        assert "   UserId=alice(1000) GroupId=alice(1000)\n" in show

    def test_bare_id_over_ssh_means_the_login_user(self, state_env, nss_on):
        out, err, code = server._run_slurm("hpc_9001", ["id"])
        assert (err, code) == ("", 0)
        assert (
            out == "uid=9001(hpc_9001) gid=9001(hpc_9001) groups=9001(hpc_9001),5000(project-a)\n"
        )

    def test_unknown_user_id_over_ssh_fails(self, state_env, nss_on):
        out, err, code = server._run_slurm("ghost", ["id", "-u"])
        assert (out, code) == ("", 1)
        assert err == "id: 'ghost': no such user\n"


class TestSshTimeoutWrapper:
    """FireCREST prefixes every SSH command with coreutils ``timeout N``."""

    @pytest.mark.parametrize(
        ("command", "expected"),
        [
            ("timeout 10 id", ["id"]),
            ("timeout 10 id -u", ["id", "-u"]),
            ("timeout 10 sacct -P -n -j 7", ["sacct", "-P", "-n", "-j", "7"]),
            (
                "timeout -s KILL -k 5 10 /usr/bin/scontrol show job 7",
                ["scontrol", "show", "job", "7"],
            ),
            ("timeout --signal=TERM 10 squeue", ["squeue"]),
            ("/usr/bin/timeout 10 sbatch job.sh", ["sbatch", "job.sh"]),
            ("id -u", ["id", "-u"]),
            ("timeout 10 ls -la", None),
            ("timeout 10", None),
            ("ls", None),
        ],
    )
    def test_slurm_argv(self, nss_on, command, expected):
        assert server._slurm_argv(command) == expected

    @pytest.mark.parametrize("command", ["id", "timeout 10 id", "id -u alice", "/usr/bin/id"])
    def test_id_goes_to_the_shell_when_nss_is_off(self, nss_off, command):
        # Only NSS mode owns ``id``; otherwise real coreutils answers, as before.
        assert server._slurm_argv(command) is None

    def test_other_slurm_bins_unaffected_by_nss(self, nss_off):
        assert server._slurm_argv("timeout 10 sacct -P") == ["sacct", "-P"]


class TestShellRunsAsLoginUser:
    def test_kwargs_when_root_in_nss_mode(self, nss_on, monkeypatch):
        monkeypatch.setattr(server.os, "geteuid", lambda: 0)
        assert server._run_as_kwargs("hpc_9001") == {
            "user": 9001,
            "group": 9001,
            "extra_groups": [9001, 5000],
        }
        assert server._run_as_kwargs("ghost") == {}

    def test_no_switch_without_root_or_nss(self, nss_on, monkeypatch):
        monkeypatch.setattr(server.os, "geteuid", lambda: 1000)
        assert server._run_as_kwargs("hpc_9001") == {}
        monkeypatch.setattr(server.os, "geteuid", lambda: 0)
        monkeypatch.delenv(nss.ENV_VAR)
        assert server._run_as_kwargs("hpc_9001") == {}

    def test_run_shell_passes_identity(self, nss_on, monkeypatch, tmp_path):
        monkeypatch.setenv("SLURM_EMULATOR_FS_ROOT", str(tmp_path / "fs"))
        monkeypatch.setattr(server.os, "geteuid", lambda: 0)
        monkeypatch.setattr(server.os, "chown", lambda *a: None)
        seen = {}

        def fake_run(argv, **kwargs):
            seen.update(kwargs)
            return subprocess.CompletedProcess(argv, 0, "ok\n", "")

        monkeypatch.setattr(server.subprocess, "run", fake_run)
        assert server._run_shell("hpc_9001", "id") == ("ok\n", "", 0)
        assert (seen["user"], seen["group"], seen["extra_groups"]) == (9001, 9001, [9001, 5000])
        assert seen["env"]["USER"] == "hpc_9001"
        assert seen["env"]["HOME"].endswith("/home/hpc_9001")


class TestIdCommand:
    """coreutils ``id`` shapes — FireCREST's IdCommand parses ``uid=`` / ``gid=`` / ``groups=``."""

    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (
                ["hpc_9001"],
                "uid=9001(hpc_9001) gid=9001(hpc_9001) groups=9001(hpc_9001),5000(project-a)",
            ),
            (["-u", "hpc_9001"], "9001"),
            (["-un", "hpc_9001"], "hpc_9001"),
            (["-u", "-n", "hpc_9001"], "hpc_9001"),
            (["-g", "hpc_9001"], "9001"),
            (["-gn", "hpc_9001"], "hpc_9001"),
            (["-G", "hpc_9001"], "9001 5000"),
            (["-Gn", "hpc_9001"], "hpc_9001 project-a"),
            (["hpc_9001", "-u"], "9001"),
            (["--user", "hpc_9001"], "9001"),
            (["--user", "--name", "hpc_9001"], "hpc_9001"),
            (["--group", "hpc_9001"], "9001"),
            (["--groups", "hpc_9001"], "9001 5000"),
            (["--groups", "--name", "hpc_9001"], "hpc_9001 project-a"),
            (
                ["-r", "hpc_9001"],
                "uid=9001(hpc_9001) gid=9001(hpc_9001) groups=9001(hpc_9001),5000(project-a)",
            ),
        ],
    )
    def test_shapes(self, emu, nss_on, args, expected):
        assert emu.execute_command("id", args) == expected

    @pytest.mark.parametrize(
        ("args", "message"),
        [
            (["--bogus", "hpc_9001"], "unrecognized option '--bogus'"),
            (["-x", "hpc_9001"], "invalid option -- 'x'"),
        ],
    )
    def test_unknown_options_are_rejected(self, emu, nss_on, capsys, args, message):
        with pytest.raises(SystemExit) as exc:
            emu.execute_command("id", args)
        assert exc.value.code == 1
        assert capsys.readouterr().err == f"id: {message}\nTry 'id --help' for more information.\n"

    def test_unknown_user(self, emu, nss_on, capsys):
        with pytest.raises(SystemExit) as exc:
            emu.execute_command("id", ["ghost"])
        assert exc.value.code == 1
        assert capsys.readouterr().err == "id: 'ghost': no such user\n"

    def test_missing_operand(self, emu, nss_on):
        assert emu.execute_command("id", []) == "id: missing operand"

    def test_legacy_output_when_off(self, emu, nss_off):
        emu.database.add_user("alice", "")
        assert emu.execute_command("id", ["-u", "alice"]) == "1000"
        assert emu.execute_command("id", ["alice"]) == "1000"
        assert emu.execute_command("id", ["ghost"]) == "id: ghost: no such user"


class TestSacct:
    @pytest.fixture
    def env(self, tmp_path):
        db = SlurmDatabase()
        db.state_file = tmp_path / "state.json"
        te = TimeEngine(start_time=NOW)
        te.set_time(NOW)
        return db, te, SacctEmulator(db, te)

    @staticmethod
    def _record(te, **kwargs):
        return UsageRecord(
            account="proj-a",
            user="hpc_9001",
            node_hours=1.0,
            billing_units=1.0,
            timestamp=te.get_current_time(),
            period=te.get_current_quarter(),
            **kwargs,
        )

    def test_uid_gid_group_columns(self, env):
        db, te, sacct = env
        db.usage_records.append(
            self._record(te, uid=9001, gid=9001, group_name="hpc_9001"),
        )
        out = sacct.handle_command(["-P", "-n", "-o", "UID,GID,Group,User"])
        assert out == "9001|9001|hpc_9001|hpc_9001"

    def test_prefix_u_resolves_to_uid_before_user(self, env):
        # slurm://src/sacct/sacct.c#fields lists UID before User, and
        # slurm://src/sacct/options.c#parse_command_line takes the first prefix match.
        db, te, sacct = env
        db.usage_records.append(self._record(te, uid=9001))
        assert sacct.handle_command(["-P", "-n", "-o", "U"]) == "9001"
        assert sacct.handle_command(["-P", "-n", "-o", "Us"]) == "hpc_9001"

    def test_fixed_width_header(self, env):
        _, _, sacct = env
        header = sacct.handle_command(["-o", "UID,GID,Group"]).splitlines()[0]
        assert header == "   UID    GID     Group "

    def test_legacy_fallback_without_identity(self, env):
        db, te, sacct = env
        db.usage_records.append(self._record(te))
        assert sacct.handle_command(["-P", "-n", "-o", "UID,GID,Group"]) == "1000|1000|hpc_9001"


class TestSacctmgrAddUser:
    @pytest.fixture
    def em(self, tmp_path):
        db = SlurmDatabase()
        db.state_file = tmp_path / "state.json"
        em = SacctmgrEmulator(db, TimeEngine())
        em.handle_command(["add", "account", "proj-a", "description=A", "organization=o"])
        return em

    def test_unknown_uid_blocks_without_immediate(self, em, nss_on):
        out = em.handle_command(["add", "user", "ghost", "account=proj-a"])
        assert out == " There is no uid for user 'ghost'"
        assert em.exit_code == 1
        assert em.database.get_user("ghost") is None

    @pytest.mark.parametrize("flag", ["-i", "--immediate"])
    def test_immediate_skips_the_prompt(self, em, nss_on, flag):
        out = em.handle_command([flag, "add", "user", "ghost", "account=proj-a"])
        assert out.startswith(" Adding User(s)")
        assert em.exit_code == 0
        assert em.database.get_user("ghost") is not None

    def test_known_user_needs_no_flag(self, em, nss_on):
        em.handle_command(["add", "user", "hpc_9001", "account=proj-a"])
        assert em.exit_code == 0
        assert em.database.get_user("hpc_9001") is not None

    def test_no_gate_when_off(self, em, nss_off):
        em.handle_command(["add", "user", "ghost", "account=proj-a"])
        assert em.exit_code == 0
        assert em.database.get_user("ghost") is not None


class TestSreportProperName:
    @pytest.fixture
    def env(self, tmp_path):
        db = SlurmDatabase()
        db.state_file = tmp_path / "state.json"
        te = TimeEngine(start_time=datetime(2024, 2, 10, 12, 0, 0))
        te.set_time(datetime(2024, 2, 10, 12, 0, 0))
        db.add_account("proj", "Project", "org")
        sim = UsageSimulator(te, db)
        sim.inject_usage("proj", "hpc_9001", 10.0, datetime(2024, 1, 5))
        return SreportEmulator(db, te)

    ARGS = (
        "cluster",
        "AccountUtilizationByUser",
        "start=2024-01-01",
        "end=2024-02-01",
        "-P",
        "-n",
        "accounts=proj",
        "format=Login,Proper",
    )

    def test_gecos_when_on(self, env, nss_on):
        rows = [line.split("|") for line in env.handle_command(list(self.ARGS)).splitlines()]
        assert ["hpc_9001", "Test Person"] in rows

    def test_empty_when_off(self, env, nss_off):
        rows = [line.split("|") for line in env.handle_command(list(self.ARGS)).splitlines()]
        assert ["hpc_9001", ""] in rows


class TestStateCompatibility:
    def test_pre_0_10_state_file_loads(self, tmp_path):
        """Jobs and usage records written before the identity fields existed."""
        db = SlurmDatabase()
        db.state_file = tmp_path / "state.json"
        state = {
            "accounts": {},
            "users": {},
            "associations": {},
            "usage_records": [
                {
                    "account": "proj",
                    "user": "alice",
                    "node_hours": 1.0,
                    "billing_units": 1.0,
                    "timestamp": NOW.isoformat(),
                    "period": "2024-Q2",
                    "raw_tres": {},
                    "job_id": 7,
                }
            ],
            "jobs": {
                "8": {
                    "job_id": "8",
                    "account": "proj",
                    "user": "alice",
                    "state": "RUNNING",
                    "submit_time": NOW.isoformat(),
                }
            },
        }
        db.state_file.write_text(json.dumps(state))
        db.load_state()
        assert (db.usage_records[0].uid, db.usage_records[0].gid) == (None, None)
        assert db.usage_records[0].group_name == ""
        assert (db.jobs["8"].uid, db.jobs["8"].gid, db.jobs["8"].group_name) == (None, None, "")
        # slurmdbd's root user and its association are seeded into old files too
        # (slurm://src/plugins/accounting_storage/mysql/as_mysql_cluster.c#as_mysql_add_clusters).
        assert db.get_user("root").default_account == "root"
        assert db.get_association("root", "root") is not None

    def test_identity_round_trips_and_reaches_accounting(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SLURM_EMULATOR_JOB_CLOCK", "time")
        db = SlurmDatabase()
        db.state_file = tmp_path / "state.json"
        te = TimeEngine(start_time=NOW)
        te.set_time(NOW)
        db.add_job(
            Job(
                job_id="1",
                account="proj",
                user="hpc_9001",
                state="RUNNING",
                submit_time=NOW,
                start_time=NOW,
                uid=9001,
                gid=9001,
                group_name="hpc_9001",
            )
        )
        db.save_state()
        fresh = SlurmDatabase()
        fresh.state_file = db.state_file
        fresh.load_state()
        assert fresh.jobs["1"].uid == 9001
        te.set_time(datetime(2024, 5, 21, 12, 0, 0))
        assert advance_job_states(fresh, te)
        record = next(r for r in fresh.usage_records if r.job_id == 1)
        assert (record.uid, record.gid, record.group_name) == (9001, 9001, "hpc_9001")


class TestCacheTtl:
    def test_identity_re_resolved_after_ttl(self, monkeypatch):
        pytest.importorskip("pwd")
        monkeypatch.setattr(nss, "_cache", {})
        monkeypatch.setattr(nss, "_misses", {})
        calls = []
        real_pwd = nss.pwd

        class CountingPwd:
            @staticmethod
            def getpwnam(name):
                calls.append(name)
                return real_pwd.getpwnam(name)

        monkeypatch.setattr(nss, "pwd", CountingPwd)
        monkeypatch.setenv(nss.CACHE_TTL_ENV_VAR, "60")
        first = nss.resolve("root")
        assert nss.resolve("root") is first
        assert calls == ["root"]
        monkeypatch.setenv(nss.CACHE_TTL_ENV_VAR, "0")
        assert nss.resolve("root") is not first
        assert calls == ["root", "root"]

    def test_default_ttl(self, monkeypatch):
        monkeypatch.delenv(nss.CACHE_TTL_ENV_VAR, raising=False)
        assert nss.positive_ttl() == nss.DEFAULT_POSITIVE_TTL
        monkeypatch.setenv(nss.CACHE_TTL_ENV_VAR, "bogus")
        assert nss.positive_ttl() == nss.DEFAULT_POSITIVE_TTL


class TestHomeOwnership:
    def test_existing_contents_are_chowned_once(self, tmp_path, monkeypatch):
        home = tmp_path / "home" / "hpc_9001"
        (home / "old").mkdir(parents=True)
        (home / "old" / "file.txt").write_text("root wrote this")
        chowned = []
        monkeypatch.setattr(server.os, "chown", lambda p, u, g: chowned.append((str(p), u, g)))
        monkeypatch.setattr(server.os, "lchown", lambda p, u, g: chowned.append((str(p), u, g)))
        identity = DIRECTORY["hpc_9001"]
        server._own_home(home, identity)
        paths = {p for p, _, _ in chowned}
        assert str(home / "old") in paths
        assert str(home / "old" / "file.txt") in paths
        assert str(home) in paths
        assert all((u, g) == (9001, 9001) for _, u, g in chowned)
        assert (home / server._OWNER_STAMP).read_text().strip() == "9001:9001"
        chowned.clear()
        server._own_home(home, identity)
        assert chowned == []  # stamp matches: nothing to do
        server._own_home(home, nss.Identity("x", 7, 7, "x"))
        assert chowned  # identity changed: walk again
