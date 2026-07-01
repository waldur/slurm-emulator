"""GNU/BSD coreutils portability for the SSH filesystem plane.

FireCREST issues GNU-style commands; Linux has GNU coreutils natively, macOS
ships BSD variants. The server prepends Homebrew GNU ``gnubin`` dirs to PATH on
macOS so those commands resolve to GNU implementations. These tests exercise the
detection and PATH routing without depending on the host OS.
"""

from __future__ import annotations

import os

import pytest

from emulator.api.ssh import server


@pytest.fixture(autouse=True)
def _clear_cache():
    server._gnu_gnubin_dirs.cache_clear()
    yield
    server._gnu_gnubin_dirs.cache_clear()


def _make_gnubin(tmp_path):
    gnubin = tmp_path / "opt" / "coreutils" / "libexec" / "gnubin"
    gnubin.mkdir(parents=True)
    return gnubin


def test_linux_has_native_gnu(monkeypatch):
    monkeypatch.setattr(server.platform, "system", lambda: "Linux")
    assert server._gnu_gnubin_dirs() == ()
    assert server.gnu_coreutils_available() is True


def test_macos_without_brew_reports_unavailable(monkeypatch, tmp_path):
    monkeypatch.setattr(server.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(server, "_BREW_PREFIXES", (str(tmp_path),))
    assert server._gnu_gnubin_dirs() == ()
    assert server.gnu_coreutils_available() is False


def test_macos_with_brew_prepends_gnubin_to_path(monkeypatch, tmp_path):
    gnubin = _make_gnubin(tmp_path)
    monkeypatch.setattr(server.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(server, "_BREW_PREFIXES", (str(tmp_path),))
    monkeypatch.setenv("SLURM_EMULATOR_FS_ROOT", str(tmp_path / "fs"))

    assert str(gnubin) in server._gnu_gnubin_dirs()
    assert server.gnu_coreutils_available() is True
    env = server._command_env("bob")
    assert env["PATH"].split(os.pathsep)[0] == str(gnubin)


def test_run_shell_routes_to_gnu_tool(monkeypatch, tmp_path):
    # A shim 'stat' that a GNU-aware caller (using -c) would invoke. If PATH
    # routing works, `stat -c` reaches the shim regardless of the host's stat.
    gnubin = _make_gnubin(tmp_path)
    shim = gnubin / "stat"
    shim.write_text('#!/bin/bash\n[ "$1" = -c ] && echo GNU-STAT || echo BSD-STAT\n')
    shim.chmod(0o755)

    monkeypatch.setattr(server.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(server, "_BREW_PREFIXES", (str(tmp_path),))
    monkeypatch.setenv("SLURM_EMULATOR_FS_ROOT", str(tmp_path / "fs"))

    out, _err, code = server._run_shell("bob", "stat -c '%s' anything")
    assert code == 0
    assert out.strip() == "GNU-STAT"
