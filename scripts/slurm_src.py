#!/usr/bin/env python3
"""Manage the local cache of real Slurm source used for parity tracing.

The emulator is verified against SchedMD's source (https://github.com/SchedMD/slurm),
one checkout per tracked version (``[tool.slurm-parity]`` in ``pyproject.toml``).
This script owns that cache — a bare clone plus one ``git worktree`` per version —
so every developer and CI job resolves ``slurm://`` references the same way.

    uv run scripts/slurm_src.py update            # clone if needed, fetch, fast-forward
    uv run scripts/slurm_src.py update --shallow  # CI: depth-1 per branch, no bare clone
    uv run scripts/slurm_src.py status            # what is checked out, how old
    uv run scripts/slurm_src.py path 26.05        # print a worktree path for shell use
    uv run scripts/slurm_src.py grep _get_print_field src/sacctmgr/common.c --versions 25.11+

Environment: ``SLURM_SRC_CACHE`` overrides the cache directory.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

from slurm_parity_config import ParityConfig, load_config


def _git(*args: str, cwd: Path | None = None, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=cwd, check=check, text=True, capture_output=True)


def _meta_version(worktree: Path) -> str:
    meta = worktree / "META"
    if not meta.exists():
        return "?"
    text = meta.read_text()
    version = re.search(r"^\s*Version:\s*(\S+)", text, re.MULTILINE)
    release = re.search(r"^\s*Release:\s*(\S+)", text, re.MULTILINE)
    v = version.group(1) if version else "?"
    if release and release.group(1) != "1":
        v += f"-{release.group(1)}"
    return v


# --- update ---------------------------------------------------------------------


def _ensure_bare(cfg: ParityConfig) -> None:
    if (cfg.bare_repo / "HEAD").exists():
        return
    cfg.cache.mkdir(parents=True, exist_ok=True)
    print(f"cloning {cfg.remote} -> {cfg.bare_repo} (bare, ~400 MB)")
    _git("clone", "--bare", cfg.remote, str(cfg.bare_repo))
    # A bare clone has no fetch refspec; add one so origin/* tracking refs exist.
    _git(
        "config",
        "remote.origin.fetch",
        "+refs/heads/*:refs/remotes/origin/*",
        cwd=cfg.bare_repo,
    )


def _update_full(cfg: ParityConfig, versions: list[str]) -> None:
    _ensure_bare(cfg)
    print("fetching origin")
    _git("fetch", "--prune", "--tags", "origin", cwd=cfg.bare_repo)
    for version in versions:
        wt = cfg.worktree(version)
        branch = cfg.upstream_branch(version)
        if not (wt / ".git").exists():
            print(f"adding worktree {wt} ({branch})")
            _git("worktree", "add", "-f", str(wt), f"origin/{branch}", cwd=cfg.bare_repo)
        _git("checkout", "-q", "-B", branch, f"origin/{branch}", cwd=wt)
        print(
            f"  {version:>7}  {_meta_version(wt):<12} {_git('log', '-1', '--format=%h %cs', cwd=wt).stdout.strip()}"
        )


def _update_shallow(cfg: ParityConfig, versions: list[str]) -> None:
    """CI variant: one depth-1 clone per branch, no shared object store."""
    cfg.cache.mkdir(parents=True, exist_ok=True)
    for version in versions:
        wt = cfg.worktree(version)
        branch = cfg.upstream_branch(version)
        if (wt / ".git").exists():
            _git("fetch", "--depth", "1", "origin", branch, cwd=wt)
            _git("checkout", "-q", "-B", branch, "FETCH_HEAD", cwd=wt)
        else:
            print(f"shallow-cloning {branch} -> {wt}")
            _git("clone", "--depth", "1", "--branch", branch, cfg.remote, str(wt))
        print(
            f"  {version:>7}  {_meta_version(wt):<12} {_git('log', '-1', '--format=%h %cs', cwd=wt).stdout.strip()}"
        )


def cmd_update(cfg: ParityConfig, args: argparse.Namespace) -> int:
    versions = cfg.resolve_versions(args.versions)
    if args.shallow:
        _update_shallow(cfg, versions)
    else:
        _update_full(cfg, versions)
    return 0


# --- status / path / grep ----------------------------------------------------------


def cmd_status(cfg: ParityConfig, _args: argparse.Namespace) -> int:
    print(f"cache:   {cfg.cache}")
    print(f"remote:  {cfg.remote}")
    print(f"primary: {cfg.primary}")
    missing = 0
    for version in cfg.versions:
        wt = cfg.worktree(version)
        if not (wt / ".git").exists():
            print(f"  {version:>7}  MISSING  ({wt}) — run: uv run scripts/slurm_src.py update")
            missing += 1
            continue
        head = _git("log", "-1", "--format=%h %cs", cwd=wt).stdout.strip()
        print(f"  {version:>7}  {_meta_version(wt):<12} {head}  {wt}")
    return 1 if missing else 0


def cmd_path(cfg: ParityConfig, args: argparse.Namespace) -> int:
    version = args.version or cfg.primary
    if version not in cfg.versions:
        sys.exit(f"unknown version {version!r}; tracked: {cfg.versions}")
    wt = cfg.worktree(version)
    if not (wt / ".git").exists():
        sys.exit(f"{wt} is not checked out — run: uv run scripts/slurm_src.py update")
    print(wt)
    return 0


def cmd_grep(cfg: ParityConfig, args: argparse.Namespace) -> int:
    """Grep a pattern across the tracked versions (default: all) to see where it exists."""
    versions = cfg.resolve_versions(args.versions)
    rc = 0
    for version in versions:
        wt = cfg.worktree(version)
        if not (wt / ".git").exists():
            print(f"[{version}] not checked out")
            rc = 1
            continue
        target = (
            [str(wt / p) for p in args.paths]
            if args.paths
            else [str(wt / "src"), str(wt / "slurm")]
        )
        res = subprocess.run(
            ["grep", "-rn", "--include=*.c", "--include=*.h", "-E", args.pattern, *target],
            check=False,
            text=True,
            capture_output=True,
        )
        hits = res.stdout.strip().splitlines()
        print(f"[{version}] {len(hits)} hit(s)")
        for h in hits[: args.limit]:
            print("   " + h.replace(str(wt) + "/", ""))
    return rc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("update", help="clone/fetch the cache and fast-forward every worktree")
    p.add_argument("--shallow", action="store_true", help="depth-1 clone per branch (CI)")
    p.add_argument("--versions", help="comma list / 'all' / '25.11+' (default: all tracked)")
    p.set_defaults(func=cmd_update)

    p = sub.add_parser("status", help="show checked-out versions")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("path", help="print the worktree path for a version")
    p.add_argument("version", nargs="?")
    p.set_defaults(func=cmd_path)

    p = sub.add_parser("grep", help="grep a pattern across tracked versions")
    p.add_argument("pattern")
    p.add_argument("paths", nargs="*", help="repo-relative paths to restrict the search")
    p.add_argument("--versions")
    p.add_argument("--limit", type=int, default=5)
    p.set_defaults(func=cmd_grep)

    args = parser.parse_args(argv)
    cfg = load_config()
    return args.func(cfg, args)


if __name__ == "__main__":
    sys.exit(main())
