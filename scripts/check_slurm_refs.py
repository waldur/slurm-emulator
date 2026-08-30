#!/usr/bin/env python3
"""Validate ``slurm://`` source references against the cached Slurm checkouts.

Every parity claim in the emulator (a comment, docstring, test or doc) must
point at real Slurm source with a *stable* reference:

    slurm://<repo-relative path>[#<anchor>][@<versions>]

    slurm://src/sacctmgr/common.c#_get_print_field
    slurm://src/sacctmgr/common.c#"Def QOS"
    slurm://slurm/slurmdb.h#slurmdb_qos_rec_t.grp_tres_mins
    slurm://src/plugins/data_parser/v0.0.45/parsers.c#ASSOC_SHORT@26.05+
    slurm://src/sacctmgr/qos_functions.c#RawUsage@25.11+

- ``path`` must exist in every version the reference applies to.
- ``anchor`` (optional) is a C identifier — or ``"quoted literal"`` — that must
  occur in that file. Dotted anchors (``struct.member``) require each part.
- ``@versions`` (optional) restricts the reference to some tracked versions:
  a comma list (``26.05,25.11``), an open range (``25.11+`` = 25.11 and newer,
  including ``master``) or ``all``. Default: every tracked version.

Line numbers are deliberately *not* part of the grammar: they drift on every
upstream commit. The checker also rejects the legacy ``file.c:123`` form and
any hard-coded local checkout path. A file that must contain such strings as
fixtures opts out with the literal marker ``slurm-refs: ignore-file``.

    uv run scripts/check_slurm_refs.py            # pre-commit: skip versions not cached
    uv run scripts/check_slurm_refs.py --strict   # CI: every tracked version must be cached
    uv run scripts/check_slurm_refs.py --summary  # per-version coverage table
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from slurm_parity_config import REPO_ROOT, ParityConfig, load_config

REF_RE = re.compile(
    r"slurm://(?P<path>[\w./+-]*[\w/+-])"
    r"(?:#(?P<anchor>\"[^\"]+\"|[A-Za-z_][\w.]*))?"
    r"(?:@(?P<versions>[\w.,+]*[\w+]))?"
    r"(?=[\s`'\")\],;:>.]|$)"
)
# Legacy "file.c:123" / "file.h:12-15" refs — no longer allowed (lines drift).
LEGACY_RE = re.compile(r"\b[A-Za-z_][\w]*\.[ch]:\d+")
# A file containing this marker (e.g. the checker's own tests) is not scanned.
IGNORE_MARKER = "slurm-refs: ignore-file"
LOCAL_PATH_RE = re.compile(r"/Users/\w+/workspace/slurm\b")
SCAN_SUFFIXES = {".py", ".md", ".txt", ".rst", ".toml", ".yaml", ".yml"}
SKIP_DIRS = {".git", ".venv", "node_modules", "__pycache__", ".pytest_cache", ".mypy_cache"}


@dataclass
class Ref:
    file: Path
    line: int
    raw: str
    path: str
    anchor: str | None
    versions: str | None


def iter_files(cfg: ParityConfig) -> list[Path]:
    roots = [REPO_ROOT / p for p in cfg.scan_paths] or [REPO_ROOT]
    out: list[Path] = []
    for root in roots:
        if root.is_file():
            out.append(root)
            continue
        for p in sorted(root.rglob("*")):
            if any(part in SKIP_DIRS for part in p.parts):
                continue
            if p.is_file() and p.suffix in SCAN_SUFFIXES:
                out.append(p)
    return out


def collect(files: list[Path], self_path: Path) -> tuple[list[Ref], list[str]]:
    refs: list[Ref] = []
    problems: list[str] = []
    for f in files:
        try:
            text = f.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        rel = f.relative_to(REPO_ROOT)
        if f == self_path or IGNORE_MARKER in text:
            continue  # grammar placeholders / fixtures, not real references
        for n, line in enumerate(text.splitlines(), 1):
            for m in REF_RE.finditer(line):
                refs.append(
                    Ref(rel, n, m.group(0), m.group("path"), m.group("anchor"), m.group("versions"))
                )
            if rel.name == "slurm-parity.md":
                continue  # grammar docs legitimately show the legacy form
            for m in LEGACY_RE.finditer(line):
                problems.append(
                    f"{rel}:{n}: legacy line reference {m.group(0)!r} — use slurm://path#symbol"
                )
            if LOCAL_PATH_RE.search(line):
                problems.append(
                    f"{rel}:{n}: hard-coded local Slurm checkout path — use slurm:// references"
                )
    return refs, problems


def _anchor_found(text: str, anchor: str) -> bool:
    if anchor.startswith('"'):
        return anchor[1:-1] in text
    return all(
        re.search(r"(?<![\w])" + re.escape(part) + r"(?![\w])", text) for part in anchor.split(".")
    )


def check(cfg: ParityConfig, refs: list[Ref], strict: bool) -> tuple[list[str], Counter, set[str]]:
    errors: list[str] = []
    checked: Counter = Counter()
    uncached: set[str] = set()
    file_cache: dict[tuple[str, str], str | None] = {}

    for ref in refs:
        try:
            versions = cfg.resolve_versions(ref.versions)
        except ValueError as exc:
            errors.append(f"{ref.file}:{ref.line}: {ref.raw}: {exc}")
            continue
        for version in versions:
            wt = cfg.worktree(version)
            if not (wt / ".git").exists():
                uncached.add(version)
                if strict:
                    errors.append(f"{ref.file}:{ref.line}: {ref.raw}: version {version} not cached")
                continue
            key = (version, ref.path)
            if key not in file_cache:
                p = wt / ref.path
                file_cache[key] = (
                    p.read_text(encoding="utf-8", errors="replace") if p.is_file() else None
                )
            text = file_cache[key]
            checked[version] += 1
            if text is None:
                errors.append(
                    f"{ref.file}:{ref.line}: {ref.raw}: {ref.path} does not exist in {version}"
                )
            elif ref.anchor and not _anchor_found(text, ref.anchor):
                errors.append(
                    f"{ref.file}:{ref.line}: {ref.raw}: anchor {ref.anchor} not found in {version}"
                )
    return errors, checked, uncached


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--strict", action="store_true", help="fail when a tracked version is not cached"
    )
    ap.add_argument("--summary", action="store_true", help="print per-version coverage")
    ap.add_argument(
        "paths", nargs="*", help="files to scan (default: [tool.slurm-parity].scan_paths)"
    )
    args = ap.parse_args(argv)

    cfg = load_config()
    files = [Path(p).resolve() for p in args.paths] if args.paths else iter_files(cfg)
    refs, problems = collect(files, Path(__file__).resolve())
    errors, checked, uncached = check(cfg, refs, args.strict)
    errors = problems + errors

    if args.summary:
        print(f"{len(refs)} slurm:// reference(s) in {len(files)} file(s)")
        for version in cfg.versions:
            tag = " (primary)" if version == cfg.primary else ""
            state = "not cached" if version in uncached else f"{checked[version]} checked"
            print(f"  {version:>7}{tag:<10} {state}")
    for e in errors:
        print(e)
    if uncached and not args.strict:
        print(
            f"note: skipped versions {sorted(uncached)} — not in cache "
            f"({cfg.cache}); run: uv run scripts/slurm_src.py update",
            file=sys.stderr,
        )
    if errors:
        print(f"{len(errors)} problem(s)", file=sys.stderr)
        return 1
    if not args.summary:
        print(f"ok: {len(refs)} slurm:// reference(s) verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
