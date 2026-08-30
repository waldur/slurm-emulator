# Tracing the emulator against real Slurm source

Every behaviour the emulator claims — an output column width, an exit code, a
JSON field path, an error string — is a claim about what real Slurm does. This
document describes how those claims are anchored to SchedMD's source code,
how the source is cached locally, and how the same claim is checked against
several Slurm versions.

## Why

Parity disputes ("does `sacctmgr` really exit 0 on *Nothing modified*?") are
settled by reading the C source, not by memory or by a screenshot from one
cluster. Recording *where* in the source a behaviour was verified makes the
next change reviewable, and makes it possible to notice when upstream changes
it.

Line numbers are not a usable anchor: they move on every upstream commit, and
the references this repo carried before this scheme were already stale within
weeks. Symbols (function names, struct members, string literals) are.

## The source cache

The tooling keeps one bare clone of <https://github.com/SchedMD/slurm> and one
`git worktree` per tracked version:

```
$SLURM_SRC_CACHE/               default: ~/.cache/slurm-emulator
├── slurm.git/                  bare clone, shared object store
├── master/                     origin/master   (next release, pre-release)
├── slurm-26.05/                origin/slurm-26.05
├── slurm-25.11/                origin/slurm-25.11
├── slurm-25.05/                origin/slurm-25.05
└── slurm-24.11/                origin/slurm-24.11
```

```bash
uv run scripts/slurm_src.py update            # clone on first run, then fetch + fast-forward
uv run scripts/slurm_src.py status            # which versions are cached, how fresh
uv run scripts/slurm_src.py path 26.05        # worktree path, e.g. for $EDITOR / grep
uv run scripts/slurm_src.py grep '"Def QOS"' src/sacctmgr/common.c   # where does X exist?
uv run scripts/slurm_src.py update --shallow  # CI: depth-1 clone per branch, no bare repo
```

The first `update` downloads ~400 MB; later runs fetch only new commits. Set
`SLURM_SRC_CACHE` to relocate the cache. Worktrees follow the **tip of each
maintenance branch** (so `slurm-26.05` may report `26.05.4-0pre1` between
patch releases); if a claim needs an exact release, cite the tag in prose
(`slurm-26-05-3-1`) next to the reference.

## Which versions are tracked

`[tool.slurm-parity]` in `pyproject.toml` is the single source of truth:

```toml
[tool.slurm-parity]
primary  = "26.05"
versions = ["master", "26.05", "25.11", "25.05", "24.11"]
```

- **primary** — the release whose behaviour the emulator claims by default
  and the version `SLURM_EMULATOR_SLURM_VERSION` falls back to.
- **versions** — every release a reference is verified against unless it
  says otherwise. The list mirrors SchedMD's support window (a release plus
  the three before it), and `master` (the next release's pre-release branch)
  is included as an early warning: a reference that stops resolving there
  means upstream is changing something the emulator relies on. When 26.11 is
  released, replace `master` with `26.11`, promote it (or keep 26.05) as
  primary, and drop `24.11`. The slurmrestd plane emulates `data_parser/v0.0.45`,
  the current version in 26.05; its references carry `@26.05+`.

`emulator/slurm_version.py` carries the same primary as a constant so the
runtime does not have to parse `pyproject.toml`; `tests/test_slurm_parity_tooling.py`
fails if the two drift apart.

## Reference grammar

```
slurm://<repo-relative path>[#<anchor>][@<versions>]
```

| Part | Meaning | Examples |
|------|---------|----------|
| path | file inside the Slurm tree; must exist in every applicable version | `src/sacctmgr/common.c`, `slurm/slurmdb.h` |
| anchor | C identifier that must occur in the file; dotted for struct members; `"quoted"` for a literal string | `#_get_print_field`, `#slurmdb_qos_rec_t.grace_time`, `#"Def QOS"` |
| versions | which tracked versions the claim applies to; default **all** | `@master`, `@26.05,25.11`, `@25.11+` (25.11 and newer, incl. master) |

Examples as they appear in code:

```python
# Mirrors sacctmgr's global exit_code (slurm://src/sacctmgr/sacctmgr.c#exit_code):
# "Nothing modified" returns SLURM_ERROR from
# slurm://src/sacctmgr/account_functions.c#sacctmgr_modify_account but only
# _modify_it() (slurm://src/sacctmgr/sacctmgr.c#_modify_it) sets exit_code=1.

"""ASSOC_SHORT (slurm://src/plugins/data_parser/v0.0.45/parsers.c#ASSOC_SHORT@26.05+)."""

# http_status_from_error() only exists from 25.11:
# slurm://src/slurmrestd/operations.c#http_status_from_error@25.11+
```

Rules of thumb:

- Anchor on the **function or table** that implements the behaviour, not on
  the line you happened to read. A whole lookup table needs one reference at
  its head, not one per row.
- Prefer identifiers to string literals; use a literal only when the
  behaviour *is* the string (an error message, a header label).
- A reference with no `@` is a claim that all tracked versions behave the same.
  If the checker reports the anchor missing in an older version, that is
  information: narrow the reference (`@25.11+`) and, if the emulator's
  behaviour must differ, branch on `emulator.slurm_version`.
- Never write `file.c:123`, never write a local filesystem path. The checker
  rejects both.

## Checking references

```bash
uv run scripts/check_slurm_refs.py             # every slurm:// ref in the repo
uv run scripts/check_slurm_refs.py --summary   # plus per-version coverage counts
uv run scripts/check_slurm_refs.py --strict    # fail if a tracked version is not cached (CI)
uv run scripts/check_slurm_refs.py emulator/commands/sacctmgr.py   # just these files
```

- **pre-commit** runs the checker on staged `.py`/`.md` files. Without a local
  cache it verifies the grammar and rejects legacy line refs, then prints a
  note about the versions it could not check — it does not block a commit on
  a missing cache.
- **CI** (`Check Slurm source references` in `.gitlab-ci.yml`) shallow-clones
  every tracked branch (cached between pipelines) and runs `--strict`.

## Launching as a specific release

The emulator targets one Slurm version at a time, selected by
`SLURM_EMULATOR_SLURM_VERSION` (default: the primary; Helm value
`slurmVersion`). `emulator/slurm_version.py` keeps one `SlurmRelease` per
tracked version:

| target | `meta.slurm.release` | slurmrestd prefix |
|--------|----------------------|-------------------|
| `24.11` | `24.11.7` | `/slurm/v0.0.42/`, `/slurmdb/v0.0.42/` |
| `25.05` | `25.05.8` | `v0.0.43` |
| `25.11` | `25.11.7` | `v0.0.44` |
| `26.05` | `26.05.3` | `v0.0.45` |
| `master` | `26.11.0` | `v0.0.46` |

Only that release's prefix is served — any other data_parser version is an
unknown URL, exactly as on a real slurmrestd that has one parser plugin loaded.
The `release` column is the newest tag on the maintenance branch; bump it when
a patch release ships (`slurm_src.py update` shows the branch's `META` version).

The response shapes that differ across the window are gated with
`at_least()` and documented with `@version` references in
`tests/test_slurmrestd_dialects.py`, which runs every dialect in one go.
Everything else the emulator serves is a subset that is identical across the
tracked versions — the parser-table diff that established this lives in the
history of that test's docstring references.

## Version-dependent behaviour

Use `at_least()` where real Slurm's behaviour genuinely differs between
tracked versions:

```python
from emulator.slurm_version import at_least

if at_least("25.11"):
    ...  # slurm://src/slurmrestd/operations.c#http_status_from_error@25.11+
```

Tests must not hard-code a data_parser version: the REST test modules use
`V = current().api_version` for URL prefixes, and CI runs the whole suite once
per tracked version (`Run tests as Slurm version`). A test whose expectation
holds only for some versions declares them, and is skipped when the active
target is not among them:

```python
@pytest.mark.slurm_version("25.11+")
def test_rest_error_status_mapping(restd): ...

@pytest.mark.slurm_version("24.11", "25.05")
def test_rest_error_status_old(restd): ...
```

The `slurm_target_version` fixture returns the active target for tests that need to
branch on it. Keep version branches rare: most of `sacctmgr`/`sacct`/`sshare`
has been identical across the tracked window (the reference summary shows
roughly 140 of 160 references verified unchanged back to 24.11).

## Workflow for a parity change

1. `uv run scripts/slurm_src.py update` — make sure the cache is current.
2. Read the real implementation in the primary version's worktree; use
   `slurm_src.py grep` to see whether it differs in the other versions.
3. Implement the emulator change and cite the source with `slurm://` refs in
   the docstring or comment next to the behaviour, tagging `@versions` where
   it does not hold everywhere.
4. Add a test whose docstring carries the same reference; mark it with
   `@pytest.mark.slurm_version(...)` if the expectation is version-specific.
5. Document any *intentional* deviation from real Slurm in the module
   docstring (existing examples: `-M` tolerance in `sacctmgr`, no interactive
   commit prompt, `sacct -X`/`-a` no-ops).
6. `uv run scripts/check_slurm_refs.py --summary` and `uv run pytest`.
7. Cite the same reference in the changelog entry.
