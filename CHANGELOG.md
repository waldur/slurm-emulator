# Changelog

All notable changes to slurm-emulator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Changed
- **BREAKING**: `sacctmgr` list/show output now matches real SLURM 26.11 —
  fixed-width columns with a dashed header by default; pass `-p`/`--parsable`
  (trailing `|`), `-P`/`--parsable2` (no trailing `|`), and `-n`/`--noheader`
  for the parsable shapes previously emitted unconditionally. Default field
  sets, headers, and widths mirror `sacctmgr/common.c`.
- **BREAKING**: `sacct` now matches real SLURM 26.11 — default format
  `JobID,JobName,Partition,Account,AllocCPUS,State,ExitCode` with header and
  dashes, `-p`/`-P`/`-n` and short flags (`-S/-E/-A/-u/-o/-X/-a/-M`)
  supported, numeric job IDs, `[DD-]HH:MM:SS` elapsed times, standard TRES
  strings (`cpu=...,mem=...G,node=1,billing=...`; the internal `node-hours`
  key is no longer exposed), and a Midnight→Now default time window.
- `sacctmgr` re-adding an existing account prints
  ` Data has not changed since time specified` (exit 0), matching
  `SLURM_NO_CHANGE_IN_DATA`.

### Fixed
- `sacctmgr` "Nothing modified" now exits 0 (stdout), matching real sacctmgr;
  genuine errors print ` error: ...` to stderr and exit 1.
- `sacctmgr add account cluster=<missing>` now exits 1.
- `sacct` exits 1 on invalid time specs and unknown format fields.
- `sshare -M <unknown>` prints the real per-name database error and
  `fatal: Could not get cluster information` to stderr with exit 1; a mix of
  valid and invalid clusters proceeds with the valid ones.

## [0.5.3] - 2026-06-10

### Fixed
- Keep exit code 0 when re-adding an existing account

## [0.5.2] - 2026-06-10

### Changed
- Model account parent hierarchy to match real Slurm

## [0.5.1] - 2026-06-09

### Fixed
- `sacctmgr list cluster`: honor `format=` and match real SLURM output

## [0.5.0] - 2026-05-20

### Added
- Add sshare command emulation

## [0.4.0] - 2026-05-11

### Added
- Parse `Partitions=p1,p2` (and single-form `Partition=p1`) on `sacctmgr add user`. One `Association` row is created per partition, matching real Slurm's `_add_assoc_cond_partition` in `as_mysql_assoc.c`.
- Silently accept `Share=parent` (and other unmodeled fairshare / limit attributes) on `sacctmgr add user`, matching real sacctmgr.
- Support `partition` format field in `sacctmgr list associations` and `sacctmgr show association format=…` (rendered per partition row).
- `SlurmDatabase.list_user_associations(user, account, cluster=…)` and `delete_user_associations(...)` helpers for per-partition row enumeration / wholesale deletion.

### Changed
- `Association` gains a single `partition: Optional[str]` field; association key becomes `user:account:cluster:partition`. State loader migrates older state files (including the prior interim shape with `partitions: list[str]` + `default_partition`) into one row per partition.
- `sacctmgr remove user where name=X and account=Y` now deletes every partition-scoped row for that pair, matching real sacctmgr.

### Fixed
- Real-Slurm parity: `sacctmgr add user … DefaultPartition=X` now returns `Unknown option: DefaultPartition=X` and does not persist the association — `DefaultPartition` is not a real `sacctmgr add user` attribute (neither `user_functions.c` nor `sacctmgr_set_assoc_rec` accepts it).
- Real-Slurm parity: `format=partitions`, `format=defaultpartition`, and `format=def_partition` now return `Unknown field 'X'` from `list associations` and `show association` — real Slurm only recognises `Partition` (`common.c` minimum prefix `Part`).
- Fix changelog insertion formatting and set 0.2.0 release date.

## [0.3.0] - 2026-04-06

### Added
- Add QOS management support (add, modify, show) in sacctmgr

### Fixed
- Fix bash 3.2 heredoc parsing error in changelog generation
- Fix mypy type error in sacctmgr _show_qos method

## [0.2.0] - 2026-03-14

### Added
- Add per-command flag validation and multi-TRES parsing
- Add multi-cluster support matching real SLURM behavior
- Add `ClusterClassification` enum with validation (capability, capacity, capapacity)
- Add cluster fields: id (auto-increment), rpc_version, flags, nodes, tres_str
- Add root account and association auto-creation on cluster add
- Add `cluster=` parameter support in `sacctmgr add account` and `sacctmgr add user`
- Add running/pending job check before cluster deletion
- Add cluster soft-delete (filtered from listings but preserved internally)
- Add backward-compatible state loading for 3 account key formats

### Changed
- Make accounts global entities instead of per-cluster (aligns with real SLURM)
- Restrict `-M` cluster flag to `sacct` only; `sacctmgr` uses `cluster=` in args
- Remove Cluster column from `sacctmgr list accounts` output
- Update `sacctmgr list clusters` format to include RPC and Classification columns

### Fixed
- Fix project URL in metadata

## [0.1.1] - 2025-12-03

### Added
- Extend node TRES support with node-hours tracking

### Changed
- Clean up setup instructions and documentation

## [0.0.1] - 2025-11-06

### Added
- Initial release of slurm-emulator
- Core emulator with time engine, database, and usage simulator
- CLI interface for interactive time-travel testing
- Command emulators: sacctmgr, sacct, sinfo, scancel
- Periodic limits calculator with decay and carryover logic
- QoS manager with threshold-based transitions
- REST API server for waldur-site-agent integration
- Scenario runner for complete sequence validation
- PyPI publishing via GitHub Actions CI/CD
- Test suite for core components
