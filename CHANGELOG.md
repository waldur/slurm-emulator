# Changelog

All notable changes to slurm-emulator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Fixed
- `sshare GrpTRESRaw` renders the `energy` TRES as joules/60 (it was hours×60,
  3600× too large) — `usage_tres_raw[i] / 60` in slurm://src/sshare/process.c#process.
- `sacctmgr show tres` and `GET /slurmdb/v0.0.4x/tres/` derive TRES ids from one
  table (`emulator/core/tres.py`: static 1–8, dynamic from 1001); `node` is now
  listed, so `sreport -T node` works and `-T node -t Percent` fails like real
  sreport (slurm://src/sreport/sreport.c#_build_tres_list).
- Usage records fold the account name like accounts/associations do, so usage
  injected under `MyProj` shows up in `sreport`/`sacct`/`sshare`.
- `sacct Partition` and slurmrestd `partition` render the record's partition.
- `POST /api/submit-report` creates one usage record per user and report (all
  TRES keys summed into node-hours), so an explicit `energy` lands on that
  record regardless of key order and the power model is applied once; a
  zero-node-hour (energy-only) record renders `cpu=0,…` rather than a full node.
- Scheduler-completed jobs carry the standard-node TRES breakdown (incl. 4 GPUs)
  so their energy matches injected usage of the same size.
- `sreport` accepts clustered short flags (`-nP`, `-nTenergy`), `-h`, empty
  `start=`/`end=` (default window, `parse_time("")` = 0) and
  `YYYY-MM-DD HH:MM` (slurm://src/common/parse_time.c#_get_time).

### Added
- `sreport cluster AccountUtilizationByUser` emulation as an aggregate usage
  source for portal energy reporting (`emulator/commands/sreport.py`,
  `uv run sreport`, SSH dispatch): `start=`/`end=` on the simulated clock,
  `accounts=`/`users=`/`cluster=`/`format=`, `-T`/`--tres=` incl. `energy`,
  `-t Seconds|Minutes|Hours|Percent|…`, `-p`/`-P`/`-n`; account total, per-user
  and sub-account rows exactly as real sreport prints them
  (slurm://src/sreport/cluster_reports.c#cluster_account_by_user,
  slurm://src/sreport/common.c#sreport_get_time_str,
  slurm://src/common/slurmdb_defs.c#slurmdb_report_set_start_end_time).
  As in real Slurm there is no `-t Joules` (energy is what `-t Seconds`
  prints) and an unknown TRES list is `sreport: fatal: No valid TRES given`.
- `energy` TRES (joules, slurm://src/common/slurmdb_defs.h#TRES_ENERGY) on
  every usage record from a per-node/per-partition/per-GPU power model
  (`emulator/core/energy.py`: `SLURM_EMULATOR_NODE_POWER_W`,
  `SLURM_EMULATOR_PARTITION_POWER_W`, `SLURM_EMULATOR_GPU_POWER_W`), for
  injected usage and for jobs completed by the scheduler; `energy` listed in
  `sacctmgr show tres`; `UsageRecord.partition`.
- `POST /api/submit-report` takes `energy` (aggregate or per user) and
  `partition` to seed exact monthly figures; `regular_access_energy` scenario.
- `sacct ConsumedEnergy` / `ConsumedEnergyRaw`
  (slurm://src/sacct/print.c#PRINT_CONSUMED_ENERGY_RAW,
  slurm://src/common/slurm_protocol_api.c#convert_num_unit2). The ReqTRES
  string is unchanged, so the site-agent invocation output is byte-identical.
- The emulator can be launched as any tracked Slurm release via
  `SLURM_EMULATOR_SLURM_VERSION` (`24.11`, `25.05`, `25.11`, `26.05` default,
  `master`; Helm value `slurmVersion`). The slurmrestd plane then serves that
  release's data_parser prefix (`/slurm/v0.0.42/` … `/slurm/v0.0.46/`), reports
  its release in `meta.slurm` and `/conf`, and renders the tables that changed
  across releases in the matching dialect: `CONTROLLER_PING` (`pinged`/`mode`
  through v0.0.44, `status` from v0.0.45), `SLURMDBD_PING` (`status` from
  v0.0.45), `PARTITION_INFO` memory-per-CPU limits (dropped on master).
  `tests/test_slurmrestd_dialects.py` covers every dialect; CI runs the full
  suite once per tracked version.
- Slurm source-parity tooling: `scripts/slurm_src.py` keeps a local cache of
  https://github.com/SchedMD/slurm (bare clone + one worktree per tracked
  version, `[tool.slurm-parity]` in `pyproject.toml`: 26.05 primary, plus
  25.11, 25.05, 24.11 and `master`), and `scripts/check_slurm_refs.py` verifies
  every `slurm://path#symbol[@versions]` reference against each version, in
  pre-commit and CI. See `docs/slurm-parity.md`.
- `emulator.slurm_version` (`SLURM_EMULATOR_SLURM_VERSION`) and the
  `slurm_version` pytest marker for version-dependent parity behaviour.

### Changed
- slurmrestd emulation now claims Slurm 26.05 / data_parser `v0.0.45` (the current
  stable release) instead of the unreleased 26.11 / `v0.0.46`: URL prefixes are
  `/slurm/v0.0.45/` and `/slurmdb/v0.0.45/`, `meta.slurm.release` is `26.05.3`,
  and the FireCREST examples set `api_version: 0.0.45`. The field tables the
  emulator serves are identical between the two parser versions
  (slurm://src/plugins/data_parser/v0.0.45/parsers.c@26.05+).
- All real-Slurm source references in code, tests and docs migrated from
  `file.c:<line>` to symbol-anchored `slurm://` references (line numbers had
  already drifted); per-row line comments on the `sacctmgr`/`sacct` field
  tables replaced by one table-level reference.

## [0.9.3] - 2026-08-28

### Added
- Add QoS GrpTRESMins and RawUsage modelling to sacctmgr emulation
- Add a landing page published alongside the Helm chart index
- Add documentation for running the FireCREST scenario on Kubernetes

### Changed
- Wire the dependency licence gate into CI

## [0.9.2] - 2026-08-20

### Added
- Add Helm chart for deploying the emulator to Kubernetes

## [0.9.1] - 2026-07-23

### Added
- Emulate account GrpSubmitJobs as an orthogonal pause lever

### Fixed
- Fix documentation for CLI command syntax, CI provider, and stale references

## [0.9.0] - 2026-07-22

### Added
- Emulate SLURM QoS and partition gating to support enforcement test-driven development

## [0.8.0] - 2026-07-01

### Added
- Add lightweight HTMX web dashboard for status and full control of the emulator
- Add FireCREST v2 conformance harness and one-command firecrest-ui stack

### Changed
- Fold account names to lower case to match real Slurm
- Isolate FireCREST integration under `examples/firecrest` and add a UI guide

## [0.7.2] - 2026-06-25

### Fixed
- Strip quotes from sacctmgr `parent=` value to match real Slurm

## [0.7.1] - 2026-06-12

### Fixed
- Match real slurmrestd association_condition response shapes and exit codes

## [0.7.0] - 2026-06-11

### Added
- Add slurmrestd REST API emulation (Slurm 26.11, v0.0.46) on port 6820 with `/slurmdb` CRUD and `/slurm` controller read endpoints, JWT-style authentication, and shared state with CLI commands

## [0.6.0] - 2026-06-11

### Changed
- Update `sacctmgr` list/show output to match real SLURM 26.11: fixed-width columns with dashed header by default, with `-p`/`--parsable`, `-P`/`--parsable2`, and `-n`/`--noheader` flags for parsable output
- Update `sacct` to match real SLURM 26.11: standard default field set with header, short flags (`-S/-E/-A/-u/-o/-X/-a/-M`), numeric job IDs, `[DD-]HH:MM:SS` elapsed times, standard TRES strings, and a midnight-to-now default time window
- Update `sacctmgr` to print "Data has not changed since time specified" with exit 0 when re-adding an existing account, matching `SLURM_NO_CHANGE_IN_DATA`

### Fixed
- Fix `sacctmgr` "Nothing modified" to exit 0 on stdout, with genuine errors going to stderr with exit 1
- Fix `sacctmgr add account` with a missing cluster to exit 1
- Fix `sacct` to exit 1 on invalid time specs and unknown format fields
- Fix `sshare -M` with an unknown cluster to print the real database error to stderr and exit 1, while proceeding with any valid clusters

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
