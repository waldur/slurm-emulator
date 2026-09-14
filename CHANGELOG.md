# Changelog

All notable changes to slurm-emulator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Fixed
- Make `sacctmgr modify user … set DefaultAccount=` re-point the user's default account with real sacctmgr's output: every user the condition matches is listed under ` Modified users...`, also when the default does not change (`  Nothing modified`, exit 1, only when no user matches), and the new account must be associated with each user on every cluster — or on the `cluster=` ones — else each missing user/cluster pair is listed and the command exits 1 (`slurm://src/sacctmgr/user_functions.c#sacctmgr_modify_user`, `slurm://src/plugins/accounting_storage/mysql/as_mysql_user.c#as_mysql_modify_users`, `slurm://src/sacctmgr/user_functions.c#_check_and_set_cluster_list`, `slurm://src/sacctmgr/user_functions.c#_check_default_assocs`) — previously a no-op, which left no way to move a default before removing that association
- Parse the `where` clause of `sacctmgr show user` and `modify user` like real sacctmgr: a bare word is a user name, `Names`/`Users` match from one letter (`n=`, `u=`), `DefaultAccount` from eight (singular only), and an unrecognised condition prints ` Unknown condition` and exits 1 (`slurm://src/sacctmgr/user_functions.c#_set_cond`) — previously `show user alice` listed every user
- Parse the `where` clause of `sacctmgr list/show association` with real keyword prefixes — `Users`/`Clusters` from one letter, `Accounts` from two (`Acct` from four), `Partitions` from three — so `list` and `show` agree, an unrecognised condition exits 1, and the site agent's `accounts=` filter no longer lists every association (`slurm://src/sacctmgr/association_functions.c#_set_cond`, `slurm://src/sacctmgr/association_functions.c#sacctmgr_set_assoc_cond`)

## [0.9.6] - 2026-09-14

### Added
- Add opt-in NSS identity mode (`SLURM_EMULATOR_NSS=1`): user names resolve through the OS name service switch (sssd → LDAP in the Docker image, `examples/nss/`), so `id` prints the coreutils shape FireCREST's `/status/userinfo` parses, `POST /job/submit` / `sbatch` record real uid/gid/group (`user_id`/`group_id`/`group_name` per `slurm://src/plugins/data_parser/v0.0.45/parsers.c#JOB_INFO@26.05+`, `scontrol show job` `UserId`/`GroupId`, sacct `UID`/`GID`/`Group` per `slurm://src/sacct/sacct.c#fields`) and refuse unknown users with `ESLURM_USER_ID_UNKNOWN` (`slurm://src/plugins/data_parser/v0.0.45/parsers.c#USER_ID@26.05+`); `sacctmgr add user` stops on a missing uid unless `-i` (`slurm://src/sacctmgr/user_functions.c#_check_uid`, `slurm://src/sacctmgr/common.c#commit_check`); `sreport` fills `Proper Name` from gecos
- Run SSH-plane shell commands as the resolved login user (uid/gid/groups) when NSS mode is on and the emulator is root, and strip a leading coreutils `timeout N` wrapper before matching emulated Slurm binaries (FireCREST sends `timeout 10 id`)
- Add `SLURM_EMULATOR_ACCOUNTING_ENFORCE` (`AccountingStorageEnforce=associations`, the default; chart `accountingEnforce`, `none` opts out): `POST /job/submit` and `sbatch` refuse a user with no association for the requested or default account with `ESLURM_INVALID_ACCOUNT` (`slurm://src/slurmctld/job_mgr.c#_job_create`, `slurm://src/common/assoc_mgr.c#assoc_mgr_fill_in_assoc`); unset/`none` keeps the legacy fallback
- Add `SLURM_EMULATOR_CLUSTER_NAME` (slurm.conf `ClusterName`, chart `clusterName`) so associations, jobs and `/conf` are filed under the deployment's cluster name
- Refuse removing a user's default association while others remain (`ESLURM_NO_REMOVE_DEFAULT_ACCOUNT`, `slurm://src/plugins/accounting_storage/mysql/as_mysql_assoc.c#as_mysql_remove_assocs`), make the first association the default, and delete the user with its last association, as slurmdbd/sacctmgr do
- Seed the `root` user and its `root` association like slurmdbd does (`slurm://src/plugins/accounting_storage/mysql/as_mysql_cluster.c#as_mysql_add_clusters`), so submissions without a user fall into `root` under enforcement
- Add `nss.enabled` / `nss.sssdConfSecret` to the Helm chart and install sssd + libnss-sss in the Docker image (started by the entrypoint only when the mode is on)

### Changed
- **Behaviour change:** job submission (`POST /job/submit`, `sbatch`) now enforces associations by default (`AccountingStorageEnforce=associations`): a user with no association for the requested or default account gets `ESLURM_INVALID_ACCOUNT` (HTTP 422) instead of silently landing in `root`. Set `SLURM_EMULATOR_ACCOUNTING_ENFORCE=none` (chart `accountingEnforce: none`) to restore the previous permissive behaviour. Deployments with a site agent `cluster_name` other than `default` must also set `SLURM_EMULATOR_CLUSTER_NAME`
- NSS identities (uid/gid/groups) are re-resolved after `SLURM_EMULATOR_NSS_CACHE_TTL` seconds (60) instead of being cached for the process lifetime

## [0.9.5] - 2026-09-03

### Fixed
- Enforce slurmdbd's check that the default QoS must be in the association's QoS list when swapping QoS via `sacctmgr modify`

## [0.9.4] - 2026-08-30

### Added
- Add `sreport cluster AccountUtilizationByUser` emulation with account aggregates and `-T energy` TRES for portal energy reporting
- Add energy TRES power model (`SLURM_EMULATOR_NODE_POWER_W`, `SLURM_EMULATOR_PARTITION_POWER_W`, `SLURM_EMULATOR_GPU_POWER_W`)
- Add `SLURM_EMULATOR_SLURM_VERSION` to launch the emulator as any tracked Slurm release (24.11, 25.05, 25.11, 26.05, master) with matching slurmrestd API version prefix and response shapes
- Add `scripts/slurm_src.py` to manage a local cache of SchedMD Slurm source worktrees per tracked version
- Add `scripts/check_slurm_refs.py` to verify `slurm://` source references in pre-commit and CI

### Changed
- Trace emulator command, slurmrestd and database behaviour to real Slurm source via `slurm://<path>#<symbol>` references
- Run the test suite once per tracked Slurm version in CI

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
