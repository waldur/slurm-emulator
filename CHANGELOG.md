# Changelog

All notable changes to slurm-emulator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

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
