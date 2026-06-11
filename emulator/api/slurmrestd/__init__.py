"""slurmrestd (SLURM REST API) emulator.

Emulates the Slurm 26.11 REST API daemon: ``/slurm/v0.0.46/...``
(slurmctld) and ``/slurmdb/v0.0.46/...`` (slurmdbd) endpoints with the
real response envelope, error numbers, and auth header handling. State
is shared with the CLI command emulators and the Waldur control API
through the JSON state files (see ``state.py``).
"""
