"""Lightweight HTMX + Jinja2 web dashboard for the SLURM Emulator.

Mounts onto the existing ``EmulatorServer`` FastAPI app (port 8080) so the UI
shares the same in-memory managers and JSON state files as the CLI and JSON API.
"""

from emulator.api.ui.routes import mount_ui

__all__ = ["mount_ui"]
