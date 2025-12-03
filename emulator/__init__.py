"""SLURM Emulator - Time manipulation and usage simulation for testing periodic limits."""

import importlib.metadata

try:
    __version__ = importlib.metadata.version("slurm-emulator")
except importlib.metadata.PackageNotFoundError:
    # Fallback for development/testing when package isn't installed
    __version__ = "0.1.1-dev"
