#!/bin/bash
# shellcheck shell=bash  (wait -n requires bash, present in python:slim)
# Run the emulator APIs in one container:
#   8080 — Waldur control API   (emulator.api.emulator_server)
#   6820 — slurmrestd emulation (emulator.api.slurmrestd.app)
#   2222 — SSH filesystem plane (emulator.api.ssh.server) — for the FireCREST
#          e2e setup, enabled with SLURM_EMULATOR_ENABLE_SSH=1
# They share state through the JSON files in /tmp.
set -eu

python3 -m uvicorn emulator.api.emulator_server:app --host 0.0.0.0 --port 8080 &
python3 -m uvicorn emulator.api.slurmrestd.app:app --host 0.0.0.0 --port 6820 &

if [ "${SLURM_EMULATOR_ENABLE_SSH:-0}" = "1" ]; then
    python3 -m emulator.api.ssh.server &
fi

# Exit as soon as any server dies so the container restarts cleanly.
wait -n
