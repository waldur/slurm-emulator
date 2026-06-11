#!/bin/bash
# shellcheck shell=bash  (wait -n requires bash, present in python:slim)
# Run both emulator APIs in one container:
#   8080 — Waldur control API   (emulator.api.emulator_server)
#   6820 — slurmrestd emulation (emulator.api.slurmrestd.app)
# They share state through the JSON files in /tmp.
set -eu

python3 -m uvicorn emulator.api.emulator_server:app --host 0.0.0.0 --port 8080 &
python3 -m uvicorn emulator.api.slurmrestd.app:app --host 0.0.0.0 --port 6820 &

# Exit as soon as either server dies so the container restarts cleanly.
wait -n
