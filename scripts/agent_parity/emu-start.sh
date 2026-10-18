#!/bin/bash
# Fresh emulator: wipe state, (re)start the slurmrestd emulator on $EMU_PORT (26821).
HERE=$(cd "$(dirname "$0")" && pwd); REPO=$(cd "$HERE/../.." && pwd)
EMU_PORT=${EMU_PORT:-26821}; export STATE_DIR=${STATE_DIR:-$HERE/work/emu-state}
source "$HERE/emu.env"
pkill -f "create_app --factory --host 127.0.0.1 --port $EMU_PORT" 2>/dev/null; sleep 1
rm -rf "$STATE_DIR"; mkdir -p "$STATE_DIR"
cd "$REPO" && nohup .venv/bin/python -m uvicorn emulator.api.slurmrestd.app:create_app --factory \
  --host 127.0.0.1 --port "$EMU_PORT" > "$HERE/work/emu-server.log" 2>&1 &
for i in $(seq 1 30); do curl -s -o /dev/null -H 'X-SLURM-USER-TOKEN: x' "http://127.0.0.1:$EMU_PORT/slurm/v0.0.45/ping/" && break; sleep 0.5; done
echo "emulator up on $EMU_PORT"
