# ruff: noqa: PTH123
"""log.py NAME N CMD...: run CMD, log {argv: [NAME, *CMD[N:]], output, exit} to $PARITY_RAW_LOG."""

import json
import os
import subprocess
import sys

name, n, cmd = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
proc = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
log = os.environ.get("PARITY_RAW_LOG")
if log:
    with open(log, "a") as f:
        f.write(
            json.dumps({"argv": [name, *cmd[n:]], "output": proc.stdout, "exit": proc.returncode})
            + "\n"
        )
sys.stdout.write(proc.stdout)
sys.exit(proc.returncode)
