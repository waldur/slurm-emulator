#!/bin/bash
# Write the command wrappers the site agent's SlurmClient runs as slurm_bin_path:
#   work/bin-real/<cmd>  -> docker exec $PARITY_CONTAINER /opt/slurm/bin/<cmd>
#   work/bin-emu/<cmd>   -> this checkout's emulator console scripts
# Every call is logged through log.py to $PARITY_RAW_LOG.
HERE=$(cd "$(dirname "$0")" && pwd); REPO=$(cd "$HERE/../.." && pwd)
NAME=${PARITY_CONTAINER:-parity-real}; EMU_PORT=${EMU_PORT:-26821}
REAL=$HERE/work/bin-real; EMU=$HERE/work/bin-emu
mkdir -p "$REAL" "$EMU"
for cmd in sacctmgr sacct scancel scontrol sinfo; do
  printf '#!/bin/bash\nexec python3 %s/log.py %s 4 docker exec %s /opt/slurm/bin/%s "$@"\n' "$HERE" "$cmd" "$NAME" "$cmd" > "$REAL/$cmd"
  printf '#!/bin/bash\nexport STATE_DIR=${STATE_DIR:-%s/work/emu-state}\nsource %s/emu.env\nexec python3 %s/log.py %s 1 %s/.venv/bin/%s "$@"\n' \
    "$HERE" "$HERE" "$HERE" "$cmd" "$REPO" "$cmd" > "$EMU/$cmd"
done
printf '#!/bin/bash\nexec python3 %s/log.py id 3 docker exec %s id "$@"\n' "$HERE" "$NAME" > "$REAL/id"
printf '#!/bin/bash\nexport STATE_DIR=${STATE_DIR:-%s/work/emu-state}\nsource %s/emu.env\nexec python3 %s/log.py id 2 %s/.venv/bin/python %s/emulator/commands/dispatcher.py id "$@"\n' \
  "$HERE" "$HERE" "$HERE" "$REPO" "$REPO" > "$EMU/id"
cat > "$REAL/submit" <<EOS
#!/bin/bash
docker exec $NAME su - "\$1" -c "/opt/slurm/bin/sbatch --parsable -A \$2 -p \$3 -t 30 --wrap 'sleep \$4'"
EOS
cat > "$REAL/jobstate" <<EOS
#!/bin/bash
s=\$(docker exec $NAME squeue -h -j "\$1" -o %T 2>/dev/null)
[ -n "\$s" ] && { echo "\$s"; exit 0; }
docker exec $NAME sacct -n -P -X -j "\$1" -o State | head -1
EOS
cat > "$EMU/submit" <<EOS
#!/bin/bash
curl -s -X POST http://127.0.0.1:$EMU_PORT/slurm/v0.0.45/job/submit -H 'X-SLURM-USER-TOKEN: x' -H "X-SLURM-USER-NAME: \$1" -H 'Content-Type: application/json' \\
  -d "{\\"job\\":{\\"name\\":\\"wrap\\",\\"account\\":\\"\$2\\",\\"partition\\":\\"\$3\\",\\"time_limit\\":30,\\"current_working_directory\\":\\"/tmp\\",\\"environment\\":[\\"SLURM_EMULATOR_RUN_SECONDS=\$4\\"],\\"script\\":\\"#!/bin/bash\\\\nsleep \$4\\"}}" \\
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["job_id"]) if not d["errors"] else sys.exit(str(d["errors"]))'
EOS
cat > "$EMU/jobstate" <<EOS
#!/bin/bash
curl -s http://127.0.0.1:$EMU_PORT/slurm/v0.0.45/job/\$1 -H 'X-SLURM-USER-TOKEN: x' | python3 -c 'import json,sys; print(json.load(sys.stdin)["jobs"][0]["job_state"][0])'
EOS
chmod +x "$REAL"/* "$EMU"/*
echo "wrappers in $REAL and $EMU"
