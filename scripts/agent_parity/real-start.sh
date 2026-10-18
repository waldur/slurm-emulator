#!/bin/bash
# Fresh real cluster from the real_slurmrestd image: empty accounting DB and
# controller state. The cluster config is scripts/agent_parity/cluster/.
HERE=$(cd "$(dirname "$0")" && pwd)
NAME=${PARITY_CONTAINER:-parity-real}; PORT=${REAL_PORT:-26820}; IMAGE=${IMAGE:-real-slurm-acct:26.05}
docker rm -f "$NAME" >/dev/null 2>&1
docker create --name "$NAME" --hostname localhost -p "127.0.0.1:$PORT:6820" "$IMAGE" /run-config/start.sh >/dev/null
docker cp "$HERE/cluster" "$NAME:/run-config"
docker start "$NAME" >/dev/null
for i in $(seq 1 90); do docker logs "$NAME" 2>&1 | grep -q READY && break; sleep 2; done
for i in $(seq 1 30); do docker exec "$NAME" sinfo -h -o %t | grep -qv idle || break; sleep 2; done
docker exec "$NAME" sinfo -h -o '%P %t %D'
