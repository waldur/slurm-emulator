#!/usr/bin/env bash
# End-to-end smoke: run FireCREST v2 with the slurm-emulator standing in for
# the real Slurm cluster (slurmrestd + SSH), reusing FireCREST's own
# Keycloak/OpenFGA/MinIO.
#
# Prereqs: docker + docker compose, git, curl.
#
# Env:
#   FIRECREST_REPO   git URL (default: https://github.com/eth-cscs/firecrest-v2)
#   FIRECREST_REF    branch/tag/sha (default: master)
#   WORKDIR          checkout dir (default: a temp dir)
#   SLURM_SERVICE    FireCREST's Slurm service name in its compose (default: slurm)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
FIRECREST_REPO="${FIRECREST_REPO:-https://github.com/eth-cscs/firecrest-v2}"
FIRECREST_REF="${FIRECREST_REF:-master}"
WORKDIR="${WORKDIR:-$(mktemp -d)}"
SLURM_SERVICE="${SLURM_SERVICE:-slurm}"
OVERLAY="${SCRIPT_DIR}/docker-compose.override.yml"

echo ">> Building slurm-emulator image (opennode/slurm-emulator:latest)"
docker build -t opennode/slurm-emulator:latest "${REPO_ROOT}"

echo ">> Cloning FireCREST (${FIRECREST_REF}) into ${WORKDIR}"
if [ ! -d "${WORKDIR}/.git" ]; then
    git clone --depth 1 --branch "${FIRECREST_REF}" "${FIRECREST_REPO}" "${WORKDIR}"
fi
cd "${WORKDIR}"

if ! grep -qE "^\s*${SLURM_SERVICE}:" docker-compose.yml; then
    echo "!! Service '${SLURM_SERVICE}' not found in FireCREST docker-compose.yml." >&2
    echo "   Set SLURM_SERVICE to the correct Slurm service name and re-run." >&2
    exit 2
fi

echo ">> Starting stack with the emulator overlay"
docker compose -f docker-compose.yml -f "${OVERLAY}" up -d

cleanup() { docker compose -f docker-compose.yml -f "${OVERLAY}" down -v || true; }
trap cleanup EXIT

echo ">> Waiting for the emulator slurmrestd to answer"
for _ in $(seq 1 30); do
    if curl -fsS -H 'X-SLURM-USER-TOKEN: x' \
        http://localhost:6820/slurm/v0.0.46/ping/ >/dev/null 2>&1; then
        echo "   slurmrestd is up"
        break
    fi
    sleep 2
done

echo ">> Emulator scheduler-plane smoke (direct)"
curl -fsS -H 'X-SLURM-USER-TOKEN: x' -H 'Content-Type: application/json' \
    -X POST http://localhost:6820/slurm/v0.0.46/job/submit \
    -d '{"job":{"name":"e2e","partition":"compute","current_working_directory":"/home/root","script":"#!/bin/bash\necho hi"}}'
echo

echo ">> FireCREST is up. Point its config at examples/firecrest/e2e/f7t-api-config.emulator.yaml"
echo "   and exercise its public API (compute + filesystem) against the emulator."
echo "   Stack will be torn down on exit."
