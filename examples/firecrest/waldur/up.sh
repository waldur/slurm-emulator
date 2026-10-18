#!/usr/bin/env bash
# Bring up Keycloak + FireCREST v2 + the slurm-emulator for the Waldur HPC
# workspace, and seed the cluster's accounts and associations. Then the
# per-job monitoring stack: Kafka, TimescaleDB, slurm-monitor (listener + REST
# API) and `sonar`, which publishes synthetic Sonar telemetry for the
# emulated nodes (backfilling the finished jobs of the last
# SONAR_BACKFILL_DAYS once, then sampling every minute).
#
# Env:
#   FIRECREST_V2_DIR  firecrest-v2 checkout (default: ../ui/.firecrest-v2, cloned)
#   FIRECREST_V2_REF  ref to clone (default: master)
#   PORT_BASE         first of five consecutive host ports; KC = base, FireCREST
#                     = base+1, slurmrestd = base+2, SSH = base+3,
#                     slurm-monitor REST = base+4
#                     (default: 8080/8000/6820/2222/12000 when unset)
#   BIND_ADDRESS      host address the ports listen on (default 127.0.0.1;
#                     0.0.0.0 to reach the stack from other machines)
#   KC_PUBLIC_URL     Keycloak's URL as browsers reach it; its login pages and
#                     token issuer carry it (default http://localhost:$KC_PORT/auth)
#   WALDUR_PUBLIC_URL homeport's URL when it is not localhost (e.g. behind a
#                     reverse proxy); added to the waldur client's redirect URIs
#   SONAR_CLUSTER     cluster name in the Sonar topics and slurm-monitor
#                     (default hpc-demo)
#   SONAR_BACKFILL_DAYS  finished jobs that ended this many days back get
#                     telemetry on the first run (default 14; slurm-monitor
#                     queries span at most 14 days)
#
# Optional: live job traffic. submit_workload.py submits a mixed workload
# (GROMACS on gpu, WRF on batch incl. failures/timeouts/a cancel, QC on debug)
# through Waldur's REST API as alice and bob -- Waldur -> FireCREST -> emulator,
# so jobs queue, run in wall-clock time and get sampled live by `sonar`.
# Resumable (state in --log-dir); see its --help:
#   uv run submit_workload.py --waldur-url http://localhost:8000 --duration 2h
#
# Re-run the telemetry backfill (e.g. after wiping the monitor-db volume):
#   docker compose stop sonar && docker compose run --rm sonar slurm-sonar-emulator --reset --once
#   docker compose up -d sonar
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

export FIRECREST_V2_DIR="${FIRECREST_V2_DIR:-$(cd ../ui && pwd)/.firecrest-v2}"
FIRECREST_V2_REF="${FIRECREST_V2_REF:-master}"
if [ -n "${PORT_BASE:-}" ]; then
    export KC_PORT=$((PORT_BASE)) FIRECREST_PORT=$((PORT_BASE + 1))
    export SLURM_REST_PORT=$((PORT_BASE + 2)) SLURM_SSH_PORT=$((PORT_BASE + 3))
    export SLURM_MONITOR_PORT=$((PORT_BASE + 4))
fi
KC_PORT="${KC_PORT:-8080}"
FIRECREST_PORT="${FIRECREST_PORT:-8000}"
SLURM_REST_PORT="${SLURM_REST_PORT:-6820}"
SLURM_MONITOR_PORT="${SLURM_MONITOR_PORT:-12000}"
KC_PUBLIC_URL="${KC_PUBLIC_URL:-http://localhost:${KC_PORT}/auth}"

if [ ! -d "${FIRECREST_V2_DIR}/.git" ]; then
    echo ">> Cloning firecrest-v2 (${FIRECREST_V2_REF}) -> ${FIRECREST_V2_DIR}"
    git clone --depth 1 --branch "${FIRECREST_V2_REF}" \
        https://github.com/eth-cscs/firecrest-v2 "${FIRECREST_V2_DIR}"
fi

# Local fixes to FireCREST (firecrest-patches/*.patch), applied once: a patch
# that is already in the checkout is skipped; one that neither applies nor is
# applied stops the script.
shopt -s nullglob
for patch in "$PWD"/firecrest-patches/*.patch; do
    if git -C "${FIRECREST_V2_DIR}" apply --reverse --check "$patch" 2>/dev/null; then
        echo ">> FireCREST patch already applied: $(basename "$patch")"
    elif git -C "${FIRECREST_V2_DIR}" apply --check "$patch" 2>/dev/null; then
        echo ">> Applying FireCREST patch: $(basename "$patch")"
        git -C "${FIRECREST_V2_DIR}" apply "$patch"
    else
        echo "!! FireCREST patch neither applies nor is applied: $patch" >&2
        git -C "${FIRECREST_V2_DIR}" apply --check "$patch" || true
        exit 1
    fi
done
shopt -u nullglob

mkdir -p secrets
[ -f secrets/ssh_private_key ] ||
    ssh-keygen -t ed25519 -N "" -f secrets/ssh_private_key -C hpc-workspace@emulator >/dev/null
# firecrest-health-check client secret from the imported kcrealm realm.
printf '%s' "2jIcgRvsAzD13OQe7Jwibv6hMf9CAaWZ" > secrets/service_account_client_secret

echo ">> docker compose up"
docker compose up --build -d --wait slurm keycloak firecrest
docker compose run --rm keycloak-setup

# The allocations Waldur will point at. Associations are enforced, so these
# decide who may charge which account:
#   alloc_climate  -- alice, bob   (project "Climate" in Waldur)
#   alloc_genomics -- alice        (project "Genomics")
#   alloc_private  -- bob          (not in Waldur: proves a workspace never
#                                   shows another allocation's jobs)
echo ">> Seeding cluster accounts and associations"
slurmdb() {
    curl -fsS -X POST "http://127.0.0.1:${SLURM_REST_PORT}/slurmdb/v0.0.45/$1" \
        -H 'X-SLURM-USER-TOKEN: seed' -H 'X-SLURM-USER-NAME: root' \
        -H 'Content-Type: application/json' -d "$2" >/dev/null
}
slurmdb accounts/ '{"accounts":[
  {"name":"alloc_climate","description":"Climate modelling","organization":"waldur"},
  {"name":"alloc_genomics","description":"Genome assembly","organization":"waldur"},
  {"name":"alloc_private","description":"Outside Waldur","organization":"other"}]}'
slurmdb associations/ '{"associations":[
  {"account":"alloc_climate","user":"alice"},
  {"account":"alloc_climate","user":"bob"},
  {"account":"alloc_genomics","user":"alice"},
  {"account":"alloc_private","user":"bob"}]}'

# Three weeks of finished jobs (+ sample output files in /home/alice) so the
# allocation's charts have history; skipped when already seeded.
echo ">> Seeding job history"
docker compose exec -T slurm python3 - < seed_history.py

# Per-job time series: Sonar -> Kafka -> slurm-monitor. depends_on orders it:
# topics are created before the listener and the generator start.
echo ">> Starting the monitoring stack (Kafka, TimescaleDB, slurm-monitor, sonar)"
docker compose up --build -d kafka monitor-db kafka-topics \
    slurm-monitor-listen slurm-monitor-api sonar

cat <<EOF

Stack is up:
  Keycloak   ${KC_PUBLIC_URL}  (realm kcrealm; alice/password, bob/password)
  FireCREST  http://localhost:${FIRECREST_PORT}/docs  (system "emulator")
  slurmrestd http://localhost:${SLURM_REST_PORT}
  slurm-monitor http://localhost:${SLURM_MONITOR_PORT}/api/v2/cluster/${SONAR_CLUSTER:-hpc-demo}/...
             (Bearer token from realm kcrealm; docs at /api/v2/docs)

Next: seed Waldur (from waldur-mastermind, with the dev stack's settings):
  KC_PORT=${KC_PORT} FIRECREST_PORT=${FIRECREST_PORT} SLURM_MONITOR_PORT=${SLURM_MONITOR_PORT} KC_PUBLIC_URL=${KC_PUBLIC_URL} \\
    uv run waldur shell < ../slurm-emulator/examples/firecrest/waldur/seed_waldur.py

Optional: live job traffic through Waldur's API (after seeding Waldur):
  uv run examples/firecrest/waldur/submit_workload.py --waldur-url http://localhost:8000 --duration 2h
EOF
