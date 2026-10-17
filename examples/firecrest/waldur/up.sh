#!/usr/bin/env bash
# Bring up Keycloak + FireCREST v2 + the slurm-emulator for the Waldur HPC
# workspace, and seed the cluster's accounts and associations.
#
# Env:
#   FIRECREST_V2_DIR  firecrest-v2 checkout (default: ../ui/.firecrest-v2, cloned)
#   FIRECREST_V2_REF  ref to clone (default: master)
#   PORT_BASE         first of five consecutive host ports; KC = base, FireCREST
#                     = base+1, slurmrestd = base+2, SSH = base+3
#                     (default: 8080/8000/6820/2222 when unset)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

export FIRECREST_V2_DIR="${FIRECREST_V2_DIR:-$(cd ../ui && pwd)/.firecrest-v2}"
FIRECREST_V2_REF="${FIRECREST_V2_REF:-master}"
if [ -n "${PORT_BASE:-}" ]; then
    export KC_PORT=$((PORT_BASE)) FIRECREST_PORT=$((PORT_BASE + 1))
    export SLURM_REST_PORT=$((PORT_BASE + 2)) SLURM_SSH_PORT=$((PORT_BASE + 3))
fi
KC_PORT="${KC_PORT:-8080}"
FIRECREST_PORT="${FIRECREST_PORT:-8000}"
SLURM_REST_PORT="${SLURM_REST_PORT:-6820}"

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

cat <<EOF

Stack is up:
  Keycloak   http://localhost:${KC_PORT}/auth  (realm kcrealm; alice/password, bob/password)
  FireCREST  http://localhost:${FIRECREST_PORT}/docs  (system "emulator")
  slurmrestd http://localhost:${SLURM_REST_PORT}

Next: seed Waldur (from waldur-mastermind, with the dev stack's settings):
  KC_PORT=${KC_PORT} FIRECREST_PORT=${FIRECREST_PORT} \\
    uv run waldur shell < ../slurm-emulator/examples/firecrest/waldur/seed_waldur.py
EOF
