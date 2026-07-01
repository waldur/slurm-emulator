#!/usr/bin/env bash
# Bring up the full FireCREST-UI stack against the slurm-emulator.
#
# Steps: clone firecrest-v2 (for the API image build), write dev secrets, remind
# about the /etc/hosts entry, then `docker compose up --build`.
#
# Env:
#   FIRECREST_V2_DIR   firecrest-v2 checkout (default: ./.firecrest-v2, cloned)
#   FIRECREST_V2_REF   ref to clone (default: master)
#   FIRECREST_UI_IMAGE UI image (default: ghcr.io/eth-cscs/firecrest-ui:latest)
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

export FIRECREST_V2_DIR="${FIRECREST_V2_DIR:-$(pwd)/.firecrest-v2}"
FIRECREST_V2_REF="${FIRECREST_V2_REF:-master}"

if [ ! -d "${FIRECREST_V2_DIR}/.git" ]; then
    echo ">> Cloning firecrest-v2 (${FIRECREST_V2_REF}) -> ${FIRECREST_V2_DIR}"
    git clone --depth 1 --branch "${FIRECREST_V2_REF}" \
        https://github.com/eth-cscs/firecrest-v2 "${FIRECREST_V2_DIR}"
fi

# Dev secrets (not committed). The emulator's SSH plane accepts any key, but the
# API's SSHStaticKeys loader needs a private key file to exist.
mkdir -p secrets
if [ ! -f secrets/ssh_private_key_fireuser ]; then
    echo ">> Generating dev SSH key"
    ssh-keygen -t ed25519 -N "" -f secrets/ssh_private_key_fireuser -C fireuser@emulator >/dev/null
fi
# firecrest-health-check client secret from the imported kcrealm realm.
printf '%s' "2jIcgRvsAzD13OQe7Jwibv6hMf9CAaWZ" > secrets/service_account_client_secret

if ! grep -qE '^\s*127\.0\.0\.1\s+keycloak\b' /etc/hosts 2>/dev/null; then
    echo
    echo "!! ACTION REQUIRED: add this line to /etc/hosts so the OIDC issuer URL"
    echo "   resolves identically in your browser and in the containers:"
    echo
    echo "       127.0.0.1 keycloak"
    echo
    echo "   sudo sh -c 'echo \"127.0.0.1 keycloak\" >> /etc/hosts'"
    echo
fi

echo ">> docker compose up --build"
exec docker compose up --build "$@"
