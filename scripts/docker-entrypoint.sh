#!/bin/bash
# shellcheck shell=bash  (wait -n requires bash, present in python:slim)
# Run the emulator APIs in one container:
#   8080 — Waldur control API   (emulator.api.emulator_server)
#   6820 — slurmrestd emulation (emulator.api.slurmrestd.app)
#   2222 — SSH filesystem plane (emulator.api.ssh.server) — for the FireCREST
#          e2e setup, enabled with SLURM_EMULATOR_ENABLE_SSH=1
# They share state through the JSON files in /tmp.
set -eu

# NSS identity mode: start sssd so libc resolves users through LDAP
# (SLURM_EMULATOR_NSS=1). The configuration is read from
# SLURM_EMULATOR_SSSD_CONF (default /etc/slurm-emulator/sssd.conf, falling
# back to /etc/sssd/sssd.conf) and *copied* to a private root:root 0600 file
# before starting — sssd refuses anything looser, and a bind mount or a
# Secret volume keeps the mode of its source (0644 on a macOS/CI host), so the
# mount itself never has to be 0600 or writable. sssd must run as root, which
# is why the chart documents that the option and a non-root securityContext
# are mutually exclusive. It daemonises itself (-D), so `wait -n` below only
# watches the emulator processes; a failure here is a loud warning, not a
# dead container, because the emulator still works with the `files` NSS
# source (root, no LDAP users).
# Same truthy set as emulator.core.nss.enabled() (1/true/yes/on, any case).
nss_on=0
case "$(printf '%s' "${SLURM_EMULATOR_NSS:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) nss_on=1 ;;
esac
if [ "$nss_on" = "1" ]; then
    sssd_src="${SLURM_EMULATOR_SSSD_CONF:-/etc/slurm-emulator/sssd.conf}"
    [ -f "$sssd_src" ] || sssd_src=/etc/sssd/sssd.conf
    if [ -f "$sssd_src" ]; then
        sssd_conf=/run/slurm-emulator/sssd.conf
        mkdir -p "$(dirname "$sssd_conf")"
        if ! install -o root -g root -m 0600 "$sssd_src" "$sssd_conf" \
            || ! sssd -D --logger=stderr -c "$sssd_conf" 2>&1; then
            echo "WARNING: SLURM_EMULATOR_NSS is on but sssd failed to start;" \
                 "LDAP users will NOT resolve and submissions by them will be" \
                 "refused (check $sssd_src)" >&2
        fi
    else
        echo "WARNING: SLURM_EMULATOR_NSS is on but no sssd.conf found at" \
             "$sssd_src; only users from /etc/passwd will resolve" >&2
    fi
fi

python3 -m uvicorn emulator.api.emulator_server:app --host 0.0.0.0 --port 8080 &
python3 -m uvicorn emulator.api.slurmrestd.app:app --host 0.0.0.0 --port 6820 &

if [ "${SLURM_EMULATOR_ENABLE_SSH:-0}" = "1" ]; then
    python3 -m emulator.api.ssh.server &
fi

# Exit as soon as any server dies so the container restarts cleanly.
wait -n
