FROM python:3.11-slim

WORKDIR /app

# NSS identity mode (SLURM_EMULATOR_NSS=1): sssd resolves user names against
# an LDAP directory so `id`, sacct and the slurmrestd job views carry real
# uids/gids. Installed in every image (the daemon only starts when asked) so
# the same tag serves both modes; ldap-utils gives ldapsearch for debugging.
# `sss` is appended to the passwd/group/shadow sources (libnss-sss's postinst
# usually does it; the sed covers images where it does not) — `files` stays
# first, so root and the image's own users still resolve without sssd.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        sssd sssd-tools libnss-sss libpam-sss ldap-utils \
    && rm -rf /var/lib/apt/lists/* \
    && sed -i -E '/^(passwd|group|shadow):/{/\bsss\b/! s/$/ sss/}' /etc/nsswitch.conf

COPY . .

RUN pip install --no-cache-dir ".[ssh]" && chmod +x scripts/docker-entrypoint.sh

# 8080 — Waldur control API, 6820 — slurmrestd emulation,
# 2222 — SSH filesystem plane (opt-in via SLURM_EMULATOR_ENABLE_SSH=1)
EXPOSE 8080 6820 2222

CMD ["/app/scripts/docker-entrypoint.sh"]
