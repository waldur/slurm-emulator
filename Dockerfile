FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir ".[ssh]" && chmod +x scripts/docker-entrypoint.sh

# 8080 — Waldur control API, 6820 — slurmrestd emulation,
# 2222 — SSH filesystem plane (opt-in via SLURM_EMULATOR_ENABLE_SSH=1)
EXPOSE 8080 6820 2222

CMD ["/app/scripts/docker-entrypoint.sh"]
