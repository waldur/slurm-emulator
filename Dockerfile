FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir . && chmod +x scripts/docker-entrypoint.sh

# 8080 — Waldur control API, 6820 — slurmrestd emulation
EXPOSE 8080 6820

CMD ["/app/scripts/docker-entrypoint.sh"]
