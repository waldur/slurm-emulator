# slurm-monitor (https://github.com/2maz/slurm-monitor, BSD-3) for the HPC
# demo: the Kafka listener that stores Sonar messages in TimescaleDB and the
# REST API that serves per-job time series from them. One image, two
# services (`slurm-monitor listen ...`, `slurm-monitor restapi ...`).
#
# Installed from git at a pinned commit.
FROM python:3.12-slim

ARG SLURM_MONITOR_REPO=https://github.com/2maz/slurm-monitor.git
ARG SLURM_MONITOR_COMMIT=972664c

# gcc: psutil has no prebuilt wheel for every platform (arm64 here).
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
        gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt
RUN git clone --filter=blob:none "${SLURM_MONITOR_REPO}" slurm-monitor \
    && git -C slurm-monitor checkout --quiet "${SLURM_MONITOR_COMMIT}" \
    && pip install --no-cache-dir /opt/slurm-monitor

# The listener writes its stats/log files to the working directory.
WORKDIR /var/lib/slurm-monitor
EXPOSE 12000
ENTRYPOINT ["slurm-monitor"]
