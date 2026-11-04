# slurm-monitor (https://github.com/2maz/slurm-monitor, BSD-3) for the HPC
# demo: the Kafka listener that stores Sonar messages in TimescaleDB and the
# REST API that serves per-job time series from them. One image, two
# services (`slurm-monitor listen ...`, `slurm-monitor restapi ...`).
#
# Installed from git at a pinned commit, with the fixes in patches/ that
# are still open upstream (one per pull request, named after it):
#   pr164  users see only their own jobs in partitions, node process samples
#          and named queries (2maz/slurm-monitor#164)
#   pr165  tokens Waldur exchanges for the slurm-monitor audience are
#          accepted (2maz/slurm-monitor#165); without it every Waldur call
#          answers 401
#   pr166  a job that is not found, or is another user's, answers 404
#          instead of 500 (2maz/slurm-monitor#166)
# When a pull request is merged, delete its patch and move the pin past it.
FROM python:3.12-slim

ARG SLURM_MONITOR_REPO=https://github.com/2maz/slurm-monitor.git
ARG SLURM_MONITOR_COMMIT=7b17664

# gcc: psutil has no prebuilt wheel for every platform (arm64 here).
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
        gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt
COPY patches/ /opt/patches/
RUN git clone --filter=blob:none "${SLURM_MONITOR_REPO}" slurm-monitor \
    && git -C slurm-monitor checkout --quiet "${SLURM_MONITOR_COMMIT}" \
    && for patch in /opt/patches/*.patch; do \
         [ -e "$patch" ] || continue; \
         echo "applying $(basename "$patch")"; \
         git -C slurm-monitor apply "$patch" || exit 1; \
       done \
    && pip install --no-cache-dir /opt/slurm-monitor

# The listener writes its stats/log files to the working directory.
WORKDIR /var/lib/slurm-monitor
EXPOSE 12000
ENTRYPOINT ["slurm-monitor"]
