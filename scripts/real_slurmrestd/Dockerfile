# A real slurmctld + slurmrestd built from SchedMD's source, to record what
# slurmrestd answers (tests/data/slurmrestd_*.json). Build context: a Slurm
# checkout copied to ./src (see README.md).
FROM debian:bookworm
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential python3 pkg-config libjson-c-dev libhttp-parser-dev libyaml-dev libjwt-dev \
    libmunge-dev munge ca-certificates && rm -rf /var/lib/apt/lists/*
COPY src /build/src
# slurmrestd refuses to run as root outside development builds; the probe
# talks to it over a local socket as root, so only that guard is lifted.
RUN sed -i 's/^#ifdef NDEBUG$/#if 0/' /build/src/src/slurmrestd/slurmrestd.c
WORKDIR /build/obj
RUN ../src/configure --prefix=/opt/slurm --sysconfdir=/etc/slurm --enable-slurmrestd \
      --without-hdf5 --without-pmix --without-ucx --without-rdma > /tmp/configure.log 2>&1 \
    || (tail -50 /tmp/configure.log; exit 1)
RUN make -j"$(nproc)" > /tmp/make.log 2>&1 || (tail -80 /tmp/make.log; exit 1)
RUN make install > /tmp/install.log 2>&1
COPY slurm.conf /etc/slurm/slurm.conf
COPY start.sh probe.py /opt/probe/
CMD ["/opt/probe/start.sh"]
