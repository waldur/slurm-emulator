# Real Slurm 26.05 with accounting: slurmctld, slurmdbd (MariaDB), four
# slurmd on one host, slurmrestd on a unix socket (rest_auth/local).
FROM debian:bookworm
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential python3 python3-venv pkg-config libjson-c-dev libhttp-parser-dev libyaml-dev \
    libjwt-dev libmunge-dev munge default-libmysqlclient-dev mariadb-server ca-certificates \
    curl git && rm -rf /var/lib/apt/lists/*
COPY src /build/src
RUN sed -i 's/^#ifdef NDEBUG$/#if 0/' /build/src/src/slurmrestd/slurmrestd.c
WORKDIR /build/obj
RUN ../src/configure --prefix=/opt/slurm --sysconfdir=/etc/slurm --enable-slurmrestd \
      --enable-multiple-slurmd --without-hdf5 --without-pmix --without-ucx --without-rdma \
      > /tmp/configure.log 2>&1 || (tail -50 /tmp/configure.log; exit 1)
RUN grep -q 'MYSQL' /tmp/configure.log; make -j"$(nproc)" > /tmp/make.log 2>&1 || (tail -80 /tmp/make.log; exit 1)
RUN make install > /tmp/install.log 2>&1 && ls /opt/slurm/lib/slurm/accounting_storage_mysql.so
ENV PATH=/opt/slurm/bin:/opt/slurm/sbin:$PATH
