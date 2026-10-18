#!/bin/sh
set -e
mkdir -p /opt/fakegpu && touch /opt/fakegpu/nvidia0 /opt/fakegpu/nvidia1 /opt/fakegpu/nvidia2 /opt/fakegpu/nvidia3
for u in alice bob carol; do id $u >/dev/null 2>&1 || useradd -m $u; done
mkdir -p /etc/munge /run/munge /var/spool/slurmctld /var/log/slurm /run/rr /etc/slurm
cp /run-config/slurm.conf /run-config/gres.conf /run-config/cgroup.conf /etc/slurm/
cp /run-config/slurmdbd.conf /etc/slurm/ && chmod 600 /etc/slurm/slurmdbd.conf
[ -f /etc/munge/munge.key ] || dd if=/dev/urandom of=/etc/munge/munge.key bs=1024 count=1 2>/dev/null
chown -R munge:munge /etc/munge /run/munge /var/lib/munge /var/log/munge; chmod 400 /etc/munge/munge.key
su -s /bin/sh munge -c munged
mysql_install_db --user=mysql >/dev/null 2>&1 || true
mysqld_safe >/dev/null 2>&1 &
for i in $(seq 1 30); do mysqladmin ping >/dev/null 2>&1 && break; sleep 1; done
mysql -e "CREATE USER IF NOT EXISTS 'slurm'@'localhost' IDENTIFIED BY 'slurm'; GRANT ALL ON slurm_acct_db.* TO 'slurm'@'localhost'; FLUSH PRIVILEGES;"
slurmdbd
for i in $(seq 1 30); do sacctmgr -n show cluster >/dev/null 2>&1 && break; sleep 1; done
sacctmgr -i add cluster demo >/dev/null 2>&1 || true
slurmctld -i
sleep 2
for n in node001 node002 node003 node004; do mkdir -p /var/spool/slurmd/$n; slurmd -N $n; done
SLURMRESTD_SECURITY=disable_unshare_sysv,disable_unshare_files,disable_user_check \
    slurmrestd -a rest_auth/local unix:/run/rr/s.sock >/var/log/slurm/slurmrestd.log 2>&1 &
sleep 1
python3 /run-config/proxy.py &
echo READY
exec sleep infinity
