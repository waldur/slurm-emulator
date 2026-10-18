#!/bin/sh
# munged + slurmctld + slurmrestd on /run/rr/s.sock (rest_auth/local, root).
set -e
mkdir -p /etc/munge /run/munge /var/spool/slurmctld /var/log/slurm /run/rr
[ -f /etc/munge/munge.key ] || dd if=/dev/urandom of=/etc/munge/munge.key bs=1024 count=1 2>/dev/null
chown -R munge:munge /etc/munge /run/munge /var/lib/munge /var/log/munge
chmod 400 /etc/munge/munge.key
su -s /bin/sh munge -c munged
/opt/slurm/sbin/slurmctld -i
SLURMRESTD_SECURITY=disable_unshare_sysv,disable_unshare_files,disable_user_check \
    exec /opt/slurm/sbin/slurmrestd -a rest_auth/local unix:/run/rr/s.sock
