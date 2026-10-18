#!/bin/bash
# Remove what cases_site_agent_slurmdb.json creates, so it can be recorded again
# on the same cluster. usage: reset_site_agent_slurmdb.sh <container>
set -u
c=${1:-parity-real}
sm() { docker exec "$c" /opt/slurm/bin/sacctmgr -i "$@" >/dev/null 2>&1; }
sm remove user e_u1,e_u2,e_u3,e_u4,e_u5,e_u6,e_u7
sm remove account e_a1,e_a2
sm remove qos e_q1
exit 0
