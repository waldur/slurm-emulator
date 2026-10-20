#!/bin/sh
# Runs inside the real cluster as root. Prints T0 (epoch) first.
sb() { u=$1; shift; su $u -c "cd /tmp && sbatch --parsable -o /dev/null -e /dev/null -A acc1 $*"; }
echo "T0=$(date +%s)"
J1=$(sb alice "-p gpu --gres=gpu:1 -n1 -c1 --mem=8G -J rocket --wrap 'sleep 20'")
J2=$(sb alice "-p debug -n2 --mem=1G -J finished --wrap 'sleep 10'")
J3=$(sb alice "-p debug -n1 --mem=1G -J running --wrap 'sleep 3000'")
J4=$(sb bob "-p debug -n1 --mem=1G -J suspended --wrap 'sleep 3000'")
J5=$(sb bob "-p debug -n1 --mem=1G -J requeued --requeue --wrap 'sleep 3000'")
J6=$(sb bob "-p debug -n1 --mem=1G -J pending --hold --wrap 'sleep 30'")
echo "JOBS=$J1 $J2 $J3 $J4 $J5 $J6"
sleep 10; scontrol suspend $J4; echo "SUSPEND1=$(date +%s)"
sleep 15; scontrol resume $J4; echo "RESUME1=$(date +%s)"
sleep 5;  scontrol requeue $J5; echo "REQUEUE=$(date +%s)"
sleep 10; scontrol suspend $J4; echo "SUSPEND2=$(date +%s)"
sleep 20; echo "QUERY=$(date +%s)"
squeue -o '%i %j %T %R'
