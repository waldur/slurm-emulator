#!/bin/sh
# GPU and CPU allocation on cluster-gres/ (node001-002 untyped gpu:4,
# node003-004 gpu:h200:4 with ThreadsPerCore=2); record_gres.py reads the
# result. Runs inside the cluster as root.
sb() { su alice -c "cd /tmp && sbatch --parsable -o /dev/null -e /dev/null -A acc1 -t 10 $* --wrap 'sleep 600'"; }
sb -p gpu -n1 -c1 --gres=gpu:1 -J untyped_gpu
sb -p gpu -n1 -c3 --gres=gpu:h200:2 -J typed_gpu
sb -p gpu -n2 --mem=2G -J no_gres_gpu_part
sb -p debug -n1 --gres=gpu:2 -J untyped_node
sb -p gpu -n1 --exclusive -J exclusive
sb -p debug -n4 -J debug_cpus
sb -p gpu -n1 --gpus=1 -J gpus_opt
sleep 4
