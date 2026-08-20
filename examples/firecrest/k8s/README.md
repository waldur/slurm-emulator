# FireCREST v2 against the emulator on Kubernetes

The [`ui/`](../ui/) and [`e2e/`](../e2e/) stacks wire FireCREST to the emulator with docker-compose. This directory covers the same scenario when the emulator is deployed by the [Helm chart](../../../charts/slurm-emulator) instead: which chart values the FireCREST path actually requires, and the four places where the chart's defaults do *not* match what the compose files set.

Scope is the emulator side. Deploying FireCREST, its Keycloak, OpenFGA and S3 to Kubernetes is FireCREST's own business — this only tells you what to point them at.

## What the scenario needs

FireCREST talks to a cluster over two planes, and the emulator serves both from one pod:

| Plane | Port | Used for | Chart default |
|---|---|---|---|
| slurmrestd (`v0.0.46`) | 6820 | submit / list / cancel jobs, accounting | **on** |
| SSH login node | 2222 | filesystem browse, upload, download, `get_job_metadata` | **off** — `ssh.enabled` |
| Control API + dashboard | 8080 | not used by FireCREST | on, keep internal |

Both planes share one state directory, so a file uploaded over SSH is visible to the job that reads it.

## Install

```bash
helm repo add slurm-emulator https://waldur.github.io/slurm-emulator/
helm repo update

kubectl create namespace firecrest

# Pin an SSH host key per cluster (see "Host key" below).
ssh-keygen -t rsa -b 4096 -N '' -f ./ssh_host_key
kubectl -n firecrest create secret generic cluster-emulator-hostkey \
  --from-file=ssh_host_key=./ssh_host_key
rm ssh_host_key ssh_host_key.pub

helm install cluster-emulator slurm-emulator/slurm-emulator \
  -n firecrest \
  -f values-cluster-emulator.yaml \
  --set auth.uiPassword="$(openssl rand -hex 16)" \
  --wait

helm test cluster-emulator -n firecrest
```

Then hand [`f7t-api-config.emulator-k8s.yaml`](f7t-api-config.emulator-k8s.yaml) to FireCREST as its `YAML_CONFIG_FILE`, or merge its `clusters:` entries into the config you already have.

For the two-cluster setup that [`ui/`](../ui/) demonstrates, repeat with [`values-cluster-emulator-2.yaml`](values-cluster-emulator-2.yaml) and its own host-key Secret.

## The four things the chart does not do for you

Everything below is a default that is right for a standalone emulator and wrong for FireCREST.

### 1. `auth.jwtKey` must stay empty

This is the one that costs an afternoon. FireCREST forwards the end user's Keycloak access token verbatim as `X-SLURM-USER-TOKEN`. That token is **RS256**; the emulator's verifier ([`slurmrestd/auth.py`](../../../emulator/api/slurmrestd/auth.py)) is HS256 only. Set `auth.jwtKey` and every single FireCREST request comes back:

```
HTTP 401  Authentication failure
```

with nothing in the FireCREST logs pointing at the token as the cause. Leaving `auth.jwtKey` empty makes the emulator accept any token, which is exactly what the compose examples rely on. The chart's `NOTES.txt` prints a note about the empty key on install — for this scenario that note is expected, not a warning to act on.

If you need the slurmrestd plane locked down, do it with a `NetworkPolicy` restricting port 6820 to the FireCREST pods, not with `auth.jwtKey`.

### 2. `ssh.enabled: true`, on port 2222

Off by default, because it is only needed for FireCREST-shaped clients. Without it the pod serves slurmrestd fine — jobs submit and list — and every `/filesystem` call fails, which reads like a FireCREST bug rather than a missing plane.

The chart listens on **2222**, while the compose files remap it to 22. Either set `ssh.port: 22` in the chart values, or leave it and set `ssh.port: 2222` in FireCREST's cluster entry, as [`f7t-api-config.emulator-k8s.yaml`](f7t-api-config.emulator-k8s.yaml) does.

### 3. Home directories are under `/data/fs/home`, not `/home`

The compose files set `SLURM_EMULATOR_FS_ROOT=/` so that the paths FireCREST sends resolve against the container's real filesystem. The chart instead roots the fake filesystem inside the PVC, at `<persistence.path>/fs` — so with the default `persistence.path: /data`, a user's home is:

```
/data/fs/home/<username>
```

FireCREST's `file_systems[].path` must match. Point it at `/home` and FireCREST lands in the container's real, empty `/home`: browsing appears to work, uploads seem to succeed, and everything vanishes on the next pod restart because that path is not on the volume.

### 4. Persistence is off by default

`persistence.enabled: true` puts the emulator clock, the accounting state, and every uploaded file on one PVC. Without it they live in the container's `/tmp`, so a pod restart silently rewinds the cluster to an empty, present-day state — mid-demo, or mid-test-run.

The values files also set `persistence.keepOnUninstall: true`, which renders `helm.sh/resource-policy: keep`. Note this only works if it is set *while the release is installed*: Helm reads the resource policy from the stored release manifest, so annotating the PVC after the fact does nothing.

## Host key

Without `ssh.hostKeySecret` the emulator generates a fresh RSA host key at every start ([`ssh/server.py`](../../../emulator/api/ssh/server.py) `_host_key`), so the cluster's SSH identity changes on every pod restart, rescheduling, and `helm upgrade`. Whether that breaks anything depends on your FireCREST build's host-key policy, but a stable key costs one Secret and removes the question. Create one Secret per cluster, keyed `ssh_host_key`, as in the install step above.

The emulator's SSH plane accepts any client key — the key FireCREST presents only has to satisfy its own static-key loader, exactly as in [`ui/`](../ui/).

## Verify

```bash
# Both planes answer.
helm test cluster-emulator -n firecrest

# slurmrestd, with an arbitrary token (this is the point of item 1).
kubectl -n firecrest run f7t-probe --rm -it --restart=Never \
  --image=curlimages/curl:8.10.1 -- \
  curl -fsS -H 'X-SLURM-USER-TOKEN: any' \
  http://cluster-emulator:6820/slurm/v0.0.46/ping/

# The SSH plane is listening.
kubectl -n firecrest run ssh-probe --rm -it --restart=Never \
  --image=busybox:1.36 -- \
  sh -c 'nc -z -w3 cluster-emulator 2222 && echo "ssh plane open"'
```

From FireCREST itself, the end-to-end check is the same as with compose: list the cluster, submit a job, watch it go PENDING → RUNNING → COMPLETED, then browse the user's directory. [`../ui-guide.md`](../ui-guide.md) walks that through with screenshots.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Every FireCREST call → `401 Authentication failure` | `auth.jwtKey` is set; the forwarded Keycloak token is RS256 | Clear it (item 1) |
| Jobs work, all `/filesystem` calls fail | SSH plane not started | `--set ssh.enabled=true` |
| SSH connection refused on port 22 | Chart listens on 2222 | Align `ssh.port` with FireCREST's cluster entry (item 2) |
| Uploads succeed but the directory is always empty, and files vanish on restart | `file_systems[].path` is `/home` instead of `/data/fs/home` | Item 3 |
| Clock and accounting reset by themselves | Persistence off, pod restarted | `--set persistence.enabled=true` |
| Whole pod in `CrashLoopBackOff` after setting a non-root `securityContext` | The SSH host key must be readable by the runtime user; when asyncssh cannot read it the entrypoint's `wait -n` takes the healthy planes down too | Add a matching `fsGroup`, or drop the custom `securityContext` |
| Job submit rejected with a schema error | `scheduler.api_version` is not `0.0.46` | The emulator serves one data_parser version; pin it |
| Two clusters show identical node lists | Both releases use the default topology | Give each its own `partitions` |

## See also

- [`../../../docs/kubernetes.md`](../../../docs/kubernetes.md) — the general operator guide for the chart (credentials, exposure, uninstall).
- [`../../../charts/slurm-emulator/README.md`](../../../charts/slurm-emulator/README.md) — full values reference.
- [`../e2e/`](../e2e/) and [`../ui/`](../ui/) — the compose equivalents of this setup.
- [`../conformance.md`](../conformance.md) — what FireCREST calls and what the emulator serves.
