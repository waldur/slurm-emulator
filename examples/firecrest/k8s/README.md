# FireCREST v2 against the emulator on Kubernetes

The [`ui/`](../ui/) and [`e2e/`](../e2e/) stacks wire FireCREST to the emulator with docker-compose. This directory covers the same scenario when the emulator is deployed by the [Helm chart](../../../charts/slurm-emulator) instead: which chart values the FireCREST path actually requires, and the four places where the chart's defaults do *not* match what the compose files set.

FireCREST v2 publishes its own Helm chart, so both halves install from a chart repo:

```bash
helm repo add slurm-emulator https://waldur.github.io/slurm-emulator/
helm repo add firecrest-v2   https://eth-cscs.github.io/firecrest-v2/charts/
helm repo update
```

This directory carries the overlay for each. Keycloak, OpenFGA and S3 remain FireCREST's own business — the overlays mark those spots `CHANGE ME`.

## What the scenario needs

FireCREST talks to a cluster over two planes, and the emulator serves both from one pod:

| Plane | Port | Used for | Chart default |
|---|---|---|---|
| slurmrestd (`v0.0.45`) | 6820 | submit / list / cancel jobs, accounting | **on** |
| SSH login node | 2222 | filesystem browse, upload, download, `get_job_metadata` | **off** — `ssh.enabled` |
| Control API + dashboard | 8080 | not used by FireCREST | on, keep internal |

Both planes share one state directory, so a file uploaded over SSH is visible to the job that reads it.

A Waldur site agent, if you add one, is a third party on the 6820 plane — see [Adding a Waldur site agent](#adding-a-waldur-site-agent).

## Install

First check you have a *working* default StorageClass, not merely a default one
— `persistence` below needs it:

```bash
kubectl get sc
```

Some clusters register a default class with no provisioner behind it (Docker
Desktop's kubeadm engine ships a `hostpath` class in exactly that state), and
then every PVC sits `Pending` forever with nothing in its events to point at.
If that is your situation, install one and pass `--set persistence.storageClass=`
to match:

```bash
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.30/deploy/local-path-storage.yaml
```

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

For the two-cluster setup that [`ui/`](../ui/) demonstrates, repeat with [`values-cluster-emulator-2.yaml`](values-cluster-emulator-2.yaml) and its own host-key Secret.

Then deploy FireCREST against it with [`values-firecrest-api.yaml`](values-firecrest-api.yaml):

```bash
helm install firecrest firecrest-v2/firecrest-api \
  -n firecrest -f values-firecrest-api.yaml
```

That overlay only fills in the emulator-facing parts of FireCREST's config — the OIDC provider, the service account, and the SSH key Secret are marked `CHANGE ME`. If you drive FireCREST's cluster config through `clusters: path:/app/clusters` and a `firecrest-cluster-configs` ConfigMap instead of chart values, [`f7t-api-config.emulator-k8s.yaml`](f7t-api-config.emulator-k8s.yaml) is the same content in that form.

## Files

| File | For |
|---|---|
| [`values-cluster-emulator.yaml`](values-cluster-emulator.yaml) | the `slurm-emulator` chart — first cluster |
| [`values-cluster-emulator-2.yaml`](values-cluster-emulator-2.yaml) | the `slurm-emulator` chart — second, independent cluster |
| [`values-firecrest-api.yaml`](values-firecrest-api.yaml) | the `firecrest-v2/firecrest-api` chart, pointed at both |
| [`values-site-agent.yaml`](values-site-agent.yaml) | the `waldur-site-agent` chart — see [Adding a Waldur site agent](#adding-a-waldur-site-agent) |
| [`f7t-api-config.emulator-k8s.yaml`](f7t-api-config.emulator-k8s.yaml) | FireCREST's raw cluster config, for the ConfigMap route |

## The five things the chart does not do for you

The first four are defaults that are right for a standalone emulator and wrong
for FireCREST. The fifth is about your identity provider rather than the chart,
and is here because it fails in the same silent way.

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

### 5. Your IdP has to emit more than a username

Two claims beyond the obvious, both of which fail *after* authentication
succeeds, so the logs point at the wrong place:

- **`given_name`** — FireCREST maps `ApiAuthUser.first_name` from it and types it
  as a plain `str`. A token without it passes signature validation and then 500s
  building the user model, with `first_name Input should be a valid string
  [input_value=None]`. A Keycloak user with no first name set does this.
- **A service account that can also log in over SSH** — see
  [`values-firecrest-api.yaml`](values-firecrest-api.yaml). The `ssh` and
  `filesystem` health probes connect as the service-account user, so it needs an
  entry in `ssh_credentials.keys` as well as a client in your IdP.

If you point FireCREST at the OpenStack emulator's embedded provider instead of
a real IdP, both of these need arranging in its preset — its stock users carry no
`given_name`, and its OIDC clients do not include a FireCREST service account.

## Host key

Without `ssh.hostKeySecret` the emulator generates a fresh RSA host key at every start ([`ssh/server.py`](../../../emulator/api/ssh/server.py) `_host_key`), so the cluster's SSH identity changes on every pod restart, rescheduling, and `helm upgrade`. Whether that breaks anything depends on your FireCREST build's host-key policy, but a stable key costs one Secret and removes the question. Create one Secret per cluster, keyed `ssh_host_key`, as in the install step above.

The emulator's SSH plane accepts any client key — the key FireCREST presents only has to satisfy its own static-key loader, exactly as in [`ui/`](../ui/).

## Adding a Waldur site agent

`waldur-site-agent` is a second, independent consumer of the same emulator. FireCREST drives jobs and files for end users; the agent creates and manages the SLURM accounts those jobs are charged to. Both reach `slurmrestd` on 6820, and neither knows about the other:

```
Waldur Mastermind ──► site agent ──┐
                                   ├──► cluster-emulator:6820  (slurmrestd)
end user ──► FireCREST ────────────┘──► cluster-emulator:2222  (SSH plane)
```

The agent has no FireCREST client and does not need one — it speaks the same `slurmrestd` plane directly, via its SLURM plugin's `execution_mode: rest`. Deploy it with its own chart and [`values-site-agent.yaml`](values-site-agent.yaml):

```bash
helm install site-agent ../../../../waldur-site-agent/helm/waldur-site-agent \
  -n firecrest -f values-site-agent.yaml
```

Fill in `waldur_api_url`, `waldur_api_token` and `waldur_offering_uuid` for your Mastermind first — those are the only `CHANGE ME` fields.

### Two things to get right

**`api_version` must be `v0.0.45`, with the `v`.** The SLURM plugin interpolates this value verbatim into `/slurmdb/{api_version}/...`, and its default is `v0.0.43`. The emulator serves `v0.0.45` only, so the default produces:

```
BackendError slurmrestd error (GET /slurm/v0.0.43/ping/): HTTP 404
```

This is the **opposite convention from FireCREST**, which builds `/slurm/v{api_version}/` and therefore wants `0.0.45` with no prefix. The same cluster, two configs, two spellings — [`values-firecrest-api.yaml`](values-firecrest-api.yaml) and [`values-site-agent.yaml`](values-site-agent.yaml) each carry a comment saying so.

**`cluster_name` is required in rest mode**, because every REST association payload carries an explicit cluster. Omitting it fails at startup with `cluster_name is required when SLURM execution_mode is 'rest'`.

The token is a non-issue as long as `auth.jwtKey` stays empty (which FireCREST requires anyway): any non-empty `SLURM_JWT` value is accepted. If you ever do set a key, mint a real one with `POST /api/token` on the control API rather than inventing a string.

### Which agents to run

The overlay enables the polling agents — `orderProcess`, `report`, `membershipSync` — and disables `eventProcess`, which needs Mastermind's STOMP broker reachable from the cluster. Turn it on once that connectivity exists.

## Verify

```bash
# Both planes answer.
helm test cluster-emulator -n firecrest

# slurmrestd, with an arbitrary token (this is the point of item 1).
kubectl -n firecrest run f7t-probe --rm -it --restart=Never \
  --image=curlimages/curl:8.10.1 -- \
  curl -fsS -H 'X-SLURM-USER-TOKEN: any' \
  http://cluster-emulator:6820/slurm/v0.0.45/ping/

# The SSH plane is listening.
kubectl -n firecrest run ssh-probe --rm -it --restart=Never \
  --image=busybox:1.36 -- \
  sh -c 'nc -z -w3 cluster-emulator 2222 && echo "ssh plane open"'
```

### Through FireCREST

Run this from inside the cluster: the emulator derives its OIDC issuer from the
request's `Host` header, so a token minted through a port-forward carries an
issuer FireCREST will not match.

```bash
kubectl -n firecrest run f7t --rm -it --restart=Never \
  --image=curlimages/curl:8.10.1 --command -- sh -c '
TOK=$(curl -s -X POST http://<your-idp>/token \
  -d "grant_type=password&username=alice&password=password&client_id=<id>&client_secret=<secret>&scope=openid profile email" \
  | sed -n "s/.*\"access_token\":\"\([^\"]*\)\".*/\1/p")
B=http://<release>-firecrest:5001
curl -s -H "Authorization: Bearer $TOK" $B/status/systems
curl -s -H "Authorization: Bearer $TOK" $B/filesystem/cluster-emulator/ops/ls?path=/data/fs/home
curl -s -X POST -H "Authorization: Bearer $TOK" -H "Content-Type: application/json" \
  -d "{\"job\":{\"name\":\"demo\",\"working_directory\":\"/data/fs/home/alice\",\"script\":\"#!/bin/bash\\nsleep 5\\n\"}}" \
  $B/compute/cluster-emulator/jobs
'
```

A healthy setup returns all three probes green, the user's home directory under
`/data/fs/home`, and a `jobId`:

```
scheduler healthy: true   ssh healthy: true   filesystem healthy: true
{"output":[{"name":"alice","type":"d", ...}]}
{"jobId":"1"}
```

Anything less is covered in [Troubleshooting](#troubleshooting) below —
in particular, all three probes green *except* `ssh` and `filesystem` means the
service account has no SSH key.

[`../ui-guide.md`](../ui-guide.md) walks the same ground through the UI with
screenshots.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Every FireCREST call → `401 Authentication failure` | `auth.jwtKey` is set; the forwarded Keycloak token is RS256 | Clear it (item 1) |
| Jobs work, all `/filesystem` calls fail | SSH plane not started | `--set ssh.enabled=true` |
| SSH connection refused on port 22 | Chart listens on 2222 | Align `ssh.port` with FireCREST's cluster entry (item 2) |
| Uploads succeed but the directory is always empty, and files vanish on restart | `file_systems[].path` is `/home` instead of `/data/fs/home` | Item 3 |
| Clock and accounting reset by themselves | Persistence off, pod restarted | `--set persistence.enabled=true` |
| Whole pod in `CrashLoopBackOff` after setting a non-root `securityContext` | The SSH host key must be readable by the runtime user; when asyncssh cannot read it the entrypoint's `wait -n` takes the healthy planes down too | Add a matching `fsGroup`, or drop the custom `securityContext` |
| Job submit rejected with a schema error | `scheduler.api_version` is not `0.0.45` | The emulator serves one data_parser version; pin it |
| Two clusters show identical node lists | Both releases use the default topology | Give each its own `partitions` |
| Site agent: `slurmrestd error (GET /slurm/v0.0.43/ping/): HTTP 404` | `rest_api.api_version` left at the plugin default | Set `v0.0.45`, with the `v` |
| Site agent: `cluster_name is required when SLURM execution_mode is 'rest'` | Missing `backend_settings.cluster_name` | Set it to the emulator's cluster name |
| FireCREST requests hit `/slurm/vv0.0.45/` | `api_version` given as `v0.0.45` on the FireCREST side | Drop the `v` — FireCREST prepends it |
| FireCREST pod CrashLoopBackOff, `ValidationError: clusters.0.probing Field required` | Setting `clusters` replaces the chart's default list wholesale, so required fields must all be present | Keep the `probing` block — it is in the overlay for exactly this reason |
| Editing the overlay does not change FireCREST's behaviour | The chart has no config checksum annotation, so a ConfigMap change alone leaves the pod running the old config | `kubectl rollout restart deploy/<release>-firecrest` |
| Every request 500s with `first_name Input should be a valid string` | The IdP emits no `given_name` claim | Item 5 above |
| `scheduler` probe green, `ssh` and `filesystem` red: `No SSH credentials found for user:<service account>` | The service account has no entry in `ssh_credentials.keys` | Item 5 above |
| All tokens suddenly `Access token is invalid` after redeploying the emulator | The embedded OIDC provider regenerates its signing key at every process start, and FireCREST caches the JWKS | `kubectl rollout restart deploy/<release>-firecrest` |
| Emulator PVC stuck `Pending`, no useful events | A default StorageClass exists but has no provisioner behind it | See the storage note in [Install](#install) |
| `slurmdb` requests return 307 and an empty body | The emulator's collection routes want a trailing slash | `curl -L`, or append `/` |

## Validated against

Everything above was checked on a live cluster (Kubernetes 1.36) against the
published chart `slurm-emulator 0.9.2` and `firecrest-v2/firecrest-api 2.5.6` —
deployed, not just rendered:

- both values files install; the pod becomes Ready and `helm test` passes
- `slurmrestd` answers `/slurm/v0.0.45/ping/` with an arbitrary token, and 401s
  with none
- setting `auth.jwtKey` makes *both* an RS256 Keycloak-shaped token and an opaque
  one 401 — clearing it restores 200
- over the SSH plane, `pwd` returns `/data/fs/home/<user>` and `$HOME` matches
- a file written there survives `kubectl rollout restart`, and the host key
  fingerprint is unchanged across it, so `ssh.hostKeySecret` is doing its job
- FireCREST reports all three health probes green, lists the user's home
  directory, and accepts a job submission that reaches the emulator

The identity notes in item 5 come from the same run: each failure quoted there is
one this configuration actually produced before it was fixed.

Not validated: a production IdP. This was exercised against the OpenStack
emulator's embedded OpenID Provider, which is a real RS256 provider but a small
one — a Keycloak or Entra deployment may differ in which claims it emits by
default, and `given_name` is the one to check first.

## See also

- [`../../../docs/kubernetes.md`](../../../docs/kubernetes.md) — the general operator guide for the chart (credentials, exposure, uninstall).
- [`../../../charts/slurm-emulator/README.md`](../../../charts/slurm-emulator/README.md) — full values reference.
- [`../e2e/`](../e2e/) and [`../ui/`](../ui/) — the compose equivalents of this setup.
- [`../conformance.md`](../conformance.md) — what FireCREST calls and what the emulator serves.
