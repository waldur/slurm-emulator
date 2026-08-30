# Deploying the SLURM Emulator on Kubernetes

This guide walks an operator through deploying the SLURM Emulator into a Kubernetes cluster as a sandbox HPC backend that other workloads — `waldur-site-agent`, FireCREST v2, integration tests, demo platforms — can target over in-cluster DNS.

**Scope:** demo / sandbox / CI environments. The emulator's web dashboard defaults to `admin`/`admin` and its slurmrestd plane accepts any token unless you set a signing key, so it is not safe to put on the public internet unchanged — see [Limitations](#limitations).

## Prerequisites

| Tool | Tested version | Why |
|---|---|---|
| `kubectl` | 1.30+ | Talking to the cluster |
| `helm` | 3.14+ | Installing the chart |
| A running cluster | k8s 1.27+ | Anywhere from kind to a managed cloud cluster |

If your cluster denies cross-namespace traffic by default, allow it explicitly with a `NetworkPolicy` or co-locate the consumer workload with the emulator namespace.

## Step 1 — Install the chart

The chart is published to GitHub Pages, so the usual install needs no checkout:

```bash
helm repo add slurm-emulator https://waldur.github.io/slurm-emulator/
helm repo update

helm install slurm-emulator slurm-emulator/slurm-emulator \
  --namespace se --create-namespace \
  --set auth.uiPassword="$(openssl rand -hex 16)" \
  --wait
```

The chart version equals the emulator release tag, and `appVersion` — the image tag the chart deploys — matches it. Omit `--version` to take the newest.

The chart source lives at [`charts/slurm-emulator/`](../charts/slurm-emulator) in this repo; to install an unreleased change, clone and install from disk instead:

```bash
git clone https://code.opennodecloud.com/waldur/slurm-emulator.git
cd slurm-emulator

helm install slurm-emulator ./charts/slurm-emulator \
  --namespace se --create-namespace \
  --wait
```

Once the rollout completes, Helm prints the in-cluster URLs. Confirm the chart with its bundled health check:

```bash
helm test slurm-emulator -n se
```

`helm test` runs a one-shot Pod that hits `/` on the control API (8080) and `/openapi.json` on the slurmrestd plane (6820). Both must identify themselves for the test to pass — the slurmrestd probe deliberately uses the unauthenticated spec route, so it holds whether or not `auth.jwtKey` is set.

### Useful variants

```bash
# Survive pod restarts: keep the clock, accounts, and usage records on a PVC.
helm install slurm-emulator ./charts/slurm-emulator \
  --namespace se --create-namespace \
  --set persistence.enabled=true --set persistence.size=2Gi

# Shape the cluster topology reported by sinfo and the partition endpoints.
helm install slurm-emulator ./charts/slurm-emulator \
  --namespace se --create-namespace \
  --set partitions="gpu:8,compute:32" \
  --set partitionQos="gpu=allow:normal,high"

# Add the SSH filesystem plane (needed by FireCREST v2).
helm install slurm-emulator ./charts/slurm-emulator \
  --namespace se --create-namespace \
  --set ssh.enabled=true
```

## Step 2 — Set credentials

Two secrets matter, and both have permissive defaults so the emulator works out of the box:

| Value | Default | Effect of leaving it |
|---|---|---|
| `auth.uiPassword` | `admin` | The web dashboard at `/ui/` — which can advance time, inject usage, and change QoS — is open to anyone who can reach port 8080 |
| `auth.jwtKey` | `""` (empty) | The slurmrestd plane accepts *any* `X-SLURM-USER-TOKEN` value |

**Neither of these protects the control API itself.** Only `/ui/` is behind Basic auth; the `/api/*` routes on the same port have no authentication and no chart value that adds any — `POST /api/time/advance`, `/api/accounts`, `/api/submit-report`, and `/api/downscale-resource` all answer unauthenticated requests. Anyone who can reach port 8080 can rewind the cluster clock and inject usage regardless of what you set here. Treat network reachability as the only control on 8080, and put an authenticating proxy in front if it must leave the cluster.

Set them at install time, or point the chart at a Secret you manage yourself:

```bash
kubectl -n se create secret generic slurm-emulator-auth \
  --from-literal=ui-username=ops \
  --from-literal=ui-password="$(openssl rand -hex 16)" \
  --from-literal=jwt-key="$(openssl rand -hex 32)"

helm upgrade slurm-emulator ./charts/slurm-emulator -n se --reuse-values \
  --set auth.existingSecret=slurm-emulator-auth
```

The expected keys are `ui-username`, `ui-password`, and (optionally) `jwt-key`. When the chart manages the Secret itself, the Deployment carries a `checksum/secret` annotation, so rotating a credential through `helm upgrade` restarts the pod; with `auth.existingSecret` you own that lifecycle and must `kubectl rollout restart` after editing the Secret.

With `auth.jwtKey` set, clients need a real HS256 token. Mint one from the control API — the emulator's stand-in for `scontrol token`:

```bash
curl -s -X POST http://localhost:8080/api/token -H 'Content-Type: application/json' -d '{"username": "root"}'
```

## Step 3 — Smoke-test the emulator from your laptop

Port-forward the control API and ask for status:

```bash
kubectl -n se port-forward svc/slurm-emulator 8080:8080 &

curl -s http://localhost:8080/
# {"message":"SLURM Emulator API","version":"...","current_time":"...","current_period":"..."}

curl -s http://localhost:8080/api/status
# {"status":"running","cluster":"default","current_time":"...","accounts":{...}}
```

The web dashboard is on the same port at <http://localhost:8080/ui/> (HTTP Basic, credentials from Step 2). See [web-ui.md](web-ui.md) for a walkthrough.

The slurmrestd plane needs its own port-forward and a token header:

```bash
kubectl -n se port-forward svc/slurm-emulator 6820:6820 &

curl -s -H 'X-SLURM-USER-TOKEN: any' http://localhost:6820/slurm/v0.0.45/ping/
curl -s -H 'X-SLURM-USER-TOKEN: any' http://localhost:6820/slurmdb/v0.0.45/accounts
```

(Stop the port-forwards with `kill %1 %2` when you're done.)

## Step 4 — Use the in-cluster endpoint from another workload

Other workloads reach the emulator via in-cluster DNS:

```text
http://slurm-emulator.<emulator-namespace>.svc.cluster.local:8080     # control API + /ui/
http://slurm-emulator.<emulator-namespace>.svc.cluster.local:6820     # slurmrestd
```

Substitute the namespace you installed into (e.g. `se`). If your release name differs from the chart name, the Service name becomes `<release>-slurm-emulator` — `kubectl get svc -n <ns>` always shows the real name.

For `waldur-site-agent`, that is the `emulator_base_url`:

```yaml
emulator_mode: true
emulator_base_url: "http://slurm-emulator.se.svc.cluster.local:8080"
```

Sanity check from a throwaway pod *in a different namespace* (simulates the consumer):

```bash
kubectl create ns consumer
kubectl run -n consumer curlcheck --restart=Never \
  --image=curlimages/curl:8.10.1 --command -- \
  sh -c 'curl -fsS http://slurm-emulator.se.svc.cluster.local:8080/'

kubectl -n consumer logs curlcheck
# {"message":"SLURM Emulator API",...}

kubectl -n consumer delete pod curlcheck
kubectl delete ns consumer
```

### Both planes share one state — with one caveat

The control API, the slurmrestd emulation, the CLI commands (`sacct`, `sacctmgr`, `sinfo`, `sshare`), and the SSH plane all read and write the same JSON state and clock files, so an account created over `POST /api/accounts` shows up at `/slurmdb/v0.0.45/accounts` right away.

The reverse does not hold. They are separate processes, and only the slurmrestd app reloads state per request — the control API on 8080 loads it once at startup ([`emulator_server.py`](../emulator/api/emulator_server.py) `__init__`). So a write made over slurmrestd, `sacctmgr`, or the SSH plane is **not** reflected in `GET /api/status` or the dashboard until the pod restarts. An in-cluster integration test that creates an account over `/slurmdb` and then asserts on the control API will read stale data; drive both from the control API, or restart the Deployment between the write and the read.

State sharing is also why persistence matters: without a PVC, the files live in the container's `/tmp` and every restart resets the cluster to a fresh, empty, present-day state.

## Step 5 (optional) — Persistence

```bash
helm upgrade slurm-emulator ./charts/slurm-emulator -n se --reuse-values \
  --set persistence.enabled=true --set persistence.size=2Gi
```

This mounts one PVC at `persistence.path` (default `/data`) and redirects all three state locations into it:

| Env var | Path under `/data` | Holds |
|---|---|---|
| `SLURM_EMULATOR_STATE_FILE` | `slurm_emulator_db.json` | Accounts, users, associations, QoS, usage records, jobs |
| `SLURM_EMULATOR_TIME_FILE` | `slurm_emulator_time.json` | The emulator clock |
| `SLURM_EMULATOR_FS_ROOT` | `fs/` | The SSH plane's fake filesystem (home dirs, job scripts, outputs) |

The Deployment uses `strategy: Recreate`, so an upgrade never has two pods contending for a `ReadWriteOnce` volume.

## Step 6 (optional) — The SSH filesystem plane

FireCREST v2 and similar HPC middlewares expect a scheduler plane *and* a shell they can run filesystem commands on. `ssh.enabled=true` starts the asyncssh server alongside the two APIs and exposes port 2222 on the Service:

```bash
helm upgrade slurm-emulator ./charts/slurm-emulator -n se --reuse-values \
  --set ssh.enabled=true
```

By default the server generates a fresh RSA host key at every start, so clients that pin host keys see a change after each restart. Pin it with a Secret:

```bash
ssh-keygen -t rsa -b 4096 -N '' -f ./ssh_host_key
kubectl -n se create secret generic slurm-emulator-hostkey --from-file=ssh_host_key=./ssh_host_key
rm ssh_host_key ssh_host_key.pub

helm upgrade slurm-emulator ./charts/slurm-emulator -n se --reuse-values \
  --set ssh.enabled=true --set ssh.hostKeySecret=slurm-emulator-hostkey
```

The SSH plane is TCP, so `ingress` and `gatewayApi` do not cover it — expose it with a `LoadBalancer`/`NodePort` Service or a Gateway API `TCPRoute` if it needs to leave the cluster.

If you are deploying the emulator specifically to back FireCREST, read [`examples/firecrest/k8s/`](../examples/firecrest/k8s/) before going further: four of this chart's defaults — `auth.jwtKey`, `ssh.enabled`, the filesystem root, and persistence — are wrong for that scenario, and three of them fail quietly.

## Step 7 (optional) — Expose externally

The chart supports either Ingress or Gateway API for the two HTTP planes; pick whichever your cluster already runs.

**Only expose on a trusted network.** Step 2 is necessary but not sufficient: the ingress example below and the default HTTPRoute rule both publish port 8080, whose `/api/*` routes are unauthenticated no matter what credentials you set. Anything reachable from outside the cluster needs an authenticating proxy in front of it.

### Option A: Ingress (`networking.k8s.io/v1`)

```bash
helm upgrade slurm-emulator ./charts/slurm-emulator \
  -n se --reuse-values \
  --set ingress.enabled=true \
  --set 'ingress.hosts[0].host=se.example.com' \
  --set 'ingress.hosts[0].paths[0].path=/' \
  --set 'ingress.hosts[0].paths[0].pathType=Prefix' \
  --set 'ingress.hosts[0].paths[0].port=8080'
```

Each path targets a single emulator port — add another entry with `port: 6820` to publish slurmrestd too. Add `ingress.className`, `ingress.annotations`, and `ingress.tls` to suit your ingress controller and cert-manager setup.

### Option B: Gateway API (`gateway.networking.k8s.io/v1`)

**With an existing shared Gateway** (most production setups):

```bash
helm upgrade slurm-emulator ./charts/slurm-emulator \
  -n se --reuse-values \
  --set gatewayApi.enabled=true \
  --set 'gatewayApi.parentRefs[0].name=shared-gateway' \
  --set 'gatewayApi.parentRefs[0].namespace=gateway-system' \
  --set 'gatewayApi.hostnames[0]=se.example.com' \
  --set 'gatewayApi.rules[0].matches[0].path.type=PathPrefix' \
  --set 'gatewayApi.rules[0].matches[0].path.value=/' \
  --set 'gatewayApi.rules[0].port=8080'
```

**Self-contained Gateway** (kind, throwaway test envs — needs a Gateway API controller installed for the `gatewayClassName` you pick):

```bash
helm upgrade slurm-emulator ./charts/slurm-emulator \
  -n se --reuse-values \
  --set gatewayApi.enabled=true \
  --set gatewayApi.createGateway=true \
  --set gatewayApi.gateway.gatewayClassName=envoy \
  --set 'gatewayApi.hostnames[0]=se.example.com'
```

Prerequisite for either Gateway API path: the standard Gateway API CRDs must already be installed in the cluster:

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/standard-install.yaml
```

Without those CRDs, `helm upgrade` will fail to apply the `Gateway`/`HTTPRoute` objects.

## Uninstall

```bash
helm uninstall slurm-emulator -n se
kubectl delete ns se       # optional, if nothing else lives there
```

**`helm uninstall` deletes the PVC and the persisted state with it.** The chart does not set `helm.sh/resource-policy: keep`, so the clock, accounts, and usage records are gone — an uninstall/reinstall cycle does not preserve them. Back the state up first if it matters:

```bash
kubectl -n se exec deploy/slurm-emulator -- \
  tar cf - -C /data slurm_emulator_db.json slurm_emulator_time.json > emulator-state.tar
```

To keep the volume across a reinstall instead, set `persistence.keepOnUninstall` **while the release is still installed**:

```bash
helm upgrade slurm-emulator ./charts/slurm-emulator -n se --reuse-values \
  --set persistence.keepOnUninstall=true
```

Helm will then leave the PVC behind, and the next install with the same release name binds to it again. It has to go through the chart: Helm reads `helm.sh/resource-policy` from the manifest stored in the release, never from the live object, so `kubectl annotate` on the PVC after the fact has no effect at all.

## Limitations

- **Single replica only.** Emulator state is in memory and flushed to one JSON file; two pods produce two diverging clusters, each with its own clock. The chart defaults `replicaCount: 1` and there is no supported way to scale out.
- **Permissive defaults, and one surface with no knob at all.** The dashboard defaults to `admin`/`admin` and slurmrestd accepts any bearer token; both are configurable (Step 2). The control API's `/api/*` routes on 8080 are unauthenticated and cannot be secured through the chart — only a fronting proxy or network policy protects them.
- **Ephemeral without a PVC.** Time travel, injected usage, and created accounts are lost on restart, which silently rewinds any test mid-flight.
- **HTTP exposure only.** `ingress` / `gatewayApi` do not cover the SSH plane.
- **Persistence is plain JSON.** There is no schema migration; an image upgrade over an existing PVC may fail to read state written by a different emulator version. Delete the PVC to start clean if that happens.
- **Not a real scheduler.** Submitted jobs advance PENDING → RUNNING → COMPLETED on a timer (`jobs.*`), nothing is actually executed, and the node inventory is synthetic.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `helm test` Pod fails with `connection refused` | Pod still starting up | Re-run `helm test` after `kubectl -n se wait --for=condition=available deploy/slurm-emulator --timeout=120s` |
| Consumer can't resolve the emulator URL | Wrong namespace in the URL | `kubectl get svc -A` to find the actual Service, then rebuild the URL as `<svc>.<ns>.svc.cluster.local:8080` |
| slurmrestd returns an auth rejection for every request | `auth.jwtKey` is set and the client sends an opaque token | Mint a token via `POST /api/token`, or clear `auth.jwtKey` for a sandbox |
| Dashboard prompts for credentials that don't work | Secret edited in place while using `auth.existingSecret` | `kubectl -n se rollout restart deploy/slurm-emulator` — env vars are read at process start |
| Accounts and time reset after a restart | Persistence disabled | `--set persistence.enabled=true` (Step 5) |
| SSH clients warn about a changed host key | No `ssh.hostKeySecret`, so a new key is generated per pod | Create and pin a host key Secret (Step 6) |
| `helm install` times out at "wait for deployment" | Image pull failed (typo in tag or registry not reachable) | `kubectl -n se describe pod` — fix the image reference and `helm upgrade` |
| `helm upgrade` errors on Gateway/HTTPRoute "no matches for kind" | Gateway API CRDs not installed | `kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.2.0/standard-install.yaml`, then re-run the upgrade |
| `Gateway` shows `PROGRAMMED: Unknown` indefinitely | No Gateway API controller in the cluster for the picked `gatewayClassName` | Install a controller (Envoy Gateway, Istio, Cilium…) or change `gatewayClassName` to one already present |

## See also

- [`../README.md`](../README.md) — Emulator overview, CLI, and API reference.
- [`../charts/slurm-emulator/README.md`](../charts/slurm-emulator/README.md) — Chart values reference.
- [`web-ui.md`](web-ui.md) — Web dashboard walkthrough.
- [`../examples/firecrest/`](../examples/firecrest) — Running FireCREST v2 against the emulator.
