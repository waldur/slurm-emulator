# slurm-emulator Helm chart

Deploys the [SLURM Emulator](https://code.opennodecloud.com/waldur/slurm-emulator) — a time-travel-enabled stand-in for a SLURM cluster — into a Kubernetes cluster.

Primary use case: a sandbox HPC backend for `waldur-site-agent` development, periodic-limit and decay testing, FireCREST v2 integration work, or demos — without a real SLURM installation. See [`docs/kubernetes.md`](../../docs/kubernetes.md) in the same repo for the full deployment guide.

## TL;DR

```bash
helm install se ./charts/slurm-emulator --namespace se --create-namespace
helm test se -n se
```

The pod runs `scripts/docker-entrypoint.sh`, which serves two HTTP planes from one process tree:

| Port | Plane | Notes |
|---|---|---|
| 8080 | Waldur control API + web dashboard at `/ui/` | Dashboard behind HTTP Basic auth |
| 6820 | slurmrestd emulation (Slurm 26.05, `v0.0.45`) | `X-SLURM-USER-TOKEN` required |
| 2222 | SSH filesystem plane | Opt-in via `ssh.enabled` |

The planes are separate processes sharing the same JSON state files, so an account created over the control API shows up in `/slurmdb` immediately. The reverse lags: only the slurmrestd app reloads state per request, while the control API reads it once at startup — see [`docs/kubernetes.md`](../../docs/kubernetes.md#both-planes-share-one-state--with-one-caveat).

## Values reference

| Key | Default | Notes |
|---|---|---|
| `image.repository` | `opennode/slurm-emulator` | |
| `image.tag` | `""` | Falls back to `.Chart.AppVersion`. |
| `image.pullPolicy` | `IfNotPresent` | Set to `Never` when using a locally loaded image (e.g. `kind load`). |
| `replicaCount` | `1` | Fixed; each replica gets its own clock and account state. |
| `auth.uiUsername` / `auth.uiPassword` | `admin` / `admin` | Web dashboard HTTP Basic credentials. |
| `auth.jwtKey` | `""` | HS256 key for slurmrestd token verification. Empty ⇒ any token is accepted. |
| `auth.existingSecret` | `""` | Pre-created Secret with keys `ui-username`, `ui-password`, and optionally `jwt-key`. |
| `ssh.enabled` | `false` | Starts the asyncssh filesystem plane and exposes its port. |
| `ssh.port` | `2222` | |
| `ssh.timeoutSeconds` | `30` | Per-command shell timeout. |
| `ssh.hostKeySecret` | `""` | Secret with an `ssh_host_key` entry. Without it a new key is generated at every pod start. |
| `partitions` | `""` | Cluster topology, e.g. `gpu:8,compute:32` or `debug:1-4,compute:5-100`. Empty keeps the image default (`debug:1-4,compute:5-100`). |
| `slurmVersion` | `""` | Slurm release the emulator presents as (`24.11`, `25.05`, `25.11`, `26.05`, `master`): sets the slurmrestd URL prefix, `meta.slurm.release` and version-specific response shapes. Empty = image default (26.05). |
| `partitionQos` | `""` | Per-partition QoS gates, e.g. `gpu=allow:normal,high;gpu=qos:normal`. |
| `jobs.clock` | `wall` | `wall` (real time) or `time` (emulator clock) for submitted-job progression. |
| `jobs.runDelaySeconds` / `jobs.runDurationSeconds` | `2` / `8` | PENDING → RUNNING → COMPLETED timings. |
| `debug` | `false` | Sets `SLURM_EMULATOR_DEBUG=1`. |
| `persistence.enabled` | `false` | When on, mounts a PVC and redirects state, clock, and SSH filesystem into it. |
| `persistence.size` | `1Gi` | |
| `persistence.path` | `/data` | Mount dir; holds `slurm_emulator_db.json`, `slurm_emulator_time.json`, and `fs/`. |
| `persistence.keepOnUninstall` | `false` | Renders `helm.sh/resource-policy: keep` so `helm uninstall` leaves the PVC. Must be set *before* the uninstall — Helm reads the policy from the stored release manifest, not the live object. |
| `service.type` | `ClusterIP` | 8080 and 6820 always exposed; 2222 added with `ssh.enabled`. |
| `ingress.enabled` | `false` | Disabled by default — see Limitations. |
| `gatewayApi.enabled` | `false` | Optional `gateway.networking.k8s.io/v1` HTTPRoute. Off for the same reason as `ingress`. |
| `gatewayApi.createGateway` | `false` | When `true`, the chart renders a `Gateway` alongside the HTTPRoute (useful in kind / throwaway envs). When `false`, set `gatewayApi.parentRefs` to attach to an existing Gateway. |
| `gatewayApi.gateway.gatewayClassName` | `""` | Required when `createGateway: true`. e.g. `envoy`, `istio`, `cilium`. |
| `resources` | 50m/192Mi → 500m/512Mi | Two uvicorn processes plus in-memory state. |
| `probes.{liveness,readiness}` | Enabled, hit `/` on port 8080 | The root route is unauthenticated; `/ui/` is not. |
| `extraEnv` | `[]` | Appended verbatim to the container env. |

## Limitations

- Single replica only. State lives in memory and is flushed to one JSON file; two replicas diverge silently, and `strategy: Recreate` exists so a rolling update never puts two pods on one `ReadWriteOnce` volume.
- Without `persistence.enabled`, the emulator clock, accounts, and usage records reset on every restart — which also resets any time travel a test had performed.
- `ingress` / `gatewayApi` cover the HTTP planes only. The SSH plane is TCP and needs a `TCPRoute` or a `LoadBalancer`/`NodePort` Service instead.
- Do not expose the dashboard without changing `auth.uiPassword`, or the slurmrestd plane without setting `auth.jwtKey` — the defaults accept `admin`/`admin` and any bearer token respectively. Note that only `/ui/` is behind Basic auth: the control API's `/api/*` routes on the same port are unauthenticated and no chart value changes that, so port 8080 needs a fronting proxy if it leaves the cluster.
- Persistence is plain JSON; switching emulator versions may break the on-disk schema.

## See also

- [`../../docs/kubernetes.md`](../../docs/kubernetes.md) — Full deployment guide.
- [`../../docs/web-ui.md`](../../docs/web-ui.md) — Web dashboard walkthrough.
- [`../../README.md`](../../README.md) — Emulator overview, CLI, and API reference.
