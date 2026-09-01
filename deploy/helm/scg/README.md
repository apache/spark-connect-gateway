# `scg` — Spark Connect Gateway Helm chart

Stateless gRPC proxy in front of a pool of Apache Spark Connect
servers. Provides session affinity, multi-tenant routing, JWT/OIDC
authentication, Prometheus metrics, and OpenTelemetry tracing.
Multi-replica HA via a shared Redis affinity store (bundled or
external).

## Quickstart

```bash
helm install scg ./deploy/helm/scg \
  --namespace spark-connect --create-namespace \
  --set backendDiscovery.static.addresses='{spark-connect-1.svc.cluster.local:15002,spark-connect-2.svc.cluster.local:15002}'
```

This stands up:

* 2 gateway replicas
* 1 bundled Redis (StatefulSet, AOF persistence, 1Gi PVC)
* ConfigMap rendered from your `values.yaml`
* ServiceAccount + Service exposing gRPC `:15003` and admin `:9090`

The defaults assume a static backend list; switch to K8s-Endpoints
discovery (the Spark Kubernetes Operator pattern) with:

```bash
helm install scg ./deploy/helm/scg \
  --set backendDiscovery.type=k8s \
  --set backendDiscovery.k8s.namespace=spark-connect \
  --set backendDiscovery.k8s.serviceName=spark-connect
```

The chart automatically attaches a `Role` granting
`endpoints/endpointslices` read access in the target namespace.

## Common configurations

### Static backends + bundled Redis (default)

```yaml
backendDiscovery:
  type: static
  static:
    addresses:
      - "spark-connect-1.svc.cluster.local:15002"
      - "spark-connect-2.svc.cluster.local:15002"
```

### K8s service discovery + external managed Redis

```yaml
backendDiscovery:
  type: k8s
  k8s:
    namespace: spark-connect
    serviceName: spark-connect
    port: 15002

redis:
  enabled: false
affinityStore:
  type: redis
  redis:
    url: rediss://elasticache.aws.example:6379
    keyPrefix: scg-prod
```

### JWT auth (HMAC secret, dev/test) and tracing

```yaml
auth:
  type: jwt
  jwt:
    issuer: https://idp.example.com
    audience: spark-connect-gateway
    key:
      kind: hmacSecret
      secret: "change-me"

tracing:
  enabled: true
  endpoint: http://otel-collector:4317
  serviceName: spark-connect-gateway
```

### Single-replica development setup

```yaml
replicaCount: 1
affinityStore:
  type: memory
redis:
  enabled: false
```

The chart guards against the obvious foot-gun: setting
`replicaCount > 1` together with `affinityStore.type: memory` causes
`helm install` to fail at template time, because that combination
silently breaks Spark Connect's per-driver session invariant.

## Probes and observability

* `/healthz` → 200 always (used as liveness)
* `/readyz` → 200 once the backend pool has at least one entry
* `/metrics` → Prometheus exposition

The Service exposes the admin port; scrape with a
`PodMonitor`/`ServiceMonitor` or annotate-and-discover.

## Values reference

The full reference (with defaults) is in [`values.yaml`](values.yaml)
— each block is annotated inline.

| Key | Default | Notes |
|---|---|---|
| `image.repository` | `spark-connect-gateway` | Prepend your registry, e.g. `myregistry.io/spark-connect-gateway` |
| `image.tag` | `""` (chart `appVersion`) | |
| `replicaCount` | `2` | Multi-replica requires `affinityStore.type: redis` |
| `service.grpcPort` | `15003` | Spark Connect clients dial `sc://<svc>:15003` |
| `service.adminPort` | `9090` | `/metrics`, `/healthz`, `/readyz` |
| `backendDiscovery.type` | `static` | Or `k8s` |
| `auth.type` | `none` | Or `static` / `jwt` / `oidc` |
| `affinityStore.type` | `redis` | Or `memory` (single-replica only) |
| `redis.enabled` | `true` | Disable to point at external Redis |
| `redis.persistence.size` | `1Gi` | StatefulSet PVC size |
| `tracing.enabled` | `false` | Off by default; logs work regardless |

## Verifying multi-replica HA

After install, you can drive the HA smoke test against the
deployed cluster — see `crates/proxy/examples/ha_smoke.rs` in the
parent repo. With `kubectl port-forward` and a `REDIS_URL` pointing
at the bundled Redis, the same harness validates the three
invariants (shared state, failover after replica death, op-id
reverse index across replicas).

## Uninstall

```bash
helm uninstall scg -n spark-connect
# The Redis PVC is intentionally NOT garbage-collected by Helm so a
# reinstall can pick up where you left off; delete it manually if
# you want a clean slate:
kubectl -n spark-connect delete pvc -l app.kubernetes.io/component=redis
```
