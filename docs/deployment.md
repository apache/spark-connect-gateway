# Deployment guide

This guide takes you from "I have a Kubernetes cluster" to "PySpark
clients are landing on the right Spark Connect drivers and staying
there across gateway-pod restarts." It is written for the operator
who will own the deployment day-2 — the README explains *what* the
gateway is; this doc explains *how to run it well*.

## Prerequisites

| | Required | Why |
|---|---|---|
| Kubernetes | ≥ 1.24 | Chart uses `discovery.k8s.io/v1` EndpointSlices (GA in 1.21) and probe HTTP/2 fields stabilized in 1.24 |
| Helm | ≥ 4.0 | Chart uses Helm 4 schema. Earlier Helm versions will refuse to install. |
| Spark Connect servers | ≥ 1 reachable from the gateway pods | The gateway is a stateless proxy; it needs *something* to forward to |
| Redis | Optional but **strongly recommended** for HA | Multi-replica gateway needs shared affinity state — see [Redis decision](#picking-the-redis-backing) |

The gateway itself does not need any cluster-scoped permissions. The
only RBAC it ever needs is namespace-scoped read on `endpoints` /
`endpointslices`, and only when `backendDiscovery.type=k8s`. The
chart attaches that automatically.

## Day 0: a minimal install

Pick a namespace and use the chart defaults:

```bash
helm install scg ./deploy/helm/scg \
  --namespace spark-connect --create-namespace
```

Defaults give you:

* 2 gateway replicas (the smallest meaningful HA configuration)
* 1 bundled Redis StatefulSet with AOF persistence + 1Gi PVC
* A static backend list pointing at
  `spark-connect-1.svc.cluster.local:15002` and
  `spark-connect-2.svc.cluster.local:15002`
* No authentication (every caller is `user_id="anonymous"`)
* No OTel tracing (structured JSON logs only)

This is **enough for a smoke test on a trusted in-cluster network**.
It is *not* a production configuration; see the rest of this guide
for how to harden it.

Verify the install:

```bash
kubectl -n spark-connect rollout status deployment/scg
kubectl -n spark-connect get pods -l app.kubernetes.io/name=scg
kubectl -n spark-connect port-forward svc/scg 9090:9090 &
curl http://localhost:9090/readyz   # expect: 200 ready
curl http://localhost:9090/healthz  # expect: 200 ok
```

If `/readyz` returns 503, the backend pool is empty. See the
[runbook](runbook.md#readyz-stuck-on-503).

## Picking the backend discovery mode

The gateway needs a list of Spark Connect server addresses. Two
ways:

### `static` — explicit list

```yaml
backendDiscovery:
  type: static
  static:
    addresses:
      - "spark-connect-1.svc.cluster.local:15002"
      - "spark-connect-2.svc.cluster.local:15002"
```

Use this when:

* Spark Connect servers run *outside* this Kubernetes cluster (VMs,
  another cluster, an EMR-managed driver).
* You're scripting the address list from outside Helm anyway (CI
  produces it).
* You want zero RBAC on the gateway pod.

### `k8s` — Endpoints watch

```yaml
backendDiscovery:
  type: k8s
  k8s:
    namespace: spark-connect
    serviceName: spark-connect
    port: 15002
```

Use this when Spark Connect servers run as pods in *this* cluster
and you scale them up/down — the gateway picks up additions and
removals within seconds without a Helm rollout. This is the natural
match for the
[`apache/spark-kubernetes-operator`](https://github.com/apache/spark-kubernetes-operator)
deployment pattern (see
[`deploy/examples/spark-connect-server/`](../deploy/examples/spark-connect-server/)
in this repo).

The chart automatically attaches a `Role` and `RoleBinding` granting
`endpoints/endpointslices` get/list/watch in the target namespace
when `type: k8s`. If your namespaced backend lives in a *different*
namespace from the gateway release, the chart puts the Role in the
backend namespace and the RoleBinding pointing at the gateway's
ServiceAccount in the release namespace.

## Picking the Redis backing

The gateway maps `(user_id, session_id) -> backend` and pins each
session to that backend forever. Where that map lives is the most
important production decision.

### `affinityStore.type: memory` — single-replica only

`HashMap` in the gateway process. Simple, fast, lost on pod
restart. **Only safe with `replicaCount: 1`** — multiple replicas
each have their own map, and a load balancer that fans out across
replicas will repeatedly re-pin sessions to different backends,
breaking Spark Connect's per-driver `SparkSession` invariant
(temp views, cached frames, conf settings disappear).

The chart enforces this at template time: `replicaCount > 1` with
`affinityStore.type: memory` causes `helm install` to fail.

Use case: dev / PoC / single-tenant on a single-pod box. Not for
HA.

### `affinityStore.type: redis` — recommended

The default. The map lives in Redis; all replicas read/write the
same dataset.

Two flavors:

**Bundled** (`redis.enabled: true`, default):

```yaml
redis:
  enabled: true
  persistence:
    enabled: true
    size: 1Gi
```

A single-replica StatefulSet with AOF persistence. Good for dev,
staging, and small-scale production. The dataset is tiny (one entry
per active Spark session, a few hundred bytes), 1Gi is over-sized
for any realistic workload.

Limitations:

* Single replica — Redis itself is a SPOF. Restart drops in-flight
  reads briefly; AOF means the dataset survives.
* No auth, no TLS — fine on a private namespace, not for shared
  clusters.

**External managed Redis** (`redis.enabled: false`):

```yaml
redis:
  enabled: false
affinityStore:
  type: redis
  redis:
    url: rediss://elasticache.aws.example:6379
    keyPrefix: scg-prod
    sessionTtlSecs: 3600
    opTtlSecs: 900
```

Use this for any production deployment that takes uptime seriously
— ElastiCache, Cloud Memorystore, Bitnami Redis Helm chart with
Sentinel, etc. The gateway only requires basic single-node
semantics; no Redis Cluster features are used.

What URL format is supported:

* `redis://host:port`
* `redis://:password@host:port`
* `redis://:password@host:port/<db-index>`
* `rediss://host:port` (TLS)

### What happens when Redis is unreachable

The gateway logs a `warn!` per failed Redis call and **degrades
gracefully** — lookups return `None`, binds quietly drop. The
visible effect is that session stickiness stops working; sessions
land on whatever the pool picks each time, the same behaviour you'd
see with the in-memory store on a single replica.

Service stays up; HA stickiness recovers as soon as Redis does.
This is a deliberate design choice — failing requests because
Redis is slow would be worse than a brief stickiness gap.

## Picking the auth mode

| `auth.type` | When to use |
|---|---|
| `none` | Trusted in-cluster network only. Every caller is `user_id="anonymous"`. The gateway's own metrics are usable but you cannot tell tenants apart. |
| `static` | Dev / single-team. Bearer tokens listed in `values.yaml`. Tokens are sealed-secret-able but rotation requires `helm upgrade`. |
| `jwt` | You already issue your own JWTs and have the verification key handy (PEM file or HMAC secret). Audience + issuer claims are checked. |
| `oidc` | Production with an external IdP (Okta, Auth0, Google, Keycloak). The gateway fetches JWKS from `discovery_url`, caches keys, refreshes on `kid` miss with a configurable floor. |

For all four, the gateway *replaces* the client-supplied
`UserContext.user_id` on the forwarded request with the verified
identity, so a malicious client cannot impersonate another user just
by lying in `UserContext`.

Production rule of thumb: `oidc` if you have an IdP, `jwt` if you're
issuing JWTs in-house, `static` for a closed dev setup, `none` only
for namespace-isolated clusters.

## Multi-tenant: picking a tenant resolver

> **Looking for a one-page multi-tenant setup walkthrough?** See
> [`multitenancy.md`](multitenancy.md) for the decision matrix,
> three complete sample configs, and migration paths. The sections
> below are the deep reference for each individual knob.

The routing key the gateway uses for session affinity is
`(tenant, user_id, session_id)`. The tenant prefix lets two
tenants share a gateway without their `session_id` namespaces
colliding — `(team-a, alice, sess-1)` is a different key from
`(team-b, alice, sess-1)`.

The `tenantResolver` config block tells the gateway *how* to figure
out which tenant each RPC belongs to. Pick by deployment shape:

| Deployment shape | `source` | `onMissing` | Notes |
|---|---|---|---|
| **Single-tenant** (default) | `from_claim` | `use_default` | The chart default. Tenant comes from the auth claim if there is one, otherwise falls back to `"default"`. Every RPC ends up in `tenant="default"`. |
| **JWT/OIDC multi-tenant** with a `tenant` claim | `from_claim` | `reject` | Tenant from JWT. Reject any RPC whose verified identity has no tenant claim — that's almost always an IdP misconfiguration in SaaS-style deployments. |
| **No auth but client cooperates** via metadata | `from_metadata` | `use_default` or `reject` | Tenant from a gRPC metadata header (default `x-tenant`). Use `reject` if every client *must* send the header; `use_default` is appropriate when missing-header clients are legitimate internal tools that should land in a default pool. |
| **Single-tenant, no auth, no headers** | `always_default` | n/a | Ignore claim and header; every RPC goes to `defaultName`. |

The chart's default values are the first row above, so a fresh
install behaves as a single-tenant deployment with zero config
changes.

**Migration note**: when you switch a running deployment from
`use_default` to `reject`, any client whose token doesn't carry a
tenant claim starts getting `Unauthenticated`. Roll the auth-side
change (issue tokens with tenant claims) before flipping
`onMissing` to `reject`. Watch
`scg_rpcs_total{code="Unauthenticated"}` during the change.

## Per-tenant pools

Building on the tenant resolver above: each tenant can route to a
*different* backend pool. A SaaS deployment isolates team-A's
queries from team-B's by giving them different Spark Connect
clusters; a per-team deployment shares one cluster.

The `backendDiscovery:` setting at the top of `values.yaml` is the
*default* pool. Add per-tenant overrides under `tenantPools`:

```yaml
backendDiscovery:
  type: static
  static:
    addresses:
      - "spark-shared-1.svc.cluster.local:15002"
      - "spark-shared-2.svc.cluster.local:15002"

tenantPools:
  onUnknownTenant: reject   # strict: only configured tenants
  overrides:
    team-a:
      type: static
      addresses:
        - "spark-team-a-1.svc.cluster.local:15002"
        - "spark-team-a-2.svc.cluster.local:15002"
    team-b:
      type: k8s
      namespace: spark-team-b
      serviceName: spark-connect
      port: 15002
```

`onUnknownTenant` is the routing equivalent of the resolver's
`onMissing` policy:

| Setting | Behaviour for a tenant not in `overrides` |
|---|---|
| `use_default` (default) | Route through the default pool. Single-pool baseline — every tenant shares the same backends. |
| `reject` | `PermissionDenied` to the client. Use for strict SaaS-style isolation; pairs naturally with `tenantResolver.onMissing=reject`. |

Each per-tenant pool gets the same active health-check treatment
as the default pool (when `healthCheck.enabled: true`) — health
probing runs independently per pool, so an unhealthy
team-a backend doesn't affect team-b routing.

**What back-compat looks like**: leaving `tenantPools.overrides`
empty (the default) means every tenant — including the `default`
tenant from the resolver — routes through the deployment's single
pool. This is the single-pool baseline. You opt into multi-tenant
routing by listing tenants you want isolated.

## Per-tenant rate limiting

When a single tenant can monopolize the shared backends — bursts
of session creation, runaway PySpark notebooks, malicious
clients — rate limiting protects the rest. The gateway implements
a token bucket per tenant (and optionally per user inside the
tenant); every RPC takes a token, RPCs that find an empty bucket
fail with `RESOURCE_EXHAUSTED`.

**Two store backends:**

* `rateLimit.store: memory` (default) — bucket state in each
  gateway replica. Simple, no extra infra, but the effective
  cluster-wide quota is `N × rpcsPerSecond` for N replicas.
* `rateLimit.store: redis` — atomic token bucket in Redis via a
  Lua script, shared across all replicas. Cluster-wide enforcement
  matches the configured rate exactly. See
  [`multitenancy.md`](multitenancy.md#distributed-rate-limiting)
  for fail-mode semantics (`onFailure: open | closed`) and the
  `scg_rate_limit_redis_errors_total` metric.

Off by default. Turn on with:

```yaml
rateLimit:
  enabled: true
  default:
    rpcsPerSecond: 100   # default tenant refill rate
    burst: 200           # max consecutive RPCs before throttling
  overrides:
    team-a:
      rpcsPerSecond: 500
      burst: 1000
    enterprise-tenant:
      rpcsPerSecond: 2000
      burst: 5000
      perUserRpcsPerSecond: 200   # tighter per-user cap inside the tenant
      perUserBurst: 400
```

The per-user dimension is opt-in. Leave `perUserRpcsPerSecond: 0`
(the default) to skip it; only the per-tenant bucket applies. Turn
it on when one tenant has many users and you want to keep any
single user from using the tenant's entire quota.

Tuning starting points:

| Workload shape | Default RPS | Burst |
|---|---|---|
| Many short Spark queries per session | 200 | 400 |
| Long ExecutePlan streams, infrequent setup RPCs | 30 | 60 |
| Mixed / unknown | 100 | 200 |

Reading `scg_rate_limit_rejected_total{tenant, scope}` tells you
whether the limits are biting and which scope (`tenant` vs `user`)
is the bottleneck. See [`observability.md`](observability.md) for
PromQL examples.

## Audit logging

The gateway always knows *what* happened (metrics) and *how it
happened* (logs/traces). Audit logging adds a third stream tuned for
*who did what, when* — the events compliance and security teams ask
for. Configure under `audit:` in `values.yaml`:

```yaml
audit:
  enabled: true              # default
  logSuccessfulRpcs: false   # default; turn on only under strict policy
```

Default events (`session.create`, `session.release`, `auth.failure`,
`rpc.error`) are emitted as JSON log lines with
`"target": "scg::audit"` — operators filter on that target in
Loki/Splunk to materialise an audit stream distinct from operational
logs. See [`observability.md`](observability.md#audit-logging) for the
event schema, sample queries, and the rationale for reusing the log
pipeline instead of adding a separate sink.

When to flip `logSuccessfulRpcs: true`:

* The deployment falls under a policy that requires per-call records
  (regulated industries, SOC 2 type-II audits scoped to data access).
* You have a log-retention budget that can absorb one extra event per
  successful RPC — the audit stream then scales with request rate.

When to leave it off (most deployments): metric counts on
`scg_rpcs_total{code="OK"}` already provide aggregate success, and
filling the audit stream with every Config call dilutes the signal
the four default events are meant to provide.

Disable the whole stream (`audit.enabled: false`) only in dev/local
environments — the per-event cost is one structured log line and
there is rarely a good reason to keep it off in production.

## Active backend health checks

By default the gateway routes to whatever its pool reports as
healthy: a static-pool member is always assumed up; a K8s
service-watch pool reflects the EndpointSlice. Neither catches a
backend that is *running* but *wedged* — the pod responds to TCP
but its gRPC server is stuck. Active probing closes that gap.

Turn it on:

```yaml
healthCheck:
  enabled: true
  intervalSecs: 5            # probe every 5s
  timeoutSecs: 2             # 2s deadline per probe
  unhealthyThreshold: 3      # 3 consecutive failures → evict
  healthyThreshold: 2        # 2 consecutive successes → re-admit
```

Each backend is probed via `grpc.health.v1.Health/Check` (the
[standard protocol](https://github.com/grpc/grpc/blob/master/doc/health-checking.md)).
A backend that doesn't ship the Health service responds with
`UNIMPLEMENTED`/`NOT_FOUND`; the gateway treats that as an
ambiguous signal and keeps the backend in rotation, since older
Spark Connect server builds don't register Health by default.

Defaults (5s × 3 = ~15s eviction window) are biased towards "don't
evict on a momentary glitch" rather than "evict instantly." Tighten
if your traffic is sensitive to a stuck-pod tail.

## Graceful shutdown

When the gateway pod gets SIGTERM (Helm rolling upgrade, K8s pod
eviction, scale-down), it does a two-step drain:

1. **Step 1 — readiness flips off.** `/readyz` starts returning
   503; the K8s Service controller removes the pod from its
   Endpoints within ~5s. New client traffic stops landing.
2. **Step 2 — wait for in-flight streams.** The gateway polls
   `scg_active_streams`. As long as ExecutePlan / ReattachExecute
   / AddArtifacts streams are still flowing, the gRPC server stays
   up. When `active_streams == 0` *or* `shutdown.deadlineSecs`
   elapses, the gRPC + admin servers shut down.

Configure the deadline:

```yaml
shutdown:
  deadlineSecs: 30
```

The chart's `terminationGracePeriodSeconds` is automatically set to
`deadlineSecs + 10` so K8s gives the gateway enough wall-clock to
finish draining before SIGKILL hits. **If you change one without the
other**, the smaller value wins:

* `deadlineSecs` smaller → drain force-quits early; in-flight
  streams seen as `Cancelled` by clients.
* `terminationGracePeriodSeconds` smaller → K8s SIGKILLs mid-drain;
  same client-side effect, plus `shutdown complete` log line never
  appears.

For long-running ExecutePlan workloads, raise both. The chart's
30s default is enough for the gRPC handshake teardown, but a
multi-minute Spark query in flight will be cut.

## Day 1: hardening

The defaults give you function; production needs all of:

1. **Switch off `auth.type: none`.** External traffic to a
   gateway running with `none` is a "we trust the network" bet that
   ages badly.
2. **Enforce the trust boundary** — see
   [the section below](#enforcing-the-trust-boundary). Auth at the
   gateway means nothing if clients can dial the backends directly.
3. **Use external Redis.** The bundled Redis is a SPOF. Pointing at
   ElastiCache / Memorystore costs little and removes the SPOF.
4. **Set resource requests/limits.** The chart defaults
   (100m CPU / 128Mi RAM) are reasonable for low traffic; size up
   to your real load.
5. **Turn on tracing if you have a collector.** The gateway exports
   OTLP/gRPC; logs already include the same correlation ID, but
   spans on a UI like Tempo or Jaeger make multi-hop investigations
   fast. See [`tracing.md`](observability.md#distributed-tracing)
   for the known limitation around inbound `traceparent`.
6. **Pin the image tag.** `image.tag: ""` resolves to the chart's
   `appVersion`. For production, set it to a specific digest:
   ```yaml
   image:
     repository: ghcr.io/<your-mirror>/spark-connect-gateway
     tag: "0.1.0@sha256:abc123..."
   ```
7. **Wire probes to your platform.** The chart already configures
   `/healthz` and `/readyz` on the admin port; if your platform
   does extra synthetic checks, point them at `/readyz` rather than
   the gRPC port (the gRPC port has no HTTP health endpoint, by
   gRPC convention).

## Enforcing the trust boundary

Every guarantee in this guide — auth, tenant isolation, rate
limits, audit — holds only while the gateway is the *only* path to
the backend Spark Connect servers. The backend trusts
`UserContext.user_id` as presented and keys sessions by
`(user_id, session_id)`; the gateway rewrites `user_id` with the
verified identity, which is only meaningful while the gateway is
the sole writer of it. Any pod that can dial a backend's gRPC port
directly can claim any identity and reach any session whose id it
knows or guesses.

Enforce the boundary at two layers; production should use both:

**Network layer — `backendNetworkPolicy`.** An opt-in NetworkPolicy
that restricts backend-pod ingress to the gateway pods:

```yaml
backendNetworkPolicy:
  enabled: true
  backendPodLabels:
    app: spark-connect-server
  grpcPort: 15002
  # NetworkPolicy is default-deny once it selects a pod: whitelist
  # any other legitimate ingress (executor→driver traffic on
  # operator-managed clusters, Spark UI) via additionalIngress.
```

Requires a CNI that enforces NetworkPolicy (Calico, Cilium, …; some
managed-cluster defaults do not).

**Auth layer — `backendToken`.** Start every backend with
`spark.connect.authenticate.token` (Spark 4.0+) and give the token
only to the gateway; the backend itself then rejects any client
that bypasses the gateway with `UNAUTHENTICATED`, without relying
on the CNI to enforce network policy:

```bash
# The backends and the gateway share the token via a Secret:
kubectl -n spark-connect create secret generic scg-backend-token \
  --from-literal=token="$(openssl rand -hex 32)"
```

```yaml
backendToken:
  enabled: true
  secretName: scg-backend-token
  secretKey: token
```

The gateway then presents `authorization: Bearer <token>` on every
gateway→backend request. Per-tenant pools can carry their own token
via `backendToken.tenantOverrides` (see `values.yaml`). The token
reaches the gateway as an env var from the Secret — it never
appears in the rendered ConfigMap. Rotation requires restarting
both the backends and the gateway pods.

The
[`e2e-trust-boundary`](../deploy/examples/e2e-trust-boundary/)
walkthrough proves the boundary end to end on a kind cluster: a
direct-to-backend connection is refused with `UNAUTHENTICATED`,
the same client succeeds through the gateway, and (negative
control) a gateway with the token disabled is refused too.

### The token is only as private as the `Config` RPC

The auth layer rests on clients not learning the token, and that
is not something the backend guarantees on its own. Spark's
`Config` RPC reads back any key the session holds — including
`spark.connect.authenticate.token`, since `SQLConf.mergeSparkConf`
copies every `SparkConf` entry into the session config and the
read path (`handleGet` / `handleGetOption` / `handleGetWithDefault`
/ `handleGetAll`) applies no denylist. In Spark's own
client-to-server model that leaks nothing: the client had to
present the token to connect at all. Behind a gateway it would,
because there the token is a gateway-only secret and users
authenticate with something else.

**The gateway therefore withholds that key from every `Config`
response**, unconditionally — a proxy cannot assume every backend
it forwards to has been patched. A client asking for it sees the
key as unset (and `GetAll` omits it), and the attempt is recorded
in the audit stream as `config.redacted`, which is worth alerting
on: a legitimate client has no reason to read the gateway↔backend
credential.

Two consequences worth planning around:

* Fixing this in Spark itself (so the backend never discloses the
  key) is the complementary half, and is being pursued upstream.
  Until then the gateway is the only thing standing between a
  client and the token, which is one more reason to keep the
  NetworkPolicy layer on.
* If you deploy a **different** proxy, or let clients reach
  backends directly for any reason, this reasoning does not carry
  over — the token is readable by anyone who can complete a
  `Config` call.

Credit: this was reported privately by an external reviewer, who
also verified it against 4.0.3, 4.1.2 and 4.2.0.

## Day 2: upgrades

`helm upgrade` re-renders templates and rolls the Deployment. Two
things that catch operators:

### ConfigMap changes don't always trigger pod rolls — but the chart
makes them.

The Deployment template includes a `checksum/config` annotation
that hashes the rendered ConfigMap. Any value change → annotation
change → pod template change → rolling restart. You should *not*
need to manually `kubectl rollout restart`.

### Redis affinity state survives rolling upgrades.

Because affinity lives in Redis (or the in-memory store, which
dies with the pod anyway), a `helm upgrade` rolling restart does
*not* reshuffle existing sessions — they stay pinned to their
original backends. The
[`session_ttl_secs`](https://github.com/.../scg-store-redis/src/lib.rs)
is refreshed on every read, so active sessions don't expire during
the rollout window.

Sessions that haven't been touched for `session_ttl_secs` *will*
expire and be re-picked on next access; tune this if your client
idle pattern is unusual.

### Rollback

`helm rollback scg <revision>` works as expected. The Redis
StatefulSet's PVC is **not** deleted on uninstall (intentional —
see below), so a rollback that brings back an older Helm release
finds its data.

If a config change broke things, prefer `helm rollback` over
`kubectl edit` of the Deployment — the latter doesn't roll the
ConfigMap back in lockstep.

## Uninstall

```bash
helm uninstall scg -n spark-connect
```

This removes the Deployment, Service, ConfigMap, ServiceAccount,
RBAC, and the Redis StatefulSet itself — but **leaves the Redis
PVC behind**. This is intentional: a re-install picks up where the
old one left off, and unwanted PVCs are easy to spot in
`kubectl get pvc`.

To wipe Redis state explicitly:

```bash
kubectl -n spark-connect delete pvc -l app.kubernetes.io/component=redis
```

If you used external Redis (`redis.enabled: false`), the data
isn't ours to clean up — flush the prefix yourself:

```bash
redis-cli -u "$REDIS_URL" --scan --pattern "scg:*" | xargs redis-cli -u "$REDIS_URL" del
```

## Validation: end-to-end test against your cluster

The repo ships [`crates/proxy/examples/ha_smoke.rs`](../crates/proxy/examples/ha_smoke.rs)
which validates the three multi-replica HA invariants (shared
state, failover after replica death, op-id reverse index across
replicas) against any reachable Redis. To run it against your
deployed cluster:

```bash
kubectl -n spark-connect port-forward svc/scg-redis 6399:6379 &
REDIS_URL=redis://127.0.0.1:6399 cargo run -p scg-proxy --example ha_smoke
```

This spins up *its own* in-process gateway pair against your
deployed Redis, which proves the Redis is well-configured and the
chart's value mapping is correct. It does not exercise the
deployed *gateway pods* themselves; for that, see
[`observability.md`](observability.md) for what to watch in
metrics during a soak test.
