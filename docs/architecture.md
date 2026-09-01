# Architecture

This document explains how the Spark Connect Gateway is designed and
laid out. It's the entry point for understanding the codebase — what
each crate does, why it exists as a separate crate, and how requests
flow through the system end-to-end.

For *operating* the gateway (install, tune, debug) read
[`deployment.md`](deployment.md) and [`runbook.md`](runbook.md)
instead. This doc is for **people reading the code**.

## 1. What problem the gateway solves

[Apache Spark Connect][spark-connect] decouples Spark clients from
Spark drivers: clients open a gRPC connection (`sc://host:15002`),
send a logical query plan, and receive results back. Each Spark
Connect server runs a Spark driver in its own JVM and keeps
`SparkSession` state — query history, registered temp views, cached
DataFrames — in driver-local memory.

That driver-local state is the fundamental constraint. Once a client
has a `SparkSession` on driver A, every subsequent RPC for that
session **must** reach driver A. Driver B has no idea what
`session_id=sess-1` means. This is fine for a single driver, but
turns into a coordination problem the moment you run more than one.

The gateway sits in front of N Spark Connect servers and solves
three things the open-source server intentionally leaves out:

1. **Session affinity** — every RPC for `(tenant, user, session_id)`
   reaches the same backend driver, no matter which gateway replica
   the client lands on.
2. **Authentication** — Spark Connect itself is unauthenticated;
   the gateway adds JWT/OIDC/static-token verification and stamps
   the verified identity onto every forwarded request.
3. **Multi-tenancy** — per-tenant pools (different drivers for
   different teams), per-tenant quotas (token-bucket rate limiting),
   audit trails, and a uniform observability surface.

The gateway is a **stateless gRPC proxy** in the sense that it holds
no Spark Connect application state itself. The only state it touches
is `(routing key) -> (backend address)`, which lives either in a
process-local map or in Redis.

[spark-connect]: https://spark.apache.org/docs/latest/spark-connect-overview.html

## 2. Top-level structure

The repo is a Cargo workspace of 15 crates. They split into four
concentric layers:

```
   ┌──────────────────────────────────────────────┐
   │ Layer 4: binary                              │
   │   gateway/                                   │
   ├──────────────────────────────────────────────┤
   │ Layer 3: request handler + observability     │
   │   proxy/    observability/  audit/           │
   ├──────────────────────────────────────────────┤
   │ Layer 2: routing decisions                   │
   │   routing/  tenant/  auth/  ratelimit/       │
   ├──────────────────────────────────────────────┤
   │ Layer 1: pluggable backends                  │
   │   store-memory/    store-redis/              │
   │   pool-static/     pool-k8s/                 │
   │   healthcheck/                               │
   ├──────────────────────────────────────────────┤
   │ Layer 0: foundations                         │
   │   genproto/  config/                         │
   └──────────────────────────────────────────────┘
```

Lower layers don't know about higher layers. Layer 2 defines the
traits (`Pool`, `AffinityStore`, `Authenticator`) that Layer 1
implements. Layer 3 composes them. Layer 4 reads YAML and assembles
the whole thing into a running server.

The split into 15 crates is **internal modularity, not a publishing
strategy**. Nothing in this workspace is meant to be consumed
standalone as a library — the gateway ships as a binary (or
container image / Helm chart). Treating each concern as its own
crate keeps build times reasonable, lets each module evolve with its
own tests, and forces the API boundaries to be explicit. See
[`multitenancy.md`](multitenancy.md) for the operator-facing view of
the same modules.

## 3. Request flow

A client RPC takes this path through the system:

```
  client ── gRPC ──▶ tonic server (proxy)
                    │
                    ├─ 1. authenticate metadata        → auth
                    ├─ 2. resolve tenant from identity → tenant
                    ├─ 3. take token bucket            → ratelimit
                    ├─ 4. derive SessionKey            → routing
                    ├─ 5. look up affinity binding     → routing → store
                    ├─ 6. (on miss) pick backend       → routing → pool
                    │                                          ↑
                    │                                   healthcheck filters
                    ├─ 7. record session.create audit  → audit
                    ├─ 8. forward to backend over gRPC → proxy
                    └─ 9. pump response stream back
```

Every step is in the proxy handler (`crates/proxy/src/handler.rs`).
The handler is one big match-on-RPC dispatch — one method per Spark
Connect RPC — but every method does the same eight steps before
forwarding.

A more precise version of the diagram, with the actual crate seams:

| Step | Crate(s) | What happens |
|---|---|---|
| accept | `proxy` (`tonic` server) | gRPC frame demuxed into a typed Request<T> |
| auth | `auth` | `AuthInterceptor::authenticate(metadata)` returns `Arc<Identity>` or `Status::unauthenticated` |
| tenant | `tenant` | `TenantResolver::resolve(metadata, &identity)` returns a non-empty tenant string |
| limit | `ratelimit` | `RateLimiter::check(tenant, user)` returns `Status::resource_exhausted` if the bucket is empty |
| key | `routing` | `SessionKey { tenant, user_id, session_id }` constructed from verified identity + request body |
| lookup | `routing` → `store-memory`/`store-redis` | `AffinityStore::lookup_session(&key)` returns the bound backend if any |
| place (on miss) | `routing` → `pool-static`/`pool-k8s` (optionally wrapped by `healthcheck`) | `SelectionStrategy::select()` chooses one of `Pool::members()` |
| bind (on miss) | `routing` → store | `AffinityStore::bind_session_if_absent` records the decision atomically |
| audit | `audit` | `session.create` event emitted on fresh binding only |
| forward | `proxy` → `Dialer` → tonic client | RPC forwarded with `x-request-id` and `traceparent` propagated |

Long-running streams (`ExecutePlan`, `ReattachExecute`, `AddArtifacts`)
follow the same shape but the response phase is a `Stream` that the
proxy pumps until the backend completes or errors. There's also a
parallel **op-id reverse index** (`AffinityStore::lookup_op`) so that
`ReattachExecute` can reach the original driver even when the
client's session_id has expired from the affinity table — the
operation_id is the more durable handle once a stream is running.

## 4. Module-by-module

### 4.1 `genproto` — generated protobuf bindings

Pure boilerplate. `build.rs` invokes `tonic-prost-build` on the
vendored `.proto` files under `proto/spark/connect/` and emits the
generated Rust into `crates/genproto/src/pb/`. Every other crate
that touches Spark Connect messages imports from `scg_genproto::pb`.

**Why a separate crate**: Regenerating the bindings on every change
to every consumer crate would be wasteful — keeping them in their
own crate means a single `tonic-prost-build` run per workspace
rebuild.

### 4.2 `config` — YAML configuration

Defines the schema for the `config.yaml` file the gateway reads at
startup. Uses `serde` derives; every operator-facing knob (auth
mode, backend discovery, affinity store choice, rate-limit settings,
audit settings, …) is a field on a typed struct.

Two-form schema for backend discovery is intentional: a shorthand
`backends: [host:port, …]` for the simple case and a tagged
`backend_discovery: { type: k8s, … }` for everything else. The
gateway main translates the parsed config into runtime types from
the other crates.

**Design choice**: `config` knows nothing about Spark Connect or
gRPC; it's just typed YAML. This keeps the config layer reusable
during future schema migrations and avoids accidentally coupling
"how do we describe a JWT setting in YAML" with "how does the JWT
authenticator actually work."

### 4.3 `routing` — the routing core

This is where the central abstractions live. Three traits + two
struct types form the contract every other crate plugs into:

* **`Pool`** — provides pool *membership*: which backends currently
  exist and are believed healthy, as `BackendMember`s (address plus
  labels/weight metadata). Implementations: static list, K8s
  Endpoints watcher. The trait is intentionally tiny (`members`,
  `mark_unhealthy`) so new pool implementations are cheap.

* **`SelectionStrategy`** — chooses which member a *fresh* session
  is placed on. One strategy instance per pool (paired in a
  `PoolEntry`), consulted only on the placement path — affinity
  hits and the operation-id index never go through it, so a
  misbehaving strategy can skew new placements but cannot break
  live sessions. Shipping implementation: round-robin; weighted,
  least-sessions, and metadata-aware strategies are the planned
  extensions behind this seam.

* **`AffinityStore`** — persists the `SessionKey -> backend address`
  binding so subsequent RPCs reach the same driver. Async trait
  because the production impl (Redis) is a network call.
  Implementations: in-process `HashMap` (single replica) or Redis.

* **`TenantRouter`** — maps a tenant string to its `PoolEntry`
  (pool + strategy). Single-tenant deployments use exactly one
  entry (often `"default"`).

* **`Router`** — the glue. `Router::resolve_session(&key)` does the
  lookup-then-pick-then-bind dance, returning either an existing
  binding or a freshly-bound one. This is the only entry point
  the proxy uses for routing.

* **`SessionKey`** — the three-tuple `(tenant, user_id, session_id)`.
  Adding the tenant prefix lets two tenants reuse the same
  `session_id` without colliding.

**Why traits, not concrete types**: every routing decision in the
proxy goes through `Router`, which only sees trait objects. Swapping
in-process affinity for Redis is a config-file change, not a code
change. Same for static vs K8s pools.

### 4.4 `store-memory` and `store-redis`

Two `AffinityStore` impls. `MemoryStore` is a pair of `RwLock<HashMap>`s
— one for sessions, one for the op-id reverse index — that lives
in the gateway process. `RedisStore` uses Redis hashes with TTLs.

**Why two**: single-replica deployments don't need Redis; multi-replica
ones do. The trait boundary is narrow enough that a misbehaving
store doesn't ripple into the routing layer. `RedisStore::lookup_session`
treats a Redis error as a cache miss with a warning log — graceful
degradation to pool-only routing on Redis outage. See
[`runbook.md`](runbook.md) for the operational implications.

### 4.5 `pool-static` and `pool-k8s`

Two `Pool` impls. `StaticPool` is a fixed `Vec<String>` decided at
startup. `K8sPool` spawns
a `kube-rs` watcher that subscribes to a Service's `Endpoints`
object and updates the live backend list whenever pods change.

The K8s watcher runs as a tokio task spawned at startup. When the
watcher sees an `Added` event for a new endpoint, it grabs the
write lock on the pool's backend list and appends; `Deleted`
removes. The proxy never blocks on the watcher.

### 4.6 `healthcheck`

`HealthAwarePool` is a wrapper that takes any `Pool` and adds active
gRPC health probing. It spawns a probe loop that calls
`grpc.health.v1.Health/Check` on every backend at a configurable
interval. After `unhealthy_threshold` consecutive failures it stops
reporting that backend from `members()`; after `healthy_threshold`
successes it re-admits.

**Design choice**: the trait is just `Pool`, so `HealthAwarePool`
composes with both static and K8s pools without either knowing
about health. The probe runs out-of-band; the hot path never blocks
on it.

**Trade-off**: a backend that returns `Unimplemented` for the
health protocol (older Spark Connect servers don't ship the
standard health service) gets treated as "ambiguous" — not counted
as a failure — so the gateway doesn't false-evict every backend
on rollout. Mass-eviction during a misconfigured probe is the
worse failure mode.

### 4.7 `auth`

Defines the `Authenticator` trait and ships four impls:

| Impl | Used when |
|---|---|
| `AnonymousAuthenticator` | Trusted in-cluster networks, no auth needed |
| `StaticTokenAuthenticator` | Dev / CI / static token allowlist |
| `JwtAuthenticator` | Local public key verification (PEM file, inline PEM, HMAC secret) |
| `OidcAuthenticator` | Remote JWKS endpoint with auto-refresh |

Each returns a verified `Identity { user_id, tenant, groups }`. The
proxy injects `user_id` into `UserContext.user_id` on the forwarded
request, overwriting whatever the client claimed — clients can't
impersonate one another.

`AuthInterceptor` wraps the chosen authenticator and is what the
proxy actually calls per-RPC. It exists to keep `auth` out of any
direct dependency on `tonic`'s middleware machinery.

### 4.8 `tenant`

`TenantResolver` takes a verified `Identity` plus the request
metadata and returns a tenant string. Three sources:

* **`FromClaim`** — read `Identity.tenant` (set by the
  authenticator from the JWT/static-token config).
* **`FromMetadata`** — read a gRPC metadata header (default
  `x-tenant`). For deployments where auth is disabled but the
  client cooperates.
* **`AlwaysDefault`** — every RPC ends up in `default`. The
  single-tenant baseline.

Combined with an `OnMissing` policy (`UseDefault` or `Reject`), this
covers permissive multi-tenant, strict SaaS-style multi-tenant, and
single-tenant deployments in one resolver.

### 4.9 `ratelimit`

Per-tenant + optional per-user token bucket. Two backends, both
exposed via the same `RateLimiter` enum:

* **In-memory** — `MemoryLimiter` keeps the buckets in a `parking_lot::Mutex<HashMap>`.
  Fast (~tens of nanoseconds per check) but per-replica: a 3-replica
  deployment with `rpcs_per_second=100` actually admits 300 RPS
  cluster-wide.

* **Redis** — `RedisLimiter` uses a single atomic Lua script
  (`HMGET` + refill + `HMSET`/`EXPIRE`) so all gateway replicas share
  one bucket. Cluster-wide enforcement matches the configured rate
  exactly. Costs one Redis round trip per RPC; fail-mode is
  configurable (admit-on-error vs reject-on-error).

The Lua script (`TOKEN_BUCKET_SCRIPT`) implements token bucket
semantics atomically: refill based on wall-clock delta since last
update, take a token if any are available, write back the new state.
Plain `EVAL` — no `redis-cell` module needed, so it works on managed
Redis services (ElastiCache, MemoryStore, Upstash) that don't allow
custom modules.

### 4.10 `audit`

Emits five event types through `tracing::info!` with a dedicated
`target = "scg::audit"`:

| Event | When |
|---|---|
| `session.create` | First time a `(tenant, user, session_id)` binding is recorded |
| `session.release` | Client called `ReleaseSession` |
| `auth.failure` | Auth interceptor rejected the RPC |
| `rpc.error` | A handler returned non-OK (Cancelled filtered out) |
| `rpc.ok` | Optional, off by default — every successful RPC |

**Why not a separate sink trait**: the JSON log formatter installed
by `observability` already picks these up because they're plain
`tracing` events. Operators filter by target in their log aggregator
(Loki, Splunk) to get an auditable stream distinct from operational
logs. Avoiding a separate "AuditSink" trait keeps deployment simple
— one log pipeline, one fewer thing to wire.

### 4.11 `observability`

Three things in one crate because they share the per-RPC
instrumentation seam:

* **`Metrics`** — Prometheus `Registry` + handles for the per-RPC
  counters and histograms. The `RpcGuard` RAII type records on
  Drop so a handler that bails early still produces a metric entry.
* **Tracing setup** — OpenTelemetry exporter setup + a JSON
  formatter for structured logs.
* **Admin HTTP server** — small `hyper` server that exposes
  `/metrics` (Prometheus format), `/healthz`, and `/readyz`.

`ReadinessProbe` is a shared `Arc<AtomicBool>` that the main loop
flips off during shutdown, so K8s drains the pod from the Service
before the gRPC server stops accepting new RPCs.

### 4.12 `proxy`

The gRPC handler. `SparkConnectProxy` implements `SparkConnectService`
(from `genproto`) and delegates routing to `Router`, auth to
`AuthInterceptor`, etc. Every RPC method follows the same template:

```rust
async fn config(&self, req: Request<ConfigRequest>) -> Result<…, Status> {
    let mut guard = self.metrics.rpc_guard("Config");
    let rid = request_id();
    let span = rpc_span("Config", &rid, req.metadata());
    let result = async {
        let (identity, tenant) = self.authenticate_and_resolve(req.metadata(), &rid, "Config").await?;
        // …derive SessionKey, call resolve_session_audited, forward to backend…
    }.instrument(span).await;
    finalise_rpc(&mut guard, &result, &self.audit, &audit_ctx, &rid, "Config");
    result
}
```

There's a separate `Dialer` that caches `tonic::Channel`s keyed by
backend address — connecting per-RPC would saturate the system on
session-create bursts.

### 4.13 `gateway`

The binary. `main.rs` is essentially a long config-to-runtime
translator: read YAML, build each component from the lower crates,
wire them together, install signal handlers, start the gRPC server.
Almost no logic of its own — the whole point of the crate split
is that the binary is mostly composition.

## 5. Key design decisions

### 5.1 Session affinity routing key includes tenant

`SessionKey = (tenant, user_id, session_id)` rather than
`(user_id, session_id)` (as Spark Connect itself uses). This is the
single most important multi-tenant design decision — without the
tenant prefix, two tenants picking the same `session_id` would
share a backend binding, defeating per-tenant pool routing.

The tenant becomes the first hash-bucket segment in
`store-redis` keys too: `{prefix}:s:{tenant}|{user}|{session}`.

### 5.2 Stateless gateway, optional shared state

The gateway process itself holds only an in-memory affinity cache.
With `affinityStore.type: memory`, multi-replica deployments are
incoherent (each replica picks its own backends for the same
session) — fine for single-replica setups. With
`affinityStore.type: redis`, all replicas share the same
`SessionKey -> backend` table, so a client can be steered to any
replica and still reach the right driver.

This is a deliberate trade-off: the gateway is **infrastructure**, so
the operator picks the consistency model based on their replica
count and HA requirements.

### 5.3 Pluggable everything via traits

Every dimension where deployments diverge is a trait:

* Backend discovery → `Pool`
* Affinity persistence → `AffinityStore`
* Authentication → `Authenticator`

This means adding (say) a Consul-watched pool or a Postgres-backed
affinity store is a new crate + an extra config branch in `gateway`,
not a refactor of the proxy. None of the higher-layer code needs to
know.

### 5.4 Identity injection, never trust the client

The proxy *overwrites* `UserContext.user_id` on every forwarded
request with the verified identity from the authenticator. Spark
Connect backend servers use `user_id` as part of their session key,
so trusting the client value would let one caller impersonate
another's session. The gateway is the only place this trust
boundary is enforced.

### 5.5 Forward-everything proxy

The proxy forwards every Spark Connect RPC verbatim — it never
parses or modifies the protobuf body except for stamping
`UserContext.user_id`. This means the gateway stays compatible with
whatever new RPC fields upstream Spark Connect adds, with no code
change needed. The cost is that new RPC types added by upstream
require a code change to be wired through (no generic passthrough),
but the Spark Connect surface is small enough that this is fine.

### 5.6 Audit through `tracing`, not a separate sink

Audit events ride the same `tracing` infrastructure as operational
logs, just filtered by `target`. Operators get one log pipeline to
manage; the audit stream is `target:"scg::audit"` in Loki / Splunk.
The alternative (a dedicated `AuditSink` trait) would add a separate
delivery path with its own buffering and durability semantics.

### 5.7 Per-tenant rate limit, optional Redis store

Two backends share the same `RateLimiter::check(tenant, user)`
signature. Operators pick `memory` for development and most
production deployments; `redis` only when strict cluster-wide
enforcement matters more than the round-trip latency.

## 6. Running the gateway

### 6.1 Build from source

```bash
cargo build --release
./target/release/gateway --config config.yaml
```

The release binary is a single executable; container images
copy it into a `gcr.io/distroless/cc` base.

### 6.2 Minimal `config.yaml`

```yaml
bind_addr: ":15003"        # where the gateway listens for client gRPC
admin_addr: ":9090"        # /metrics, /healthz, /readyz (set null to disable)

backends:                  # shorthand: static list
  - "spark-connect-1.svc.cluster.local:15002"
  - "spark-connect-2.svc.cluster.local:15002"

affinity_store:
  type: memory             # in-process; single-replica only

auth:
  type: none               # anonymous; trusted networks only

shutdown:
  deadline_secs: 30        # drain in-flight streams up to this before SIGKILL
```

Point a PySpark client at it:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15003").getOrCreate()
spark.range(10).count()
```

### 6.3 Helm chart (Kubernetes)

The chart at `deploy/helm/scg/` ships a Deployment + Service +
ConfigMap + ServiceAccount + RBAC (when K8s discovery is enabled)
+ optional bundled Redis StatefulSet. See
[`deployment.md`](deployment.md) for the full operator-facing
walkthrough; the chart's own
[`deploy/helm/scg/README.md`](../deploy/helm/scg/README.md) lists
every value.

```bash
helm install scg ./deploy/helm/scg -n spark-connect --create-namespace
```

### 6.4 Configuration reference

The complete YAML schema is in `crates/config/src/lib.rs` — every
field has rustdoc explaining what it does. The Helm `values.yaml`
re-exposes the same fields with camelCase keys (chart convention)
and the template at `deploy/helm/scg/templates/configmap.yaml`
translates between the two. The decision matrices for each section
live in `deployment.md`:

| Config block | Decision guide |
|---|---|
| `backend_discovery` | [Picking the backend discovery mode](deployment.md#picking-the-backend-discovery-mode) |
| `affinity_store` | [Picking the Redis backing](deployment.md#picking-the-redis-backing) |
| `auth` | [Picking the auth mode](deployment.md#picking-the-auth-mode) |
| `tenant_resolver` + `tenant_pools` | [Multi-tenancy guide](multitenancy.md) |
| `rate_limit` | [Per-tenant rate limiting](deployment.md#per-tenant-rate-limiting) |
| `audit` | [Audit logging](deployment.md#audit-logging) |
| `tracing` | [Distributed tracing](observability.md#distributed-tracing) |
| `health_check` | [Active backend health checks](deployment.md#active-backend-health-checks) |
| `shutdown` | [Graceful shutdown](deployment.md#graceful-shutdown) |

## 7. Where to look next

Reading order, depending on what you're after:

| Goal | Start here |
|---|---|
| Read the routing core | `crates/routing/src/lib.rs` |
| See the request flow end-to-end | `crates/proxy/src/handler.rs` (the per-RPC methods) |
| Understand startup | `crates/gateway/src/main.rs` |
| Add a new backend pool type | Implement `Pool` (see `crates/pool-static/src/lib.rs` for the simplest example) |
| Add a new affinity store | Implement `AffinityStore` (see `crates/store-memory/src/lib.rs`) |
| Add a new auth method | Implement `Authenticator` (see `crates/auth/src/anonymous.rs` for the smallest example) |
| Trace a metric to its source | `crates/observability/src/metrics.rs` defines every `scg_*` series |
| Trace an audit event to its source | `crates/audit/src/lib.rs` — one method per event type |
| Reproduce perf numbers | `crates/proxy/examples/load.rs` — six scenarios documented in [`perf-baseline.md`](perf-baseline.md) |

The implementation plan that drove the build-out lives in the
sibling repo at
[`../plans/IMPLEMENTATION-PLAN-OSS-Spark-Connect-Gateway.md`](../../plans/IMPLEMENTATION-PLAN-OSS-Spark-Connect-Gateway.md)
— it has the phase-by-phase task list and the rationale for
several "why is it like this" decisions that aren't visible from
the code alone.
