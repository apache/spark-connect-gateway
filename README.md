# spark-connect-gateway

A stateless gRPC proxy that fronts a pool of [Apache Spark Connect][1]
servers, providing session affinity, multi-tenant routing, auth, and
observability features the open-source server intentionally leaves out.

> Rust workspace built with [`tonic`](https://github.com/hyperium/tonic).
> Production-ready surface: JWT/OIDC auth, K8s service-watch discovery,
> Redis-backed multi-replica HA, per-tenant routing + rate limiting,
> structured audit logging, Prometheus metrics, OpenTelemetry tracing,
> Helm chart with sensible defaults.

## What it does

- Accepts `sc://` traffic on `:15003`.
- Forwards every Spark Connect RPC (`ExecutePlan`, `AnalyzePlan`, `Config`,
  `AddArtifacts`, `Interrupt`, `ReattachExecute`, `ReleaseExecute`,
  `ReleaseSession`, `FetchErrorDetails`, `CloneSession`, `ArtifactStatus`,
  `GetStatus`) to a chosen backend.
- Pins each `(user_id, session_id)` to the same backend for the lifetime of
  the session — required because open-source Spark Connect keeps
  `SparkSession` state in driver-local memory.
- Routes `ReattachExecute` / `ReleaseExecute` / `Interrupt` by `operation_id`
  via a reverse index, so reconnecting clients reach the backend that owns
  the operation even if the session affinity has expired.
- Round-robins new sessions across a static list of backend addresses.

## Why a gateway?

Open-source Spark Connect ships an excellent client-server protocol but
deliberately leaves multi-instance coordination out of scope. For anything
beyond a single Spark driver — multi-tenant platforms, HA, fleet-level
observability — you need a layer in front of the servers. See
[`OPEN_SOURCE_SPARK_CONNECT_GATEWAY_ANALYSIS.md`][3] in the plan repo for
the full motivation and competitive landscape.

## Why Rust?

- gRPC streaming proxy is exactly Rust's sweet spot — async/await + Tokio
  yield lower memory footprint and tail latency than Go for sustained
  `ExecutePlan` streams.
- `hyper` has best-in-class HTTP/2 trailing-header support, which gRPC
  requires.
- Aligns with [`Kimahriman/spark-connect-proxy`][5], the only existing OSS
  Spark-Connect-native proxy.

## Quick start

### Build and test

```bash
cargo build --workspace
cargo test --workspace
```

### Run locally against a Spark Connect server

1. Start a Spark Connect server on `localhost:15002` (see
   [`test/integration/README.md`](test/integration/README.md) for a Docker
   one-liner).

2. Write `config.yaml`:

   ```yaml
   bind_addr: ":15003"
   backends:
     - "127.0.0.1:15002"
   ```

3. Run the gateway:

   ```bash
   cargo run --bin gateway -- --config config.yaml
   ```

4. Point a Spark Connect client at it:

   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.remote("sc://localhost:15003").getOrCreate()
   spark.range(10).count()  # → 10
   ```

### Observability

The gateway exposes a Prometheus `/metrics` endpoint, plus
`/healthz` and `/readyz` probes, on a separate `admin_addr`
(default `:9090`). Set `admin_addr: null` to disable.

Metric set (snake_case, `scg_` prefix, label cardinality bounded):

| Metric | Type | Labels | What |
|---|---|---|---|
| `scg_rpcs_total` | counter | `rpc`, `code` | Per-RPC totals tagged by final gRPC status code |
| `scg_rpc_duration_seconds` | histogram | `rpc` | Gateway-side end-to-end duration |
| `scg_auth_failures_total` | counter | `reason` | Failed auth (`missing_token`, `invalid_token`, `expired`, `unknown_kid`, `unknown`) |
| `scg_backend_pool_size` | gauge | — | Current healthy-backend count |
| `scg_active_streams` | gauge | — | In-flight streaming RPCs (`ExecutePlan`, `ReattachExecute`, `AddArtifacts`) |

Per-RPC structured logs include a correlation ID (`rid`); the same
ID is forwarded to the backend via `x-request-id` metadata so backend
logs can be joined.

#### Distributed tracing (OTLP)

Off by default. Add a `tracing:` block to enable OpenTelemetry span
export to an OTLP/gRPC collector:

```yaml
tracing:
  endpoint: "http://otel-collector:4317"
  service_name: "spark-connect-gateway"
  sample_ratio: 1.0           # ParentBased(TraceIdRatioBased(N))
  export_timeout_secs: 10
```

Each RPC opens an `info`-level `scg_rpc` span carrying
`rpc_method`, `rpc_system="grpc"`, `rpc_service`, and the same
`scg_rid` correlation ID surfaced in the JSON logs. Inbound W3C
`traceparent` metadata becomes the parent of that span; the gateway
re-injects its own `traceparent` (alongside `x-request-id`) on the
outbound request, so a Spark Connect server that participates in
tracing joins the same trace.

When the `endpoint` is omitted or the whole `tracing:` block is
absent, the gateway runs with structured JSON logs only — no OTel
SDK is initialized.

> **Known limitation (root spans only).** Today, only RPCs where the
> gateway is the trace root (no inbound `traceparent`) export their
> spans reliably end-to-end via OTLP. RPCs that arrive *with* a W3C
> `traceparent` set by an upstream caller continue to log to JSON
> (with the inbound `scg_rid` correlation ID), but their `scg_rpc`
> span is dropped before reaching the OTLP exporter due to a
> versioning mismatch in the `tracing-opentelemetry` ↔
> `opentelemetry_sdk` interaction around `Context::with_remote_span_context`.
> Tracking upstream — distributed-trace continuity from upstream
> caller → gateway → backend will return once the SDK fix lands; the
> gateway → backend hop already injects `traceparent` correctly so
> the backend half of the trace will start working as soon as the
> gateway-side path is fixed.

Scrape config example for Prometheus:

```yaml
scrape_configs:
  - job_name: spark-connect-gateway
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        regex: admin
        action: keep
```

### Authentication

By default the gateway runs without authentication (every caller is
`user_id: anonymous`). Production deployments configure one of three
authenticators:

```yaml
# Bearer-token allowlist (dev / single-team):
auth:
  type: static
  tokens:
    - { token: "alice-secret", user_id: "alice", tenant: "team-a", groups: ["devs"] }
    - { token: "bob-secret",   user_id: "bob" }
```

```yaml
# JWT signed by a known IdP, verified against a local public key:
auth:
  type: jwt
  algorithms: ["RS256"]
  issuer: "https://idp.example.com"
  audience: "spark-connect-gateway"
  key:
    kind: pem_file
    path: /etc/gateway/idp-pub.pem
```

```yaml
# OIDC / JWKS — gateway fetches keys from the IdP, refreshes on rotation:
auth:
  type: oidc
  algorithms: ["RS256"]
  discovery_url: "https://idp.example.com/.well-known/openid-configuration"
  audience: "spark-connect-gateway"
```

In all three cases the gateway *replaces* whatever `user_id` the client
declares in `UserContext` with the verified identity from the
authenticator — clients cannot impersonate other users.

Clients pass the credential via gRPC metadata:

```python
# PySpark Spark Connect client picks up the token from the URI:
spark = SparkSession.builder.remote(
    "sc://localhost:15003/;token=alice-secret"
).getOrCreate()
```

Authenticating clients at the gateway only helps if clients cannot
dial the backends directly. For that, start the backends with
`spark.connect.authenticate.token` (Spark 4.0+) and give the token
only to the gateway:

```yaml
# Presented as `authorization: Bearer …` on every gateway→backend
# request; per-tenant pools can override it. Backends then reject
# clients that bypass the gateway with UNAUTHENTICATED.
backend_token:
  kind: env            # inline | env | file
  name: SCG_BACKEND_TOKEN
```

See [Enforcing the trust
boundary](docs/deployment.md#enforcing-the-trust-boundary) for the
full story (including the complementary NetworkPolicy).

### Sharing affinity across gateway replicas (Redis)

The default in-memory affinity store works only for a single gateway
replica — every replica keeps its own `(user_id, session_id) -> backend`
table, so a client that lands on replica B after replica A bound it
to backend X gets re-bound to a different backend, breaking the
Spark Connect per-driver session invariant.

To run the gateway with `replicas > 1` (e.g. behind a Kubernetes
`Service` for HA), point the gateway at a Redis instance:

```yaml
bind_addr: ":15003"
backends: ["spark-connect-1:15002", "spark-connect-2:15002"]

affinity_store:
  type: redis
  url: "redis://redis-cluster:6379"        # supports redis://:pw@host:6379/2
  key_prefix: "scg-prod"                   # default "scg"
  session_ttl_secs: 3600                   # default 1h, refreshed on reads
  op_ttl_secs: 900                         # default 15min
```

Both stickiness invariants are preserved across replicas:

* `bind_session_if_absent` uses Redis `SET … NX EX` — when two
  replicas race on the same session, exactly one wins; the loser
  reads the winner's value back and routes to the same backend.
* The op-id reverse index (used by `ReattachExecute` /
  `ReleaseExecute` / `Interrupt`) lives in `{prefix}:o:{op_id}`,
  so a client reconnecting through a different replica still
  reaches the original driver.

If Redis becomes unreachable, the gateway logs `warn!` per failed
operation and degrades to pool-only routing — sessions land on
whatever backend the pool picks each time, which is exactly the
single-replica in-memory behaviour. Service remains available; HA
stickiness recovers as soon as Redis does.

`affinity_store` defaults to `type: memory` (no entry needed),
matching the single-replica baseline.

#### Verifying HA locally

`crates/proxy/examples/ha_smoke.rs` spins up two real
`SparkConnectProxy` instances backed by one Redis and a shared pool
of fake backends, then drives RPCs through different replicas to
prove three invariants:

* **Shared state** — a session bound through replica A resolves to
  the same backend through replica B.
* **Failover** — after replica A is killed, the same session through
  replica B still hits the original backend.
* **Op-id reverse index across replicas** — `ReattachExecute(op_id,
  session_id="different")` arriving at replica B (after A is gone)
  still reaches the backend that ran the original `ExecutePlan`.

Run with a Redis listening on `:6399` (or override via `REDIS_URL`):

```bash
redis-server --port 6399 --daemonize yes
cargo run -p scg-proxy --example ha_smoke
```

Exits zero on success; panics with a descriptive assertion message
on any failure, so a CI script can wrap it directly.

### Deploy on Kubernetes (Helm)

A Helm chart is shipped at [`deploy/helm/scg/`](deploy/helm/scg/).
Quickstart:

```bash
helm install scg ./deploy/helm/scg \
  --namespace spark-connect --create-namespace
```

Defaults give you 2 gateway replicas + a bundled Redis StatefulSet
(AOF-persisted), with a static backend list pointing at
`spark-connect-{1,2}.svc.cluster.local:15002`. Switch to K8s
service-watch discovery, JWT auth, OTLP tracing, or an external
managed Redis with a few values flips — see the chart's
[`values.yaml`](deploy/helm/scg/values.yaml) and
[`README.md`](deploy/helm/scg/README.md) for the full reference.

The chart fails template-time if you set `replicaCount > 1` together
with `affinityStore.type: memory`, since that combination silently
breaks Spark Connect's per-driver session invariant — `redis` is the
default for exactly that reason.

### Operator docs

For day-2 operations, see:

* [`docs/architecture.md`](docs/architecture.md) — design overview
  for people reading the code: layered crate structure, request
  flow end-to-end, what each of the 15 crates does and why it's
  its own crate, key design decisions, where to look first.
* [`docs/deployment.md`](docs/deployment.md) — from-zero deployment
  runbook: prerequisites, install, hardening, upgrades, uninstall.
* [`docs/multitenancy.md`](docs/multitenancy.md) — multi-tenant
  setup guide: decision matrix, three sample configs (permissive,
  strict, single-tenant), migration paths.
* [`docs/observability.md`](docs/observability.md) — every `scg_*`
  metric explained, PromQL examples, log line anatomy, distributed
  tracing guide, suggested alerts.
* [`docs/runbook.md`](docs/runbook.md) — symptom → diagnosis → fix
  for the failures you'll actually hit (CrashLoopBackOff, `/readyz`
  503, broken stickiness, Redis outage, auth failure spikes, K8s
  RBAC, latency regressions).
* [`docs/perf-baseline.md`](docs/perf-baseline.md) — performance
  baseline numbers (unary throughput, streaming concurrency,
  health-check overhead, drain semantics under load) reproducible
  via the load harness in `crates/proxy/examples/load.rs`.

### Run on Kubernetes (auto-discovery)

End-to-end walkthroughs (each runs on a fresh kind cluster):

* [`deploy/examples/e2e-smoke/`](deploy/examples/e2e-smoke/) —
  Helm install, real Spark Connect servers, PySpark client through
  the gateway. Verifies session affinity and audit events. Start
  here.
* [`deploy/examples/e2e-scale-test/`](deploy/examples/e2e-scale-test/) —
  scale the backend Deployment up and down at runtime; verify the
  K8s `Endpoints` watcher in `pool-k8s` picks up the changes
  without a gateway restart.
* [`deploy/examples/e2e-multi-replica-redis/`](deploy/examples/e2e-multi-replica-redis/) —
  two gateway replicas backed by the bundled Redis StatefulSet;
  verify affinity bindings survive a gateway pod restart because
  they live in Redis, not in pod memory.
* [`deploy/examples/e2e-trust-boundary/`](deploy/examples/e2e-trust-boundary/) —
  backends enforce `spark.connect.authenticate.token` and only the
  gateway holds it; verify a direct-to-backend connection is refused
  with `UNAUTHENTICATED` while the same client succeeds through the
  gateway.

For production-style manifests using the upstream
[`apache/spark-kubernetes-operator`][4], see
[`deploy/examples/spark-connect-server/`](deploy/examples/spark-connect-server/).

Once those servers (and a fronting `Service`) exist, point the gateway at the
Service's `Endpoints` and let the gateway pick up backends automatically:

```yaml
bind_addr: ":15003"
backend_discovery:
  type: k8s
  namespace: spark-connect
  service_name: spark-connect
  port: 15002
```

The gateway watches the `Endpoints` object via `kube-rs`. When pods are added,
removed, or restarted, the gateway's backend list updates within seconds —
no `kubectl rollout` of the gateway, no config edit. The gateway pod needs a
`ServiceAccount` with `get`, `list`, and `watch` on `endpoints` in the target
namespace; the Helm chart at `deploy/helm/scg/` wires up the `ServiceAccount`
and `Role` automatically.

## Architecture

```
client (sc://) ──▶ gateway ──┬──▶ Spark Connect server #1
                             ├──▶ Spark Connect server #2
                             └──▶ Spark Connect server #N
```

- **No state in the gateway process** beyond an in-memory affinity cache
  (single-replica deployments). Multi-replica deployments swap in
  `scg-store-redis` so all replicas share the same affinity table.
- **No interpretation of Spark Connect plans.** The gateway forwards every
  message verbatim, which means it stays compatible with whatever upstream
  Spark Connect adds in future versions.

## Workspace layout

```
crates/
  gateway/        # binary entry point
  proxy/          # SparkConnectService impl that forwards every RPC
  routing/        # SessionKey, Pool/AffinityStore traits, Router, TenantRouter
  store-memory/   # in-memory AffinityStore (single-replica)
  store-redis/    # Redis-backed AffinityStore (multi-replica HA)
  pool-static/    # static backend pool (round-robin)
  pool-k8s/       # K8s Endpoints-watch backend pool
  healthcheck/    # HealthAwarePool — active gRPC health probes
  auth/           # static-token / JWT / OIDC authenticators
  tenant/         # tenant resolver (from-claim / from-metadata / fixed)
  ratelimit/      # in-memory + Redis-backed token bucket
  audit/          # structured audit-event emitter
  observability/  # metrics, tracing, admin server
  config/         # YAML config loader
  genproto/       # tonic-generated bindings for spark.connect.*
proto/spark/connect/
  *.proto        # vendored read-only mirror of upstream
deploy/examples/
  spark-connect-server/  # K8s manifests (apache/spark-kubernetes-operator)
test/integration/
  README.md, client_smoke.py  # real PySpark E2E
```

## Regenerating proto bindings

The `crates/genproto/build.rs` script invokes `tonic-prost-build` on every
`cargo build`. To force a regeneration:

```bash
cargo clean -p scg-genproto
cargo build -p scg-genproto
```

`protoc` must be on `$PATH` (e.g. `brew install protobuf`).

## What ships today

**Proxy core.** Streaming forward of every `SparkConnectService` RPC.
Session affinity pinned by `(tenant, user_id, session_id)` so a
client's RPCs always reach the same backend driver. Operation-id
reverse index so `ReattachExecute` / `ReleaseExecute` / `Interrupt`
reach the right driver even after the session binding has expired.

**Backend discovery.** Static list, or K8s Endpoints watch via
`kube-rs` — pool updates within seconds of pod changes, no gateway
restart.

**Multi-replica HA.** Redis-backed shared affinity store. Two-step
graceful shutdown drains in-flight streams before the gRPC server
stops.

**Auth.** Pluggable: anonymous (default — trusted networks only),
static token, JWT (local public key), OIDC (remote JWKS / discovery).
Verified `Identity` overwrites the client-supplied `UserContext.user_id`
on forward.

**Multi-tenancy.** Tenant resolver reads from the auth claim, a gRPC
metadata header, or a fixed string. Per-tenant backend pool overrides
with `UseDefault` / `Reject` policies for unknown tenants. Per-tenant
token-bucket rate limiting with optional per-user sub-bucket; in-memory
or Redis-shared.

**Audit + observability.** Structured audit events (`session.create`,
`session.release`, `auth.failure`, `rpc.error`) with `target=scg::audit`.
Prometheus metrics covering RPC throughput / duration / auth failures /
pool size / active streams / rate-limit rejections. OpenTelemetry tracing
with W3C `traceparent` propagation. Active gRPC health probing.

**Deployment.** Helm chart with sensible defaults (2-replica HA + bundled
Redis); separate values overlays for K8s discovery, JWT/OIDC auth,
external Redis, tracing.

**Known gaps.** Weighted backend selection per tenant, cold-start
provisioning of new tenant pools, per-tenant warm pools, and
multi-cluster discovery are roadmap items — see
[ROADMAP.md](ROADMAP.md) for the full list with current status,
planned shapes, and deliberate non-goals. Distributed-trace
continuity for inbound `traceparent` is limited by upstream
`tracing-opentelemetry` ↔ `opentelemetry_sdk` plumbing; root-span
traces work end-to-end.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). The vendored
Spark Connect protos under `proto/spark/connect/` are themselves under
the Apache 2.0 license held by the Apache Software Foundation; see
[NOTICE](NOTICE) for details.

Contributions are accepted under the same license — by submitting a
pull request you agree that your contribution is licensed under
Apache 2.0.

[1]: https://spark.apache.org/docs/latest/spark-connect-overview.html
[2]: ../plans/IMPLEMENTATION-PLAN-OSS-Spark-Connect-Gateway.md
[3]: ../plans/OPEN_SOURCE_SPARK_CONNECT_GATEWAY_ANALYSIS.md
[4]: https://github.com/apache/spark-kubernetes-operator
[5]: https://github.com/Kimahriman/spark-connect-proxy
