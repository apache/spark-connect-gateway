# Observability guide

What the gateway exposes, what it means, and where to look when
something is off. Operator-facing — pair with
[`runbook.md`](runbook.md) for "what do I do about it" and
[`deployment.md`](deployment.md) for "how do I configure it."

The gateway exposes three observation surfaces:

* **Prometheus metrics** at `/metrics` on the admin port (default
  `:9090`)
* **Structured JSON logs** to stdout, with a per-RPC correlation ID
  (`rid`)
* **OpenTelemetry traces** over OTLP/gRPC, when `tracing.enabled`

All three carry the same correlation ID for any given RPC, so you
can pivot from a metric anomaly to the corresponding log line to
the corresponding span.

## Suggested SLIs / SLOs

A starting point — adjust thresholds to your traffic.

| SLI | Definition | Starting SLO |
|---|---|---|
| **Availability** | `1 - rate(scg_rpcs_total{code!~"OK\|Cancelled\|InvalidArgument\|NotFound\|AlreadyExists\|FailedPrecondition\|OutOfRange\|Unimplemented\|Unauthenticated\|PermissionDenied"}[5m]) / rate(scg_rpcs_total[5m])` | 99.9% over 30d |
| **Tail latency** | `histogram_quantile(0.99, sum by (le, rpc) (rate(scg_rpc_duration_seconds_bucket{rpc="ExecutePlan"}[5m])))` | < 1s for non-streaming RPCs |
| **Auth failure rate** | `rate(scg_auth_failures_total[5m])` | Baseline + alarm on 5x departure |
| **Backend availability** | `scg_backend_pool_size > 0` | Always |

The status-code filter on availability deliberately excludes client
errors (`InvalidArgument`, `Unauthenticated`, etc.) — they are
*correct* gateway responses to bad input.

## Prometheus metrics

Cardinality budget: total label cardinality of `scg_*` is bounded
at chart-install time (12 RPC names × 17 status codes + small
fixed sets). `session_id`, `operation_id`, and `user_id` are
**never** metric labels — they are unbounded and belong in logs,
audit events, and trace attributes only. The one identity-adjacent
label is `tenant`, which appears solely on the two rate-limit
counters (`scg_rate_limit_rejected_total`,
`scg_rate_limit_redis_errors_total`); its cardinality is bounded
by the tenant set the resolver admits, so deployments deriving
tenants from token claims under a permissive unknown-tenant
policy should count that toward their cardinality budget.

### `scg_rpcs_total{rpc, code}` — counter

Total per-RPC RPCs handled, labelled by RPC method and final gRPC
status code.

```promql
# Throughput
sum by (rpc) (rate(scg_rpcs_total[5m]))

# Error breakdown
sum by (rpc, code) (rate(scg_rpcs_total{code!="OK"}[5m]))

# Per-RPC error rate
sum by (rpc) (rate(scg_rpcs_total{code!="OK"}[5m]))
  / sum by (rpc) (rate(scg_rpcs_total[5m]))
```

### `scg_rpc_duration_seconds{rpc}` — histogram

Gateway-side end-to-end duration including the backend forward.
Buckets: 0.5ms, 1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms,
500ms, 1s, 2.5s, 5s, 10s, 30s, 60s.

```promql
# p99 by RPC
histogram_quantile(0.99,
  sum by (le, rpc) (rate(scg_rpc_duration_seconds_bucket[5m])))

# Mean
sum(rate(scg_rpc_duration_seconds_sum[5m]))
  / sum(rate(scg_rpc_duration_seconds_count[5m]))
```

`ExecutePlan` and `ReattachExecute` are server-streaming RPCs;
their `_duration_seconds` measures the entire stream lifetime.
Long-running queries will produce histogram entries in the 30s+
buckets — that's expected, not a failure mode.

### `scg_auth_failures_total{reason}` — counter

Authentication failures, labelled by a small fixed-cardinality
reason: `missing_token`, `invalid_token`, `expired`, `unknown_kid`,
`unknown`.

```promql
# Auth failure rate, by reason
sum by (reason) (rate(scg_auth_failures_total[5m]))

# Spike in unknown_kid suggests upstream IdP key rotation the
# gateway hasn't picked up yet
rate(scg_auth_failures_total{reason="unknown_kid"}[5m]) > 0.1
```

`reason="unknown_kid"` is the canary for IdP key rotation: a fresh
JWKS arrived at the IdP, the gateway's cache hasn't refreshed
because the floor-rate-limit is in effect. The OIDC authenticator
self-heals — it'll refresh on the next call after the floor expires.

### `scg_rate_limit_rejected_total{tenant, scope}` — counter

RPCs rejected by the per-tenant rate limiter. `scope`
is `"tenant"` or `"user"` depending on which bucket emptied first.
`tenant` cardinality is bounded by the configured tenant set —
`overrides` keys plus the literal `default` for fall-through
tenants.

```promql
# Per-tenant reject rate
sum by (tenant) (rate(scg_rate_limit_rejected_total[5m]))

# Which scope is biting — tenant-wide vs. one user inside a tenant?
sum by (scope) (rate(scg_rate_limit_rejected_total{tenant="team-a"}[5m]))

# Tenants close to their limit (sustained reject rate)
sum by (tenant) (rate(scg_rate_limit_rejected_total[15m])) > 0
```

A sustained non-zero rate on a specific `(tenant, scope)` pair
means the configured limit is too tight (or that a single client
is running away). Bump `rateLimit.overrides.<tenant>` in
`values.yaml` and `helm upgrade`.

### `scg_rate_limit_redis_errors_total{tenant, reason}` — counter

Backend errors from the Redis-backed rate limiter.
Counts *errors*, not rejects: a fail-open deployment increments
this without firing `scg_rate_limit_rejected_total`. `reason` is
one of `tenant_bucket` or `user_bucket`. Only ever nonzero when
`rateLimit.store: redis`.

```promql
# Are we losing limiter visibility right now?
sum(rate(scg_rate_limit_redis_errors_total[1m])) > 0

# Which tenants are affected — useful for noisy-neighbor outages
sum by (tenant) (rate(scg_rate_limit_redis_errors_total[5m]))
```

A sustained nonzero rate is a Redis problem (network, auth,
restart, slow log), not a quota problem. On a fail-open deployment
this means quotas aren't being enforced for the affected requests;
on fail-closed it means RPCs are being thrown out without ever
touching a backend. Alert at the same threshold you'd alert on
Redis availability for the affinity store.

### `scg_backend_pool_size` — gauge

Current count of healthy backends the gateway will route to. Set
by:
* `static` pool: at startup, equal to `len(addresses)`. Never
  changes.
* `k8s` pool: updated whenever the Endpoints watcher emits an
  event. Starts at 0 until the first list event arrives.

```promql
# Alarm: pool went empty
scg_backend_pool_size == 0

# Alarm: pool shrank significantly (k8s discovery only)
delta(scg_backend_pool_size[5m]) < -2
```

A 0 pool size is the fast-path explanation for `/readyz` returning
503 and clients seeing `UNAVAILABLE`; see the
[runbook](runbook.md#readyz-stuck-on-503).

### `scg_active_streams` — gauge

In-flight streaming RPCs (`ExecutePlan`, `ReattachExecute`,
`AddArtifacts`). Useful for capacity planning — sustained high
values mean clients have many concurrent long-running queries.

```promql
# Streams per replica
sum by (pod) (scg_active_streams)

# Stream churn (drives goroutine churn under tonic)
rate(scg_active_streams[1m])
```

Spikes here without a matching spike in `scg_rpc_duration_seconds`
mean clients are opening streams but not reading from them — usually
a sign of buggy client code or a misbehaving gRPC keepalive.

## Structured JSON logs

Every RPC produces one or more log lines. The default formatter is
`tracing_subscriber::fmt().json()`, which yields one JSON object
per line. Send to stdout (the chart does this); aggregate with your
log pipeline of choice.

### Anatomy of a log line

```json
{
  "timestamp": "2026-05-09T08:33:35.146218Z",
  "level": "INFO",
  "fields": {
    "message": "forwarding",
    "rid": "ac54ca1b-894e-44e5-b9c9-8af38a3c1735",
    "rpc": "Config",
    "user": "alice",
    "session": "sess-1",
    "addr": "spark-connect-1.svc.cluster.local:15002"
  },
  "span": {
    "rpc_method": "Config",
    "rpc_service": "spark.connect.SparkConnectService",
    "rpc_system": "grpc",
    "scg_rid": "ac54ca1b-894e-44e5-b9c9-8af38a3c1735",
    "name": "scg_rpc"
  }
}
```

Fields you'll grep for:

| Field | Use |
|---|---|
| `rid` | Correlation ID per RPC. Same value appears in: outbound `x-request-id` metadata to the backend, OTLP span attributes (`scg_rid`), and the response trailers in some failure paths. **The single best primary key for cross-service investigation.** |
| `rpc` | Spark Connect RPC name (`Config`, `ExecutePlan`, …) |
| `user` | Verified `user_id` from the authenticator (not the client-supplied claim) |
| `session` | `session_id` from the request body |
| `addr` | Backend the gateway forwarded to |
| `error` | Present on failure log lines; carries the inner Status message |

### Useful queries

Loki / Grafana Logs:

```logql
# Trace one RPC end-to-end
{namespace="spark-connect"} | json | rid="ac54ca1b-..."

# Auth failures with reason
{namespace="spark-connect"} |= "auth" | json | level="WARN"

# All forwards to one backend
{namespace="spark-connect"} | json | addr="spark-connect-1.svc.cluster.local:15002"
```

Splunk:

```spl
index=k8s namespace="spark-connect" | spath "fields.rid" | search "fields.rid"="ac54ca1b-*"
```

## Audit logging

The gateway emits a separate, narrow stream of structured events for
security- and compliance-relevant transitions. Audit events share the
same JSON log pipeline as operational logs but use a dedicated tracing
`target` (`scg::audit`) so they can be split out in the aggregator.

Five event types, controlled by `audit.enabled` (default `true`) and
`audit.logSuccessfulRpcs` (default `false`):

| `event`           | When                                                           | Default on? |
|-------------------|----------------------------------------------------------------|-------------|
| `session.create`  | A `(tenant, user, session_id)` is bound to a backend the first time | yes |
| `session.release` | A client called `ReleaseSession` and the gateway forgot the binding | yes |
| `auth.failure`    | Auth interceptor rejected the RPC (`reason` matches the metric label) | yes |
| `rpc.error`       | A handler returned a non-OK Status (Cancelled is filtered out)  | yes |
| `rpc.ok`          | Successful RPC — only when `logSuccessfulRpcs: true`            | no |

Every audit event carries `rid` (correlation ID) plus the fields
relevant to the event (`tenant`, `user_id`, `session_id`, `backend`,
`rpc`, `code`, `message`, …). `session.create` and `session.release`
additionally carry `groups` — a comma-joined string of the identity's
group memberships from the auth backend's `groups` / `groupsClaim`
config. Empty when the authenticator doesn't surface groups
(anonymous, token without `groups`, JWT without a `groupsClaim`).
Per-RPC events (`rpc.error`, `rpc.ok`) deliberately omit `groups` —
they fire frequently enough that doubling the field count matters.

Successful RPCs are intentionally *not* logged by default because
`scg_rpcs_total{code="OK"}` already counts them and filling the
audit stream with every Config call defeats the purpose. Flip
`logSuccessfulRpcs: true` only when the deployment is subject to a
strict-monitoring policy that requires per-call audit.

### Routing audit events to a dedicated stream

Audit events are normal JSON log lines with `"target": "scg::audit"`,
so any aggregator that can split on a structured field works.

Loki / Grafana Logs:

```logql
# All audit events
{namespace="spark-connect"} | json | target="scg::audit"

# Just auth failures, grouped by reason
{namespace="spark-connect"} | json | target="scg::audit" | event="auth.failure"

# Session create/release pairs for a tenant
{namespace="spark-connect"} | json | target="scg::audit" | tenant="team-a" |~ "session\\."
```

Splunk:

```spl
index=k8s namespace="spark-connect" "target"="scg::audit"
| spath event | search event="rpc.error"
| stats count by tenant, code
```

### Why not a separate audit sink?

The audit pipeline reuses the JSON formatter rather than adding a
file/Kafka/S3 sink trait. Trade-off: operators get one log pipeline
to manage and existing log retention applies automatically, but the
gateway never holds audit events in process memory or guarantees
delivery beyond best-effort. If your compliance posture needs
write-and-forget durability, intercept `target=scg::audit` events in
a dedicated `tracing_subscriber::Layer` — the field schema is part
of the API contract.

## Distributed tracing

When `tracing.enabled: true`, every RPC opens a `scg_rpc` span on
the gateway and (a) parents it to any inbound W3C `traceparent`
header (when present), (b) injects a fresh `traceparent` for the
backend hop. Spans carry `rpc_method`, `rpc_system="grpc"`,
`rpc_service`, and `scg_rid` matching the log line.

### What you see in Tempo / Jaeger

For each RPC:

* The gateway span (`scg_rpc`, `rpc_method=ExecutePlan`)
* If the backend itself participates in tracing, a child span on
  the backend joined via the propagated `traceparent`
* Internal h2 / hyper / tonic spans below — useful for protocol-level
  debugging, noisy for application work. Filter by `target=scg_proxy`
  in your tracing UI to see only application spans.

### Known limitation: inbound-traceparent path

When a client sends an inbound `traceparent` header (i.e. it
already participates in distributed tracing upstream of the
gateway), the gateway's `scg_rpc` span is currently dropped before
reaching the OTLP exporter due to a versioning mismatch in
`tracing-opentelemetry`. The JSON log line still records the span
attributes; only the OTel export path is affected.

What this means in practice:

* PySpark clients **do not send `traceparent`** by default, so the
  common path produces full traces.
* If your pipeline puts the gateway behind another OTel-instrumented
  service mesh / API gateway that auto-injects `traceparent`, the
  gateway → backend hop will be visible in logs and metrics but the
  span won't appear in the trace UI.

This is a known limitation upstream in `tracing-opentelemetry` ↔
`opentelemetry_sdk` — see the README's "Known gaps" note. The
structured log line is the authoritative record for now.

## What "normal" looks like

The gateway is a forwarding proxy — its hot path is auth + routing
+ tonic forward, with no business logic of its own. Reasonable
healthy numbers from a single replica on a modest pod
(1 vCPU, 512Mi):

| Metric | Healthy band | Notes |
|---|---|---|
| `scg_rpcs_total` rate | 1k–10k unary RPC/s sustained | Real Spark workloads rarely run this hot on unary |
| `scg_rpc_duration_seconds` p99 (unary) | 1–5ms | Dominated by tonic HTTP/2 framing, not by gateway logic |
| `scg_rpc_duration_seconds` p99 (`ExecutePlan`) | bounded by query length | This metric measures stream lifetime; long queries are expected |
| `scg_active_streams` | proportional to concurrent users | Spikes are OK; sustained high values are a capacity-planning signal |
| `scg_auth_failures_total` | near-zero baseline | Spikes correlate with IdP key rotation or client config changes |
| `scg_backend_pool_size` | constant on static, varies on k8s | Drops to 0 ⇒ critical; see [runbook](runbook.md#readyz-stuck-on-503) |

For exact numbers from a synthetic harness on a beefy
workstation — useful for relative comparison ("am I 5x slower than
the harness?") — see [`perf-baseline.md`](perf-baseline.md). Run
`cargo run -p scg-proxy --example load --release -- ...` against
your own deployment for an apples-to-apples number.

## Suggested alerts (starting points)

Adjust thresholds to your traffic baseline.

```yaml
- alert: SCGGatewayDown
  expr: up{job="scg"} == 0
  for: 2m
  severity: critical

- alert: SCGNoHealthyBackends
  expr: scg_backend_pool_size == 0
  for: 5m
  severity: critical

- alert: SCGHighErrorRate
  expr: |
    sum by (rpc) (rate(scg_rpcs_total{code!~"OK|Cancelled|InvalidArgument|NotFound|AlreadyExists|FailedPrecondition|OutOfRange|Unimplemented|Unauthenticated|PermissionDenied"}[5m]))
      / sum by (rpc) (rate(scg_rpcs_total[5m]))
      > 0.05
  for: 10m
  severity: warning

- alert: SCGAuthFailureSpike
  expr: rate(scg_auth_failures_total[5m]) > 5 * avg_over_time(rate(scg_auth_failures_total[5m])[1h:5m])
  for: 5m
  severity: warning

- alert: SCGUnknownKidPersistent
  expr: rate(scg_auth_failures_total{reason="unknown_kid"}[15m]) > 0.1
  for: 15m
  severity: warning
  # OIDC self-heals via JWKS refresh; persistent unknown_kid suggests
  # the IdP rotated keys and our refresh floor is too aggressive.

- alert: SCGTailLatency
  expr: |
    histogram_quantile(0.99,
      sum by (le, rpc) (rate(scg_rpc_duration_seconds_bucket{rpc!~"ExecutePlan|ReattachExecute|AddArtifacts"}[5m])))
      > 1
  for: 10m
  severity: warning
  # Excludes streaming RPCs whose latency is the query lifetime, not gateway latency.
```

## Tying it together: investigating a failure

Typical flow when a metric alert fires:

1. **Metric** identifies the *what*: `SCGHighErrorRate` on
   `rpc=ExecutePlan`.
2. **Logs** identify *who and why*: filter to `rpc="ExecutePlan"`
   level `ERROR`, find a recent `rid`, look at the line — usually
   includes `error="<Status message>"` and `addr` of the offending
   backend.
3. **Backend logs** for that `addr` filtered by the same `rid`
   (forwarded as `x-request-id`) tell you whether it's a Spark-side
   failure (e.g. plan analysis error) or a network failure.
4. **Trace UI** with the same `rid` (in span attribute `scg_rid`)
   lets you see latency breakdown by phase — useful when the
   distinction between "gateway is slow" and "backend is slow"
   matters.

If step 1 shows pool size = 0, jump straight to the
[runbook entry](runbook.md#readyz-stuck-on-503) — the rest of the
investigation chain doesn't apply when nothing was forwarded.
