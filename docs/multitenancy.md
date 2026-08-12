# Multi-tenancy guide

The Spark Connect Gateway can serve many tenants through a single
deployment without their workloads interfering with each other. This
guide is the **operator entry point** for multi-tenant setups: pick a
deployment shape, find the sample config, follow the links to the
deep references.

If you're after a specific topic, the dedicated guides go deeper:

* [`deployment.md`](deployment.md) — full chart reference, every
  knob, install/upgrade/uninstall.
* [`observability.md`](observability.md) — metrics, audit log
  schema, log/PromQL examples.
* [`runbook.md`](runbook.md) — symptom → diagnosis → fix.

## What "multi-tenant" means here

A **tenant** is whatever string the gateway resolves for an inbound
RPC. The session-affinity routing key is `(tenant, user_id,
session_id)`, so `(team-a, alice, sess-1)` is a different binding
from `(team-b, alice, sess-1)` — two tenants can reuse session ids
without colliding.

What the gateway gives you at the data plane:

| Capability | How |
|---|---|
| Per-tenant identity from auth | `tenantResolver` reads the JWT/static-token tenant claim, a gRPC metadata header, or a fixed string. |
| Per-tenant backend pools | `tenantPools.overrides` pins a tenant to a dedicated Spark Connect cluster; everything else falls through to a shared default. |
| Per-tenant quotas | `rateLimit.overrides` sets a token-bucket RPS / burst per tenant; opt-in per-user dimension inside a tenant; per-replica or Redis-shared via `rateLimit.store`. |
| Per-pool backend credentials | `backendToken` presents a `spark.connect.authenticate.token` bearer to each pool's backends (per-tenant overrides supported), so backends themselves refuse clients that bypass the gateway — see [Enforcing the trust boundary](deployment.md#enforcing-the-trust-boundary). |
| Tenant-aware audit trail | Every audit event (`session.create`, `auth.failure`, `rpc.error`, …) carries the resolved tenant in a structured field. |
| Strict-isolation mode | Pair `onMissing=reject` (resolver) with `onUnknownTenant=reject` (pools) so RPCs from unmapped tenants never reach a backend. |

What's deliberately **not** here — see [What's not here
yet](#whats-not-here-yet) at the end.

## The four building blocks

Each knob is documented at length in `deployment.md`. The summary
below is just enough to make the [decision guide](#decision-guide)
make sense.

### 1. `tenantResolver` — where does the tenant string come from?

`source` is one of `from_claim` (auth identity), `from_metadata`
(`x-tenant` header), or `always_default` (single-tenant). `onMissing`
decides what happens when the source yields nothing — `use_default`
falls back to `defaultName`, `reject` returns `Unauthenticated`. See
[Multi-tenant: picking a tenant resolver](deployment.md#multi-tenant-picking-a-tenant-resolver)
for the full decision table.

### 2. `tenantPools` — which backends does each tenant use?

`backendDiscovery` at the top of `values.yaml` configures the
**default** pool; `tenantPools.overrides.<tenant>` adds a dedicated
pool (`static` or `k8s` discovery). `tenantPools.onUnknownTenant`
mirrors the resolver's `onMissing` policy at the routing layer. See
[Per-tenant pools](deployment.md#per-tenant-pools).

### 3. `rateLimit` — protect tenants from each other's bursts

Off by default. Set `rateLimit.enabled: true` and supply
`rateLimit.default` plus optional `rateLimit.overrides.<tenant>`.
Quota violations surface as `RESOURCE_EXHAUSTED` and increment
`scg_rate_limit_rejected_total{tenant, scope}`. See
[Per-tenant rate limiting](deployment.md#per-tenant-rate-limiting).

### 4. `audit` — who did what, when

Emits four event types by default (`session.create`,
`session.release`, `auth.failure`, `rpc.error`) tagged with
`target=scg::audit` so you can split them out in Loki/Splunk. Every
event carries the tenant. See
[Audit logging](deployment.md#audit-logging) for chart
config and [the event schema](observability.md#audit-logging) for
fields and sample queries.

## Decision guide

Three deployment shapes cover almost everything we've seen.

### Shape A: **Permissive multi-tenant** (shared backends, per-tenant metrics)

You want one Spark Connect cluster shared across many tenants, but
you want metrics, audit, and (optionally) quotas labeled per tenant.
No tenant-specific pools. New tenants don't need a config change.

Pick this when:
* All tenants tolerate sharing the same Spark cluster.
* Onboarding a new tenant should be a no-op for the gateway.
* You still want per-tenant observability and quotas.

See [Sample 1](#sample-1-permissive-multi-tenant).

### Shape B: **Strict multi-tenant** (pinned pools per tenant)

Each tenant gets its own Spark Connect cluster. The gateway rejects
RPCs from any tenant that isn't explicitly mapped — onboarding is a
config change. Tokens without a tenant claim are also rejected.

Pick this when:
* Tenants must be isolated at the data plane (different Spark
  clusters per team, regulated tenants, noisy-neighbor risk too
  high to share).
* Operators are okay rolling a chart upgrade per new tenant.

See [Sample 2](#sample-2-strict-multi-tenant).

### Shape C: **Single-tenant**

You want the gateway's full feature surface (auth, observability,
audit) but only have one tenant. Every RPC routes to
`tenant="default"`.

Pick this when:
* You don't need multi-tenant routing yet.
* You want the audit stream + per-tenant metric labels even though
  there's only one tenant.

See [Sample 3](#sample-3-single-tenant) — this is the chart's
default, no per-tenant config needed.

## Sample configs

Each block below is a complete excerpt for the multi-tenant-relevant
sections of `values.yaml`. Drop them into your existing values file
alongside auth / discovery / TLS / etc. — only the multi-tenant
knobs are shown here.

### Sample 1: Permissive multi-tenant

```yaml
# JWT-based auth with a tenant claim; clients without one land in
# the "default" bucket.
tenantResolver:
  source: from_claim
  onMissing: use_default
  defaultName: default

# No per-tenant pool overrides — every tenant uses the default
# backend pool. New tenants need zero config.
tenantPools:
  onUnknownTenant: use_default
  overrides: {}

# Per-tenant quotas keep one tenant from monopolizing the shared
# backends. The default applies to every tenant unless overridden.
rateLimit:
  enabled: true
  default:
    rpcsPerSecond: 100
    burst: 200
  overrides:
    team-a:
      rpcsPerSecond: 500
      burst: 1000

audit:
  enabled: true
  logSuccessfulRpcs: false
```

What you get:
* `scg_rpcs_total`, `scg_rate_limit_rejected_total{tenant}`, and
  every audit event carry the resolved tenant.
* `team-a` gets 5× the quota of the default tier; everyone else
  shares the default.
* A token without a tenant claim still works — it routes as
  `tenant="default"`.

### Sample 2: Strict multi-tenant

```yaml
# JWT must carry a tenant claim. Unauthenticated otherwise.
tenantResolver:
  source: from_claim
  onMissing: reject
  defaultName: default

# Each tenant has its own Spark Connect cluster. Anything not in
# `overrides` is PermissionDenied — operators must register new
# tenants before they can connect.
tenantPools:
  onUnknownTenant: reject
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

# Per-tenant quotas, optional per-user dimension on the enterprise
# tier to stop one user inside team-c from using its entire quota.
rateLimit:
  enabled: true
  default:
    rpcsPerSecond: 100
    burst: 200
  overrides:
    team-a:
      rpcsPerSecond: 500
      burst: 1000
    team-c:
      rpcsPerSecond: 2000
      burst: 5000
      perUserRpcsPerSecond: 200
      perUserBurst: 400

audit:
  enabled: true
  logSuccessfulRpcs: false
```

What you get:
* A misconfigured client (no tenant claim, or a typo tenant string)
  fails at the gateway — the request never reaches a backend.
* Each tenant's pool is health-checked independently when
  `healthCheck.enabled: true`; an unhealthy team-a backend doesn't
  affect team-b routing.
* Adding a new tenant is `helm upgrade` with a new entry under
  `tenantPools.overrides`.

### Sample 3: Single-tenant

```yaml
# Defaults — included only to make the shape explicit. You can omit
# this entire block; the chart defaults already produce this
# behaviour.
tenantResolver:
  source: from_claim
  onMissing: use_default
  defaultName: default

tenantPools:
  onUnknownTenant: use_default
  overrides: {}

rateLimit:
  enabled: false

audit:
  enabled: true
  logSuccessfulRpcs: false
```

This is the no-multi-tenant-config shape. Every RPC ends up in
`tenant="default"`; pool routing is unchanged; audit captures the
four default events; metrics gain the `tenant` label but it's the
same value everywhere.

## Migration paths

### Adopting the gateway without multi-tenancy

Helm upgrade. No values.yaml changes needed — the chart's defaults
preserve single-pool single-tenant behaviour. Watch
`scg_rpcs_total{code="OK"}` stay flat across the rollout.

### Permissive → Strict

You're starting from Sample 1 and want to lock down to Sample 2.
Order matters because flipping `onMissing` to `reject` is
client-affecting:

1. **Pool first**: add `tenantPools.overrides` entries for every
   tenant currently in production. Keep `onUnknownTenant:
   use_default` for now — new entries just override the pool, no
   client gets rejected.
2. **Verify** the new pools are healthy:
   `scg_backend_pool_size > 0` for each tenant; `kubectl logs`
   shows requests routing to the new pools.
3. **Flip pool policy**:
   `tenantPools.onUnknownTenant: reject`. Any client whose tenant
   isn't in your overrides list starts seeing `PermissionDenied` —
   make sure the list is complete first. See
   [Tenant getting PermissionDenied with "no configured
   pool"](runbook.md#tenant-getting-permissiondenied-with-no-configured-pool)
   for the recovery path if you miss one.
4. **Update auth-side**: ensure your IdP issues tenant claims for
   every legitimate client. Roll this change *before* step 5.
5. **Flip resolver policy**:
   `tenantResolver.onMissing: reject`. Any client whose token has no
   tenant claim now gets `Unauthenticated` — see
   [Clients suddenly getting Unauthenticated after a config
   change](runbook.md#clients-suddenly-getting-unauthenticated-after-a-config-change).

### Adding a new tenant under Strict mode

1. Add the tenant under `tenantPools.overrides.<name>` (and
   `rateLimit.overrides.<name>` if it needs a different quota).
2. `helm upgrade` — the chart rolls the pods with the new ConfigMap.
3. Issue tokens with the matching tenant claim.

Without step 1, the new tenant's first RPC hits
`PermissionDenied: tenant "<name>" has no configured pool` and the
audit log records `rpc.error` events with the resolved tenant —
exactly the signal you want for "I forgot to register this tenant".

## Verifying isolation

If you want confidence the multi-tenant stack is doing what you
expect in your environment, the
[`crates/proxy/tests/multitenant_e2e.rs`](../crates/proxy/tests/multitenant_e2e.rs)
integration test wires every multi-tenant feature together and
asserts isolation along four axes:

* **Pool + affinity** — two tenants using the same `session_id`
  bind to different backends and stay there on repeats.
* **Rate limit** — one tenant exhausting its quota leaves the
  other unaffected; metrics increment only on the offending tenant.
* **Audit labeling** — every audit event carries the resolved
  tenant from the verified identity, not anything client-forgeable.
* **Auth-level Reject** — `onMissing=reject` + a tenantless token
  produces `Unauthenticated` before the request reaches a backend.

The same four invariants are the right things to assert in your
own staging environment. The Go/Python/JVM Spark Connect clients
all accept a Bearer token via metadata, so you can drive equivalent
assertions from any of them.

## Distributed rate limiting

The rate limiter has two backends. Pick by your replica count:

| `rateLimit.store` | Where state lives | Effective cluster-wide quota |
|---|---|---|
| `memory` (default) | Per gateway replica | `N × default.rpcsPerSecond` for N replicas |
| `redis` | One Redis instance, shared | Exactly `default.rpcsPerSecond` |

The memory backend is fine for single-replica deployments or
back-pressure-style limiting where being inside 2× of the configured
quota is acceptable. Switch to `redis` when you need a strict
cluster-wide cap — typical SaaS billing-tier enforcement.

Sample `redis` config:

```yaml
rateLimit:
  enabled: true
  store: redis
  redis:
    url: "redis://redis.spark-connect.svc:6379"
    keyPrefix: "scg-rl"
    keyTtlSecs: 3600
    onFailure: open      # open | closed
  default:
    rpcsPerSecond: 100
    burst: 200
```

**Fail mode** is the question worth thinking about up front:

* `onFailure: open` (default) — when Redis is unreachable, admit
  the RPC and bump `scg_rate_limit_redis_errors_total{tenant, reason}`.
  Matches the Redis affinity-store's fail-soft behaviour:
  availability over strict quotas. The error metric makes the
  outage visible so you can alert on a sustained nonzero rate.
* `onFailure: closed` — when Redis is unreachable, reject the RPC
  with `ResourceExhausted`. Pick this if a Redis outage must not
  become a quota-bypass vector (regulated SaaS). Note that this
  makes Redis a hard request-path dependency — Redis down means
  every RPC throttled.

**Operational notes:**

* The limiter uses a Lua script via plain `EVAL`/`EVALSHA`, not the
  `redis-cell` module. It works on any Redis 6+, including managed
  offerings (ElastiCache, MemoryStore, Upstash) that don't allow
  `loadmodule`.
* Reuse the same Redis instance as the affinity store — the limiter
  uses a different `keyPrefix` (default `scg-rl` vs. affinity's
  `scg`) so a `FLUSH` of one won't disturb the other. Helm chart
  defaults assume a shared `redis` Service.
* Bucket keys self-expire after `keyTtlSecs` so abandoned tenants
  don't leak Redis memory.

Verify by running two gateway replicas with the same Redis: their
combined RPS for one tenant must stay under `default.rpcsPerSecond +
burst`, not double it. The integration test
[`crates/ratelimit/tests/redis_integration.rs`](../crates/ratelimit/tests/redis_integration.rs)
covers this case under `two_replicas_share_the_bucket`.

## What's not here yet

The gateway deliberately scopes itself to the **data plane**.
Tenant-related roadmap items, summarised (the project-wide list
with planned shapes lives in [`ROADMAP.md`](../ROADMAP.md)):

* **Weighted backend selection per tenant** — every tenant pool
  currently uses round-robin / pool-internal load distribution.
  A weighted scheme for tiered backends inside one tenant is on
  the roadmap.
* **Cold-start provisioning** — the gateway routes to existing
  backends; it doesn't ask K8s to create a Spark Connect server on
  demand for a tenant. Stand up pools out-of-band.
* **Warm pool per tenant** — no pre-provisioning logic.
