# Runbook

Symptom → diagnosis → fix for the failures operators will actually
hit. Each entry is short on purpose: a one-line symptom, what to
grep, what to check, what to do.

For metric definitions and PromQL examples see
[`observability.md`](observability.md). For chart values and
config knobs see [`deployment.md`](deployment.md).

## Quick links

* [Gateway pods CrashLoopBackOff](#gateway-pods-crashloopbackoff)
* [`/readyz` stuck on 503](#readyz-stuck-on-503)
* [Clients see `UNAVAILABLE: no healthy backend available`](#clients-see-unavailable-no-healthy-backend-available)
* [Sessions re-pin to different backends](#sessions-re-pin-to-different-backends)
* [`scg_auth_failures_total` spiking](#scg_auth_failures_total-spiking)
* [`reason=unknown_kid` failures persist](#reasonunknown_kid-failures-persist)
* [Redis is down](#redis-is-down)
* [`PermissionError` on K8s endpoints](#permissionerror-on-k8s-endpoints)
* [P99 latency suddenly doubled](#p99-latency-suddenly-doubled)
* [Clients suddenly getting Unauthenticated after a config change](#clients-suddenly-getting-unauthenticated-after-a-config-change)
* [Tenant getting PermissionDenied with "no configured pool"](#tenant-getting-permissiondenied-with-no-configured-pool)
* [Clients seeing ResourceExhausted unexpectedly](#clients-seeing-resourceexhausted-unexpectedly)
* [Streams killed during rolling upgrade](#streams-killed-during-rolling-upgrade)
* [Backend marked unhealthy but it's actually fine](#backend-marked-unhealthy-but-its-actually-fine)

---

## Gateway pods CrashLoopBackOff

**Symptom:** `kubectl get pods -l app.kubernetes.io/name=scg` shows
`CrashLoopBackOff`. No metrics, no logs from the gateway itself.

**Check:** `kubectl logs -p <pod>` (the `-p` reads the *previous*
container — a CrashLoop pod's current container is starting up). The
last few lines of the previous run show the panic message.

**Common causes:**

| First log line | Cause | Fix |
|---|---|---|
| `loading <path>: ...` | ConfigMap mount failed or ConfigMap missing | `kubectl get configmap` should show `scg-config`. If absent, `helm upgrade` to re-render. |
| `building <auth-kind> authenticator: ...` | JWT/OIDC config rejected — bad PEM, wrong algorithm | Verify the values; for OIDC, curl `discoveryUrl` from inside a debug pod. |
| `connecting to redis at ...` | Redis URL wrong or Redis pod not ready | If using bundled Redis, `kubectl get pod -l app.kubernetes.io/component=redis`. If external, verify the URL from a debug pod with `redis-cli`. |
| `binding ...: address already in use` | Port collision; rare. | Likely a host-network experiment. Check `service.grpcPort` / `service.adminPort`. |

The gateway intentionally fails fast at startup rather than
silently degrading — a misconfigured deployment should not look
healthy.

---

## `/readyz` stuck on 503

**Symptom:** Liveness probe passes (`/healthz` 200), readiness
fails (`/readyz` 503). Pod stays in `Running` but K8s won't add it
to the Service endpoints.

**Why:** `/readyz` returns 200 only once the backend pool has at
least one entry. 503 = pool is empty.

**Check:** `scg_backend_pool_size` metric.

| `backendDiscovery.type` | Likely cause |
|---|---|
| `static` | All addresses unreachable. The pool size *is* `len(addresses)`, but backends behind those addresses are down. |
| `k8s` | The watched Service has no Endpoints / EndpointSlices yet, *or* the gateway lacks permission to read them. |

**Fix (static):** check that the named addresses resolve and accept
TCP from inside the cluster:

```bash
kubectl -n spark-connect run -it --rm debug --image=busybox --restart=Never -- \
  nc -zv spark-connect-1.svc.cluster.local 15002
```

**Fix (k8s):** confirm the Service the gateway is watching has
Endpoints:

```bash
kubectl -n <backend-namespace> get endpoints <service-name>
```

If empty, no backend pods are ready — that's a Spark Connect
problem, not a gateway problem. If non-empty, see
[K8s endpoints permission](#permissionerror-on-k8s-endpoints).

---

## Clients see `UNAVAILABLE: no healthy backend available`

**Symptom:** PySpark client gets `grpc._channel._InactiveRpcError`
with status `UNAVAILABLE` and message `no healthy backend
available`.

**Why:** Same as `/readyz` 503 — the gateway *is* up but its pool
is empty. The Service routed the client to the gateway, the
gateway found nothing to forward to. The metric
`scg_rpcs_total{code="Unavailable"}` increments.

**Fix:** see the [readyz section](#readyz-stuck-on-503).

If this is happening *briefly* during a Spark Connect server
rolling restart, that's expected — the K8s discovery pool reflects
real Endpoints. Wait it out, or front the gateway with retry-aware
client behaviour.

---

## Sessions re-pin to different backends

**Symptom:** A PySpark session that worked before suddenly says
`Table or view not found` for a temp view it created earlier, or
`spark.conf.get(...)` returns the default for a key it just set.
Operationally: same `(user_id, session_id)` lands on different
backends across requests.

**Why:** Affinity is broken. Two flavors:

### Flavor 1: Multi-replica with `affinityStore.type: memory`

The chart **disallows** `replicaCount > 1` with `type: memory` at
template time, so this only happens if the cluster was bootstrapped
manually or by an old chart version.

**Check:**
```bash
kubectl -n spark-connect get cm scg-config -o jsonpath='{.data.config\.yaml}' | grep -A1 affinity_store
kubectl -n spark-connect get deploy scg -o jsonpath='{.spec.replicas}'
```

If `type: memory` and `replicas > 1`, that's the bug.

**Fix:** `helm upgrade` with `affinityStore.type=redis` (the chart
default).

### Flavor 2: Redis is unreachable

Affinity is configured Redis but the gateway can't reach it. Lookups
return `None`, the gateway re-picks each time.

**Check:** look at gateway logs for `redis: lookup_session
failed` warnings:

```bash
kubectl -n spark-connect logs -l app.kubernetes.io/name=scg --tail=200 | grep -i redis
```

**Fix:** see [Redis is down](#redis-is-down).

---

## `scg_auth_failures_total` spiking

**Symptom:** Auth failure counter went up sharply, clients see
`UNAUTHENTICATED`.

**Check the `reason` label** — it tells you which subsystem to look
at:

| `reason` | Diagnosis |
|---|---|
| `missing_token` | Clients are not sending credentials. Either the client config changed or auth was just turned on without telling them. |
| `invalid_token` | Tokens reach the gateway but fail signature/structure validation. Often a key rotation that hasn't been propagated. |
| `expired` | Tokens are well-formed and valid, just past their `exp`. Client clock skew or token lifetime mismatch. |
| `unknown_kid` | JWT carries a `kid` not in the gateway's JWKS cache. Most often: IdP just rotated keys; see [its own section](#reasonunknown_kid-failures-persist). |
| `unknown` | Rare — inner authenticator returned an error message that didn't match any of the above. Read the log line. |

**Cross-reference with logs** — every auth failure also produces a
warn log line with the inner Status message, which is more specific
than the metric label.

---

## `reason=unknown_kid` failures persist

**Symptom:** `unknown_kid` rate stays high for >15 minutes. (Brief
spikes after IdP rotation are normal — the gateway refreshes JWKS
on next miss after the floor expires, default 60 seconds.)

**Why:** Either the IdP rotated keys and the new `kid` isn't in the
JWKS endpoint yet, or the gateway can't reach the JWKS endpoint at
all.

**Check:**

```bash
# Curl JWKS from inside the cluster, see if the new kid is there
kubectl -n spark-connect run -it --rm debug --image=curlimages/curl --restart=Never -- \
  curl -s "$JWKS_URL"
```

**Fix:**

| What you saw | Action |
|---|---|
| JWKS unreachable | Network policy / DNS / IdP outage; not a gateway problem. |
| New `kid` *is* in JWKS but gateway hasn't refreshed | Lower `auth.oidc.refreshFloorSecs` (default 60). Note: too-low values let a malicious request cause excess JWKS fetches; default is intentionally cautious. |
| New `kid` is *not* in JWKS | The IdP isn't publishing the new key. Talk to IdP owner. |

---

## Redis is down

**Symptom:** Logs full of `redis: ... failed` warnings; sessions
re-pin (see above); no exception thrown to clients.

**Why:** the gateway intentionally degrades to pool-only routing
when Redis is unreachable, rather than failing requests. Service
stays up, but stickiness is gone — Spark Connect's per-driver
session invariant breaks until Redis recovers.

**Check:** is it bundled or external?

```bash
kubectl -n spark-connect get statefulset
```

* Bundled: `kubectl get pod -l app.kubernetes.io/component=redis`,
  check probes, `kubectl logs` it.
* External: `redis-cli -u "$URL" ping` from a debug pod.

**Fix:**

* If bundled Redis crashed: it'll restart from PVC. AOF means the
  affinity dataset comes back. Active sessions whose lookups
  failed during the outage may have been re-pinned; new bindings
  established during the outage are not in Redis (the warn-and-drop
  path) and will need to be re-established on the next call.
* If external Redis is down: that's outside the gateway's scope.
  The gateway is doing the right thing during the outage; treat
  this as a Redis incident.

**Aftermath:** sessions that were re-pinned during the outage may
have lost server-side state on the original backend. Clients
typically discover this on the next operation that depends on a
temp view or cached frame. Worth a heads-up to client teams when
you publish the post-mortem.

---

## `PermissionError` on K8s endpoints

**Symptom:** With `backendDiscovery.type=k8s`, gateway logs:

```
spawning K8s Endpoints watcher: ... endpoints "spark-connect" is forbidden:
User "system:serviceaccount:..." cannot list resource "endpoints"
```

**Why:** The chart attaches RBAC for `endpoints` /
`endpointslices` get/list/watch in
`backendDiscovery.k8s.namespace`. If the watched namespace differs
from the release namespace, or if the chart is outdated and used
ClusterRole/ClusterRoleBinding terminology that's been disabled,
this fails.

**Check:**

```bash
kubectl -n <backend-namespace> get role,rolebinding -l app.kubernetes.io/name=scg
kubectl auth can-i list endpoints \
  --as system:serviceaccount:<release-ns>:scg \
  -n <backend-namespace>
```

**Fix:** make sure the chart's RBAC values point at the right
namespace:

```bash
helm upgrade scg ./deploy/helm/scg \
  --set backendDiscovery.type=k8s \
  --set backendDiscovery.k8s.namespace=<actual-backend-ns> \
  ...
```

Or, if the gateway runs in a different namespace from the
backends, ensure the RoleBinding's `subjects[].namespace` matches
the gateway's release namespace, not the backend namespace.

---

## P99 latency suddenly doubled

**Symptom:** `histogram_quantile(0.99, ...)` on
`scg_rpc_duration_seconds_bucket` jumped, no obvious traffic
increase, clients report slowness.

**Diagnosis order:**

1. Is `scg_active_streams` also up? If yes, you have more concurrent
   long-running queries — work-amplification is real, not a gateway
   regression.

2. Is the slowness on **all** RPCs or one? Filter the histogram by
   `rpc=` — `Config` and `AnalyzePlan` are unary, fast (10s of ms);
   `ExecutePlan` is the streaming query lifetime, expected to be
   long. A spike isolated to unary RPCs points at the gateway/forward
   path; spikes on `ExecutePlan` usually mean the backend is slow.

3. Is the slowness on **all backends** or one? Use the structured
   log `addr` field in a Loki/Splunk query, group by backend. One
   slow backend = node-level issue (CPU pressure, disk pressure on
   the Spark driver). All backends slow = gateway path or shared
   dependency (Redis latency, OTLP exporter back-pressure if
   tracing enabled).

4. Is **Redis latency** the cause? If `tracing.enabled`, the gateway
   span has the breakdown. Without tracing, look for
   `redis: lookup_session failed` warnings or grep for unusually long
   gaps between log lines for the same `rid`.

**Common fixes:**

| Cause | Fix |
|---|---|
| Backend node under pressure | Cordon the node, drain it, let K8s pool re-pick (this is why we built K8s discovery). |
| Redis latency | If bundled, the StatefulSet pod is competing for resources; bump `redis.resources`. If external, talk to the Redis team. |
| OTLP back-pressure | Check the collector. The gateway exports in batches and won't block on a slow collector indefinitely, but tail latency suffers when the export queue fills. Reduce `tracing.sampleRatio` or scale the collector. |
| Pool just shrank | Each remaining backend is doing more work. Check `scg_backend_pool_size` against your baseline. |

If none of these explain it, capture a CPU profile from one slow
gateway pod (`kubectl debug` + `perf` / `samply`); the gateway's
hot paths are auth, request_id generation, and the tonic forward —
unusual time anywhere else is a clue.

---

## Clients suddenly getting Unauthenticated after a config change

**Symptom:** A `helm upgrade` rolled out a change touching auth or
the tenant resolver. Immediately after, `scg_auth_failures_total`
spikes and clients see `Status::Unauthenticated`, even though their
tokens haven't changed and `auth.type` looks correct.

**Why:** The tenant resolver is a second place where
`Unauthenticated` can come from (besides auth itself). Two policies
trigger it:

| Cause | Symptom in logs |
|---|---|
| `tenantResolver.source=from_claim` + `onMissing=reject` + token missing tenant claim | `tenant_resolver: rejecting RPC — no tenant available` |
| `tenantResolver.source=from_metadata` + `onMissing=reject` + client didn't send the header | Same log line; check the resolver's `metadataHeader` |

**Check:**
```bash
kubectl -n spark-connect logs -l app.kubernetes.io/name=scg --tail=200 | grep "tenant_resolver: rejecting"
```

**Fix:**

* **If you meant to enforce strict tenancy** — fix the upstream
  IdP to emit tenant claims (or fix clients to send the header).
  This is the design intent.
* **If the change to `onMissing=reject` was premature** — roll
  back to `use_default` until the upstream is ready, then flip
  forward. The deployment guide flags this transition.

`scg_auth_failures_total` increments for these too, but the log
line is what distinguishes "token signature failed" from "token
fine but missing tenant claim".

## Tenant getting PermissionDenied with "no configured pool"

**Symptom:** A client whose tenant claim used to work now gets
`Status::PermissionDenied` with the message `tenant "X" has no
configured pool`.

**Why:** `tenantPools.onUnknownTenant` is set to `reject` and the
tenant in question isn't listed under `tenantPools.overrides`.
This happens after one of:

* Someone added new tenants on the IdP side but didn't add a
  matching pool entry to the gateway config.
* The chart was upgraded with `onUnknownTenant: reject` enabled
  but without listing every existing tenant.

**Check:** which tenants are configured?

```bash
kubectl -n spark-connect get cm scg-config -o jsonpath='{.data.config\.yaml}' | grep -A 100 '^tenant_pools:'
```

Compare against the actual tenant strings hitting the gateway:

```bash
# Pull a few rejected RPCs from logs — `tenant_resolver` reports
# the resolved tenant on every RPC, including the rejected ones.
kubectl -n spark-connect logs -l app.kubernetes.io/name=scg --tail=500 | grep -i "permission_denied\|no configured pool"
```

**Fix (one of):**

| Cause | Action |
|---|---|
| New tenant needs its own pool | Add it under `tenantPools.overrides`. `helm upgrade` rolls the pods with new config. |
| New tenant should share the default pool | Switch `onUnknownTenant` back to `use_default`. Every uncatalogued tenant now lands on the default pool. |
| Tenant string is wrong (IdP misconfiguration sending a typo) | Fix the IdP. The gateway is doing the right thing — rejecting an unknown tenant. |

When you flip `onUnknownTenant` from `use_default` to `reject`,
expect a brief uptick in `scg_rpcs_total{code="PermissionDenied"}`
from any tenant you forgot to list. The deployment-time guard
in the chart can't help here because the chart doesn't know which
tenants your IdP will produce.

## Clients seeing ResourceExhausted unexpectedly

**Symptom:** Clients report
`Status::ResourceExhausted` with messages like `tenant "X" rate
limit exceeded` or `user "Y" rate limit exceeded (tenant "X")`.

**Why:** Per-tenant rate limiting is enabled (`rateLimit.enabled:
true`) and the client's RPC rate is above the configured bucket.
Either:

* The limit is reasonable but the client is genuinely runaway.
* The limit is too tight for normal workload.
* The deployment was upgraded with `rateLimit.enabled: true`
  without sizing the buckets for actual traffic.

**Check:** which scope is biting and how often?

```bash
# Reject rate per (tenant, scope) over the last 5 minutes
kubectl -n spark-connect port-forward svc/scg 9090:9090 &
curl -s http://localhost:9090/metrics | grep scg_rate_limit_rejected_total
```

Or via PromQL:
```promql
sum by (tenant, scope) (rate(scg_rate_limit_rejected_total[5m]))
```

**Fix:**

| Scope | Action |
|---|---|
| `scope=tenant` sustained > 0 | Either the whole tenant is running hot → bump `rateLimit.overrides.<tenant>.rpcsPerSecond` + `.burst`, or it's a runaway client → find it via gateway logs filtered by tenant. |
| `scope=user` sustained > 0 | One user inside the tenant is using the tenant's whole quota → either raise `perUserRpcsPerSecond` or talk to the user. |
| Brief spike during deploy/burst | Expected. Bump `burst` (not `rpcsPerSecond`) if you want short bursts tolerated. |

`helm upgrade` with the new `rateLimit` values rolls the pods.
Existing buckets in the dropped pods are gone; new pods start
with full burst.

## Streams killed during rolling upgrade

**Symptom:** During `helm upgrade`, clients with active
ExecutePlan streams see `Status::Cancelled` mid-query, even though
the upgrade should have been graceful. Gateway logs show
`shutdown: drain deadline reached; forcing shutdown`.

**Why:** A long-running stream took longer than
`shutdown.deadlineSecs` (default 30) to finish, so the drain loop
gave up and the gRPC server tore connections down.

**Check:**
```bash
kubectl -n spark-connect logs -l app.kubernetes.io/name=scg --tail=200 | grep "drain"
```

The line tells you `final_active_streams=N` — that's how many
streams were still flowing at the deadline.

**Fix:**

| Cause | Action |
|---|---|
| `shutdown.deadlineSecs` too small for your workload | Bump it. Typical value for Spark Connect with multi-minute queries: 300s. Remember the chart auto-sets `terminationGracePeriodSeconds` to `deadlineSecs + 10`, so K8s grace adjusts too. |
| Long-running streams during a routine rollout | Schedule rollouts away from peak query time, or use `kubectl rollout pause`/`resume` to do them one pod at a time. |
| Stream is genuinely stuck (backend not yielding) | Check the backend driver. Drain is doing the right thing — at some point you have to SIGKILL. |

**Verify the fix locally** with the example: `cargo run -p scg-proxy
--example drain_smoke`. It opens a 1.5s stream and triggers drain
mid-flight; passing means a stream of that duration completes
without being killed.

---

## Backend marked unhealthy but it's actually fine

**Symptom:** With `healthCheck.enabled: true`, a backend you know
is healthy stops getting traffic. `scg_backend_pool_size` dropped
even though no K8s event happened.

**Check:** gateway logs for the eviction:

```bash
kubectl -n spark-connect logs -l app.kubernetes.io/name=scg --tail=500 | grep "healthcheck.*UNHEALTHY"
```

**Common causes:**

| Log signal | Cause | Fix |
|---|---|---|
| `healthcheck: connect failed` | Network blip / DNS hiccup; the backend is fine but the probe couldn't reach it from this gateway pod | Check NetworkPolicy / kube-dns. If transient, the backend will be re-admitted after `healthyThreshold` successful probes. |
| `healthcheck: probe failed ... DeadlineExceeded` | Backend's gRPC server is loaded; Health is responding but slowly | Bump `healthCheck.timeoutSecs`. |
| `healthcheck: probe failed ... Status: ...` (any other code) | Backend returned a real RPC error from Health.Check (not Unimplemented) | The backend really thinks it's unhealthy. Look at backend logs. |

If the backend doesn't ship `grpc.health.v1.Health` at all (older
Spark Connect releases), the gateway *should* treat
`Unimplemented` / `NotFound` as ambiguous and keep it healthy.
If you see persistent eviction with no clear failure reason in
logs, that's a regression — file an issue.
