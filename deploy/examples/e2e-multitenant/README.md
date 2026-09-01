# Multi-tenant routing end-to-end walkthrough

Verifies the gateway's per-tenant backend pool routing: two
tenants, two completely separate Spark Connect deployments, two
JWTs each claiming a different tenant — and the gateway routes
each session to the right pool. Sessions never cross-pollinate.

This is the only e2e walkthrough that exercises the
`tenantPools.overrides` + `tenantResolver.source: from_claim`
path. The other walkthroughs all run with a single pool and
either `auth.type: none` or single-tenant JWT. None of them prove
that routing isolation actually holds across tenants.

The whole walkthrough takes ~15 minutes on a laptop with the
Spark and gateway images already cached.

## What this exercises

* The chart's `tenantPools.overrides.*` ConfigMap templating with
  two `type: k8s` pool entries pointing at distinct Services.
* The gateway's startup sequence picks up each override pool and
  spawns its own K8s `Endpoints` watcher (one watcher per pool).
* `tenantResolver.source: from_claim` extracts the tenant string
  from the JWT's `tenant` claim and uses it as the routing key.
* Per-tenant `Pool` isolation: a session whose JWT says
  `tenant=team-a` reaches a `spark-connect-team-a` pod;
  `tenant=team-b` reaches a `spark-connect-team-b` pod. Audit
  log's `backend` field is how we prove it.
* `tenantPools.onUnknownTenant: reject`: a token claiming
  `tenant=team-ghost` (no matching override) is rejected with
  `PermissionDenied`, with an `rpc.error` audit event.
* `tenantResolver.onMissing: reject`: a token with no `tenant`
  claim at all is rejected with `Unauthenticated` — *but* does
  not emit a structured audit event. See the troubleshooting
  section below.

## What this does NOT exercise

* **Cross-namespace tenant isolation**. Both team pools live in
  the same `spark-connect` namespace. Real SaaS deployments
  usually put each tenant in its own namespace (or cluster) for
  NetworkPolicy / RBAC reasons; the chart's `type: k8s` override
  takes a namespace argument, so this is configuration, not
  code.
* **Per-tenant rate limiting**. The chart supports it via
  `rateLimit.*` but it's a separate dimension; see the chart's
  `values.yaml` for the shape.
* **Tenant-scoped Redis affinity store partitioning**. The
  walkthrough uses memory store; with Redis the keys would be
  prefixed by tenant already (`scg:s:team-a|alice|<sid>` vs
  `scg:s:team-b|bob|<sid>`) — verified by unit tests, not here.

## Prerequisites

```
docker            # any recent version
kind              # brew install kind
kubectl
helm 4.x
python 3.11+      # for the PySpark client
openssl           # to sign the HS256 JWTs (ships with macOS / most Linux distros)
```

Footprint: ~5 GiB RAM (kind + 2 Spark Connect server JVMs + 2
drivers + gateway), ~3 GiB disk. One more JVM than the
single-pool walkthroughs.

## Step-by-step

All commands run from the repo root.

### 1. Build the gateway image, create the kind cluster, load the image

```bash
docker build -t scg:e2e .
kind create cluster --name scg-mt --wait 60s
kind load docker-image scg:e2e --name scg-mt
```

See [e2e-smoke/README.md](../e2e-smoke/README.md#1-build-the-gateway-image)
for the build-time troubleshooting if your network blocks crates.io.

### 2. Deploy the two team Spark Connect backends

```bash
kubectl create namespace spark-connect
kubectl apply -f deploy/examples/e2e-multitenant/spark-connect-server.yaml

# Wait for both teams' pods.
kubectl wait --for=condition=ready pod \
  -l 'app in (spark-connect-team-a,spark-connect-team-b)' \
  -n spark-connect --timeout=300s
```

Confirm each team has its own Endpoint with a distinct pod IP:

```bash
kubectl get endpoints -n spark-connect
# NAME                   ENDPOINTS          AGE
# spark-connect-team-a   10.244.0.5:15002   2m
# spark-connect-team-b   10.244.0.6:15002   2m
```

Record both IPs — they're what the audit log will name in step 6.

### 3. Install the gateway with per-tenant pool overrides

```bash
helm install scg ./deploy/helm/scg \
  -n spark-connect \
  -f deploy/examples/e2e-multitenant/values.yaml

kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=scg \
  -n spark-connect --timeout=120s
```

Verify the gateway brought up one watcher per tenant override:

```bash
kubectl logs -n spark-connect deploy/scg --tail=20 \
  | grep -E "tenant override pool ready|starting"
```

Expected:

```
"message":"tenant override pool ready","tenant":"team-a","size":0
"message":"tenant override pool ready","tenant":"team-b","size":0
"message":"spark-connect-gateway starting ...","tenant_source":"from_claim","tenant_on_missing":"reject"
```

Each `tenant override pool ready` line corresponds to one entry
in `tenantPools.overrides`. The watcher's `size:0` here reflects
the pool *at construction time* — moments later the K8s watcher
reports `count:1` for each pool once the Endpoints data arrives.
(There is no default pool in this walkthrough: `backendDiscovery`
is omitted, which is allowed under `onUnknownTenant: reject`
because the default pool could never be selected anyway. The
startup log notes this with `no default pool configured`.)

### 4. Save the JWT signer helper

The same helper as e2e-auth-jwt:

```bash
cat > /tmp/sign-jwt.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
SECRET="$1"
CLAIMS="$2"
b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }
HEADER='{"alg":"HS256","typ":"JWT"}'
H=$(printf '%s' "$HEADER" | b64url)
P=$(printf '%s' "$CLAIMS" | b64url)
SIG=$(printf '%s' "$H.$P" | openssl dgst -sha256 -hmac "$SECRET" -binary | b64url)
printf '%s.%s.%s\n' "$H" "$P" "$SIG"
SH
chmod +x /tmp/sign-jwt.sh

export SCG_JWT_SECRET="e2e-multitenant-walkthrough-secret-not-for-production"
```

### 5. Drive PySpark as two distinct tenants

```bash
kubectl port-forward -n spark-connect svc/scg 15003:15003 &
kubectl port-forward -n spark-connect svc/scg 9090:9090 &
```

Set up the venv if you don't already have one:

```bash
python3 -m venv --upgrade-deps /tmp/scg-e2e-venv
/tmp/scg-e2e-venv/bin/pip install 'pyspark[connect]'
```

> `--upgrade-deps` is important on macOS: a bare `python3 -m venv`
> sometimes produces a venv without a working `pip` binary, leaving
> you with errors like `No module named pip.__main__`. The flag
> bootstraps `pip` into the venv as part of creation.

Sign one token per tenant and drive a session through each:

```bash
NOW=$(date +%s)
EXP=$((NOW + 600))

TOKEN_A=$(/tmp/sign-jwt.sh "$SCG_JWT_SECRET" \
  "{\"sub\":\"alice\",\"iss\":\"scg-e2e-issuer\",\"aud\":\"scg\",\"exp\":$EXP,\"iat\":$NOW,\"tenant\":\"team-a\",\"groups\":[\"engineers\"]}")

TOKEN_B=$(/tmp/sign-jwt.sh "$SCG_JWT_SECRET" \
  "{\"sub\":\"bob\",\"iss\":\"scg-e2e-issuer\",\"aud\":\"scg\",\"exp\":$EXP,\"iat\":$NOW,\"tenant\":\"team-b\",\"groups\":[\"analysts\"]}")

echo "=== team-a (alice) ==="
/tmp/scg-e2e-venv/bin/python3 - <<PY
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15003/;token=$TOKEN_A").getOrCreate()
print("count =", spark.range(10).count())
spark.stop()
PY

echo "=== team-b (bob) ==="
/tmp/scg-e2e-venv/bin/python3 - <<PY
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15003/;token=$TOKEN_B").getOrCreate()
print("count =", spark.range(10).count())
spark.stop()
PY
```

Both should print `count = 10`. The result alone doesn't prove
isolation — step 6 does.

### 6. Verify tenant routing in the audit log

```bash
kubectl logs -n spark-connect deploy/scg --tail=200 \
  | grep '"event":"session.create"' \
  | /tmp/scg-e2e-venv/bin/python3 -c "
import sys, json
for line in sys.stdin:
    f = json.loads(line).get('fields', {})
    print('user_id={} tenant={} backend={}'.format(
        f.get('user_id'), f.get('tenant'), f.get('backend')))
"
```

Expected (exact IPs depend on your kind run; what matters is
that team-a's `backend` matches `spark-connect-team-a`'s Endpoint
from step 2, and team-b's matches `spark-connect-team-b`'s):

```
user_id=alice tenant=team-a backend=10.244.0.5:15002
user_id=bob   tenant=team-b backend=10.244.0.6:15002
```

**This is the core assertion**: alice's session reached the
team-a pod (10.244.0.5), bob's reached the team-b pod
(10.244.0.6), and they never swapped. If both lines show the
same backend IP, tenant routing isn't working — see
troubleshooting.

### 7. Drive the reject paths

**Unknown tenant** (the JWT is signed correctly, issuer and
audience match, but the `tenant` claim names a tenant with no
override):

```bash
TOKEN_X=$(/tmp/sign-jwt.sh "$SCG_JWT_SECRET" \
  "{\"sub\":\"mallory\",\"iss\":\"scg-e2e-issuer\",\"aud\":\"scg\",\"exp\":$EXP,\"iat\":$NOW,\"tenant\":\"team-ghost\"}")

/tmp/scg-e2e-venv/bin/python3 - <<PY 2>&1 | grep -E "status|details" | head -3
from pyspark.sql import SparkSession
try:
    SparkSession.builder.remote("sc://localhost:15003/;token=$TOKEN_X").getOrCreate().range(1).count()
except Exception as e:
    print(str(e)[:400])
PY
```

Expected:

```
status = StatusCode.PERMISSION_DENIED
details = "tenant \"team-ghost\" has no configured pool"
```

**Missing tenant claim** (signature and issuer/audience valid,
but the JWT has no `tenant` claim at all):

```bash
TOKEN_NT=$(/tmp/sign-jwt.sh "$SCG_JWT_SECRET" \
  "{\"sub\":\"nobody\",\"iss\":\"scg-e2e-issuer\",\"aud\":\"scg\",\"exp\":$EXP,\"iat\":$NOW}")

/tmp/scg-e2e-venv/bin/python3 - <<PY 2>&1 | grep -E "status|details" | head -3
from pyspark.sql import SparkSession
try:
    SparkSession.builder.remote("sc://localhost:15003/;token=$TOKEN_NT").getOrCreate().range(1).count()
except Exception as e:
    print(str(e)[:400])
PY
```

Expected:

```
status = StatusCode.UNAUTHENTICATED
details = "tenant required but not provided"
```

Note the **different gRPC status codes**: unknown tenant is
`PermissionDenied` (you stated who you are, but you're not
allowed), missing claim is `Unauthenticated` (you didn't state
who you are at all). The wording in the binary is consistent
with the gRPC code-of-conduct.

### 8. Verify audit and metrics for the reject paths

```bash
echo "=== session.create count ==="
kubectl logs -n spark-connect deploy/scg --tail=500 \
  | grep -c '"event":"session.create"'

echo "=== rpc.error events ==="
kubectl logs -n spark-connect deploy/scg --tail=500 \
  | grep '"event":"rpc.error"' \
  | /tmp/scg-e2e-venv/bin/python3 -c "
import sys, json
for line in sys.stdin:
    f = json.loads(line).get('fields', {})
    print('code={} msg={}'.format(f.get('code'), (f.get('message') or '')[:80]))
" | sort | uniq -c
```

Expected:

```
=== session.create count ===
2

=== rpc.error events ===
  N code=PermissionDenied msg=tenant "team-ghost" has no configured pool
```

`session.create` is `2` — the two successful sessions from step
5. The `team-ghost` and missing-claim attempts produced no
`session.create` (no binding was ever made), which is the right
behaviour.

`rpc.error` captures the **unknown-tenant** case as a
`PermissionDenied` event with the descriptive message. The
**missing-tenant-claim** case (step 7's second test) shows up as
an `auth.failure` event with `reason=missing_tenant` instead —
inspect it the same way as in
[e2e-auth-jwt step 8](../e2e-auth-jwt/README.md#8-confirm-audit--metrics-counted-the-failures):

```bash
kubectl logs -n spark-connect deploy/scg --tail=400 \
  | grep '"event":"auth.failure"' \
  | /tmp/scg-e2e-venv/bin/python3 -c "
import sys, json
for line in sys.stdin:
    f = json.loads(line).get('fields', {})
    print('reason=' + f.get('reason','?'))
" | sort | uniq -c
```

Expected:

```
  N reason=missing_tenant
```

And the matching metric:

```bash
curl -s http://localhost:9090/metrics | grep "^scg_auth_failures_total"
# scg_auth_failures_total{reason="missing_tenant"} N
```

## What you've proved

| Property | How |
|---|---|
| Chart's `tenantPools.overrides` ConfigMap templating works | step 3: two `tenant override pool ready` lines |
| Per-tenant pool isolation is real | step 6: alice → team-a pod, bob → team-b pod |
| `tenantResolver` reads JWT `tenant` claim | step 6: `tenant` field in audit log matches the JWT |
| Unknown tenant is rejected and audited | step 7: `PermissionDenied`; step 8: `rpc.error` event |
| Missing tenant claim is rejected and audited | step 7: `Unauthenticated`; step 8: `auth.failure` with `reason=missing_tenant` |
| RPC reaches the correct backend after routing | step 5: `count = 10` from each tenant |

## Tearing down

```bash
# Kill background port-forwards first
helm uninstall scg -n spark-connect
kubectl delete -f deploy/examples/e2e-multitenant/spark-connect-server.yaml
kubectl delete namespace spark-connect
kind delete cluster --name scg-mt
```

## Troubleshooting

### Both audit lines in step 6 show the same backend IP

Tenant routing isn't taking effect. Check, in order:

1. **The JWT's `tenant` claim is actually present.** Decode the
   token (paste at <https://jwt.io>, or use
   `python3 -c "import json,base64; print(json.loads(base64.urlsafe_b64decode(token.split('.')[1] + '==')))"`).
   If `tenant` is missing, your signer omitted it.
2. **`tenantResolver.source` is `from_claim`.** A
   `from_metadata` resolver would read a gRPC header instead and
   ignore the JWT claim.
3. **`tenantPools.overrides` actually contains both tenants.**
   Inspect the gateway ConfigMap: `kubectl get configmap -n
   spark-connect scg -o jsonpath='{.data.config\.yaml}'`.

### Missing-tenant-claim rejection: what to grep for

The proxy treats the resolver's reject as an auth failure for
audit / metric purposes: a single `auth.failure` event with
`reason=missing_tenant`, and a matching bump to
`scg_auth_failures_total{reason="missing_tenant"}`. The
correlation ID (`rid`) on the audit event matches the gateway's
WARN log line:

```bash
kubectl logs -n spark-connect deploy/scg | grep "tenant_resolver: rejecting"
```

so the same RPC can be pivoted across both views (audit pipeline
for compliance / alerting, structured log for operator debugging).

### `scg_backend_pool_size` shows `0`, not `2`

By design. The unlabelled `scg_backend_pool_size` gauge tracks
only the **default pool** — and this walkthrough has none
(`backendDiscovery` is omitted under the reject policy), so the
gauge reads `0`. Adding a `tenant` label instead would let
untrusted tenant strings inflate Prometheus cardinality. Per-
tenant pool sizes show up in the gateway log
as `"k8s pool: backend list updated","count":N` lines, one per
override pool, but not in the metrics endpoint. Comments in
`crates/gateway/src/main.rs` explain the trade-off in more
depth.

### `tenant override pool ready` only appears for one tenant

The other tenant's override is missing or malformed in
`values.yaml`. Common causes:

1. **YAML indentation broken** — `tenantPools.overrides.<name>`
   must be a map; a stray dash makes it a list and the chart
   templates skip it.
2. **`type` not in `static|k8s`** — anything else causes the
   chart to `fail` at install time. Re-run `helm install` and
   read the error message.

### A team's pod is up but its tenant pool stays empty

The gateway's K8s watcher only sees pods that are *Ready* and in
the Service's Endpoints. Check:

```bash
kubectl get pods -n spark-connect -o wide       # READY column
kubectl get endpoints -n spark-connect          # team's IP must appear here
```

If the pod is Running but `READY 0/1`, the `tcpSocket :15002`
readiness probe hasn't passed yet — Spark JVMs take 20–40 s to
bind the gRPC port from a cold start.
