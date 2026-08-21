# JWT auth end-to-end walkthrough

Verifies the gateway's `auth.type: jwt` path against a real
Kubernetes deployment and a real PySpark client: a valid token
maps to the JWT's `user_id` / `tenant` / `groups` claims and
reaches a Spark Connect backend; tampered, expired, or missing
tokens are rejected with `UNAUTHENTICATED` and produce
`auth.failure` audit events plus `scg_auth_failures_total` metric
increments.

This is the only e2e walkthrough where the gateway is *not*
configured with `auth.type: none`. The three other walkthroughs
([`e2e-smoke`](../e2e-smoke/),
[`e2e-scale-test`](../e2e-scale-test/),
[`e2e-multi-replica-redis`](../e2e-multi-replica-redis/))
deliberately skip auth so PySpark doesn't need a bearer token,
which makes them simpler to run. This walkthrough fills the gap.

The whole walkthrough takes ~15 minutes on a laptop with the
Spark and gateway images already cached.

## What this exercises

* The Helm chart's `auth.type: jwt` ConfigMap templating with
  the `hmacSecret` key source (one of three supported kinds).
* `JwtAuthenticator::authenticate` end-to-end through a gRPC
  interceptor on every RPC.
* PySpark Connect's bearer-token transport
  (`sc://localhost:15003/;token=<jwt>`).
* Identity propagation: `sub` / `tenant` / `groups` claims from
  the JWT flow into the gateway's audit log as
  `user_id` / `tenant` / `groups` fields on `session.create`.
* Failure paths produce `auth.failure` audit events with the
  right `reason`, and bump
  `scg_auth_failures_total{reason="..."}`.

## What this does NOT exercise

* **RSA / EC signing**. We use HMAC (`HS256`) because it removes
  the need to generate and distribute a key pair for the
  walkthrough. The chart supports `pemFile` and `pemInline` key
  kinds for asymmetric algorithms; those follow the same
  templating shape.
* **OIDC / remote JWKS**. Setting up a fake OIDC issuer in-cluster
  is its own undertaking; the OIDC code path is covered by
  unit tests in [`crates/auth/src/oidc.rs`](../../../crates/auth/src/oidc.rs).
* **TLS termination**. PySpark normally requires SSL when a token
  is present, but its localhost branch uses
  `grpc.local_channel_credentials()` instead — that's the door
  we walk through here. Production deployments must terminate
  TLS at an Ingress / service mesh in front of the gateway; never
  ship a token over plaintext to a non-localhost address.
* **Multi-tenant routing on top of JWT-derived tenants**. The
  walkthrough sets `tenantClaim: tenant` so the audit log shows
  the claim flowed through, but the gateway is configured with a
  single pool — no per-tenant override.

## Prerequisites

```
docker            # any recent version
kind              # brew install kind
kubectl
helm 4.x
python 3.11+      # for the PySpark client
openssl           # to sign the HS256 JWT (ships with macOS / most Linux distros)
```

Same hardware footprint as `e2e-smoke` — ~4 GiB RAM, ~3 GiB disk.

## Step-by-step

All commands run from the repo root.

### 1. Build the gateway image, create the kind cluster, load the image

```bash
docker build -t scg:e2e .
kind create cluster --name scg-auth --wait 60s
kind load docker-image scg:e2e --name scg-auth
```

See [e2e-smoke/README.md](../e2e-smoke/README.md#1-build-the-gateway-image)
for the build-time troubleshooting if your network blocks crates.io.

### 2. Deploy the Spark Connect server backends

```bash
kubectl create namespace spark-connect
kubectl apply -f deploy/examples/e2e-auth-jwt/spark-connect-server.yaml

kubectl wait --for=condition=ready pod \
  -l app=spark-connect-server \
  -n spark-connect --timeout=300s
```

The manifest is identical to e2e-smoke's — two `apache/spark:4.0.0`
replicas with `start-connect-server.sh --wait`. Auth is a gateway
concern; backends don't participate.

### 3. Install the gateway with `auth.type: jwt`

```bash
helm install scg ./deploy/helm/scg \
  -n spark-connect \
  -f deploy/examples/e2e-auth-jwt/values.yaml
```

Wait for the gateway pod, then verify the auth backend:

```bash
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=scg \
  -n spark-connect --timeout=120s

kubectl logs -n spark-connect deploy/scg --tail=10 | grep "starting"
# Expect: "auth":"jwt" — not "none".
```

The shared HMAC secret in `values.yaml` is templated as plaintext
into the gateway's ConfigMap. That is **fine for this walkthrough
only** — production should mount the secret from a `Secret` and
reference it through environment expansion or a sidecar. The
chart's HMAC support today only takes the literal string inline;
filing a chart enhancement to consume a `Secret` reference is
left for future work.

### 4. Save a shell helper for signing JWTs

```bash
cat > /tmp/sign-jwt.sh <<'SH'
#!/usr/bin/env bash
# Sign an HS256 JWT. Args: $1=secret, $2=JSON claims. Prints the
# compact-serialized JWT to stdout.
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
```

This 8-line script is enough because we only need to demonstrate
the auth path, not implement a real signer. Production code
should use a proper JWT library.

The shared secret literal lives in `values.yaml` — same string
used by gateway and client:

```bash
export SCG_JWT_SECRET="e2e-auth-jwt-walkthrough-secret-not-for-production"
```

### 5. Sign a valid token and drive PySpark

```bash
kubectl port-forward -n spark-connect svc/scg 15003:15003 &
# Also forward the admin port for step 8:
kubectl port-forward -n spark-connect svc/scg 9090:9090 &
```

Set up the venv if you haven't already (same as e2e-smoke):

```bash
python3 -m venv /tmp/scg-e2e-venv
/tmp/scg-e2e-venv/bin/pip install 'pyspark[connect]'
```

Sign a token claiming `sub=alice, tenant=team-a, groups=[devs,admins]`,
matching the chart's `issuer` and `audience`:

```bash
NOW=$(date +%s)
EXP=$((NOW + 600))
TOKEN=$(/tmp/sign-jwt.sh "$SCG_JWT_SECRET" \
  "{\"sub\":\"alice\",\"iss\":\"scg-e2e-issuer\",\"aud\":\"scg\",\"exp\":$EXP,\"iat\":$NOW,\"tenant\":\"team-a\",\"groups\":[\"devs\",\"admins\"]}")
echo "Token: ${TOKEN:0:50}..."
```

Drive PySpark with the token embedded in the connection URL:

```bash
/tmp/scg-e2e-venv/bin/python3 - <<PY
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote(
    "sc://localhost:15003/;token=$TOKEN"
).getOrCreate()
print("count =", spark.range(10).count())
spark.stop()
PY
```

Expected:

```
count = 10
```

PySpark normally insists on TLS when a token is present, but it
has a localhost-only branch that wraps the bearer token in
`grpc.local_channel_credentials()` instead. That branch is why
this walkthrough works without provisioning a certificate. Any
non-localhost client must terminate TLS upstream of the gateway.

### 6. Confirm the JWT identity reached the audit log

```bash
kubectl logs -n spark-connect deploy/scg --tail=200 \
  | grep '"event":"session.create"' \
  | /tmp/scg-e2e-venv/bin/python3 -c "
import sys, json
for line in sys.stdin:
    f = json.loads(line).get('fields', {})
    print('user_id =', f.get('user_id'))
    print('tenant  =', f.get('tenant'))
    print('groups  =', f.get('groups'))
    print('backend =', f.get('backend'))
"
```

Expected:

```
user_id = alice
tenant  = team-a
groups  = devs,admins
backend = 10.244.0.5:15002
```

`user_id` is `alice` — *not* `anonymous`. That's the proof the
JWT's `sub` claim reached the audit pipeline. The `tenant` and
`groups` fields come from the corresponding claims (since we
set `tenantClaim` and `groupsClaim` in `values.yaml`).

### 7. Drive the failure paths

Three rejection cases worth exercising. Each should fail with
`UNAUTHENTICATED` on the client and produce a matching audit
event on the gateway.

**Expired token:**

```bash
PAST=$((NOW - 7200))
EXPIRED=$(/tmp/sign-jwt.sh "$SCG_JWT_SECRET" \
  "{\"sub\":\"alice\",\"iss\":\"scg-e2e-issuer\",\"aud\":\"scg\",\"exp\":$PAST,\"iat\":$((PAST - 100))}")
/tmp/scg-e2e-venv/bin/python3 - <<PY 2>&1 | grep -E "details|status"
from pyspark.sql import SparkSession
try:
    SparkSession.builder.remote("sc://localhost:15003/;token=$EXPIRED").getOrCreate().range(1).count()
except Exception as e:
    print(str(e)[:300])
PY
```

Expected:

```
status = StatusCode.UNAUTHENTICATED
details = "invalid JWT"
```

**Wrong secret:**

```bash
WRONG=$(/tmp/sign-jwt.sh "wrong-secret" \
  "{\"sub\":\"alice\",\"iss\":\"scg-e2e-issuer\",\"aud\":\"scg\",\"exp\":$EXP,\"iat\":$NOW}")
# Same PySpark try/except block as above, swap in $WRONG. Same expected output.
```

**No token at all:**

```bash
# Connect without the ;token= clause. Same try/except block.
# Expected message: "missing or malformed Authorization: Bearer header".
```

A subtle but important property: the gateway returns the same
generic `"invalid JWT"` message for **all** signature-validation
failures (expired / wrong signature / issuer mismatch / audience
mismatch). The `auth.failure` audit event likewise uses a small
fixed set of `reason` values — `invalid_token`, `missing_token`,
`unknown_kid` — none of which leak the specific reason. This is
intentional: a verifier that distinguishes "your signature is
wrong" from "your token has expired" lets an attacker probe for
detail. Operational debugging happens at the gateway's WARN log
line (which *does* carry the underlying jsonwebtoken error), not
in the client-facing response.

### 8. Confirm audit + metrics counted the failures

```bash
echo "=== audit auth.failure events ==="
kubectl logs -n spark-connect deploy/scg --tail=400 \
  | grep '"event":"auth.failure"' \
  | /tmp/scg-e2e-venv/bin/python3 -c "
import sys, json
for line in sys.stdin:
    f = json.loads(line).get('fields', {})
    print('reason=' + f.get('reason','?'))
" | sort | uniq -c
```

Expected (counts will vary by how many test sessions you ran;
the *kinds* are what matters):

```
   N reason=invalid_token
   M reason=missing_token
```

```bash
echo "=== auth_failures_total metric ==="
curl -s http://localhost:9090/metrics | grep "^scg_auth_failures_total"
```

Expected:

```
scg_auth_failures_total{reason="invalid_token"} N
scg_auth_failures_total{reason="missing_token"} M
```

Each line in the audit log corresponds to exactly one
counter-bump on the metric — the two views agree.

## What you've proved

| Property | How |
|---|---|
| Gateway boots with JWT auth wired in | step 3: `"auth":"jwt"` in the startup log |
| Valid JWT → identity propagates to audit | step 6: `user_id=alice`, not `anonymous` |
| Claim mapping (`sub`, `tenant`, `groups`) works | step 6: all three fields populated |
| RPC reaches a real Spark backend after auth | step 5: `count = 10` from `range(10)` |
| Expired / forged / missing token → `UNAUTHENTICATED` | step 7: gRPC `details="invalid JWT"` |
| Failure paths emit audit + metric, with bounded reasons | step 8: `invalid_token`, `missing_token` |
| Failures don't leak detail to the client | step 7: same generic message for all bad-JWT cases |

## Tearing down

```bash
# Kill background port-forwards
helm uninstall scg -n spark-connect
kubectl delete -f deploy/examples/e2e-auth-jwt/spark-connect-server.yaml
kubectl delete namespace spark-connect
kind delete cluster --name scg-auth
```

## Troubleshooting

### Gateway log says `"auth":"none"` after the helm install

The values.yaml didn't take effect. Common causes:

1. **Wrong `-f` path** — double-check
   `deploy/examples/e2e-auth-jwt/values.yaml` (not one of the
   other walkthroughs').
2. **Earlier release still installed.** `helm install` silently
   keeps the previous values when the release exists; use
   `helm upgrade --install`, or `helm uninstall scg` first.

### PySpark fails with "TLS required but use_ssl=False"

You're connecting to a non-localhost address. PySpark only
exempts the localhost path from its token-requires-TLS check.
Use `port-forward` (which serves at `localhost:15003`) for this
walkthrough; for non-local clients, terminate TLS upstream.

### The token works once but later requests get `UNAUTHENTICATED`

Check `exp`. The `iat`/`exp` interval in step 5 is 10 minutes;
re-sign with a fresh `NOW` if you take a coffee break mid-test.
The gateway accepts tokens up to 60s past their `exp` (the
`jsonwebtoken` crate's default leeway).

### `auth.failure` audit events appear with `reason=unknown` and no obvious cause

`unknown` is reserved for the small remainder of paths not
classifiable into one of the named reasons (`missing_token`,
`invalid_token`, `unknown_kid` for OIDC). If you see it in JWT
mode, the `tracing` log line at WARN level for the same `rid`
will carry the underlying jsonwebtoken error — that's where to
read the actual cause.

### I want to use RSA instead of HMAC

The chart supports it. Generate a 2048-bit RSA key with
`openssl genrsa -out signing.key 2048`, extract the public half
with `openssl rsa -in signing.key -pubout > signing.pem`, then in
`values.yaml`:

```yaml
auth:
  type: jwt
  jwt:
    algorithms: [RS256]
    key:
      kind: pemInline
      pem: |
        -----BEGIN PUBLIC KEY-----
        ... (contents of signing.pem) ...
        -----END PUBLIC KEY-----
```

Sign tokens with the *private* key on the client side; the
gateway only needs the public half. This is what every realistic
deployment does — the secret never has to leave the IdP.
