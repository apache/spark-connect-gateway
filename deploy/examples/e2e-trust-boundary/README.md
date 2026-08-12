# Kubernetes end-to-end test: trust-boundary enforcement

Proves the property the gateway's auth story depends on: **clients
cannot reach the backend Spark Connect servers except through the
gateway**. The backends enforce a pre-shared token
(`spark.connect.authenticate.token`, Spark 4.0+) that only the
gateway holds, so the boundary is enforced by the backend itself —
not assumed from network topology.

Three assertions, run in order:

1. **Direct to a backend, no token → refused.** A PySpark client
   dialing the backend Service directly gets
   `UNAUTHENTICATED: "No authentication token provided"`.
2. **Through the gateway → succeeds.** The same client through the
   gateway works, because the gateway stamps
   `authorization: Bearer <token>` on every gateway→backend request
   (`backendToken` in the chart).
3. **Negative control: gateway without the token → refused too.**
   Disabling `backendToken` and retrying shows the backend refusing
   the gateway itself — the token is the discriminator, nothing
   else.
4. **The token cannot be read back through the gateway.** A client
   asking the `Config` RPC for `spark.connect.authenticate.token`
   sees it as unset, and the gateway records a `config.redacted`
   audit event.

See [Enforcing the trust
boundary](../../../docs/deployment.md#enforcing-the-trust-boundary)
for the production guidance (this token layer plus a
NetworkPolicy). Note the NetworkPolicy half cannot be demonstrated
on a default kind cluster — kind's default CNI (kindnetd) does not
enforce NetworkPolicy at all, which is itself a good illustration
of why the token layer matters: it does not depend on the CNI.

The token layer has a dependency of its own, though: it only holds
while clients cannot *read* the token. Spark's `Config` RPC will
hand back any config key it holds, so the gateway withholds
`spark.connect.authenticate.token` from every `Config` response —
see [The token is only as private as the `Config`
RPC](../../../docs/deployment.md#the-token-is-only-as-private-as-the-config-rpc).
Step 8 below checks that.

## What this does NOT exercise

* Client-side auth at the gateway (`auth.type: none` here, to keep
  the walkthrough focused) — see
  [`e2e-auth-jwt`](../e2e-auth-jwt/).
* Per-tenant tokens (`backendToken.tenantOverrides`) — same
  mechanism, one env var per tenant pool.
* Multi-backend routing — see [`e2e-smoke`](../e2e-smoke/); this
  walkthrough runs one backend replica.

## Prerequisites

Same as [`e2e-smoke`](../e2e-smoke/README.md#prerequisites): docker,
kind, kubectl, helm 4.x, python 3.11+ with a
`pip install 'pyspark[connect]'` virtualenv. **Every command runs
from the repo root.**

## Step-by-step

### 1. Build the gateway image and create the cluster

```bash
docker build -t scg:e2e .
kind create cluster --name scg-e2e --wait 60s
kind load docker-image scg:e2e --name scg-e2e
```

(See the e2e-smoke README for the corporate-proxy note on the
Docker build.)

### 2. Create the shared token Secret

One Secret, read by both sides: the backend enforces the token, the
gateway presents it.

```bash
kubectl create namespace spark-connect
kubectl -n spark-connect create secret generic scg-backend-token \
  --from-literal=token="$(openssl rand -hex 32)"
```

### 3. Deploy the token-enforcing backend

```bash
kubectl apply -f deploy/examples/e2e-trust-boundary/spark-connect-server.yaml
kubectl wait --for=condition=ready pod \
  -l app=spark-connect-server -n spark-connect --timeout=300s
```

The manifest starts `apache/spark:4.0.0` with
`spark.connect.authenticate.token=$(SCG_BACKEND_TOKEN)` — the env
var comes from the Secret, expanded by Kubernetes before the
process starts.

### 4. Assertion 1: direct connection is refused

Port-forward *straight to the backend*, bypassing the gateway
(the port-forward stands in for "any pod that can dial the backend
Service"):

```bash
kubectl port-forward -n spark-connect svc/spark-connect 15002:15002 &
```

```bash
/tmp/scg-e2e-venv/bin/python3 - <<'PY'
from pyspark.sql import SparkSession
try:
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    spark.range(10).count()
    print("UNEXPECTED: direct connection succeeded")
except Exception as e:
    root = e
    while root.__cause__ is not None:
        root = root.__cause__
    print("direct connection refused:", str(root)[:160])
PY
```

Expected — the backend's own interceptor refuses the connection:

```
direct connection refused: <_MultiThreadedRendezvous of RPC that terminated with:
	status = StatusCode.UNAUTHENTICATED
	details = "No authentication token provided"
```

Kill the port-forward before the next step.

### 5. Install the gateway with the token

```bash
helm install scg ./deploy/helm/scg \
  -n spark-connect \
  -f deploy/examples/e2e-trust-boundary/values.yaml
kubectl -n spark-connect rollout status deployment/scg
```

The values file points `backendToken` at the Secret from step 2.
Startup logs confirm the outbound credential is armed (the token
value itself is never logged):

```bash
kubectl logs -n spark-connect deploy/scg | grep "outbound backend"
# {"...","fields":{"message":"outbound backend authentication enabled",
#   "default_pool_token":true,"tenant_overrides":0}}
```

### 6. Assertion 2: the same client succeeds through the gateway

```bash
kubectl port-forward -n spark-connect svc/scg 15003:15003 &
```

```bash
/tmp/scg-e2e-venv/bin/python3 - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15003").getOrCreate()
print("count =", spark.range(10).count())
spark.range(50).createOrReplaceTempView("t")
print("temp view count =", spark.sql("SELECT count(*) FROM t").collect()[0][0])
spark.stop()
PY
```

Expected:

```
count = 10
temp view count = 50
```

The client presented no credential of its own (`auth.type: none` at
the gateway); the gateway added the pool token on the
gateway→backend hop. Note the client's *own* `authorization`
metadata — when gateway auth is enabled — is never forwarded to the
backend; the backend only ever sees the pool token.

### 7. Assertion 3 (negative control): tokenless gateway is refused

Prove the token is what makes the difference — disable it and the
backend refuses the gateway too:

```bash
helm upgrade scg ./deploy/helm/scg -n spark-connect \
  -f deploy/examples/e2e-trust-boundary/values.yaml \
  --set backendToken.enabled=false
kubectl -n spark-connect rollout status deployment/scg
# restart the port-forward, then re-run the step-6 client
```

Expected — the same `UNAUTHENTICATED` as the direct attempt in
step 4, this time surfaced through the gateway:

```
	status = StatusCode.UNAUTHENTICATED
	details = "No authentication token provided"
```

Restore it and the step-6 client works again:

```bash
helm upgrade scg ./deploy/helm/scg -n spark-connect \
  -f deploy/examples/e2e-trust-boundary/values.yaml
```

### 8. Assertion 4: the token cannot be read back through the gateway

Restore `backendToken` if you disabled it in step 7, restart the
port-forward to the gateway, then ask for the token itself:

```bash
/tmp/scg-e2e-venv/bin/python3 - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15003").getOrCreate()
print("token via gateway   =", spark.conf.get("spark.connect.authenticate.token"))
# Control: a key set only on the backend command line, to show the
# Config RPC itself still works and really is reading server config.
print("marker via gateway  =", spark.conf.get("spark.poc.marker", "<unset>"))
spark.stop()
PY
```

Expected — the token reads as unset while ordinary keys are
unaffected:

```
token via gateway   = None
marker via gateway  = <unset>
```

(Add `--conf spark.poc.marker=server-side-only-value` to the
backend manifest if you want the control to return a value.)

The withholding is recorded in the audit stream:

```bash
kubectl logs -n spark-connect deploy/scg | grep config.redacted
# {"...","fields":{"message":"config key withheld from client",
#   "event":"config.redacted","tenant":"default","user_id":"anonymous",
#   "key":"spark.connect.authenticate.token"}}
```

Without this filter the same call returns the token verbatim, and
any user the gateway authorizes can then use it to reach the
backend directly — defeating assertions 1–3.

## Tearing down

```bash
helm uninstall scg -n spark-connect
kubectl delete -f deploy/examples/e2e-trust-boundary/spark-connect-server.yaml
kubectl delete secret scg-backend-token -n spark-connect
kubectl delete namespace spark-connect
kind delete cluster --name scg-e2e
```

## Troubleshooting

* **Step 6 fails with `UNAUTHENTICATED` even with `backendToken`
  enabled** — the gateway and the backend are reading different
  token values. Check both read the *same* Secret
  (`kubectl -n spark-connect get secret scg-backend-token`), and
  remember rotation requires restarting both sides: the backend
  reads the token at JVM start, the gateway at process start.
* **Gateway or backend pod stuck in `CreateContainerConfigError`**
  — the `scg-backend-token` Secret doesn't exist in the namespace;
  kubelet can't resolve the `secretKeyRef` env var. Create the
  Secret (step 2) before deploying either side. (Outside
  Kubernetes, the same misconfiguration surfaces as the gateway's
  own startup error: `backend token env var … is not set`.)
* **Backend pod `CrashLoopBackOff`** — see the e2e-smoke README
  (`--wait` flag, `--packages`/Ivy issues).
