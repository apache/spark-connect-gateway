# Kubeflow Spark Operator integration walkthrough

Verifies that the gateway works end-to-end against Spark Connect
servers managed by the [Kubeflow Spark
Operator](https://github.com/kubeflow/spark-operator). Two driver
pods get stood up via the operator's `SparkConnect` CRD, the
gateway watches them through an aggregating Kubernetes Service,
and multiple PySpark sessions land on both drivers (round-robin
through the gateway's K8s-discovery pool).

The other walkthroughs use plain `Deployment` + `Service`
manifests. This one shows the gateway is operator-agnostic: its
only Kubernetes integration point is *watching a Service's
Endpoints*, so any provisioner that ends up producing such a
Service composes with the gateway. The Kubeflow operator is the
most common Spark-on-Kubernetes operator in the wider community
(separate from the newer [apache/spark-kubernetes-operator][k8s-op]
that the SPIP also points at), so it's the most representative
operator to validate against.

The whole walkthrough takes ~15–20 minutes on a laptop with the
Spark and gateway images already cached.

[k8s-op]: https://github.com/apache/spark-kubernetes-operator

## What this exercises

* The Kubeflow Spark Operator's `SparkConnect` CRD
  (`apiVersion: sparkoperator.k8s.io/v1alpha1`) — one CR per
  driver pod.
* An **aggregating Service** pattern: two driver pods tagged
  with a shared label, one plain `Service` selecting that label,
  Endpoints list containing both pod IPs. This is the bridge
  between the operator's one-CR-per-driver model and the
  gateway's one-Service-per-pool model.
* The gateway's `pool-k8s` watcher reacting correctly when an
  operator (not a `Deployment`) is the entity producing driver
  pods.
* PySpark client traffic distributed across both Kubeflow-managed
  drivers via the gateway's round-robin picker.

## What this does NOT exercise

* **Per-tenant routing**. Both drivers form a single pool, like
  e2e-smoke. For per-tenant override pools with the operator,
  combine this walkthrough's per-CR pattern with the
  [e2e-multitenant](../e2e-multitenant/) values.yaml shape.
* **Operator-driven scaling.** The walkthrough stands up two
  static `SparkConnect` CRs. Dynamic scale-up/down of driver
  pools is the operator's responsibility; the gateway has been
  shown to react to Endpoints changes in
  [e2e-scale-test](../e2e-scale-test/) and that behaviour
  applies here regardless of who produces the Endpoint
  transitions.
* **JWT / OIDC auth** — see [e2e-auth-jwt](../e2e-auth-jwt/).
  Auth is orthogonal to the operator integration.
* **Redis affinity store** — see
  [e2e-multi-replica-redis](../e2e-multi-replica-redis/).

## Prerequisites

```
docker             # any recent version
kind               # brew install kind
kubectl
helm 4.x
python 3.11+       # for the PySpark client
```

Footprint: ~5 GiB free RAM (kind node + 2 Spark driver JVMs + 2
executor JVMs + gateway + operator + webhook), ~3 GiB free disk
(`docker.io/library/spark:4.0.1` image is the biggest single
pull at ~2 GiB).

## Step-by-step

All commands run from the repo root.

### 1. Build the gateway image, create the kind cluster, load the image

```bash
docker build -t scg:e2e .
kind create cluster --name scg-kfop --wait 60s
kind load docker-image scg:e2e --name scg-kfop
```

See [e2e-smoke/README.md](../e2e-smoke/README.md#1-build-the-gateway-image)
for the build-time troubleshooting if your network blocks crates.io.

### 2. Install the Kubeflow Spark Operator

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update spark-operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set webhook.enable=true

kubectl wait --for=condition=available --timeout=120s \
  deployment -l app.kubernetes.io/name=spark-operator \
  -n spark-operator
```

The webhook is required for `SparkConnect` resources — it
validates the CR before the controller acts on it.

Verify the three CRDs landed and both controller pods are
Running:

```bash
kubectl get crd | grep sparkoperator.k8s.io
# scheduledsparkapplications.sparkoperator.k8s.io
# sparkapplications.sparkoperator.k8s.io
# sparkconnects.sparkoperator.k8s.io       ← this is what we'll use

kubectl get pods -n spark-operator
# spark-operator-controller-...   1/1   Running
# spark-operator-webhook-...      1/1   Running
```

### 3. Apply two `SparkConnect` CRs plus the aggregating Service

```bash
kubectl apply -f deploy/examples/e2e-kubeflow-spark-operator/spark-connect-pool.yaml

# Wait until both driver pods are ready (this also covers the
# Spark image pull on a cold cluster; allow more time the first
# time).
kubectl wait --for=condition=ready pod \
  -l scg-pool=shared \
  -n default --timeout=300s
```

Inspect what the operator built and confirm the aggregating
Service collected both driver pods:

```bash
kubectl get sparkconnect -n default
# NAME         AGE   STATUS   PODNAME
# scg-test-a   …     Ready    scg-test-a-server
# scg-test-b   …     Ready    scg-test-b-server

kubectl get svc -n default
# scg-pool             ClusterIP   …   15002/TCP                                  ← the one the gateway watches
# scg-test-a-server    ClusterIP   …   7078/TCP,7079/TCP,4040/TCP,15002/TCP       ← operator-built, not used by gateway
# scg-test-b-server    ClusterIP   …   7078/TCP,7079/TCP,4040/TCP,15002/TCP       ← operator-built, not used by gateway

kubectl get endpoints scg-pool -n default
# NAME       ENDPOINTS                              AGE
# scg-pool   10.244.0.7:15002,10.244.0.8:15002      …
```

The two IPs in `scg-pool`'s Endpoints list match the two driver
pods (`kubectl get pods -l scg-pool=shared -o wide`). The
operator's own per-CR Services still exist — they expose the
Spark UI on `:4040`, the block manager, etc. — but the gateway
ignores them; the aggregating Service is its single entry point.

### 4. Install the gateway pointing at `scg-pool`

```bash
helm install scg ./deploy/helm/scg \
  -n default \
  -f deploy/examples/e2e-kubeflow-spark-operator/values.yaml

kubectl wait --for=condition=ready pod \
  -l app.kubernetes.io/name=scg \
  -n default --timeout=120s

kubectl logs -n default deploy/scg --tail=10 \
  | grep -E "starting|backend list"
```

The startup log should report `service:"scg-pool"` and
`backend list updated, count: 2`:

```
… "spark-connect-gateway starting (will populate backends from K8s Endpoints)", … "service":"scg-pool", "port":15002 …
… "k8s pool: backend list updated", "count":2 …
```

### 5. Drive PySpark traffic across both drivers

```bash
kubectl port-forward -n default svc/scg 15003:15003 &
kubectl port-forward -n default svc/scg 9090:9090 &
```

Set up the venv if you haven't already:

```bash
python3 -m venv --upgrade-deps /tmp/scg-e2e-venv
/tmp/scg-e2e-venv/bin/pip install 'pyspark[connect]'
```

> `--upgrade-deps` is important on macOS: a bare `python3 -m
> venv` sometimes produces a venv whose `pip` is missing. The
> flag bootstraps `pip` properly.

Single-session smoke first (same shape as e2e-smoke step 6):

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

Then four short-lived sessions in a loop, one fresh
`SparkSession` per iteration:

```bash
/tmp/scg-e2e-venv/bin/python3 - <<'PY'
from pyspark.sql import SparkSession
for i in range(4):
    spark = SparkSession.builder.remote("sc://localhost:15003").getOrCreate()
    n = spark.range(10).count()
    print(f"session {i}: count={n} sid={spark.client._session_id[:8]}")
    spark.stop()
PY
```

All four should report `count=10` with distinct session ids.

### 6. Verify both Kubeflow-managed drivers actually received traffic

```bash
echo "=== driver pod IPs ==="
kubectl get pods -n default -l scg-pool=shared \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.podIP}{"\n"}{end}'

echo "=== session → backend (from gateway audit log) ==="
kubectl logs -n default deploy/scg --tail=500 \
  | grep '"event":"session.create"' \
  | /tmp/scg-e2e-venv/bin/python3 -c "
import sys, json
for line in sys.stdin:
    f = json.loads(line).get('fields', {})
    print(f.get('session_id','?')[:8], '->', f.get('backend','?'))
"
```

Expected (exact IPs depend on your kind run; what matters is
that **both pod IPs appear** in the audit log):

```
=== driver pod IPs ===
scg-test-a-server 10.244.0.7
scg-test-b-server 10.244.0.8

=== session → backend ===
82295e89 -> 10.244.0.7:15002
1deb58d6 -> 10.244.0.8:15002
7de8deb4 -> 10.244.0.7:15002
83bfa9d3 -> 10.244.0.8:15002
502c2113 -> 10.244.0.7:15002
```

This is the assertion the walkthrough exists to prove. Both
operator-managed drivers received PySpark traffic through the
gateway; routing was not stuck on a single driver.

### 7. Verify the metrics endpoint

```bash
curl -s http://localhost:9090/metrics | grep -E "^scg_backend_pool_size|^scg_active_streams"
```

Expected:

```
scg_active_streams 0
scg_backend_pool_size 2
```

`scg_backend_pool_size` matches the count of driver pods the
operator stood up.

## What you've proved

| Property | How |
|---|---|
| Kubeflow operator's `SparkConnect` CRD is installed and the operator reacts | step 2: CRD list shows `sparkconnects.sparkoperator.k8s.io`; controller + webhook pods Running |
| Two driver pods exist and share the pool label | step 3: `kubectl get pods -l scg-pool=shared` shows both |
| Aggregating Service collects both drivers | step 3: `kubectl get endpoints scg-pool` lists both pod IPs |
| Gateway discovers them as one pool | step 4: `"backend list updated","count":2` |
| Both drivers receive PySpark traffic | step 6: audit log lists both pod IPs across sessions |
| `scg_backend_pool_size` metric tracks the count | step 7: `2` |

Combined with the other walkthroughs (which prove auth, audit,
multi-tenant routing, HA, etc.), this completes the picture:
**no code change is needed to use the gateway with the Kubeflow
Spark Operator** — the operator's output topology, modulo the
aggregating-Service trick, is the same shape the gateway already
expects.

## Tearing down

```bash
# Kill background port-forwards first.
helm uninstall scg -n default
kubectl delete sparkconnect --all -n default
kubectl delete -f deploy/examples/e2e-kubeflow-spark-operator/spark-connect-pool.yaml
helm uninstall spark-operator -n spark-operator
kind delete cluster --name scg-kfop
```

## Troubleshooting

### `kubectl apply` of the `SparkConnect` CR fails with a webhook error

The Kubeflow operator's validating webhook is admitting the
request; if the webhook deployment isn't Ready yet the API
server retries for a short window and then fails. Re-run the
`kubectl wait` from step 2 until both `spark-operator-*` pods
are Running, then re-apply step 3.

### Driver pods are stuck `Pending` after step 3

Usually a resource issue. The CR requests 1 CPU + 1 GiB per
driver; if your kind cluster doesn't have headroom (default
`kind` allocates whatever Docker Desktop gives it), reduce the
limits in `spark-connect-pool.yaml` or grow Docker Desktop's
resources.

The other common cause is the image pull — `apache/spark:4.0.1`
is ~2 GiB cold, and the Kubeflow operator's example uses
`docker.io/library/spark:4.0.1` instead of `apache/spark:4.0.1`.
If your network can't reach Docker Hub, pre-pull and
`kind load docker-image docker.io/library/spark:4.0.1
--name scg-kfop`.

### Gateway log shows `count: 1` even though both CRs are Ready

The aggregating Service's selector probably doesn't match both
driver pods. Check the pod labels:

```bash
kubectl get pods -n default -l scg-pool=shared
```

If only one pod is listed, the other CR's `server.template.metadata.labels`
is missing the `scg-pool: shared` label — re-check
`spark-connect-pool.yaml` for indentation drift after edits.

### Both drivers run but session.create events only mention one of them

Each PySpark `SparkSession` is *one* session — affinity binds it
to *one* backend for its lifetime. Running step 5's single-session
script alone will only show one backend in audit. The multi-
session loop is what proves both drivers are reachable.

If the multi-session loop still only hits one backend, see the
analogous troubleshooting block in
[e2e-smoke's README](../e2e-smoke/README.md#step-7s-multi-session-run-shows-only-one-backend-in-the-audit-log) —
the root cause is identical (round-robin advances on every
`pick`, not just session creation, so per-session RPC count
parity matters).

### `kubectl get endpoints scg-pool` is empty

The `scg-pool` Service exists (step 3 listed it under
`kubectl get svc`) but no pods match its selector. Either the
driver pods aren't Ready (the Service's Endpoints list only
contains Ready pods) or the labels don't match (see the
previous entry).
