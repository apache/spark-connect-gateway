# Kubernetes end-to-end smoke test

Walks through standing up the Spark Connect Gateway on a local kind
cluster, running a real PySpark client through it against real
Spark Connect servers, and verifying the chain works end to end. The
purpose is verification, not benchmarking — the assertion is "every
piece works", not "every piece is fast".

The whole walkthrough takes ~15 minutes on a laptop with a warm
Docker cache (~30 minutes cold).

## What this exercises

* The gateway binary built into a container image.
* The Helm chart installing on a fresh Kubernetes cluster.
* The K8s `Endpoints`-watching backend pool (`pool-k8s`) actually
  watching a real `Service` and picking up real backends.
* Session affinity holding across multiple RPCs in one session
  (verified via Spark `TempView` survival — only works if every
  RPC reaches the same driver).
* Round-robin routing across multiple backends, verified by
  driving several PySpark sessions through the gateway and
  checking the audit log lists both pod IPs as `backend` values.
* Audit events emitted to the gateway's structured log stream
  (`session.create`, `session.release`, `rpc.error`).
* Prometheus metrics emitted at the admin endpoint.

## What this does NOT exercise

* Multi-replica gateway HA with shared affinity — see the
  [`e2e-multi-replica-redis`](../e2e-multi-replica-redis/) walkthrough
  for that, or the in-process `ha_smoke.rs` example for the unit-test
  flavour.
* Auth (the smoke test uses `auth.type: none` so PySpark doesn't
  need a token).
* Multi-tenancy (single-tenant deployment).
* Performance numbers (see `docs/perf-baseline.md` for the
  in-process harness; the kind-based numbers are too noisy to be
  useful as a baseline).

## Prerequisites

```
docker            # any recent version
kind              # brew install kind
kubectl
helm 4.x
python 3.11+      # for the PySpark client
```

The smoke test needs ~4 GiB free RAM (kind node + 2 Spark Connect
server JVMs + Spark drivers) and ~3 GiB free disk (Spark image,
gateway image, kind node image).

## Step-by-step

The directory you're reading lives at `deploy/examples/e2e-smoke/`.
**Every command in this guide runs from the repo root**, not from
this directory — the Dockerfile is at the repo root and its
`COPY . .` build step expects to see the whole workspace
(`crates/`, `Cargo.toml`, `proto/`).

### 1. Build the gateway image

```bash
# from the repo root:
docker build -t scg:e2e .
```

The image is ~110 MiB. Most of the build time is `cargo build
--release` from scratch; pre-warming the cargo cache on host
doesn't help inside the build container, but layer caching makes
re-builds quick.

If the build fails with `Could not connect to server` against
`index.crates.io`, your network blocks public registries (corporate
proxy, restricted dev environment). Point cargo at whatever proxy
your environment uses by adding the following to the build stage of
`Dockerfile` before the `cargo build` line, then rebuild:

```dockerfile
RUN cat > /usr/local/cargo/config.toml <<'EOF'
[source.crates-io]
replace-with = "proxy"

[source.proxy]
registry = "sparse+https://YOUR-INTERNAL-CRATES-PROXY/"
EOF
```

This patch is environment-specific so it lives outside the
canonical `Dockerfile`.

### 2. Create the kind cluster

```bash
kind create cluster --name scg-e2e --wait 60s
```

Verify:

```bash
kubectl get nodes
# NAME                    STATUS   ROLES           AGE   VERSION
# scg-e2e-control-plane   Ready    control-plane   30s   v1.35.0
```

### 3. Load the gateway image into kind

kind nodes don't share the host Docker daemon's image cache. Load
the image explicitly:

```bash
kind load docker-image scg:e2e --name scg-e2e
```

This is the step that makes `image.pullPolicy: Never` in the Helm
values work — the image is already on the kind node so kubelet
never asks a registry for it.

### 4. Deploy the Spark Connect backends

```bash
kubectl create namespace spark-connect
kubectl apply -f deploy/examples/e2e-smoke/spark-connect-server.yaml
```

This creates a 2-replica Deployment of `apache/spark:4.0.0`
running `start-connect-server.sh --wait` (the `--wait` flag is
critical — without it the launcher script forks the JVM and
exits, taking the container with it).

First boot pulls the ~700 MiB Spark image. Wait until both pods
report `Ready`:

```bash
kubectl wait --for=condition=ready pod \
  -l app=spark-connect-server \
  -n spark-connect --timeout=300s
```

### 5. Install the gateway via Helm

```bash
helm install scg ./deploy/helm/scg \
  -n spark-connect \
  -f deploy/examples/e2e-smoke/values.yaml
```

The values file overrides the chart defaults to:

* Use the local image `scg:e2e` with `pullPolicy: Never`
* Single replica (no Redis, no HA)
* In-memory affinity store (no bundled Redis StatefulSet)
* K8s `Endpoints`-watch discovery pointed at the
  `spark-connect` Service from step 4
* `auth: none`, audit on

Verify the gateway is up and discovered both backends:

```bash
kubectl logs -n spark-connect deploy/scg --tail=20 | grep "backend list updated"
# {"timestamp":"...","level":"INFO","fields":{"message":"k8s pool: backend list updated","count":2}}
```

If `count` says `1` for a few seconds before going to `2`, that's
expected — pods become Endpoints members as they pass their
readiness probe.

### 6. Run a PySpark client through the gateway

```bash
# In a new terminal:
kubectl port-forward -n spark-connect svc/scg 15003:15003
```

Set up a virtualenv with PySpark and its Connect extras:

```bash
python3 -m venv /tmp/scg-e2e-venv
/tmp/scg-e2e-venv/bin/pip install 'pyspark[connect]'
```

(If `pip` is blocked by the same corporate proxy that may have
hit you in step 1, configure pip with whatever proxy your
environment uses — e.g. `pip install --index-url
https://YOUR-INTERNAL-PYPI-PROXY/simple 'pyspark[connect]'`.)

Run a smoke query against the gateway:

```bash
/tmp/scg-e2e-venv/bin/python3 - <<'PY'
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15003").getOrCreate()

# Basic query
print("count =", spark.range(10).count())
print("sum   =", spark.range(100).agg({"id": "sum"}).collect()[0][0])

# Session affinity: TempView only exists in one driver's memory.
# If a later RPC hit a different driver, this would fail.
spark.range(50).createOrReplaceTempView("t")
print("temp view count =", spark.sql("SELECT count(*) FROM t").collect()[0][0])
print("temp view max   =", spark.sql("SELECT max(id) FROM t").collect()[0][0])

spark.stop()
PY
```

Expected output:

```
count = 10
sum   = 4950
temp view count = 50
temp view max   = 49
```

### 7. Drive multiple sessions; verify both backends get traffic

The previous step opened a *single* `SparkSession`, which under
session affinity binds to exactly one of the two backends. To
prove the second backend is actually reachable through the
gateway — not just discovered by the K8s watcher — open several
sessions in a loop and check the audit log:

```bash
/tmp/scg-e2e-venv/bin/python3 - <<'PY'
from pyspark.sql import SparkSession

# Each iteration produces a fresh SparkSession with a new
# session_id, so the gateway's round-robin picker assigns
# successive sessions to different backends from the pool.
for i in range(4):
    spark = SparkSession.builder.remote("sc://localhost:15003").getOrCreate()
    n = spark.range(10).count()
    sid = spark.client._session_id
    print(f"session {i}: count={n} session_id={sid}")
    spark.stop()
PY
```

Then inspect which backend each session bound to:

```bash
kubectl logs -n spark-connect deploy/scg --tail=500 \
  | grep '"event":"session.create"' \
  | /tmp/scg-e2e-venv/bin/python3 -c "
import sys, json
for line in sys.stdin:
    f = json.loads(line).get('fields', {})
    print(f.get('session_id', '?')[:8], '->', f.get('backend', '?'))
"
```

Expected (exact session ids and pod IPs will differ; the
**number of distinct `backend` values is what matters**):

```
4d2c702c -> 10.244.0.5:15002
35636f1a -> 10.244.0.6:15002
b6fd6df7 -> 10.244.0.5:15002
e7f1cba9 -> 10.244.0.6:15002
```

Both pod IPs from `kubectl get endpoints spark-connect -n spark-connect`
should appear at least once. If every line shows the same backend,
the gateway is somehow funnelling all sessions to one pod — see the
troubleshooting note at the bottom of this README.

### 8. Verify the audit log

```bash
kubectl logs -n spark-connect deploy/scg --tail=200 \
  | grep '"event":"session.create"\|"event":"session.release"' \
  | python3 -c "import sys, json; [print(json.loads(l)['fields']['event'], json.loads(l)['fields'].get('session_id',''), json.loads(l)['fields'].get('backend','')) for l in sys.stdin]"
```

You should see one `session.create` event per `SparkSession` your
scripts created (one from step 6, four from step 7), all with the
same backend address *within* a session, followed by
`session.release` events as each `spark.stop()` ran.

### 9. Verify the metrics endpoint

```bash
# In another new terminal:
kubectl port-forward -n spark-connect svc/scg 9090:9090

# Then:
curl -s http://localhost:9090/metrics | grep "^scg_" | head
```

Two assertions worth eyeballing:

* `scg_backend_pool_size 2` matches the two Spark Connect backends
  the gateway discovered in step 5. If it reads `0`, the
  K8s-watcher → metric wiring is broken: the gauge should follow
  the same number that the `"backend list updated"` log line in
  step 5 reported.
* The `scg_rpc_duration_seconds_bucket` histogram bucket counts
  should match the number of RPCs your PySpark client issued
  (PySpark sends a handful of `Config` / `AnalyzePlan` calls per
  `getOrCreate`, then `ExecutePlan` per DataFrame operation).

## Tearing down

```bash
# Kill any background port-forwards first
helm uninstall scg -n spark-connect
kubectl delete -f deploy/examples/e2e-smoke/spark-connect-server.yaml
kubectl delete namespace spark-connect
kind delete cluster --name scg-e2e
```

## Troubleshooting

### Spark Connect pods stuck in `CrashLoopBackOff`

Most common cause: the container is using `start-connect-server.sh`
without the `--wait` flag, so the launcher daemonizes and the
container exits. The manifest in this directory includes `--wait`;
if you're using a different one, add it.

Less common cause: the manifest passes `--packages
org.apache.spark:spark-connect_2.13:...`. The runtime user in
`apache/spark:4.0.0` has no `$HOME`, so Ivy can't write its
download cache. The Spark Connect server jar is already bundled in
the image (`/opt/spark/jars/spark-connect_2.13-4.0.0.jar`); drop
the `--packages` line.

### Gateway logs show `count: 0` for the backend list and never increases

Either the K8s ServiceAccount lacks `endpoints` get/list/watch RBAC,
or the `serviceName` / `namespace` in the Helm values don't point at
a real Service.

Check RBAC:

```bash
kubectl get role,rolebinding -n spark-connect
# Should show scg-endpoints-watcher Role + RoleBinding
```

Check the Service exists and has Endpoints:

```bash
kubectl get endpoints spark-connect -n spark-connect
# NAME             ENDPOINTS                              AGE
# spark-connect   10.244.0.10:15002,10.244.0.11:15002    2m
```

### Step 7's multi-session run shows only one backend in the audit log

Two common causes:

1. **The pool only had one backend when the sessions ran.** Check
   `scg_backend_pool_size` (step 9) and the `"backend list updated"`
   log line — if either says `1`, the second Spark Connect pod was
   not Ready during step 7. Re-run step 7 after both pods report
   `READY 1/1` in `kubectl get pods -n spark-connect`.
2. **The Python loop somehow reused the same session.** PySpark
   caches the active `SparkSession` on the driver process, so
   skipping `spark.stop()` between iterations makes `getOrCreate()`
   return the *same* session — and therefore the same backend. The
   sample in step 7 calls `spark.stop()` at the end of each iteration
   for this reason; if you adapted the script, keep that call.

Round-robin advances on every backend `pick()` — that includes
the RPCs a single session sends, not just session creation. With
two backends, whether successive *sessions* land on different
backends therefore depends on how many RPCs each session
internally drives the gateway to pick. PySpark's
`getOrCreate` + `range(10).count()` happens to advance the
cursor an odd number of times per session, so sessions
alternate. If you adapt the script to do more work per session
and end up always hitting one backend, that is a property of
your workload, not a bug — increase the loop count or add a
shorter "control" session that does one trivial RPC to confirm
the second backend is reachable.

### PySpark client fails with `[PACKAGE_NOT_INSTALLED]`

PySpark Connect needs several optional dependencies (`pandas`,
`pyarrow`, `grpcio-status`, `zstandard`). The `pyspark[connect]`
extra installs them all at once; using a bare `pyspark` install
will fail at session creation with these errors.

### Query succeeds but a `Config` RPC shows up as `rpc.error` in the audit log

PySpark 4.1+ tries to read SQL configs that don't exist on Spark
4.0.0 backends (e.g. `spark.connect.session.planCompression.threshold`).
The backend returns `Internal: SQL_CONF_NOT_FOUND`, the client
catches it and proceeds. Harmless. If you want a clean run, match
the PySpark and Spark Connect server versions exactly.
