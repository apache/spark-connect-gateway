# Multi-replica gateway with Redis affinity store

Walks through standing up the gateway as a **2-replica Deployment**
backed by the chart's bundled Redis, then verifying the affinity
store is genuinely shared. The two replicas can serve the same
session because the `(user, session) → backend` binding lives in
Redis, not in pod memory.

This is the canonical HA shape for production: client traffic load
balances across both gateway pods, either one can die and come back
without losing session affinity.

The whole walkthrough takes ~15 minutes on a laptop with the Spark
and Redis images already cached.

## What this exercises

* The `scg-store-redis` crate against a real bundled Redis
  StatefulSet (not the testcontainers-driven unit tests).
* The Helm chart's Redis-enabled topology: 2 gateway replicas +
  1 Redis StatefulSet pod + the Service that joins them.
* The chart's `scg.redis.host` / `scg.redis.url` templating that
  synthesizes the in-cluster Redis URL from the bundled
  StatefulSet.
* Gateway pod restart with affinity state surviving in Redis.

## What this does NOT exercise

* Sticky cross-replica routing under a real load balancer (use
  the in-process [`ha_smoke.rs`](../../crates/proxy/examples/ha_smoke.rs)
  example — it spins up two gateway processes against the same
  Redis and drives RPCs through each, proving the binding is
  reused). The kind-based walkthrough validates infrastructure
  topology; `ha_smoke` validates the cross-replica semantics.
* Redis failure recovery (chart's bundled Redis is a 1-replica
  StatefulSet — production deployments point at a managed Redis).

## Prerequisites

```
docker        # any recent version
kind          # brew install kind
kubectl
helm 4.x
python 3.11+  # for the PySpark client
```

Hardware footprint is the e2e-smoke baseline (~4 GiB free RAM,
~3 GiB free disk) plus headroom for a second gateway pod (~50 MB)
and the Redis container (~10 MB).

## Step-by-step

All commands run from the repo root.

### 1. Build the gateway image, create the kind cluster, load the image

```bash
# from the repo root:
docker build -t scg:e2e .
kind create cluster --name scg-redis --wait 60s
kind load docker-image scg:e2e --name scg-redis
```

See [e2e-smoke/README.md](../e2e-smoke/README.md#1-build-the-gateway-image)
for the build-time troubleshooting if your network blocks crates.io.

### 2. Deploy the Spark Connect server backends

```bash
kubectl create namespace spark-connect
kubectl apply -f deploy/examples/e2e-multi-replica-redis/spark-connect-server.yaml

kubectl wait --for=condition=ready pod \
  -l app=spark-connect-server \
  -n spark-connect --timeout=300s
```

### 3. Install the gateway with Redis and 2 replicas

```bash
helm install scg ./deploy/helm/scg \
  -n spark-connect \
  -f deploy/examples/e2e-multi-replica-redis/values.yaml
```

The chart this time brings up three new resources beyond what
the e2e-smoke walkthrough produced:

* **A Redis StatefulSet** — single-replica `redis:7-alpine` with
  a PersistentVolumeClaim for the AOF file.
* **A Redis Service** — `scg-redis` in the same namespace; the
  gateway dials this hostname.
* **A second gateway pod** — `replicaCount: 2`.

Wait for everything to come up:

```bash
kubectl wait --for=condition=ready pod \
  -l app.kubernetes.io/name=scg \
  -n spark-connect --timeout=120s

kubectl get pods -n spark-connect
# NAME                                    READY   STATUS    RESTARTS   AGE
# scg-redis-0                             1/1     Running   0          1m
# scg-xxxxxxxxxx-xxxxx                    1/1     Running   0          1m
# scg-xxxxxxxxxx-yyyyy                    1/1     Running   0          1m
# spark-connect-server-xxxxxxxxxx-xxxxx   1/1     Running   0          2m
# spark-connect-server-xxxxxxxxxx-yyyyy   1/1     Running   0          2m
```

### 4. Verify the gateway connected to Redis

Each gateway replica logs its `affinity_store` choice on startup:

```bash
kubectl logs -n spark-connect -l app.kubernetes.io/name=scg \
  --tail=5 | grep "spark-connect-gateway starting"
```

Each pod should report `"affinity_store":"redis"` in the
structured log line. (memory would mean the values.yaml didn't
take effect.)

### 5. Drive a PySpark client through the gateway

Same as e2e-smoke. In a new terminal:

```bash
kubectl port-forward -n spark-connect svc/scg 15003:15003
```

(`svc/scg` is the gateway Service; the port-forward picks one of
the two pods by round-robin, but for this step it doesn't matter
which.)

Set up the venv if you haven't already:

```bash
python3 -m venv /tmp/scg-e2e-venv
/tmp/scg-e2e-venv/bin/pip install 'pyspark[connect]'
```

Run a smoke query:

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

Expected output:

```
count = 10
temp view count = 50
```

### 6. Inspect Redis to confirm the binding lives there

Open a `redis-cli` shell inside the Redis pod:

```bash
kubectl exec -it -n spark-connect statefulset/scg-redis -- redis-cli
```

Then:

```
127.0.0.1:6379> KEYS scg:s:*
1) "scg:s:default|anonymous|abc12345-…"
127.0.0.1:6379> GET scg:s:default|anonymous|abc12345-…
"10.244.0.11:15002"
127.0.0.1:6379> TTL scg:s:default|anonymous|abc12345-…
(integer) 3597
```

What this shows:

* The key format is `{prefix}:s:{tenant}|{user_id}|{session_id}`
  with the chart's default prefix `scg`. Single tenant, anonymous
  user, the PySpark-generated session id.
* The value is the backend address the gateway picked — a real
  Spark Connect server pod IP.
* The TTL is the chart's `sessionTtlSecs: 3600` minus however long
  the session has been live.

Type `EXIT` to leave the redis-cli.

### 7. Kill one gateway replica; affinity survives

Pick one gateway pod and delete it:

```bash
kubectl delete pod -n spark-connect \
  $(kubectl get pods -n spark-connect -l app.kubernetes.io/name=scg \
      -o jsonpath='{.items[0].metadata.name}')
```

Kubernetes immediately starts a replacement (Deployment controller
sees `replicas: 2` and one is missing). Wait for both to be Ready
again:

```bash
kubectl wait --for=condition=ready pod \
  -l app.kubernetes.io/name=scg \
  -n spark-connect --timeout=60s
```

The Redis binding key from step 6 is **still there** —
the affinity store lives in Redis, not in the gateway pod's
memory. Verify:

```bash
kubectl exec -it -n spark-connect statefulset/scg-redis -- \
  redis-cli KEYS 'scg:s:*'
# 1) "scg:s:default|anonymous|abc12345-…"
```

The killed replica's process state is gone, but the binding the
*system* knows about is unchanged. If a client reconnects to the
new replica with the same `session_id`, it would route to the same
backend.

This is what `affinity_store: memory` cannot do: with a memory
store, killing a gateway pod loses its half of the binding table.
A multi-replica memory-store deployment is incoherent — two
clients with the same `session_id` landing on different replicas
get different backends.

### 8. Verify the structured-log audit stream still works

Same as e2e-smoke. Audit events emit on both replicas; collect
them across pods with the deployment selector:

```bash
kubectl logs -n spark-connect -l app.kubernetes.io/name=scg \
  --tail=200 \
  | grep '"event":"session.create"\|"event":"session.release"'
```

## What you've proved

| Property | How |
|---|---|
| 2 gateway replicas can coexist behind one Service | step 3: `kubectl get pods` shows both Running |
| Both replicas dial the same Redis | step 4: both report `affinity_store: redis` |
| Session bindings persist in Redis, not pod memory | step 6: `redis-cli KEYS` shows the binding key |
| Gateway pod restart doesn't lose binding state | step 7: binding key survives `kubectl delete pod` |
| PySpark client correctness is preserved through the Redis path | step 5: query returns the right answer |

The "actual cross-replica session reuse" assertion (a client
that bound through replica A reaches the same backend through
replica B) is unit-tested in the in-process
[`ha_smoke.rs`](../../crates/proxy/examples/ha_smoke.rs) example,
which spins up two gateway processes pointed at the same Redis
container and drives RPCs through each. The K8s walkthrough here
proves the topology; `ha_smoke` proves the semantics.

## Tearing down

```bash
helm uninstall scg -n spark-connect
kubectl delete -f deploy/examples/e2e-multi-replica-redis/spark-connect-server.yaml
# The PVC the Redis StatefulSet created is intentionally NOT
# garbage-collected by `helm uninstall`. Delete it manually if you
# want a clean slate:
kubectl delete pvc -n spark-connect -l app.kubernetes.io/name=scg
kubectl delete namespace spark-connect
kind delete cluster --name scg-redis
```

## Troubleshooting

### The Redis pod is stuck in `Pending`

The PVC can't bind. Check:

```bash
kubectl get pvc -n spark-connect
kubectl get storageclass
```

A kind cluster ships a default `standard` storage class with
dynamic provisioning, so this is rare. If you're on a cluster
without a default storage class, either set
`redis.persistence.storageClass` in values.yaml to a class that
exists, or disable persistence (`redis.persistence.enabled: false`)
for the walkthrough — the binding is fine to lose between runs.

### Gateway pod log says `affinity_store: memory` instead of `redis`

The values.yaml didn't take effect. Common causes:

1. **Wrong `-f` path on `helm install`** — double-check you
   passed `deploy/examples/e2e-multi-replica-redis/values.yaml`
   (not e2e-smoke's, which uses memory).
2. **An earlier release named `scg` still installed.** Helm
   silently keeps the previous values on `helm install` if the
   release exists; use `helm upgrade --install` or
   `helm uninstall scg` first.

### `redis-cli KEYS 'scg:s:*'` returns empty

Either the PySpark client step never bound a session (the query
failed before `getOrCreate` completed), or the gateway is talking
to a different Redis than the one you're inspecting. Check the
gateway log for `redis` errors:

```bash
kubectl logs -n spark-connect -l app.kubernetes.io/name=scg \
  | grep -i "redis"
```

A line containing `connecting rate-limit redis at …` or similar
identifies which URL the gateway dialed. It should match
`redis://scg-redis:6379` (the chart's synthesized URL).

### Gateway log shows `redis connect failed; retrying` during startup

Expected on cold cluster boot. The gateway and Redis pods come up
in parallel, and the gateway often reaches its `RedisStore::connect`
step before the Redis pod is `Ready`. Look for log lines like:

```
{"level":"WARN", "message":"redis connect failed; retrying",
 "target":"affinity-store redis at redis://scg-redis:6379",
 "attempt":1, "error":"... Connection refused ..."}
```

The gateway retries every 2 seconds for up to 60 seconds, then
gives up. A 2-3 attempt run during cold cluster boot is normal;
look for a follow-up `redis connect succeeded after retry` line.

If the gateway exits with `connecting to ... failed after N
attempts`, it tried for the full 60 seconds without success —
that's a real misconfiguration (wrong Redis URL, NetworkPolicy
blocking the port, Redis pod failing to start). Check the Redis
pod first:

```bash
kubectl get pods -n spark-connect scg-redis-0
kubectl describe pod -n spark-connect scg-redis-0
```
