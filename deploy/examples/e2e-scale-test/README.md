# K8s endpoint-watcher scale test

Walks through standing up the gateway against a Spark Connect server
Deployment, then **scaling that Deployment up and down at runtime**
to verify the gateway's `pool-k8s` watcher picks up the changes
without a gateway restart.

This is what differentiates the K8s discovery backend from the
static one: pods can come and go, and the gateway adapts in
seconds. If your operations involve `kubectl scale` (or a Spark
operator scaling driver pods), this is the path you depend on.

The whole walkthrough takes ~15 minutes on a laptop with the Spark
image already cached (~30 minutes cold).

## What this exercises

* The K8s `Endpoints`-watching backend pool (`pool-k8s`) reacting
  to pod additions and removals **at runtime** — not just at
  gateway startup.
* The structured log line `"k8s pool: backend list updated"`
  firing on each Endpoints transition.

## What this does NOT exercise

* PySpark client correctness (use the
  [`e2e-smoke`](../e2e-smoke/) walkthrough for that).
* HA across multiple gateway replicas (use
  [`e2e-multi-replica-redis`](../e2e-multi-replica-redis/) for
  that).
* Affinity behaviour during a backend pod death — when a backend
  pod is deleted, sessions bound to it can't recover (their
  driver-local `SparkSession` state is gone). The gateway notices
  the binding's address is no longer in the pool and the next RPC
  for that session returns `Unavailable`. This is the documented
  Spark Connect contract, not a gateway bug.

## Prerequisites

```
docker        # any recent version
kind          # brew install kind
kubectl
helm 4.x
```

Same hardware footprint as the e2e-smoke walkthrough (~4 GiB free
RAM, ~3 GiB free disk) plus headroom for a third Spark Connect
pod during the scale-up step.

## Step-by-step

All commands run from the repo root.

### 1. Build the gateway image

```bash
# from the repo root:
docker build -t scg:e2e .
```

See [e2e-smoke/README.md](../e2e-smoke/README.md#1-build-the-gateway-image)
for the troubleshooting if your network blocks crates.io.

### 2. Create the kind cluster and load the image

```bash
kind create cluster --name scg-scale --wait 60s
kind load docker-image scg:e2e --name scg-scale
```

### 3. Deploy two Spark Connect server replicas + the gateway

```bash
kubectl create namespace spark-connect
kubectl apply -f deploy/examples/e2e-scale-test/spark-connect-server.yaml

kubectl wait --for=condition=ready pod \
  -l app=spark-connect-server \
  -n spark-connect --timeout=300s

helm install scg ./deploy/helm/scg \
  -n spark-connect \
  -f deploy/examples/e2e-scale-test/values.yaml
```

Wait for the gateway to come up and confirm it discovered both
existing backends:

```bash
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=scg \
  -n spark-connect --timeout=60s

kubectl logs -n spark-connect deploy/scg | grep "k8s pool"
# {"timestamp":"...","level":"INFO","fields":{"message":"k8s pool: backend list updated","count":1}}
# {"timestamp":"...","level":"INFO","fields":{"message":"k8s pool: backend list updated","count":2}}
```

Two `backend list updated` lines: the watcher saw one pod, then
the other. `count` represents the live size of the pool after each
Endpoints transition. (The two-step climb is normal — the two
pods rarely pass their readiness probe at the same exact moment.)

### 4. Start tailing the gateway log

In a new terminal:

```bash
kubectl logs -n spark-connect deploy/scg -f \
  | grep --line-buffered "k8s pool"
```

Leave this running for the rest of the walkthrough. Each pool
update appears live.

### 5. Scale Spark Connect from 2 → 3

```bash
kubectl scale deployment spark-connect-server \
  -n spark-connect --replicas=3
```

Watch the gateway log: within ~30–60 seconds (the time it takes
for the third pod to become Ready and join the Service Endpoints)
you should see:

```
"message":"k8s pool: backend list updated","count":3
```

The 30–60-second lag is dominated by the Spark JVM warm-up on
the new pod, *not* by the K8s watcher. The watcher reacts to the
Endpoints change within milliseconds, but a pod isn't in
Endpoints until its readiness probe passes — which requires the
JVM to bind port 15002.

To see the full sequence:

```bash
# In yet another terminal:
kubectl get endpoints spark-connect -n spark-connect -w
```

Watch the `ENDPOINTS` column: it goes from two IPs to three as
the new pod becomes ready.

### 6. Scale Spark Connect from 3 → 1

```bash
kubectl scale deployment spark-connect-server \
  -n spark-connect --replicas=1
```

The Deployment controller picks two pods to terminate. Both move
to `Terminating`; their entries leave the Endpoints; the watcher
fires:

```
"message":"k8s pool: backend list updated","count":2
"message":"k8s pool: backend list updated","count":1
```

This usually happens **almost immediately** (sub-second from
`kubectl scale` to the log line). K8s removes a pod from the
Service's Endpoints as soon as it enters Terminating state — it
doesn't wait for the JVM to actually exit. The watcher then sees
the Endpoints change and updates the pool. In practice the two
log lines (`3 → 2 → 1`) usually arrive within milliseconds of
each other because the Deployment controller removes both pods
from Endpoints at the same time.

### 7. (Optional) Scale back up to verify recovery

```bash
kubectl scale deployment spark-connect-server \
  -n spark-connect --replicas=2
```

The watcher should see `count: 2` again once the new pod is
Ready. This proves the watcher isn't a one-shot — it stays
subscribed for the lifetime of the gateway pod.

## What you've proved

| Property | How |
|---|---|
| The watcher catches pod additions | step 5: count `2 → 3` after `kubectl scale --replicas=3` |
| The watcher catches pod removals | step 6: count `3 → 2 → 1` after `kubectl scale --replicas=1` |
| Lag is bounded by pod readiness, not by the watcher itself | step 5: ~30s for the new pod to pass `tcpSocket` probe, then `<1s` from Endpoints event to log line |
| The watcher isn't a one-shot | step 7: works again on a second scale-up |

Combined with the [e2e-smoke](../e2e-smoke/) walkthrough (which
proves PySpark client correctness against a stable backend set),
this completes the picture for the K8s discovery path.

## Tearing down

```bash
helm uninstall scg -n spark-connect
kubectl delete -f deploy/examples/e2e-scale-test/spark-connect-server.yaml
kubectl delete namespace spark-connect
kind delete cluster --name scg-scale
```

## Troubleshooting

### The new pod is Running but the gateway pool didn't update

Two common causes:

1. **The new pod is `Running` but not `Ready`.** A `Running` pod
   that hasn't passed its readiness probe is not in the Service
   Endpoints. Check `kubectl get pods -n spark-connect -o wide`
   for the `READY` column — `1/1` means the pod is in Endpoints,
   `0/1` means it's not. The gateway only sees Ready pods.

2. **The pod's labels don't match the Service selector.** Look
   at the Service:

   ```bash
   kubectl get svc spark-connect -n spark-connect -o yaml | grep -A 3 selector
   #   selector:
   #     app: spark-connect-server
   ```

   And the pod's labels:

   ```bash
   kubectl get pods -n spark-connect --show-labels
   ```

   The Service's selector keys must all match the pod labels. If
   the manifest you `apply`'d uses a different `app: ...` label,
   the Service won't see the pod and Endpoints stays empty.

### The pool count goes up but the log line is missing

Make sure you're tailing with `-f` and that `grep` is line-buffered
(`grep --line-buffered`). Without `--line-buffered`, grep batches
stdout and you may not see the line until the next log batch flushes.

### `kubectl get endpoints` prints a deprecation warning

```
Warning: v1 Endpoints is deprecated in v1.33+; use discovery.k8s.io/v1 EndpointSlice
```

Cosmetic from `kubectl`'s side — the data still comes through.
The gateway's watcher uses the older `v1 Endpoints` API; switching
it to `EndpointSlice` is on the upstream `kube-rs` roadmap. For the
walkthrough, ignore the warning.

### `kubectl scale` returns success but nothing happens

The Deployment controller decides what to do based on what pods
match the Deployment's selector. If you accidentally edited the
manifest's `selector` after applying, the controller may now
manage a different set of pods than expected. `kubectl describe
deployment spark-connect-server -n spark-connect` shows the
selector and the "current" replica count.
