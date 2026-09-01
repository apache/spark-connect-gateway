# Reference Spark Connect server deployment

These manifests stand up a small pool of Spark Connect server pods that the
gateway can route to. They depend on the upstream
[`apache/spark-kubernetes-operator`](https://github.com/apache/spark-kubernetes-operator)
being installed in the cluster — see its README for installation.

## Layout

- `namespace.yaml` — dedicated namespace for the example.
- `spark-connect-server-1.yaml`, `spark-connect-server-2.yaml` — two instances
  of `SparkConnectServer` (one CR per instance, so the gateway has two
  distinct backend addresses to choose between).
- `service.yaml` — a single `ClusterIP` service that fronts both instances by
  label selector. The gateway can be configured to either:
  - point its static backend list at each instance's pod IP / per-instance
    Service (recommended, so stickiness is observable), or
  - point a single entry at this Service (acceptable, but kube-proxy will
    pick a pod per TCP connection — fine for unary RPCs, but Spark Connect
    streams must always land on the same pod for the lifetime of a session).

## Apply

```bash
kubectl apply -f namespace.yaml
kubectl apply -f spark-connect-server-1.yaml
kubectl apply -f spark-connect-server-2.yaml
kubectl apply -f service.yaml
```

## Get backend addresses

```bash
kubectl -n spark-connect get pods -l app=spark-connect -o wide
```

Use each pod's IP plus port 15002 in the gateway's `config.yaml`.
