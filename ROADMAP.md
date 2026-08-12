# Roadmap

Where the gateway is heading, what is deliberately out of scope,
and what would move an item from "planned" to "in progress".
Items here are direction, not commitment — no dates. If one of
these matters to your deployment, open an issue describing your
topology; concrete use cases are what reprioritise this list.

The gateway's scope principle (see
[`docs/architecture.md`](docs/architecture.md)): it is a **data
plane** component. It routes Spark Connect RPCs to existing
drivers and keeps sessions sticky. Features that would turn it
into a control plane — creating drivers, managing their
lifecycle — stay out, and integration points are exposed
instead.

## Multi-cluster support

**Today:** Kubernetes discovery is single-cluster. The
`Endpoints` watcher authenticates with the in-cluster
ServiceAccount and watches Services in the cluster the gateway
runs in.

Two network planes matter here, and they fail differently:

* *Control plane* — watching a **remote** cluster's Endpoints
  only needs its API server, which is normally reachable from
  outside (that is how `kubectl` works from anywhere). Extending
  the per-pool discovery config with an optional
  kubeconfig/context is a short, contained change: every pool
  already runs its own independent watcher task.
* *Data plane* — the watch hands back pod IPs from the remote
  cluster's pod CIDR, and those are **not routable** from the
  gateway's cluster by default. Discovery would succeed while
  every connection fails.

**What works today without any gateway change:** static backend
pools (`backendDiscovery.type: static`) accept any reachable
`host:port`, and per-tenant pool overrides can mix static and
K8s discovery. Expose the remote cluster's Spark Connect servers
behind a LoadBalancer / NodePort / ingress and list those
addresses in a static pool. You trade away dynamic discovery for
the remote pool; affinity, auth, audit, and metrics behave
identically.

**Planned shape:** an optional per-pool kubeconfig / context /
API-server endpoint in the K8s discovery config, so one gateway
can watch pools across clusters. Pays off in deployments that
already run flat networking between clusters (Cilium
ClusterMesh, Submariner, Istio multi-cluster — with
non-overlapping pod CIDRs), where the watched pod IPs are
directly dialable.

**Open question:** gateway-side address translation ("watched
pod X, dial ingress Y") would support multi-cluster *without* a
mesh, but adds real config-surface complexity. Not speculating
this into the design until a concrete deployment needs it —
if that's you, please open an issue with your topology.

*This item exists because an early evaluator asked for the
multi-cluster position to be written down. More of that feedback
is welcome.*

## Cold-start-on-demand integration hook

**Today:** pools must be standing before the first client
connects. A session arriving for a tenant whose pool is empty
gets `Unavailable`.

**Planned shape:** two cooperating pieces, neither of which has
the gateway creating drivers itself:

* Gateway side — a configurable wait-or-retry window on
  empty-pool, instead of immediately failing the RPC.
* Provisioner side — whatever signal the deployment's operator
  wants to act on (an Endpoints subscription, a webhook, a CRD
  the gateway writes). The provisioning act stays the
  provisioner's job.

Most production deployments keep warm pools and never hit this;
it matters for SaaS shapes with long-tail tenants.

## Per-tenant warm pools and weighted backend selection

**Today:** every tenant pool uses round-robin across its
backends; there is no pre-provisioning logic and no way to
weight backends within one tenant's pool (e.g. a tier of larger
drivers preferred over smaller ones).

**Planned shape:** the pluggable selection seam now exists in
`scg-routing` (`SelectionStrategy`, one instance per pool, with
round-robin as the shipping default; pool members carry
labels/weight metadata for strategies to key on). Next steps
behind that seam: a `weight` attribute on static pool config
entries, a least-sessions strategy, and metadata-aware selection
(which also serves version-aware placement during rolling
upgrades). Warm-pool management belongs to the provisioner (see
above); the gateway's part is at most exposing "pool below
desired size" signals.

## Helm chart: HMAC secret via Kubernetes `Secret`

**Today:** `auth.jwt.key.kind: hmacSecret` templates the secret
string as plaintext into the gateway's ConfigMap (see the note
in [`deploy/examples/e2e-auth-jwt/`](deploy/examples/e2e-auth-jwt/README.md)).
Fine for walkthroughs; wrong posture for production — ConfigMaps
have looser RBAC and no encryption-at-rest expectations.

**Planned shape:** a `secretRef: {name, key}` alternative in the
chart that mounts from a `Secret` and an env-var (or file) read
path in the gateway binary. Asymmetric deployments (`pemFile` /
`pemInline` with a *public* key, or OIDC) don't have this
problem — the public half is not sensitive — which is one more
reason production should prefer them over HMAC anyway.

## Durable audit sink

**Today:** audit events ride the same `tracing` → stdout
pipeline as operational logs, distinguished by
`target="scg::audit"` (see
[`docs/observability.md`](docs/observability.md#audit-logging)).
Delivery is best-effort: a pod evicted before the log shipper
catches up can lose the tail.

**Planned shape:** an optional `tracing_subscriber::Layer` that
intercepts `target="scg::audit"` events and writes them
synchronously to a durable sink (Kafka, SQS, append-only
object storage) **in addition to** stdout. The field schema is
already treated as an API contract, so the layer can be added
without touching the emitting code. Only needed where compliance
requires at-least-once audit delivery; the stdout pipeline stays
the default.

## `Endpoints` → `EndpointSlice` migration

**Today:** the K8s watcher consumes the legacy `core/v1
Endpoints` resource — universally supported, one object per
Service, but deprecated in favour of `discovery.k8s.io/v1
EndpointSlice` since K8s 1.33 (you may see kubectl warnings; the
data still flows). `Endpoints` also truncates beyond 1000
addresses, which no current deployment approaches.

**Planned shape:** switch (or dual-source) the watcher to
`EndpointSlice`. Mostly mechanical; partially gated on upstream
`kube-rs` ergonomics for slice aggregation.

## Distributed-trace continuity for inbound `traceparent`

**Today:** the gateway opens a root `scg_rpc` span per RPC and
injects a fresh `traceparent` toward the backend. Parenting the
gateway span to an *inbound* `traceparent` (so the client's
trace and the gateway's trace join) is limited by upstream
`tracing-opentelemetry` ↔ `opentelemetry_sdk` plumbing.
Root-span traces work end-to-end today.

**Planned shape:** revisit when the upstream crates settle;
no gateway-side design work needed beyond adopting it.

## Non-goals

These are deliberate scope exclusions, not unprioritised work.
Argued in full in the project SPIP; summarised here so the
roadmap isn't read as "everything else is coming eventually".

* **Distributing one Spark query across multiple drivers.** A
  `SparkSession` is owned by exactly one driver; the gateway
  balances at *session* granularity.
* **Replicating session state across drivers.** When a driver
  dies its sessions die with it; the gateway surfaces this
  cleanly (`Unavailable`) rather than pretending otherwise.
* **Extending or replacing the Spark Connect protocol.** The
  gateway adds nothing to the wire protocol; it is transparent to
  payloads and depends only on a small set of request-envelope
  fields.
* **Driver lifecycle management.** Provisioning, scaling
  decisions, and warm-starting drivers belong to the
  provisioner (spark-kubernetes-operator, Kubeflow Spark
  Operator, HPA, …). The gateway reacts to pool changes; it
  does not initiate them. See the
  [Kubeflow operator walkthrough](deploy/examples/e2e-kubeflow-spark-operator/README.md)
  for how the composition works in practice.
* **TLS termination.** Delegated to the Ingress / service mesh
  in front of the gateway.
* **Sidecar or in-driver embedding.** The gateway is its own
  deployable; embedding it in one driver would couple routing to
  that driver's view of the world.
