# Performance baseline

Reference numbers for the gateway. The point of this document
isn't to claim "the gateway does N QPS" — your actual numbers will
differ widely with hardware, network, and workload. The point is to
have a reproducible *starting point* so:

* Future feature work (per-tenant routing, rate limiting, audit)
  can compare its overhead against a known baseline.
* Operators alarmed by their own perf numbers have something to
  compare to.
* Regressions get caught — re-run the harness on a release branch
  and a number jumping the wrong way is visible.

All numbers come from the load harness at
[`crates/proxy/examples/load.rs`](../crates/proxy/examples/load.rs).
**Run it yourself**, don't trust the table.

## Reproducing

```bash
cargo build -p scg-proxy --example load --release

./target/release/examples/load unary --workers 32 --duration-secs 30
./target/release/examples/load streaming --concurrency 100 --messages 100
./target/release/examples/load hc-overhead --workers 32 --duration-secs 20
./target/release/examples/load drain-under-load --concurrency 100 --messages 20 --delay-ms 50 --deadline-secs 5
./target/release/examples/load overhead --workers 32 --duration-secs 20
./target/release/examples/load redis-affinity --workers 32 --duration-secs 20  # requires a Redis on 127.0.0.1:6379
```

The harness runs entirely in-process: gateway, fake Spark Connect
backends, gRPC clients all in the same Tokio runtime. This is
**deliberately not a network benchmark** — the point is to measure
the gateway's overhead, not localhost loopback throughput. For real
deployments add network and RPC-shape variance accordingly.

## Test environment used for the numbers below

| | |
|---|---|
| CPU | Apple M4 Max (16 cores) |
| Memory | 64 GiB |
| OS | macOS 26.4 (Darwin 25.4) |
| Toolchain | Rust 1.94 stable, profile = release (LTO=thin, codegen-units=1) |
| Backends | 2 in-process FakeBackends |

## Scenario 1 — Unary throughput + latency

Config RPC, 32 workers running closed-loop (each waits for response
before sending next), 15s, 64 distinct session IDs (exercises the
in-memory affinity store on the bind path for the first round, then
the lookup path).

```
duration:            30.00s
total RPCs:         982560
errors:                  0
QPS:                 32752
latency (ms)    p50=0.959  p95=1.406  p99=1.615  p999=1.922  max=9.631
```

(Median of three 30s runs; per-run QPS spread is ±2%.)

What this tells you: a single gateway process can sustain ~32k
unary RPCs/sec on this hardware, with sub-2ms p99 latency. The
max is dominated by Tokio scheduler tail behaviour, not by any
gateway logic — same number whether you run with 8 workers or 32.

For most Spark Connect workloads, unary RPCs (Config / AnalyzePlan
/ ReleaseSession / etc.) are infrequent compared to ExecutePlan
streams, so the throughput ceiling rarely matters. Latency on
unary IS noticeable: a slow gateway shows up as everything-feels-
sticky.

## Scenario 2 — Streaming concurrency

100 concurrent ExecutePlan streams, each yielding 100 messages with
no per-message delay (i.e. as fast as the gateway can forward).

```
peak active_streams observed during fan-out: 0  (see note below)
duration:             0.01s
total RPCs:            100
errors:                  0
QPS (streams):        8000
latency (ms)    p50=7.715  p95=8.015  p99=8.063  p999=8.119  max=8.119
messages forwarded: 10000 (1141591 msg/s)
```

(Median of five back-to-back runs; the variance is high because the
whole scenario completes in ~10ms and any tokio scheduler glitch
dominates. Run-to-run p99 spread was 6.7ms → 22ms, msg/s spread was
430k → 1.35M. The median is the honest summary.)

100 streams × 100 messages = 10,000 messages forwarded in ~10ms.
The per-stream latency p99 is ~8ms — that's how long it takes to
forward 100 server-streamed messages through the gateway in the
typical case.

> *Note: `peak active_streams=0` is a measurement artefact, not a
> bug. The harness samples `scg_active_streams` once after fan-out;
> at the no-delay setting all streams complete before the sample
> point. Add `--delay-ms 10` and you'll see active_streams climb
> to ~100. The `drain-under-load` scenario does this correctly.*

## Scenario 3 — Health-check overhead

Same as Scenario 1 (32 workers × 10s), comparing
`affinityStore=memory + healthCheck.enabled=false` (baseline) to
`healthCheck.enabled=true` with default tunings (5s interval, 2s
timeout).

```
baseline (no HC):  rpcs=658996 errors=0 qps=32950 p50=0.952ms p99=1.611ms
with HC:           rpcs=654171 errors=0 qps=32709 p50=0.960ms p99=1.624ms

delta:             p50 +8µs   p99 +13µs   QPS −0.7%
```

The probe loop runs every 5s in a separate task; it's not on the
hot path. Differences are at the noise floor (sub-microsecond
latency delta, < 1% QPS). **Active health checking is essentially
free at this rate.** If you crank `intervalSecs` down to 1s and
add 100 backends, the math changes; benchmark before doing that.

## Scenario 4 — Graceful drain under load

Two configurations to show both ends of the deadline tradeoff.

### Clean drain (deadline > stream length)

100 concurrent streams, each 20 messages × 50ms delay = ~1s per
stream. Drain deadline 5s.

```
active_streams at drain trigger: 100 (target 100)
drain outcome:      clean
drain elapsed:      1.09s
streams completed:  100
streams cancelled:  0
```

Drain finishes ~1 second after trigger, exactly the natural stream
length. Every stream completes normally; client sees no
cancellations.

### Deadline-hit (deadline < stream length)

100 concurrent streams, each 50 messages × 100ms delay = ~5s per
stream. Drain deadline 2s.

```
active_streams at drain trigger: 100 (target 100)
drain outcome:      deadline-hit
drain elapsed:      2.00s
streams completed:  100  (see note)
streams cancelled:  0
test wall-clock:    5.13s
```

Drain reports `deadline-hit` at exactly 2s. **Note:** `streams
completed=100, cancelled=0` even though we hit the deadline —
this is correct behaviour and matches production semantics. When
the gateway's drain loop times out it triggers `serve_with_shutdown`,
but tonic gives the in-flight streams time to close gracefully.
The streams complete at their natural pace (~5s wall-clock total),
and only K8s `terminationGracePeriodSeconds` (the chart sets it
to `deadline_secs + 10`) finally SIGKILLs the pod if streams are
still running.

In production this means:

| | If `terminationGracePeriodSeconds > stream length` | If shorter |
|---|---|---|
| Clean | streams complete | — |
| Deadline-hit but streams short | streams complete naturally | streams complete naturally |
| Deadline-hit but streams long | streams complete naturally | streams cancelled by SIGKILL |

The chart's default of `deadline_secs + 10` for terminationGracePeriod
is enough buffer for normal cases. For very long Spark queries,
raise `shutdown.deadlineSecs` and the chart auto-raises the K8s
grace period to match.

## Scenario 5 — Per-feature hot-path overhead

Walks through five rig configurations (baseline → progressively
enabling tenant resolver, rate limiter, audit) and reports the
unary-RPC overhead added by each. The bucket is sized so it never
rejects; the audit logger uses default settings (no `rpc.ok`
events). The point is to measure the *check* / *emit* cost on
every RPC, not the work each feature protects against.

```
config                                  p50    p99    QPS      Δp50      Δp99      ΔQPS
baseline (with_auth_and_metrics)       0.967  1.644   32402   +0.000    +0.000     +0.0%
+ tenant_resolver                      0.973  1.639   32320   +0.006    -0.005     -0.3%
+ tenant_resolver + rate_limit         0.964  1.633   32578   -0.003    -0.011     +0.5%
+ tenant_resolver + audit              0.961  1.628   32660   -0.006    -0.016     +0.8%
+ all three                            0.970  1.633   32416   +0.003    -0.011     +0.0%
```

**Every delta is within the run-to-run noise floor (±2% QPS, single-
microsecond latency).** Half the deltas are negative — that's pure
variance, not the limiter making things faster. The honest reading:
tenant resolver + rate limiter + audit are *free* on this hot path
under this workload. The cost of each feature is dominated by what
it's supposed to do (resolve a JWT claim, take a Redis token in
distributed mode, emit a `session.create` event on a fresh binding)
— not by the per-RPC check.

This matches the design intent: rate limiter's fast path is a no-op
when no bucket is active, audit's per-event cost is one
`tracing::info!`, tenant resolver's cost is a `HashMap` lookup keyed
on a small `String`.

## Scenario 6 — Redis affinity store round-trip

Compares in-process `MemoryStore` against a `RedisStore` backed by
a real Redis (Docker on macOS, default port). Every RPC's affinity
path does a `GET` + `EXPIRE` against Redis on lookup and a
`SET NX EX` on first bind — two round trips per cache hit.

```
config                p50      p99      QPS
memory affinity       0.953ms  1.622ms  32896
redis affinity        7.671ms  11.751ms  4153

delta                 +6.7ms   +10.1ms  -87.4%
```

The +6.7ms p50 corresponds to ~3.3ms per Redis round trip on this
network (Docker bridge on macOS). That's slow by Linux-in-K8s
standards but fine for the harness's purpose, which is to show the
*shape*: per-RPC affinity-store latency dominates everything else
when using Redis.

Production expectations:

* **In-cluster Redis on Linux K8s** — typical inter-pod RTT is
  100–500µs; one round trip per RPC adds ~0.5–1ms p50, not 3.3ms.
  Plan for ~5–15× the in-memory QPS rather than ~85× lower.
* **Redis with TLS / cross-AZ** — closer to the Docker-on-macOS
  numbers here. Provision accordingly.
* **The QPS ratio is the load-test bottleneck, not the
  steady-state ceiling.** A real client opens a session once
  (`Config` + `AnalyzePlan`) and then drives a long `ExecutePlan`
  stream — the streaming path doesn't hit the affinity store after
  the first lookup. The throughput hit on Redis only matters when
  your workload is dominated by session create/destroy churn.

## What changes affect these numbers

The harness is deliberately blind to network and to real Spark
work. Things that *will* shift these numbers in real deployments
(documented here so you don't get surprised):

| Change | Direction | How much, roughly |
|---|---|---|
| Real network instead of localhost | latency up | typical 0.1–1ms inter-pod RTT in the same K8s zone |
| Real Spark Connect backend | latency dominated by query, not by us | gateway becomes invisible compared to even a trivial DataFrame op |
| `affinityStore.type=redis` (vs memory) | latency up | see Scenario 6 — Docker-on-macOS measured at ~3.3ms/round-trip; in-cluster Linux Redis closer to 0.5–1ms |
| `tracing.enabled=true` with active collector | tail latency up | typical p99 +0.5–2ms while the OTLP exporter batches |
| `auth.type=oidc` | first-RPC latency up | JWKS fetch is amortized; steady-state cost ~unchanged |
| 4× the workers / streams | throughput sublinear past ~CPU count | Tokio multi-thread runtime saturates around #cores |

## When to re-run

* Before every release branch (perf regression check).
* After any change to the routing hot path, the auth interceptor,
  or the metrics handle.
* When new hot-path features land — per-tenant routing, rate
  limiting, and audit each add some overhead; this baseline is
  the before-and-after comparison.

## Limitations

This harness deliberately does *not*:

* Test real-network latency (single-process, localhost only).
* Test real Spark Connect drivers (FakeBackend has fixed responses).
* Test multi-replica HA under load (use `ha_smoke` for HA semantics
  + this harness for per-replica perf, not both at once).
* Soak test (durations are seconds-to-minutes; for hour-scale leak
  detection, run unary in a loop and watch RSS).

Add criterion microbenchmarks if you need per-function profiling;
add a real K8s soak test if you need long-tail behaviour. Neither
replaces this baseline.
