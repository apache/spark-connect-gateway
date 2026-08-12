// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Load-test harness.
//!
//! Spins up a SparkConnectProxy + N FakeBackends in-process and
//! drives traffic at it through real gRPC clients. Measures
//! throughput, latency distribution, and the cost of the optional
//! features (active health probing, graceful drain).
//!
//! NOT a microbenchmark. The interesting question is end-to-end
//! gateway behaviour under realistic-ish concurrency, not the
//! per-instruction cost of `Router::resolve_session`. Use criterion
//! for the latter.
//!
//! Usage:
//!
//!   cargo run -p scg-proxy --example load --release -- \
//!       unary --workers 32 --duration-secs 30
//!
//!   cargo run -p scg-proxy --example load --release -- \
//!       streaming --concurrency 100 --messages 100
//!
//!   cargo run -p scg-proxy --example load --release -- \
//!       hc-overhead --workers 32 --duration-secs 20
//!
//!   cargo run -p scg-proxy --example load --release -- \
//!       drain-under-load --concurrency 100 --deadline-secs 10
//!
//!   cargo run -p scg-proxy --example load --release -- \
//!       overhead --workers 32 --duration-secs 20
//!
//!   cargo run -p scg-proxy --example load --release -- \
//!       redis-affinity --workers 32 --duration-secs 20 \
//!       --redis-url redis://127.0.0.1:6379

use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{Stream, StreamExt};
use hdrhistogram::Histogram;
use scg_audit::{AuditConfig, AuditLogger};
use scg_auth::{AnonymousAuthenticator, AuthInterceptor};
use scg_genproto::pb;
use scg_healthcheck::{HealthAwarePool, HealthCheckConfig};
use scg_observability::{Metrics, ReadinessProbe};
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_ratelimit::{
    BucketRate, LimiterObserver, MemoryLimiter, RateLimiter, RejectScope, TenantLimits,
};
use scg_routing::{AffinityStore, Pool, Router};
use scg_store_memory::MemoryStore;
use scg_store_redis::{RedisStore, RedisStoreConfig};
use scg_tenant::{OnMissing, TenantResolver, TenantResolverConfig, TenantSource};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

// ---------- Helpers ---------------------------------------------------------

/// No-op limiter observer — the perf rig configures a generous
/// bucket that never rejects, so we don't care about the on-reject
/// signal. Matches the noop pattern in the in-memory limiter's own
/// tests.
struct NoopLimiterObs;
impl LimiterObserver for NoopLimiterObs {
    fn on_reject(&self, _tenant: &str, _scope: RejectScope) {}
}

// ---------- Fake backend ----------------------------------------------------

#[derive(Clone, Default)]
struct FakeBackend {
    /// Per-message delay for streaming RPCs. 0 = as fast as possible.
    stream_delay_ms: u64,
    stream_messages: usize,
}

#[tonic::async_trait]
impl pb::spark_connect_service_server::SparkConnectService for FakeBackend {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = Result<pb::ExecutePlanResponse, Status>> + Send + 'static>>;
    type ReattachExecuteStream = Self::ExecutePlanStream;

    async fn config(
        &self,
        req: Request<pb::ConfigRequest>,
    ) -> Result<Response<pb::ConfigResponse>, Status> {
        let body = req.into_inner();
        Ok(Response::new(pb::ConfigResponse {
            session_id: body.session_id,
            ..Default::default()
        }))
    }

    async fn execute_plan(
        &self,
        req: Request<pb::ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let body = req.into_inner();
        let n = self.stream_messages;
        let delay_ms = self.stream_delay_ms;
        let stream = async_stream::stream! {
            for i in 0..n {
                if delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                yield Ok(pb::ExecutePlanResponse {
                    session_id: body.session_id.clone(),
                    operation_id: body.operation_id.clone().unwrap_or_default(),
                    response_id: format!("msg-{}", i),
                    ..Default::default()
                });
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    // The other RPCs are unused by the load harness — return Unimplemented.
    async fn analyze_plan(
        &self,
        _: Request<pb::AnalyzePlanRequest>,
    ) -> Result<Response<pb::AnalyzePlanResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn artifact_status(
        &self,
        _: Request<pb::ArtifactStatusesRequest>,
    ) -> Result<Response<pb::ArtifactStatusesResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn interrupt(
        &self,
        _: Request<pb::InterruptRequest>,
    ) -> Result<Response<pb::InterruptResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn release_execute(
        &self,
        _: Request<pb::ReleaseExecuteRequest>,
    ) -> Result<Response<pb::ReleaseExecuteResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn release_session(
        &self,
        _: Request<pb::ReleaseSessionRequest>,
    ) -> Result<Response<pb::ReleaseSessionResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn fetch_error_details(
        &self,
        _: Request<pb::FetchErrorDetailsRequest>,
    ) -> Result<Response<pb::FetchErrorDetailsResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn clone_session(
        &self,
        _: Request<pb::CloneSessionRequest>,
    ) -> Result<Response<pb::CloneSessionResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn get_status(
        &self,
        _: Request<pb::GetStatusRequest>,
    ) -> Result<Response<pb::GetStatusResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn reattach_execute(
        &self,
        _: Request<pb::ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        Err(Status::unimplemented("n/a"))
    }
    async fn add_artifacts(
        &self,
        _: Request<tonic::Streaming<pb::AddArtifactsRequest>>,
    ) -> Result<Response<pb::AddArtifactsResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
}

// ---------- Test rig --------------------------------------------------------

struct Rig {
    grpc: Channel,
    metrics: Metrics,
    readiness: ReadinessProbe,
    /// Dropped → backends shut down.
    _be_kills: Vec<oneshot::Sender<()>>,
    /// Sender → gateway shuts down.
    gw_kill: Option<oneshot::Sender<()>>,
}

#[derive(Clone, Debug)]
struct RigOpts {
    n_backends: usize,
    stream_messages: usize,
    stream_delay_ms: u64,
    health_check: bool,
    /// When `Some`, build a `TenantResolver` (always-default source)
    /// and route every RPC through it. Measures the tenant-resolution
    /// hot-path cost vs the baseline rig that uses `with_auth_and_metrics`
    /// (no resolver wired in).
    tenant_resolver: bool,
    /// When `Some`, wire in a `MemoryLimiter` with a generous quota
    /// (high enough that no test request gets throttled) so we can
    /// measure the per-RPC bucket-check cost in isolation.
    rate_limit: bool,
    /// When `true`, attach an enabled `AuditLogger`. Every RPC fires
    /// at least one tracing event; with `log_successful_rpcs=false`
    /// the cost is dominated by the session.create event on the
    /// binding path.
    audit: bool,
    /// When `Some(url)`, swap the in-process `MemoryStore` for a
    /// `RedisStore` pointed at this URL. Measures the affinity-store
    /// round-trip cost. The Redis must already be reachable; the
    /// harness fails fast if connect errors.
    affinity_redis_url: Option<String>,
}

impl Default for RigOpts {
    fn default() -> Self {
        Self {
            n_backends: 2,
            stream_messages: 100,
            stream_delay_ms: 0,
            health_check: false,
            tenant_resolver: false,
            rate_limit: false,
            audit: false,
            affinity_redis_url: None,
        }
    }
}

async fn spawn_rig(opts: RigOpts) -> Rig {
    // Backends.
    let mut backends = Vec::with_capacity(opts.n_backends);
    let mut be_kills = Vec::with_capacity(opts.n_backends);
    for _ in 0..opts.n_backends {
        let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = lis.local_addr().unwrap().to_string();
        let backend = FakeBackend {
            stream_delay_ms: opts.stream_delay_ms,
            stream_messages: opts.stream_messages,
        };
        let svc = pb::spark_connect_service_server::SparkConnectServiceServer::new(backend.clone());
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(TcpListenerStream::new(lis), async {
                    let _ = rx.await;
                })
                .await
                .ok();
        });
        backends.push(addr);
        be_kills.push(tx);
    }

    // Gateway.
    let metrics = Metrics::new().unwrap();
    let readiness = ReadinessProbe::new(true);
    let inner: Arc<dyn Pool> = Arc::new(StaticPool::new(backends.clone()).unwrap());
    let pool: Arc<dyn Pool> = if opts.health_check {
        let hc_cfg = HealthCheckConfig {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(2),
            unhealthy_threshold: 3,
            healthy_threshold: 2,
        };
        let wrapped = HealthAwarePool::new(inner, hc_cfg);
        let _probe = wrapped.spawn_probe();
        wrapped
    } else {
        inner
    };
    let store: Arc<dyn AffinityStore> = if let Some(url) = &opts.affinity_redis_url {
        // Unique key prefix per rig so consecutive harness runs
        // don't trip over each other: each run uses fresh backend
        // ports, so stale bindings from a previous run would point
        // at dead addresses.
        let prefix = format!(
            "scg-perf-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        );
        let cfg = RedisStoreConfig {
            url: url.clone(),
            key_prefix: prefix,
            ..Default::default()
        };
        Arc::new(
            RedisStore::connect(cfg)
                .await
                .expect("connect to Redis affinity store"),
        )
    } else {
        Arc::new(MemoryStore::new())
    };
    let router = Arc::new(Router::single_pool(pool, store));
    let dialer = Dialer::new();
    let auth = AuthInterceptor::new(Arc::new(AnonymousAuthenticator));

    // Decide between the back-compat constructor (matches the
    // pre-Phase-3 perf baseline) and the full constructor. The
    // full path runs through `authenticate_and_resolve` + tenant
    // resolver + optional rate limit + optional audit, so each
    // toggle has an additive overhead that the `overhead` scenario
    // measures step by step.
    let proxy = if opts.tenant_resolver || opts.rate_limit || opts.audit {
        let tr = TenantResolver::new(TenantResolverConfig {
            source: TenantSource::AlwaysDefault,
            on_missing: OnMissing::UseDefault,
            default_name: "default".into(),
        });
        let limiter = if opts.rate_limit {
            // Generous quota — 1M rps, 1M burst — so the limiter
            // never actually rejects. We only want to measure the
            // bucket-check cost on the hot path.
            let limits = TenantLimits {
                tenant: BucketRate {
                    rpcs_per_second: 1_000_000.0,
                    burst: 1_000_000,
                },
                per_user: BucketRate::disabled(),
            };
            Some(RateLimiter::Memory(MemoryLimiter::new(
                limits,
                Default::default(),
                Arc::new(NoopLimiterObs),
            )))
        } else {
            None
        };
        let audit = if opts.audit {
            AuditLogger::new(AuditConfig {
                enabled: true,
                log_successful_rpcs: false,
            })
        } else {
            AuditLogger::disabled()
        };
        SparkConnectProxy::with_all(router, dialer, auth, metrics.clone(), tr, limiter, audit)
    } else {
        SparkConnectProxy::with_auth_and_metrics(router, dialer, auth, metrics.clone())
    };

    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap();
    let (gw_tx, gw_rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(pb::spark_connect_service_server::SparkConnectServiceServer::new(proxy))
            .serve_with_incoming_shutdown(TcpListenerStream::new(lis), async {
                let _ = gw_rx.await;
            })
            .await
            .ok();
    });

    let endpoint = Endpoint::from_shared(format!("http://{}", addr)).unwrap();
    let grpc = endpoint
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();

    Rig {
        grpc,
        metrics,
        readiness,
        _be_kills: be_kills,
        gw_kill: Some(gw_tx),
    }
}

// ---------- Latency reporter -----------------------------------------------

fn new_hist() -> Histogram<u64> {
    // 1µs..60s, 3 significant digits.
    Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap()
}

fn print_summary(label: &str, hist: &Histogram<u64>, rpcs: u64, errors: u64, duration: Duration) {
    let qps = rpcs as f64 / duration.as_secs_f64();
    println!("---");
    println!("[{}]", label);
    println!("  duration:       {:>10.2}s", duration.as_secs_f64());
    println!("  total RPCs:     {:>10}", rpcs);
    println!("  errors:         {:>10}", errors);
    println!("  QPS:            {:>10.0}", qps);
    println!(
        "  latency (ms)    p50={:.3}  p95={:.3}  p99={:.3}  p999={:.3}  max={:.3}",
        hist.value_at_quantile(0.50) as f64 / 1000.0,
        hist.value_at_quantile(0.95) as f64 / 1000.0,
        hist.value_at_quantile(0.99) as f64 / 1000.0,
        hist.value_at_quantile(0.999) as f64 / 1000.0,
        hist.max() as f64 / 1000.0,
    );
}

// ---------- Scenario 1: unary throughput -----------------------------------

async fn scenario_unary(workers: usize, duration: Duration, n_sessions: usize) {
    println!(
        "[scenario] unary throughput: workers={} duration={:?} sessions={}",
        workers, duration, n_sessions
    );
    let rig = spawn_rig(RigOpts::default()).await;
    let stop_at = Instant::now() + duration;
    let total = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let hist_handles: Vec<_> = (0..workers)
        .map(|wid| {
            let ch = rig.grpc.clone();
            let total = total.clone();
            let errors = errors.clone();
            tokio::spawn(async move {
                let mut h = new_hist();
                let mut client =
                    pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);
                let mut local = 0u64;
                while Instant::now() < stop_at {
                    let session_id = format!("session-{}", local % n_sessions as u64);
                    let started = Instant::now();
                    let res = client
                        .config(Request::new(pb::ConfigRequest {
                            session_id,
                            ..Default::default()
                        }))
                        .await;
                    let elapsed_us = started.elapsed().as_micros() as u64;
                    h.record(elapsed_us.max(1)).ok();
                    match res {
                        Ok(_) => {
                            total.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    local += 1;
                    let _ = wid; // silence unused; useful when debugging by worker
                }
                h
            })
        })
        .collect();
    let started = Instant::now();
    let mut combined = new_hist();
    for h in hist_handles {
        let part = h.await.unwrap();
        combined.add(part).ok();
    }
    let elapsed = started.elapsed();
    print_summary(
        "unary",
        &combined,
        total.load(Ordering::Relaxed),
        errors.load(Ordering::Relaxed),
        elapsed,
    );
    drop(rig);
}

// ---------- Scenario 2: streaming concurrency -------------------------------

async fn scenario_streaming(concurrency: usize, messages: usize, n_sessions: usize) {
    println!(
        "[scenario] streaming concurrency: concurrency={} msgs/stream={} sessions={}",
        concurrency, messages, n_sessions
    );
    let rig = spawn_rig(RigOpts {
        stream_messages: messages,
        ..RigOpts::default()
    })
    .await;
    let total = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let handles: Vec<_> = (0..concurrency)
        .map(|i| {
            let ch = rig.grpc.clone();
            let total = total.clone();
            let errors = errors.clone();
            tokio::spawn(async move {
                let mut h = new_hist();
                let mut client =
                    pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);
                let session_id = format!("session-{}", i % n_sessions);
                let started = Instant::now();
                let stream = client
                    .execute_plan(Request::new(pb::ExecutePlanRequest {
                        session_id,
                        operation_id: Some(format!("op-{}", i)),
                        ..Default::default()
                    }))
                    .await;
                match stream {
                    Ok(resp) => {
                        let mut s = resp.into_inner();
                        let mut count = 0u64;
                        while let Some(item) = s.next().await {
                            if item.is_err() {
                                errors.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            count += 1;
                        }
                        let elapsed_us = started.elapsed().as_micros() as u64;
                        h.record(elapsed_us.max(1)).ok();
                        total.fetch_add(count, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                h
            })
        })
        .collect();
    let mut combined = new_hist();
    let peak_active = rig.metrics.active_streams_value();
    println!(
        "  peak active_streams observed during fan-out: {}",
        peak_active
    );
    for h in handles {
        let part = h.await.unwrap();
        combined.add(part).ok();
    }
    let elapsed = started.elapsed();
    print_summary(
        "streaming (per-stream completion)",
        &combined,
        concurrency as u64,
        errors.load(Ordering::Relaxed),
        elapsed,
    );
    let total_msgs = total.load(Ordering::Relaxed);
    println!(
        "  messages forwarded: {} ({:.0} msg/s)",
        total_msgs,
        total_msgs as f64 / elapsed.as_secs_f64()
    );
    drop(rig);
}

// ---------- Scenario 3: health-check overhead ------------------------------

async fn scenario_hc_overhead(workers: usize, duration: Duration, n_sessions: usize) {
    println!(
        "[scenario] health-check overhead: workers={} duration={:?} (each)",
        workers, duration
    );
    println!("  baseline (no HC):");
    let baseline = run_unary_internal(workers, duration, n_sessions, RigOpts::default()).await;
    println!("  with HC:");
    let with_hc = run_unary_internal(
        workers,
        duration,
        n_sessions,
        RigOpts {
            health_check: true,
            ..RigOpts::default()
        },
    )
    .await;
    println!("---");
    println!("[hc-overhead summary]");
    println!(
        "  p50 (ms): baseline={:.3}  with_hc={:.3}  delta={:+.3}",
        baseline.0.value_at_quantile(0.50) as f64 / 1000.0,
        with_hc.0.value_at_quantile(0.50) as f64 / 1000.0,
        (with_hc.0.value_at_quantile(0.50) as i64 - baseline.0.value_at_quantile(0.50) as i64)
            as f64
            / 1000.0,
    );
    println!(
        "  p99 (ms): baseline={:.3}  with_hc={:.3}  delta={:+.3}",
        baseline.0.value_at_quantile(0.99) as f64 / 1000.0,
        with_hc.0.value_at_quantile(0.99) as f64 / 1000.0,
        (with_hc.0.value_at_quantile(0.99) as i64 - baseline.0.value_at_quantile(0.99) as i64)
            as f64
            / 1000.0,
    );
    let qps_baseline = baseline.1 as f64 / duration.as_secs_f64();
    let qps_with_hc = with_hc.1 as f64 / duration.as_secs_f64();
    println!(
        "  QPS: baseline={:.0}  with_hc={:.0}  delta={:+.1}%",
        qps_baseline,
        qps_with_hc,
        (qps_with_hc - qps_baseline) / qps_baseline * 100.0
    );
}

async fn run_unary_internal(
    workers: usize,
    duration: Duration,
    n_sessions: usize,
    opts: RigOpts,
) -> (Histogram<u64>, u64) {
    let rig = spawn_rig(opts).await;
    let stop_at = Instant::now() + duration;
    let total = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let hist_handles: Vec<_> = (0..workers)
        .map(|_wid| {
            let ch = rig.grpc.clone();
            let total = total.clone();
            let errors = errors.clone();
            tokio::spawn(async move {
                let mut h = new_hist();
                let mut client =
                    pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);
                let mut local = 0u64;
                while Instant::now() < stop_at {
                    let session_id = format!("session-{}", local % n_sessions as u64);
                    let started = Instant::now();
                    let res = client
                        .config(Request::new(pb::ConfigRequest {
                            session_id,
                            ..Default::default()
                        }))
                        .await;
                    let elapsed_us = started.elapsed().as_micros() as u64;
                    h.record(elapsed_us.max(1)).ok();
                    if res.is_err() {
                        errors.fetch_add(1, Ordering::Relaxed);
                    } else {
                        total.fetch_add(1, Ordering::Relaxed);
                    }
                    local += 1;
                }
                h
            })
        })
        .collect();
    let mut combined = new_hist();
    for h in hist_handles {
        combined.add(h.await.unwrap()).ok();
    }
    let total = total.load(Ordering::Relaxed);
    let errors = errors.load(Ordering::Relaxed);
    println!(
        "    rpcs={} errors={} qps={:.0} p50={:.3}ms p99={:.3}ms",
        total,
        errors,
        total as f64 / duration.as_secs_f64(),
        combined.value_at_quantile(0.50) as f64 / 1000.0,
        combined.value_at_quantile(0.99) as f64 / 1000.0,
    );
    drop(rig);
    (combined, total)
}

// ---------- Scenario 5: per-feature hot-path overhead -----------------------

/// Walk through five rig configurations and report per-RPC overhead
/// added by each multi-tenant feature stacked on top of the bare
/// baseline. The bucket and the audit logger are configured so that
/// they never reject and only emit on the binding path, respectively
/// — the goal is to measure the *check* / *emit* cost on every RPC,
/// not the cost of the work they protect against.
async fn scenario_overhead(workers: usize, duration: Duration, n_sessions: usize) {
    println!(
        "[scenario] per-feature overhead: workers={} duration={:?} (each)",
        workers, duration
    );

    let configs: &[(&str, RigOpts)] = &[
        ("baseline (with_auth_and_metrics)", RigOpts::default()),
        (
            "+ tenant_resolver",
            RigOpts {
                tenant_resolver: true,
                ..RigOpts::default()
            },
        ),
        (
            "+ tenant_resolver + rate_limit",
            RigOpts {
                tenant_resolver: true,
                rate_limit: true,
                ..RigOpts::default()
            },
        ),
        (
            "+ tenant_resolver + audit",
            RigOpts {
                tenant_resolver: true,
                audit: true,
                ..RigOpts::default()
            },
        ),
        (
            "+ all three",
            RigOpts {
                tenant_resolver: true,
                rate_limit: true,
                audit: true,
                ..RigOpts::default()
            },
        ),
    ];

    let mut results: Vec<(String, Histogram<u64>, u64)> = Vec::with_capacity(configs.len());
    for (label, opts) in configs {
        println!("  {}:", label);
        let (h, total) = run_unary_internal(workers, duration, n_sessions, opts.clone()).await;
        results.push((label.to_string(), h, total));
    }

    println!("---");
    println!("[overhead summary]");
    let (_, baseline_h, baseline_total) = &results[0];
    let baseline_p50 = baseline_h.value_at_quantile(0.50) as f64 / 1000.0;
    let baseline_p99 = baseline_h.value_at_quantile(0.99) as f64 / 1000.0;
    let baseline_qps = *baseline_total as f64 / duration.as_secs_f64();

    println!(
        "  {:38}  p50    p99    QPS      Δp50      Δp99      ΔQPS",
        "config"
    );
    for (label, h, total) in &results {
        let p50 = h.value_at_quantile(0.50) as f64 / 1000.0;
        let p99 = h.value_at_quantile(0.99) as f64 / 1000.0;
        let qps = *total as f64 / duration.as_secs_f64();
        println!(
            "  {:38} {:>5.3}  {:>5.3}  {:>6.0}   {:>+6.3}    {:>+6.3}    {:>+5.1}%",
            label,
            p50,
            p99,
            qps,
            p50 - baseline_p50,
            p99 - baseline_p99,
            (qps - baseline_qps) / baseline_qps * 100.0,
        );
    }
}

// ---------- Scenario 6: Redis affinity store round-trip ---------------------

/// Compare in-memory affinity store against a real Redis-backed one.
/// Requires a Redis reachable at `--redis-url` (default
/// redis://127.0.0.1:6379). Measures the extra round-trip cost of
/// every lookup/bind hitting Redis.
async fn scenario_redis_affinity(
    workers: usize,
    duration: Duration,
    n_sessions: usize,
    redis_url: String,
) {
    println!(
        "[scenario] redis affinity store: workers={} duration={:?} url={}",
        workers, duration, redis_url
    );

    println!("  baseline (memory affinity):");
    let baseline = run_unary_internal(workers, duration, n_sessions, RigOpts::default()).await;
    println!("  with Redis affinity:");
    let with_redis = run_unary_internal(
        workers,
        duration,
        n_sessions,
        RigOpts {
            affinity_redis_url: Some(redis_url),
            ..RigOpts::default()
        },
    )
    .await;

    println!("---");
    println!("[redis-affinity summary]");
    let p50_b = baseline.0.value_at_quantile(0.50) as f64 / 1000.0;
    let p99_b = baseline.0.value_at_quantile(0.99) as f64 / 1000.0;
    let p50_r = with_redis.0.value_at_quantile(0.50) as f64 / 1000.0;
    let p99_r = with_redis.0.value_at_quantile(0.99) as f64 / 1000.0;
    let qps_b = baseline.1 as f64 / duration.as_secs_f64();
    let qps_r = with_redis.1 as f64 / duration.as_secs_f64();
    println!(
        "  p50 (ms): memory={:.3}  redis={:.3}  delta={:+.3}",
        p50_b,
        p50_r,
        p50_r - p50_b
    );
    println!(
        "  p99 (ms): memory={:.3}  redis={:.3}  delta={:+.3}",
        p99_b,
        p99_r,
        p99_r - p99_b
    );
    println!(
        "  QPS: memory={:.0}  redis={:.0}  delta={:+.1}%",
        qps_b,
        qps_r,
        (qps_r - qps_b) / qps_b * 100.0
    );
}

// ---------- Scenario 4: drain under load -----------------------------------

async fn scenario_drain_under_load(
    concurrency: usize,
    messages: usize,
    delay_ms: u64,
    deadline_secs: u64,
) {
    println!(
        "[scenario] drain under load: concurrency={} msgs/stream={} per-msg-delay={}ms deadline={}s",
        concurrency, messages, delay_ms, deadline_secs
    );
    let mut rig = spawn_rig(RigOpts {
        stream_messages: messages,
        stream_delay_ms: delay_ms,
        ..RigOpts::default()
    })
    .await;

    let completed = Arc::new(AtomicU64::new(0));
    let cancelled = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let stream_handles: Vec<_> = (0..concurrency)
        .map(|i| {
            let ch = rig.grpc.clone();
            let completed = completed.clone();
            let cancelled = cancelled.clone();
            tokio::spawn(async move {
                let mut client =
                    pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);
                let session_id = format!("session-{}", i);
                let stream = client
                    .execute_plan(Request::new(pb::ExecutePlanRequest {
                        session_id,
                        operation_id: Some(format!("op-{}", i)),
                        ..Default::default()
                    }))
                    .await;
                let Ok(resp) = stream else {
                    cancelled.fetch_add(1, Ordering::Relaxed);
                    return;
                };
                let mut s = resp.into_inner();
                let mut got_error = false;
                while let Some(item) = s.next().await {
                    if item.is_err() {
                        got_error = true;
                        break;
                    }
                }
                if got_error {
                    cancelled.fetch_add(1, Ordering::Relaxed);
                } else {
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    // Wait until the gateway sees all the streams in flight.
    let target_active = concurrency as i64;
    let fanout_deadline = Instant::now() + Duration::from_secs(10);
    while rig.metrics.active_streams_value() < target_active && Instant::now() < fanout_deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let active_at_drain = rig.metrics.active_streams_value();
    println!(
        "  active_streams at drain trigger: {} (target {})",
        active_at_drain, target_active
    );

    // Trigger drain (mirrors gateway main).
    let drain_start = Instant::now();
    rig.readiness.mark_not_ready();
    let metrics = rig.metrics.clone();
    let drain_deadline = Duration::from_secs(deadline_secs);
    let drained = tokio::time::timeout(drain_deadline, async {
        loop {
            if metrics.active_streams_value() <= 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let drain_elapsed = drain_start.elapsed();
    let drain_outcome = if drained.is_ok() {
        "clean"
    } else {
        "deadline-hit"
    };
    let final_active = metrics.active_streams_value();

    // Now actually shut down the gateway so streams that were
    // cancelled by the deadline see their errors.
    if let Some(tx) = rig.gw_kill.take() {
        let _ = tx.send(());
    }
    for h in stream_handles {
        let _ = h.await;
    }
    let total_elapsed = started.elapsed();

    println!("---");
    println!("[drain-under-load summary]");
    println!("  concurrency:        {}", concurrency);
    println!("  drain outcome:      {}", drain_outcome);
    println!("  drain elapsed:      {:?}", drain_elapsed);
    println!("  active@deadline:    {}", final_active);
    println!(
        "  streams completed:  {}",
        completed.load(Ordering::Relaxed)
    );
    println!(
        "  streams cancelled:  {}",
        cancelled.load(Ordering::Relaxed)
    );
    println!("  test wall-clock:    {:?}", total_elapsed);
}

// ---------- CLI -------------------------------------------------------------

fn parse_arg<T: std::str::FromStr>(args: &[String], flag: &str, default: T) -> T {
    let mut i = 0;
    while i < args.len() {
        if args[i] == flag {
            if let Some(v) = args.get(i + 1) {
                if let Ok(parsed) = v.parse() {
                    return parsed;
                }
            }
        }
        i += 1;
    }
    default
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let argv: Vec<String> = std::env::args().collect();
    let scenario = argv.get(1).map(String::as_str).unwrap_or("unary");

    let workers = parse_arg(&argv, "--workers", 32usize);
    let duration_secs = parse_arg(&argv, "--duration-secs", 20u64);
    let concurrency = parse_arg(&argv, "--concurrency", 64usize);
    let messages = parse_arg(&argv, "--messages", 100usize);
    let delay_ms = parse_arg(&argv, "--delay-ms", 0u64);
    let deadline_secs = parse_arg(&argv, "--deadline-secs", 10u64);
    let n_sessions = parse_arg(&argv, "--sessions", 64usize);

    let redis_url = parse_arg::<String>(&argv, "--redis-url", "redis://127.0.0.1:6379".to_string());

    match scenario {
        "unary" => {
            scenario_unary(workers, Duration::from_secs(duration_secs), n_sessions).await;
        }
        "streaming" => {
            scenario_streaming(concurrency, messages, n_sessions).await;
        }
        "hc-overhead" => {
            scenario_hc_overhead(workers, Duration::from_secs(duration_secs), n_sessions).await;
        }
        "drain-under-load" => {
            scenario_drain_under_load(concurrency, messages, delay_ms, deadline_secs).await;
        }
        "overhead" => {
            scenario_overhead(workers, Duration::from_secs(duration_secs), n_sessions).await;
        }
        "redis-affinity" => {
            scenario_redis_affinity(
                workers,
                Duration::from_secs(duration_secs),
                n_sessions,
                redis_url,
            )
            .await;
        }
        other => {
            eprintln!(
                "unknown scenario: {} — try one of: unary streaming hc-overhead drain-under-load overhead redis-affinity",
                other
            );
            std::process::exit(2);
        }
    }
}
