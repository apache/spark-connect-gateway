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

//! Integration test for per-tenant rate limiting.
//!
//! Drives RPCs through a real gRPC server with the rate limiter
//! enabled and asserts:
//!
//! * burst RPCs go through; the next one gets ResourceExhausted
//! * tenant A exhausting its quota doesn't block tenant B
//! * per-tenant overrides take precedence over the default
//! * the metric `scg_rate_limit_rejected_total{tenant, scope}`
//!   increments on rejection

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use scg_auth::{AnonymousAuthenticator, AuthInterceptor};
use scg_genproto::pb;
use scg_observability::Metrics;
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_ratelimit::{BucketRate, LimiterObserver, RateLimiter, RejectScope, TenantLimits};
use scg_routing::{AffinityStore, Pool, Router, TenantRouter};
use scg_store_memory::MemoryStore;
use scg_tenant::{OnMissing, TenantResolver, TenantResolverConfig, TenantSource};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

/// Minimal backend — every Config call succeeds. Tests below count
/// successful vs ResourceExhausted RPCs at the client side, so the
/// backend just needs to not crash.
#[derive(Default)]
struct OkBackend;

#[tonic::async_trait]
impl pb::spark_connect_service_server::SparkConnectService for OkBackend {
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
    async fn execute_plan(
        &self,
        _: Request<pb::ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
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

async fn spawn_rig(
    limiter: Option<RateLimiter>,
) -> (
    Channel,
    Metrics,
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let be_lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let be_addr = be_lis.local_addr().unwrap().to_string();
    let (be_tx, be_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(
                pb::spark_connect_service_server::SparkConnectServiceServer::new(OkBackend),
            )
            .serve_with_incoming_shutdown(TcpListenerStream::new(be_lis), async {
                let _ = be_rx.await;
            })
            .await
            .ok();
    });

    let metrics = Metrics::new().unwrap();
    let pool: Arc<dyn Pool> = Arc::new(StaticPool::new(vec![be_addr]).unwrap());
    let store: Arc<dyn AffinityStore> = Arc::new(MemoryStore::new());
    let tr = TenantRouter::single(pool);
    let router = Arc::new(Router::new(tr, store));
    let dialer = Dialer::new();
    let resolver = TenantResolver::new(TenantResolverConfig {
        source: TenantSource::FromMetadata {
            header: "x-tenant".into(),
        },
        on_missing: OnMissing::UseDefault,
        default_name: "default".into(),
    });
    let proxy = SparkConnectProxy::with_all(
        router,
        dialer,
        AuthInterceptor::new(Arc::new(AnonymousAuthenticator)),
        metrics.clone(),
        resolver,
        limiter,
        scg_audit::AuditLogger::disabled(),
    );

    let gw_lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gw_addr = gw_lis.local_addr().unwrap();
    let (gw_tx, gw_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(pb::spark_connect_service_server::SparkConnectServiceServer::new(proxy))
            .serve_with_incoming_shutdown(TcpListenerStream::new(gw_lis), async {
                let _ = gw_rx.await;
            })
            .await
            .ok();
    });

    let endpoint = Endpoint::from_shared(format!("http://{}", gw_addr)).unwrap();
    let ch = endpoint
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();
    (ch, metrics, be_tx, gw_tx)
}

fn metrics_observer(metrics: &Metrics) -> Arc<dyn LimiterObserver> {
    struct Obs(Metrics);
    impl LimiterObserver for Obs {
        fn on_reject(&self, tenant: &str, scope: RejectScope) {
            self.0.record_rate_limit_reject(tenant, scope.as_str());
        }
    }
    Arc::new(Obs(metrics.clone()))
}

async fn config_with_tenant(ch: &Channel, session: &str, tenant: &str) -> Result<(), Status> {
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch.clone());
    let mut req = Request::new(pb::ConfigRequest {
        session_id: session.into(),
        ..Default::default()
    });
    req.metadata_mut()
        .insert("x-tenant", MetadataValue::try_from(tenant).unwrap());
    c.config(req).await.map(|_| ())
}

fn rejected_count(metrics: &Metrics, tenant: &str, scope: &str) -> u64 {
    let mfs = metrics.registry().gather();
    for mf in mfs {
        if mf.name() != "scg_rate_limit_rejected_total" {
            continue;
        }
        for m in mf.get_metric() {
            let labels = m.get_label();
            let t = labels
                .iter()
                .find(|l| l.name() == "tenant")
                .map(|l| l.value());
            let s = labels
                .iter()
                .find(|l| l.name() == "scope")
                .map(|l| l.value());
            if t == Some(tenant) && s == Some(scope) {
                return m.get_counter().value() as u64;
            }
        }
    }
    0
}

#[tokio::test]
async fn burst_admits_then_rejects_at_tenant_scope() {
    // Need a non-trivial Metrics to act as the observer's sink.
    let observer_metrics = Metrics::new().unwrap();
    let mut overrides = HashMap::new();
    // 1 RPS / burst 3 → first 3 succeed, 4th gets ResourceExhausted.
    overrides.insert(
        "team-a".to_string(),
        TenantLimits {
            tenant: BucketRate {
                rpcs_per_second: 1.0,
                burst: 3,
            },
            per_user: BucketRate::disabled(),
        },
    );
    let limiter = RateLimiter::new(
        TenantLimits::default(),
        overrides,
        metrics_observer(&observer_metrics),
    );

    let (ch, _metrics, _be, _gw) = spawn_rig(Some(limiter)).await;

    for i in 0..3 {
        config_with_tenant(&ch, &format!("s-{}", i), "team-a")
            .await
            .unwrap_or_else(|e| panic!("RPC {} should succeed, got {:?}", i, e));
    }
    let err = config_with_tenant(&ch, "s-4", "team-a").await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);

    // Metric incremented at tenant scope.
    assert!(rejected_count(&observer_metrics, "team-a", "tenant") >= 1);
}

#[tokio::test]
async fn one_tenant_exhaustion_does_not_block_others() {
    let observer_metrics = Metrics::new().unwrap();
    let mut overrides = HashMap::new();
    let one_token = TenantLimits {
        tenant: BucketRate {
            rpcs_per_second: 0.1, // refill barely matters within the test
            burst: 1,
        },
        per_user: BucketRate::disabled(),
    };
    overrides.insert("team-a".to_string(), one_token);
    overrides.insert("team-b".to_string(), one_token);
    let limiter = RateLimiter::new(
        TenantLimits::default(),
        overrides,
        metrics_observer(&observer_metrics),
    );

    let (ch, _metrics, _be, _gw) = spawn_rig(Some(limiter)).await;

    // Exhaust team-a.
    config_with_tenant(&ch, "s-a-1", "team-a").await.unwrap();
    let err = config_with_tenant(&ch, "s-a-2", "team-a")
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);

    // team-b is unaffected.
    config_with_tenant(&ch, "s-b-1", "team-b").await.unwrap();
}

#[tokio::test]
async fn default_bucket_applies_to_unlisted_tenants() {
    let observer_metrics = Metrics::new().unwrap();
    // No explicit overrides; default = 0.1 RPS, burst 2.
    let limiter = RateLimiter::new(
        TenantLimits {
            tenant: BucketRate {
                rpcs_per_second: 0.1,
                burst: 2,
            },
            per_user: BucketRate::disabled(),
        },
        HashMap::new(),
        metrics_observer(&observer_metrics),
    );

    let (ch, _metrics, _be, _gw) = spawn_rig(Some(limiter)).await;

    // A random unlisted tenant still gets the default bucket.
    config_with_tenant(&ch, "s1", "random-tenant")
        .await
        .unwrap();
    config_with_tenant(&ch, "s2", "random-tenant")
        .await
        .unwrap();
    let err = config_with_tenant(&ch, "s3", "random-tenant")
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
}

#[tokio::test]
async fn no_limiter_admits_everything() {
    let (ch, _metrics, _be, _gw) = spawn_rig(None).await;
    for i in 0..50 {
        config_with_tenant(&ch, &format!("s-{}", i), "any-tenant")
            .await
            .unwrap();
    }
}
