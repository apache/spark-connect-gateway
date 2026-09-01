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

//! Multi-tenant end-to-end integration test.
//!
//! One gateway, two tenants (`team-a`, `team-b`), three backends
//! (`be-a`, `be-b`, `be-default`), and the full multi-tenant stack wired
//! together: static-token auth (with per-token tenant claim) →
//! tenant resolver (`FromClaim`) → tenant-routed pools → per-tenant
//! rate limiter → session affinity → audit log.
//!
//! The point of this file is *cross-feature* isolation. Each
//! individual feature already has its own integration test
//! (`multitenant_pool.rs`, `ratelimit_integration.rs`,
//! `audit_integration.rs`, `auth_integration.rs`); 3.9 verifies the
//! features compose correctly when they're all on at once.
//!
//! Four axes of isolation are asserted:
//!
//! 1. **Pool + affinity isolation** — Same `session_id` from two
//!    tenants binds to two *different* backends, and the bindings
//!    don't leak across tenant boundaries even on repeat calls.
//! 2. **Rate-limit isolation** — `team-a`'s tight quota does not
//!    affect `team-b`; rejections increment the metric only on the
//!    tenant whose bucket exhausted.
//! 3. **Audit tenant labeling** — Every audit event carries the
//!    correct tenant label, taken from the resolved tenant (not the
//!    client-supplied `UserContext.user_id` or anything else
//!    forgeable by the caller).
//! 4. **Auth-level tenant binding** — A token configured for
//!    `team-a` is rejected when its bearer is the only thing
//!    distinguishing the request from `team-b`. Combined with
//!    `OnMissing::Reject`, the gateway returns PermissionDenied or
//!    Unauthenticated before the request ever reaches a backend.

mod common;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use common::{AuditCapture, CapturedEvent};
use futures::Stream;
use scg_audit::{AuditConfig, AuditLogger};
use scg_auth::token::{StaticTokenAuthenticator, TokenEntry};
use scg_auth::AuthInterceptor;
use scg_genproto::pb;
use scg_observability::Metrics;
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_ratelimit::{BucketRate, LimiterObserver, RateLimiter, RejectScope, TenantLimits};
use scg_routing::{AffinityStore, PoolEntry, Router, TenantRouter, UnknownTenantPolicy};
use scg_store_memory::MemoryStore;
use scg_tenant::{OnMissing, TenantResolver, TenantResolverConfig, TenantSource};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

/// Backend that tags its `ConfigResponse.session_id` with its own id
/// so the test driver can identify which backend handled an RPC.
#[derive(Clone)]
struct TaggedBackend {
    id: String,
}

impl TaggedBackend {
    fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

#[tonic::async_trait]
impl pb::spark_connect_service_server::SparkConnectService for TaggedBackend {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = Result<pb::ExecutePlanResponse, Status>> + Send + 'static>>;
    type ReattachExecuteStream = Self::ExecutePlanStream;

    async fn config(
        &self,
        req: Request<pb::ConfigRequest>,
    ) -> Result<Response<pb::ConfigResponse>, Status> {
        let body = req.into_inner();
        Ok(Response::new(pb::ConfigResponse {
            session_id: format!("{}@{}", body.session_id, self.id),
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

async fn spawn_backend(id: &'static str) -> (String, tokio::sync::oneshot::Sender<()>) {
    let svc =
        pb::spark_connect_service_server::SparkConnectServiceServer::new(TaggedBackend::new(id));
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(TcpListenerStream::new(lis), async {
                let _ = rx.await;
            })
            .await
            .ok();
    });
    (addr, tx)
}

/// Metrics-backed rate-limit observer, mirroring how `gateway/main.rs`
/// wires the limiter to Prometheus counters in production.
fn metrics_observer(metrics: &Metrics) -> Arc<dyn LimiterObserver> {
    struct Obs(Metrics);
    impl LimiterObserver for Obs {
        fn on_reject(&self, tenant: &str, scope: RejectScope) {
            self.0.record_rate_limit_reject(tenant, scope.as_str());
        }
    }
    Arc::new(Obs(metrics.clone()))
}

fn rejected_count(metrics: &Metrics, tenant: &str, scope: &str) -> u64 {
    for mf in metrics.registry().gather() {
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

struct Rig {
    channel: Channel,
    metrics: Metrics,
    _backends: Vec<tokio::sync::oneshot::Sender<()>>,
    _gw_shutdown: tokio::sync::oneshot::Sender<()>,
}

/// Build the multi-tenant rig used by all tests in this file.
///
/// `unknown_policy` lets the caller swap between `UseDefault`
/// (permissive single-tenant fall-through) and `Reject` (strict
/// SaaS-style isolation). The auth-level test wants the strict
/// policy.
async fn spawn_rig(unknown_policy: UnknownTenantPolicy) -> Rig {
    let (be_a, ka) = spawn_backend("be-a").await;
    let (be_b, kb) = spawn_backend("be-b").await;
    let (be_default, kd) = spawn_backend("be-default").await;

    let mut tenants: HashMap<String, PoolEntry> = HashMap::new();
    tenants.insert(
        "team-a".into(),
        PoolEntry::round_robin(Arc::new(StaticPool::new(vec![be_a]).unwrap())),
    );
    tenants.insert(
        "team-b".into(),
        PoolEntry::round_robin(Arc::new(StaticPool::new(vec![be_b]).unwrap())),
    );
    let default_pool = PoolEntry::round_robin(Arc::new(StaticPool::new(vec![be_default]).unwrap()));
    let tr = TenantRouter::new(tenants, Some(default_pool), unknown_policy);

    let metrics = Metrics::new().unwrap();
    let store: Arc<dyn AffinityStore> = Arc::new(MemoryStore::new());
    let router = Arc::new(Router::new(tr, store));
    let dialer = Dialer::new();

    // Static-token auth with one token per tenant. The token's
    // `tenant` claim is the only thing distinguishing a request — the
    // resolver below trusts it via `FromClaim`, so a stolen token
    // gives access to its tenant and nothing else.
    let token_auth = StaticTokenAuthenticator::new(vec![
        TokenEntry {
            token: "tok-a".into(),
            user_id: "alice".into(),
            tenant: Some("team-a".into()),
            groups: vec![],
        },
        TokenEntry {
            token: "tok-b".into(),
            user_id: "bob".into(),
            tenant: Some("team-b".into()),
            groups: vec![],
        },
    ])
    .unwrap();
    let auth = AuthInterceptor::new(Arc::new(token_auth));

    let resolver = TenantResolver::new(TenantResolverConfig {
        source: TenantSource::FromClaim,
        on_missing: match unknown_policy {
            UnknownTenantPolicy::UseDefault => OnMissing::UseDefault,
            UnknownTenantPolicy::Reject => OnMissing::Reject,
        },
        default_name: "default".into(),
    });

    // Tight quota on team-a so the rate-limit assertion can drive it
    // to ResourceExhausted quickly without waiting for token refill.
    // team-b uses the default (generous) quota.
    let mut overrides = HashMap::new();
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
        metrics_observer(&metrics),
    );

    let audit = AuditLogger::new(AuditConfig {
        enabled: true,
        log_successful_rpcs: false,
    });

    let proxy = SparkConnectProxy::with_all(
        router,
        dialer,
        auth,
        metrics.clone(),
        resolver,
        Some(limiter),
        audit,
    );

    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap();
    let (gw_tx, gw_rx) = tokio::sync::oneshot::channel();
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
    let channel = endpoint
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();
    Rig {
        channel,
        metrics,
        _backends: vec![ka, kb, kd],
        _gw_shutdown: gw_tx,
    }
}

/// Issue a Config call using the given bearer token and return the
/// backend id that handled it (extracted from `session_id@backend`).
async fn config_as(ch: &Channel, session: &str, token: &str) -> Result<String, Status> {
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch.clone());
    let mut req = Request::new(pb::ConfigRequest {
        session_id: session.into(),
        ..Default::default()
    });
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {}", token)).unwrap(),
    );
    let resp = c.config(req).await?;
    Ok(resp
        .into_inner()
        .session_id
        .rsplit('@')
        .next()
        .unwrap()
        .to_string())
}

/// Issue a Config call with no Authorization header — for the
/// unauthenticated case.
async fn config_no_auth(ch: &Channel, session: &str) -> Result<(), Status> {
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch.clone());
    let req = Request::new(pb::ConfigRequest {
        session_id: session.into(),
        ..Default::default()
    });
    c.config(req).await.map(|_| ())
}

fn audit_events_for_tenant<'a>(
    events: &'a [CapturedEvent],
    tenant: &str,
) -> Vec<&'a CapturedEvent> {
    events
        .iter()
        .filter(|e| e.fields.get("tenant").map(|s| s.as_str()) == Some(tenant))
        .collect()
}

// -------------------------------------------------------------------
// Axis 1: pool routing + session-affinity isolation
// -------------------------------------------------------------------

#[tokio::test]
async fn same_session_id_across_tenants_lands_on_different_backends() {
    // Hold the capture lease for the test's lifetime so audit events
    // emitted by this test's rig don't pollute another test's
    // snapshot. The lease serializes tests within this file even
    // though we don't read events here.
    let _cap = AuditCapture::lease();
    let rig = spawn_rig(UnknownTenantPolicy::UseDefault).await;

    // Both clients pick the *same* session_id; the only thing
    // distinguishing them is the bearer token (→ tenant claim).
    let be_for_a = config_as(&rig.channel, "shared-sid", "tok-a")
        .await
        .unwrap();
    let be_for_b = config_as(&rig.channel, "shared-sid", "tok-b")
        .await
        .unwrap();
    assert_eq!(be_for_a, "be-a");
    assert_eq!(be_for_b, "be-b");

    // Repeat both — affinity must hold inside each tenant, and must
    // not bleed across tenants.
    let again_a = config_as(&rig.channel, "shared-sid", "tok-a")
        .await
        .unwrap();
    let again_b = config_as(&rig.channel, "shared-sid", "tok-b")
        .await
        .unwrap();
    assert_eq!(again_a, "be-a", "team-a affinity drifted to {}", again_a);
    assert_eq!(again_b, "be-b", "team-b affinity drifted to {}", again_b);
}

// -------------------------------------------------------------------
// Axis 2: per-tenant rate-limit isolation
// -------------------------------------------------------------------

#[tokio::test]
async fn one_tenant_exhausting_quota_does_not_affect_the_other() {
    let _cap = AuditCapture::lease();
    let rig = spawn_rig(UnknownTenantPolicy::UseDefault).await;

    // team-a is configured with burst=3. The first 3 succeed; the
    // 4th must be ResourceExhausted.
    for i in 0..3 {
        config_as(&rig.channel, &format!("s-{}", i), "tok-a")
            .await
            .expect("team-a within burst");
    }
    let err = config_as(&rig.channel, "s-overflow", "tok-a")
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);

    // Metric increments on team-a, scope=tenant. team-b has zero.
    assert_eq!(rejected_count(&rig.metrics, "team-a", "tenant"), 1);
    assert_eq!(rejected_count(&rig.metrics, "team-b", "tenant"), 0);

    // team-b uses the (generous) default quota; with team-a's bucket
    // wedged, team-b should still sail through.
    for i in 0..5 {
        config_as(&rig.channel, &format!("s-b-{}", i), "tok-b")
            .await
            .expect("team-b unaffected by team-a's exhaustion");
    }
    assert_eq!(rejected_count(&rig.metrics, "team-b", "tenant"), 0);
}

// -------------------------------------------------------------------
// Axis 3: audit events carry the resolved tenant
// -------------------------------------------------------------------

#[tokio::test]
async fn audit_events_label_the_correct_tenant() {
    let cap = AuditCapture::lease();
    let rig = spawn_rig(UnknownTenantPolicy::UseDefault).await;

    config_as(&rig.channel, "sess-a", "tok-a").await.unwrap();
    config_as(&rig.channel, "sess-b", "tok-b").await.unwrap();

    let events = cap.snapshot();

    // Each tenant got exactly one session.create labeled with itself.
    let a_events: Vec<_> = audit_events_for_tenant(&events, "team-a")
        .into_iter()
        .filter(|e| e.fields.get("event").map(|s| s.as_str()) == Some("session.create"))
        .collect();
    let b_events: Vec<_> = audit_events_for_tenant(&events, "team-b")
        .into_iter()
        .filter(|e| e.fields.get("event").map(|s| s.as_str()) == Some("session.create"))
        .collect();
    assert_eq!(a_events.len(), 1, "team-a session.create count");
    assert_eq!(b_events.len(), 1, "team-b session.create count");

    // Cross-check the user_id on each event was the verified
    // identity, not anything else — the test relies on this when
    // claiming the resolver pulled `tenant` from the same token.
    assert_eq!(
        a_events[0].fields.get("user_id").map(|s| s.as_str()),
        Some("alice"),
    );
    assert_eq!(
        b_events[0].fields.get("user_id").map(|s| s.as_str()),
        Some("bob"),
    );

    // session_id labels match what each client supplied — the only
    // way they would collide here is a routing-key bug.
    assert_eq!(
        a_events[0].fields.get("session_id").map(|s| s.as_str()),
        Some("sess-a"),
    );
    assert_eq!(
        b_events[0].fields.get("session_id").map(|s| s.as_str()),
        Some("sess-b"),
    );
}

// -------------------------------------------------------------------
// Axis 4: auth-level tenant binding
// -------------------------------------------------------------------

#[tokio::test]
async fn unauthenticated_request_is_rejected_before_routing() {
    let _cap = AuditCapture::lease();
    let rig = spawn_rig(UnknownTenantPolicy::UseDefault).await;
    let err = config_no_auth(&rig.channel, "anything").await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn unknown_tenant_token_rejected_under_reject_policy() {
    let _cap = AuditCapture::lease();
    // Add a third token whose tenant claim is missing — the
    // resolver's Reject policy must turn that into a hard rejection
    // before the request reaches a backend.
    //
    // We can't extend the rig's auth list without rebuilding, so
    // construct a one-off authenticator inline. Everything else
    // (pool topology, resolver, limiter) mirrors `spawn_rig`.
    let (be_a, _ka) = spawn_backend("be-a").await;
    let (be_b, _kb) = spawn_backend("be-b").await;

    let mut tenants: HashMap<String, PoolEntry> = HashMap::new();
    tenants.insert(
        "team-a".into(),
        PoolEntry::round_robin(Arc::new(StaticPool::new(vec![be_a]).unwrap())),
    );
    tenants.insert(
        "team-b".into(),
        PoolEntry::round_robin(Arc::new(StaticPool::new(vec![be_b]).unwrap())),
    );
    // No default pool + Reject policy + tenantless token = the gateway
    // must surface Unauthenticated (from the resolver) and never
    // attempt to forward.
    let tr = TenantRouter::new(tenants, None, UnknownTenantPolicy::Reject);

    let metrics = Metrics::new().unwrap();
    let store: Arc<dyn AffinityStore> = Arc::new(MemoryStore::new());
    let router = Arc::new(Router::new(tr, store));
    let dialer = Dialer::new();

    let token_auth = StaticTokenAuthenticator::new(vec![TokenEntry {
        token: "rogue".into(),
        user_id: "mallory".into(),
        tenant: None, // <-- no tenant claim
        groups: vec![],
    }])
    .unwrap();
    let auth = AuthInterceptor::new(Arc::new(token_auth));
    let resolver = TenantResolver::new(TenantResolverConfig {
        source: TenantSource::FromClaim,
        on_missing: OnMissing::Reject,
        default_name: "default".into(),
    });
    let proxy = SparkConnectProxy::with_components(router, dialer, auth, metrics, resolver);

    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap();
    let (_gw_tx, gw_rx) = tokio::sync::oneshot::channel::<()>();
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
    let ch = endpoint
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();

    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);
    let mut req = Request::new(pb::ConfigRequest {
        session_id: "doesnt-matter".into(),
        ..Default::default()
    });
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from("Bearer rogue").unwrap(),
    );
    let err = c.config(req).await.unwrap_err();
    // Missing-tenant + on_missing=Reject → Unauthenticated from the
    // resolver. PermissionDenied is also accepted in case a future
    // policy refinement reclassifies it; either proves "the request
    // was rejected before it reached a backend".
    assert!(
        matches!(
            err.code(),
            tonic::Code::Unauthenticated | tonic::Code::PermissionDenied,
        ),
        "expected hard rejection, got {:?}",
        err.code(),
    );
}
