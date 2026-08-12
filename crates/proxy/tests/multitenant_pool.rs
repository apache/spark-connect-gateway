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

//! Integration test for per-tenant pool routing.
//!
//! Spawns three fake backends (team-a-be, team-b-be, default-be),
//! wires them into a single gateway with a per-tenant pool map, and
//! drives RPCs through each tenant. Asserts:
//!
//! * `tenant=team-a` lands on `team-a-be` even though `team-b-be`
//!   and `default-be` are reachable.
//! * `tenant=team-b` lands on `team-b-be`.
//! * `tenant=unknown` falls back to `default-be` under
//!   `UseDefault` policy.
//! * Under `Reject` policy, the same `unknown` tenant gets
//!   `PermissionDenied` and the backends never see the call.

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
use scg_routing::{AffinityStore, PoolEntry, Router, TenantRouter, UnknownTenantPolicy};
use scg_store_memory::MemoryStore;
use scg_tenant::{OnMissing, TenantResolver, TenantResolverConfig, TenantSource};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

/// Backend that tags every response with its own id so the driver
/// can verify which backend handled a given RPC.
#[derive(Clone, Default)]
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

async fn spawn_gateway(tenant_router: TenantRouter) -> (Channel, tokio::sync::oneshot::Sender<()>) {
    let metrics = Metrics::new().unwrap();
    let store: Arc<dyn AffinityStore> = Arc::new(MemoryStore::new());
    let router = Arc::new(Router::new(tenant_router, store));
    let dialer = Dialer::new();
    // Use the metadata-based tenant resolver so the test client can
    // declare which tenant to route through without standing up real
    // JWT plumbing.
    let resolver = TenantResolver::new(TenantResolverConfig {
        source: TenantSource::FromMetadata {
            header: "x-tenant".into(),
        },
        on_missing: OnMissing::UseDefault,
        default_name: "default".into(),
    });
    let proxy = SparkConnectProxy::with_components(
        router,
        dialer,
        AuthInterceptor::new(Arc::new(AnonymousAuthenticator)),
        metrics,
        resolver,
    );

    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(pb::spark_connect_service_server::SparkConnectServiceServer::new(proxy))
            .serve_with_incoming_shutdown(TcpListenerStream::new(lis), async {
                let _ = rx.await;
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
    (ch, tx)
}

/// Issue a Config call with a chosen tenant and return the backend
/// id that processed it (extracted from `session_id@backend_id`).
async fn config_with_tenant(ch: &Channel, session: &str, tenant: &str) -> String {
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch.clone());
    let mut req = Request::new(pb::ConfigRequest {
        session_id: session.into(),
        ..Default::default()
    });
    req.metadata_mut()
        .insert("x-tenant", MetadataValue::try_from(tenant).unwrap());
    let resp = c.config(req).await.expect("Config RPC succeeds");
    resp.into_inner()
        .session_id
        .rsplit('@')
        .next()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn per_tenant_pools_route_to_their_own_backends() {
    let (be_a, _ka) = spawn_backend("be-a").await;
    let (be_b, _kb) = spawn_backend("be-b").await;
    let (be_default, _kd) = spawn_backend("be-default").await;

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
    let tr = TenantRouter::new(tenants, Some(default_pool), UnknownTenantPolicy::UseDefault);

    let (ch, _gw) = spawn_gateway(tr).await;

    // Same session_id, different tenants → different backends.
    let a = config_with_tenant(&ch, "s1", "team-a").await;
    let b = config_with_tenant(&ch, "s1", "team-b").await;
    assert_eq!(a, "be-a");
    assert_eq!(b, "be-b");

    // Unknown tenant under UseDefault → default backend.
    let d = config_with_tenant(&ch, "s1", "stranger-tenant").await;
    assert_eq!(d, "be-default");
}

#[tokio::test]
async fn reject_policy_returns_permission_denied_for_unknown_tenant() {
    let (be_a, _ka) = spawn_backend("be-a").await;

    let mut tenants: HashMap<String, PoolEntry> = HashMap::new();
    tenants.insert(
        "team-a".into(),
        PoolEntry::round_robin(Arc::new(StaticPool::new(vec![be_a]).unwrap())),
    );
    // Reject policy, no default pool. An unknown tenant must fail
    // loudly with PermissionDenied.
    let tr = TenantRouter::new(tenants, None, UnknownTenantPolicy::Reject);

    let (ch, _gw) = spawn_gateway(tr).await;
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);
    let mut req = Request::new(pb::ConfigRequest {
        session_id: "s1".into(),
        ..Default::default()
    });
    req.metadata_mut()
        .insert("x-tenant", MetadataValue::try_from("stranger").unwrap());
    let err = c.config(req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn back_compat_single_pool_via_default_only() {
    // Empty `overrides` map: every tenant routes through the
    // configured default. This is the single-tenant baseline.
    let (be_default, _kd) = spawn_backend("be-default").await;
    let default_pool = PoolEntry::round_robin(Arc::new(StaticPool::new(vec![be_default]).unwrap()));
    let tr = TenantRouter::new(
        HashMap::new(),
        Some(default_pool),
        UnknownTenantPolicy::UseDefault,
    );

    let (ch, _gw) = spawn_gateway(tr).await;
    let a = config_with_tenant(&ch, "s1", "team-a").await;
    let b = config_with_tenant(&ch, "s1", "team-b").await;
    let d = config_with_tenant(&ch, "s1", "default").await;
    assert_eq!(a, "be-default");
    assert_eq!(b, "be-default");
    assert_eq!(d, "be-default");
}
