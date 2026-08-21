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

//! Integration tests for tenant resolution: verifies that
//! the routing key seen by the affinity store carries the tenant
//! the resolver produced, that resolver=Reject failure surfaces as
//! Unauthenticated at the client, and that two tenants with the
//! same session_id stay isolated.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use parking_lot::Mutex;
use scg_auth::{
    token::{StaticTokenAuthenticator, TokenEntry},
    AnonymousAuthenticator, AuthInterceptor,
};
use scg_genproto::pb;
use scg_observability::Metrics;
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_routing::{AffinityStore, Pool, Router, SessionKey};
use scg_store_memory::MemoryStore;
use scg_tenant::{OnMissing, TenantResolver, TenantResolverConfig, TenantSource};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

/// AffinityStore wrapper that records every `bind_session_if_absent`
/// key the proxy hands it. Tests inspect the records to confirm the
/// routing key carries the expected tenant.
struct RecordingStore {
    inner: MemoryStore,
    binds: Arc<Mutex<Vec<SessionKey>>>,
}

impl RecordingStore {
    fn new() -> (Self, Arc<Mutex<Vec<SessionKey>>>) {
        let binds = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                inner: MemoryStore::new(),
                binds: binds.clone(),
            },
            binds,
        )
    }
}

#[async_trait::async_trait]
impl AffinityStore for RecordingStore {
    async fn lookup_session(&self, key: &SessionKey) -> Option<String> {
        self.inner.lookup_session(key).await
    }
    async fn bind_session_if_absent(&self, key: SessionKey, backend: String) -> String {
        self.binds.lock().push(key.clone());
        self.inner.bind_session_if_absent(key, backend).await
    }
    async fn forget_session(&self, key: &SessionKey) {
        self.inner.forget_session(key).await
    }
    async fn lookup_op(&self, op_id: &str) -> Option<String> {
        self.inner.lookup_op(op_id).await
    }
    async fn bind_op(&self, op_id: String, backend: String) {
        self.inner.bind_op(op_id, backend).await
    }
    async fn forget_op(&self, op_id: &str) {
        self.inner.forget_op(op_id).await
    }
}

#[derive(Default)]
struct EchoBackend;

#[tonic::async_trait]
impl pb::spark_connect_service_server::SparkConnectService for EchoBackend {
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

    // Unused but trait-required.
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

async fn rig(
    auth: AuthInterceptor,
    resolver: TenantResolver,
) -> (
    Channel,
    Arc<Mutex<Vec<SessionKey>>>,
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    // Backend
    let be_lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let be_addr = be_lis.local_addr().unwrap().to_string();
    let (be_tx, be_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(
                pb::spark_connect_service_server::SparkConnectServiceServer::new(EchoBackend),
            )
            .serve_with_incoming_shutdown(TcpListenerStream::new(be_lis), async {
                let _ = be_rx.await;
            })
            .await
            .ok();
    });

    let metrics = Metrics::new().unwrap();
    let pool: Arc<dyn Pool> = Arc::new(StaticPool::new(vec![be_addr]).unwrap());
    let (recording, binds) = RecordingStore::new();
    let store: Arc<dyn AffinityStore> = Arc::new(recording);
    let router = Arc::new(Router::single_pool(pool, store));
    let dialer = Dialer::new();
    let proxy = SparkConnectProxy::with_components(router, dialer, auth, metrics, resolver);

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
    (ch, binds, be_tx, gw_tx)
}

fn anon_auth() -> AuthInterceptor {
    AuthInterceptor::new(Arc::new(AnonymousAuthenticator))
}

fn token_auth_with_tenant(tenant: Option<&str>) -> AuthInterceptor {
    let mut entry = TokenEntry {
        token: "alice-secret".into(),
        user_id: "alice".into(),
        tenant: None,
        groups: vec![],
    };
    if let Some(t) = tenant {
        entry.tenant = Some(t.into());
    }
    let inner = StaticTokenAuthenticator::new(vec![entry]).unwrap();
    AuthInterceptor::new(Arc::new(inner))
}

fn bearer_req<T>(body: T, tok: &str) -> Request<T> {
    let mut req = Request::new(body);
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {}", tok)).unwrap(),
    );
    req
}

#[tokio::test]
async fn from_claim_default_falls_back_to_default_tenant() {
    // anon auth → Identity.tenant is None → resolver default = "default"
    let resolver = TenantResolver::new(TenantResolverConfig::default());
    let (ch, binds, _be, _gw) = rig(anon_auth(), resolver).await;
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);
    c.config(Request::new(pb::ConfigRequest {
        session_id: "s1".into(),
        ..Default::default()
    }))
    .await
    .expect("RPC ok");
    let captured = binds.lock().clone();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].tenant, "default");
    assert_eq!(captured[0].user_id, "anonymous");
    assert_eq!(captured[0].session_id, "s1");
}

#[tokio::test]
async fn from_claim_uses_token_tenant_when_present() {
    let auth = token_auth_with_tenant(Some("team-a"));
    let resolver = TenantResolver::new(TenantResolverConfig::default());
    let (ch, binds, _be, _gw) = rig(auth, resolver).await;
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);
    c.config(bearer_req(
        pb::ConfigRequest {
            session_id: "s1".into(),
            ..Default::default()
        },
        "alice-secret",
    ))
    .await
    .expect("RPC ok");
    let captured = binds.lock().clone();
    assert_eq!(captured[0].tenant, "team-a");
}

#[tokio::test]
async fn from_claim_reject_without_tenant_returns_unauthenticated() {
    // Token has no tenant → on_missing=Reject → Unauthenticated.
    let auth = token_auth_with_tenant(None);
    let resolver = TenantResolver::new(TenantResolverConfig {
        source: TenantSource::FromClaim,
        on_missing: OnMissing::Reject,
        default_name: "default".into(),
    });
    let (ch, binds, _be, _gw) = rig(auth, resolver).await;
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);
    let err = c
        .config(bearer_req(
            pb::ConfigRequest {
                session_id: "s1".into(),
                ..Default::default()
            },
            "alice-secret",
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    // Backend never gets called → no bind recorded.
    assert!(binds.lock().is_empty());
}

#[tokio::test]
async fn from_metadata_reads_x_tenant_header() {
    let resolver = TenantResolver::new(TenantResolverConfig {
        source: TenantSource::FromMetadata {
            header: "x-tenant".into(),
        },
        on_missing: OnMissing::Reject,
        default_name: "default".into(),
    });
    let (ch, binds, _be, _gw) = rig(anon_auth(), resolver).await;
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);
    let mut req = Request::new(pb::ConfigRequest {
        session_id: "s1".into(),
        ..Default::default()
    });
    req.metadata_mut()
        .insert("x-tenant", MetadataValue::try_from("team-b").unwrap());
    c.config(req).await.expect("RPC ok");
    let captured = binds.lock().clone();
    assert_eq!(captured[0].tenant, "team-b");
}

#[tokio::test]
async fn always_default_ignores_claim_and_header() {
    let auth = token_auth_with_tenant(Some("should-be-ignored"));
    let resolver = TenantResolver::new(TenantResolverConfig {
        source: TenantSource::AlwaysDefault,
        on_missing: OnMissing::UseDefault,
        default_name: "fixed-tenant".into(),
    });
    let (ch, binds, _be, _gw) = rig(auth, resolver).await;
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);
    let mut req = bearer_req(
        pb::ConfigRequest {
            session_id: "s1".into(),
            ..Default::default()
        },
        "alice-secret",
    );
    req.metadata_mut()
        .insert("x-tenant", MetadataValue::try_from("also-ignored").unwrap());
    c.config(req).await.expect("RPC ok");
    let captured = binds.lock().clone();
    assert_eq!(captured[0].tenant, "fixed-tenant");
}

#[tokio::test]
async fn two_tenants_with_same_session_id_are_isolated() {
    // Two clients with different tenants both use session_id="s1".
    // The store should see two distinct keys, not one.
    let resolver = TenantResolver::new(TenantResolverConfig {
        source: TenantSource::FromMetadata {
            header: "x-tenant".into(),
        },
        on_missing: OnMissing::UseDefault,
        default_name: "default".into(),
    });
    let (ch, binds, _be, _gw) = rig(anon_auth(), resolver).await;
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);

    for tenant in ["team-a", "team-b"] {
        let mut req = Request::new(pb::ConfigRequest {
            session_id: "s1".into(),
            ..Default::default()
        });
        req.metadata_mut()
            .insert("x-tenant", MetadataValue::try_from(tenant).unwrap());
        c.config(req).await.expect("RPC ok");
    }

    let captured = binds.lock().clone();
    assert_eq!(captured.len(), 2, "two distinct binds");
    let tenants: Vec<&str> = captured.iter().map(|k| k.tenant.as_str()).collect();
    assert!(tenants.contains(&"team-a"));
    assert!(tenants.contains(&"team-b"));
    // Both have session_id="s1" but the keys differ because tenants differ.
    assert_ne!(captured[0], captured[1]);
}
