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

//! Integration test for `CloneSession` binding semantics.
//!
//! A cloned session's state lives on the driver that executed the
//! clone, so the gateway must bind the *new* session id to that same
//! backend. Without the binding, follow-up RPCs on the cloned session
//! go through pool selection and land on an arbitrary backend — with
//! two backends in the pool, a coin flip away from a broken session.
//!
//! The rig runs two real backend servers so a missing binding has a
//! real chance of being observable, and asserts the binding directly
//! against the shared `MemoryStore` rather than sampling routing
//! behaviour probabilistically.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::AuditCapture;
use scg_audit::{AuditConfig, AuditLogger};
use scg_auth::{AnonymousAuthenticator, AuthInterceptor};
use scg_genproto::pb;
use scg_observability::Metrics;
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_routing::{AffinityStore, Pool, Router, SessionKey, TenantRouter};
use scg_store_memory::MemoryStore;
use scg_tenant::{OnMissing, TenantResolver, TenantResolverConfig, TenantSource};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

/// Backend that supports `Config` (to establish a parent binding) and
/// `CloneSession` (honouring a client-chosen id, generating one
/// otherwise). Everything else is `Unimplemented`.
#[derive(Default)]
struct CloneBackend;

#[tonic::async_trait]
impl pb::spark_connect_service_server::SparkConnectService for CloneBackend {
    type ExecutePlanStream = std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<pb::ExecutePlanResponse, Status>> + Send + 'static>,
    >;
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

    async fn clone_session(
        &self,
        req: Request<pb::CloneSessionRequest>,
    ) -> Result<Response<pb::CloneSessionResponse>, Status> {
        let body = req.into_inner();
        let new_id = body
            .new_session_id
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("{}-clone", body.session_id));
        Ok(Response::new(pb::CloneSessionResponse {
            session_id: body.session_id,
            new_session_id: new_id,
            ..Default::default()
        }))
    }

    async fn release_session(
        &self,
        _: Request<pb::ReleaseSessionRequest>,
    ) -> Result<Response<pb::ReleaseSessionResponse>, Status> {
        Err(Status::unimplemented("n/a"))
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
    async fn fetch_error_details(
        &self,
        _: Request<pb::FetchErrorDetailsRequest>,
    ) -> Result<Response<pb::FetchErrorDetailsResponse>, Status> {
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

async fn spawn_backend() -> (String, tokio::sync::oneshot::Sender<()>) {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(
                pb::spark_connect_service_server::SparkConnectServiceServer::new(CloneBackend),
            )
            .serve_with_incoming_shutdown(TcpListenerStream::new(lis), async {
                let _ = rx.await;
            })
            .await
            .ok();
    });
    (addr, tx)
}

struct Rig {
    channel: Channel,
    store: Arc<MemoryStore>,
    _backend_shutdowns: Vec<tokio::sync::oneshot::Sender<()>>,
    _gw_shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn spawn_rig_two_backends() -> Rig {
    let (addr_a, tx_a) = spawn_backend().await;
    let (addr_b, tx_b) = spawn_backend().await;

    let metrics = Metrics::new().unwrap();
    let store = Arc::new(MemoryStore::new());
    let store_dyn: Arc<dyn AffinityStore> = store.clone();
    let pool: Arc<dyn Pool> = Arc::new(StaticPool::new(vec![addr_a, addr_b]).unwrap());
    let router = Arc::new(Router::new(TenantRouter::single(pool), store_dyn));
    let resolver = TenantResolver::new(TenantResolverConfig {
        source: TenantSource::FromMetadata {
            header: "x-tenant".into(),
        },
        on_missing: OnMissing::UseDefault,
        default_name: "default".into(),
    });
    let audit = AuditLogger::new(AuditConfig {
        enabled: true,
        log_successful_rpcs: false,
    });
    let proxy = SparkConnectProxy::with_all(
        router,
        Dialer::new(),
        AuthInterceptor::new(Arc::new(AnonymousAuthenticator)),
        metrics,
        resolver,
        None,
        audit,
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

    let channel = Endpoint::from_shared(format!("http://{}", gw_addr))
        .unwrap()
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();

    Rig {
        channel,
        store,
        _backend_shutdowns: vec![tx_a, tx_b],
        _gw_shutdown: gw_tx,
    }
}

fn key(session_id: &str) -> SessionKey {
    // AnonymousAuthenticator yields user_id="anonymous"; the resolver
    // defaults the tenant to "default" when no x-tenant header is set.
    SessionKey::with_tenant("default", "anonymous", session_id)
}

#[tokio::test]
async fn cloned_session_is_bound_to_parents_backend() {
    let cap = AuditCapture::lease();
    let rig = spawn_rig_two_backends().await;
    let mut c =
        pb::spark_connect_service_client::SparkConnectServiceClient::new(rig.channel.clone());

    // Establish the parent binding.
    c.config(pb::ConfigRequest {
        session_id: "parent-1".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let parent_addr = rig
        .store
        .lookup_session(&key("parent-1"))
        .await
        .expect("parent session bound");

    // Clone with a server-generated new id.
    let resp = c
        .clone_session(pb::CloneSessionRequest {
            session_id: "parent-1".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let cloned = resp.new_session_id;
    assert!(!cloned.is_empty());

    // The cloned id must be bound to the same backend as the parent —
    // not left to pool selection.
    assert_eq!(
        rig.store.lookup_session(&key(&cloned)).await.as_deref(),
        Some(parent_addr.as_str()),
        "cloned session must be bound to the parent's backend"
    );

    // And the fresh binding is audited: one session.create for the
    // parent (from Config), one for the clone, with the same backend.
    assert_eq!(cap.count_events("session.create"), 2);
    let clone_evt = cap
        .snapshot()
        .into_iter()
        .find(|e| {
            e.fields.get("event").map(|s| s.as_str()) == Some("session.create")
                && e.fields.get("session_id").map(|s| s.as_str()) == Some(cloned.as_str())
        })
        .expect("session.create for the cloned session");
    assert_eq!(
        clone_evt.fields.get("backend").map(|s| s.as_str()),
        Some(parent_addr.as_str())
    );
}

#[tokio::test]
async fn client_chosen_clone_id_is_bound_to_parents_backend() {
    let _cap = AuditCapture::lease();
    let rig = spawn_rig_two_backends().await;
    let mut c =
        pb::spark_connect_service_client::SparkConnectServiceClient::new(rig.channel.clone());

    c.config(pb::ConfigRequest {
        session_id: "parent-2".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    let parent_addr = rig
        .store
        .lookup_session(&key("parent-2"))
        .await
        .expect("parent session bound");

    // Clone with a client-chosen id, as the proto allows.
    let resp = c
        .clone_session(pb::CloneSessionRequest {
            session_id: "parent-2".into(),
            new_session_id: Some("my-clone-id".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.new_session_id, "my-clone-id");

    assert_eq!(
        rig.store
            .lookup_session(&key("my-clone-id"))
            .await
            .as_deref(),
        Some(parent_addr.as_str()),
    );
}
