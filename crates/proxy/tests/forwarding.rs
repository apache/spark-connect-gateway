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

//! In-process integration tests: gateway proxy in front of one or more fake
//! Spark Connect backends. Validates forwarding semantics without needing a
//! real Spark distribution.
//!
//! Four scenarios:
//!
//! 1. Unary RPC forwarding (`Config`).
//! 2. Server-streaming forwarding emits all upstream messages (`ExecutePlan`).
//! 3. Session affinity stickiness across many calls (no matter how many
//!    different sessions are interleaved).
//! 4. Op-id reverse index — `ReattachExecute` with a *different* `session_id`
//!    still routes to the backend that handled the original `ExecutePlan`.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use scg_genproto::pb;
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_routing::{AffinityStore, Pool, Router};
use scg_store_memory::MemoryStore;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

// ----- Fake backend -------------------------------------------------------

/// Minimal SparkConnectService implementation. Each backend tags responses
/// with its `id` so tests can prove which backend served a given RPC.
#[derive(Default)]
struct FakeBackend {
    id: String,
    config_count: std::sync::atomic::AtomicU64,
    execute_count: std::sync::atomic::AtomicU64,
}

impl FakeBackend {
    fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            ..Default::default()
        }
    }
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
        self.config_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let body = req.into_inner();
        Ok(Response::new(pb::ConfigResponse {
            session_id: format!("{}@{}", body.session_id, self.id),
            ..Default::default()
        }))
    }

    async fn execute_plan(
        &self,
        req: Request<pb::ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        self.execute_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let body = req.into_inner();
        let id = self.id.clone();
        // Emit 3 messages so we can prove server-stream forwarding emits them all.
        let stream = async_stream::stream! {
            for _ in 0..3 {
                yield Ok(pb::ExecutePlanResponse {
                    session_id: format!("{}@{}", body.session_id, id),
                    operation_id: body.operation_id.clone().unwrap_or_default(),
                    ..Default::default()
                });
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn reattach_execute(
        &self,
        req: Request<pb::ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        self.execute_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let body = req.into_inner();
        let id = self.id.clone();
        let stream = async_stream::stream! {
            yield Ok(pb::ExecutePlanResponse {
                session_id: format!("{}@{}", body.session_id, id),
                operation_id: body.operation_id.clone(),
                ..Default::default()
            });
        };
        Ok(Response::new(Box::pin(stream)))
    }

    // The fake doesn't need to implement the rest for these tests. The
    // default `Unimplemented` is fine and won't be triggered.
    async fn analyze_plan(
        &self,
        _: Request<pb::AnalyzePlanRequest>,
    ) -> Result<Response<pb::AnalyzePlanResponse>, Status> {
        Err(Status::unimplemented("not used in tests"))
    }
    async fn artifact_status(
        &self,
        _: Request<pb::ArtifactStatusesRequest>,
    ) -> Result<Response<pb::ArtifactStatusesResponse>, Status> {
        Err(Status::unimplemented("not used in tests"))
    }
    async fn interrupt(
        &self,
        _: Request<pb::InterruptRequest>,
    ) -> Result<Response<pb::InterruptResponse>, Status> {
        Err(Status::unimplemented("not used in tests"))
    }
    async fn release_execute(
        &self,
        _: Request<pb::ReleaseExecuteRequest>,
    ) -> Result<Response<pb::ReleaseExecuteResponse>, Status> {
        Err(Status::unimplemented("not used in tests"))
    }
    async fn release_session(
        &self,
        _: Request<pb::ReleaseSessionRequest>,
    ) -> Result<Response<pb::ReleaseSessionResponse>, Status> {
        Err(Status::unimplemented("not used in tests"))
    }
    async fn fetch_error_details(
        &self,
        _: Request<pb::FetchErrorDetailsRequest>,
    ) -> Result<Response<pb::FetchErrorDetailsResponse>, Status> {
        Err(Status::unimplemented("not used in tests"))
    }
    async fn clone_session(
        &self,
        _: Request<pb::CloneSessionRequest>,
    ) -> Result<Response<pb::CloneSessionResponse>, Status> {
        Err(Status::unimplemented("not used in tests"))
    }
    async fn get_status(
        &self,
        _: Request<pb::GetStatusRequest>,
    ) -> Result<Response<pb::GetStatusResponse>, Status> {
        Err(Status::unimplemented("not used in tests"))
    }
    async fn add_artifacts(
        &self,
        _: Request<tonic::Streaming<pb::AddArtifactsRequest>>,
    ) -> Result<Response<pb::AddArtifactsResponse>, Status> {
        Err(Status::unimplemented("not used in tests"))
    }
}

// ----- Test harness -------------------------------------------------------

struct BackendHandle {
    addr: String,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for BackendHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

async fn start_backend(id: &str) -> BackendHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let svc =
        pb::spark_connect_service_server::SparkConnectServiceServer::new(FakeBackend::new(id));
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = rx.await;
            })
            .await
            .ok();
    });
    BackendHandle {
        addr,
        shutdown: Some(tx),
    }
}

struct GatewayHandle {
    channel: Channel,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for GatewayHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

async fn start_gateway(backends: Vec<String>) -> GatewayHandle {
    let pool: Arc<dyn Pool> = Arc::new(StaticPool::new(backends).unwrap());
    let store: Arc<dyn AffinityStore> = Arc::new(MemoryStore::new());
    let router = Arc::new(Router::single_pool(pool, store));
    let dialer = Dialer::new();
    let svc = SparkConnectProxy::new(router, dialer);
    let server = pb::spark_connect_service_server::SparkConnectServiceServer::new(svc);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(server)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = rx.await;
            })
            .await
            .ok();
    });

    // Give the listener a moment to be ready and then dial in.
    let endpoint = Endpoint::from_shared(format!("http://{}", addr)).unwrap();
    let channel = endpoint
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();

    GatewayHandle {
        channel,
        shutdown: Some(tx),
    }
}

fn client(
    h: &GatewayHandle,
) -> pb::spark_connect_service_client::SparkConnectServiceClient<Channel> {
    pb::spark_connect_service_client::SparkConnectServiceClient::new(h.channel.clone())
}

// ----- Tests --------------------------------------------------------------

#[tokio::test]
async fn unary_forward_returns_backend_tag() {
    let be = start_backend("be1").await;
    let gw = start_gateway(vec![be.addr.clone()]).await;

    let resp = client(&gw)
        .config(pb::ConfigRequest {
            session_id: "sess-1".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.session_id, "sess-1@be1");
}

#[tokio::test]
async fn server_stream_emits_all_messages() {
    let be = start_backend("be1").await;
    let gw = start_gateway(vec![be.addr.clone()]).await;

    let mut stream = client(&gw)
        .execute_plan(pb::ExecutePlanRequest {
            session_id: "sess-1".into(),
            operation_id: Some("op-1".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();

    let mut count = 0;
    while let Some(_msg) = stream.message().await.unwrap() {
        count += 1;
    }
    assert_eq!(count, 3);
}

#[tokio::test]
async fn session_affinity_is_sticky_across_calls() {
    let a = start_backend("A").await;
    let b = start_backend("B").await;
    let gw = start_gateway(vec![a.addr.clone(), b.addr.clone()]).await;

    let mut c = client(&gw);
    // First call binds sess-1 to whichever backend round-robin picks first.
    let r1 = c
        .config(pb::ConfigRequest {
            session_id: "sess-1".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let first = r1.session_id; // "sess-1@A" or "sess-1@B"

    for i in 0..5 {
        let r = c
            .config(pb::ConfigRequest {
                session_id: "sess-1".into(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(r.session_id, first, "stickiness broken on call {}", i);
    }
}

#[tokio::test]
async fn reattach_routes_via_op_id_even_with_different_session_id() {
    let a = start_backend("A").await;
    let b = start_backend("B").await;
    let gw = start_gateway(vec![a.addr.clone(), b.addr.clone()]).await;

    let mut c = client(&gw);

    // First, run an ExecutePlan with op-xyz on sess-1. The gateway records
    // (op-xyz → backend) in the reverse index.
    let mut s1 = c
        .execute_plan(pb::ExecutePlanRequest {
            session_id: "sess-1".into(),
            operation_id: Some("op-xyz".into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let mut original_suffix = String::new();
    while let Some(msg) = s1.message().await.unwrap() {
        original_suffix = msg.session_id["sess-1".len()..].to_string(); // "@A" or "@B"
    }

    // Now reattach with a *different* session id. The gateway must still
    // route to the backend that handled the original ExecutePlan, because
    // the op-id reverse index outranks session affinity.
    let mut s2 = c
        .reattach_execute(pb::ReattachExecuteRequest {
            session_id: "different-session".into(),
            operation_id: "op-xyz".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let mut reattach_suffix = String::new();
    while let Some(msg) = s2.message().await.unwrap() {
        reattach_suffix = msg.session_id["different-session".len()..].to_string();
    }

    assert_eq!(
        original_suffix, reattach_suffix,
        "reattach landed on the wrong backend (orig {:?}, reattach {:?})",
        original_suffix, reattach_suffix
    );
}
