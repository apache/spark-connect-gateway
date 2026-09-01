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

//! Integration test: proxy with auth enabled rejects unauthenticated
//! callers and stamps the verified user_id onto requests forwarded to
//! the backend (proving client-supplied user_id is *not* trusted).

use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use parking_lot::Mutex;
use scg_auth::{
    token::{StaticTokenAuthenticator, TokenEntry},
    AuthInterceptor,
};
use scg_genproto::pb;
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_routing::{AffinityStore, Pool, Router};
use scg_store_memory::MemoryStore;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

/// A backend that records each Config request's UserContext so the
/// test can assert the proxy stamped the right user_id onto it.
#[derive(Default)]
struct RecordingBackend {
    last_user_id: Mutex<Option<String>>,
    seen: AtomicI64,
}

#[tonic::async_trait]
impl pb::spark_connect_service_server::SparkConnectService for RecordingBackend {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = Result<pb::ExecutePlanResponse, Status>> + Send + 'static>>;
    type ReattachExecuteStream = Self::ExecutePlanStream;

    async fn config(
        &self,
        req: Request<pb::ConfigRequest>,
    ) -> Result<Response<pb::ConfigResponse>, Status> {
        self.seen.fetch_add(1, Ordering::SeqCst);
        let body = req.into_inner();
        *self.last_user_id.lock() = body.user_context.as_ref().map(|u| u.user_id.clone());
        Ok(Response::new(pb::ConfigResponse {
            session_id: body.session_id,
            ..Default::default()
        }))
    }

    // Stub the rest — only Config is exercised in these tests.
    async fn analyze_plan(
        &self,
        _: Request<pb::AnalyzePlanRequest>,
    ) -> Result<Response<pb::AnalyzePlanResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn artifact_status(
        &self,
        _: Request<pb::ArtifactStatusesRequest>,
    ) -> Result<Response<pb::ArtifactStatusesResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn interrupt(
        &self,
        _: Request<pb::InterruptRequest>,
    ) -> Result<Response<pb::InterruptResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn release_execute(
        &self,
        _: Request<pb::ReleaseExecuteRequest>,
    ) -> Result<Response<pb::ReleaseExecuteResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn release_session(
        &self,
        _: Request<pb::ReleaseSessionRequest>,
    ) -> Result<Response<pb::ReleaseSessionResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn fetch_error_details(
        &self,
        _: Request<pb::FetchErrorDetailsRequest>,
    ) -> Result<Response<pb::FetchErrorDetailsResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn clone_session(
        &self,
        _: Request<pb::CloneSessionRequest>,
    ) -> Result<Response<pb::CloneSessionResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn get_status(
        &self,
        _: Request<pb::GetStatusRequest>,
    ) -> Result<Response<pb::GetStatusResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn execute_plan(
        &self,
        _: Request<pb::ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn reattach_execute(
        &self,
        _: Request<pb::ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        Err(Status::unimplemented("not used"))
    }
    async fn add_artifacts(
        &self,
        _: Request<tonic::Streaming<pb::AddArtifactsRequest>>,
    ) -> Result<Response<pb::AddArtifactsResponse>, Status> {
        Err(Status::unimplemented("not used"))
    }
}

struct TestRig {
    channel: Channel,
    backend: Arc<RecordingBackend>,
    _be_shutdown: tokio::sync::oneshot::Sender<()>,
    _gw_shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn rig() -> TestRig {
    // Backend
    let backend = Arc::new(RecordingBackend::default());
    let svc =
        pb::spark_connect_service_server::SparkConnectServiceServer::from_arc(backend.clone());
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let be_addr = lis.local_addr().unwrap().to_string();
    let (be_tx, be_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(TcpListenerStream::new(lis), async {
                let _ = be_rx.await;
            })
            .await
            .ok();
    });

    // Gateway with static-token auth
    let pool: Arc<dyn Pool> = Arc::new(StaticPool::new(vec![be_addr]).unwrap());
    let store: Arc<dyn AffinityStore> = Arc::new(MemoryStore::new());
    let router = Arc::new(Router::single_pool(pool, store));
    let dialer = Dialer::new();

    let auth = StaticTokenAuthenticator::new(vec![TokenEntry {
        token: "alice-secret".into(),
        user_id: "alice".into(),
        tenant: None,
        groups: vec![],
    }])
    .unwrap();
    let interceptor = AuthInterceptor::new(Arc::new(auth));
    let proxy = SparkConnectProxy::with_auth(router, dialer, interceptor);

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
    let channel = endpoint
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();
    TestRig {
        channel,
        backend,
        _be_shutdown: be_tx,
        _gw_shutdown: gw_tx,
    }
}

fn client(ch: Channel) -> pb::spark_connect_service_client::SparkConnectServiceClient<Channel> {
    pb::spark_connect_service_client::SparkConnectServiceClient::new(ch)
}

#[tokio::test]
async fn unauthenticated_request_rejected() {
    let rig = rig().await;
    let mut c = client(rig.channel.clone());
    let err = c
        .config(pb::ConfigRequest {
            session_id: "s1".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert_eq!(
        rig.backend.seen.load(Ordering::SeqCst),
        0,
        "backend must not have been called"
    );
}

#[tokio::test]
async fn invalid_token_rejected() {
    let rig = rig().await;
    let mut c = client(rig.channel.clone());

    let mut req = Request::new(pb::ConfigRequest {
        session_id: "s1".into(),
        ..Default::default()
    });
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from("Bearer wrong-secret").unwrap(),
    );
    let err = c.config(req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert_eq!(rig.backend.seen.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn valid_token_authenticates_and_overwrites_user_id() {
    let rig = rig().await;
    let mut c = client(rig.channel.clone());

    // Client *claims* to be "evil-impostor" — gateway must overwrite
    // the user_id with the verified identity ("alice").
    let mut req = Request::new(pb::ConfigRequest {
        session_id: "s1".into(),
        user_context: Some(pb::UserContext {
            user_id: "evil-impostor".into(),
            ..Default::default()
        }),
        ..Default::default()
    });
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from("Bearer alice-secret").unwrap(),
    );
    let resp = c.config(req).await.unwrap().into_inner();
    assert_eq!(resp.session_id, "s1");
    assert_eq!(rig.backend.seen.load(Ordering::SeqCst), 1);
    let stamped = rig.backend.last_user_id.lock().clone();
    assert_eq!(
        stamped.as_deref(),
        Some("alice"),
        "gateway must overwrite client-supplied user_id with verified identity, \
         not trust 'evil-impostor'"
    );
}

#[tokio::test]
async fn missing_user_context_gets_one_stamped() {
    let rig = rig().await;
    let mut c = client(rig.channel.clone());

    // Client sends no UserContext at all.
    let mut req = Request::new(pb::ConfigRequest {
        session_id: "s1".into(),
        user_context: None,
        ..Default::default()
    });
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from("Bearer alice-secret").unwrap(),
    );
    c.config(req).await.unwrap();
    let stamped = rig.backend.last_user_id.lock().clone();
    assert_eq!(stamped.as_deref(), Some("alice"));
}
