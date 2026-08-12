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

//! Integration test: a forwarded RPC bumps `scg_rpcs_total`, an
//! unauthenticated request bumps `scg_auth_failures_total`, and a
//! Prometheus scrape against the admin server returns both as text.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use scg_auth::{
    token::{StaticTokenAuthenticator, TokenEntry},
    AuthInterceptor,
};
use scg_genproto::pb;
use scg_observability::{serve_admin, AdminConfig, Metrics, ReadinessProbe};
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_routing::{AffinityStore, Pool, Router};
use scg_store_memory::MemoryStore;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

/// Backend that just echoes session_id for simplicity.
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

struct Rig {
    grpc: Channel,
    admin_url: String,
    _be_shutdown: tokio::sync::oneshot::Sender<()>,
    _gw_shutdown: tokio::sync::oneshot::Sender<()>,
    _admin_shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn rig_with_metrics() -> (Rig, Metrics) {
    // Backend
    let svc = pb::spark_connect_service_server::SparkConnectServiceServer::new(EchoBackend);
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

    // Gateway with static-token auth + shared Metrics
    let metrics = Metrics::new().unwrap();
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
    let proxy = SparkConnectProxy::with_auth_and_metrics(
        router,
        dialer,
        AuthInterceptor::new(Arc::new(auth)),
        metrics.clone(),
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
    let grpc = endpoint
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();

    // Admin server on a separate ephemeral port
    let admin_lis = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let admin_addr = admin_lis.local_addr().unwrap();
    drop(admin_lis);
    let admin_cfg = AdminConfig {
        bind_addr: admin_addr,
    };
    let metrics_for_admin = metrics.clone();
    let readiness = ReadinessProbe::new(true);
    let (admin_tx, admin_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let shutdown = async move {
            let _ = admin_rx.await;
        };
        let _ = serve_admin(admin_cfg, metrics_for_admin, readiness, shutdown).await;
    });

    // Wait briefly for admin server to bind
    for _ in 0..50 {
        if std::net::TcpStream::connect(admin_addr).is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let admin_url = format!("http://{}", admin_addr);
    (
        Rig {
            grpc,
            admin_url,
            _be_shutdown: be_tx,
            _gw_shutdown: gw_tx,
            _admin_shutdown: admin_tx,
        },
        metrics,
    )
}

async fn scrape(url: &str) -> String {
    let body = reqwest::get(format!("{}/metrics", url))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    body
}

#[tokio::test]
async fn successful_rpc_increments_rpcs_total() {
    let (rig, _metrics) = rig_with_metrics().await;
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(rig.grpc.clone());

    let mut req = Request::new(pb::ConfigRequest {
        session_id: "s1".into(),
        ..Default::default()
    });
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from("Bearer alice-secret").unwrap(),
    );
    c.config(req).await.unwrap();

    // /metrics should now contain a non-zero counter for rpc=Config code=OK.
    let body = scrape(&rig.admin_url).await;
    assert!(
        body.contains("scg_rpcs_total"),
        "/metrics missing scg_rpcs_total:\n{body}"
    );
    assert!(
        body.lines().any(|l| l.starts_with("scg_rpcs_total{")
            && l.contains("rpc=\"Config\"")
            && l.contains("code=\"OK\"")),
        "no Config OK counter line:\n{body}"
    );
}

#[tokio::test]
async fn unauthenticated_request_increments_auth_failures() {
    let (rig, _metrics) = rig_with_metrics().await;
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(rig.grpc.clone());

    let err = c
        .config(pb::ConfigRequest {
            session_id: "s1".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    let body = scrape(&rig.admin_url).await;
    assert!(
        body.contains("scg_auth_failures_total"),
        "/metrics missing scg_auth_failures_total:\n{body}"
    );
    assert!(
        body.lines()
            .any(|l| l.starts_with("scg_auth_failures_total{")
                && l.contains("reason=\"missing_token\"")),
        "no missing_token failure line:\n{body}"
    );
    // The RPC still gets recorded with code=Unauthenticated.
    assert!(
        body.lines()
            .any(|l| l.starts_with("scg_rpcs_total{") && l.contains("code=\"Unauthenticated\"")),
        "no Unauthenticated rpc total line:\n{body}"
    );
}

#[tokio::test]
async fn healthz_and_readyz_endpoints_work() {
    let (rig, _metrics) = rig_with_metrics().await;
    let body = reqwest::get(format!("{}/healthz", rig.admin_url))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "ok");

    let body = reqwest::get(format!("{}/readyz", rig.admin_url))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(body, "ready");
}
