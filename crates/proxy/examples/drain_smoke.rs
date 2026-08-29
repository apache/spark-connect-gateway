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

//! Graceful-shutdown smoke test.
//!
//! Exercises the two-step drain logic the gateway main does on
//! SIGTERM, but in-process (no actual signals — drain is triggered
//! by sending on a channel the same way `shutdown_signal()` does in
//! production).
//!
//! Verifies:
//!
//!   1. Before drain, a long-running ExecutePlan stream is in
//!      progress.
//!   2. Drain is triggered → readiness flips to not-ready immediately.
//!   3. The in-flight stream continues to receive messages and
//!      eventually completes normally — it is *not* killed.
//!   4. After the stream completes, the gRPC server actually shuts
//!      down (i.e. the drain loop didn't deadlock).
//!
//! Run with:
//! ```bash
//! cargo run -p scg-proxy --example drain_smoke
//! ```
//!
//! No external dependencies (no Redis, no Docker).

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use scg_auth::{AnonymousAuthenticator, AuthInterceptor};
use scg_genproto::pb;
use scg_observability::{Metrics, ReadinessProbe};
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_routing::{AffinityStore, Pool, Router};
use scg_store_memory::MemoryStore;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

/// Backend that streams 5 ExecutePlan responses with a small delay
/// between each, totalling ~1.5s. Long enough that we can:
///   - start the stream,
///   - trigger drain mid-stream,
///   - observe drain *waits* for it,
///   - then see the stream finish.
struct SlowBackend;

#[tonic::async_trait]
impl pb::spark_connect_service_server::SparkConnectService for SlowBackend {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = Result<pb::ExecutePlanResponse, Status>> + Send + 'static>>;
    type ReattachExecuteStream = Self::ExecutePlanStream;

    async fn execute_plan(
        &self,
        req: Request<pb::ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let body = req.into_inner();
        let stream = async_stream::stream! {
            for i in 0..5 {
                tokio::time::sleep(Duration::from_millis(300)).await;
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

    async fn config(
        &self,
        _: Request<pb::ConfigRequest>,
    ) -> Result<Response<pb::ConfigResponse>, Status> {
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

#[tokio::main]
async fn main() {
    // 1. Backend
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let be_addr = lis.local_addr().unwrap().to_string();
    let (be_kill_tx, be_kill_rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(
                pb::spark_connect_service_server::SparkConnectServiceServer::new(SlowBackend),
            )
            .serve_with_incoming_shutdown(TcpListenerStream::new(lis), async {
                let _ = be_kill_rx.await;
            })
            .await
            .ok();
    });

    // 2. Gateway with the same drain plumbing main uses.
    let metrics = Metrics::new().unwrap();
    let readiness = ReadinessProbe::new(true);
    let pool: Arc<dyn Pool> = Arc::new(StaticPool::new(vec![be_addr.clone()]).unwrap());
    let store: Arc<dyn AffinityStore> = Arc::new(MemoryStore::new());
    let router = Arc::new(Router::single_pool(pool, store));
    let dialer = Dialer::new();
    let proxy = SparkConnectProxy::with_auth_and_metrics(
        router,
        dialer,
        AuthInterceptor::new(Arc::new(AnonymousAuthenticator)),
        metrics.clone(),
    );

    let gw_lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gw_addr = gw_lis.local_addr().unwrap();

    // Drain plumbing — mirrors gateway main.
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let readiness_for_drain = readiness.clone();
    let metrics_for_drain = metrics.clone();
    let (drain_trigger_tx, drain_trigger_rx) = oneshot::channel();
    let deadline = Duration::from_secs(5);
    tokio::spawn(async move {
        let _ = drain_trigger_rx.await;
        eprintln!("[drain_smoke] drain triggered; flipping readiness off");
        readiness_for_drain.mark_not_ready();
        let drained = tokio::time::timeout(deadline, async {
            loop {
                if metrics_for_drain.active_streams_value() <= 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        match drained {
            Ok(()) => eprintln!("[drain_smoke] drain finished cleanly"),
            Err(_) => eprintln!("[drain_smoke] drain deadline reached"),
        }
        let _ = shutdown_tx.send(true);
    });

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(pb::spark_connect_service_server::SparkConnectServiceServer::new(proxy))
            .serve_with_incoming_shutdown(TcpListenerStream::new(gw_lis), async move {
                let _ = shutdown_rx.changed().await;
            })
            .await
            .ok();
    });

    // 3. Client opens a long-running stream.
    let endpoint = Endpoint::from_shared(format!("http://{}", gw_addr)).unwrap();
    let ch: Channel = endpoint
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .unwrap();
    let mut client = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch);

    let started_at = std::time::Instant::now();
    let stream = client
        .execute_plan(Request::new(pb::ExecutePlanRequest {
            session_id: "drain-test".into(),
            operation_id: Some("op-drain".into()),
            ..Default::default()
        }))
        .await
        .expect("ExecutePlan accepted")
        .into_inner();
    let mut stream = Box::pin(stream);

    // Pull the first message so we know the stream is genuinely
    // active before triggering drain.
    let first = stream
        .next()
        .await
        .expect("first message")
        .expect("ok response");
    eprintln!(
        "[drain_smoke] received first message: response_id={}",
        first.response_id
    );

    assert!(readiness.is_ready(), "before drain, /readyz should be 200");
    assert!(
        metrics.active_streams_value() >= 1,
        "active stream should be counted; got {}",
        metrics.active_streams_value()
    );

    // 4. Trigger drain. Readiness flips immediately.
    let _ = drain_trigger_tx.send(());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !readiness.is_ready(),
        "readiness must flip to not-ready as soon as drain is triggered"
    );

    // 5. Drain (and the gRPC server) waits for our stream to end.
    //    Pull the rest. Each message takes ~300ms; total should be
    //    around 1.2s after the first.
    let mut count = 1;
    while let Some(msg) = stream.next().await {
        let m = msg.expect("ok response");
        eprintln!(
            "[drain_smoke] received message during drain: response_id={}",
            m.response_id
        );
        count += 1;
    }
    let stream_duration = started_at.elapsed();
    assert_eq!(
        count, 5,
        "all 5 stream messages must arrive even after drain triggered"
    );
    assert!(
        stream_duration >= Duration::from_millis(1400),
        "stream should take at least ~1.5s; got {:?}",
        stream_duration
    );
    eprintln!(
        "[drain_smoke] stream completed in {:?} with {} messages",
        stream_duration, count
    );

    // 6. After the stream ended, active_streams should hit 0 and the
    //    server should shut down within ~200ms (the drain loop's
    //    poll interval).
    let server_ended = tokio::time::timeout(Duration::from_secs(2), server_handle)
        .await
        .expect("gateway shut down within timeout");
    server_ended.expect("gateway task ran to completion");
    eprintln!("[drain_smoke] gateway shut down cleanly after drain");

    let _ = be_kill_tx.send(());
    println!("[drain_smoke] all drain invariants passed");
}
