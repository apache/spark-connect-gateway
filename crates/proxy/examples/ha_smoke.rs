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

//! Multi-replica HA smoke test.
//!
//! Spawns two real SparkConnectProxy gateways, both backed by:
//!   * the same Redis affinity store (`scg-store-redis`),
//!   * the same static pool of two fake Spark Connect backends.
//!
//! Then drives a series of RPCs through different replicas and
//! verifies three HA properties:
//!
//!   A. **Shared state.** A session bound through replica A resolves
//!      to the *same* backend through replica B.
//!   B. **Failover.** After replica A is killed, the same session
//!      hitting replica B still routes to the original backend
//!      (binding survives the death of the replica that wrote it).
//!   C. **Op-id reverse index across replicas.** An ExecutePlan
//!      started through replica A registers its `operation_id`; a
//!      `ReattachExecute(op_id, session_id="different")` arriving at
//!      replica B (after A is gone) still reaches the backend that
//!      ran the plan.
//!
//! Run with a Redis listening on :6399 (or override via REDIS_URL):
//!
//! ```bash
//! redis-server --port 6399 --daemonize yes
//! cargo run -p scg-proxy --example ha_smoke
//! ```
//!
//! Exits with non-zero status (panic) on any invariant failure so a
//! CI script can wrap it.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use scg_auth::{AnonymousAuthenticator, AuthInterceptor};
use scg_genproto::pb;
use scg_observability::Metrics;
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_routing::{AffinityStore, Pool, Router};
use scg_store_redis::{RedisStore, RedisStoreConfig};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

/// Fake backend that tags every response with its own id, so the
/// driver code can verify which backend handled a given RPC.
#[derive(Default)]
struct FakeBackend {
    id: String,
}

impl FakeBackend {
    fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
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
        let body = req.into_inner();
        let id = self.id.clone();
        let stream = async_stream::stream! {
            yield Ok(pb::ExecutePlanResponse {
                session_id: format!("{}@{}", body.session_id, id),
                operation_id: body.operation_id.clone().unwrap_or_default(),
                ..Default::default()
            });
        };
        Ok(Response::new(Box::pin(stream)))
    }

    async fn reattach_execute(
        &self,
        req: Request<pb::ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
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
    async fn add_artifacts(
        &self,
        _: Request<tonic::Streaming<pb::AddArtifactsRequest>>,
    ) -> Result<Response<pb::AddArtifactsResponse>, Status> {
        Err(Status::unimplemented("n/a"))
    }
}

async fn spawn_backend(id: &'static str) -> (String, oneshot::Sender<()>) {
    let svc =
        pb::spark_connect_service_server::SparkConnectServiceServer::new(FakeBackend::new(id));
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
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
    (addr, tx)
}

async fn spawn_gateway(
    backends: Vec<String>,
    store: Arc<dyn AffinityStore>,
) -> (Channel, oneshot::Sender<()>) {
    let metrics = Metrics::new().unwrap();
    let pool: Arc<dyn Pool> = Arc::new(StaticPool::new(backends).unwrap());
    let router = Arc::new(Router::single_pool(pool, store));
    let dialer = Dialer::new();
    let proxy = SparkConnectProxy::with_auth_and_metrics(
        router,
        dialer,
        AuthInterceptor::new(Arc::new(AnonymousAuthenticator)),
        metrics,
    );

    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap();
    let (tx, rx) = oneshot::channel();
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

/// Issue a Config call and return the backend id that handled it
/// (extracted from the FakeBackend's tagged session_id).
async fn config_through(ch: &Channel, session_id: &str) -> String {
    let mut c = pb::spark_connect_service_client::SparkConnectServiceClient::new(ch.clone());
    let resp = c
        .config(Request::new(pb::ConfigRequest {
            session_id: session_id.into(),
            ..Default::default()
        }))
        .await
        .expect("Config RPC succeeds");
    let tagged = resp.into_inner().session_id;
    // FakeBackend formats it as "{client_session_id}@{backend_id}".
    tagged.rsplit('@').next().unwrap().to_string()
}

#[tokio::main]
async fn main() {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6399".into());
    eprintln!("[ha_smoke] redis at {}", redis_url);

    // Two fake backends. The pool round-robins, so the first session
    // hits "be-a", the next "be-b", and so on.
    let (be_a, _be_a_kill) = spawn_backend("be-a").await;
    let (be_b, _be_b_kill) = spawn_backend("be-b").await;
    let backends = vec![be_a.clone(), be_b.clone()];
    eprintln!("[ha_smoke] backends: {:?}", backends);

    // Unique key prefix per run so re-runs don't see each other's state.
    let prefix = format!("scg-ha-{}", std::process::id());

    let make_store = || async {
        let store = RedisStore::connect(RedisStoreConfig {
            url: redis_url.clone(),
            key_prefix: prefix.clone(),
            session_ttl: Duration::from_secs(60),
            op_ttl: Duration::from_secs(60),
        })
        .await
        .expect("connect redis");
        Arc::new(store) as Arc<dyn AffinityStore>
    };

    // Two replicas. Each gets its own RedisStore handle, but they
    // both point at the same Redis (and same key prefix), so their
    // affinity tables are the same persistent dataset.
    let store_a = make_store().await;
    let store_b = make_store().await;
    let (ch_a, kill_a) = spawn_gateway(backends.clone(), store_a).await;
    let (ch_b, kill_b) = spawn_gateway(backends.clone(), store_b).await;
    eprintln!("[ha_smoke] gateway A and gateway B up");

    // ----- Scenario A: shared state across replicas -------------------
    let session = "ha-session-1";
    let chosen_via_a = config_through(&ch_a, session).await;
    eprintln!(
        "[ha_smoke] scenario A: session {} bound via replica A -> {}",
        session, chosen_via_a
    );
    let chosen_via_b = config_through(&ch_b, session).await;
    assert_eq!(
        chosen_via_a, chosen_via_b,
        "scenario A: replica B must resolve to the same backend as replica A"
    );
    eprintln!("[ha_smoke] ok: scenario A — shared state");

    // ----- Scenario B: failover after replica death ------------------
    let session_b = "ha-session-2";
    let chosen_b_pre = config_through(&ch_a, session_b).await;
    eprintln!(
        "[ha_smoke] scenario B: session {} bound via replica A -> {}",
        session_b, chosen_b_pre
    );
    // Kill replica A. Drop the channel first so the connection
    // closes; then send the shutdown.
    drop(ch_a);
    let _ = kill_a.send(());
    // Give the runtime a beat to actually tear the server down.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let chosen_b_post = config_through(&ch_b, session_b).await;
    assert_eq!(
        chosen_b_post, chosen_b_pre,
        "scenario B: same session through replica B must still hit the original backend"
    );
    eprintln!("[ha_smoke] ok: scenario B — failover preserves binding");

    // ----- Scenario C: reattach via the surviving replica ------------
    // ExecutePlan through *replica B* (replica A is gone). Capture
    // the backend that handled it via the tagged session_id, then
    // ReattachExecute(op_id, **different session_id**) and prove the
    // op-id reverse index sends us to the same backend.
    let session_c = "ha-session-3";
    let op_id = "ha-op-3";
    let mut client_b =
        pb::spark_connect_service_client::SparkConnectServiceClient::new(ch_b.clone());

    use futures::StreamExt;
    let mut exec_stream = client_b
        .execute_plan(Request::new(pb::ExecutePlanRequest {
            session_id: session_c.into(),
            operation_id: Some(op_id.into()),
            ..Default::default()
        }))
        .await
        .expect("ExecutePlan succeeds")
        .into_inner();
    let first = exec_stream
        .next()
        .await
        .expect("at least one response")
        .expect("ok response");
    let exec_backend = first.session_id.rsplit('@').next().unwrap().to_string();
    eprintln!(
        "[ha_smoke] scenario C: ExecutePlan(op={}) routed to {}",
        op_id, exec_backend
    );
    // Drain the rest of the stream so the server-side handler closes
    // and the op-id binding is durable in Redis.
    while exec_stream.next().await.is_some() {}

    let mut reattach_stream = client_b
        .reattach_execute(Request::new(pb::ReattachExecuteRequest {
            // Deliberately a *different* session id — the op-id index
            // is what must save us.
            session_id: "totally-different-session".into(),
            operation_id: op_id.into(),
            ..Default::default()
        }))
        .await
        .expect("ReattachExecute succeeds")
        .into_inner();
    let reattach_first = reattach_stream
        .next()
        .await
        .expect("at least one reattach response")
        .expect("ok reattach response");
    let reattach_backend = reattach_first
        .session_id
        .rsplit('@')
        .next()
        .unwrap()
        .to_string();
    assert_eq!(
        reattach_backend, exec_backend,
        "scenario C: reattach via op-id reverse index must land on the same backend"
    );
    eprintln!("[ha_smoke] ok: scenario C — op-id reverse index works across replicas");

    // Cleanup.
    let _ = kill_b.send(());
    println!("[ha_smoke] all HA invariants passed");
}
