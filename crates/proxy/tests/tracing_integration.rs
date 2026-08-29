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

//! Integration test: gateway emits OTel spans, propagates W3C
//! traceparent to backend, and adopts an inbound traceparent as the
//! parent of its own span.
//!
//! Strategy: install a single global tracing subscriber feeding an
//! `InMemorySpanExporter`. Each scenario clears the buffer and
//! inspects the spans produced by exactly one RPC.
//!
//! All scenarios live in one `#[tokio::test]` because `tracing` and
//! `opentelemetry::global` are process-wide; multiple tests in the
//! same binary would race.
//!
//! ## Why this test is `#[ignore]` by default
//!
//! Cargo unifies feature flags across the workspace's dependency
//! graph, which means `cargo test --workspace` can pull additional
//! features into `opentelemetry` (e.g. `spec_unstable_logs_enabled`,
//! activated transitively when `kube` is in scope) that don't show
//! up under `cargo test -p scg-proxy --test tracing_integration`.
//! The resulting binary links a slightly different
//! `tracing-opentelemetry`, and on this combination the layer
//! silently drops application spans — only h2 internal spans reach
//! the in-memory exporter. The production gateway is unaffected
//! (it always builds the same way), but the test must opt out of
//! workspace-wide runs to stay reliable. Run it explicitly with:
//!
//! ```bash
//! cargo test -p scg-proxy --test tracing_integration -- --ignored
//! ```

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SimpleSpanProcessor};
use scg_genproto::pb;
use scg_observability::{install_test_subscriber, Metrics, TRACEPARENT_HEADER};
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_routing::{AffinityStore, Pool, Router};
use scg_store_memory::MemoryStore;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

/// Backend that captures the inbound metadata so the test can assert
/// that the gateway forwarded a `traceparent` header.
#[derive(Clone, Default)]
struct CapturingBackend {
    captured: Arc<parking_lot::Mutex<Option<tonic::metadata::MetadataMap>>>,
}

#[tonic::async_trait]
impl pb::spark_connect_service_server::SparkConnectService for CapturingBackend {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = Result<pb::ExecutePlanResponse, Status>> + Send + 'static>>;
    type ReattachExecuteStream = Self::ExecutePlanStream;

    async fn config(
        &self,
        req: Request<pb::ConfigRequest>,
    ) -> Result<Response<pb::ConfigResponse>, Status> {
        *self.captured.lock() = Some(req.metadata().clone());
        let body = req.into_inner();
        Ok(Response::new(pb::ConfigResponse {
            session_id: body.session_id,
            ..Default::default()
        }))
    }

    // The remaining methods aren't exercised by these tests but the
    // trait requires them.
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

/// Locate the gateway span produced for `rpc_method`. We tag handlers
/// with a `rpc.method` attribute carrying the unprefixed RPC name.
fn find_rpc_span<'a>(
    spans: &'a [opentelemetry_sdk::trace::SpanData],
    rpc_method: &str,
) -> Option<&'a opentelemetry_sdk::trace::SpanData> {
    spans.iter().find(|s| {
        s.attributes.iter().any(|kv| {
            kv.key.as_str() == "rpc_method"
                && kv.value.as_str() == std::borrow::Cow::Borrowed(rpc_method)
        })
    })
}

/// Block until a span tagged `rpc.method=<rpc>` shows up in the
/// exporter, or `deadline` elapses. The handler-side span closes
/// shortly after tonic emits the response trailer, so the client's
/// `.await` completing isn't a synchronization point. Returns the
/// snapshot the assertion path will inspect.
async fn wait_for_span(
    exporter: &InMemorySpanExporter,
    rpc_method: &str,
    deadline: Duration,
) -> Vec<opentelemetry_sdk::trace::SpanData> {
    let start = std::time::Instant::now();
    loop {
        let spans = exporter.get_finished_spans().expect("spans accessible");
        if find_rpc_span(&spans, rpc_method).is_some() {
            return spans;
        }
        if start.elapsed() >= deadline {
            return spans;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

struct Rig {
    grpc: Channel,
    captured: Arc<parking_lot::Mutex<Option<tonic::metadata::MetadataMap>>>,
    _be_shutdown: tokio::sync::oneshot::Sender<()>,
    _gw_shutdown: tokio::sync::oneshot::Sender<()>,
}

async fn rig() -> Rig {
    let backend = CapturingBackend::default();
    let captured = backend.captured.clone();
    let svc = pb::spark_connect_service_server::SparkConnectServiceServer::new(backend);
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

    let metrics = Metrics::new().unwrap();
    let pool: Arc<dyn Pool> = Arc::new(StaticPool::new(vec![be_addr]).unwrap());
    let store: Arc<dyn AffinityStore> = Arc::new(MemoryStore::new());
    let router = Arc::new(Router::single_pool(pool, store));
    let dialer = Dialer::new();
    // Auth disabled — these tests focus on tracing propagation, not
    // identity. The proxy still authenticates (Anonymous) and stamps
    // a UserContext.
    let proxy = SparkConnectProxy::with_auth_and_metrics(
        router,
        dialer,
        scg_auth::AuthInterceptor::new(Arc::new(scg_auth::AnonymousAuthenticator)),
        metrics,
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

    Rig {
        grpc,
        captured,
        _be_shutdown: be_tx,
        _gw_shutdown: gw_tx,
    }
}

#[tokio::test]
#[ignore = "feature-skewed across workspace builds; run with -- --ignored under -p scg-proxy"]
async fn tracing_emits_spans_and_propagates_traceparent() {
    // Install one shared in-memory exporter for the whole test
    // binary. SimpleSpanProcessor flushes synchronously on export, so
    // we don't need to wait for batches.
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_span_processor(SimpleSpanProcessor::new(exporter.clone()))
        .build();
    install_test_subscriber(&provider);

    let rig = rig().await;
    let mut client =
        pb::spark_connect_service_client::SparkConnectServiceClient::new(rig.grpc.clone());

    // ---- Scenario 1: no inbound traceparent → fresh root span -----
    exporter.reset();
    *rig.captured.lock() = None;

    client
        .config(Request::new(pb::ConfigRequest {
            session_id: "s-no-parent".into(),
            ..Default::default()
        }))
        .await
        .expect("Config RPC succeeds");

    // Server-side span closes when the handler future drops. tonic
    // drops it slightly after the response trailer is sent, so the
    // client `.await` returning doesn't guarantee the span is in the
    // exporter yet — give the runtime a beat.
    let spans = wait_for_span(&exporter, "Config", Duration::from_secs(2)).await;
    let config_span =
        find_rpc_span(&spans, "Config").expect("a span tagged rpc_method=Config was emitted");
    // No parent → SpanId of the parent is zero / unset.
    assert!(
        !config_span
            .parent_span_id
            .to_string()
            .chars()
            .any(|c| c != '0'),
        "expected root span (no parent), got parent {:?}",
        config_span.parent_span_id
    );

    let captured = rig.captured.lock().clone().expect("backend saw the call");
    let tp = captured
        .get(TRACEPARENT_HEADER)
        .expect("gateway forwarded a traceparent")
        .to_str()
        .unwrap();
    // Format: 00-<traceid>-<spanid>-<flags>
    let parts: Vec<&str> = tp.split('-').collect();
    assert_eq!(parts.len(), 4, "malformed traceparent: {tp}");
    let outbound_trace_id = parts[1];
    let outbound_span_id = parts[2];
    // The outbound traceparent's trace id matches the gateway span's trace id.
    assert_eq!(
        outbound_trace_id,
        config_span.span_context.trace_id().to_string()
    );
    // The outbound parent span (what the backend treats as its parent) is
    // the gateway's own span — the gateway span's id, not its parent.
    assert_eq!(
        outbound_span_id,
        config_span.span_context.span_id().to_string()
    );

    // ---- Scenario 2: inbound traceparent → adopted as parent ------
    exporter.reset();
    *rig.captured.lock() = None;

    let inbound_trace = "0af7651916cd43dd8448eb211c80319c";
    let inbound_parent = "b7ad6b7169203331";
    let inbound_tp = format!("00-{}-{}-01", inbound_trace, inbound_parent);
    let mut req = Request::new(pb::ConfigRequest {
        session_id: "s-with-parent".into(),
        ..Default::default()
    });
    req.metadata_mut().insert(
        TRACEPARENT_HEADER,
        MetadataValue::try_from(inbound_tp.clone()).unwrap(),
    );
    client.config(req).await.expect("Config RPC succeeds");

    let spans = wait_for_span(&exporter, "Config", Duration::from_secs(2)).await;
    let gw_span =
        find_rpc_span(&spans, "Config").expect("Config span emitted under propagated parent");
    // The gateway span's trace id must equal the inbound trace id.
    assert_eq!(gw_span.span_context.trace_id().to_string(), inbound_trace);
    // The gateway span's parent must be the inbound parent span id.
    assert_eq!(gw_span.parent_span_id.to_string(), inbound_parent);

    // The forwarded traceparent must keep the same trace id (so the
    // backend joins the same distributed trace) but carry the
    // gateway's own span id as the new parent.
    let captured = rig.captured.lock().clone().expect("backend saw the call");
    let tp = captured
        .get(TRACEPARENT_HEADER)
        .expect("gateway forwarded a traceparent")
        .to_str()
        .unwrap();
    let parts: Vec<&str> = tp.split('-').collect();
    assert_eq!(parts[1], inbound_trace, "trace id changed across hop");
    assert_eq!(
        parts[2],
        gw_span.span_context.span_id().to_string(),
        "outbound parent should be the gateway span id"
    );

    // Cleanup so the test process exits cleanly.
    let _ = provider.shutdown();
}
