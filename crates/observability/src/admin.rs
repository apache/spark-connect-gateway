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

//! Admin HTTP server: `/metrics`, `/healthz`, `/readyz`.
//!
//! Bound on a separate `admin_addr` from the gRPC server so that:
//!
//! * scrape traffic doesn't compete with hot-path gRPC for the
//!   listener's accept queue,
//! * the gRPC port can be exposed externally while `/metrics` stays
//!   internal-only,
//! * a Kubernetes Service can target `:9090` for liveness / readiness
//!   probes without leaking gRPC.
//!
//! Implementation uses a minimal Hyper 1.x server. We don't pull in
//! `axum` or `warp` because the routing surface is three endpoints.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use prometheus::Encoder;
use tokio::net::TcpListener;
use tracing::{debug, info, warn};

use crate::metrics::Metrics;

/// Configuration for the admin server.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// Address to bind, e.g. `0.0.0.0:9090`.
    pub bind_addr: SocketAddr,
}

/// Read-only readiness signal driven by the gateway. Static-pool
/// deployments mark themselves ready immediately; dynamic-pool
/// deployments mark themselves ready once the watcher emits its first
/// healthy backend list.
#[derive(Clone)]
pub struct ReadinessProbe {
    inner: Arc<std::sync::atomic::AtomicBool>,
}

impl ReadinessProbe {
    pub fn new(initial: bool) -> Self {
        Self {
            inner: Arc::new(std::sync::atomic::AtomicBool::new(initial)),
        }
    }
    pub fn mark_ready(&self) {
        self.inner.store(true, std::sync::atomic::Ordering::Release);
    }
    /// Flip readiness back to false. Used during drain on SIGTERM so
    /// `/readyz` returns 503 and Kubernetes removes this pod from the
    /// Service endpoints before any in-flight RPCs are torn down.
    pub fn mark_not_ready(&self) {
        self.inner
            .store(false, std::sync::atomic::Ordering::Release);
    }
    pub fn is_ready(&self) -> bool {
        self.inner.load(std::sync::atomic::Ordering::Acquire)
    }
}

impl Default for ReadinessProbe {
    fn default() -> Self {
        Self::new(false)
    }
}

/// Run the admin server until `shutdown` resolves. Returns when the
/// listener has stopped accepting new connections.
///
/// Each accepted connection is handled on a fresh tokio task.
pub async fn serve_admin<F>(
    cfg: AdminConfig,
    metrics: Metrics,
    readiness: ReadinessProbe,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind(cfg.bind_addr).await?;
    info!(addr = %cfg.bind_addr, "admin server listening");

    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("admin server shutting down");
                return Ok(());
            }
            accept = listener.accept() => {
                let (stream, peer) = match accept {
                    Ok(x) => x,
                    Err(e) => {
                        warn!(error = %e, "admin accept error");
                        continue;
                    }
                };
                debug!(%peer, "admin: connection accepted");
                let metrics = metrics.clone();
                let readiness = readiness.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req| {
                        let metrics = metrics.clone();
                        let readiness = readiness.clone();
                        async move { Ok::<_, Infallible>(handle(req, &metrics, &readiness).await) }
                    });
                    if let Err(e) = http1::Builder::new().serve_connection(io, svc).await {
                        debug!(error = %e, "admin connection ended");
                    }
                });
            }
        }
    }
}

async fn handle(
    req: Request<Incoming>,
    metrics: &Metrics,
    readiness: &ReadinessProbe,
) -> Response<Full<Bytes>> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => render_metrics(metrics),
        (&Method::GET, "/healthz") => simple(StatusCode::OK, "ok"),
        (&Method::GET, "/readyz") => {
            if readiness.is_ready() {
                simple(StatusCode::OK, "ready")
            } else {
                simple(StatusCode::SERVICE_UNAVAILABLE, "not ready")
            }
        }
        _ => simple(StatusCode::NOT_FOUND, "not found"),
    }
}

fn render_metrics(metrics: &Metrics) -> Response<Full<Bytes>> {
    let encoder = prometheus::TextEncoder::new();
    let mfs = metrics.registry().gather();
    let mut buf = Vec::new();
    if let Err(e) = encoder.encode(&mfs, &mut buf) {
        warn!(error = %e, "failed to encode metrics");
        return simple(StatusCode::INTERNAL_SERVER_ERROR, "encoding failed");
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", encoder.format_type())
        .body(Full::new(Bytes::from(buf)))
        .expect("static response builder")
}

fn simple(status: StatusCode, body: &'static str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(body)))
        .expect("static response builder")
}
