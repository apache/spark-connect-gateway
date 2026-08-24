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

//! Metrics, structured spans, and the admin HTTP server.
//!
//! The crate exposes three things:
//!
//! * [`Metrics`] — a `Clone`-able handle owning a Prometheus `Registry`
//!   and the per-RPC counters / histograms. Hand one to anything that
//!   needs to instrument requests.
//! * [`request_id`] — generates a per-RPC UUID v4 used as the
//!   correlation ID stamped into both the `tracing::Span` and the
//!   outbound gRPC `x-request-id` metadata.
//! * [`admin`] — a small Hyper-based HTTP server that serves
//!   `/metrics`, `/healthz`, `/readyz` on a separate admin port.

pub mod admin;
pub mod metrics;
pub mod tracing;

pub use admin::{serve_admin, AdminConfig, ReadinessProbe};
pub use metrics::{Metrics, MetricsError, RpcGuard, StreamGuard};
#[cfg(feature = "testing")]
pub use tracing::install_test_subscriber;
pub use tracing::{
    extract_parent, init_tracing, inject_context, TracingConfig, TracingError, TracingHandle,
    TRACEPARENT_HEADER, TRACER_NAME,
};

use uuid::Uuid;

/// Generate a fresh correlation ID for the current RPC.
pub fn request_id() -> String {
    Uuid::new_v4().to_string()
}

/// gRPC metadata key under which the gateway forwards correlation IDs
/// to backends. Public so the proxy crate can use the same constant.
pub const REQUEST_ID_HEADER: &str = "x-request-id";
