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

//! Distributed tracing — OpenTelemetry exporter + W3C traceparent
//! propagation across the gateway → backend hop.
//!
//! Two responsibilities:
//!
//! * `init_tracing(cfg)` — install the global `tracing_subscriber` with a
//!   `tracing-opentelemetry` layer that forwards spans to an OTLP/gRPC
//!   exporter. Returns a [`TracingHandle`] whose `shutdown()` flushes
//!   spans before the process exits.
//! * `extract_parent` / `inject_traceparent` — translate W3C traceparent
//!   metadata between tonic `MetadataMap`s and OpenTelemetry `Context`s,
//!   so a request's parent trace from the inbound side becomes the
//!   parent of every outbound RPC the gateway makes.
//!
//! Tracing is disabled by default; passing a `TracingConfig` with
//! `endpoint == None` returns a no-op handle and installs only the
//! existing JSON formatter (so logs keep working unchanged).

use std::time::Duration;

use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use opentelemetry_semantic_conventions::resource as semres;
use tonic::metadata::MetadataMap;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Tracer name reported on every span emitted by the gateway. The
/// service name (an OTel `Resource` attribute) is set separately via
/// `TracingConfig::service_name`.
pub const TRACER_NAME: &str = "scg.gateway";

/// W3C trace-context header. Lower-case because gRPC metadata keys are
/// case-insensitive and stored lower-cased.
pub const TRACEPARENT_HEADER: &str = "traceparent";
pub const TRACESTATE_HEADER: &str = "tracestate";

/// Runtime tracing configuration. Constructed by the gateway from its
/// YAML config, or by tests directly.
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Logical service name reported as `service.name` on every span.
    pub service_name: String,
    /// Service version reported as `service.version`.
    pub service_version: String,
    /// OTLP/gRPC collector endpoint, e.g. `http://otel-collector:4317`.
    /// `None` disables span export — only the JSON log formatter is
    /// installed.
    pub endpoint: Option<String>,
    /// `TraceIdRatioBased` ratio in `[0.0, 1.0]`. Wrapped in
    /// `ParentBased` so a sampled parent always wins.
    pub sample_ratio: f64,
    /// Per-batch export deadline for the OTLP exporter.
    pub export_timeout: Duration,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            service_name: "spark-connect-gateway".into(),
            service_version: env!("CARGO_PKG_VERSION").into(),
            endpoint: None,
            sample_ratio: 1.0,
            export_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TracingError {
    #[error("building OTLP exporter: {0}")]
    Exporter(#[from] opentelemetry_sdk::error::OTelSdkError),
    #[error("setting tracing subscriber: {0}")]
    SetSubscriber(#[from] tracing_subscriber::util::TryInitError),
    #[error("OTLP exporter init: {0}")]
    OtlpInit(String),
}

/// RAII handle returned by [`init_tracing`]. When the gateway shuts
/// down, call [`TracingHandle::shutdown`] to flush in-flight spans
/// before the process exits.
pub struct TracingHandle {
    provider: Option<SdkTracerProvider>,
}

impl TracingHandle {
    /// A handle that does nothing on shutdown. Used for the
    /// `endpoint == None` case and in tests where the caller installs
    /// its own provider.
    pub fn noop() -> Self {
        Self { provider: None }
    }

    /// Force-flush and shut down the OTLP exporter. Safe to call
    /// multiple times; subsequent calls are no-ops.
    pub fn shutdown(&mut self) {
        if let Some(p) = self.provider.take() {
            // `shutdown` returns Result; we log on error but never
            // propagate — at this point the process is exiting.
            if let Err(e) = p.shutdown() {
                tracing::warn!(error = %e, "OTel tracer provider shutdown failed");
            }
        }
    }
}

impl Drop for TracingHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Install the global tracing subscriber:
///
/// * Always: a `tracing_subscriber` JSON-formatter layer driven by
///   `RUST_LOG` (defaults to `info`).
/// * If `cfg.endpoint.is_some()`: also a `tracing-opentelemetry` layer
///   that exports spans to the configured OTLP/gRPC endpoint, plus a
///   global `TraceContextPropagator` so W3C traceparent extraction /
///   injection works.
///
/// Returns a [`TracingHandle`] whose `shutdown` must be called before
/// the process exits, otherwise the final batch of spans may be lost.
pub fn init_tracing(cfg: TracingConfig) -> Result<TracingHandle, TracingError> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer().json().with_target(false);

    let Some(endpoint) = cfg.endpoint.clone() else {
        // Logs only — keep the existing JSON formatter behaviour.
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .try_init()?;
        return Ok(TracingHandle::noop());
    };

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .with_timeout(cfg.export_timeout)
        .build()
        .map_err(|e| TracingError::OtlpInit(e.to_string()))?;

    let resource = Resource::builder()
        .with_attribute(KeyValue::new(
            semres::SERVICE_NAME,
            cfg.service_name.clone(),
        ))
        .with_attribute(KeyValue::new(
            semres::SERVICE_VERSION,
            cfg.service_version.clone(),
        ))
        .build();

    // Wrap the configured ratio in ParentBased so a remote sampled
    // decision always wins; fresh local roots fall through to the
    // ratio sampler.
    //
    // KNOWN LIMITATION: when the inbound request carries a W3C
    // `traceparent` header, the OTel SDK's interaction with
    // `tracing-opentelemetry::set_parent` does not propagate the
    // inbound trace_id onto the gateway's span (the parent_cx is set
    // via `Context::with_remote_span_context`, which is invisible to
    // `Context::has_active_span()` and to several internal codepaths
    // in `tracer::build_with_context`). The resulting gateway span
    // is dropped before reaching the OTLP exporter — fmt-side logs
    // still record it (so JSON logs + correlation IDs work), but the
    // distributed trace cannot link gateway → backend hops on those
    // RPCs. Spans for RPCs without an inbound traceparent (i.e. the
    // gateway is the trace root) export normally. Tracking upstream
    // for resolution; switch back to a richer sampler when fixed.
    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
        cfg.sample_ratio.clamp(0.0, 1.0),
    )));

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .with_sampler(sampler)
        .build();

    // Install the W3C traceparent propagator globally so extract /
    // inject helpers below pick it up without re-creating an instance.
    global::set_text_map_propagator(TraceContextPropagator::new());

    let tracer = provider.tracer(TRACER_NAME);
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_layer)
        .try_init()?;

    Ok(TracingHandle {
        provider: Some(provider),
    })
}

/// Tonic [`MetadataMap`] adapter for OpenTelemetry's `Extractor` /
/// `Injector` traits. Both use lower-case keys.
struct MetadataCarrier<'a>(&'a MetadataMap);

impl<'a> Extractor for MetadataCarrier<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|k| match k {
                tonic::metadata::KeyRef::Ascii(k) => k.as_str(),
                tonic::metadata::KeyRef::Binary(k) => k.as_str(),
            })
            .collect()
    }
}

struct MetadataInjector<'a>(&'a mut MetadataMap);

impl<'a> Injector for MetadataInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(k), Ok(v)) = (
            tonic::metadata::MetadataKey::<tonic::metadata::Ascii>::from_bytes(key.as_bytes()),
            tonic::metadata::MetadataValue::try_from(value),
        ) {
            self.0.insert(k, v);
        }
    }
}

/// Extract a W3C trace-context parent from inbound gRPC metadata.
/// Returns the empty `Context` when no traceparent is present (the
/// resulting span will be a fresh trace root).
pub fn extract_parent(metadata: &MetadataMap) -> Context {
    let propagator = TraceContextPropagator::new();
    propagator.extract(&MetadataCarrier(metadata))
}

/// Inject the OpenTelemetry context held by `cx` into outbound gRPC
/// metadata as a W3C `traceparent` (and `tracestate` when present).
pub fn inject_context(cx: &Context, metadata: &mut MetadataMap) {
    let propagator = TraceContextPropagator::new();
    propagator.inject_context(cx, &mut MetadataInjector(metadata));
}

/// Test-only helper: install a global `tracing` subscriber that
/// forwards spans to the supplied [`SdkTracerProvider`], plus the W3C
/// trace-context propagator so [`extract_parent`] / [`inject_context`]
/// work. Returns silently if a global subscriber is already installed
/// (every test in a binary shares the same one).
///
/// Pair with `opentelemetry_sdk::trace::InMemorySpanExporter` for
/// round-trip assertions; see `crates/proxy/tests/tracing_integration.rs`.
#[cfg(feature = "testing")]
pub fn install_test_subscriber(provider: &opentelemetry_sdk::trace::SdkTracerProvider) {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        global::set_text_map_propagator(TraceContextPropagator::new());
        let tracer = provider.tracer(TRACER_NAME);
        let layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let _ = tracing_subscriber::registry().with(layer).try_init();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TraceContextExt;

    #[test]
    fn extract_with_no_traceparent_yields_invalid_span_context() {
        let md = MetadataMap::new();
        let cx = extract_parent(&md);
        assert!(!cx.span().span_context().is_valid());
    }

    #[test]
    fn inject_then_extract_round_trips_traceparent() {
        // Build a Context whose active span has a known SpanContext.
        use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState};
        let span_ctx = SpanContext::new(
            TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap(),
            SpanId::from_hex("b7ad6b7169203331").unwrap(),
            TraceFlags::SAMPLED,
            true, // remote
            TraceState::default(),
        );
        let cx = Context::new().with_remote_span_context(span_ctx.clone());

        let mut md = MetadataMap::new();
        inject_context(&cx, &mut md);

        let tp = md
            .get(TRACEPARENT_HEADER)
            .expect("traceparent injected")
            .to_str()
            .unwrap();
        // W3C format: 00-<traceid>-<spanid>-<flags>
        assert!(tp.starts_with("00-0af7651916cd43dd8448eb211c80319c-"));

        let extracted = extract_parent(&md);
        let extracted_span = extracted.span();
        let extracted_sc = extracted_span.span_context();
        assert!(extracted_sc.is_valid());
        assert_eq!(extracted_sc.trace_id(), span_ctx.trace_id());
        assert_eq!(extracted_sc.span_id(), span_ctx.span_id());
    }

    #[test]
    fn noop_handle_shutdown_is_idempotent() {
        let mut h = TracingHandle::noop();
        h.shutdown();
        h.shutdown(); // must not panic
    }
}
