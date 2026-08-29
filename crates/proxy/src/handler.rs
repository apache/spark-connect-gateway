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

//! `SparkConnectService` implementation that forwards every RPC to a
//! backend chosen by the [`Router`].

use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use opentelemetry::trace::TraceContextExt;
use scg_audit::AuditLogger;
use scg_auth::{AnonymousAuthenticator, AuthInterceptor, Identity};
use scg_genproto::pb;
use scg_observability::{
    extract_parent, inject_context, request_id, Metrics, RpcGuard, REQUEST_ID_HEADER,
};
use scg_ratelimit::RateLimiter;
use scg_routing::{Router, SessionKey};
use scg_tenant::TenantResolver;
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::config_filter::ConfigGuard;
use crate::dial::Dialer;
use crate::outbound::BackendTokens;

/// gRPC handler implementing `SparkConnectService` as a forwarding proxy.
///
/// On every RPC the proxy:
///
/// 1. Starts a per-RPC [`RpcGuard`] that records duration + the final
///    gRPC code on Drop.
/// 2. Generates a correlation ID; stamps it on the tracing span and on
///    the outbound `x-request-id` metadata.
/// 3. Authenticates the inbound request (via the configured
///    [`AuthInterceptor`]; defaults to `Anonymous` when auth is
///    disabled in config).
/// 4. Overwrites the request's `UserContext.user_id` with the verified
///    identity — never trusting the value the client supplied.
/// 5. Resolves a backend through the [`Router`].
/// 6. Forwards the (rewritten) request and pumps the response back.
pub struct SparkConnectProxy {
    router: Arc<Router>,
    dialer: Arc<Dialer>,
    auth: AuthInterceptor,
    /// Metrics handle. Required (we always want metrics): pass a
    /// freshly built `Metrics::new()?` in tests where you don't care
    /// about scraping.
    metrics: Metrics,
    /// Resolves the tenant for every RPC after auth. The routing key
    /// is `(tenant, user_id, session_id)`; without a resolver every
    /// session ends up in `tenant="default"`.
    tenant_resolver: TenantResolver,
    /// Per-tenant rate limiter. `None` skips the limiter check
    /// entirely; a disabled limiter (`Some` with no buckets
    /// enabled) is also free via [`RateLimiter::is_active`].
    rate_limiter: Option<RateLimiter>,
    /// Audit logger. Always present (defaults to disabled) so
    /// handlers can call into it unconditionally without
    /// `Option::map` ceremony.
    audit: AuditLogger,
    /// Per-pool `authorization: Bearer …` credentials presented on
    /// the gateway→backend hop, for backends that enforce
    /// `spark.connect.authenticate.token`. Defaults to none.
    backend_tokens: BackendTokens,
}

impl SparkConnectProxy {
    /// Build a proxy with auth disabled, a fresh throwaway
    /// `Metrics`, and the default (back-compat) tenant resolver
    /// (every RPC ends up in `tenant="default"`). Used by tests and
    /// minimal single-tenant deployments.
    ///
    /// Production deployments use [`SparkConnectProxy::builder`] (or
    /// the older [`with_auth_and_metrics`]) and hand in a `Metrics`
    /// shared with the admin server.
    pub fn new(router: Arc<Router>, dialer: Arc<Dialer>) -> Self {
        let metrics = Metrics::new().expect("Metrics::new() in test scaffolding");
        Self::with_auth_and_metrics(
            router,
            dialer,
            AuthInterceptor::new(Arc::new(AnonymousAuthenticator)),
            metrics,
        )
    }

    /// Build a proxy with the given auth interceptor and a fresh
    /// `Metrics`. Convenient for auth-only tests.
    pub fn with_auth(router: Arc<Router>, dialer: Arc<Dialer>, auth: AuthInterceptor) -> Self {
        let metrics = Metrics::new().expect("Metrics::new() in test scaffolding");
        Self::with_auth_and_metrics(router, dialer, auth, metrics)
    }

    /// Convenience constructor: auth + metrics, default tenant
    /// resolver, no rate limiter. Equivalent to calling
    /// [`SparkConnectProxy::with_components`] with
    /// `TenantResolver::new(TenantResolverConfig::default())` and
    /// `rate_limiter = None`.
    pub fn with_auth_and_metrics(
        router: Arc<Router>,
        dialer: Arc<Dialer>,
        auth: AuthInterceptor,
        metrics: Metrics,
    ) -> Self {
        Self::with_components(
            router,
            dialer,
            auth,
            metrics,
            TenantResolver::new(scg_tenant::TenantResolverConfig::default()),
        )
    }

    /// Production constructor with tenant resolver but no rate
    /// limiter and a disabled audit logger. Most existing tests
    /// and examples use this.
    pub fn with_components(
        router: Arc<Router>,
        dialer: Arc<Dialer>,
        auth: AuthInterceptor,
        metrics: Metrics,
        tenant_resolver: TenantResolver,
    ) -> Self {
        Self::with_all(
            router,
            dialer,
            auth,
            metrics,
            tenant_resolver,
            None,
            AuditLogger::disabled(),
        )
    }

    /// Full constructor: every component. Used by
    /// `crates/gateway/main` once the operator's config is loaded.
    pub fn with_all(
        router: Arc<Router>,
        dialer: Arc<Dialer>,
        auth: AuthInterceptor,
        metrics: Metrics,
        tenant_resolver: TenantResolver,
        rate_limiter: Option<RateLimiter>,
        audit: AuditLogger,
    ) -> Self {
        Self {
            router,
            dialer,
            auth,
            metrics,
            tenant_resolver,
            rate_limiter,
            audit,
            backend_tokens: BackendTokens::none(),
        }
    }

    /// Attach per-pool backend credentials (chainable). Without this
    /// the gateway sends no `authorization` header to backends.
    pub fn with_backend_tokens(mut self, tokens: BackendTokens) -> Self {
        self.backend_tokens = tokens;
        self
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Stamp everything an outbound request needs: correlation ID,
    /// W3C traceparent, and — when configured — the backend pool's
    /// `authorization: Bearer …` credential for `tenant`.
    fn stamp_outbound<T>(&self, req: &mut Request<T>, request_id: &str, tenant: &str) {
        stamp_propagation(req, request_id);
        self.backend_tokens.apply(tenant, req);
    }

    fn client(
        &self,
        addr: &str,
    ) -> Result<
        pb::spark_connect_service_client::SparkConnectServiceClient<tonic::transport::Channel>,
        Status,
    > {
        let ch = self
            .dialer
            .channel(addr)
            .map_err(|e| Status::unavailable(format!("dial backend {}: {}", addr, e)))?;
        Ok(pb::spark_connect_service_client::SparkConnectServiceClient::new(ch))
    }

    /// Authenticate an inbound RPC. Returns the verified `Identity`
    /// or the `Status::unauthenticated` the proxy should hand back
    /// to the client. Auth failures bump `scg_auth_failures_total`
    /// and emit an `auth.failure` audit event.
    async fn authenticate(
        &self,
        metadata: &MetadataMap,
        rid: &str,
        rpc: &'static str,
    ) -> Result<Arc<Identity>, Status> {
        match self.auth.authenticate(metadata).await {
            Ok(ext) => Ok(ext.0),
            Err(status) => {
                let reason = auth_failure_reason(&status);
                self.metrics.record_auth_failure(reason);
                self.audit.auth_failure(rid, rpc, reason);
                Err(status)
            }
        }
    }

    /// Authenticate and resolve the tenant for an inbound RPC. Used
    /// by every RPC handler — the tenant becomes the first segment
    /// of the routing key. A tenant-resolver `Reject` policy on a
    /// missing tenant surfaces as `Status::unauthenticated`.
    ///
    /// When a rate limiter is configured, this also takes a token
    /// from the (tenant, user) bucket pair. Quota violations bubble
    /// up as `Status::resource_exhausted` to the client and bump
    /// `scg_rate_limit_rejected_total{tenant, scope}`.
    ///
    /// `rid` and `rpc` are threaded through so auth-failure audit
    /// events carry the correlation ID and the RPC name. Tenant
    /// resolution failures (configured-source yielded nothing and
    /// `on_missing = Reject`) are treated as auth failures for
    /// audit / metric purposes — the JWT may have been signed
    /// correctly, but without a tenant the gateway can't route,
    /// and the operational signal is the same as a bad token:
    /// "this caller is not authorised to enter."
    async fn authenticate_and_resolve(
        &self,
        metadata: &MetadataMap,
        rid: &str,
        rpc: &'static str,
    ) -> Result<(Arc<Identity>, String), Status> {
        let identity = self.authenticate(metadata, rid, rpc).await?;
        let tenant = match self.tenant_resolver.resolve(metadata, &identity) {
            Ok(t) => t,
            Err(status) => {
                self.metrics.record_auth_failure("missing_tenant");
                self.audit.auth_failure(rid, rpc, "missing_tenant");
                return Err(status);
            }
        };
        if let Some(limiter) = &self.rate_limiter {
            limiter.check(&tenant, &identity.user_id).await?;
        }
        Ok((identity, tenant))
    }

    /// Resolve a session through the router and, when the binding
    /// is fresh, emit a `session.create` audit event. Callers use
    /// this in place of `router.resolve_session` whenever they
    /// want audit coverage; the empty-session_id path (zero key)
    /// never emits the event because there's nothing to record.
    async fn resolve_session_audited(
        &self,
        key: &SessionKey,
        rid: &str,
        identity: &Identity,
    ) -> Result<Option<String>, Status> {
        let outcome = self.router.resolve_session_detailed(key).await?;
        if let Some(o) = &outcome {
            if o.newly_bound {
                self.audit.session_create(
                    rid,
                    &key.tenant,
                    &identity.user_id,
                    &identity.groups,
                    &key.session_id,
                    &o.addr,
                );
            }
        }
        Ok(outcome.map(|o| o.addr))
    }
}

/// Map an auth Status into a small, fixed-cardinality reason label.
fn auth_failure_reason(status: &Status) -> &'static str {
    let m = status.message();
    if m.contains("missing") {
        "missing_token"
    } else if m.contains("expired") {
        "expired"
    } else if m.contains("kid") {
        "unknown_kid"
    } else if m.contains("invalid") {
        "invalid_token"
    } else {
        "unknown"
    }
}

/// Build the routing key from a verified identity, a resolved
/// tenant, and the request's session id. The identity *replaces*
/// whatever `user_id` the client claimed in `UserContext`; the
/// tenant comes from the [`TenantResolver`] (auth claim, gRPC
/// metadata header, or fixed deployment-wide string).
fn key_from_identity(tenant: &str, session_id: &str, id: &Identity) -> SessionKey {
    SessionKey::with_tenant(tenant, id.user_id.as_str(), session_id)
}

/// Overwrite (or create) `UserContext.user_id` to match the verified
/// identity. Backend Spark Connect servers consume this value as part
/// of their SparkSession key, so trusting the client value would let
/// callers impersonate one another.
fn stamp_user_context(uc: &mut Option<pb::UserContext>, id: &Identity) {
    match uc {
        Some(ctx) => ctx.user_id = id.user_id.clone(),
        None => {
            *uc = Some(pb::UserContext {
                user_id: id.user_id.clone(),
                ..Default::default()
            });
        }
    }
}

/// Convert a possibly-empty backend selection into an `Unavailable` Status.
/// A None is most commonly produced when a dynamic pool (the K8s
/// Endpoints watcher) hasn't seen any healthy endpoint yet, e.g.
/// during gateway boot before the watcher's initial list event.
fn require_addr(addr: Result<Option<String>, Status>) -> Result<String, Status> {
    // Two failure modes:
    // * `Err(Status)` — the tenant has no configured pool and the
    //   policy is `Reject`. Forward the `PermissionDenied` to the
    //   client unchanged.
    // * `Ok(None)` — the tenant's pool has no healthy backend
    //   (K8s discovery during startup, Redis down so we can't
    //   resolve stickiness, etc.). Surface as `Unavailable`.
    addr?.ok_or_else(|| Status::unavailable("no healthy backend available"))
}

/// Map a Status code into the small fixed string used in metric labels.
fn status_code_label(status: &Status) -> &'static str {
    match status.code() {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "Cancelled",
        tonic::Code::Unknown => "Unknown",
        tonic::Code::InvalidArgument => "InvalidArgument",
        tonic::Code::DeadlineExceeded => "DeadlineExceeded",
        tonic::Code::NotFound => "NotFound",
        tonic::Code::AlreadyExists => "AlreadyExists",
        tonic::Code::PermissionDenied => "PermissionDenied",
        tonic::Code::ResourceExhausted => "ResourceExhausted",
        tonic::Code::FailedPrecondition => "FailedPrecondition",
        tonic::Code::Aborted => "Aborted",
        tonic::Code::OutOfRange => "OutOfRange",
        tonic::Code::Unimplemented => "Unimplemented",
        tonic::Code::Internal => "Internal",
        tonic::Code::Unavailable => "Unavailable",
        tonic::Code::DataLoss => "DataLoss",
        tonic::Code::Unauthenticated => "Unauthenticated",
    }
}

/// Inject the gateway's correlation ID onto an outbound request's
/// metadata so backend logs can correlate.
fn stamp_request_id<T>(req: &mut Request<T>, request_id: &str) {
    if let Ok(v) = MetadataValue::try_from(request_id) {
        req.metadata_mut().insert(REQUEST_ID_HEADER, v);
    }
}

/// Build a per-RPC tracing span and parent it to whatever traceparent
/// the inbound metadata carries (no-op when absent — the span becomes a
/// fresh trace root).
///
/// Returns the span; the caller `Instrument`s the handler future with
/// it. Span name follows the OpenTelemetry semantic-conventions form
/// `<service>/<rpc>` so distributed-trace UIs render it nicely.
fn rpc_span(rpc: &'static str, rid: &str, inbound: &MetadataMap) -> Span {
    // Field names use underscores rather than dots: Span attribute
    // names with `.` round-trip through tracing-opentelemetry, but
    // some feature combos in the dependency graph silently drop them
    // before they reach the OTel exporter. Use snake_case here and
    // map to the dotted OTel semantic-conventions name in
    // post-processing if needed.
    let span = tracing::info_span!(
        "scg_rpc",
        rpc_method = rpc,
        rpc_system = "grpc",
        rpc_service = "spark.connect.SparkConnectService",
        scg_rid = %rid,
    );
    let parent_cx = extract_parent(inbound);
    if parent_cx.span().span_context().is_valid() {
        // Failure here just means the OTel layer isn't installed
        // (e.g. tracing is disabled in config) — the span is still
        // useful for the JSON formatter, so we silently ignore.
        let _ = span.set_parent(parent_cx);
    }
    span
}

/// Stamp both the gateway's correlation ID and the current span's
/// W3C traceparent onto an outbound request — backend logs / spans
/// can now join the same trace as the gateway.
fn stamp_propagation<T>(req: &mut Request<T>, request_id: &str) {
    stamp_request_id(req, request_id);
    let cx = Span::current().context();
    inject_context(&cx, req.metadata_mut());
}

/// Update an `RpcGuard` with the final code from a `Status` (or "OK"
/// when the result is Ok).
fn finalise_guard<T>(guard: &mut RpcGuard, result: &Result<T, Status>) {
    match result {
        Ok(_) => guard.record("OK"),
        Err(s) => guard.record(status_code_label(s)),
    }
}

/// Hold the (tenant, user_id) captured once
/// `authenticate_and_resolve` succeeded, so audit emissions on the
/// way out (`rpc.ok`, `rpc.error`) can carry them. `None` means the
/// handler hit a failure *before* identity was known (typically an
/// `Unauthenticated` from auth) — those are already covered by
/// `auth.failure`, so we skip the rpc-level audit event to avoid
/// duplicate noise.
#[derive(Default)]
struct AuditCtx {
    identity: Option<(String, String)>,
}

impl AuditCtx {
    fn set(&mut self, tenant: &str, user_id: &str) {
        self.identity = Some((tenant.to_string(), user_id.to_string()));
    }
}

/// Update the metrics guard *and* emit the matching audit event
/// (`rpc.ok` if `log_successful_rpcs` is on, otherwise just `rpc.error`
/// for non-OK results). The Cancelled code is filtered out: clients
/// dropping streams are noisy and not security-relevant.
fn finalise_rpc<T>(
    guard: &mut RpcGuard,
    result: &Result<T, Status>,
    audit: &AuditLogger,
    ctx: &AuditCtx,
    rid: &str,
    rpc: &'static str,
) {
    finalise_guard(guard, result);
    let Some((tenant, user_id)) = ctx.identity.as_ref() else {
        return;
    };
    match result {
        Ok(_) => audit.rpc_ok(rid, rpc, tenant, user_id),
        Err(s) if s.code() == tonic::Code::Cancelled => {}
        Err(s) => audit.rpc_error(rid, rpc, tenant, user_id, status_code_label(s), s.message()),
    }
}

/// Forward a tonic streaming response to a fresh server-stream sent through
/// `tx`. We don't pipe directly to the inbound `Streaming` because we need
/// to return early on the first error and report it as a Status.
type StreamItem<T> = Result<T, Status>;

#[tonic::async_trait]
impl pb::spark_connect_service_server::SparkConnectService for SparkConnectProxy {
    type ExecutePlanStream =
        Pin<Box<dyn Stream<Item = StreamItem<pb::ExecutePlanResponse>> + Send + 'static>>;
    type ReattachExecuteStream = Self::ExecutePlanStream;

    // ----- Unary RPCs ----------------------------------------------------

    async fn analyze_plan(
        &self,
        req: Request<pb::AnalyzePlanRequest>,
    ) -> Result<Response<pb::AnalyzePlanResponse>, Status> {
        let mut guard = self.metrics.rpc_guard("AnalyzePlan");
        let rid = request_id();
        let span = rpc_span("AnalyzePlan", &rid, req.metadata());
        let mut audit_ctx = AuditCtx::default();
        let result: Result<Response<pb::AnalyzePlanResponse>, Status> = async {
            let (identity, tenant) = self.authenticate_and_resolve(req.metadata(), &rid, "AnalyzePlan").await?;
            audit_ctx.set(&tenant, &identity.user_id);
            let mut body = req.into_inner();
            stamp_user_context(&mut body.user_context, &identity);
            let key = key_from_identity(&tenant, &body.session_id, &identity);
            let addr = require_addr(self.resolve_session_audited(&key, &rid, &identity).await)?;
            info!(rid = %rid, rpc = "AnalyzePlan", user = %identity.user_id, session = %key.session_id, %addr, "forwarding");
            let mut c = self.client(&addr)?;
            let mut outbound = Request::new(body);
            self.stamp_outbound(&mut outbound, &rid, &tenant);
            c.analyze_plan(outbound).await
        }
        .instrument(span)
        .await;
        finalise_rpc(
            &mut guard,
            &result,
            &self.audit,
            &audit_ctx,
            &rid,
            "AnalyzePlan",
        );
        result
    }

    async fn config(
        &self,
        req: Request<pb::ConfigRequest>,
    ) -> Result<Response<pb::ConfigResponse>, Status> {
        let mut guard = self.metrics.rpc_guard("Config");
        let rid = request_id();
        let span = rpc_span("Config", &rid, req.metadata());
        let mut audit_ctx = AuditCtx::default();
        let result: Result<Response<pb::ConfigResponse>, Status> = async {
            let (identity, tenant) = self.authenticate_and_resolve(req.metadata(), &rid, "Config").await?;
            audit_ctx.set(&tenant, &identity.user_id);
            let mut body = req.into_inner();
            stamp_user_context(&mut body.user_context, &identity);
            let key = key_from_identity(&tenant, &body.session_id, &identity);
            let addr = require_addr(self.resolve_session_audited(&key, &rid, &identity).await)?;
            info!(rid = %rid, rpc = "Config", user = %identity.user_id, session = %key.session_id, %addr, "forwarding");
            // Spark's Config handler will hand back any key it holds,
            // including the gateway↔backend pre-shared token. Decide
            // what to withhold before the body is moved.
            let guard = ConfigGuard::for_request(&body);
            let mut c = self.client(&addr)?;
            let mut outbound = Request::new(body);
            self.stamp_outbound(&mut outbound, &rid, &tenant);
            let mut resp = c.config(outbound).await?;
            for withheld in guard.apply(resp.get_mut()) {
                self.audit
                    .config_redacted(&rid, &tenant, &identity.user_id, &withheld);
                warn!(
                    rid = %rid,
                    user = %identity.user_id,
                    tenant = %tenant,
                    key = %withheld,
                    "Config: withheld a backend-only key from the client"
                );
            }
            Ok(resp)
        }
        .instrument(span)
        .await;
        finalise_rpc(&mut guard, &result, &self.audit, &audit_ctx, &rid, "Config");
        result
    }

    async fn artifact_status(
        &self,
        req: Request<pb::ArtifactStatusesRequest>,
    ) -> Result<Response<pb::ArtifactStatusesResponse>, Status> {
        let mut guard = self.metrics.rpc_guard("ArtifactStatus");
        let rid = request_id();
        let span = rpc_span("ArtifactStatus", &rid, req.metadata());
        let mut audit_ctx = AuditCtx::default();
        let result: Result<Response<pb::ArtifactStatusesResponse>, Status> = async {
            let (identity, tenant) = self
                .authenticate_and_resolve(req.metadata(), &rid, "ArtifactStatus")
                .await?;
            audit_ctx.set(&tenant, &identity.user_id);
            let mut body = req.into_inner();
            stamp_user_context(&mut body.user_context, &identity);
            let key = key_from_identity(&tenant, &body.session_id, &identity);
            let addr = require_addr(self.resolve_session_audited(&key, &rid, &identity).await)?;
            let mut c = self.client(&addr)?;
            let mut outbound = Request::new(body);
            self.stamp_outbound(&mut outbound, &rid, &tenant);
            c.artifact_status(outbound).await
        }
        .instrument(span)
        .await;
        finalise_rpc(
            &mut guard,
            &result,
            &self.audit,
            &audit_ctx,
            &rid,
            "ArtifactStatus",
        );
        result
    }

    async fn interrupt(
        &self,
        req: Request<pb::InterruptRequest>,
    ) -> Result<Response<pb::InterruptResponse>, Status> {
        let mut guard = self.metrics.rpc_guard("Interrupt");
        let rid = request_id();
        let span = rpc_span("Interrupt", &rid, req.metadata());
        let mut audit_ctx = AuditCtx::default();
        let result: Result<Response<pb::InterruptResponse>, Status> = async {
            let (identity, tenant) = self
                .authenticate_and_resolve(req.metadata(), &rid, "Interrupt")
                .await?;
            audit_ctx.set(&tenant, &identity.user_id);
            let mut body = req.into_inner();
            stamp_user_context(&mut body.user_context, &identity);
            let key = key_from_identity(&tenant, &body.session_id, &identity);
            // Interrupt may target a specific operation id (one of several
            // InterruptType variants); when present, route by op id.
            let op_id = match body.interrupt.as_ref() {
                Some(pb::interrupt_request::Interrupt::OperationId(id)) => id.clone(),
                _ => String::new(),
            };
            let addr = require_addr(self.router.resolve_op(&op_id, &key).await)?;
            let mut c = self.client(&addr)?;
            let mut outbound = Request::new(body);
            self.stamp_outbound(&mut outbound, &rid, &tenant);
            c.interrupt(outbound).await
        }
        .instrument(span)
        .await;
        finalise_rpc(
            &mut guard,
            &result,
            &self.audit,
            &audit_ctx,
            &rid,
            "Interrupt",
        );
        result
    }

    async fn release_execute(
        &self,
        req: Request<pb::ReleaseExecuteRequest>,
    ) -> Result<Response<pb::ReleaseExecuteResponse>, Status> {
        let mut guard = self.metrics.rpc_guard("ReleaseExecute");
        let rid = request_id();
        let span = rpc_span("ReleaseExecute", &rid, req.metadata());
        let mut audit_ctx = AuditCtx::default();
        let result: Result<Response<pb::ReleaseExecuteResponse>, Status> = async {
            let (identity, tenant) = self
                .authenticate_and_resolve(req.metadata(), &rid, "ReleaseExecute")
                .await?;
            audit_ctx.set(&tenant, &identity.user_id);
            let mut body = req.into_inner();
            stamp_user_context(&mut body.user_context, &identity);
            let key = key_from_identity(&tenant, &body.session_id, &identity);
            let op_id = body.operation_id.clone();
            let addr = require_addr(self.router.resolve_op(&op_id, &key).await)?;
            let mut c = self.client(&addr)?;
            let mut outbound = Request::new(body);
            self.stamp_outbound(&mut outbound, &rid, &tenant);
            let resp = c.release_execute(outbound).await?;
            // On a successful release the server has dropped the operation, so
            // we drop our reverse-index entry too.
            self.router.forget_op(&op_id).await;
            Ok(resp)
        }
        .instrument(span)
        .await;
        finalise_rpc(
            &mut guard,
            &result,
            &self.audit,
            &audit_ctx,
            &rid,
            "ReleaseExecute",
        );
        result
    }

    async fn release_session(
        &self,
        req: Request<pb::ReleaseSessionRequest>,
    ) -> Result<Response<pb::ReleaseSessionResponse>, Status> {
        let mut guard = self.metrics.rpc_guard("ReleaseSession");
        let rid = request_id();
        let span = rpc_span("ReleaseSession", &rid, req.metadata());
        let mut audit_ctx = AuditCtx::default();
        let result: Result<Response<pb::ReleaseSessionResponse>, Status> = async {
            let (identity, tenant) = self
                .authenticate_and_resolve(req.metadata(), &rid, "ReleaseSession")
                .await?;
            audit_ctx.set(&tenant, &identity.user_id);
            let mut body = req.into_inner();
            stamp_user_context(&mut body.user_context, &identity);
            let key = key_from_identity(&tenant, &body.session_id, &identity);
            let addr = require_addr(self.resolve_session_audited(&key, &rid, &identity).await)?;
            let mut c = self.client(&addr)?;
            let mut outbound = Request::new(body);
            self.stamp_outbound(&mut outbound, &rid, &tenant);
            let resp = c.release_session(outbound).await?;
            self.router.forget_session(&key).await;
            self.audit.session_release(
                &rid,
                &key.tenant,
                &identity.user_id,
                &identity.groups,
                &key.session_id,
                &addr,
            );
            Ok(resp)
        }
        .instrument(span)
        .await;
        finalise_rpc(
            &mut guard,
            &result,
            &self.audit,
            &audit_ctx,
            &rid,
            "ReleaseSession",
        );
        result
    }

    async fn fetch_error_details(
        &self,
        req: Request<pb::FetchErrorDetailsRequest>,
    ) -> Result<Response<pb::FetchErrorDetailsResponse>, Status> {
        let mut guard = self.metrics.rpc_guard("FetchErrorDetails");
        let rid = request_id();
        let span = rpc_span("FetchErrorDetails", &rid, req.metadata());
        let mut audit_ctx = AuditCtx::default();
        let result: Result<Response<pb::FetchErrorDetailsResponse>, Status> = async {
            let (identity, tenant) = self
                .authenticate_and_resolve(req.metadata(), &rid, "FetchErrorDetails")
                .await?;
            audit_ctx.set(&tenant, &identity.user_id);
            let mut body = req.into_inner();
            stamp_user_context(&mut body.user_context, &identity);
            let key = key_from_identity(&tenant, &body.session_id, &identity);
            let addr = require_addr(self.resolve_session_audited(&key, &rid, &identity).await)?;
            let mut c = self.client(&addr)?;
            let mut outbound = Request::new(body);
            self.stamp_outbound(&mut outbound, &rid, &tenant);
            c.fetch_error_details(outbound).await
        }
        .instrument(span)
        .await;
        finalise_rpc(
            &mut guard,
            &result,
            &self.audit,
            &audit_ctx,
            &rid,
            "FetchErrorDetails",
        );
        result
    }

    async fn clone_session(
        &self,
        req: Request<pb::CloneSessionRequest>,
    ) -> Result<Response<pb::CloneSessionResponse>, Status> {
        let mut guard = self.metrics.rpc_guard("CloneSession");
        let rid = request_id();
        let span = rpc_span("CloneSession", &rid, req.metadata());
        let mut audit_ctx = AuditCtx::default();
        let result: Result<Response<pb::CloneSessionResponse>, Status> = async {
            let (identity, tenant) = self
                .authenticate_and_resolve(req.metadata(), &rid, "CloneSession")
                .await?;
            audit_ctx.set(&tenant, &identity.user_id);
            let mut body = req.into_inner();
            stamp_user_context(&mut body.user_context, &identity);
            let key = key_from_identity(&tenant, &body.session_id, &identity);
            let addr = require_addr(self.resolve_session_audited(&key, &rid, &identity).await)?;
            let mut c = self.client(&addr)?;
            let mut outbound = Request::new(body);
            self.stamp_outbound(&mut outbound, &rid, &tenant);
            let resp = c.clone_session(outbound).await?;

            // The cloned session's state lives on the driver that
            // executed the clone. Bind the new session id to that same
            // backend so follow-up RPCs route there instead of being
            // placed on an arbitrary backend by pool selection.
            let new_sid = resp.get_ref().new_session_id.clone();
            if !new_sid.is_empty() {
                let clone_key = key_from_identity(&tenant, &new_sid, &identity);
                let winner = self.router.remember_session(&clone_key, addr.clone()).await;
                if winner == addr {
                    self.audit.session_create(
                        &rid,
                        &tenant,
                        &identity.user_id,
                        &identity.groups,
                        &new_sid,
                        &addr,
                    );
                } else {
                    // A racing RPC already bound the cloned id to a
                    // different backend — the clone's state is on
                    // `addr` but routing will follow `winner`. Surface
                    // loudly; the next RPC on the cloned session will
                    // see single-server unknown-session semantics.
                    warn!(
                        rid = %rid,
                        cloned_session = %new_sid,
                        clone_backend = %addr,
                        bound_backend = %winner,
                        "CloneSession: cloned session id was already bound elsewhere"
                    );
                }
            }
            Ok(resp)
        }
        .instrument(span)
        .await;
        finalise_rpc(
            &mut guard,
            &result,
            &self.audit,
            &audit_ctx,
            &rid,
            "CloneSession",
        );
        result
    }

    async fn get_status(
        &self,
        req: Request<pb::GetStatusRequest>,
    ) -> Result<Response<pb::GetStatusResponse>, Status> {
        let mut guard = self.metrics.rpc_guard("GetStatus");
        let rid = request_id();
        let span = rpc_span("GetStatus", &rid, req.metadata());
        let mut audit_ctx = AuditCtx::default();
        let result: Result<Response<pb::GetStatusResponse>, Status> = async {
            let (identity, tenant) = self
                .authenticate_and_resolve(req.metadata(), &rid, "GetStatus")
                .await?;
            audit_ctx.set(&tenant, &identity.user_id);
            let mut body = req.into_inner();
            stamp_user_context(&mut body.user_context, &identity);
            let key = key_from_identity(&tenant, &body.session_id, &identity);
            let addr = require_addr(self.resolve_session_audited(&key, &rid, &identity).await)?;
            let mut c = self.client(&addr)?;
            let mut outbound = Request::new(body);
            self.stamp_outbound(&mut outbound, &rid, &tenant);
            c.get_status(outbound).await
        }
        .instrument(span)
        .await;
        finalise_rpc(
            &mut guard,
            &result,
            &self.audit,
            &audit_ctx,
            &rid,
            "GetStatus",
        );
        result
    }

    // ----- Server-streaming RPCs ----------------------------------------

    async fn execute_plan(
        &self,
        req: Request<pb::ExecutePlanRequest>,
    ) -> Result<Response<Self::ExecutePlanStream>, Status> {
        let mut guard = self.metrics.rpc_guard("ExecutePlan");
        // The stream guard is moved into the forwarded stream below so it
        // outlives the response — a streaming RPC's "lifetime" for
        // scg_active_streams is the lifetime of the *stream*, not of
        // this handler function.
        let stream_guard = self.metrics.stream_guard();
        let rid = request_id();
        let span = rpc_span("ExecutePlan", &rid, req.metadata());
        let metrics = self.metrics.clone();
        let mut audit_ctx = AuditCtx::default();
        let result: Result<Response<Self::ExecutePlanStream>, Status> = async {
            let (identity, tenant) = self.authenticate_and_resolve(req.metadata(), &rid, "ExecutePlan").await?;
            audit_ctx.set(&tenant, &identity.user_id);
            let mut body = req.into_inner();
            stamp_user_context(&mut body.user_context, &identity);
            let key = key_from_identity(&tenant, &body.session_id, &identity);
            let addr = require_addr(self.resolve_session_audited(&key, &rid, &identity).await)?;
            info!(rid = %rid, rpc = "ExecutePlan", user = %identity.user_id, session = %key.session_id, %addr, "forwarding stream");

            // Bind operation_id → backend so a follow-up ReattachExecute reaches
            // the same driver even if its session id is missing or has been
            // forgotten by the affinity cache.
            if let Some(op_id) = body.operation_id.clone() {
                if !op_id.is_empty() {
                    self.router.remember_op(op_id, addr.clone()).await;
                }
            }

            let mut c = self.client(&addr)?;
            let mut outbound = Request::new(body);
            self.stamp_outbound(&mut outbound, &rid, &tenant);
            let upstream = c.execute_plan(outbound).await?.into_inner();
            Ok(Response::new(forward_server_stream(upstream, stream_guard)))
        }
        .instrument(span)
        .await;
        // If we never built a stream (early error), the guard would be
        // dropped by the .await above; otherwise the stream owns it now.
        // Either way active_streams accounting is correct.
        let _ = metrics;
        finalise_rpc(
            &mut guard,
            &result,
            &self.audit,
            &audit_ctx,
            &rid,
            "ExecutePlan",
        );
        result
    }

    async fn reattach_execute(
        &self,
        req: Request<pb::ReattachExecuteRequest>,
    ) -> Result<Response<Self::ReattachExecuteStream>, Status> {
        let mut guard = self.metrics.rpc_guard("ReattachExecute");
        let stream_guard = self.metrics.stream_guard();
        let rid = request_id();
        let span = rpc_span("ReattachExecute", &rid, req.metadata());
        let mut audit_ctx = AuditCtx::default();
        let result: Result<Response<Self::ReattachExecuteStream>, Status> = async {
            let (identity, tenant) = self
                .authenticate_and_resolve(req.metadata(), &rid, "ReattachExecute")
                .await?;
            audit_ctx.set(&tenant, &identity.user_id);
            let mut body = req.into_inner();
            stamp_user_context(&mut body.user_context, &identity);
            let key = key_from_identity(&tenant, &body.session_id, &identity);
            let addr = require_addr(self.router.resolve_op(&body.operation_id, &key).await)?;
            let mut c = self.client(&addr)?;
            let mut outbound = Request::new(body);
            self.stamp_outbound(&mut outbound, &rid, &tenant);
            let upstream = c.reattach_execute(outbound).await?.into_inner();
            Ok(Response::new(forward_server_stream(upstream, stream_guard)))
        }
        .instrument(span)
        .await;
        finalise_rpc(
            &mut guard,
            &result,
            &self.audit,
            &audit_ctx,
            &rid,
            "ReattachExecute",
        );
        result
    }

    // ----- Client-streaming RPCs ----------------------------------------

    async fn add_artifacts(
        &self,
        req: Request<Streaming<pb::AddArtifactsRequest>>,
    ) -> Result<Response<pb::AddArtifactsResponse>, Status> {
        let mut guard = self.metrics.rpc_guard("AddArtifacts");
        let _stream_guard = self.metrics.stream_guard();
        let rid = request_id();
        let span = rpc_span("AddArtifacts", &rid, req.metadata());
        let mut audit_ctx = AuditCtx::default();
        let result: Result<Response<pb::AddArtifactsResponse>, Status> = async {
            // Authenticate from the request metadata before we touch the
            // streaming body — the credential lives in HTTP/2 headers, not
            // in any of the message frames.
            let (identity, tenant) = self
                .authenticate_and_resolve(req.metadata(), &rid, "AddArtifacts")
                .await?;
            audit_ctx.set(&tenant, &identity.user_id);
            let mut inbound = req.into_inner();

            // We need the first message to make the routing decision, then we
            // forward it plus the remainder to the chosen backend.
            let mut first = inbound
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("AddArtifacts: empty client stream"))?;
            stamp_user_context(&mut first.user_context, &identity);

            let key = key_from_identity(&tenant, &first.session_id, &identity);
            let addr = require_addr(self.resolve_session_audited(&key, &rid, &identity).await)?;
            let mut c = self.client(&addr)?;

            let (tx, rx) = tokio::sync::mpsc::channel::<pb::AddArtifactsRequest>(8);
            tx.send(first)
                .await
                .map_err(|_| Status::cancelled("backend closed"))?;

            tokio::spawn(async move {
                while let Ok(Some(m)) = inbound.message().await {
                    if tx.send(m).await.is_err() {
                        break;
                    }
                }
            });

            let mut outbound = Request::new(ReceiverStream::new(rx));
            self.stamp_outbound(&mut outbound, &rid, &tenant);
            let resp = c.add_artifacts(outbound).await?;
            Ok(resp)
        }
        .instrument(span)
        .await;
        if let Err(ref e) = result {
            warn!(rid = %rid, error = %e, "AddArtifacts failed");
        }
        finalise_rpc(
            &mut guard,
            &result,
            &self.audit,
            &audit_ctx,
            &rid,
            "AddArtifacts",
        );
        result
    }
}

fn forward_server_stream<T: Send + 'static>(
    upstream: Streaming<T>,
    // The stream guard must outlive the stream, not the handler
    // function — otherwise scg_active_streams returns to 0 the
    // moment we hand the Response back, even though the stream is
    // still flowing. Capture it in the async_stream so it drops
    // when the *stream* is dropped (i.e. when the client closes,
    // the stream completes, or it errors).
    stream_guard: scg_observability::StreamGuard,
) -> Pin<Box<dyn Stream<Item = StreamItem<T>> + Send + 'static>> {
    Box::pin(async_stream::stream! {
        let _guard = stream_guard;
        let mut upstream = upstream;
        while let Some(item) = upstream.next().await {
            yield item;
        }
    })
}
