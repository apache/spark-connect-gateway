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

//! YAML configuration for the gateway.
//!
//! Two equivalent forms are accepted for backend discovery:
//!
//! ```yaml
//! # Shorthand — equivalent to a static-list `backend_discovery`:
//! backends:
//!   - "host1:15002"
//!   - "host2:15002"
//! ```
//!
//! ```yaml
//! # Tagged static form:
//! backend_discovery:
//!   type: static
//!   addresses: ["host1:15002", "host2:15002"]
//! ```
//!
//! ```yaml
//! # Tagged K8s form — watches an Endpoints object:
//! backend_discovery:
//!   type: k8s
//!   namespace: spark-connect
//!   service_name: spark-connect
//!   port: 15002
//! ```

use serde::Deserialize;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("read config {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parse config {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },
    #[error(
        "config: must specify either `backends` or `backend_discovery` \
         (omitting both is allowed only when \
         `tenant_pools.on_unknown_tenant: reject` is set and at least \
         one tenant override is configured)"
    )]
    NoDiscoverySource,
    #[error("config: cannot specify both `backends` and `backend_discovery`")]
    ConflictingDiscovery,
    #[error("config: static backend list must contain at least one address")]
    EmptyStatic,
    #[error("config: backend token env var {name} is not set")]
    TokenEnvMissing { name: String },
    #[error("config: read backend token file {path}: {source}")]
    TokenFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("config: backend token resolved to an empty string")]
    TokenEmpty,
}

/// One of the supported backend discovery sources.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackendDiscovery {
    /// Fixed list of `host:port` addresses, configured at startup.
    Static { addresses: Vec<String> },
    /// Watch a Kubernetes Service's Endpoints object.
    K8s {
        namespace: String,
        service_name: String,
        port: u16,
    },
}

/// Where a backend pre-shared token comes from. Backends started
/// with `spark.connect.authenticate.token` (Spark 4.0+) require a
/// matching `Bearer` credential on every request; the gateway
/// presents it on the gateway→backend hop so that operators can
/// hold the token *only* in the gateway and have the backend itself
/// reject clients that try to bypass it.
///
/// `env` / `file` keep the token out of the YAML (and out of the
/// ConfigMap the Helm chart renders it into); `inline` is for
/// walkthroughs and tests.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BackendTokenSource {
    Inline { token: String },
    Env { name: String },
    File { path: String },
}

impl BackendTokenSource {
    /// Resolve the token value. Reads the env var / file once, at
    /// startup — the backend's own token is fixed for the server's
    /// lifetime, so rotation implies restarting both sides anyway.
    pub fn resolve(&self) -> Result<String, ConfigError> {
        let token = match self {
            Self::Inline { token } => token.clone(),
            Self::Env { name } => std::env::var(name)
                .map_err(|_| ConfigError::TokenEnvMissing { name: name.clone() })?,
            Self::File { path } => std::fs::read_to_string(path)
                .map_err(|e| ConfigError::TokenFile {
                    path: path.clone(),
                    source: e,
                })?
                .trim_end_matches(['\r', '\n'])
                .to_string(),
        };
        if token.is_empty() {
            return Err(ConfigError::TokenEmpty);
        }
        Ok(token)
    }
}

/// Authentication configuration. Defaults to `none` so an unset
/// `auth:` block lets every caller through as `user_id="anonymous"`
/// — fine for trusted in-cluster networks, not for external exposure.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    /// No auth — anyone reaching the gateway is `user_id: anonymous`.
    /// Acceptable on a trusted network; **not** for production.
    #[default]
    None,
    /// Bearer-token allowlist. See [`scg-auth::token`].
    Static { tokens: Vec<TokenEntry> },
    /// Local-key JWT verification. See [`scg-auth::jwt`].
    Jwt(JwtSettings),
    /// Remote JWKS / OIDC verification. See [`scg-auth::oidc`].
    Oidc(OidcSettings),
}

/// One entry in the static-token table — kept here (rather than only in
/// `scg-auth`) so config files can describe auth without depending on
/// the auth crate's serde shape.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenEntry {
    pub token: String,
    pub user_id: String,
    #[serde(default)]
    pub tenant: Option<String>,
    #[serde(default)]
    pub groups: Vec<String>,
}

/// JWT verification settings; mirrors `scg_auth::jwt::JwtConfig`.
#[derive(Debug, Clone, Deserialize)]
pub struct JwtSettings {
    pub key: KeySource,
    pub algorithms: Vec<String>,
    #[serde(default)]
    pub issuer: Option<String>,
    #[serde(default)]
    pub audience: Option<String>,
    #[serde(default = "default_user_id_claim")]
    pub user_id_claim: String,
    #[serde(default)]
    pub tenant_claim: Option<String>,
    #[serde(default)]
    pub groups_claim: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KeySource {
    PemFile { path: String },
    PemInline { pem: String },
    HmacSecret { secret: String },
}

/// OIDC verification settings; mirrors `scg_auth::oidc::OidcConfig`.
#[derive(Debug, Clone, Deserialize)]
pub struct OidcSettings {
    #[serde(default)]
    pub jwks_url: Option<String>,
    #[serde(default)]
    pub discovery_url: Option<String>,
    pub algorithms: Vec<String>,
    #[serde(default)]
    pub issuer: Option<String>,
    #[serde(default)]
    pub audience: Option<String>,
    #[serde(default = "default_user_id_claim")]
    pub user_id_claim: String,
    #[serde(default)]
    pub tenant_claim: Option<String>,
    #[serde(default)]
    pub groups_claim: Option<String>,
    #[serde(default = "default_refresh_floor_secs")]
    pub refresh_floor_secs: u64,
}

fn default_user_id_claim() -> String {
    "sub".into()
}
fn default_refresh_floor_secs() -> u64 {
    60
}

/// Where the gateway keeps its `(user_id, session_id) -> backend`
/// affinity table. Default `memory` is single-replica only.
/// `redis` is required for HA across multiple gateway replicas —
/// without it, two replicas will pin the same session to different
/// backends and Spark Connect's per-driver session state stops
/// being consistent.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AffinityStoreConfig {
    #[default]
    Memory,
    Redis(RedisStoreSettings),
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisStoreSettings {
    /// `redis://` URL. Supports password (`redis://:pw@host:6379`)
    /// and database index (`redis://host:6379/2`).
    pub url: String,
    /// Key prefix; lets multiple gateway deployments share a Redis
    /// without colliding. Default `scg`.
    #[serde(default = "default_redis_prefix")]
    pub key_prefix: String,
    /// TTL for `(user_id, session_id) -> backend` bindings (seconds).
    /// Refreshed on every read.
    #[serde(default = "default_session_ttl_secs")]
    pub session_ttl_secs: u64,
    /// TTL for `op_id -> backend` bindings (seconds).
    #[serde(default = "default_op_ttl_secs")]
    pub op_ttl_secs: u64,
}

fn default_redis_prefix() -> String {
    "scg".into()
}
fn default_session_ttl_secs() -> u64 {
    60 * 60
}
fn default_op_ttl_secs() -> u64 {
    15 * 60
}

/// Audit logging configuration. Records security- and
/// compliance-relevant events as structured `tracing` events with
/// `target = "scg::audit"`. On by default — the per-event cost is
/// negligible and the compliance value is high.
#[derive(Debug, Clone, Deserialize)]
pub struct AuditSettings {
    /// Master switch. `true` (default) writes session lifecycle,
    /// auth failures, and RPC errors to the audit stream.
    #[serde(default = "default_audit_enabled")]
    pub enabled: bool,
    /// When `true`, every successful RPC also emits an `rpc.ok`
    /// audit event. Off by default — successful RPCs are already
    /// counted in `scg_rpcs_total{code="OK"}` and emitting one
    /// audit event per successful RPC drowns out the events
    /// operators actually care about. Switch on under strict
    /// monitoring.
    #[serde(default)]
    pub log_successful_rpcs: bool,
}

fn default_audit_enabled() -> bool {
    true
}

impl Default for AuditSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            log_successful_rpcs: false,
        }
    }
}

/// Per-tenant rate-limit configuration. Token bucket
/// per tenant with an optional per-user sub-bucket; both are
/// disabled (`rpcs_per_second: 0`) by default so the limiter is a
/// no-op until the operator opts in.
///
/// Tenants not listed in `overrides` use `default`; an inbound RPC
/// is admitted only when *both* applicable buckets (tenant +
/// per-user, if enabled) have tokens.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RateLimitSettings {
    /// Master switch. When `false` (default), no rate limiting is
    /// performed even if `default` / `overrides` are populated.
    #[serde(default)]
    pub enabled: bool,
    /// Backend store for the bucket state. `memory` (default)
    /// enforces quotas per gateway replica; `redis` shares state
    /// across all replicas via a Lua-driven token bucket. See
    /// `crates/ratelimit/src/redis.rs` for the wire format and the
    /// trade-off between the two stores.
    #[serde(default)]
    pub store: RateLimitStore,
    /// Redis connection settings — only consulted when `store: redis`.
    #[serde(default)]
    pub redis: RateLimitRedisSettings,
    #[serde(default)]
    pub default: BucketSettings,
    #[serde(default)]
    pub overrides: std::collections::HashMap<String, BucketSettings>,
}

/// Which backend stores the token-bucket state.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitStore {
    /// Each gateway replica enforces its own bucket. Fine for
    /// single-replica deployments or back-pressure-style limiting.
    #[default]
    Memory,
    /// Atomic token bucket in Redis, shared across all replicas.
    /// The effective cluster-wide quota matches the configured
    /// rates exactly.
    Redis,
}

/// Redis settings for the distributed limiter. Defaults are
/// dev-friendly — production should override `url`.
#[derive(Debug, Clone, Deserialize)]
pub struct RateLimitRedisSettings {
    /// Redis URL. e.g. `redis://redis.spark-connect.svc:6379`.
    #[serde(default = "default_rate_limit_redis_url")]
    pub url: String,
    /// Key prefix. All limiter keys live under `{key_prefix}:t:*`
    /// (tenant) and `{key_prefix}:u:*` (user). Default `scg-rl` —
    /// distinct from the affinity store's prefix so flushing one
    /// doesn't disturb the other.
    #[serde(default = "default_rate_limit_redis_key_prefix")]
    pub key_prefix: String,
    /// TTL for an idle bucket key in seconds. Default 3600 (one
    /// hour) — long enough that an idle tenant doesn't lose its
    /// bucket, short enough that one-off keys GC themselves.
    #[serde(default = "default_rate_limit_redis_key_ttl_secs")]
    pub key_ttl_secs: u64,
    /// Behaviour when Redis is unreachable. `open` (default) admits
    /// the RPC and bumps `scg_rate_limit_redis_errors_total`;
    /// `closed` rejects the RPC with `ResourceExhausted`.
    #[serde(default)]
    pub on_failure: RateLimitFailMode,
}

impl Default for RateLimitRedisSettings {
    fn default() -> Self {
        Self {
            url: default_rate_limit_redis_url(),
            key_prefix: default_rate_limit_redis_key_prefix(),
            key_ttl_secs: default_rate_limit_redis_key_ttl_secs(),
            on_failure: RateLimitFailMode::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitFailMode {
    /// Admit the RPC; bump the error metric. Recommended default —
    /// availability over strict quotas.
    #[default]
    Open,
    /// Reject the RPC with `ResourceExhausted`. Use for strict-SaaS
    /// deployments where a Redis outage must not become a
    /// quota-bypass vector; makes Redis a hard request-path
    /// dependency.
    Closed,
}

fn default_rate_limit_redis_url() -> String {
    "redis://localhost:6379".into()
}
fn default_rate_limit_redis_key_prefix() -> String {
    "scg-rl".into()
}
fn default_rate_limit_redis_key_ttl_secs() -> u64 {
    3600
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct BucketSettings {
    /// Per-tenant token-bucket refill rate (RPCs/second). `0`
    /// disables the per-tenant bucket entirely.
    #[serde(default)]
    pub rpcs_per_second: f64,
    /// Per-tenant token-bucket capacity (max consecutive RPCs
    /// before the limiter kicks in).
    #[serde(default)]
    pub burst: u64,
    /// Per-user token-bucket refill rate inside the tenant. `0`
    /// (default) disables this dimension — only the per-tenant
    /// bucket is consulted.
    #[serde(default)]
    pub per_user_rpcs_per_second: f64,
    #[serde(default)]
    pub per_user_burst: u64,
}

/// Per-tenant backend pool overrides. A multi-tenant deployment
/// lists one entry per tenant that needs its own pool;
/// any tenant *not* listed here routes through the deployment's
/// default pool (the existing `backends:` / `backend_discovery:`
/// settings).
///
/// The fallback `policy` decides what happens when an inbound RPC
/// carries a tenant that has neither an explicit override nor (in
/// the `Reject` case) any pool at all. Default `UseDefault` matches
/// the single-tenant baseline — everything routes to the default
/// pool. `Reject` is the right choice for SaaS-style deployments
/// where unconfigured tenants must not get any access.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TenantPoolsSettings {
    /// Tenant name → its own pool's discovery configuration. The
    /// `default` tenant is **not** special here; if you want the
    /// default pool to be different from what `backends:` /
    /// `backend_discovery:` provides, list it as an override too.
    #[serde(default)]
    pub overrides: std::collections::HashMap<String, TenantPoolConfig>,
    /// What to do when an inbound RPC has a tenant that's not in
    /// `overrides`. `use_default` (default) routes through the
    /// deployment's default pool; `reject` returns
    /// `PermissionDenied` to the client.
    #[serde(default = "default_unknown_tenant_policy")]
    pub on_unknown_tenant: UnknownTenantPolicySetting,
}

/// One tenant's pool configuration: a discovery source plus,
/// optionally, its own backend token. The discovery fields sit at
/// the same YAML level as `backend_token` (serde-flattened), so
/// existing override entries keep parsing unchanged:
///
/// ```yaml
/// tenant_pools:
///   overrides:
///     team-a:
///       type: static
///       addresses: ["a-1:15002"]
///       backend_token:          # optional
///         kind: env
///         name: TEAM_A_BACKEND_TOKEN
/// ```
///
/// A tenant override *without* `backend_token` inherits the
/// top-level `backend_token` (if any).
#[derive(Debug, Clone, Deserialize)]
pub struct TenantPoolConfig {
    #[serde(flatten)]
    pub discovery: BackendDiscovery,
    #[serde(default)]
    pub backend_token: Option<BackendTokenSource>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownTenantPolicySetting {
    #[default]
    UseDefault,
    Reject,
}

fn default_unknown_tenant_policy() -> UnknownTenantPolicySetting {
    UnknownTenantPolicySetting::UseDefault
}

/// How the gateway figures out which tenant an inbound RPC belongs
/// to. The resolved tenant becomes the first segment of the routing
/// key, so two tenants with the same `session_id` get isolated
/// affinity buckets.
///
/// Deployments without a `tenant_resolver:` block fall back to
/// `from_claim + use_default + "default"`, so every RPC ends up
/// in `tenant="default"` — the single-tenant baseline.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum TenantResolverSource {
    /// Read from `Identity.tenant` produced by the auth interceptor
    /// (JWT/OIDC `tenant` claim, static-token `tenant` field).
    FromClaim,
    /// Read from a gRPC metadata header. For deployments where auth
    /// is disabled but clients still cooperate by declaring a
    /// tenant.
    FromMetadata {
        #[serde(default = "default_tenant_header")]
        header: String,
    },
    /// Always use `default_name`. Single-tenant deployments that
    /// don't want to bother with auth claims or headers.
    AlwaysDefault,
}

fn default_tenant_header() -> String {
    "x-tenant".into()
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantOnMissing {
    /// Fall back to `default_name` when the source yields nothing.
    /// The default — preserves single-tenant behaviour for
    /// deployments that haven't opted into multi-tenant routing.
    UseDefault,
    /// Fail the RPC with `Unauthenticated`. Used by SaaS-style
    /// deployments where a missing tenant claim almost always means
    /// the IdP is misconfigured.
    Reject,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TenantResolverSettings {
    #[serde(flatten)]
    pub source: TenantResolverSource,
    #[serde(default = "default_tenant_on_missing")]
    pub on_missing: TenantOnMissing,
    #[serde(default = "default_tenant_name")]
    pub default_name: String,
}

fn default_tenant_on_missing() -> TenantOnMissing {
    TenantOnMissing::UseDefault
}
fn default_tenant_name() -> String {
    "default".into()
}

impl Default for TenantResolverSettings {
    fn default() -> Self {
        Self {
            source: TenantResolverSource::FromClaim,
            on_missing: TenantOnMissing::UseDefault,
            default_name: default_tenant_name(),
        }
    }
}

/// Active gRPC health-check probing for backend pool members. Wraps
/// the configured pool with a probe loop that calls
/// `grpc.health.v1.Health/Check` on each backend and removes
/// repeatedly-failing ones from `pick()`. Off by default to avoid
/// breaking deployments where backends don't ship the standard
/// Health service.
#[derive(Debug, Clone, Deserialize)]
pub struct HealthCheckSettings {
    /// Master switch. `false` (default) skips active probing —
    /// routing fails through to the next session on a forward
    /// error instead.
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_hc_interval_secs")]
    pub interval_secs: u64,
    #[serde(default = "default_hc_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_hc_unhealthy_threshold")]
    pub unhealthy_threshold: u32,
    #[serde(default = "default_hc_healthy_threshold")]
    pub healthy_threshold: u32,
}

impl Default for HealthCheckSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: default_hc_interval_secs(),
            timeout_secs: default_hc_timeout_secs(),
            unhealthy_threshold: default_hc_unhealthy_threshold(),
            healthy_threshold: default_hc_healthy_threshold(),
        }
    }
}

fn default_hc_interval_secs() -> u64 {
    5
}
fn default_hc_timeout_secs() -> u64 {
    2
}
fn default_hc_unhealthy_threshold() -> u32 {
    3
}
fn default_hc_healthy_threshold() -> u32 {
    2
}

/// Graceful shutdown behaviour. On SIGINT/SIGTERM, the gateway
/// flips `/readyz` to 503 (so K8s drains it from the Service), then
/// waits for in-flight streaming RPCs (`ExecutePlan`,
/// `ReattachExecute`, `AddArtifacts`) to complete, up to
/// `deadline_secs`.
#[derive(Debug, Clone, Deserialize)]
pub struct ShutdownSettings {
    /// Hard ceiling on the drain period. After this many seconds the
    /// gateway forcibly shuts down regardless of in-flight streams.
    /// Pick something compatible with your K8s
    /// `terminationGracePeriodSeconds` (the chart defaults to 30).
    #[serde(default = "default_shutdown_deadline_secs")]
    pub deadline_secs: u64,
}

impl Default for ShutdownSettings {
    fn default() -> Self {
        Self {
            deadline_secs: default_shutdown_deadline_secs(),
        }
    }
}

fn default_shutdown_deadline_secs() -> u64 {
    30
}

/// Distributed-tracing configuration. Off by default — configs
/// without a `tracing:` section keep working.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TracingSettings {
    /// OTLP/gRPC collector endpoint (e.g. `http://otel-collector:4317`).
    /// `None` disables span export — only the JSON log formatter runs.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// `service.name` resource attribute reported on every span.
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// `service.version` resource attribute. Defaults to the
    /// gateway's compile-time CARGO_PKG_VERSION when omitted.
    #[serde(default)]
    pub service_version: Option<String>,
    /// `TraceIdRatioBased` sampling ratio in `[0.0, 1.0]`. Wrapped in
    /// `ParentBased` at runtime so a sampled remote parent always wins.
    #[serde(default = "default_sample_ratio")]
    pub sample_ratio: f64,
    /// Per-batch OTLP export deadline, in seconds.
    #[serde(default = "default_export_timeout_secs")]
    pub export_timeout_secs: u64,
}

fn default_service_name() -> String {
    "spark-connect-gateway".into()
}
fn default_sample_ratio() -> f64 {
    1.0
}
fn default_export_timeout_secs() -> u64 {
    10
}

/// Raw YAML shape — accepts either the legacy `backends` shorthand or the
/// tagged `backend_discovery` form, never both.
#[derive(Debug, Deserialize)]
struct RawConfig {
    #[serde(default = "default_bind_addr")]
    bind_addr: String,
    #[serde(default)]
    backends: Option<Vec<String>>,
    #[serde(default)]
    backend_discovery: Option<BackendDiscovery>,
    #[serde(default)]
    backend_token: Option<BackendTokenSource>,
    #[serde(default)]
    auth: Option<AuthConfig>,
    /// Address for the admin / metrics HTTP server. `null` disables it.
    /// Default `0.0.0.0:9090`.
    #[serde(default = "default_admin_addr_opt")]
    admin_addr: Option<String>,
    #[serde(default)]
    tracing: Option<TracingSettings>,
    #[serde(default)]
    affinity_store: Option<AffinityStoreConfig>,
    #[serde(default)]
    health_check: Option<HealthCheckSettings>,
    #[serde(default)]
    shutdown: Option<ShutdownSettings>,
    #[serde(default)]
    tenant_resolver: Option<TenantResolverSettings>,
    #[serde(default)]
    tenant_pools: Option<TenantPoolsSettings>,
    #[serde(default)]
    rate_limit: Option<RateLimitSettings>,
    #[serde(default)]
    audit: Option<AuditSettings>,
}

fn default_admin_addr_opt() -> Option<String> {
    Some(":9090".into())
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: String,
    /// The default pool's discovery source. `None` is permitted only
    /// under `tenant_pools.on_unknown_tenant: reject` with at least
    /// one tenant override — in that mode the default pool is never
    /// selected (unmatched tenants are rejected before pool
    /// selection), so requiring a discovery source would force
    /// operators to configure a pool that cannot receive traffic.
    pub discovery: Option<BackendDiscovery>,
    /// Pre-shared token the gateway presents (as `authorization:
    /// Bearer <token>`) on every gateway→backend request. Applies to
    /// all pools unless a tenant override carries its own
    /// `backend_token`. `None` sends no credential — only safe when
    /// the backends don't enforce `spark.connect.authenticate.token`.
    pub backend_token: Option<BackendTokenSource>,
    pub auth: AuthConfig,
    /// `Some(addr)` to enable the admin HTTP server, `None` to skip it.
    pub admin_addr: Option<String>,
    /// Distributed-tracing settings. `None` keeps tracing off (the
    /// gateway only emits structured JSON logs in that case).
    pub tracing: Option<TracingSettings>,
    /// Where to keep affinity state. Defaults to in-memory; use
    /// `redis` for multi-replica HA.
    pub affinity_store: AffinityStoreConfig,
    /// Active gRPC health-check probing for backends. Off by default.
    pub health_check: HealthCheckSettings,
    /// Graceful shutdown / drain settings.
    pub shutdown: ShutdownSettings,
    /// How to figure out the tenant for each inbound RPC. Defaults
    /// to the back-compat behaviour (every RPC -> tenant="default").
    pub tenant_resolver: TenantResolverSettings,
    /// Per-tenant backend pool overrides + unknown-tenant policy.
    /// Empty `overrides` + `use_default` reproduces the single-pool
    /// baseline.
    pub tenant_pools: TenantPoolsSettings,
    /// Per-tenant rate limiting. Disabled by default.
    pub rate_limit: RateLimitSettings,
    /// Structured audit logging. Enabled by default.
    pub audit: AuditSettings,
}

fn default_bind_addr() -> String {
    ":15003".into()
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path_str = path.as_ref().display().to_string();
        let data = std::fs::read_to_string(path.as_ref()).map_err(|e| ConfigError::Io {
            path: path_str.clone(),
            source: e,
        })?;
        let raw: RawConfig = serde_yaml::from_str(&data).map_err(|e| ConfigError::Parse {
            path: path_str,
            source: e,
        })?;
        Self::from_raw(raw)
    }

    fn from_raw(raw: RawConfig) -> Result<Self, ConfigError> {
        let tenant_pools = raw.tenant_pools.unwrap_or_default();
        let discovery = match (raw.backends, raw.backend_discovery) {
            (Some(_), Some(_)) => return Err(ConfigError::ConflictingDiscovery),
            (None, None) => {
                // No default pool is acceptable only when routing can
                // never select it: strict multi-tenant deployments
                // where every admitted tenant has its own pool and
                // everything else is rejected.
                let strict = matches!(
                    tenant_pools.on_unknown_tenant,
                    UnknownTenantPolicySetting::Reject
                ) && !tenant_pools.overrides.is_empty();
                if !strict {
                    return Err(ConfigError::NoDiscoverySource);
                }
                None
            }
            (Some(addrs), None) => {
                if addrs.is_empty() {
                    return Err(ConfigError::EmptyStatic);
                }
                Some(BackendDiscovery::Static { addresses: addrs })
            }
            (None, Some(d)) => {
                if let BackendDiscovery::Static { addresses } = &d {
                    if addresses.is_empty() {
                        return Err(ConfigError::EmptyStatic);
                    }
                }
                Some(d)
            }
        };
        let bind_addr = if raw.bind_addr.is_empty() {
            default_bind_addr()
        } else {
            raw.bind_addr
        };
        Ok(Self {
            bind_addr,
            discovery,
            backend_token: raw.backend_token,
            auth: raw.auth.unwrap_or_default(),
            admin_addr: raw.admin_addr.filter(|s| !s.is_empty()),
            tracing: raw.tracing,
            affinity_store: raw.affinity_store.unwrap_or_default(),
            health_check: raw.health_check.unwrap_or_default(),
            shutdown: raw.shutdown.unwrap_or_default(),
            tenant_resolver: raw.tenant_resolver.unwrap_or_default(),
            tenant_pools,
            rate_limit: raw.rate_limit.unwrap_or_default(),
            audit: raw.audit.unwrap_or_default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write(text: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "{}", text).unwrap();
        f
    }

    #[test]
    fn loads_legacy_backends_shorthand() {
        let f = write(
            r#"
bind_addr: ":15003"
backends:
  - "127.0.0.1:15002"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.bind_addr, ":15003");
        match c.discovery {
            Some(BackendDiscovery::Static { addresses }) => {
                assert_eq!(addresses, vec!["127.0.0.1:15002"]);
            }
            other => panic!("expected Static, got {:?}", other),
        }
    }

    #[test]
    fn loads_tagged_static() {
        let f = write(
            r#"
backend_discovery:
  type: static
  addresses: ["a:1", "b:2"]
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.discovery {
            Some(BackendDiscovery::Static { addresses }) => {
                assert_eq!(addresses, vec!["a:1", "b:2"]);
            }
            other => panic!("expected Static, got {:?}", other),
        }
    }

    #[test]
    fn loads_tagged_k8s() {
        let f = write(
            r#"
backend_discovery:
  type: k8s
  namespace: spark-connect
  service_name: spark-connect
  port: 15002
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.discovery {
            Some(BackendDiscovery::K8s {
                namespace,
                service_name,
                port,
            }) => {
                assert_eq!(namespace, "spark-connect");
                assert_eq!(service_name, "spark-connect");
                assert_eq!(port, 15002);
            }
            other => panic!("expected K8s, got {:?}", other),
        }
    }

    #[test]
    fn omitted_discovery_allowed_under_reject_with_overrides() {
        // Strict multi-tenant mode: every admitted tenant has its own
        // pool, unknown tenants are rejected — the default pool would
        // never be selected, so requiring one would be dead config.
        let f = write(
            r#"
tenant_pools:
  on_unknown_tenant: reject
  overrides:
    team-a:
      type: static
      addresses: ["a:15002"]
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert!(c.discovery.is_none());
        assert!(matches!(
            c.tenant_pools.on_unknown_tenant,
            UnknownTenantPolicySetting::Reject
        ));
        assert_eq!(c.tenant_pools.overrides.len(), 1);
    }

    #[test]
    fn omitted_discovery_rejected_under_use_default() {
        // With use_default (explicit or implicit), the default pool
        // is reachable, so a discovery source is mandatory.
        let f = write(
            r#"
tenant_pools:
  on_unknown_tenant: use_default
  overrides:
    team-a:
      type: static
      addresses: ["a:15002"]
"#,
        );
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::NoDiscoverySource
        ));
    }

    #[test]
    fn omitted_discovery_rejected_without_overrides() {
        // reject + zero overrides would reject every RPC — that's a
        // config mistake, not a deployment shape; fail loudly.
        let f = write(
            r#"
tenant_pools:
  on_unknown_tenant: reject
"#,
        );
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::NoDiscoverySource
        ));
    }

    #[test]
    fn empty_backends_shorthand_rejected() {
        let f = write("backends: []\n");
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::EmptyStatic
        ));
    }

    #[test]
    fn empty_static_in_tagged_form_rejected() {
        let f = write(
            r#"
backend_discovery:
  type: static
  addresses: []
"#,
        );
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::EmptyStatic
        ));
    }

    #[test]
    fn missing_discovery_rejected() {
        let f = write("bind_addr: ':15003'\n");
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::NoDiscoverySource
        ));
    }

    #[test]
    fn conflicting_discovery_rejected() {
        let f = write(
            r#"
backends: ["a:1"]
backend_discovery:
  type: static
  addresses: ["b:2"]
"#,
        );
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::ConflictingDiscovery
        ));
    }

    #[test]
    fn defaults_bind_addr() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.bind_addr, ":15003");
    }

    #[test]
    fn auth_defaults_to_none() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(matches!(c.auth, AuthConfig::None));
    }

    #[test]
    fn loads_static_auth() {
        let f = write(
            r#"
backends: ["a:1"]
auth:
  type: static
  tokens:
    - token: "alice-secret"
      user_id: "alice"
      tenant: "team-a"
      groups: ["devs"]
    - token: "bob-secret"
      user_id: "bob"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.auth {
            AuthConfig::Static { tokens } => {
                assert_eq!(tokens.len(), 2);
                assert_eq!(tokens[0].user_id, "alice");
                assert_eq!(tokens[0].tenant.as_deref(), Some("team-a"));
                assert_eq!(tokens[1].user_id, "bob");
                assert!(tokens[1].tenant.is_none());
            }
            other => panic!("expected Static, got {:?}", other),
        }
    }

    #[test]
    fn loads_jwt_auth() {
        let f = write(
            r#"
backends: ["a:1"]
auth:
  type: jwt
  algorithms: ["RS256"]
  issuer: "https://idp.example.com"
  audience: "spark-connect-gateway"
  key:
    kind: pem_file
    path: "/etc/gateway/idp-pub.pem"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.auth {
            AuthConfig::Jwt(s) => {
                assert_eq!(s.algorithms, vec!["RS256"]);
                assert_eq!(s.issuer.as_deref(), Some("https://idp.example.com"));
                match s.key {
                    KeySource::PemFile { path } => assert_eq!(path, "/etc/gateway/idp-pub.pem"),
                    other => panic!("expected PemFile, got {:?}", other),
                }
            }
            other => panic!("expected Jwt, got {:?}", other),
        }
    }

    #[test]
    fn tracing_defaults_to_off() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(c.tracing.is_none());
    }

    #[test]
    fn loads_tracing_block() {
        let f = write(
            r#"
backends: ["a:1"]
tracing:
  endpoint: "http://otel-collector:4317"
  service_name: "scg-staging"
  sample_ratio: 0.25
  export_timeout_secs: 5
"#,
        );
        let c = Config::load(f.path()).unwrap();
        let t = c.tracing.expect("tracing settings parsed");
        assert_eq!(t.endpoint.as_deref(), Some("http://otel-collector:4317"));
        assert_eq!(t.service_name, "scg-staging");
        assert!((t.sample_ratio - 0.25).abs() < 1e-9);
        assert_eq!(t.export_timeout_secs, 5);
    }

    #[test]
    fn tracing_block_endpoint_can_be_omitted_for_log_only() {
        // A `tracing:` block without an endpoint is legal — the gateway
        // skips OTLP export but still respects the other knobs (e.g.
        // service_name) for when the user later sets an endpoint.
        let f = write(
            r#"
backends: ["a:1"]
tracing:
  service_name: "scg-test"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        let t = c.tracing.expect("tracing settings parsed");
        assert!(t.endpoint.is_none());
        assert_eq!(t.service_name, "scg-test");
        // Defaults round-trip:
        assert!((t.sample_ratio - 1.0).abs() < 1e-9);
        assert_eq!(t.export_timeout_secs, 10);
    }

    #[test]
    fn affinity_store_defaults_to_memory() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(matches!(c.affinity_store, AffinityStoreConfig::Memory));
    }

    #[test]
    fn loads_redis_affinity_store() {
        let f = write(
            r#"
backends: ["a:1"]
affinity_store:
  type: redis
  url: "redis://redis-cluster:6379"
  key_prefix: "scg-prod"
  session_ttl_secs: 7200
  op_ttl_secs: 600
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.affinity_store {
            AffinityStoreConfig::Redis(s) => {
                assert_eq!(s.url, "redis://redis-cluster:6379");
                assert_eq!(s.key_prefix, "scg-prod");
                assert_eq!(s.session_ttl_secs, 7200);
                assert_eq!(s.op_ttl_secs, 600);
            }
            other => panic!("expected Redis, got {:?}", other),
        }
    }

    #[test]
    fn redis_affinity_store_has_sane_defaults() {
        let f = write(
            r#"
backends: ["a:1"]
affinity_store:
  type: redis
  url: "redis://localhost:6379"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.affinity_store {
            AffinityStoreConfig::Redis(s) => {
                assert_eq!(s.key_prefix, "scg");
                assert_eq!(s.session_ttl_secs, 3600);
                assert_eq!(s.op_ttl_secs, 900);
            }
            other => panic!("expected Redis, got {:?}", other),
        }
    }

    #[test]
    fn health_check_defaults_to_disabled() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(!c.health_check.enabled);
        // Default values present even when block omitted:
        assert_eq!(c.health_check.interval_secs, 5);
        assert_eq!(c.health_check.unhealthy_threshold, 3);
    }

    #[test]
    fn loads_health_check_block() {
        let f = write(
            r#"
backends: ["a:1"]
health_check:
  enabled: true
  interval_secs: 10
  timeout_secs: 3
  unhealthy_threshold: 5
  healthy_threshold: 3
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert!(c.health_check.enabled);
        assert_eq!(c.health_check.interval_secs, 10);
        assert_eq!(c.health_check.timeout_secs, 3);
        assert_eq!(c.health_check.unhealthy_threshold, 5);
        assert_eq!(c.health_check.healthy_threshold, 3);
    }

    #[test]
    fn shutdown_defaults() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.shutdown.deadline_secs, 30);
    }

    #[test]
    fn loads_shutdown_block() {
        let f = write(
            r#"
backends: ["a:1"]
shutdown:
  deadline_secs: 90
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.shutdown.deadline_secs, 90);
    }

    #[test]
    fn tenant_resolver_defaults_to_from_claim_use_default() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(matches!(
            c.tenant_resolver.source,
            TenantResolverSource::FromClaim
        ));
        assert!(matches!(
            c.tenant_resolver.on_missing,
            TenantOnMissing::UseDefault
        ));
        assert_eq!(c.tenant_resolver.default_name, "default");
    }

    #[test]
    fn loads_from_claim_reject() {
        let f = write(
            r#"
backends: ["a:1"]
tenant_resolver:
  source: from_claim
  on_missing: reject
  default_name: "default"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert!(matches!(
            c.tenant_resolver.source,
            TenantResolverSource::FromClaim
        ));
        assert!(matches!(
            c.tenant_resolver.on_missing,
            TenantOnMissing::Reject
        ));
    }

    #[test]
    fn loads_from_metadata_with_custom_header() {
        let f = write(
            r#"
backends: ["a:1"]
tenant_resolver:
  source: from_metadata
  header: "x-org"
  on_missing: use_default
  default_name: "shared"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.tenant_resolver.source {
            TenantResolverSource::FromMetadata { header } => assert_eq!(header, "x-org"),
            other => panic!("expected FromMetadata, got {:?}", other),
        }
        assert_eq!(c.tenant_resolver.default_name, "shared");
    }

    #[test]
    fn from_metadata_header_defaults_to_x_tenant() {
        let f = write(
            r#"
backends: ["a:1"]
tenant_resolver:
  source: from_metadata
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.tenant_resolver.source {
            TenantResolverSource::FromMetadata { header } => assert_eq!(header, "x-tenant"),
            other => panic!("expected FromMetadata, got {:?}", other),
        }
    }

    #[test]
    fn loads_always_default() {
        let f = write(
            r#"
backends: ["a:1"]
tenant_resolver:
  source: always_default
  default_name: "single-tenant"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert!(matches!(
            c.tenant_resolver.source,
            TenantResolverSource::AlwaysDefault
        ));
        assert_eq!(c.tenant_resolver.default_name, "single-tenant");
    }

    #[test]
    fn tenant_pools_default_empty_use_default() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(c.tenant_pools.overrides.is_empty());
        assert!(matches!(
            c.tenant_pools.on_unknown_tenant,
            UnknownTenantPolicySetting::UseDefault
        ));
    }

    #[test]
    fn loads_tenant_pools_with_overrides() {
        let f = write(
            r#"
backends: ["default-a:1", "default-b:1"]
tenant_pools:
  on_unknown_tenant: reject
  overrides:
    team-a:
      type: static
      addresses: ["a-1:15002", "a-2:15002"]
    team-b:
      type: k8s
      namespace: spark-b
      service_name: spark-connect
      port: 15002
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.tenant_pools.overrides.len(), 2);
        assert!(matches!(
            c.tenant_pools.on_unknown_tenant,
            UnknownTenantPolicySetting::Reject
        ));
        let team_a = c.tenant_pools.overrides.get("team-a").unwrap();
        assert!(team_a.backend_token.is_none());
        match &team_a.discovery {
            BackendDiscovery::Static { addresses } => {
                assert_eq!(
                    addresses,
                    &vec!["a-1:15002".to_string(), "a-2:15002".to_string()]
                )
            }
            other => panic!("expected Static, got {:?}", other),
        }
        match &c.tenant_pools.overrides.get("team-b").unwrap().discovery {
            BackendDiscovery::K8s {
                namespace,
                service_name,
                port,
            } => {
                assert_eq!(namespace, "spark-b");
                assert_eq!(service_name, "spark-connect");
                assert_eq!(*port, 15002);
            }
            other => panic!("expected K8s, got {:?}", other),
        }
    }

    #[test]
    fn backend_token_defaults_to_none() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(c.backend_token.is_none());
    }

    #[test]
    fn loads_inline_backend_token() {
        let f = write(
            r#"
backends: ["a:1"]
backend_token:
  kind: inline
  token: "deadbeef"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        let src = c.backend_token.expect("backend_token parsed");
        assert_eq!(src.resolve().unwrap(), "deadbeef");
    }

    #[test]
    fn loads_env_backend_token() {
        let f = write(
            r#"
backends: ["a:1"]
backend_token:
  kind: env
  name: SCG_TEST_BACKEND_TOKEN
"#,
        );
        let c = Config::load(f.path()).unwrap();
        let src = c.backend_token.expect("backend_token parsed");
        assert!(matches!(
            src.resolve().unwrap_err(),
            ConfigError::TokenEnvMissing { .. }
        ));
        // Env-var manipulation is process-global; this test relies on
        // the name being unique to it, so set/remove is safe even
        // with the parallel test runner.
        std::env::set_var("SCG_TEST_BACKEND_TOKEN", "from-env");
        assert_eq!(src.resolve().unwrap(), "from-env");
        std::env::remove_var("SCG_TEST_BACKEND_TOKEN");
    }

    #[test]
    fn loads_file_backend_token_and_trims_newline() {
        let tok = write("s3cr3t\n");
        let f = write(&format!(
            "backends: [\"a:1\"]\nbackend_token:\n  kind: file\n  path: \"{}\"\n",
            tok.path().display()
        ));
        let c = Config::load(f.path()).unwrap();
        let src = c.backend_token.expect("backend_token parsed");
        assert_eq!(src.resolve().unwrap(), "s3cr3t");
    }

    #[test]
    fn empty_backend_token_rejected_at_resolve() {
        let src = BackendTokenSource::Inline { token: "".into() };
        assert!(matches!(
            src.resolve().unwrap_err(),
            ConfigError::TokenEmpty
        ));
    }

    #[test]
    fn tenant_override_can_carry_its_own_backend_token() {
        // `backend_token` sits at the same level as the flattened
        // discovery fields inside an override entry.
        let f = write(
            r#"
backends: ["default:15002"]
backend_token:
  kind: inline
  token: "default-token"
tenant_pools:
  overrides:
    team-a:
      type: static
      addresses: ["a-1:15002"]
      backend_token:
        kind: inline
        token: "team-a-token"
    team-b:
      type: static
      addresses: ["b-1:15002"]
"#,
        );
        let c = Config::load(f.path()).unwrap();
        let a = c.tenant_pools.overrides.get("team-a").unwrap();
        assert_eq!(
            a.backend_token.as_ref().unwrap().resolve().unwrap(),
            "team-a-token"
        );
        // team-b has no token of its own — inheritance of the
        // top-level token happens at wiring time, not parse time.
        let b = c.tenant_pools.overrides.get("team-b").unwrap();
        assert!(b.backend_token.is_none());
        assert!(matches!(
            &b.discovery,
            BackendDiscovery::Static { addresses } if addresses == &vec!["b-1:15002".to_string()]
        ));
    }

    #[test]
    fn rate_limit_defaults_to_disabled() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(!c.rate_limit.enabled);
        assert_eq!(c.rate_limit.default.rpcs_per_second, 0.0);
        assert!(c.rate_limit.overrides.is_empty());
    }

    #[test]
    fn loads_rate_limit_with_overrides() {
        let f = write(
            r#"
backends: ["a:1"]
rate_limit:
  enabled: true
  default:
    rpcs_per_second: 100
    burst: 200
  overrides:
    team-a:
      rpcs_per_second: 500
      burst: 1000
      per_user_rpcs_per_second: 50
      per_user_burst: 100
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert!(c.rate_limit.enabled);
        assert_eq!(c.rate_limit.default.rpcs_per_second, 100.0);
        assert_eq!(c.rate_limit.default.burst, 200);
        let a = c.rate_limit.overrides.get("team-a").unwrap();
        assert_eq!(a.rpcs_per_second, 500.0);
        assert_eq!(a.burst, 1000);
        assert_eq!(a.per_user_rpcs_per_second, 50.0);
        assert_eq!(a.per_user_burst, 100);
    }

    #[test]
    fn rate_limit_store_defaults_to_memory() {
        let f = write(
            r#"
backends: ["a:1"]
rate_limit:
  enabled: true
  default:
    rpcs_per_second: 100
    burst: 200
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.rate_limit.store, RateLimitStore::Memory);
        // Redis defaults are present but irrelevant when store=memory.
        assert_eq!(c.rate_limit.redis.url, "redis://localhost:6379");
        assert_eq!(c.rate_limit.redis.key_prefix, "scg-rl");
        assert_eq!(c.rate_limit.redis.on_failure, RateLimitFailMode::Open);
    }

    #[test]
    fn loads_rate_limit_with_redis_store() {
        let f = write(
            r#"
backends: ["a:1"]
rate_limit:
  enabled: true
  store: redis
  redis:
    url: "redis://shared.svc:6379"
    key_prefix: "myrl"
    key_ttl_secs: 7200
    on_failure: closed
  default:
    rpcs_per_second: 100
    burst: 200
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.rate_limit.store, RateLimitStore::Redis);
        assert_eq!(c.rate_limit.redis.url, "redis://shared.svc:6379");
        assert_eq!(c.rate_limit.redis.key_prefix, "myrl");
        assert_eq!(c.rate_limit.redis.key_ttl_secs, 7200);
        assert_eq!(c.rate_limit.redis.on_failure, RateLimitFailMode::Closed);
    }

    #[test]
    fn audit_defaults_to_enabled_signals_only() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(c.audit.enabled);
        assert!(!c.audit.log_successful_rpcs);
    }

    #[test]
    fn loads_audit_block_with_successful_rpcs_on() {
        let f = write(
            r#"
backends: ["a:1"]
audit:
  enabled: true
  log_successful_rpcs: true
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert!(c.audit.enabled);
        assert!(c.audit.log_successful_rpcs);
    }

    #[test]
    fn audit_can_be_disabled() {
        let f = write(
            r#"
backends: ["a:1"]
audit:
  enabled: false
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert!(!c.audit.enabled);
    }

    #[test]
    fn loads_oidc_auth() {
        let f = write(
            r#"
backends: ["a:1"]
auth:
  type: oidc
  algorithms: ["RS256"]
  discovery_url: "https://idp.example.com/.well-known/openid-configuration"
  audience: "spark-connect-gateway"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.auth {
            AuthConfig::Oidc(s) => {
                assert_eq!(
                    s.discovery_url.as_deref(),
                    Some("https://idp.example.com/.well-known/openid-configuration")
                );
                assert!(s.jwks_url.is_none());
            }
            other => panic!("expected Oidc, got {:?}", other),
        }
    }
}
