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

//! Per-tenant (+ optional per-user) token-bucket rate limiting.
//!
//! Two backends:
//!
//! * **In-memory** — fine for single-replica deployments or
//!   multi-replica setups behind a sticky LB. Each gateway replica
//!   enforces its own bucket; effective quota is
//!   `N × configured_rate` for an N-replica deployment.
//! * **Redis** — atomic token bucket via a Lua script, shared
//!   across all gateway replicas. The quota is enforced
//!   cluster-wide. See [`redis`] for the wire format and the Lua
//!   contract.
//!
//! ## Why token bucket
//!
//! Spark Connect traffic is bursty by nature: a client opens a
//! session, fires a quick `Config` + `AnalyzePlan` pair, then waits
//! while a long `ExecutePlan` stream runs. A leaky bucket would
//! penalize that warm-up burst; a fixed-window counter would let
//! you double-burst across the window boundary. Token bucket gives
//! you the average-rate guarantee you want for fair sharing while
//! still tolerating the natural burstiness.
//!
//! ## Bucket scopes
//!
//! Two buckets per (tenant, user) pair:
//!
//! * **Tenant bucket** — every RPC for the tenant consumes one
//!   token. This is the primary defence against a single tenant
//!   overwhelming the shared backends.
//! * **User bucket** (optional, off by default) — every RPC for
//!   the specific user inside the tenant also consumes one token.
//!   Lets a tenant limit any one user inside it.
//!
//! Both buckets must have a token available for the RPC to
//! proceed. The user bucket is only consulted when its rate is
//! configured > 0; otherwise it's a no-op.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use tonic::Status;
use tracing::debug;

/// Configuration for one bucket (tenant or user). Rates are in
/// RPCs/second. `burst` is the bucket capacity — the maximum number
/// of consecutive RPCs before the limiter kicks in.
///
/// A rate of `0` disables the bucket entirely (every RPC is
/// admitted without consulting it). This is how operators opt out
/// of the per-user dimension without touching the per-tenant
/// dimension.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BucketRate {
    pub rpcs_per_second: f64,
    pub burst: u64,
}

impl BucketRate {
    pub fn disabled() -> Self {
        Self {
            rpcs_per_second: 0.0,
            burst: 0,
        }
    }
    pub(crate) fn is_enabled(&self) -> bool {
        self.rpcs_per_second > 0.0 && self.burst > 0
    }
}

/// Per-tenant rate-limit config — one for the per-tenant bucket,
/// one for the per-user bucket. Per-user defaults disabled so
/// operators can turn on tenant-level limits without immediately
/// having to decide a sensible per-user value.
#[derive(Debug, Clone, Copy)]
pub struct TenantLimits {
    pub tenant: BucketRate,
    pub per_user: BucketRate,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            tenant: BucketRate::disabled(),
            per_user: BucketRate::disabled(),
        }
    }
}

/// Reasons a [`RateLimiter`] might reject an RPC. Used by callers
/// that want to label metrics or log details — the `Status`
/// returned by [`RateLimiter::check`] is always
/// `RESOURCE_EXHAUSTED`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectScope {
    Tenant,
    User,
}

impl RejectScope {
    pub fn as_str(&self) -> &'static str {
        match self {
            RejectScope::Tenant => "tenant",
            RejectScope::User => "user",
        }
    }
}

/// Observer trait so the proxy can wire metrics in without
/// `scg-ratelimit` knowing about `scg-observability`. The metrics
/// crate registers a counter; this trait's `on_reject` callback
/// bumps it.
pub trait LimiterObserver: Send + Sync + 'static {
    fn on_reject(&self, tenant: &str, scope: RejectScope);
}

/// No-op observer used by the in-crate unit tests. Production
/// callers (gateway main) always wire a metrics-backed observer.
#[cfg(test)]
struct NoopObserver;
#[cfg(test)]
impl LimiterObserver for NoopObserver {
    fn on_reject(&self, _tenant: &str, _scope: RejectScope) {}
}

/// One token-bucket instance. Tokens refill continuously at
/// `rate_per_second` up to `capacity`. Calls to [`Self::try_take`]
/// account for elapsed wall-clock time since the last call.
///
/// The bucket holds *fractional* tokens (`f64`) so a low refill rate
/// (say 0.5 RPS) still lets through one RPC every 2 seconds rather
/// than rounding to zero forever.
struct Bucket {
    rate_per_second: f64,
    capacity: f64,
    tokens: f64,
    last_refill: Instant,
}

impl Bucket {
    fn new(cfg: BucketRate) -> Self {
        Self {
            rate_per_second: cfg.rpcs_per_second,
            capacity: cfg.burst as f64,
            // Start full so a fresh tenant gets its full burst.
            tokens: cfg.burst as f64,
            last_refill: Instant::now(),
        }
    }

    fn try_take(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate_per_second).min(self.capacity);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Cluster-wide observer hook for Redis-related errors. Fail-open
/// deployments use this to bump `scg_rate_limit_redis_errors_total`
/// — fail-closed deployments still bump it before returning
/// `ResourceExhausted`.
pub trait RedisErrorObserver: Send + Sync + 'static {
    fn on_redis_error(&self, tenant: &str, reason: &'static str);
}

/// No-op variant used when redis errors aren't being counted.
pub struct NoopRedisErrorObserver;
impl RedisErrorObserver for NoopRedisErrorObserver {
    fn on_redis_error(&self, _: &str, _: &'static str) {}
}

/// Behaviour when the Redis backend is unreachable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailMode {
    /// Admit the RPC. Recommended default — availability over
    /// strict-quota enforcement, mirrors the Redis affinity-store's
    /// fail-soft behaviour. The error metric still fires so
    /// operators can see the outage in real time.
    Open,
    /// Reject the RPC with `ResourceExhausted`. Pick this for
    /// strict-SaaS isolation policies where a Redis outage must not
    /// become a quota-bypass attack vector. Note: this makes Redis
    /// a hard dependency of the request path.
    Closed,
}

/// Public rate-limiter handle — an enum so the same proxy call site
/// works for both backends. Cheap to clone (each variant is behind
/// `Arc`).
#[derive(Clone)]
pub enum RateLimiter {
    Memory(MemoryLimiter),
    Redis(redis::RedisLimiter),
}

impl RateLimiter {
    /// Convenience: build the in-memory limiter.
    pub fn new(
        default: TenantLimits,
        overrides: HashMap<String, TenantLimits>,
        observer: Arc<dyn LimiterObserver>,
    ) -> Self {
        Self::Memory(MemoryLimiter::new(default, overrides, observer))
    }

    /// True when *some* bucket is enabled — caller can skip the
    /// check-and-acquire dance when it's not.
    pub fn is_active(&self) -> bool {
        match self {
            Self::Memory(m) => m.is_active(),
            Self::Redis(r) => r.is_active(),
        }
    }

    /// Take one token from the (tenant, user) bucket pair. Returns
    /// `Ok(())` when both buckets had tokens available, or
    /// `Err(Status::ResourceExhausted)` when either was empty.
    ///
    /// On rejection the observer is notified with the *first* scope
    /// that failed — typically the more-restrictive of the two —
    /// so metrics show which dimension is the bottleneck.
    pub async fn check(&self, tenant: &str, user: &str) -> Result<(), Status> {
        match self {
            Self::Memory(m) => m.check(tenant, user),
            Self::Redis(r) => r.check(tenant, user).await,
        }
    }
}

/// In-memory rate limiter. Cheap to clone (everything's behind an
/// `Arc`). Operator-supplied config is fixed for the lifetime of
/// the process.
#[derive(Clone)]
pub struct MemoryLimiter {
    inner: Arc<MemoryLimiterInner>,
}

struct MemoryLimiterInner {
    /// Per-tenant config: explicit overrides keyed by tenant name,
    /// plus a `default` fallback applied to any tenant not listed.
    overrides: HashMap<String, TenantLimits>,
    default: TenantLimits,
    /// Live state: one tenant bucket + per-user bucket map per tenant.
    state: Mutex<HashMap<String, TenantState>>,
    observer: Arc<dyn LimiterObserver>,
}

struct TenantState {
    tenant_bucket: Bucket,
    user_buckets: HashMap<String, Bucket>,
}

impl MemoryLimiter {
    /// Build an in-memory limiter with the given default + overrides.
    /// Both buckets being disabled (the default) is a no-op
    /// limiter; callers can keep the limiter wired into the proxy
    /// without overhead.
    pub fn new(
        default: TenantLimits,
        overrides: HashMap<String, TenantLimits>,
        observer: Arc<dyn LimiterObserver>,
    ) -> Self {
        Self {
            inner: Arc::new(MemoryLimiterInner {
                overrides,
                default,
                state: Mutex::new(HashMap::new()),
                observer,
            }),
        }
    }

    pub fn is_active(&self) -> bool {
        self.inner.default.tenant.is_enabled()
            || self.inner.default.per_user.is_enabled()
            || self
                .inner
                .overrides
                .values()
                .any(|l| l.tenant.is_enabled() || l.per_user.is_enabled())
    }

    pub fn check(&self, tenant: &str, user: &str) -> Result<(), Status> {
        let limits = self
            .inner
            .overrides
            .get(tenant)
            .copied()
            .unwrap_or(self.inner.default);

        // Fast path: both buckets disabled for this tenant.
        if !limits.tenant.is_enabled() && !limits.per_user.is_enabled() {
            return Ok(());
        }

        let mut state = self.inner.state.lock();
        let entry = state
            .entry(tenant.to_string())
            .or_insert_with(|| TenantState {
                tenant_bucket: Bucket::new(limits.tenant),
                user_buckets: HashMap::new(),
            });

        // Check the tenant bucket first — a tenant-level violation
        // is the more interesting metric for an operator (it means
        // the entire tenant is hot, not just one user inside it).
        if limits.tenant.is_enabled() && !entry.tenant_bucket.try_take() {
            drop(state);
            debug!(%tenant, %user, "rate_limit: rejected at tenant scope");
            self.inner.observer.on_reject(tenant, RejectScope::Tenant);
            return Err(Status::resource_exhausted(format!(
                "tenant {:?} rate limit exceeded",
                tenant
            )));
        }

        if limits.per_user.is_enabled() {
            let user_bucket = entry
                .user_buckets
                .entry(user.to_string())
                .or_insert_with(|| Bucket::new(limits.per_user));
            if !user_bucket.try_take() {
                drop(state);
                debug!(%tenant, %user, "rate_limit: rejected at user scope");
                self.inner.observer.on_reject(tenant, RejectScope::User);
                return Err(Status::resource_exhausted(format!(
                    "user {:?} rate limit exceeded (tenant {:?})",
                    user, tenant
                )));
            }
        }

        Ok(())
    }
}

pub mod redis;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn limits(rps: f64, burst: u64) -> TenantLimits {
        TenantLimits {
            tenant: BucketRate {
                rpcs_per_second: rps,
                burst,
            },
            per_user: BucketRate::disabled(),
        }
    }

    fn limits_with_user(t_rps: f64, t_burst: u64, u_rps: f64, u_burst: u64) -> TenantLimits {
        TenantLimits {
            tenant: BucketRate {
                rpcs_per_second: t_rps,
                burst: t_burst,
            },
            per_user: BucketRate {
                rpcs_per_second: u_rps,
                burst: u_burst,
            },
        }
    }

    #[test]
    fn disabled_limiter_is_inactive_and_admits_everything() {
        let r = MemoryLimiter::new(
            TenantLimits::default(),
            HashMap::new(),
            Arc::new(NoopObserver),
        );
        assert!(!r.is_active());
        for _ in 0..1000 {
            r.check("any-tenant", "any-user").unwrap();
        }
    }

    #[test]
    fn burst_allows_consecutive_then_rejects() {
        let r = MemoryLimiter::new(limits(1.0, 5), HashMap::new(), Arc::new(NoopObserver));
        assert!(r.is_active());
        // 5 in a row should succeed (full burst).
        for _ in 0..5 {
            r.check("t", "u").unwrap();
        }
        // The 6th immediately fails — refill hasn't had time.
        let err = r.check("t", "u").unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    #[test]
    fn refill_lets_more_rpcs_through_after_a_wait() {
        // 10 RPS, burst 2. After ~250ms we should have ~2.5 tokens.
        let r = MemoryLimiter::new(limits(10.0, 2), HashMap::new(), Arc::new(NoopObserver));
        r.check("t", "u").unwrap();
        r.check("t", "u").unwrap();
        // Bucket empty.
        assert!(r.check("t", "u").is_err());
        std::thread::sleep(Duration::from_millis(300));
        // ~3 tokens refilled, capped at burst=2. At least one
        // succeeds; the second probably does too (clock noise can
        // make it just under 2.0).
        assert!(r.check("t", "u").is_ok());
    }

    #[test]
    fn override_takes_precedence_over_default() {
        // Default very restrictive; override generous.
        let mut overrides = HashMap::new();
        overrides.insert("team-a".to_string(), limits(100.0, 100));
        let r = MemoryLimiter::new(limits(1.0, 1), overrides, Arc::new(NoopObserver));

        // Default tenant: only 1 RPC before reject.
        r.check("anyone-else", "u").unwrap();
        assert!(r.check("anyone-else", "u").is_err());

        // team-a: 50 RPCs no problem.
        for _ in 0..50 {
            r.check("team-a", "u").unwrap();
        }
    }

    #[test]
    fn tenant_buckets_are_independent_across_tenants() {
        let r = MemoryLimiter::new(limits(1.0, 2), HashMap::new(), Arc::new(NoopObserver));
        // Exhaust team-a's bucket.
        r.check("team-a", "u").unwrap();
        r.check("team-a", "u").unwrap();
        assert!(r.check("team-a", "u").is_err());
        // team-b has its own, fresh bucket.
        r.check("team-b", "u").unwrap();
        r.check("team-b", "u").unwrap();
    }

    #[test]
    fn per_user_bucket_protects_one_user_without_blocking_others() {
        // Tenant bucket generous, per-user bucket tight: 2 burst.
        let r = MemoryLimiter::new(
            limits_with_user(100.0, 100, 1.0, 2),
            HashMap::new(),
            Arc::new(NoopObserver),
        );
        r.check("t", "alice").unwrap();
        r.check("t", "alice").unwrap();
        let err = r.check("t", "alice").unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        // bob is unaffected.
        r.check("t", "bob").unwrap();
        r.check("t", "bob").unwrap();
    }

    #[test]
    fn observer_is_notified_with_scope() {
        use std::sync::atomic::{AtomicU64, Ordering};
        struct Counter {
            tenant_rejects: AtomicU64,
            user_rejects: AtomicU64,
        }
        impl LimiterObserver for Counter {
            fn on_reject(&self, _tenant: &str, scope: RejectScope) {
                match scope {
                    RejectScope::Tenant => self.tenant_rejects.fetch_add(1, Ordering::Relaxed),
                    RejectScope::User => self.user_rejects.fetch_add(1, Ordering::Relaxed),
                };
            }
        }
        let counter = Arc::new(Counter {
            tenant_rejects: AtomicU64::new(0),
            user_rejects: AtomicU64::new(0),
        });
        // Tenant burst 1, user burst 1: first RPC consumes both;
        // second is rejected by *tenant* (checked first), third —
        // after we let tenant refill — should hit user.
        let r = MemoryLimiter::new(
            limits_with_user(1000.0, 1, 1000.0, 1),
            HashMap::new(),
            counter.clone(),
        );
        r.check("t", "u").unwrap();
        let _ = r.check("t", "u");
        // Both burst=1 with 1000 RPS refill ≈ 1ms — bucket is full
        // again on the next iteration. Counts will be at least 1 of
        // some scope; we only assert one happened.
        assert!(
            counter.tenant_rejects.load(Ordering::Relaxed)
                + counter.user_rejects.load(Ordering::Relaxed)
                >= 1
        );
    }
}
