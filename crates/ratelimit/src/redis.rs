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

//! Redis-backed distributed token bucket.
//!
//! The in-memory limiter in the parent module enforces quotas per
//! *gateway replica*. A 3-replica deployment with `rpcsPerSecond: 100`
//! actually admits up to 300 RPS cluster-wide before any throttling
//! fires — fine for back-pressure, wrong for a strict-SaaS quota.
//! This module shares the bucket state in Redis so all replicas
//! enforce the same numbers.
//!
//! ## Algorithm: token bucket via Lua
//!
//! Each `(tenant, user)` pair is one Redis hash with two fields:
//!
//! * `tokens` — floating-point token balance.
//! * `ts` — last-refill timestamp in milliseconds since the Unix epoch.
//!
//! Every RPC runs [`TOKEN_BUCKET_SCRIPT`] via `EVAL` (or `EVALSHA`
//! once cached). The Lua does the entire refill-and-take atomically
//! under Redis's single-threaded execution, so no two replicas can
//! ever observe the same `tokens` value and both decrement it. The
//! script returns `1` on admit, `0` on reject, which the Rust caller
//! turns into an `Ok(()) / ResourceExhausted` pair.
//!
//! ## Why not `redis-cell`?
//!
//! `CL.THROTTLE` from the redis-cell module is a one-line GCRA
//! implementation that we'd happily use — except it requires
//! `loadmodule redis-cell.so`, which most managed Redis offerings
//! (ElastiCache, MemoryStore, Upstash) don't permit. Sticking to
//! plain `EVAL` keeps the chart usable everywhere.
//!
//! ## Fail mode
//!
//! When Redis is unreachable, [`RedisLimiter::check`] consults
//! [`FailMode`]:
//!
//! * `Open` (default) — admit the RPC. Availability over strict
//!   quota; matches the Redis affinity-store's fail-soft
//!   behaviour. The `redis_error_observer` is notified so operators
//!   can alert on a sustained nonzero rate.
//! * `Closed` — reject the RPC. Use when a Redis outage must not
//!   become a quota-bypass attack vector. Makes Redis a hard
//!   dependency of every RPC.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ::redis::aio::ConnectionManager;
use ::redis::{RedisError, Script};
use tokio::sync::Mutex;
use tonic::Status;
use tracing::{debug, warn};

use crate::{BucketRate, FailMode, LimiterObserver, RedisErrorObserver, RejectScope, TenantLimits};

/// Token-bucket Lua script. Inputs and outputs:
///
/// * `KEYS[1]` — the bucket hash key (e.g. `scg-rl:t:team-a`)
/// * `ARGV[1]` — refill rate (tokens/second, float)
/// * `ARGV[2]` — bucket capacity (max tokens, float)
/// * `ARGV[3]` — current time in milliseconds (we send it from Rust
///   instead of using `TIME` so the test can inject deterministic
///   clocks; production uses `now()` in `RedisLimiter::check`)
/// * `ARGV[4]` — TTL for the key in seconds (so abandoned buckets
///   GC themselves; default to a generous multiple of the refill
///   period)
///
/// Returns `1` when a token was consumed, `0` otherwise.
///
/// The script is idempotent across retries only in the sense that
/// each `EVAL` is a single atomic operation — there is no "undo"
/// for an admitted call. Callers that retry on transient network
/// errors will *not* double-count rejections.
pub(crate) const TOKEN_BUCKET_SCRIPT: &str = r#"
local key      = KEYS[1]
local rate     = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now_ms   = tonumber(ARGV[3])
local ttl      = tonumber(ARGV[4])

local data    = redis.call('HMGET', key, 'tokens', 'ts')
local tokens  = tonumber(data[1])
local last_ms = tonumber(data[2])

if tokens == nil then
    -- Fresh bucket starts full so a new tenant gets its burst.
    tokens  = capacity
    last_ms = now_ms
end

-- Refill based on wall-clock elapsed; cap at capacity.
local elapsed_s = math.max(0, (now_ms - last_ms) / 1000.0)
tokens = math.min(capacity, tokens + elapsed_s * rate)

local admitted = 0
if tokens >= 1.0 then
    tokens = tokens - 1.0
    admitted = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now_ms)
redis.call('EXPIRE', key, ttl)
return admitted
"#;

/// Configuration for the Redis-backed limiter. Keep it small —
/// extension points (key prefix, TTLs) are tuneable but rarely
/// need to be.
#[derive(Debug, Clone)]
pub struct RedisLimiterConfig {
    /// Redis URL (e.g. `redis://host:6379`). Reuses the same client
    /// flavour as `scg-store-redis`.
    pub url: String,
    /// All keys live under `{key_prefix}:t:*` (tenant bucket) and
    /// `{key_prefix}:u:*` (user bucket). Defaults to `"scg-rl"`.
    /// Use a different prefix from the affinity store so a `FLUSH`
    /// of one doesn't take out the other.
    pub key_prefix: String,
    /// Bucket-key TTL. Abandoned `(tenant, user)` pairs are
    /// GC'd by Redis after this. Defaults to one hour — long enough
    /// that an idle bucket survives a coffee break, short enough
    /// that a one-off CI test doesn't leak forever.
    pub key_ttl: Duration,
    /// What to do when Redis is unreachable. See [`FailMode`].
    pub fail_mode: FailMode,
}

impl Default for RedisLimiterConfig {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".into(),
            key_prefix: "scg-rl".into(),
            key_ttl: Duration::from_secs(3600),
            fail_mode: FailMode::Open,
        }
    }
}

/// Errors that surface during connection setup. Per-request errors
/// (timeouts, ConnectionRefused at runtime) don't bubble up — they
/// route through [`FailMode`] inside `check`.
#[derive(Debug, thiserror::Error)]
pub enum RedisLimiterError {
    #[error("redis connect failed: {0}")]
    Connect(#[from] RedisError),
}

/// Distributed rate limiter. Cheap to clone — connection + script
/// handle live behind `Arc`.
#[derive(Clone)]
pub struct RedisLimiter {
    inner: Arc<Inner>,
}

struct Inner {
    cfg: RedisLimiterConfig,
    /// Per-tenant config; everything not in `overrides` falls back
    /// to `default`. Same shape as the in-memory limiter.
    overrides: HashMap<String, TenantLimits>,
    default: TenantLimits,
    /// Shared async connection. `ConnectionManager` reconnects
    /// transparently after Redis hiccups; the inner `Mutex` is so
    /// `&self` can hand out a `&mut Connection` to issue commands.
    conn: Mutex<ConnectionManager>,
    /// Reject observer (same one the memory limiter uses) so metric
    /// labels stay consistent across the two backends.
    observer: Arc<dyn LimiterObserver>,
    /// Redis-error observer. Bumps a separate metric (errors are
    /// distinct from rejections — a fail-open RPC fires this
    /// without firing the reject observer).
    redis_error_observer: Arc<dyn RedisErrorObserver>,
    /// Pre-compiled Lua script. Redis caches it by SHA on first use;
    /// subsequent calls go through EVALSHA implicitly.
    script: Script,
}

impl RedisLimiter {
    /// Connect to Redis. Fails fast on misconfigured URL or
    /// unreachable host at *boot* time (operators should hear about
    /// these immediately). Per-request errors take the [`FailMode`]
    /// path instead.
    pub async fn connect(
        cfg: RedisLimiterConfig,
        default: TenantLimits,
        overrides: HashMap<String, TenantLimits>,
        observer: Arc<dyn LimiterObserver>,
        redis_error_observer: Arc<dyn RedisErrorObserver>,
    ) -> Result<Self, RedisLimiterError> {
        let client = ::redis::Client::open(cfg.url.clone())?;
        let conn = ConnectionManager::new(client).await?;
        let script = Script::new(TOKEN_BUCKET_SCRIPT);
        Ok(Self {
            inner: Arc::new(Inner {
                cfg,
                overrides,
                default,
                conn: Mutex::new(conn),
                observer,
                redis_error_observer,
                script,
            }),
        })
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

    fn tenant_key(&self, tenant: &str) -> String {
        format!("{}:t:{}", self.inner.cfg.key_prefix, tenant)
    }

    fn user_key(&self, tenant: &str, user: &str) -> String {
        format!("{}:u:{}|{}", self.inner.cfg.key_prefix, tenant, user)
    }

    /// Run the token-bucket script for a single bucket and return
    /// `Ok(true)` (admitted), `Ok(false)` (rejected), or
    /// `Err(redis_error)`. Callers translate that triple into the
    /// fail-mode + observer dance.
    async fn take_one(&self, key: String, rate: &BucketRate) -> Result<bool, RedisError> {
        let now_ms = current_time_ms();
        let ttl = self.inner.cfg.key_ttl.as_secs() as i64;
        let mut conn = self.inner.conn.lock().await;
        let admitted: i64 = self
            .inner
            .script
            .key(&key)
            .arg(rate.rpcs_per_second)
            .arg(rate.burst as f64)
            .arg(now_ms)
            .arg(ttl)
            .invoke_async(&mut *conn)
            .await?;
        Ok(admitted == 1)
    }

    /// Tenant-then-user bucket check. Returns Ok(()) on admit, or
    /// `ResourceExhausted` on reject. Redis errors route through
    /// `FailMode`.
    pub async fn check(&self, tenant: &str, user: &str) -> Result<(), Status> {
        let limits = self
            .inner
            .overrides
            .get(tenant)
            .copied()
            .unwrap_or(self.inner.default);

        if !limits.tenant.is_enabled() && !limits.per_user.is_enabled() {
            return Ok(());
        }

        // Tenant bucket first: a tenant-wide reject is the more
        // diagnostically useful signal for an operator.
        if limits.tenant.is_enabled() {
            match self.take_one(self.tenant_key(tenant), &limits.tenant).await {
                Ok(true) => {}
                Ok(false) => {
                    debug!(%tenant, %user, "rate_limit(redis): rejected at tenant scope");
                    self.inner.observer.on_reject(tenant, RejectScope::Tenant);
                    return Err(Status::resource_exhausted(format!(
                        "tenant {:?} rate limit exceeded",
                        tenant
                    )));
                }
                Err(e) => {
                    if let Some(s) = self.handle_redis_error(tenant, "tenant_bucket", e) {
                        return Err(s);
                    }
                    // fail-open: fall through to the user check.
                }
            }
        }

        if limits.per_user.is_enabled() {
            match self
                .take_one(self.user_key(tenant, user), &limits.per_user)
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    debug!(%tenant, %user, "rate_limit(redis): rejected at user scope");
                    self.inner.observer.on_reject(tenant, RejectScope::User);
                    return Err(Status::resource_exhausted(format!(
                        "user {:?} rate limit exceeded (tenant {:?})",
                        user, tenant
                    )));
                }
                Err(e) => {
                    if let Some(s) = self.handle_redis_error(tenant, "user_bucket", e) {
                        return Err(s);
                    }
                }
            }
        }

        Ok(())
    }

    /// Centralised fail-mode routing. Returns `Some(Status)` when
    /// the caller should reject, `None` when it should keep going
    /// (fail-open). Bumps the error observer either way.
    fn handle_redis_error(
        &self,
        tenant: &str,
        reason: &'static str,
        err: RedisError,
    ) -> Option<Status> {
        warn!(%tenant, reason, error = %err, "rate_limit(redis): backend error");
        self.inner
            .redis_error_observer
            .on_redis_error(tenant, reason);
        match self.inner.cfg.fail_mode {
            FailMode::Open => None,
            FailMode::Closed => Some(Status::resource_exhausted(
                "rate limiter unavailable (fail-closed)",
            )),
        }
    }
}

fn current_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
