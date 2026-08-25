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

//! Integration tests for the Redis-backed [`RedisLimiter`].
//!
//! Spins up a real Redis via testcontainers and drives the limiter
//! against it. Requires a working Docker daemon; tests are marked
//! `#[ignore]` so plain `cargo test --workspace` doesn't fail in
//! Docker-less environments. Run deliberately with:
//!
//! ```bash
//! cargo test -p scg-ratelimit -- --ignored
//! ```
//!
//! Coverage:
//!
//! * Burst-then-reject across the bucket capacity.
//! * Two tenants are independent (one exhausting doesn't affect
//!   the other).
//! * Two-replica simulation: two `RedisLimiter` instances pointing
//!   at the same Redis enforce a shared bucket — what an in-memory
//!   limiter explicitly does *not* do.
//! * Fail-mode semantics when Redis is unreachable: `Open` admits,
//!   `Closed` rejects.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use scg_ratelimit::redis::{RedisLimiter, RedisLimiterConfig};
use scg_ratelimit::{
    BucketRate, FailMode, LimiterObserver, NoopRedisErrorObserver, RedisErrorObserver, RejectScope,
    TenantLimits,
};
use testcontainers_modules::{
    redis::{Redis, REDIS_PORT},
    testcontainers::{runners::AsyncRunner, ContainerAsync},
};

#[derive(Default)]
struct Counter {
    tenant: AtomicU64,
    user: AtomicU64,
}

impl LimiterObserver for Counter {
    fn on_reject(&self, _tenant: &str, scope: RejectScope) {
        match scope {
            RejectScope::Tenant => self.tenant.fetch_add(1, Ordering::Relaxed),
            RejectScope::User => self.user.fetch_add(1, Ordering::Relaxed),
        };
    }
}

#[derive(Default)]
struct RedisErrCounter {
    errors: AtomicU64,
}

impl RedisErrorObserver for RedisErrCounter {
    fn on_redis_error(&self, _: &str, _: &'static str) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
}

/// Start a real Redis container, return its URL plus the container
/// handle (drop to stop). Container start can fail if Docker isn't
/// running — that surfaces as the `Err` from `start`.
async fn spawn_redis() -> (String, ContainerAsync<Redis>) {
    let node = Redis::default().start().await.expect("start redis");
    let host = node.get_host().await.expect("redis host");
    let port = node
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("redis port");
    let url = format!("redis://{}:{}", host, port);
    (url, node)
}

fn limits(rps: f64, burst: u64) -> TenantLimits {
    TenantLimits {
        tenant: BucketRate {
            rpcs_per_second: rps,
            burst,
        },
        per_user: BucketRate::disabled(),
    }
}

/// Build a limiter bound to the given URL with the given per-tenant
/// limits as the default.
async fn limiter_for(url: &str, default: TenantLimits, fail_mode: FailMode) -> RedisLimiter {
    let cfg = RedisLimiterConfig {
        url: url.into(),
        key_prefix: format!("scg-rl-test-{}", rand_suffix()),
        key_ttl: Duration::from_secs(30),
        fail_mode,
    };
    let observer = Arc::new(Counter::default());
    let err_obs: Arc<dyn RedisErrorObserver> = Arc::new(NoopRedisErrorObserver);
    RedisLimiter::connect(cfg, default, HashMap::new(), observer, err_obs)
        .await
        .expect("connect redis limiter")
}

/// Tests share a Redis instance; each test gets its own key prefix
/// so they don't collide.
fn rand_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| format!("{}-{}", d.as_nanos(), std::process::id()))
        .unwrap_or_else(|_| "0".into())
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn burst_admits_then_rejects() {
    let (url, _node) = spawn_redis().await;
    // 1 RPS, burst 5: 5 admits, 6th rejects (refill < 1 token).
    let l = limiter_for(&url, limits(1.0, 5), FailMode::Open).await;
    for _ in 0..5 {
        l.check("t", "u").await.expect("within burst");
    }
    let err = l.check("t", "u").await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn tenants_are_independent() {
    let (url, _node) = spawn_redis().await;
    let l = limiter_for(&url, limits(1.0, 2), FailMode::Open).await;

    // Exhaust team-a.
    l.check("team-a", "u").await.unwrap();
    l.check("team-a", "u").await.unwrap();
    assert!(l.check("team-a", "u").await.is_err());

    // team-b's bucket is independent.
    l.check("team-b", "u").await.unwrap();
    l.check("team-b", "u").await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn two_replicas_share_the_bucket() {
    // The whole point of the Redis backend: an in-memory limiter
    // would double the effective quota when you double the
    // replicas. The Redis one must not.
    let (url, _node) = spawn_redis().await;

    // Share a *fixed* key prefix between both limiters so they
    // actually contend on the same Redis keys.
    let cfg = |fm| RedisLimiterConfig {
        url: url.clone(),
        key_prefix: "scg-rl-test-shared".into(),
        key_ttl: Duration::from_secs(30),
        fail_mode: fm,
    };
    let obs = Arc::new(Counter::default());
    let err_obs: Arc<dyn RedisErrorObserver> = Arc::new(NoopRedisErrorObserver);
    let l1 = RedisLimiter::connect(
        cfg(FailMode::Open),
        limits(1.0, 3),
        HashMap::new(),
        obs.clone(),
        err_obs.clone(),
    )
    .await
    .unwrap();
    let l2 = RedisLimiter::connect(
        cfg(FailMode::Open),
        limits(1.0, 3),
        HashMap::new(),
        obs.clone(),
        err_obs,
    )
    .await
    .unwrap();

    // Burst=3 shared. Two RPCs through replica 1, two through
    // replica 2: the 4th overall must reject regardless of which
    // replica it hits.
    l1.check("shared-t", "u").await.unwrap();
    l1.check("shared-t", "u").await.unwrap();
    l2.check("shared-t", "u").await.unwrap();
    let err = l2.check("shared-t", "u").await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn refill_admits_after_wait() {
    let (url, _node) = spawn_redis().await;
    // 10 RPS, burst 2 — ~200ms gives us 2 fresh tokens.
    let l = limiter_for(&url, limits(10.0, 2), FailMode::Open).await;
    l.check("t", "u").await.unwrap();
    l.check("t", "u").await.unwrap();
    assert!(l.check("t", "u").await.is_err());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(l.check("t", "u").await.is_ok());
}

#[tokio::test]
async fn fail_open_admits_when_redis_unreachable() {
    // Connect against a port we know is closed. The connect itself
    // may succeed in the lazy `ConnectionManager` path — what
    // matters is that `check` does not return an error.
    let cfg = RedisLimiterConfig {
        // Reserved port that should be closed.
        url: "redis://127.0.0.1:1".into(),
        key_prefix: "scg-rl-test-failopen".into(),
        key_ttl: Duration::from_secs(30),
        fail_mode: FailMode::Open,
    };
    let obs = Arc::new(Counter::default());
    let err_obs = Arc::new(RedisErrCounter::default());
    // ConnectionManager may still succeed here (it's lazy); skip
    // the test if connect itself fails because that would be the
    // boot-time error path, not the per-RPC fail-mode path.
    let limiter = match RedisLimiter::connect(
        cfg,
        limits(1.0, 1),
        HashMap::new(),
        obs.clone(),
        err_obs.clone(),
    )
    .await
    {
        Ok(l) => l,
        Err(_) => return,
    };

    // The check must not return an error — fail-open admits.
    let res = limiter.check("t", "u").await;
    assert!(res.is_ok(), "fail-open should admit, got {:?}", res);
    // …and the error metric should fire.
    assert!(err_obs.errors.load(Ordering::Relaxed) >= 1);
}

#[tokio::test]
async fn fail_closed_rejects_when_redis_unreachable() {
    let cfg = RedisLimiterConfig {
        url: "redis://127.0.0.1:1".into(),
        key_prefix: "scg-rl-test-failclosed".into(),
        key_ttl: Duration::from_secs(30),
        fail_mode: FailMode::Closed,
    };
    let obs = Arc::new(Counter::default());
    let err_obs = Arc::new(RedisErrCounter::default());
    let limiter = match RedisLimiter::connect(
        cfg,
        limits(1.0, 1),
        HashMap::new(),
        obs.clone(),
        err_obs.clone(),
    )
    .await
    {
        Ok(l) => l,
        Err(_) => return,
    };

    let err = limiter.check("t", "u").await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    assert!(err_obs.errors.load(Ordering::Relaxed) >= 1);
}
