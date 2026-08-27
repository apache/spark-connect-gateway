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

//! Smoke test against a real local Redis. Same scenarios as the
//! testcontainers-backed integration tests but without Docker — when
//! Docker Hub is unreachable, run a Redis on `:6399` (e.g.
//! `redis-server --port 6399 --daemonize yes`) and execute:
//!
//! ```bash
//! cargo run -p scg-store-redis --example smoke
//! ```
//!
//! Exits with non-zero status if any invariant fails so CI / scripts
//! can wrap it.

use std::time::Duration;

use scg_routing::{AffinityStore, SessionKey};
use scg_store_redis::{RedisStore, RedisStoreConfig};

#[tokio::main]
async fn main() {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6399".into());
    eprintln!("[smoke] connecting to {}", url);

    // Use a unique key prefix per run so re-runs don't pollute each other.
    let prefix = format!("scg-smoke-{}", std::process::id());
    let cfg = RedisStoreConfig {
        url,
        key_prefix: prefix,
        session_ttl: Duration::from_secs(30),
        op_ttl: Duration::from_secs(30),
    };
    let store = RedisStore::connect(cfg.clone())
        .await
        .expect("connect to redis");

    // 1. Bind / resolve / forget roundtrip.
    let k = SessionKey::new("alice", "sess-1");
    assert!(store.lookup_session(&k).await.is_none(), "fresh key");
    let bound = store
        .bind_session_if_absent(k.clone(), "be-a:15002".into())
        .await;
    assert_eq!(bound, "be-a:15002");
    assert_eq!(
        store.lookup_session(&k).await.as_deref(),
        Some("be-a:15002")
    );
    eprintln!("[smoke] ok: bind+resolve roundtrip");

    // 2. Atomicity / stickiness invariant.
    let second = store
        .bind_session_if_absent(k.clone(), "be-b:1".into())
        .await;
    assert_eq!(
        second, "be-a:15002",
        "stickiness invariant: first write wins"
    );
    eprintln!("[smoke] ok: bind_session_if_absent stickiness");

    // 3. Op-id reverse index.
    assert!(store.lookup_op("op-1").await.is_none());
    store.bind_op("op-1".into(), "be-a:15002".into()).await;
    assert_eq!(store.lookup_op("op-1").await.as_deref(), Some("be-a:15002"));
    store.forget_op("op-1").await;
    assert!(store.lookup_op("op-1").await.is_none());
    eprintln!("[smoke] ok: op-id reverse index");

    // 4. Two stores against the same Redis observe each other's writes
    //    — the multi-replica claim.
    let store_b = RedisStore::connect(cfg.clone()).await.expect("connect b");
    let k2 = SessionKey::new("alice", "shared");
    let _ = store
        .bind_session_if_absent(k2.clone(), "be-x:1".into())
        .await;
    assert_eq!(
        store_b.lookup_session(&k2).await.as_deref(),
        Some("be-x:1"),
        "replica B must see replica A's binding"
    );
    let race = store_b
        .bind_session_if_absent(k2.clone(), "be-y:1".into())
        .await;
    assert_eq!(race, "be-x:1", "replica A's earlier write wins");
    eprintln!("[smoke] ok: cross-replica state visibility + atomicity");

    // 5. TTL expiry — short TTL store.
    let short_cfg = RedisStoreConfig {
        session_ttl: Duration::from_secs(1),
        op_ttl: Duration::from_secs(1),
        key_prefix: format!("scg-smoke-ttl-{}", std::process::id()),
        ..cfg.clone()
    };
    let short = RedisStore::connect(short_cfg).await.expect("connect short");
    let k3 = SessionKey::new("u", "ttl");
    short
        .bind_session_if_absent(k3.clone(), "be-z:1".into())
        .await;
    assert!(short.lookup_session(&k3).await.is_some());
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        short.lookup_session(&k3).await.is_none(),
        "TTL-bound key must expire"
    );
    eprintln!("[smoke] ok: TTL expiry");

    // Cleanup — forget the persistent keys so the Redis instance stays
    // tidy for the next smoke run on a shared host.
    store.forget_session(&k).await;
    store.forget_session(&k2).await;

    println!("[smoke] all invariants passed");
}
