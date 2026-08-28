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

//! Integration tests for [`scg_store_redis::RedisStore`] against a
//! real Redis spun up via testcontainers.
//!
//! These tests require a working Docker daemon. CI environments
//! without Docker can either skip them (cargo test will simply error
//! on container start, surfaced as an `Err` from `start()`) or run
//! `cargo test -p scg-store-redis --features ...` once we add an
//! explicit feature gate. For now we mark them `#[ignore]` so a
//! plain `cargo test --workspace` doesn't fail in environments where
//! Docker isn't around. Run them deliberately with:
//!
//! ```bash
//! cargo test -p scg-store-redis -- --ignored
//! ```

use std::time::Duration;

use scg_routing::{AffinityStore, SessionKey};
use scg_store_redis::{RedisStore, RedisStoreConfig};
use testcontainers_modules::{
    redis::{Redis, REDIS_PORT},
    testcontainers::{runners::AsyncRunner, ContainerAsync},
};

/// Spin up a Redis container and connect a fresh `RedisStore` to it.
/// The returned `_node` keeps the container alive for the test
/// duration; drop it (end of test) and the container goes away.
async fn rig() -> (RedisStore, ContainerAsync<Redis>) {
    let node = Redis::default().start().await.expect("start redis");
    let host = node.get_host().await.expect("host");
    let port = node
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("host port");
    let cfg = RedisStoreConfig {
        url: format!("redis://{}:{}", host, port),
        // Keep TTLs comfortable so the test doesn't race the clock.
        session_ttl: Duration::from_secs(30),
        op_ttl: Duration::from_secs(30),
        ..Default::default()
    };
    let store = RedisStore::connect(cfg).await.expect("connect redis");
    (store, node)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn session_bind_resolve_forget_roundtrip() {
    let (store, _node) = rig().await;
    let k = SessionKey::new("alice", "sess-1");

    assert!(store.lookup_session(&k).await.is_none());
    let bound = store
        .bind_session_if_absent(k.clone(), "be-a:15002".into())
        .await;
    assert_eq!(bound, "be-a:15002");
    assert_eq!(
        store.lookup_session(&k).await.as_deref(),
        Some("be-a:15002")
    );

    store.forget_session(&k).await;
    assert!(store.lookup_session(&k).await.is_none());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn bind_session_if_absent_is_atomic() {
    let (store, _node) = rig().await;
    let k = SessionKey::new("alice", "sticky");

    let first = store
        .bind_session_if_absent(k.clone(), "be-a:1".into())
        .await;
    assert_eq!(first, "be-a:1");
    // A second concurrent caller racing to bind a different value must
    // observe the first winner — this is the stickiness invariant.
    let second = store
        .bind_session_if_absent(k.clone(), "be-b:1".into())
        .await;
    assert_eq!(second, "be-a:1");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn op_reverse_index_roundtrip() {
    let (store, _node) = rig().await;
    assert!(store.lookup_op("op-1").await.is_none());
    store.bind_op("op-1".into(), "be-a:1".into()).await;
    assert_eq!(store.lookup_op("op-1").await.as_deref(), Some("be-a:1"));
    store.forget_op("op-1").await;
    assert!(store.lookup_op("op-1").await.is_none());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn two_stores_against_same_redis_share_state() {
    // The whole point of Redis state: replica A binds, replica B
    // resolves to the same backend. Without this we have no HA.
    let node = Redis::default().start().await.expect("start redis");
    let host = node.get_host().await.expect("host");
    let port = node
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("host port");
    let url = format!("redis://{}:{}", host, port);

    let store_a = RedisStore::connect(RedisStoreConfig {
        url: url.clone(),
        ..Default::default()
    })
    .await
    .expect("connect a");
    let store_b = RedisStore::connect(RedisStoreConfig {
        url,
        ..Default::default()
    })
    .await
    .expect("connect b");

    let k = SessionKey::new("alice", "shared");
    let a_bound = store_a
        .bind_session_if_absent(k.clone(), "be-a:1".into())
        .await;
    assert_eq!(a_bound, "be-a:1");

    // Replica B must see the same binding without ever calling bind.
    assert_eq!(store_b.lookup_session(&k).await.as_deref(), Some("be-a:1"));

    // And if replica B races to bind a different backend, replica A's
    // earlier write wins.
    let b_bound = store_b
        .bind_session_if_absent(k.clone(), "be-b:1".into())
        .await;
    assert_eq!(b_bound, "be-a:1");
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn session_ttl_expires() {
    let node = Redis::default().start().await.expect("start redis");
    let host = node.get_host().await.expect("host");
    let port = node
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("host port");
    let store = RedisStore::connect(RedisStoreConfig {
        url: format!("redis://{}:{}", host, port),
        // 1-second TTL so this test stays under a few seconds.
        session_ttl: Duration::from_secs(1),
        op_ttl: Duration::from_secs(1),
        ..Default::default()
    })
    .await
    .expect("connect");

    let k = SessionKey::new("u", "ttl");
    store
        .bind_session_if_absent(k.clone(), "be-a:1".into())
        .await;
    assert!(store.lookup_session(&k).await.is_some());

    // Wait long enough for Redis to expire the key. Each lookup also
    // refreshes the TTL, so we deliberately don't call lookup during
    // the wait.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(store.lookup_session(&k).await.is_none());
}
