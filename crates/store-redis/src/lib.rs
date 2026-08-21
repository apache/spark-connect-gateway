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

//! Redis-backed [`AffinityStore`] for cross-replica HA.
//!
//! The sibling [`scg-store-memory::MemoryStore`] only works when the
//! gateway runs as a single replica. When operators want to scale
//! out (Helm chart with `replicas > 1` and `kind: Deployment`), the
//! affinity table needs to live somewhere all replicas can see —
//! otherwise a Spark Connect session pinned to backend `B` by replica
//! 1 will be re-pinned to a different backend by replica 2 the next
//! time the client lands on it, breaking the
//! `(user_id, session_id) -> backend` invariant.
//!
//! Schema in Redis:
//!
//! * `{prefix}:s:{user_id}|{session_id}` -> backend address. TTL
//!   refreshed on every successful resolve, so idle sessions
//!   eventually fall out and let the gateway re-pick.
//! * `{prefix}:o:{op_id}` -> backend address. TTL is independent and
//!   typically shorter — operations are bounded by the lifetime of an
//!   `ExecutePlan` invocation.
//!
//! Concurrency invariant for sticky routing relies on `SET ... NX
//! EX`: if two gateway replicas race to bind the same `(user_id,
//! session_id)`, exactly one `SET` succeeds; the other reads the
//! winner's value back and returns it from `bind_session_if_absent`.
//!
//! Error policy: this layer does not propagate Redis failures up the
//! `AffinityStore` trait (the trait is intentionally infallible to
//! keep `Router` simple). On a Redis outage, lookups return `None`
//! and binds quietly drop, which makes the gateway degrade to
//! pool-only routing — sessions land on whatever backend the pool
//! picks each time, the same behaviour you'd see with no affinity
//! cache at all. We log a `warn!` so the outage shows up in logs.

use std::time::Duration;

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use scg_routing::{AffinityStore, SessionKey};
use tokio::sync::Mutex;
use tracing::warn;

#[derive(Debug, thiserror::Error)]
pub enum RedisStoreError {
    #[error("opening redis client: {0}")]
    Open(#[from] redis::RedisError),
}

/// Configuration for [`RedisStore`].
#[derive(Debug, Clone)]
pub struct RedisStoreConfig {
    /// `redis://` URL (e.g. `redis://localhost:6379` or
    /// `redis://:password@host:6379/2`).
    pub url: String,
    /// Key prefix; all keys live under `{prefix}:s:` and `{prefix}:o:`.
    /// Lets multiple gateway deployments share a Redis without
    /// colliding.
    pub key_prefix: String,
    /// TTL for session bindings. Refreshed on every read. Pick a value
    /// noticeably longer than typical client idle timeouts so
    /// reconnecting clients keep their stickiness.
    pub session_ttl: Duration,
    /// TTL for op-id bindings. Operations are bounded by their
    /// `ExecutePlan` lifetime; a few minutes is usually plenty.
    pub op_ttl: Duration,
}

impl Default for RedisStoreConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".into(),
            key_prefix: "scg".into(),
            session_ttl: Duration::from_secs(60 * 60), // 1h
            op_ttl: Duration::from_secs(15 * 60),      // 15min
        }
    }
}

pub struct RedisStore {
    conn: Mutex<ConnectionManager>,
    cfg: RedisStoreConfig,
}

impl RedisStore {
    /// Open a connection to Redis. On a successful return the
    /// `ConnectionManager` is alive but lazy — actual TCP work
    /// happens on the first command. Failures here are usually
    /// configuration / DNS errors, not "Redis is down".
    pub async fn connect(cfg: RedisStoreConfig) -> Result<Self, RedisStoreError> {
        let client = redis::Client::open(cfg.url.clone())?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self {
            conn: Mutex::new(conn),
            cfg,
        })
    }

    fn session_key(&self, k: &SessionKey) -> String {
        // {prefix}:s:{tenant}|{user_id}|{session_id}
        format!(
            "{}:s:{}|{}|{}",
            self.cfg.key_prefix, k.tenant, k.user_id, k.session_id
        )
    }

    fn op_key(&self, op_id: &str) -> String {
        format!("{}:o:{}", self.cfg.key_prefix, op_id)
    }
}

#[async_trait]
impl AffinityStore for RedisStore {
    async fn lookup_session(&self, key: &SessionKey) -> Option<String> {
        let k = self.session_key(key);
        let mut conn = self.conn.lock().await;
        // GET + EXPIRE in two round-trips. Could be combined with
        // GETEX (Redis 6.2+) once we want to drop the round-trip.
        match conn.get::<_, Option<String>>(&k).await {
            Ok(Some(v)) => {
                let _: Result<bool, _> =
                    conn.expire(&k, self.cfg.session_ttl.as_secs() as i64).await;
                Some(v)
            }
            Ok(None) => None,
            Err(e) => {
                warn!(error = %e, key = %k, "redis: lookup_session failed; treating as miss");
                None
            }
        }
    }

    async fn bind_session_if_absent(&self, key: SessionKey, backend: String) -> String {
        let k = self.session_key(&key);
        let mut conn = self.conn.lock().await;
        let ttl_secs = self.cfg.session_ttl.as_secs();
        // SET NX EX — atomic create-if-absent with TTL.
        match redis::cmd("SET")
            .arg(&k)
            .arg(&backend)
            .arg("NX")
            .arg("EX")
            .arg(ttl_secs)
            .query_async::<Option<String>>(&mut *conn)
            .await
        {
            Ok(Some(_)) => backend, // SET returned "OK" → we won the race.
            Ok(None) => {
                // SET returned nil → key already existed; read it.
                match conn.get::<_, Option<String>>(&k).await {
                    Ok(Some(existing)) => existing,
                    // The race-tie window where the existing binding
                    // expired between SET NX and GET is rare enough to
                    // accept the rebind; just return our intended value.
                    Ok(None) => backend,
                    Err(e) => {
                        warn!(error = %e, key = %k, "redis: GET after SET-NX miss failed; rebinding");
                        backend
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, key = %k, "redis: bind_session_if_absent failed; degrading to pool pick");
                // Redis is down. The router will use the backend we
                // were handed (the pool's pick); subsequent calls
                // will pick again until Redis recovers.
                backend
            }
        }
    }

    async fn forget_session(&self, key: &SessionKey) {
        let k = self.session_key(key);
        let mut conn = self.conn.lock().await;
        if let Err(e) = conn.del::<_, i64>(&k).await {
            warn!(error = %e, key = %k, "redis: forget_session failed");
        }
    }

    async fn lookup_op(&self, op_id: &str) -> Option<String> {
        let k = self.op_key(op_id);
        let mut conn = self.conn.lock().await;
        match conn.get::<_, Option<String>>(&k).await {
            Ok(Some(v)) => {
                let _: Result<bool, _> = conn.expire(&k, self.cfg.op_ttl.as_secs() as i64).await;
                Some(v)
            }
            Ok(None) => None,
            Err(e) => {
                warn!(error = %e, key = %k, "redis: lookup_op failed; treating as miss");
                None
            }
        }
    }

    async fn bind_op(&self, op_id: String, backend: String) {
        let k = self.op_key(&op_id);
        let mut conn = self.conn.lock().await;
        let ttl_secs = self.cfg.op_ttl.as_secs();
        if let Err(e) = redis::cmd("SET")
            .arg(&k)
            .arg(&backend)
            .arg("EX")
            .arg(ttl_secs)
            .query_async::<()>(&mut *conn)
            .await
        {
            warn!(error = %e, key = %k, "redis: bind_op failed");
        }
    }

    async fn forget_op(&self, op_id: &str) {
        let k = self.op_key(op_id);
        let mut conn = self.conn.lock().await;
        if let Err(e) = conn.del::<_, i64>(&k).await {
            warn!(error = %e, key = %k, "redis: forget_op failed");
        }
    }
}
