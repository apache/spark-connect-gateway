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

//! In-process `AffinityStore` for single-replica deployments and
//! tests. For multi-replica HA use `scg-store-redis` — the
//! `AffinityStore` trait is kept narrow specifically so swapping
//! the backing store does not ripple into the proxy layer.

use parking_lot::RwLock;
use scg_routing::{AffinityStore, SessionKey};
use std::collections::HashMap;

#[derive(Default)]
pub struct MemoryStore {
    sessions: RwLock<HashMap<SessionKey, String>>,
    ops: RwLock<HashMap<String, String>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl AffinityStore for MemoryStore {
    async fn lookup_session(&self, key: &SessionKey) -> Option<String> {
        self.sessions.read().get(key).cloned()
    }

    async fn bind_session_if_absent(&self, key: SessionKey, backend: String) -> String {
        let mut g = self.sessions.write();
        g.entry(key).or_insert(backend).clone()
    }

    async fn forget_session(&self, key: &SessionKey) {
        self.sessions.write().remove(key);
    }

    async fn lookup_op(&self, op_id: &str) -> Option<String> {
        self.ops.read().get(op_id).cloned()
    }

    async fn bind_op(&self, op_id: String, backend: String) {
        self.ops.write().insert(op_id, backend);
    }

    async fn forget_op(&self, op_id: &str) {
        self.ops.write().remove(op_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn session_stickiness_invariant() {
        let s = MemoryStore::new();
        let k = SessionKey::new("alice", "sess-1");
        assert!(s.lookup_session(&k).await.is_none());
        let first = s
            .bind_session_if_absent(k.clone(), "be-a:15002".into())
            .await;
        assert_eq!(first, "be-a:15002");
        // Re-binding must not move an existing session — stickiness invariant.
        let second = s
            .bind_session_if_absent(k.clone(), "be-b:15002".into())
            .await;
        assert_eq!(second, "be-a:15002");
        s.forget_session(&k).await;
        assert!(s.lookup_session(&k).await.is_none());
    }

    #[tokio::test]
    async fn op_reverse_index() {
        let s = MemoryStore::new();
        assert!(s.lookup_op("op-1").await.is_none());
        s.bind_op("op-1".into(), "be-a:15002".into()).await;
        assert_eq!(s.lookup_op("op-1").await.as_deref(), Some("be-a:15002"));
        s.forget_op("op-1").await;
        assert!(s.lookup_op("op-1").await.is_none());
    }
}
