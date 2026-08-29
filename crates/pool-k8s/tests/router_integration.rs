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

//! Integration test: K8sPool wired into the Router exhibits the same
//! stickiness invariants as the static pool, and gracefully reports
//! "no backend" when the watcher hasn't populated yet.
//!
//! We don't talk to a real K8s API server here; the watcher is mocked
//! by calling K8sPool::set_backends() directly. The point is to prove
//! that Pool::pick() returning None propagates correctly through
//! Router and that, once the pool is populated, sticky routing works.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use scg_pool_k8s::K8sPool;
use scg_routing::{AffinityStore, Router, SessionKey};

#[derive(Default)]
struct StubStore {
    sessions: Mutex<HashMap<SessionKey, String>>,
    ops: Mutex<HashMap<String, String>>,
}

#[async_trait::async_trait]
impl AffinityStore for StubStore {
    async fn lookup_session(&self, k: &SessionKey) -> Option<String> {
        self.sessions.lock().get(k).cloned()
    }
    async fn bind_session_if_absent(&self, k: SessionKey, v: String) -> String {
        self.sessions.lock().entry(k).or_insert(v).clone()
    }
    async fn forget_session(&self, k: &SessionKey) {
        self.sessions.lock().remove(k);
    }
    async fn lookup_op(&self, o: &str) -> Option<String> {
        self.ops.lock().get(o).cloned()
    }
    async fn bind_op(&self, o: String, v: String) {
        self.ops.lock().insert(o, v);
    }
    async fn forget_op(&self, o: &str) {
        self.ops.lock().remove(o);
    }
}

#[tokio::test]
async fn empty_pool_propagates_none_through_router() {
    let pool = Arc::new(K8sPool::new());
    let router = Router::single_pool(pool.clone(), Arc::new(StubStore::default()));
    let key = SessionKey::new("u", "s");
    assert!(
        router.resolve_session(&key).await.unwrap().is_none(),
        "router should return None when pool is empty"
    );
    assert!(
        router.resolve_op("op-1", &key).await.unwrap().is_none(),
        "router should return None for op resolution too"
    );
}

#[tokio::test]
async fn stickiness_holds_across_membership_changes() {
    let pool = Arc::new(K8sPool::new());
    let router = Router::single_pool(pool.clone(), Arc::new(StubStore::default()));

    pool.set_backends(vec!["a:1".into(), "b:1".into()]);
    let key = SessionKey::new("u", "s");
    let first = router.resolve_session(&key).await.unwrap().unwrap();

    // Even if the K8s watcher emits a totally different membership,
    // the existing session must keep its binding.
    pool.set_backends(vec!["x:1".into(), "y:1".into(), "z:1".into()]);
    for _ in 0..5 {
        assert_eq!(
            router.resolve_session(&key).await.unwrap().unwrap(),
            first,
            "stickiness must outlive pool membership churn"
        );
    }
}

#[tokio::test]
async fn new_session_after_repopulation_picks_from_new_set() {
    let pool = Arc::new(K8sPool::new());
    let router = Router::single_pool(pool.clone(), Arc::new(StubStore::default()));

    // Pool empty at first.
    let early = SessionKey::new("u", "early");
    assert!(router.resolve_session(&early).await.unwrap().is_none());

    // Watcher fires → new sessions can now route.
    pool.set_backends(vec!["a:1".into(), "b:1".into()]);
    let late = SessionKey::new("u", "late");
    let chosen = router.resolve_session(&late).await.unwrap().unwrap();
    assert!(["a:1", "b:1"].contains(&chosen.as_str()));
}
