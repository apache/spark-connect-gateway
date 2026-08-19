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

//! Routing core: SessionKey, Pool trait, AffinityStore trait,
//! TenantRouter, and Router.
//!
//! Backend selection is broken into pieces so each can evolve
//! independently:
//!
//! * `Pool` — *which* backend should serve a fresh session?
//! * `AffinityStore` — *which* backend already serves an existing session?
//! * `TenantRouter` — map a tenant string to its pool. Single-tenant
//!   deployments still work — they configure exactly one entry
//!   (often the implicit `"default"`).
//! * `Router` — glue that asks the store first, then the pool from
//!   `TenantRouter`, and remembers the decision.

use std::collections::HashMap;
use std::sync::Arc;

use tonic::Status;

/// Identifies a Spark Connect session within a tenant. The triple
/// `(tenant, user_id, session_id)` is the affinity routing key.
/// Single-tenant deployments implicitly use the literal string
/// `"default"` for the tenant component (see [`SessionKey::new`]).
///
/// Spark Connect itself keys `SparkSession` only on `(user_id,
/// session_id)`. Adding the tenant prefix lets multiple tenants share
/// a gateway without their `session_id` namespaces colliding —
/// `(team-a, alice, sess-1)` is a different key from `(team-b,
/// alice, sess-1)`.
///
/// `user_id` may be empty if the client did not set it; `session_id`
/// must not be empty for the affinity store to route a request.
/// `tenant` is always non-empty in production (the tenant resolver
/// guarantees this).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionKey {
    pub tenant: String,
    pub user_id: String,
    pub session_id: String,
}

impl SessionKey {
    /// Build a [`SessionKey`] without an explicit tenant — used by
    /// tests and single-tenant call sites. The tenant is set to
    /// `"default"`, matching the back-compat behaviour of
    /// `TenantResolverConfig::default()`.
    pub fn new(user_id: impl Into<String>, session_id: impl Into<String>) -> Self {
        Self::with_tenant("default", user_id, session_id)
    }

    /// Build a [`SessionKey`] for an explicit tenant. This is what
    /// production handlers call after the tenant resolver yields a
    /// tenant string.
    pub fn with_tenant(
        tenant: impl Into<String>,
        user_id: impl Into<String>,
        session_id: impl Into<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            user_id: user_id.into(),
            session_id: session_id.into(),
        }
    }

    pub(crate) fn is_zero(&self) -> bool {
        self.session_id.is_empty()
    }
}

/// One backend in a pool: an address plus the metadata a selection
/// strategy may key on. Discovery sources fill what they know —
/// static config can declare labels and weights explicitly; the K8s
/// watcher currently supplies addresses only (pod-label surfacing is
/// planned work).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendMember {
    pub addr: String,
    /// Free-form key/value metadata (e.g. `spark-version`), for
    /// metadata-aware selection strategies. Empty unless the
    /// discovery source provides it.
    pub labels: std::collections::BTreeMap<String, String>,
    /// Relative weight for weighted strategies. Defaults to 1;
    /// round-robin ignores it.
    pub weight: u32,
}

impl BackendMember {
    /// A member with default metadata (no labels, weight 1).
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            labels: std::collections::BTreeMap::new(),
            weight: 1,
        }
    }
}

/// Provides pool *membership*: which backends currently exist and
/// are believed healthy. Implementations must be safe for concurrent
/// use.
///
/// Two shipping implementations: `scg-pool-static` (fixed list at
/// startup) and `scg-pool-k8s` (Endpoints watch). The K8s pool can
/// be empty during startup or after a flap.
///
/// Deliberately *not* part of this trait: choosing which member a
/// new session is placed on. That is [`SelectionStrategy`]'s job —
/// membership sources and placement policies vary independently.
pub trait Pool: Send + Sync + 'static {
    /// Snapshot of the currently-healthy members. Order is
    /// implementation-defined; strategies must not rely on it beyond
    /// a single call.
    fn members(&self) -> Vec<BackendMember>;

    /// Best-effort hint that `addr` is unreachable. Implementations may
    /// remove `addr` from the rotation, decrement a health score, or
    /// ignore the hint entirely. The K8s pool uses this for passive
    /// failure detection alongside its active service-watch.
    fn mark_unhealthy(&self, _addr: &str) {}
}

/// Inputs a [`SelectionStrategy`] may consult beyond the candidate
/// list itself. Empty today; declared `#[non_exhaustive]` so fields
/// (e.g. per-backend session counts for least-sessions placement)
/// can be added without breaking implementors.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct SelectionContext {}

/// Chooses which pool member a *new* session is placed on. Invoked
/// only on the placement path — affinity hits, the operation-id
/// reverse index, and bind-if-absent race arbitration never consult
/// the strategy, so a misbehaving strategy can skew new placements
/// but cannot break live sessions.
///
/// Strategies may be stateful (round-robin holds a cursor); one
/// strategy instance serves one pool, so state never mixes across
/// tenants.
pub trait SelectionStrategy: Send + Sync + 'static {
    /// Return the index of the chosen candidate, or `None` to decline
    /// (e.g. `candidates` is empty). Called with a consistent
    /// snapshot; must be non-blocking.
    fn select(
        &self,
        key: &SessionKey,
        candidates: &[BackendMember],
        ctx: &SelectionContext,
    ) -> Option<usize>;
}

/// The default strategy: rotate over the candidates with an atomic
/// cursor. Ignores labels, weights, and the session key.
#[derive(Debug, Default)]
pub struct RoundRobinStrategy {
    cursor: std::sync::atomic::AtomicU64,
}

impl SelectionStrategy for RoundRobinStrategy {
    fn select(
        &self,
        _key: &SessionKey,
        candidates: &[BackendMember],
        _ctx: &SelectionContext,
    ) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }
        let idx = self
            .cursor
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Some((idx % candidates.len() as u64) as usize)
    }
}

/// Persistence layer for sticky routing decisions. Two shipping
/// backends: `scg-store-memory` (in-process, single-replica) and
/// `scg-store-redis` (shared across replicas for HA).
///
/// The trait is `async_trait` because the Redis backing is a
/// network call. The in-memory impl wraps its sync work in async-fn
/// signatures with no `await` points, so callers pay only the
/// trait-object dispatch cost.
#[async_trait::async_trait]
pub trait AffinityStore: Send + Sync + 'static {
    async fn lookup_session(&self, key: &SessionKey) -> Option<String>;
    /// Insert `(key, backend)` only if no binding for `key` exists.
    /// Returns the *winning* binding (existing or freshly inserted).
    async fn bind_session_if_absent(&self, key: SessionKey, backend: String) -> String;
    async fn forget_session(&self, key: &SessionKey);

    async fn lookup_op(&self, op_id: &str) -> Option<String>;
    async fn bind_op(&self, op_id: String, backend: String);
    async fn forget_op(&self, op_id: &str);
}

/// What to do when a request arrives for a tenant that has no
/// explicit pool entry. The `default` pool, if one is configured, is
/// what `UseDefault` falls back to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownTenantPolicy {
    /// Route the unknown tenant to the default pool. If no default
    /// pool is configured either, the request fails as if the pool
    /// were empty (`Unavailable`).
    UseDefault,
    /// Refuse to serve the unknown tenant — surface `PermissionDenied`
    /// to the client. SaaS-style deployments that want hard isolation
    /// between configured tenants pick this.
    Reject,
}

/// A pool paired with the strategy that places new sessions on it.
/// Membership and placement policy vary independently — the pairing
/// happens here, one strategy instance per pool so strategy state
/// (e.g. a round-robin cursor) never mixes across pools.
#[derive(Clone)]
pub struct PoolEntry {
    pub pool: Arc<dyn Pool>,
    pub strategy: Arc<dyn SelectionStrategy>,
}

impl PoolEntry {
    /// Pair `pool` with the default round-robin strategy.
    pub fn round_robin(pool: Arc<dyn Pool>) -> Self {
        Self {
            pool,
            strategy: Arc::new(RoundRobinStrategy::default()),
        }
    }

    /// Pair `pool` with an explicit strategy.
    pub fn with_strategy(pool: Arc<dyn Pool>, strategy: Arc<dyn SelectionStrategy>) -> Self {
        Self { pool, strategy }
    }

    /// Apply this entry's strategy to its pool's current members.
    fn select_addr(&self, key: &SessionKey) -> Option<String> {
        let members = self.pool.members();
        let ctx = SelectionContext::default();
        let idx = self.strategy.select(key, &members, &ctx)?;
        members.get(idx).map(|m| m.addr.clone())
    }
}

/// Maps a tenant string to a [`PoolEntry`]. Construct once at
/// startup from the operator's config. Cheap to clone (everything
/// is `Arc`).
///
/// The lookup order is:
///
/// 1. `tenants` map (exact match on tenant string).
/// 2. `default` entry if `policy == UseDefault`.
/// 3. `Err(PermissionDenied)` if `policy == Reject`.
pub struct TenantRouter {
    tenants: HashMap<String, PoolEntry>,
    default: Option<PoolEntry>,
    policy: UnknownTenantPolicy,
}

impl TenantRouter {
    /// Build a router from explicit per-tenant pool entries, an
    /// optional shared default entry, and the unknown-tenant policy.
    ///
    /// `default = None` + `policy = UseDefault` is allowed but
    /// degrades to "everything except the explicit tenants fails"
    /// — equivalent to `policy = Reject` for unknown tenants, but
    /// without the explicit `PermissionDenied` (you get
    /// `Unavailable` from the empty pool path). Set the policy
    /// explicitly if you want clean error semantics.
    pub fn new(
        tenants: HashMap<String, PoolEntry>,
        default: Option<PoolEntry>,
        policy: UnknownTenantPolicy,
    ) -> Self {
        Self {
            tenants,
            default,
            policy,
        }
    }

    /// Single-pool convenience: every tenant routes to the same pool
    /// with round-robin placement. Used by single-tenant deployments
    /// and by tests that don't care about per-tenant isolation.
    pub fn single(pool: Arc<dyn Pool>) -> Self {
        Self {
            tenants: HashMap::new(),
            default: Some(PoolEntry::round_robin(pool)),
            policy: UnknownTenantPolicy::UseDefault,
        }
    }

    /// Pick the pool entry for `tenant`. Returns:
    /// * `Ok(Some(entry))` when a pool is available (explicit or default)
    /// * `Ok(None)` when no pool exists and policy says it's OK (e.g.
    ///   `UseDefault` without a default pool — caller emits the usual
    ///   "no healthy backend" error)
    /// * `Err(Status)` when `policy == Reject` and the tenant is unknown.
    pub fn pool_for(&self, tenant: &str) -> Result<Option<PoolEntry>, Status> {
        if let Some(p) = self.tenants.get(tenant) {
            return Ok(Some(p.clone()));
        }
        match self.policy {
            UnknownTenantPolicy::UseDefault => Ok(self.default.clone()),
            UnknownTenantPolicy::Reject => Err(Status::permission_denied(format!(
                "tenant {:?} has no configured pool",
                tenant
            ))),
        }
    }
}

/// Outcome of a session-resolution call. `addr` is the backend
/// for the session; `newly_bound` distinguishes a freshly-bound
/// session (the gateway just decided which backend it lives on)
/// from an existing one (the affinity store already had the
/// binding). Used by audit logging to fire `session.create`
/// exactly once per session lifetime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveOutcome {
    pub addr: String,
    pub newly_bound: bool,
}

/// Resolves a request to a concrete backend address.
pub struct Router {
    tenants: TenantRouter,
    store: Arc<dyn AffinityStore>,
}

impl Router {
    /// Build a Router from a [`TenantRouter`] and an affinity store.
    pub fn new(tenants: TenantRouter, store: Arc<dyn AffinityStore>) -> Self {
        Self { tenants, store }
    }

    /// Single-pool convenience constructor for single-tenant
    /// deployments. Equivalent to
    /// `Router::new(TenantRouter::single(pool), store)`.
    pub fn single_pool(pool: Arc<dyn Pool>, store: Arc<dyn AffinityStore>) -> Self {
        Self::new(TenantRouter::single(pool), store)
    }

    /// Resolve a backend for `key`. If a binding exists it is returned;
    /// otherwise a fresh backend is picked from the tenant's pool,
    /// recorded, and returned.
    ///
    /// Result variants:
    ///
    /// * `Ok(Some(addr))` — a backend was found (existing binding or
    ///   freshly picked).
    /// * `Ok(None)` — no binding exists *and* the tenant's pool
    ///   currently has no healthy backend (e.g. K8s service-watch
    ///   pool during startup). Caller surfaces `Unavailable`.
    /// * `Err(Status)` — the tenant has no configured pool and the
    ///   policy is `Reject`. Caller forwards the `PermissionDenied`
    ///   directly to the client.
    ///
    /// A `SessionKey` with an empty `session_id` falls through to a
    /// fresh pick, but that binding is *not* recorded — without a
    /// stable session id we cannot honour stickiness on the next call.
    pub async fn resolve_session(&self, key: &SessionKey) -> Result<Option<String>, Status> {
        Ok(self.resolve_session_detailed(key).await?.map(|r| r.addr))
    }

    /// Same as [`resolve_session`] but distinguishes a freshly-bound
    /// session from an existing one — useful for audit logging
    /// (`session.create` events fire only on the freshly-bound
    /// path). Most callers should use `resolve_session` and ignore
    /// the binding flavour.
    pub async fn resolve_session_detailed(
        &self,
        key: &SessionKey,
    ) -> Result<Option<ResolveOutcome>, Status> {
        let Some(entry) = self.tenants.pool_for(&key.tenant)? else {
            return Ok(None);
        };
        if key.is_zero() {
            // Empty session_id is never bound — the affinity store
            // ignores it. Counts as `newly_bound = false` for audit
            // purposes (there's nothing to record).
            return Ok(entry.select_addr(key).map(|addr| ResolveOutcome {
                addr,
                newly_bound: false,
            }));
        }
        if let Some(existing) = self.store.lookup_session(key).await {
            return Ok(Some(ResolveOutcome {
                addr: existing,
                newly_bound: false,
            }));
        }
        let Some(chosen) = entry.select_addr(key) else {
            return Ok(None);
        };
        let winner = self
            .store
            .bind_session_if_absent(key.clone(), chosen.clone())
            .await;
        Ok(Some(ResolveOutcome {
            addr: winner.clone(),
            // If our `bind` returned a different value than what we
            // tried to insert, someone else won the race — we did
            // not freshly bind this session.
            newly_bound: winner == chosen,
        }))
    }

    /// Resolve a backend for an operation, falling back to session
    /// routing when the operation is unknown.
    ///
    /// Used by `ReattachExecute` / `ReleaseExecute` / `Interrupt`: a
    /// client may reattach to a long-running operation that was
    /// started on a specific backend, and the gateway must route back
    /// to that same backend even if the affinity cache for the
    /// session has already expired or is missing.
    pub async fn resolve_op(
        &self,
        op_id: &str,
        key: &SessionKey,
    ) -> Result<Option<String>, Status> {
        if !op_id.is_empty() {
            if let Some(b) = self.store.lookup_op(op_id).await {
                return Ok(Some(b));
            }
        }
        self.resolve_session(key).await
    }

    /// Hint that `addr` is currently unreachable. Routed to the pool
    /// owning `tenant`. Best-effort — unknown tenant is a no-op.
    pub fn mark_unhealthy(&self, tenant: &str, addr: &str) {
        if let Ok(Some(entry)) = self.tenants.pool_for(tenant) {
            entry.pool.mark_unhealthy(addr);
        }
    }

    pub async fn remember_op(&self, op_id: String, backend: String) {
        if op_id.is_empty() {
            return;
        }
        self.store.bind_op(op_id, backend).await;
    }

    /// Bind `key` to a *specific* backend, bypassing pool selection.
    /// Used when the correct backend is dictated by protocol
    /// semantics rather than placement policy — e.g. `CloneSession`
    /// creates a new session whose state lives on the driver that
    /// executed the clone, so the cloned session's key must be bound
    /// to that same driver.
    ///
    /// Returns the winning binding: `backend` if this call bound the
    /// key (or a racing call bound the same address), or the
    /// pre-existing address if the key was already bound elsewhere.
    /// A zero key (empty session id) is never bound and returns
    /// `backend` unchanged.
    pub async fn remember_session(&self, key: &SessionKey, backend: String) -> String {
        if key.is_zero() {
            return backend;
        }
        self.store
            .bind_session_if_absent(key.clone(), backend)
            .await
    }

    pub async fn forget_op(&self, op_id: &str) {
        if op_id.is_empty() {
            return;
        }
        self.store.forget_op(op_id).await;
    }

    pub async fn forget_session(&self, key: &SessionKey) {
        self.store.forget_session(key).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex as PLMutex;
    use std::collections::HashMap;

    /// Fixed three-member pool; placement order is supplied by the
    /// entry's RoundRobinStrategy, not by the pool itself.
    struct SeqPool;
    impl Pool for SeqPool {
        fn members(&self) -> Vec<BackendMember> {
            vec![
                BackendMember::new("a"),
                BackendMember::new("b"),
                BackendMember::new("c"),
            ]
        }
    }

    /// Pool that always reports empty — used to test the "no backend"
    /// path through Router::resolve_session.
    struct EmptyPool;
    impl Pool for EmptyPool {
        fn members(&self) -> Vec<BackendMember> {
            Vec::new()
        }
    }

    struct StubStore {
        sessions: PLMutex<HashMap<SessionKey, String>>,
        ops: PLMutex<HashMap<String, String>>,
    }
    impl Default for StubStore {
        fn default() -> Self {
            Self {
                sessions: PLMutex::new(HashMap::new()),
                ops: PLMutex::new(HashMap::new()),
            }
        }
    }
    #[async_trait::async_trait]
    impl AffinityStore for StubStore {
        async fn lookup_session(&self, k: &SessionKey) -> Option<String> {
            self.sessions.lock().get(k).cloned()
        }
        async fn bind_session_if_absent(&self, k: SessionKey, v: String) -> String {
            let mut g = self.sessions.lock();
            g.entry(k).or_insert(v).clone()
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

    fn router() -> Router {
        Router::single_pool(Arc::new(SeqPool), Arc::new(StubStore::default()))
    }

    #[tokio::test]
    async fn stickiness_is_honoured() {
        let r = router();
        let k = SessionKey::new("u1", "s1");
        let first = r.resolve_session(&k).await.unwrap();
        for _ in 0..10 {
            assert_eq!(r.resolve_session(&k).await.unwrap(), first);
        }
    }

    #[tokio::test]
    async fn distinct_sessions_can_diverge() {
        let r = router();
        let a = r
            .resolve_session(&SessionKey::new("u1", "s1"))
            .await
            .unwrap();
        let b = r
            .resolve_session(&SessionKey::new("u1", "s2"))
            .await
            .unwrap();
        assert_ne!(a, b, "round-robin should diverge across sessions");
    }

    #[tokio::test]
    async fn empty_session_does_not_bind() {
        let store = Arc::new(StubStore::default());
        let r = Router::single_pool(Arc::new(SeqPool), store.clone() as Arc<dyn AffinityStore>);
        r.resolve_session(&SessionKey::new("u", "")).await.unwrap();
        assert!(store
            .lookup_session(&SessionKey::new("u", "anything"))
            .await
            .is_none());
    }

    #[tokio::test]
    async fn op_lookup_overrides_session() {
        let r = router();
        let k = SessionKey::new("u", "s");
        let _first = r.resolve_op("op-unknown", &k).await.unwrap();
        r.remember_op("op-1".to_string(), "explicit:1".to_string())
            .await;
        assert_eq!(
            r.resolve_op("op-1", &k).await.unwrap().as_deref(),
            Some("explicit:1")
        );
    }

    #[tokio::test]
    async fn empty_pool_returns_none() {
        let r = Router::single_pool(Arc::new(EmptyPool), Arc::new(StubStore::default()));
        let k = SessionKey::new("u", "s");
        assert!(r.resolve_session(&k).await.unwrap().is_none());
        assert!(r.resolve_op("op", &k).await.unwrap().is_none());
    }

    // ---- SelectionStrategy tests ------------------------------------

    #[test]
    fn round_robin_strategy_rotates_over_candidates() {
        let strat = RoundRobinStrategy::default();
        let members = vec![
            BackendMember::new("a"),
            BackendMember::new("b"),
            BackendMember::new("c"),
        ];
        let k = SessionKey::new("u", "s");
        let ctx = SelectionContext::default();
        let picks: Vec<usize> = (0..4)
            .map(|_| strat.select(&k, &members, &ctx).unwrap())
            .collect();
        assert_eq!(picks, vec![0, 1, 2, 0]);
    }

    #[test]
    fn round_robin_strategy_declines_empty_candidates() {
        let strat = RoundRobinStrategy::default();
        let k = SessionKey::new("u", "s");
        let ctx = SelectionContext::default();
        assert!(strat.select(&k, &[], &ctx).is_none());
    }

    #[tokio::test]
    async fn placement_through_router_rotates_via_strategy() {
        // The behaviour previously pinned by the pools' own
        // round-robin tests: successive *new* sessions land on
        // successive members.
        let r = router();
        let mut got = Vec::new();
        for i in 0..4 {
            let k = SessionKey::new("u", format!("s{}", i));
            got.push(r.resolve_session(&k).await.unwrap().unwrap());
        }
        assert_eq!(got, vec!["a", "b", "c", "a"]);
    }

    // ---- TenantRouter tests -----------------------------------------

    fn fixed_pool(addr: &'static str) -> PoolEntry {
        struct FixedPool(&'static str);
        impl Pool for FixedPool {
            fn members(&self) -> Vec<BackendMember> {
                vec![BackendMember::new(self.0)]
            }
        }
        PoolEntry::round_robin(Arc::new(FixedPool(addr)))
    }

    #[tokio::test]
    async fn tenant_router_picks_per_tenant_pool() {
        let mut tenants = HashMap::new();
        tenants.insert("team-a".to_string(), fixed_pool("a:1"));
        tenants.insert("team-b".to_string(), fixed_pool("b:1"));
        let tr = TenantRouter::new(tenants, None, UnknownTenantPolicy::Reject);
        let store: Arc<dyn AffinityStore> = Arc::new(StubStore::default());
        let r = Router::new(tr, store);

        let got_a = r
            .resolve_session(&SessionKey::with_tenant("team-a", "u", "s1"))
            .await
            .unwrap();
        assert_eq!(got_a.as_deref(), Some("a:1"));

        let got_b = r
            .resolve_session(&SessionKey::with_tenant("team-b", "u", "s1"))
            .await
            .unwrap();
        assert_eq!(got_b.as_deref(), Some("b:1"));
    }

    #[tokio::test]
    async fn tenant_router_falls_back_to_default_when_use_default() {
        let mut tenants = HashMap::new();
        tenants.insert("team-a".to_string(), fixed_pool("a:1"));
        let tr = TenantRouter::new(
            tenants,
            Some(fixed_pool("default:1")),
            UnknownTenantPolicy::UseDefault,
        );
        let store: Arc<dyn AffinityStore> = Arc::new(StubStore::default());
        let r = Router::new(tr, store);

        let got = r
            .resolve_session(&SessionKey::with_tenant("unknown-tenant", "u", "s1"))
            .await
            .unwrap();
        assert_eq!(got.as_deref(), Some("default:1"));
    }

    #[tokio::test]
    async fn tenant_router_rejects_unknown_tenant_under_reject_policy() {
        let mut tenants = HashMap::new();
        tenants.insert("team-a".to_string(), fixed_pool("a:1"));
        let tr = TenantRouter::new(tenants, None, UnknownTenantPolicy::Reject);
        let store: Arc<dyn AffinityStore> = Arc::new(StubStore::default());
        let r = Router::new(tr, store);

        let err = r
            .resolve_session(&SessionKey::with_tenant("unknown", "u", "s1"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test]
    async fn tenant_router_no_default_no_explicit_returns_none() {
        // UseDefault policy + no default pool + unknown tenant → Ok(None).
        // The caller surfaces this as the usual "no healthy backend" error.
        let tr = TenantRouter::new(HashMap::new(), None, UnknownTenantPolicy::UseDefault);
        let store: Arc<dyn AffinityStore> = Arc::new(StubStore::default());
        let r = Router::new(tr, store);
        let got = r
            .resolve_session(&SessionKey::with_tenant("nobody", "u", "s1"))
            .await
            .unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn remember_session_binds_fresh_key_to_given_backend() {
        let r = router();
        let k = SessionKey::new("u1", "cloned-1");
        let winner = r.remember_session(&k, "b".into()).await;
        assert_eq!(winner, "b");
        // Follow-up resolution honours the explicit binding — it does
        // NOT go through pool selection (SeqPool would have yielded
        // "a" for a first pick).
        assert_eq!(r.resolve_session(&k).await.unwrap().as_deref(), Some("b"));
    }

    #[tokio::test]
    async fn remember_session_respects_existing_binding() {
        let r = router();
        let k = SessionKey::new("u1", "s1");
        // Bind via normal resolution first (SeqPool starts at "a").
        let placed = r.resolve_session(&k).await.unwrap().unwrap();
        // An explicit remember to a different address loses the race:
        // the pre-existing binding wins and is returned.
        let winner = r.remember_session(&k, "z".into()).await;
        assert_eq!(winner, placed);
        assert_eq!(
            r.resolve_session(&k).await.unwrap().as_deref(),
            Some(placed.as_str())
        );
    }

    #[tokio::test]
    async fn remember_session_ignores_zero_key() {
        let r = router();
        let k = SessionKey::new("u1", "");
        let winner = r.remember_session(&k, "b".into()).await;
        assert_eq!(winner, "b");
        // Zero keys are never bound; resolution still goes through
        // pool selection.
        let got = r.resolve_session(&k).await.unwrap();
        assert_eq!(got.as_deref(), Some("a"));
    }
}
