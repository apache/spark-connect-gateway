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

//! Active gRPC health probing wrapped around a [`Pool`].
//!
//! The base [`scg_routing::Pool`] trait is purely round-robin: it
//! hands out backends without knowing whether they actually answer.
//! A backend pod that has wedged but not crashed (process alive,
//! gRPC server not responding) will keep getting traffic until K8s
//! kills the pod or the pool's source-of-truth (e.g. K8s
//! Endpoints) reflects the failure.
//!
//! [`HealthAwarePool`] wraps any [`Pool`] and adds the
//! [gRPC Health Check Protocol]:
//!
//! 1. A background task probes `grpc.health.v1.Health/Check` against
//!    every backend in `inner.all_healthy()` every `interval`.
//! 2. After `unhealthy_threshold` consecutive failures, the backend
//!    is removed from `pick()` results.
//! 3. Probes continue against the unhealthy set; after
//!    `healthy_threshold` consecutive successes the backend is
//!    re-admitted.
//! 4. `pick()` retries up to `inner.all_healthy().len()` times if it
//!    draws an unhealthy backend, so traffic keeps flowing as long
//!    as anything is up.
//!
//! [gRPC Health Check Protocol]: https://github.com/grpc/grpc/blob/master/doc/health-checking.md

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use scg_routing::{BackendMember, Pool};
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::ServingStatus;
use tracing::{debug, info, warn};

/// Per-backend health-check tuning. Defaults are chosen for the
/// "thousands of backends, sub-second tail" world we're not in:
/// fast enough to evict a wedged pod within ~15s, slow enough not to
/// pummel the backends.
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    pub interval: Duration,
    pub timeout: Duration,
    /// Consecutive failures required to mark a backend unhealthy.
    pub unhealthy_threshold: u32,
    /// Consecutive successes required to re-admit a backend.
    pub healthy_threshold: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(2),
            unhealthy_threshold: 3,
            healthy_threshold: 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HealthState {
    Healthy,
    Unhealthy,
}

#[derive(Debug, Clone, Copy)]
struct BackendStatus {
    state: HealthState,
    /// Counter resets on transition: while Healthy, counts consecutive
    /// failures; while Unhealthy, counts consecutive successes.
    streak: u32,
}

impl BackendStatus {
    fn fresh() -> Self {
        // New backends are presumed healthy. We only stop trusting
        // them once we've seen `unhealthy_threshold` failures —
        // otherwise a momentary connection blip during the *first*
        // probe would falsely evict every backend on startup.
        Self {
            state: HealthState::Healthy,
            streak: 0,
        }
    }
}

/// `Pool` adapter that filters its inner pool's `members()` by an
/// actively-probed health view. Placement over the filtered members
/// stays with the selection strategy — this wrapper only decides who
/// is a candidate at all.
pub struct HealthAwarePool {
    inner: Arc<dyn Pool>,
    cfg: HealthCheckConfig,
    /// Map of `host:port` -> current health status. Backends that
    /// the inner pool no longer reports are GC'd on each probe round.
    statuses: RwLock<HashMap<String, BackendStatus>>,
}

impl HealthAwarePool {
    /// Wrap `inner` with active health checking using `cfg`. The
    /// returned pool is immediately usable; spawn the probe task with
    /// [`HealthAwarePool::spawn_probe`] to start mutating the health
    /// view.
    pub fn new(inner: Arc<dyn Pool>, cfg: HealthCheckConfig) -> Arc<Self> {
        Arc::new(Self {
            inner,
            cfg,
            statuses: RwLock::new(HashMap::new()),
        })
    }

    /// Spawn the background probe loop. Returns a `JoinHandle` that
    /// callers can `abort()` on shutdown; dropping the handle does
    /// *not* stop the probes (the task holds an `Arc<Self>`).
    pub fn spawn_probe(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(me.cfg.interval);
            // Burst-tolerant: skip ticks if a probe round took longer
            // than `interval` rather than queueing them up.
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                me.probe_all().await;
            }
        })
    }

    /// One probe round: fetch the inner pool's full backend list,
    /// dial each in parallel, update each status entry, and GC
    /// backends the inner pool no longer knows about.
    async fn probe_all(&self) {
        let backends: Vec<String> = self.inner.members().into_iter().map(|m| m.addr).collect();
        if backends.is_empty() {
            // Nothing to probe; clear the status map so a restart
            // doesn't carry over stale entries.
            self.statuses.write().clear();
            return;
        }

        let mut tasks = Vec::with_capacity(backends.len());
        for addr in &backends {
            let addr = addr.clone();
            let timeout = self.cfg.timeout;
            tasks.push(tokio::spawn(async move {
                let ok = probe_one(&addr, timeout).await;
                (addr, ok)
            }));
        }

        let mut results = Vec::with_capacity(tasks.len());
        for t in tasks {
            if let Ok(r) = t.await {
                results.push(r);
            }
        }

        // Now apply. Hold the write lock for the whole apply so
        // pick() / all_healthy() see a consistent view.
        let mut g = self.statuses.write();
        // GC: drop entries no longer present in `backends`.
        let live: std::collections::HashSet<&str> = backends.iter().map(String::as_str).collect();
        g.retain(|k, _| live.contains(k.as_str()));

        for (addr, ok) in results {
            let entry = g.entry(addr.clone()).or_insert_with(BackendStatus::fresh);
            apply_probe(entry, ok, &self.cfg, &addr);
        }
    }

    fn healthy_members(&self) -> Vec<BackendMember> {
        let g = self.statuses.read();
        // If a backend is in `inner.members()` but not yet in our
        // status map, treat it as healthy (default). This avoids a
        // gap on the first probe round where everything would look
        // unhealthy.
        let inner = self.inner.members();
        let mut out = Vec::with_capacity(inner.len());
        for m in inner {
            match g.get(&m.addr) {
                Some(s) if s.state == HealthState::Unhealthy => continue,
                _ => out.push(m),
            }
        }
        out
    }
}

impl Pool for HealthAwarePool {
    fn members(&self) -> Vec<BackendMember> {
        self.healthy_members()
    }

    fn mark_unhealthy(&self, addr: &str) {
        // Caller observed a forward error against `addr`. We don't
        // immediately evict — that would race with the probe loop
        // and risk oscillation — but we *do* prime the streak so the
        // next probe failure flips us over the threshold faster.
        let mut g = self.statuses.write();
        if let Some(status) = g.get_mut(addr) {
            if status.state == HealthState::Healthy {
                status.streak = status.streak.saturating_add(1);
                debug!(%addr, streak = status.streak, "healthcheck: handler reported error");
            }
        }
        self.inner.mark_unhealthy(addr);
    }
}

/// Open a one-shot connection and call `Health.Check`. We don't
/// reuse channels here on purpose — a wedged backend may have an
/// open TCP connection that hangs forever; tearing it down per probe
/// keeps us honest.
async fn probe_one(addr: &str, timeout: Duration) -> bool {
    let url = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    };
    let endpoint = match tonic::transport::Endpoint::from_shared(url) {
        Ok(e) => e.connect_timeout(timeout).timeout(timeout),
        Err(e) => {
            warn!(%addr, error = %e, "healthcheck: invalid endpoint URL");
            return false;
        }
    };
    let channel = match endpoint.connect().await {
        Ok(c) => c,
        Err(e) => {
            debug!(%addr, error = %e, "healthcheck: connect failed");
            return false;
        }
    };
    let mut client = HealthClient::new(channel);
    // Empty service name = "the entire backend." Spark Connect's
    // tonic server registers Health under the standard convention.
    let req = HealthCheckRequest {
        service: String::new(),
    };
    match client.check(req).await {
        Ok(resp) => {
            let status = resp.into_inner().status;
            // ServingStatus::Serving == healthy.
            status == ServingStatus::Serving as i32
        }
        Err(e) => {
            // Backend may be a Spark Connect server without Health
            // registered. We treat NOT_FOUND / UNIMPLEMENTED as an
            // ambiguous signal and keep the backend healthy — the
            // alternative is evicting every backend that doesn't ship
            // grpc.health.v1, which would be a regression for older
            // Spark versions.
            match e.code() {
                tonic::Code::NotFound | tonic::Code::Unimplemented => {
                    debug!(%addr, "healthcheck: backend has no Health service; treating as healthy");
                    true
                }
                _ => {
                    debug!(%addr, error = %e, "healthcheck: probe failed");
                    false
                }
            }
        }
    }
}

/// Update one `BackendStatus` based on the latest probe outcome.
fn apply_probe(status: &mut BackendStatus, ok: bool, cfg: &HealthCheckConfig, addr: &str) {
    match (status.state, ok) {
        (HealthState::Healthy, true) => {
            status.streak = 0;
        }
        (HealthState::Healthy, false) => {
            status.streak = status.streak.saturating_add(1);
            if status.streak >= cfg.unhealthy_threshold {
                info!(%addr, "healthcheck: backend marked UNHEALTHY");
                status.state = HealthState::Unhealthy;
                status.streak = 0;
            }
        }
        (HealthState::Unhealthy, true) => {
            status.streak = status.streak.saturating_add(1);
            if status.streak >= cfg.healthy_threshold {
                info!(%addr, "healthcheck: backend back to HEALTHY");
                status.state = HealthState::Healthy;
                status.streak = 0;
            }
        }
        (HealthState::Unhealthy, false) => {
            status.streak = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(unhealthy: u32, healthy: u32) -> HealthCheckConfig {
        HealthCheckConfig {
            unhealthy_threshold: unhealthy,
            healthy_threshold: healthy,
            ..Default::default()
        }
    }

    #[test]
    fn streak_to_unhealthy_then_back() {
        let mut s = BackendStatus::fresh();
        let c = cfg(3, 2);

        // Two failures: still healthy.
        apply_probe(&mut s, false, &c, "x:1");
        apply_probe(&mut s, false, &c, "x:1");
        assert_eq!(s.state, HealthState::Healthy);

        // Third: flip.
        apply_probe(&mut s, false, &c, "x:1");
        assert_eq!(s.state, HealthState::Unhealthy);

        // One success: not enough.
        apply_probe(&mut s, true, &c, "x:1");
        assert_eq!(s.state, HealthState::Unhealthy);

        // Second success: re-admitted.
        apply_probe(&mut s, true, &c, "x:1");
        assert_eq!(s.state, HealthState::Healthy);
    }

    #[test]
    fn intermittent_failure_resets_streak() {
        let mut s = BackendStatus::fresh();
        let c = cfg(3, 2);
        apply_probe(&mut s, false, &c, "x:1");
        apply_probe(&mut s, false, &c, "x:1");
        // One success in the middle resets the failure streak.
        apply_probe(&mut s, true, &c, "x:1");
        apply_probe(&mut s, false, &c, "x:1");
        assert_eq!(s.state, HealthState::Healthy);
    }

    #[test]
    fn intermittent_success_resets_recovery_streak() {
        let mut s = BackendStatus {
            state: HealthState::Unhealthy,
            streak: 0,
        };
        let c = cfg(3, 3);
        apply_probe(&mut s, true, &c, "x:1");
        apply_probe(&mut s, true, &c, "x:1");
        // One failure resets the recovery streak.
        apply_probe(&mut s, false, &c, "x:1");
        apply_probe(&mut s, true, &c, "x:1");
        apply_probe(&mut s, true, &c, "x:1");
        // Still need one more success to re-admit (threshold=3).
        assert_eq!(s.state, HealthState::Unhealthy);
        apply_probe(&mut s, true, &c, "x:1");
        assert_eq!(s.state, HealthState::Healthy);
    }
}
