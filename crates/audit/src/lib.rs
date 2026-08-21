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

//! Structured audit logging for the Spark Connect Gateway.
//!
//! Records security- and compliance-relevant events as
//! `tracing::info!` events with `target = "scg::audit"`. The
//! existing JSON formatter installed by `scg-observability` picks
//! them up automatically; operators filter by target in their log
//! aggregator (Loki, Splunk, etc.) to get an auditable stream
//! distinct from operational logs.
//!
//! ## Event taxonomy
//!
//! Four event types, deliberately narrow:
//!
//! | Event | When |
//! |-------|------|
//! | `session.create` | First time a `(tenant, user, session_id)` is bound to a backend |
//! | `session.release` | Client called `ReleaseSession` and the gateway forgot the binding |
//! | `auth.failure` | Authentication rejected an RPC (any of the failure reasons from `scg-auth`) |
//! | `rpc.error` | Handler returned a non-OK `Status` — surfaced separately so compliance can see failures without mining every log line |
//!
//! Successful RPCs are deliberately *not* logged by default —
//! they're already counted in `scg_rpcs_total{code="OK"}` and
//! filling the audit stream with every Config call defeats the
//! purpose. The optional `log_successful_rpcs` config switch turns
//! them on for strict-monitoring environments.
//!
//! ## Why not a sink trait
//!
//! We pipe through `tracing` rather than introducing a separate
//! "AuditSink" abstraction because:
//! * The structured-log infrastructure (JSON formatter, levels,
//!   field types) already exists.
//! * Operators already have one log pipeline; one fewer thing to
//!   route.
//! * Filtering by `target` is a one-line query in Loki/Splunk.
//!
//! If we ever need a dedicated sink (file, Kafka, S3) we can add
//! a `tracing_subscriber::Layer` that intercepts `target =
//! "scg::audit"` events without changing this API.

use tracing::info;

/// Audit-logging configuration. Constructed from `scg-config`'s
/// `AuditSettings`.
#[derive(Debug, Clone)]
pub struct AuditConfig {
    /// Master switch. Defaults `true` — compliance value is high
    /// and the per-event cost (one `tracing::info!`) is negligible.
    /// Operators can flip to `false` in dev / local environments.
    pub enabled: bool,
    /// When `true`, every successful RPC also emits an `rpc.ok`
    /// audit event. Defaults `false` to keep the audit stream
    /// signal-rich; only switch on under strict monitoring.
    pub log_successful_rpcs: bool,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            log_successful_rpcs: false,
        }
    }
}

/// Audit logger. Cheap to clone (config is small + `Copy`-like).
#[derive(Debug, Clone)]
pub struct AuditLogger {
    cfg: AuditConfig,
}

impl AuditLogger {
    pub fn new(cfg: AuditConfig) -> Self {
        Self { cfg }
    }

    /// Build a no-op logger (`enabled = false`). Used by tests
    /// that don't care about audit output, and as the proxy's
    /// default when the operator hasn't configured audit.
    pub fn disabled() -> Self {
        Self::new(AuditConfig {
            enabled: false,
            log_successful_rpcs: false,
        })
    }

    /// `session.create` — a `(tenant, user, session_id)` was bound
    /// to `backend` for the first time. Fires on the binding-path
    /// of the affinity store (not on every lookup).
    ///
    /// `groups` is the verified identity's group membership (from
    /// the JWT/OIDC `groups` claim or the static-token config).
    /// Recorded as a comma-joined string so the audit stream stays
    /// flat JSON; downstream tooling can split if it needs an array.
    /// Per-RPC events deliberately *don't* carry groups — they fire
    /// frequently enough that doubling the field count matters.
    pub fn session_create(
        &self,
        rid: &str,
        tenant: &str,
        user_id: &str,
        groups: &[String],
        session_id: &str,
        backend: &str,
    ) {
        if !self.cfg.enabled {
            return;
        }
        info!(
            target: "scg::audit",
            event = "session.create",
            rid = %rid,
            tenant = %tenant,
            user_id = %user_id,
            groups = %groups.join(","),
            session_id = %session_id,
            backend = %backend,
            "session created",
        );
    }

    /// `session.release` — the client called `ReleaseSession`. The
    /// gateway has dropped its affinity record; the backend has
    /// dropped the SparkSession.
    pub fn session_release(
        &self,
        rid: &str,
        tenant: &str,
        user_id: &str,
        groups: &[String],
        session_id: &str,
        backend: &str,
    ) {
        if !self.cfg.enabled {
            return;
        }
        info!(
            target: "scg::audit",
            event = "session.release",
            rid = %rid,
            tenant = %tenant,
            user_id = %user_id,
            groups = %groups.join(","),
            session_id = %session_id,
            backend = %backend,
            "session released",
        );
    }

    /// `auth.failure` — the auth interceptor rejected an RPC.
    /// `reason` is the same fixed-cardinality string used by the
    /// metric (`missing_token`, `invalid_token`, `expired`,
    /// `unknown_kid`, `unknown`).
    pub fn auth_failure(&self, rid: &str, rpc: &str, reason: &str) {
        if !self.cfg.enabled {
            return;
        }
        info!(
            target: "scg::audit",
            event = "auth.failure",
            rid = %rid,
            rpc = %rpc,
            reason = %reason,
            "authentication failed",
        );
    }

    /// `config.redacted` — a client asked for a config key the
    /// gateway refuses to relay (currently the backend's own
    /// pre-shared token) and the value was stripped from the
    /// response. Security-relevant on its own: a legitimate client
    /// has no reason to read the gateway↔backend credential, so
    /// these events are worth alerting on.
    pub fn config_redacted(&self, rid: &str, tenant: &str, user_id: &str, key: &str) {
        if !self.cfg.enabled {
            return;
        }
        info!(
            target: "scg::audit",
            event = "config.redacted",
            rid = %rid,
            tenant = %tenant,
            user_id = %user_id,
            key = %key,
            "config key withheld from client",
        );
    }

    /// `rpc.error` — a handler returned a non-OK Status. `code` is
    /// the canonical gRPC code name (e.g. `Unauthenticated`,
    /// `PermissionDenied`, `ResourceExhausted`). Cancelled and
    /// OK results are filtered by the caller.
    pub fn rpc_error(
        &self,
        rid: &str,
        rpc: &str,
        tenant: &str,
        user_id: &str,
        code: &str,
        message: &str,
    ) {
        if !self.cfg.enabled {
            return;
        }
        info!(
            target: "scg::audit",
            event = "rpc.error",
            rid = %rid,
            rpc = %rpc,
            tenant = %tenant,
            user_id = %user_id,
            code = %code,
            message = %message,
            "rpc returned non-OK status",
        );
    }

    /// `rpc.ok` — optional. Only emitted when
    /// `log_successful_rpcs = true`. Off by default to keep the
    /// audit stream signal-rich.
    pub fn rpc_ok(&self, rid: &str, rpc: &str, tenant: &str, user_id: &str) {
        if !self.cfg.enabled || !self.cfg.log_successful_rpcs {
            return;
        }
        info!(
            target: "scg::audit",
            event = "rpc.ok",
            rid = %rid,
            rpc = %rpc,
            tenant = %tenant,
            user_id = %user_id,
            "rpc completed ok",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::Arc;
    use tracing::subscriber::with_default;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Layer;

    /// Layer that captures all events with `target = "scg::audit"`
    /// into a Vec. Used to verify the audit API actually emits
    /// what it promises without needing to install the global JSON
    /// formatter.
    #[derive(Clone, Default)]
    struct CaptureLayer {
        events: Arc<Mutex<Vec<CapturedEvent>>>,
    }

    #[derive(Debug)]
    struct CapturedEvent {
        target: String,
        fields: std::collections::HashMap<String, String>,
    }

    impl<S: tracing::Subscriber> Layer<S> for CaptureLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let target = event.metadata().target().to_string();
            let mut fields = std::collections::HashMap::new();
            struct Vis<'a>(&'a mut std::collections::HashMap<String, String>);
            impl<'a> tracing::field::Visit for Vis<'a> {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    self.0.insert(field.name().into(), format!("{:?}", value));
                }
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    self.0.insert(field.name().into(), value.into());
                }
            }
            event.record(&mut Vis(&mut fields));
            self.events.lock().push(CapturedEvent { target, fields });
        }
    }

    fn with_capture<F: FnOnce(&CaptureLayer)>(f: F) {
        let cap = CaptureLayer::default();
        let sub = tracing_subscriber::registry().with(cap.clone());
        with_default(sub, || f(&cap));
    }

    #[test]
    fn disabled_logger_emits_nothing() {
        with_capture(|cap| {
            let a = AuditLogger::disabled();
            a.session_create("rid-1", "t", "u", &[], "s", "b:1");
            a.auth_failure("rid-2", "Config", "missing_token");
            a.rpc_error("rid-3", "Config", "t", "u", "Internal", "boom");
            assert!(cap.events.lock().is_empty());
        });
    }

    #[test]
    fn session_create_emits_expected_fields() {
        with_capture(|cap| {
            let a = AuditLogger::new(AuditConfig::default());
            a.session_create(
                "rid-1",
                "team-a",
                "alice",
                &["devs".into(), "admins".into()],
                "sess-1",
                "be:15002",
            );
            let events = cap.events.lock();
            assert_eq!(events.len(), 1);
            let e = &events[0];
            assert_eq!(e.target, "scg::audit");
            assert_eq!(
                e.fields.get("event").map(|s| s.as_str()),
                Some("session.create")
            );
            assert_eq!(e.fields.get("tenant").map(|s| s.as_str()), Some("team-a"));
            assert_eq!(e.fields.get("user_id").map(|s| s.as_str()), Some("alice"));
            assert_eq!(
                e.fields.get("session_id").map(|s| s.as_str()),
                Some("sess-1")
            );
            assert_eq!(
                e.fields.get("backend").map(|s| s.as_str()),
                Some("be:15002")
            );
            assert_eq!(
                e.fields.get("groups").map(|s| s.as_str()),
                Some("devs,admins"),
            );
        });
    }

    #[test]
    fn session_create_empty_groups_renders_empty_string() {
        with_capture(|cap| {
            let a = AuditLogger::new(AuditConfig::default());
            a.session_create("rid-1", "team-a", "alice", &[], "sess-1", "be:15002");
            let events = cap.events.lock();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].fields.get("groups").map(|s| s.as_str()), Some(""));
        });
    }

    #[test]
    fn auth_failure_emits_reason() {
        with_capture(|cap| {
            let a = AuditLogger::new(AuditConfig::default());
            a.auth_failure("rid-1", "Config", "invalid_token");
            let events = cap.events.lock();
            assert_eq!(events.len(), 1);
            let e = &events[0];
            assert_eq!(
                e.fields.get("event").map(|s| s.as_str()),
                Some("auth.failure")
            );
            assert_eq!(
                e.fields.get("reason").map(|s| s.as_str()),
                Some("invalid_token")
            );
        });
    }

    #[test]
    fn rpc_error_emits_code_and_message() {
        with_capture(|cap| {
            let a = AuditLogger::new(AuditConfig::default());
            a.rpc_error(
                "rid-1",
                "ExecutePlan",
                "team-b",
                "bob",
                "ResourceExhausted",
                "rate limit exceeded",
            );
            let events = cap.events.lock();
            assert_eq!(events.len(), 1);
            let e = &events[0];
            assert_eq!(e.fields.get("event").map(|s| s.as_str()), Some("rpc.error"));
            assert_eq!(
                e.fields.get("code").map(|s| s.as_str()),
                Some("ResourceExhausted")
            );
        });
    }

    #[test]
    fn rpc_ok_only_fires_when_enabled() {
        with_capture(|cap| {
            // Default config: log_successful_rpcs is false.
            let a = AuditLogger::new(AuditConfig::default());
            a.rpc_ok("rid-1", "Config", "t", "u");
            assert!(cap.events.lock().is_empty());

            // Opt in.
            let a = AuditLogger::new(AuditConfig {
                enabled: true,
                log_successful_rpcs: true,
            });
            a.rpc_ok("rid-2", "Config", "t", "u");
            let events = cap.events.lock();
            assert_eq!(events.len(), 1);
            assert_eq!(
                events[0].fields.get("event").map(|s| s.as_str()),
                Some("rpc.ok")
            );
        });
    }
}
