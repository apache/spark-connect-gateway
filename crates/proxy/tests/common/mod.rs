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

// `tests/common/mod.rs` is compiled into every integration-test binary
// that does `mod common;`, but each binary only uses a subset of the
// helpers — Rust's dead-code analysis runs per-binary and can't see
// the cross-binary usage. Suppress here so a helper that's used in
// only one test file doesn't fire a warning in the other.
#![allow(dead_code)]

//! Shared test helpers for the proxy integration tests.
//!
//! The big-ticket item here is [`AuditCapture`], a process-wide
//! tracing subscriber that records every `target = "scg::audit"`
//! event into an in-memory buffer. Tests use it to assert that the
//! audit pipeline emits what it should.
//!
//! ## Why a global subscriber, not `set_default`?
//!
//! Earlier versions installed a per-test capture via
//! `tracing::subscriber::set_default(...)`, which scopes the
//! subscriber to the current thread. That works when the test's
//! tokio runtime happens to keep every task on the current thread,
//! but the gRPC server is `tokio::spawn`'d onto a worker pool and
//! its audit events frequently fire on a different thread — which
//! the per-thread subscriber doesn't see. The result was a flaky
//! test that passed in isolation and failed under workspace
//! parallelism.
//!
//! `tracing::subscriber::set_global_default` is process-wide, so it
//! sees every thread, but it can only be called once. We wrap it in
//! a `OnceLock` and serialize the tests that use it with a mutex —
//! integration tests that need audit capture all hold the same
//! `AuditCapture::lease`, so they run one-at-a-time within their
//! file (and across files, since the global state is shared).

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, OnceLock};

use parking_lot::Mutex as PlMutex;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

/// A single captured `target = "scg::audit"` event, flattened into a
/// `name -> value` map. `String` values for both keys and values
/// keep the assertion API simple — tests compare with string
/// literals.
#[derive(Debug, Clone)]
pub struct CapturedEvent {
    pub fields: HashMap<String, String>,
}

/// Shared event buffer. The tracing `Layer` writes into it; tests
/// read from it via `AuditCapture::snapshot`.
#[derive(Clone, Default)]
struct EventSink {
    events: std::sync::Arc<PlMutex<Vec<CapturedEvent>>>,
}

impl<S: tracing::Subscriber> Layer<S> for EventSink {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "scg::audit" {
            return;
        }
        let mut fields = HashMap::new();
        struct Vis<'a>(&'a mut HashMap<String, String>);
        impl<'a> tracing::field::Visit for Vis<'a> {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                self.0.insert(f.name().into(), format!("{:?}", v));
            }
            fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
                self.0.insert(f.name().into(), v.into());
            }
        }
        event.record(&mut Vis(&mut fields));
        self.events.lock().push(CapturedEvent { fields });
    }
}

/// Process-wide audit capture handle. Acquire one via
/// [`AuditCapture::lease`]; the returned guard releases the
/// global-subscriber lock on drop so other tests can proceed.
///
/// Tests do not need to (and cannot) install the subscriber
/// themselves — the first `lease()` call installs it; later calls
/// reuse it. Events from prior leases are cleared at the start of
/// each new lease.
pub struct AuditCapture<'a> {
    sink: EventSink,
    _guard: MutexGuard<'a, ()>,
}

impl<'a> AuditCapture<'a> {
    pub fn lease() -> AuditCapture<'static> {
        let (sink, mutex) = init();
        let guard = mutex.lock().unwrap_or_else(|p| p.into_inner());
        sink.events.lock().clear();
        AuditCapture {
            sink: sink.clone(),
            _guard: guard,
        }
    }

    pub fn snapshot(&self) -> Vec<CapturedEvent> {
        self.sink.events.lock().clone()
    }

    pub fn count_events(&self, event_name: &str) -> usize {
        self.snapshot()
            .iter()
            .filter(|e| e.fields.get("event").map(|s| s.as_str()) == Some(event_name))
            .count()
    }

    pub fn find_event(&self, event_name: &str) -> Option<CapturedEvent> {
        self.snapshot()
            .into_iter()
            .find(|e| e.fields.get("event").map(|s| s.as_str()) == Some(event_name))
    }
}

/// Lazily install the global subscriber on first use. Returns the
/// shared sink (so we can snapshot events) and the lease mutex (so
/// callers serialize). `set_global_default` is called at most once
/// per process.
fn init() -> (&'static EventSink, &'static Mutex<()>) {
    static STATE: OnceLock<(EventSink, Mutex<()>)> = OnceLock::new();
    let (sink, mutex) = STATE.get_or_init(|| {
        let sink = EventSink::default();
        // Build the subscriber and stash it — every thread sees the
        // same `Layer`, so events from tokio worker threads land in
        // our shared buffer.
        let subscriber = tracing_subscriber::registry().with(sink.clone());
        // Setting the global default can fail if some other code in
        // the test binary already set one. Tests that care about the
        // audit stream all go through this helper, so the only way
        // we lose the race is if a non-capture test in the same
        // binary also installed a global subscriber. Currently none
        // do, so we treat a `set_global_default` error as fatal.
        tracing::subscriber::set_global_default(subscriber)
            .expect("install audit-capture subscriber: another global subscriber already exists");
        (sink, Mutex::new(()))
    });
    (sink, mutex)
}
