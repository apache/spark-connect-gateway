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

//! Integration test for [`HealthAwarePool`] against real gRPC
//! servers. Verifies that:
//!
//! 1. A backend whose `Health` server is alive stays in
//!    `members()`.
//! 2. A backend that goes down (server task dropped) is evicted
//!    after `unhealthy_threshold` consecutive failed probes.
//! 3. `members()` keeps reporting only the surviving backend.

use std::sync::Arc;
use std::time::Duration;

use scg_healthcheck::{HealthAwarePool, HealthCheckConfig};
use scg_routing::{BackendMember, Pool};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic_health::server::health_reporter;

/// Static pool stub: returns whatever vec we feed it. We rebuild it
/// for each test so we control which backends "exist" from the
/// inner pool's perspective; the wrapper's job is to filter by
/// health.
struct StubPool {
    backends: parking_lot::RwLock<Vec<String>>,
}

impl StubPool {
    fn new(backends: Vec<String>) -> Arc<Self> {
        Arc::new(Self {
            backends: parking_lot::RwLock::new(backends),
        })
    }
}

impl Pool for StubPool {
    fn members(&self) -> Vec<BackendMember> {
        self.backends
            .read()
            .iter()
            .map(BackendMember::new)
            .collect()
    }
}

/// Spawn a gRPC server with the standard `Health` service registered
/// and reporting `Serving`. Returns its host:port and a shutdown
/// sender.
async fn spawn_health_server() -> (String, oneshot::Sender<()>) {
    let (reporter, health_svc) = health_reporter();
    // Empty service name = "the entire backend"; our probe code uses
    // the same convention.
    reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;

    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(health_svc)
            .serve_with_incoming_shutdown(TcpListenerStream::new(lis), async {
                let _ = rx.await;
            })
            .await
            .ok();
    });
    (addr, tx)
}

#[tokio::test]
async fn evicts_dead_backend_then_keeps_alive_one() {
    let (alive, _alive_kill) = spawn_health_server().await;
    let (dying, dying_kill) = spawn_health_server().await;

    let stub = StubPool::new(vec![alive.clone(), dying.clone()]);
    // Aggressive: 200ms interval, 200ms timeout, 2-failure eviction
    // → eviction of `dying` should land within ~600ms after kill.
    let cfg = HealthCheckConfig {
        interval: Duration::from_millis(200),
        timeout: Duration::from_millis(300),
        unhealthy_threshold: 2,
        healthy_threshold: 2,
    };
    let wrapper = HealthAwarePool::new(stub.clone() as Arc<dyn Pool>, cfg);
    let _probe = wrapper.spawn_probe();

    // Let one probe round complete; both should still be healthy.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let healthy: Vec<String> = wrapper.members().into_iter().map(|m| m.addr).collect();
    assert!(healthy.contains(&alive), "alive backend should be present");
    assert!(
        healthy.contains(&dying),
        "dying backend should still be present pre-kill"
    );

    // Kill `dying`. Give the OS a moment to actually tear the
    // listener down before probes start failing.
    let _ = dying_kill.send(());
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Wait for the wrapper to evict it. With 200ms interval and
    // unhealthy_threshold=2, we expect eviction within ~600ms.
    let mut evicted = false;
    for _ in 0..30 {
        let healthy: Vec<String> = wrapper.members().into_iter().map(|m| m.addr).collect();
        if !healthy.contains(&dying) && healthy.contains(&alive) {
            evicted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        evicted,
        "dying backend should be evicted within the test window"
    );

    // members() must keep reporting exactly the surviving backend —
    // any selection strategy applied on top can only choose it.
    for _ in 0..5 {
        let m: Vec<String> = wrapper.members().into_iter().map(|x| x.addr).collect();
        assert_eq!(m, vec![alive.clone()]);
    }
}
