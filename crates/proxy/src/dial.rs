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

//! Per-backend tonic Channel cache. We don't want to pay the dial cost on
//! every RPC, so each unique backend address is opened lazily and reused.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::transport::{Channel, Endpoint};

#[derive(Debug, thiserror::Error)]
pub enum DialError {
    #[error("invalid backend uri {addr}: {source}")]
    BadUri {
        addr: String,
        #[source]
        source: tonic::transport::Error,
    },
}

#[derive(Default)]
pub struct Dialer {
    inner: Mutex<HashMap<String, Channel>>,
}

impl Dialer {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Returns a (cached) lazy Channel for `addr`. The Channel is created in
    /// "lazy connect" mode — actual TCP/HTTP/2 setup happens on first use,
    /// and tonic transparently reconnects on transport failures.
    pub fn channel(&self, addr: &str) -> Result<Channel, DialError> {
        if let Some(ch) = self.inner.lock().get(addr).cloned() {
            return Ok(ch);
        }
        let uri = ensure_scheme(addr);
        let endpoint = Endpoint::from_shared(uri.clone()).map_err(|e| DialError::BadUri {
            addr: addr.to_string(),
            source: e,
        })?;
        // Lazy connect: tonic returns a Channel immediately and reconnects
        // automatically on transport errors. This matches Go grpc.NewClient.
        let channel = endpoint.connect_lazy();
        self.inner.lock().insert(addr.to_string(), channel.clone());
        Ok(channel)
    }
}

/// Bare `host:port` strings need a scheme for `Endpoint::from_shared`.
fn ensure_scheme(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    }
}
