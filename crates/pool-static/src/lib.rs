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

//! Backend pool whose membership is fixed at startup. Pairs with
//! `scg-pool-k8s` for the dynamic K8s-Endpoints-watch variant; both
//! implement the same `Pool` trait and can be swapped via config.

use scg_routing::{BackendMember, Pool};

#[derive(Debug, thiserror::Error)]
pub enum StaticPoolError {
    #[error("static pool: at least one backend address required")]
    Empty,
}

pub struct StaticPool {
    backends: Vec<String>,
}

impl StaticPool {
    pub fn new<I, S>(addresses: I) -> Result<Self, StaticPoolError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let backends: Vec<String> = addresses.into_iter().map(Into::into).collect();
        if backends.is_empty() {
            return Err(StaticPoolError::Empty);
        }
        Ok(Self { backends })
    }
}

impl Pool for StaticPool {
    fn members(&self) -> Vec<BackendMember> {
        // Static pools have no notion of health — every configured
        // backend is presumed healthy. Wrap with `scg-healthcheck`'s
        // `HealthAwarePool` to add active gRPC health probing; on a
        // bare StaticPool a failed forward surfaces as an error to
        // the client and the selection strategy moves onward for the
        // next session. Labels/weights are defaults until the config
        // grows per-address metadata.
        self.backends.iter().map(BackendMember::new).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn members_returns_configured_list_with_default_metadata() {
        let p = StaticPool::new(["a", "b", "c"]).unwrap();
        let m = p.members();
        assert_eq!(
            m.iter().map(|b| b.addr.as_str()).collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        assert!(m.iter().all(|b| b.weight == 1 && b.labels.is_empty()));
    }

    #[test]
    fn empty_rejected() {
        assert!(StaticPool::new(Vec::<String>::new()).is_err());
    }
}
