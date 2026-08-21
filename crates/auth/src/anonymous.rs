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

//! `AnonymousAuthenticator` — used when auth is explicitly disabled.
//!
//! Returns a fixed `Identity { user_id: "anonymous", … }`: the
//! gateway accepts every request without authentication. Fine for
//! trusted in-cluster networks, never for external exposure.

use async_trait::async_trait;
use tonic::metadata::MetadataMap;
use tonic::Status;

use crate::{Authenticator, Identity};

#[derive(Debug, Default)]
pub struct AnonymousAuthenticator;

#[async_trait]
impl Authenticator for AnonymousAuthenticator {
    async fn authenticate(&self, _metadata: &MetadataMap) -> Result<Identity, Status> {
        Ok(Identity::user("anonymous"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn always_accepts() {
        let a = AnonymousAuthenticator;
        let id = a.authenticate(&MetadataMap::new()).await.unwrap();
        assert_eq!(id.user_id, "anonymous");
    }
}
