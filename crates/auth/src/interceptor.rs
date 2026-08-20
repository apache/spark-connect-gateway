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

//! Tonic interceptor that runs an `Authenticator` and stamps the
//! verified `Identity` onto the request's extensions.
//!
//! Note on async vs sync interceptors:
//!
//! Tonic's built-in `Interceptor` trait is *sync*. Some authenticators
//! (notably OIDC with JWKS rotation) need to await network I/O, which
//! a sync interceptor can't do. We therefore expose this interceptor
//! via a free function that the proxy calls explicitly at the head of
//! every RPC handler — see `crates/proxy` for the call site.
//!
//! That keeps the framework boundary clean: the proxy is responsible
//! for reading the inbound metadata, calling `authenticate`, attaching
//! the resulting identity, and *then* dispatching to the per-RPC
//! method body. The Spark Connect service surface is small enough
//! (under twenty RPCs) that calling `authenticate` per handler is
//! manageable; a tower-layer-based async interceptor would be a
//! future refactor.

use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::Status;

use crate::identity::IdentityExt;
use crate::Authenticator;

/// Owned wrapper around an `Arc<dyn Authenticator>`. The proxy holds
/// one of these; calling [`AuthInterceptor::authenticate`] is what
/// every RPC does as its first step.
#[derive(Clone)]
pub struct AuthInterceptor {
    inner: Arc<dyn Authenticator>,
}

impl AuthInterceptor {
    pub fn new(inner: Arc<dyn Authenticator>) -> Self {
        Self { inner }
    }

    /// Run authentication against the metadata; on success return the
    /// `IdentityExt` the proxy should attach to the request before
    /// dispatching.
    pub async fn authenticate(&self, metadata: &MetadataMap) -> Result<IdentityExt, Status> {
        let id = self.inner.authenticate(metadata).await?;
        Ok(IdentityExt(Arc::new(id)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AnonymousAuthenticator;

    #[tokio::test]
    async fn delegates_to_inner_authenticator() {
        let i = AuthInterceptor::new(Arc::new(AnonymousAuthenticator));
        let ext = i.authenticate(&MetadataMap::new()).await.unwrap();
        assert_eq!(ext.0.user_id, "anonymous");
    }
}
