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

//! `StaticTokenAuthenticator` — Bearer-token allowlist.
//!
//! Loads a `token → user_id` map at startup; on each request, looks up
//! the inbound `Authorization: Bearer <token>` and returns the
//! matching `Identity`. Constant-time comparison prevents the
//! authenticator from leaking *which* token is wrong via timing.
//!
//! Intended for dev / test / single-team use. JWT or OIDC is the right
//! choice for anything beyond that.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;
use serde::Deserialize;
use subtle::ConstantTimeEq;
use tonic::metadata::MetadataMap;
use tonic::Status;
use tracing::debug;

use crate::{bearer_token, Authenticator, Identity};

/// One entry in the static token table.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenEntry {
    /// The opaque token string clients present.
    pub token: String,
    /// What `user_id` to attach to authenticated requests.
    pub user_id: String,
    /// Optional tenant (workspace / org / project).
    #[serde(default)]
    pub tenant: Option<String>,
    /// Optional groups. Recorded on `session.create` /
    /// `session.release` audit events so operators can see which
    /// memberships an identity carries; not currently consulted for
    /// authorization (the gateway has no RBAC layer yet).
    #[serde(default)]
    pub groups: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum StaticTokenError {
    #[error("static token authenticator: at least one token required")]
    Empty,
    #[error("static token authenticator: duplicate token entry for {0:?}")]
    Duplicate(String),
}

#[derive(Debug)]
pub struct StaticTokenAuthenticator {
    /// We keep tokens behind a RwLock so a future "reload" hook can
    /// rotate them without restarting. Reads on the hot path acquire a
    /// read lock, which is uncontended in steady state.
    inner: Arc<RwLock<HashMap<String, IdentityTemplate>>>,
}

/// What we materialize into an `Identity` on a hit. Stored alongside
/// the token to avoid reallocating per-request.
#[derive(Debug, Clone)]
struct IdentityTemplate {
    user_id: String,
    tenant: Option<String>,
    groups: Vec<String>,
}

impl StaticTokenAuthenticator {
    pub fn new(entries: Vec<TokenEntry>) -> Result<Self, StaticTokenError> {
        if entries.is_empty() {
            return Err(StaticTokenError::Empty);
        }
        let mut map = HashMap::with_capacity(entries.len());
        for e in entries {
            if map.contains_key(&e.token) {
                return Err(StaticTokenError::Duplicate(e.token));
            }
            map.insert(
                e.token,
                IdentityTemplate {
                    user_id: e.user_id,
                    tenant: e.tenant,
                    groups: e.groups,
                },
            );
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(map)),
        })
    }

    /// Constant-time lookup. Iterates the full table on every call so
    /// timing does not depend on which slot is occupied.
    fn lookup(&self, presented: &str) -> Option<IdentityTemplate> {
        let table = self.inner.read();
        let mut found: Option<IdentityTemplate> = None;
        for (stored, tmpl) in table.iter() {
            // ConstantTimeEq is constant-time *for the byte slices it
            // compares*. Different lengths return false fast — that
            // does leak length info, but token length isn't sensitive
            // (clients see their own token's length anyway).
            if stored.as_bytes().ct_eq(presented.as_bytes()).into() {
                found = Some(tmpl.clone());
                // Continue iterating so timing doesn't depend on hit position.
            }
        }
        found
    }
}

#[async_trait]
impl Authenticator for StaticTokenAuthenticator {
    async fn authenticate(&self, metadata: &MetadataMap) -> Result<Identity, Status> {
        let token = bearer_token(metadata).ok_or_else(|| {
            Status::unauthenticated("missing or malformed Authorization: Bearer header")
        })?;
        match self.lookup(token) {
            Some(tmpl) => {
                debug!(user_id = %tmpl.user_id, "static token auth: ok");
                Ok(Identity {
                    user_id: tmpl.user_id,
                    tenant: tmpl.tenant,
                    groups: tmpl.groups,
                })
            }
            None => {
                debug!("static token auth: unknown token");
                Err(Status::unauthenticated("invalid token"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth() -> StaticTokenAuthenticator {
        StaticTokenAuthenticator::new(vec![
            TokenEntry {
                token: "alice-secret".into(),
                user_id: "alice".into(),
                tenant: Some("team-a".into()),
                groups: vec!["devs".into()],
            },
            TokenEntry {
                token: "bob-secret".into(),
                user_id: "bob".into(),
                tenant: None,
                groups: vec![],
            },
        ])
        .unwrap()
    }

    fn md(value: &str) -> MetadataMap {
        let mut md = MetadataMap::new();
        md.insert("authorization", value.parse().unwrap());
        md
    }

    #[tokio::test]
    async fn known_token_resolves_to_identity() {
        let id = auth()
            .authenticate(&md("Bearer alice-secret"))
            .await
            .unwrap();
        assert_eq!(id.user_id, "alice");
        assert_eq!(id.tenant.as_deref(), Some("team-a"));
        assert_eq!(id.groups, vec!["devs"]);
    }

    #[tokio::test]
    async fn second_token_is_independent() {
        let id = auth().authenticate(&md("Bearer bob-secret")).await.unwrap();
        assert_eq!(id.user_id, "bob");
        assert!(id.tenant.is_none());
    }

    #[tokio::test]
    async fn unknown_token_rejected() {
        let err = auth().authenticate(&md("Bearer nope")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn missing_header_rejected() {
        let err = auth().authenticate(&MetadataMap::new()).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn empty_table_rejected() {
        assert!(matches!(
            StaticTokenAuthenticator::new(vec![]).unwrap_err(),
            StaticTokenError::Empty
        ));
    }

    #[test]
    fn duplicate_token_rejected() {
        let res = StaticTokenAuthenticator::new(vec![
            TokenEntry {
                token: "x".into(),
                user_id: "a".into(),
                tenant: None,
                groups: vec![],
            },
            TokenEntry {
                token: "x".into(),
                user_id: "b".into(),
                tenant: None,
                groups: vec![],
            },
        ]);
        assert!(matches!(res.unwrap_err(), StaticTokenError::Duplicate(_)));
    }
}
