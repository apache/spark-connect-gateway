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

//! Credentials the gateway presents on the gateway→backend hop.
//!
//! Backends started with `spark.connect.authenticate.token` (Spark
//! 4.0+) reject any request whose `authorization` metadata is not
//! exactly `Bearer <token>`. Holding that token *only* in the
//! gateway is how a deployment enforces the trust boundary: a
//! client that dials a backend directly is refused with
//! `UNAUTHENTICATED`, while gateway-mediated traffic carries the
//! credential.
//!
//! Tokens are per pool. Lookup mirrors pool selection: a tenant
//! with its own entry uses it; every other tenant (including
//! unknown tenants routed to the default pool) uses the default
//! token; no default means no header.

use std::collections::HashMap;

use tonic::metadata::MetadataValue;
use tonic::Request;

/// A configured token can't be carried in an `authorization` header.
/// Raised at startup, never per-request. The token value itself is
/// deliberately absent from the message.
#[derive(Debug, thiserror::Error)]
#[error("backend token for pool `{pool}` is not a valid header value (must be visible ASCII)")]
pub struct InvalidBackendToken {
    pub pool: String,
}

/// Pre-encoded `authorization` values, one per pool that has a
/// token configured. Built once at startup; applied to every
/// outbound request.
#[derive(Default)]
pub struct BackendTokens {
    default: Option<MetadataValue<tonic::metadata::Ascii>>,
    per_tenant: HashMap<String, MetadataValue<tonic::metadata::Ascii>>,
}

impl BackendTokens {
    /// No tokens anywhere — the gateway sends no backend credential.
    pub fn none() -> Self {
        Self::default()
    }

    /// Build from resolved token strings. `per_tenant` only needs
    /// entries for tenants whose pool uses a *different* token than
    /// the default; everything else falls back to `default`.
    pub fn new(
        default: Option<String>,
        per_tenant: HashMap<String, String>,
    ) -> Result<Self, InvalidBackendToken> {
        let encode = |pool: &str, token: String| -> Result<_, InvalidBackendToken> {
            let err = || InvalidBackendToken {
                pool: pool.to_string(),
            };
            // HeaderValue would accept any opaque byte ≥ 0x20, but a
            // token the backend can never match is a misconfiguration
            // — Spark compares against `Bearer <token>` as a string.
            if !token.is_ascii() {
                return Err(err());
            }
            let mut v = MetadataValue::try_from(format!("Bearer {token}")).map_err(|_| err())?;
            // Keep the credential out of h2 header logging.
            v.set_sensitive(true);
            Ok(v)
        };
        let default = default.map(|t| encode("default", t)).transpose()?;
        let per_tenant = per_tenant
            .into_iter()
            .map(|(t, tok)| {
                let v = encode(&t, tok)?;
                Ok((t, v))
            })
            .collect::<Result<HashMap<_, _>, InvalidBackendToken>>()?;
        Ok(Self {
            default,
            per_tenant,
        })
    }

    /// True when at least one pool has a token configured.
    pub fn is_configured(&self) -> bool {
        self.default.is_some() || !self.per_tenant.is_empty()
    }

    /// Number of tenant-specific token entries.
    pub fn tenant_override_count(&self) -> usize {
        self.per_tenant.len()
    }

    /// Stamp the `authorization` header for `tenant`'s pool onto an
    /// outbound request. No-op when neither the tenant nor the
    /// default has a token.
    pub fn apply<T>(&self, tenant: &str, req: &mut Request<T>) {
        let value = self.per_tenant.get(tenant).or(self.default.as_ref());
        if let Some(v) = value {
            req.metadata_mut().insert("authorization", v.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header<T>(req: &Request<T>) -> Option<&str> {
        req.metadata()
            .get("authorization")
            .map(|v| v.to_str().expect("ascii"))
    }

    #[test]
    fn no_tokens_is_a_noop() {
        let tokens = BackendTokens::none();
        assert!(!tokens.is_configured());
        let mut req = Request::new(());
        tokens.apply("any-tenant", &mut req);
        assert!(header(&req).is_none());
    }

    #[test]
    fn default_token_applies_to_every_tenant() {
        let tokens = BackendTokens::new(Some("shared".into()), HashMap::new()).unwrap();
        for tenant in ["default", "team-a", "unknown"] {
            let mut req = Request::new(());
            tokens.apply(tenant, &mut req);
            assert_eq!(header(&req), Some("Bearer shared"));
        }
    }

    #[test]
    fn tenant_entry_wins_over_default() {
        let per_tenant = HashMap::from([("team-a".to_string(), "a-token".to_string())]);
        let tokens = BackendTokens::new(Some("shared".into()), per_tenant).unwrap();
        let mut req = Request::new(());
        tokens.apply("team-a", &mut req);
        assert_eq!(header(&req), Some("Bearer a-token"));
        let mut req = Request::new(());
        tokens.apply("team-b", &mut req);
        assert_eq!(header(&req), Some("Bearer shared"));
    }

    #[test]
    fn tenant_entry_without_default_leaves_others_bare() {
        let per_tenant = HashMap::from([("team-a".to_string(), "a-token".to_string())]);
        let tokens = BackendTokens::new(None, per_tenant).unwrap();
        let mut req = Request::new(());
        tokens.apply("team-b", &mut req);
        assert!(header(&req).is_none());
    }

    #[test]
    fn header_is_marked_sensitive() {
        let tokens = BackendTokens::new(Some("s".into()), HashMap::new()).unwrap();
        let mut req = Request::new(());
        tokens.apply("t", &mut req);
        assert!(req.metadata().get("authorization").unwrap().is_sensitive());
    }

    #[test]
    fn non_ascii_token_is_rejected_at_build() {
        assert!(BackendTokens::new(Some("秘密".into()), HashMap::new()).is_err());
    }
}
