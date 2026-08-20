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

//! Authentication for the Spark Connect Gateway.
//!
//! This crate exposes a single `Authenticator` trait that takes a tonic
//! `MetadataMap` (i.e. the gRPC headers attached to a request) and
//! returns a verified `Identity` — or a `Status::unauthenticated` if no
//! valid credential is present.
//!
//! Three concrete implementations are shipped:
//!
//! * [`AnonymousAuthenticator`] — accepts everything, attaches a fixed
//!   `Identity { user_id: "anonymous", … }`. Used when auth is
//!   explicitly disabled (`auth: { type: none }`) and in unit tests.
//! * [`StaticTokenAuthenticator`] — Bearer-token allowlist with
//!   constant-time comparison.
//! * [`JwtAuthenticator`] — verifies a signed JWT against a local PEM /
//!   JWK key (no network required).
//! * [`OidcAuthenticator`] (in [`crate::oidc`]) — fetches JWKS from a
//!   remote IdP and verifies signatures against rotating keys.
//!
//! All four implement the same trait, so the gateway picks the right
//! one at startup based on config and treats them uniformly.

pub mod anonymous;
pub mod identity;
pub mod interceptor;
pub mod jwt;
pub mod oidc;
pub mod token;

pub use anonymous::AnonymousAuthenticator;
pub use identity::Identity;
pub use interceptor::AuthInterceptor;
pub use jwt::{JwtAuthenticator, JwtConfig, JwtError, KeySource};
pub use oidc::{OidcAuthenticator, OidcConfig, OidcError};
pub use token::{StaticTokenAuthenticator, StaticTokenError, TokenEntry};

use async_trait::async_trait;
use tonic::metadata::MetadataMap;
use tonic::Status;

/// Verifies caller credentials and returns a trusted `Identity`.
///
/// Implementations must be `Send + Sync + 'static` so the gateway can
/// hold them in an `Arc` and clone references into per-request futures.
#[async_trait]
pub trait Authenticator: Send + Sync + 'static {
    /// Inspect `metadata`, validate any credential present, and return
    /// the verified identity. On failure, return a tonic `Status` —
    /// typically `Status::unauthenticated(...)`.
    async fn authenticate(&self, metadata: &MetadataMap) -> Result<Identity, Status>;
}

/// Helper that pulls `authorization: Bearer <token>` out of a
/// MetadataMap. Returns `None` if the header is absent or malformed,
/// rather than a Status, so callers can decide how to react (e.g. a
/// "demand auth" implementation rejects, an "anonymous fallback"
/// implementation does not).
pub(crate) fn bearer_token(metadata: &MetadataMap) -> Option<&str> {
    let raw = metadata.get("authorization")?.to_str().ok()?;
    let trimmed = raw.trim();
    let lower_prefix = trimmed.get(..7)?;
    if lower_prefix.eq_ignore_ascii_case("Bearer ") {
        Some(trimmed[7..].trim())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn md(value: &str) -> MetadataMap {
        let mut md = MetadataMap::new();
        md.insert("authorization", value.parse().unwrap());
        md
    }

    #[test]
    fn parses_standard_bearer() {
        assert_eq!(bearer_token(&md("Bearer abc123")), Some("abc123"));
    }

    #[test]
    fn parses_lower_case_bearer() {
        assert_eq!(bearer_token(&md("bearer abc123")), Some("abc123"));
    }

    #[test]
    fn ignores_extra_whitespace() {
        assert_eq!(bearer_token(&md("  Bearer    abc123  ")), Some("abc123"));
    }

    #[test]
    fn rejects_non_bearer() {
        assert!(bearer_token(&md("Basic dXNlcjpwYXNz")).is_none());
    }

    #[test]
    fn rejects_missing_header() {
        assert!(bearer_token(&MetadataMap::new()).is_none());
    }
}
