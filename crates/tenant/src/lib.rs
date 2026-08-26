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

//! Tenant resolution for multi-tenant routing.
//!
//! The gateway's routing key is `(tenant, user_id, session_id)`. The
//! tenant comes from one of three sources depending on deployment:
//!
//! * From the verified `Identity` produced by the auth interceptor
//!   (most common — JWT/OIDC carry a tenant claim).
//! * From an explicit gRPC metadata header (auth disabled but the
//!   client self-declares which tenant it speaks for).
//! * Fixed for the whole deployment (single-tenant deployments).
//!
//! Two policies on what to do when the source doesn't yield a tenant:
//!
//! * `UseDefault` — fall back to a configured name. Preserves
//!   single-tenant behaviour when the operator hasn't opted into
//!   multi-tenant routing.
//! * `Reject` — return `Unauthenticated` to the client. The right
//!   choice for SaaS-style deployments where a missing tenant claim
//!   is almost always an IdP misconfiguration.
//!
//! See `docs/deployment.md` for the operator-facing decision matrix.

use scg_auth::Identity;
use tonic::metadata::MetadataMap;
use tonic::Status;
use tracing::warn;

/// Where the gateway looks to find a tenant for an inbound RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantSource {
    /// Read from the verified `Identity.tenant` produced by the auth
    /// interceptor. Most JWT/OIDC deployments use this.
    FromClaim,
    /// Read from a gRPC metadata header. Used when auth is disabled
    /// but the client cooperates by declaring a tenant.
    FromMetadata { header: String },
    /// Always use the configured `default_name`. Single-tenant
    /// deployments that don't want to bother with auth claims or
    /// metadata headers.
    AlwaysDefault,
}

/// What to do when the configured source returns nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnMissing {
    /// Fall back to the configured `default_name`. Preserves
    /// single-tenant behaviour when the auth claim / metadata header
    /// is absent.
    UseDefault,
    /// Fail the RPC with `Unauthenticated`. Used by deployments that
    /// require every caller to identify a tenant; a missing tenant
    /// usually means the IdP configuration is broken and the safe
    /// default is to fail loudly.
    Reject,
}

/// Runtime configuration for a [`TenantResolver`]. Constructed from
/// `scg-config`'s `TenantResolverSettings`.
#[derive(Debug, Clone)]
pub struct TenantResolverConfig {
    pub source: TenantSource,
    pub on_missing: OnMissing,
    pub default_name: String,
}

impl Default for TenantResolverConfig {
    /// Single-tenant baseline: read tenant from the auth claim if
    /// present, fall back to `"default"` otherwise. A deployment
    /// that doesn't configure a `tenant_resolver:` block keeps
    /// single-tenant behaviour — every inbound RPC ends up in
    /// `tenant="default"`.
    fn default() -> Self {
        Self {
            source: TenantSource::FromClaim,
            on_missing: OnMissing::UseDefault,
            default_name: "default".into(),
        }
    }
}

/// Decides the tenant for each inbound RPC. Cheap to clone; share
/// across all RPC handlers.
#[derive(Debug, Clone)]
pub struct TenantResolver {
    cfg: TenantResolverConfig,
}

impl TenantResolver {
    pub fn new(cfg: TenantResolverConfig) -> Self {
        Self { cfg }
    }

    /// Resolve the tenant for an inbound RPC. Returns the tenant
    /// string (always non-empty) or `Unauthenticated` when
    /// `on_missing = Reject` and the configured source yielded
    /// nothing.
    pub fn resolve(&self, metadata: &MetadataMap, identity: &Identity) -> Result<String, Status> {
        let candidate: Option<String> = match &self.cfg.source {
            TenantSource::FromClaim => identity.tenant.clone(),
            TenantSource::FromMetadata { header } => metadata
                .get(header.as_str())
                .and_then(|v| v.to_str().ok())
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            TenantSource::AlwaysDefault => Some(self.cfg.default_name.clone()),
        };

        match candidate {
            Some(t) if !t.is_empty() => Ok(t),
            _ => match self.cfg.on_missing {
                OnMissing::UseDefault => Ok(self.cfg.default_name.clone()),
                OnMissing::Reject => {
                    warn!(
                        source = ?self.cfg.source,
                        user = %identity.user_id,
                        "tenant_resolver: rejecting RPC — no tenant available and on_missing=Reject"
                    );
                    Err(Status::unauthenticated("tenant required but not provided"))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    fn id_with_tenant(t: &str) -> Identity {
        let mut i = Identity::user("alice");
        i.tenant = Some(t.into());
        i
    }

    fn id_without_tenant() -> Identity {
        Identity::user("alice")
    }

    fn md_with(header: &str, value: &str) -> MetadataMap {
        let mut m = MetadataMap::new();
        let key =
            tonic::metadata::MetadataKey::<tonic::metadata::Ascii>::from_bytes(header.as_bytes())
                .unwrap();
        m.insert(key, MetadataValue::try_from(value).unwrap());
        m
    }

    #[test]
    fn from_claim_uses_default_when_missing() {
        let r = TenantResolver::new(TenantResolverConfig {
            source: TenantSource::FromClaim,
            on_missing: OnMissing::UseDefault,
            default_name: "default".into(),
        });
        let got = r
            .resolve(&MetadataMap::new(), &id_without_tenant())
            .unwrap();
        assert_eq!(got, "default");
    }

    #[test]
    fn from_claim_rejects_when_missing() {
        let r = TenantResolver::new(TenantResolverConfig {
            source: TenantSource::FromClaim,
            on_missing: OnMissing::Reject,
            default_name: "default".into(),
        });
        let err = r
            .resolve(&MetadataMap::new(), &id_without_tenant())
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn from_claim_honours_present_claim() {
        let r = TenantResolver::new(TenantResolverConfig {
            source: TenantSource::FromClaim,
            on_missing: OnMissing::Reject,
            default_name: "default".into(),
        });
        let got = r
            .resolve(&MetadataMap::new(), &id_with_tenant("team-a"))
            .unwrap();
        assert_eq!(got, "team-a");
    }

    #[test]
    fn from_metadata_reads_configured_header() {
        let r = TenantResolver::new(TenantResolverConfig {
            source: TenantSource::FromMetadata {
                header: "x-tenant".into(),
            },
            on_missing: OnMissing::Reject,
            default_name: "default".into(),
        });
        let md = md_with("x-tenant", "team-b");
        let got = r.resolve(&md, &id_without_tenant()).unwrap();
        assert_eq!(got, "team-b");
    }

    #[test]
    fn from_metadata_uses_default_when_header_missing() {
        let r = TenantResolver::new(TenantResolverConfig {
            source: TenantSource::FromMetadata {
                header: "x-tenant".into(),
            },
            on_missing: OnMissing::UseDefault,
            default_name: "fallback".into(),
        });
        let got = r
            .resolve(&MetadataMap::new(), &id_without_tenant())
            .unwrap();
        assert_eq!(got, "fallback");
    }

    #[test]
    fn from_metadata_rejects_when_header_missing() {
        let r = TenantResolver::new(TenantResolverConfig {
            source: TenantSource::FromMetadata {
                header: "x-tenant".into(),
            },
            on_missing: OnMissing::Reject,
            default_name: "default".into(),
        });
        let err = r
            .resolve(&MetadataMap::new(), &id_without_tenant())
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn from_metadata_treats_empty_header_value_as_missing() {
        let r = TenantResolver::new(TenantResolverConfig {
            source: TenantSource::FromMetadata {
                header: "x-tenant".into(),
            },
            on_missing: OnMissing::Reject,
            default_name: "default".into(),
        });
        let md = md_with("x-tenant", "");
        let err = r.resolve(&md, &id_without_tenant()).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn always_default_ignores_claim_and_header() {
        let r = TenantResolver::new(TenantResolverConfig {
            source: TenantSource::AlwaysDefault,
            on_missing: OnMissing::Reject, // never triggered
            default_name: "fixed".into(),
        });
        let md = md_with("x-tenant", "ignored");
        let got = r.resolve(&md, &id_with_tenant("also-ignored")).unwrap();
        assert_eq!(got, "fixed");
    }

    #[test]
    fn back_compat_default_resolves_to_default() {
        let r = TenantResolver::new(TenantResolverConfig::default());
        let got = r
            .resolve(&MetadataMap::new(), &id_without_tenant())
            .unwrap();
        assert_eq!(got, "default");
    }
}
