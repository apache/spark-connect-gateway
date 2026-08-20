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

//! `JwtAuthenticator` — verifies a signed JWT against a local key.
//!
//! No network access at request time: the public key is loaded once
//! at startup. Suitable for deployments where the IdP signs tokens
//! offline and the public key is provisioned via config / Secret.
//!
//! For deployments where the IdP rotates keys via JWKS, see the
//! [`crate::oidc`] module.

use std::collections::HashMap;

use async_trait::async_trait;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use tonic::metadata::MetadataMap;
use tonic::Status;
use tracing::debug;

use crate::{bearer_token, Authenticator, Identity};

/// Where to find the verification key.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KeySource {
    /// Path to a PEM file on disk (RSA, EC, or Ed25519 public key).
    PemFile { path: String },
    /// Inline PEM contents (useful when injecting via env var or
    /// Kubernetes Secret).
    PemInline { pem: String },
    /// Inline HMAC secret. Discouraged for production.
    HmacSecret { secret: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct JwtConfig {
    pub key: KeySource,
    /// Permitted signing algorithms. Multiple values mean "any of",
    /// matching `jsonwebtoken::Validation::algorithms`.
    pub algorithms: Vec<String>,
    #[serde(default)]
    pub issuer: Option<String>,
    #[serde(default)]
    pub audience: Option<String>,
    /// Claim name that maps to `Identity.user_id`. Defaults to `sub`.
    #[serde(default = "default_user_id_claim")]
    pub user_id_claim: String,
    /// Optional tenant claim (mapped to `Identity.tenant`).
    #[serde(default)]
    pub tenant_claim: Option<String>,
    /// Optional groups claim. JWT must encode this as an array of
    /// strings.
    #[serde(default)]
    pub groups_claim: Option<String>,
}

fn default_user_id_claim() -> String {
    "sub".into()
}

#[derive(Debug, thiserror::Error)]
pub enum JwtError {
    #[error("read PEM file {path}: {source}")]
    KeyRead {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parse PEM key: {0}")]
    KeyParse(jsonwebtoken::errors::Error),
    #[error("unknown JWT algorithm: {0}")]
    UnknownAlg(String),
    #[error("at least one algorithm must be configured")]
    NoAlgorithms,
    #[error("user_id_claim must not be empty")]
    EmptyUserIdClaim,
}

pub struct JwtAuthenticator {
    decoding_key: DecodingKey,
    validation: Validation,
    user_id_claim: String,
    tenant_claim: Option<String>,
    groups_claim: Option<String>,
}

impl JwtAuthenticator {
    pub fn new(cfg: JwtConfig) -> Result<Self, JwtError> {
        if cfg.algorithms.is_empty() {
            return Err(JwtError::NoAlgorithms);
        }
        if cfg.user_id_claim.is_empty() {
            return Err(JwtError::EmptyUserIdClaim);
        }
        let algos: Vec<Algorithm> = cfg
            .algorithms
            .iter()
            .map(|a| parse_algorithm(a))
            .collect::<Result<_, _>>()?;

        let decoding_key = match &cfg.key {
            KeySource::PemFile { path } => {
                let bytes = std::fs::read(path).map_err(|e| JwtError::KeyRead {
                    path: path.clone(),
                    source: e,
                })?;
                build_pem_key(&algos, &bytes)?
            }
            KeySource::PemInline { pem } => build_pem_key(&algos, pem.as_bytes())?,
            KeySource::HmacSecret { secret } => DecodingKey::from_secret(secret.as_bytes()),
        };

        let mut validation = Validation::new(algos[0]);
        validation.algorithms = algos;
        if let Some(iss) = cfg.issuer {
            validation.set_issuer(&[iss]);
        }
        if let Some(aud) = cfg.audience {
            validation.set_audience(&[aud]);
        } else {
            validation.validate_aud = false;
        }

        Ok(Self {
            decoding_key,
            validation,
            user_id_claim: cfg.user_id_claim,
            tenant_claim: cfg.tenant_claim,
            groups_claim: cfg.groups_claim,
        })
    }
}

fn parse_algorithm(name: &str) -> Result<Algorithm, JwtError> {
    match name.to_ascii_uppercase().as_str() {
        "HS256" => Ok(Algorithm::HS256),
        "HS384" => Ok(Algorithm::HS384),
        "HS512" => Ok(Algorithm::HS512),
        "RS256" => Ok(Algorithm::RS256),
        "RS384" => Ok(Algorithm::RS384),
        "RS512" => Ok(Algorithm::RS512),
        "PS256" => Ok(Algorithm::PS256),
        "PS384" => Ok(Algorithm::PS384),
        "PS512" => Ok(Algorithm::PS512),
        "ES256" => Ok(Algorithm::ES256),
        "ES384" => Ok(Algorithm::ES384),
        "EDDSA" => Ok(Algorithm::EdDSA),
        other => Err(JwtError::UnknownAlg(other.into())),
    }
}

fn build_pem_key(algos: &[Algorithm], bytes: &[u8]) -> Result<DecodingKey, JwtError> {
    // Pick the parser by the first listed algorithm. Mixing RSA and EC
    // keys in one config doesn't make sense; if it ever did, callers
    // can run separate authenticators.
    let alg = algos[0];
    let res = match alg {
        Algorithm::RS256
        | Algorithm::RS384
        | Algorithm::RS512
        | Algorithm::PS256
        | Algorithm::PS384
        | Algorithm::PS512 => DecodingKey::from_rsa_pem(bytes),
        Algorithm::ES256 | Algorithm::ES384 => DecodingKey::from_ec_pem(bytes),
        Algorithm::EdDSA => DecodingKey::from_ed_pem(bytes),
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => {
            // Reaching here means caller passed `kind: pem_*` with an
            // HS algorithm — odd but handle by treating bytes as
            // secret.
            return Ok(DecodingKey::from_secret(bytes));
        }
    };
    res.map_err(JwtError::KeyParse)
}

#[async_trait]
impl Authenticator for JwtAuthenticator {
    async fn authenticate(&self, metadata: &MetadataMap) -> Result<Identity, Status> {
        let token = bearer_token(metadata).ok_or_else(|| {
            Status::unauthenticated("missing or malformed Authorization: Bearer header")
        })?;
        let claims = decode::<HashMap<String, serde_json::Value>>(
            token,
            &self.decoding_key,
            &self.validation,
        )
        .map_err(|e| {
            debug!(error = %e, "jwt validation failed");
            Status::unauthenticated("invalid JWT")
        })?;

        let user_id = claims
            .claims
            .get(&self.user_id_claim)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Status::unauthenticated(format!(
                    "JWT missing required claim {:?}",
                    self.user_id_claim
                ))
            })?
            .to_string();

        let tenant = self
            .tenant_claim
            .as_ref()
            .and_then(|c| claims.claims.get(c))
            .and_then(|v| v.as_str())
            .map(str::to_string);

        let groups: Vec<String> = self
            .groups_claim
            .as_ref()
            .and_then(|c| claims.claims.get(c))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Identity {
            user_id,
            tenant,
            groups,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde::Serialize;

    #[derive(Serialize)]
    struct Claims<'a> {
        sub: &'a str,
        exp: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        tenant: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        groups: Option<Vec<&'a str>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        iss: Option<&'a str>,
    }

    fn future_exp() -> usize {
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 600) as usize
    }

    fn past_exp() -> usize {
        // jsonwebtoken's default leeway is 60s, so we need to be
        // comfortably outside that window for the expiry check to fire.
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 600) as usize
    }

    fn sign(claims: Claims<'_>, secret: &[u8]) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    }

    fn md(token: &str) -> MetadataMap {
        let mut md = MetadataMap::new();
        md.insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        md
    }

    fn auth(secret: &str, issuer: Option<&str>) -> JwtAuthenticator {
        JwtAuthenticator::new(JwtConfig {
            key: KeySource::HmacSecret {
                secret: secret.into(),
            },
            algorithms: vec!["HS256".into()],
            issuer: issuer.map(str::to_string),
            audience: None,
            user_id_claim: "sub".into(),
            tenant_claim: Some("tenant".into()),
            groups_claim: Some("groups".into()),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn valid_token_resolves_identity() {
        let token = sign(
            Claims {
                sub: "alice",
                exp: future_exp(),
                tenant: Some("team-a"),
                groups: Some(vec!["devs", "admins"]),
                iss: None,
            },
            b"my-secret",
        );
        let id = auth("my-secret", None)
            .authenticate(&md(&token))
            .await
            .unwrap();
        assert_eq!(id.user_id, "alice");
        assert_eq!(id.tenant.as_deref(), Some("team-a"));
        assert_eq!(id.groups, vec!["devs", "admins"]);
    }

    #[tokio::test]
    async fn expired_token_rejected() {
        let token = sign(
            Claims {
                sub: "alice",
                exp: past_exp(),
                tenant: None,
                groups: None,
                iss: None,
            },
            b"my-secret",
        );
        let err = auth("my-secret", None)
            .authenticate(&md(&token))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn wrong_secret_rejected() {
        let token = sign(
            Claims {
                sub: "alice",
                exp: future_exp(),
                tenant: None,
                groups: None,
                iss: None,
            },
            b"my-secret",
        );
        let err = auth("OTHER-SECRET", None)
            .authenticate(&md(&token))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn issuer_mismatch_rejected() {
        let token = sign(
            Claims {
                sub: "alice",
                exp: future_exp(),
                tenant: None,
                groups: None,
                iss: Some("attacker"),
            },
            b"my-secret",
        );
        let err = auth("my-secret", Some("expected-iss"))
            .authenticate(&md(&token))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn missing_user_id_claim_rejected() {
        // Build a token without `sub`; we can't use Claims helper so
        // hand-roll one with serde_json.
        let claims = serde_json::json!({ "exp": future_exp() });
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"my-secret"),
        )
        .unwrap();
        let err = auth("my-secret", None)
            .authenticate(&md(&token))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }
}
