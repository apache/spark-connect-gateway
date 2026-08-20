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

//! `OidcAuthenticator` — verifies JWTs against a remote JWKS endpoint.
//!
//! On startup we fetch the JWKS once (or, if `discovery_url` is set,
//! we resolve the OIDC discovery document first to find the JWKS URL).
//! Keys are cached in memory; if a token's `kid` is missing from the
//! cache, we refresh once before failing the request — that handles
//! the IdP rotating keys without us paying a network round-trip per
//! request.
//!
//! Validation rules (issuer, audience, claim mapping) are reused from
//! [`crate::jwt`] — JWKS only changes *where* the verification key
//! comes from.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use parking_lot::RwLock;
use serde::Deserialize;
use tokio::sync::Mutex as AsyncMutex;
use tonic::metadata::MetadataMap;
use tonic::Status;
use tracing::{debug, warn};

use crate::{bearer_token, Authenticator, Identity};

#[derive(Debug, Clone, Deserialize)]
pub struct OidcConfig {
    /// JWKS URL. Either this or `discovery_url` must be set.
    #[serde(default)]
    pub jwks_url: Option<String>,
    /// OIDC discovery document URL. We resolve `jwks_uri` from it.
    #[serde(default)]
    pub discovery_url: Option<String>,
    pub algorithms: Vec<String>,
    #[serde(default)]
    pub issuer: Option<String>,
    #[serde(default)]
    pub audience: Option<String>,
    #[serde(default = "default_user_id_claim")]
    pub user_id_claim: String,
    #[serde(default)]
    pub tenant_claim: Option<String>,
    #[serde(default)]
    pub groups_claim: Option<String>,
    /// Minimum interval between forced refreshes when an unknown `kid`
    /// is seen. Prevents a barrage of bogus tokens from hammering the
    /// IdP. Defaults to 60s.
    #[serde(default = "default_refresh_floor_secs")]
    pub refresh_floor_secs: u64,
}

fn default_user_id_claim() -> String {
    "sub".into()
}
fn default_refresh_floor_secs() -> u64 {
    60
}

#[derive(Debug, thiserror::Error)]
pub enum OidcError {
    #[error("oidc: must set jwks_url or discovery_url")]
    NoJwksSource,
    #[error("oidc: at least one algorithm must be configured")]
    NoAlgorithms,
    #[error("oidc: unknown algorithm {0}")]
    UnknownAlg(String),
    #[error("oidc: HTTP error fetching {url}: {source}")]
    Http {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("oidc: discovery document at {url} missing jwks_uri")]
    DiscoveryMissingJwks { url: String },
    #[error("oidc: parse JWKS at {url}: {source}")]
    JwksParse {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("oidc: bad PEM in JWKS for kid {kid}: {source}")]
    BadKey {
        kid: String,
        #[source]
        source: jsonwebtoken::errors::Error,
    },
}

#[derive(Debug, Deserialize)]
struct DiscoveryDoc {
    jwks_uri: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Jwks {
    keys: Vec<JwksKey>,
}

#[derive(Debug, Deserialize)]
struct JwksKey {
    kid: Option<String>,
    kty: String,
    // We deserialize but don't enforce `alg` per-key; per-token alg is
    // already constrained by the configured algorithms list.
    #[serde(default, rename = "alg")]
    _alg: Option<String>,
    // RSA modulus / exponent
    #[serde(default)]
    n: Option<String>,
    #[serde(default)]
    e: Option<String>,
    // EC coordinates
    #[serde(default)]
    crv: Option<String>,
    #[serde(default)]
    x: Option<String>,
    #[serde(default)]
    y: Option<String>,
}

struct CachedKeys {
    keys: HashMap<String, DecodingKey>,
    last_refreshed: Instant,
}

pub struct OidcAuthenticator {
    jwks_url: String,
    http: reqwest::Client,
    cache: Arc<RwLock<CachedKeys>>,
    refresh_lock: Arc<AsyncMutex<()>>,
    refresh_floor: Duration,

    algorithms: Vec<Algorithm>,
    issuer: Option<String>,
    audience: Option<String>,
    user_id_claim: String,
    tenant_claim: Option<String>,
    groups_claim: Option<String>,
}

impl OidcAuthenticator {
    /// Build the authenticator and perform an initial JWKS fetch.
    pub async fn new(cfg: OidcConfig) -> Result<Self, OidcError> {
        if cfg.algorithms.is_empty() {
            return Err(OidcError::NoAlgorithms);
        }
        let algorithms: Vec<Algorithm> = cfg
            .algorithms
            .iter()
            .map(|a| parse_algorithm(a))
            .collect::<Result<_, _>>()?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("reqwest client");

        let jwks_url = match (&cfg.jwks_url, &cfg.discovery_url) {
            (Some(u), _) => u.clone(),
            (None, Some(u)) => resolve_jwks_url(&http, u).await?,
            (None, None) => return Err(OidcError::NoJwksSource),
        };

        let keys = fetch_jwks(&http, &jwks_url).await?;
        let cache = Arc::new(RwLock::new(CachedKeys {
            keys,
            last_refreshed: Instant::now(),
        }));

        Ok(Self {
            jwks_url,
            http,
            cache,
            refresh_lock: Arc::new(AsyncMutex::new(())),
            refresh_floor: Duration::from_secs(cfg.refresh_floor_secs),
            algorithms,
            issuer: cfg.issuer,
            audience: cfg.audience,
            user_id_claim: cfg.user_id_claim,
            tenant_claim: cfg.tenant_claim,
            groups_claim: cfg.groups_claim,
        })
    }

    /// Look up the decoding key for `kid`, refreshing the JWKS once
    /// (subject to the refresh floor) on miss.
    async fn key_for(&self, kid: &str) -> Option<DecodingKey> {
        if let Some(k) = self.cache.read().keys.get(kid) {
            return Some(k.clone());
        }
        // Miss → try to refresh, but rate-limited.
        let guard = self.refresh_lock.lock().await;
        // Re-check under lock — another caller may have refreshed already.
        if let Some(k) = self.cache.read().keys.get(kid) {
            return Some(k.clone());
        }
        let elapsed = self.cache.read().last_refreshed.elapsed();
        if elapsed < self.refresh_floor {
            debug!(?elapsed, "oidc: skipping refresh — within floor");
            return None;
        }
        match fetch_jwks(&self.http, &self.jwks_url).await {
            Ok(new_keys) => {
                // Scope the write guard so it's dropped before the
                // subsequent read — `parking_lot::RwLock` deadlocks
                // if the same thread holds a write guard while
                // attempting to acquire a read guard.
                {
                    let mut g = self.cache.write();
                    g.keys = new_keys;
                    g.last_refreshed = Instant::now();
                }
                drop(guard);
                self.cache.read().keys.get(kid).cloned()
            }
            Err(e) => {
                warn!(error = %e, "oidc: JWKS refresh failed");
                None
            }
        }
    }

    fn validation(&self) -> Validation {
        let mut v = Validation::new(self.algorithms[0]);
        v.algorithms = self.algorithms.clone();
        if let Some(iss) = &self.issuer {
            v.set_issuer(std::slice::from_ref(iss));
        }
        if let Some(aud) = &self.audience {
            v.set_audience(std::slice::from_ref(aud));
        } else {
            v.validate_aud = false;
        }
        v
    }
}

#[async_trait]
impl Authenticator for OidcAuthenticator {
    async fn authenticate(&self, metadata: &MetadataMap) -> Result<Identity, Status> {
        let token = bearer_token(metadata).ok_or_else(|| {
            Status::unauthenticated("missing or malformed Authorization: Bearer header")
        })?;

        let header = decode_header(token).map_err(|e| {
            debug!(error = %e, "oidc: malformed JWT header");
            Status::unauthenticated("invalid JWT header")
        })?;
        let kid = header
            .kid
            .ok_or_else(|| Status::unauthenticated("JWT missing kid; OIDC requires keyed JWTs"))?;
        let key = self.key_for(&kid).await.ok_or_else(|| {
            debug!(%kid, "oidc: no matching key");
            Status::unauthenticated("JWT kid not in JWKS")
        })?;

        let claims = decode::<HashMap<String, serde_json::Value>>(token, &key, &self.validation())
            .map_err(|e| {
                debug!(error = %e, "oidc: validation failed");
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

async fn resolve_jwks_url(http: &reqwest::Client, url: &str) -> Result<String, OidcError> {
    let resp = http.get(url).send().await.map_err(|e| OidcError::Http {
        url: url.into(),
        source: e,
    })?;
    let doc: DiscoveryDoc = resp.json().await.map_err(|e| OidcError::Http {
        url: url.into(),
        source: e,
    })?;
    doc.jwks_uri
        .ok_or_else(|| OidcError::DiscoveryMissingJwks { url: url.into() })
}

async fn fetch_jwks(
    http: &reqwest::Client,
    url: &str,
) -> Result<HashMap<String, DecodingKey>, OidcError> {
    let resp = http.get(url).send().await.map_err(|e| OidcError::Http {
        url: url.into(),
        source: e,
    })?;
    let jwks: Jwks = resp.json().await.map_err(|e| OidcError::JwksParse {
        url: url.into(),
        source: e,
    })?;
    let mut out = HashMap::with_capacity(jwks.keys.len());
    for k in jwks.keys {
        let kid = k.kid.clone().unwrap_or_default();
        if kid.is_empty() {
            // No kid → can't index; skip.
            continue;
        }
        let dk = match k.kty.as_str() {
            "RSA" => match (k.n.as_deref(), k.e.as_deref()) {
                (Some(n), Some(e)) => {
                    DecodingKey::from_rsa_components(n, e).map_err(|err| OidcError::BadKey {
                        kid: kid.clone(),
                        source: err,
                    })?
                }
                _ => continue,
            },
            "EC" => match (k.crv.as_deref(), k.x.as_deref(), k.y.as_deref()) {
                (Some(_crv), Some(x), Some(y)) => {
                    DecodingKey::from_ec_components(x, y).map_err(|err| OidcError::BadKey {
                        kid: kid.clone(),
                        source: err,
                    })?
                }
                _ => continue,
            },
            other => {
                debug!(kty = %other, "oidc: skipping unsupported key type");
                continue;
            }
        };
        out.insert(kid, dk);
    }
    Ok(out)
}

fn parse_algorithm(name: &str) -> Result<Algorithm, OidcError> {
    match name.to_ascii_uppercase().as_str() {
        "RS256" => Ok(Algorithm::RS256),
        "RS384" => Ok(Algorithm::RS384),
        "RS512" => Ok(Algorithm::RS512),
        "PS256" => Ok(Algorithm::PS256),
        "PS384" => Ok(Algorithm::PS384),
        "PS512" => Ok(Algorithm::PS512),
        "ES256" => Ok(Algorithm::ES256),
        "ES384" => Ok(Algorithm::ES384),
        "EDDSA" => Ok(Algorithm::EdDSA),
        other => Err(OidcError::UnknownAlg(other.into())),
    }
}

#[cfg(test)]
mod tests {
    //! OIDC tests stand up a `wiremock` HTTP server that plays the role
    //! of the identity provider, exposing a JWKS document (and
    //! optionally an OIDC discovery document) backed by an RSA key
    //! pair we generate per-test. Tokens are signed with the private
    //! half using `jsonwebtoken`; the authenticator fetches the
    //! public half over HTTP and verifies. The whole loop runs
    //! in-process — no real IdP, no internet, no test fixtures on
    //! disk.
    //!
    //! Key rotation is exercised by swapping the JWKS body the mock
    //! server returns mid-test and driving a token signed under the
    //! new key through the authenticator; the first miss triggers a
    //! single re-fetch (subject to `refresh_floor_secs`) so the
    //! refresh path is the one being measured, not the static cache.
    use super::*;
    use base64::Engine;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::traits::PublicKeyParts;
    use rsa::{RsaPrivateKey, RsaPublicKey};
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// One RSA key pair plus its JWKS representation. Lives across a
    /// test so we can both sign tokens and serve the public half.
    struct TestKey {
        kid: String,
        private_pem: String,
        jwks_entry: serde_json::Value,
    }

    fn gen_key(kid: &str) -> TestKey {
        let mut rng = rand::thread_rng();
        let priv_key = RsaPrivateKey::new(&mut rng, 2048).expect("generate RSA key");
        let pub_key = RsaPublicKey::from(&priv_key);

        let private_pem = priv_key
            .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
            .expect("encode private key as PEM")
            .to_string();

        let n = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(pub_key.n().to_bytes_be());
        let e = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(pub_key.e().to_bytes_be());
        let jwks_entry = json!({
            "kid": kid,
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "n": n,
            "e": e,
        });

        TestKey {
            kid: kid.into(),
            private_pem,
            jwks_entry,
        }
    }

    fn jwks_body(keys: &[&TestKey]) -> serde_json::Value {
        json!({
            "keys": keys.iter().map(|k| k.jwks_entry.clone()).collect::<Vec<_>>()
        })
    }

    fn future_exp() -> usize {
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 600) as usize
    }

    fn past_exp() -> usize {
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 600) as usize
    }

    fn sign(key: &TestKey, claims: serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(key.kid.clone());
        encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(key.private_pem.as_bytes()).unwrap(),
        )
        .unwrap()
    }

    /// Sign with a kid that doesn't match the encoded key — useful for
    /// the "kid not in JWKS" path.
    fn sign_with_kid(key: &TestKey, kid: &str, claims: serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.into());
        encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(key.private_pem.as_bytes()).unwrap(),
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

    fn base_cfg(jwks_url: String) -> OidcConfig {
        OidcConfig {
            jwks_url: Some(jwks_url),
            discovery_url: None,
            algorithms: vec!["RS256".into()],
            issuer: None,
            audience: None,
            user_id_claim: "sub".into(),
            tenant_claim: Some("tenant".into()),
            groups_claim: Some("groups".into()),
            refresh_floor_secs: 0, // tests advance rotation explicitly
        }
    }

    #[tokio::test]
    async fn valid_token_resolves_identity() {
        let key = gen_key("key-1");
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(jwks_body(&[&key])))
            .mount(&server)
            .await;

        let auth = OidcAuthenticator::new(base_cfg(format!("{}/jwks", server.uri())))
            .await
            .unwrap();
        let token = sign(
            &key,
            json!({
                "sub": "alice",
                "exp": future_exp(),
                "tenant": "team-a",
                "groups": ["devs", "admins"],
            }),
        );
        let id = auth.authenticate(&md(&token)).await.unwrap();
        assert_eq!(id.user_id, "alice");
        assert_eq!(id.tenant.as_deref(), Some("team-a"));
        assert_eq!(id.groups, vec!["devs", "admins"]);
    }

    #[tokio::test]
    async fn discovery_url_resolves_jwks_uri() {
        let key = gen_key("key-1");
        let server = MockServer::start().await;
        let jwks_uri = format!("{}/keys", server.uri());
        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "jwks_uri": jwks_uri })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/keys"))
            .respond_with(ResponseTemplate::new(200).set_body_json(jwks_body(&[&key])))
            .mount(&server)
            .await;

        let mut cfg = base_cfg(String::new());
        cfg.jwks_url = None;
        cfg.discovery_url = Some(format!("{}/.well-known/openid-configuration", server.uri()));

        let auth = OidcAuthenticator::new(cfg).await.unwrap();
        let token = sign(&key, json!({ "sub": "alice", "exp": future_exp() }));
        let id = auth.authenticate(&md(&token)).await.unwrap();
        assert_eq!(id.user_id, "alice");
    }

    #[tokio::test]
    async fn discovery_doc_missing_jwks_uri_is_clear_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/openid-configuration"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "issuer": "x" })))
            .mount(&server)
            .await;

        let mut cfg = base_cfg(String::new());
        cfg.jwks_url = None;
        cfg.discovery_url = Some(format!("{}/.well-known/openid-configuration", server.uri()));

        let err = match OidcAuthenticator::new(cfg).await {
            Ok(_) => panic!("expected DiscoveryMissingJwks error"),
            Err(e) => e,
        };
        assert!(
            matches!(err, OidcError::DiscoveryMissingJwks { .. }),
            "got: {err:?}"
        );
    }

    #[tokio::test]
    async fn initial_jwks_fetch_failure_propagates() {
        // No mock mounted → wiremock returns 404 for every request.
        let server = MockServer::start().await;
        let err = match OidcAuthenticator::new(base_cfg(format!("{}/jwks", server.uri()))).await {
            Ok(_) => panic!("expected JwksParse or Http error"),
            Err(e) => e,
        };
        assert!(
            matches!(err, OidcError::JwksParse { .. } | OidcError::Http { .. }),
            "got: {err:?}"
        );
    }

    #[tokio::test]
    async fn expired_token_rejected() {
        let key = gen_key("key-1");
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(jwks_body(&[&key])))
            .mount(&server)
            .await;

        let auth = OidcAuthenticator::new(base_cfg(format!("{}/jwks", server.uri())))
            .await
            .unwrap();
        let token = sign(&key, json!({ "sub": "alice", "exp": past_exp() }));
        let err = auth.authenticate(&md(&token)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn issuer_mismatch_rejected() {
        let key = gen_key("key-1");
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(jwks_body(&[&key])))
            .mount(&server)
            .await;

        let mut cfg = base_cfg(format!("{}/jwks", server.uri()));
        cfg.issuer = Some("https://expected".into());

        let auth = OidcAuthenticator::new(cfg).await.unwrap();
        let token = sign(
            &key,
            json!({
                "sub": "alice",
                "exp": future_exp(),
                "iss": "https://attacker",
            }),
        );
        let err = auth.authenticate(&md(&token)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn audience_mismatch_rejected() {
        let key = gen_key("key-1");
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(jwks_body(&[&key])))
            .mount(&server)
            .await;

        let mut cfg = base_cfg(format!("{}/jwks", server.uri()));
        cfg.audience = Some("scg".into());

        let auth = OidcAuthenticator::new(cfg).await.unwrap();
        let token = sign(
            &key,
            json!({
                "sub": "alice",
                "exp": future_exp(),
                "aud": "some-other-service",
            }),
        );
        let err = auth.authenticate(&md(&token)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn missing_kid_in_header_rejected() {
        let key = gen_key("key-1");
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(jwks_body(&[&key])))
            .mount(&server)
            .await;

        let auth = OidcAuthenticator::new(base_cfg(format!("{}/jwks", server.uri())))
            .await
            .unwrap();
        // Sign without setting header.kid.
        let token = encode(
            &Header::new(Algorithm::RS256),
            &json!({ "sub": "alice", "exp": future_exp() }),
            &EncodingKey::from_rsa_pem(key.private_pem.as_bytes()).unwrap(),
        )
        .unwrap();
        let err = auth.authenticate(&md(&token)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn unknown_kid_triggers_refresh_then_succeeds() {
        // Initial JWKS has only key-1; mid-test we'll rotate so that
        // key-2 appears. A token signed with key-2 should drive the
        // authenticator into the unknown-kid refresh path and succeed
        // on the retry.
        let key_old = gen_key("key-1");
        let key_new = gen_key("key-2");
        let server = MockServer::start().await;

        // First, only the old key is present.
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(jwks_body(&[&key_old])))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // After the first response is consumed, the rotated body is
        // served. wiremock matches mocks in priority + insertion order
        // so this one fires for subsequent GETs.
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(jwks_body(&[&key_old, &key_new])),
            )
            .mount(&server)
            .await;

        let auth = OidcAuthenticator::new(base_cfg(format!("{}/jwks", server.uri())))
            .await
            .unwrap();
        // Token signed under the new key, with the new key's kid.
        let token = sign(&key_new, json!({ "sub": "bob", "exp": future_exp() }));
        let id = auth.authenticate(&md(&token)).await.unwrap();
        assert_eq!(id.user_id, "bob");
    }

    #[tokio::test]
    async fn unknown_kid_still_rejected_after_refresh_if_absent() {
        // Even after a refresh, if the kid is not in the JWKS, reject.
        let key = gen_key("key-1");
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(jwks_body(&[&key])))
            .mount(&server)
            .await;

        let auth = OidcAuthenticator::new(base_cfg(format!("{}/jwks", server.uri())))
            .await
            .unwrap();
        // Sign with key-1 bytes but claim kid=ghost.
        let token = sign_with_kid(&key, "ghost", json!({ "sub": "x", "exp": future_exp() }));
        let err = auth.authenticate(&md(&token)).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[tokio::test]
    async fn refresh_floor_blocks_repeated_misses() {
        // With a non-zero refresh floor, a second unknown-kid attempt
        // within the window must NOT hit the JWKS endpoint again.
        let key = gen_key("key-1");
        let server = MockServer::start().await;
        // Expect at most ONE GET — the initial fetch. The refresh
        // floor must suppress any miss-driven re-fetch within the
        // window.
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(jwks_body(&[&key])))
            .expect(1)
            .mount(&server)
            .await;

        let mut cfg = base_cfg(format!("{}/jwks", server.uri()));
        cfg.refresh_floor_secs = 60;
        let auth = OidcAuthenticator::new(cfg).await.unwrap();

        // Two consecutive unknown-kid attempts.
        let ghost_token = sign_with_kid(&key, "ghost", json!({ "sub": "x", "exp": future_exp() }));
        let _ = auth.authenticate(&md(&ghost_token)).await;
        let _ = auth.authenticate(&md(&ghost_token)).await;

        // Mock's `.expect(1)` verifies on Drop that the endpoint was
        // hit exactly once.
    }

    #[tokio::test]
    async fn malformed_jwks_response_is_clear_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/jwks"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json at all"))
            .mount(&server)
            .await;

        let err = match OidcAuthenticator::new(base_cfg(format!("{}/jwks", server.uri()))).await {
            Ok(_) => panic!("expected JwksParse error"),
            Err(e) => e,
        };
        assert!(matches!(err, OidcError::JwksParse { .. }), "got: {err:?}");
    }
}
