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

//! Withholding backend-only secrets from `Config` RPC responses.
//!
//! The backend's `spark.connect.authenticate.token` is the shared
//! secret between the gateway and its backends: the gateway presents
//! it so that a client bypassing the gateway is refused. Spark's
//! `SparkConnectConfigHandler` applies no denylist on the read path
//! (`handleGet` / `handleGetOption` / `handleGetWithDefault` /
//! `handleGetAll` all hand back whatever `RuntimeConfig` returns),
//! and `SQLConf.mergeSparkConf` copies every `SparkConf` entry —
//! static ones included — into the session config. A client can
//! therefore ask the backend for the token and read it back.
//!
//! In Spark's own direct client-to-server model that discloses
//! nothing: the client had to present the token to get connected.
//! Behind a gateway the token means something else — the client
//! authenticates with its own credential (JWT/OIDC) and is *not*
//! supposed to hold the gateway↔backend secret — so relaying the
//! value would hand any authorized user the means to bypass the
//! gateway entirely.
//!
//! The gateway therefore strips the value on the way out, and does
//! so unconditionally: a proxy cannot assume every backend it
//! forwards to has been patched.
//!
//! Two details make this less trivial than a key comparison:
//!
//! * `GetAll { prefix }` returns keys with the **prefix stripped**
//!   (`handleGetAll` does `key.substring(prefix.length)`), so a
//!   request for prefix `spark.connect.` comes back as
//!   `authenticate.token`. Matching has to re-join the prefix, or a
//!   prefixed query walks straight past the filter.
//! * Responses are filtered rather than requests. Dropping keys from
//!   the request would desynchronise positional expectations in
//!   clients that assume one response pair per requested key.
//! * "Unset" does not look the same on every operation: `GetAll`
//!   omits the key, `Get` / `GetOption` return a pair with no value,
//!   and `GetWithDefault` resolves to the default the caller passed
//!   in. Substituting the wrong one still withholds the secret, but
//!   makes the gateway's filtering observable.

use std::collections::HashMap;

use scg_genproto::pb;

/// Config keys the gateway never relays to a client. Compared
/// case-insensitively — Spark's own lookup is case-sensitive, so a
/// differently-cased request cannot retrieve the value anyway, but
/// matching loosely costs nothing and removes a class of doubt.
const WITHHELD_KEYS: &[&str] = &["spark.connect.authenticate.token"];

fn is_withheld(key: &str) -> bool {
    WITHHELD_KEYS
        .iter()
        .any(|blocked| key.eq_ignore_ascii_case(blocked))
}

/// What a withheld key should look like in the response, so that it
/// is indistinguishable from a key the backend never had.
enum Substitute {
    /// `GetAll` enumerates whatever exists, so an absent key simply
    /// is not listed. (Dropping the pair is also required, not just
    /// tidier: the Spark client asserts `hasValue` on every pair
    /// `GetAll` returns.)
    Drop,
    /// `Get` / `GetOption` report an unset key as a pair with no
    /// value, and callers expect one pair per requested key.
    NoValue,
    /// `GetWithDefault` resolves an unset key to the default the
    /// caller supplied, keyed here by config key. A key the caller
    /// asked about with no default of its own maps to `None`.
    Defaults(HashMap<String, Option<String>>),
}

/// Response-side redaction plan derived from a `ConfigRequest`.
pub(crate) struct ConfigGuard {
    /// Prefix `handleGetAll` stripped from the response keys, if the
    /// operation was a prefixed `GetAll`.
    prefix: String,
    substitute: Substitute,
}

impl ConfigGuard {
    pub(crate) fn for_request(req: &pb::ConfigRequest) -> Self {
        use pb::config_request::operation::OpType;
        match req.operation.as_ref().and_then(|o| o.op_type.as_ref()) {
            Some(OpType::GetAll(get_all)) => Self {
                prefix: get_all.prefix.clone().unwrap_or_default(),
                substitute: Substitute::Drop,
            },
            Some(OpType::GetWithDefault(get_with_default)) => {
                let defaults = get_with_default
                    .pairs
                    .iter()
                    .filter(|pair| is_withheld(&pair.key))
                    .map(|pair| (pair.key.clone(), pair.value.clone()))
                    .collect();
                Self {
                    prefix: String::new(),
                    substitute: Substitute::Defaults(defaults),
                }
            }
            _ => Self {
                prefix: String::new(),
                substitute: Substitute::NoValue,
            },
        }
    }

    /// Strip withheld values from `resp`. Returns the fully-qualified
    /// keys that were redacted, for audit.
    pub(crate) fn apply(&self, resp: &mut pb::ConfigResponse) -> Vec<String> {
        let mut redacted = Vec::new();
        // `GetAll` strips the prefix from the keys it returns, so the
        // pair key alone is not the config key.
        let full_key = |key: &str| format!("{}{}", self.prefix, key);

        match &self.substitute {
            Substitute::Drop => {
                resp.pairs.retain(|pair| {
                    let key = full_key(&pair.key);
                    if is_withheld(&key) {
                        redacted.push(key);
                        false
                    } else {
                        true
                    }
                });
            }
            Substitute::NoValue => {
                for pair in &mut resp.pairs {
                    let key = full_key(&pair.key);
                    if is_withheld(&key) {
                        redacted.push(key);
                        pair.value = None;
                    }
                }
            }
            Substitute::Defaults(defaults) => {
                for pair in &mut resp.pairs {
                    let key = full_key(&pair.key);
                    if is_withheld(&key) {
                        redacted.push(key.clone());
                        pair.value = defaults.get(&key).cloned().flatten();
                    }
                }
            }
        }
        redacted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(key: &str, value: Option<&str>) -> pb::KeyValue {
        pb::KeyValue {
            key: key.into(),
            value: value.map(str::to_string),
        }
    }

    fn request(op: pb::config_request::operation::OpType) -> pb::ConfigRequest {
        pb::ConfigRequest {
            operation: Some(pb::config_request::Operation { op_type: Some(op) }),
            ..Default::default()
        }
    }

    fn get_request(keys: &[&str]) -> pb::ConfigRequest {
        request(pb::config_request::operation::OpType::Get(
            pb::config_request::Get {
                keys: keys.iter().map(|k| k.to_string()).collect(),
            },
        ))
    }

    fn get_with_default_request(pairs: &[(&str, Option<&str>)]) -> pb::ConfigRequest {
        request(pb::config_request::operation::OpType::GetWithDefault(
            pb::config_request::GetWithDefault {
                pairs: pairs.iter().map(|(k, v)| pair(k, *v)).collect(),
            },
        ))
    }

    fn get_all_request(prefix: Option<&str>) -> pb::ConfigRequest {
        request(pb::config_request::operation::OpType::GetAll(
            pb::config_request::GetAll {
                prefix: prefix.map(str::to_string),
            },
        ))
    }

    fn response(pairs: Vec<pb::KeyValue>) -> pb::ConfigResponse {
        pb::ConfigResponse {
            pairs,
            ..Default::default()
        }
    }

    #[test]
    fn get_of_the_token_returns_no_value() {
        let guard = ConfigGuard::for_request(&get_request(&["spark.connect.authenticate.token"]));
        let mut resp = response(vec![pair(
            "spark.connect.authenticate.token",
            Some("SUPERSECRET"),
        )]);
        let redacted = guard.apply(&mut resp);

        assert_eq!(redacted, vec!["spark.connect.authenticate.token"]);
        // One pair per requested key is preserved; the value is gone.
        assert_eq!(resp.pairs.len(), 1);
        assert_eq!(resp.pairs[0].key, "spark.connect.authenticate.token");
        assert_eq!(resp.pairs[0].value, None);
    }

    #[test]
    fn unrelated_keys_pass_through_untouched() {
        let guard = ConfigGuard::for_request(&get_request(&[
            "spark.sql.shuffle.partitions",
            "spark.hadoop.fs.s3a.secret.key",
        ]));
        let mut resp = response(vec![
            pair("spark.sql.shuffle.partitions", Some("200")),
            // The operator's own secret — not ours to withhold; the
            // client set it and Spark's redaction policy owns it.
            pair("spark.hadoop.fs.s3a.secret.key", Some("user-secret")),
        ]);
        let redacted = guard.apply(&mut resp);

        assert!(redacted.is_empty());
        assert_eq!(resp.pairs[0].value.as_deref(), Some("200"));
        assert_eq!(resp.pairs[1].value.as_deref(), Some("user-secret"));
    }

    #[test]
    fn get_all_omits_the_token_entirely() {
        let guard = ConfigGuard::for_request(&get_all_request(None));
        let mut resp = response(vec![
            pair("spark.sql.shuffle.partitions", Some("200")),
            pair("spark.connect.authenticate.token", Some("SUPERSECRET")),
            pair("spark.app.name", Some("demo")),
        ]);
        let redacted = guard.apply(&mut resp);

        assert_eq!(redacted, vec!["spark.connect.authenticate.token"]);
        let keys: Vec<&str> = resp.pairs.iter().map(|p| p.key.as_str()).collect();
        assert_eq!(keys, vec!["spark.sql.shuffle.partitions", "spark.app.name"]);
    }

    #[test]
    fn get_all_with_prefix_matches_on_the_rejoined_key() {
        // `handleGetAll` returns keys with the prefix removed, so the
        // response carries `authenticate.token`, not the full key.
        // Matching the bare pair key would miss it entirely — this is
        // the bypass this test exists to prevent.
        let guard = ConfigGuard::for_request(&get_all_request(Some("spark.connect.")));
        let mut resp = response(vec![
            pair("authenticate.token", Some("SUPERSECRET")),
            pair("session.planCache.enabled", Some("true")),
        ]);
        let redacted = guard.apply(&mut resp);

        assert_eq!(redacted, vec!["spark.connect.authenticate.token"]);
        let keys: Vec<&str> = resp.pairs.iter().map(|p| p.key.as_str()).collect();
        assert_eq!(keys, vec!["session.planCache.enabled"]);
    }

    #[test]
    fn get_all_with_a_deeper_prefix_still_matches() {
        let guard = ConfigGuard::for_request(&get_all_request(Some("spark.connect.authenticate.")));
        let mut resp = response(vec![pair("token", Some("SUPERSECRET"))]);
        let redacted = guard.apply(&mut resp);

        assert_eq!(redacted, vec!["spark.connect.authenticate.token"]);
        assert!(resp.pairs.is_empty());
    }

    #[test]
    fn get_all_with_unrelated_prefix_is_untouched() {
        let guard = ConfigGuard::for_request(&get_all_request(Some("spark.sql.")));
        let mut resp = response(vec![pair("shuffle.partitions", Some("200"))]);
        let redacted = guard.apply(&mut resp);

        assert!(redacted.is_empty());
        assert_eq!(resp.pairs[0].value.as_deref(), Some("200"));
    }

    #[test]
    fn case_variations_are_withheld_too() {
        let guard = ConfigGuard::for_request(&get_request(&["SPARK.CONNECT.AUTHENTICATE.TOKEN"]));
        let mut resp = response(vec![pair(
            "SPARK.CONNECT.AUTHENTICATE.TOKEN",
            Some("SUPERSECRET"),
        )]);
        let redacted = guard.apply(&mut resp);

        assert_eq!(redacted.len(), 1);
        assert_eq!(resp.pairs[0].value, None);
    }

    #[test]
    fn get_with_default_resolves_to_the_callers_default() {
        // What an unset key does on this operation: the caller's own
        // default comes back. Returning no value would still withhold
        // the secret, but it would tell the caller that something
        // filtered the response.
        let guard = ConfigGuard::for_request(&get_with_default_request(&[
            ("spark.connect.authenticate.token", Some("fallback")),
            ("spark.sql.shuffle.partitions", Some("100")),
        ]));
        let mut resp = response(vec![
            pair("spark.connect.authenticate.token", Some("SUPERSECRET")),
            pair("spark.sql.shuffle.partitions", Some("200")),
        ]);
        let redacted = guard.apply(&mut resp);

        assert_eq!(redacted, vec!["spark.connect.authenticate.token"]);
        assert_eq!(resp.pairs[0].value.as_deref(), Some("fallback"));
        // The backend's answer for everything else is untouched — the
        // caller's default is not a substitute for a real value.
        assert_eq!(resp.pairs[1].value.as_deref(), Some("200"));
    }

    #[test]
    fn get_with_default_without_a_default_yields_no_value() {
        // `GetWithDefault` allows a pair with no value; an unset key
        // then resolves to nothing.
        let guard = ConfigGuard::for_request(&get_with_default_request(&[(
            "spark.connect.authenticate.token",
            None,
        )]));
        let mut resp = response(vec![pair(
            "spark.connect.authenticate.token",
            Some("SUPERSECRET"),
        )]);
        let redacted = guard.apply(&mut resp);

        assert_eq!(redacted.len(), 1);
        assert_eq!(resp.pairs[0].value, None);
    }

    #[test]
    fn get_option_of_an_unset_key_is_unchanged() {
        // A value-less pair is exactly what Spark returns for an
        // unset key — the redacted shape must be indistinguishable.
        let guard = ConfigGuard::for_request(&get_request(&["spark.not.set"]));
        let mut resp = response(vec![pair("spark.not.set", None)]);
        let redacted = guard.apply(&mut resp);

        assert!(redacted.is_empty());
        assert_eq!(resp.pairs[0].value, None);
    }
}
