//! Fetch signed bundles from T03 and register them.

use std::sync::Arc;

use p256::ecdsa::VerifyingKey;

use super::bundle::SignedBundle;
use crate::engine::Engine;
use crate::error::{PeqlError, Result};
use crate::store::Registered;

pub struct PlatformBundleSource {
    base_url: String,
    client: reqwest::Client,
    verifying_key: Option<VerifyingKey>,
    auth_header: Option<(String, String)>,
}

impl PlatformBundleSource {
    /// Point at a T03 base URL, e.g. `https://t03.internal`.
    pub fn new(base_url: impl Into<String>) -> Self {
        PlatformBundleSource {
            base_url: base_url.into(),
            client: reqwest::Client::new(),
            verifying_key: None,
            auth_header: None,
        }
    }

    /// Verify every bundle's signature against `key`. Without one, trust rests on the transport.
    pub fn with_verifying_key(mut self, key: VerifyingKey) -> Self {
        self.verifying_key = Some(key);
        self
    }

    /// Send an auth header (e.g. `Authorization: Bearer ...`) with each request.
    pub fn with_auth(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.auth_header = Some((name.into(), value.into()));
        self
    }

    /// `GET {base}/v1/contracts/{contract}/bundle`, verified.
    pub async fn fetch(&self, contract: &str) -> Result<SignedBundle> {
        let url = format!(
            "{}/v1/contracts/{}/bundle",
            self.base_url.trim_end_matches('/'),
            contract
        );
        let mut req = self.client.get(&url);
        if let Some((name, value)) = &self.auth_header {
            req = req.header(name, value);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| PeqlError::Invalid(format!("fetch {url}: {e}")))?;
        let status = resp.status();
        let body = resp
            .bytes()
            .await
            .map_err(|e| PeqlError::Invalid(format!("read {url}: {e}")))?;
        if !status.is_success() {
            return Err(PeqlError::Invalid(format!(
                "T03 returned {status} for {url}"
            )));
        }
        let signed =
            SignedBundle::from_json(&body).map_err(|e| PeqlError::Invalid(e.to_string()))?;
        if let Some(key) = &self.verifying_key {
            signed
                .verify(key)
                .map_err(|e| PeqlError::Invalid(format!("bundle signature: {e}")))?;
        }
        Ok(signed)
    }

    /// Fetch, verify, and register a contract with `engine`.
    pub async fn register(&self, engine: &Engine, contract: &str) -> Result<Arc<Registered>> {
        let signed = self.fetch(contract).await?;
        if signed.bundle.contract != contract {
            return Err(PeqlError::Invalid(format!(
                "asked T03 for `{contract}` and received `{}`",
                signed.bundle.contract
            )));
        }
        engine.register_bundle(&signed.bundle)
    }
}
