//! A parcel bundle with its issuer's ECDSA P-256 signature (feature `signed-bundle`).
//!
//! The engine only verifies: the signature proves who issued a bundle, and recompiling it to
//! its compilation hash (as every registration does) proves what it says. Signing belongs to
//! the issuer; [`SignedBundle::signing_payload`] is the exact byte string it signs.

use std::sync::Arc;

use p256::ecdsa::Signature;
use p256::ecdsa::signature::Verifier;
use parcel_runtime::bundle::Bundle;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use p256::ecdsa::VerifyingKey;

use crate::engine::Engine;
use crate::error::{PeqlError, Result};
use crate::store::Registered;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct VerifyError(pub String);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedBundle {
    pub bundle: Bundle,
    /// DER-encoded signature over [`SignedBundle::signing_payload`], hex.
    pub signature_hex: String,
    pub metadata: SignedBundleMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedBundleMetadata {
    /// [`canonical_digest`] of the bundle, hex.
    pub bundle_hash_hex: String,
    pub signed_at_unix_ms: u64,
    /// Which generation of the issuer's key signed it.
    pub key_generation: u32,
}

impl SignedBundle {
    pub fn from_json(bytes: &[u8]) -> std::result::Result<SignedBundle, VerifyError> {
        serde_json::from_slice(bytes).map_err(|e| VerifyError(format!("signed bundle: {e}")))
    }

    /// The bytes the issuer signs: [`SIGNING_PURPOSE`], a NUL, then a digest of the bundle's
    /// meaning, the key generation and the signing time.
    pub fn signing_payload(&self) -> Vec<u8> {
        signing_payload(
            &canonical_digest(&self.bundle),
            self.metadata.key_generation,
            self.metadata.signed_at_unix_ms,
        )
    }

    /// Check the signature. The bundle's content is checked separately, by recompiling it.
    pub fn verify(&self, key: &VerifyingKey) -> std::result::Result<(), VerifyError> {
        let digest = canonical_digest(&self.bundle);
        if hex::encode(&digest) != self.metadata.bundle_hash_hex {
            return Err(VerifyError(
                "the bundle does not match the hash its metadata records".into(),
            ));
        }
        let der = hex::decode(&self.signature_hex)
            .map_err(|e| VerifyError(format!("signature hex: {e}")))?;
        let sig = Signature::from_der(&der).map_err(|e| VerifyError(format!("signature: {e}")))?;
        key.verify(&self.signing_payload(), &sig)
            .map_err(|e| VerifyError(format!("signature verification failed: {e}")))
    }

    /// Verify the signature, then register the bundle (which recompiles it to its hash).
    pub fn register(&self, engine: &Engine, key: &VerifyingKey) -> Result<Arc<Registered>> {
        self.verify(key)
            .map_err(|e| PeqlError::Invalid(format!("bundle signature: {e}")))?;
        engine.register_bundle(&self.bundle)
    }
}

fn field(h: &mut Sha256, bytes: &[u8]) {
    h.update((bytes.len() as u32).to_be_bytes());
    h.update(bytes);
}

/// sha256 over what a bundle means: its format, name, version, contract and compilation
/// hashes, and every function module it carries. The compilation hash covers the compiled
/// artifacts, and recompiling the bundle proves they match it.
pub fn canonical_digest(b: &Bundle) -> Vec<u8> {
    let mut h = Sha256::new();
    field(&mut h, b.format.as_bytes());
    field(&mut h, b.contract.as_bytes());
    field(&mut h, &b.version.to_le_bytes());
    field(&mut h, b.contract_hash.as_bytes());
    field(&mut h, b.compilation_hash.as_bytes());
    let mut functions: Vec<_> = b.functions.iter().collect();
    functions.sort_by(|x, y| (&x.owner, &x.manifest.name).cmp(&(&y.owner, &y.manifest.name)));
    field(&mut h, &(functions.len() as u32).to_be_bytes());
    for f in functions {
        field(&mut h, f.owner.as_bytes());
        field(&mut h, f.manifest.name.as_bytes());
        field(&mut h, &f.manifest.version.to_le_bytes());
        field(&mut h, f.module.as_bytes());
    }
    h.finalize().to_vec()
}

/// The purpose a bundle signature is made for: the issuer signs
/// `SIGNING_PURPOSE || 0x00 || sha256(…)`, so a bundle signature never verifies as any other
/// signature the same key makes, and no other signature verifies as a bundle.
pub const SIGNING_PURPOSE: &[u8] = b"griot/bundle/v1";

fn signing_payload(digest: &[u8], key_generation: u32, signed_at_unix_ms: u64) -> Vec<u8> {
    let mut h = Sha256::new();
    field(&mut h, digest);
    field(&mut h, &key_generation.to_le_bytes());
    field(&mut h, &signed_at_unix_ms.to_le_bytes());
    let mut message = SIGNING_PURPOSE.to_vec();
    message.push(0);
    message.extend_from_slice(&h.finalize());
    message
}
