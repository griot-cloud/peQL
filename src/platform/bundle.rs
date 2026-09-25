//! A parcel bundle with the T03 authority's ECDSA P-256 signature.

use p256::ecdsa::signature::{Signer, Verifier};
use p256::ecdsa::{Signature, SigningKey, VerifyingKey};
use parcel_runtime::bundle::Bundle;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct VerifyError(pub String);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedBundle {
    pub bundle: Bundle,
    /// DER-encoded signature over the signing payload, hex.
    pub signature_hex: String,
    pub metadata: SignedBundleMetadata,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedBundleMetadata {
    /// [`canonical_digest`] of the bundle, hex.
    pub bundle_hash_hex: String,
    pub signed_at_unix_ms: u64,
    /// Which generation of the authority's key signed it.
    pub key_generation: u32,
}

impl SignedBundle {
    pub fn from_json(bytes: &[u8]) -> Result<SignedBundle, VerifyError> {
        serde_json::from_slice(bytes).map_err(|e| VerifyError(format!("signed bundle: {e}")))
    }

    /// Sign a bundle: what the authority does.
    pub fn sign(
        bundle: Bundle,
        key: &SigningKey,
        key_generation: u32,
        signed_at_unix_ms: u64,
    ) -> SignedBundle {
        let digest = canonical_digest(&bundle);
        let payload = signing_payload(&digest, key_generation, signed_at_unix_ms);
        let sig: Signature = key.sign(&payload);
        SignedBundle {
            bundle,
            signature_hex: hex::encode(sig.to_der().as_bytes()),
            metadata: SignedBundleMetadata {
                bundle_hash_hex: hex::encode(digest),
                signed_at_unix_ms,
                key_generation,
            },
        }
    }

    /// Check the signature. The bundle's content is checked separately, by recompiling it.
    pub fn verify(&self, key: &VerifyingKey) -> Result<(), VerifyError> {
        let digest = canonical_digest(&self.bundle);
        if hex::encode(&digest) != self.metadata.bundle_hash_hex {
            return Err(VerifyError(
                "the bundle does not match the hash its metadata records".into(),
            ));
        }
        let payload = signing_payload(
            &digest,
            self.metadata.key_generation,
            self.metadata.signed_at_unix_ms,
        );
        let der = hex::decode(&self.signature_hex)
            .map_err(|e| VerifyError(format!("signature hex: {e}")))?;
        let sig = Signature::from_der(&der).map_err(|e| VerifyError(format!("signature: {e}")))?;
        key.verify(&payload, &sig)
            .map_err(|e| VerifyError(format!("signature verification failed: {e}")))
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

fn signing_payload(digest: &[u8], key_generation: u32, signed_at_unix_ms: u64) -> Vec<u8> {
    let mut h = Sha256::new();
    field(&mut h, b"t03:parcel-bundle-signing-payload:v1");
    field(&mut h, digest);
    field(&mut h, &key_generation.to_le_bytes());
    field(&mut h, &signed_at_unix_ms.to_le_bytes());
    h.finalize().to_vec()
}
