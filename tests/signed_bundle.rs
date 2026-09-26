//! Signed parcel bundles (feature `signed-bundle`): the engine verifies, the issuer signs.
#![cfg(feature = "signed-bundle")]

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey};
use peql::Engine;
use peql::signed_bundle::{SignedBundle, SignedBundleMetadata, canonical_digest};

const USERS: &str = r#"
contract: demo/users
version: 3
owner: demo
binding: {parquet: users/}
expose: [{name: id, type: int64}, {name: email, type: utf8}]
rules:
  - {id: analytics, op: decide, expr: "ctx.purpose == 'analytics'"}
  - {id: mask, op: transform, column: email, expr: "ctx.tenant == 'demo' ? row.email : hash_sha256(row.email)"}
"#;

fn bundle(dir: &std::path::Path) -> parcel_runtime::bundle::Bundle {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
    ]);
    let e = Engine::in_memory(dir);
    e.register_contract(USERS, &schema)
        .unwrap()
        .bundle()
        .unwrap()
}

fn key() -> SigningKey {
    SigningKey::from_slice(&[7u8; 32]).unwrap()
}

/// What an issuer does: sign the payload the engine will check.
fn sign(bundle: parcel_runtime::bundle::Bundle, key: &SigningKey) -> SignedBundle {
    let mut signed = SignedBundle {
        metadata: SignedBundleMetadata {
            bundle_hash_hex: hex::encode(canonical_digest(&bundle)),
            signed_at_unix_ms: 1_700_000_000_000,
            key_generation: 1,
        },
        bundle,
        signature_hex: String::new(),
    };
    let sig: Signature = key.sign(&signed.signing_payload());
    signed.signature_hex = hex::encode(sig.to_der().as_bytes());
    signed
}

#[test]
fn signatures_verify_and_tampering_is_caught() {
    let dir = tempfile::tempdir().unwrap();
    let signed = sign(bundle(dir.path()), &key());
    let vk = *key().verifying_key();
    signed.verify(&vk).unwrap();
    let json = serde_json::to_vec(&signed).unwrap();
    SignedBundle::from_json(&json).unwrap().verify(&vk).unwrap();

    let mut tampered = signed.clone();
    tampered.bundle.version = 4;
    assert!(tampered.verify(&vk).is_err());
    let mut later = signed.clone();
    later.metadata.signed_at_unix_ms += 1;
    assert!(later.verify(&vk).is_err());
    let other = SigningKey::from_slice(&[9u8; 32]).unwrap();
    assert!(signed.verify(other.verifying_key()).is_err());
}

/// The signed bytes start with the purpose, and a bundle signed under the retired
/// `t03:parcel-bundle-signing-payload:v1` domain (or with no purpose) does not verify.
#[test]
fn only_the_griot_bundle_purpose_verifies() {
    use sha2::{Digest, Sha256};
    let dir = tempfile::tempdir().unwrap();
    let signed = sign(bundle(dir.path()), &key());
    assert!(signed.signing_payload().starts_with(b"griot/bundle/v1\0"));
    let vk = *key().verifying_key();

    let digest = canonical_digest(&signed.bundle);
    let fields = |h: &mut Sha256, bytes: &[u8]| {
        h.update((bytes.len() as u32).to_be_bytes());
        h.update(bytes);
    };
    let mut old = Sha256::new();
    fields(&mut old, b"t03:parcel-bundle-signing-payload:v1");
    fields(&mut old, &digest);
    fields(&mut old, &signed.metadata.key_generation.to_le_bytes());
    fields(&mut old, &signed.metadata.signed_at_unix_ms.to_le_bytes());
    let old = old.finalize().to_vec();
    let unprefixed = signed.signing_payload()[b"griot/bundle/v1\0".len()..].to_vec();

    for message in [old, unprefixed] {
        let mut resigned = signed.clone();
        let sig: Signature = key().sign(&message);
        resigned.signature_hex = hex::encode(sig.to_der().as_bytes());
        assert!(resigned.verify(&vk).is_err());
    }
}

#[test]
fn a_verified_bundle_registers_and_a_forged_one_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let signed = sign(bundle(dir.path()), &key());
    let engine = Engine::in_memory(dir.path());
    let reg = signed.register(&engine, key().verifying_key()).unwrap();
    assert_eq!(reg.compilation.contract.version, 3);
    assert_eq!(reg.owner(), Some("demo"));

    let forged = sign(
        bundle(dir.path()),
        &SigningKey::from_slice(&[9u8; 32]).unwrap(),
    );
    let fresh = Engine::in_memory(dir.path());
    let err = forged
        .register(&fresh, key().verifying_key())
        .unwrap_err()
        .to_string();
    assert!(err.contains("bundle signature"), "{err}");
    assert!(fresh.contracts().is_empty());
}
