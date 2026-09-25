//! Signed parcel bundles from the platform authority (feature `platform`).
#![cfg(feature = "platform")]

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use p256::ecdsa::SigningKey;
use peql::Engine;
use peql::platform::{PlatformBundleSource, SignedBundle};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

#[test]
fn signatures_verify_and_tampering_is_caught() {
    let dir = tempfile::tempdir().unwrap();
    let signed = SignedBundle::sign(bundle(dir.path()), &key(), 1, 1_700_000_000_000);
    let vk = *key().verifying_key();
    signed.verify(&vk).unwrap();
    let json = serde_json::to_vec(&signed).unwrap();
    SignedBundle::from_json(&json).unwrap().verify(&vk).unwrap();

    let mut tampered = signed.clone();
    tampered.bundle.version = 4;
    assert!(tampered.verify(&vk).is_err());
    let other = SigningKey::from_slice(&[9u8; 32]).unwrap();
    assert!(signed.verify(other.verifying_key()).is_err());
}

#[tokio::test]
async fn fetched_bundles_are_verified_and_registered() {
    let dir = tempfile::tempdir().unwrap();
    let signed = SignedBundle::sign(bundle(dir.path()), &key(), 1, 1_700_000_000_000);
    let body = Arc::new(serde_json::to_vec(&signed).unwrap());
    // A one-route stand-in for T03.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            let body = body.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let n = s.read(&mut buf).await.unwrap();
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let (status, payload): (&str, Vec<u8>) =
                    if req.starts_with("GET /v1/contracts/demo/users/bundle") {
                        ("200 OK", body.to_vec())
                    } else {
                        ("404 Not Found", Vec::new())
                    };
                let head = format!(
                    "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    payload.len()
                );
                s.write_all(head.as_bytes()).await.unwrap();
                s.write_all(&payload).await.unwrap();
            });
        }
    });
    let engine = Engine::in_memory(dir.path());
    let source = PlatformBundleSource::new(format!("http://{addr}"))
        .with_verifying_key(*key().verifying_key());
    let reg = source.register(&engine, "demo/users").await.unwrap();
    assert_eq!(reg.compilation.contract.version, 3);
    assert_eq!(reg.owner(), Some("demo"));
    // The wrong key refuses it.
    let wrong = PlatformBundleSource::new(format!("http://{addr}"))
        .with_verifying_key(*SigningKey::from_slice(&[9u8; 32]).unwrap().verifying_key());
    assert!(
        wrong
            .register(&Engine::in_memory(dir.path()), "demo/users")
            .await
            .is_err()
    );
    assert!(source.fetch("demo/other").await.is_err());
}
