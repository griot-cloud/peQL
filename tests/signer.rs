//! The envelope signer at the other end of a socket: one line of envelope in, one line out.
#![cfg(unix)]

mod support;

use std::path::Path;
use std::sync::Arc;

use peql::{Engine, PeqlError, SocketSigner, WriteMode};
use support::*;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

/// A signer that answers every envelope with `answer(envelope)`.
async fn answer_one<S>(stream: S, answer: fn(&serde_json::Value) -> serde_json::Value)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut stream = BufReader::new(stream);
    let mut line = String::new();
    stream.read_line(&mut line).await.unwrap();
    let envelope: serde_json::Value = serde_json::from_str(&line).unwrap();
    let mut out = serde_json::to_vec(&answer(&envelope)).unwrap();
    out.push(b'\n');
    stream.get_mut().write_all(&out).await.unwrap();
}

fn spawn_unix_signer(path: &Path, answer: fn(&serde_json::Value) -> serde_json::Value) {
    let listener = tokio::net::UnixListener::bind(path).unwrap();
    tokio::spawn(async move {
        loop {
            let (s, _) = listener.accept().await.unwrap();
            tokio::spawn(answer_one(s, answer));
        }
    });
}

/// Signs what it can see: the query and result hashes, and who asked.
fn notary(envelope: &serde_json::Value) -> serde_json::Value {
    let a = &envelope["attestation"];
    serde_json::json!({
        "jws": format!(
            "h.{}.{}.{}",
            a["query_sha256"].as_str().unwrap(),
            a["result_sha256"].as_str().unwrap(),
            envelope["caller"]["tenant"].as_str().unwrap(),
        )
    })
}

fn refuser(_: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({"error": "no key for this tenant"})
}

async fn engine(dir: &Path) -> Engine {
    let e = Engine::open(dir).unwrap();
    e.register_contract(READINGS, &schema()).unwrap();
    e.write("demo/readings", vec![batch(1, 20)], WriteMode::Append)
        .await
        .unwrap();
    e
}

const SQL: &str = r#"SELECT id, kwh FROM "demo/readings""#;

#[tokio::test]
async fn every_answer_is_signed_over_a_unix_socket() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("signer.sock");
    spawn_unix_signer(&sock, notary);
    let e = engine(dir.path())
        .await
        .with_signer(Arc::new(SocketSigner::unix(&sock)));
    let res = e.query(SQL, &owner()).await.unwrap();
    let a = &res.envelope.attestation;
    assert_eq!(
        res.signature.as_deref(),
        Some(format!("h.{}.{}.demo", a.query_sha256, a.result_sha256).as_str())
    );
    assert_eq!(ids(&res.batches), owner_ids(20));
}

#[tokio::test]
async fn over_tcp_too() {
    let dir = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (s, _) = listener.accept().await.unwrap();
            tokio::spawn(answer_one(s, notary));
        }
    });
    let e = engine(dir.path())
        .await
        .with_signer(Arc::new(SocketSigner::tcp(addr.to_string())));
    assert!(e.query(SQL, &owner()).await.unwrap().signature.is_some());
}

#[tokio::test]
async fn no_signature_no_answer() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("signer.sock");
    spawn_unix_signer(&sock, refuser);
    let e = engine(dir.path())
        .await
        .with_signer(Arc::new(SocketSigner::unix(&sock)));
    match e.query(SQL, &owner()).await {
        Err(PeqlError::Signing(m)) => assert!(m.contains("no key for this tenant"), "{m}"),
        other => panic!(
            "expected a signing failure, got {:?}",
            other.map(|r| r.envelope)
        ),
    }

    // Nobody listening is a failure too, not an unsigned answer.
    let other = tempfile::tempdir().unwrap();
    let e = engine(other.path())
        .await
        .with_signer(Arc::new(SocketSigner::unix(dir.path().join("absent.sock"))));
    assert!(matches!(
        e.query(SQL, &owner()).await,
        Err(PeqlError::Signing(_))
    ));
}

#[tokio::test]
async fn without_a_signer_nothing_is_signed() {
    let dir = tempfile::tempdir().unwrap();
    let res = engine(dir.path()).await.query(SQL, &owner()).await.unwrap();
    assert!(res.signature.is_none());
    assert_eq!(res.envelope.caller.tenant, "demo");
}
