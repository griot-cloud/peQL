//! Vector search crosses Flight SQL while rechecking the current caller's contract view.
#![cfg(all(unix, feature = "flight"))]
mod vector_support;
use arrow_flight::{decode::DecodedPayload, sql::client::FlightSqlServiceClient};
use futures::StreamExt;
use hyper_util::rt::TokioIo;
use peql::{
    Caller,
    audit::{MemoryAudit, Outcome},
    flight::{CALLER_HEADER, FlightSql, caller_header, serve_unix},
};
use std::{path::Path, sync::Arc};
use tonic::transport::{Channel, Endpoint, Uri};
use vector_support::*;
async fn client(path: &Path, caller: &Caller) -> FlightSqlServiceClient<Channel> {
    let path = path.to_path_buf();
    let channel = Endpoint::try_from("http://[::]:0")
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let path = path.clone();
            async move {
                Ok::<_, std::io::Error>(TokioIo::new(tokio::net::UnixStream::connect(path).await?))
            }
        }))
        .await
        .unwrap();
    let mut client = FlightSqlServiceClient::new(channel);
    client.set_header(CALLER_HEADER, caller_header(caller));
    client
}
#[tokio::test]
async fn flight_ranks_the_current_callers_masked_rows_and_audits_refusals() {
    let dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(MemoryAudit::default());
    let engine = Arc::new(engine(dir.path(), audit.clone(), true).await);
    let path = dir.path().join("vector.sock");
    let listener = tokio::net::UnixListener::bind(&path).unwrap();
    let server = tokio::spawn(serve_unix(FlightSql::new(engine), listener));
    let mut owner_client = client(&path, &owner()).await;
    let mut guest_client = client(&path, &guest()).await;
    let query = sql("embedding", "[1, 0]", 1, "cosine");
    let info = owner_client.execute(query.clone(), None).await.unwrap();
    // The owner's descriptive ticket cannot make DoGet execute as the owner.
    let mut decoded = guest_client
        .do_get(info.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap()
        .into_inner();
    let mut batches = Vec::new();
    let mut envelope = serde_json::Value::Null;
    while let Some(frame) = decoded.next().await {
        let frame = frame.unwrap();
        match frame.payload {
            DecodedPayload::Schema(_) => {
                envelope = serde_json::from_slice::<serde_json::Value>(&frame.inner.app_metadata)
                    .unwrap()["envelope"]
                    .clone();
            }
            DecodedPayload::RecordBatch(batch) => batches.push(batch),
            DecodedPayload::None => {}
        }
    }
    assert_eq!(ids(&batches), vec![3]);
    assert_eq!(envelope["caller"]["tenant"], "partner");
    assert_eq!(envelope["rows"], 1);
    assert_eq!(envelope["contracts"][0]["contract"], "demo/vectors");
    let mut refused = guest();
    refused.purpose = "advertising".into();
    guest_client.set_header(CALLER_HEADER, caller_header(&refused));
    let error = guest_client.execute(query.clone(), None).await.unwrap_err();
    assert!(error.to_string().contains("denied"), "{error}");
    guest_client.set_header(CALLER_HEADER, caller_header(&guest()));
    let error = guest_client
        .execute(sql("embedding", "[1]", 1, "cosine"), None)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("dimension"), "{error}");
    let mut unpublished = guest();
    unpublished.tenant = "unpublished".into();
    guest_client.set_header(CALLER_HEADER, caller_header(&unpublished));
    assert!(guest_client.execute(query, None).await.is_err());
    // Scoped, not dropped: clippy's `await_holding_lock` judges the guard's lexical scope,
    // and the server's shutdown below awaits.
    {
        let records = audit.records.lock().unwrap();
        assert_eq!(records.len(), 4);
        assert!(matches!(records[0].outcome, Outcome::Answered));
        assert!(matches!(records[1].outcome, Outcome::Refused(_)));
        assert!(matches!(records[2].outcome, Outcome::Failed(_)));
        assert!(matches!(records[3].outcome, Outcome::Refused(_)));
    }
    drop(decoded);
    drop(owner_client);
    drop(guest_client);
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
