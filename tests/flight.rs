//! Flight SQL over a Unix socket: GetFlightInfo refuses before any scan, DoGet answers with the
//! envelope in the stream, DoPut writes under the contract, by its owner only.
#![cfg(all(unix, feature = "flight"))]

mod support;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_flight::decode::DecodedPayload;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{
    CommandStatementIngest, TableDefinitionOptions, TableExistsOption, TableNotExistOption,
};
use datafusion::arrow::array::RecordBatch;
use futures::StreamExt;
use hyper_util::rt::TokioIo;
use peql::flight::{CALLER_HEADER, FlightSql, caller_header, serve_unix};
use peql::{Caller, Engine, SocketSigner};
use support::*;
use tonic::transport::{Channel, Endpoint, Uri};

async fn serve(service: FlightSql, sock: &Path) {
    let listener = tokio::net::UnixListener::bind(sock).unwrap();
    tokio::spawn(serve_unix(service, listener));
}

async fn client(sock: &Path, caller: Option<&Caller>) -> FlightSqlServiceClient<Channel> {
    let sock: PathBuf = sock.to_path_buf();
    // The URI is ignored: every connection is the socket.
    let channel = Endpoint::try_from("http://[::]:0")
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let sock = sock.clone();
            async move {
                Ok::<_, std::io::Error>(TokioIo::new(tokio::net::UnixStream::connect(sock).await?))
            }
        }))
        .await
        .unwrap();
    let mut c = FlightSqlServiceClient::new(channel);
    if let Some(caller) = caller {
        c.set_header(CALLER_HEADER, caller_header(caller));
    }
    c
}

fn ingest(mode: TableExistsOption) -> CommandStatementIngest {
    CommandStatementIngest {
        table_definition_options: Some(TableDefinitionOptions {
            if_not_exist: TableNotExistOption::Fail as i32,
            if_exists: mode as i32,
        }),
        table: "demo/readings".into(),
        ..Default::default()
    }
}

async fn put(
    c: &mut FlightSqlServiceClient<Channel>,
    mode: TableExistsOption,
    batch: RecordBatch,
) -> Result<i64, String> {
    c.execute_ingest(ingest(mode), futures::stream::iter(vec![Ok(batch)]))
        .await
        .map_err(|e| e.to_string())
}

/// GetFlightInfo, then DoGet on its ticket: the batches and the first message's app metadata.
async fn get(
    c: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> Result<(Vec<RecordBatch>, serde_json::Value), String> {
    let info = c
        .execute(sql.into(), None)
        .await
        .map_err(|e| e.to_string())?;
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    read(c, ticket).await
}

async fn read(
    c: &mut FlightSqlServiceClient<Channel>,
    ticket: arrow_flight::Ticket,
) -> Result<(Vec<RecordBatch>, serde_json::Value), String> {
    let mut decoded = c
        .do_get(ticket)
        .await
        .map_err(|e| e.to_string())?
        .into_inner();
    let mut batches = Vec::new();
    let mut meta = serde_json::Value::Null;
    while let Some(d) = decoded.next().await {
        let d = d.map_err(|e| e.to_string())?;
        match d.payload {
            DecodedPayload::Schema(_) => {
                meta = serde_json::from_slice(&d.inner.app_metadata).unwrap();
            }
            DecodedPayload::RecordBatch(b) => batches.push(b),
            DecodedPayload::None => {}
        }
    }
    Ok((batches, meta))
}

fn engine(dir: &Path) -> Engine {
    let e = Engine::open(dir).unwrap();
    e.register_contract(READINGS, &schema()).unwrap();
    e.publish("demo/readings", "partner").unwrap();
    e
}

#[tokio::test]
async fn do_put_then_do_get_over_a_unix_socket() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("flight.sock");
    serve(FlightSql::new(Arc::new(engine(dir.path()))), &sock).await;

    let mut ana = client(&sock, Some(&owner())).await;
    assert_eq!(
        put(&mut ana, TableExistsOption::Append, batch(1, 30)).await,
        Ok(30)
    );
    assert_eq!(
        put(&mut ana, TableExistsOption::Append, batch(31, 40)).await,
        Ok(10)
    );

    let (batches, meta) = get(&mut ana, r#"SELECT id, meter FROM "demo/readings""#)
        .await
        .unwrap();
    assert_eq!(ids(&batches), owner_ids(40));
    let envelope = &meta["envelope"];
    assert_eq!(envelope["rows"], owner_ids(40).len());
    assert_eq!(envelope["caller"]["tenant"], "demo");
    assert_eq!(envelope["contracts"][0]["contract"], "demo/readings");
    assert!(envelope["attestation"]["result_sha256"].is_string());
    assert!(meta["signature"].is_null());

    // A guest reads the same contract through its view.
    let mut gus = client(&sock, Some(&guest())).await;
    let (batches, _) = get(&mut gus, r#"SELECT id, meter FROM "demo/readings""#)
        .await
        .unwrap();
    assert_eq!(ids(&batches), guest_ids(40));
    assert!(
        strings(&batches, "meter")
            .iter()
            .all(|m| m.as_deref() == Some("***"))
    );

    // Replace is an overwrite.
    assert_eq!(
        put(&mut ana, TableExistsOption::Replace, batch(1, 10)).await,
        Ok(10)
    );
    let (batches, _) = get(&mut ana, r#"SELECT id FROM "demo/readings""#)
        .await
        .unwrap();
    assert_eq!(ids(&batches), owner_ids(10));
}

#[tokio::test]
async fn prepared_statements_answer_like_statements() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("flight.sock");
    serve(FlightSql::new(Arc::new(engine(dir.path()))), &sock).await;
    let mut ana = client(&sock, Some(&owner())).await;
    put(&mut ana, TableExistsOption::Append, batch(1, 20))
        .await
        .unwrap();

    let mut stmt = ana
        .prepare(
            r#"SELECT region, COUNT(*) AS n FROM "demo/readings" GROUP BY region"#.into(),
            None,
        )
        .await
        .unwrap();
    let fields: Vec<_> = stmt
        .dataset_schema()
        .unwrap()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(fields, ["region", "n"]);
    let info = stmt.execute().await.unwrap();
    let (batches, meta) = read(&mut ana, info.endpoint[0].ticket.clone().unwrap())
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    assert_eq!(meta["envelope"]["rows"], 2);
    stmt.close().await.unwrap();
}

#[tokio::test]
async fn refusals_come_back_before_any_scan() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("flight.sock");
    let e = Arc::new(engine(dir.path()));
    serve(FlightSql::new(e.clone()), &sock).await;
    let mut ana = client(&sock, Some(&owner())).await;
    put(&mut ana, TableExistsOption::Append, batch(1, 20))
        .await
        .unwrap();
    let audited = || {
        std::fs::read_to_string(dir.path().join("_peql").join("audit.jsonl"))
            .unwrap_or_default()
            .lines()
            .count()
    };
    let before = audited();

    // `decide` refuses at GetFlightInfo, before any scan; the refusal is audited.
    let mut billing = client(&sock, Some(&Caller::new("bo", "demo", "billing"))).await;
    let err = get(&mut billing, r#"SELECT id FROM "demo/readings""#)
        .await
        .unwrap_err();
    assert!(
        err.contains("PermissionDenied") || err.contains("permission"),
        "{err}"
    );
    assert!(err.contains("analytics"), "{err}");
    assert_eq!(audited(), before + 1);

    // Only queries, only contracts, and never without a caller.
    let err = get(&mut ana, "DROP TABLE x").await.unwrap_err();
    assert!(err.contains("refused"), "{err}");
    let err = get(&mut ana, "SELECT * FROM secrets").await.unwrap_err();
    assert!(err.contains("no contract named"), "{err}");
    let mut anonymous = client(&sock, None).await;
    let err = get(&mut anonymous, r#"SELECT id FROM "demo/readings""#)
        .await
        .unwrap_err();
    assert!(err.contains(CALLER_HEADER), "{err}");

    // A ticket is only SQL: DoGet checks again for its own caller.
    let info = ana
        .execute(r#"SELECT id FROM "demo/readings""#.into(), None)
        .await
        .unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let err = read(&mut billing, ticket).await.unwrap_err();
    assert!(err.contains("analytics"), "{err}");

    // Writes are the owner's; `fail if exists` is refused, since the contract exists.
    let mut gus = client(&sock, Some(&guest())).await;
    let err = put(&mut gus, TableExistsOption::Append, batch(1, 2))
        .await
        .unwrap_err();
    assert!(err.contains("owner"), "{err}");
    let err = put(&mut ana, TableExistsOption::Fail, batch(1, 2))
        .await
        .unwrap_err();
    assert!(err.contains("append or replace"), "{err}");
    let mut other = ingest(TableExistsOption::Append);
    other.table = "demo/absent".into();
    let err = ana
        .execute_ingest(other, futures::stream::iter(vec![Ok(batch(1, 2))]))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no contract named"), "{err}");
}

#[tokio::test]
async fn one_fixed_caller_and_a_signed_envelope() {
    let dir = tempfile::tempdir().unwrap();
    let signer_sock = dir.path().join("signer.sock");
    let listener = tokio::net::UnixListener::bind(&signer_sock).unwrap();
    tokio::spawn(async move {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        loop {
            let (s, _) = listener.accept().await.unwrap();
            let mut s = BufReader::new(s);
            let mut line = String::new();
            s.read_line(&mut line).await.unwrap();
            let env: serde_json::Value = serde_json::from_str(&line).unwrap();
            let jws = format!(
                "sig-for-{}",
                env["attestation"]["result_sha256"].as_str().unwrap()
            );
            s.get_mut()
                .write_all(format!("{{\"jws\":\"{jws}\"}}\n").as_bytes())
                .await
                .unwrap();
        }
    });
    let e = engine(dir.path()).with_signer(Arc::new(SocketSigner::unix(&signer_sock)));
    e.write("demo/readings", vec![batch(1, 20)], peql::WriteMode::Append)
        .await
        .unwrap();
    let sock = dir.path().join("flight.sock");
    serve(FlightSql::for_caller(Arc::new(e), guest()), &sock).await;

    // The header names the owner; the service answers for the caller it was built for.
    let mut c = client(&sock, Some(&owner())).await;
    let (batches, meta) = get(&mut c, r#"SELECT id FROM "demo/readings""#)
        .await
        .unwrap();
    assert_eq!(ids(&batches), guest_ids(20));
    assert_eq!(meta["envelope"]["caller"]["tenant"], "partner");
    let sha = meta["envelope"]["attestation"]["result_sha256"]
        .as_str()
        .unwrap();
    assert_eq!(meta["signature"], format!("sig-for-{sha}"));
}
