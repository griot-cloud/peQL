//! Lance datasets under contracts (feature `lance`): opened from a path, and through a mock of
//! the T04 storaged socket that serves each object by path.
#![cfg(all(unix, feature = "lance"))]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use lance::deps::arrow_array::{self as la, RecordBatchIterator};
use lance::deps::arrow_schema as ls;
use peql::lance_table::LanceTableProvider;
use peql::{Caller, Engine};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const USERS: &str = r#"
contract: demo/users
version: 1
owner: demo
binding: {parquet: unused/}
expose: [{name: id, type: int64}, {name: email, type: utf8}, {name: tier, type: utf8}]
rules:
  - {id: gold_only, op: admit, expr: "row.tier == 'gold' || ctx.tenant == 'demo'"}
  - {id: mask, op: transform, column: email, expr: "ctx.tenant == 'demo' ? row.email : redact(row.email)"}
"#;

async fn write_dataset(dir: &Path) -> String {
    let schema = Arc::new(ls::Schema::new(vec![
        ls::Field::new("id", ls::DataType::Int64, false),
        ls::Field::new("email", ls::DataType::Utf8, true),
        ls::Field::new("tier", ls::DataType::Utf8, true),
    ]));
    let batch = la::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(la::Int64Array::from((1..=40).collect::<Vec<i64>>())),
            Arc::new(la::StringArray::from(
                (1..=40).map(|i| format!("u{i}@x.io")).collect::<Vec<_>>(),
            )),
            Arc::new(la::StringArray::from(
                (1..=40)
                    .map(|i| if i % 4 == 0 { "gold" } else { "basic" })
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let uri = dir.join("users.lance").display().to_string();
    lance::Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        uri.as_str(),
        None,
    )
    .await
    .unwrap();
    uri
}

fn peql_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("tier", DataType::Utf8, true),
    ])
}

async fn check(e: &Engine) {
    let outsider = Caller::new("u", "partner", "analytics");
    e.publish("demo/users", "partner").unwrap();
    let res = e
        .query(
            r#"SELECT id, email FROM "demo/users" WHERE id > 10 ORDER BY id"#,
            &outsider,
        )
        .await
        .unwrap();
    let ids: Vec<i64> = res
        .batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ids, (12..=40).step_by(4).collect::<Vec<i64>>());
    for b in &res.batches {
        let emails = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(emails.iter().all(|v| v == Some("***")));
    }
    let owner = e
        .query(
            r#"SELECT COUNT(*) FROM "demo/users""#,
            &Caller::new("o", "demo", "analytics"),
        )
        .await
        .unwrap();
    assert_eq!(owner.envelope.rows, 1);
    assert!(owner.envelope.scan.rows_scanned >= 40);
}

#[tokio::test]
async fn a_lance_dataset_under_a_contract() {
    let dir = tempfile::tempdir().unwrap();
    let uri = write_dataset(dir.path()).await;
    let e = Engine::in_memory(dir.path());
    e.register_contract(USERS, &peql_schema()).unwrap();
    let table = LanceTableProvider::open_uri(&uri).await.unwrap();
    e.bind_table("demo/users", Arc::new(table)).await.unwrap();
    check(&e).await;
}

/// A stand-in for T04: serves read (0x30), stat (0x31) and list (0x32) from a directory.
async fn serve_storaged(socket: PathBuf, root: PathBuf) {
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    loop {
        let (mut s, _) = listener.accept().await.unwrap();
        let root = root.clone();
        tokio::spawn(async move {
            let mut len = [0u8; 4];
            s.read_exact(&mut len).await.unwrap();
            let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
            s.read_exact(&mut body).await.unwrap();
            let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                req["tenant_id"], "demo",
                "T04 sees the tenant on every call"
            );
            let path = |p: &serde_json::Value| root.join(p.as_str().unwrap_or(""));
            let frame = |v: serde_json::Value| {
                let b = serde_json::to_vec(&v).unwrap();
                let mut out = (b.len() as u32).to_be_bytes().to_vec();
                out.extend(b);
                out
            };
            match req["opcode"].as_str().unwrap() {
                "0x30" => {
                    let data = std::fs::read(path(&req["path"])).unwrap();
                    let off = req["offset"].as_u64().unwrap() as usize;
                    let end = (off + req["length"].as_u64().unwrap() as usize).min(data.len());
                    s.write_all(&frame(serde_json::json!({"byte_count": end - off})))
                        .await
                        .unwrap();
                    s.write_all(&data[off..end]).await.unwrap();
                }
                "0x31" => {
                    let reply = match std::fs::metadata(path(&req["path"])) {
                        Ok(m) => {
                            serde_json::json!({"size": m.len(), "content_type": "application/x-lance", "format_version": "2"})
                        }
                        Err(_) => {
                            serde_json::json!({"error": "no such object", "error_code": "NOT_FOUND"})
                        }
                    };
                    s.write_all(&frame(reply)).await.unwrap();
                }
                "0x32" => {
                    let prefix = req["prefix"].as_str().unwrap_or("").to_owned();
                    let mut objects = Vec::new();
                    let mut stack = vec![root.clone()];
                    while let Some(d) = stack.pop() {
                        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
                            let p = e.path();
                            if p.is_dir() {
                                stack.push(p);
                            } else {
                                let rel = p.strip_prefix(&root).unwrap().display().to_string();
                                if rel.starts_with(&prefix) {
                                    objects.push(serde_json::json!({"path": rel, "size": e.metadata().unwrap().len()}));
                                }
                            }
                        }
                    }
                    s.write_all(&frame(serde_json::json!({"objects": objects})))
                        .await
                        .unwrap();
                }
                other => panic!("unexpected opcode {other}"),
            }
        });
    }
}

#[tokio::test]
async fn a_lance_dataset_through_storaged() {
    let dir = tempfile::tempdir().unwrap();
    let uri = write_dataset(dir.path()).await;
    let socket = dir.path().join("t04.sock");
    tokio::spawn(serve_storaged(socket.clone(), PathBuf::from(&uri)));
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let e = Engine::in_memory(dir.path());
    e.register_contract(USERS, &peql_schema()).unwrap();
    let table = LanceTableProvider::open("asset-1", "demo", "jwt", socket.to_str().unwrap())
        .await
        .unwrap_or_else(|x| panic!("{x}"));
    e.bind_table("demo/users", Arc::new(table)).await.unwrap();
    check(&e).await;
}
