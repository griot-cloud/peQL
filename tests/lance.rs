//! Lance datasets under contracts (feature `lance`), opened from a path.
#![cfg(all(unix, feature = "lance"))]

use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use lance::deps::arrow_array::{self as la, RecordBatchIterator};
use lance::deps::arrow_schema as ls;
use peql::lance_table::LanceTableProvider;
use peql::{Caller, Engine};

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
