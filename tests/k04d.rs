//! The platform's tenant engine and its worker pool, both governed by parcel contracts.

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use peql::pool::{LongRunningPoolManager, PoolConfig, QueryTask};
use peql::{Caller, ContractBundleHandle, Engine, InitConfig, K04DEngine};

const USERS: &str = r#"
contract: demo/users
version: 1
owner: demo
binding: {parquet: users/}
expose: [{name: id, type: int64}, {name: email, type: utf8}]
rules:
  - {id: analytics, op: decide, expr: "ctx.purpose == 'analytics'"}
  - {id: mask, op: transform, column: email, expr: "ctx.tenant == 'demo' ? row.email : redact(row.email)"}
"#;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
    ]))
}

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a@x.io", "b@x.io", "c@x.io"])),
        ],
    )
    .unwrap()
}

fn config() -> InitConfig {
    InitConfig {
        tenant_id: "demo".into(),
        contract_bundle_endpoint: "unix:///run/griot/t04.sock".into(),
        attestation_endpoint: "unix:///run/griot/t05.sock".into(),
        max_result_rows: 1000,
        storaged_socket: "/run/griot/t04.sock".into(),
    }
}

fn bundle_bytes() -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::in_memory(dir.path());
    e.register_contract(USERS, &schema())
        .unwrap()
        .bundle()
        .unwrap()
        .to_json()
        .unwrap()
        .into_bytes()
}

#[tokio::test]
async fn the_tenant_engine_is_governed() {
    let mut k = K04DEngine::new_with_config(config()).unwrap();
    let caller = Caller::new("u", "demo", "analytics");
    // No bundle, no queries.
    assert!(k.query("SELECT 1", &caller).await.is_err());
    // A bundle for another tenant is refused.
    assert!(
        k.inject_contract_bundle(ContractBundleHandle::from_x02_bytes(
            "demo/users",
            "other",
            bundle_bytes()
        ))
        .is_err()
    );
    k.inject_contract_bundle(ContractBundleHandle::from_x02_bytes(
        "demo/users",
        "demo",
        bundle_bytes(),
    ))
    .unwrap();
    // Registered data binds to the contract; raw table names do not exist.
    k.register_memory_table("demo/users", schema(), vec![vec![batch()]])
        .await
        .unwrap();
    assert!(
        k.register_memory_table("raw_users", schema(), vec![vec![batch()]])
            .await
            .is_err()
    );
    let own = k
        .query(r#"SELECT email FROM "demo/users" ORDER BY id"#, &caller)
        .await
        .unwrap();
    assert_eq!(own.envelope.rows, 3);
    let partner = Caller::new("p", "partner", "analytics");
    assert!(
        k.query(r#"SELECT email FROM "demo/users""#, &partner)
            .await
            .is_err(),
        "unpublished"
    );
    k.engine().publish("demo/users", "partner").unwrap();
    let masked = k
        .query(r#"SELECT email FROM "demo/users" ORDER BY id"#, &partner)
        .await
        .unwrap();
    let col = masked.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    assert!(col.iter().all(|v| v == Some("***")));
    // The row limit holds.
    let mut small = config();
    small.max_result_rows = 2;
    let mut k2 = K04DEngine::new_with_config(small).unwrap();
    k2.inject_contract_bundle(ContractBundleHandle::from_x02_bytes(
        "demo/users",
        "demo",
        bundle_bytes(),
    ))
    .unwrap();
    k2.register_memory_table("demo/users", schema(), vec![vec![batch()]])
        .await
        .unwrap();
    assert!(
        k2.query(r#"SELECT id FROM "demo/users""#, &caller)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn the_pool_answers_refuses_and_drains() {
    let mut k = K04DEngine::new_with_config(config()).unwrap();
    k.inject_contract_bundle(ContractBundleHandle::from_x02_bytes(
        "demo/users",
        "demo",
        bundle_bytes(),
    ))
    .unwrap();
    k.register_memory_table("demo/users", schema(), vec![vec![batch()]])
        .await
        .unwrap();
    let pool = LongRunningPoolManager::start(PoolConfig::default(), k.engine().clone(), None).await;
    let ask = |purpose: &str| {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            QueryTask {
                sql: r#"SELECT COUNT(*) FROM "demo/users""#.into(),
                caller: Caller::new("u", "demo", purpose),
                correlation_id: format!("c-{purpose}"),
                reply: tx,
            },
            rx,
        )
    };
    let (task, rx) = ask("analytics");
    pool.submit(task).await.unwrap();
    let ok = rx.await.unwrap().unwrap();
    assert_eq!(ok.correlation_id, "c-analytics");
    assert_eq!(ok.envelope.rows, 1);
    assert!(ok.attestation_jws.is_none());
    let (task, rx) = ask("marketing");
    pool.submit(task).await.unwrap();
    assert!(rx.await.unwrap().is_err());
    pool.shutdown().await;
    let (task, _rx) = ask("analytics");
    assert!(pool.submit(task).await.is_err());
}
