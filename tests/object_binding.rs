//! Bindings in an object store: written, listed, streamed and re-read through `object_store`.
//!
//! The in-memory store runs everywhere and takes the same code path as S3. With the `s3`
//! feature and `PEQL_TEST_S3_ENDPOINT` set (e.g. a local MinIO at `http://127.0.0.1:9000`,
//! with `PEQL_TEST_S3_BUCKET`, `PEQL_TEST_S3_ACCESS_KEY`, `PEQL_TEST_S3_SECRET_KEY`), the same
//! scenario runs against that endpoint.

mod support;

use std::sync::Arc;

use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use peql::{Engine, ObjectStoreParquet, WriteMode};
use support::*;

async fn keys(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<String> {
    let mut k: Vec<String> = store
        .list(Some(&ObjectPath::from(prefix)))
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await
        .unwrap();
    k.sort();
    k
}

/// Write, read as owner and guest, re-open from the store alone, overwrite.
async fn scenario(base: &str, store: Arc<dyn ObjectStore>, prefix: &str) {
    let disk = tempfile::tempdir().unwrap();
    let engine = Engine::open(disk.path()).unwrap().with_bindings(Arc::new(
        ObjectStoreParquet::new(base, store.clone()).unwrap(),
    ));
    engine.register_contract(READINGS, &schema()).unwrap();
    engine.publish("demo/readings", "partner").unwrap();

    let report = engine
        .write("demo/readings", vec![batch(1, 40)], WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(report.rows_written, 40);
    assert!(report.verdict.valid);

    // Hive partitions, Parquet only, and the manifest beside the data; nothing on the disk.
    let written = keys(&store, &format!("{prefix}readings")).await;
    assert!(
        written
            .iter()
            .any(|k| k.contains("readings/region=EA/") && k.ends_with(".parquet")),
        "{written:?}"
    );
    assert!(
        written
            .iter()
            .any(|k| k.ends_with("readings/_peql/manifests/demo__readings.json")),
        "{written:?}"
    );
    assert!(!disk.path().join("readings").exists());
    let manifest = engine.manifest("demo/readings").unwrap().unwrap();
    assert_eq!(manifest.row_count, 40);
    assert!(manifest.files.iter().all(|f| f.path.starts_with("region=")));
    assert!(manifest.flags_current(&manifest.contract_hash));

    let res = engine
        .query(r#"SELECT id, meter FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(40));
    assert!(res.envelope.scan.bytes_read > 0);
    assert!(res.envelope.contracts[0].flags_materialised);

    let res = engine
        .query(r#"SELECT id, meter FROM "demo/readings""#, &guest())
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), guest_ids(40));
    assert!(
        strings(&res.batches, "meter")
            .iter()
            .all(|m| m.as_deref() == Some("***"))
    );

    // Another engine, with only the store and the contract, reads the manifest from the store.
    let other = Engine::in_memory(disk.path()).with_bindings(Arc::new(
        ObjectStoreParquet::new(base, store.clone()).unwrap(),
    ));
    other.register_contract(READINGS, &schema()).unwrap();
    let res = other
        .query(
            r#"SELECT COUNT(*) AS n FROM "demo/readings" WHERE region = 'WA'"#,
            &owner(),
        )
        .await
        .unwrap();
    assert_eq!(res.envelope.rows, 1);
    assert_eq!(
        other.manifest("demo/readings").unwrap().unwrap().data_hash,
        manifest.data_hash
    );

    // Replace the data: the old files go, the manifest follows.
    engine
        .write("demo/readings", vec![batch(1, 10)], WriteMode::Overwrite)
        .await
        .unwrap();
    let res = engine
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(10));
    assert_eq!(
        engine.manifest("demo/readings").unwrap().unwrap().row_count,
        10
    );
}

#[tokio::test]
async fn an_s3_binding_through_the_object_store_path() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    scenario("s3://lake/tenant-a/", store, "tenant-a/").await;
}

#[tokio::test]
async fn bindings_resolve_inside_the_store_or_not_at_all() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let disk = tempfile::tempdir().unwrap();
    let engine = Engine::in_memory(disk.path()).with_bindings(Arc::new(
        ObjectStoreParquet::new("s3://lake/tenant-a/", store).unwrap(),
    ));
    for (version, (binding, ok)) in [
        ("s3://lake/tenant-a/readings/", true),
        ("s3://other/readings/", false),
        ("../escape/", false),
    ]
    .into_iter()
    .enumerate()
    {
        let doc = READINGS
            .replace("parquet: readings/", &format!("parquet: {binding}"))
            .replace("version: 1", &format!("version: {}", version + 1));
        engine.register_contract(&doc, &schema()).unwrap();
        let res = engine
            .write("demo/readings", vec![batch(1, 4)], WriteMode::Overwrite)
            .await;
        assert_eq!(res.is_ok(), ok, "{binding}: {:?}", res.err());
    }
    assert!(ObjectStoreParquet::new("lake/tenant-a", Arc::new(InMemory::new())).is_err());
}

#[cfg(feature = "s3")]
#[tokio::test]
async fn an_s3_binding_against_a_live_endpoint() {
    let Ok(endpoint) = std::env::var("PEQL_TEST_S3_ENDPOINT") else {
        eprintln!("PEQL_TEST_S3_ENDPOINT is not set: the live S3 test does not run");
        return;
    };
    let var = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_owned());
    let bucket = var("PEQL_TEST_S3_BUCKET", "peql");
    let store = object_store::aws::AmazonS3Builder::new()
        .with_endpoint(&endpoint)
        .with_allow_http(true)
        .with_region(var("PEQL_TEST_S3_REGION", "us-east-1"))
        .with_bucket_name(&bucket)
        .with_access_key_id(var("PEQL_TEST_S3_ACCESS_KEY", "minioadmin"))
        .with_secret_access_key(var("PEQL_TEST_S3_SECRET_KEY", "minioadmin"))
        .build()
        .unwrap();
    let prefix = format!("peql-test-{}/", uuid::Uuid::new_v4());
    scenario(&format!("s3://{bucket}/{prefix}"), Arc::new(store), &prefix).await;
}
