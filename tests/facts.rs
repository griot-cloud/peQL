//! Manifests an earlier peQL recorded beside Parquet files (the bytes a b9002f7 write
//! recorded, in its format) are not recorded manifests: a write over them is published
//! without rebuilding them, a read validates the data once and records its manifest, and
//! a manifest in this format that does not decode is refused.

mod support;

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStoreExt, PutPayload};
use peql::{Engine, ObjectStoreParquet, PeqlError, WriteMode};
use support::*;

const EARLIER: &[u8] = include_bytes!("fixtures/facts-b9002f7/parquet-manifest.json");

#[tokio::test]
async fn a_manifest_of_an_earlier_peql_beside_local_files_is_rebuilt_by_a_read() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::in_memory(dir.path());
    engine.register_contract(READINGS, &schema()).unwrap();
    engine
        .write("demo/readings", vec![batch(1, 40)], WriteMode::Append)
        .await
        .unwrap();
    let path = dir
        .path()
        .join("readings/_peql/manifests/demo__readings.json");
    std::fs::write(&path, EARLIER).unwrap();

    let engine = Engine::in_memory(dir.path());
    engine.register_contract(READINGS, &schema()).unwrap();
    assert!(engine.manifest("demo/readings").unwrap().is_none());
    let r = engine
        .write("demo/readings", vec![batch(41, 50)], WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(r.verdict.row_count, 10);
    assert!(engine.manifest("demo/readings").unwrap().is_none());

    let res = engine
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(50));
    let read = engine.manifest("demo/readings").unwrap().unwrap();
    assert_eq!(read.row_count, 50);
    let whole = engine.validate("demo/readings").await.unwrap();
    assert_eq!((whole.row_count, &whole.data_hash), (50, &read.data_hash));

    std::fs::write(&path, b"{\"format\": 2}").unwrap();
    match engine
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
    {
        Err(PeqlError::Io(e)) => {
            assert!(e.to_string().contains(&path.display().to_string()), "{e}")
        }
        other => panic!("{:?}", other.map(|r| r.envelope.rows)),
    }
}

#[tokio::test]
async fn a_manifest_of_an_earlier_peql_in_an_object_store_is_rebuilt_by_a_read() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let disk = tempfile::tempdir().unwrap();
    let open = || {
        let e = Engine::open(disk.path()).unwrap().with_bindings(Arc::new(
            ObjectStoreParquet::new("s3://lake/tenant-a/", store.clone()).unwrap(),
        ));
        e.register_contract(READINGS, &schema()).unwrap();
        e
    };
    open()
        .write("demo/readings", vec![batch(1, 40)], WriteMode::Append)
        .await
        .unwrap();
    let path = ObjectPath::from("tenant-a/readings/_peql/manifests/demo__readings.json");
    store
        .put(&path, PutPayload::from_static(EARLIER))
        .await
        .unwrap();

    let engine = open();
    engine
        .write("demo/readings", vec![batch(41, 50)], WriteMode::Append)
        .await
        .unwrap();
    assert!(engine.manifest("demo/readings").unwrap().is_none());
    let res = engine
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(50));
    assert_eq!(
        engine.manifest("demo/readings").unwrap().unwrap().row_count,
        50
    );

    store
        .put(&path, PutPayload::from_static(b"[1, 2]"))
        .await
        .unwrap();
    match open()
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
    {
        Err(PeqlError::Invalid(m)) => assert!(m.contains("demo__readings.json"), "{m}"),
        other => panic!("{:?}", other.map(|r| r.envelope.rows)),
    }
}
