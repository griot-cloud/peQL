//! Contracts bound to Iceberg tables: every write is a snapshot, an overwrite leaves the
//! previous snapshot readable, a read can be pinned to a snapshot, and peQL's facts for each
//! snapshot are kept with it. The catalog is iceberg-rust's in-memory one over a directory.
#![cfg(feature = "iceberg")]

mod support;

use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use iceberg::Catalog;
use peql::iceberg_table::{IcebergTables, SUMMARY_CONTRACT, SUMMARY_CONTRACT_HASH, SUMMARY_WRITE};
use peql::{AsOf, Caller, Engine, PeqlError, WriteMode};
use support::*;

/// The readings contract bound, by its document, to `demo.readings`.
fn readings() -> String {
    let doc = READINGS.replace("parquet: readings/", "iceberg: demo.readings");
    assert_ne!(
        doc, READINGS,
        "the readings contract is bound to `parquet: readings/`"
    );
    doc
}

struct Setup {
    _dir: tempfile::TempDir,
    catalog: Arc<dyn Catalog>,
    engine: Engine,
}

async fn setup() -> Setup {
    let dir = tempfile::tempdir().unwrap();
    let catalog = iceberg_catalog(&dir.path().join("warehouse"), "demo").await;
    let engine =
        Engine::in_memory(dir.path()).with_bindings(Arc::new(IcebergTables::new(catalog.clone())));
    engine.register_contract(&readings(), &schema()).unwrap();
    engine.publish("demo/readings", "partner").unwrap();
    Setup {
        _dir: dir,
        catalog,
        engine,
    }
}

async fn table(catalog: &Arc<dyn Catalog>, name: &str) -> iceberg::table::Table {
    catalog
        .load_table(&table_ident("demo", name))
        .await
        .unwrap()
}

/// The files a snapshot holds, as paths on disk.
async fn snapshot_paths(t: &iceberg::table::Table, snapshot: i64) -> Vec<String> {
    let snap = t.metadata().snapshot_by_id(snapshot).unwrap();
    let list = t.manifest_list_reader(snap).load().await.unwrap();
    let mut out = Vec::new();
    for m in list.entries() {
        for e in t.manifest_reader().read(m).await.unwrap().entries() {
            if e.is_alive() {
                out.push(e.file_path().trim_start_matches("file://").to_owned());
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn a_contract_reads_and_writes_an_iceberg_table() {
    let s = setup().await;
    let engine = &s.engine;

    let report = engine
        .write("demo/readings", vec![batch(1, 40)], WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(report.rows_written, 40);
    assert!(report.verdict.valid);

    let t = table(&s.catalog, "readings").await;
    let snap = t.metadata().current_snapshot().unwrap();
    assert_eq!(report.snapshot.unwrap().snapshot_id, snap.snapshot_id());
    assert_eq!(report.snapshot.unwrap().parent_snapshot_id, None);
    assert_eq!(report.verdict.snapshot_id, Some(snap.snapshot_id()));
    let summary = &snap.summary().additional_properties;
    assert_eq!(summary[SUMMARY_CONTRACT], "demo/readings");
    assert_eq!(
        summary[SUMMARY_CONTRACT_HASH],
        engine
            .get("demo/readings")
            .unwrap()
            .compilation
            .contract
            .contract_hash
    );
    assert_eq!(summary[SUMMARY_WRITE], "append");
    // Partitioned on disk as the Parquet binding lays it out, every column in every file.
    let paths = snapshot_paths(&t, snap.snapshot_id()).await;
    assert!(paths.iter().any(|p| p.contains("/region=EA/")), "{paths:?}");

    let manifest = engine.manifest("demo/readings").unwrap().unwrap();
    assert_eq!(manifest.row_count, 40);
    assert_eq!(manifest.snapshot_id, Some(snap.snapshot_id()));
    assert!(manifest.flags_current(&manifest.contract_hash));
    assert!(manifest.files.iter().all(|f| f.path.contains("region=")));

    let res = engine
        .query(r#"SELECT id, meter FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(40));
    assert!(res.envelope.contracts[0].flags_materialised);
    assert_eq!(
        res.envelope.contracts[0].snapshot_id,
        Some(snap.snapshot_id())
    );

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
    // Live rules give the same rows as stored flags.
    engine.set_use_stored(false);
    let live = engine
        .query(r#"SELECT id FROM "demo/readings""#, &guest())
        .await
        .unwrap();
    assert_eq!(ids(&live.batches), guest_ids(40));
    assert!(!live.envelope.contracts[0].flags_materialised);
    engine.set_use_stored(true);

    // Another engine with only the catalog and the contract reads the facts of the snapshot.
    let other = Engine::in_memory(s._dir.path())
        .with_bindings(Arc::new(IcebergTables::new(s.catalog.clone())));
    other.register_contract(&readings(), &schema()).unwrap();
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
    // Validation is reproducible over the same snapshot.
    let again = engine.validate("demo/readings").await.unwrap();
    assert_eq!(again.data_hash, manifest.data_hash);
    assert_eq!(again.snapshot_id, Some(snap.snapshot_id()));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_append_makes_a_snapshot_whose_parent_is_the_previous_one() {
    let s = setup().await;
    let first = s
        .engine
        .write("demo/readings", vec![batch(1, 20)], WriteMode::Append)
        .await
        .unwrap()
        .snapshot
        .unwrap();
    let second = s
        .engine
        .write("demo/readings", vec![batch(21, 40)], WriteMode::Append)
        .await
        .unwrap();
    let commit = second.snapshot.unwrap();
    assert_eq!(commit.parent_snapshot_id, Some(first.snapshot_id));
    // The report is the append's own rows; the snapshot's facts are the table's.
    assert_eq!(second.verdict.row_count, 20);
    let facts = s
        .engine
        .manifest_as_of("demo/readings", commit.snapshot_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(facts.row_count, 40);

    let t = table(&s.catalog, "readings").await;
    assert_eq!(t.metadata().current_snapshot_id(), Some(commit.snapshot_id));
    let snap = t.metadata().snapshot_by_id(commit.snapshot_id).unwrap();
    assert_eq!(snap.parent_snapshot_id(), Some(first.snapshot_id));
    assert_eq!(t.metadata().snapshots().count(), 2);

    let res = s
        .engine
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(40));
    // The first snapshot still reads its own rows.
    let res = s
        .engine
        .query_as_of(
            r#"SELECT id FROM "demo/readings""#,
            &owner(),
            &AsOf::current().with("demo/readings", first.snapshot_id),
        )
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(20));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_overwrite_leaves_the_previous_snapshot_readable_and_its_files_in_place() {
    let s = setup().await;
    let engine = &s.engine;
    let first = engine
        .write("demo/readings", vec![batch(1, 40)], WriteMode::Append)
        .await
        .unwrap()
        .snapshot
        .unwrap();
    let t = table(&s.catalog, "readings").await;
    let first_files = snapshot_paths(&t, first.snapshot_id).await;
    assert!(!first_files.is_empty());

    let report = engine
        .write("demo/readings", vec![batch(101, 110)], WriteMode::Overwrite)
        .await
        .unwrap();
    let second = report.snapshot.unwrap();
    assert_eq!(second.parent_snapshot_id, Some(first.snapshot_id));
    assert_eq!(report.verdict.row_count, 10);

    // The current table has only the new rows.
    let res = engine
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(
        ids(&res.batches),
        (101..=110).filter(|i| i % 10 != 0).collect::<Vec<_>>()
    );
    let t = table(&s.catalog, "readings").await;
    let snap = t.metadata().current_snapshot().unwrap();
    assert_eq!(snap.snapshot_id(), second.snapshot_id);
    assert_eq!(
        snap.summary().additional_properties[SUMMARY_WRITE],
        "overwrite"
    );
    assert_eq!(snap.summary().additional_properties["total-records"], "10");
    let second_files = snapshot_paths(&t, second.snapshot_id).await;
    assert!(second_files.iter().all(|f| !first_files.contains(f)));

    // The previous snapshot reads its rows through the engine, under the contract.
    let as_of = AsOf::current().with("demo/readings", first.snapshot_id);
    let res = engine
        .query_as_of(r#"SELECT id, meter FROM "demo/readings""#, &guest(), &as_of)
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), guest_ids(40));
    assert!(
        strings(&res.batches, "meter")
            .iter()
            .all(|m| m.as_deref() == Some("***"))
    );
    assert_eq!(
        res.envelope.contracts[0].snapshot_id,
        Some(first.snapshot_id)
    );
    // A refusal is the same refusal as of any snapshot.
    let refused = engine
        .query_as_of(
            r#"SELECT id FROM "demo/readings""#,
            &Caller::new("m", "partner", "marketing"),
            &as_of,
        )
        .await;
    assert!(
        matches!(refused, Err(PeqlError::Denied { ref rule, .. }) if rule == "analytics"),
        "{:?}",
        refused.err()
    );
    // And its files are where they were.
    for f in &first_files {
        assert!(std::path::Path::new(f).exists(), "{f} is gone");
    }

    // The planned view of the old snapshot, for an executor of the host's.
    let planned = engine
        .view_as_of("demo/readings", &owner(), first.snapshot_id, Some(1))
        .await
        .unwrap();
    let batches = datafusion::physical_plan::collect(planned.plan.clone(), planned.ctx.task_ctx())
        .await
        .unwrap();
    assert_eq!(ids(&batches), owner_ids(40));
    assert_eq!(planned.contracts[0].snapshot_id, Some(first.snapshot_id));

    // A snapshot the table does not have is refused.
    let missing = engine
        .query_as_of(
            r#"SELECT id FROM "demo/readings""#,
            &owner(),
            &AsOf::current().with("demo/readings", 42),
        )
        .await;
    assert!(missing.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn two_overwrites_from_one_snapshot_cannot_both_commit() {
    let s = setup().await;
    let engine = &s.engine;
    let base = engine
        .write("demo/readings", vec![batch(1, 20)], WriteMode::Append)
        .await
        .unwrap()
        .snapshot
        .unwrap();

    let a = engine
        .begin_write("demo/readings", WriteMode::Overwrite)
        .await
        .unwrap();
    let b = engine
        .begin_write("demo/readings", WriteMode::Overwrite)
        .await
        .unwrap();
    engine.write_part(&a, vec![batch(201, 210)]).await.unwrap();
    engine.write_part(&b, vec![batch(301, 310)]).await.unwrap();
    let won = engine.finish_write(a).await.unwrap().snapshot.unwrap();
    assert_eq!(won.parent_snapshot_id, Some(base.snapshot_id));
    let lost = engine.finish_write(b).await;
    assert!(
        matches!(lost, Err(PeqlError::Conflict(_))),
        "{:?}",
        lost.map(|r| r.snapshot)
    );

    let t = table(&s.catalog, "readings").await;
    assert_eq!(t.metadata().current_snapshot_id(), Some(won.snapshot_id));
    assert_eq!(t.metadata().snapshots().count(), 2);
    let res = engine
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(
        ids(&res.batches),
        (201..=210).filter(|i| i % 10 != 0).collect::<Vec<_>>()
    );

    // An append started from the base still commits: appends do not replace.
    let c = engine
        .begin_write("demo/readings", WriteMode::Append)
        .await
        .unwrap();
    engine.write_part(&c, vec![batch(401, 405)]).await.unwrap();
    let appended = engine.finish_write(c).await.unwrap().snapshot.unwrap();
    assert_eq!(appended.parent_snapshot_id, Some(won.snapshot_id));
}

#[tokio::test(flavor = "multi_thread")]
async fn each_snapshot_keeps_its_own_facts() {
    let s = setup().await;
    let engine = &s.engine;
    let first = engine
        .write("demo/readings", vec![batch(1, 40)], WriteMode::Append)
        .await
        .unwrap();
    let second = engine
        .write("demo/readings", vec![batch(41, 45)], WriteMode::Overwrite)
        .await
        .unwrap();
    let (s1, s2) = (
        first.snapshot.unwrap().snapshot_id,
        second.snapshot.unwrap().snapshot_id,
    );

    let m1 = engine
        .manifest_as_of("demo/readings", s1)
        .await
        .unwrap()
        .unwrap();
    let m2 = engine
        .manifest_as_of("demo/readings", s2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((m1.snapshot_id, m1.row_count), (Some(s1), 40));
    assert_eq!((m2.snapshot_id, m2.row_count), (Some(s2), 5));
    assert_eq!(m1.data_hash, first.verdict.data_hash);
    assert_eq!(m2.data_hash, second.verdict.data_hash);
    assert_ne!(m1.data_hash, m2.data_hash);
    assert_eq!(m1.files, first.files);
    // The current facts are the current snapshot's.
    assert_eq!(engine.manifest("demo/readings").unwrap().unwrap(), m2);

    // Recorded beside the table's metadata, keyed by the snapshot.
    let t = table(&s.catalog, "readings").await;
    let facts = format!(
        "{}/metadata/peql/{s1}/demo__readings.json",
        t.metadata()
            .location()
            .trim_start_matches("file://")
            .trim_end_matches('/')
    );
    assert!(std::path::Path::new(&facts).exists(), "{facts}");

    // A verifier reproduces the first snapshot's verdict over the first snapshot's data.
    let plan = engine
        .get("demo/readings")
        .unwrap()
        .compilation
        .validation
        .plan
        .clone();
    let v = engine
        .validate_with_as_of("demo/readings", plan, s1)
        .await
        .unwrap();
    assert_eq!(
        (v.data_hash.as_str(), v.row_count, v.snapshot_id),
        (m1.data_hash.as_str(), 40, Some(s1))
    );

    // A read as of the first snapshot is answered from the first snapshot's facts.
    let res = engine
        .query_as_of(
            r#"SELECT COUNT(*) AS n FROM "demo/readings""#,
            &owner(),
            &AsOf::current().with("demo/readings", s1),
        )
        .await
        .unwrap();
    assert_eq!(res.envelope.contracts[0].snapshot_id, Some(s1));

    // A contract bound to the same table records its own facts for each snapshot it reads.
    engine
        .register_contract(
            &readings().replace("contract: demo/readings", "contract: demo/readings-copy"),
            &schema(),
        )
        .unwrap();
    assert!(
        engine
            .manifest_as_of("demo/readings-copy", s1)
            .await
            .unwrap()
            .is_none()
    );
    engine
        .query_as_of(
            r#"SELECT COUNT(*) FROM "demo/readings-copy""#,
            &owner(),
            &AsOf::current().with("demo/readings-copy", s1),
        )
        .await
        .unwrap();
    let copy = engine
        .manifest_as_of("demo/readings-copy", s1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((copy.row_count, &copy.data_hash), (40, &m1.data_hash));
}

const PLAIN_V1: &str = r#"
contract: demo/plain
version: 1
binding:
  iceberg: demo.plain
expose:
  - {name: id, type: int64}
  - {name: a, type: utf8}
  - {name: b, type: utf8}
rules:
  - {id: everyone, op: decide, expr: "true"}
"#;

const PLAIN_V2: &str = r#"
contract: demo/plain
version: 2
binding:
  iceberg: demo.plain
expose:
  - {name: id, type: int64}
  - {name: a, type: utf8}
  - {name: c, type: int64}
rules:
  - {id: everyone, op: decide, expr: "true"}
"#;

fn plain(columns: &[(&str, DataType)], ids: &[i64]) -> RecordBatch {
    let schema: SchemaRef = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(n, t)| Field::new(*n, t.clone(), *n != "id"))
            .collect::<Vec<_>>(),
    ));
    let arrays = columns
        .iter()
        .map(|(n, t)| -> datafusion::arrow::array::ArrayRef {
            match (n, t) {
                (&"id", _) => Arc::new(Int64Array::from(ids.to_vec())),
                (_, DataType::Utf8) => Arc::new(StringArray::from(
                    ids.iter().map(|i| format!("{n}{i}")).collect::<Vec<_>>(),
                )),
                _ => Arc::new(Int64Array::from(
                    ids.iter().map(|i| i * 100).collect::<Vec<_>>(),
                )),
            }
        })
        .collect();
    RecordBatch::try_new(schema, arrays).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_pinned_before_a_schema_change_has_that_snapshots_columns() {
    let s = setup().await;
    let engine = &s.engine;
    let v1_cols = [
        ("id", DataType::Int64),
        ("a", DataType::Utf8),
        ("b", DataType::Utf8),
    ];
    let v2_cols = [
        ("id", DataType::Int64),
        ("a", DataType::Utf8),
        ("c", DataType::Int64),
    ];
    let v1 = plain(&v1_cols, &[]).schema();
    engine.register_contract(PLAIN_V1, &v1).unwrap();
    let s1 = engine
        .write(
            "demo/plain",
            vec![plain(&v1_cols, &[1, 2])],
            WriteMode::Append,
        )
        .await
        .unwrap()
        .snapshot
        .unwrap()
        .snapshot_id;

    // The contract changes shape: `b` goes, `c` comes. The table's schema follows the write.
    engine
        .register_contract(PLAIN_V2, &plain(&v2_cols, &[]).schema())
        .unwrap();
    let s2 = engine
        .write("demo/plain", vec![plain(&v2_cols, &[3])], WriteMode::Append)
        .await
        .unwrap()
        .snapshot
        .unwrap()
        .snapshot_id;
    let t = table(&s.catalog, "plain").await;
    let current: Vec<String> = t
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect();
    assert_eq!(current, ["id", "a", "c"]);

    let everyone = Caller::new("x", "demo", "any");
    let res = engine
        .query(r#"SELECT * FROM "demo/plain" ORDER BY id"#, &everyone)
        .await
        .unwrap();
    assert_eq!(
        res.schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "a", "c"]
    );
    assert_eq!(ids(&res.batches), [1, 2, 3]);
    assert_eq!(res.envelope.contracts[0].snapshot_id, Some(s2));

    // The first version, frozen at the first snapshot, reads the first snapshot's columns.
    engine
        .register_contract(
            &PLAIN_V1.replace("contract: demo/plain", "contract: demo/plain-v1"),
            &v1,
        )
        .unwrap();
    let res = engine
        .query_as_of(
            r#"SELECT * FROM "demo/plain-v1" ORDER BY id"#,
            &everyone,
            &AsOf::current().with("demo/plain-v1", s1),
        )
        .await
        .unwrap();
    assert_eq!(
        res.schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "a", "b"]
    );
    assert_eq!(ids(&res.batches), [1, 2]);
    assert_eq!(
        strings(&res.batches, "b"),
        [Some("b1".into()), Some("b2".into())]
    );

    // The current contract cannot read the first snapshot: it has no `c`.
    let stale = engine
        .query_as_of(
            r#"SELECT * FROM "demo/plain""#,
            &everyone,
            &AsOf::current().with("demo/plain", s1),
        )
        .await;
    assert!(stale.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unwritten_iceberg_contract_is_reported_and_snapshots_need_a_table() {
    let s = setup().await;
    let q = s
        .engine
        .query(r#"SELECT COUNT(*) FROM "demo/readings""#, &owner())
        .await;
    assert!(
        matches!(q, Err(PeqlError::NotWritten { .. })),
        "{:?}",
        q.err()
    );

    // A Parquet binding has no snapshots to read as of.
    let dir = tempfile::tempdir().unwrap();
    let parquet = Engine::in_memory(dir.path());
    parquet.register_contract(READINGS, &schema()).unwrap();
    parquet
        .write("demo/readings", vec![batch(1, 4)], WriteMode::Append)
        .await
        .unwrap();
    let r = parquet
        .query_as_of(
            r#"SELECT id FROM "demo/readings""#,
            &owner(),
            &AsOf::current().with("demo/readings", 1),
        )
        .await;
    assert!(matches!(r, Err(PeqlError::Invalid(_))), "{:?}", r.err());
    // Nor may a read name a snapshot of a contract it does not read.
    let r = s
        .engine
        .query_as_of(
            "SELECT 1",
            &owner(),
            &AsOf::current().with("demo/readings", 1),
        )
        .await;
    assert!(r.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_binding_with_nested_namespaces_names_the_table_in_the_innermost() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = iceberg_catalog(&dir.path().join("warehouse"), "lake").await;
    let nested = iceberg::NamespaceIdent::from_strs(["lake", "demo"]).unwrap();
    catalog
        .create_namespace(&nested, std::collections::HashMap::new())
        .await
        .unwrap();
    let engine =
        Engine::in_memory(dir.path()).with_bindings(Arc::new(IcebergTables::new(catalog.clone())));
    engine
        .register_contract(
            &READINGS.replace("parquet: readings/", "iceberg: lake.demo.readings"),
            &schema(),
        )
        .unwrap();
    engine
        .write("demo/readings", vec![batch(1, 10)], WriteMode::Append)
        .await
        .unwrap();

    let t = catalog
        .load_table(&iceberg::TableIdent::new(nested, "readings".into()))
        .await
        .unwrap();
    assert!(t.metadata().current_snapshot().is_some());
    assert!(
        !catalog
            .table_exists(&table_ident("lake", "readings"))
            .await
            .unwrap()
    );
    let res = engine
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(10));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_parquet_binding_is_not_read_through_a_catalog() {
    let s = setup().await;
    s.engine
        .register_contract(
            &READINGS.replace("contract: demo/readings", "contract: demo/files"),
            &schema(),
        )
        .unwrap();
    let w = s
        .engine
        .write("demo/files", vec![batch(1, 4)], WriteMode::Append)
        .await;
    match w {
        Err(PeqlError::Invalid(m)) => assert_eq!(
            m,
            "`demo/files` is bound to `binding: {parquet: readings/}`, which this engine's \
             bindings do not reach"
        ),
        other => panic!("{:?}", other.map(|r| r.rows_written)),
    }
}

/// What another writer does just before a commit: see [`Interleaved`].
type Hook = Box<dyn FnOnce() -> futures::future::BoxFuture<'static, ()> + Send>;

/// A catalog that lets another writer act just before the commits it is handed: each queued
/// hook runs, in order, before one commit, and reaches the catalog behind this one. The races
/// a write's requirements exist for, made to happen.
struct Interleaved {
    inner: Arc<dyn Catalog>,
    hooks: std::sync::Mutex<std::collections::VecDeque<Hook>>,
}

impl std::fmt::Debug for Interleaved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Interleaved").finish_non_exhaustive()
    }
}

impl Interleaved {
    fn new(inner: Arc<dyn Catalog>) -> Arc<Interleaved> {
        Arc::new(Interleaved {
            inner,
            hooks: Default::default(),
        })
    }

    /// Run `hook` just before the next commit that has none yet.
    fn before_commit<F>(&self, hook: impl FnOnce() -> F + Send + 'static)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.hooks
            .lock()
            .unwrap()
            .push_back(Box::new(move || Box::pin(hook())));
    }
}

#[async_trait::async_trait]
impl Catalog for Interleaved {
    async fn list_namespaces(
        &self,
        parent: Option<&iceberg::NamespaceIdent>,
    ) -> iceberg::Result<Vec<iceberg::NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }
    async fn create_namespace(
        &self,
        namespace: &iceberg::NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }
    async fn get_namespace(
        &self,
        namespace: &iceberg::NamespaceIdent,
    ) -> iceberg::Result<iceberg::Namespace> {
        self.inner.get_namespace(namespace).await
    }
    async fn namespace_exists(&self, namespace: &iceberg::NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }
    async fn update_namespace(
        &self,
        namespace: &iceberg::NamespaceIdent,
        properties: std::collections::HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }
    async fn drop_namespace(&self, namespace: &iceberg::NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }
    async fn list_tables(
        &self,
        namespace: &iceberg::NamespaceIdent,
    ) -> iceberg::Result<Vec<iceberg::TableIdent>> {
        self.inner.list_tables(namespace).await
    }
    async fn create_table(
        &self,
        namespace: &iceberg::NamespaceIdent,
        creation: iceberg::TableCreation,
    ) -> iceberg::Result<iceberg::table::Table> {
        self.inner.create_table(namespace, creation).await
    }
    async fn load_table(
        &self,
        table: &iceberg::TableIdent,
    ) -> iceberg::Result<iceberg::table::Table> {
        self.inner.load_table(table).await
    }
    async fn drop_table(&self, table: &iceberg::TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }
    async fn purge_table(&self, table: &iceberg::TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(table).await
    }
    async fn table_exists(&self, table: &iceberg::TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }
    async fn rename_table(
        &self,
        src: &iceberg::TableIdent,
        dest: &iceberg::TableIdent,
    ) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }
    async fn register_table(
        &self,
        table: &iceberg::TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<iceberg::table::Table> {
        self.inner.register_table(table, metadata_location).await
    }
    async fn update_table(
        &self,
        commit: iceberg::TableCommit,
    ) -> iceberg::Result<iceberg::table::Table> {
        let hook = self.hooks.lock().unwrap().pop_front();
        if let Some(hook) = hook {
            hook().await;
        }
        self.inner.update_table(commit).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_catalog_refuses_an_overwrite_once_the_table_has_moved() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Interleaved::new(iceberg_catalog(&dir.path().join("warehouse"), "demo").await);
    let engine =
        Engine::in_memory(dir.path()).with_bindings(Arc::new(IcebergTables::new(catalog.clone())));
    engine.register_contract(&readings(), &schema()).unwrap();
    let base = engine
        .write("demo/readings", vec![batch(1, 20)], WriteMode::Append)
        .await
        .unwrap()
        .snapshot
        .unwrap();

    // Another writer commits between the overwrite's last look at the table and its commit.
    let inner = catalog.inner.clone();
    catalog.before_commit(move || async move {
        use iceberg::transaction::{ApplyTransactionAction, Transaction};
        let t = inner
            .load_table(&table_ident("demo", "readings"))
            .await
            .unwrap();
        let tx = Transaction::new(&t);
        tx.fast_append()
            .set_snapshot_properties(std::collections::HashMap::from([(
                "writer".to_owned(),
                "another".to_owned(),
            )]))
            .apply(tx)
            .unwrap()
            .commit(inner.as_ref())
            .await
            .unwrap();
    });
    let lost = engine
        .write("demo/readings", vec![batch(201, 210)], WriteMode::Overwrite)
        .await;
    assert!(
        matches!(lost, Err(PeqlError::Conflict(_))),
        "{:?}",
        lost.map(|r| r.snapshot)
    );
    let t = catalog
        .load_table(&table_ident("demo", "readings"))
        .await
        .unwrap();
    let current = t.metadata().current_snapshot().unwrap();
    assert_eq!(current.parent_snapshot_id(), Some(base.snapshot_id));
    assert_eq!(current.summary().additional_properties["writer"], "another");
    // The table still holds the base's rows: the overwrite did not land.
    let res = engine
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(20));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_engine_frozen_at_a_snapshot_reads_it_in_every_read_and_writes_nothing() {
    let s = setup().await;
    let v1_cols = [
        ("id", DataType::Int64),
        ("a", DataType::Utf8),
        ("b", DataType::Utf8),
    ];
    let v2_cols = [
        ("id", DataType::Int64),
        ("a", DataType::Utf8),
        ("c", DataType::Int64),
    ];
    let v1 = plain(&v1_cols, &[]).schema();
    s.engine.register_contract(PLAIN_V1, &v1).unwrap();
    let s1 = s
        .engine
        .write(
            "demo/plain",
            vec![plain(&v1_cols, &[1, 2])],
            WriteMode::Append,
        )
        .await
        .unwrap()
        .snapshot
        .unwrap()
        .snapshot_id;
    // The second version drops `b` and rewrites the table.
    s.engine
        .register_contract(PLAIN_V2, &plain(&v2_cols, &[]).schema())
        .unwrap();
    s.engine
        .write(
            "demo/plain",
            vec![plain(&v2_cols, &[3, 4, 5])],
            WriteMode::Overwrite,
        )
        .await
        .unwrap();

    // A host's view of the first version, frozen at the first snapshot: a plain query reads it.
    let frozen = Engine::in_memory(s._dir.path())
        .with_bindings(Arc::new(IcebergTables::new(s.catalog.clone())))
        .with_as_of(AsOf::current().with("demo/plain", s1));
    frozen.register_contract(PLAIN_V1, &v1).unwrap();
    assert_eq!(frozen.as_of().get("demo/plain"), Some(s1));
    let everyone = Caller::new("x", "demo", "any");
    let res = frozen
        .query(r#"SELECT * FROM "demo/plain" ORDER BY id"#, &everyone)
        .await
        .unwrap();
    assert_eq!(
        res.schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["id", "a", "b"]
    );
    assert_eq!(ids(&res.batches), [1, 2]);
    assert_eq!(res.envelope.contracts[0].snapshot_id, Some(s1));
    let planned = frozen.view("demo/plain", &everyone).await.unwrap();
    assert_eq!(planned.contracts[0].snapshot_id, Some(s1));

    // Nothing is written under it.
    let refused = frozen
        .write("demo/plain", vec![plain(&v1_cols, &[9])], WriteMode::Append)
        .await;
    assert!(
        matches!(&refused, Err(PeqlError::Invalid(m)) if m.contains("read as of snapshot")),
        "{refused:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_replaces_small_files_with_one_snapshot_and_keeps_every_row() {
    let s = setup().await;
    let cols = [
        ("id", DataType::Int64),
        ("a", DataType::Utf8),
        ("b", DataType::Utf8),
    ];
    s.engine
        .register_contract(PLAIN_V1, &plain(&cols, &[]).schema())
        .unwrap();
    let mut last = None;
    for ids in [[1, 2], [3, 4], [5, 6]] {
        last = s
            .engine
            .write("demo/plain", vec![plain(&cols, &ids)], WriteMode::Append)
            .await
            .unwrap()
            .snapshot;
    }
    let before = last.unwrap().snapshot_id;
    let t = table(&s.catalog, "plain").await;
    assert_eq!(snapshot_paths(&t, before).await.len(), 3);

    // Nothing is small enough to be worth it: nothing happens.
    assert!(
        s.engine
            .compact("demo/plain", 1, 2)
            .await
            .unwrap()
            .is_none()
    );

    let report = s
        .engine
        .compact("demo/plain", u64::MAX, 2)
        .await
        .unwrap()
        .expect("three small files are compacted");
    let commit = report.snapshot.unwrap();
    assert_eq!(commit.parent_snapshot_id, Some(before));
    assert_eq!(report.rows_written, 6);
    assert!(report.verdict.valid);
    let t = table(&s.catalog, "plain").await;
    assert_eq!(t.metadata().current_snapshot_id(), Some(commit.snapshot_id));
    assert_eq!(snapshot_paths(&t, commit.snapshot_id).await.len(), 1);
    assert_eq!(
        t.metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get(SUMMARY_WRITE)
            .map(String::as_str),
        Some("rewrite")
    );

    let everyone = Caller::new("x", "demo", "any");
    let now = s
        .engine
        .query(r#"SELECT * FROM "demo/plain" ORDER BY id"#, &everyone)
        .await
        .unwrap();
    assert_eq!(ids(&now.batches), [1, 2, 3, 4, 5, 6]);
    // The snapshot before is still readable, from its own files.
    let then = s
        .engine
        .query_as_of(
            r#"SELECT * FROM "demo/plain" ORDER BY id"#,
            &everyone,
            &AsOf::current().with("demo/plain", before),
        )
        .await
        .unwrap();
    assert_eq!(ids(&then.batches), [1, 2, 3, 4, 5, 6]);
    for path in snapshot_paths(&t, before).await {
        assert!(std::path::Path::new(&path).exists(), "{path}");
    }
    // One file left: nothing more to compact.
    assert!(
        s.engine
            .compact("demo/plain", u64::MAX, 2)
            .await
            .unwrap()
            .is_none()
    );
}

/// A table whose warehouse is in another store: the write's files land under the engine's
/// staging directory, go to the table through its `FileIO` at the commit, and the staging
/// directory is gone after it. Appends, an overwrite and a read as of the first snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn a_table_in_another_store_is_written_through_its_file_io() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = iceberg_catalog_in_memory("demo").await;
    let engine = Engine::open(dir.path())
        .unwrap()
        .with_bindings(Arc::new(IcebergTables::new(catalog.clone())));
    engine.register_contract(&readings(), &schema()).unwrap();

    let first = engine
        .write("demo/readings", vec![batch(1, 40)], WriteMode::Append)
        .await
        .unwrap();
    let first = first.snapshot.unwrap().snapshot_id;
    let t = table(&catalog, "readings").await;
    assert!(
        t.metadata().location().starts_with("memory://"),
        "{}",
        t.metadata().location()
    );
    let paths = snapshot_paths(&t, first).await;
    assert!(!paths.is_empty());
    for p in &paths {
        assert!(p.starts_with("memory://warehouse/"), "{p}");
        assert!(
            t.file_io().exists(p).await.unwrap(),
            "{p} is in the table's store"
        );
    }
    let staging = dir.path().join("_peql/staging");
    assert!(
        !staging.exists() || std::fs::read_dir(&staging).unwrap().next().is_none(),
        "nothing is left staged"
    );

    engine
        .write("demo/readings", vec![batch(41, 50)], WriteMode::Append)
        .await
        .unwrap();
    let res = engine
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(50));

    engine
        .write("demo/readings", vec![batch(1, 5)], WriteMode::Overwrite)
        .await
        .unwrap();
    let res = engine
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(5));
    let res = engine
        .query_as_of(
            r#"SELECT id FROM "demo/readings""#,
            &owner(),
            &AsOf::current().with("demo/readings", first),
        )
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(40));
    assert!(
        !staging.exists() || std::fs::read_dir(&staging).unwrap().next().is_none(),
        "nothing is left staged"
    );
}

/// An engine over `catalog` with `contract` registered: another writer to the same tables.
fn other_writer(
    dir: &std::path::Path,
    catalog: Arc<dyn Catalog>,
    contract: &str,
    schema: SchemaRef,
) -> Arc<Engine> {
    let engine = Engine::in_memory(dir).with_bindings(Arc::new(IcebergTables::new(catalog)));
    engine.register_contract(contract, &schema).unwrap();
    Arc::new(engine)
}

const PLAIN_COLUMNS: [(&str, DataType); 3] = [
    ("id", DataType::Int64),
    ("a", DataType::Utf8),
    ("b", DataType::Utf8),
];

/// `demo/plain` written three times, two rows each, through a catalog that lets another
/// writer act before a commit; the other writer, and the snapshot the three writes made.
async fn three_small_files(dir: &std::path::Path) -> (Arc<Interleaved>, Engine, Arc<Engine>, i64) {
    let catalog = Interleaved::new(iceberg_catalog(&dir.join("warehouse"), "demo").await);
    let engine = Engine::in_memory(dir.join("one"))
        .with_bindings(Arc::new(IcebergTables::new(catalog.clone())));
    let schema = plain(&PLAIN_COLUMNS, &[]).schema();
    engine.register_contract(PLAIN_V1, &schema).unwrap();
    let other = other_writer(&dir.join("two"), catalog.inner.clone(), PLAIN_V1, schema);
    let mut last = None;
    for ids in [[1, 2], [3, 4], [5, 6]] {
        last = engine
            .write(
                "demo/plain",
                vec![plain(&PLAIN_COLUMNS, &ids)],
                WriteMode::Append,
            )
            .await
            .unwrap()
            .snapshot;
    }
    (catalog, engine, other, last.unwrap().snapshot_id)
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_commits_over_appends_that_land_while_it_runs_and_keeps_their_rows() {
    let dir = tempfile::tempdir().unwrap();
    let (catalog, engine, other, read) = three_small_files(dir.path()).await;

    // Another writer appends just before the compaction's commit, and again just before the
    // commit it makes once it has carried the first append forward.
    let appended = Arc::new(std::sync::Mutex::new(Vec::new()));
    for ids in [[7, 8], [9, 10]] {
        let (other, appended) = (other.clone(), appended.clone());
        catalog.before_commit(move || async move {
            let r = other
                .write(
                    "demo/plain",
                    vec![plain(&PLAIN_COLUMNS, &ids)],
                    WriteMode::Append,
                )
                .await
                .unwrap();
            appended
                .lock()
                .unwrap()
                .push(r.snapshot.unwrap().snapshot_id);
        });
    }
    let report = engine
        .compact("demo/plain", u64::MAX, 2)
        .await
        .unwrap()
        .expect("three small files are compacted");
    let appended = appended.lock().unwrap().clone();
    assert_eq!(
        appended.len(),
        2,
        "both appends landed during the compaction"
    );
    let commit = report.snapshot.unwrap();
    assert_eq!(commit.parent_snapshot_id, Some(appended[1]));
    assert_eq!(report.rows_written, 6, "the rows of the snapshot read");
    assert!(report.verdict.valid);
    assert_eq!(report.verdict.verdict.row_count, 6, "the rows rewritten");
    let facts = engine
        .manifest_as_of("demo/plain", report.snapshot.unwrap().snapshot_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        facts.row_count, 10,
        "the table's rows, appended ones among them"
    );
    assert_eq!(facts.files.len(), 3);

    let t = table(&catalog.inner, "plain").await;
    assert_eq!(t.metadata().current_snapshot_id(), Some(commit.snapshot_id));
    let snap = t.metadata().current_snapshot().unwrap();
    assert_eq!(
        snap.summary()
            .additional_properties
            .get(SUMMARY_WRITE)
            .map(String::as_str),
        Some("rewrite")
    );
    assert_eq!(
        snap.summary()
            .additional_properties
            .get("total-records")
            .map(String::as_str),
        Some("10")
    );
    assert_eq!(
        snap.summary()
            .additional_properties
            .get("total-data-files")
            .map(String::as_str),
        Some("3")
    );
    // One file for the six rows read, and each append's own file, carried forward.
    let now = snapshot_paths(&t, commit.snapshot_id).await;
    assert_eq!(now.len(), 3, "{now:?}");
    let last_append = snapshot_paths(&t, appended[1]).await;
    for read_file in snapshot_paths(&t, read).await {
        assert!(!now.contains(&read_file), "{read_file} was rewritten");
    }
    for appended_file in last_append.iter().filter(|p| !now.contains(p)) {
        assert!(
            snapshot_paths(&t, read).await.contains(appended_file),
            "{appended_file} was appended and is gone"
        );
    }

    let everyone = Caller::new("x", "demo", "any");
    let rows = engine
        .query(r#"SELECT * FROM "demo/plain" ORDER BY id"#, &everyone)
        .await
        .unwrap();
    assert_eq!(ids(&rows.batches), (1..=10).collect::<Vec<_>>());
    assert_eq!(
        strings(&rows.batches, "b"),
        (1..=10).map(|i| Some(format!("b{i}"))).collect::<Vec<_>>()
    );
    // The snapshot read is still readable, from its own files.
    let then = engine
        .query_as_of(
            r#"SELECT id FROM "demo/plain" ORDER BY id"#,
            &everyone,
            &AsOf::current().with("demo/plain", read),
        )
        .await
        .unwrap();
    assert_eq!(ids(&then.batches), [1, 2, 3, 4, 5, 6]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_compaction_the_table_moves_under_at_every_commit_gives_up_and_keeps_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let (catalog, engine, other, _) = three_small_files(dir.path()).await;
    const MOVES: i64 = 40;
    for i in 0..MOVES {
        let other = other.clone();
        catalog.before_commit(move || async move {
            other
                .write(
                    "demo/plain",
                    vec![plain(&PLAIN_COLUMNS, &[100 + i])],
                    WriteMode::Append,
                )
                .await
                .unwrap();
        });
    }
    let refused = engine.compact("demo/plain", u64::MAX, 2).await;
    match refused {
        Err(PeqlError::Conflict(m)) => assert!(m.contains("attempts"), "{m}"),
        other => panic!("{:?}", other.map(|r| r.map(|r| r.snapshot))),
    }
    let left = catalog.hooks.lock().unwrap().len() as i64;
    assert!(left > 0, "the compaction stopped trying");
    let everyone = Caller::new("x", "demo", "any");
    let rows = engine
        .query(r#"SELECT id FROM "demo/plain" ORDER BY id"#, &everyone)
        .await
        .unwrap();
    let mut want: Vec<i64> = (1..=6).collect();
    want.extend(100..100 + MOVES - left);
    assert_eq!(ids(&rows.batches), want);
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_is_refused_once_an_overwrite_replaced_the_files_it_read() {
    let dir = tempfile::tempdir().unwrap();
    let (catalog, engine, other, read) = three_small_files(dir.path()).await;

    let overwrote = Arc::new(std::sync::Mutex::new(None));
    {
        let (other, overwrote) = (other.clone(), overwrote.clone());
        catalog.before_commit(move || async move {
            let r = other
                .write(
                    "demo/plain",
                    vec![plain(&PLAIN_COLUMNS, &[100, 101])],
                    WriteMode::Overwrite,
                )
                .await
                .unwrap();
            *overwrote.lock().unwrap() = Some(r.snapshot.unwrap().snapshot_id);
        });
    }
    let refused = engine.compact("demo/plain", u64::MAX, 2).await;
    let overwrote = overwrote.lock().unwrap().expect("the overwrite landed");
    match refused {
        Err(PeqlError::Conflict(m)) => {
            assert!(m.contains("replaced or removed"), "{m}");
            assert!(m.contains(&format!("since snapshot {read}")), "{m}");
            assert!(m.contains(&format!("{overwrote} (overwrite)")), "{m}");
        }
        other => panic!("{:?}", other.map(|r| r.map(|r| r.snapshot))),
    }
    let t = table(&catalog.inner, "plain").await;
    assert_eq!(t.metadata().current_snapshot_id(), Some(overwrote));
    let everyone = Caller::new("x", "demo", "any");
    let rows = engine
        .query(r#"SELECT id FROM "demo/plain" ORDER BY id"#, &everyone)
        .await
        .unwrap();
    assert_eq!(ids(&rows.batches), [100, 101]);
}

/// `demo/plain` with a column `c` of `ty` beside the first version's.
fn plain_with_c(ty: &str) -> String {
    let doc = PLAIN_V1.replace("version: 1", "version: 2").replace(
        "  - {name: b, type: utf8}\n",
        &format!("  - {{name: b, type: utf8}}\n  - {{name: c, type: {ty}}}\n"),
    );
    assert!(doc.contains("name: c"), "{doc}");
    doc
}

/// `demo/plain` written once at its first version, through a catalog that lets another writer
/// act before a commit; both writers then hold `demo/plain` with a column `c`, of the types
/// given, as one engine and the other.
async fn two_writers_adding_c(
    dir: &std::path::Path,
    ours: (&str, DataType),
    theirs: (&str, DataType),
) -> (Arc<Interleaved>, Engine, Arc<Engine>) {
    let catalog = Interleaved::new(iceberg_catalog(&dir.join("warehouse"), "demo").await);
    let engine = Engine::in_memory(dir.join("one"))
        .with_bindings(Arc::new(IcebergTables::new(catalog.clone())));
    engine
        .register_contract(PLAIN_V1, &plain(&PLAIN_COLUMNS, &[]).schema())
        .unwrap();
    engine
        .write(
            "demo/plain",
            vec![plain(&PLAIN_COLUMNS, &[1, 2])],
            WriteMode::Append,
        )
        .await
        .unwrap();
    let with = |ty: DataType| {
        let mut cols = PLAIN_COLUMNS.to_vec();
        cols.push(("c", ty));
        plain(&cols, &[]).schema()
    };
    engine
        .register_contract(&plain_with_c(ours.0), &with(ours.1))
        .unwrap();
    let other = other_writer(
        &dir.join("two"),
        catalog.inner.clone(),
        &plain_with_c(theirs.0),
        with(theirs.1),
    );
    (catalog, engine, other)
}

#[tokio::test(flavor = "multi_thread")]
async fn two_writers_adding_one_column_at_once_both_write_and_the_schema_changes_once() {
    let dir = tempfile::tempdir().unwrap();
    let (catalog, engine, other) = two_writers_adding_c(
        dir.path(),
        ("int64", DataType::Int64),
        ("int64", DataType::Int64),
    )
    .await;
    let cols = [
        ("id", DataType::Int64),
        ("a", DataType::Utf8),
        ("b", DataType::Utf8),
        ("c", DataType::Int64),
    ];
    // The other writer adds `c` and writes, just before this writer's schema change commits.
    {
        let (other, cols) = (other.clone(), cols.clone());
        catalog.before_commit(move || async move {
            other
                .write("demo/plain", vec![plain(&cols, &[3])], WriteMode::Append)
                .await
                .unwrap();
        });
    }
    engine
        .write("demo/plain", vec![plain(&cols, &[4])], WriteMode::Append)
        .await
        .unwrap();

    let t = table(&catalog.inner, "plain").await;
    assert_eq!(
        t.metadata().schemas_iter().count(),
        2,
        "the first schema and the one change"
    );
    let current: Vec<String> = t
        .metadata()
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect();
    assert_eq!(current, ["id", "a", "b", "c"]);
    let everyone = Caller::new("x", "demo", "any");
    let rows = engine
        .query(r#"SELECT id, c FROM "demo/plain" ORDER BY id"#, &everyone)
        .await
        .unwrap();
    assert_eq!(ids(&rows.batches), [1, 2, 3, 4]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_column_another_writer_added_with_another_type_is_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let (catalog, engine, other) = two_writers_adding_c(
        dir.path(),
        ("int64", DataType::Int64),
        ("utf8", DataType::Utf8),
    )
    .await;
    {
        let other = other.clone();
        catalog.before_commit(move || async move {
            let cols = [
                ("id", DataType::Int64),
                ("a", DataType::Utf8),
                ("b", DataType::Utf8),
                ("c", DataType::Utf8),
            ];
            other
                .write("demo/plain", vec![plain(&cols, &[3])], WriteMode::Append)
                .await
                .unwrap();
        });
    }
    let cols = [
        ("id", DataType::Int64),
        ("a", DataType::Utf8),
        ("b", DataType::Utf8),
        ("c", DataType::Int64),
    ];
    let refused = engine
        .write("demo/plain", vec![plain(&cols, &[4])], WriteMode::Append)
        .await;
    match refused {
        Err(PeqlError::Invalid(m)) => {
            assert!(m.contains("column `c`"), "{m}");
            assert!(m.contains("is Utf8; the contract writes Int64"), "{m}");
        }
        other => panic!("{:?}", other.map(|r| r.snapshot)),
    }
    let t = table(&catalog.inner, "plain").await;
    assert_eq!(t.metadata().schemas_iter().count(), 2);
}

/// An Iceberg storage over the local filesystem that records every path whose bytes are read.
mod counting {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use iceberg::io::{
        FileMetadata, FileRead, FileWrite, InputFile, LocalFsStorage, OutputFile, Storage,
        StorageConfig, StorageFactory,
    };
    use serde::{Deserialize, Serialize};

    /// Every path read, in order, by any table on a [`CountingStorageFactory`].
    pub static READS: Mutex<Vec<String>> = Mutex::new(Vec::new());

    fn read(path: &str) {
        READS.lock().unwrap().push(path.to_owned());
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    pub struct CountingStorage {
        inner: LocalFsStorage,
    }

    #[async_trait]
    #[typetag::serde(name = "peql_test_counting_storage")]
    impl Storage for CountingStorage {
        async fn exists(&self, path: &str) -> iceberg::Result<bool> {
            self.inner.exists(path).await
        }
        async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
            self.inner.metadata(path).await
        }
        async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
            read(path);
            self.inner.read(path).await
        }
        async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
            read(path);
            self.inner.reader(path).await
        }
        async fn write(&self, path: &str, bs: Bytes) -> iceberg::Result<()> {
            self.inner.write(path, bs).await
        }
        async fn writer(&self, path: &str) -> iceberg::Result<Box<dyn FileWrite>> {
            self.inner.writer(path).await
        }
        async fn delete(&self, path: &str) -> iceberg::Result<()> {
            self.inner.delete(path).await
        }
        async fn delete_prefix(&self, path: &str) -> iceberg::Result<()> {
            self.inner.delete_prefix(path).await
        }
        async fn delete_stream(&self, paths: BoxStream<'static, String>) -> iceberg::Result<()> {
            self.inner.delete_stream(paths).await
        }
        fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
            Ok(InputFile::new(Arc::new(self.clone()), path.to_owned()))
        }
        fn new_output(&self, path: &str) -> iceberg::Result<OutputFile> {
            Ok(OutputFile::new(Arc::new(self.clone()), path.to_owned()))
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct CountingStorageFactory;

    #[typetag::serde(name = "peql_test_counting_storage_factory")]
    impl StorageFactory for CountingStorageFactory {
        fn build(&self, _config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
            Ok(Arc::new(CountingStorage::default()))
        }
    }
}

/// The paths of `warehouse` read while `f` runs.
async fn reads_while<T>(
    warehouse: &str,
    f: impl std::future::Future<Output = T>,
) -> (T, Vec<String>) {
    counting::READS.lock().unwrap().clear();
    let out = f.await;
    let reads = counting::READS
        .lock()
        .unwrap()
        .drain(..)
        .filter(|p| p.contains(warehouse))
        .collect();
    (out, reads)
}

/// An append to a table of many snapshots reads none of its data files, footers or
/// manifests, and validates none of its rows: what it reads (the table's metadata, the
/// parent's manifest list and the parent's facts) is the same whatever the table holds.
#[tokio::test(flavor = "multi_thread")]
async fn an_append_reads_nothing_of_the_table_it_did_not_write() {
    use iceberg::CatalogBuilder;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    let dir = tempfile::tempdir().unwrap();
    let warehouse = dir.path().join("warehouse");
    std::fs::create_dir_all(&warehouse).unwrap();
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(counting::CountingStorageFactory))
        .load(
            "peql-counting",
            std::collections::HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                warehouse.display().to_string(),
            )]),
        )
        .await
        .unwrap();
    catalog
        .create_namespace(
            &iceberg::NamespaceIdent::new("demo".into()),
            std::collections::HashMap::new(),
        )
        .await
        .unwrap();
    let catalog: Arc<dyn Catalog> = Arc::new(catalog);
    let engine =
        Engine::in_memory(dir.path()).with_bindings(Arc::new(IcebergTables::new(catalog.clone())));
    engine.register_contract(&readings(), &schema()).unwrap();
    let warehouse = warehouse.display().to_string();

    let append = |from: i64| {
        let engine = &engine;
        async move {
            engine
                .write(
                    "demo/readings",
                    vec![batch(from, from + 9)],
                    WriteMode::Append,
                )
                .await
                .unwrap()
        }
    };
    for i in 0..3 {
        append(i * 10 + 1).await;
    }
    let (small, small_reads) = reads_while(&warehouse, append(31)).await;
    for i in 4..16 {
        append(i * 10 + 1).await;
    }
    let t = table(&catalog, "readings").await;
    let current = t.metadata().current_snapshot_id().unwrap();
    assert!(snapshot_paths(&t, current).await.len() >= 2 * 16);
    let (big, big_reads) = reads_while(&warehouse, append(161)).await;

    for p in &big_reads {
        let name = p.rsplit('/').next().unwrap();
        assert!(!p.ends_with(".parquet"), "read a data file: {p}");
        assert!(
            !name.ends_with(".avro") || name.starts_with("snap-"),
            "read a manifest: {p}"
        );
    }
    // The parent's facts are read; no other snapshot's.
    let parent = big.snapshot.unwrap().parent_snapshot_id.unwrap();
    for p in big_reads.iter().filter(|p| p.contains("/metadata/peql/")) {
        assert!(
            p.contains(&format!("/metadata/peql/{parent}/"))
                || p.contains("/metadata/peql/writes/"),
            "read the facts of another snapshot: {p}"
        );
    }
    assert_eq!(
        small_reads.len(),
        big_reads.len(),
        "an append to 4 snapshots read {small_reads:#?}; to 16, {big_reads:#?}"
    );

    // The report is the append's; the facts are the table's.
    assert_eq!((small.rows_written, big.rows_written), (10, 10));
    assert_eq!(big.verdict.row_count, 10);
    assert_eq!(big.verdict.failures["non_negative"], 1);
    assert_eq!(big.verdict.data_hash, peql::manifest::data_hash(&big.files));
    let facts = engine.manifest("demo/readings").unwrap().unwrap();
    assert_eq!(facts.row_count, 170);
    assert_eq!(facts.failures["non_negative"], 17);
    // And they are what a validation of the whole table finds.
    let whole = engine.validate("demo/readings").await.unwrap();
    assert_eq!(
        (whole.row_count, &whole.failures, &whole.data_hash),
        (facts.row_count, &facts.failures, &facts.data_hash)
    );
}

/// A snapshot whose parent's facts are not recorded (its writer has not recorded them yet,
/// or stopped before it did) takes them from the facts each write since recorded before it
/// committed, back to the nearest snapshot with facts, and records them on the way.
#[tokio::test(flavor = "multi_thread")]
async fn missing_facts_are_rebuilt_from_the_writes_since_the_last_recorded() {
    let s = setup().await;
    let engine = &s.engine;
    let mut snapshots = Vec::new();
    for from in [1, 11, 21] {
        let r = engine
            .write(
                "demo/readings",
                vec![batch(from, from + 9)],
                WriteMode::Append,
            )
            .await
            .unwrap();
        snapshots.push(r.snapshot.unwrap().snapshot_id);
    }
    let t = table(&s.catalog, "readings").await;
    let root = t
        .metadata()
        .location()
        .trim_start_matches("file://")
        .trim_end_matches('/')
        .to_owned();
    for s in &snapshots[1..] {
        std::fs::remove_dir_all(format!("{root}/metadata/peql/{s}")).unwrap();
    }

    let r = engine
        .write("demo/readings", vec![batch(31, 40)], WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(r.verdict.row_count, 10);
    let last = r.snapshot.unwrap().snapshot_id;
    for s in &snapshots[1..] {
        assert!(
            std::path::Path::new(&format!("{root}/metadata/peql/{s}/demo__readings.json")).exists(),
            "the facts of {s} are recorded again"
        );
    }
    let facts = engine
        .manifest_as_of("demo/readings", last)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(facts.row_count, 40);
    let whole = engine.validate("demo/readings").await.unwrap();
    assert_eq!(
        (whole.row_count, &whole.failures, &whole.data_hash),
        (40, &facts.failures, &facts.data_hash)
    );
}

const UNIQUE: &str = r#"
contract: demo/unique
version: 1
binding:
  iceberg: demo.unique
expose:
  - {name: id, type: int64}
  - {name: kwh, type: int64}
rules:
  - {id: everyone, op: decide, expr: "true"}
  - {id: ids_unique, op: guarantee, expr: "dataset.id.distinct_count == dataset.row_count", on_fail: deny}
  - {id: kwh_bounded, op: guarantee, expr: "dataset.kwh.max <= 1000", on_fail: deny}
"#;

/// Rules over the whole table are decided only by validating it: no write decides one, and
/// no read is refused because one is undecided. The facts keep each one's last decision, the
/// snapshot it was made on and its outcome, carried forward unchanged by later appends; a
/// deny-level one that failed when last decided makes the table not servable until a
/// validation finds it holding. Statistics that combine are kept up to date by every write.
#[tokio::test(flavor = "multi_thread")]
async fn rules_over_the_whole_table_are_decided_only_by_validate() {
    let s = setup().await;
    let engine = &s.engine;
    let cols = [("id", DataType::Int64), ("kwh", DataType::Int64)];
    engine
        .register_contract(UNIQUE, &plain(&cols, &[]).schema())
        .unwrap();
    let everyone = Caller::new("x", "demo", "any");
    let read = || engine.query(r#"SELECT id FROM "demo/unique""#, &everyone);
    let append = |ids: &'static [i64]| {
        engine.write("demo/unique", vec![plain(&cols, ids)], WriteMode::Append)
    };
    let facts = || engine.manifest("demo/unique").unwrap().unwrap();

    // Writes decide nothing about the table: its rules are undecided, and it is served.
    let r = append(&[1, 2]).await.unwrap();
    assert!(r.verdict.guarantees.is_empty());
    append(&[2]).await.unwrap();
    assert!(facts().guarantees.is_empty(), "{:?}", facts().guarantees);
    assert!(facts().valid);
    assert_eq!(ids(&read().await.unwrap().batches), [1, 2, 2]);

    // A validation decides them, of the snapshot it validates.
    let whole = engine.validate("demo/unique").await.unwrap();
    let decided_on = whole.snapshot_id.unwrap();
    assert_eq!(whole.breached, ["ids_unique"]);
    let checked = facts().guarantees;
    assert_eq!(
        (
            checked["ids_unique"].holds,
            checked["ids_unique"].snapshot_id
        ),
        (false, Some(decided_on))
    );
    assert_eq!(
        (
            checked["kwh_bounded"].holds,
            checked["kwh_bounded"].snapshot_id
        ),
        (true, Some(decided_on))
    );
    assert!(matches!(read().await, Err(PeqlError::NotServable { .. })));

    // An append carries the decisions forward unchanged, even past a bound it breaks; the
    // maximum it changes is kept up to date, and judged by nothing.
    let r = append(&[20]).await.unwrap();
    assert!(r.verdict.valid);
    let after = facts();
    assert_eq!(after.guarantees, checked);
    assert_eq!(after.stats["kwh__max"], serde_json::json!(2000));
    assert!(!after.stats.contains_key("id__distinct_count"));
    assert_eq!(after.breached, ["ids_unique"]);

    // The next validation decides again, of the snapshot it validates.
    let whole = engine.validate("demo/unique").await.unwrap();
    assert_eq!(whole.breached, ["ids_unique", "kwh_bounded"]);
    let now = facts().guarantees;
    assert_eq!(now["kwh_bounded"].snapshot_id, whole.snapshot_id);
    assert!(!now["kwh_bounded"].holds);
}

/// A compaction followed by a validation in the same engine, as a host's table maintenance
/// runs them: the validation decides the compacted snapshot, records each decision in that
/// snapshot's facts, and appends after carry them forward unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn a_validation_after_a_compaction_decides_the_compacted_snapshot() {
    let s = setup().await;
    let engine = &s.engine;
    let cols = [("id", DataType::Int64), ("kwh", DataType::Int64)];
    engine
        .register_contract(UNIQUE, &plain(&cols, &[]).schema())
        .unwrap();
    for ids in [[1, 2], [3, 4], [5, 6]] {
        engine
            .write("demo/unique", vec![plain(&cols, &ids)], WriteMode::Append)
            .await
            .unwrap();
    }
    let compacted = engine
        .compact("demo/unique", u64::MAX, 2)
        .await
        .unwrap()
        .expect("three small files are compacted")
        .snapshot
        .unwrap()
        .snapshot_id;
    // The compaction decides nothing either.
    let facts = engine
        .manifest_as_of("demo/unique", compacted)
        .await
        .unwrap()
        .unwrap();
    assert!(facts.guarantees.is_empty());
    assert_eq!(facts.row_count, 6);

    let whole = engine.validate("demo/unique").await.unwrap();
    assert_eq!(whole.snapshot_id, Some(compacted));
    assert!(whole.valid, "{:?}", whole.breached);
    let facts = engine
        .manifest_as_of("demo/unique", compacted)
        .await
        .unwrap()
        .unwrap();
    for id in ["ids_unique", "kwh_bounded"] {
        let k = &facts.guarantees[id];
        assert_eq!((k.holds, k.snapshot_id), (true, Some(compacted)), "{id}");
    }
    let r = engine
        .write("demo/unique", vec![plain(&cols, &[7])], WriteMode::Append)
        .await
        .unwrap();
    let after = engine
        .manifest_as_of("demo/unique", r.snapshot.unwrap().snapshot_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.guarantees, facts.guarantees);
    assert_eq!(after.row_count, 7);
}

/// Engines writing under different compilations of one contract name share the table: a
/// reader under one compilation does not combine the facts another compilation's writes
/// recorded; it validates the snapshot whole under its own and reads every row.
#[tokio::test(flavor = "multi_thread")]
async fn a_reader_under_another_compilation_validates_rather_than_combining() {
    let s = setup().await;
    s.engine
        .write("demo/readings", vec![batch(1, 20)], WriteMode::Append)
        .await
        .unwrap();
    // Another engine writes under a second form of the same contract.
    let second = readings()
        .replace("version: 1", "version: 2")
        .replace(
            "  - {id: non_negative,",
            "  - {id: kwh_known, op: assert, expr: \"has(row.kwh)\", on_fail: report}\n  - {id: non_negative,",
        );
    let other = Engine::in_memory(s._dir.path().join("two"))
        .with_bindings(Arc::new(IcebergTables::new(s.catalog.clone())));
    other.register_contract(&second, &schema()).unwrap();
    assert_ne!(
        other
            .get("demo/readings")
            .unwrap()
            .compilation
            .contract
            .compilation_hash,
        s.engine
            .get("demo/readings")
            .unwrap()
            .compilation
            .contract
            .compilation_hash
    );
    for from in [21, 31] {
        other
            .write(
                "demo/readings",
                vec![batch(from, from + 9)],
                WriteMode::Append,
            )
            .await
            .unwrap();
    }
    // A fresh engine under the first form.
    let reader = Engine::in_memory(s._dir.path().join("three"))
        .with_bindings(Arc::new(IcebergTables::new(s.catalog.clone())));
    reader.register_contract(&readings(), &schema()).unwrap();
    let res = reader
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(40));
    let facts = reader.manifest("demo/readings").unwrap().unwrap();
    assert_eq!(facts.row_count, 40);
    assert_eq!(
        facts.compilation_hash,
        reader
            .get("demo/readings")
            .unwrap()
            .compilation
            .contract
            .compilation_hash
    );
}

/// The facts an earlier peQL recorded for a snapshot (the bytes a b9002f7 write recorded, in
/// its format) are not recorded facts here. A write over them commits and records its own
/// facts, and the table's facts stay unrecorded rather than rebuilt by the write; a read is
/// answered, validating the snapshot once and recording its facts; a validation rebuilds them
/// as a validation of the whole computes them. Facts in this format that do not decode are
/// refused, naming the file.
#[tokio::test(flavor = "multi_thread")]
async fn facts_of_an_earlier_peql_are_rebuilt_and_corrupt_facts_refused() {
    let s = setup().await;
    let first = s
        .engine
        .write("demo/readings", vec![batch(1, 40)], WriteMode::Append)
        .await
        .unwrap()
        .snapshot
        .unwrap()
        .snapshot_id;
    let t = table(&s.catalog, "readings").await;
    let root = t
        .metadata()
        .location()
        .trim_start_matches("file://")
        .trim_end_matches('/')
        .to_owned();
    // As an earlier peQL left the table: its snapshot's facts in that format, no write facts.
    std::fs::write(
        format!("{root}/metadata/peql/{first}/demo__readings.json"),
        include_bytes!("fixtures/facts-b9002f7/iceberg-snapshot-facts.json"),
    )
    .unwrap();
    std::fs::remove_dir_all(format!("{root}/metadata/peql/writes")).unwrap();
    let fresh = |name: &str| {
        let e = Engine::in_memory(s._dir.path().join(name))
            .with_bindings(Arc::new(IcebergTables::new(s.catalog.clone())));
        e.register_contract(&readings(), &schema()).unwrap();
        e
    };
    let engine = fresh("upgraded");
    assert!(
        engine
            .manifest_as_of("demo/readings", first)
            .await
            .unwrap()
            .is_none()
    );

    let r = engine
        .write("demo/readings", vec![batch(41, 50)], WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(r.verdict.row_count, 10);
    assert_eq!(r.table, None, "the table's facts are not recorded");
    let second = r.snapshot.unwrap().snapshot_id;
    let t = table(&s.catalog, "readings").await;
    let write_id = &t
        .metadata()
        .snapshot_by_id(second)
        .unwrap()
        .summary()
        .additional_properties[peql::iceberg_table::SUMMARY_WRITE_ID];
    assert!(
        std::path::Path::new(&format!(
            "{root}/metadata/peql/writes/{write_id}/demo__readings.json"
        ))
        .exists()
    );
    let facts_of_second = format!("{root}/metadata/peql/{second}/demo__readings.json");
    assert!(!std::path::Path::new(&facts_of_second).exists());
    assert!(
        engine
            .manifest_as_of("demo/readings", second)
            .await
            .unwrap()
            .is_none()
    );

    let res = engine
        .query(r#"SELECT id FROM "demo/readings""#, &owner())
        .await
        .unwrap();
    assert_eq!(ids(&res.batches), owner_ids(50));
    assert!(std::path::Path::new(&facts_of_second).exists());

    let rebuilt = engine.validate("demo/readings").await.unwrap();
    let plan = engine
        .get("demo/readings")
        .unwrap()
        .compilation
        .validation
        .plan
        .clone();
    let whole = engine.validate_with("demo/readings", plan).await.unwrap();
    let facts = engine.manifest("demo/readings").unwrap().unwrap();
    assert_eq!(
        (facts.row_count, &facts.failures, &facts.data_hash),
        (whole.row_count, &whole.failures, &whole.data_hash)
    );
    assert_eq!(rebuilt.data_hash, whole.data_hash);
    assert_eq!(facts.row_count, 50);

    // Corrupt facts are refused, by name.
    let this_format = format!(
        "{{\"format\": {}, \"contract\": 7}}",
        peql::manifest::FACTS_FORMAT
    );
    for corrupt in [this_format.as_bytes(), &b"{ not json"[..]] {
        std::fs::write(&facts_of_second, corrupt).unwrap();
        match fresh("reader")
            .query(r#"SELECT id FROM "demo/readings""#, &owner())
            .await
        {
            Err(PeqlError::Invalid(m)) => assert!(m.contains(&facts_of_second), "{m}"),
            other => panic!("{:?}", other.map(|r| r.envelope.rows)),
        }
    }
}

/// `a` with a null every third id, `b` with none.
fn plain_with_nulls(ids: &[i64]) -> RecordBatch {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|i| (i % 3 != 0).then(|| format!("a{i}")))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter().map(|i| format!("b{i}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// A write reports the table's row count and per-column null counts after it, from the
/// facts it left: after two appends, what a validation of the whole table finds.
#[tokio::test(flavor = "multi_thread")]
async fn a_write_reports_the_tables_rows_and_nulls_from_its_facts() {
    let s = setup().await;
    let engine = &s.engine;
    engine
        .register_contract(PLAIN_V1, &plain_with_nulls(&[]).schema())
        .unwrap();
    let first = engine
        .write(
            "demo/plain",
            vec![plain_with_nulls(&[1, 2, 3, 4])],
            WriteMode::Append,
        )
        .await
        .unwrap();
    let t = first.table.unwrap();
    assert_eq!((t.row_count, t.null_counts["a"]), (4, 1));
    let second = engine
        .write(
            "demo/plain",
            vec![plain_with_nulls(&[5, 6, 7, 8, 9])],
            WriteMode::Append,
        )
        .await
        .unwrap();
    let reported = second.table.unwrap();
    assert_eq!(reported.row_count, 9);
    assert_eq!(
        reported.null_counts,
        [("a", 3), ("b", 0), ("id", 0)]
            .map(|(c, n)| (c.to_owned(), n))
            .into()
    );
    engine.validate("demo/plain").await.unwrap();
    let whole = engine
        .manifest("demo/plain")
        .unwrap()
        .unwrap()
        .table_stats(["id", "a", "b"]);
    assert_eq!(reported, whole);
    let counted = engine
        .query(
            r#"SELECT COUNT(*) AS n, COUNT(*) - COUNT(a) AS a_nulls FROM "demo/plain""#,
            &Caller::new("x", "demo", "any"),
        )
        .await
        .unwrap();
    let col = |name: &str| {
        counted.batches[0]
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };
    assert_eq!((col("n"), col("a_nulls")), (9, 3));
}
