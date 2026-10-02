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

struct Setup {
    _dir: tempfile::TempDir,
    catalog: Arc<dyn Catalog>,
    tables: Arc<IcebergTables>,
    engine: Engine,
}

async fn setup() -> Setup {
    let dir = tempfile::tempdir().unwrap();
    let catalog = iceberg_catalog(&dir.path().join("warehouse"), "demo").await;
    let tables = Arc::new(
        IcebergTables::new(catalog.clone())
            .with_table("demo/readings", table_ident("demo", "readings")),
    );
    let engine = Engine::in_memory(dir.path()).with_bindings(tables.clone());
    engine.register_contract(READINGS, &schema()).unwrap();
    engine.publish("demo/readings", "partner").unwrap();
    Setup {
        _dir: dir,
        catalog,
        tables,
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
    let other = Engine::in_memory(s._dir.path()).with_bindings(Arc::new(
        IcebergTables::new(s.catalog.clone())
            .with_table("demo/readings", table_ident("demo", "readings")),
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
    assert_eq!(second.verdict.row_count, 40);

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
    assert_eq!(m1.files.len(), first.files);
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
    s.tables
        .bind("demo/readings-copy", table_ident("demo", "readings"));
    engine
        .register_contract(
            &READINGS.replace("contract: demo/readings", "contract: demo/readings-copy"),
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
  parquet: unused/
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
  parquet: unused/
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
    s.tables.bind("demo/plain", table_ident("demo", "plain"));
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
    s.tables.bind("demo/plain-v1", table_ident("demo", "plain"));
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

/// A catalog that, once armed, lets another writer commit just before the next commit it is
/// handed: the race an overwrite's requirements exist for.
#[derive(Debug)]
struct Interleaved {
    inner: Arc<dyn Catalog>,
    armed: std::sync::atomic::AtomicBool,
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
        if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            use iceberg::transaction::{ApplyTransactionAction, Transaction};
            let t = self.inner.load_table(commit.identifier()).await?;
            let tx = Transaction::new(&t);
            tx.fast_append()
                .set_snapshot_properties(std::collections::HashMap::from([(
                    "writer".to_owned(),
                    "another".to_owned(),
                )]))
                .apply(tx)?
                .commit(self.inner.as_ref())
                .await?;
        }
        self.inner.update_table(commit).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_catalog_refuses_an_overwrite_once_the_table_has_moved() {
    let dir = tempfile::tempdir().unwrap();
    let catalog = Arc::new(Interleaved {
        inner: iceberg_catalog(&dir.path().join("warehouse"), "demo").await,
        armed: std::sync::atomic::AtomicBool::new(false),
    });
    let engine = Engine::in_memory(dir.path()).with_bindings(Arc::new(
        IcebergTables::new(catalog.clone())
            .with_table("demo/readings", table_ident("demo", "readings")),
    ));
    engine.register_contract(READINGS, &schema()).unwrap();
    let base = engine
        .write("demo/readings", vec![batch(1, 20)], WriteMode::Append)
        .await
        .unwrap()
        .snapshot
        .unwrap();

    // Another writer commits between the overwrite's last look at the table and its commit.
    catalog
        .armed
        .store(true, std::sync::atomic::Ordering::SeqCst);
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
