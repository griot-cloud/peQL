//! Contracts bound to Iceberg tables (feature `iceberg`).
//!
//! [`IcebergTables`] resolves a contract to a table in an Iceberg catalog the caller supplies;
//! peQL never builds a catalog of its own and does not know what is behind the one it is given.
//! A read is a view over the table as of one snapshot: the current one, or one the caller
//! names ([`crate::Engine::query_as_of`]). A write lands its data files as the Parquet binding
//! lands them, then commits one snapshot through the catalog: an append is the catalog crate's
//! own; an overwrite replaces the table's files in a snapshot that may only commit over the
//! snapshot the write began from, so two overwrites begun from one snapshot cannot both commit.
//! No write deletes a file, and every earlier snapshot stays readable until it is expired.
//!
//! What peQL knows about a snapshot's data ([`Manifest`]: the verdict, statistics, data hash
//! and each file's contract hash and flags) is kept with that snapshot: the writing contract's
//! name and hashes in the snapshot's summary, and the rest, per contract, in one file keyed by
//! the snapshot id beside the table's metadata (`metadata/peql/<snapshot>/<contract>.json`).
//!
//! ```no_run
//! # async fn f(catalog: std::sync::Arc<dyn iceberg::Catalog>) -> peql::Result<()> {
//! use std::sync::Arc;
//! use iceberg::{NamespaceIdent, TableIdent};
//! use peql::iceberg_table::IcebergTables;
//!
//! let tables = IcebergTables::new(catalog).with_table(
//!     "demo/readings",
//!     TableIdent::new(NamespaceIdent::new("demo".into()), "readings".into()),
//! );
//! let engine = peql::Engine::open("/var/lib/peql")?.with_bindings(Arc::new(tables));
//! # Ok(()) }
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::catalog::view::ViewTable;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, cast};
use datafusion::parquet::file::FOOTER_SIZE;
use datafusion::parquet::file::metadata::{FooterTail, ParquetMetaData, ParquetMetaDataReader};
use datafusion_iceberg::IcebergStaticTableProvider;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestListWriter,
    TableMetadata,
};
use iceberg::table::Table;
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use parcel_core::CompiledContract;
use parcel_runtime::plan::col_ref;
use uuid::Uuid;

use crate::binding::{self, BindingResolver, DataHash, Location};
use crate::error::{PeqlError, Result};
use crate::manifest::{FileEntry, Manifest};

/// Snapshot summary keys peQL writes: the contract the snapshot's files were written under.
pub const SUMMARY_CONTRACT: &str = "peql.contract";
pub const SUMMARY_CONTRACT_HASH: &str = "peql.contract-hash";
pub const SUMMARY_COMPILATION_HASH: &str = "peql.compilation-hash";
/// `append` or `overwrite`: what the write did, whatever operation the snapshot records.
pub const SUMMARY_WRITE: &str = "peql.write";

/// Where a contract's facts for one snapshot live, under the table's location.
const FACTS_DIR: &str = "metadata/peql";

/// Contracts bound to tables in one Iceberg catalog. Each contract is bound to a table by
/// [`IcebergTables::with_table`]; a contract with no table has no binding here.
#[derive(Debug)]
pub struct IcebergTables {
    catalog: Arc<dyn Catalog>,
    tables: RwLock<HashMap<String, TableIdent>>,
}

impl IcebergTables {
    /// Tables in `catalog`, which the caller built and owns.
    pub fn new(catalog: Arc<dyn Catalog>) -> IcebergTables {
        IcebergTables {
            catalog,
            tables: RwLock::default(),
        }
    }

    /// Bind `contract` to `table`.
    pub fn with_table(self, contract: &str, table: TableIdent) -> IcebergTables {
        self.bind(contract, table);
        self
    }

    /// Bind `contract` to `table`, replacing any table it was bound to.
    pub fn bind(&self, contract: &str, table: TableIdent) {
        self.tables
            .write()
            .expect("lock")
            .insert(contract.to_owned(), table);
    }

    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }

    /// The table a contract is bound to, if it is bound here.
    pub fn table(&self, contract: &str) -> Option<IcebergLocation> {
        let table = self.tables.read().expect("lock").get(contract).cloned()?;
        Some(IcebergLocation {
            catalog: self.catalog.clone(),
            table,
        })
    }
}

#[async_trait]
impl BindingResolver for IcebergTables {
    async fn provider(
        &self,
        contract: &CompiledContract,
        stored: bool,
    ) -> Result<Arc<dyn TableProvider>> {
        let loc = self.table(&contract.name).ok_or_else(|| {
            PeqlError::Invalid(format!("`{}` is bound to no Iceberg table", contract.name))
        })?;
        let table = loc.load().await?.ok_or_else(|| PeqlError::NotWritten {
            contract: contract.name.clone(),
        })?;
        let snapshot =
            table
                .metadata()
                .current_snapshot_id()
                .ok_or_else(|| PeqlError::NotWritten {
                    contract: contract.name.clone(),
                })?;
        loc.provider(&table, contract, stored, snapshot).await
    }

    fn location(&self, contract: &CompiledContract) -> Option<Location> {
        self.table(&contract.name).map(Location::Iceberg)
    }
}

/// One table in one catalog.
#[derive(Clone, Debug)]
pub struct IcebergLocation {
    pub catalog: Arc<dyn Catalog>,
    pub table: TableIdent,
}

/// A data file a snapshot holds, alive in it.
#[derive(Clone, Debug)]
pub struct SnapshotFile {
    /// As the table's metadata records it.
    pub path: String,
    /// Relative to the table's location.
    pub relative: String,
    pub bytes: u64,
}

impl IcebergLocation {
    /// The same table, however it was reached: two contracts over one table share data.
    pub fn key(&self) -> String {
        format!("iceberg:{}", self.table)
    }

    /// The table as the catalog has it now, or `None` when the catalog has no such table.
    pub async fn load(&self) -> Result<Option<Table>> {
        match self.catalog.load_table(&self.table).await {
            Ok(t) => Ok(Some(t)),
            Err(e) if e.kind() == ErrorKind::TableNotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// The table as of `snapshot`, with the columns the contract reads: with `stored`, every
    /// column peQL writes; without, the raw row columns. Built afresh for each call, so a
    /// schema change is never hidden behind a provider built before it.
    pub async fn provider(
        &self,
        table: &Table,
        contract: &CompiledContract,
        stored: bool,
        snapshot: i64,
    ) -> Result<Arc<dyn TableProvider>> {
        let inner =
            IcebergStaticTableProvider::try_new_from_table_snapshot(table.clone(), snapshot)
                .await?;
        let have = inner.schema();
        let want = if stored {
            table_schema(contract)
        } else {
            contract.row_schema.clone()
        };
        let mut select: Vec<Expr> = Vec::with_capacity(want.fields().len());
        for f in want.fields() {
            let Ok(found) = have.field_with_name(f.name()) else {
                return Err(PeqlError::Invalid(format!(
                    "`{}` reads column `{}`, which snapshot {snapshot} of {} does not have",
                    contract.name,
                    f.name(),
                    self.table
                )));
            };
            let c = col_ref(f.name());
            select.push(if found.data_type() == f.data_type() {
                c.alias(f.name())
            } else {
                cast(c, f.data_type().clone()).alias(f.name())
            });
        }
        let plan = LogicalPlanBuilder::scan(
            format!("__peql_iceberg_{snapshot}"),
            provider_as_source(Arc::new(inner)),
            None,
        )?
        .project(select)?
        .build()?;
        Ok(Arc::new(ViewTable::new(plan, None)))
    }

    /// The data files alive in `snapshot`, in path order.
    pub async fn files(&self, table: &Table, snapshot: i64) -> Result<Vec<SnapshotFile>> {
        let snap = table
            .metadata()
            .snapshot_by_id(snapshot)
            .ok_or_else(|| self.no_snapshot(snapshot))?;
        let list = table.manifest_list_reader(snap).load().await?;
        let root = table.metadata().location().trim_end_matches('/').to_owned();
        let mut out = Vec::new();
        for m in list.entries() {
            let manifest = table.manifest_reader().read(m).await?;
            for e in manifest.entries() {
                if !e.is_alive() || e.content_type() != DataContentType::Data {
                    continue;
                }
                let path = e.file_path().to_owned();
                let relative = path
                    .strip_prefix(&root)
                    .map(|p| p.trim_start_matches('/').to_owned())
                    .unwrap_or_else(|| path.clone());
                out.push(SnapshotFile {
                    bytes: e.data_file().file_size_in_bytes(),
                    path,
                    relative,
                });
            }
        }
        out.sort_by(|a, b| a.relative.cmp(&b.relative));
        Ok(out)
    }

    /// Manifest entries from each file's footer, read with two ranged reads.
    pub async fn file_entries(
        &self,
        table: &Table,
        files: &[SnapshotFile],
        flag_columns: &[String],
    ) -> Result<Vec<FileEntry>> {
        let mut out = Vec::with_capacity(files.len());
        for f in files {
            let meta = footer(table, &f.path, f.bytes).await?;
            out.push(binding::entry_from_footer(
                f.relative.clone(),
                f.bytes,
                &meta,
                flag_columns,
            ));
        }
        Ok(out)
    }

    /// The hash [`binding::data_hash`] computes over a directory, over a snapshot's files.
    pub async fn data_hash(&self, table: &Table, files: &[SnapshotFile]) -> Result<String> {
        const CHUNK: u64 = 1 << 20;
        let mut acc = DataHash::default();
        for f in files {
            let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
            let reader = table.file_io().new_input(&f.path)?.reader().await?;
            let mut at = 0;
            while at < f.bytes {
                let end = (at + CHUNK).min(f.bytes);
                sha2::Digest::update(&mut hasher, &reader.read(at..end).await?);
                at = end;
            }
            acc.add(&f.relative, &hex::encode(sha2::Digest::finalize(hasher)));
        }
        Ok(acc.finish())
    }

    fn facts_path(table: &Table, snapshot: i64, contract: &str) -> String {
        format!(
            "{}/{FACTS_DIR}/{snapshot}/{}.json",
            table.metadata().location().trim_end_matches('/'),
            contract.replace('/', "__")
        )
    }

    /// What one contract knows about the data of one snapshot, if it has been recorded.
    pub async fn load_facts(
        &self,
        table: &Table,
        snapshot: i64,
        contract: &str,
    ) -> Result<Option<Manifest>> {
        let input = table
            .file_io()
            .new_input(Self::facts_path(table, snapshot, contract))?;
        if !input.exists().await? {
            return Ok(None);
        }
        let manifest: Manifest = serde_json::from_slice(&input.read().await?).map_err(|e| {
            PeqlError::Invalid(format!(
                "facts of `{contract}` for snapshot {snapshot} of {}: {e}",
                self.table
            ))
        })?;
        if manifest.snapshot_id != Some(snapshot) {
            return Err(PeqlError::Invalid(format!(
                "the facts of `{contract}` filed under snapshot {snapshot} of {} describe snapshot {:?}",
                self.table, manifest.snapshot_id
            )));
        }
        Ok(Some(manifest))
    }

    /// Record a contract's facts for the snapshot they describe.
    pub async fn save_facts(&self, table: &Table, manifest: &Manifest) -> Result<()> {
        let snapshot = manifest.snapshot_id.ok_or_else(|| {
            PeqlError::Invalid(format!(
                "the facts of `{}` name no snapshot",
                manifest.contract
            ))
        })?;
        let body = serde_json::to_vec_pretty(manifest)
            .map_err(|e| PeqlError::Invalid(format!("facts: {e}")))?;
        table
            .file_io()
            .new_output(Self::facts_path(table, snapshot, &manifest.contract))?
            .write(Bytes::from(body))
            .await?;
        Ok(())
    }

    fn no_snapshot(&self, snapshot: i64) -> PeqlError {
        PeqlError::Invalid(format!("{} has no snapshot {snapshot}", self.table))
    }

    /// Begin a write under `contract`: create the table if the catalog has none, and make its
    /// schema the columns the contract writes (columns the contract no longer writes are
    /// removed from the schema, not from any snapshot). The write starts from the table's
    /// current snapshot.
    pub(crate) async fn begin_write(
        &self,
        contract: &CompiledContract,
        overwrite: bool,
    ) -> Result<IcebergWrite> {
        let want = table_schema(contract);
        let table = match self.load().await? {
            Some(t) => self.evolve(t, &want).await?,
            None => {
                let schema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(&want)?;
                self.catalog
                    .create_table(
                        self.table.namespace(),
                        TableCreation::builder()
                            .name(self.table.name().to_owned())
                            .schema(schema)
                            .build(),
                    )
                    .await?
            }
        };
        let location = table.metadata().location().trim_end_matches('/').to_owned();
        let dir_id = Uuid::new_v4();
        let prefix = format!("{location}/data/{dir_id}");
        let dir = local_dir(&prefix)?;
        std::fs::create_dir_all(&dir)?;
        let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
            table.metadata().current_schema(),
        )?);
        Ok(IcebergWrite {
            location: self.clone(),
            base: table.metadata().current_snapshot_id(),
            schema,
            prefix,
            dir,
            overwrite,
        })
    }

    /// Make the table's current schema the columns in `want`, by name. A column whose type
    /// changed is refused: the table would read its old files as something they are not.
    async fn evolve(&self, table: Table, want: &Schema) -> Result<Table> {
        let current = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())?;
        let wanted = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(want)?;
        let mut update = None;
        let tx = Transaction::new(&table);
        for f in want.fields() {
            match current.field_with_name(f.name()) {
                Ok(have) => {
                    let w = wanted.field_by_name(f.name()).expect("converted from want");
                    let h = table
                        .metadata()
                        .current_schema()
                        .field_by_name(f.name())
                        .expect("converted from the schema");
                    if !same_type(&h.field_type, &w.field_type) {
                        return Err(PeqlError::Invalid(format!(
                            "column `{}` of {} is {:?}; the contract writes {:?}",
                            f.name(),
                            self.table,
                            have.data_type(),
                            f.data_type()
                        )));
                    }
                }
                Err(_) => {
                    let ty = wanted
                        .field_by_name(f.name())
                        .expect("converted from want")
                        .field_type
                        .as_ref()
                        .clone();
                    update = Some(
                        update
                            .unwrap_or_else(|| tx.update_schema())
                            .add_column(AddColumn::optional(f.name(), ty)),
                    );
                }
            }
        }
        for f in current.fields() {
            if want.field_with_name(f.name()).is_err() {
                update = Some(
                    update
                        .unwrap_or_else(|| tx.update_schema())
                        .delete_column(f.name()),
                );
            }
        }
        match update {
            None => Ok(table),
            Some(u) => Ok(u.apply(tx)?.commit(self.catalog.as_ref()).await?),
        }
    }
}

/// Whether two Iceberg types are the same, ignoring the ids of nested fields.
fn same_type(a: &iceberg::spec::Type, b: &iceberg::spec::Type) -> bool {
    use iceberg::spec::Type;
    match (a, b) {
        (Type::Primitive(x), Type::Primitive(y)) => x == y,
        (Type::List(x), Type::List(y)) => {
            x.element_field.required == y.element_field.required
                && same_type(&x.element_field.field_type, &y.element_field.field_type)
        }
        (Type::Map(x), Type::Map(y)) => {
            same_type(&x.key_field.field_type, &y.key_field.field_type)
                && same_type(&x.value_field.field_type, &y.value_field.field_type)
        }
        (Type::Struct(x), Type::Struct(y)) => {
            x.fields().len() == y.fields().len()
                && x.fields()
                    .iter()
                    .zip(y.fields())
                    .all(|(f, g)| f.name == g.name && same_type(&f.field_type, &g.field_type))
        }
        _ => false,
    }
}

/// The columns peQL writes for a contract, partition columns among them: an Iceberg table's
/// files hold every column, and the partition directories only lay them out.
pub fn table_schema(contract: &CompiledContract) -> SchemaRef {
    let mut fields: Vec<Field> = contract
        .scan_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    for flag in &contract.flags {
        fields.push(Field::new(
            &flag.column,
            datafusion::arrow::datatypes::DataType::Boolean,
            true,
        ));
    }
    for d in &contract.derived {
        fields.push(Field::new(&d.column, d.ty.to_arrow(), true));
    }
    Arc::new(Schema::new(fields))
}

/// The directory a local table location names. peQL writes data files with DataFusion's
/// writer, which writes to the local filesystem here.
fn local_dir(location: &str) -> Result<PathBuf> {
    let path = location.strip_prefix("file://").unwrap_or(location);
    if location.contains("://") && !location.starts_with("file://")
        || !Path::new(path).is_absolute()
    {
        return Err(PeqlError::Invalid(format!(
            "`{location}` is not a local path; peQL writes Iceberg tables on the local filesystem"
        )));
    }
    Ok(PathBuf::from(path))
}

async fn footer(table: &Table, path: &str, size: u64) -> Result<ParquetMetaData> {
    let bad = |e: &dyn std::fmt::Display| PeqlError::Invalid(format!("{path}: {e}"));
    let footer_size = FOOTER_SIZE as u64;
    if size < footer_size {
        return Err(bad(&"too short to be Parquet"));
    }
    let reader = table.file_io().new_input(path)?.reader().await?;
    let tail = reader.read(size - footer_size..size).await?;
    let tail: [u8; FOOTER_SIZE] = tail.as_ref().try_into().map_err(|_| bad(&"short read"))?;
    let len = FooterTail::try_new(&tail)
        .map_err(|e| bad(&e))?
        .metadata_length() as u64;
    let end = size - footer_size;
    if len > end {
        return Err(bad(&"footer longer than the file"));
    }
    let buf = reader.read(end - len..end).await?;
    ParquetMetaDataReader::decode_metadata(&buf).map_err(|e| bad(&e))
}

/// The snapshot a write committed, and the one before it.
pub use crate::engine::SnapshotCommit;

/// A write in progress to one Iceberg table.
#[derive(Debug)]
pub(crate) struct IcebergWrite {
    location: IcebergLocation,
    /// The snapshot current when the write began; an overwrite commits only over it.
    base: Option<i64>,
    /// The table's schema as Arrow, with each column's field id: what a data file holds.
    schema: SchemaRef,
    /// Where this write's data files go, as the table records paths.
    prefix: String,
    dir: PathBuf,
    overwrite: bool,
}

impl IcebergWrite {
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The directory the write's data files go to, as DataFusion's writer takes it.
    pub(crate) fn url(&self) -> Result<String> {
        binding::local_url(&self.dir, true)
    }

    /// Commit the data files the parts wrote as one snapshot, with the writing contract in
    /// its summary. Returns the table as committed.
    pub(crate) async fn commit(
        &self,
        contract: &CompiledContract,
    ) -> Result<(Table, SnapshotCommit)> {
        let table =
            self.location.load().await?.ok_or_else(|| {
                PeqlError::Invalid(format!("{} was dropped", self.location.table))
            })?;
        let files = self.data_files(&table)?;
        let mut summary = HashMap::from([
            (SUMMARY_CONTRACT.to_owned(), contract.name.clone()),
            (
                SUMMARY_CONTRACT_HASH.to_owned(),
                contract.contract_hash.clone(),
            ),
            (
                SUMMARY_COMPILATION_HASH.to_owned(),
                contract.compilation_hash.clone(),
            ),
        ]);
        let committed = if self.overwrite {
            summary.insert(SUMMARY_WRITE.to_owned(), "overwrite".to_owned());
            self.commit_overwrite(&table, files, summary).await?
        } else {
            summary.insert(SUMMARY_WRITE.to_owned(), "append".to_owned());
            let tx = Transaction::new(&table);
            tx.fast_append()
                .add_data_files(files)
                .set_snapshot_properties(summary)
                .apply(tx)?
                .commit(self.location.catalog.as_ref())
                .await?
        };
        let snapshot = committed.metadata().current_snapshot().ok_or_else(|| {
            PeqlError::Invalid(format!("{} committed no snapshot", self.location.table))
        })?;
        let commit = SnapshotCommit {
            snapshot_id: snapshot.snapshot_id(),
            parent_snapshot_id: snapshot.parent_snapshot_id(),
        };
        Ok((committed, commit))
    }

    /// Every Parquet file under the write's directory, as an Iceberg data file. A file
    /// without field ids is refused: the table reads columns by id.
    fn data_files(&self, table: &Table) -> Result<Vec<DataFile>> {
        let spec_id = table.metadata().default_partition_spec_id();
        let mut out = Vec::new();
        for path in binding::list_files(&self.dir)? {
            let reader = datafusion::parquet::file::reader::SerializedFileReader::new(
                std::fs::File::open(&path)?,
            )
            .map_err(|e| PeqlError::Invalid(format!("{}: {e}", path.display())))?;
            let meta = datafusion::parquet::file::reader::FileReader::metadata(&reader);
            let schema = meta.file_metadata().schema_descr();
            for i in 0..schema.num_columns() {
                let root = schema.get_column_root(i);
                if !root.get_basic_info().has_id() {
                    return Err(PeqlError::Invalid(format!(
                        "{} was written without field ids (column `{}`)",
                        path.display(),
                        root.name()
                    )));
                }
            }
            let relative = path
                .strip_prefix(&self.dir)
                .map_err(|_| {
                    PeqlError::Invalid(format!("{} is outside the write", path.display()))
                })?
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            let file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(format!("{}/{relative}", self.prefix))
                .file_format(DataFileFormat::Parquet)
                .partition_spec_id(spec_id)
                .record_count(meta.file_metadata().num_rows() as u64)
                .file_size_in_bytes(std::fs::metadata(&path)?.len())
                .build()
                .map_err(|e| PeqlError::Invalid(format!("{}: {e}", path.display())))?;
            out.push(file);
        }
        Ok(out)
    }

    /// Replace the table's files with `files` in one snapshot that commits only if the
    /// table's current snapshot is still the one the write began from.
    ///
    /// iceberg-rust has no overwrite at this commit, and the two ways to put a snapshot of
    /// one's own into `Catalog::update_table` are closed to a caller outside the crate:
    /// `TableCommit`'s builder is `pub(crate)`, and so is `TransactionAction`. So the
    /// replacing snapshot is produced by the crate's own append, over a base whose current
    /// snapshot (the one the write began from, by id) lists no manifests and counts no rows:
    /// the snapshot it makes holds only `files`, its parent is the snapshot the write began
    /// from, and its commit carries the crate's requirements that the table is the same
    /// table and that `main` is still at that snapshot. The catalog checks those
    /// requirements as it moves the pointer, so of two overwrites begun from one snapshot
    /// one commits and the other is refused. The snapshot records the operation `append`
    /// (the crate's), and `peql.write = overwrite`.
    async fn commit_overwrite(
        &self,
        table: &Table,
        files: Vec<DataFile>,
        summary: HashMap<String, String>,
    ) -> Result<Table> {
        let catalog = self.location.catalog.clone();
        if table.metadata().current_snapshot_id() != self.base {
            return Err(self.conflict(table.metadata().current_snapshot_id()));
        }
        let Some(base) = self.base else {
            // Nothing to replace: the crate's append over no snapshot, which commits only
            // while the table still has none.
            let pinned = Pinned {
                inner: catalog,
                table: self.location.table.clone(),
                base: None,
                as_base: table.clone(),
            };
            let tx = Transaction::new(table);
            return Ok(tx
                .fast_append()
                .add_data_files(files)
                .set_snapshot_properties(summary)
                .apply(tx)?
                .commit(&pinned)
                .await
                .map_err(|e| pinned.refused(e))?);
        };
        let md = table.metadata();
        if md.format_version() != FormatVersion::V2 {
            return Err(PeqlError::Invalid(format!(
                "{} is Iceberg format {:?}; peQL overwrites format 2 tables",
                self.location.table,
                md.format_version()
            )));
        }
        let empty = format!(
            "{}/peql-overwrite-base-{}.avro",
            md.metadata_location()?,
            Uuid::new_v4()
        );
        let mut writer = ManifestListWriter::v2(
            table.file_io().new_output(&empty)?.writer().await?,
            base,
            None,
            md.last_sequence_number(),
        );
        writer.add_manifests(std::iter::empty())?;
        writer.close().await?;
        let result = async {
            let as_base = emptied(table, base, &empty)?;
            let pinned = Pinned {
                inner: catalog,
                table: self.location.table.clone(),
                base: Some(base),
                as_base: as_base.clone(),
            };
            let tx = Transaction::new(&as_base);
            tx.fast_append()
                .with_check_duplicate(false)
                .add_data_files(files)
                .set_snapshot_properties(summary)
                .apply(tx)?
                .commit(&pinned)
                .await
                .map_err(|e| pinned.refused(e))
        }
        .await;
        table.file_io().delete(&empty).await?;
        result
    }

    fn conflict(&self, now: Option<i64>) -> PeqlError {
        PeqlError::Conflict(format!(
            "{} is at snapshot {now:?}; the overwrite began from {:?}",
            self.location.table, self.base
        ))
    }
}

/// `table` with snapshot `base` listing the manifests in `manifest_list` (none) and counting
/// nothing: the base an overwrite is produced over. The table's metadata is changed only in
/// this copy, through its serialised form; the catalog never sees it.
fn emptied(table: &Table, base: i64, manifest_list: &str) -> Result<Table> {
    let mut md = serde_json::to_value(table.metadata())
        .map_err(|e| PeqlError::Invalid(format!("metadata of {}: {e}", table.identifier())))?;
    let snapshots = md
        .get_mut("snapshots")
        .and_then(|s| s.as_array_mut())
        .ok_or_else(|| PeqlError::Invalid(format!("{} lists no snapshots", table.identifier())))?;
    let snap = snapshots
        .iter_mut()
        .find(|s| s.get("snapshot-id").and_then(|v| v.as_i64()) == Some(base))
        .ok_or_else(|| {
            PeqlError::Invalid(format!("{} has no snapshot {base}", table.identifier()))
        })?;
    snap["manifest-list"] = serde_json::Value::String(manifest_list.to_owned());
    if let Some(summary) = snap.get_mut("summary").and_then(|s| s.as_object_mut()) {
        summary.retain(|k, _| !k.starts_with("total-"));
        for k in [
            "total-records",
            "total-files-size",
            "total-data-files",
            "total-delete-files",
            "total-position-deletes",
            "total-equality-deletes",
        ] {
            summary.insert(k.to_owned(), serde_json::Value::String("0".into()));
        }
    }
    let md: TableMetadata = serde_json::from_value(md)
        .map_err(|e| PeqlError::Invalid(format!("metadata of {}: {e}", table.identifier())))?;
    // The crate keeps a table's runtime to itself; the copy runs on the current one.
    let mut b = Table::builder()
        .metadata(md)
        .identifier(table.identifier().clone())
        .file_io(table.file_io().clone())
        .runtime(iceberg::Runtime::try_current()?);
    if let Some(loc) = table.metadata_location() {
        b = b.metadata_location(loc);
    }
    Ok(b.build()?)
}

/// The catalog an overwrite commits through: the caller's, except that loading the table
/// gives the overwrite's base, and only while the table's current snapshot is still the one
/// the overwrite began from. A commit the catalog refuses is retried by the crate after a
/// load, which this refuses; so a refused overwrite is never rebased onto another snapshot.
#[derive(Debug)]
struct Pinned {
    inner: Arc<dyn Catalog>,
    table: TableIdent,
    base: Option<i64>,
    as_base: Table,
}

impl Pinned {
    fn refused(&self, e: iceberg::Error) -> PeqlError {
        if e.kind() == ErrorKind::CatalogCommitConflicts {
            PeqlError::Conflict(format!(
                "{}: the table moved since the overwrite began from snapshot {:?}: {e}",
                self.table, self.base
            ))
        } else {
            e.into()
        }
    }
}

#[async_trait]
impl Catalog for Pinned {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }
    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }
    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        let current = self.inner.load_table(table).await?;
        if table != &self.table {
            return Ok(current);
        }
        let now = current.metadata().current_snapshot_id();
        if now != self.base {
            return Err(iceberg::Error::new(
                ErrorKind::CatalogCommitConflicts,
                format!(
                    "{table} is at snapshot {now:?}; the overwrite began from {:?}",
                    self.base
                ),
            ));
        }
        if current.metadata().uuid() != self.as_base.metadata().uuid() {
            return Err(iceberg::Error::new(
                ErrorKind::CatalogCommitConflicts,
                format!("{table} was replaced since the overwrite began"),
            ));
        }
        // The base, at the catalog's current metadata location: the commit is made against
        // what the catalog holds now, with the requirements the crate derives from the base.
        let mut b = Table::builder()
            .metadata(self.as_base.metadata_ref())
            .identifier(table.clone())
            .file_io(current.file_io().clone())
            .runtime(iceberg::Runtime::try_current()?);
        if let Some(loc) = current.metadata_location() {
            b = b.metadata_location(loc);
        }
        b.build()
    }
    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }
    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(table).await
    }
    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }
    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }
    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.inner.update_table(commit).await
    }
}
