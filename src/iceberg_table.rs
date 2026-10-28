//! Contracts bound to Iceberg tables (feature `iceberg`).
//!
//! [`IcebergTables`] resolves a contract to the table its document names
//! (`binding: {iceberg: sales.orders}`) in an Iceberg catalog the caller supplies; peQL never
//! builds a catalog of its own and does not know what is behind the one it is given.
//! A read is a view over the table as of one snapshot: the current one, or one the caller
//! names ([`crate::Engine::query_as_of`]). A write lands its data files as the Parquet binding
//! lands them, then commits one snapshot through the catalog: an append is the catalog crate's
//! own; an overwrite replaces the table's files in a snapshot that may only commit over the
//! snapshot the write began from, so two overwrites begun from one snapshot cannot both commit;
//! a rewrite (a compaction) replaces one snapshot's files with files holding the same rows and
//! keeps every file appended since, so appends landing while it runs do not refuse it.
//! No write deletes a file, and every earlier snapshot stays readable until it is expired.
//!
//! What peQL knows about a snapshot's data ([`Manifest`]: the verdict, statistics, data hash
//! and each file's contract hash, flags and hash) is kept with that snapshot: the writing
//! contract's name and hashes in the snapshot's summary, and the rest, per contract, in one
//! file keyed by the snapshot id beside the table's metadata
//! (`metadata/peql/<snapshot>/<contract>.json`). A write records the facts of the rows it
//! wrote ([`WriteFacts`]) before it commits, per contract bound to the table, in
//! `metadata/peql/writes/<write>/<contract>.json`, and names the write in the snapshot's
//! summary ([`SUMMARY_WRITE_ID`]); a snapshot's facts are its parent's combined with its
//! write's, so no write reads a file it did not write.
//!
//! ```no_run
//! # async fn f(catalog: std::sync::Arc<dyn iceberg::Catalog>) -> peql::Result<()> {
//! use std::sync::Arc;
//! use peql::iceberg_table::IcebergTables;
//!
//! // Every contract whose binding is `{iceberg: <namespace>.<table>}` reads and writes that
//! // table in `catalog`.
//! let tables = IcebergTables::new(catalog);
//! let engine = peql::Engine::open("/var/lib/peql")?.with_bindings(Arc::new(tables));
//! # Ok(()) }
//! ```

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::catalog::view::ViewTable;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, cast, lit};
use datafusion::parquet::file::FOOTER_SIZE;
use datafusion::parquet::file::metadata::{FooterTail, ParquetMetaData, ParquetMetaDataReader};
use datafusion::scalar::ScalarValue;
use datafusion_iceberg::IcebergStaticTableProvider;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, FormatVersion, ManifestContentType,
    ManifestFile, ManifestListWriter, TableMetadata,
};
use iceberg::table::Table;
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction, UpdateSchemaAction};
use iceberg::{
    Catalog, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use parcel_core::CompiledContract;
use parcel_core::document::Source;
use parcel_runtime::plan::col_ref;
use uuid::Uuid;

use crate::binding::{self, BindingResolver, Location};
use crate::error::{PeqlError, Result};
use crate::facts::WriteFacts;
use crate::manifest::{self, FileEntry, Manifest};

/// Snapshot summary keys peQL writes: the contract the snapshot's files were written under.
pub const SUMMARY_CONTRACT: &str = "peql.contract";
pub const SUMMARY_CONTRACT_HASH: &str = "peql.contract-hash";
pub const SUMMARY_COMPILATION_HASH: &str = "peql.compilation-hash";
/// `append` or `overwrite`: what the write did, whatever operation the snapshot records.
pub const SUMMARY_WRITE: &str = "peql.write";
/// The write that made the snapshot, whose facts are under `metadata/peql/writes/<id>/`.
pub const SUMMARY_WRITE_ID: &str = "peql.write-id";

/// Where a contract's facts for one snapshot live, under the table's location.
const FACTS_DIR: &str = "metadata/peql";
/// Where the facts of each write's rows live, under the table's location.
const WRITES_DIR: &str = "metadata/peql/writes";

/// Contracts bound to tables in one Iceberg catalog. A contract whose document says
/// `binding: {iceberg: <namespace>.<table>}` is bound to that table in the catalog: every part
/// but the last is a namespace level, the last is the table, as parcel splits them. A contract
/// bound to anything else has no binding here.
#[derive(Debug)]
pub struct IcebergTables {
    catalog: Arc<dyn Catalog>,
}

impl IcebergTables {
    /// Tables in `catalog`, which the caller built and owns.
    pub fn new(catalog: Arc<dyn Catalog>) -> IcebergTables {
        IcebergTables { catalog }
    }

    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }

    /// The table a contract's binding names, if it names an Iceberg table.
    pub fn table(&self, contract: &CompiledContract) -> Option<IcebergLocation> {
        let Source::Iceberg(t) = &contract.binding.source else {
            return None;
        };
        let namespace = NamespaceIdent::from_vec(t.namespace.clone())
            .expect("parcel's Iceberg identifier has at least one namespace part");
        Some(IcebergLocation {
            catalog: self.catalog.clone(),
            table: TableIdent::new(namespace, t.table.clone()),
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
        let loc = self
            .table(contract)
            .ok_or_else(|| binding::unresolved(contract))?;
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
        self.table(contract).map(Location::Iceberg)
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

    /// The table as of `snapshot`, every column it has, as stored: what a compaction copies.
    /// A column no version of the contract names is a column of the rows all the same.
    pub async fn whole(&self, table: &Table, snapshot: i64) -> Result<Arc<dyn TableProvider>> {
        Ok(Arc::new(
            IcebergStaticTableProvider::try_new_from_table_snapshot(table.clone(), snapshot)
                .await?,
        ))
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

    /// Manifest entries from each file's footer, read with two ranged reads, and the sha256
    /// of its bytes: what a validation of the whole snapshot records.
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
                sha256(table, f).await?,
            ));
        }
        Ok(out)
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
        let Some(manifest) = manifest::decode::<Manifest>(&input.read().await?).map_err(|e| {
            PeqlError::Invalid(format!(
                "facts of `{contract}` for snapshot {snapshot} of {} ({}): {e}",
                self.table,
                input.location()
            ))
        })?
        else {
            return Ok(None);
        };
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
        let body =
            manifest::encode(manifest).map_err(|e| PeqlError::Invalid(format!("facts: {e}")))?;
        table
            .file_io()
            .new_output(Self::facts_path(table, snapshot, &manifest.contract))?
            .write(Bytes::from(body))
            .await?;
        Ok(())
    }

    fn write_facts_path(table: &Table, write: &str, contract: &str) -> String {
        format!(
            "{}/{WRITES_DIR}/{write}/{}.json",
            table.metadata().location().trim_end_matches('/'),
            contract.replace('/', "__")
        )
    }

    /// Record the facts of one write's rows under one contract, before the write commits.
    pub(crate) async fn save_write_facts(
        &self,
        table: &Table,
        write: &str,
        facts: &WriteFacts,
    ) -> Result<()> {
        let body = manifest::encode(facts)
            .map_err(|e| PeqlError::Invalid(format!("facts of write {write}: {e}")))?;
        table
            .file_io()
            .new_output(Self::write_facts_path(table, write, &facts.facts.contract))?
            .write(Bytes::from(body))
            .await?;
        Ok(())
    }

    /// The facts of the rows the write that made `snapshot` wrote, under `contract`; `None`
    /// for a snapshot peQL did not write, or whose write did not record facts under it.
    pub async fn load_write_facts(
        &self,
        table: &Table,
        snapshot: i64,
        contract: &str,
    ) -> Result<Option<WriteFacts>> {
        let snap = table
            .metadata()
            .snapshot_by_id(snapshot)
            .ok_or_else(|| self.no_snapshot(snapshot))?;
        let Some(write) = snap.summary().additional_properties.get(SUMMARY_WRITE_ID) else {
            return Ok(None);
        };
        let input = table
            .file_io()
            .new_input(Self::write_facts_path(table, write, contract))?;
        if !input.exists().await? {
            return Ok(None);
        }
        manifest::decode(&input.read().await?).map_err(|e| {
            PeqlError::Invalid(format!(
                "facts of `{contract}` for write {write} of {} ({}): {e}",
                self.table,
                input.location()
            ))
        })
    }

    fn no_snapshot(&self, snapshot: i64) -> PeqlError {
        PeqlError::Invalid(format!("{} has no snapshot {snapshot}", self.table))
    }

    /// Begin a write under `contract`: create the table if the catalog has none, and add to
    /// its schema the columns the contract writes that it lacks. No column is ever removed
    /// from the schema by a write: another version of the contract, or a snapshot, still
    /// reads it, and the rows this write adds have no value for a column the contract does
    /// not name (null), so a table column the contract lacks that is required is refused.
    /// The write starts from the table's current snapshot. Its data files land in the table's own directory when the table is
    /// on this filesystem; otherwise under `staging`, and go to the table through its
    /// `FileIO` when the write commits.
    pub(crate) async fn begin_write(
        &self,
        contract: &CompiledContract,
        kind: WriteKind,
        staging: &Path,
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
        if !matches!(kind, WriteKind::Rewrite(_)) {
            // A row this write adds has no value for a column the contract does not name.
            for f in table.metadata().current_schema().as_struct().fields() {
                if f.required && want.field_with_name(&f.name).is_err() {
                    return Err(PeqlError::Invalid(format!(
                        "column `{}` of {} is required, and `{}` does not write it: a write \
                         may not leave a required column without a value",
                        f.name, self.table, contract.name
                    )));
                }
            }
        }
        let location = table.metadata().location().trim_end_matches('/').to_owned();
        let dir_id = Uuid::new_v4();
        let prefix = format!("{location}/data/{dir_id}");
        let (dir, staged) = match local_dir(&prefix)? {
            Some(dir) => (dir, false),
            None => (staging.join(dir_id.to_string()), true),
        };
        std::fs::create_dir_all(&dir)?;
        let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
            table.metadata().current_schema(),
        )?);
        Ok(IcebergWrite {
            id: dir_id.to_string(),
            location: self.clone(),
            base: table.metadata().current_snapshot_id(),
            schema,
            prefix,
            dir,
            staged,
            kind,
        })
    }

    /// Add to the table's current schema the columns in `want` that it lacks, by name, and
    /// remove none: a column `want` does not name stays, for the other versions of the
    /// contract and the snapshots that read it. A column whose type changed is refused: the
    /// table would read its old files as something they are not.
    ///
    /// The change is computed over the table as it is, and committed; when another writer
    /// changed the schema first, it is computed again over the table as that writer left it
    /// and committed again, as the table's retry properties allow ([`CommitRetry`]). A table
    /// that already has the columns (by name, of the same types) is left as it is, with no
    /// commit, so of writers making one change at once, one commits it and the others find it
    /// made. A column another writer gave a different type is refused, naming both types.
    async fn evolve(&self, table: Table, want: &Schema) -> Result<Table> {
        let mut table = table;
        let mut refused = None;
        let mut retry = CommitRetry::of(&table)?.start();
        loop {
            let tx = Transaction::new(&table);
            let Some(update) = self.schema_change(&table, want, &tx, refused.is_some())? else {
                return Ok(table);
            };
            match update.apply(tx)?.commit(self.catalog.as_ref()).await {
                Ok(committed) => {
                    table = committed;
                    // The crate's own retry may have applied the change over a schema
                    // another writer had changed again: look once more.
                    let tx = Transaction::new(&table);
                    if self
                        .schema_change(&table, want, &tx, refused.is_some())?
                        .is_none()
                    {
                        return Ok(table);
                    }
                }
                Err(e) => {
                    let now = self.load().await?.ok_or_else(|| {
                        PeqlError::Invalid(format!(
                            "{} was dropped while its schema was changed: {e}",
                            self.table
                        ))
                    })?;
                    let (was, is) = (table.metadata(), now.metadata());
                    if is.uuid() == was.uuid()
                        && is.current_schema_id() == was.current_schema_id()
                        && is.current_schema() == was.current_schema()
                    {
                        // Nobody changed the schema: the refusal is the change's own.
                        return Err(e.into());
                    }
                    refused = Some(e);
                    table = now;
                }
            }
            if !retry.wait().await {
                break;
            }
            table = self.load().await?.ok_or_else(|| {
                PeqlError::Invalid(format!(
                    "{} was dropped while its schema was changed",
                    self.table
                ))
            })?;
        }
        let tx = Transaction::new(&table);
        match self.schema_change(&table, want, &tx, true)? {
            None => Ok(table),
            Some(_) => Err(PeqlError::Conflict(format!(
                "the schema of {} changed under each of {} attempts to make it have the \
                 contract's columns; the last refusal: {}",
                self.table,
                retry.attempts(),
                refused.map_or_else(|| "none".to_owned(), |e| e.to_string())
            ))),
        }
    }

    /// The change that adds to `table`'s current schema the columns in `want` that it lacks,
    /// or `None` when it has them. Nothing is removed. `raced` says the schema was changed by another writer since this write
    /// first looked, for the refusal of a column whose type differs.
    fn schema_change(
        &self,
        table: &Table,
        want: &Schema,
        tx: &Transaction,
        raced: bool,
    ) -> Result<Option<UpdateSchemaAction>> {
        let schema = table.metadata().current_schema();
        let current = iceberg::arrow::schema_to_arrow_schema(schema)?;
        let wanted = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(want)?;
        let mut update: Option<UpdateSchemaAction> = None;
        for f in want.fields() {
            let w = wanted.field_by_name(f.name()).expect("converted from want");
            match current.field_with_name(f.name()) {
                Ok(have) => {
                    let h = schema
                        .field_by_name(f.name())
                        .expect("converted from the schema");
                    if !same_type(&h.field_type, &w.field_type) {
                        return Err(PeqlError::Invalid(format!(
                            "column `{}` of {} is {:?}; the contract writes {:?}{}",
                            f.name(),
                            self.table,
                            have.data_type(),
                            f.data_type(),
                            if raced {
                                " (another writer changed the table's schema meanwhile)"
                            } else {
                                ""
                            }
                        )));
                    }
                }
                Err(_) => {
                    let ty = w.field_type.as_ref().clone();
                    update = Some(
                        update
                            .unwrap_or_else(|| tx.update_schema())
                            .add_column(AddColumn::optional(f.name(), ty)),
                    );
                }
            }
        }
        Ok(update)
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

/// The files one write wrote to its local directory at `url`, read with the contract's raw
/// row columns: what the write's validation reads. The files hold every column the table
/// does, partition columns among them, so their directories name no partition.
pub(crate) fn written_files(
    contract: &CompiledContract,
    url: &str,
) -> Result<Arc<dyn TableProvider>> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    let options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
    let config = ListingTableConfig::new(ListingTableUrl::parse(url)?)
        .with_listing_options(options)
        .with_schema(contract.row_schema.clone());
    Ok(Arc::new(ListingTable::try_new(config)?))
}

/// The directory a table location names on this filesystem, where DataFusion's writer writes
/// the data files; `None` for a location in another store (`s3://`, `gs://`, ...).
fn local_dir(location: &str) -> Result<Option<PathBuf>> {
    if location.contains("://") && !location.starts_with("file://") {
        return Ok(None);
    }
    let path = location.strip_prefix("file://").unwrap_or(location);
    if !Path::new(path).is_absolute() {
        return Err(PeqlError::Invalid(format!(
            "`{location}` is neither an absolute path nor a store URL"
        )));
    }
    Ok(Some(PathBuf::from(path)))
}

/// How much of a staged file goes to the table's store per write.
const UPLOAD_CHUNK: usize = 8 << 20;

/// The sha256 of a snapshot file's bytes, read in chunks through the table's `FileIO`.
async fn sha256(table: &Table, f: &SnapshotFile) -> Result<String> {
    const CHUNK: u64 = 1 << 20;
    let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
    let reader = table.file_io().new_input(&f.path)?.reader().await?;
    let mut at = 0;
    while at < f.bytes {
        let end = (at + CHUNK).min(f.bytes);
        sha2::Digest::update(&mut hasher, &reader.read(at..end).await?);
        at = end;
    }
    Ok(hex::encode(sha2::Digest::finalize(hasher)))
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

/// How a commit that the table moved under is tried again: a schema change, or a rewrite's
/// carrying forward of what was appended since its snapshot. The table's own Iceberg retry
/// properties say how often and how long to wait; a table that sets none has Iceberg's
/// defaults (Iceberg documentation, Configuration, "Table behavior properties"):
///
/// | property | default |
/// |---|---|
/// | `commit.retry.num-retries` | 4 retries after the first attempt |
/// | `commit.retry.min-wait-ms` | 100 |
/// | `commit.retry.max-wait-ms` | 60000 (1 minute) |
/// | `commit.retry.total-timeout-ms` | 1800000 (30 minutes) |
///
/// The defaults are the Iceberg crate's own constants for these properties
/// (`TableProperties::PROPERTY_COMMIT_*_DEFAULT`), read through the table's typed
/// properties, so a table's value and the default come from one place. The wait before the
/// n-th retry is `min-wait-ms * 2^n`, plus a random share of itself up to the same again
/// (so writers that conflicted together do not meet again together), at most `max-wait-ms`;
/// no retry starts once `total-timeout-ms` has passed since the first attempt. One policy
/// serves every commit that is computed again here, and the crate's own commits retry by
/// the same properties.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CommitRetry {
    retries: usize,
    min_wait: Duration,
    max_wait: Duration,
    total: Duration,
}

impl CommitRetry {
    fn of(table: &Table) -> Result<CommitRetry> {
        let p = table.metadata().table_properties();
        Ok(CommitRetry {
            retries: p.commit_num_retries()?,
            min_wait: Duration::from_millis(p.commit_min_retry_wait_ms()?),
            max_wait: Duration::from_millis(p.commit_max_retry_wait_ms()?),
            total: Duration::from_millis(p.commit_total_retry_timeout_ms()?),
        })
    }

    fn start(self) -> Retrying {
        Retrying {
            policy: self,
            retried: 0,
            started: Instant::now(),
        }
    }

    /// The wait before retry number `retried + 1`; `unit`, in `[0, 1]`, is the random share.
    fn wait_for(&self, retried: usize, unit: f64) -> Duration {
        let base = self
            .min_wait
            .saturating_mul(1u32.checked_shl(retried.min(31) as u32).unwrap_or(u32::MAX));
        let base = base.min(self.max_wait);
        base.saturating_add(base.mul_f64(unit.clamp(0.0, 1.0)))
            .min(self.max_wait)
    }
}

/// A [`CommitRetry`] that has begun: the attempts made so far.
#[derive(Debug)]
struct Retrying {
    policy: CommitRetry,
    retried: usize,
    started: Instant,
}

impl Retrying {
    /// Wait as long as the next retry is due, and say that it is. `false`, with no wait,
    /// once the retries are used or the total time would be passed.
    async fn wait(&mut self) -> bool {
        if self.retried >= self.policy.retries {
            return false;
        }
        let unit = (Uuid::new_v4().as_u128() >> 64) as f64 / u64::MAX as f64;
        let wait = self.policy.wait_for(self.retried, unit);
        if self.started.elapsed().saturating_add(wait) > self.policy.total {
            return false;
        }
        self.retried += 1;
        tokio::time::sleep(wait).await;
        true
    }

    /// The attempts made, the first among them.
    fn attempts(&self) -> usize {
        self.retried + 1
    }
}

/// What a write's snapshot does to the table's files.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteKind {
    /// Adds its files to the table's.
    Append,
    /// Replaces the table's files with its own, over the snapshot the write began from only.
    Overwrite,
    /// Replaces the files of this snapshot with its own, which hold the same rows, and keeps
    /// every file added since: see [`IcebergWrite::commit_rewrite`].
    Rewrite(i64),
}

/// A write in progress to one Iceberg table.
#[derive(Debug)]
pub(crate) struct IcebergWrite {
    /// Names the write: its data directory and its facts.
    id: String,
    location: IcebergLocation,
    /// The snapshot current when the write began; an overwrite commits only over it.
    base: Option<i64>,
    /// The table's schema as Arrow, with each column's field id: what a data file holds.
    schema: SchemaRef,
    /// Where this write's data files go, as the table records paths.
    prefix: String,
    /// Where DataFusion's writer writes them.
    dir: PathBuf,
    /// Whether `dir` is a staging directory, not the table's own: its files go to the table
    /// through its `FileIO` at the commit, and the directory is removed after it.
    staged: bool,
    kind: WriteKind,
}

impl Drop for IcebergWrite {
    fn drop(&mut self) {
        if self.staged {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

impl IcebergWrite {
    /// `plan` as a data file of this table holds it: every column of the table's schema, in
    /// its order, cast to its type and carrying its field id. A column the plan lacks is null
    /// (a data file may lack an optional column; readers see null), and one the table
    /// requires is refused by name: no null is written into a required column.
    pub(crate) fn project(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for f in self.schema.fields() {
            let metadata: std::collections::BTreeMap<String, String> = f
                .metadata()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let value = if plan.schema().has_column_with_unqualified_name(f.name()) {
                cast(col_ref(f.name()), f.data_type().clone())
            } else if f.is_nullable() {
                lit(ScalarValue::try_from(f.data_type())?)
            } else {
                return Err(PeqlError::Invalid(format!(
                    "column `{}` of {} is required, and the rows written have no value for it",
                    f.name(),
                    self.location.table
                )));
            };
            columns.push(value.alias_with_metadata(f.name(), Some(metadata.into())));
        }
        Ok(LogicalPlanBuilder::from(plan).project(columns)?.build()?)
    }

    /// The local directory the write's data files go to, and only its own.
    pub(crate) fn dir(&self) -> &Path {
        &self.dir
    }

    /// A path under the write's directory as the table's facts name it: relative to the
    /// table's location, as [`IcebergLocation::files`] gives it.
    pub(crate) fn table_relative(&self, under_dir: &str) -> String {
        format!("data/{}/{under_dir}", self.id)
    }

    /// Commit the data files the parts wrote as one snapshot, with the writing contract and
    /// the write in its summary, once the facts of the write under every contract in `facts`
    /// are recorded. Returns the table as committed.
    pub(crate) async fn commit(
        &self,
        contract: &CompiledContract,
        facts: &[WriteFacts],
    ) -> Result<(Table, SnapshotCommit)> {
        let table =
            self.location.load().await?.ok_or_else(|| {
                PeqlError::Invalid(format!("{} was dropped", self.location.table))
            })?;
        for f in facts {
            self.location.save_write_facts(&table, &self.id, f).await?;
        }
        let files = self.data_files(&table)?;
        if self.staged {
            self.upload(&table).await?;
        }
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
            (SUMMARY_WRITE_ID.to_owned(), self.id.clone()),
        ]);
        let committed = match self.kind {
            WriteKind::Append => {
                summary.insert(SUMMARY_WRITE.to_owned(), "append".to_owned());
                // The files are under this write's own directory, so no snapshot holds them:
                // the duplicate check, which reads every manifest of the table, is not made.
                let tx = Transaction::new(&table);
                tx.fast_append()
                    .with_check_duplicate(false)
                    .add_data_files(files)
                    .set_snapshot_properties(summary)
                    .apply(tx)?
                    .commit(self.location.catalog.as_ref())
                    .await?
            }
            WriteKind::Overwrite => {
                summary.insert(SUMMARY_WRITE.to_owned(), "overwrite".to_owned());
                self.commit_overwrite(&table, files, summary).await?
            }
            WriteKind::Rewrite(of) => {
                summary.insert(SUMMARY_WRITE.to_owned(), "rewrite".to_owned());
                self.commit_rewrite(table, files, summary, of).await?
            }
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

    /// Every staged file to where the table records it, through the table's `FileIO`.
    async fn upload(&self, table: &Table) -> Result<()> {
        use std::io::Read;
        for path in binding::list_files(&self.dir)? {
            let to = format!("{}/{}", self.prefix, self.relative(&path)?);
            let mut writer = table.file_io().new_output(&to)?.writer().await?;
            let mut file = std::fs::File::open(&path)?;
            loop {
                let mut chunk = vec![0u8; UPLOAD_CHUNK];
                let n = file.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                chunk.truncate(n);
                writer.write(Bytes::from(chunk)).await?;
            }
            writer.close().await?;
        }
        Ok(())
    }

    /// A data file's path under the write's directory, `/`-separated.
    fn relative(&self, path: &Path) -> Result<String> {
        Ok(path
            .strip_prefix(&self.dir)
            .map_err(|_| PeqlError::Invalid(format!("{} is outside the write", path.display())))?
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/"))
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
            let relative = self.relative(&path)?;
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
                write: "overwrite",
            };
            let tx = Transaction::new(table);
            return tx
                .fast_append()
                .add_data_files(files)
                .set_snapshot_properties(summary)
                .apply(tx)?
                .commit(&pinned)
                .await
                .map_err(|e| pinned.refused(e));
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
            let as_base = listing(table, base, &empty, &Totals::default())?;
            let pinned = Pinned {
                inner: catalog,
                table: self.location.table.clone(),
                base: Some(base),
                as_base: as_base.clone(),
                write: "overwrite",
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

    /// Replace the files of snapshot `of` with `files`, which hold its rows, and keep every
    /// data file added to the table since: the commit a rewrite (a compaction) makes, which
    /// a table appended to while it was rewritten still takes.
    ///
    /// The snapshot is produced as an overwrite's is (see [`IcebergWrite::commit_overwrite`]),
    /// over a base whose current snapshot lists, instead of its own manifests, only those
    /// added since `of`: so it holds `files` and every file appended since, and commits only
    /// while the table is still at the snapshot it was produced over. When another writer
    /// moves the table first, what is carried forward is computed again over the table as
    /// that writer left it, as the table's retry properties allow ([`CommitRetry`]).
    ///
    /// Refused ([`PeqlError::Conflict`]) when, since `of`, a snapshot replaced or removed any
    /// of `of`'s files (an overwrite, a delete, another rewrite), added delete files (they
    /// could delete rows of `files` that they no longer reach), or when `of` is no longer an
    /// ancestor of the table's current snapshot.
    async fn commit_rewrite(
        &self,
        table: Table,
        files: Vec<DataFile>,
        summary: HashMap<String, String>,
        of: i64,
    ) -> Result<Table> {
        let mut table = table;
        let md = table.metadata();
        if md.format_version() != FormatVersion::V2 {
            return Err(PeqlError::Invalid(format!(
                "{} is Iceberg format {:?}; peQL rewrites format 2 tables",
                self.location.table,
                md.format_version()
            )));
        }
        let mut rewritten = Rewritten::of(&self.location, &table, of).await?;
        let mut retry = CommitRetry::of(&table)?.start();
        // Read what was appended since `of` before the first attempt, so that no attempt
        // spends the table's retries on reading manifests: an attempt reads only the ones
        // that landed after the one before it.
        table = self.reload(of).await?;
        let now = Self::current(&self.location, &table, of)?;
        rewritten.carried(&self.location, &table, now).await?;
        let mut refused;
        loop {
            table = self.reload(of).await?;
            let now = Self::current(&self.location, &table, of)?;
            let (carried, totals) = rewritten.carried(&self.location, &table, now).await?;
            let md = table.metadata();
            let list = format!(
                "{}/peql-rewrite-base-{}.avro",
                md.metadata_location()?,
                Uuid::new_v4()
            );
            let mut writer = ManifestListWriter::v2(
                table.file_io().new_output(&list)?.writer().await?,
                now,
                table
                    .metadata()
                    .snapshot_by_id(now)
                    .and_then(|s| s.parent_snapshot_id()),
                md.last_sequence_number(),
            );
            writer.add_manifests(carried.into_iter())?;
            writer.close().await?;
            let result = async {
                let as_base = listing(&table, now, &list, &totals)?;
                let pinned = Pinned {
                    inner: self.location.catalog.clone(),
                    table: self.location.table.clone(),
                    base: Some(now),
                    as_base: as_base.clone(),
                    write: "rewrite",
                };
                let tx = Transaction::new(&as_base);
                Ok::<_, PeqlError>(
                    tx.fast_append()
                        .with_check_duplicate(false)
                        .add_data_files(files.clone())
                        .set_snapshot_properties(summary.clone())
                        .apply(tx)?
                        .commit(&pinned)
                        .await,
                )
            }
            .await;
            table.file_io().delete(&list).await?;
            match result? {
                Ok(committed) => return Ok(committed),
                Err(e) if e.kind() == ErrorKind::CatalogCommitConflicts => refused = e,
                Err(e) => return Err(e.into()),
            }
            if !retry.wait().await {
                break;
            }
        }
        Err(PeqlError::Conflict(format!(
            "{} moved under each of {} attempts to commit the rewrite of snapshot {of}; \
             the last refusal: {}",
            self.location.table,
            retry.attempts(),
            refused
        )))
    }

    /// The table as the catalog has it now, while snapshot `of` is rewritten.
    async fn reload(&self, of: i64) -> Result<Table> {
        self.location.load().await?.ok_or_else(|| {
            PeqlError::Invalid(format!(
                "{} was dropped while snapshot {of} was rewritten",
                self.location.table
            ))
        })
    }

    /// The table's current snapshot, while snapshot `of` is rewritten.
    fn current(location: &IcebergLocation, table: &Table, of: i64) -> Result<i64> {
        table.metadata().current_snapshot_id().ok_or_else(|| {
            PeqlError::Conflict(format!(
                "{} has no current snapshot; snapshot {of} was being rewritten",
                location.table
            ))
        })
    }

    fn conflict(&self, now: Option<i64>) -> PeqlError {
        PeqlError::Conflict(format!(
            "{} is at snapshot {now:?}; the overwrite began from {:?}",
            self.location.table, self.base
        ))
    }
}

/// The snapshot a rewrite replaces the files of: its manifests. Manifests are immutable, so
/// while every one of them is still listed by the table's current snapshot, none of the files
/// the rewrite replaces has been replaced, removed or deleted from; that, and no delete files
/// since, is all a rewrite must hold to be committed over appends (Iceberg's rewrite
/// validates the same, and nothing about the files added meanwhile).
struct Rewritten {
    snapshot: i64,
    manifests: HashSet<String>,
    /// The manifests added since the snapshot that had to be read, and what their files add
    /// up to: a manifest is immutable, so it is read once however many attempts the rewrite
    /// makes.
    scanned: HashMap<String, Totals>,
}

/// What a snapshot's data files add up to, as its summary counts them.
#[derive(Clone, Default)]
struct Totals {
    records: u64,
    files: u64,
    bytes: u64,
}

impl Totals {
    fn add(&mut self, other: &Totals) {
        self.records += other.records;
        self.files += other.files;
        self.bytes += other.bytes;
    }
}

impl Rewritten {
    async fn of(location: &IcebergLocation, table: &Table, snapshot: i64) -> Result<Rewritten> {
        let snap = table
            .metadata()
            .snapshot_by_id(snapshot)
            .ok_or_else(|| location.no_snapshot(snapshot))?;
        let list = table.manifest_list_reader(snap).load().await?;
        let manifests = list
            .entries()
            .iter()
            .map(|m| m.manifest_path.clone())
            .collect();
        Ok(Rewritten {
            snapshot,
            manifests,
            scanned: HashMap::new(),
        })
    }

    /// The manifests of `table`'s snapshot `now` that the rewrite keeps, every one added
    /// since the rewritten snapshot, and what their data files add up to; refused when
    /// anything since replaced or removed the rewritten snapshot's files, or could delete
    /// rows of them.
    async fn carried(
        &mut self,
        location: &IcebergLocation,
        table: &Table,
        now: i64,
    ) -> Result<(Vec<ManifestFile>, Totals)> {
        let md = table.metadata();
        let of = self.snapshot;
        // The snapshots since, newest first, as `<id> (<what it did>)`.
        let mut since = Vec::new();
        let mut at = Some(now);
        while at != Some(of) {
            let Some(snap) = at.and_then(|id| md.snapshot_by_id(id)) else {
                return Err(PeqlError::Conflict(format!(
                    "{}: snapshot {of}, being rewritten, is no longer an ancestor of the \
                     current snapshot {now}",
                    location.table
                )));
            };
            let summary = snap.summary();
            let did = summary
                .additional_properties
                .get(SUMMARY_WRITE)
                .map_or_else(|| summary.operation.as_str().to_owned(), String::clone);
            since.push(format!("{} ({did})", snap.snapshot_id()));
            at = snap.parent_snapshot_id();
        }
        let refused = |why: String| {
            PeqlError::Conflict(format!(
                "{}: {why} since snapshot {of}, whose files were being rewritten (snapshots \
                 since: {})",
                location.table,
                since.join(", ")
            ))
        };
        let snap = md
            .snapshot_by_id(now)
            .ok_or_else(|| location.no_snapshot(now))?;
        let list = table.manifest_list_reader(snap).load().await?;
        let listed: HashSet<&str> = list
            .entries()
            .iter()
            .map(|m| m.manifest_path.as_str())
            .collect();
        if let Some(gone) = self.manifests.iter().find(|m| !listed.contains(m.as_str())) {
            return Err(refused(format!(
                "its files were replaced or removed (manifest {gone} is no longer listed)"
            )));
        }
        // A manifest is immutable and its list entry counts its files and rows, so what an
        // attempt must know of the manifests added since `of` it reads from the list, and
        // reads no manifest: the cost of an attempt does not grow with the appends made
        // while the rewrite ran, and a manifest costs a read of the store only when its
        // entry lacks a count.
        let mut carried = Vec::new();
        let mut totals = Totals::default();
        let mut counted = Vec::new();
        for m in list.entries() {
            if self.manifests.contains(&m.manifest_path) {
                continue;
            }
            if m.content != ManifestContentType::Data {
                return Err(refused(format!(
                    "snapshot {} added delete files",
                    m.added_snapshot_id
                )));
            }
            carried.push(m.clone());
            if let Some(seen) = self.scanned.get(&m.manifest_path) {
                totals.add(seen);
                continue;
            }
            match (
                m.added_files_count,
                m.existing_files_count,
                m.added_rows_count,
                m.existing_rows_count,
            ) {
                (Some(af), Some(ef), Some(ar), Some(er)) => {
                    totals.files += u64::from(af) + u64::from(ef);
                    totals.records += ar + er;
                    counted.push(m);
                }
                _ => {
                    let seen = self.scan(table, m).await.map_err(|e| match e {
                        PeqlError::Conflict(why) => refused(why),
                        e => e,
                    })?;
                    totals.add(&seen);
                }
            }
        }
        // The bytes of the files are not in the list. The table's snapshots count them as
        // they go (`total-files-size`), so those since `of` are the difference of the two
        // counts; only when a snapshot does not count them are the manifests read.
        let size = |id: i64| {
            md.snapshot_by_id(id)?
                .summary()
                .additional_properties
                .get("total-files-size")?
                .parse::<u64>()
                .ok()
        };
        match (size(now), size(of)) {
            (Some(n), Some(o)) if n >= o => totals.bytes = n - o,
            _ => {
                for m in counted {
                    let seen = self.scan(table, m).await?;
                    totals.bytes += seen.bytes;
                }
            }
        }
        Ok((carried, totals))
    }

    /// What the data files of manifest `m`, added since the rewritten snapshot, add up to;
    /// read once, whatever the number of attempts.
    async fn scan(&mut self, table: &Table, m: &ManifestFile) -> Result<Totals> {
        if let Some(seen) = self.scanned.get(&m.manifest_path) {
            return Ok(seen.clone());
        }
        let mut of_manifest = Totals::default();
        for e in table.manifest_reader().read(m).await?.entries() {
            if e.is_alive() {
                if e.content_type() != DataContentType::Data {
                    return Err(PeqlError::Conflict(format!(
                        "snapshot {} added delete files",
                        m.added_snapshot_id
                    )));
                }
                of_manifest.records += e.data_file().record_count();
                of_manifest.files += 1;
                of_manifest.bytes += e.data_file().file_size_in_bytes();
            }
        }
        self.scanned
            .insert(m.manifest_path.clone(), of_manifest.clone());
        Ok(of_manifest)
    }
}

/// `table` with snapshot `base` listing the manifests in `manifest_list`, whose data files
/// add up to `totals`: the base an overwrite (no manifests) or a rewrite (those it carries
/// forward) is produced over. The table's metadata is changed only in this copy, through its
/// serialised form; the catalog never sees it.
fn listing(table: &Table, base: i64, manifest_list: &str, totals: &Totals) -> Result<Table> {
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
        for (k, v) in [
            ("total-records", totals.records),
            ("total-files-size", totals.bytes),
            ("total-data-files", totals.files),
            ("total-delete-files", 0),
            ("total-position-deletes", 0),
            ("total-equality-deletes", 0),
        ] {
            summary.insert(k.to_owned(), serde_json::Value::String(v.to_string()));
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

/// The catalog an overwrite or a rewrite commits through: the caller's, except that loading
/// the table gives the write's base, and only while the table's current snapshot is still the
/// one the base was made over. A commit the catalog refuses is retried by the crate after a
/// load, which this refuses; so a refused commit is never rebased onto another snapshot by
/// the crate (a rewrite computes its base again itself).
#[derive(Debug)]
struct Pinned {
    inner: Arc<dyn Catalog>,
    table: TableIdent,
    base: Option<i64>,
    as_base: Table,
    /// `overwrite` or `rewrite`, for the refusal.
    write: &'static str,
}

impl Pinned {
    fn refused(&self, e: iceberg::Error) -> PeqlError {
        if e.kind() == ErrorKind::CatalogCommitConflicts {
            PeqlError::Conflict(format!(
                "{}: the table moved since the {} was made over snapshot {:?}: {e}",
                self.table, self.write, self.base
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
                    "{table} is at snapshot {now:?}; the {} was made over {:?}",
                    self.write, self.base
                ),
            ));
        }
        if current.metadata().uuid() != self.as_base.metadata().uuid() {
            return Err(iceberg::Error::new(
                ErrorKind::CatalogCommitConflicts,
                format!("{table} was replaced since the {} was made", self.write),
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
        // A commit the catalog refused is computed again by the caller (a rewrite) or is a
        // conflict (an overwrite), never retried by the crate: its retry would wait out its
        // backoff only to be refused by `load_table` above.
        self.inner
            .update_table(commit)
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::CatalogCommitConflicts => e.with_retryable(false),
                _ => e,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(retries: usize, min: u64, max: u64, total: u64) -> CommitRetry {
        CommitRetry {
            retries,
            min_wait: Duration::from_millis(min),
            max_wait: Duration::from_millis(max),
            total: Duration::from_millis(total),
        }
    }

    #[test]
    fn the_wait_doubles_from_the_minimum_with_a_random_share_and_stops_at_the_maximum() {
        let p = policy(10, 100, 1_000, 60_000);
        let none: Vec<u128> = (0..6).map(|n| p.wait_for(n, 0.0).as_millis()).collect();
        assert_eq!(none, [100, 200, 400, 800, 1_000, 1_000]);
        let most: Vec<u128> = (0..6).map(|n| p.wait_for(n, 1.0).as_millis()).collect();
        assert_eq!(most, [200, 400, 800, 1_000, 1_000, 1_000]);
        // Two writers that conflicted together do not wait alike.
        assert_ne!(p.wait_for(1, 0.2), p.wait_for(1, 0.7));
        // A shift past the width of the counter is the maximum, not an overflow.
        assert_eq!(p.wait_for(500, 0.5), Duration::from_millis(1_000));
    }

    #[tokio::test]
    async fn retries_are_the_tables_and_end_with_the_total_time() {
        let mut r = policy(2, 1, 5, 60_000).start();
        assert!(r.wait().await);
        assert!(r.wait().await);
        assert!(!r.wait().await, "two retries were allowed");
        assert_eq!(r.attempts(), 3);

        let mut none = policy(0, 1, 5, 60_000).start();
        assert!(!none.wait().await);
        assert_eq!(none.attempts(), 1);

        // The wait would pass the total time: no retry, and no wait.
        let mut late = policy(5, 1_000, 1_000, 10).start();
        let at = Instant::now();
        assert!(!late.wait().await);
        assert!(at.elapsed() < Duration::from_millis(500));
    }
}
