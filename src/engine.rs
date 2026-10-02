//! The engine: register contracts, write under them, validate them, and answer SQL in which
//! every table is a contract. parcel decides what each rule means; the engine binds the
//! caller, finds the data, and runs what parcel compiled.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::catalog::view::ViewTable;
use datafusion::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, SortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use parcel_core::document::{AssertOnFail, GuaranteeOnFail};
use parcel_core::registry::{FunctionEntry, FunctionManifest};
use parcel_core::{ContractDoc, compile_with};
use parcel_runtime::Caller;
use parcel_runtime::bundle::Bundle;
use parcel_runtime::plan::{col_ref, conform, dataset_value, enrich_plan, param_values};
use parcel_runtime::reference::{self, Scope};
use serde::Serialize;
use uuid::Uuid;

use crate::audit::{AuditLog, AuditRecord, JsonlAudit, MemoryAudit, Outcome};
use crate::binding::{
    self, BindingResolver, CONTRACT_HASH_KEY, CONTRACT_NAME_KEY, LocalParquet, Location,
};
use crate::budget::BudgetStore;
use crate::cache::{CacheKey, QueryCache};
use crate::envelope::{Asker, Attestation, Envelope, EnvelopeSigner, Resolution, ScanStats};
use crate::error::{PeqlError, Result};
use crate::functions::FunctionStore;
use crate::gate::{BoundTable, Gate, GateBarrier, GatedQueryPlanner, ensure_gated};
use crate::guard;
use crate::manifest::Manifest;
use crate::shape::SuppressExec;
use crate::store::{ContractStore, DirStore, MemoryStore, PUBLIC, Registered};

/// The validation verdict, with the hash of the data it describes.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Verdict {
    #[serde(flatten)]
    pub verdict: parcel_runtime::plan::Verdict,
    pub data_hash: String,
    /// The table snapshot the data is, for data in a table with snapshots (Iceberg).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
}

impl std::ops::Deref for Verdict {
    type Target = parcel_runtime::plan::Verdict;
    fn deref(&self) -> &Self::Target {
        &self.verdict
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WriteReport {
    pub rows_written: usize,
    pub files: usize,
    pub verdict: Verdict,
    /// The snapshot the write committed, for a table with snapshots (Iceberg).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<SnapshotCommit>,
}

/// The snapshot a write committed to a table with snapshots, and the one it followed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct SnapshotCommit {
    pub snapshot_id: i64,
    /// The table's snapshot before this one; `None` for a table's first.
    pub parent_snapshot_id: Option<i64>,
}

/// Which snapshot of its table each contract a read names is read as of. A contract not
/// named here is read at its table's current snapshot. Only contracts bound to a table with
/// snapshots (Iceberg) can be named.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AsOf(BTreeMap<String, i64>);

impl AsOf {
    /// Every contract at its table's current snapshot.
    pub fn current() -> AsOf {
        AsOf::default()
    }
    /// Read `contract` as of `snapshot_id`.
    pub fn with(mut self, contract: &str, snapshot_id: i64) -> AsOf {
        self.0.insert(contract.to_owned(), snapshot_id);
        self
    }
    pub fn get(&self, contract: &str) -> Option<i64> {
        self.0.get(contract).copied()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    Append,
    Overwrite,
}

pub struct QueryResult {
    /// The schema of the answer, which holds even when there are no batches.
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub envelope: Envelope,
    /// The envelope signed by the engine's [`EnvelopeSigner`], a compact JWS, when it has one.
    pub signature: Option<String>,
}

/// What a query would return and which contracts would govern it, found without running it.
#[derive(Clone, Debug)]
pub struct Checked {
    /// The schema of the answer.
    pub schema: SchemaRef,
    pub contracts: Vec<Resolution>,
}

/// Where a contract's data comes from: its binding's files, or a table bound in their place.
enum Bound {
    Files(Location),
    Table(Arc<dyn TableProvider>),
}

/// A write in progress under one contract: see [`Engine::begin_write`].
pub struct Writing {
    reg: Arc<Registered>,
    location: Location,
    rows: AtomicUsize,
    /// The in-memory bytes of the parts written so far, for the width of a row.
    bytes: AtomicUsize,
    /// An overwrite whose old files are still there.
    overwrite: tokio::sync::Mutex<bool>,
    /// What one part may hold while it is written, when the writer bounds it.
    memory: Option<usize>,
    /// A write to an Iceberg table, which [`Engine::finish_write`] commits as one snapshot.
    #[cfg(feature = "iceberg")]
    iceberg: Option<crate::iceberg_table::IcebergWrite>,
}

impl Writing {
    /// The contract written under.
    pub fn contract(&self) -> &str {
        self.reg.name()
    }
    /// Rows written so far.
    pub fn rows(&self) -> usize {
        self.rows.load(Ordering::SeqCst)
    }

    /// Bound what writing one part holds to about `bytes`, beside the part itself, and what
    /// [`Engine::finish_write`]'s validation holds to the same: an out-of-core writer (Moruna's
    /// `PeqlSink`) gives each part in flight a share of its memory budget. The part's writers
    /// and the sort of a clustered layout reserve from a pool of `bytes`, and the sort spills
    /// past what the writers leave it; each file's row groups are flushed at an eighth of it and
    /// its output buffered in another eighth, so a part partitioned into a few files is written
    /// inside it; bloom filters are sized for the rows a part has rather than for a million; the
    /// part is written as one stream. A part whose open files would need more than `bytes` is
    /// refused with DataFusion's `ResourcesExhausted` rather than held. Without it a part is
    /// written in row groups of up to a million rows and sorted in memory.
    pub fn with_memory(mut self, bytes: usize) -> Writing {
        self.memory = Some(bytes.max(MIN_WRITE_MEMORY));
        self
    }

    /// The bytes a row of the parts written so far takes in memory, at least one.
    fn row_bytes(&self) -> usize {
        let rows = self.rows.load(Ordering::SeqCst).max(1);
        (self.bytes.load(Ordering::SeqCst) / rows).max(1)
    }
}

/// The least [`Writing::with_memory`] accepts: a row group of a megabyte.
const MIN_WRITE_MEMORY: usize = 4 << 20;

pub struct Engine {
    store: Arc<dyn ContractStore>,
    functions: Arc<FunctionStore>,
    bindings: Arc<dyn BindingResolver>,
    budgets: Arc<BudgetStore>,
    audit: Arc<dyn AuditLog>,
    cache: Option<Arc<QueryCache>>,
    /// Tables bound in place of a contract's files (embedding, tests).
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    /// Manifests of contracts bound to tables, which have nowhere to write one.
    table_manifests: RwLock<HashMap<String, Manifest>>,
    /// Documents a contract may inherit from that are not registered themselves.
    documents: RwLock<BTreeMap<String, ContractDoc>>,
    use_stored: RwLock<bool>,
    /// Manifests of contracts whose files are in an object store, as last read or written.
    object_manifests: RwLock<HashMap<String, Manifest>>,
    /// The facts of contracts bound to Iceberg tables for their table's current snapshot, as
    /// last read or written.
    #[cfg(feature = "iceberg")]
    iceberg_manifests: RwLock<HashMap<String, Manifest>>,
    signer: Option<Arc<dyn EnvelopeSigner>>,
    #[cfg(feature = "flight")]
    spool_root: PathBuf,
}

impl Engine {
    /// A workspace on disk: contracts, functions, budgets and the audit log under
    /// `<root>/_peql/`; relative bindings resolve under `root`.
    pub fn open(root: impl AsRef<Path>) -> Result<Engine> {
        let root = root.as_ref();
        std::fs::create_dir_all(root.join("_peql"))?;
        Ok(Engine {
            #[cfg(feature = "flight")]
            spool_root: root.join("_peql/flight"),
            store: Arc::new(DirStore::open(root)?),
            functions: Arc::new(FunctionStore::open(root)?),
            bindings: Arc::new(LocalParquet {
                base: root.to_path_buf(),
            }),
            budgets: Arc::new(BudgetStore::open(root.join("_peql").join("budgets.json"))?),
            audit: Arc::new(JsonlAudit::new(root.join("_peql").join("audit.jsonl"))),
            cache: None,
            tables: RwLock::default(),
            table_manifests: RwLock::default(),
            documents: RwLock::default(),
            use_stored: RwLock::new(true),
            object_manifests: RwLock::default(),
            #[cfg(feature = "iceberg")]
            iceberg_manifests: RwLock::default(),
            signer: None,
        })
    }

    /// Everything in memory; relative bindings resolve under `base`.
    pub fn in_memory(base: impl Into<PathBuf>) -> Engine {
        let base = base.into();
        Engine {
            #[cfg(feature = "flight")]
            spool_root: base.join("_peql/flight"),
            store: Arc::new(MemoryStore::default()),
            functions: Arc::new(FunctionStore::in_memory()),
            bindings: Arc::new(LocalParquet { base }),
            budgets: Arc::new(BudgetStore::in_memory()),
            audit: Arc::new(MemoryAudit::default()),
            cache: None,
            tables: RwLock::default(),
            table_manifests: RwLock::default(),
            documents: RwLock::default(),
            use_stored: RwLock::new(true),
            object_manifests: RwLock::default(),
            #[cfg(feature = "iceberg")]
            iceberg_manifests: RwLock::default(),
            signer: None,
        }
    }

    pub fn with_store(mut self, store: Arc<dyn ContractStore>) -> Engine {
        self.store = store;
        self
    }
    pub fn with_bindings(mut self, bindings: Arc<dyn BindingResolver>) -> Engine {
        self.bindings = bindings;
        self
    }
    pub fn with_budgets(mut self, budgets: Arc<BudgetStore>) -> Engine {
        self.budgets = budgets;
        self
    }
    pub fn with_audit(mut self, audit: Arc<dyn AuditLog>) -> Engine {
        self.audit = audit;
        self
    }

    /// Sign every answer's envelope. A query whose envelope cannot be signed fails: with a
    /// signer configured, no answer leaves without its certificate.
    pub fn with_signer(mut self, signer: Arc<dyn EnvelopeSigner>) -> Engine {
        self.signer = Some(signer);
        self
    }

    /// Cache answers; see [`crate::cache`] for what a cached answer depends on.
    pub fn with_cache(mut self, cache: Arc<QueryCache>) -> Engine {
        self.cache = Some(cache);
        self
    }

    pub fn cache(&self) -> Option<&QueryCache> {
        self.cache.as_deref()
    }

    pub fn budgets(&self) -> &BudgetStore {
        &self.budgets
    }
    pub fn functions(&self) -> &FunctionStore {
        &self.functions
    }
    pub fn store(&self) -> &dyn ContractStore {
        self.store.as_ref()
    }

    /// Whether views may read stored flags and derived columns (default) or must evaluate every
    /// rule live. Both give the same rows; stored is faster.
    pub fn set_use_stored(&self, on: bool) {
        *self.use_stored.write().expect("lock") = on;
    }

    // ── registration ─────────────────────────────────────────────────────────

    /// Make a document known without registering it, so contracts can inherit from it.
    pub fn add_document(&self, doc: ContractDoc) {
        self.documents
            .write()
            .expect("lock")
            .insert(doc.contract.clone(), doc);
    }

    fn document(&self, name: &str) -> Option<ContractDoc> {
        self.store
            .current(name)
            .map(|r| r.doc.clone())
            .or_else(|| self.documents.read().expect("lock").get(name).cloned())
    }

    /// Compile a contract (YAML or JSON) against the schema of the data it binds, and store it.
    pub fn register_contract(&self, source: &str, schema: &Schema) -> Result<Arc<Registered>> {
        let doc = ContractDoc::parse(source).map_err(|d| PeqlError::Compile(vec![d]))?;
        let lookup = |n: &str| self.document(n);
        let owner = parcel_core::inherit::resolve(&doc, &lookup)
            .map_err(PeqlError::Compile)?
            .doc
            .owner;
        let registry = self.functions.registry_for(owner.as_deref());
        let compilation =
            compile_with(&doc, schema, &registry, &lookup).map_err(PeqlError::Compile)?;
        let mut ancestors = Vec::new();
        let mut next = doc.inherits.clone();
        while let Some(p) = next {
            let parent = self
                .document(&p)
                .ok_or_else(|| PeqlError::UnknownContract(p.clone()))?;
            next = parent.inherits.clone();
            ancestors.push(parent);
        }
        let functions = self.functions.modules_for(&compilation.contract.functions);
        let reg = Arc::new(Registered {
            doc,
            ancestors,
            schema: schema.clone(),
            functions,
            compilation,
        });
        self.store.put(reg.clone())?;
        Ok(reg)
    }

    /// Register what `parcel compile -o` produced, after recompiling it to the same hash.
    pub fn register_bundle(&self, bundle: &Bundle) -> Result<Arc<Registered>> {
        self.functions.adopt(&bundle.functions)?;
        for a in &bundle.ancestors {
            self.add_document(a.clone());
        }
        let reg = Arc::new(Registered::from_bundle(bundle)?);
        self.store.put(reg.clone())?;
        Ok(reg)
    }

    /// Verify, load and store a tenant's WebAssembly function; contracts `owner` owns may call it.
    pub fn register_function(
        &self,
        module: &[u8],
        manifest: &FunctionManifest,
        owner: &str,
    ) -> Result<FunctionEntry> {
        self.functions.register(module, manifest, owner)
    }

    /// Share a contract with a tenant, or with everyone ([`PUBLIC`]).
    pub fn publish(&self, name: &str, audience: &str) -> Result<()> {
        self.get(name)?;
        self.store.publish(name, audience)
    }

    pub fn unpublish(&self, name: &str, audience: &str) -> Result<()> {
        self.store.unpublish(name, audience)
    }

    /// The current version of a contract (operator view: no visibility check).
    pub fn get(&self, name: &str) -> Result<Arc<Registered>> {
        self.store
            .current(name)
            .ok_or_else(|| PeqlError::UnknownContract(name.to_owned()))
    }

    pub fn contracts(&self) -> Vec<Arc<Registered>> {
        self.store.list()
    }

    /// A contract as a caller may see it: invisible and absent are the same error.
    fn visible(&self, name: &str, caller: &Caller) -> Result<Arc<Registered>> {
        let r = self
            .store
            .current(name)
            .ok_or_else(|| PeqlError::UnknownContract(name.to_owned()))?;
        let ok = match r.owner() {
            None => true,
            Some(owner) if owner == caller.tenant => true,
            Some(_) => {
                let a = self.store.audiences(name);
                a.contains(PUBLIC) || a.contains(&caller.tenant)
            }
        };
        if ok {
            Ok(r)
        } else {
            Err(PeqlError::UnknownContract(name.to_owned()))
        }
    }

    // ── bindings and manifests ──────────────────────────────────────────────

    /// Serve a contract from a table instead of its binding's files. Its manifest is computed
    /// now, by validating the table.
    pub async fn bind_table(&self, name: &str, table: Arc<dyn TableProvider>) -> Result<Verdict> {
        let reg = self.get(name)?;
        self.tables
            .write()
            .expect("lock")
            .insert(name.to_owned(), table);
        let verdict = self.validate(name).await?;
        let cc = &reg.compilation.contract;
        self.table_manifests.write().expect("lock").insert(
            name.to_owned(),
            Manifest {
                contract: cc.name.clone(),
                contract_hash: cc.contract_hash.clone(),
                compilation_hash: cc.compilation_hash.clone(),
                written_at: Utc::now(),
                row_count: verdict.verdict.row_count,
                valid: verdict.verdict.valid,
                breached: verdict.verdict.breached.clone(),
                stats: verdict.verdict.stats.clone(),
                data_hash: verdict.data_hash.clone(),
                row_schema: parcel_runtime::bundle::schema_to_defs(&cc.row_schema),
                files: Vec::new(),
                snapshot_id: None,
            },
        );
        Ok(verdict)
    }

    /// Serve a contract from record batches held in memory.
    pub async fn bind_batches(&self, name: &str, batches: Vec<RecordBatch>) -> Result<Verdict> {
        let reg = self.get(name)?;
        let schema = reg.compilation.contract.row_schema.clone();
        let batches = batches
            .into_iter()
            .map(|b| conform(b, &schema))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let table = Arc::new(MemTable::try_new(schema, vec![batches])?);
        self.bind_table(name, table).await
    }

    fn bound(&self, reg: &Registered) -> Result<Bound> {
        if let Some(t) = self.tables.read().expect("lock").get(reg.name()) {
            return Ok(Bound::Table(t.clone()));
        }
        self.bindings
            .location(&reg.compilation.contract)
            .map(Bound::Files)
            .ok_or_else(|| {
                PeqlError::Invalid(format!("`{}` has no binding peQL can read", reg.name()))
            })
    }

    pub fn manifest(&self, name: &str) -> Result<Option<Manifest>> {
        let reg = self.get(name)?;
        match self.bound(&reg)? {
            Bound::Table(_) => Ok(self
                .table_manifests
                .read()
                .expect("lock")
                .get(name)
                .cloned()),
            Bound::Files(Location::Local(root)) => Ok(Manifest::load(&root, name)?),
            Bound::Files(Location::Object(_)) => Ok(self
                .object_manifests
                .read()
                .expect("lock")
                .get(name)
                .cloned()),
            #[cfg(feature = "iceberg")]
            Bound::Files(Location::Iceberg(_)) => Ok(self
                .iceberg_manifests
                .read()
                .expect("lock")
                .get(name)
                .cloned()),
        }
    }

    /// What a contract bound to a table with snapshots (Iceberg) knows about one snapshot's
    /// data, as recorded with that snapshot; `None` when nothing has been recorded for it.
    pub async fn manifest_as_of(&self, name: &str, snapshot_id: i64) -> Result<Option<Manifest>> {
        #[cfg(not(feature = "iceberg"))]
        let _ = snapshot_id;
        let reg = self.get(name)?;
        match self.bound(&reg)? {
            #[cfg(feature = "iceberg")]
            Bound::Files(Location::Iceberg(t)) => {
                let table = t.load().await?.ok_or_else(|| PeqlError::NotWritten {
                    contract: name.to_owned(),
                })?;
                if table.metadata().snapshot_by_id(snapshot_id).is_none() {
                    return Err(PeqlError::Invalid(format!(
                        "{} has no snapshot {snapshot_id}",
                        t.table
                    )));
                }
                t.load_facts(&table, snapshot_id, name).await
            }
            _ => Err(no_snapshots(name)),
        }
    }

    /// Where a contract's files are, as its binding resolves them; `None` for a contract served
    /// from a table. Two contracts bound to the same files have one location whatever their
    /// names, so this is what an executor compares to tell whether a write lands where a read
    /// reads.
    pub fn location(&self, name: &str) -> Result<Option<Location>> {
        let reg = self.get(name)?;
        match self.bound(&reg)? {
            Bound::Table(_) => Ok(None),
            Bound::Files(location) => Ok(Some(location)),
        }
    }

    /// A contract registered over files written elsewhere has no manifest yet: make one. For
    /// files in an object store, read the manifest again, since another engine may write there.
    /// For an Iceberg table, read the facts of its current snapshot, or record them.
    pub async fn ensure_manifest(&self, name: &str) -> Result<()> {
        let reg = self.get(name)?;
        match self.bound(&reg)? {
            Bound::Table(_) => Ok(()),
            Bound::Files(Location::Local(root)) => {
                if Manifest::load(&root, name)?.is_some() || binding::list_files(&root)?.is_empty()
                {
                    return Ok(());
                }
                self.refresh(name, Utc::now()).await?;
                Ok(())
            }
            Bound::Files(Location::Object(o)) => {
                if let Some(m) = o.load_manifest(name).await? {
                    self.object_manifests
                        .write()
                        .expect("lock")
                        .insert(name.to_owned(), m);
                } else if !o.list_files().await?.is_empty() {
                    self.refresh(name, Utc::now()).await?;
                }
                Ok(())
            }
            #[cfg(feature = "iceberg")]
            Bound::Files(Location::Iceberg(t)) => {
                let current = match t.load().await? {
                    Some(table) => table.metadata().current_snapshot_id().map(|s| (table, s)),
                    None => None,
                };
                let Some((table, snapshot)) = current else {
                    self.iceberg_manifests.write().expect("lock").remove(name);
                    return Ok(());
                };
                match t.load_facts(&table, snapshot, name).await? {
                    Some(m) => {
                        self.iceberg_manifests
                            .write()
                            .expect("lock")
                            .insert(name.to_owned(), m);
                    }
                    None => {
                        self.refresh_in(name, Utc::now(), None, Some(snapshot))
                            .await?;
                    }
                }
                Ok(())
            }
        }
    }

    /// The facts of a contract bound to an Iceberg table for one snapshot, recorded now if
    /// they were not.
    async fn ensure_manifest_as_of(&self, name: &str, snapshot_id: i64) -> Result<Manifest> {
        if let Some(m) = self.manifest_as_of(name, snapshot_id).await? {
            return Ok(m);
        }
        Ok(self
            .refresh_in(name, Utc::now(), None, Some(snapshot_id))
            .await?
            .1)
    }

    // ── write and validate ──────────────────────────────────────────────────

    /// Write batches under a contract (parcel design 10): enrich, compute flags and derived
    /// columns, cluster and partition, stamp the contract hash, validate, write the manifest.
    /// One [`Engine::begin_write`], one [`Engine::write_part`], one [`Engine::finish_write`].
    pub async fn write(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
    ) -> Result<WriteReport> {
        let w = self.begin_write(name, mode).await?;
        self.write_part(&w, batches).await?;
        self.finish_write(w).await
    }

    /// Start a write under a contract that arrives in parts, as an out-of-core writer (Moruna's
    /// `PeqlSink`) delivers it. With [`WriteMode::Overwrite`] the contract's files are removed
    /// before the first part lands. The data is written by [`Engine::write_part`], as many times as there are parts,
    /// and the manifest is refreshed once, by [`Engine::finish_write`]; until then the
    /// manifest describes the data as it was.
    ///
    /// A contract bound to an Iceberg table is written to the table as it is when the write
    /// begins: the table is created if the catalog has none, and its schema made the columns
    /// the contract writes. Nothing is removed: [`Engine::finish_write`] commits the parts'
    /// files as one snapshot, which for an overwrite replaces the table's files and commits
    /// only over the snapshot current here.
    pub async fn begin_write(&self, name: &str, mode: WriteMode) -> Result<Writing> {
        let reg = self.get(name)?;
        let Bound::Files(location) = self.bound(&reg)? else {
            return Err(PeqlError::Invalid(format!(
                "`{name}` is bound to a table; only file bindings are written"
            )));
        };
        if location.is_single_file() {
            return Err(PeqlError::Invalid(format!(
                "`{name}` binds a single file; bind a directory to write to it"
            )));
        }
        if let Location::Local(root) = &location {
            std::fs::create_dir_all(root)?;
        }
        #[cfg(feature = "iceberg")]
        let iceberg = match &location {
            Location::Iceberg(t) => Some(
                t.begin_write(&reg.compilation.contract, mode == WriteMode::Overwrite)
                    .await?,
            ),
            _ => None,
        };
        #[cfg(feature = "iceberg")]
        let clears = iceberg.is_none() && mode == WriteMode::Overwrite;
        #[cfg(not(feature = "iceberg"))]
        let clears = mode == WriteMode::Overwrite;
        Ok(Writing {
            reg,
            location,
            rows: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            overwrite: tokio::sync::Mutex::new(clears),
            memory: None,
            #[cfg(feature = "iceberg")]
            iceberg,
        })
    }

    /// An overwrite removes the contract's files once, before the first part lands (or at the
    /// finish of a write with no parts), and only once that part has been planned, so a part
    /// that does not conform leaves the data as it was.
    async fn clear_for_overwrite(&self, w: &Writing) -> Result<()> {
        let mut pending = w.overwrite.lock().await;
        if *pending {
            match &w.location {
                Location::Local(root) => {
                    for f in binding::list_files(root)? {
                        std::fs::remove_file(f)?;
                    }
                }
                Location::Object(o) => o.remove_files().await?,
                // An Iceberg overwrite removes no file; its snapshot replaces the old one.
                #[cfg(feature = "iceberg")]
                Location::Iceberg(_) => {}
            }
            *pending = false;
        }
        Ok(())
    }

    /// Write one part of a write begun by [`Engine::begin_write`]: conform it to the row
    /// schema, enrich, compute flags and derived columns, cluster and partition, and write
    /// Parquet files stamped with the contract hash. Parts may be written concurrently; each
    /// lands in files of its own. Returns the rows written.
    pub async fn write_part(&self, w: &Writing, batches: Vec<RecordBatch>) -> Result<usize> {
        let c = &w.reg.compilation;
        let cc = &c.contract;
        let batches = batches
            .into_iter()
            .map(|b| conform(b, &cc.row_schema))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();

        // An Iceberg table's files hold every column, partition columns among them.
        #[cfg(feature = "iceberg")]
        let keep_partitions = w.iceberg.is_some();
        #[cfg(not(feature = "iceberg"))]
        let keep_partitions = false;
        let ctx = match w.memory {
            Some(memory) => self.bounded_session(memory, 1, None, keep_partitions)?,
            None => self.session_with(
                self.config_for(None).set_bool(
                    "datafusion.execution.keep_partition_by_columns",
                    keep_partitions,
                ),
                None,
            ),
        };
        let mem = MemTable::try_new(cc.row_schema.clone(), vec![batches])?;
        let scan = LogicalPlanBuilder::scan("incoming", provider_as_source(Arc::new(mem)), None)?
            .build()?;
        // Stage 1: enrich. Stage 2: flags and derived columns, which may read what stage 1 produced.
        let plan = enrich_plan(scan, cc)?;
        let mut select: Vec<Expr> = cc
            .scan_schema
            .fields()
            .iter()
            .map(|f| col_ref(f.name()))
            .collect();
        for flag in &c.write.flags {
            select.push(flag.expr.clone().alias(&flag.column));
        }
        for d in &c.write.derived {
            select.push(d.expr.clone().alias(&d.column));
        }
        let plan = LogicalPlanBuilder::from(plan).project(select)?.build()?;
        // An Iceberg table reads its files' columns by field id: each column is written as
        // the table's schema has it, with its id.
        #[cfg(feature = "iceberg")]
        let plan = match &w.iceberg {
            Some(iw) => {
                let columns = iw.schema().fields().iter().map(|f| {
                    let metadata: BTreeMap<String, String> = f
                        .metadata()
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    datafusion::logical_expr::cast(col_ref(f.name()), f.data_type().clone())
                        .alias_with_metadata(f.name(), Some(metadata.into()))
                });
                LogicalPlanBuilder::from(plan).project(columns)?.build()?
            }
            None => plan,
        };
        let df = ctx
            .execute_logical_plan(parcel_core::compile::resolve(plan)?)
            .await?;
        self.clear_for_overwrite(w).await?;

        let layout = &c.write.layout;
        let sort: Vec<SortExpr> = layout
            .cluster_by
            .iter()
            .map(|c| col_ref(c).sort(false, false))
            .collect();
        let mut options =
            DataFrameWriteOptions::new().with_partition_by(layout.partition_by.clone());
        if !sort.is_empty() {
            options = options.with_sort_by(sort);
        }
        let mut parquet = TableParquetOptions::default();
        parquet
            .key_value_metadata
            .insert(CONTRACT_HASH_KEY.into(), Some(cc.contract_hash.clone()));
        parquet
            .key_value_metadata
            .insert(CONTRACT_NAME_KEY.into(), Some(cc.name.clone()));
        parquet.global.statistics_enabled = Some("page".into());
        // One stream per file: the parallel serialiser keeps every file's buffers until the
        // process ends, a part's worth per file written (measured, DataFusion 55.1).
        parquet.global.allow_single_file_parallelism = false;
        if let Some(memory) = w.memory {
            parquet.global.max_row_group_bytes = Some(
                datafusion::config::MaxRowGroupBytes::try_new(memory / 8)
                    .map_err(|e| PeqlError::Invalid(e.to_string()))?,
            );
        }
        for c in &layout.bloom {
            let options = parquet
                .column_specific_options
                .entry(c.clone())
                .or_default();
            options.bloom_filter_enabled = Some(true);
            if w.memory.is_some() {
                options.bloom_filter_ndv = Some(rows.max(1) as u64);
            }
        }
        let url = match &w.location {
            Location::Local(root) => binding::local_url(root, true)?,
            Location::Object(o) => o.url(true),
            #[cfg(feature = "iceberg")]
            Location::Iceberg(t) => w
                .iceberg
                .as_ref()
                .ok_or_else(|| PeqlError::Invalid(format!("{} was not begun as a write", t.table)))?
                .url()?,
        };
        df.write_parquet(&url, options, Some(parquet)).await?;
        w.rows.fetch_add(rows, Ordering::SeqCst);
        w.bytes.fetch_add(bytes, Ordering::SeqCst);
        Ok(rows)
    }

    /// Finish a write begun by [`Engine::begin_write`]: the data changed, so refresh every
    /// contract bound to it, the writer's last, which validates it and writes its manifest.
    ///
    /// For an Iceberg table, the parts' files are committed first, as one snapshot, and every
    /// contract bound to the table records its facts for that snapshot; the report names it.
    pub async fn finish_write(&self, w: Writing) -> Result<WriteReport> {
        self.clear_for_overwrite(&w).await?;
        let name = w.reg.name();
        #[cfg(feature = "iceberg")]
        let committed = match &w.iceberg {
            Some(iw) => Some(iw.commit(&w.reg.compilation.contract).await?.1),
            None => None,
        };
        #[cfg(not(feature = "iceberg"))]
        let committed: Option<SnapshotCommit> = None;
        let at = committed.map(|c| c.snapshot_id);
        let written_at = Utc::now();
        let key = w.location.key()?;
        for other in self.store.list() {
            if other.name() == name {
                continue;
            }
            if let Ok(Bound::Files(r)) = self.bound(&other)
                && r.key().ok().as_ref() == Some(&key)
            {
                self.refresh_in(other.name(), written_at, Some(&w), at)
                    .await?;
            }
        }
        let (verdict, manifest) = self.refresh_in(name, written_at, Some(&w), at).await?;
        Ok(WriteReport {
            rows_written: w.rows.load(Ordering::SeqCst),
            files: manifest.files.len(),
            verdict,
            snapshot: committed,
        })
    }

    /// Validate a contract over its files and save its manifest.
    async fn refresh(&self, name: &str, written_at: DateTime<Utc>) -> Result<(Verdict, Manifest)> {
        self.refresh_in(name, written_at, None, None).await
    }

    /// [`Engine::refresh`], validating inside a write's memory when it has one. For an Iceberg
    /// table, of snapshot `at` (the current one when `None`), whose facts are recorded with it.
    async fn refresh_in(
        &self,
        name: &str,
        written_at: DateTime<Utc>,
        w: Option<&Writing>,
        at: Option<i64>,
    ) -> Result<(Verdict, Manifest)> {
        let reg = self.get(name)?;
        let cc = &reg.compilation.contract;
        let Bound::Files(location) = self.bound(&reg)? else {
            return Err(PeqlError::Invalid(format!("`{name}` has no files")));
        };
        #[cfg(feature = "iceberg")]
        let at = match &location {
            Location::Iceberg(t) => Some(match at {
                Some(s) => s,
                None => t
                    .load()
                    .await?
                    .and_then(|table| table.metadata().current_snapshot_id())
                    .ok_or_else(|| PeqlError::NotWritten {
                        contract: name.to_owned(),
                    })?,
            }),
            _ => at,
        };
        let plan = reg.compilation.validation.plan.clone();
        let verdict = match w.and_then(|w| w.memory.map(|m| (m, w.row_bytes()))) {
            Some((memory, row_bytes)) => {
                // A scan holds a few batches: each is an eighth of the write's memory.
                let batch = (memory / 8 / row_bytes).clamp(64, 8192);
                let ctx = self.bounded_session(memory, 1, Some(batch), false)?;
                self.validate_in(&ctx, name, plan, at).await?
            }
            None => self.validate_in(&self.session(), name, plan, at).await?,
        };
        let flag_columns: Vec<String> = cc.flags.iter().map(|f| f.column.clone()).collect();
        let files = match &location {
            Location::Local(root) => binding::list_files(root)?
                .iter()
                .map(|f| binding::file_entry(root, f, &flag_columns))
                .collect::<Result<Vec<_>>>()?,
            Location::Object(o) => {
                o.file_entries(&o.list_files().await?, &flag_columns)
                    .await?
            }
            #[cfg(feature = "iceberg")]
            Location::Iceberg(t) => {
                let (table, snapshot) = self.iceberg_snapshot(t, name, at).await?;
                let files = t.files(&table, snapshot).await?;
                t.file_entries(&table, &files, &flag_columns).await?
            }
        };
        let manifest = Manifest {
            contract: cc.name.clone(),
            contract_hash: cc.contract_hash.clone(),
            compilation_hash: cc.compilation_hash.clone(),
            written_at,
            row_count: verdict.verdict.row_count,
            valid: verdict.verdict.valid,
            breached: verdict.verdict.breached.clone(),
            stats: verdict.verdict.stats.clone(),
            data_hash: verdict.data_hash.clone(),
            row_schema: parcel_runtime::bundle::schema_to_defs(&cc.row_schema),
            files,
            snapshot_id: verdict.snapshot_id,
        };
        match &location {
            Location::Local(root) => manifest.save(root)?,
            Location::Object(o) => {
                o.save_manifest(&manifest).await?;
                self.object_manifests
                    .write()
                    .expect("lock")
                    .insert(name.to_owned(), manifest.clone());
            }
            #[cfg(feature = "iceberg")]
            Location::Iceberg(t) => {
                let (table, snapshot) = self.iceberg_snapshot(t, name, at).await?;
                t.save_facts(&table, &manifest).await?;
                if table.metadata().current_snapshot_id() == Some(snapshot) {
                    self.iceberg_manifests
                        .write()
                        .expect("lock")
                        .insert(name.to_owned(), manifest.clone());
                }
            }
        }
        Ok((verdict, manifest))
    }

    /// Run the contract's validation plan over its data (peQL design 4.9a). For an Iceberg
    /// table, over its current snapshot, which the verdict names.
    pub async fn validate(&self, name: &str) -> Result<Verdict> {
        let plan = self.get(name)?.compilation.validation.plan.clone();
        self.validate_with(name, plan).await
    }

    /// Run a validation plan obtained elsewhere (e.g. decoded from a bundle) over the
    /// contract's data. This is what a certificate verifier does: same plan, same data.
    pub async fn validate_with(&self, name: &str, plan: LogicalPlan) -> Result<Verdict> {
        self.validate_in(&self.session(), name, plan, None).await
    }

    /// [`Engine::validate_with`] over one snapshot of a contract's Iceberg table: what a
    /// verifier runs for a certificate that names the snapshot.
    pub async fn validate_with_as_of(
        &self,
        name: &str,
        plan: LogicalPlan,
        snapshot_id: i64,
    ) -> Result<Verdict> {
        self.validate_in(&self.session(), name, plan, Some(snapshot_id))
            .await
    }

    /// [`Engine::validate_with`] in a given session; for an Iceberg table, of snapshot `at`
    /// (the current one when `None`).
    async fn validate_in(
        &self,
        session: &SessionContext,
        name: &str,
        plan: LogicalPlan,
        at: Option<i64>,
    ) -> Result<Verdict> {
        let reg = self.get(name)?;
        let bound = self.bound(&reg)?;
        #[cfg(feature = "iceberg")]
        if let Bound::Files(Location::Iceberg(t)) = &bound {
            let (table, snapshot) = self.iceberg_snapshot(t, name, at).await?;
            let provider = t
                .provider(&table, &reg.compilation.contract, false, snapshot)
                .await?;
            let verdict =
                parcel_runtime::plan::validate_in(session, &reg.compilation, plan, provider)
                    .await?;
            let files = t.files(&table, snapshot).await?;
            return Ok(Verdict {
                verdict,
                data_hash: t.data_hash(&table, &files).await?,
                snapshot_id: Some(snapshot),
            });
        }
        if at.is_some() {
            return Err(no_snapshots(name));
        }
        let provider = match &bound {
            Bound::Table(t) => t.clone(),
            Bound::Files(_) => {
                self.bindings
                    .provider(&reg.compilation.contract, false)
                    .await?
            }
        };
        let verdict =
            parcel_runtime::plan::validate_in(session, &reg.compilation, plan, provider).await?;
        let data_hash = match bound {
            Bound::Files(Location::Local(root)) => {
                binding::data_hash(&root, &binding::list_files(&root)?)?
            }
            Bound::Files(Location::Object(o)) => o.data_hash(&o.list_files().await?).await?,
            #[cfg(feature = "iceberg")]
            Bound::Files(Location::Iceberg(_)) => unreachable!("validated above"),
            Bound::Table(t) => {
                let batches = self.session().read_table(t)?.collect().await?;
                parcel_core::hash::sha256_hex(&crate::envelope::ipc_bytes(&batches))
            }
        };
        Ok(Verdict {
            verdict,
            data_hash,
            snapshot_id: None,
        })
    }

    /// A contract's Iceberg table and the snapshot `at` names (the current one when `None`).
    #[cfg(feature = "iceberg")]
    async fn iceberg_snapshot(
        &self,
        t: &crate::iceberg_table::IcebergLocation,
        name: &str,
        at: Option<i64>,
    ) -> Result<(iceberg::table::Table, i64)> {
        let not_written = || PeqlError::NotWritten {
            contract: name.to_owned(),
        };
        let table = t.load().await?.ok_or_else(not_written)?;
        let snapshot = match at {
            Some(s) => {
                if table.metadata().snapshot_by_id(s).is_none() {
                    return Err(PeqlError::Invalid(format!(
                        "{} has no snapshot {s}",
                        t.table
                    )));
                }
                s
            }
            None => table
                .metadata()
                .current_snapshot_id()
                .ok_or_else(not_written)?,
        };
        Ok((table, snapshot))
    }

    // ── resolve, view, describe ─────────────────────────────────────────────

    /// Decide, check guarantees and choose shapes for one caller (peQL design 4.3).
    pub fn resolve(&self, name: &str, caller: &Caller) -> Result<(Resolution, Manifest)> {
        self.resolve_with(name, caller, || self.manifest(name))
    }

    /// [`Engine::resolve`] over the manifest `manifest` gives, read only once the caller has
    /// been admitted.
    fn resolve_with(
        &self,
        name: &str,
        caller: &Caller,
        manifest: impl FnOnce() -> Result<Option<Manifest>>,
    ) -> Result<(Resolution, Manifest)> {
        let reg = self.visible(name, caller)?;
        let c = &reg.compilation;
        let cc = &c.contract;
        parcel_runtime::verify_pins(&cc.functions)?;
        if let Some(rule) = parcel_runtime::plan::refusal(cc, caller)? {
            return Err(PeqlError::Denied {
                contract: cc.name.clone(),
                rule,
            });
        }
        let manifest = manifest()?.ok_or_else(|| PeqlError::NotWritten {
            contract: cc.name.clone(),
        })?;
        if !manifest.valid {
            return Err(PeqlError::NotServable {
                contract: cc.name.clone(),
                breached: manifest.breached.clone(),
            });
        }
        let ctx = reference::context(&Scope {
            ctx: Some(reference::ctx_value_typed(caller, &cc.ctx_other)),
            dataset: Some(dataset_value(
                c,
                &manifest.stats,
                manifest.written_at,
                &manifest.contract_hash,
            )),
            row: None,
            pins: cc.functions.clone(),
        });
        let mut annotations = Vec::new();
        for g in &cc.guarantees {
            if !reference::eval_bool(&g.cel, &ctx).map_err(PeqlError::Invalid)? {
                match g.on_fail {
                    GuaranteeOnFail::Deny => {
                        return Err(PeqlError::GuaranteeFailed {
                            contract: cc.name.clone(),
                            rule: g.id.clone(),
                        });
                    }
                    GuaranteeOnFail::Annotate => annotations.push(g.id.clone()),
                }
            }
        }
        let shapes = parcel_runtime::plan::active_shapes(cc, caller)?
            .into_iter()
            .map(|s| s.id.clone())
            .collect();
        let stored = matches!(self.bound(&reg)?, Bound::Files(_))
            && *self.use_stored.read().expect("lock")
            && manifest.flags_current(&cc.contract_hash);
        Ok((
            Resolution {
                contract: cc.name.clone(),
                version: cc.version,
                contract_hash: cc.contract_hash.clone(),
                compilation_hash: cc.compilation_hash.clone(),
                decisions: cc.decisions.iter().map(|d| d.id.clone()).collect(),
                annotations,
                shapes,
                flags_materialised: stored,
                snapshot_id: manifest.snapshot_id,
            },
            manifest,
        ))
    }

    /// The plan that stands in for a contract, for one caller (peQL design 4.5): scan, filter
    /// on admits and drop-level flags, project the exposed columns, bind `ctx`, gate. A query
    /// names contracts; each name is this plan, and the query's shapes apply over them.
    async fn contract_view(
        &self,
        name: &str,
        caller: &Caller,
        resolution: &Resolution,
    ) -> Result<LogicalPlan> {
        let reg = self.visible(name, caller)?;
        let cc = &reg.compilation.contract;
        let stored = resolution.flags_materialised;
        let provider = match self.bound(&reg)? {
            Bound::Table(t) => t,
            #[cfg(feature = "iceberg")]
            Bound::Files(Location::Iceberg(t)) => {
                let (table, snapshot) = self
                    .iceberg_snapshot(&t, name, resolution.snapshot_id)
                    .await?;
                t.provider(&table, cc, stored, snapshot).await?
            }
            Bound::Files(_) => self.bindings.provider(cc, stored).await?,
        };
        let bound: Arc<dyn TableProvider> = Arc::new(BoundTable {
            contract: cc.name.clone(),
            inner: provider,
        });
        let scan = LogicalPlanBuilder::scan(
            format!("__peql_{}", cc.name.replace('/', "_")),
            provider_as_source(bound),
            None,
        )?
        .build()?;
        let scan = if stored { scan } else { enrich_plan(scan, cc)? };
        let (admits, projection) = if stored {
            (&cc.admits_stored, &cc.projection_stored)
        } else {
            (&cc.admits, &cc.projection)
        };
        let mut filters: Vec<Expr> = admits.iter().map(|(_, e)| e.clone()).collect();
        for f in &cc.flags {
            if f.on_fail == AssertOnFail::Drop {
                filters.push(if stored {
                    col_ref(&f.column)
                } else {
                    f.expr.clone()
                });
            }
        }
        let mut b = LogicalPlanBuilder::from(scan);
        if let Some(f) = filters.into_iter().reduce(Expr::and) {
            b = b.filter(f)?;
        }
        let plan = b
            .project(projection.iter().map(|(n, e)| e.clone().alias(n)))?
            .build()?;
        let plan =
            parcel_core::compile::resolve(plan)?.with_param_values(param_values(cc, caller)?)?;
        Ok(Gate::plan(&cc.name, &cc.compilation_hash, plan))
    }

    /// What a caller would see: the exposed schema, if the contract admits them at all.
    pub fn describe(&self, name: &str, caller: &Caller) -> Result<SchemaRef> {
        let reg = self.visible(name, caller)?;
        let cc = &reg.compilation.contract;
        if let Some(rule) = parcel_runtime::plan::refusal(cc, caller)? {
            return Err(PeqlError::Denied {
                contract: cc.name.clone(),
                rule,
            });
        }
        Ok(cc.exposed_schema.clone())
    }

    /// The contracts a caller may see.
    pub fn list_for(&self, caller: &Caller) -> Vec<Arc<Registered>> {
        self.store
            .list()
            .into_iter()
            .filter(|r| self.visible(r.name(), caller).is_ok())
            .collect()
    }

    // ── query ───────────────────────────────────────────────────────────────

    /// A session with parcel's functions, gates, the gate barrier, and the object stores the
    /// bindings read: where a [`Engine::view`] plan is planned and run.
    pub fn session(&self) -> SessionContext {
        self.session_for(None)
    }

    /// [`Engine::session`] for work that holds about `memory` bytes: `partitions` partitions,
    /// batches of `batch` rows when given, operators that reserve memory (a write's files, a
    /// clustered write's sort) reserving from a pool of `memory`, and written files buffered in
    /// an eighth of it.
    fn bounded_session(
        &self,
        memory: usize,
        partitions: usize,
        batch: Option<usize>,
        keep_partition_columns: bool,
    ) -> Result<SessionContext> {
        use datafusion::execution::memory_pool::FairSpillPool;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        let mut config = self.config_for(Some(partitions));
        let options = config.options_mut();
        options.execution.minimum_parallel_output_files =
            datafusion::config::ConfigNonZeroUsize::try_new(1)
                .map_err(|e| PeqlError::Invalid(e.to_string()))?;
        options.execution.objectstore_writer_buffer_size = (memory / 8).max(1 << 20);
        options.execution.sort_spill_reservation_bytes = memory / 8;
        options.execution.keep_partition_by_columns = keep_partition_columns;
        if let Some(rows) = batch {
            options.execution.batch_size = datafusion::config::ConfigNonZeroUsize::try_new(rows)
                .map_err(|e| PeqlError::Invalid(e.to_string()))?;
        }
        let env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(memory)))
            .build_arc()?;
        Ok(self.session_with(config, Some(env)))
    }

    /// [`Engine::session`] planning for `partitions` partitions, when given, instead of one per
    /// core: the parallelism an executor that runs the plan inside a memory budget can hold.
    fn session_for(&self, partitions: Option<usize>) -> SessionContext {
        self.session_with(self.config_for(partitions), None)
    }

    fn config_for(&self, partitions: Option<usize>) -> SessionConfig {
        let mut config = SessionConfig::new()
            .with_information_schema(false)
            .set_bool("datafusion.execution.parquet.pushdown_filters", true)
            .set_bool("datafusion.execution.parquet.reorder_filters", true)
            .set_bool("datafusion.sql_parser.enable_ident_normalization", false);
        if let Some(partitions) = partitions {
            config = config.with_target_partitions(partitions.max(1));
        }
        config
    }

    fn session_with(
        &self,
        config: SessionConfig,
        env: Option<Arc<datafusion::execution::runtime_env::RuntimeEnv>>,
    ) -> SessionContext {
        let mut builder = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_query_planner(Arc::new(GatedQueryPlanner));
        if let Some(env) = env {
            builder = builder.with_runtime_env(env);
        }
        let state = builder.build();
        let ctx = SessionContext::new_with_state(state);
        ctx.add_optimizer_rule(Arc::new(GateBarrier));
        for udf in parcel_core::udfs::parcel_udfs() {
            ctx.register_udf(udf);
        }
        for (url, store) in self.bindings.object_stores() {
            ctx.register_object_store(url.as_ref(), store);
        }
        ctx
    }

    /// Resolve every contract the SQL names, register their views, and plan the query with
    /// shapes applied.
    async fn prepare(
        &self,
        sql: &str,
        caller: &Caller,
        partitions: Option<usize>,
        as_of: &AsOf,
    ) -> Result<Prepared> {
        guard::check_sql(sql)?;
        let (expanded_sql, searches) = crate::vector::expand(sql)?;
        let ctx = self.session_for(partitions);
        let state = ctx.state();
        let statement =
            state.sql_to_statement(&expanded_sql, &datafusion::config::Dialect::Generic)?;
        let refs = state.resolve_table_references(&statement)?;
        let mut resolutions: Vec<Resolution> = Vec::new();
        let mut active = Vec::new();
        let mut fingerprints = Vec::new();
        for t in refs {
            let name = t.table().to_owned();
            if resolutions.iter().any(|r| r.contract == name) {
                continue;
            }
            // Only contracts exist: a reference with a catalog or schema names nothing.
            if t.schema().is_some() {
                return Err(PeqlError::UnknownContract(t.to_string()));
            }
            self.visible(&name, caller)?;
            let (resolution, manifest) = match as_of.get(&name) {
                None => {
                    self.ensure_manifest(&name).await?;
                    self.resolve(&name, caller)?
                }
                Some(snapshot) => {
                    let pinned = self.ensure_manifest_as_of(&name, snapshot).await?;
                    self.resolve_with(&name, caller, || Ok(Some(pinned)))?
                }
            };
            let view = self.contract_view(&name, caller, &resolution).await?;
            ctx.register_table(t.clone(), Arc::new(ViewTable::new(view, None)))?;
            let reg = self.get(&name)?;
            for s in parcel_runtime::plan::active_shapes(&reg.compilation.contract, caller)? {
                active.push(s.clone());
            }
            fingerprints.push((
                name.clone(),
                format!(
                    "{}|{}|{:?}|{}|{}|{:?}",
                    resolution.compilation_hash,
                    params_fingerprint(&reg.compilation.contract, caller)?,
                    resolution.shapes,
                    manifest.data_hash,
                    manifest.written_at.timestamp_micros(),
                    manifest.snapshot_id,
                ),
            ));
            resolutions.push(resolution);
        }
        for named in as_of.0.keys() {
            if !resolutions.iter().any(|r| &r.contract == named) {
                return Err(PeqlError::Invalid(format!(
                    "the read names a snapshot of `{named}`, which the query does not read"
                )));
            }
        }
        for search in searches {
            // A CTE cannot shadow a vector target and erase its contract resolution.
            if !resolutions.iter().any(|r| r.contract == search.table) {
                return Err(PeqlError::UnknownContract(search.table));
            }
            let schema = self.describe(&search.table, caller)?;
            ctx.register_udf(search.udf(schema.as_ref())?);
        }
        let plan = ctx.state().create_logical_plan(&expanded_sql).await?;
        guard::check_plan(&plan)?;
        let optimized = ctx.state().optimize(&plan)?;
        let active_refs: Vec<&parcel_core::compile::ShapeRule> = active.iter().collect();
        let shaped = parcel_runtime::shape::apply(plan, &optimized, &active_refs)?;
        Ok(Prepared {
            cache_key: CacheKey::new(sql, &fingerprints),
            ctx,
            plan: shaped.plan,
            resolutions,
            charges: shaped
                .charges
                .into_iter()
                .map(|c| (c.budget, c.epsilon))
                .collect(),
            suppress_k: shaped.suppress_k,
            has_aggregate: shaped.has_aggregate,
        })
    }

    /// The physical plan of a prepared query, with the shapes that act while it runs, refused
    /// unless every scan is under its contract's gate.
    async fn physical(&self, p: &Prepared) -> Result<Arc<dyn ExecutionPlan>> {
        let mut physical = p.ctx.state().create_physical_plan(&p.plan).await?;
        if let (Some(k), false) = (p.suppress_k, p.has_aggregate) {
            physical = Arc::new(SuppressExec::new(k, physical));
        }
        ensure_gated(physical.as_ref())?;
        Ok(physical)
    }

    /// Plan a prepared query for one execution: the physical plan, with the budgets it spends
    /// charged now, all or none. Every answer peQL gives is planned here.
    async fn planned(&self, p: Prepared, caller: &Caller) -> Result<Planned> {
        let plan = self.physical(&p).await?;
        let budgets = if p.charges.is_empty() {
            BTreeMap::new()
        } else {
            self.budgets
                .charge_all(&format!("{}/{}", caller.tenant, caller.id), &p.charges)?
        };
        let mut charges = BTreeMap::new();
        for (budget, epsilon) in &p.charges {
            *charges.entry(budget.clone()).or_insert(0.0) += epsilon;
        }
        Ok(Planned {
            ctx: p.ctx,
            plan,
            contracts: p.resolutions,
            suppress_k: p.suppress_k,
            charges,
            budgets,
        })
    }

    /// The physical plan a query would run, as text: shows pruning and pushed-down filters.
    /// For operators; callers cannot `EXPLAIN`. Nothing is charged.
    pub async fn explain(&self, sql: &str, caller: &Caller) -> Result<String> {
        let p = self.prepare(sql, caller, None, &AsOf::current()).await?;
        let physical = self.physical(&p).await?;
        Ok(datafusion::physical_plan::displayable(physical.as_ref())
            .indent(true)
            .to_string())
    }

    /// Everything [`Engine::query`] checks before it reads a row, and the schema it would
    /// answer with: the statement guard, visibility, `decide`, `guarantee`, the plan guard.
    /// A refusal here is the refusal the query would meet.
    /// Failed checks are audited as a query's are; a check that passes is not, since
    /// the query that follows is. Nothing is charged.
    pub async fn check(&self, sql: &str, caller: &Caller) -> Result<Checked> {
        let started = Instant::now();
        match self.prepare(sql, caller, None, &AsOf::current()).await {
            Ok(p) => Ok(Checked {
                schema: Arc::new(p.plan.schema().as_arrow().clone()),
                contracts: p.resolutions,
            }),
            Err(e) => {
                let outcome = if e.is_refusal() {
                    Outcome::Refused(e.to_string())
                } else {
                    Outcome::Failed(e.to_string())
                };
                self.audit(
                    Uuid::new_v4(),
                    sql,
                    caller,
                    started,
                    outcome,
                    0,
                    Vec::new(),
                    Vec::new(),
                )?;
                Err(e)
            }
        }
    }

    /// Whether `caller` may write under `name`: the contract's owner tenant may, and so may
    /// anyone for a contract with no owner. Nothing else about a write depends on the caller,
    /// so an embedding that accepts writes from callers checks this before [`Engine::write`].
    pub fn authorize_write(&self, name: &str, caller: &Caller) -> Result<()> {
        let reg = self.visible(name, caller)?;
        match reg.owner() {
            Some(owner) if owner != caller.tenant => Err(PeqlError::Denied {
                contract: name.to_owned(),
                rule: "writes are the owner's".into(),
            }),
            _ => Ok(()),
        }
    }

    /// A contract as one caller may read it, planned for an executor that runs the plan
    /// itself (Moruna's `PlanSource`): `SELECT *` over the contract, with every shape that
    /// applies to the caller in the plan and the budgets it spends charged now. See
    /// [`Engine::plan`], which this is for one contract.
    pub async fn view(&self, name: &str, caller: &Caller) -> Result<Planned> {
        self.view_for(name, caller, None).await
    }

    /// [`Engine::view`] planned for `partitions` partitions, when given, instead of one per
    /// core (see [`Engine::plan_for`]).
    pub async fn view_for(
        &self,
        name: &str,
        caller: &Caller,
        partitions: Option<usize>,
    ) -> Result<Planned> {
        let sql = format!("SELECT * FROM \"{}\"", name.replace('"', "\"\""));
        self.plan_for(&sql, caller, partitions).await
    }

    /// [`Engine::view_for`] of a contract's Iceberg table as of one snapshot: what a host
    /// serves for a contract version frozen at that snapshot. The contract's rules apply as
    /// they do to the current data; the resolution names the snapshot.
    pub async fn view_as_of(
        &self,
        name: &str,
        caller: &Caller,
        snapshot_id: i64,
        partitions: Option<usize>,
    ) -> Result<Planned> {
        let sql = format!("SELECT * FROM \"{}\"", name.replace('"', "\"\""));
        self.plan_as_of(
            &sql,
            caller,
            partitions,
            &AsOf::current().with(name, snapshot_id),
        )
        .await
    }

    /// SQL in which every table is a contract, planned for an executor that runs the plan
    /// itself: resolved, gated, shaped, and charged, exactly as [`Engine::query`] plans it.
    /// The budgets are charged once, here, for one execution of [`Planned::plan`]; the
    /// planning is audited with its charges, and a refusal (including an exhausted budget)
    /// is audited and returned as `query` would return it. What the executor does with the
    /// batches is its own: no envelope is made, since no answer is formed here.
    pub async fn plan(&self, sql: &str, caller: &Caller) -> Result<Planned> {
        self.plan_for(sql, caller, None).await
    }

    /// [`Engine::plan`] for `partitions` partitions, when given, instead of one per core. An
    /// executor that runs the plan inside a memory budget plans for the parallelism the budget
    /// holds: every partition of a scan, an exchange or a merge runs at once and holds its own
    /// batches, so the partition count is what bounds the plan's memory outside its operators'
    /// pool. The answer does not depend on it.
    pub async fn plan_for(
        &self,
        sql: &str,
        caller: &Caller,
        partitions: Option<usize>,
    ) -> Result<Planned> {
        self.plan_as_of(sql, caller, partitions, &AsOf::current())
            .await
    }

    /// [`Engine::plan_for`] with the contracts `as_of` names read as of their snapshots.
    pub async fn plan_as_of(
        &self,
        sql: &str,
        caller: &Caller,
        partitions: Option<usize>,
        as_of: &AsOf,
    ) -> Result<Planned> {
        let started = Instant::now();
        let out = match self.prepare(sql, caller, partitions, as_of).await {
            Ok(p) => self.planned(p, caller).await,
            Err(e) => Err(e),
        };
        let (outcome, charges, contracts) = match &out {
            Ok(p) => (
                Outcome::Planned,
                p.charges.clone().into_iter().collect(),
                contract_ids(&p.contracts),
            ),
            Err(e) if e.is_refusal() => (Outcome::Refused(e.to_string()), Vec::new(), Vec::new()),
            Err(e) => (Outcome::Failed(e.to_string()), Vec::new(), Vec::new()),
        };
        self.audit(
            Uuid::new_v4(),
            sql,
            caller,
            started,
            outcome,
            0,
            charges,
            contracts,
        )?;
        out
    }

    /// Run SQL in which every table is a contract.
    pub async fn query(&self, sql: &str, caller: &Caller) -> Result<QueryResult> {
        self.query_as_of(sql, caller, &AsOf::current()).await
    }

    /// [`Engine::query`] with the contracts `as_of` names read as of their snapshots: each
    /// under its rules, over the data of that snapshot, with that snapshot's facts deciding
    /// its guarantees. The envelope names each snapshot read.
    pub async fn query_as_of(
        &self,
        sql: &str,
        caller: &Caller,
        as_of: &AsOf,
    ) -> Result<QueryResult> {
        let started = Instant::now();
        let audit_id = Uuid::new_v4();
        let out = self.run(sql, caller, audit_id, as_of).await;
        let (outcome, rows, charges, contracts) = match &out {
            Ok(r) => (
                Outcome::Answered,
                r.envelope.rows,
                r.envelope.charges.clone().into_iter().collect(),
                contract_ids(&r.envelope.contracts),
            ),
            Err(e) if e.is_refusal() => {
                (Outcome::Refused(e.to_string()), 0, Vec::new(), Vec::new())
            }
            Err(e) => (Outcome::Failed(e.to_string()), 0, Vec::new(), Vec::new()),
        };
        self.audit(
            audit_id, sql, caller, started, outcome, rows, charges, contracts,
        )?;
        out
    }

    #[cfg(feature = "flight")]
    pub(crate) async fn query_spooled(&self, sql: &str, caller: &Caller) -> Result<SpoolResult> {
        let started = Instant::now();
        let audit_id = Uuid::new_v4();
        let out = self.spool(sql, caller, audit_id).await;
        let (outcome, rows, charges, contracts) = match &out {
            Ok(r) => (
                Outcome::Answered,
                r.envelope.rows,
                r.envelope.charges.clone().into_iter().collect(),
                contract_ids(&r.envelope.contracts),
            ),
            Err(e) if e.is_refusal() => {
                (Outcome::Refused(e.to_string()), 0, Vec::new(), Vec::new())
            }
            Err(e) => (Outcome::Failed(e.to_string()), 0, Vec::new(), Vec::new()),
        };
        self.audit(
            audit_id, sql, caller, started, outcome, rows, charges, contracts,
        )?;
        out
    }

    #[cfg(feature = "flight")]
    async fn spool(&self, sql: &str, caller: &Caller, audit_id: Uuid) -> Result<SpoolResult> {
        use datafusion::arrow::ipc::{reader::StreamReader, writer::StreamWriter};
        use futures::StreamExt;
        use sha2::{Digest, Sha256};
        use std::io::{Read, Seek, SeekFrom};
        let prepared = self.prepare(sql, caller, None, &AsOf::current()).await?;
        let planned = self.planned(prepared, caller).await?;
        std::fs::create_dir_all(&self.spool_root)?;
        let mut file = tempfile::tempfile_in(&self.spool_root)?;
        let mut rows = 0;
        let mut stream = datafusion::physical_plan::execute_stream(
            planned.plan.clone(),
            planned.ctx.task_ctx(),
        )?;
        {
            let mut writer = None;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                if writer.is_none() {
                    writer = Some(
                        StreamWriter::try_new(&mut file, &batch.schema())
                            .map_err(datafusion::error::DataFusionError::from)?,
                    );
                }
                writer
                    .as_mut()
                    .unwrap()
                    .write(&batch)
                    .map_err(datafusion::error::DataFusionError::from)?;
                rows += batch.num_rows();
            }
            if let Some(mut writer) = writer {
                writer
                    .finish()
                    .map_err(datafusion::error::DataFusionError::from)?;
            }
        }
        file.seek(SeekFrom::Start(0))?;
        let mut hash = Sha256::new();
        let mut buffer = [0_u8; 65536];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        let envelope = Envelope {
            caller: Asker::of(caller),
            contracts: planned.contracts,
            rows,
            suppress_k: planned.suppress_k,
            charges: planned.charges,
            budgets: planned.budgets,
            scan: ScanStats::from_plan(planned.plan.as_ref()),
            attestation: Attestation {
                query_sha256: parcel_core::hash::sha256_hex(sql.as_bytes()),
                result_sha256: hex::encode(hash.finalize()),
                at: Utc::now(),
            },
            audit_id,
            cached: false,
        };
        let signature = self.sign(&envelope).await?;
        file.seek(SeekFrom::Start(0))?;
        let batches = if file.metadata()?.len() == 0 {
            None
        } else {
            Some(
                StreamReader::try_new(file, None)
                    .map_err(datafusion::error::DataFusionError::from)?,
            )
        };
        Ok(SpoolResult {
            schema: planned.plan.schema(),
            batches,
            envelope,
            signature,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn audit(
        &self,
        id: Uuid,
        sql: &str,
        caller: &Caller,
        started: Instant,
        outcome: Outcome,
        rows: usize,
        charges: Vec<(String, f64)>,
        contracts: Vec<String>,
    ) -> Result<()> {
        self.audit.record(&AuditRecord {
            id,
            at: Utc::now(),
            caller_id: caller.id.clone(),
            tenant: caller.tenant.clone(),
            purpose: caller.purpose.clone(),
            sql_sha256: parcel_core::hash::sha256_hex(sql.as_bytes()),
            contracts,
            outcome,
            rows,
            elapsed_ms: started.elapsed().as_millis() as u64,
            charges,
        })
    }

    async fn run(
        &self,
        sql: &str,
        caller: &Caller,
        audit_id: Uuid,
        as_of: &AsOf,
    ) -> Result<QueryResult> {
        let p = self.prepare(sql, caller, None, as_of).await?;
        if let Some(batches) = self.cache.as_ref().and_then(|c| c.get(&p.cache_key)) {
            let rows = batches.iter().map(|b| b.num_rows()).sum();
            let envelope = Envelope {
                caller: Asker::of(caller),
                contracts: p.resolutions,
                rows,
                suppress_k: p.suppress_k,
                charges: BTreeMap::new(),
                budgets: BTreeMap::new(),
                scan: ScanStats::default(),
                attestation: Attestation::of(sql, &batches),
                audit_id,
                cached: true,
            };
            let signature = self.sign(&envelope).await?;
            let schema = match batches.first() {
                Some(b) => b.schema(),
                None => Arc::new(p.plan.schema().as_arrow().clone()),
            };
            return Ok(QueryResult {
                schema,
                batches,
                envelope,
                signature,
            });
        }
        let cache_key = p.cache_key.clone();
        let planned = self.planned(p, caller).await?;
        let batches =
            datafusion::physical_plan::collect(planned.plan.clone(), planned.ctx.task_ctx())
                .await?;
        let rows = batches.iter().map(|b| b.num_rows()).sum();
        if let Some(c) = &self.cache {
            c.put(cache_key, batches.clone());
        }
        let envelope = Envelope {
            caller: Asker::of(caller),
            contracts: planned.contracts,
            rows,
            suppress_k: planned.suppress_k,
            charges: planned.charges,
            budgets: planned.budgets,
            scan: ScanStats::from_plan(planned.plan.as_ref()),
            attestation: Attestation::of(sql, &batches),
            audit_id,
            cached: false,
        };
        let signature = self.sign(&envelope).await?;
        Ok(QueryResult {
            schema: planned.plan.schema(),
            batches,
            envelope,
            signature,
        })
    }

    async fn sign(&self, envelope: &Envelope) -> Result<Option<String>> {
        match &self.signer {
            Some(s) => s.sign(envelope).await.map(Some).map_err(PeqlError::Signing),
            None => Ok(None),
        }
    }
}

/// A query planned for one execution by an executor of the caller's choosing: what
/// [`Engine::view`] and [`Engine::plan`] return, and what [`Engine::query`] runs. The plan is
/// gated (every scan under its contract's `GateExec`), carries every shape that applies to
/// the caller (group suppression, aggregate and row noise, and whole-result suppression as a
/// [`SuppressExec`]), and its budgets are already charged. Run it in [`Planned::ctx`].
pub struct Planned {
    /// The session the plan was made in, whose functions and object stores it runs with.
    pub ctx: SessionContext,
    /// Executed once, each partition once. The charge is for one release of what it
    /// computes: running it again (after DataFusion's `reset_plan_states`) draws fresh
    /// noise that nothing has paid for, so an executor does so only to recompute what it
    /// discards, never to release a second answer.
    pub plan: Arc<dyn ExecutionPlan>,
    /// What the resolver decided for every contract the query named.
    pub contracts: Vec<Resolution>,
    /// The `suppress` threshold in force.
    pub suppress_k: Option<u64>,
    /// Epsilon charged per budget.
    pub charges: BTreeMap<String, f64>,
    /// Privacy budget left per budget after the charge.
    pub budgets: BTreeMap<String, f64>,
}

impl Planned {
    /// The schema of the plan's batches.
    pub fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }
}

struct Prepared {
    cache_key: CacheKey,
    ctx: SessionContext,
    plan: LogicalPlan,
    resolutions: Vec<Resolution>,
    charges: Vec<(String, f64)>,
    suppress_k: Option<u64>,
    has_aggregate: bool,
}

/// What a call that names a snapshot meets for a contract whose data has none.
fn no_snapshots(name: &str) -> PeqlError {
    PeqlError::Invalid(format!(
        "`{name}` is not bound to a table with snapshots; only an Iceberg binding is read as of one"
    ))
}

/// `name@version#compilation_hash` for each contract, as the audit log names them.
fn contract_ids(contracts: &[Resolution]) -> Vec<String> {
    contracts
        .iter()
        .map(|c| format!("{}@{}#{}", c.contract, c.version, c.compilation_hash))
        .collect()
}

/// The caller's bound context as the contract sees it: its parameter values, in order.
fn params_fingerprint(cc: &parcel_core::CompiledContract, caller: &Caller) -> Result<String> {
    let datafusion::common::ParamValues::Map(m) = param_values(cc, caller)? else {
        return Ok(String::new());
    };
    let mut kv: Vec<(String, String)> = m.into_iter().map(|(k, v)| (k, format!("{v:?}"))).collect();
    kv.sort();
    Ok(format!("{kv:?}"))
}

#[cfg(feature = "flight")]
pub(crate) struct SpoolResult {
    pub schema: SchemaRef,
    pub batches: Option<datafusion::arrow::ipc::reader::StreamReader<std::fs::File>>,
    pub envelope: Envelope,
    pub signature: Option<String>,
}
