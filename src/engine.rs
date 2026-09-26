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
    /// An overwrite whose old files are still there.
    overwrite: tokio::sync::Mutex<bool>,
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
}

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
    signer: Option<Arc<dyn EnvelopeSigner>>,
}

impl Engine {
    /// A workspace on disk: contracts, functions, budgets and the audit log under
    /// `<root>/_peql/`; relative bindings resolve under `root`.
    pub fn open(root: impl AsRef<Path>) -> Result<Engine> {
        let root = root.as_ref();
        std::fs::create_dir_all(root.join("_peql"))?;
        Ok(Engine {
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
            signer: None,
        })
    }

    /// Everything in memory; relative bindings resolve under `base`.
    pub fn in_memory(base: impl Into<PathBuf>) -> Engine {
        Engine {
            store: Arc::new(MemoryStore::default()),
            functions: Arc::new(FunctionStore::in_memory()),
            bindings: Arc::new(LocalParquet { base: base.into() }),
            budgets: Arc::new(BudgetStore::in_memory()),
            audit: Arc::new(MemoryAudit::default()),
            cache: None,
            tables: RwLock::default(),
            table_manifests: RwLock::default(),
            documents: RwLock::default(),
            use_stored: RwLock::new(true),
            object_manifests: RwLock::default(),
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

    /// The raw data, for validation: row columns only.
    async fn raw_provider(&self, reg: &Registered) -> Result<Arc<dyn TableProvider>> {
        match self.bound(reg)? {
            Bound::Table(t) => Ok(t),
            Bound::Files(_) => {
                self.bindings
                    .provider(&reg.compilation.contract, false)
                    .await
            }
        }
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
        }
    }

    /// A contract registered over files written elsewhere has no manifest yet: make one. For
    /// files in an object store, read the manifest again, since another engine may write there.
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
        }
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
        Ok(Writing {
            reg,
            location,
            rows: AtomicUsize::new(0),
            overwrite: tokio::sync::Mutex::new(mode == WriteMode::Overwrite),
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

        let ctx = self.session();
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
        for c in &layout.bloom {
            parquet
                .column_specific_options
                .entry(c.clone())
                .or_default()
                .bloom_filter_enabled = Some(true);
        }
        let url = match &w.location {
            Location::Local(root) => binding::local_url(root, true)?,
            Location::Object(o) => o.url(true),
        };
        df.write_parquet(&url, options, Some(parquet)).await?;
        w.rows.fetch_add(rows, Ordering::SeqCst);
        Ok(rows)
    }

    /// Finish a write begun by [`Engine::begin_write`]: the data changed, so refresh every
    /// contract bound to it, the writer's last, which validates it and writes its manifest.
    pub async fn finish_write(&self, w: Writing) -> Result<WriteReport> {
        self.clear_for_overwrite(&w).await?;
        let name = w.reg.name();
        let written_at = Utc::now();
        let key = w.location.key()?;
        for other in self.store.list() {
            if other.name() == name {
                continue;
            }
            if let Ok(Bound::Files(r)) = self.bound(&other)
                && r.key().ok().as_ref() == Some(&key)
            {
                self.refresh(other.name(), written_at).await?;
            }
        }
        let (verdict, manifest) = self.refresh(name, written_at).await?;
        Ok(WriteReport {
            rows_written: w.rows.load(Ordering::SeqCst),
            files: manifest.files.len(),
            verdict,
        })
    }

    /// Validate a contract over its files and save its manifest.
    async fn refresh(&self, name: &str, written_at: DateTime<Utc>) -> Result<(Verdict, Manifest)> {
        let reg = self.get(name)?;
        let cc = &reg.compilation.contract;
        let Bound::Files(location) = self.bound(&reg)? else {
            return Err(PeqlError::Invalid(format!("`{name}` has no files")));
        };
        let verdict = self.validate(name).await?;
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
        }
        Ok((verdict, manifest))
    }

    /// Run the contract's validation plan over its data (peQL design 4.9a).
    pub async fn validate(&self, name: &str) -> Result<Verdict> {
        let plan = self.get(name)?.compilation.validation.plan.clone();
        self.validate_with(name, plan).await
    }

    /// Run a validation plan obtained elsewhere (e.g. decoded from a bundle) over the
    /// contract's data. This is what a certificate verifier does: same plan, same data.
    pub async fn validate_with(&self, name: &str, plan: LogicalPlan) -> Result<Verdict> {
        let reg = self.get(name)?;
        let provider = self.raw_provider(&reg).await?;
        let verdict =
            parcel_runtime::plan::validate_in(&self.session(), &reg.compilation, plan, provider)
                .await?;
        let data_hash = match self.bound(&reg)? {
            Bound::Files(Location::Local(root)) => {
                binding::data_hash(&root, &binding::list_files(&root)?)?
            }
            Bound::Files(Location::Object(o)) => o.data_hash(&o.list_files().await?).await?,
            Bound::Table(t) => {
                let batches = self.session().read_table(t)?.collect().await?;
                parcel_core::hash::sha256_hex(&crate::envelope::ipc_bytes(&batches))
            }
        };
        Ok(Verdict { verdict, data_hash })
    }

    // ── resolve, view, describe ─────────────────────────────────────────────

    /// Decide, check guarantees and choose shapes for one caller (peQL design 4.3).
    pub fn resolve(&self, name: &str, caller: &Caller) -> Result<(Resolution, Manifest)> {
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
        let manifest = self.manifest(name)?.ok_or_else(|| PeqlError::NotWritten {
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
        let config = SessionConfig::new()
            .with_information_schema(false)
            .set_bool("datafusion.execution.parquet.pushdown_filters", true)
            .set_bool("datafusion.execution.parquet.reorder_filters", true)
            .set_bool("datafusion.sql_parser.enable_ident_normalization", false);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_query_planner(Arc::new(GatedQueryPlanner))
            .build();
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
    async fn prepare(&self, sql: &str, caller: &Caller) -> Result<Prepared> {
        guard::check_sql(sql)?;
        let ctx = self.session();
        let state = ctx.state();
        let statement = state.sql_to_statement(sql, &datafusion::config::Dialect::Generic)?;
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
            self.ensure_manifest(&name).await?;
            let (resolution, manifest) = self.resolve(&name, caller)?;
            let view = self.contract_view(&name, caller, &resolution).await?;
            ctx.register_table(t.clone(), Arc::new(ViewTable::new(view, None)))?;
            let reg = self.get(&name)?;
            for s in parcel_runtime::plan::active_shapes(&reg.compilation.contract, caller)? {
                active.push(s.clone());
            }
            fingerprints.push((
                name.clone(),
                format!(
                    "{}|{}|{:?}|{}|{}",
                    resolution.compilation_hash,
                    params_fingerprint(&reg.compilation.contract, caller)?,
                    resolution.shapes,
                    manifest.data_hash,
                    manifest.written_at.timestamp_micros(),
                ),
            ));
            resolutions.push(resolution);
        }
        let plan = ctx.state().create_logical_plan(sql).await?;
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
        let p = self.prepare(sql, caller).await?;
        let physical = self.physical(&p).await?;
        Ok(datafusion::physical_plan::displayable(physical.as_ref())
            .indent(true)
            .to_string())
    }

    /// Everything [`Engine::query`] checks before it reads a row, and the schema it would
    /// answer with: the statement guard, visibility, `decide`, `guarantee`, the plan guard.
    /// A refusal here is the refusal the query would meet.
    /// Refusals are audited as a query's are; a check that passes is not, since the query
    /// that follows is. Nothing is charged.
    pub async fn check(&self, sql: &str, caller: &Caller) -> Result<Checked> {
        let started = Instant::now();
        match self.prepare(sql, caller).await {
            Ok(p) => Ok(Checked {
                schema: Arc::new(p.plan.schema().as_arrow().clone()),
                contracts: p.resolutions,
            }),
            Err(e) => {
                if e.is_refusal() {
                    self.audit(
                        Uuid::new_v4(),
                        sql,
                        caller,
                        started,
                        Outcome::Refused(e.to_string()),
                        0,
                        Vec::new(),
                        Vec::new(),
                    )?;
                }
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
        let sql = format!("SELECT * FROM \"{}\"", name.replace('"', "\"\""));
        self.plan(&sql, caller).await
    }

    /// SQL in which every table is a contract, planned for an executor that runs the plan
    /// itself: resolved, gated, shaped, and charged, exactly as [`Engine::query`] plans it.
    /// The budgets are charged once, here, for one execution of [`Planned::plan`]; the
    /// planning is audited with its charges, and a refusal (including an exhausted budget)
    /// is audited and returned as `query` would return it. What the executor does with the
    /// batches is its own: no envelope is made, since no answer is formed here.
    pub async fn plan(&self, sql: &str, caller: &Caller) -> Result<Planned> {
        let started = Instant::now();
        let out = match self.prepare(sql, caller).await {
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
        let started = Instant::now();
        let audit_id = Uuid::new_v4();
        let out = self.run(sql, caller, audit_id).await;
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

    async fn run(&self, sql: &str, caller: &Caller, audit_id: Uuid) -> Result<QueryResult> {
        let p = self.prepare(sql, caller).await?;
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
