//! The engine: register contracts, write under them, validate them, and answer SQL in which
//! every table is a contract. parcel decides what each rule means; the engine binds the
//! caller, finds the data, and runs what parcel compiled.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
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
use crate::binding::{self, BindingResolver, CONTRACT_HASH_KEY, CONTRACT_NAME_KEY, LocalParquet};
use crate::budget::BudgetStore;
use crate::cache::{CacheKey, QueryCache};
use crate::envelope::{Attestation, Envelope, Resolution, ScanStats};
use crate::error::{PeqlError, Result};
use crate::functions::FunctionStore;
use crate::gate::{BoundTable, Gate, GateBarrier, GatedQueryPlanner, ensure_gated};
use crate::guard;
use crate::manifest::Manifest;
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
    pub batches: Vec<RecordBatch>,
    pub envelope: Envelope,
}

/// Where a contract's data comes from when it is not its binding's files.
enum Bound {
    Files(PathBuf),
    Table(Arc<dyn TableProvider>),
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
            .root(&reg.compilation.contract)
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
            Bound::Files(root) => Ok(Manifest::load(&root, name)?),
        }
    }

    /// A contract registered over files written elsewhere has no manifest yet: make one.
    pub async fn ensure_manifest(&self, name: &str) -> Result<()> {
        let reg = self.get(name)?;
        let Bound::Files(root) = self.bound(&reg)? else {
            return Ok(());
        };
        if Manifest::load(&root, name)?.is_some() || binding::list_files(&root)?.is_empty() {
            return Ok(());
        }
        self.refresh(name, Utc::now()).await?;
        Ok(())
    }

    // ── write and validate ──────────────────────────────────────────────────

    /// Write batches under a contract (parcel design 10): enrich, compute flags and derived
    /// columns, cluster and partition, stamp the contract hash, validate, write the manifest.
    pub async fn write(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
        mode: WriteMode,
    ) -> Result<WriteReport> {
        let reg = self.get(name)?;
        let c = &reg.compilation;
        let cc = &c.contract;
        let Bound::Files(root) = self.bound(&reg)? else {
            return Err(PeqlError::Invalid(format!(
                "`{name}` is bound to a table; only file bindings are written"
            )));
        };
        if root.extension().is_some_and(|e| e == "parquet") {
            return Err(PeqlError::Invalid(format!(
                "`{name}` binds a single file; bind a directory to write to it"
            )));
        }
        std::fs::create_dir_all(&root)?;
        let batches = batches
            .into_iter()
            .map(|b| conform(b, &cc.row_schema))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let rows_written: usize = batches.iter().map(|b| b.num_rows()).sum();

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

        if mode == WriteMode::Overwrite {
            for f in binding::list_files(&root)? {
                std::fs::remove_file(f)?;
            }
        }
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
        df.write_parquet(
            &format!("{}/", root.canonicalize()?.display()),
            options,
            Some(parquet),
        )
        .await?;

        // The data changed: refresh every contract bound to it, the writer's last.
        let written_at = Utc::now();
        let key = root.canonicalize()?;
        for other in self.store.list() {
            if other.name() == name {
                continue;
            }
            if let Ok(Bound::Files(r)) = self.bound(&other)
                && r.canonicalize().ok() == Some(key.clone())
            {
                self.refresh(other.name(), written_at).await?;
            }
        }
        let (verdict, manifest) = self.refresh(name, written_at).await?;
        Ok(WriteReport {
            rows_written,
            files: manifest.files.len(),
            verdict,
        })
    }

    /// Validate a contract over its files and save its manifest.
    async fn refresh(&self, name: &str, written_at: DateTime<Utc>) -> Result<(Verdict, Manifest)> {
        let reg = self.get(name)?;
        let cc = &reg.compilation.contract;
        let Bound::Files(root) = self.bound(&reg)? else {
            return Err(PeqlError::Invalid(format!("`{name}` has no files")));
        };
        let verdict = self.validate(name).await?;
        let flag_columns: Vec<String> = cc.flags.iter().map(|f| f.column.clone()).collect();
        let files = binding::list_files(&root)?
            .iter()
            .map(|f| binding::file_entry(&root, f, &flag_columns))
            .collect::<Result<Vec<_>>>()?;
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
        manifest.save(&root)?;
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
        let verdict = parcel_runtime::plan::validate(&reg.compilation, plan, provider).await?;
        let data_hash = match self.bound(&reg)? {
            Bound::Files(root) => binding::data_hash(&root, &binding::list_files(&root)?)?,
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
    /// on admits and drop-level flags, project the exposed columns, bind `ctx`, gate.
    pub async fn view(
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

    /// A session with parcel's functions, gates, and the gate barrier.
    fn session(&self) -> SessionContext {
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
            let view = self.view(&name, caller, &resolution).await?;
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

    async fn physical(&self, p: &Prepared) -> Result<Arc<dyn ExecutionPlan>> {
        let physical = p.ctx.state().create_physical_plan(&p.plan).await?;
        ensure_gated(physical.as_ref())?;
        Ok(physical)
    }

    /// The physical plan a query would run, as text: shows pruning and pushed-down filters.
    /// For operators; callers cannot `EXPLAIN`.
    pub async fn explain(&self, sql: &str, caller: &Caller) -> Result<String> {
        let p = self.prepare(sql, caller).await?;
        let physical = self.physical(&p).await?;
        Ok(datafusion::physical_plan::displayable(physical.as_ref())
            .indent(true)
            .to_string())
    }

    /// Run SQL in which every table is a contract.
    pub async fn query(&self, sql: &str, caller: &Caller) -> Result<QueryResult> {
        let started = Instant::now();
        let audit_id = Uuid::new_v4();
        let out = self.run(sql, caller, audit_id).await;
        let (outcome, rows, charges, contracts) = match &out {
            Ok((r, charges)) => (
                Outcome::Answered,
                r.envelope.rows,
                charges.clone(),
                r.envelope
                    .contracts
                    .iter()
                    .map(|c| format!("{}@{}#{}", c.contract, c.version, c.compilation_hash))
                    .collect(),
            ),
            Err(e) if e.is_refusal() => {
                (Outcome::Refused(e.to_string()), 0, Vec::new(), Vec::new())
            }
            Err(e) => (Outcome::Failed(e.to_string()), 0, Vec::new(), Vec::new()),
        };
        self.audit.record(&AuditRecord {
            id: audit_id,
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
        })?;
        out.map(|(r, _)| r)
    }

    async fn run(
        &self,
        sql: &str,
        caller: &Caller,
        audit_id: Uuid,
    ) -> Result<(QueryResult, Vec<(String, f64)>)> {
        let p = self.prepare(sql, caller).await?;
        if let Some(batches) = self.cache.as_ref().and_then(|c| c.get(&p.cache_key)) {
            let rows = batches.iter().map(|b| b.num_rows()).sum();
            let envelope = Envelope {
                contracts: p.resolutions,
                rows,
                suppress_k: p.suppress_k,
                budgets: BTreeMap::new(),
                scan: ScanStats::default(),
                attestation: Attestation::of(sql, &batches),
                audit_id,
                cached: true,
            };
            return Ok((QueryResult { batches, envelope }, Vec::new()));
        }
        let physical = self.physical(&p).await?;
        let budgets = if p.charges.is_empty() {
            BTreeMap::new()
        } else {
            self.budgets
                .charge_all(&format!("{}/{}", caller.tenant, caller.id), &p.charges)?
        };
        let mut batches =
            datafusion::physical_plan::collect(physical.clone(), p.ctx.task_ctx()).await?;
        if let (Some(k), false) = (p.suppress_k, p.has_aggregate) {
            batches = parcel_runtime::shape::suppress_ungrouped(batches, k);
        }
        let rows = batches.iter().map(|b| b.num_rows()).sum();
        if let Some(c) = &self.cache {
            c.put(p.cache_key.clone(), batches.clone());
        }
        let envelope = Envelope {
            contracts: p.resolutions,
            rows,
            suppress_k: p.suppress_k,
            budgets,
            scan: ScanStats::from_plan(physical.as_ref()),
            attestation: Attestation::of(sql, &batches),
            audit_id,
            cached: false,
        };
        Ok((QueryResult { batches, envelope }, p.charges))
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

/// The caller's bound context as the contract sees it: its parameter values, in order.
fn params_fingerprint(cc: &parcel_core::CompiledContract, caller: &Caller) -> Result<String> {
    let datafusion::common::ParamValues::Map(m) = param_values(cc, caller)? else {
        return Ok(String::new());
    };
    let mut kv: Vec<(String, String)> = m.into_iter().map(|(k, v)| (k, format!("{v:?}"))).collect();
    kv.sort();
    Ok(format!("{kv:?}"))
}
