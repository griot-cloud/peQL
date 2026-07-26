//! [`GriotEngine`] — the high-level, contract-resolving query API.
//!
//! ```ignore
//! let engine = GriotEngine::from_json_contracts_dir("./contracts")?;
//! let rows = engine
//!     .query(r#"SELECT * FROM "sales/orders/v1""#, Caller::new("user:bob", "analytics", "globex"))
//!     .await?;
//! ```
//!
//! Each query runs in a fresh DataFusion session whose default catalog is a
//! caller-bound [`GriotCatalogProvider`], so the caller's identity flows into
//! contract resolution without any global state.

use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::binding::BindingResolver;
use crate::catalog::{GriotCatalogProvider, GriotSchemaProvider};
use crate::contract_source::{Caller, ContractError, ContractSource, JsonContractSource};
use crate::physical::scan_metrics_exec::BYTES_SCANNED_METRIC;
use crate::{DdlGuard, EngineError};

/// DataFusion's `ProjectionPushdown` physical-optimizer rule, which we drop for
/// governed queries.
///
/// # Why it is removed (Task #26)
///
/// The governed [`crate::contract_table_provider::ContractTableProvider`] always
/// scans the raw table in **full** (its enforcement operators need every column
/// the contract references, not just the SELECT list) and then applies the
/// caller's projection on top of the governed plan itself. So projection
/// *pushdown into the provider* is already handled internally and DataFusion's
/// generic `ProjectionPushdown` offers no benefit here.
///
/// Worse, it is actively unsound for us: `MaskingExec` legitimately **changes a
/// column's Arrow type** (a `hash_sha256` mask on a numeric/temporal column casts
/// it to `Utf8`). `ProjectionPushdown` treats a masked column as a plain,
/// type-preserving column reference and pushes the projection past `MaskingExec`
/// toward the raw scan, then its `SanityCheckPlan` sibling rejects the plan with
/// a "Schema mismatch" (`Utf8` expected vs. the raw `Int64` it pushed to). This
/// broke every `ORDER BY` / non-trivial query on a dataset with a masked numeric
/// column (found in live E2E, 2026-07-05). Dropping the rule keeps enforcement
/// intact and the plan sound; the only cost is a full-column scan we already do.
const PROJECTION_PUSHDOWN_RULE: &str = "ProjectionPushdown";

/// The catalog name bare quoted dataset references resolve under.
const CATALOG_NAME: &str = "griot";
/// The schema name within the griot catalog.
const SCHEMA_NAME: &str = "data";

/// A query engine that resolves a contract + a data location for every query and
/// returns governed rows.
pub struct GriotEngine {
    source: Arc<dyn ContractSource>,
    binding: Arc<dyn BindingResolver>,
}

impl GriotEngine {
    /// Build from any contract source + binding resolver.
    pub fn new(source: Arc<dyn ContractSource>, binding: Arc<dyn BindingResolver>) -> Self {
        Self { source, binding }
    }

    /// Build an open-source engine from a directory of JSON contracts. The same
    /// [`JsonContractSource`] supplies both the policy and the (local Parquet)
    /// binding.
    pub fn from_json_contracts_dir(
        dir: impl AsRef<std::path::Path>,
    ) -> Result<Self, ContractError> {
        let src = Arc::new(JsonContractSource::from_dir(dir)?);
        Ok(Self::new(
            src.clone() as Arc<dyn ContractSource>,
            src as Arc<dyn BindingResolver>,
        ))
    }

    /// Build an open-source engine from in-memory JSON contract documents.
    pub fn from_json_contracts<I, S>(docs: I) -> Result<Self, ContractError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let src = Arc::new(JsonContractSource::from_json_strs(docs)?);
        Ok(Self::new(
            src.clone() as Arc<dyn ContractSource>,
            src as Arc<dyn BindingResolver>,
        ))
    }

    /// Run `sql` as `caller`, returning governed rows.
    ///
    /// Datasets are referenced by their (quoted) URI, e.g.
    /// `SELECT * FROM "sales/orders/v1"`. Masking, row filtering and DP noise
    /// from the governing contract are applied inside the query plan.
    ///
    /// This is a thin wrapper over [`GriotEngine::query_with_stats`] that
    /// drops the scan-accounting [`QueryStats`] for callers that don't need
    /// it. Behaviourally identical to the pre-ADR-0052-follow-up-#42 `query`.
    pub async fn query(&self, sql: &str, caller: Caller) -> Result<Vec<RecordBatch>, EngineError> {
        Ok(self.query_with_stats(sql, caller).await?.batches)
    }

    /// Run `sql` as `caller`, returning governed rows PLUS scan-level
    /// accounting metadata (ADR-0052 follow-up #42).
    ///
    /// Identical governed-query semantics to [`GriotEngine::query`] — same
    /// contract resolution, same masking/row-filter/DP enforcement, same
    /// errors. The only difference is that this method retains the physical
    /// plan after execution and sums the `bytes_scanned` metric (see
    /// [`crate::physical::scan_metrics_exec::ScanMetricsExec`]) across every
    /// scan node in the tree, giving callers a real "what did the engine
    /// have to read" figure instead of having to approximate it from the
    /// (already filtered/masked/limited) result batches.
    pub async fn query_with_stats(
        &self,
        sql: &str,
        caller: Caller,
    ) -> Result<QueryOutcome, EngineError> {
        // Defence-in-depth: reject unsafe DDL before planning.
        DdlGuard::reject_unsafe_ddl(sql)?;

        let schema = Arc::new(GriotSchemaProvider::new(
            self.source.clone(),
            self.binding.clone(),
            caller,
        ));
        let catalog = Arc::new(GriotCatalogProvider::new(SCHEMA_NAME, schema));

        let ctx = governed_session_context(
            SessionConfig::new().with_default_catalog_and_schema(CATALOG_NAME, SCHEMA_NAME),
        );
        ctx.register_catalog(CATALOG_NAME, catalog);

        let df = ctx.sql(sql).await?;
        let task_ctx = ctx.task_ctx();
        let plan = df.create_physical_plan().await?;
        let batches = datafusion::physical_plan::collect(plan.clone(), task_ctx).await?;
        let stats = scan_stats_from_plan(plan.as_ref());
        Ok(QueryOutcome { batches, stats })
    }
}

/// The result of a governed query: the enforced rows plus the raw scan-level
/// accounting the physical plan reported (ADR-0052 follow-up #42).
#[derive(Debug, Clone)]
pub struct QueryOutcome {
    /// The enforced (masked / row-filtered / projected / limited) result rows
    /// — identical to what [`GriotEngine::query`] returns.
    pub batches: Vec<RecordBatch>,
    /// Scan-level accounting, summed across every scan node in the physical
    /// plan. All-zero (not absent) for a query that touches no table (e.g.
    /// `SELECT 1`) or whose scan produced zero rows.
    pub stats: QueryStats,
}

/// Raw (pre-enforcement) scan volume for a single query, summed across every
/// table it read.
///
/// "Raw" means: before `RowFilterExec`/`MaskingExec` narrow the rows, and
/// before the query's own projection/`LIMIT` trims the result — this is the
/// volume the engine had to read off storage to answer the query, which is
/// what a usage/billing meter should charge on. It is intentionally NOT the
/// size of `QueryOutcome::batches` (the enforced result), which can be many
/// orders of magnitude smaller for a selective filter or an aggregate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryStats {
    /// Total bytes scanned, summed across every table this query read.
    ///
    /// Measured as the Arrow in-memory size
    /// (`RecordBatch::get_array_memory_size`) of the raw batches each scan
    /// produced — the truest figure available without a native file-level
    /// byte-scan metric from the underlying `TableProvider`. See
    /// [`crate::physical::scan_metrics_exec::ScanMetricsExec`]'s module doc
    /// for why the platform's and the OSS engine's current `MemTable`-backed
    /// readers don't expose one natively.
    pub bytes_scanned: u64,
    /// Total rows scanned (raw, pre-filter), summed across every table this
    /// query read.
    pub rows_scanned: u64,
}

/// Walk `plan` and every descendant, summing the `bytes_scanned` metric
/// (and the co-located `output_rows` metric on the same node) wherever a
/// [`crate::physical::scan_metrics_exec::ScanMetricsExec`] — or any future
/// scan node that reports a metric of that name — appears in the tree.
///
/// Metrics are recorded per-partition and DataFusion's `MetricsSet::sum_by_name`
/// already aggregates across partitions for a single node, so this only needs
/// to sum across NODES, not partitions.
fn scan_stats_from_plan(plan: &dyn ExecutionPlan) -> QueryStats {
    let mut stats = QueryStats::default();
    accumulate_scan_stats(plan, &mut stats);
    stats
}

fn accumulate_scan_stats(plan: &dyn ExecutionPlan, stats: &mut QueryStats) {
    if let Some(metrics) = plan.metrics() {
        if let Some(bytes) = metrics.sum_by_name(BYTES_SCANNED_METRIC) {
            stats.bytes_scanned += bytes.as_usize() as u64;
            if let Some(rows) = metrics.output_rows() {
                stats.rows_scanned += rows as u64;
            }
        }
    }
    for child in plan.children() {
        accumulate_scan_stats(child.as_ref(), stats);
    }
}

/// Build a [`SessionContext`] configured to run **governed** queries against a
/// [`crate::contract_table_provider::ContractTableProvider`].
///
/// It is the DEFAULT DataFusion session with one change: the `ProjectionPushdown`
/// physical-optimizer rule is removed (see [`PROJECTION_PUSHDOWN_RULE`]). Any
/// caller registering a `ContractTableProvider` MUST plan through a context built
/// here (not a bare `SessionContext::new()`), or a masked numeric column will
/// trip the generic projection pushdown's schema check. Exposed so tests can
/// exercise the exact planning path the engine uses.
pub fn governed_session_context(config: SessionConfig) -> SessionContext {
    let physical_rules = PhysicalOptimizer::default()
        .rules
        .into_iter()
        .filter(|rule| rule.name() != PROJECTION_PUSHDOWN_RULE)
        .collect::<Vec<_>>();

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rules(physical_rules)
        .build();
    SessionContext::new_with_state(state)
}
