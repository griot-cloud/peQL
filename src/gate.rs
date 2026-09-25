//! The gate (peQL design 4.6). Every contract's view ends in a `Gate` node, which the planner
//! turns into [`GateExec`]: a pass-through operator that proves the view was built and counts
//! what leaves it. The engine refuses to run a physical plan missing a gate for any contract
//! the query named ([`ensure_gated`]).
//!
//! The gate is also a barrier for the caller's predicates. A predicate pushed below the
//! contract's filter could raise an error on a row the contract hides, and the error would
//! reveal that the row exists. So only predicates that cannot fail (comparisons, boolean
//! logic, `IN` lists, `IS NULL`, `LIKE`) cross the gate, where they reach the scan and prune;
//! everything else is evaluated above it, on rows the caller may see.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, Result as DFResult};
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::expr::{InList, Like};
use datafusion::logical_expr::utils::{conjunction, split_conjunction};
use datafusion::logical_expr::{
    Expr, Extension, Filter, LogicalPlan, Operator, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use futures::StreamExt;

/// The logical gate: the top of one contract's view.
#[derive(Clone, Debug)]
pub struct Gate {
    pub contract: String,
    pub compilation_hash: String,
    pub input: LogicalPlan,
}

impl PartialEq for Gate {
    fn eq(&self, o: &Self) -> bool {
        self.contract == o.contract
            && self.compilation_hash == o.compilation_hash
            && self.input == o.input
    }
}
impl Eq for Gate {}
impl Hash for Gate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.contract.hash(state);
        self.compilation_hash.hash(state);
        self.input.hash(state);
    }
}
impl PartialOrd for Gate {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        (&self.contract, &self.compilation_hash).partial_cmp(&(&o.contract, &o.compilation_hash))
    }
}

impl Gate {
    pub fn plan(contract: &str, compilation_hash: &str, input: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(Gate {
                contract: contract.to_owned(),
                compilation_hash: compilation_hash.to_owned(),
                input,
            }),
        })
    }
}

impl UserDefinedLogicalNodeCore for Gate {
    fn name(&self) -> &str {
        "Gate"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Gate: contract={}", self.contract)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        Ok(Gate {
            contract: self.contract.clone(),
            compilation_hash: self.compilation_hash.clone(),
            input: inputs.swap_remove(0),
        })
    }
    /// Pass-through: the optimiser may prune columns the caller never reads.
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        Some(vec![output_columns.to_vec()])
    }
    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

/// Moves the caller's predicates that cannot fail from above a gate to below it. DataFusion's
/// own filter pushdown stops at the gate (it keeps every column), so nothing else crosses.
#[derive(Debug, Default)]
pub struct GateBarrier;

impl OptimizerRule for GateBarrier {
    fn name(&self) -> &str {
        "peql_gate_barrier"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform_up_with_subqueries(|node| {
            let LogicalPlan::Filter(f) = &node else {
                return Ok(Transformed::no(node));
            };
            let LogicalPlan::Extension(ext) = f.input.as_ref() else {
                return Ok(Transformed::no(node));
            };
            let Some(gate) = ext.node.as_any().downcast_ref::<Gate>() else {
                return Ok(Transformed::no(node));
            };
            let (safe, rest): (Vec<&Expr>, Vec<&Expr>) = split_conjunction(&f.predicate)
                .into_iter()
                .partition(|e| cannot_fail(e));
            let Some(below) = conjunction(safe.into_iter().cloned()) else {
                return Ok(Transformed::no(node));
            };
            let inner = LogicalPlan::Filter(Filter::try_new(below, Arc::new(gate.input.clone()))?);
            let gated = Gate::plan(&gate.contract, &gate.compilation_hash, inner);
            let out = match conjunction(rest.into_iter().cloned()) {
                Some(above) => LogicalPlan::Filter(Filter::try_new(above, Arc::new(gated))?),
                None => gated,
            };
            Ok(Transformed::new(out, true, TreeNodeRecursion::Continue))
        })
    }
}

/// True for an expression that cannot raise an error on any input.
pub fn cannot_fail(e: &Expr) -> bool {
    let mut ok = true;
    let _ = e.apply(|x| {
        ok &= match x {
            Expr::Column(_)
            | Expr::Literal(..)
            | Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::TryCast(_)
            | Expr::Between(_) => true,
            Expr::BinaryExpr(b) => matches!(
                b.op,
                Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::And
                    | Operator::Or
                    | Operator::IsDistinctFrom
                    | Operator::IsNotDistinctFrom
            ),
            Expr::InList(InList { list, .. }) => {
                list.iter().all(|l| matches!(l, Expr::Literal(..)))
            }
            Expr::Like(Like { escape_char, .. }) => escape_char.is_none(),
            // A cast of a constant is folded before execution.
            Expr::Cast(c) => matches!(c.expr.as_ref(), Expr::Literal(..)),
            _ => false,
        };
        Ok(if ok {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    });
    ok
}

/// The physical gate: passes batches through and counts them.
#[derive(Debug)]
pub struct GateExec {
    pub contract: String,
    pub compilation_hash: String,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl GateExec {
    pub fn new(contract: String, compilation_hash: String, input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = input.properties().clone();
        GateExec {
            contract,
            compilation_hash,
            input,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for GateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GateExec: contract={}", self.contract)
    }
}

impl ExecutionPlan for GateExec {
    fn name(&self) -> &str {
        "GateExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GateExec::new(
            self.contract.clone(),
            self.compilation_hash.clone(),
            children.swap_remove(0),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let stream = input.map(move |b| {
            let b = b?;
            baseline.record_output(b.num_rows());
            Ok(b)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct GatePlanner;

#[async_trait]
impl ExtensionPlanner for GatePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &dyn datafusion::catalog::Session,
        _planning_ctx: &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        Ok(node.as_any().downcast_ref::<Gate>().map(|g| {
            Arc::new(GateExec::new(
                g.contract.clone(),
                g.compilation_hash.clone(),
                physical_inputs[0].clone(),
            )) as Arc<dyn ExecutionPlan>
        }))
    }
}

/// The query planner peQL sessions use: DataFusion's, plus gates.
#[derive(Debug)]
pub struct GatedQueryPlanner;

#[async_trait]
impl QueryPlanner for GatedQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &dyn datafusion::catalog::Session,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(GatePlanner)])
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Contract data as the view reads it: the binding's table, with every scan marked by a
/// [`ScanExec`] so [`ensure_gated`] can prove no contract data is read outside a gate.
#[derive(Debug)]
pub struct BoundTable {
    pub contract: String,
    pub inner: Arc<dyn datafusion::catalog::TableProvider>,
}

#[async_trait]
impl datafusion::catalog::TableProvider for BoundTable {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
    fn table_type(&self) -> datafusion::datasource::TableType {
        datafusion::datasource::TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(ScanExec::new(self.contract.clone(), inner)))
    }
}

/// Marks a scan of contract data and counts what it reads: rows, and bytes as Arrow holds them.
/// Parquet scans report their own I/O besides.
#[derive(Debug)]
pub struct ScanExec {
    pub contract: String,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

/// The metric [`ScanExec`] records bytes under.
pub const SCAN_BYTES: &str = "scan_bytes";

impl ScanExec {
    pub fn new(contract: String, input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = input.properties().clone();
        ScanExec {
            contract,
            input,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for ScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ScanExec: contract={}", self.contract)
    }
}

impl ExecutionPlan for ScanExec {
    fn name(&self) -> &str {
        "ScanExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ScanExec::new(
            self.contract.clone(),
            children.swap_remove(0),
        )))
    }
    /// Transparent to filters from below the gate, so they reach the Parquet scan and prune.
    fn gather_filters_for_pushdown(
        &self,
        _phase: datafusion::physical_plan::filter_pushdown::FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        _config: &datafusion::config::ConfigOptions,
    ) -> DFResult<datafusion::physical_plan::filter_pushdown::FilterDescription> {
        datafusion::physical_plan::filter_pushdown::FilterDescription::from_children(
            parent_filters,
            &self.children(),
        )
    }
    fn handle_child_pushdown_result(
        &self,
        _phase: datafusion::physical_plan::filter_pushdown::FilterPushdownPhase,
        child_pushdown_result: datafusion::physical_plan::filter_pushdown::ChildPushdownResult,
        _config: &datafusion::config::ConfigOptions,
    ) -> DFResult<
        datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation<
            Arc<dyn ExecutionPlan>,
        >,
    > {
        Ok(
            datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation::if_all(
                child_pushdown_result,
            ),
        )
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let bytes = datafusion::physical_plan::metrics::MetricBuilder::new(&self.metrics)
            .counter(SCAN_BYTES, partition);
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let stream = input.map(move |b| {
            let b = b?;
            baseline.record_output(b.num_rows());
            bytes.add(b.get_array_memory_size());
            Ok(b)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// The contracts whose gates appear in a physical plan.
pub fn gates(plan: &dyn ExecutionPlan) -> Vec<String> {
    let mut out = Vec::new();
    fn walk(p: &dyn ExecutionPlan, out: &mut Vec<String>) {
        if let Some(g) = p.downcast_ref::<GateExec>() {
            out.push(g.contract.clone());
        }
        for c in p.children() {
            walk(c.as_ref(), out);
        }
    }
    walk(plan, &mut out);
    out
}

/// Refuse a plan in which any scan of contract data is not under that contract's gate.
pub fn ensure_gated(plan: &dyn ExecutionPlan) -> crate::error::Result<()> {
    fn walk(p: &dyn ExecutionPlan, gate: Option<&str>) -> crate::error::Result<()> {
        let gate = match p.downcast_ref::<GateExec>() {
            Some(g) => Some(g.contract.as_str()),
            None => gate,
        };
        if let Some(s) = p.downcast_ref::<ScanExec>()
            && gate != Some(s.contract.as_str())
        {
            return Err(crate::error::PeqlError::Ungated(s.contract.clone()));
        }
        for c in p.children() {
            walk(c.as_ref(), gate)?;
        }
        Ok(())
    }
    walk(plan, None)
}
