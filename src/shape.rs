//! Shapes that act while a plan runs. `parcel_runtime::shape` rewrites the caller's logical
//! plan (group suppression, aggregate noise); what it cannot express as a rewrite is here, as
//! a physical operator, so a planned query carries every shape wherever it is executed.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};

/// `suppress` over a query with no aggregate: the whole result is one group, so a result of
/// fewer than `k` rows is withheld entirely. The operator holds at most the first `k` rows
/// before it releases anything, then streams the rest; it runs over one partition, since the
/// count is the whole result's.
#[derive(Debug)]
pub struct SuppressExec {
    pub k: u64,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl SuppressExec {
    /// Suppress `input`'s whole result below `k` rows, merging its partitions first.
    pub fn new(k: u64, input: Arc<dyn ExecutionPlan>) -> SuppressExec {
        let input: Arc<dyn ExecutionPlan> = if input.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(input))
        } else {
            input
        };
        let properties = input.properties().clone();
        SuppressExec {
            k,
            input,
            properties,
        }
    }
}

impl DisplayAs for SuppressExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SuppressExec: k={}", self.k)
    }
}

impl ExecutionPlan for SuppressExec {
    fn name(&self) -> &str {
        "SuppressExec"
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
        Ok(Arc::new(SuppressExec::new(self.k, children.swap_remove(0))))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let head = hold_first(input, self.k as usize);
        let stream = futures::stream::once(head).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// The input's first `k` rows held back, then released with the rest of the stream; nothing at
/// all if the input ends before `k` rows.
async fn hold_first(
    mut input: SendableRecordBatchStream,
    k: usize,
) -> DFResult<BoxStream<'static, DFResult<RecordBatch>>> {
    let mut held = Vec::new();
    let mut rows = 0usize;
    while rows < k {
        match input.next().await {
            Some(b) => {
                let b = b?;
                rows += b.num_rows();
                held.push(b);
            }
            None => return Ok(futures::stream::empty().boxed()),
        }
    }
    Ok(futures::stream::iter(held.into_iter().map(Ok))
        .chain(input)
        .boxed())
}
