//! `ScanMetricsExec` — records the raw (pre-enforcement) scan volume of a
//! table read: total rows and total bytes that flowed out of the raw
//! `TableProvider::scan()` plan, before `ContractApprovedExec` /
//! `RowFilterExec` / `MaskingExec` narrow it down.
//!
//! # Why this exists (ADR-0052 follow-up #42)
//!
//! K04D's cost-metering emitter needs a "bytes scanned" figure to charge
//! query credits on the actual I/O a query performed, not on the size of the
//! (already filtered/masked/limited) *result* it returned. DataFusion's own
//! physical operators expose scan volume via the standard [`MetricsSet`] —
//! but only for scan nodes that track it themselves (e.g. a file-format
//! `DataSourceExec` backed by `ParquetSource`, which records a per-file
//! `bytes_scanned` counter as bytes are read off the object store).
//!
//! Neither the platform's Iceberg reader (`iceberg_source.rs` in the K04D
//! shell) nor this crate's own open-source [`crate::binding::load_parquet_as_provider`]
//! use that path today: both read every data/Parquet file fully into memory
//! and hand DataFusion a plain [`datafusion::datasource::MemTable`] — and
//! `MemTable`'s physical `DataSourceExec` (`MemorySourceConfig`) never
//! overrides `DataSource::metrics()`, so it reports an always-empty
//! `MetricsSet` (verified against datafusion 47's source). There is nothing
//! to "read off" the plan for either the platform's or the open-source
//! engine's actual read path today.
//!
//! `ScanMetricsExec` closes that gap generically, for **any** `BindingResolver`
//! implementation (open-source or platform — per the "engine serves both
//! OSS and platform" law, this is one code path, not two). It sits directly
//! above the raw `inner.scan()` plan in [`crate::contract_table_provider::ContractTableProvider::scan`],
//! below every contract-enforcement operator, so every batch the raw scan
//! produces is counted exactly once: rows via the standard `output_rows`
//! metric, bytes via a `bytes_scanned` counter (the batch's Arrow in-memory
//! size, [`datafusion::arrow::record_batch::RecordBatch::get_array_memory_size`],
//! summed across every batch the raw scan produced).
//!
//! If a future `BindingResolver` DOES back onto a real streaming scan node
//! that tracks its own native `bytes_scanned` (e.g. a `ParquetExec`-backed
//! Iceberg reader), that node's metric coexists with this one at a distinct
//! plan node — [`crate::engine::GriotEngine::query_with_stats`] sums by
//! metric NAME across the whole physical-plan tree, so every scan node that
//! reports `bytes_scanned` contributes, with no double counting (this
//! operator wraps the scan node directly; the two are never both present
//! for the same table today, but the aggregation is correct either way).
//!
//! This deliberately measures the RAW scan, not the enforced result:
//! `RowFilterExec`, `MaskingExec`, the query's own projection and `LIMIT`
//! all run ABOVE this node. A `SELECT count(*)` or a highly selective filter
//! still reports the full underlying table's scan volume — the correct
//! billing semantics ("what did the engine have to read to answer this
//! query", not "what did it hand back").

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

/// The metric name [`crate::engine::GriotEngine::query_with_stats`] sums
/// across the physical plan tree to produce
/// [`crate::engine::QueryStats::bytes_scanned`].
pub const BYTES_SCANNED_METRIC: &str = "bytes_scanned";

/// Physical operator that records the raw scan volume (rows + bytes) of the
/// plan it wraps, as a pure passthrough — it changes no data, only counts it.
#[derive(Debug)]
pub struct ScanMetricsExec {
    inner: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl ScanMetricsExec {
    /// Wrap `inner` (the raw, ungoverned scan plan) to record its scan volume.
    pub fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
        let properties = inner.properties().clone();
        Self {
            inner,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for ScanMetricsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ScanMetricsExec")
    }
}

impl ExecutionPlan for ScanMetricsExec {
    fn name(&self) -> &str {
        "ScanMetricsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let inner = children.into_iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "ScanMetricsExec::with_new_children requires exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(ScanMetricsExec::new(inner)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let inner_stream = self.inner.execute(partition, context)?;
        let schema = self.schema();

        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let bytes_scanned =
            MetricBuilder::new(&self.metrics).counter(BYTES_SCANNED_METRIC, partition);

        let stream = inner_stream.map(move |batch_result| {
            if let Ok(batch) = &batch_result {
                output_rows.add(batch.num_rows());
                bytes_scanned.add(batch.get_array_memory_size());
            }
            batch_result
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    fn sample_batch(n: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from((0..n).collect::<Vec<_>>()))],
        )
        .unwrap()
    }

    /// `ScanMetricsExec` wrapping a `MemTable` scan (the platform's + the
    /// open-source engine's actual production shape today) reports non-zero
    /// `output_rows` and `bytes_scanned` after execution, even though the
    /// wrapped `MemTable` plan itself reports none (proves the gap this
    /// operator closes, per the module doc).
    #[tokio::test]
    async fn records_rows_and_bytes_over_a_memtable_scan() {
        let batch = sample_batch(5);
        let schema = batch.schema();
        let mem = MemTable::try_new(schema, vec![vec![batch.clone()]]).unwrap();

        let ctx = SessionContext::new();
        let inner_plan = mem.scan(&ctx.state(), None, &[], None).await.unwrap();

        // Confirm the inner MemTable plan itself has no scan metrics — the
        // documented gap this operator closes.
        let inner_metrics = inner_plan.metrics();
        assert!(
            inner_metrics.is_none() || inner_metrics.unwrap().output_rows().is_none(),
            "MemTable-backed DataSourceExec unexpectedly reports output_rows; \
             re-check the datafusion version-specific assumption in the module docs"
        );

        let wrapped: Arc<dyn ExecutionPlan> = Arc::new(ScanMetricsExec::new(inner_plan));
        let task_ctx = ctx.task_ctx();
        let batches = collect(wrapped.clone(), task_ctx).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 5);

        let metrics = wrapped
            .metrics()
            .expect("ScanMetricsExec always reports metrics");
        assert_eq!(metrics.output_rows(), Some(5));
        let bytes = metrics
            .sum_by_name(BYTES_SCANNED_METRIC)
            .expect("bytes_scanned metric present")
            .as_usize();
        assert_eq!(bytes, batch.get_array_memory_size());
        assert!(bytes > 0, "sample batch has non-zero Arrow memory size");
    }

    /// A scan that produces zero batches (empty table) yields metrics of
    /// exactly zero, not an absent metric — the engine's summing code must
    /// not be confused by "0" vs "no scan happened at all".
    #[tokio::test]
    async fn empty_scan_reports_zero_not_absent() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mem = MemTable::try_new(schema, vec![vec![]]).unwrap();

        let ctx = SessionContext::new();
        let inner_plan = mem.scan(&ctx.state(), None, &[], None).await.unwrap();
        let wrapped: Arc<dyn ExecutionPlan> = Arc::new(ScanMetricsExec::new(inner_plan));
        let task_ctx = ctx.task_ctx();
        let batches = collect(wrapped.clone(), task_ctx).await.unwrap();
        assert!(batches.iter().all(|b| b.num_rows() == 0));

        let metrics = wrapped.metrics().unwrap();
        assert_eq!(metrics.output_rows(), Some(0));
        assert_eq!(
            metrics
                .sum_by_name(BYTES_SCANNED_METRIC)
                .unwrap()
                .as_usize(),
            0
        );
    }
}
