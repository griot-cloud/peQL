//! Lance datasets as contract data (feature `lance`).
//!
//! [`LanceTableProvider`] reads a Lance dataset from any URI Lance understands (a local
//! directory, S3, ...). Scans stream batch by batch; projections, limits and the filters
//! Lance can evaluate are handed to Lance, and DataFusion re-checks the filters. Lance builds on an older Arrow than peQL, so batches cross by Arrow IPC.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use futures::{StreamExt, TryStreamExt};
use lance::Dataset;
use lance::deps::arrow_array::RecordBatch as LanceBatch;

#[derive(Debug, thiserror::Error)]
pub enum LanceTableError {
    #[error("lance: {0}")]
    Lance(String),
    #[error("arrow bridge: {0}")]
    Bridge(String),
}

// ── Arrow bridge: Lance's Arrow and peQL's, by IPC ─────────────────────────────

fn to_peql(batch: &LanceBatch) -> Result<RecordBatch, LanceTableError> {
    let mut buf = Vec::new();
    {
        let mut w = arrow_ipc_lance::writer::StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| LanceTableError::Bridge(e.to_string()))?;
        w.write(batch)
            .map_err(|e| LanceTableError::Bridge(e.to_string()))?;
        w.finish()
            .map_err(|e| LanceTableError::Bridge(e.to_string()))?;
    }
    let mut r =
        datafusion::arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(buf), None)
            .map_err(|e| LanceTableError::Bridge(e.to_string()))?;
    r.next()
        .ok_or_else(|| LanceTableError::Bridge("empty IPC stream".into()))?
        .map_err(|e| LanceTableError::Bridge(e.to_string()))
}

fn schema_to_peql(
    schema: &lance::deps::arrow_schema::Schema,
) -> Result<SchemaRef, LanceTableError> {
    let mut buf = Vec::new();
    {
        let mut w = arrow_ipc_lance::writer::StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| LanceTableError::Bridge(e.to_string()))?;
        w.finish()
            .map_err(|e| LanceTableError::Bridge(e.to_string()))?;
    }
    let r = datafusion::arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(buf), None)
        .map_err(|e| LanceTableError::Bridge(e.to_string()))?;
    Ok(r.schema())
}

// ── The table ───────────────────────────────────────────────────────────────────

pub struct LanceTableProvider {
    dataset: Arc<Dataset>,
    schema: SchemaRef,
}

impl fmt::Debug for LanceTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LanceTableProvider({})", self.dataset.uri())
    }
}

impl LanceTableProvider {
    /// A dataset at any URI Lance reads: `/data/users.lance`, `s3://bucket/users.lance`, ...
    pub async fn open_uri(uri: &str) -> Result<Self, LanceTableError> {
        let dataset = Dataset::open(uri)
            .await
            .map_err(|e| LanceTableError::Lance(e.to_string()))?;
        Self::from_dataset(dataset)
    }

    fn from_dataset(dataset: Dataset) -> Result<Self, LanceTableError> {
        let arrow: lance::deps::arrow_schema::Schema = dataset.schema().into();
        Ok(LanceTableProvider {
            schema: schema_to_peql(&arrow)?,
            dataset: Arc::new(dataset),
        })
    }
}

#[async_trait]
impl TableProvider for LanceTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Lance prunes with what it can evaluate; DataFusion re-checks every filter.
        Ok(filters
            .iter()
            .map(|f| {
                if filter_sql(f).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let filter = filters
            .iter()
            .filter_map(filter_sql)
            .map(|f| format!("({f})"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let partition = Arc::new(LanceScan {
            dataset: self.dataset.clone(),
            schema: schema.clone(),
            columns,
            filter: (!filter.is_empty()).then_some(filter),
            limit,
        });
        Ok(Arc::new(StreamingTableExec::try_new(
            schema,
            vec![partition],
            None,
            vec![],
            false,
            limit,
        )?))
    }
}

/// A filter as SQL Lance can evaluate, when it cannot fail and so means the same in both engines.
fn filter_sql(e: &Expr) -> Option<String> {
    if !crate::gate::cannot_fail(e) {
        return None;
    }
    datafusion::sql::unparser::expr_to_sql(e)
        .ok()
        .map(|s| s.to_string())
}

#[derive(Debug)]
struct LanceScan {
    dataset: Arc<Dataset>,
    schema: SchemaRef,
    columns: Vec<String>,
    filter: Option<String>,
    limit: Option<usize>,
}

impl PartitionStream for LanceScan {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let dataset = self.dataset.clone();
        let columns = self.columns.clone();
        let filter = self.filter.clone();
        let limit = self.limit;
        let lance_err = |e: lance::Error| DataFusionError::External(Box::new(e));
        let opened = futures::stream::once(async move {
            let mut scanner = dataset.scan();
            scanner.project(&columns).map_err(lance_err)?;
            if let Some(f) = &filter {
                scanner.filter(f).map_err(lance_err)?;
            }
            if let Some(n) = limit {
                scanner.limit(Some(n as i64), None).map_err(lance_err)?;
            }
            let stream = scanner.try_into_stream().await.map_err(lance_err)?;
            Ok::<_, DataFusionError>(stream.map(move |b| {
                let b = b.map_err(lance_err)?;
                to_peql(&b).map_err(|e| DataFusionError::External(Box::new(e)))
            }))
        })
        .try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), opened))
    }
}
