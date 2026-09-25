//! Lance datasets as contract data (feature `lance`).
//!
//! [`LanceTableProvider`] reads a Lance dataset from any URI Lance understands (a local
//! directory, S3, ...) or, on the Griot platform, through the T04 storaged socket, where every
//! object read carries its path and is checked by T04. Scans stream batch by batch; projections,
//! limits and the filters Lance can evaluate are handed to Lance, and DataFusion re-checks the
//! filters. Lance builds on an older Arrow than peQL, so batches cross by Arrow IPC.

use std::fmt;
use std::ops::Range;
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
use object_store::path::Path as ObjectPath;

use crate::storaged_client::StoragedClient;

#[derive(Debug, thiserror::Error)]
pub enum LanceTableError {
    #[error("storaged: {0}")]
    Storaged(#[from] crate::storaged_client::StoragedError),
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

    /// A dataset served by T04: every object is read through the storaged socket, with its path.
    pub async fn open(
        asset_id: &str,
        tenant_id: &str,
        principal_jwt: &str,
        storaged_socket: &str,
    ) -> Result<Self, LanceTableError> {
        let provider = Arc::new(StoragedProvider {
            client: StoragedClient::new(storaged_socket),
            tenant_id: tenant_id.to_owned(),
            principal_jwt: principal_jwt.to_owned(),
        });
        let registry = lance_io::object_store::ObjectStoreRegistry::default();
        registry.insert(STORAGED_SCHEME, provider);
        let session = Arc::new(lance::session::Session::new(
            0,
            64 * 1024 * 1024,
            Arc::new(registry),
        ));
        let dataset = lance::dataset::builder::DatasetBuilder::from_uri(format!(
            "{STORAGED_SCHEME}://{asset_id}/"
        ))
        .with_session(session)
        .load()
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

// ── storaged as an object store ────────────────────────────────────────────────

const STORAGED_SCHEME: &str = "storaged";

#[derive(Debug)]
struct StoragedProvider {
    client: StoragedClient,
    tenant_id: String,
    principal_jwt: String,
}

#[async_trait]
impl lance_io::object_store::providers::ObjectStoreProvider for StoragedProvider {
    async fn new_store(
        &self,
        base: url::Url,
        params: &lance_io::object_store::ObjectStoreParams,
    ) -> lance::Result<lance_io::object_store::ObjectStore> {
        let asset_id = base.host_str().unwrap_or_default().to_owned();
        let store = Arc::new(StoragedObjectStore {
            client: self.client.clone(),
            asset_id,
            tenant_id: self.tenant_id.clone(),
            principal_jwt: self.principal_jwt.clone(),
        });
        Ok(lance_io::object_store::ObjectStore::new(
            store,
            base,
            params.block_size,
            None,
            false,
            true,
            8,
            3,
            None,
        ))
    }

    fn extract_path(&self, url: &url::Url) -> lance::Result<ObjectPath> {
        Ok(ObjectPath::from(url.path().trim_start_matches('/')))
    }
}

/// Read-only access to the objects of one T04 asset, each by its path.
#[derive(Debug)]
struct StoragedObjectStore {
    client: StoragedClient,
    asset_id: String,
    tenant_id: String,
    principal_jwt: String,
}

impl fmt::Display for StoragedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "storaged://{}", self.asset_id)
    }
}

fn os_err(e: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic {
        store: "storaged",
        source: Box::new(e),
    }
}

fn read_only() -> object_store::Error {
    object_store::Error::NotSupported {
        source: "storaged assets are read-only".into(),
    }
}

impl StoragedObjectStore {
    async fn meta(&self, location: &ObjectPath) -> object_store::Result<object_store::ObjectMeta> {
        let stat = self
            .client
            .stat_object(
                &self.asset_id,
                Some(location.as_ref()),
                &self.tenant_id,
                &self.principal_jwt,
            )
            .await
            .map_err(os_err)?;
        Ok(object_store::ObjectMeta {
            location: location.clone(),
            last_modified: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
            size: stat.size,
            e_tag: None,
            version: None,
        })
    }

    async fn read(
        &self,
        location: &ObjectPath,
        range: Range<u64>,
    ) -> object_store::Result<bytes::Bytes> {
        self.client
            .read_object(
                &self.asset_id,
                Some(location.as_ref()),
                range.start,
                range.end - range.start,
                &self.tenant_id,
                &self.principal_jwt,
            )
            .await
            .map_err(os_err)
    }
}

#[async_trait]
impl object_store::ObjectStore for StoragedObjectStore {
    async fn put_opts(
        &self,
        _location: &ObjectPath,
        _payload: object_store::PutPayload,
        _opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        Err(read_only())
    }

    async fn put_multipart_opts(
        &self,
        _location: &ObjectPath,
        _opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        Err(read_only())
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let meta = self.meta(location).await?;
        let range = match options.range {
            Some(object_store::GetRange::Bounded(r)) => r.start..r.end.min(meta.size),
            Some(object_store::GetRange::Offset(o)) => o..meta.size,
            Some(object_store::GetRange::Suffix(n)) => meta.size.saturating_sub(n)..meta.size,
            None => 0..meta.size,
        };
        let bytes = if options.head {
            bytes::Bytes::new()
        } else {
            self.read(location, range.clone()).await?
        };
        Ok(object_store::GetResult {
            payload: object_store::GetResultPayload::Stream(
                futures::stream::once(async move { Ok(bytes) }).boxed(),
            ),
            meta,
            range,
            attributes: Default::default(),
            extensions: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectPath>> {
        locations.map(|_| Err(read_only())).boxed()
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        let client = self.client.clone();
        let (asset, tenant, jwt) = (
            self.asset_id.clone(),
            self.tenant_id.clone(),
            self.principal_jwt.clone(),
        );
        let prefix = prefix.map(|p| p.to_string()).unwrap_or_default();
        futures::stream::once(async move {
            let objects = client
                .list_objects(&asset, &prefix, &tenant, &jwt)
                .await
                .map_err(os_err)?;
            Ok::<_, object_store::Error>(futures::stream::iter(objects.into_iter().map(|o| {
                Ok(object_store::ObjectMeta {
                    location: ObjectPath::from(o.path),
                    last_modified: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
                    size: o.size,
                    e_tag: None,
                    version: None,
                })
            })))
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<object_store::ListResult> {
        let all: Vec<object_store::ObjectMeta> = self.list(prefix).try_collect().await?;
        let base = prefix.map(|p| format!("{p}/")).unwrap_or_default();
        let mut objects = Vec::new();
        let mut common_prefixes = std::collections::BTreeSet::new();
        for m in all {
            let rest = m
                .location
                .as_ref()
                .strip_prefix(base.as_str())
                .unwrap_or(m.location.as_ref())
                .to_owned();
            match rest.split_once('/') {
                Some((dir, _)) => {
                    common_prefixes.insert(ObjectPath::from(format!("{base}{dir}")));
                }
                None => objects.push(m),
            }
        }
        Ok(object_store::ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
            extensions: Default::default(),
        })
    }

    async fn copy_opts(
        &self,
        _from: &ObjectPath,
        _to: &ObjectPath,
        _options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        Err(read_only())
    }
}
