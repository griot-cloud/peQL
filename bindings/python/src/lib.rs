//! PyO3 bindings for peQL. The native module moves data as Arrow IPC bytes; the Python
//! wrapper (`python/peql/__init__.py`) turns them into pyarrow objects, so the native side has
//! no pyarrow version coupling.

use std::io::Cursor;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::reader::StreamReader;
use pyo3::exceptions::{PyPermissionError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use peql::format::{ResultFormat, ResultFormatter};
use peql::{PeqlError, WriteMode};

/// Who runs a query, as the application authenticated them.
#[pyclass]
#[derive(Clone)]
struct Caller {
    inner: peql::Caller,
}

#[pymethods]
impl Caller {
    #[new]
    #[pyo3(signature = (id, purpose, tenant, tier=None, classification=None, roles=None, clearance=None))]
    fn new(
        id: String,
        purpose: String,
        tenant: String,
        tier: Option<String>,
        classification: Option<String>,
        roles: Option<Vec<String>>,
        clearance: Option<i64>,
    ) -> Self {
        let mut inner = peql::Caller::new(&id, &tenant, &purpose);
        inner.tier = tier.unwrap_or_default();
        inner.classification = classification.unwrap_or_default();
        inner.roles = roles.unwrap_or_default();
        inner.clearance = clearance.unwrap_or_default();
        Caller { inner }
    }

    fn __repr__(&self) -> String {
        format!(
            "Caller(id='{}', purpose='{}', tenant='{}')",
            self.inner.id, self.inner.purpose, self.inner.tenant
        )
    }
}

#[pyclass]
struct Engine {
    inner: peql::Engine,
    rt: tokio::runtime::Runtime,
}

fn to_py(e: PeqlError) -> PyErr {
    if e.is_refusal() {
        PyPermissionError::new_err(e.to_string())
    } else {
        PyRuntimeError::new_err(e.to_string())
    }
}

fn err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

fn read_ipc(bytes: &[u8]) -> PyResult<(Schema, Vec<RecordBatch>)> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None).map_err(err)?;
    let schema = reader.schema().as_ref().clone();
    let batches = reader.collect::<Result<Vec<_>, _>>().map_err(err)?;
    Ok((schema, batches))
}

#[pymethods]
impl Engine {
    /// A workspace on disk.
    #[staticmethod]
    fn open(root: String) -> PyResult<Self> {
        Ok(Engine {
            inner: peql::Engine::open(&root).map_err(to_py)?,
            rt: tokio::runtime::Runtime::new().map_err(err)?,
        })
    }

    /// Everything in memory; relative bindings resolve under `base`.
    #[staticmethod]
    fn in_memory(base: String) -> PyResult<Self> {
        Ok(Engine {
            inner: peql::Engine::in_memory(base),
            rt: tokio::runtime::Runtime::new().map_err(err)?,
        })
    }

    /// Compile a contract document against a schema (an Arrow IPC stream) and register it.
    fn register(&self, source: String, schema_ipc: &[u8]) -> PyResult<String> {
        let (schema, _) = read_ipc(schema_ipc)?;
        let reg = self.inner.register_contract(&source, &schema).map_err(to_py)?;
        Ok(reg.name().to_owned())
    }

    /// Register a parcel bundle (JSON, from `parcel compile -o`).
    fn register_bundle(&self, bundle_json: String) -> PyResult<String> {
        let bundle = parcel_runtime::bundle::Bundle::from_json(&bundle_json).map_err(err)?;
        let reg = self.inner.register_bundle(&bundle).map_err(to_py)?;
        Ok(reg.name().to_owned())
    }

    /// Write an Arrow IPC stream under a contract; returns the report as JSON.
    #[pyo3(signature = (name, data_ipc, append=false))]
    fn write(&self, py: Python<'_>, name: String, data_ipc: &[u8], append: bool) -> PyResult<String> {
        let (_, batches) = read_ipc(data_ipc)?;
        let mode = if append { WriteMode::Append } else { WriteMode::Overwrite };
        let report = py
            .allow_threads(|| self.rt.block_on(self.inner.write(&name, batches, mode)))
            .map_err(to_py)?;
        serde_json::to_string(&serde_json::json!({
            "rows_written": report.rows_written,
            "files": report.files,
            "verdict": report.verdict,
        }))
        .map_err(err)
    }

    /// The validation verdict as JSON.
    fn validate(&self, py: Python<'_>, name: String) -> PyResult<String> {
        let v = py
            .allow_threads(|| self.rt.block_on(self.inner.validate(&name)))
            .map_err(to_py)?;
        serde_json::to_string(&v).map_err(err)
    }

    /// Run `sql` as `caller`: the result as Arrow IPC file bytes, and the envelope as JSON.
    fn query<'py>(&self, py: Python<'py>, sql: String, caller: &Caller) -> PyResult<(Bound<'py, PyBytes>, String)> {
        let caller = caller.inner.clone();
        let res = py
            .allow_threads(|| self.rt.block_on(self.inner.query(&sql, &caller)))
            .map_err(to_py)?;
        let ipc = ResultFormatter::format_results(&res.batches, ResultFormat::Arrow).map_err(err)?;
        let envelope = serde_json::to_string(&res.envelope).map_err(err)?;
        Ok((PyBytes::new(py, &ipc), envelope))
    }

    /// What a caller would see: (column, type) pairs.
    fn describe(&self, name: String, caller: &Caller) -> PyResult<Vec<(String, String)>> {
        let schema = self.inner.describe(&name, &caller.inner).map_err(to_py)?;
        Ok(schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().to_string()))
            .collect())
    }

    /// Share a contract with a tenant, or with everyone (`"public"`).
    fn publish(&self, name: String, audience: String) -> PyResult<()> {
        self.inner.publish(&name, &audience).map_err(to_py)
    }

    fn unpublish(&self, name: String, audience: String) -> PyResult<()> {
        self.inner.unpublish(&name, &audience).map_err(to_py)
    }

    /// Names of every registered contract.
    fn contracts(&self) -> Vec<String> {
        self.inner.contracts().iter().map(|r| r.name().to_owned()).collect()
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Engine>()?;
    m.add_class::<Caller>()?;
    Ok(())
}
