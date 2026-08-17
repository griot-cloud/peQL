//! The governed snapshot handle, the per-engine graph cache, and governed
//! output assembly.
//!
//! [`GraphSession`] is the only way the SQL functions reach graph bytes:
//! `governed(graph_ref)` resolves the caller's contract **first** (deny =
//! uniform not-available, indistinguishable from unknown — no existence
//! oracle), then loads + verifies the bundle (cached per engine), then compiles
//! the caller's [`GraphPolicy`] (cached per policy fingerprint, so two callers
//! with different policies never share a governed structure — G02 R5).
//!
//! Output assembly reuses the engine's *real* enforcement operators: assembled
//! traversal batches are executed through `ContractApprovedExec → MaskingExec`,
//! so graph masking semantics are byte-identical to tabular masking.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, UInt64Array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use sha2::{Digest, Sha256};

use super::bundle::load_bundle;
use super::policy::compile_policy;
use super::types::{EdgeFile, GraphData, GraphPolicy};
use crate::binding::{BindingResolver, DatasetRef};
use crate::contract_source::{Caller, ContractError, ContractSource};
use crate::physical::contract_approved_exec::ContractApprovedExec;
use crate::physical::masking_exec::MaskingExec;
use crate::policy::Decision;

/// The uniform "not available" message (G02 R1/R7/T2): unknown graph and
/// denied graph are byte-identical, so denial is not an existence oracle.
pub fn unavailable_msg(graph_ref: &str) -> String {
    format!("graph dataset '{graph_ref}' is not available to this caller")
}

/// A caller's governed view of one loaded snapshot: immutable raw data + the
/// caller's compiled visibility overlay. Produced only through contract
/// resolution (no un-governed path).
#[derive(Debug, Clone)]
pub struct GovernedGraph {
    /// The shared, immutable raw snapshot.
    pub data: Arc<GraphData>,
    /// This caller's compiled governance overlay.
    pub policy: Arc<GraphPolicy>,
}

/// Per-engine cache of loaded snapshots and per-policy governed views (R5).
#[derive(Debug, Default)]
pub struct GraphCache {
    raw: Mutex<HashMap<PathBuf, Arc<GraphData>>>,
    governed: Mutex<HashMap<(PathBuf, String), Arc<GovernedGraph>>>,
}

impl GraphCache {
    /// Number of raw snapshots currently cached (test observability, T5).
    pub fn raw_len(&self) -> usize {
        self.raw.lock().unwrap().len()
    }

    /// Number of governed (per-policy) views currently cached.
    pub fn governed_len(&self) -> usize {
        self.governed.lock().unwrap().len()
    }
}

/// A caller-bound graph resolution session, handed to the SQL functions.
///
/// (Manual `Debug`: the source/binding seams are trait objects.)
pub struct GraphSession {
    /// The contract seam (same as tabular).
    pub source: Arc<dyn ContractSource>,
    /// The binding seam (locates the bundle directory).
    pub binding: Arc<dyn BindingResolver>,
    /// Who is asking.
    pub caller: Caller,
    /// The engine-lifetime cache.
    pub cache: Arc<GraphCache>,
}

impl std::fmt::Debug for GraphSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphSession")
            .field("caller", &self.caller.id)
            .finish()
    }
}

impl GraphSession {
    /// Resolve `graph_ref` for this caller into a governed snapshot.
    ///
    /// Order is contract-first, bytes-second (G02 §8.2): no bundle byte is read
    /// before the caller's policy is resolved and allows access.
    pub async fn governed(&self, graph_ref: &str) -> Result<Arc<GovernedGraph>, DataFusionError> {
        let dataset = DatasetRef::new(graph_ref);

        // 1. Contract first.
        let resolved = match self.source.resolve(&dataset, &self.caller).await {
            Ok(p) => p,
            Err(ContractError::NotFound(_)) => {
                return Err(DataFusionError::Plan(unavailable_msg(graph_ref)))
            }
            Err(e) => return Err(DataFusionError::Plan(format!("graph '{graph_ref}': {e}"))),
        };
        if let Decision::Deny { .. } = resolved.decision {
            // Byte-identical to the unknown-graph error (T2).
            return Err(DataFusionError::Plan(unavailable_msg(graph_ref)));
        }
        let resolved = Arc::new(resolved);

        // 2. Locate the bundle. (The contract exists and allows the caller, so
        //    disclosing "this isn't a graph" is not an oracle.)
        let dir = self.binding.resolve_graph_dir(&dataset).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "'{graph_ref}' is not a graph dataset (no graph_snapshot binding); \
                 query it as a table instead"
            ))
        })?;

        // 3. Policy fingerprint: the governed-structure cache key includes the
        //    governance identity, never just the snapshot (R5).
        let mut hasher = Sha256::new();
        hasher.update(resolved.to_bundle_bytes());
        hasher.update(
            resolved
                .graph_edge_filter
                .as_deref()
                .unwrap_or("")
                .as_bytes(),
        );
        if let Some(proj) = &resolved.projection {
            hasher.update(proj.join(",").as_bytes());
        }
        let fp = hex::encode(hasher.finalize());

        if let Some(g) = self
            .cache
            .governed
            .lock()
            .unwrap()
            .get(&(dir.clone(), fp.clone()))
        {
            return Ok(g.clone());
        }

        // 4. Raw snapshot (shared across callers; immutable).
        let data = {
            let cached = self.cache.raw.lock().unwrap().get(&dir).cloned();
            match cached {
                Some(d) => d,
                None => {
                    let d =
                        Arc::new(load_bundle(&dir).map_err(|e| {
                            DataFusionError::Plan(format!("graph '{graph_ref}': {e}"))
                        })?);
                    self.cache
                        .raw
                        .lock()
                        .unwrap()
                        .insert(dir.clone(), d.clone());
                    d
                }
            }
        };

        // 5. Caller's governed view.
        let gp = compile_policy(&data, resolved)
            .await
            .map_err(|e| DataFusionError::Plan(format!("graph '{graph_ref}': {e}")))?;
        let governed = Arc::new(GovernedGraph {
            data,
            policy: Arc::new(gp),
        });
        self.cache
            .governed
            .lock()
            .unwrap()
            .insert((dir, fp), governed.clone());
        Ok(governed)
    }
}

// ─── Governed output assembly ─────────────────────────────────────────────────

/// An extra output column to append after the entity columns.
pub struct ExtraCol {
    /// Column name.
    pub name: String,
    /// Values (row-aligned with the selected entity rows).
    pub array: ArrayRef,
}

impl GovernedGraph {
    /// The snapshot version (disclosed on every result, G02 R7).
    pub fn snapshot_version(&self) -> u64 {
        self.data.manifest.snapshot_version
    }

    fn take_columns(
        src: &RecordBatch,
        indices: &[i32],
    ) -> Result<(Vec<Field>, Vec<ArrayRef>), DataFusionError> {
        let idx = Int32Array::from(indices.to_vec());
        let mut fields = Vec::with_capacity(src.num_columns());
        let mut arrays = Vec::with_capacity(src.num_columns());
        for (i, field) in src.schema().fields().iter().enumerate() {
            let taken = take(src.column(i), &idx, None)?;
            fields.push(Field::new(field.name(), field.data_type().clone(), true));
            arrays.push(taken);
        }
        Ok((fields, arrays))
    }

    /// Assemble a governed **node-rows** batch: the full canonical node columns
    /// for `positions`, plus `extras`, plus the `snapshot_version` disclosure
    /// column — with the caller's column masks applied through the real
    /// masking operator.
    pub async fn node_output(
        &self,
        positions: &[i32],
        extras: Vec<ExtraCol>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), DataFusionError> {
        let (fields, arrays) = Self::take_columns(&self.data.nodes, positions)?;
        self.finish_output(fields, arrays, positions.len(), extras)
            .await
    }

    /// Assemble a governed **edge-rows** batch (full canonical edge columns for
    /// the given rows of the given file, plus resolved `src_name`/`dst_name`,
    /// plus extras + `snapshot_version`), masked.
    pub async fn edge_output(
        &self,
        rows: &[(EdgeFile, i32)],
        extras: Vec<ExtraCol>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), DataFusionError> {
        // Split per file, take, then interleave back in input order.
        // Simpler v1: take row-by-row via per-file takes preserving order —
        // build indices per file with their output slots.
        let n = rows.len();
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();

        // Take everything from each file separately, then stitch by slot.
        let fwd_rows: Vec<i32> = rows
            .iter()
            .filter(|(f, _)| *f == EdgeFile::Fwd)
            .map(|(_, r)| *r)
            .collect();
        let rev_rows: Vec<i32> = rows
            .iter()
            .filter(|(f, _)| *f == EdgeFile::Rev)
            .map(|(_, r)| *r)
            .collect();
        let (f_fields, f_arrays) = Self::take_columns(&self.data.edges, &fwd_rows)?;
        let (_, r_arrays) = Self::take_columns(&self.data.edges_rev, &rev_rows)?;

        // Interleave: build a take-index over the concatenation [fwd..., rev...]
        // that restores the caller's row order.
        let mut fwd_seen = 0i32;
        let mut rev_seen = 0i32;
        let order: Vec<i32> = rows
            .iter()
            .map(|(f, _)| match f {
                EdgeFile::Fwd => {
                    fwd_seen += 1;
                    fwd_seen - 1
                }
                EdgeFile::Rev => {
                    rev_seen += 1;
                    fwd_rows.len() as i32 + (rev_seen - 1)
                }
            })
            .collect();
        let order_idx = Int32Array::from(order);

        for (i, field) in f_fields.iter().enumerate() {
            let concatenated =
                datafusion::arrow::compute::concat(&[f_arrays[i].as_ref(), r_arrays[i].as_ref()])?;
            let stitched = take(&concatenated, &order_idx, None)?;
            fields.push(field.clone());
            arrays.push(stitched);
        }

        // Resolved endpoint names (post-stitch, via src_pos/dst_pos columns).
        for (col, out_name) in [("src_pos", "src_name"), ("dst_pos", "dst_name")] {
            let pos_idx = fields
                .iter()
                .position(|f| f.name() == col)
                .ok_or_else(|| DataFusionError::Internal(format!("edge batch missing {col}")))?;
            let poses = arrays[pos_idx]
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| DataFusionError::Internal(format!("{col} not int32")))?;
            let names: Vec<Option<String>> = (0..poses.len())
                .map(|i| {
                    if poses.is_null(i) {
                        None
                    } else {
                        Some(self.data.names[poses.value(i) as usize].clone())
                    }
                })
                .collect();
            fields.push(Field::new(
                out_name,
                datafusion::arrow::datatypes::DataType::Utf8,
                true,
            ));
            arrays.push(Arc::new(datafusion::arrow::array::StringArray::from(names)) as ArrayRef);
        }

        self.finish_output(fields, arrays, n, extras).await
    }

    /// Append extras + snapshot_version, then run the batch through the real
    /// masking operators (`ContractApprovedExec → MaskingExec`).
    async fn finish_output(
        &self,
        mut fields: Vec<Field>,
        mut arrays: Vec<ArrayRef>,
        rows: usize,
        extras: Vec<ExtraCol>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), DataFusionError> {
        for e in extras {
            fields.push(Field::new(&e.name, e.array.data_type().clone(), true));
            arrays.push(e.array);
        }
        fields.push(Field::new(
            "snapshot_version",
            datafusion::arrow::datatypes::DataType::UInt64,
            true,
        ));
        arrays.push(Arc::new(UInt64Array::from(vec![self.snapshot_version(); rows])) as ArrayRef);

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;

        // Governed masking through the real operator stack.
        let bundle = self.policy.resolved.to_bundle_handle();
        let inner: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
        let approved: Arc<dyn ExecutionPlan> = Arc::new(
            ContractApprovedExec::new(bundle.clone(), inner)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        let masked: Arc<dyn ExecutionPlan> = Arc::new(
            MaskingExec::new(bundle, approved)
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        let out_schema = masked.schema();
        let ctx = datafusion::prelude::SessionContext::new();
        let batches = datafusion::physical_plan::collect(masked, ctx.task_ctx()).await?;
        Ok((out_schema, batches))
    }
}
