//! [`ContractTableProvider`] — a DataFusion table whose every scan is governed.
//!
//! It wraps a raw (ungoverned) [`TableProvider`] together with a
//! [`ResolvedPolicy`]. When DataFusion asks it to `scan()`, it reads the raw
//! data and threads it through the contract enforcement operators —
//! `ContractApprovedExec` → `RowFilterExec` → `MaskingExec` →
//! (`LaplaceNoiseExec`) — before any rows leave the table. There is no scan path
//! that skips them.
//!
//! The inner table is scanned in full (no projection pushdown) so the operators
//! can see every column the contract references (a row filter may key off a
//! column the query did not select); the caller's projection and limit are then
//! applied on top of the governed plan so the output schema matches what
//! DataFusion expects.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::physical::contract_approved_exec::ContractApprovedExec;
use crate::physical::laplace_noise_exec::LaplaceNoiseExec;
use crate::physical::masking_exec::{masked_schema_for_bundle, MaskingExec};
use crate::physical::row_filter_exec::RowFilterExec;
use crate::physical::PhysicalError;
use crate::policy::ResolvedPolicy;
use crate::ContractBundleHandle;

/// A table provider that enforces a contract on every scan.
#[derive(Debug)]
pub struct ContractTableProvider {
    inner: Arc<dyn TableProvider>,
    bundle: ContractBundleHandle,
    tenant_id: String,
    has_dp: bool,
    /// The GOVERNED table schema DataFusion plans against.
    ///
    /// Equal to the inner schema except that any column masked with a
    /// string-producing policy (hash/tokenize/partial) on a non-string source is
    /// retyped to `Utf8` — matching the physical `MaskingExec` output. The
    /// logical schema MUST match the physical scan schema, otherwise DataFusion's
    /// `ProjectionPushdown` rejects the plan with a "Schema mismatch" error.
    /// (Task #26.)
    governed_schema: SchemaRef,
    /// The contract's read PROJECTION as indices into the FULL governed schema
    /// (the columns the contract exposes, in the contract's declared order).
    /// `None` = expose every column (the base contract's identity projection).
    /// `Some(idx)` narrows `governed_schema` to exactly these columns: `SELECT *`
    /// returns only them and a non-exposed column is unresolvable — a VIEW hides
    /// its dropped columns, it is not a mask-only overlay. Derived from
    /// `policy.projection` (the read template's SELECT list).
    contract_projection: Option<Vec<usize>>,
}

impl ContractTableProvider {
    /// Wrap `inner` so it is governed by `policy`.
    pub fn new(inner: Arc<dyn TableProvider>, policy: &ResolvedPolicy) -> Self {
        let bundle = policy.to_bundle_handle();
        // Compute the governed (masked) schema up front so `schema()` and the
        // physical `MaskingExec` agree on masked-column types. On a bundle-parse
        // failure (which `MaskingExec::new` will also hit and surface loudly at
        // scan time), fall back to the inner schema rather than panicking here.
        let inner_schema = inner.schema();
        let full_governed =
            masked_schema_for_bundle(&inner_schema, &bundle).unwrap_or(inner_schema);

        // Resolve the contract's projection (column NAMES) to indices into the
        // governed schema, in the contract's declared order. A projected column
        // absent from the table is omitted — narrowing only ever REMOVES columns,
        // so a name mismatch can never leak an unexposed column (fail-closed).
        let contract_projection: Option<Vec<usize>> = policy.projection.as_ref().map(|cols| {
            cols.iter()
                .filter_map(|name| full_governed.index_of(name).ok())
                .collect()
        });

        // The schema DataFusion plans against: narrowed to the projected columns
        // when the contract sets a projection, else the full governed schema.
        // `project` cannot fail — every index came from `index_of` on this schema.
        let governed_schema = match &contract_projection {
            Some(idx) => Arc::new(
                full_governed
                    .project(idx)
                    .expect("projection indices are valid governed-schema positions"),
            ),
            None => full_governed,
        };

        Self {
            inner,
            bundle,
            tenant_id: policy.tenant_id.clone(),
            has_dp: policy.has_dp(),
            governed_schema,
            contract_projection,
        }
    }
}

fn phys_to_df(e: PhysicalError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// Project `plan` down to `indices` (so the governed full-schema plan matches
/// the projection DataFusion requested for the query).
fn apply_projection(
    plan: Arc<dyn ExecutionPlan>,
    indices: &[usize],
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let schema = plan.schema();
    let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = indices
        .iter()
        .map(|&i| {
            let field = schema.field(i);
            let col = Arc::new(Column::new(field.name(), i)) as Arc<dyn PhysicalExpr>;
            (col, field.name().to_string())
        })
        .collect();
    Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
}

#[async_trait]
impl TableProvider for ContractTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // The GOVERNED schema: masked non-string columns are retyped to Utf8 so
        // the logical plan matches the physical governed scan (Task #26).
        self.governed_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Scan the raw table in full — the contract operators need every column
        // the contract references, regardless of the query's SELECT list.
        let inner_plan = self.inner.scan(state, None, &[], None).await?;

        // Build the contract enforcement stack. `ContractApprovedExec` is the
        // proof the scan was contract-checked; the downstream operators refuse
        // to run without it upstream.
        let approved: Arc<dyn ExecutionPlan> = Arc::new(
            ContractApprovedExec::new(self.bundle.clone(), inner_plan).map_err(phys_to_df)?,
        );
        let filtered: Arc<dyn ExecutionPlan> =
            Arc::new(RowFilterExec::new(self.bundle.clone(), approved).map_err(phys_to_df)?);
        let masked: Arc<dyn ExecutionPlan> =
            Arc::new(MaskingExec::new(self.bundle.clone(), filtered).map_err(phys_to_df)?);
        let governed: Arc<dyn ExecutionPlan> = if self.has_dp {
            Arc::new(
                LaplaceNoiseExec::new_permissive(
                    self.bundle.clone(),
                    self.tenant_id.clone(),
                    masked,
                )
                .map_err(phys_to_df)?,
            )
        } else {
            masked
        };

        // Compose the CONTRACT projection with the QUERY's projection, then honor
        // the limit, on top of the governed (full-schema) plan.
        //
        // `self.contract_projection` holds the exposed columns as indices into the
        // full governed plan. The query's `projection` indexes the NARROWED
        // `schema()` the planner saw. So a query index `i` maps to the full-plan
        // index `contract_projection[i]`; a bare `SELECT *` (query projection
        // `None`) selects exactly the contract's exposed columns. This is what
        // makes a view hide its non-exposed columns AND still respect the query's
        // own SELECT list — enforced here, in the engine, for every caller.
        let final_indices: Option<Vec<usize>> = match (&self.contract_projection, projection) {
            (Some(view_idx), Some(q)) => Some(q.iter().map(|&i| view_idx[i]).collect()),
            (Some(view_idx), None) => Some(view_idx.clone()),
            (None, Some(q)) => Some(q.clone()),
            (None, None) => None,
        };
        let projected = match &final_indices {
            Some(indices) => apply_projection(governed, indices)?,
            None => governed,
        };
        let limited = match limit {
            Some(n) => {
                Arc::new(GlobalLimitExec::new(projected, 0, Some(n))) as Arc<dyn ExecutionPlan>
            }
            None => projected,
        };

        Ok(limited)
    }
}
