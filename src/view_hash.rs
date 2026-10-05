//! The hash of a contract's view: what a view is, never what it reads.
//!
//! A contract is a view, and a view is a plan, not data. Two engines that
//! build the same view of a contract over a table of the same schema compute
//! the same hash; a change of the contract, the compiler, a pinned function,
//! or the table's columns (a name, a type, nullability, a column added or
//! dropped) changes it. Rows written under the same schema leave it as it is,
//! and no row is read to compute it.
//!
//! The hash covers the contract's `compilation_hash` (the contract, the
//! schema it was compiled against, parcel's version and the functions it
//! pins), the table's columns, and the plan the engine stands in for the
//! contract before any caller's parameters are bound: the scan of the
//! contract's row columns with its enrichers, the filter of its admits and
//! drop-level asserts, and the projection of its exposed columns, each node
//! with its schema. The gate and the parameter values, which are one
//! caller's, are not part of it.

use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::datasource::{empty::EmptyTable, provider_as_source};
use datafusion::logical_expr::{Expr, LogicalPlanBuilder};
use parcel_core::compile::Compilation;
use parcel_core::document::AssertOnFail;
use parcel_runtime::compiled::CompiledBytes;
use sha2::Digest as _;

use crate::error::{PeqlError, Result};

/// The unbound plan of `compilation`'s view, indented, each node with its
/// schema.
pub fn unbound_view(compilation: &Compilation) -> Result<String> {
    let cc = &compilation.contract;
    let scan = LogicalPlanBuilder::scan(
        format!("__peql_{}", cc.name.replace('/', "_")),
        provider_as_source(Arc::new(EmptyTable::new(cc.row_schema.clone()))),
        None,
    )?
    .build()?;
    let scan = parcel_runtime::plan::enrich_plan(scan, cc)?;
    let mut filters: Vec<Expr> = cc.admits.iter().map(|(_, e)| e.clone()).collect();
    for f in &cc.flags {
        if f.on_fail == AssertOnFail::Drop {
            filters.push(f.expr.clone());
        }
    }
    let mut builder = LogicalPlanBuilder::from(scan);
    if let Some(f) = filters.into_iter().reduce(Expr::and) {
        builder = builder.filter(f)?;
    }
    let plan = builder
        .project(cc.projection.iter().map(|(n, e)| e.clone().alias(n)))?
        .build()?;
    let plan = parcel_core::compile::resolve(plan)?;
    Ok(plan.display_indent_schema().to_string())
}

fn column(f: &Field) -> serde_json::Value {
    serde_json::json!({
        "name": f.name(),
        "type": f.data_type().to_string(),
        "nullable": f.is_nullable(),
    })
}

/// `sha256:<hex>` of the view of `compilation` over a table whose columns
/// are `table`'s. The schema's metadata (field and schema metadata alike) is
/// not part of it: only the columns.
pub fn compiled_view_hash(compilation: &Compilation, table: &Schema) -> Result<String> {
    let definition = serde_json::json!({
        "compilation_hash": compilation.contract.compilation_hash,
        "columns": table.fields().iter().map(|f| column(f)).collect::<Vec<_>>(),
        "view": unbound_view(compilation)?,
    });
    let canonical = serde_json::to_vec(&definition)
        .map_err(|e| PeqlError::Invalid(format!("the view's definition: {e}")))?;
    Ok(format!(
        "sha256:{}",
        hex::encode(sha2::Sha256::digest(canonical))
    ))
}

/// [`compiled_view_hash`] of a contract in parcel's compiled form
/// ([`CompiledBytes`]), as [`crate::Engine::register_compiled`] registers it:
/// taken as given, never compiled. Whoever hands over the bytes vouches for
/// them.
pub fn view_hash(compiled: &[u8], table: &Schema) -> Result<String> {
    let compilation = Compilation::from_bytes(compiled).map_err(PeqlError::Invalid)?;
    compiled_view_hash(&compilation, table)
}
