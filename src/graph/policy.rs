//! Compile a caller's [`ResolvedPolicy`] into a [`GraphPolicy`] — the
//! visibility bitmasks the traversal algorithms consult as walls.
//!
//! The node filter (`ResolvedPolicy::row_filter`, authored over node columns)
//! and the graph-only edge filter (`ResolvedPolicy::graph_edge_filter`) are SQL
//! boolean predicates. Rather than inventing a predicate evaluator, we evaluate
//! them **with DataFusion itself** against the snapshot's node/edge batches:
//! `SELECT pos FROM nodes WHERE <filter>` → the visible set. Full SQL predicate
//! power, one engine, zero drift from the tabular row-filter semantics.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use super::types::{GraphData, GraphError, GraphPolicy};
use crate::policy::ResolvedPolicy;

/// Evaluate `filter` (a SQL boolean predicate) against `batch` (which must
/// carry the `key` int32 column) and return the set of surviving keys.
async fn surviving_keys(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    table: &str,
    key: &str,
    filter: &str,
) -> Result<HashSet<i32>, GraphError> {
    let ctx = SessionContext::new();
    let mem = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
        .map_err(|e| GraphError::Internal(format!("policy MemTable: {e}")))?;
    ctx.register_table(table, Arc::new(mem))
        .map_err(|e| GraphError::Internal(format!("policy register: {e}")))?;

    let sql = format!("SELECT \"{key}\" FROM {table} WHERE {filter}");
    let df = ctx.sql(&sql).await.map_err(|e| {
        GraphError::InvalidArgument(format!(
            "graph policy filter '{filter}' failed to plan: {e}"
        ))
    })?;
    let batches = df.collect().await.map_err(|e| {
        GraphError::InvalidArgument(format!("graph policy filter '{filter}' failed to run: {e}"))
    })?;

    let mut keep = HashSet::new();
    for b in &batches {
        let col = b
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| GraphError::Internal(format!("{key} column is not int32")))?;
        for i in 0..col.len() {
            if !col.is_null(i) {
                keep.insert(col.value(i));
            }
        }
    }
    Ok(keep)
}

/// Compile `resolved` into the caller's [`GraphPolicy`] over `data`.
///
/// Semantics (G02 R4 / `docs/GRAPH-QUERY.md` §3):
/// - `row_filter` keeps only matching **nodes**; everything else is a wall.
/// - `graph_edge_filter` keeps only matching **edges** (evaluated on the edge
///   columns, e.g. `edge_type != 'depends_on'`).
/// - An edge is visible iff it survives the edge filter **and** both its
///   endpoints are visible (edges incident to a wall are invisible).
pub async fn compile_policy(
    data: &GraphData,
    resolved: Arc<ResolvedPolicy>,
) -> Result<GraphPolicy, GraphError> {
    // ── node visibility ──
    let node_visible: Vec<bool> = match &resolved.row_filter {
        None => vec![true; data.node_count()],
        Some(filter) => {
            let keep = surviving_keys(&data.nodes, "nodes", "pos", filter).await?;
            (0..data.node_count() as i32)
                .map(|p| keep.contains(&p))
                .collect()
        }
    };

    // ── edge visibility (authored filter), per file ──
    let (mut fwd_edge_visible, mut rev_edge_visible) = match &resolved.graph_edge_filter {
        None => (
            vec![true; data.fwd_dst.len()],
            vec![true; data.rev_src.len()],
        ),
        Some(filter) => {
            let fwd_keep = surviving_keys(&data.edges, "edges", "row", filter).await?;
            let rev_keep = surviving_keys(&data.edges_rev, "edges", "row", filter).await?;
            (
                (0..data.fwd_dst.len() as i32)
                    .map(|r| fwd_keep.contains(&r))
                    .collect(),
                (0..data.rev_src.len() as i32)
                    .map(|r| rev_keep.contains(&r))
                    .collect(),
            )
        }
    };

    // ── derived: an edge touching an invisible node is invisible ──
    for (row, vis) in fwd_edge_visible.iter_mut().enumerate() {
        let s = data.fwd_src[row] as usize;
        let d = data.fwd_dst[row] as usize;
        *vis = *vis && node_visible[s] && node_visible[d];
    }
    for (row, vis) in rev_edge_visible.iter_mut().enumerate() {
        let s = data.rev_src[row] as usize;
        let d = data.rev_dst[row] as usize;
        *vis = *vis && node_visible[s] && node_visible[d];
    }

    Ok(GraphPolicy {
        node_visible,
        fwd_edge_visible,
        rev_edge_visible,
        resolved,
    })
}
