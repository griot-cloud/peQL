//! The graph SQL table functions (G02 R2): `graph_node`, `graph_neighbors`,
//! `graph_edges`, `graph_subtree`, `graph_path`, `graph_reachable`, plus the
//! relational `graph_nodes` (R11).
//!
//! Each function is a DataFusion UDTF registered **per caller-bound session**
//! (like the catalog), so caller identity flows into contract resolution with
//! no global state. `TableFunctionImpl::call` resolves the contract, loads the
//! governed snapshot (cached), runs the traversal, assembles + **masks** the
//! output through the real operators, and returns an ordinary `MemTable` — so
//! results compose with the rest of the query (joins, `UNNEST`, CTEs) exactly
//! like any table (R3).
//!
//! Arguments are **positional** (DataFusion 47 UDTFs receive a positional
//! literal list; named `=>` notation is not part of its UDTF surface — G02 D3
//! fallback). Optional trailing arguments may be omitted or passed as `NULL`.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Int32Array, StringArray};
use datafusion::catalog::TableFunctionImpl;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;

use super::snapshot::{ExtraCol, GovernedGraph, GraphSession};
use super::traverse;
use super::types::{
    resolve_node, Caps, Direction, EdgeFile, GraphError, Truncatable, TypeFilter, HARD_MAX_DEPTH,
};

// ─── Argument helpers ─────────────────────────────────────────────────────────

fn lit_str(args: &[Expr], idx: usize, name: &str) -> DFResult<Option<String>> {
    match args.get(idx) {
        None => Ok(None),
        Some(Expr::Literal(ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v))) => Ok(v.clone()),
        Some(Expr::Literal(ScalarValue::Null)) => Ok(None),
        Some(other) => Err(DataFusionError::Plan(format!(
            "graph function argument '{name}' (position {idx}) must be a string literal, got {other}"
        ))),
    }
}

fn required_str(args: &[Expr], idx: usize, name: &str) -> DFResult<String> {
    lit_str(args, idx, name)?.ok_or_else(|| {
        DataFusionError::Plan(format!(
            "graph function requires the '{name}' argument (position {idx})"
        ))
    })
}

fn lit_i64(args: &[Expr], idx: usize, name: &str) -> DFResult<Option<i64>> {
    match args.get(idx) {
        None => Ok(None),
        Some(Expr::Literal(ScalarValue::Null)) => Ok(None),
        Some(Expr::Literal(v)) => match v {
            ScalarValue::Int64(x) => Ok(*x),
            ScalarValue::Int32(x) => Ok(x.map(i64::from)),
            ScalarValue::UInt64(x) => Ok(x.map(|x| x as i64)),
            other => Err(DataFusionError::Plan(format!(
                "graph function argument '{name}' must be an integer literal, got {other}"
            ))),
        },
        Some(other) => Err(DataFusionError::Plan(format!(
            "graph function argument '{name}' must be an integer literal, got {other}"
        ))),
    }
}

fn lit_bool(args: &[Expr], idx: usize, name: &str) -> DFResult<Option<bool>> {
    match args.get(idx) {
        None => Ok(None),
        Some(Expr::Literal(ScalarValue::Null)) => Ok(None),
        Some(Expr::Literal(ScalarValue::Boolean(v))) => Ok(*v),
        Some(other) => Err(DataFusionError::Plan(format!(
            "graph function argument '{name}' must be a boolean literal, got {other}"
        ))),
    }
}

fn parse_direction(args: &[Expr], idx: usize, default: Direction) -> DFResult<Direction> {
    match lit_str(args, idx, "direction")? {
        None => Ok(default),
        Some(s) => Direction::parse(&s).map_err(plan_err),
    }
}

fn parse_types(args: &[Expr], idx: usize, data: &super::types::GraphData) -> DFResult<TypeFilter> {
    match lit_str(args, idx, "edge_types")? {
        None => Ok(TypeFilter::AllAuthored),
        Some(csv) => {
            let mut ids = Vec::new();
            for name in csv.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                let id = data.edge_type_id(name).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "unknown edge type '{name}' (this snapshot has: {})",
                        data.edge_type_names.join(", ")
                    ))
                })?;
                ids.push(id);
            }
            Ok(TypeFilter::Only(ids))
        }
    }
}

fn depth_arg(args: &[Expr], idx: usize, default: usize) -> DFResult<usize> {
    match lit_i64(args, idx, "max_depth")? {
        None => Ok(default),
        Some(d) if d >= 1 => Ok((d as usize).min(HARD_MAX_DEPTH)),
        Some(d) => Err(DataFusionError::Plan(format!(
            "max_depth must be >= 1, got {d}"
        ))),
    }
}

fn plan_err(e: GraphError) -> DataFusionError {
    DataFusionError::Plan(e.to_string())
}

/// Run `fut` to completion from a sync context. UDTF `call()` is synchronous
/// but planning runs on the engine's async runtime; on a multi-thread runtime
/// we `block_in_place`, otherwise (current-thread tests) a local executor is
/// safe because the graph path performs no tokio-timer/IO awaits.
fn run_blocking<F: std::future::Future>(fut: F) -> F::Output {
    use tokio::runtime::{Handle, RuntimeFlavor};
    match Handle::try_current() {
        Ok(h) if h.runtime_flavor() == RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| h.block_on(fut))
        }
        _ => futures::executor::block_on(fut),
    }
}

// ─── Shared plumbing ──────────────────────────────────────────────────────────

/// Which graph verb a [`GraphFn`] instance implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphVerb {
    /// `graph_node(ref, node)`
    Node,
    /// `graph_neighbors(ref, node [, direction [, edge_types]])`
    Neighbors,
    /// `graph_edges(ref [, node [, direction [, edge_types]]])`
    Edges,
    /// `graph_subtree(ref, node [, max_depth [, follow_call_refs]])`
    Subtree,
    /// `graph_path(ref, from, to [, direction [, max_depth [, edge_types]]])`
    Path,
    /// `graph_reachable(ref, node [, direction [, max_depth [, edge_types]]])`
    Reachable,
    /// `graph_nodes(ref)` — the relational node scan (R11).
    NodesScan,
}

/// One registered graph table function, bound to a caller session.
#[derive(Debug)]
pub struct GraphFn {
    verb: GraphVerb,
    session: Arc<GraphSession>,
}

impl GraphFn {
    /// Bind `verb` to `session`.
    pub fn new(verb: GraphVerb, session: Arc<GraphSession>) -> Self {
        Self { verb, session }
    }
}

fn utf8_extra(name: &str, vals: Vec<Option<String>>) -> ExtraCol {
    ExtraCol {
        name: name.to_string(),
        array: Arc::new(StringArray::from(vals)) as ArrayRef,
    }
}

fn i32_extra(name: &str, vals: Vec<i32>) -> ExtraCol {
    ExtraCol {
        name: name.to_string(),
        array: Arc::new(Int32Array::from(vals)) as ArrayRef,
    }
}

fn bool_extra(name: &str, val: bool, rows: usize) -> ExtraCol {
    ExtraCol {
        name: name.to_string(),
        array: Arc::new(BooleanArray::from(vec![val; rows])) as ArrayRef,
    }
}

/// The four edge attribute columns (type/condition/status/description) for a
/// set of edge hits, row-aligned.
type EdgeAttrCols = (
    Vec<Option<String>>,
    Vec<Option<String>>,
    Vec<Option<String>>,
    Vec<Option<String>>,
);

/// Read a Utf8 edge column value for a (file, row) hit.
fn edge_col(g: &GovernedGraph, file: EdgeFile, row: i32, col: &str) -> DFResult<Option<String>> {
    let batch = match file {
        EdgeFile::Fwd => &g.data.edges,
        EdgeFile::Rev => &g.data.edges_rev,
    };
    let idx = batch
        .schema()
        .index_of(col)
        .map_err(|e| DataFusionError::Internal(format!("edge batch missing column {col}: {e}")))?;
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Internal(format!("edge column {col} is not Utf8")))?;
    use datafusion::arrow::array::Array;
    Ok(if arr.is_null(row as usize) {
        None
    } else {
        Some(arr.value(row as usize).to_string())
    })
}

fn edge_extras(g: &GovernedGraph, hits: &[(EdgeFile, i32)]) -> DFResult<EdgeAttrCols> {
    let mut ty = Vec::with_capacity(hits.len());
    let mut cond = Vec::with_capacity(hits.len());
    let mut status = Vec::with_capacity(hits.len());
    let mut desc = Vec::with_capacity(hits.len());
    for (file, row) in hits {
        ty.push(edge_col(g, *file, *row, "edge_type")?);
        cond.push(edge_col(g, *file, *row, "condition")?);
        status.push(edge_col(g, *file, *row, "status")?);
        desc.push(edge_col(g, *file, *row, "description")?);
    }
    Ok((ty, cond, status, desc))
}

impl GraphFn {
    async fn build(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        let graph_ref = required_str(args, 0, "graph_ref")?;
        let g = self.session.governed(&graph_ref).await?;
        let data = &g.data;
        let pol = &g.policy;

        let (schema, batches) = match self.verb {
            GraphVerb::Node => {
                let key = required_str(args, 1, "node")?;
                let pos = resolve_node(data, pol, &key).map_err(plan_err)?;
                g.node_output(&[pos], vec![]).await?
            }

            GraphVerb::NodesScan => {
                let visible: Vec<i32> = (0..data.node_count() as i32)
                    .filter(|p| pol.node_visible[*p as usize])
                    .collect();
                g.node_output(&visible, vec![]).await?
            }

            GraphVerb::Neighbors => {
                let key = required_str(args, 1, "node")?;
                let pos = resolve_node(data, pol, &key).map_err(plan_err)?;
                let direction = parse_direction(args, 2, Direction::Both)?;
                let types = parse_types(args, 3, data)?;
                let hits = traverse::neighbors(data, pol, pos, direction, &types);

                let nodes: Vec<i32> = hits.iter().map(|h| h.node_pos).collect();
                let rows: Vec<(EdgeFile, i32)> =
                    hits.iter().map(|h| (h.edge_file, h.edge_row)).collect();
                let (ty, cond, status, desc) = edge_extras(&g, &rows)?;
                let dir_vals: Vec<Option<String>> = hits
                    .iter()
                    .map(|h| {
                        Some(match h.direction {
                            Direction::Out => "out".to_string(),
                            _ => "in".to_string(),
                        })
                    })
                    .collect();
                g.node_output(
                    &nodes,
                    vec![
                        utf8_extra("edge_type", ty),
                        utf8_extra("edge_condition", cond),
                        utf8_extra("edge_status", status),
                        utf8_extra("edge_description", desc),
                        utf8_extra("direction", dir_vals),
                    ],
                )
                .await?
            }

            GraphVerb::Edges => {
                let anchor = match lit_str(args, 1, "node")? {
                    None => None,
                    Some(key) => Some(resolve_node(data, pol, &key).map_err(plan_err)?),
                };
                let direction = parse_direction(args, 2, Direction::Both)?;
                let types = parse_types(args, 3, data)?;
                let Truncatable { rows, truncated } =
                    traverse::edges_of(data, pol, anchor, direction, &types, Caps::default());
                let hits: Vec<(EdgeFile, i32)> =
                    rows.iter().map(|h| (h.edge_file, h.edge_row)).collect();
                let n = hits.len();
                g.edge_output(&hits, vec![bool_extra("truncated", truncated, n)])
                    .await?
            }

            GraphVerb::Subtree => {
                let key = required_str(args, 1, "node")?;
                let pos = resolve_node(data, pol, &key).map_err(plan_err)?;
                let max_depth = depth_arg(args, 2, HARD_MAX_DEPTH)?;
                let follow = lit_bool(args, 3, "follow_call_refs")?.unwrap_or(false);
                let Truncatable { rows, truncated } =
                    traverse::subtree(data, pol, pos, max_depth, follow, Caps::default());
                let nodes: Vec<i32> = rows.iter().map(|r| r.node_pos).collect();
                let depths: Vec<i32> = rows.iter().map(|r| r.depth as i32).collect();
                let n = nodes.len();
                g.node_output(
                    &nodes,
                    vec![
                        i32_extra("depth", depths),
                        bool_extra("truncated", truncated, n),
                    ],
                )
                .await?
            }

            GraphVerb::Path => {
                let from_key = required_str(args, 1, "from_node")?;
                let to_key = required_str(args, 2, "to_node")?;
                let from = resolve_node(data, pol, &from_key).map_err(plan_err)?;
                let to = resolve_node(data, pol, &to_key).map_err(plan_err)?;
                let direction = parse_direction(args, 3, Direction::Out)?;
                let max_depth = depth_arg(args, 4, 12)?;
                let types = parse_types(args, 5, data)?;
                let Truncatable { rows, truncated } =
                    traverse::shortest_path(data, pol, from, to, direction, &types, max_depth);

                let nodes: Vec<i32> = rows.iter().map(|h| h.node_pos).collect();
                let steps: Vec<i32> = rows.iter().map(|h| h.step as i32).collect();
                let mut ty = Vec::new();
                let mut cond = Vec::new();
                let mut status = Vec::new();
                let mut desc = Vec::new();
                for h in &rows {
                    if h.has_edge {
                        ty.push(edge_col(&g, h.edge_file, h.edge_row, "edge_type")?);
                        cond.push(edge_col(&g, h.edge_file, h.edge_row, "condition")?);
                        status.push(edge_col(&g, h.edge_file, h.edge_row, "status")?);
                        desc.push(edge_col(&g, h.edge_file, h.edge_row, "description")?);
                    } else {
                        ty.push(None);
                        cond.push(None);
                        status.push(None);
                        desc.push(None);
                    }
                }
                let n = nodes.len();
                g.node_output(
                    &nodes,
                    vec![
                        i32_extra("step", steps),
                        utf8_extra("edge_type", ty),
                        utf8_extra("edge_condition", cond),
                        utf8_extra("edge_status", status),
                        utf8_extra("edge_description", desc),
                        bool_extra("truncated", truncated, n),
                    ],
                )
                .await?
            }

            GraphVerb::Reachable => {
                let key = required_str(args, 1, "node")?;
                let pos = resolve_node(data, pol, &key).map_err(plan_err)?;
                let direction = parse_direction(args, 2, Direction::Out)?;
                let max_depth = depth_arg(args, 3, 12)?;
                let types = parse_types(args, 4, data)?;
                let Truncatable { rows, truncated } = traverse::reachable(
                    data,
                    pol,
                    pos,
                    direction,
                    &types,
                    max_depth,
                    Caps::default(),
                );
                let nodes: Vec<i32> = rows.iter().map(|r| r.node_pos).collect();
                let depths: Vec<i32> = rows.iter().map(|r| r.min_depth as i32).collect();
                let first: Vec<Option<String>> = rows
                    .iter()
                    .map(|r| Some(data.edge_type_names[r.first_edge_type as usize].clone()))
                    .collect();
                let n = nodes.len();
                g.node_output(
                    &nodes,
                    vec![
                        i32_extra("min_depth", depths),
                        utf8_extra("first_edge_type", first),
                        bool_extra("truncated", truncated, n),
                    ],
                )
                .await?
            }
        };

        let table = MemTable::try_new(schema, vec![batches])
            .map_err(|e| DataFusionError::Internal(format!("graph result table: {e}")))?;
        Ok(Arc::new(table))
    }
}

impl TableFunctionImpl for GraphFn {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        run_blocking(self.build(args))
    }
}

/// Register all graph table functions on `ctx`, bound to `session`'s caller.
pub fn register_graph_functions(ctx: &SessionContext, session: Arc<GraphSession>) {
    for (name, verb) in [
        ("graph_node", GraphVerb::Node),
        ("graph_neighbors", GraphVerb::Neighbors),
        ("graph_edges", GraphVerb::Edges),
        ("graph_subtree", GraphVerb::Subtree),
        ("graph_path", GraphVerb::Path),
        ("graph_reachable", GraphVerb::Reachable),
        ("graph_nodes", GraphVerb::NodesScan),
    ] {
        ctx.register_udtf(name, Arc::new(GraphFn::new(verb, session.clone())));
    }
}
