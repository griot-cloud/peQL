//! Graph query capability (2.0) — governed traversal over compiled
//! process-graph snapshot bundles.
//!
//! A graph snapshot (G01 §9: `nodes.parquet` + `edges.parquet` +
//! `edges_rev.parquet` + `manifest.json`) is a **contract-bound dataset**. The
//! compiler already baked the adjacency index into the file layout (CSR:
//! per-node `out_start/out_count` slices into the src-sorted edge file, and
//! `in_start/in_count` into the dst-sorted reverse file), so traversal here is
//! array arithmetic — no query-time index construction.
//!
//! Governance (`docs/GRAPH-QUERY.md`): one [`crate::policy::ResolvedPolicy`]
//! governs both the graph's relational tables and the traversal functions. A
//! policy-filtered node is a **wall** — non-existent *and* non-traversable —
//! and a graph-only edge filter can hide relationships between visible nodes.
//! The governed snapshot is produced only through contract resolution; there is
//! no un-governed path to graph bytes.
//!
//! Module layout:
//! - [`types`] — shared data structures (the coordination contract).
//! - [`bundle`] — bundle loading + verification (digests, format) → [`types::GraphData`].
//! - [`policy`] — compile a `ResolvedPolicy` into a [`types::GraphPolicy`]
//!   (visibility bitmasks) via DataFusion predicate evaluation.
//! - [`traverse`] — the pure traversal algorithms (neighbors / edges / subtree /
//!   path / reachable) over `GraphData` + `GraphPolicy`.
//! - [`snapshot`] — the governed snapshot handle + cache; assembles governed
//!   `RecordBatch` outputs (masking applied via the existing operators).
//! - [`functions`] — the SQL table functions (`graph_node`, `graph_neighbors`,
//!   `graph_edges`, `graph_subtree`, `graph_path`, `graph_reachable`,
//!   `graph_nodes`) registered per caller-bound session.

pub mod bundle;
pub mod functions;
pub mod policy;
pub mod snapshot;
pub mod traverse;
pub mod types;
