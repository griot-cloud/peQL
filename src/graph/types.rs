//! Shared graph types — the coordination contract between the bundle loader,
//! the policy compiler, the traversal algorithms, and the SQL functions.
//!
//! Positions (`pos` / edge `row`) are dense 0-based ints equal to physical row
//! order (G01 invariant I1) and are **snapshot-scoped**: never persist them
//! across snapshots (I6). ULIDs and names are the durable keys.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use serde::Deserialize;

// ─── Manifest ─────────────────────────────────────────────────────────────────

/// `manifest.json` — the fields the engine consumes (G01 §9.3).
#[derive(Debug, Clone, Deserialize)]
pub struct GraphManifest {
    /// Bundle schema version. The engine supports exactly `1` in v1.
    pub bundle_format: u32,
    /// ULID of the graph itself (stable across snapshots).
    pub graph_id: String,
    /// Human slug, e.g. `zijani-operations`.
    pub graph_slug: String,
    /// `process` today; opaque to the engine (R9: surfaced, never branched on).
    pub graph_kind: String,
    /// Monotonic snapshot version.
    pub snapshot_version: u64,
    /// Node/edge counts.
    pub counts: GraphCounts,
    /// Edge-type inventory — the source of truth for authored + derived types.
    pub edge_types: Vec<String>,
    /// Per-file sha256 digests (hex), keyed by file name.
    pub files: HashMap<String, FileDigest>,
    /// Compiler warnings (opaque to the engine; carried for disclosure).
    #[serde(default)]
    pub warnings: serde_json::Value,
    /// Compilation timestamp (informational).
    #[serde(default)]
    pub compiled_at: Option<String>,
    /// Compiler version (informational).
    #[serde(default)]
    pub compiler_version: Option<String>,
}

/// Node/edge counts from the manifest.
#[derive(Debug, Clone, Deserialize)]
pub struct GraphCounts {
    /// Total nodes.
    pub nodes: u64,
    /// Total edges (authored + derived).
    pub edges: u64,
    /// Derived (`contains`/`contains_ref`) edges.
    #[serde(default)]
    pub derived_edges: u64,
}

/// A per-file digest entry in the manifest.
#[derive(Debug, Clone, Deserialize)]
pub struct FileDigest {
    /// Hex-encoded sha256 of the file bytes.
    pub sha256: String,
}

// ─── Loaded graph data ────────────────────────────────────────────────────────

/// The v1 size envelope (G02 R8/§10). Exceeding it is a hard, named error.
pub const MAX_NODES_V1: usize = 50_000;
/// Edge half of the envelope.
pub const MAX_EDGES_V1: usize = 200_000;

/// Which edge file a row index points into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeFile {
    /// `edges.parquet` — sorted by `(src_pos, edge_type, dst_pos)`.
    Fwd,
    /// `edges_rev.parquet` — sorted by `(dst_pos, edge_type, src_pos)`.
    Rev,
}

/// An immutable, loaded, verified snapshot bundle — the raw (ungoverned) graph.
///
/// Produced only by [`crate::graph::bundle::load_bundle`]. Shared read-only
/// across callers (per-caller governance lives in [`GraphPolicy`], never here).
///
/// The `RecordBatch`es are **canonicalized**: every attribute column has the
/// canonical Arrow type (a compiler-emitted all-null `Null` column, e.g.
/// `system_ref` in real bundles, is cast to nullable `Utf8`), so output schemas
/// are stable across bundles.
#[derive(Debug)]
pub struct GraphData {
    /// The parsed manifest.
    pub manifest: GraphManifest,

    /// All node attribute rows (canonicalized), `pos` = row index.
    pub nodes: RecordBatch,
    /// Forward edge rows (canonicalized), `row` = row index.
    pub edges: RecordBatch,
    /// Reverse edge rows (canonicalized), `row` = row index.
    pub edges_rev: RecordBatch,

    // ── decoded traversal columns (all length == node/edge count) ──
    /// Per node: start of its slice in `edges` (rows where it is `src`).
    pub out_start: Vec<i32>,
    /// Per node: length of that slice.
    pub out_count: Vec<i32>,
    /// Per node: start of its slice in `edges_rev` (rows where it is `dst`).
    pub in_start: Vec<i32>,
    /// Per node: length of that slice.
    pub in_count: Vec<i32>,
    /// Per node: parent position (`None` = root).
    pub parent_pos: Vec<Option<i32>>,
    /// Per node: called process position (`None` = not a call node).
    pub call_ref_pos: Vec<Option<i32>>,
    /// Per node: children positions (derived from `parent_pos`), each list
    /// ascending by pos.
    pub children: Vec<Vec<i32>>,
    /// Per node: name (for output resolution + suggestions).
    pub names: Vec<String>,

    /// Per forward-edge row: destination node pos.
    pub fwd_dst: Vec<i32>,
    /// Per forward-edge row: source node pos.
    pub fwd_src: Vec<i32>,
    /// Per forward-edge row: edge-type id (index into [`Self::edge_type_names`]).
    pub fwd_type: Vec<u16>,
    /// Per reverse-edge row: source node pos (the neighbor when walking `in`).
    pub rev_src: Vec<i32>,
    /// Per reverse-edge row: destination node pos.
    pub rev_dst: Vec<i32>,
    /// Per reverse-edge row: edge-type id.
    pub rev_type: Vec<u16>,

    /// The edge-type dictionary (ids are indices into this).
    pub edge_type_names: Vec<String>,

    // ── lookup indexes ──
    /// Exact node name → pos.
    pub name_to_pos: HashMap<String, i32>,
    /// Case-insensitive, trimmed name → pos (for forgiving lookup + suggestions).
    pub name_ci_to_pos: HashMap<String, i32>,
    /// ULID → pos.
    pub id_to_pos: HashMap<String, i32>,
}

impl GraphData {
    /// Number of nodes.
    pub fn node_count(&self) -> usize {
        self.names.len()
    }

    /// Number of (logical) edges.
    pub fn edge_count(&self) -> usize {
        self.fwd_dst.len()
    }

    /// Edge-type id for `name`, if the snapshot contains that type.
    pub fn edge_type_id(&self, name: &str) -> Option<u16> {
        self.edge_type_names
            .iter()
            .position(|n| n == name)
            .map(|i| i as u16)
    }

    /// Ids of the **authored** edge types — everything except the derived
    /// `contains` / `contains_ref` (the default traversal set, G02 §07).
    pub fn authored_type_ids(&self) -> Vec<u16> {
        self.edge_type_names
            .iter()
            .enumerate()
            .filter(|(_, n)| n.as_str() != "contains" && n.as_str() != "contains_ref")
            .map(|(i, _)| i as u16)
            .collect()
    }
}

// ─── Node reference resolution ────────────────────────────────────────────────

/// `true` if `s` is shaped like a ULID (26 Crockford base-32 chars) — such keys
/// are tried as `node_id` first, then as names (G02 §07 disambiguation rule).
pub fn is_ulid_shaped(s: &str) -> bool {
    s.len() == 26
        && s.chars()
            .all(|c| c.is_ascii_digit() || (c.is_ascii_alphabetic() && !"ILOUilou".contains(c)))
}

/// Resolve a node reference (name or ULID) to a position, honoring the caller's
/// visibility: a policy-filtered node resolves exactly like an unknown one (no
/// existence oracle, G02 R4/R7).
///
/// On failure returns [`GraphError::UnknownNode`] carrying up to 5 near-miss
/// suggestions (case/whitespace-insensitive first, then substring matches) —
/// suggestions are drawn **only from nodes visible to the caller**.
pub fn resolve_node(data: &GraphData, policy: &GraphPolicy, key: &str) -> Result<i32, GraphError> {
    let visible = |pos: i32| policy.node_visible[pos as usize];

    if is_ulid_shaped(key) {
        if let Some(&pos) = data.id_to_pos.get(key) {
            if visible(pos) {
                return Ok(pos);
            }
        }
    }
    if let Some(&pos) = data.name_to_pos.get(key) {
        if visible(pos) {
            return Ok(pos);
        }
    }
    let ci = key.trim().to_lowercase();
    if let Some(&pos) = data.name_ci_to_pos.get(&ci) {
        if visible(pos) {
            return Ok(pos);
        }
    }

    // Near-miss suggestions from the caller-visible name set only.
    let mut suggestions: Vec<String> = data
        .names
        .iter()
        .enumerate()
        .filter(|(pos, _)| policy.node_visible[*pos])
        .filter(|(_, n)| {
            let n_ci = n.trim().to_lowercase();
            n_ci.contains(&ci) || ci.contains(&n_ci)
        })
        .map(|(_, n)| n.clone())
        .take(5)
        .collect();
    suggestions.sort();

    Err(GraphError::UnknownNode {
        graph: data.manifest.graph_slug.clone(),
        key: key.to_string(),
        suggestions,
    })
}

// ─── Per-caller governance overlay ────────────────────────────────────────────

/// The caller's compiled governance view of one snapshot.
///
/// Compiled once per (snapshot, policy) by [`crate::graph::policy`]; immutable
/// thereafter (G02 R4/R5). A `false` node is a **wall**: it appears in no
/// result and is never traversed *through*. A `false` edge row is invisible
/// even between two visible nodes.
#[derive(Debug, Clone)]
pub struct GraphPolicy {
    /// Per node pos: visible to this caller?
    pub node_visible: Vec<bool>,
    /// Per forward-edge row: visible? (already AND-ed with endpoint visibility)
    pub fwd_edge_visible: Vec<bool>,
    /// Per reverse-edge row: visible? (already AND-ed with endpoint visibility)
    pub rev_edge_visible: Vec<bool>,
    /// The originating policy (masks etc.) for output-side enforcement.
    pub resolved: Arc<crate::policy::ResolvedPolicy>,
}

impl GraphPolicy {
    /// An allow-all policy over `data` (owner view).
    pub fn allow_all(data: &GraphData, resolved: Arc<crate::policy::ResolvedPolicy>) -> Self {
        Self {
            node_visible: vec![true; data.node_count()],
            fwd_edge_visible: vec![true; data.fwd_dst.len()],
            rev_edge_visible: vec![true; data.rev_src.len()],
            resolved,
        }
    }
}

// ─── Traversal parameters & results ───────────────────────────────────────────

/// Traversal direction relative to the anchor node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Follow edges leaving the anchor (`src → dst`).
    Out,
    /// Follow edges arriving at the anchor.
    In,
    /// Both.
    Both,
}

impl Direction {
    /// Parse `'out' | 'in' | 'both'` (case-insensitive).
    pub fn parse(s: &str) -> Result<Self, GraphError> {
        match s.to_ascii_lowercase().as_str() {
            "out" => Ok(Direction::Out),
            "in" => Ok(Direction::In),
            "both" => Ok(Direction::Both),
            other => Err(GraphError::InvalidArgument(format!(
                "direction must be 'out', 'in' or 'both', got '{other}'"
            ))),
        }
    }
}

/// Which edge types a traversal follows.
#[derive(Debug, Clone)]
pub enum TypeFilter {
    /// All **authored** types (derived `contains`/`contains_ref` excluded) —
    /// the default everywhere except `graph_subtree` (G02 §07).
    AllAuthored,
    /// Exactly these type ids.
    Only(Vec<u16>),
}

impl TypeFilter {
    /// Materialise the allowed id set against a concrete snapshot.
    pub fn allowed_ids(&self, data: &GraphData) -> Vec<u16> {
        match self {
            TypeFilter::AllAuthored => data.authored_type_ids(),
            TypeFilter::Only(ids) => ids.clone(),
        }
    }
}

/// Safety caps (G02 R8). Defaults: depth 12, results 10 000.
#[derive(Debug, Clone, Copy)]
pub struct Caps {
    /// Maximum traversal depth.
    pub max_depth: usize,
    /// Maximum result rows before truncation.
    pub max_results: usize,
}

/// Hard engine maxima the per-call overrides may not exceed.
pub const HARD_MAX_DEPTH: usize = 64;
/// Hard cap on result rows.
pub const HARD_MAX_RESULTS: usize = 100_000;

impl Default for Caps {
    fn default() -> Self {
        Self {
            max_depth: 12,
            max_results: 10_000,
        }
    }
}

/// One neighbor hit: the neighbor node + the connecting edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NeighborHit {
    /// The neighbor's position.
    pub node_pos: i32,
    /// Which file `edge_row` indexes.
    pub edge_file: EdgeFile,
    /// The connecting edge's row in that file.
    pub edge_row: i32,
    /// `Out` = the edge leaves the anchor; `In` = it arrives at the anchor.
    pub direction: Direction,
}

/// One edge hit (edge-centric view).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeHit {
    /// Which file `edge_row` indexes.
    pub edge_file: EdgeFile,
    /// The edge's row in that file.
    pub edge_row: i32,
}

/// One subtree row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubtreeRow {
    /// The node.
    pub node_pos: i32,
    /// Depth below the anchor (anchor = 0).
    pub depth: u32,
    /// The node's parent pos (`None` for the anchor / roots).
    pub parent_pos: Option<i32>,
}

/// One hop of a shortest path. Row 0 is the origin with `edge_row = None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PathHop {
    /// 0-based hop index.
    pub step: u32,
    /// The node reached at this hop.
    pub node_pos: i32,
    /// The edge traversed to reach it (`None` for step 0).
    pub edge_file: EdgeFile,
    /// The traversed edge's row (`-1` sentinel never used; see `has_edge`).
    pub edge_row: i32,
    /// `false` for step 0 (no incoming edge).
    pub has_edge: bool,
}

/// One reachable-closure row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReachRow {
    /// The reachable node.
    pub node_pos: i32,
    /// Its minimum BFS depth from the anchor (≥ 1; anchor excluded).
    pub min_depth: u32,
    /// The edge-type id by which BFS first reached it.
    pub first_edge_type: u16,
}

/// A traversal result plus its truncation disclosure (G02 R7).
#[derive(Debug, Clone)]
pub struct Truncatable<T> {
    /// The rows.
    pub rows: Vec<T>,
    /// `true` if a depth or result cap cut the traversal short.
    pub truncated: bool,
}

// ─── Errors ───────────────────────────────────────────────────────────────────

/// Graph-capability errors (agent-grade per G02 R7).
#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    /// The bundle's `bundle_format` is not supported.
    #[error(
        "graph bundle '{bundle}': unsupported bundle_format {found} (engine supports {supported})"
    )]
    UnsupportedFormat {
        /// Bundle path/slug.
        bundle: String,
        /// Format found in the manifest.
        found: u32,
        /// Format this engine supports.
        supported: u32,
    },

    /// A file's digest does not match the manifest — tampered or truncated.
    #[error("graph bundle '{bundle}': sha256 mismatch for {file} (bundle rejected; expected {expected}, got {actual})")]
    DigestMismatch {
        /// Bundle path/slug.
        bundle: String,
        /// Offending file.
        file: String,
        /// Manifest digest.
        expected: String,
        /// Recomputed digest.
        actual: String,
    },

    /// A required bundle file is missing or unreadable.
    #[error("graph bundle '{bundle}': cannot read {file}: {reason}")]
    FileUnreadable {
        /// Bundle path/slug.
        bundle: String,
        /// Offending file.
        file: String,
        /// io/parquet reason.
        reason: String,
    },

    /// The bundle violates a structural invariant the engine relies on (I1–I5).
    #[error("graph bundle '{bundle}': invariant violation: {detail}")]
    InvariantViolation {
        /// Bundle path/slug.
        bundle: String,
        /// What was violated.
        detail: String,
    },

    /// The snapshot exceeds the v1 size envelope (G02 §10).
    #[error("graph bundle '{bundle}' exceeds the v1 envelope ({nodes} nodes / {edges} edges > {max_nodes}/{max_edges})")]
    EnvelopeExceeded {
        /// Bundle path/slug.
        bundle: String,
        /// Node count found.
        nodes: usize,
        /// Edge count found.
        edges: usize,
        /// Node cap.
        max_nodes: usize,
        /// Edge cap.
        max_edges: usize,
    },

    /// Node lookup failed (unknown, or filtered for this caller — uniform).
    #[error("graph '{graph}': no node matches '{key}' (lookup keys: name or node_id ULID){}",
        if .suggestions.is_empty() { String::new() }
        else { format!("; near misses: {}", .suggestions.join(", ")) })]
    UnknownNode {
        /// Graph slug.
        graph: String,
        /// The failed lookup key.
        key: String,
        /// Up to 5 near-miss names visible to the caller.
        suggestions: Vec<String>,
    },

    /// A function argument was invalid.
    #[error("invalid graph function argument: {0}")]
    InvalidArgument(String),

    /// Anything unexpected.
    #[error("graph internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulid_shape() {
        assert!(is_ulid_shaped("01J9ZK2M3N4P5Q6R7S8T9V0WX1"));
        assert!(is_ulid_shaped("GE0VHBCFVA3R5VB7QJSSB6GB2W"));
        assert!(!is_ulid_shaped("daily_sales_reconciliation"));
        assert!(!is_ulid_shaped("short"));
        // 'I', 'L', 'O', 'U' are not Crockford base-32.
        assert!(!is_ulid_shaped("ILOU9ZK2M3N4P5Q6R7S8T9V0WX"));
    }

    #[test]
    fn direction_parse() {
        assert_eq!(Direction::parse("OUT").unwrap(), Direction::Out);
        assert_eq!(Direction::parse("in").unwrap(), Direction::In);
        assert_eq!(Direction::parse("Both").unwrap(), Direction::Both);
        assert!(Direction::parse("sideways").is_err());
    }
}
