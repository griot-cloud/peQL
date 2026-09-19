//! Unit/integration tests for the pure graph-traversal algorithms
//! (`peql::graph::traverse`) over hand-built [`GraphData`] fixtures.
//!
//! The centrepiece is the governance **wall** suite (mini-T7): a
//! policy-hidden node must never appear in any result and must never be
//! traversed *through* — hiding a node re-routes or severs paths, never
//! leaks topology.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

use peql::graph::traverse::{edges_of, neighbors, reachable, shortest_path, subtree};
use peql::graph::types::{
    Caps, Direction, EdgeFile, GraphData, GraphManifest, GraphPolicy, NeighborHit, PathHop,
    ReachRow, SubtreeRow, TypeFilter,
};
use peql::policy::ResolvedPolicy;

// ─── Fixture builder ──────────────────────────────────────────────────────────

/// A minimal single-column `RecordBatch` of `len` rows. The traversal
/// algorithms never read the attribute batches — they only consult the decoded
/// CSR vectors — so a `pos` column is enough to keep `GraphData` well-formed.
fn stub_batch(len: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("pos", DataType::Int32, false)]));
    let pos: Vec<i32> = (0..len as i32).collect();
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(pos))]).unwrap()
}

/// Build a [`GraphData`] by hand: `edges` are `(src, dst, edge_type)` in any
/// order; the helper computes the CSR exactly like a compiled bundle —
/// forward file sorted `(src, type, dst)`, reverse file sorted
/// `(dst, type, src)` — plus offsets, children (from `parents`, ascending by
/// pos) and `n0..nK` names.
fn tiny_graph(
    nodes: usize,
    edges: &[(i32, i32, u16)],
    parents: &[Option<i32>],
    call_refs: &[Option<i32>],
    type_names: &[&str],
) -> GraphData {
    assert_eq!(parents.len(), nodes);
    assert_eq!(call_refs.len(), nodes);

    let mut fwd: Vec<(i32, i32, u16)> = edges.to_vec();
    fwd.sort_by_key(|&(s, d, t)| (s, t, d));
    let mut rev: Vec<(i32, i32, u16)> = edges.to_vec();
    rev.sort_by_key(|&(s, d, t)| (d, t, s));

    let fwd_src: Vec<i32> = fwd.iter().map(|e| e.0).collect();
    let fwd_dst: Vec<i32> = fwd.iter().map(|e| e.1).collect();
    let fwd_type: Vec<u16> = fwd.iter().map(|e| e.2).collect();
    let rev_src: Vec<i32> = rev.iter().map(|e| e.0).collect();
    let rev_dst: Vec<i32> = rev.iter().map(|e| e.1).collect();
    let rev_type: Vec<u16> = rev.iter().map(|e| e.2).collect();

    let mut out_count = vec![0i32; nodes];
    for &s in &fwd_src {
        out_count[s as usize] += 1;
    }
    let mut out_start = vec![0i32; nodes];
    let mut acc = 0i32;
    for p in 0..nodes {
        out_start[p] = acc;
        acc += out_count[p];
    }

    let mut in_count = vec![0i32; nodes];
    for &d in &rev_dst {
        in_count[d as usize] += 1;
    }
    let mut in_start = vec![0i32; nodes];
    acc = 0;
    for p in 0..nodes {
        in_start[p] = acc;
        acc += in_count[p];
    }

    let mut children: Vec<Vec<i32>> = vec![Vec::new(); nodes];
    for (pos, parent) in parents.iter().enumerate() {
        if let Some(p) = parent {
            children[*p as usize].push(pos as i32); // ascending: pos iterates up
        }
    }

    let names: Vec<String> = (0..nodes).map(|i| format!("n{i}")).collect();
    let name_to_pos: HashMap<String, i32> = names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.clone(), i as i32))
        .collect();
    let name_ci_to_pos = name_to_pos.clone();

    let manifest: GraphManifest = serde_json::from_value(serde_json::json!({
        "bundle_format": 1,
        "graph_id": "01J9ZK2M3N4P5Q6R7S8T9V0WX1",
        "graph_slug": "tiny",
        "graph_kind": "process",
        "snapshot_version": 1,
        "counts": { "nodes": nodes, "edges": edges.len() },
        "edge_types": type_names,
        "files": {}
    }))
    .unwrap();

    GraphData {
        manifest,
        nodes: stub_batch(nodes),
        edges: stub_batch(edges.len()),
        edges_rev: stub_batch(edges.len()),
        out_start,
        out_count,
        in_start,
        in_count,
        parent_pos: parents.to_vec(),
        call_ref_pos: call_refs.to_vec(),
        children,
        names,
        fwd_dst,
        fwd_src,
        fwd_type,
        rev_src,
        rev_dst,
        rev_type,
        edge_type_names: type_names.iter().map(|s| s.to_string()).collect(),
        name_to_pos,
        name_ci_to_pos,
        id_to_pos: HashMap::new(),
    }
}

/// Owner view: everything visible.
fn allow_all(data: &GraphData) -> GraphPolicy {
    GraphPolicy::allow_all(data, Arc::new(ResolvedPolicy::allow_all("c", "1", "t")))
}

/// A policy hiding the given node positions, with edge visibility derived the
/// way the policy compiler derives it: an edge row is visible iff **both**
/// endpoints are visible (no authored edge filter here).
fn policy_hiding(data: &GraphData, hidden: &[i32]) -> GraphPolicy {
    let mut node_visible = vec![true; data.node_count()];
    for &h in hidden {
        node_visible[h as usize] = false;
    }
    let fwd_edge_visible = (0..data.fwd_src.len())
        .map(|r| node_visible[data.fwd_src[r] as usize] && node_visible[data.fwd_dst[r] as usize])
        .collect();
    let rev_edge_visible = (0..data.rev_src.len())
        .map(|r| node_visible[data.rev_src[r] as usize] && node_visible[data.rev_dst[r] as usize])
        .collect();
    GraphPolicy {
        node_visible,
        fwd_edge_visible,
        rev_edge_visible,
        resolved: Arc::new(ResolvedPolicy::allow_all("c", "1", "t")),
    }
}

fn hit(node_pos: i32, edge_file: EdgeFile, edge_row: i32, direction: Direction) -> NeighborHit {
    NeighborHit {
        node_pos,
        edge_file,
        edge_row,
        direction,
    }
}

fn srow(node_pos: i32, depth: u32, parent_pos: Option<i32>) -> SubtreeRow {
    SubtreeRow {
        node_pos,
        depth,
        parent_pos,
    }
}

fn origin(node_pos: i32) -> PathHop {
    PathHop {
        step: 0,
        node_pos,
        edge_file: EdgeFile::Fwd,
        edge_row: 0,
        has_edge: false,
    }
}

fn hop(step: u32, node_pos: i32, edge_file: EdgeFile, edge_row: i32) -> PathHop {
    PathHop {
        step,
        node_pos,
        edge_file,
        edge_row,
        has_edge: true,
    }
}

fn rrow(node_pos: i32, min_depth: u32, first_edge_type: u16) -> ReachRow {
    ReachRow {
        node_pos,
        min_depth,
        first_edge_type,
    }
}

/// Diamond: 0→1, 0→2, 1→3, 2→3 (all `flows_to`).
/// Forward rows: 0:(0→1) 1:(0→2) 2:(1→3) 3:(2→3).
/// Reverse rows: 0:(1←0) 1:(2←0) 2:(3←1) 3:(3←2).
fn diamond() -> GraphData {
    tiny_graph(
        4,
        &[(0, 1, 0), (0, 2, 0), (1, 3, 0), (2, 3, 0)],
        &[None; 4],
        &[None; 4],
        &["flows_to"],
    )
}

// ─── neighbors ────────────────────────────────────────────────────────────────

#[test]
fn neighbors_out_diamond() {
    let data = diamond();
    let policy = allow_all(&data);
    let hits = neighbors(&data, &policy, 0, Direction::Out, &TypeFilter::AllAuthored);
    assert_eq!(
        hits,
        vec![
            hit(1, EdgeFile::Fwd, 0, Direction::Out),
            hit(2, EdgeFile::Fwd, 1, Direction::Out),
        ]
    );
}

#[test]
fn neighbors_in_diamond() {
    let data = diamond();
    let policy = allow_all(&data);
    let hits = neighbors(&data, &policy, 3, Direction::In, &TypeFilter::AllAuthored);
    assert_eq!(
        hits,
        vec![
            hit(1, EdgeFile::Rev, 2, Direction::In),
            hit(2, EdgeFile::Rev, 3, Direction::In),
        ]
    );
}

#[test]
fn neighbors_both_out_group_first() {
    let data = diamond();
    let policy = allow_all(&data);
    let hits = neighbors(&data, &policy, 1, Direction::Both, &TypeFilter::AllAuthored);
    assert_eq!(
        hits,
        vec![
            hit(3, EdgeFile::Fwd, 2, Direction::Out), // out group before in group
            hit(0, EdgeFile::Rev, 0, Direction::In),
        ]
    );
    // Direction narrowing returns exactly one group.
    assert_eq!(
        neighbors(&data, &policy, 1, Direction::Out, &TypeFilter::AllAuthored),
        vec![hit(3, EdgeFile::Fwd, 2, Direction::Out)]
    );
    assert_eq!(
        neighbors(&data, &policy, 1, Direction::In, &TypeFilter::AllAuthored),
        vec![hit(0, EdgeFile::Rev, 0, Direction::In)]
    );
}

#[test]
fn neighbors_parallel_edges_yield_k_rows() {
    // Two parallel edges of the same type plus one of another type: forward
    // file sorted (src, type, dst) puts the two type-0 rows first.
    let data = tiny_graph(
        2,
        &[(0, 1, 1), (0, 1, 0), (0, 1, 0)],
        &[None; 2],
        &[None; 2],
        &["a", "b"],
    );
    let policy = allow_all(&data);
    let hits = neighbors(&data, &policy, 0, Direction::Out, &TypeFilter::AllAuthored);
    assert_eq!(
        hits,
        vec![
            hit(1, EdgeFile::Fwd, 0, Direction::Out), // type 0
            hit(1, EdgeFile::Fwd, 1, Direction::Out), // type 0 (stable by row)
            hit(1, EdgeFile::Fwd, 2, Direction::Out), // type 1 after type 0
        ]
    );
    // Type filter keeps only the matching edge.
    assert_eq!(
        neighbors(
            &data,
            &policy,
            0,
            Direction::Out,
            &TypeFilter::Only(vec![1])
        ),
        vec![hit(1, EdgeFile::Fwd, 2, Direction::Out)]
    );
}

#[test]
fn neighbors_contains_excluded_unless_explicit() {
    let data = tiny_graph(
        3,
        &[(0, 1, 0), (0, 2, 1)],
        &[None; 3],
        &[None; 3],
        &["flows_to", "contains"],
    );
    let policy = allow_all(&data);
    // Derived `contains` is excluded from the authored default set.
    assert_eq!(
        neighbors(&data, &policy, 0, Direction::Out, &TypeFilter::AllAuthored),
        vec![hit(1, EdgeFile::Fwd, 0, Direction::Out)]
    );
    // But an explicit Only([contains]) includes it.
    assert_eq!(
        neighbors(
            &data,
            &policy,
            0,
            Direction::Out,
            &TypeFilter::Only(vec![1])
        ),
        vec![hit(2, EdgeFile::Fwd, 1, Direction::Out)]
    );
}

// ─── edges_of ─────────────────────────────────────────────────────────────────

#[test]
fn edges_of_anchor_both_fwd_then_rev() {
    let data = diamond();
    let policy = allow_all(&data);
    let res = edges_of(
        &data,
        &policy,
        Some(1),
        Direction::Both,
        &TypeFilter::AllAuthored,
        Caps::default(),
    );
    assert!(!res.truncated);
    let files_rows: Vec<(EdgeFile, i32)> =
        res.rows.iter().map(|e| (e.edge_file, e.edge_row)).collect();
    assert_eq!(files_rows, vec![(EdgeFile::Fwd, 2), (EdgeFile::Rev, 0)]);

    // A sink node has only reverse-role rows.
    let res = edges_of(
        &data,
        &policy,
        Some(3),
        Direction::Both,
        &TypeFilter::AllAuthored,
        Caps::default(),
    );
    let files_rows: Vec<(EdgeFile, i32)> =
        res.rows.iter().map(|e| (e.edge_file, e.edge_row)).collect();
    assert_eq!(files_rows, vec![(EdgeFile::Rev, 2), (EdgeFile::Rev, 3)]);
}

#[test]
fn edges_of_none_scans_forward_file_once() {
    let data = diamond();
    let policy = allow_all(&data);
    // `direction` is ignored without an anchor; each logical edge appears once.
    let res = edges_of(
        &data,
        &policy,
        None,
        Direction::In,
        &TypeFilter::AllAuthored,
        Caps::default(),
    );
    assert!(!res.truncated);
    let files_rows: Vec<(EdgeFile, i32)> =
        res.rows.iter().map(|e| (e.edge_file, e.edge_row)).collect();
    assert_eq!(
        files_rows,
        vec![
            (EdgeFile::Fwd, 0),
            (EdgeFile::Fwd, 1),
            (EdgeFile::Fwd, 2),
            (EdgeFile::Fwd, 3),
        ]
    );
}

#[test]
fn edges_of_max_results_truncates() {
    let data = diamond();
    let policy = allow_all(&data);
    let caps = Caps {
        max_depth: 12,
        max_results: 2,
    };
    let res = edges_of(
        &data,
        &policy,
        None,
        Direction::Out,
        &TypeFilter::AllAuthored,
        caps,
    );
    assert!(res.truncated);
    assert_eq!(res.rows.len(), 2);
    assert_eq!(res.rows[0].edge_row, 0);
    assert_eq!(res.rows[1].edge_row, 1);

    // Exactly at the cap: no truncation flag.
    let caps = Caps {
        max_depth: 12,
        max_results: 4,
    };
    let res = edges_of(
        &data,
        &policy,
        None,
        Direction::Out,
        &TypeFilter::AllAuthored,
        caps,
    );
    assert!(!res.truncated);
    assert_eq!(res.rows.len(), 4);
}

// ─── subtree ──────────────────────────────────────────────────────────────────

/// Three-level tree: 0 → {1, 2}; 1 → {3, 4}; 2 → {5}.
fn tree() -> GraphData {
    tiny_graph(
        6,
        &[],
        &[None, Some(0), Some(0), Some(1), Some(1), Some(2)],
        &[None; 6],
        &["flows_to", "contains"],
    )
}

#[test]
fn subtree_depth_first_preorder() {
    let data = tree();
    let policy = allow_all(&data);
    let res = subtree(&data, &policy, 0, 12, false, Caps::default());
    assert!(!res.truncated);
    assert_eq!(
        res.rows,
        vec![
            srow(0, 0, None),
            srow(1, 1, Some(0)),
            srow(3, 2, Some(1)),
            srow(4, 2, Some(1)),
            srow(2, 1, Some(0)),
            srow(5, 2, Some(2)),
        ]
    );
}

#[test]
fn subtree_max_depth_cut_sets_truncated() {
    let data = tree();
    let policy = allow_all(&data);
    let res = subtree(&data, &policy, 0, 1, false, Caps::default());
    assert!(res.truncated); // 1 and 2 have unexpanded children
    assert_eq!(
        res.rows,
        vec![srow(0, 0, None), srow(1, 1, Some(0)), srow(2, 1, Some(0))]
    );
    // A depth cap that exactly covers the tree does not truncate.
    let res = subtree(&data, &policy, 0, 2, false, Caps::default());
    assert!(!res.truncated);
    assert_eq!(res.rows.len(), 6);
}

#[test]
fn subtree_max_results_truncates() {
    let data = tree();
    let policy = allow_all(&data);
    let caps = Caps {
        max_depth: 12,
        max_results: 3,
    };
    let res = subtree(&data, &policy, 0, 12, false, caps);
    assert!(res.truncated);
    assert_eq!(
        res.rows,
        vec![srow(0, 0, None), srow(1, 1, Some(0)), srow(3, 2, Some(1))]
    );
}

#[test]
fn subtree_invisible_mid_node_prunes_branch() {
    let data = tree();
    let policy = policy_hiding(&data, &[1]);
    let res = subtree(&data, &policy, 0, 12, false, Caps::default());
    assert!(!res.truncated);
    // 1 gone, and its whole branch (3, 4) never entered — the wall.
    assert_eq!(
        res.rows,
        vec![srow(0, 0, None), srow(2, 1, Some(0)), srow(5, 2, Some(2))]
    );
}

/// Call-ref graph: 0 → {1, 2} (both call nodes referencing subprocess 3);
/// 3 → {4} is a separate containment root.
fn call_graph() -> GraphData {
    tiny_graph(
        5,
        &[],
        &[None, Some(0), Some(0), None, Some(3)],
        &[None, Some(3), Some(3), None, None],
        &["flows_to", "contains", "contains_ref"],
    )
}

#[test]
fn subtree_shared_call_target_appears_once() {
    let data = call_graph();
    let policy = allow_all(&data);
    let res = subtree(&data, &policy, 0, 12, true, Caps::default());
    assert!(!res.truncated);
    // 1 enters subprocess 3 (first encounter wins, traversal parent = 1);
    // when 2 is visited, 3 is already emitted and is not entered again.
    assert_eq!(
        res.rows,
        vec![
            srow(0, 0, None),
            srow(1, 1, Some(0)),
            srow(3, 2, Some(1)),
            srow(4, 3, Some(3)),
            srow(2, 1, Some(0)),
        ]
    );
}

#[test]
fn subtree_call_target_not_entered_without_flag() {
    let data = call_graph();
    let policy = allow_all(&data);
    let res = subtree(&data, &policy, 0, 12, false, Caps::default());
    assert!(!res.truncated);
    // The call nodes themselves appear (they are tree children); the called
    // subprocess is not entered.
    assert_eq!(
        res.rows,
        vec![srow(0, 0, None), srow(1, 1, Some(0)), srow(2, 1, Some(0))]
    );
}

#[test]
fn subtree_hidden_call_target_is_a_wall() {
    let data = call_graph();
    let policy = policy_hiding(&data, &[3]);
    let res = subtree(&data, &policy, 0, 12, true, Caps::default());
    assert!(!res.truncated);
    // The hidden subprocess (and everything below it) never appears even with
    // follow_call_refs on.
    assert_eq!(
        res.rows,
        vec![srow(0, 0, None), srow(1, 1, Some(0)), srow(2, 1, Some(0))]
    );
}

// ─── shortest_path ────────────────────────────────────────────────────────────

/// Chain 0→1→2→3. Forward rows 0:(0→1) 1:(1→2) 2:(2→3).
fn chain4() -> GraphData {
    tiny_graph(
        4,
        &[(0, 1, 0), (1, 2, 0), (2, 3, 0)],
        &[None; 4],
        &[None; 4],
        &["flows_to"],
    )
}

#[test]
fn path_simple_chain() {
    let data = chain4();
    let policy = allow_all(&data);
    let res = shortest_path(
        &data,
        &policy,
        0,
        3,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
    );
    assert!(!res.truncated);
    assert_eq!(
        res.rows,
        vec![
            origin(0),
            hop(1, 1, EdgeFile::Fwd, 0),
            hop(2, 2, EdgeFile::Fwd, 1),
            hop(3, 3, EdgeFile::Fwd, 2),
        ]
    );
}

#[test]
fn path_tie_break_lowest_neighbor_pos() {
    // Diamond: 0→1→3 and 0→2→3 are equal length; BFS must pick via 1.
    let data = diamond();
    let policy = allow_all(&data);
    let res = shortest_path(
        &data,
        &policy,
        0,
        3,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
    );
    assert!(!res.truncated);
    assert_eq!(
        res.rows,
        vec![
            origin(0),
            hop(1, 1, EdgeFile::Fwd, 0),
            hop(2, 3, EdgeFile::Fwd, 2),
        ]
    );
}

#[test]
fn path_cycle_terminates() {
    let data = tiny_graph(
        3,
        &[(0, 1, 0), (1, 2, 0), (2, 0, 0)],
        &[None; 3],
        &[None; 3],
        &["flows_to"],
    );
    let policy = allow_all(&data);
    let res = shortest_path(
        &data,
        &policy,
        0,
        2,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
    );
    assert!(!res.truncated);
    assert_eq!(
        res.rows,
        vec![
            origin(0),
            hop(1, 1, EdgeFile::Fwd, 0),
            hop(2, 2, EdgeFile::Fwd, 1),
        ]
    );
}

#[test]
fn path_from_equals_to() {
    let data = diamond();
    let policy = allow_all(&data);
    let res = shortest_path(
        &data,
        &policy,
        1,
        1,
        Direction::Both,
        &TypeFilter::AllAuthored,
        12,
    );
    assert!(!res.truncated);
    assert_eq!(res.rows, vec![origin(1)]);
}

#[test]
fn path_provably_none_is_not_truncated() {
    // Directional dead end: 0→1 only, asking for 1→0 outbound.
    let data = tiny_graph(2, &[(0, 1, 0)], &[None; 2], &[None; 2], &["flows_to"]);
    let policy = allow_all(&data);
    let res = shortest_path(
        &data,
        &policy,
        1,
        0,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
    );
    assert!(res.rows.is_empty());
    assert!(!res.truncated); // BFS exhausted: provably no path
}

#[test]
fn path_depth_cut_is_truncated() {
    let data = chain4();
    let policy = allow_all(&data);
    let res = shortest_path(
        &data,
        &policy,
        0,
        3,
        Direction::Out,
        &TypeFilter::AllAuthored,
        2, // 3 hops needed
    );
    assert!(res.rows.is_empty());
    assert!(res.truncated); // cut while node 3 was still reachable
}

// ─── reachable ────────────────────────────────────────────────────────────────

#[test]
fn reachable_cycle_each_node_once_at_min_depth() {
    let data = tiny_graph(
        3,
        &[(0, 1, 0), (1, 2, 0), (2, 0, 0)],
        &[None; 3],
        &[None; 3],
        &["flows_to"],
    );
    let policy = allow_all(&data);
    let res = reachable(
        &data,
        &policy,
        0,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
        Caps::default(),
    );
    assert!(!res.truncated);
    // Anchor excluded; the cycle terminates via the visited set.
    assert_eq!(res.rows, vec![rrow(1, 1, 0), rrow(2, 2, 0)]);
}

#[test]
fn reachable_direction_in_walks_upstream() {
    let data = chain4();
    let policy = allow_all(&data);
    let res = reachable(
        &data,
        &policy,
        3,
        Direction::In,
        &TypeFilter::AllAuthored,
        12,
        Caps::default(),
    );
    assert!(!res.truncated);
    assert_eq!(res.rows, vec![rrow(2, 1, 0), rrow(1, 2, 0), rrow(0, 3, 0)]);
}

#[test]
fn reachable_ordered_by_depth_then_pos() {
    let data = diamond();
    let policy = allow_all(&data);
    let res = reachable(
        &data,
        &policy,
        0,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
        Caps::default(),
    );
    assert!(!res.truncated);
    assert_eq!(res.rows, vec![rrow(1, 1, 0), rrow(2, 1, 0), rrow(3, 2, 0)]);
}

#[test]
fn reachable_first_edge_type_deterministic_across_parents() {
    // 0→1 (type a), 0→2 (type b), 1→3 (type b), 2→3 (type a).
    // Node 3 is reached at depth 2 via both types; parent 1 was discovered
    // first and expands first (FIFO), so type b (id 1) wins.
    let data = tiny_graph(
        4,
        &[(0, 1, 0), (0, 2, 1), (1, 3, 1), (2, 3, 0)],
        &[None; 4],
        &[None; 4],
        &["a", "b"],
    );
    let policy = allow_all(&data);
    let res = reachable(
        &data,
        &policy,
        0,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
        Caps::default(),
    );
    assert_eq!(res.rows, vec![rrow(1, 1, 0), rrow(2, 1, 1), rrow(3, 2, 1)]);
}

#[test]
fn reachable_first_edge_type_parallel_edges_same_parent() {
    // 0→1 twice: type 1 and type 0. Forward sort (src, type, dst) puts the
    // type-0 row first, and candidates sort by (pos, row): type 0 discovers.
    let data = tiny_graph(
        2,
        &[(0, 1, 1), (0, 1, 0)],
        &[None; 2],
        &[None; 2],
        &["a", "b"],
    );
    let policy = allow_all(&data);
    let res = reachable(
        &data,
        &policy,
        0,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
        Caps::default(),
    );
    assert_eq!(res.rows, vec![rrow(1, 1, 0)]);
}

#[test]
fn reachable_max_results_truncates() {
    let data = chain4();
    let policy = allow_all(&data);
    let caps = Caps {
        max_depth: 12,
        max_results: 2,
    };
    let res = reachable(
        &data,
        &policy,
        0,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
        caps,
    );
    assert!(res.truncated);
    assert_eq!(res.rows, vec![rrow(1, 1, 0), rrow(2, 2, 0)]);
}

#[test]
fn reachable_depth_cut_vs_complete_closure() {
    // Depth cut with more genuinely reachable: chain, max_depth 1.
    let data = chain4();
    let policy = allow_all(&data);
    let res = reachable(
        &data,
        &policy,
        0,
        Direction::Out,
        &TypeFilter::AllAuthored,
        1,
        Caps::default(),
    );
    assert!(res.truncated);
    assert_eq!(res.rows, vec![rrow(1, 1, 0)]);

    // Cycle closed exactly at the depth cap: the frontier remains but can
    // reach nothing new — not truncated.
    let cycle = tiny_graph(
        3,
        &[(0, 1, 0), (1, 2, 0), (2, 0, 0)],
        &[None; 3],
        &[None; 3],
        &["flows_to"],
    );
    let policy = allow_all(&cycle);
    let res = reachable(
        &cycle,
        &policy,
        0,
        Direction::Out,
        &TypeFilter::AllAuthored,
        2,
        Caps::default(),
    );
    assert!(!res.truncated);
    assert_eq!(res.rows, vec![rrow(1, 1, 0), rrow(2, 2, 0)]);
}

// ─── The wall (mini-T7) ───────────────────────────────────────────────────────

/// A=0, X=1, B=2, C=3. Routes A→X→B and A→C→B.
/// Forward rows: 0:(0→1) 1:(0→3) 2:(1→2) 3:(3→2).
/// Reverse rows: 0:(1←0) 1:(2←1) 2:(2←3) 3:(3←0).
fn wall_graph() -> GraphData {
    tiny_graph(
        4,
        &[(0, 1, 0), (1, 2, 0), (0, 3, 0), (3, 2, 0)],
        &[None; 4],
        &[None; 4],
        &["flows_to"],
    )
}

#[test]
fn wall_path_reroutes_around_hidden_node() {
    let data = wall_graph();
    let policy = policy_hiding(&data, &[1]); // X hidden
    let res = shortest_path(
        &data,
        &policy,
        0,
        2,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
    );
    assert!(!res.truncated);
    // The path goes A→C→B (2 hops), never through X.
    assert_eq!(
        res.rows,
        vec![
            origin(0),
            hop(1, 3, EdgeFile::Fwd, 1),
            hop(2, 2, EdgeFile::Fwd, 3),
        ]
    );
}

#[test]
fn wall_path_severed_when_both_routes_blocked() {
    let data = wall_graph();
    let policy = policy_hiding(&data, &[1, 3]); // X and C hidden
    let res = shortest_path(
        &data,
        &policy,
        0,
        2,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
    );
    assert!(res.rows.is_empty());
    assert!(!res.truncated); // provably no path — not a depth cut
}

#[test]
fn wall_reachable_respects_open_and_blocked_routes() {
    let data = wall_graph();

    // One route open: B is reachable via C, X is not in the closure.
    let policy = policy_hiding(&data, &[1]);
    let res = reachable(
        &data,
        &policy,
        0,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
        Caps::default(),
    );
    assert!(!res.truncated);
    assert_eq!(res.rows, vec![rrow(3, 1, 0), rrow(2, 2, 0)]);

    // Both routes blocked: B is unreachable.
    let policy = policy_hiding(&data, &[1, 3]);
    let res = reachable(
        &data,
        &policy,
        0,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
        Caps::default(),
    );
    assert!(!res.truncated);
    assert!(res.rows.is_empty());
}

#[test]
fn wall_hidden_node_appears_in_no_output() {
    let data = wall_graph();
    let policy = policy_hiding(&data, &[1]); // X hidden
    let x = 1i32;
    // Rows of the forward/reverse files that touch X.
    let fwd_touching_x: Vec<i32> = (0..data.fwd_src.len() as i32)
        .filter(|&r| data.fwd_src[r as usize] == x || data.fwd_dst[r as usize] == x)
        .collect();
    let rev_touching_x: Vec<i32> = (0..data.rev_src.len() as i32)
        .filter(|&r| data.rev_src[r as usize] == x || data.rev_dst[r as usize] == x)
        .collect();

    // neighbors — from every visible node, in both directions.
    for anchor in [0, 2, 3] {
        for dir in [Direction::Out, Direction::In, Direction::Both] {
            for h in neighbors(&data, &policy, anchor, dir, &TypeFilter::AllAuthored) {
                assert_ne!(h.node_pos, x, "X leaked as a neighbor of {anchor}");
                let touching = match h.edge_file {
                    EdgeFile::Fwd => &fwd_touching_x,
                    EdgeFile::Rev => &rev_touching_x,
                };
                assert!(!touching.contains(&h.edge_row), "edge touching X leaked");
            }
        }
    }

    // edges_of — anchored and global.
    for anchor in [None, Some(0), Some(2), Some(3)] {
        let res = edges_of(
            &data,
            &policy,
            anchor,
            Direction::Both,
            &TypeFilter::AllAuthored,
            Caps::default(),
        );
        for e in res.rows {
            let touching = match e.edge_file {
                EdgeFile::Fwd => &fwd_touching_x,
                EdgeFile::Rev => &rev_touching_x,
            };
            assert!(!touching.contains(&e.edge_row), "edge touching X leaked");
        }
    }
    // The global scan keeps exactly the two X-free edges.
    let res = edges_of(
        &data,
        &policy,
        None,
        Direction::Out,
        &TypeFilter::AllAuthored,
        Caps::default(),
    );
    let rows: Vec<i32> = res.rows.iter().map(|e| e.edge_row).collect();
    assert_eq!(rows, vec![1, 3]);

    // subtree, shortest_path, reachable — X in no row.
    for anchor in [0, 2, 3] {
        for row in subtree(&data, &policy, anchor, 12, true, Caps::default()).rows {
            assert_ne!(row.node_pos, x);
            assert_ne!(row.parent_pos, Some(x));
        }
        let res = reachable(
            &data,
            &policy,
            anchor,
            Direction::Both,
            &TypeFilter::AllAuthored,
            12,
            Caps::default(),
        );
        for row in res.rows {
            assert_ne!(row.node_pos, x, "X leaked from reachable({anchor})");
        }
    }
    for (from, to) in [(0, 2), (0, 3), (3, 2)] {
        for row in shortest_path(
            &data,
            &policy,
            from,
            to,
            Direction::Both,
            &TypeFilter::AllAuthored,
            12,
        )
        .rows
        {
            assert_ne!(row.node_pos, x, "X leaked on path {from}->{to}");
        }
    }
}

#[test]
fn wall_hidden_anchor_yields_empty_everywhere() {
    let data = wall_graph();
    let policy = policy_hiding(&data, &[1]);
    // The hidden node used as an anchor behaves as non-existent: every
    // algorithm returns empty without truncation.
    assert!(neighbors(&data, &policy, 1, Direction::Both, &TypeFilter::AllAuthored).is_empty());
    let res = edges_of(
        &data,
        &policy,
        Some(1),
        Direction::Both,
        &TypeFilter::AllAuthored,
        Caps::default(),
    );
    assert!(res.rows.is_empty() && !res.truncated);
    let res = subtree(&data, &policy, 1, 12, true, Caps::default());
    assert!(res.rows.is_empty() && !res.truncated);
    let res = shortest_path(
        &data,
        &policy,
        1,
        2,
        Direction::Out,
        &TypeFilter::AllAuthored,
        12,
    );
    assert!(res.rows.is_empty() && !res.truncated);
    let res = reachable(
        &data,
        &policy,
        1,
        Direction::Both,
        &TypeFilter::AllAuthored,
        12,
        Caps::default(),
    );
    assert!(res.rows.is_empty() && !res.truncated);
}
