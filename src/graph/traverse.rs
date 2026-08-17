//! Pure traversal algorithms over [`GraphData`] + [`GraphPolicy`].
//!
//! Governance (the **wall** rule, `docs/GRAPH-QUERY.md` §3.2 / G02 test T7) is
//! enforced identically in all five algorithms:
//!
//! - an invisible node is never **emitted** and never **expanded through** —
//!   no path routes across it, and nothing reachable only via it is reachable;
//! - an invisible edge row is never used nor reported.
//!
//! The [`GraphPolicy`] edge masks are already AND-ed with endpoint visibility
//! by the policy compiler; the algorithms nevertheless re-check the neighbor
//! node's visibility before every emit *and* every expansion (defence in depth
//! against a policy constructed outside the compiler).
//!
//! Every algorithm is deterministic: expansion and output orders are fixed by
//! explicit sorts, never by incidental slice order.

use super::types::{
    Caps, Direction, EdgeFile, EdgeHit, GraphData, GraphPolicy, NeighborHit, PathHop, ReachRow,
    SubtreeRow, Truncatable, TypeFilter,
};

// ─── Shared expansion machinery ───────────────────────────────────────────────

/// Row range of `pos`'s outgoing edges in the forward file (CSR slice).
fn fwd_slice(data: &GraphData, pos: i32) -> std::ops::Range<usize> {
    let start = data.out_start[pos as usize] as usize;
    start..start + data.out_count[pos as usize] as usize
}

/// Row range of `pos`'s incoming edges in the reverse file (CSR slice).
fn rev_slice(data: &GraphData, pos: i32) -> std::ops::Range<usize> {
    let start = data.in_start[pos as usize] as usize;
    start..start + data.in_count[pos as usize] as usize
}

/// Tie-break rank for edge files (`Fwd` sorts before `Rev`).
fn file_rank(file: EdgeFile) -> u8 {
    match file {
        EdgeFile::Fwd => 0,
        EdgeFile::Rev => 1,
    }
}

/// One qualifying (visible edge → visible neighbor) expansion candidate.
#[derive(Debug, Clone, Copy)]
struct Candidate {
    /// The neighbor node reached through the edge.
    neighbor: i32,
    /// Which file `edge_row` indexes.
    edge_file: EdgeFile,
    /// The connecting edge's row in that file.
    edge_row: i32,
    /// The edge's type id.
    edge_type: u16,
}

/// Every qualifying candidate around `node` per `direction`: edge type in
/// `allowed`, edge row visible, neighbor node visible. Sorted by
/// `(neighbor pos, file, edge_row)` — the G02 §7.5 deterministic expansion
/// order ("lowest neighbor pos first").
fn sorted_candidates(
    data: &GraphData,
    policy: &GraphPolicy,
    node: i32,
    direction: Direction,
    allowed: &[u16],
) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    if matches!(direction, Direction::Out | Direction::Both) {
        for r in fwd_slice(data, node) {
            let t = data.fwd_type[r];
            let dst = data.fwd_dst[r];
            if allowed.contains(&t)
                && policy.fwd_edge_visible[r]
                && policy.node_visible[dst as usize]
            {
                candidates.push(Candidate {
                    neighbor: dst,
                    edge_file: EdgeFile::Fwd,
                    edge_row: r as i32,
                    edge_type: t,
                });
            }
        }
    }
    if matches!(direction, Direction::In | Direction::Both) {
        for r in rev_slice(data, node) {
            let t = data.rev_type[r];
            let src = data.rev_src[r];
            if allowed.contains(&t)
                && policy.rev_edge_visible[r]
                && policy.node_visible[src as usize]
            {
                candidates.push(Candidate {
                    neighbor: src,
                    edge_file: EdgeFile::Rev,
                    edge_row: r as i32,
                    edge_type: t,
                });
            }
        }
    }
    candidates.sort_unstable_by_key(|c| (c.neighbor, file_rank(c.edge_file), c.edge_row));
    candidates
}

/// `true` if any node in `frontier` still has a qualifying **undiscovered**
/// neighbor — i.e. a depth cap genuinely cut the search short (as opposed to
/// the frontier merely existing with nothing new left to reach).
fn frontier_expandable(
    data: &GraphData,
    policy: &GraphPolicy,
    frontier: &[i32],
    discovered: &[bool],
    direction: Direction,
    allowed: &[u16],
) -> bool {
    frontier.iter().any(|&node| {
        sorted_candidates(data, policy, node, direction, allowed)
            .iter()
            .any(|c| !discovered[c.neighbor as usize])
    })
}

// ─── The five algorithms ──────────────────────────────────────────────────────

/// Neighbors of `anchor` (G02 §7.2). One hit per (neighbor, qualifying edge) —
/// a neighbor connected by *k* qualifying edges appears *k* times.
/// Order: direction (out first), then edge_type id, then neighbor pos, then
/// edge row (stable).
pub fn neighbors(
    data: &GraphData,
    policy: &GraphPolicy,
    anchor: i32,
    direction: Direction,
    types: &TypeFilter,
) -> Vec<NeighborHit> {
    let mut hits = Vec::new();
    if !policy.node_visible[anchor as usize] {
        return hits;
    }
    let allowed = types.allowed_ids(data);

    if matches!(direction, Direction::Out | Direction::Both) {
        // (edge_type, neighbor pos, edge row) — the required out-group order.
        let mut out: Vec<(u16, i32, i32)> = Vec::new();
        for r in fwd_slice(data, anchor) {
            let t = data.fwd_type[r];
            let dst = data.fwd_dst[r];
            if allowed.contains(&t)
                && policy.fwd_edge_visible[r]
                && policy.node_visible[dst as usize]
            {
                out.push((t, dst, r as i32));
            }
        }
        out.sort_unstable();
        hits.extend(out.into_iter().map(|(_, node_pos, edge_row)| NeighborHit {
            node_pos,
            edge_file: EdgeFile::Fwd,
            edge_row,
            direction: Direction::Out,
        }));
    }
    if matches!(direction, Direction::In | Direction::Both) {
        let mut inc: Vec<(u16, i32, i32)> = Vec::new();
        for r in rev_slice(data, anchor) {
            let t = data.rev_type[r];
            let src = data.rev_src[r];
            if allowed.contains(&t)
                && policy.rev_edge_visible[r]
                && policy.node_visible[src as usize]
            {
                inc.push((t, src, r as i32));
            }
        }
        inc.sort_unstable();
        hits.extend(inc.into_iter().map(|(_, node_pos, edge_row)| NeighborHit {
            node_pos,
            edge_file: EdgeFile::Rev,
            edge_row,
            direction: Direction::In,
        }));
    }
    hits
}

/// Edge-centric view (G02 §7.3): `anchor`'s incident qualifying visible edges
/// (`Out` = forward-slice rows, `In` = reverse-slice rows, `Both` = both — an
/// edge appears once per role), or **all** qualifying visible edges from the
/// forward file when `anchor` is `None` (each logical edge once; `direction`
/// ignored). Order: forward rows ascending, then reverse rows ascending.
/// `caps.max_results` truncates; `truncated` is set only if a further row
/// would have qualified.
pub fn edges_of(
    data: &GraphData,
    policy: &GraphPolicy,
    anchor: Option<i32>,
    direction: Direction,
    types: &TypeFilter,
    caps: Caps,
) -> Truncatable<EdgeHit> {
    let allowed = types.allowed_ids(data);
    let cap = caps.max_results;
    let mut rows: Vec<EdgeHit> = Vec::new();
    let mut truncated = false;

    'collect: {
        match anchor {
            Some(a) => {
                if !policy.node_visible[a as usize] {
                    break 'collect;
                }
                if matches!(direction, Direction::Out | Direction::Both) {
                    for r in fwd_slice(data, a) {
                        if allowed.contains(&data.fwd_type[r])
                            && policy.fwd_edge_visible[r]
                            && policy.node_visible[data.fwd_dst[r] as usize]
                        {
                            if rows.len() >= cap {
                                truncated = true;
                                break 'collect;
                            }
                            rows.push(EdgeHit {
                                edge_file: EdgeFile::Fwd,
                                edge_row: r as i32,
                            });
                        }
                    }
                }
                if matches!(direction, Direction::In | Direction::Both) {
                    for r in rev_slice(data, a) {
                        if allowed.contains(&data.rev_type[r])
                            && policy.rev_edge_visible[r]
                            && policy.node_visible[data.rev_src[r] as usize]
                        {
                            if rows.len() >= cap {
                                truncated = true;
                                break 'collect;
                            }
                            rows.push(EdgeHit {
                                edge_file: EdgeFile::Rev,
                                edge_row: r as i32,
                            });
                        }
                    }
                }
            }
            None => {
                for r in 0..data.fwd_type.len() {
                    if allowed.contains(&data.fwd_type[r])
                        && policy.fwd_edge_visible[r]
                        && policy.node_visible[data.fwd_src[r] as usize]
                        && policy.node_visible[data.fwd_dst[r] as usize]
                    {
                        if rows.len() >= cap {
                            truncated = true;
                            break 'collect;
                        }
                        rows.push(EdgeHit {
                            edge_file: EdgeFile::Fwd,
                            edge_row: r as i32,
                        });
                    }
                }
            }
        }
    }

    Truncatable { rows, truncated }
}

/// Containment expansion (G02 §7.4): the anchor (depth 0, `parent_pos` from
/// the data) plus its descendants via the derived containment tree
/// ([`GraphData::children`]). Depth-first **preorder**, children ascending by
/// pos.
///
/// With `follow_call_refs`, a visited node's `call_ref_pos` target (if
/// visible) is entered as an additional child at `depth + 1` **after** its
/// real children; a visited-set over all emitted nodes guarantees a shared
/// subprocess appears exactly once (first encounter in traversal order wins).
/// Emitted call-target rows carry the *traversal* parent (the calling node).
///
/// Walls: an invisible child is not emitted and its whole subtree is never
/// entered. Nodes at `depth == max_depth` are emitted but not expanded —
/// leaving qualifying children unexpanded sets `truncated`, as does hitting
/// `caps.max_results`.
pub fn subtree(
    data: &GraphData,
    policy: &GraphPolicy,
    anchor: i32,
    max_depth: usize,
    follow_call_refs: bool,
    caps: Caps,
) -> Truncatable<SubtreeRow> {
    let mut rows: Vec<SubtreeRow> = Vec::new();
    let mut truncated = false;
    if !policy.node_visible[anchor as usize] {
        return Truncatable { rows, truncated };
    }

    let mut visited = vec![false; data.node_count()];
    // (node, depth, parent to report). Popped order = depth-first preorder.
    let mut stack: Vec<(i32, usize, Option<i32>)> =
        vec![(anchor, 0, data.parent_pos[anchor as usize])];

    while let Some((pos, depth, parent)) = stack.pop() {
        let p = pos as usize;
        if visited[p] || !policy.node_visible[p] {
            continue;
        }
        if rows.len() >= caps.max_results {
            truncated = true;
            break;
        }
        visited[p] = true;
        rows.push(SubtreeRow {
            node_pos: pos,
            depth: depth as u32,
            parent_pos: parent,
        });

        // Qualifying expansions: real children (ascending pos), then the call
        // target (after the real children) when following call refs.
        let mut expansions: Vec<i32> = data.children[p]
            .iter()
            .copied()
            .filter(|&c| policy.node_visible[c as usize] && !visited[c as usize])
            .collect();
        if follow_call_refs {
            if let Some(target) = data.call_ref_pos[p] {
                if policy.node_visible[target as usize] && !visited[target as usize] {
                    expansions.push(target);
                }
            }
        }
        if expansions.is_empty() {
            continue;
        }
        if depth >= max_depth {
            // Depth cap hit with qualifying children left unexpanded.
            truncated = true;
            continue;
        }
        // Reverse push so the lowest-pos child is popped (visited) first.
        for &child in expansions.iter().rev() {
            stack.push((child, depth + 1, Some(pos)));
        }
    }

    Truncatable { rows, truncated }
}

/// Reconstruct the discovered BFS path `from → … → to` from the parent map.
/// Row 0 is the origin (`has_edge: false`, dummy `Fwd`/`0` edge fields).
fn reconstruct_path(
    parent: &[Option<(i32, EdgeFile, i32)>],
    from: i32,
    to: i32,
) -> Truncatable<PathHop> {
    let mut hops: Vec<PathHop> = Vec::new();
    let mut cur = to;
    while cur != from {
        let (prev, edge_file, edge_row) =
            parent[cur as usize].expect("BFS parent chain intact between from and to");
        hops.push(PathHop {
            step: 0, // fixed after the reverse below
            node_pos: cur,
            edge_file,
            edge_row,
            has_edge: true,
        });
        cur = prev;
    }
    hops.push(PathHop {
        step: 0,
        node_pos: from,
        edge_file: EdgeFile::Fwd,
        edge_row: 0,
        has_edge: false,
    });
    hops.reverse();
    for (i, hop) in hops.iter_mut().enumerate() {
        hop.step = i as u32;
    }
    Truncatable {
        rows: hops,
        truncated: false,
    }
}

/// One shortest path by hop count (G02 §7.5). BFS from `from` toward `to`
/// over qualifying visible edges; a node's BFS parent is fixed by first
/// discovery, and each frontier node expands its neighbors in ascending
/// `(neighbor pos, edge row)` order (deterministic tie-break: lowest neighbor
/// pos first), parents in FIFO order.
///
/// `from == to` returns the single origin row. An empty result with
/// `truncated == false` is a **proof of no path** (BFS exhausted within
/// `max_depth`); with `truncated == true` the search was cut by `max_depth`
/// while undiscovered nodes were still expandable.
pub fn shortest_path(
    data: &GraphData,
    policy: &GraphPolicy,
    from: i32,
    to: i32,
    direction: Direction,
    types: &TypeFilter,
    max_depth: usize,
) -> Truncatable<PathHop> {
    let empty = |truncated: bool| Truncatable {
        rows: Vec::new(),
        truncated,
    };
    if !policy.node_visible[from as usize] || !policy.node_visible[to as usize] {
        return empty(false);
    }
    if from == to {
        return Truncatable {
            rows: vec![PathHop {
                step: 0,
                node_pos: from,
                edge_file: EdgeFile::Fwd,
                edge_row: 0,
                has_edge: false,
            }],
            truncated: false,
        };
    }

    let allowed = types.allowed_ids(data);
    let mut discovered = vec![false; data.node_count()];
    discovered[from as usize] = true;
    let mut parent: Vec<Option<(i32, EdgeFile, i32)>> = vec![None; data.node_count()];

    // Level-synchronous BFS: `frontier` holds depth-`depth` nodes in FIFO
    // (discovery) order.
    let mut frontier: Vec<i32> = vec![from];
    let mut depth = 0usize;
    while depth < max_depth && !frontier.is_empty() {
        let mut next: Vec<i32> = Vec::new();
        for &node in &frontier {
            for cand in sorted_candidates(data, policy, node, direction, &allowed) {
                let np = cand.neighbor as usize;
                if discovered[np] {
                    continue;
                }
                discovered[np] = true;
                parent[np] = Some((node, cand.edge_file, cand.edge_row));
                if cand.neighbor == to {
                    return reconstruct_path(&parent, from, to);
                }
                next.push(cand.neighbor);
            }
        }
        frontier = next;
        depth += 1;
    }

    if frontier.is_empty() {
        // BFS exhausted within the depth budget: provably no path.
        empty(false)
    } else {
        // Depth cut. Disclose truncation only if the frontier could still
        // reach something new.
        empty(frontier_expandable(
            data,
            policy,
            &frontier,
            &discovered,
            direction,
            &allowed,
        ))
    }
}

/// Reachability closure (G02 §7.6): every distinct visible node reachable
/// from `anchor` (anchor excluded), each once at its minimum BFS depth, with
/// `first_edge_type` = the type of the edge by which BFS **first** discovered
/// it (deterministic: parents expand in FIFO order, each in ascending
/// `(neighbor pos, edge row)` candidate order). Results ordered by
/// `(min_depth, pos)`. Cycles terminate via the discovered-set.
///
/// `truncated` is set when `caps.max_results` cuts the row list, or when the
/// `max_depth` cut left the final frontier with undiscovered qualifying
/// neighbors still reachable.
pub fn reachable(
    data: &GraphData,
    policy: &GraphPolicy,
    anchor: i32,
    direction: Direction,
    types: &TypeFilter,
    max_depth: usize,
    caps: Caps,
) -> Truncatable<ReachRow> {
    let mut rows: Vec<ReachRow> = Vec::new();
    let mut truncated = false;
    if !policy.node_visible[anchor as usize] {
        return Truncatable { rows, truncated };
    }

    let allowed = types.allowed_ids(data);
    let mut discovered = vec![false; data.node_count()];
    discovered[anchor as usize] = true;

    let mut frontier: Vec<i32> = vec![anchor];
    let mut depth = 0usize;
    'bfs: while depth < max_depth && !frontier.is_empty() {
        // This level's discoveries, in discovery (FIFO expansion) order.
        let mut level: Vec<ReachRow> = Vec::new();
        let mut next: Vec<i32> = Vec::new();
        for &node in &frontier {
            for cand in sorted_candidates(data, policy, node, direction, &allowed) {
                let np = cand.neighbor as usize;
                if discovered[np] {
                    continue;
                }
                discovered[np] = true;
                level.push(ReachRow {
                    node_pos: cand.neighbor,
                    min_depth: (depth + 1) as u32,
                    first_edge_type: cand.edge_type,
                });
                next.push(cand.neighbor);
            }
        }
        // Output order within one depth: ascending pos.
        level.sort_unstable_by_key(|r| r.node_pos);
        for row in level {
            if rows.len() >= caps.max_results {
                truncated = true;
                break 'bfs;
            }
            rows.push(row);
        }
        frontier = next;
        depth += 1;
    }

    if !truncated
        && !frontier.is_empty()
        && frontier_expandable(data, policy, &frontier, &discovered, direction, &allowed)
    {
        // The depth cap stopped the walk while new nodes were still reachable.
        truncated = true;
    }

    Truncatable { rows, truncated }
}
