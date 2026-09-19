# Graph queries

peQL exposes graph snapshots through SQL table functions. The standalone
binding points to a directory with `manifest.json`, `nodes.parquet`,
`edges.parquet`, and `edges_rev.parquet`. The loader checks the manifest's
`bundle_format`, SHA-256 digest for each Parquet file, size limits, and
structural invariants before returning a snapshot. It does not verify a
certificate in the standalone path.

## Call a function

```sql
SELECT name, kind, owner
FROM graph_neighbors(
  'process-graphs/zijani-operations/v1',
  'Collection quarantined; jericans to segregated disposal'
)
```

The function arguments are positional literals. Optional trailing arguments
can be omitted or passed as `NULL`. `direction` accepts `out`, `in`, or `both`.
`edge_types` is a comma-separated string. When omitted, traversal includes
authored edge types other than `contains` and `contains_ref`; subtree traversal
uses containment edges.

| Function | Positional arguments | Result |
| --- | --- | --- |
| `graph_nodes` | `(graph_ref)` | Visible node rows. |
| `graph_node` | `(graph_ref, node)` | One node by name or ULID. |
| `graph_neighbors` | `(graph_ref, node [, direction [, edge_types]])` | Neighbor rows and connecting edge attributes. |
| `graph_edges` | `(graph_ref [, node [, direction [, edge_types]]])` | Visible edge rows, optionally attached to a node. |
| `graph_subtree` | `(graph_ref, node [, max_depth [, follow_call_refs]])` | Anchor and containment descendants. |
| `graph_path` | `(graph_ref, from, to [, direction [, max_depth [, edge_types]]])` | One shortest path, one row per hop. |
| `graph_reachable` | `(graph_ref, node [, direction [, max_depth [, edge_types]]])` | Distinct reachable nodes. |

The results are ordinary DataFusion tables, so SQL can project, filter, join,
aggregate, or use them in a CTE. Results include `snapshot_version`; traversal
results subject to caps include a `truncated` indicator. Use the actual result
schema when selecting additional columns because each function adds different
traversal fields.

## How visibility affects traversal

The engine resolves the graph's contract for the caller before loading graph
bytes. A denied or unknown graph receives the same unavailable error. For an
allowed graph, peQL evaluates `node_filter` and `edge_filter` against the
snapshot and compiles them into visibility masks.

A filtered node is omitted from output and cannot be used as an intermediate
hop. A filtered edge cannot connect two visible nodes. For example, if the
only path from A to C goes through filtered node B, `graph_path(A, C)` has no
visible path. The traversal algorithms consult the masks while walking the
graph; filtering only the final rows would disclose hidden topology.

The engine applies `masks` to assembled output values. The node and edge SQL
functions use this governed snapshot and output assembly. The underlying
Parquet files are not registered as raw tables in the caller's session.

## Limits and ordering

Path and reachability default to 12 levels. Subtree defaults to a maximum of
64. Traversals cap results at 10,000 rows in the current SQL functions, and
supplied `max_depth` values are capped at 64. Traversal uses visited sets to
terminate on cycles. Functions define
stable traversal order; add SQL `ORDER BY` when a consumer requires a
particular final row order.

The loader accepts at most 50,000 nodes and 200,000 edges in the current
snapshot format. Graph positions are internal to one snapshot. Use names or
ULIDs as identifiers outside the engine.

## Run the graph example

```bash
cargo run --example graph_query
```

The example uses the checked-in `zijani-operations-v1` snapshot and compares
owner and outside-tenant results. {doc}`CONTRACT-FORMAT` describes the graph
contract fields.
