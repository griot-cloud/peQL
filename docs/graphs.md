# Graph traversal

peQL has no graph functions of its own. A graph (for example a business-process snapshot) is two
contracts, one over its nodes and one over its edges, and traversal is recursive SQL over their
views. The contracts govern it like any other query.

```yaml
contract: process/nodes
binding: {parquet: graph/nodes/}
expose: [{name: pos, type: int32}, {name: name, type: utf8}]
rules:
  - {id: visible, op: admit, expr: "row.visibility == 'public' || 'ops' in ctx.roles"}
---
contract: process/edges
binding: {parquet: graph/edges/}
expose: [{name: src, type: int32}, {name: dst, type: int32}, {name: edge_type, type: utf8}]
rules:
  - {id: not_internal, op: admit, expr: "row.edge_type != 'depends_on' || 'ops' in ctx.roles"}
```

Everything reachable from a node:

```sql
WITH RECURSIVE reach(pos, depth) AS (
  SELECT pos, 0 FROM "process/nodes" WHERE pos = 0
  UNION
  SELECT e.dst, r.depth + 1
  FROM reach r
  JOIN "process/edges" e ON e.src = r.pos
  JOIN "process/nodes" n ON n.pos = e.dst
  WHERE r.depth < 20
)
SELECT DISTINCT pos FROM reach ORDER BY pos
```

A node the caller cannot see is absent from the nodes view, so the join stops there: no path
runs through it, and nothing reachable only through it is reached. An edge the caller cannot
see is absent from the edges view and is never followed. Always bound the depth.

Neighbours, subtrees and paths follow the same pattern: a join of the edges view with the nodes
view, recursive where the question is. `tests/graph.rs` checks the wall behaviour.

peQL 0.3 had seven graph table functions over its own snapshot format; they were removed in 0.4
so that graph queries go through the same contracts and gate as every other query.
