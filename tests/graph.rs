//! Graph traversal without graph code in the engine: nodes and edges are two contracts, and
//! traversal is recursive SQL over their views. A node the caller cannot see is a wall (no
//! path runs through it) and an edge the caller cannot see is never used.

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use peql::{Caller, Engine, WriteMode};

const NODES: &str = r#"
contract: process/nodes
version: 1
binding: {parquet: graph/nodes/}
expose: [{name: pos, type: int32}, {name: name, type: utf8}]
rules:
  - {id: visible, op: admit, expr: "row.visibility == 'public' || 'ops' in ctx.roles"}
"#;

const EDGES: &str = r#"
contract: process/edges
version: 1
binding: {parquet: graph/edges/}
expose: [{name: src, type: int32}, {name: dst, type: int32}, {name: edge_type, type: utf8}]
rules:
  - {id: not_internal, op: admit, expr: "row.edge_type != 'depends_on' || 'ops' in ctx.roles"}
"#;

//   0 → 1 → 2 → 3,  0 → 4 → 5 → 3,  4 ⇢ 6 (depends_on).  Node 1 is internal.
async fn engine(dir: &std::path::Path) -> Engine {
    let nodes = Arc::new(Schema::new(vec![
        Field::new("pos", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("visibility", DataType::Utf8, false),
    ]));
    let edges = Arc::new(Schema::new(vec![
        Field::new("src", DataType::Int32, false),
        Field::new("dst", DataType::Int32, false),
        Field::new("edge_type", DataType::Utf8, false),
    ]));
    let e = Engine::in_memory(dir);
    e.register_contract(NODES, &nodes).unwrap();
    e.register_contract(EDGES, &edges).unwrap();
    let vis = [
        "public", "internal", "public", "public", "public", "public", "public",
    ];
    e.write(
        "process/nodes",
        vec![
            RecordBatch::try_new(
                nodes,
                vec![
                    Arc::new(Int32Array::from((0..7).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(
                        (0..7).map(|i| format!("step {i}")).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(vis.to_vec())),
                ],
            )
            .unwrap(),
        ],
        WriteMode::Overwrite,
    )
    .await
    .unwrap();
    let (src, dst, ty): (Vec<i32>, Vec<i32>, Vec<&str>) = [
        (0, 1, "next"),
        (1, 2, "next"),
        (2, 3, "next"),
        (0, 4, "next"),
        (4, 5, "next"),
        (5, 3, "next"),
        (4, 6, "depends_on"),
    ]
    .into_iter()
    .fold((vec![], vec![], vec![]), |mut a, (s, d, t)| {
        a.0.push(s);
        a.1.push(d);
        a.2.push(t);
        a
    });
    e.write(
        "process/edges",
        vec![
            RecordBatch::try_new(
                edges,
                vec![
                    Arc::new(Int32Array::from(src)),
                    Arc::new(Int32Array::from(dst)),
                    Arc::new(StringArray::from(ty)),
                ],
            )
            .unwrap(),
        ],
        WriteMode::Overwrite,
    )
    .await
    .unwrap();
    e
}

/// Every node reachable from node 0, through visible nodes and edges only.
const REACHABLE: &str = r#"
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
"#;

async fn reachable(e: &Engine, caller: &Caller) -> Vec<String> {
    let res = e.query(REACHABLE, caller).await.unwrap();
    res.batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows())
                .map(|i| array_value_to_string(b.column(0), i).unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn hidden_nodes_are_walls_and_hidden_edges_are_unused() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path()).await;
    // Node 1 is a wall: 2 is reachable only through it. 3 is still reachable via 4 and 5.
    // The depends_on edge to 6 is hidden.
    let analyst = Caller::new("a", "t", "analytics");
    assert_eq!(reachable(&e, &analyst).await, ["0", "3", "4", "5"]);
    // An operator sees every node and edge.
    let ops = Caller::new("o", "t", "analytics").with_roles(&["ops"]);
    assert_eq!(
        reachable(&e, &ops).await,
        ["0", "1", "2", "3", "4", "5", "6"]
    );
}
