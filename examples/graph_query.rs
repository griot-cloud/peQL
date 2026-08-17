//! Example 7 — governed graph traversal over a REAL compiled process graph.
//!
//! The 2.0 headline: an agent holds a step name from contract search and asks
//! the engine to walk the business's process map — in plain SQL, under the
//! same contract governance as every table. The snapshot here is the real
//! `zijani-operations` bundle (271 nodes / 562 edges) compiled by the G01
//! pipeline; traversal runs on its precompiled CSR offsets.
//!
//! Shows: relational scans, neighbors, subtree, reachability, owner-vs-outsider
//! masking, and the governance wall (a policy-hidden node is absent AND
//! non-traversable — no topology leak).
//!
//! Run with:
//!   cargo run --example graph_query

use datafusion::arrow::util::pretty::pretty_format_batches;
use griot::contract_source::Caller;
use griot::engine::GriotEngine;

const GRAPH: &str = "process-graphs/zijani-operations/v1";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/graph/zijani-operations-v1"
    );

    // ── The graph contract (the DPI surface for the process map) ──────────
    // Owned by zijani. Outsiders get `owner` masked and every `process` node
    // hidden — hidden means WALLED: absent from results and non-traversable.
    let contract = serde_json::json!({
        "contract_id": "zijani_ops_graph",
        "version": "1",
        "dataset": GRAPH,
        "binding": { "graph_snapshot": fixture },
        "owner_tenant": "zijani",
        "purposes": ["process_analysis"],
        "masks": { "owner": "redact" },
        "node_filter": "kind != 'process'"
    })
    .to_string();

    let engine = GriotEngine::from_json_contracts([contract])?;
    let owner = Caller::new("user:ops", "process_analysis", "zijani");
    let outsider = Caller::new("svc:partner", "process_analysis", "globex");

    // ── 1. The map at a glance (relational scan + ordinary SQL) ───────────
    println!("== 1. Who runs this business? (owner view — relational GROUP BY) ==");
    let out = engine
        .query(
            &format!(
                r#"SELECT owner, count(*) AS steps
                   FROM graph_nodes('{GRAPH}')
                   WHERE kind IN ('step','decision')
                   GROUP BY owner ORDER BY steps DESC LIMIT 5"#
            ),
            owner.clone(),
        )
        .await?;
    println!("{}\n", pretty_format_batches(&out)?);

    // ── 2. Walk the graph: what surrounds a step? ─────────────────────────
    let anchor = "Collection quarantined; jericans to segregated disposal";
    println!("== 2. Neighbors of '{anchor}' (owner) ==");
    let out = engine
        .query(
            &format!(
                r#"SELECT name, kind, edge_type, direction
                   FROM graph_neighbors('{GRAPH}', '{anchor}')"#
            ),
            owner.clone(),
        )
        .await?;
    println!("{}\n", pretty_format_batches(&out)?);

    // ── 3. Everything upstream/downstream (impact analysis) ───────────────
    println!("== 3. Reachable within 3 hops (owner) ==");
    let out = engine
        .query(
            &format!(
                r#"SELECT min_depth, name, first_edge_type
                   FROM graph_reachable('{GRAPH}', '{anchor}', 'both', 3)
                   ORDER BY min_depth LIMIT 8"#
            ),
            owner.clone(),
        )
        .await?;
    println!("{}\n", pretty_format_batches(&out)?);

    // ── 4. Same question, outside tenant: masked + walled ─────────────────
    println!("== 4. The SAME neighbors query as the outside tenant ==");
    let out = engine
        .query(
            &format!(
                r#"SELECT name, kind, owner, edge_type
                   FROM graph_neighbors('{GRAPH}', '{anchor}')"#
            ),
            outsider.clone(),
        )
        .await?;
    println!("{}", pretty_format_batches(&out)?);
    println!("owner column is masked; any `process` node is walled off entirely.\n");

    // ── 5. The wall is not an existence oracle ────────────────────────────
    println!("== 5. Outsider asks for a hidden process node directly ==");
    match engine
        .query(
            &format!(r#"SELECT * FROM graph_node('{GRAPH}', 'ZJ-11 Jerican Disposal')"#),
            outsider,
        )
        .await
    {
        Ok(_) => println!("  (unexpected: hidden node visible)"),
        Err(e) => println!("  refused like any unknown node: {e}\n"),
    }

    Ok(())
}
