//! End-to-end tests for the graph SQL functions against the REAL
//! `zijani-operations` snapshot bundle (271 nodes / 562 edges), through
//! `GriotEngine::query` — contract resolution, governed traversal, masking,
//! walls, caching, and SQL composability.
//!
//! G02 acceptance mapping: T1/T2 (resolution + uniform deny), T3 (schemas +
//! unknown-node suggestions), T4 (composability), T5 (cache), T7 (topology
//! leak / walls + masks), T10 (relational scans under the same policy).

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;

use griot::contract_source::Caller;
use griot::engine::GriotEngine;

const GRAPH_REF: &str = "process-graphs/zijani-operations/v1";

fn fixture_dir() -> String {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/graph/zijani-operations-v1"
    )
    .to_string()
}

/// A graph contract over the real bundle: owned by `zijani`; outsiders get
/// `owner` masked, `system` (process) nodes hidden, and `hands_off_to`
/// relationships hidden.
fn graph_contract(node_filter: Option<&str>, edge_filter: Option<&str>) -> String {
    let mut c = serde_json::json!({
        "contract_id": "zijani_ops_graph",
        "version": "1",
        "dataset": GRAPH_REF,
        "binding": { "graph_snapshot": fixture_dir() },
        "owner_tenant": "zijani",
        "purposes": ["process_analysis"],
        "masks": { "owner": "redact" }
    });
    if let Some(nf) = node_filter {
        c["node_filter"] = serde_json::Value::String(nf.to_string());
    }
    if let Some(ef) = edge_filter {
        c["edge_filter"] = serde_json::Value::String(ef.to_string());
    }
    c.to_string()
}

fn engine(node_filter: Option<&str>, edge_filter: Option<&str>) -> GriotEngine {
    GriotEngine::from_json_contracts([graph_contract(node_filter, edge_filter)]).unwrap()
}

fn owner() -> Caller {
    Caller::new("user:ops", "process_analysis", "zijani")
}

fn outsider() -> Caller {
    Caller::new("user:ext", "process_analysis", "globex")
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn str_col(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().index_of(name).unwrap();
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i).to_string())
            });
        }
    }
    out
}

// A process node name that exists in the fixture.
const PROCESS_NODE: &str = "ZJ-11 Jerican Disposal";

// ─── T1/T3: resolution + basic verbs on the real bundle ───────────────────────

#[tokio::test]
async fn owner_sees_all_nodes_relationally() {
    let e = engine(None, None);
    let out = e
        .query(
            &format!(r#"SELECT name, kind, owner FROM graph_nodes('{GRAPH_REF}')"#),
            owner(),
        )
        .await
        .unwrap();
    assert_eq!(rows(&out), 271, "the full zijani graph");
    // Owner sees raw owner values.
    assert!(str_col(&out, "owner")
        .iter()
        .flatten()
        .any(|o| o != "***" && !o.is_empty()));
}

#[tokio::test]
async fn graph_node_lookup_and_snapshot_disclosure() {
    let e = engine(None, None);
    let out = e
        .query(
            &format!(r#"SELECT name, kind, snapshot_version FROM graph_node('{GRAPH_REF}', '{PROCESS_NODE}')"#),
            owner(),
        )
        .await
        .unwrap();
    assert_eq!(rows(&out), 1);
    assert_eq!(str_col(&out, "kind")[0].as_deref(), Some("process"));
    // R7: snapshot_version disclosed on every result.
    let sv = &out[0]
        .column(out[0].schema().index_of("snapshot_version").unwrap())
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        .unwrap()
        .value(0);
    assert_eq!(*sv, 1);
}

#[tokio::test]
async fn neighbors_subtree_path_reachable_run() {
    let e = engine(None, None);
    // Neighbors of a real process node (both directions, all authored types).
    let n = e
        .query(
            &format!(r#"SELECT name, edge_type, direction FROM graph_neighbors('{GRAPH_REF}', '{PROCESS_NODE}')"#),
            owner(),
        )
        .await
        .unwrap();
    assert!(rows(&n) > 0, "process node has authored neighbors");

    // Subtree: contains-expansion of the same process.
    let s = e
        .query(
            &format!(r#"SELECT name, depth FROM graph_subtree('{GRAPH_REF}', '{PROCESS_NODE}')"#),
            owner(),
        )
        .await
        .unwrap();
    assert!(rows(&s) > 1, "subtree includes the anchor and children");

    // Reachable downstream of the anchor.
    let r = e
        .query(
            &format!(r#"SELECT name, min_depth, first_edge_type FROM graph_reachable('{GRAPH_REF}', '{PROCESS_NODE}', 'both', 4)"#),
            owner(),
        )
        .await
        .unwrap();
    assert!(rows(&r) > 0);

    // A path from the anchor to one of its reachable nodes.
    let target = str_col(&r, "name")[0].clone().unwrap();
    let p = e
        .query(
            &format!(r#"SELECT step, name FROM graph_path('{GRAPH_REF}', '{PROCESS_NODE}', '{target}', 'both')"#),
            owner(),
        )
        .await
        .unwrap();
    assert!(rows(&p) >= 2, "path has origin + at least one hop");
}

// ─── T2: deny and unknown are byte-identical ──────────────────────────────────

#[tokio::test]
async fn deny_is_uniform_not_found() {
    let e = engine(None, None);

    // Denied (wrong purpose) on a real graph.
    let denied = e
        .query(
            &format!(r#"SELECT * FROM graph_nodes('{GRAPH_REF}')"#),
            Caller::new("user:ext", "espionage", "globex"),
        )
        .await
        .unwrap_err()
        .to_string();

    // Unknown graph, same caller.
    let unknown = e
        .query(
            r#"SELECT * FROM graph_nodes('process-graphs/does-not-exist/v9')"#,
            Caller::new("user:ext", "espionage", "globex"),
        )
        .await
        .unwrap_err()
        .to_string();

    let tmpl = |r: &str| format!("graph dataset '{r}' is not available to this caller");
    assert!(denied.contains(&tmpl(GRAPH_REF)), "denied: {denied}");
    assert!(
        unknown.contains(&tmpl("process-graphs/does-not-exist/v9")),
        "unknown: {unknown}"
    );
    // Same template — no existence oracle.
    assert_eq!(
        denied.replace(GRAPH_REF, "X"),
        unknown.replace("process-graphs/does-not-exist/v9", "X")
    );
}

// ─── T3: unknown node carries near-miss suggestions ───────────────────────────

#[tokio::test]
async fn unknown_node_suggests_near_misses() {
    let e = engine(None, None);
    let err = e
        .query(
            &format!(
                r#"SELECT * FROM graph_node('{GRAPH_REF}', 'ZJ-11 jerican disposal MISSPELT')"#
            ),
            owner(),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no node matches"), "{err}");
    assert!(err.contains("name or node_id"), "{err}");
}

// ─── T7: masks + walls (the topology-leak battery, on the real bundle) ────────

#[tokio::test]
async fn outsider_owner_column_is_masked_in_every_verb() {
    let e = engine(None, None);
    for sql in [
        format!(r#"SELECT owner FROM graph_nodes('{GRAPH_REF}')"#),
        format!(r#"SELECT owner FROM graph_node('{GRAPH_REF}', '{PROCESS_NODE}')"#),
        format!(r#"SELECT owner FROM graph_neighbors('{GRAPH_REF}', '{PROCESS_NODE}')"#),
        format!(r#"SELECT owner FROM graph_subtree('{GRAPH_REF}', '{PROCESS_NODE}')"#),
        format!(r#"SELECT owner FROM graph_reachable('{GRAPH_REF}', '{PROCESS_NODE}', 'both', 3)"#),
    ] {
        let out = e.query(&sql, outsider()).await.unwrap();
        for v in str_col(&out, "owner").iter().flatten() {
            assert_eq!(v, "***", "masked owner leaked through: {sql}");
        }
    }
}

#[tokio::test]
async fn walls_hide_and_block_traversal() {
    // Hide every `process` node from outsiders.
    let e = engine(Some("kind != 'process'"), None);

    // 1. Hidden nodes appear in no relational result.
    let out = e
        .query(
            &format!(r#"SELECT kind, count(*) AS n FROM graph_nodes('{GRAPH_REF}') GROUP BY kind"#),
            outsider(),
        )
        .await
        .unwrap();
    let kinds = str_col(&out, "kind");
    assert!(
        !kinds.iter().flatten().any(|k| k == "process"),
        "process nodes must be invisible"
    );

    // 2. Direct lookup behaves as unknown (no existence oracle).
    let err = e
        .query(
            &format!(r#"SELECT * FROM graph_node('{GRAPH_REF}', '{PROCESS_NODE}')"#),
            outsider(),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no node matches"), "{err}");

    // 3. The owner still sees everything (different policy, different cache).
    let all = e
        .query(
            &format!(r#"SELECT * FROM graph_node('{GRAPH_REF}', '{PROCESS_NODE}')"#),
            owner(),
        )
        .await
        .unwrap();
    assert_eq!(rows(&all), 1);

    // 4. No neighbor result ever contains a hidden node, in either direction.
    let n = e
        .query(
            &format!(
                r#"SELECT kind FROM graph_neighbors('{GRAPH_REF}',
                     'Collection quarantined; jericans to segregated disposal', 'both')"#
            ),
            outsider(),
        )
        .await
        .unwrap();
    assert!(
        !str_col(&n, "kind").iter().flatten().any(|k| k == "process"),
        "a wall leaked into neighbors"
    );

    // 5. Reachability respects walls. Hide a node KNOWN to be in the owner's
    //    reachable set and assert the outsider's set (a) excludes it and
    //    (b) is a strict subset of the owner's — nothing new ever becomes
    //    reachable through governance, and the hidden node itself vanishes.
    //    (Through-routing is exhaustively unit-proven in graph_traverse's
    //    wall suite; this is the live-bundle end-to-end check.)
    let anchor = "Collection quarantined; jericans to segregated disposal";
    let reach_sql =
        |a: &str| format!(r#"SELECT name FROM graph_reachable('{GRAPH_REF}', '{a}', 'both', 6)"#);
    let r_own = e.query(&reach_sql(anchor), owner()).await.unwrap();
    let own_names: Vec<String> = str_col(&r_own, "name").into_iter().flatten().collect();
    let hidden = own_names.first().expect("owner reaches something").clone();

    let e2 = engine(Some(&format!("name != '{hidden}'")), None);
    let r_out = e2.query(&reach_sql(anchor), outsider()).await.unwrap();
    let out_names: Vec<String> = str_col(&r_out, "name").into_iter().flatten().collect();

    assert!(
        !out_names.contains(&hidden),
        "hidden node '{hidden}' leaked into the outsider's reachable set"
    );
    assert!(
        out_names.len() < own_names.len(),
        "walls must shrink the reachable set (outsider {} vs owner {})",
        out_names.len(),
        own_names.len()
    );
    assert!(
        out_names.iter().all(|n| own_names.contains(n)),
        "governance must never ADD reachability"
    );
}

#[tokio::test]
async fn edge_filter_hides_relationships_between_visible_nodes() {
    // Hide the hands_off_to relationship for outsiders; nodes stay visible.
    let e = engine(None, Some("edge_type != 'hands_off_to'"));

    let all_edges_owner = e
        .query(
            &format!(r#"SELECT edge_type FROM graph_edges('{GRAPH_REF}')"#),
            owner(),
        )
        .await
        .unwrap();
    let owner_types = str_col(&all_edges_owner, "edge_type");
    assert!(
        owner_types.iter().flatten().any(|t| t == "hands_off_to"),
        "fixture has hands_off_to edges"
    );

    let all_edges_out = e
        .query(
            &format!(r#"SELECT edge_type FROM graph_edges('{GRAPH_REF}')"#),
            outsider(),
        )
        .await
        .unwrap();
    assert!(
        !str_col(&all_edges_out, "edge_type")
            .iter()
            .flatten()
            .any(|t| t == "hands_off_to"),
        "edge_filter must hide the relationship"
    );
    // Node set is untouched by an edge-only filter.
    let n = e
        .query(
            &format!(r#"SELECT count(*) AS n FROM graph_nodes('{GRAPH_REF}')"#),
            outsider(),
        )
        .await
        .unwrap();
    let count = n[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 271);
}

// ─── T4/T10: SQL composability + relational aggregation ───────────────────────

#[tokio::test]
async fn composes_with_sql_aggregation_and_ctes() {
    let e = engine(None, None);
    let out = e
        .query(
            &format!(
                r#"WITH manual AS (
                     SELECT name, owner, kind FROM graph_nodes('{GRAPH_REF}')
                     WHERE kind IN ('step', 'decision')
                   )
                   SELECT owner, count(*) AS steps
                   FROM manual GROUP BY owner ORDER BY steps DESC LIMIT 5"#
            ),
            owner(),
        )
        .await
        .unwrap();
    assert!(rows(&out) > 0 && rows(&out) <= 5);
}

// ─── T5: session cache — loads once; policies never shared ────────────────────

#[tokio::test]
async fn cache_loads_once_and_isolates_policies() {
    let e = engine(Some("kind != 'process'"), None);

    e.query(
        &format!(r#"SELECT count(*) FROM graph_nodes('{GRAPH_REF}')"#),
        owner(),
    )
    .await
    .unwrap();
    assert_eq!(e.graph_cache().raw_len(), 1);
    assert_eq!(e.graph_cache().governed_len(), 1);

    // Same caller again: no new entries (bundle not re-read).
    e.query(
        &format!(r#"SELECT count(*) FROM graph_nodes('{GRAPH_REF}')"#),
        owner(),
    )
    .await
    .unwrap();
    assert_eq!(e.graph_cache().raw_len(), 1);
    assert_eq!(e.graph_cache().governed_len(), 1);

    // Different policy (outsider gets walls): raw shared, governed separate.
    e.query(
        &format!(r#"SELECT count(*) FROM graph_nodes('{GRAPH_REF}')"#),
        outsider(),
    )
    .await
    .unwrap();
    assert_eq!(e.graph_cache().raw_len(), 1, "raw snapshot shared");
    assert_eq!(
        e.graph_cache().governed_len(),
        2,
        "different policies never share a governed structure"
    );
}
