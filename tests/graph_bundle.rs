//! Integration tests for the graph snapshot bundle loader
//! (`peql::graph::bundle::load_bundle`) against the real compiled fixture
//! bundle in `fixtures/graph/zijani-operations-v1/`.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;

use peql::graph::bundle::load_bundle;
use peql::graph::types::{is_ulid_shaped, resolve_node, GraphError, GraphPolicy};
use peql::policy::ResolvedPolicy;

/// The real compiled fixture bundle (271 nodes / 562 edges).
const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/graph/zijani-operations-v1"
);

/// Copy the fixture bundle's files into `dst` so a test can tamper with its
/// own private copy.
fn copy_fixture_to(dst: &Path) {
    for entry in fs::read_dir(FIXTURE).expect("read fixture dir") {
        let entry = entry.expect("fixture dir entry");
        if entry.file_type().expect("file type").is_file() {
            fs::copy(entry.path(), dst.join(entry.file_name())).expect("copy fixture file");
        }
    }
}

/// Assert no dictionary-encoded column survived canonicalization.
fn assert_no_dictionary_columns(batch: &RecordBatch, which: &str) {
    for field in batch.schema().fields() {
        assert!(
            !matches!(field.data_type(), DataType::Dictionary(_, _)),
            "{which}: column '{}' is still dictionary-encoded after canonicalization",
            field.name()
        );
    }
}

#[test]
fn happy_path_loads_and_canonicalizes_the_fixture() {
    let data = load_bundle(Path::new(FIXTURE)).expect("fixture bundle must load");

    // Counts + manifest surface.
    assert_eq!(data.node_count(), 271);
    assert_eq!(data.edge_count(), 562);
    assert_eq!(data.manifest.graph_slug, "zijani-operations");
    assert_eq!(data.manifest.snapshot_version, 1);

    // The manifest's edge_types inventory IS the id space.
    assert_eq!(data.edge_type_names, data.manifest.edge_types);
    let contains_id = data
        .edge_type_id("contains")
        .expect("fixture has 'contains'");
    let authored = data.authored_type_ids();
    assert!(
        !authored.contains(&contains_id),
        "authored excludes derived"
    );
    assert!(!authored.is_empty(), "fixture has authored edge types");

    // Vector lengths line up.
    assert_eq!(data.out_start.len(), 271);
    assert_eq!(data.out_count.len(), 271);
    assert_eq!(data.in_start.len(), 271);
    assert_eq!(data.in_count.len(), 271);
    assert_eq!(data.parent_pos.len(), 271);
    assert_eq!(data.call_ref_pos.len(), 271);
    assert_eq!(data.children.len(), 271);
    assert_eq!(data.names.len(), 271);
    assert_eq!(data.fwd_src.len(), 562);
    assert_eq!(data.fwd_dst.len(), 562);
    assert_eq!(data.fwd_type.len(), 562);
    assert_eq!(data.rev_src.len(), 562);
    assert_eq!(data.rev_dst.len(), 562);
    assert_eq!(data.rev_type.len(), 562);

    // Name + ULID lookup maps resolve real nodes.
    assert_eq!(data.name_to_pos[data.names[0].as_str()], 0);
    let node_ids = data
        .nodes
        .column_by_name("node_id")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .expect("node_id column is utf8");
    assert_eq!(data.id_to_pos[node_ids.value(0)], 0);

    // Canonicalization: the compiler-emitted all-null Null column became a
    // nullable all-null Utf8 column.
    let schema = data.nodes.schema();
    let (idx, system_ref) = schema
        .column_with_name("system_ref")
        .expect("system_ref column exists");
    assert_eq!(system_ref.data_type(), &DataType::Utf8);
    assert!(system_ref.is_nullable());
    let col = data.nodes.column(idx);
    assert_eq!(col.null_count(), col.len(), "system_ref is all-null");

    // No dictionary-typed column remains anywhere.
    assert_no_dictionary_columns(&data.nodes, "nodes");
    assert_no_dictionary_columns(&data.edges, "edges");
    assert_no_dictionary_columns(&data.edges_rev, "edges_rev");
}

#[test]
fn csr_slices_agree_with_brute_force_scan() {
    let data = load_bundle(Path::new(FIXTURE)).expect("fixture bundle must load");
    let n = data.node_count();

    // 25 evenly spaced nodes: the out slice must equal the row-order scan of
    // forward rows anchored at that node (both ascending, so order included).
    let step = n / 25;
    for k in 0..25 {
        let p = (k * step) as i32;
        let start = data.out_start[p as usize] as usize;
        let count = data.out_count[p as usize] as usize;
        let via_slice: Vec<i32> = data.fwd_dst[start..start + count].to_vec();

        let brute: Vec<i32> = (0..data.edge_count())
            .filter(|&r| data.fwd_src[r] == p)
            .map(|r| data.fwd_dst[r])
            .collect();
        assert_eq!(via_slice, brute, "out slice of node {p} disagrees");
    }

    // Mirror one check on the in slice of the first node that has one.
    let p = (0..n as i32)
        .find(|&p| data.in_count[p as usize] > 0)
        .expect("some node has incoming edges");
    let start = data.in_start[p as usize] as usize;
    let count = data.in_count[p as usize] as usize;
    let via_slice: Vec<i32> = data.rev_src[start..start + count].to_vec();
    let brute: Vec<i32> = (0..data.edge_count())
        .filter(|&r| data.rev_dst[r] == p)
        .map(|r| data.rev_src[r])
        .collect();
    assert_eq!(via_slice, brute, "in slice of node {p} disagrees");
}

#[test]
fn tampered_edge_file_is_rejected_by_digest() {
    let dir = tempfile::tempdir().expect("tempdir");
    copy_fixture_to(dir.path());

    // Flip one byte in the middle of edges.parquet.
    let edges_path = dir.path().join("edges.parquet");
    let mut bytes = fs::read(&edges_path).expect("read edges.parquet");
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    fs::write(&edges_path, bytes).expect("write tampered edges.parquet");

    match load_bundle(dir.path()) {
        Err(GraphError::DigestMismatch {
            file,
            expected,
            actual,
            ..
        }) => {
            assert_eq!(file, "edges.parquet");
            assert_ne!(expected, actual);
        }
        other => panic!("expected DigestMismatch for edges.parquet, got {other:?}"),
    }
}

#[test]
fn unsupported_bundle_format_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    copy_fixture_to(dir.path());

    let manifest_path = dir.path().join("manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read manifest"))
            .expect("parse manifest");
    manifest["bundle_format"] = serde_json::json!(99);
    fs::write(
        &manifest_path,
        serde_json::to_vec(&manifest).expect("serialize manifest"),
    )
    .expect("write manifest");

    match load_bundle(dir.path()) {
        Err(GraphError::UnsupportedFormat {
            found, supported, ..
        }) => {
            assert_eq!(found, 99);
            assert_eq!(supported, 1);
        }
        other => panic!("expected UnsupportedFormat, got {other:?}"),
    }
}

#[test]
fn missing_bundle_file_is_rejected_naming_the_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    copy_fixture_to(dir.path());
    fs::remove_file(dir.path().join("edges_rev.parquet")).expect("delete edges_rev.parquet");

    match load_bundle(dir.path()) {
        Err(GraphError::FileUnreadable { file, .. }) => {
            assert_eq!(file, "edges_rev.parquet");
        }
        other => panic!("expected FileUnreadable for edges_rev.parquet, got {other:?}"),
    }
}

#[test]
fn node_resolution_by_name_ulid_and_case_insensitive_name() {
    let data = load_bundle(Path::new(FIXTURE)).expect("fixture bundle must load");
    let policy = GraphPolicy::allow_all(&data, Arc::new(ResolvedPolicy::allow_all("c", "1", "t")));

    // Exact name.
    let name = data.names[0].as_str();
    assert_eq!(
        resolve_node(&data, &policy, name).expect("resolve by name"),
        0
    );

    // ULID.
    let node_ids = data
        .nodes
        .column_by_name("node_id")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .expect("node_id column is utf8");
    let ulid = node_ids.value(0);
    assert!(is_ulid_shaped(ulid), "fixture node_id '{ulid}' is a ULID");
    assert_eq!(
        resolve_node(&data, &policy, ulid).expect("resolve by ULID"),
        0
    );

    // Case-insensitive name: uppercase a real name and it still resolves to a
    // node whose name matches case-insensitively.
    let shouty = name.to_uppercase();
    let pos = resolve_node(&data, &policy, &shouty).expect("resolve case-insensitively");
    assert_eq!(
        data.names[pos as usize].trim().to_lowercase(),
        name.trim().to_lowercase()
    );

    // Unknown key: UnknownNode, with near-miss suggestions for a prefix of a
    // real name.
    let long_name = data
        .names
        .iter()
        .find(|n| n.len() > 10 && n.is_ascii())
        .expect("fixture has a long ascii name");
    let prefix = &long_name[..long_name.len() - 2];
    match resolve_node(&data, &policy, prefix) {
        Ok(pos) => {
            // The prefix may itself be another real node name; that is a valid
            // resolution, not a failure of the suggestion path.
            assert_eq!(
                data.names[pos as usize].trim().to_lowercase(),
                prefix.trim().to_lowercase()
            );
        }
        Err(GraphError::UnknownNode {
            graph,
            key,
            suggestions,
        }) => {
            assert_eq!(graph, "zijani-operations");
            assert_eq!(key, prefix);
            assert!(
                suggestions.iter().any(|s| s == long_name),
                "suggestions {suggestions:?} should include '{long_name}'"
            );
        }
        Err(other) => panic!("expected UnknownNode, got {other:?}"),
    }

    // A key resembling nothing resolves to nothing.
    match resolve_node(&data, &policy, "zz-no-such-node-zz") {
        Err(GraphError::UnknownNode { key, .. }) => assert_eq!(key, "zz-no-such-node-zz"),
        other => panic!("expected UnknownNode, got {other:?}"),
    }
}
