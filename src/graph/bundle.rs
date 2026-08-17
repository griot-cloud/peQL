//! Bundle loading + verification (G02 R6, `docs/GRAPH-QUERY.md` §2, phase P1).
//!
//! [`load_bundle`] turns a snapshot bundle directory (`nodes.parquet` +
//! `edges.parquet` + `edges_rev.parquet` + `manifest.json`) into a verified,
//! canonicalized [`GraphData`]. Verification is fail-loud: a digest mismatch,
//! an unsupported `bundle_format`, or any structural-invariant violation
//! (I1–I5) rejects the whole bundle — the engine never traverses a corrupt
//! snapshot. Certificate verification (`certificate.gdcpc.signed`) is the
//! platform path's concern and happens outside this loader.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{new_null_array, Array, Int32Array, StringArray};
use datafusion::arrow::compute::{cast, concat_batches};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};

use super::types::{GraphData, GraphError, GraphManifest, MAX_EDGES_V1, MAX_NODES_V1};

/// The manifest file name.
const MANIFEST_FILE: &str = "manifest.json";
/// The node attribute + CSR file name.
const NODES_FILE: &str = "nodes.parquet";
/// The forward (src-sorted) edge file name.
const EDGES_FILE: &str = "edges.parquet";
/// The reverse (dst-sorted) edge file name.
const EDGES_REV_FILE: &str = "edges_rev.parquet";
/// The only `bundle_format` this engine supports.
const SUPPORTED_FORMAT: u32 = 1;

/// Load, verify (`bundle_format` + per-file sha256 digests, G02 R6),
/// canonicalize and decode a snapshot bundle directory into a [`GraphData`].
///
/// Pipeline: parse `manifest.json` → check `bundle_format` → for each Parquet
/// file verify its digest against the manifest **before** parsing the same
/// bytes → check the v1 size envelope → canonicalize the Arrow schemas
/// (dictionary columns → plain values, all-null `Null` columns → nullable
/// `Utf8`) → decode the CSR traversal vectors and lookup maps → verify the
/// structural invariants (dense positions, slice coverage, in-bounds refs).
/// Any failure is a named [`GraphError`]; a partially-loaded bundle is never
/// returned.
pub fn load_bundle(dir: &Path) -> Result<GraphData, GraphError> {
    let bundle = dir.display().to_string();

    // ── 1. Manifest ──
    let manifest_bytes =
        std::fs::read(dir.join(MANIFEST_FILE)).map_err(|e| GraphError::FileUnreadable {
            bundle: bundle.clone(),
            file: MANIFEST_FILE.to_string(),
            reason: e.to_string(),
        })?;
    let manifest: GraphManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|e| GraphError::FileUnreadable {
            bundle: bundle.clone(),
            file: MANIFEST_FILE.to_string(),
            reason: format!("invalid manifest JSON: {e}"),
        })?;

    // ── 2. Format ──
    if manifest.bundle_format != SUPPORTED_FORMAT {
        return Err(GraphError::UnsupportedFormat {
            bundle,
            found: manifest.bundle_format,
            supported: SUPPORTED_FORMAT,
        });
    }

    // ── 3. Digest-verify + parse the three Parquet files ──
    let nodes_raw = read_verified_parquet(dir, &bundle, &manifest, NODES_FILE)?;
    let edges_raw = read_verified_parquet(dir, &bundle, &manifest, EDGES_FILE)?;
    let edges_rev_raw = read_verified_parquet(dir, &bundle, &manifest, EDGES_REV_FILE)?;

    // ── 4. Size envelope (G02 §10) ──
    let node_count = nodes_raw.num_rows();
    let edge_count = edges_raw.num_rows();
    if node_count > MAX_NODES_V1 || edge_count > MAX_EDGES_V1 {
        return Err(GraphError::EnvelopeExceeded {
            bundle,
            nodes: node_count,
            edges: edge_count,
            max_nodes: MAX_NODES_V1,
            max_edges: MAX_EDGES_V1,
        });
    }

    // ── 5. Canonicalize (dictionary → plain, Null → nullable Utf8) ──
    let nodes = canonicalize(&nodes_raw, NODES_FILE, &bundle)?;
    let edges = canonicalize(&edges_raw, EDGES_FILE, &bundle)?;
    let edges_rev = canonicalize(&edges_rev_raw, EDGES_REV_FILE, &bundle)?;

    // ── 6/7. Decode + verify. All invariant failures name the bundle. ──
    let violation = |detail: String| GraphError::InvariantViolation {
        bundle: bundle.clone(),
        detail,
    };

    // Counts must match the manifest, and the reverse file mirrors the forward.
    if node_count as u64 != manifest.counts.nodes {
        return Err(violation(format!(
            "{NODES_FILE} has {node_count} rows but manifest.counts.nodes = {}",
            manifest.counts.nodes
        )));
    }
    if edge_count as u64 != manifest.counts.edges {
        return Err(violation(format!(
            "{EDGES_FILE} has {edge_count} rows but manifest.counts.edges = {}",
            manifest.counts.edges
        )));
    }
    if edges_rev.num_rows() != edge_count {
        return Err(violation(format!(
            "{EDGES_REV_FILE} has {} rows but {EDGES_FILE} has {edge_count}",
            edges_rev.num_rows()
        )));
    }

    // I1: dense 0..N positions, in physical row order.
    check_dense(&nodes, "pos", NODES_FILE, &bundle)?;
    check_dense(&edges, "row", EDGES_FILE, &bundle)?;
    check_dense(&edges_rev, "row", EDGES_REV_FILE, &bundle)?;

    // Node columns.
    let out_start = required_i32_vec(&nodes, "out_start", NODES_FILE, &bundle)?;
    let out_count = required_i32_vec(&nodes, "out_count", NODES_FILE, &bundle)?;
    let in_start = required_i32_vec(&nodes, "in_start", NODES_FILE, &bundle)?;
    let in_count = required_i32_vec(&nodes, "in_count", NODES_FILE, &bundle)?;
    let parent_pos = optional_i32_vec(&nodes, "parent_pos", NODES_FILE, &bundle)?;
    let call_ref_pos = optional_i32_vec(&nodes, "call_ref_pos", NODES_FILE, &bundle)?;
    let names = string_vec(&nodes, "name", NODES_FILE, &bundle)?;
    let node_ids = string_vec(&nodes, "node_id", NODES_FILE, &bundle)?;

    // Edge-type id space = the manifest's inventory order.
    let edge_type_names = manifest.edge_types.clone();
    let type_ids: HashMap<&str, u16> = edge_type_names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i as u16))
        .collect();

    let fwd = decode_edges(&edges, EDGES_FILE, &bundle, &type_ids)?;
    let rev = decode_edges(&edges_rev, EDGES_REV_FILE, &bundle, &type_ids)?;

    // I4: every node reference in-bounds.
    check_pos_bounds(&fwd.src, node_count, "src_pos", EDGES_FILE, &bundle)?;
    check_pos_bounds(&fwd.dst, node_count, "dst_pos", EDGES_FILE, &bundle)?;
    check_pos_bounds(&rev.src, node_count, "src_pos", EDGES_REV_FILE, &bundle)?;
    check_pos_bounds(&rev.dst, node_count, "dst_pos", EDGES_REV_FILE, &bundle)?;
    for (pos, parent) in parent_pos.iter().enumerate() {
        if let Some(p) = parent {
            if *p < 0 || *p as usize >= node_count {
                return Err(violation(format!(
                    "{NODES_FILE}: parent_pos[{pos}] = {p} out of bounds (node count {node_count})"
                )));
            }
        }
    }
    for (pos, call) in call_ref_pos.iter().enumerate() {
        if let Some(c) = call {
            if *c < 0 || *c as usize >= node_count {
                return Err(violation(format!(
                    "{NODES_FILE}: call_ref_pos[{pos}] = {c} out of bounds (node count {node_count})"
                )));
            }
        }
    }

    // I3: the CSR slices exactly cover each edge file, and every row in a
    // node's slice names that node as its anchor endpoint.
    let out_sum: i64 = out_count.iter().map(|&c| c as i64).sum();
    if out_sum != edge_count as i64 {
        return Err(violation(format!(
            "sum(out_count) = {out_sum} but {EDGES_FILE} has {edge_count} rows"
        )));
    }
    let in_sum: i64 = in_count.iter().map(|&c| c as i64).sum();
    if in_sum != edge_count as i64 {
        return Err(violation(format!(
            "sum(in_count) = {in_sum} but {EDGES_REV_FILE} has {edge_count} rows"
        )));
    }
    check_slices(&out_start, &out_count, &fwd.src, "out", EDGES_FILE, &bundle)?;
    check_slices(
        &in_start,
        &in_count,
        &rev.dst,
        "in",
        EDGES_REV_FILE,
        &bundle,
    )?;

    // Children lists from parent_pos: iterating pos ascending keeps each list
    // ascending (I1 gives physical order = pos order).
    let mut children: Vec<Vec<i32>> = vec![Vec::new(); node_count];
    for (pos, parent) in parent_pos.iter().enumerate() {
        if let Some(p) = parent {
            children[*p as usize].push(pos as i32);
        }
    }

    // Lookup maps. First occurrence wins so lookups are deterministic (the
    // compiler deduplicates names; this is belt-and-braces, not a gate).
    let mut name_to_pos = HashMap::with_capacity(node_count);
    let mut name_ci_to_pos = HashMap::with_capacity(node_count);
    let mut id_to_pos = HashMap::with_capacity(node_count);
    for (pos, name) in names.iter().enumerate() {
        name_to_pos.entry(name.clone()).or_insert(pos as i32);
        name_ci_to_pos
            .entry(name.trim().to_lowercase())
            .or_insert(pos as i32);
    }
    for (pos, id) in node_ids.iter().enumerate() {
        id_to_pos.entry(id.clone()).or_insert(pos as i32);
    }

    Ok(GraphData {
        manifest,
        nodes,
        edges,
        edges_rev,
        out_start,
        out_count,
        in_start,
        in_count,
        parent_pos,
        call_ref_pos,
        children,
        names,
        fwd_dst: fwd.dst,
        fwd_src: fwd.src,
        fwd_type: fwd.ty,
        rev_src: rev.src,
        rev_dst: rev.dst,
        rev_type: rev.ty,
        edge_type_names,
        name_to_pos,
        name_ci_to_pos,
        id_to_pos,
    })
}

/// Read `file`'s raw bytes, verify its sha256 against the manifest (G02 R6),
/// then parse **the same verified bytes** as Parquet into one concatenated
/// [`RecordBatch`].
fn read_verified_parquet(
    dir: &Path,
    bundle: &str,
    manifest: &GraphManifest,
    file: &str,
) -> Result<RecordBatch, GraphError> {
    let raw = std::fs::read(dir.join(file)).map_err(|e| GraphError::FileUnreadable {
        bundle: bundle.to_string(),
        file: file.to_string(),
        reason: e.to_string(),
    })?;

    let expected = manifest
        .files
        .get(file)
        .ok_or_else(|| GraphError::InvariantViolation {
            bundle: bundle.to_string(),
            detail: format!("manifest.files has no digest entry for {file}"),
        })?;
    let actual = hex::encode(Sha256::digest(&raw));
    if !actual.eq_ignore_ascii_case(&expected.sha256) {
        return Err(GraphError::DigestMismatch {
            bundle: bundle.to_string(),
            file: file.to_string(),
            expected: expected.sha256.clone(),
            actual,
        });
    }

    let unreadable = |reason: String| GraphError::FileUnreadable {
        bundle: bundle.to_string(),
        file: file.to_string(),
        reason,
    };

    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(raw))
        .map_err(|e| unreadable(format!("parquet open: {e}")))?;
    let schema = builder.schema().clone();
    let reader = builder
        .build()
        .map_err(|e| unreadable(format!("parquet read: {e}")))?;
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| unreadable(format!("parquet decode: {e}")))?;
    concat_batches(&schema, &batches).map_err(|e| unreadable(format!("batch concat: {e}")))
}

/// Canonicalize a batch's schema so output schemas are stable across bundles:
/// every dictionary-encoded column is cast to its plain value type, and every
/// compiler-emitted all-null `Null` column (e.g. `system_ref`) becomes an
/// all-null **nullable `Utf8`** column of the same length.
fn canonicalize(batch: &RecordBatch, file: &str, bundle: &str) -> Result<RecordBatch, GraphError> {
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns = Vec::with_capacity(batch.num_columns());

    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        match field.data_type() {
            DataType::Dictionary(_, value_type) => {
                let plain = cast(column, value_type).map_err(|e| GraphError::Internal(format!(
                    "graph bundle '{bundle}': {file}: cannot cast dictionary column '{}' to {value_type}: {e}",
                    field.name()
                )))?;
                fields.push(Field::new(
                    field.name(),
                    value_type.as_ref().clone(),
                    field.is_nullable(),
                ));
                columns.push(plain);
            }
            DataType::Null => {
                fields.push(Field::new(field.name(), DataType::Utf8, true));
                columns.push(new_null_array(&DataType::Utf8, column.len()));
            }
            _ => {
                fields.push(field.as_ref().clone());
                columns.push(column.clone());
            }
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| {
        GraphError::Internal(format!(
            "graph bundle '{bundle}': {file}: canonicalized batch rebuild failed: {e}"
        ))
    })
}

/// The decoded per-row vectors of one edge file.
struct EdgeVectors {
    /// Per row: source node pos.
    src: Vec<i32>,
    /// Per row: destination node pos.
    dst: Vec<i32>,
    /// Per row: edge-type id (index into the manifest's `edge_types`).
    ty: Vec<u16>,
}

/// Decode one (canonicalized) edge batch into its traversal vectors. An
/// `edge_type` value absent from the manifest inventory is an invariant
/// violation — the manifest is the source of truth for the id space.
fn decode_edges(
    batch: &RecordBatch,
    file: &str,
    bundle: &str,
    type_ids: &HashMap<&str, u16>,
) -> Result<EdgeVectors, GraphError> {
    let src = required_i32_vec(batch, "src_pos", file, bundle)?;
    let dst = required_i32_vec(batch, "dst_pos", file, bundle)?;
    let type_col = str_col(batch, "edge_type", file, bundle)?;

    let mut ty = Vec::with_capacity(type_col.len());
    for row in 0..type_col.len() {
        if type_col.is_null(row) {
            return Err(GraphError::InvariantViolation {
                bundle: bundle.to_string(),
                detail: format!("{file}: edge_type is null at row {row}"),
            });
        }
        let name = type_col.value(row);
        let id = type_ids
            .get(name)
            .ok_or_else(|| GraphError::InvariantViolation {
                bundle: bundle.to_string(),
                detail: format!(
                    "{file}: edge_type '{name}' at row {row} is not in the manifest's edge_types inventory"
                ),
            })?;
        ty.push(*id);
    }

    Ok(EdgeVectors { src, dst, ty })
}

/// Fetch a named `Int32` column, or fail with a named invariant violation.
fn i32_col<'a>(
    batch: &'a RecordBatch,
    name: &str,
    file: &str,
    bundle: &str,
) -> Result<&'a Int32Array, GraphError> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .ok_or_else(|| GraphError::InvariantViolation {
            bundle: bundle.to_string(),
            detail: format!("{file}: column '{name}' is missing or not int32"),
        })
}

/// Fetch a named `Utf8` column, or fail with a named invariant violation.
fn str_col<'a>(
    batch: &'a RecordBatch,
    name: &str,
    file: &str,
    bundle: &str,
) -> Result<&'a StringArray, GraphError> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| GraphError::InvariantViolation {
            bundle: bundle.to_string(),
            detail: format!("{file}: column '{name}' is missing or not utf8"),
        })
}

/// Decode a non-nullable `Int32` column to a `Vec<i32>`; a null anywhere is an
/// invariant violation.
fn required_i32_vec(
    batch: &RecordBatch,
    name: &str,
    file: &str,
    bundle: &str,
) -> Result<Vec<i32>, GraphError> {
    let col = i32_col(batch, name, file, bundle)?;
    if col.null_count() != 0 {
        return Err(GraphError::InvariantViolation {
            bundle: bundle.to_string(),
            detail: format!("{file}: column '{name}' has nulls but must be non-null"),
        });
    }
    Ok(col.values().to_vec())
}

/// Decode a nullable `Int32` column to a `Vec<Option<i32>>`.
fn optional_i32_vec(
    batch: &RecordBatch,
    name: &str,
    file: &str,
    bundle: &str,
) -> Result<Vec<Option<i32>>, GraphError> {
    Ok(i32_col(batch, name, file, bundle)?.iter().collect())
}

/// Decode a non-nullable `Utf8` column to a `Vec<String>`.
fn string_vec(
    batch: &RecordBatch,
    name: &str,
    file: &str,
    bundle: &str,
) -> Result<Vec<String>, GraphError> {
    let col = str_col(batch, name, file, bundle)?;
    let mut out = Vec::with_capacity(col.len());
    for row in 0..col.len() {
        if col.is_null(row) {
            return Err(GraphError::InvariantViolation {
                bundle: bundle.to_string(),
                detail: format!("{file}: column '{name}' is null at row {row}"),
            });
        }
        out.push(col.value(row).to_string());
    }
    Ok(out)
}

/// I1: `name` must be the dense sequence `0..N` in physical row order.
fn check_dense(
    batch: &RecordBatch,
    name: &str,
    file: &str,
    bundle: &str,
) -> Result<(), GraphError> {
    let vals = required_i32_vec(batch, name, file, bundle)?;
    for (i, v) in vals.iter().enumerate() {
        if *v != i as i32 {
            return Err(GraphError::InvariantViolation {
                bundle: bundle.to_string(),
                detail: format!("{file}: {name} at row {i} is {v}, expected dense 0..N"),
            });
        }
    }
    Ok(())
}

/// I4: every value in `positions` is a valid node pos in `[0, node_count)`.
fn check_pos_bounds(
    positions: &[i32],
    node_count: usize,
    name: &str,
    file: &str,
    bundle: &str,
) -> Result<(), GraphError> {
    for (row, p) in positions.iter().enumerate() {
        if *p < 0 || *p as usize >= node_count {
            return Err(GraphError::InvariantViolation {
                bundle: bundle.to_string(),
                detail: format!(
                    "{file}: {name}[{row}] = {p} out of bounds (node count {node_count})"
                ),
            });
        }
    }
    Ok(())
}

/// I3: each node's `[start, start+count)` slice is in-bounds, and every row in
/// the slice names that node as its anchor endpoint (`src_pos` in the forward
/// file, `dst_pos` in the reverse file).
fn check_slices(
    starts: &[i32],
    counts: &[i32],
    anchors: &[i32],
    which: &str,
    file: &str,
    bundle: &str,
) -> Result<(), GraphError> {
    let edge_count = anchors.len() as i64;
    for (pos, (&start, &count)) in starts.iter().zip(counts.iter()).enumerate() {
        if start < 0 || count < 0 || start as i64 + count as i64 > edge_count {
            return Err(GraphError::InvariantViolation {
                bundle: bundle.to_string(),
                detail: format!(
                    "{file}: node {pos} {which} slice [{start}, {start}+{count}) exceeds {edge_count} rows"
                ),
            });
        }
        for r in start..start + count {
            let anchor = anchors[r as usize];
            if anchor != pos as i32 {
                return Err(GraphError::InvariantViolation {
                    bundle: bundle.to_string(),
                    detail: format!(
                        "{file}: row {r} in node {pos}'s {which} slice has anchor pos {anchor}"
                    ),
                });
            }
        }
    }
    Ok(())
}
