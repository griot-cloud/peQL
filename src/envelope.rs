//! What a caller learns besides the rows (peQL design 4.10): which contracts governed the
//! query and how, what the scan read, and hashes that tie the answer to the question.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricValue;
use serde::Serialize;
use uuid::Uuid;

use crate::gate::{GateExec, SCAN_BYTES, ScanExec};

/// What the resolver decided for one contract and one caller (peQL design 4.3).
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Resolution {
    pub contract: String,
    pub version: u32,
    pub contract_hash: String,
    pub compilation_hash: String,
    /// Every decide rule that ran and allowed the caller.
    pub decisions: Vec<String>,
    /// Guarantees that failed with `on_fail: annotate`.
    pub annotations: Vec<String>,
    /// Shape rules that apply to this caller.
    pub shapes: Vec<String>,
    /// Whether flags and derived columns were read from storage (true) or evaluated live.
    pub flags_materialised: bool,
}

/// What the scans of a query read.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct ScanStats {
    /// Rows the scans produced, after any predicate pushed into them.
    pub rows_scanned: usize,
    /// Rows that left the contracts' views.
    pub rows_released: usize,
    /// Arrow memory of the scanned batches.
    pub scanned_bytes: usize,
    /// Bytes read from Parquet files.
    pub bytes_read: usize,
    pub files_pruned: usize,
    pub row_groups_pruned: usize,
}

impl ScanStats {
    pub fn from_plan(plan: &dyn ExecutionPlan) -> ScanStats {
        let mut s = ScanStats::default();
        walk(plan, &mut s);
        s
    }
}

fn walk(p: &dyn ExecutionPlan, s: &mut ScanStats) {
    if let Some(m) = p.metrics() {
        if p.is::<ScanExec>() {
            s.rows_scanned += m.output_rows().unwrap_or(0);
            s.scanned_bytes += m.sum_by_name(SCAN_BYTES).map(|v| v.as_usize()).unwrap_or(0);
        } else if p.is::<GateExec>() {
            s.rows_released += m.output_rows().unwrap_or(0);
        } else {
            for metric in m.iter() {
                match metric.value() {
                    MetricValue::Count { name, count } if name == "bytes_scanned" => {
                        s.bytes_read += count.value();
                    }
                    MetricValue::PruningMetrics {
                        name,
                        pruning_metrics,
                    } => match name.as_ref() {
                        "files_ranges_pruned_statistics" => {
                            s.files_pruned += pruning_metrics.pruned()
                        }
                        "row_groups_pruned_statistics" | "row_groups_pruned_bloom_filter" => {
                            s.row_groups_pruned += pruning_metrics.pruned()
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
        }
    }
    for c in p.children() {
        walk(c.as_ref(), s);
    }
}

/// Ties an answer to its question: what an attestation signs.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Attestation {
    pub query_sha256: String,
    /// sha256 of the result as an Arrow IPC stream.
    pub result_sha256: String,
    pub at: DateTime<Utc>,
}

impl Attestation {
    pub fn of(sql: &str, batches: &[RecordBatch]) -> Attestation {
        Attestation {
            query_sha256: parcel_core::hash::sha256_hex(sql.as_bytes()),
            result_sha256: parcel_core::hash::sha256_hex(&ipc_bytes(batches)),
            at: Utc::now(),
        }
    }
}

/// The batches as one Arrow IPC stream (empty when there are none).
pub fn ipc_bytes(batches: &[RecordBatch]) -> Vec<u8> {
    let Some(first) = batches.first() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut w = datafusion::arrow::ipc::writer::StreamWriter::try_new(&mut out, &first.schema())
        .expect("an in-memory IPC writer");
    for b in batches {
        w.write(b).expect("batches of one schema");
    }
    w.finish().expect("finish IPC stream");
    drop(w);
    out
}

/// Who asked, as the engine was told.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Asker {
    pub id: String,
    pub tenant: String,
    pub purpose: String,
}

impl Asker {
    pub fn of(caller: &parcel_runtime::Caller) -> Asker {
        Asker {
            id: caller.id.clone(),
            tenant: caller.tenant.clone(),
            purpose: caller.purpose.clone(),
        }
    }
}

/// Everything a caller learns about a query besides the rows.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Envelope {
    pub caller: Asker,
    pub contracts: Vec<Resolution>,
    pub rows: usize,
    /// Groups smaller than this were removed.
    pub suppress_k: Option<u64>,
    /// Epsilon this query charged, per budget.
    pub charges: BTreeMap<String, f64>,
    /// Privacy budget left per budget after this query.
    pub budgets: BTreeMap<String, f64>,
    pub scan: ScanStats,
    pub attestation: Attestation,
    /// The audit record written for this query.
    pub audit_id: Uuid,
    /// Served from the result cache.
    pub cached: bool,
}

/// Signs a query's envelope, returning a compact JWS: the provenance certificate for the
/// answer. The engine holds no key; whoever does implements this (see
/// [`crate::signer::SocketSigner`] for one reached over a socket). The envelope names the
/// caller as the engine was told it; a signer that authenticated the caller itself should bind
/// its own knowledge, not the envelope's claim.
#[async_trait]
pub trait EnvelopeSigner: Send + Sync {
    async fn sign(&self, envelope: &Envelope) -> std::result::Result<String, String>;
}
