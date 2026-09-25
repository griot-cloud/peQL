//! The audit log (peQL design 4.11): one record per query, refused or answered.

use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Result;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AuditRecord {
    pub id: Uuid,
    pub at: DateTime<Utc>,
    pub caller_id: String,
    pub tenant: String,
    pub purpose: String,
    /// sha256 of the SQL text. The text itself is not kept: it may contain values.
    pub sql_sha256: String,
    /// `name@version#compilation_hash` for every contract the query named.
    pub contracts: Vec<String>,
    pub outcome: Outcome,
    pub rows: usize,
    pub elapsed_ms: u64,
    /// Epsilon charged per budget.
    pub charges: Vec<(String, f64)>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "reason")]
pub enum Outcome {
    Answered,
    Refused(String),
    Failed(String),
}

pub trait AuditLog: Send + Sync {
    fn record(&self, r: &AuditRecord) -> Result<()>;
}

/// Keeps records in memory; for tests and embedding.
#[derive(Default, Debug)]
pub struct MemoryAudit {
    pub records: Mutex<Vec<AuditRecord>>,
}

impl AuditLog for MemoryAudit {
    fn record(&self, r: &AuditRecord) -> Result<()> {
        self.records.lock().expect("audit lock").push(r.clone());
        Ok(())
    }
}

/// Appends one JSON line per record.
#[derive(Debug)]
pub struct JsonlAudit {
    path: PathBuf,
    lock: Mutex<()>,
}

impl JsonlAudit {
    pub fn new(path: impl Into<PathBuf>) -> JsonlAudit {
        JsonlAudit {
            path: path.into(),
            lock: Mutex::new(()),
        }
    }
}

impl AuditLog for JsonlAudit {
    fn record(&self, r: &AuditRecord) -> Result<()> {
        let _g = self.lock.lock().expect("audit lock");
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let line = serde_json::to_string(r).map_err(std::io::Error::other)?;
        writeln!(f, "{line}")?;
        Ok(())
    }
}
