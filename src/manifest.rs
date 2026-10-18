//! The dataset manifest (parcel design 10, stage 6): what one contract knows about the data
//! bound to it. A write combines it with the facts of the rows it wrote ([`crate::facts`]),
//! a validation of the whole data computes it afresh; the resolver reads it for `guarantee`
//! rules and to decide whether stored flags can replace live rules.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use parcel_runtime::compiled::ColumnDef;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Manifests live here under the binding root, one per contract bound to the data.
pub const MANIFEST_DIR: &str = "_peql/manifests";

/// The format of every facts file peQL records (manifests, a snapshot's facts, a write's
/// facts), in each file's `format` key. A file of another format, or with none, was recorded
/// by another peQL and is treated as not recorded: the facts are computed again. A file of
/// this format that does not decode is corrupt and refused.
pub const FACTS_FORMAT: u64 = 2;

#[derive(Serialize)]
struct Stored<'a, T: Serialize> {
    format: u64,
    #[serde(flatten)]
    facts: &'a T,
}

/// Facts as recorded: their JSON with the [`FACTS_FORMAT`] they are in.
pub fn encode<T: Serialize>(facts: &T) -> std::result::Result<Vec<u8>, String> {
    serde_json::to_vec_pretty(&Stored {
        format: FACTS_FORMAT,
        facts,
    })
    .map_err(|e| e.to_string())
}

/// Recorded facts, or `None` for facts of another format; an error for bytes that are not a
/// JSON object, or facts of this format that do not decode.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> std::result::Result<Option<T>, String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    let Some(object) = value.as_object() else {
        return Err("not a JSON object".to_owned());
    };
    if object.get("format").and_then(serde_json::Value::as_u64) != Some(FACTS_FORMAT) {
        return Ok(None);
    }
    serde_json::from_value(value)
        .map(Some)
        .map_err(|e| e.to_string())
}

/// The last decision of one guarantee over the whole data: made only by validating it whole
/// ([`crate::Engine::validate`]), and carried forward by every append and rewrite until the
/// next validation. An overwrite replaces the data the decision was about, so it drops it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GuaranteeCheck {
    pub holds: bool,
    /// The snapshot it was decided on, for data in a table with snapshots (Iceberg).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    pub decided_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlagStatus {
    AllPass,
    AllFail,
    Mixed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FileEntry {
    /// Path relative to the binding root.
    pub path: String,
    pub rows: i64,
    pub bytes: u64,
    /// The contract hash the file was written under.
    pub contract_hash: String,
    /// Per assert flag column: whether every row passes, fails, or both.
    pub flags: BTreeMap<String, FlagStatus>,
    /// sha256 of the file's bytes, recorded when the file is written or first validated, so
    /// the data hash of a table is computed from its files' entries, never by reading them.
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub contract: String,
    pub contract_hash: String,
    pub compilation_hash: String,
    pub written_at: DateTime<Utc>,
    pub row_count: i64,
    /// Failing rows per assert.
    pub failures: BTreeMap<String, i64>,
    /// Whether the data satisfies every deny-level rule: no deny-level assert fails a row, and
    /// no deny-level guarantee failed when last decided.
    pub valid: bool,
    pub breached: Vec<String>,
    /// Each guarantee decided by the data alone, as last decided by validating the whole data;
    /// a guarantee not decided since the data was first written is absent.
    pub guarantees: BTreeMap<String, GuaranteeCheck>,
    /// Every statistic known for the data, keyed by its column name. One that cannot be
    /// combined from a write's (a distinct count) is absent once a write appended rows, until
    /// the whole data is validated again. No rule is judged over them by a write.
    pub stats: BTreeMap<String, serde_json::Value>,
    /// Hash over the bytes of every file, in path order: what a certificate signs. Computed
    /// from the files' entries ([`data_hash`]).
    pub data_hash: String,
    /// The row schema the contract was compiled against, so a restarted engine can recompile.
    pub row_schema: Vec<ColumnDef>,
    pub files: Vec<FileEntry>,
    /// The table snapshot this describes, for data in a table with snapshots (Iceberg).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
}

impl Manifest {
    fn path(root: &Path, contract: &str) -> PathBuf {
        root.join(MANIFEST_DIR)
            .join(format!("{}.json", contract.replace('/', "__")))
    }

    /// The manifest one contract keeps for the data it binds.
    pub fn load(root: &Path, contract: &str) -> std::io::Result<Option<Manifest>> {
        let p = Self::path(root, contract);
        if !p.exists() {
            return Ok(None);
        }
        decode(&std::fs::read(&p)?)
            .map_err(|e| std::io::Error::other(format!("{}: {e}", p.display())))
    }

    /// Remove the manifest one contract keeps, when it no longer describes the data.
    pub fn remove(root: &Path, contract: &str) -> std::io::Result<()> {
        match std::fs::remove_file(Self::path(root, contract)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    /// Written to a temporary file and renamed, so a reader never sees half a manifest.
    pub fn save(&self, root: &Path) -> std::io::Result<()> {
        let p = Self::path(root, &self.contract);
        std::fs::create_dir_all(p.parent().expect("manifest dir"))?;
        crate::atomic::write(&p, encode(self).map_err(std::io::Error::other)?)
    }

    /// True when every file was written under this contract hash, so stored flags and
    /// derived columns can replace live rules.
    pub fn flags_current(&self, contract_hash: &str) -> bool {
        !self.files.is_empty() && self.files.iter().all(|f| f.contract_hash == contract_hash)
    }
}

/// sha256 over (path, sha256 of bytes) for every file, in path order: the data hash of the
/// files the entries describe.
pub fn data_hash(files: &[FileEntry]) -> String {
    let mut sorted: Vec<&FileEntry> = files.iter().collect();
    sorted.sort_by(|a, b| a.path.cmp(&b.path));
    let mut acc = String::new();
    for f in sorted {
        acc.push_str(&f.path);
        acc.push(':');
        acc.push_str(&f.sha256);
        acc.push('\n');
    }
    parcel_core::hash::sha256_hex(acc.as_bytes())
}
