//! The dataset manifest (parcel design 10, stage 6): what one contract knows about the data
//! bound to it. The write path writes it; the resolver reads it for `guarantee` rules and to
//! decide whether stored flags can replace live rules.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use parcel_runtime::bundle::ColumnDef;
use serde::{Deserialize, Serialize};

/// Manifests live here under the binding root, one per contract bound to the data.
pub const MANIFEST_DIR: &str = "_peql/manifests";

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
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub contract: String,
    pub contract_hash: String,
    pub compilation_hash: String,
    pub written_at: DateTime<Utc>,
    pub row_count: i64,
    /// Whether the data satisfies every deny-level rule: the validation verdict.
    pub valid: bool,
    pub breached: Vec<String>,
    /// Every statistic the validation plan computed, keyed by its column name.
    pub stats: BTreeMap<String, serde_json::Value>,
    /// Hash over the bytes of every file, in path order: what a certificate signs.
    pub data_hash: String,
    /// The row schema the contract was compiled against, so a restarted engine can recompile.
    pub row_schema: Vec<ColumnDef>,
    pub files: Vec<FileEntry>,
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
        serde_json::from_str(&std::fs::read_to_string(p)?)
            .map(Some)
            .map_err(std::io::Error::other)
    }

    /// Written to a temporary file and renamed, so a reader never sees half a manifest.
    pub fn save(&self, root: &Path) -> std::io::Result<()> {
        let p = Self::path(root, &self.contract);
        std::fs::create_dir_all(p.parent().expect("manifest dir"))?;
        let tmp = p.with_extension("json.tmp");
        std::fs::write(
            &tmp,
            serde_json::to_string_pretty(self).map_err(std::io::Error::other)?,
        )?;
        std::fs::rename(tmp, p)
    }

    /// True when every file was written under this contract hash, so stored flags and
    /// derived columns can replace live rules.
    pub fn flags_current(&self, contract_hash: &str) -> bool {
        !self.files.is_empty() && self.files.iter().all(|f| f.contract_hash == contract_hash)
    }
}
