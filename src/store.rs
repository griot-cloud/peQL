//! The contract store (peQL design 4.2): compiled contracts by name and version. A contract
//! is compiled once, by whoever registers it, and served from here; a query never compiles.
//! Persisted as parcel's compiled form, loaded as it was stored and never compiled again. A
//! stored contract is a cache of what was registered: one that no longer loads (another parcel
//! version, an unreadable file) is set aside as [`Stale`], reported and never served, and the
//! store opens with the rest.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use datafusion::arrow::datatypes::Schema;
use parcel_core::{Compilation, ContractDoc};
use parcel_runtime::bundle::{Bundle, BundledFunction};
use parcel_runtime::compiled::CompiledBytes;
use serde::{Deserialize, Serialize};

use crate::error::{PeqlError, Result};

/// Audience for a contract published to every tenant.
pub const PUBLIC: &str = "public";

/// One registered version of a contract: what parcel compiled and what it compiled from.
#[derive(Debug)]
pub struct Registered {
    pub doc: ContractDoc,
    pub schema: Schema,
    pub functions: Vec<BundledFunction>,
    pub compilation: Compilation,
}

impl Registered {
    pub fn name(&self) -> &str {
        &self.compilation.contract.name
    }

    pub fn owner(&self) -> Option<&str> {
        self.compilation.contract.owner.as_deref()
    }

    /// Recompile a bundle and accept it only when it gives the same compilation hash: a bundle
    /// nobody vouches for is trusted for nothing it says.
    pub fn from_bundle(bundle: &Bundle) -> Result<Registered> {
        let compilation = bundle.verify().map_err(PeqlError::Invalid)?;
        Ok(Registered {
            doc: bundle.document.clone(),
            schema: bundle.schema().map_err(PeqlError::Invalid)?,
            functions: bundle.functions.clone(),
            compilation,
        })
    }

    /// A contract as parcel compiled it ([`CompiledBytes`]), taken as given: never compiled
    /// here. Whoever hands it over vouches that `compiled` is what `doc` compiles to.
    pub fn from_compiled(
        doc: ContractDoc,
        compiled: &[u8],
        functions: Vec<BundledFunction>,
    ) -> Result<Registered> {
        let compilation = Compilation::from_bytes(compiled).map_err(PeqlError::Invalid)?;
        if compilation.contract.name != doc.contract {
            return Err(PeqlError::Invalid(format!(
                "the compiled contract is `{}`; its document is `{}`",
                compilation.contract.name, doc.contract
            )));
        }
        Ok(Registered {
            doc,
            schema: compilation.contract.row_schema.as_ref().clone(),
            functions,
            compilation,
        })
    }
}

/// How the directory store keeps one registered version: its document, the functions it is
/// pinned to, and its compiled form exactly as parcel wrote it.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredContract {
    document: ContractDoc,
    functions: Vec<BundledFunction>,
    /// [`CompiledBytes`], which are UTF-8.
    compiled: String,
}

/// Where compiled contracts live. Visibility is the engine's decision, from `owner` and
/// [`ContractStore::audiences`].
pub trait ContractStore: Send + Sync {
    fn put(&self, c: Arc<Registered>) -> Result<()>;
    /// The current (latest registered) version.
    fn current(&self, name: &str) -> Option<Arc<Registered>>;
    fn version(&self, name: &str, version: u32) -> Option<Arc<Registered>>;
    /// The current version of every contract.
    fn list(&self) -> Vec<Arc<Registered>>;
    /// Tenants a contract is published to, besides its owner. [`PUBLIC`] means everyone.
    fn audiences(&self, name: &str) -> BTreeSet<String>;
    fn publish(&self, name: &str, audience: &str) -> Result<()>;
    fn unpublish(&self, name: &str, audience: &str) -> Result<()>;
    /// Stored bundles that could not be loaded, and why. None of them is served.
    fn stale(&self) -> Vec<Stale>;
}

/// A stored contract that did not load: unreadable, or compiled by another version of parcel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stale {
    pub path: PathBuf,
    /// The contract, from the directory the bundle is stored in.
    pub name: String,
    /// The version, from the file's name; `None` when the name carries none.
    pub version: Option<u32>,
    /// Why it did not load.
    pub cause: String,
}

impl std::fmt::Display for Stale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.path.display(), self.cause)
    }
}

#[derive(Default, Debug)]
struct Contents {
    versions: BTreeMap<String, BTreeMap<u32, Arc<Registered>>>,
    audiences: BTreeMap<String, BTreeSet<String>>,
}

/// In memory: for tests and embedding.
#[derive(Default, Debug)]
pub struct MemoryStore {
    inner: RwLock<Contents>,
}

impl ContractStore for MemoryStore {
    fn put(&self, c: Arc<Registered>) -> Result<()> {
        let mut g = self.inner.write().expect("store lock");
        g.versions
            .entry(c.name().to_owned())
            .or_default()
            .insert(c.compilation.contract.version, c);
        Ok(())
    }
    fn current(&self, name: &str) -> Option<Arc<Registered>> {
        let g = self.inner.read().expect("store lock");
        g.versions.get(name)?.values().next_back().cloned()
    }
    fn version(&self, name: &str, version: u32) -> Option<Arc<Registered>> {
        let g = self.inner.read().expect("store lock");
        g.versions.get(name)?.get(&version).cloned()
    }
    fn list(&self) -> Vec<Arc<Registered>> {
        let g = self.inner.read().expect("store lock");
        g.versions
            .values()
            .filter_map(|v| v.values().next_back().cloned())
            .collect()
    }
    fn audiences(&self, name: &str) -> BTreeSet<String> {
        let g = self.inner.read().expect("store lock");
        g.audiences.get(name).cloned().unwrap_or_default()
    }
    fn publish(&self, name: &str, audience: &str) -> Result<()> {
        let mut g = self.inner.write().expect("store lock");
        g.audiences
            .entry(name.to_owned())
            .or_default()
            .insert(audience.to_owned());
        Ok(())
    }
    fn unpublish(&self, name: &str, audience: &str) -> Result<()> {
        let mut g = self.inner.write().expect("store lock");
        if let Some(a) = g.audiences.get_mut(name) {
            a.remove(audience);
        }
        Ok(())
    }
    fn stale(&self) -> Vec<Stale> {
        Vec::new()
    }
}

/// On disk: `<root>/_peql/contracts/<name>/v<version>.peql.json` and `published.json`.
/// Every stored contract is loaded when the store opens; one that does not load is [`Stale`]
/// until a contract of the same name and version is put over it.
#[derive(Debug)]
pub struct DirStore {
    dir: PathBuf,
    memory: MemoryStore,
    stale: RwLock<Vec<Stale>>,
}

impl DirStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<DirStore> {
        let dir = root.into().join("_peql").join("contracts");
        std::fs::create_dir_all(&dir)?;
        let store = DirStore {
            dir,
            memory: MemoryStore::default(),
            stale: RwLock::default(),
        };
        for entry in std::fs::read_dir(&store.dir)? {
            let contract_dir = entry?.path();
            if !contract_dir.is_dir() {
                continue;
            }
            let mut files: Vec<PathBuf> = std::fs::read_dir(&contract_dir)?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.to_string_lossy().ends_with(STORED))
                .collect();
            files.sort();
            let name = decode(&contract_dir);
            for f in files {
                match load(&f) {
                    Ok(reg) => store.memory.put(Arc::new(reg))?,
                    Err(cause) => {
                        let stale = Stale {
                            version: file_version(&f),
                            path: f,
                            name: name.clone(),
                            cause,
                        };
                        tracing::warn!(
                            path = %stale.path.display(),
                            cause = %stale.cause,
                            "stored contract bundle is stale; not served"
                        );
                        store.stale.write().expect("store lock").push(stale);
                    }
                }
            }
            let published = contract_dir.join("published.json");
            if published.exists() {
                let names: BTreeSet<String> =
                    serde_json::from_str(&std::fs::read_to_string(&published)?)
                        .map_err(|e| PeqlError::Invalid(format!("{}: {e}", published.display())))?;
                for a in names {
                    store.memory.publish(&name, &a)?;
                }
            }
        }
        Ok(store)
    }

    fn contract_dir(&self, name: &str) -> PathBuf {
        self.dir.join(name.replace('/', "__"))
    }

    fn save_audiences(&self, name: &str) -> Result<()> {
        let dir = self.contract_dir(name);
        std::fs::create_dir_all(&dir)?;
        let a = self.memory.audiences(name);
        std::fs::write(
            dir.join("published.json"),
            serde_json::to_string_pretty(&a).map_err(|e| PeqlError::Invalid(e.to_string()))?,
        )?;
        Ok(())
    }
}

/// A stored contract, loaded as stored; the error is the cause it is stale.
fn load(path: &Path) -> std::result::Result<Registered, String> {
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    let stored: StoredContract = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    Registered::from_compiled(
        stored.document,
        stored.compiled.as_bytes(),
        stored.functions,
    )
    .map_err(|e| e.to_string())
}

const STORED: &str = ".peql.json";

/// The version in `v<version>.peql.json`.
fn file_version(path: &Path) -> Option<u32> {
    path.file_name()?
        .to_str()?
        .strip_suffix(STORED)?
        .strip_prefix('v')?
        .parse()
        .ok()
}

fn decode(dir: &Path) -> String {
    dir.file_name()
        .map(|n| n.to_string_lossy().replace("__", "/"))
        .unwrap_or_default()
}

impl ContractStore for DirStore {
    fn put(&self, c: Arc<Registered>) -> Result<()> {
        let dir = self.contract_dir(c.name());
        std::fs::create_dir_all(&dir)?;
        let compiled = c.compilation.to_bytes().map_err(PeqlError::Invalid)?;
        let stored = StoredContract {
            document: c.doc.clone(),
            functions: c.functions.clone(),
            compiled: String::from_utf8(compiled)
                .map_err(|e| PeqlError::Invalid(format!("the compiled contract: {e}")))?,
        };
        let json = serde_json::to_string(&stored).map_err(|e| PeqlError::Invalid(e.to_string()))?;
        let path = dir.join(format!("v{:010}{STORED}", c.compilation.contract.version));
        crate::atomic::write(&path, json)?;
        self.stale
            .write()
            .expect("store lock")
            .retain(|s| s.path != path);
        self.memory.put(c)
    }
    fn current(&self, name: &str) -> Option<Arc<Registered>> {
        self.memory.current(name)
    }
    fn version(&self, name: &str, version: u32) -> Option<Arc<Registered>> {
        self.memory.version(name, version)
    }
    fn list(&self) -> Vec<Arc<Registered>> {
        self.memory.list()
    }
    fn audiences(&self, name: &str) -> BTreeSet<String> {
        self.memory.audiences(name)
    }
    fn publish(&self, name: &str, audience: &str) -> Result<()> {
        self.memory.publish(name, audience)?;
        self.save_audiences(name)
    }
    fn unpublish(&self, name: &str, audience: &str) -> Result<()> {
        self.memory.unpublish(name, audience)?;
        self.save_audiences(name)
    }
    fn stale(&self) -> Vec<Stale> {
        self.stale.read().expect("store lock").clone()
    }
}
