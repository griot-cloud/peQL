//! The contract store (peQL design 4.2): compiled contracts by name and version. A contract
//! is registered once, compiled by parcel then, and served from here; a query never compiles.
//! Persisted as parcel bundles, each verified by recompiling when loaded.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use datafusion::arrow::datatypes::Schema;
use parcel_core::{Compilation, ContractDoc};
use parcel_runtime::bundle::{Bundle, BundledFunction};

use crate::error::{PeqlError, Result};

/// Audience for a contract published to every tenant.
pub const PUBLIC: &str = "public";

/// One registered version of a contract: what parcel compiled and what it compiled from.
#[derive(Debug)]
pub struct Registered {
    pub doc: ContractDoc,
    /// The documents it inherits from, parent first.
    pub ancestors: Vec<ContractDoc>,
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

    /// The portable form: what `parcel compile -o` writes and a verifier re-runs.
    pub fn bundle(&self) -> Result<Bundle> {
        Bundle::with_functions(
            &self.doc,
            &self.ancestors,
            &self.schema,
            &self.compilation,
            self.functions.clone(),
        )
        .map_err(PeqlError::from)
    }

    /// Recompile a bundle and accept it only when it gives the same compilation hash.
    pub fn from_bundle(bundle: &Bundle) -> Result<Registered> {
        let compilation = bundle.verify().map_err(PeqlError::Invalid)?;
        Ok(Registered {
            doc: bundle.document.clone(),
            ancestors: bundle.ancestors.clone(),
            schema: bundle.schema().map_err(PeqlError::Invalid)?,
            functions: bundle.functions.clone(),
            compilation,
        })
    }
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
}

/// On disk: `<root>/_peql/contracts/<name>/v<version>.parcel.json` and `published.json`.
/// Every bundle is verified when the store opens.
#[derive(Debug)]
pub struct DirStore {
    dir: PathBuf,
    memory: MemoryStore,
}

impl DirStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<DirStore> {
        let dir = root.into().join("_peql").join("contracts");
        std::fs::create_dir_all(&dir)?;
        let store = DirStore {
            dir,
            memory: MemoryStore::default(),
        };
        for entry in std::fs::read_dir(&store.dir)? {
            let contract_dir = entry?.path();
            if !contract_dir.is_dir() {
                continue;
            }
            let mut files: Vec<PathBuf> = std::fs::read_dir(&contract_dir)?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.to_string_lossy().ends_with(".parcel.json"))
                .collect();
            files.sort();
            for f in files {
                let bundle = Bundle::from_json(&std::fs::read_to_string(&f)?)
                    .map_err(|e| PeqlError::Invalid(format!("{}: {e}", f.display())))?;
                let reg = Registered::from_bundle(&bundle)
                    .map_err(|e| PeqlError::Invalid(format!("{}: {e}", f.display())))?;
                store.memory.put(Arc::new(reg))?;
            }
            let published = contract_dir.join("published.json");
            if published.exists() {
                let names: BTreeSet<String> =
                    serde_json::from_str(&std::fs::read_to_string(&published)?)
                        .map_err(|e| PeqlError::Invalid(format!("{}: {e}", published.display())))?;
                let name = decode(&contract_dir);
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

fn decode(dir: &std::path::Path) -> String {
    dir.file_name()
        .map(|n| n.to_string_lossy().replace("__", "/"))
        .unwrap_or_default()
}

impl ContractStore for DirStore {
    fn put(&self, c: Arc<Registered>) -> Result<()> {
        let dir = self.contract_dir(c.name());
        std::fs::create_dir_all(&dir)?;
        let json = c
            .bundle()?
            .to_json()
            .map_err(|e| PeqlError::Invalid(e.to_string()))?;
        let path = dir.join(format!(
            "v{:010}.parcel.json",
            c.compilation.contract.version
        ));
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, json)?;
        std::fs::rename(tmp, path)?;
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
}
