//! The function registry store (peQL design 4.2a): tenants' WebAssembly functions, verified
//! by parcel-runtime at registration and kept on disk by owner. A contract may call the
//! built-ins and its owner's functions, and nothing else.

use std::path::PathBuf;
use std::sync::RwLock;

use parcel_core::Registry;
use parcel_core::registry::{FunctionEntry, FunctionManifest};
use parcel_runtime::bundle::BundledFunction;
use serde::{Deserialize, Serialize};

use crate::error::{PeqlError, Result};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredFunction {
    owner: String,
    manifest: FunctionManifest,
    hash: String,
}

/// Registered functions, optionally persisted under `<root>/_peql/functions/<owner>/`.
#[derive(Debug)]
pub struct FunctionStore {
    dir: Option<PathBuf>,
    registry: RwLock<Registry>,
    modules: RwLock<Vec<BundledFunction>>,
}

impl FunctionStore {
    pub fn in_memory() -> FunctionStore {
        FunctionStore {
            dir: None,
            registry: RwLock::new(Registry::builtin()),
            modules: RwLock::new(Vec::new()),
        }
    }

    /// Load every stored function, checking each module still matches its recorded hash.
    pub fn open(root: impl Into<PathBuf>) -> Result<FunctionStore> {
        let dir = root.into().join("_peql").join("functions");
        let store = FunctionStore {
            dir: Some(dir.clone()),
            ..FunctionStore::in_memory()
        };
        let Ok(owners) = std::fs::read_dir(&dir) else {
            return Ok(store);
        };
        for owner in owners.flatten() {
            for f in std::fs::read_dir(owner.path())?.flatten() {
                let p = f.path();
                if p.extension().is_none_or(|e| e != "json") {
                    continue;
                }
                let rec: StoredFunction = serde_json::from_str(&std::fs::read_to_string(&p)?)
                    .map_err(|e| PeqlError::Invalid(format!("{}: {e}", p.display())))?;
                let module = std::fs::read(p.with_extension("wasm"))?;
                let entry = store.load(&module, &rec.manifest, &rec.owner)?;
                if entry.hash != rec.hash {
                    return Err(PeqlError::Invalid(format!(
                        "{}: the stored module no longer matches its hash",
                        p.display()
                    )));
                }
            }
        }
        Ok(store)
    }

    fn load(
        &self,
        module: &[u8],
        manifest: &FunctionManifest,
        owner: &str,
    ) -> Result<FunctionEntry> {
        if let Some(existing) = self.registry.read().expect("lock").get(&manifest.name)
            && existing.owner != owner
        {
            return Err(PeqlError::Invalid(format!(
                "`{}` is already registered by `{}`",
                manifest.name, existing.owner
            )));
        }
        let entry = parcel_runtime::wasm::install(module, manifest, owner)?;
        self.registry.write().expect("lock").insert(entry.clone());
        let mut modules = self.modules.write().expect("lock");
        modules.retain(|m| !(m.owner == owner && m.manifest.name == manifest.name));
        modules.push(BundledFunction {
            owner: owner.to_owned(),
            manifest: manifest.clone(),
            module: hex::encode(module),
        });
        Ok(entry)
    }

    /// Verify a module (imports nothing, speaks the ABI, passes a smoke batch), load it, and store it.
    pub fn register(
        &self,
        module: &[u8],
        manifest: &FunctionManifest,
        owner: &str,
    ) -> Result<FunctionEntry> {
        let entry = self.load(module, manifest, owner)?;
        if let Some(dir) = &self.dir {
            let dir = dir.join(owner);
            std::fs::create_dir_all(&dir)?;
            std::fs::write(dir.join(format!("{}.wasm", manifest.name)), module)?;
            let rec = StoredFunction {
                owner: owner.to_owned(),
                manifest: manifest.clone(),
                hash: entry.hash.clone(),
            };
            std::fs::write(
                dir.join(format!("{}.json", manifest.name)),
                serde_json::to_string_pretty(&rec)
                    .map_err(|e| PeqlError::Invalid(e.to_string()))?,
            )?;
        }
        Ok(entry)
    }

    /// Load the functions a bundle carries, as registering it would.
    pub fn adopt(&self, functions: &[BundledFunction]) -> Result<()> {
        for f in functions {
            let module = hex::decode(&f.module).map_err(|e| PeqlError::Invalid(e.to_string()))?;
            self.load(&module, &f.manifest, &f.owner)?;
        }
        Ok(())
    }

    /// What a contract of `owner` may call.
    pub fn registry_for(&self, owner: Option<&str>) -> Registry {
        self.registry.read().expect("lock").visible_to(owner)
    }

    /// The modules a compiled contract is pinned to, to carry in its bundle.
    pub fn modules_for(
        &self,
        pins: &std::collections::BTreeSet<parcel_core::registry::FunctionPin>,
    ) -> Vec<BundledFunction> {
        let registry = self.registry.read().expect("lock");
        let modules = self.modules.read().expect("lock");
        pins.iter()
            .filter_map(|pin| {
                let e = registry
                    .get(&pin.name)
                    .filter(|e| e.hash == pin.hash && !e.is_builtin())?;
                modules
                    .iter()
                    .find(|m| m.owner == e.owner && m.manifest.name == e.name)
                    .cloned()
            })
            .collect()
    }

    /// Every tenant function registered.
    pub fn list(&self) -> Vec<FunctionEntry> {
        self.registry
            .read()
            .expect("lock")
            .entries()
            .filter(|e| !e.is_builtin())
            .cloned()
            .collect()
    }
}
