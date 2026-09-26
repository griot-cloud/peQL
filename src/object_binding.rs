//! Bindings in an object store: `s3://bucket/prefix/`, or any store `object_store` speaks.
//!
//! [`ObjectStoreParquet`] resolves a contract's binding to a prefix in one store and serves it
//! the way [`crate::binding::LocalParquet`] serves a directory: a streaming listing table with
//! hive partitions and statistics pruning, written by the same write path, with its manifests
//! beside the data under `<prefix>/_peql/manifests/`. Nothing is read whole: listings, footers
//! and hashes go through ranged and streamed reads.
//!
//! ```no_run
//! # #[cfg(feature = "s3")]
//! # fn f() -> peql::Result<()> {
//! use std::sync::Arc;
//! use object_store::aws::AmazonS3Builder;
//! use peql::object_binding::ObjectStoreParquet;
//!
//! let store = AmazonS3Builder::new()
//!     .with_bucket_name("lake")
//!     .with_region("eu-west-1")
//!     .with_access_key_id("…")
//!     .with_secret_access_key("…")
//!     .build()
//!     .unwrap();
//! let bindings = ObjectStoreParquet::new("s3://lake/tenant-a/", Arc::new(store))?;
//! let engine = peql::Engine::open("/var/lib/peql")?.with_bindings(Arc::new(bindings));
//! # Ok(()) }
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::catalog::TableProvider;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::file::FOOTER_SIZE;
use datafusion::parquet::file::metadata::{FooterTail, ParquetMetaDataReader};
use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload};
use parcel_core::CompiledContract;

use crate::binding::{self, BindingResolver, DataHash, Location};
use crate::error::{PeqlError, Result};
use crate::manifest::{FileEntry, MANIFEST_DIR, Manifest};

/// Parquet under one prefix of one object store. A binding that is a URL must name this
/// store (`s3://lake/...` for a resolver over `s3://lake/`); a relative binding resolves under
/// the resolver's prefix. Anything else has no binding here.
#[derive(Clone, Debug)]
pub struct ObjectStoreParquet {
    store_url: ObjectStoreUrl,
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
}

impl ObjectStoreParquet {
    /// `base` is the store and prefix bindings resolve under: `s3://bucket/` or
    /// `s3://bucket/some/prefix/`. The store is the one that serves that bucket.
    pub fn new(base: &str, store: Arc<dyn ObjectStore>) -> Result<ObjectStoreParquet> {
        let (scheme, rest) = base
            .split_once("://")
            .ok_or_else(|| PeqlError::Invalid(format!("`{base}` is not a store URL")))?;
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(PeqlError::Invalid(format!("`{base}` names no bucket")));
        }
        Ok(ObjectStoreParquet {
            store_url: ObjectStoreUrl::parse(format!("{scheme}://{bucket}"))?,
            store,
            prefix: ObjectPath::from(prefix),
        })
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// Where a contract's binding lives in this store, if it lives here.
    pub fn object_location(&self, contract: &CompiledContract) -> Option<ObjectLocation> {
        let raw = contract.binding.parquet.as_str();
        let path = match raw.split_once("://") {
            Some(_) => {
                let rest = raw.strip_prefix(self.store_url.as_str())?;
                ObjectPath::from(rest)
            }
            None => {
                let mut p = self.prefix.clone();
                for part in raw.split('/').filter(|s| !s.is_empty() && *s != ".") {
                    if part == ".." {
                        return None;
                    }
                    p = p.join(part);
                }
                p
            }
        };
        Some(ObjectLocation {
            store_url: self.store_url.clone(),
            store: self.store.clone(),
            path,
        })
    }
}

#[async_trait]
impl BindingResolver for ObjectStoreParquet {
    async fn provider(
        &self,
        contract: &CompiledContract,
        stored: bool,
    ) -> Result<Arc<dyn TableProvider>> {
        let loc = self.object_location(contract).ok_or_else(|| {
            PeqlError::Invalid(format!(
                "`{}` binds `{}`, which is not in {}",
                contract.name,
                contract.binding.parquet,
                self.store_url.as_str()
            ))
        })?;
        let single = loc.is_single_file();
        binding::listing_table_at(contract, &loc.url(!single), single, stored)
    }

    fn location(&self, contract: &CompiledContract) -> Option<Location> {
        self.object_location(contract).map(Location::Object)
    }

    fn object_stores(&self) -> Vec<(ObjectStoreUrl, Arc<dyn ObjectStore>)> {
        vec![(self.store_url.clone(), self.store.clone())]
    }
}

/// A file or prefix in an object store.
#[derive(Clone, Debug)]
pub struct ObjectLocation {
    /// The store: `s3://bucket`.
    pub store_url: ObjectStoreUrl,
    pub store: Arc<dyn ObjectStore>,
    /// The file, or the prefix of the files, within the store.
    pub path: ObjectPath,
}

impl ObjectLocation {
    pub fn is_single_file(&self) -> bool {
        self.path.extension() == Some("parquet")
    }

    /// The location as a URL; `dir` marks a prefix with a trailing `/`.
    pub fn url(&self, dir: bool) -> String {
        let mut s = format!("{}{}", self.store_url.as_str(), self.path);
        if dir && !s.ends_with('/') {
            s.push('/');
        }
        s
    }

    fn manifest_path(&self, contract: &str) -> ObjectPath {
        let mut p = self.path.clone();
        for part in MANIFEST_DIR.split('/') {
            p = p.join(part);
        }
        p.join(format!("{}.json", contract.replace('/', "__")).as_str())
    }

    fn relative(&self, file: &ObjectPath) -> String {
        match file.prefix_match(&self.path) {
            Some(parts) if !self.is_single_file() => parts
                .map(|p| p.as_ref().to_owned())
                .collect::<Vec<_>>()
                .join("/"),
            _ => file.filename().unwrap_or_default().to_owned(),
        }
    }

    /// Every Parquet object under the prefix (the object itself for a single file), in path
    /// order, skipping `_peql/`.
    pub async fn list_files(&self) -> Result<Vec<ObjectMeta>> {
        if self.is_single_file() {
            return match self.store.head(&self.path).await {
                Ok(m) => Ok(vec![m]),
                Err(object_store::Error::NotFound { .. }) => Ok(Vec::new()),
                Err(e) => Err(store_err(e)),
            };
        }
        let mut files: Vec<ObjectMeta> = self
            .store
            .list(Some(&self.path))
            .try_filter(|m| {
                let keep = m.location.extension() == Some("parquet")
                    && !m.location.parts().any(|p| p.as_ref() == "_peql");
                futures::future::ready(keep)
            })
            .try_collect()
            .await
            .map_err(store_err)?;
        files.sort_by(|a, b| a.location.cmp(&b.location));
        Ok(files)
    }

    /// Delete every data file (manifests stay; the refresh that follows rewrites them).
    pub async fn remove_files(&self) -> Result<()> {
        for f in self.list_files().await? {
            self.store.delete(&f.location).await.map_err(store_err)?;
        }
        Ok(())
    }

    /// Manifest entries from each file's footer, read with two ranged reads.
    pub async fn file_entries(
        &self,
        files: &[ObjectMeta],
        flag_columns: &[String],
    ) -> Result<Vec<FileEntry>> {
        let mut out = Vec::with_capacity(files.len());
        for f in files {
            let meta = self.footer(f).await?;
            out.push(binding::entry_from_footer(
                self.relative(&f.location),
                f.size,
                &meta,
                flag_columns,
            ));
        }
        Ok(out)
    }

    async fn footer(
        &self,
        f: &ObjectMeta,
    ) -> Result<datafusion::parquet::file::metadata::ParquetMetaData> {
        let bad = |e: &dyn std::fmt::Display| PeqlError::Invalid(format!("{}: {e}", f.location));
        let footer_size = FOOTER_SIZE as u64;
        if f.size < footer_size {
            return Err(bad(&"too short to be Parquet"));
        }
        let tail = self
            .store
            .get_range(&f.location, f.size - footer_size..f.size)
            .await
            .map_err(store_err)?;
        let tail: [u8; FOOTER_SIZE] = tail.as_ref().try_into().map_err(|_| bad(&"short read"))?;
        let len = FooterTail::try_new(&tail)
            .map_err(|e| bad(&e))?
            .metadata_length() as u64;
        let end = f.size - footer_size;
        if len > end {
            return Err(bad(&"footer longer than the file"));
        }
        let buf = self
            .store
            .get_range(&f.location, end - len..end)
            .await
            .map_err(store_err)?;
        ParquetMetaDataReader::decode_metadata(&buf).map_err(|e| bad(&e))
    }

    /// The same hash [`binding::data_hash`] computes over local files, streamed.
    pub async fn data_hash(&self, files: &[ObjectMeta]) -> Result<String> {
        let mut acc = DataHash::default();
        for f in files {
            let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
            let mut stream = self
                .store
                .get(&f.location)
                .await
                .map_err(store_err)?
                .into_stream();
            while let Some(chunk) = stream.try_next().await.map_err(store_err)? {
                sha2::Digest::update(&mut hasher, &chunk);
            }
            acc.add(
                &self.relative(&f.location),
                &hex::encode(sha2::Digest::finalize(hasher)),
            );
        }
        Ok(acc.finish())
    }

    pub async fn load_manifest(&self, contract: &str) -> Result<Option<Manifest>> {
        match self.store.get(&self.manifest_path(contract)).await {
            Ok(r) => {
                let bytes = r.bytes().await.map_err(store_err)?;
                serde_json::from_slice(&bytes)
                    .map(Some)
                    .map_err(|e| PeqlError::Invalid(format!("manifest of `{contract}`: {e}")))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(store_err(e)),
        }
    }

    /// One put: object stores replace an object whole, so a reader never sees half of one.
    pub async fn save_manifest(&self, manifest: &Manifest) -> Result<()> {
        let body = serde_json::to_vec_pretty(manifest)
            .map_err(|e| PeqlError::Invalid(format!("manifest: {e}")))?;
        self.store
            .put(
                &self.manifest_path(&manifest.contract),
                PutPayload::from(Bytes::from(body)),
            )
            .await
            .map_err(store_err)?;
        Ok(())
    }
}

fn store_err(e: object_store::Error) -> PeqlError {
    PeqlError::DataFusion(datafusion::error::DataFusionError::ObjectStore(Box::new(e)))
}
