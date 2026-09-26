//! Binding resolution (peQL design 4.4): a contract's binding as a DataFusion table.
//! Parquet bindings are listing tables over a directory or an object store prefix: streamed,
//! never loaded whole, with hive partitions and statistics pruning.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::file::statistics::Statistics;
use object_store::ObjectStore;
use parcel_core::CompiledContract;

use crate::error::{PeqlError, Result};
use crate::manifest::{FileEntry, FlagStatus};

/// Key-value metadata stamped into every Parquet file peQL writes.
pub const CONTRACT_HASH_KEY: &str = "parcel.contract_hash";
pub const CONTRACT_NAME_KEY: &str = "parcel.contract";

/// Turns a contract's binding into the table its view reads.
#[async_trait]
pub trait BindingResolver: Send + Sync {
    /// The data the contract binds. With `stored`, it includes the flag, derived and `_other`
    /// columns peQL writes; without, only the raw row columns.
    async fn provider(
        &self,
        contract: &CompiledContract,
        stored: bool,
    ) -> Result<Arc<dyn TableProvider>>;
    /// Where the data and its manifests live, when peQL can read and write there.
    fn location(&self, contract: &CompiledContract) -> Option<Location>;
    /// Object stores a session must know to read and write this resolver's locations.
    fn object_stores(&self) -> Vec<(ObjectStoreUrl, Arc<dyn ObjectStore>)> {
        Vec::new()
    }
}

/// Where a contract's files are: a file or directory on the local filesystem, or a file or
/// prefix in an object store ([`crate::object_binding`]).
#[derive(Clone, Debug)]
pub enum Location {
    Local(PathBuf),
    Object(crate::object_binding::ObjectLocation),
}

impl Location {
    /// Whether the binding names one Parquet file rather than a directory of them.
    pub fn is_single_file(&self) -> bool {
        match self {
            Location::Local(p) => p.is_file() || p.extension().is_some_and(|e| e == "parquet"),
            Location::Object(o) => o.is_single_file(),
        }
    }

    /// The same location, however it was spelled: two contracts over one key share files.
    pub fn key(&self) -> Result<String> {
        match self {
            Location::Local(p) => Ok(p.canonicalize()?.display().to_string()),
            Location::Object(o) => Ok(o.url(true)),
        }
    }
}

/// Parquet directories on the local filesystem; relative bindings resolve under `base`.
#[derive(Clone, Debug)]
pub struct LocalParquet {
    pub base: PathBuf,
}

impl LocalParquet {
    pub fn root(&self, contract: &CompiledContract) -> PathBuf {
        let raw = contract.binding.parquet.trim_start_matches("file://");
        let p = Path::new(raw);
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            self.base.join(p)
        }
    }
}

#[async_trait]
impl BindingResolver for LocalParquet {
    async fn provider(
        &self,
        contract: &CompiledContract,
        stored: bool,
    ) -> Result<Arc<dyn TableProvider>> {
        listing_table(contract, &self.root(contract), stored)
    }

    fn location(&self, contract: &CompiledContract) -> Option<Location> {
        Some(Location::Local(self.root(contract)))
    }
}

/// Schema of the files on disk, without partition columns. With `stored`: the scan schema
/// plus flag and derived columns, as peQL writes them; without: the raw row columns.
pub fn file_schema(contract: &CompiledContract, stored: bool) -> SchemaRef {
    let parts = &contract.binding.partitioned_by;
    let base = if stored {
        &contract.scan_schema
    } else {
        &contract.row_schema
    };
    let mut fields: Vec<Field> = base
        .fields()
        .iter()
        .filter(|f| !parts.contains(f.name()))
        .map(|f| f.as_ref().clone())
        .collect();
    if !stored {
        return Arc::new(Schema::new(fields));
    }
    for flag in &contract.flags {
        fields.push(Field::new(&flag.column, DataType::Boolean, true));
    }
    for d in &contract.derived {
        fields.push(Field::new(&d.column, d.ty.to_arrow(), true));
    }
    Arc::new(Schema::new(fields))
}

/// `path`, canonical, as the string DataFusion parses into a listing URL; `dir` marks a
/// directory with a trailing separator. On Windows `canonicalize` returns the verbatim form
/// (`\\?\D:\data`), which DataFusion does not read as a local path, so the prefix is removed.
pub fn local_url(path: &Path, dir: bool) -> Result<String> {
    let canonical = path.canonicalize()?;
    let mut s = canonical.display().to_string();
    if cfg!(windows)
        && let Some(rest) = s.strip_prefix(r"\\?\")
        && !rest.starts_with(r"UNC\")
    {
        s = rest.to_string();
    }
    if dir && !s.ends_with(std::path::MAIN_SEPARATOR) {
        s.push(std::path::MAIN_SEPARATOR);
    }
    Ok(s)
}

/// A listing table over every Parquet file under `root`. A single-file binding
/// (`binding: {parquet: data/orders.parquet}`) reads that file with the raw row schema.
pub fn listing_table(
    contract: &CompiledContract,
    root: &Path,
    stored: bool,
) -> Result<Arc<dyn TableProvider>> {
    if root.is_file() {
        return listing_table_at(contract, &local_url(root, false)?, true, stored);
    }
    std::fs::create_dir_all(root)?;
    listing_table_at(contract, &local_url(root, true)?, false, stored)
}

/// A listing table at a URL DataFusion lists: a local path, or an object store URL whose
/// store the session has registered. Scans stream; files and row groups are pruned by
/// statistics, and hive partitions become columns.
pub fn listing_table_at(
    contract: &CompiledContract,
    url: &str,
    single_file: bool,
    stored: bool,
) -> Result<Arc<dyn TableProvider>> {
    let format = Arc::new(ParquetFormat::default().with_enable_pruning(true));
    let url = ListingTableUrl::parse(url)?;
    if single_file {
        let config = ListingTableConfig::new(url)
            .with_listing_options(ListingOptions::new(format).with_file_extension(".parquet"))
            .with_schema(contract.row_schema.clone());
        return Ok(Arc::new(ListingTable::try_new(config)?));
    }
    let partition_cols: Vec<(String, DataType)> = contract
        .binding
        .partitioned_by
        .iter()
        .map(|p| {
            let f = contract.row_schema.field_with_name(p).map_err(|_| {
                PeqlError::Invalid(format!("partition column `{p}` is not in the data"))
            })?;
            Ok((p.clone(), f.data_type().clone()))
        })
        .collect::<Result<_>>()?;
    let options = ListingOptions::new(format)
        .with_file_extension(".parquet")
        .with_table_partition_cols(partition_cols);
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(file_schema(contract, stored));
    Ok(Arc::new(ListingTable::try_new(config)?))
}

/// Every Parquet file under the binding, in path order.
pub fn list_files(root: &Path) -> Result<Vec<PathBuf>> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
        for entry in std::fs::read_dir(dir)? {
            let p = entry?.path();
            if p.is_dir() {
                if p.file_name().is_some_and(|n| n == "_peql") {
                    continue;
                }
                walk(&p, out)?;
            } else if p.extension().is_some_and(|e| e == "parquet") {
                out.push(p);
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    if root.is_file() {
        out.push(root.to_path_buf());
    } else if root.exists() {
        walk(root, &mut out)?;
    }
    out.sort();
    Ok(out)
}

/// One file's footer: row count, the contract hash it was written under, and per-flag status.
pub fn file_entry(root: &Path, path: &Path, flag_columns: &[String]) -> Result<FileEntry> {
    let reader = SerializedFileReader::new(File::open(path)?)
        .map_err(|e| PeqlError::Invalid(format!("{}: {e}", path.display())))?;
    Ok(entry_from_footer(
        path.strip_prefix(root)
            .unwrap_or(path)
            .display()
            .to_string(),
        std::fs::metadata(path)?.len(),
        reader.metadata(),
        flag_columns,
    ))
}

/// A manifest entry from a file's decoded footer, wherever the file lives.
pub fn entry_from_footer(
    path: String,
    bytes: u64,
    meta: &ParquetMetaData,
    flag_columns: &[String],
) -> FileEntry {
    let file_meta = meta.file_metadata();
    let contract_hash = file_meta
        .key_value_metadata()
        .and_then(|kv| kv.iter().find(|k| k.key == CONTRACT_HASH_KEY))
        .and_then(|k| k.value.clone())
        .unwrap_or_default();
    let schema = file_meta.schema_descr();
    let mut flags = BTreeMap::new();
    for flag in flag_columns {
        let Some(idx) = (0..schema.num_columns()).find(|i| schema.column(*i).name() == flag) else {
            continue;
        };
        let (mut any_true, mut any_false) = (false, false);
        for rg in meta.row_groups() {
            match rg.column(idx).statistics() {
                // Flags are null-safe, so min and max decide the status.
                Some(Statistics::Boolean(s)) => {
                    any_true |= s.max_opt() == Some(&true);
                    any_false |= s.min_opt() == Some(&false);
                }
                _ => {
                    any_true = true;
                    any_false = true;
                }
            }
        }
        let status = match (any_true, any_false) {
            (true, false) => FlagStatus::AllPass,
            (false, true) => FlagStatus::AllFail,
            _ => FlagStatus::Mixed,
        };
        flags.insert(flag.clone(), status);
    }
    FileEntry {
        path,
        rows: file_meta.num_rows(),
        bytes,
        contract_hash,
        flags,
    }
}

/// sha256 over (relative path, sha256 of bytes) for every file, in path order.
pub fn data_hash(root: &Path, files: &[PathBuf]) -> Result<String> {
    use std::io::Read;
    let mut acc = DataHash::default();
    for f in files {
        let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
        let mut file = File::open(f)?;
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            sha2::Digest::update(&mut hasher, &buf[..n]);
        }
        acc.add(
            &f.strip_prefix(root).unwrap_or(f).display().to_string(),
            &hex::encode(sha2::Digest::finalize(hasher)),
        );
    }
    Ok(acc.finish())
}

/// The data hash, built one file at a time, in path order.
#[derive(Default)]
pub struct DataHash(String);

impl DataHash {
    pub fn add(&mut self, relative_path: &str, sha256_hex: &str) {
        self.0.push_str(relative_path);
        self.0.push(':');
        self.0.push_str(sha256_hex);
        self.0.push('\n');
    }
    pub fn finish(self) -> String {
        parcel_core::hash::sha256_hex(self.0.as_bytes())
    }
}
