//! Result cache. An answer depends on more than the SQL and the tenant: on each contract's
//! compiled rules, the caller's bound context (roles, clearance, time), which shapes apply,
//! and the data. The key covers all of them, so two callers see one cached answer only when
//! the contracts would give them the same one, and a write or a new contract version never
//! serves a stale result. A cached answer with noise is served as it was: the same noisy
//! answer twice spends no more privacy.

use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use datafusion::arrow::array::RecordBatch;
use lru::LruCache;

/// The fingerprint of everything a result depends on.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub digest: String,
    /// Contracts the query read, for invalidation.
    pub contracts: Vec<String>,
}

impl CacheKey {
    /// `parts` are, per contract: name, compilation hash, bound parameters, active shapes and data hash.
    pub fn new(sql: &str, parts: &[(String, String)]) -> CacheKey {
        let mut text = String::from(sql);
        for (name, fingerprint) in parts {
            text.push('\u{1e}');
            text.push_str(name);
            text.push('\u{1f}');
            text.push_str(fingerprint);
        }
        CacheKey {
            digest: parcel_core::hash::sha256_hex(text.as_bytes()),
            contracts: parts.iter().map(|(n, _)| n.clone()).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryCacheConfig {
    pub max_entries: usize,
    pub max_bytes: usize,
    pub ttl_secs: u64,
}

impl Default for QueryCacheConfig {
    fn default() -> Self {
        QueryCacheConfig {
            max_entries: 10_000,
            max_bytes: 256 * 1024 * 1024,
            ttl_secs: 60,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub entry_count: usize,
    pub total_bytes: usize,
    pub hits: u64,
    pub misses: u64,
}

struct Entry {
    batches: Vec<RecordBatch>,
    bytes: usize,
    at: Instant,
}

struct Inner {
    lru: LruCache<CacheKey, Entry>,
    total_bytes: usize,
    hits: u64,
    misses: u64,
}

pub struct QueryCache {
    inner: Mutex<Inner>,
    config: QueryCacheConfig,
}

impl std::fmt::Debug for QueryCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "QueryCache({:?})", self.stats())
    }
}

impl QueryCache {
    pub fn new(config: QueryCacheConfig) -> QueryCache {
        QueryCache {
            inner: Mutex::new(Inner {
                lru: LruCache::new(NonZeroUsize::new(config.max_entries.max(1)).expect("non-zero")),
                total_bytes: 0,
                hits: 0,
                misses: 0,
            }),
            config,
        }
    }

    pub fn get(&self, key: &CacheKey) -> Option<Vec<RecordBatch>> {
        let mut g = self.inner.lock().expect("cache lock");
        let ttl = Duration::from_secs(self.config.ttl_secs);
        let expired = g.lru.peek(key).is_some_and(|e| e.at.elapsed() > ttl);
        if expired && let Some(e) = g.lru.pop(key) {
            g.total_bytes -= e.bytes;
        }
        match g.lru.get(key).map(|e| e.batches.clone()) {
            Some(b) => {
                g.hits += 1;
                Some(b)
            }
            None => {
                g.misses += 1;
                None
            }
        }
    }

    pub fn put(&self, key: CacheKey, batches: Vec<RecordBatch>) {
        let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        if bytes > self.config.max_bytes {
            return;
        }
        let mut g = self.inner.lock().expect("cache lock");
        while g.total_bytes + bytes > self.config.max_bytes {
            match g.lru.pop_lru() {
                Some((_, e)) => g.total_bytes -= e.bytes,
                None => break,
            }
        }
        if let Some((_, old)) = g.lru.push(
            key,
            Entry {
                batches,
                bytes,
                at: Instant::now(),
            },
        ) {
            g.total_bytes -= old.bytes;
        }
        g.total_bytes += bytes;
    }

    /// Drop every answer that read `contract`.
    pub fn invalidate_contract(&self, contract: &str) {
        let mut g = self.inner.lock().expect("cache lock");
        let keys: Vec<CacheKey> = g
            .lru
            .iter()
            .filter(|(k, _)| k.contracts.iter().any(|c| c == contract))
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys {
            if let Some(e) = g.lru.pop(&k) {
                g.total_bytes -= e.bytes;
            }
        }
    }

    pub fn invalidate_all(&self) {
        let mut g = self.inner.lock().expect("cache lock");
        g.lru.clear();
        g.total_bytes = 0;
    }

    pub fn stats(&self) -> CacheStats {
        let g = self.inner.lock().expect("cache lock");
        CacheStats {
            entry_count: g.lru.len(),
            total_bytes: g.total_bytes,
            hits: g.hits,
            misses: g.misses,
        }
    }
}
