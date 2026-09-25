//! A bounded pool of query workers for long-running deployments (K04D). Workers share one
//! governed [`Engine`]; the pool adds a queue per worker, tenant affinity, and a graceful
//! drain on shutdown. Results can be signed by an [`EnvelopeSigner`] (T05 on the platform).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use parcel_runtime::Caller;
use tokio::sync::{RwLock, mpsc, oneshot};

use crate::engine::Engine;
use crate::envelope::Envelope;
use crate::error::PeqlError;

/// Signs a result envelope, returning a compact JWS.
#[async_trait]
pub trait EnvelopeSigner: Send + Sync {
    async fn sign(
        &self,
        envelope: &Envelope,
        tenant: &str,
        correlation_id: &str,
    ) -> Result<String, String>;
}

pub struct QueryTask {
    pub sql: String,
    pub caller: Caller,
    pub correlation_id: String,
    pub reply: oneshot::Sender<QueryResult>,
}

pub type QueryResult = Result<QueryResultOk, PoolError>;

#[derive(Debug)]
pub struct QueryResultOk {
    pub batches: Vec<RecordBatch>,
    pub envelope: Envelope,
    /// The signed envelope, when the pool has a signer.
    pub attestation_jws: Option<String>,
    pub correlation_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error("pool is shutting down")]
    Shutdown,
    #[error("pool queue full: all {worker_count} workers busy")]
    QueueFull { worker_count: usize },
    #[error(transparent)]
    Engine(#[from] PeqlError),
    #[error("signing the envelope failed: {0}")]
    Signing(String),
}

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub worker_count: usize,
    pub queue_depth: usize,
    pub drain_timeout_secs: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        PoolConfig {
            worker_count: 4,
            queue_depth: 64,
            drain_timeout_secs: 30,
        }
    }
}

struct Worker {
    tx: mpsc::Sender<QueryTask>,
    in_flight: Arc<AtomicU64>,
    alive: Arc<AtomicBool>,
}

pub struct LongRunningPoolManager {
    workers: Vec<Worker>,
    config: PoolConfig,
    affinity: RwLock<HashMap<String, usize>>,
    shutdown: Arc<AtomicBool>,
}

impl LongRunningPoolManager {
    pub async fn start(
        config: PoolConfig,
        engine: Arc<Engine>,
        signer: Option<Arc<dyn EnvelopeSigner>>,
    ) -> Arc<Self> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let workers = (0..config.worker_count.max(1))
            .map(|id| {
                let (tx, rx) = mpsc::channel::<QueryTask>(config.queue_depth.max(1));
                let in_flight = Arc::new(AtomicU64::new(0));
                let alive = Arc::new(AtomicBool::new(true));
                tokio::spawn(worker_loop(
                    id,
                    rx,
                    engine.clone(),
                    signer.clone(),
                    in_flight.clone(),
                    alive.clone(),
                    shutdown.clone(),
                ));
                Worker {
                    tx,
                    in_flight,
                    alive,
                }
            })
            .collect();
        tracing::info!(workers = config.worker_count, "query pool started");
        Arc::new(LongRunningPoolManager {
            workers,
            config,
            affinity: RwLock::default(),
            shutdown,
        })
    }

    pub async fn submit(&self, task: QueryTask) -> Result<(), PoolError> {
        if self.shutdown.load(Ordering::SeqCst) {
            return Err(PoolError::Shutdown);
        }
        let w = self.select_worker(&task.caller.tenant).await;
        self.workers[w]
            .tx
            .try_send(task)
            .map_err(|_| PoolError::QueueFull {
                worker_count: self.workers.len(),
            })
    }

    /// Stop taking work and wait (up to the drain timeout) for queries in flight.
    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let deadline =
            tokio::time::Instant::now() + Duration::from_secs(self.config.drain_timeout_secs);
        while self.in_flight() > 0 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub fn in_flight(&self) -> u64 {
        self.workers
            .iter()
            .map(|w| w.in_flight.load(Ordering::Relaxed))
            .sum()
    }

    /// A tenant's queries go to the same worker while it has room, else the least busy one.
    async fn select_worker(&self, tenant: &str) -> usize {
        if let Some(&w) = self.affinity.read().await.get(tenant) {
            let worker = &self.workers[w];
            if worker.alive.load(Ordering::Relaxed) && worker.tx.capacity() > 0 {
                return w;
            }
        }
        let best = self
            .workers
            .iter()
            .enumerate()
            .filter(|(_, w)| w.alive.load(Ordering::Relaxed))
            .min_by_key(|(_, w)| w.in_flight.load(Ordering::Relaxed))
            .map(|(i, _)| i)
            .unwrap_or(0);
        self.affinity.write().await.insert(tenant.to_owned(), best);
        best
    }
}

async fn worker_loop(
    id: usize,
    mut rx: mpsc::Receiver<QueryTask>,
    engine: Arc<Engine>,
    signer: Option<Arc<dyn EnvelopeSigner>>,
    in_flight: Arc<AtomicU64>,
    alive: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
) {
    loop {
        if shutdown.load(Ordering::SeqCst) {
            while let Ok(task) = rx.try_recv() {
                let _ = task.reply.send(Err(PoolError::Shutdown));
            }
            break;
        }
        let task = match tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            Ok(Some(t)) => t,
            Ok(None) => break,
            Err(_) => continue,
        };
        in_flight.fetch_add(1, Ordering::Relaxed);
        let result = run(&engine, signer.as_deref(), &task).await;
        in_flight.fetch_sub(1, Ordering::Relaxed);
        let _ = task.reply.send(result);
    }
    alive.store(false, Ordering::SeqCst);
    tracing::info!(worker = id, "query pool worker stopped");
}

async fn run(
    engine: &Engine,
    signer: Option<&dyn EnvelopeSigner>,
    task: &QueryTask,
) -> QueryResult {
    let res = engine.query(&task.sql, &task.caller).await?;
    let attestation_jws = match signer {
        Some(s) => Some(
            s.sign(&res.envelope, &task.caller.tenant, &task.correlation_id)
                .await
                .map_err(PoolError::Signing)?,
        ),
        None => None,
    };
    Ok(QueryResultOk {
        batches: res.batches,
        envelope: res.envelope,
        attestation_jws,
        correlation_id: task.correlation_id.clone(),
    })
}

/// T05, the platform notary, signs envelopes over its Unix socket.
#[cfg(unix)]
#[async_trait]
impl EnvelopeSigner for crate::t05_client::T05Client {
    async fn sign(
        &self,
        envelope: &Envelope,
        tenant: &str,
        correlation_id: &str,
    ) -> Result<String, String> {
        let json = serde_json::to_string(envelope).map_err(|e| e.to_string())?;
        self.sign_envelope(&json, env!("CARGO_PKG_VERSION"), tenant, correlation_id)
            .await
            .map(|r| r.jws)
            .map_err(|e| e.to_string())
    }
}
