//! peQL: a query engine where every table is a data contract.
//!
//! Contracts are written in [parcel](https://github.com/griot-cloud/parcel), which compiles
//! each one into artifacts: expressions a view splices into a query, a validation plan, and
//! a write plan. peQL is the runtime for those artifacts. It stores compiled contracts,
//! writes data under them, and answers SQL in which every `FROM` names a contract. The
//! contract decides what each caller gets; nothing reaches the data except through its view.
//!
//! ```text
//! Engine::query(sql, caller)
//!   guard      one query, nothing else
//!   store      the compiled contract, if the caller may see it
//!   resolve    decide · guarantee · which shapes apply        (parcel-runtime)
//!   view       scan → admits → projection → ctx bound → Gate  (parcel's expressions)
//!   shapes     suppress · aggregate noise; budgets charged     (parcel_runtime::shape)
//!   execute    DataFusion; refused unless every scan is gated
//!   envelope   resolutions · scan stats · attestation · audit
//! ```

pub mod audit;
pub mod binding;
pub mod budget;
pub mod cache;
pub mod engine;
pub mod envelope;
pub mod error;
pub mod format;
pub mod functions;
pub mod gate;
pub mod guard;
pub mod k04d;
pub mod manifest;
pub mod pool;
pub mod store;

/// Griot platform: signed bundles from T03 (feature `platform`).
#[cfg(feature = "platform")]
pub mod platform;

/// Griot platform clients: the T04 storaged byte-read socket and the T05 signing socket.
#[cfg(unix)]
pub mod storaged_client;
#[cfg(unix)]
pub mod t05_client;

/// Lance datasets as contract data (feature `lance`).
#[cfg(all(unix, feature = "lance"))]
pub mod lance_table;

pub use engine::{Engine, QueryResult, Verdict, WriteMode, WriteReport};
pub use envelope::{Envelope, Resolution};
pub use error::{PeqlError, Result};
pub use k04d::{ContractBundleHandle, InitConfig, K04DEngine, sealed};
pub use manifest::Manifest;
/// The caller of a query, as the embedding application authenticated them.
pub use parcel_runtime::Caller;
