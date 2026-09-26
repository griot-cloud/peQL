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
//!   envelope   resolutions · scan stats · attestation · audit · signature
//! ```
//!
//! Around `query`: [`Engine::check`] runs every check without reading a row (what Flight
//! SQL's `GetFlightInfo` answers), [`Engine::view`] is the gated plan an out-of-core executor
//! reads, and [`Engine::write`] is the only way data lands.

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
pub mod manifest;
pub mod object_binding;
pub mod signer;
pub mod store;

/// Flight SQL over tonic (feature `flight`).
#[cfg(feature = "flight")]
pub mod flight;

/// Parcel bundles signed by their issuer, verified (feature `signed-bundle`).
#[cfg(feature = "signed-bundle")]
pub mod signed_bundle;

/// Lance datasets as contract data (feature `lance`).
#[cfg(all(unix, feature = "lance"))]
pub mod lance_table;

pub use binding::{BindingResolver, LocalParquet, Location};
pub use engine::{Checked, Engine, QueryResult, Verdict, WriteMode, WriteReport};
pub use envelope::{Envelope, EnvelopeSigner, Resolution};
pub use error::{PeqlError, Result};
pub use manifest::Manifest;
pub use object_binding::ObjectStoreParquet;
/// The caller of a query, as the embedding application authenticated them.
pub use parcel_runtime::Caller;
pub use signer::SocketSigner;
