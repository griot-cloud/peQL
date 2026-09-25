//! Griot platform adapter (feature `platform`): contracts arrive from the T03 contract
//! authority as signed parcel bundles over HTTP. The signature proves who issued a bundle;
//! recompiling it to its compilation hash (as every registration does) proves what it says.

pub mod bundle;
pub mod source;

pub use bundle::{SignedBundle, VerifyError};
pub use source::PlatformBundleSource;
