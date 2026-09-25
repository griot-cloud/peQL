//! The Griot platform's tenant-scoped engine (K04D). One instance serves one tenant; contracts
//! arrive as parcel bundles over the platform's X02 socket, and every query is governed by
//! them. In 0.3 this engine ran SQL over raw registered tables with no enforcement; in 0.4
//! it is a thin shell over [`Engine`], and registering data binds it to a contract.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use parcel_runtime::Caller;
use parcel_runtime::bundle::Bundle;
use uuid::Uuid;

use crate::engine::{Engine, QueryResult};
use crate::error::{PeqlError, Result};

/// Construction is sealed: only this crate implements [`sealed::EngineCore`], so no other
/// implementation can stand in for the governed engine.
///
/// ```compile_fail
/// struct FakeEngine;
/// impl peql::sealed::EngineCore for FakeEngine {
///     fn tenant_id(&self) -> &str { "evil-tenant" }
///     fn has_contract_bundle(&self) -> bool { true }
/// }
/// ```
pub mod sealed {
    mod private {
        pub trait Sealed {}
        impl Sealed for super::super::K04DEngine {}
    }
    pub trait EngineCore: private::Sealed {
        fn tenant_id(&self) -> &str;
        fn has_contract_bundle(&self) -> bool;
    }
}

#[derive(Debug, Clone)]
pub struct InitConfig {
    /// The tenant this instance serves.
    pub tenant_id: String,
    /// Where contract bundles come from, e.g. `unix:///run/griot/t04.sock`.
    pub contract_bundle_endpoint: String,
    /// Where result envelopes are signed (T05).
    pub attestation_endpoint: String,
    /// Results larger than this are refused.
    pub max_result_rows: usize,
    /// The T04 storaged byte-read socket, for Lance bindings.
    pub storaged_socket: String,
}

impl InitConfig {
    pub fn validate(&self) -> Result<()> {
        for (field, value) in [
            ("tenant_id", &self.tenant_id),
            ("contract_bundle_endpoint", &self.contract_bundle_endpoint),
            ("attestation_endpoint", &self.attestation_endpoint),
            ("storaged_socket", &self.storaged_socket),
        ] {
            if value.is_empty() {
                return Err(PeqlError::Invalid(format!(
                    "invalid engine configuration: {field} must not be empty"
                )));
            }
        }
        if self.max_result_rows == 0 {
            return Err(PeqlError::Invalid(
                "invalid engine configuration: max_result_rows must be > 0".into(),
            ));
        }
        Ok(())
    }
}

/// A contract bundle as received over X02: a parcel bundle, JSON.
#[derive(Debug, Clone)]
pub struct ContractBundleHandle {
    pub(crate) handle_id: Uuid,
    pub(crate) raw_bytes: Vec<u8>,
    pub(crate) contract_id: String,
    pub(crate) tenant_id: String,
}

impl ContractBundleHandle {
    pub fn from_x02_bytes(
        contract_id: impl Into<String>,
        tenant_id: impl Into<String>,
        raw: impl Into<Vec<u8>>,
    ) -> Self {
        ContractBundleHandle {
            handle_id: Uuid::new_v4(),
            raw_bytes: raw.into(),
            contract_id: contract_id.into(),
            tenant_id: tenant_id.into(),
        }
    }
    pub fn contract_id(&self) -> &str {
        &self.contract_id
    }
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
    pub fn handle_id(&self) -> Uuid {
        self.handle_id
    }
    pub fn bundle(&self) -> Result<Bundle> {
        let text = std::str::from_utf8(&self.raw_bytes)
            .map_err(|e| PeqlError::Invalid(format!("bundle is not UTF-8: {e}")))?;
        Bundle::from_json(text).map_err(|e| PeqlError::Invalid(format!("bundle: {e}")))
    }
}

pub struct K04DEngine {
    config: InitConfig,
    engine: Arc<Engine>,
    bundles: Vec<ContractBundleHandle>,
}

impl sealed::EngineCore for K04DEngine {
    fn tenant_id(&self) -> &str {
        &self.config.tenant_id
    }
    fn has_contract_bundle(&self) -> bool {
        !self.bundles.is_empty()
    }
}

impl K04DEngine {
    /// An engine for one tenant. Relative bindings resolve under the working directory.
    pub fn new_with_config(config: InitConfig) -> Result<Self> {
        config.validate()?;
        Ok(K04DEngine {
            config,
            engine: Arc::new(Engine::in_memory(".")),
            bundles: Vec::new(),
        })
    }

    /// Serve on top of an existing engine (shared by a worker pool).
    pub fn with_engine(config: InitConfig, engine: Arc<Engine>) -> Result<Self> {
        config.validate()?;
        Ok(K04DEngine {
            config,
            engine,
            bundles: Vec::new(),
        })
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    /// Register a contract from its bundle. The bundle is recompiled and refused unless it
    /// gives the same compilation hash.
    pub fn inject_contract_bundle(&mut self, bundle: ContractBundleHandle) -> Result<()> {
        if bundle.tenant_id != self.config.tenant_id {
            return Err(PeqlError::Invalid(format!(
                "bundle for tenant `{}` injected into the engine of `{}`",
                bundle.tenant_id, self.config.tenant_id
            )));
        }
        let reg = self.engine.register_bundle(&bundle.bundle()?)?;
        if reg.name() != bundle.contract_id {
            return Err(PeqlError::Invalid(format!(
                "handle names `{}` but the bundle is `{}`",
                bundle.contract_id,
                reg.name()
            )));
        }
        tracing::debug!(tenant = %self.config.tenant_id, contract = %bundle.contract_id, "contract bundle injected");
        self.bundles.push(bundle);
        Ok(())
    }

    pub fn contract_bundles(&self) -> &[ContractBundleHandle] {
        &self.bundles
    }

    /// Run SQL for a caller. Every table is a contract registered from a bundle.
    pub async fn query(&self, sql: &str, caller: &Caller) -> Result<QueryResult> {
        if self.bundles.is_empty() {
            return Err(PeqlError::Invalid(
                "no contract bundle: inject one before querying".into(),
            ));
        }
        let res = self.engine.query(sql, caller).await?;
        if res.envelope.rows > self.config.max_result_rows {
            return Err(PeqlError::Invalid(format!(
                "the result has {} rows, more than this engine's limit of {}",
                res.envelope.rows, self.config.max_result_rows
            )));
        }
        Ok(res)
    }

    /// Serve a registered contract from batches held in memory.
    pub async fn register_memory_table(
        &self,
        contract: &str,
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> Result<()> {
        let table = Arc::new(MemTable::try_new(schema, partitions)?);
        self.engine.bind_table(contract, table).await?;
        Ok(())
    }

    /// Serve a registered contract from a Parquet file or directory.
    pub async fn register_parquet_table(&self, contract: &str, path: &str) -> Result<()> {
        let reg = self.engine.get(contract)?;
        let table = crate::binding::listing_table(
            &reg.compilation.contract,
            std::path::Path::new(path),
            false,
        )?;
        self.engine.bind_table(contract, table).await?;
        Ok(())
    }

    /// Serve a registered contract from a Lance dataset read through storaged.
    #[cfg(all(unix, feature = "lance"))]
    pub async fn register_lance_table(
        &self,
        contract: &str,
        asset_id: &str,
        principal_jwt: &str,
    ) -> Result<()> {
        let provider = crate::lance_table::LanceTableProvider::open(
            asset_id,
            &self.config.tenant_id,
            principal_jwt,
            &self.config.storaged_socket,
        )
        .await
        .map_err(|e| PeqlError::Invalid(format!("lance table `{asset_id}`: {e}")))?;
        self.engine.bind_table(contract, Arc::new(provider)).await?;
        Ok(())
    }
}
