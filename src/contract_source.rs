//! Contract sources: turning a contract + a caller into a [`ResolvedPolicy`].
//!
//! [`JsonContractSource`] is the open-source path — it reads a simple JSON
//! contract format (see `docs/CONTRACT-FORMAT.md`) and evaluates a small set of
//! per-caller rules. The platform path ([`crate::platform`], feature
//! `platform`) consumes T03's signed bundle instead. Both produce the same
//! [`ResolvedPolicy`], so the engine is identical underneath.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use serde::Deserialize;

use crate::binding::{load_parquet_as_provider, BindingError, BindingResolver, DatasetRef};
use crate::policy::{Decision, DpParam, MaskAction, PolicyError, ResolvedPolicy};

/// The identity and intent of whoever is running a query.
///
/// The contract source uses this to decide visibility — e.g. a caller from the
/// owning tenant may see raw data while everyone else sees masked data.
#[derive(Debug, Clone)]
pub struct Caller {
    /// Stable principal id (e.g. `"user:alice"` or `"service:k03"`).
    pub id: String,
    /// The declared purpose of this query (must satisfy the contract).
    pub purpose: String,
    /// The caller's tenant.
    pub tenant: String,
    /// Authorisation tier (e.g. `bronze` / `silver` / `gold`).
    pub tier: String,
    /// Data classification the caller is cleared for.
    pub classification: String,
}

impl Caller {
    /// Convenience constructor for the common case (id + purpose + tenant).
    pub fn new(
        id: impl Into<String>,
        purpose: impl Into<String>,
        tenant: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            purpose: purpose.into(),
            tenant: tenant.into(),
            tier: "bronze".to_string(),
            classification: "internal".to_string(),
        }
    }
}

/// Resolves a contract for a caller into an engine-ready [`ResolvedPolicy`].
#[async_trait]
pub trait ContractSource: Send + Sync {
    /// Evaluate the contract governing `dataset` against `caller`.
    async fn resolve(
        &self,
        dataset: &DatasetRef,
        caller: &Caller,
    ) -> Result<ResolvedPolicy, ContractError>;
}

// ─── JSON contract format (open-source) ───────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
struct JsonContract {
    contract_id: String,
    version: String,
    dataset: String,
    binding: JsonBinding,
    owner_tenant: String,
    #[serde(default)]
    purposes: Vec<String>,
    #[serde(default)]
    columns: Vec<JsonColumn>,
    #[serde(default)]
    row_filter: Option<String>,
    #[serde(default)]
    dp_columns: HashMap<String, DpParam>,
    /// Flat column→mask map (graph-style authoring); merged with
    /// `columns[].mask`. Either spelling works for either dataset shape.
    #[serde(default)]
    masks: HashMap<String, String>,
    /// Graph alias for `row_filter`: a predicate over NODE columns. For graph
    /// datasets a filtered node is a WALL (absent and non-traversable).
    #[serde(default)]
    node_filter: Option<String>,
    /// Graph-only: a predicate over EDGE columns hiding relationships even
    /// between visible nodes (e.g. `edge_type != 'depends_on'`).
    #[serde(default)]
    edge_filter: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct JsonBinding {
    /// Path to a local Parquet file holding this dataset's rows (tabular).
    #[serde(default)]
    parquet: Option<String>,
    /// Path to a local graph snapshot bundle DIRECTORY (nodes/edges Parquet +
    /// manifest.json) for graph datasets. A `file://` prefix is tolerated.
    #[serde(default)]
    graph_snapshot: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct JsonColumn {
    name: String,
    #[serde(default)]
    mask: Option<String>,
}

/// An open-source contract source backed by JSON contract documents.
///
/// Contracts are keyed by their `dataset` field; that is the name a query
/// targets (`SELECT * FROM "<dataset>"`).
pub struct JsonContractSource {
    contracts: HashMap<String, JsonContract>,
}

impl JsonContractSource {
    /// Build from in-memory JSON contract strings.
    pub fn from_json_strs<I, S>(docs: I) -> Result<Self, ContractError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut contracts = HashMap::new();
        for doc in docs {
            let c: JsonContract = serde_json::from_str(doc.as_ref())
                .map_err(|e| ContractError::Parse(e.to_string()))?;
            contracts.insert(c.dataset.clone(), c);
        }
        Ok(Self { contracts })
    }

    /// Load every `*.json` contract under `dir`.
    pub fn from_dir(dir: impl AsRef<Path>) -> Result<Self, ContractError> {
        let dir = dir.as_ref();
        let mut docs = Vec::new();
        let entries = std::fs::read_dir(dir)
            .map_err(|e| ContractError::Parse(format!("read_dir {dir:?}: {e}")))?;
        for entry in entries {
            let path = entry
                .map_err(|e| ContractError::Parse(e.to_string()))?
                .path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let text = std::fs::read_to_string(&path)
                    .map_err(|e| ContractError::Parse(format!("read {path:?}: {e}")))?;
                docs.push(text);
            }
        }
        Self::from_json_strs(docs)
    }

    fn get(&self, dataset: &DatasetRef) -> Result<&JsonContract, ContractError> {
        self.contracts
            .get(dataset.as_str())
            .ok_or_else(|| ContractError::NotFound(dataset.to_string()))
    }
}

#[async_trait]
impl ContractSource for JsonContractSource {
    async fn resolve(
        &self,
        dataset: &DatasetRef,
        caller: &Caller,
    ) -> Result<ResolvedPolicy, ContractError> {
        let contract = self.get(dataset)?;

        // Purpose gate: the caller's declared purpose must be allowed.
        if !contract.purposes.is_empty() && !contract.purposes.contains(&caller.purpose) {
            return Ok(ResolvedPolicy {
                contract_id: contract.contract_id.clone(),
                contract_version: contract.version.clone(),
                tenant_id: contract.owner_tenant.clone(),
                decision: Decision::Deny {
                    reason: format!(
                        "purpose '{}' is not permitted by contract '{}' (allowed: {:?})",
                        caller.purpose, contract.contract_id, contract.purposes
                    ),
                },
                column_masks: HashMap::new(),
                row_filter: None,
                dp_columns: HashMap::new(),
                projection: None,
                graph_edge_filter: None,
            });
        }

        // `columns`, when declared, IS the contract's read projection — the set of
        // columns this contract EXPOSES, in declared order. A contract that lists a
        // subset therefore HIDES the rest (`SELECT *` returns only the declared
        // columns; anything else is unresolvable), exactly like a platform view.
        // Omitting `columns` entirely = expose every column of the bound table.
        let projection: Option<Vec<String>> = if contract.columns.is_empty() {
            None
        } else {
            Some(contract.columns.iter().map(|c| c.name.clone()).collect())
        };

        // The owning tenant sees raw VALUES (no masks / filter / DP) but still only
        // the columns the contract exposes — the projection is the contract's shape,
        // not a per-caller restriction.
        let is_owner = caller.tenant == contract.owner_tenant;
        if is_owner {
            let mut policy = ResolvedPolicy::allow_all(
                contract.contract_id.clone(),
                contract.version.clone(),
                contract.owner_tenant.clone(),
            );
            policy.projection = projection;
            return Ok(policy);
        }

        let mut column_masks = HashMap::new();
        for col in &contract.columns {
            if let Some(mask) = &col.mask {
                let action = MaskAction::parse(mask)?;
                if action != MaskAction::Noop {
                    column_masks.insert(col.name.clone(), action);
                }
            }
        }
        // The flat `masks` map (graph-style authoring) merges in on top.
        for (col, mask) in &contract.masks {
            let action = MaskAction::parse(mask)?;
            if action != MaskAction::Noop {
                column_masks.insert(col.clone(), action);
            }
        }

        Ok(ResolvedPolicy {
            contract_id: contract.contract_id.clone(),
            contract_version: contract.version.clone(),
            tenant_id: contract.owner_tenant.clone(),
            decision: Decision::Allow,
            column_masks,
            // `node_filter` is the graph spelling of `row_filter`; for graphs a
            // filtered node is a wall (docs/GRAPH-QUERY.md §3).
            row_filter: contract.row_filter.clone().or(contract.node_filter.clone()),
            dp_columns: contract.dp_columns.clone(),
            projection,
            graph_edge_filter: contract.edge_filter.clone(),
        })
    }
}

#[async_trait]
impl BindingResolver for JsonContractSource {
    async fn resolve(&self, dataset: &DatasetRef) -> Result<Arc<dyn TableProvider>, BindingError> {
        let contract = self
            .contracts
            .get(dataset.as_str())
            .ok_or_else(|| BindingError::NotFound(dataset.to_string()))?;
        let parquet = contract
            .binding
            .parquet
            .as_ref()
            .ok_or_else(|| BindingError::NotFound(format!(
                "{dataset} has no tabular binding (it is a graph dataset; use the graph_* functions)"
            )))?;
        load_parquet_as_provider(Path::new(parquet))
    }

    fn resolve_graph_dir(&self, dataset: &DatasetRef) -> Option<std::path::PathBuf> {
        let raw = self
            .contracts
            .get(dataset.as_str())?
            .binding
            .graph_snapshot
            .as_deref()?;
        Some(std::path::PathBuf::from(
            raw.strip_prefix("file://").unwrap_or(raw),
        ))
    }
}

/// Errors resolving a contract.
#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    /// No contract governs the requested dataset.
    #[error("no contract for dataset '{0}'")]
    NotFound(String),

    /// A contract document could not be parsed.
    #[error("contract parse error: {0}")]
    Parse(String),

    /// A masking token in the contract was invalid.
    #[error(transparent)]
    Policy(#[from] PolicyError),

    /// A platform (T03) bundle could not be fetched, verified, or mapped
    /// (feature `platform`).
    #[error("platform bundle error: {0}")]
    Platform(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract_json(columns: &str) -> String {
        format!(
            r#"{{
              "contract_id": "acme/orders",
              "version": "1",
              "dataset": "orders",
              "binding": {{ "parquet": "/tmp/orders.parquet" }},
              "owner_tenant": "acme",
              "columns": {columns}
            }}"#
        )
    }

    async fn policy_for(columns: &str, tenant: &str) -> ResolvedPolicy {
        let src = JsonContractSource::from_json_strs([contract_json(columns)]).unwrap();
        ContractSource::resolve(
            &src,
            &DatasetRef::new("orders"),
            &Caller::new("u", "analytics", tenant),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn declared_columns_are_the_read_projection() {
        // A contract declaring a SUBSET exposes exactly that subset — the
        // open-source equivalent of a platform view.
        let p = policy_for(
            r#"[{"name":"order_id"},{"name":"email","mask":"hash_sha256"}]"#,
            "globex",
        )
        .await;
        assert_eq!(
            p.projection.as_deref(),
            Some(["order_id".to_string(), "email".to_string()].as_slice())
        );
        assert_eq!(p.column_masks.get("email"), Some(&MaskAction::HashSha256));
    }

    #[tokio::test]
    async fn the_owner_sees_the_projection_too() {
        // Hiding a column is a property of the CONTRACT, not of who is asking:
        // the owner gets raw values but the same narrowed column set.
        let p = policy_for(r#"[{"name":"order_id"},{"name":"email"}]"#, "acme").await;
        assert_eq!(
            p.projection.as_deref(),
            Some(["order_id".to_string(), "email".to_string()].as_slice())
        );
        assert!(p.column_masks.is_empty(), "owner reads raw values");
    }

    #[tokio::test]
    async fn omitting_columns_exposes_everything() {
        // Back-compat: no `columns` block = no restriction.
        let p = policy_for("[]", "globex").await;
        assert_eq!(p.projection, None);
    }
}
