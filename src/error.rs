//! Everything a peQL call can refuse or fail with.

use parcel_core::Diagnostic;

#[derive(Debug, thiserror::Error)]
pub enum PeqlError {
    #[error("contract does not compile:\n{}", .0.iter().map(|d| format!("  {d}")).collect::<Vec<_>>().join("\n"))]
    Compile(Vec<Diagnostic>),
    /// Also what a caller sees for a contract that exists but is not published to them.
    #[error("no contract named `{0}`")]
    UnknownContract(String),
    #[error("denied by `{contract}` rule `{rule}`")]
    Denied { contract: String, rule: String },
    #[error("`{contract}` has no data yet; write to it first")]
    NotWritten { contract: String },
    #[error("`{contract}` is not servable: {}", .breached.join(", "))]
    NotServable {
        contract: String,
        breached: Vec<String>,
    },
    #[error("`{contract}` guarantee `{rule}` does not hold")]
    GuaranteeFailed { contract: String, rule: String },
    #[error("privacy budget `{budget}` is exhausted for this caller")]
    BudgetExhausted { budget: String },
    #[error("only queries are allowed: `{verb}` is refused")]
    Refused { verb: String },
    #[error("the plan for this query has no gate for `{0}`; it will not run")]
    Ungated(String),
    #[error("the envelope could not be signed: {0}")]
    Signing(String),
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl PeqlError {
    /// Whether the query was refused by policy rather than failing.
    pub fn is_refusal(&self) -> bool {
        matches!(
            self,
            PeqlError::Denied { .. }
                | PeqlError::NotServable { .. }
                | PeqlError::GuaranteeFailed { .. }
                | PeqlError::BudgetExhausted { .. }
                | PeqlError::Refused { .. }
                | PeqlError::UnknownContract(_)
        )
    }
}

pub type Result<T> = std::result::Result<T, PeqlError>;

impl From<String> for PeqlError {
    fn from(s: String) -> Self {
        PeqlError::Invalid(s)
    }
}

impl From<parcel_runtime::plan::RuntimeError> for PeqlError {
    fn from(e: parcel_runtime::plan::RuntimeError) -> Self {
        match e {
            parcel_runtime::plan::RuntimeError::DataFusion(d) => PeqlError::DataFusion(d),
            parcel_runtime::plan::RuntimeError::Invalid(s) => PeqlError::Invalid(s),
        }
    }
}
