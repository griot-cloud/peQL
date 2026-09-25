//! The privacy budget ledger (peQL design 4.8): epsilon spent per caller per named budget.
//! `noise` shapes produce charges (parcel_runtime::shape); this is where they are paid.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::{PeqlError, Result};

/// Epsilon a caller may spend from a budget that has no limit of its own.
pub const DEFAULT_LIMIT: f64 = 10.0;

#[derive(Default, Debug, Serialize, Deserialize)]
struct Ledger {
    limits: BTreeMap<String, f64>,
    /// "budget\u{1f}caller" -> spent
    spent: BTreeMap<String, f64>,
}

/// In memory, or kept in a JSON file so spending survives restarts.
#[derive(Debug)]
pub struct BudgetStore {
    ledger: Mutex<Ledger>,
    file: Option<PathBuf>,
    default_limit: f64,
    /// Record spending but never refuse (for development and migration).
    permissive: bool,
}

impl Default for BudgetStore {
    fn default() -> Self {
        BudgetStore {
            ledger: Mutex::default(),
            file: None,
            default_limit: DEFAULT_LIMIT,
            permissive: false,
        }
    }
}

impl BudgetStore {
    pub fn in_memory() -> BudgetStore {
        BudgetStore::default()
    }

    /// Records spending, never refuses a query.
    pub fn permissive() -> BudgetStore {
        BudgetStore {
            permissive: true,
            ..BudgetStore::default()
        }
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<BudgetStore> {
        let path = path.into();
        let ledger = match std::fs::read_to_string(&path) {
            Ok(text) => serde_json::from_str(&text)
                .map_err(|e| PeqlError::Invalid(format!("{}: {e}", path.display())))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ledger::default(),
            Err(e) => return Err(e.into()),
        };
        Ok(BudgetStore {
            ledger: Mutex::new(ledger),
            file: Some(path),
            ..BudgetStore::default()
        })
    }

    pub fn set_limit(&self, budget: &str, epsilon: f64) -> Result<()> {
        let mut l = self.ledger.lock().expect("budget lock");
        l.limits.insert(budget.to_owned(), epsilon);
        self.save(&l)
    }

    pub fn limit(&self, budget: &str) -> f64 {
        let l = self.ledger.lock().expect("budget lock");
        l.limits.get(budget).copied().unwrap_or(self.default_limit)
    }

    /// Charge every budget a query spends, all or none. Returns what is left of each.
    pub fn charge_all(
        &self,
        caller: &str,
        charges: &[(String, f64)],
    ) -> Result<BTreeMap<String, f64>> {
        let mut l = self.ledger.lock().expect("budget lock");
        let key = |b: &str| format!("{b}\u{1f}{caller}");
        for (budget, epsilon) in charges {
            let limit = l.limits.get(budget).copied().unwrap_or(self.default_limit);
            let spent = l.spent.get(&key(budget)).copied().unwrap_or(0.0);
            if !self.permissive && spent + epsilon > limit + 1e-12 {
                return Err(PeqlError::BudgetExhausted {
                    budget: budget.clone(),
                });
            }
        }
        let mut left = BTreeMap::new();
        for (budget, epsilon) in charges {
            let limit = l.limits.get(budget).copied().unwrap_or(self.default_limit);
            let s = l.spent.entry(key(budget)).or_insert(0.0);
            *s += epsilon;
            left.insert(budget.clone(), (limit - *s).max(0.0));
        }
        self.save(&l)?;
        Ok(left)
    }

    fn save(&self, l: &Ledger) -> Result<()> {
        if let Some(path) = &self.file {
            let tmp = path.with_extension("tmp");
            std::fs::write(
                &tmp,
                serde_json::to_string_pretty(l).map_err(|e| PeqlError::Invalid(e.to_string()))?,
            )?;
            std::fs::rename(tmp, path)?;
        }
        Ok(())
    }
}
