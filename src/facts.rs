//! What a write knows about the rows it wrote, and what the data it wrote to is known to be
//! after it, computed from what was known before and the write's own facts: a write never
//! reads data it did not write.
//!
//! A write validates its own rows under the contract's validation plan. The row rules
//! (`assert`) decide the write's verdict; the dataset rules (`guarantee`) are about the whole
//! data, so they are decided over the data's statistics, which a write combines with its own:
//! row counts, failure counts, null counts and rates, pass rates, minima and maxima. A
//! statistic that does not combine (a distinct count, a `dataset.other` aggregate) is not known
//! after an append; a guarantee that reads one is undecided until the whole data is validated
//! again ([`crate::Engine::validate`]).

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use parcel_core::Compilation;
use parcel_core::compile::GuaranteeRule;
use parcel_core::document::{AssertOnFail, GuaranteeOnFail};
use parcel_core::ir::{ColumnStat, DatasetField};
use parcel_core::types::Type;
use parcel_runtime::plan::{dataset_value, parse_decimal};
use parcel_runtime::reference::{self, Scope};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::error::{PeqlError, Result};
use crate::manifest::{self, Manifest};

/// What a write did to the data's files.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Written {
    /// Added its files to the data's.
    Append,
    /// Replaced the data's files with its own.
    Overwrite,
    /// Replaced some of the data's files with its own, which hold the same rows (a compaction).
    Rewrite,
}

/// The facts of one write under one contract.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WriteFacts {
    pub written: Written,
    /// The rows written, as a manifest of their own: the verdict of the row rules over them,
    /// their statistics, and the files the write added.
    pub facts: Manifest,
    /// The files a rewrite replaced, by path; empty for any other write.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<String>,
}

/// The verdict of the row rules over a write's rows, from the validation plan's verdict over
/// them: valid when no deny-level assert fails. Guarantees are left out: they are about the
/// whole data, not about the rows one write brings.
pub fn of_rows(
    c: &Compilation,
    mut verdict: parcel_runtime::plan::Verdict,
) -> parcel_runtime::plan::Verdict {
    verdict.breached = failed_asserts(c, &verdict.failures);
    verdict.valid = verdict.breached.is_empty();
    verdict.guarantees.clear();
    verdict
}

/// Deny-level asserts with failing rows, in the contract's order.
fn failed_asserts(c: &Compilation, failures: &BTreeMap<String, i64>) -> Vec<String> {
    c.contract
        .flags
        .iter()
        .filter(|f| f.on_fail == AssertOnFail::Deny)
        .filter(|f| failures.get(&f.assert_id).copied().unwrap_or(0) > 0)
        .map(|f| f.assert_id.clone())
        .collect()
}

/// Whether `stats` lacks a statistic guarantee `g` reads.
pub fn undecided(c: &Compilation, g: &GuaranteeRule, stats: &BTreeMap<String, Value>) -> bool {
    let computed: BTreeSet<&str> = c
        .validation
        .stats
        .iter()
        .map(|s| s.column.as_str())
        .collect();
    g.reads
        .iter()
        .any(|r| computed.contains(r.as_str()) && !stats.contains_key(r))
}

/// The verdict over data with these failure counts and statistics: every deny-level assert
/// without failing rows, and every deny-level guarantee decided by the data alone holding.
/// A guarantee whose statistics are not known is not decided here; the resolver refuses it.
fn decide(
    c: &Compilation,
    failures: &BTreeMap<String, i64>,
    stats: &BTreeMap<String, Value>,
    written_at: DateTime<Utc>,
) -> Result<Vec<String>> {
    let cc = &c.contract;
    let mut breached = failed_asserts(c, failures);
    let ctx = reference::context(&Scope {
        dataset: Some(dataset_value(c, stats, written_at, &cc.contract_hash)),
        pins: cc.functions.clone(),
        ..Scope::default()
    });
    for g in &cc.guarantees {
        if g.on_fail != GuaranteeOnFail::Deny
            || !c.validation.data_guarantees.contains(&g.id)
            || undecided(c, g, stats)
        {
            continue;
        }
        if !reference::eval_bool(&g.cel, &ctx).map_err(PeqlError::Invalid)? {
            breached.push(g.id.clone());
        }
    }
    Ok(breached)
}

/// What a contract knows about its data after a write, from what it knew before (`before`,
/// `None` for data with no rows) and the write's facts, at `written_at` and, for a table
/// with snapshots, the snapshot the write committed. `Ok(None)` when `before` or the write's
/// facts were computed under another compilation of the contract (another engine wrote under
/// another form of it): then nothing is known until the data is validated under this one.
pub fn after_write(
    c: &Compilation,
    before: Option<&Manifest>,
    write: &WriteFacts,
    written_at: DateTime<Utc>,
    snapshot_id: Option<i64>,
) -> Result<Option<Manifest>> {
    let cc = &c.contract;
    let w = &write.facts;
    if w.compilation_hash != cc.compilation_hash {
        return Ok(None);
    }
    let before = match (write.written, before) {
        (Written::Overwrite, _) | (Written::Append, None) => None,
        (Written::Rewrite, None) => {
            return Err(PeqlError::Invalid(format!(
                "a rewrite under `{}` replaced files of data that had none",
                cc.name
            )));
        }
        (_, Some(b)) if b.compilation_hash != cc.compilation_hash => return Ok(None),
        (_, b) => b,
    };
    let mut m = match before {
        None => Manifest {
            written_at,
            ..w.clone()
        },
        Some(b) if write.written == Written::Rewrite => {
            let removed: BTreeSet<&str> = write.removed.iter().map(String::as_str).collect();
            let gone: Vec<_> = b
                .files
                .iter()
                .filter(|f| removed.contains(f.path.as_str()))
                .collect();
            if gone.len() != removed.len() {
                return Err(PeqlError::Invalid(format!(
                    "a rewrite under `{}` replaced files its facts do not list",
                    cc.name
                )));
            }
            let rows: i64 = gone.iter().map(|f| f.rows).sum();
            if rows != w.row_count {
                return Err(PeqlError::Invalid(format!(
                    "a rewrite under `{}` replaced files of {rows} rows with {} rows",
                    cc.name, w.row_count
                )));
            }
            let mut files: Vec<_> = b
                .files
                .iter()
                .filter(|f| !removed.contains(f.path.as_str()))
                .cloned()
                .collect();
            files.extend(w.files.iter().cloned());
            Manifest { files, ..b.clone() }
        }
        Some(b) => {
            let row_count = b.row_count + w.row_count;
            let failures: BTreeMap<String, i64> = c
                .validation
                .asserts
                .iter()
                .map(|a| {
                    let n = |m: &Manifest| m.failures.get(a).copied().unwrap_or(0);
                    (a.clone(), n(b) + n(w))
                })
                .collect();
            let stats = combined_stats(c, b, w, row_count, &failures);
            let mut files = b.files.clone();
            files.extend(w.files.iter().cloned());
            Manifest {
                written_at,
                row_count,
                failures,
                stats,
                files,
                ..w.clone()
            }
        }
    };
    m.files.sort_by(|a, b| a.path.cmp(&b.path));
    m.data_hash = manifest::data_hash(&m.files);
    m.snapshot_id = snapshot_id;
    m.breached = decide(c, &m.failures, &m.stats, m.written_at)?;
    m.valid = m.breached.is_empty();
    Ok(Some(m))
}

/// The statistics of two sets of rows taken together, for every statistic that combines.
fn combined_stats(
    c: &Compilation,
    a: &Manifest,
    b: &Manifest,
    rows: i64,
    failures: &BTreeMap<String, i64>,
) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    for s in &c.validation.stats {
        let both = || Some((a.stats.get(&s.column)?, b.stats.get(&s.column)?));
        let v = match &s.field {
            DatasetField::RowCount => Some(json!(rows)),
            DatasetField::AssertionPassRate { assertion } => {
                let failed = failures.get(assertion).copied().unwrap_or(0);
                Some(json!(if rows > 0 {
                    1.0 - failed as f64 / rows as f64
                } else {
                    1.0
                }))
            }
            DatasetField::Column {
                stat: ColumnStat::NullCount,
                ..
            } => both().and_then(|(x, y)| Some(json!(x.as_i64()? + y.as_i64()?))),
            DatasetField::Column {
                stat: ColumnStat::NullRate,
                ..
            } => both().and_then(|(x, y)| {
                let nulls = |rate: &Value, rows: i64| -> Option<i64> {
                    Some((rate.as_f64()? * rows as f64).round() as i64)
                };
                let n = nulls(x, a.row_count)? + nulls(y, b.row_count)?;
                Some(json!(if rows > 0 {
                    n as f64 / rows as f64
                } else {
                    0.0
                }))
            }),
            DatasetField::Column {
                stat: ColumnStat::Min,
                ..
            } => both().and_then(|(x, y)| extreme(&s.ty, x, y, Ordering::Less)),
            DatasetField::Column {
                stat: ColumnStat::Max,
                ..
            } => both().and_then(|(x, y)| extreme(&s.ty, x, y, Ordering::Greater)),
            DatasetField::Column {
                stat: ColumnStat::DistinctCount,
                ..
            }
            | DatasetField::Other(_)
            | DatasetField::WrittenAt
            | DatasetField::ContractHash => None,
        };
        if let Some(v) = v {
            out.insert(s.column.clone(), v);
        }
    }
    out
}

/// The least (`Less`) or greatest (`Greater`) of two values of a statistic of type `ty`; a
/// null is the statistic of rows with no value, so the other one stands.
fn extreme(ty: &Type, x: &Value, y: &Value, want: Ordering) -> Option<Value> {
    if x.is_null() {
        return Some(y.clone());
    }
    if y.is_null() {
        return Some(x.clone());
    }
    let ord = match ty {
        Type::Int => x.as_i64()?.cmp(&y.as_i64()?),
        Type::Uint => x.as_u64()?.cmp(&y.as_u64()?),
        Type::Double => x.as_f64()?.partial_cmp(&y.as_f64()?)?,
        Type::Decimal(scale) => {
            parse_decimal(x.as_str()?, *scale)?.cmp(&parse_decimal(y.as_str()?, *scale)?)
        }
        Type::String => x.as_str()?.cmp(y.as_str()?),
        Type::Bool => x.as_bool()?.cmp(&y.as_bool()?),
        Type::Timestamp => {
            let t = |v: &Value| DateTime::parse_from_rfc3339(v.as_str()?).ok();
            t(x)?.cmp(&t(y)?)
        }
        _ => return None,
    };
    Some(if ord == want { x } else { y }.clone())
}
