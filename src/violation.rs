//! What happens to rows that break a contract's row rules.
//!
//! A row breaks its contract when it fails an `assert` whose `on_fail` is `drop` or `deny`;
//! an assert with `on_fail: report` only counts its failures and is never broken. Rules about
//! the whole data (`guarantee`s) are not row rules: [`crate::Engine::validate`] decides them.
//!
//! A read never returns a row that breaks its contract. By default
//! ([`OnReadViolation::Exclude`]) the contract's view leaves those rows out and the answer
//! says, per rule, how many it left out ([`crate::Resolution::excluded`]); with
//! [`OnReadViolation::Refuse`] a read of data that holds any such row is refused, naming each
//! rule and its count. A contract's context terms (`decide`) refuse a read whatever its mode.
//!
//! A write does with its own rows what the writer asks ([`OnWriteViolation`]): by default it
//! refuses and commits nothing; it may instead commit only the rows that pass and hand the
//! others back with the rules each broke, or commit every row and mark the write invalid.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, ListBuilder, RecordBatch, StringBuilder};
use datafusion::arrow::compute::kernels::boolean::{and, not};
use datafusion::arrow::compute::{filter_record_batch, prep_null_mask_filter};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parcel_core::compile::Flag;
use parcel_core::document::AssertOnFail;
use serde::{Deserialize, Serialize};

use crate::error::{PeqlError, Result};

/// The column of the rows a write sets aside that names the rules each row broke, in the
/// contract's order.
pub const VIOLATIONS_COLUMN: &str = "_violations";

/// What a write does when rows it is given break the contract's row rules.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnWriteViolation {
    /// Nothing is committed; the refusal names each rule broken and how many rows broke it.
    #[default]
    Refuse,
    /// Only the rows that pass are committed; the others are handed back to the writer with
    /// the rules each broke ([`VIOLATIONS_COLUMN`]), and the report counts them per rule.
    Exclude,
    /// Every row is committed; the write's verdict marks it invalid.
    Warn,
}

/// What a read does when the data it reads holds rows that break the contract's row rules.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnReadViolation {
    /// The view leaves the rows out; the answer counts them per rule.
    #[default]
    Exclude,
    /// The read is refused, naming each rule broken and how many rows broke it.
    Refuse,
}

impl FromStr for OnWriteViolation {
    type Err = PeqlError;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "refuse" => Ok(OnWriteViolation::Refuse),
            "exclude" => Ok(OnWriteViolation::Exclude),
            "warn" => Ok(OnWriteViolation::Warn),
            other => Err(PeqlError::Invalid(format!(
                "`{other}` is not a write's on_violation: refuse, exclude or warn"
            ))),
        }
    }
}

impl FromStr for OnReadViolation {
    type Err = PeqlError;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "exclude" => Ok(OnReadViolation::Exclude),
            "refuse" => Ok(OnReadViolation::Refuse),
            other => Err(PeqlError::Invalid(format!(
                "`{other}` is not a read's on_violation: exclude or refuse"
            ))),
        }
    }
}

/// Whether a row can break an assert that fails with `on_fail`: `drop` and `deny` are broken
/// by a failing row; `report` only counts it.
pub fn enforced(on_fail: AssertOnFail) -> bool {
    matches!(on_fail, AssertOnFail::Drop | AssertOnFail::Deny)
}

/// The flags of the asserts a row can break, in the contract's order.
pub fn enforced_flags(flags: &[Flag]) -> Vec<&Flag> {
    flags.iter().filter(|f| enforced(f.on_fail)).collect()
}

/// Rows breaking each rule, from per-assert failure counts: the asserts a row can break that
/// any row fails, in the contract's order.
pub fn broken(flags: &[Flag], failures: &BTreeMap<String, i64>) -> Vec<(String, u64)> {
    enforced_flags(flags)
        .into_iter()
        .filter_map(|f| {
            let n = failures.get(&f.assert_id).copied().unwrap_or(0);
            (n > 0).then(|| (f.assert_id.clone(), n as u64))
        })
        .collect()
}

/// `rule (n rows), …` for a refusal.
pub fn describe(violations: &[(String, u64)]) -> String {
    violations
        .iter()
        .map(|(rule, n)| format!("{rule} ({n} row{})", if *n == 1 { "" } else { "s" }))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Rows split by the contract's row rules.
pub struct Split {
    /// The rows that break no rule, with every column they came with.
    pub passing: Vec<RecordBatch>,
    pub passing_rows: usize,
    /// The rows that break a rule: the row schema's columns and [`VIOLATIONS_COLUMN`].
    pub breaking: Option<RecordBatch>,
    /// Rows breaking each rule; a row that breaks two rules is counted under both.
    pub counts: BTreeMap<String, u64>,
}

/// Split rows that carry the flag column of every rule in `flags` (true when the row passes)
/// into the rows that pass every rule and the rows that break one, the latter as `row_schema`
/// with the rules each broke.
pub fn split(batches: &[RecordBatch], flags: &[&Flag], row_schema: &SchemaRef) -> Result<Split> {
    if row_schema.field_with_name(VIOLATIONS_COLUMN).is_ok() {
        return Err(PeqlError::Invalid(format!(
            "the contract has a column `{VIOLATIONS_COLUMN}`, which names the rules a set-aside \
             row broke"
        )));
    }
    let item = Arc::new(Field::new("item", DataType::Utf8, false));
    let mut fields: Vec<Field> = row_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(
        VIOLATIONS_COLUMN,
        DataType::List(item.clone()),
        false,
    ));
    let breaking_schema = Arc::new(Schema::new(fields));
    let mut out = Split {
        passing: Vec::with_capacity(batches.len()),
        passing_rows: 0,
        breaking: None,
        counts: BTreeMap::new(),
    };
    let mut breaking = Vec::new();
    for batch in batches {
        let mut passes: Vec<BooleanArray> = Vec::with_capacity(flags.len());
        for f in flags {
            let column = batch.column_by_name(&f.column).ok_or_else(|| {
                PeqlError::Invalid(format!("the written rows have no flag `{}`", f.column))
            })?;
            let column = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    PeqlError::Invalid(format!("the flag `{}` is not boolean", f.column))
                })?;
            // A flag is never null; were one, the row would not pass.
            let pass = match column.nulls() {
                Some(_) => prep_null_mask_filter(column),
                None => column.clone(),
            };
            let failed = pass.len() - pass.true_count();
            if failed > 0 {
                *out.counts.entry(f.assert_id.clone()).or_default() += failed as u64;
            }
            passes.push(pass);
        }
        let mut all = BooleanArray::from(vec![true; batch.num_rows()]);
        for p in &passes {
            all = and(&all, p)?;
        }
        let kept = filter_record_batch(batch, &all)?;
        out.passing_rows += kept.num_rows();
        out.passing.push(kept);
        if all.true_count() == batch.num_rows() {
            continue;
        }
        let failing = not(&all)?;
        let mut columns = Vec::with_capacity(breaking_schema.fields().len());
        for f in row_schema.fields() {
            let column = batch.column_by_name(f.name()).ok_or_else(|| {
                PeqlError::Invalid(format!("the written rows have no column `{}`", f.name()))
            })?;
            columns.push(datafusion::arrow::compute::filter(column, &failing)?);
        }
        let mut reasons = ListBuilder::new(StringBuilder::new()).with_field(item.clone());
        for row in 0..batch.num_rows() {
            if !failing.value(row) {
                continue;
            }
            for (f, pass) in flags.iter().zip(&passes) {
                if !pass.value(row) {
                    reasons.values().append_value(&f.assert_id);
                }
            }
            reasons.append(true);
        }
        columns.push(Arc::new(reasons.finish()));
        breaking.push(RecordBatch::try_new(breaking_schema.clone(), columns)?);
    }
    if !breaking.is_empty() {
        out.breaking = Some(datafusion::arrow::compute::concat_batches(
            &breaking_schema,
            &breaking,
        )?);
    }
    Ok(out)
}
