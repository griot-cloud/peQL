//! A small metered contract and its data, shared by the Flight, signer and object-store tests.
#![allow(dead_code)]

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use peql::Caller;

/// Guests see eastern readings only, with meters masked; negative readings are dropped.
pub const READINGS: &str = r#"
contract: demo/readings
version: 1
owner: demo
binding:
  parquet: readings/
  partitioned_by: [region]
expose:
  - {name: id, type: int64}
  - {name: region, type: utf8}
  - {name: meter, type: utf8}
  - {name: kwh, type: int64}
rules:
  - {id: analytics, op: decide, expr: "ctx.purpose == 'analytics'"}
  - {id: east_for_guests, op: admit, expr: "ctx.tenant == 'demo' || row.region == 'EA'"}
  - {id: non_negative, op: assert, expr: "row.kwh >= 0", on_fail: drop}
  - {id: mask_meter, op: transform, column: meter, expr: "ctx.tenant == 'demo' ? row.meter : redact(row.meter)"}
"#;

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("meter", DataType::Utf8, true),
        Field::new("kwh", DataType::Int64, true),
    ]))
}

/// Readings `from..=to`: even ids in the east, every tenth reading negative.
pub fn batch(from: i64, to: i64) -> RecordBatch {
    let ids: Vec<i64> = (from..=to).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|i| if i % 2 == 0 { "EA" } else { "WA" })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter().map(|i| format!("M{i:04}")).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                ids.iter()
                    .map(|i| if i % 10 == 0 { -1 } else { *i })
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

pub fn owner() -> Caller {
    Caller::new("ana", "demo", "analytics")
}

pub fn guest() -> Caller {
    Caller::new("gus", "partner", "analytics")
}

/// The `id` column of every batch, sorted.
pub fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let i = b.schema().index_of("id").unwrap();
            b.column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    out.sort();
    out
}

/// Every value of a string column.
pub fn strings(batches: &[RecordBatch], column: &str) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|b| {
            let i = b.schema().index_of(column).unwrap();
            let a = datafusion::arrow::compute::cast(b.column(i), &DataType::Utf8).unwrap();
            let a = a.as_any().downcast_ref::<StringArray>().unwrap().clone();
            (0..a.len())
                .map(|r| a.is_valid(r).then(|| a.value(r).to_owned()))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// What the owner sees of readings `1..=n`: all but the negative ones.
pub fn owner_ids(n: i64) -> Vec<i64> {
    (1..=n).filter(|i| i % 10 != 0).collect()
}

/// What a guest sees: eastern readings that are not negative.
pub fn guest_ids(n: i64) -> Vec<i64> {
    (1..=n).filter(|i| i % 2 == 0 && i % 10 != 0).collect()
}
