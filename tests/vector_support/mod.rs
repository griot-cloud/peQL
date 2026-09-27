#![allow(dead_code)]
use datafusion::arrow::{
    array::{FixedSizeListArray, Int64Array, RecordBatch},
    datatypes::{DataType, Field, Float32Type, Schema, SchemaRef},
};
use parcel_core::registry::FunctionManifest;
use peql::{Caller, Engine, WriteMode, audit::MemoryAudit};
use std::sync::Arc;

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
        Field::new(
            "private_embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
    ]))
}
pub fn batch() -> RecordBatch {
    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        [
            Some(vec![Some(1.0), Some(0.0)]),
            Some(vec![Some(0.8), Some(0.2)]),
            Some(vec![Some(0.0), Some(1.0)]),
        ],
        2,
    );
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(vectors.clone()),
            Arc::new(vectors),
        ],
    )
    .unwrap()
}
pub fn owner() -> Caller {
    Caller::new("owner", "demo", "analytics")
}
pub fn guest() -> Caller {
    Caller::new("guest", "partner", "analytics")
}
pub fn sql(column: &str, query: &str, k: usize, metric: &str) -> String {
    format!(
        "SELECT * FROM vector_search('demo/vectors', '{column}', {query}, {k}, '{metric}') AS v"
    )
}
pub fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(b.schema().index_of("id").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}
fn identity_module() -> Vec<u8> {
    wat::parse_str(r#"(module
      (memory (export "memory") 1)
      (func (export "parcel_abi_version") (result i32) i32.const 1)
      (func (export "parcel_alloc") (param i32) (result i32) i32.const 8192)
      (func (export "parcel_free") (param i32 i32))
      (func (export "parcel_fn_embed_identity") (param $ptr i32) (param $len i32) (result i64)
        i32.const 4096 i32.const 0 i32.store8
        i32.const 4097 local.get $ptr i32.const 8 i32.add local.get $len i32.const 8 i32.sub memory.copy
        i64.const 4096 i64.const 32 i64.shl
        local.get $len i32.const 7 i32.sub i64.extend_i32_u i64.or))"#).unwrap()
}
pub async fn engine(dir: &std::path::Path, audit: Arc<MemoryAudit>, mask: bool) -> Engine {
    let engine = Engine::in_memory(dir).with_audit(audit);
    let manifest: FunctionManifest = yaml_serde::from_str("name: embed_identity\nversion: 1\nsignatures: ['(fixed_size_list<float32, 2>) -> fixed_size_list<float32, 2>']\n").unwrap();
    engine
        .register_function(&identity_module(), &manifest, "demo")
        .unwrap();
    let expression = if mask {
        "ctx.tenant == 'demo' || row.id != 2 ? embed_identity(row.embedding) : null"
    } else {
        "embed_identity(row.embedding)"
    };
    let contract = format!(
        "contract: demo/vectors\nversion: 1\nowner: demo\nbinding: {{parquet: vectors/}}\nexpose:\n  - {{name: id, type: int64}}\n  - {{name: embedding, type: 'fixed_size_list<float32, 2>'}}\nrules:\n  - {{id: purpose, op: decide, expr: \"ctx.purpose == 'analytics'\"}}\n  - {{id: visible, op: admit, expr: \"ctx.tenant == 'demo' || row.id != 1\"}}\n  - {{id: produced, op: transform, column: embedding, expr: \"{expression}\"}}\n"
    );
    engine.register_contract(&contract, &schema()).unwrap();
    engine.publish("demo/vectors", "partner").unwrap();
    engine
        .write("demo/vectors", vec![batch()], WriteMode::Overwrite)
        .await
        .unwrap();
    engine
}
