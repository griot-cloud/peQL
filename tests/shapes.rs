//! Shape operators: noise with privacy budgets, and deterministic sampling.

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use peql::{Caller, Engine, PeqlError as EngineError, WriteMode};

const HR: &str = r#"
contract: hr/salaries
version: 1
binding: {parquet: salaries/}
expose:
  - {name: employee_id, type: int64}
  - {name: dept, type: utf8}
  - {name: salary, type: float64}
rules:
  - id: salary_dp
    op: shape
    operator: noise
    column: salary
    params: {sensitivity: 1000, epsilon: 0.5, budget: hr_budget}
    unless: ctx.tenant == 'acme'
"#;

const PREVIEW: &str = r#"
contract: hr/preview
version: 1
binding: {parquet: salaries/}
expose:
  - {name: employee_id, type: int64}
  - {name: dept, type: utf8}
rules:
  - id: preview
    op: shape
    operator: sample
    params: {fraction: 0.3, key: employee_id}
    unless: ctx.tenant == 'acme'
"#;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("employee_id", DataType::Int64, false),
        Field::new("dept", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
    ]))
}

const N: i64 = 2000;

fn batch() -> RecordBatch {
    let depts = ["eng", "ops", "sales", "finance"];
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from((0..N).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..N).map(|i| depts[(i % 4) as usize]).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..N)
                    .map(|i| 50_000.0 + (i % 97) as f64 * 1000.0)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn engine(dir: &std::path::Path) -> Engine {
    let e = Engine::in_memory(dir);
    e.register_contract(HR, &schema()).unwrap();
    e.register_contract(PREVIEW, &schema()).unwrap();
    e.write("hr/salaries", vec![batch()], WriteMode::Overwrite)
        .await
        .unwrap();
    e
}

fn f64_at(r: &peql::QueryResult, col: usize) -> Vec<f64> {
    let mut out = Vec::new();
    for b in &r.batches {
        let c = datafusion::arrow::compute::cast(b.column(col), &DataType::Float64).unwrap();
        let c = c.as_any().downcast_ref::<Float64Array>().unwrap();
        out.extend(c.iter().map(|v| v.unwrap()));
    }
    out
}

#[tokio::test]
async fn noise_perturbs_aggregates_and_spends_budget() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path()).await;
    engine.budgets().set_limit("hr_budget", 1.0).unwrap();
    let sql = r#"SELECT dept, SUM(salary) AS total FROM "hr/salaries" GROUP BY dept ORDER BY dept"#;

    let exact = engine
        .query(sql, &Caller::new("o", "acme", "analytics"))
        .await
        .unwrap();
    let exact = f64_at(&exact, 1);

    let globex = Caller::new("w", "globex", "analytics");
    let noisy = engine.query(sql, &globex).await.unwrap();
    assert_eq!(noisy.envelope.budgets["hr_budget"], 0.5);
    let noisy = f64_at(&noisy, 1);
    let scale = 1000.0 / 0.5;
    assert_ne!(exact, noisy, "noise was applied");
    for (e, n) in exact.iter().zip(&noisy) {
        assert!(
            (e - n).abs() < 40.0 * scale,
            "noise within Laplace tail bounds: {e} vs {n}"
        );
    }

    // Second query spends the rest; the third is refused.
    engine.query(sql, &globex).await.unwrap();
    let refused = engine.query(sql, &globex).await;
    assert!(
        matches!(refused, Err(EngineError::BudgetExhausted { .. })),
        "{:?}",
        refused.err()
    );
    // Budgets are per caller.
    assert!(
        engine
            .query(sql, &Caller::new("k", "globex", "analytics"))
            .await
            .is_ok()
    );

    // Aggregates that do not read the column spend nothing and are exact.
    let counts = engine
        .query(
            r#"SELECT dept, COUNT(*) FROM "hr/salaries" GROUP BY dept"#,
            &Caller::new("z", "globex", "analytics"),
        )
        .await
        .unwrap();
    assert!(counts.envelope.budgets.is_empty());
    assert_eq!(f64_at(&counts, 1).iter().sum::<f64>(), N as f64);
}

#[tokio::test]
async fn noised_columns_only_leave_through_aggregates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path()).await;
    let globex = Caller::new("w", "globex", "analytics");
    let raw = engine
        .query(r#"SELECT salary FROM "hr/salaries" LIMIT 3"#, &globex)
        .await;
    assert!(raw.is_err());
    let keyed = engine
        .query(
            r#"SELECT salary, COUNT(*) FROM "hr/salaries" GROUP BY salary"#,
            &globex,
        )
        .await;
    assert!(keyed.is_err());
    // Rows without the noised column are fine.
    let ok = engine
        .query(
            r#"SELECT employee_id, dept FROM "hr/salaries" LIMIT 3"#,
            &globex,
        )
        .await
        .unwrap();
    assert_eq!(ok.envelope.rows, 3);
}

#[tokio::test]
async fn sampling_is_deterministic_and_proportional() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path()).await;
    let globex = Caller::new("w", "globex", "analytics");
    let sql = r#"SELECT employee_id FROM "hr/preview" ORDER BY employee_id"#;
    let a = engine.query(sql, &globex).await.unwrap();
    let b = engine.query(sql, &globex).await.unwrap();
    let ids = |r: &peql::QueryResult| f64_at(r, 0);
    assert_eq!(ids(&a), ids(&b), "the same caller sees the same sample");
    let n = a.envelope.rows as f64;
    assert!(
        (n - 0.3 * N as f64).abs() < 90.0,
        "sample of {n} rows from {N} at 0.3"
    );
    // Two contracts over one copy of the data each keep their own manifest and verdict.
    let preview = engine.manifest("hr/preview").unwrap().unwrap();
    let salaries = engine.manifest("hr/salaries").unwrap().unwrap();
    assert_eq!(preview.contract, "hr/preview");
    assert_ne!(preview.contract_hash, salaries.contract_hash);
    assert_eq!(preview.data_hash, salaries.data_hash);
    let full = engine
        .query(sql, &Caller::new("o", "acme", "analytics"))
        .await
        .unwrap();
    assert_eq!(
        full.envelope.rows, N as usize,
        "acme is exempt from sampling"
    );
}
