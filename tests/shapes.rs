//! Shape operators: noise with privacy budgets, and deterministic sampling. Every test runs
//! through `Engine::query` and through `Engine::plan`.

mod paths;

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use paths::{Answer, BOTH, answer};
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

fn f64_at(r: &Answer, col: usize) -> Vec<f64> {
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
    for path in BOTH {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path()).await;
        engine.budgets().set_limit("hr_budget", 1.0).unwrap();
        let sql =
            r#"SELECT dept, SUM(salary) AS total FROM "hr/salaries" GROUP BY dept ORDER BY dept"#;

        let exact = answer(&engine, sql, &Caller::new("o", "acme", "analytics"), path)
            .await
            .unwrap();
        let exact = f64_at(&exact, 1);

        let globex = Caller::new("w", "globex", "analytics");
        let noisy = answer(&engine, sql, &globex, path).await.unwrap();
        assert_eq!(noisy.budgets["hr_budget"], 0.5, "{path:?}");
        assert_eq!(noisy.charges["hr_budget"], 0.5, "{path:?}");
        let noisy = f64_at(&noisy, 1);
        let scale = 1000.0 / 0.5;
        assert_ne!(exact, noisy, "noise was applied ({path:?})");
        for (e, n) in exact.iter().zip(&noisy) {
            assert!(
                (e - n).abs() < 40.0 * scale,
                "noise within Laplace tail bounds: {e} vs {n} ({path:?})"
            );
        }

        // Second query spends the rest; the third is refused.
        answer(&engine, sql, &globex, path).await.unwrap();
        let refused = answer(&engine, sql, &globex, path).await;
        assert!(
            matches!(refused, Err(EngineError::BudgetExhausted { .. })),
            "{path:?}: {:?}",
            refused.err().map(|e| e.to_string())
        );
        // Budgets are per caller.
        assert!(
            answer(&engine, sql, &Caller::new("k", "globex", "analytics"), path)
                .await
                .is_ok()
        );

        // Aggregates that do not read the column spend nothing and are exact.
        let counts = answer(
            &engine,
            r#"SELECT dept, COUNT(*) FROM "hr/salaries" GROUP BY dept"#,
            &Caller::new("z", "globex", "analytics"),
            path,
        )
        .await
        .unwrap();
        assert!(counts.budgets.is_empty(), "{path:?}");
        assert_eq!(f64_at(&counts, 1).iter().sum::<f64>(), N as f64);
    }
}

#[tokio::test]
async fn noised_columns_only_leave_through_aggregates() {
    for path in BOTH {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine(dir.path()).await;
        let globex = Caller::new("w", "globex", "analytics");
        let raw = answer(
            &engine,
            r#"SELECT salary FROM "hr/salaries" LIMIT 3"#,
            &globex,
            path,
        )
        .await;
        assert!(raw.is_err(), "{path:?}");
        let keyed = answer(
            &engine,
            r#"SELECT salary, COUNT(*) FROM "hr/salaries" GROUP BY salary"#,
            &globex,
            path,
        )
        .await;
        assert!(keyed.is_err(), "{path:?}");
        // The whole contract as a view reads the noised column other than through an
        // aggregate, so it is refused as the same `SELECT *` is.
        assert!(engine.view("hr/salaries", &globex).await.is_err());
        // Rows without the noised column are fine.
        let ok = answer(
            &engine,
            r#"SELECT employee_id, dept FROM "hr/salaries" LIMIT 3"#,
            &globex,
            path,
        )
        .await
        .unwrap();
        assert_eq!(ok.rows, 3, "{path:?}");
    }
}

#[tokio::test]
async fn sampling_is_deterministic_and_proportional() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path()).await;
    let globex = Caller::new("w", "globex", "analytics");
    let sql = r#"SELECT employee_id FROM "hr/preview" ORDER BY employee_id"#;
    let ids = |r: &Answer| f64_at(r, 0);
    let a = answer(&engine, sql, &globex, paths::Path::Query)
        .await
        .unwrap();
    for path in BOTH {
        let b = answer(&engine, sql, &globex, path).await.unwrap();
        assert_eq!(
            ids(&a),
            ids(&b),
            "the same caller sees the same sample ({path:?})"
        );
    }
    // The contract as a view is the same sample.
    let view = paths::run(engine.view("hr/preview", &globex).await.unwrap()).await;
    let mut viewed = f64_at(&view, 0);
    viewed.sort_by(f64::total_cmp);
    assert_eq!(ids(&a), viewed);
    let n = a.rows as f64;
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
    for path in BOTH {
        let full = answer(&engine, sql, &Caller::new("o", "acme", "analytics"), path)
            .await
            .unwrap();
        assert_eq!(full.rows, N as usize, "acme is exempt from sampling");
    }
}
