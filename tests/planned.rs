//! A planned query is the query: `Engine::view` and `Engine::plan` hand an executor the plan
//! `Engine::query` runs, with every shape in it and its budgets charged once, at planning.

mod paths;

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::physical_plan::execution_plan::reset_plan_states;
use paths::{Answer, BOTH, Path, answer, run};
use peql::audit::{MemoryAudit, Outcome};
use peql::{Caller, Engine, PeqlError, WriteMode};

/// Guests see every region but `XS`; the tenant `tiny` sees only `XS`, which has three rows.
const CELLS: &str = r#"
contract: demo/cells
version: 1
owner: demo
binding: {parquet: cells/, partitioned_by: [region]}
expose:
  - {name: id, type: int64}
  - {name: region, type: utf8}
  - {name: dept, type: utf8}
  - {name: amount, type: int64}
rules:
  - {id: regions, op: admit, expr: "ctx.tenant == 'demo' || (ctx.tenant == 'tiny' ? row.region == 'XS' : row.region != 'XS')"}
  - id: small_cells
    op: shape
    operator: suppress
    params: {k: 5}
    unless: ctx.tenant == 'demo'
"#;

/// Salaries noised row by row for everyone but the owner, paid from the budget `pay`.
const PAY: &str = r#"
contract: demo/pay
version: 1
owner: demo
binding: {parquet: pay/}
expose:
  - {name: id, type: int64}
  - {name: dept, type: utf8}
  - {name: salary, type: float64}
rules:
  - id: dp
    op: shape
    operator: noise
    column: salary
    params: {sensitivity: 100, epsilon: 1.0, budget: pay, at: row}
    unless: ctx.tenant == 'demo'
"#;

/// Salaries released only through noised aggregates, paid from the budget `totals`.
const TOTALS: &str = r#"
contract: demo/totals
version: 1
owner: demo
binding: {parquet: pay/}
expose:
  - {name: id, type: int64}
  - {name: dept, type: utf8}
  - {name: salary, type: float64}
rules:
  - id: dp_sum
    op: shape
    operator: noise
    column: salary
    params: {sensitivity: 100, epsilon: 0.5, budget: totals}
    unless: ctx.tenant == 'demo'
"#;

const N: i64 = 3000;
const DEPTS: [&str; 4] = ["eng", "ops", "sales", "hr"];

fn cells_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("dept", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
    ]))
}

/// Region of cell `i`: three rows in `XS`, the rest split between `EA` and `WA`.
fn region(i: i64) -> &'static str {
    if i < 3 {
        "XS"
    } else if i % 2 == 0 {
        "EA"
    } else {
        "WA"
    }
}

fn cells(from: i64, to: i64) -> RecordBatch {
    let ids: Vec<i64> = (from..to).collect();
    RecordBatch::try_new(
        cells_schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(
                ids.iter().map(|i| region(*i)).collect::<Vec<_>>(),
            )),
            // `hr` is rare: three rows, all in `WA`, a small cell.
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|i| {
                        if i % 1000 == 7 {
                            "hr"
                        } else {
                            DEPTS[(i % 3) as usize]
                        }
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                ids.iter().map(|i| i * 10).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn pay_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("dept", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
    ]))
}

fn pay() -> RecordBatch {
    let ids: Vec<i64> = (0..N).collect();
    RecordBatch::try_new(
        pay_schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|i| DEPTS[(i % 4) as usize])
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                ids.iter()
                    .map(|i| 40_000.0 + (i % 101) as f64 * 100.0)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn engine(dir: &std::path::Path, audit: Arc<MemoryAudit>) -> Engine {
    let e = Engine::in_memory(dir).with_audit(audit);
    e.register_contract(CELLS, &cells_schema()).unwrap();
    e.register_contract(PAY, &pay_schema()).unwrap();
    e.register_contract(TOTALS, &pay_schema()).unwrap();
    for c in ["demo/cells", "demo/pay", "demo/totals"] {
        e.publish(c, peql::store::PUBLIC).unwrap();
    }
    // Several parts, so the data is several files and the plan several partitions.
    let w = e
        .begin_write("demo/cells", WriteMode::Overwrite)
        .await
        .unwrap();
    for part in 0..4 {
        e.write_part(&w, vec![cells(part * N / 4, (part + 1) * N / 4)])
            .await
            .unwrap();
    }
    e.finish_write(w).await.unwrap();
    e.write("demo/pay", vec![pay()], WriteMode::Overwrite)
        .await
        .unwrap();
    e
}

fn owner() -> Caller {
    Caller::new("ana", "demo", "analytics")
}
fn guest() -> Caller {
    Caller::new("gus", "partner", "analytics")
}
fn tiny() -> Caller {
    Caller::new("tim", "tiny", "analytics")
}

fn int_column(batches: &[RecordBatch], name: &str) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let c = b.column(b.schema().index_of(name).unwrap()).clone();
            let c = datafusion::arrow::compute::cast(&c, &DataType::Int64).unwrap();
            c.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn f64_column(batches: &[RecordBatch], name: &str) -> Vec<f64> {
    batches
        .iter()
        .flat_map(|b| {
            let c = b.column(b.schema().index_of(name).unwrap()).clone();
            let c = datafusion::arrow::compute::cast(&c, &DataType::Float64).unwrap();
            c.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

/// `(id, rest of the row as text)`, sorted by id.
fn keyed_rows(batches: &[RecordBatch], skip: &[&str]) -> Vec<(i64, String)> {
    let mut out = Vec::new();
    for b in batches {
        let id = b.schema().index_of("id").unwrap();
        let ids = b.column(id).as_any().downcast_ref::<Int64Array>().unwrap();
        for r in 0..b.num_rows() {
            let rest: Vec<String> = b
                .schema()
                .fields()
                .iter()
                .enumerate()
                .filter(|(c, f)| *c != id && !skip.contains(&f.name().as_str()))
                .map(|(c, _)| {
                    datafusion::arrow::util::display::array_value_to_string(b.column(c), r).unwrap()
                })
                .collect();
            out.push((ids.value(r), rest.join("|")));
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn a_view_is_the_query_under_suppress() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), Arc::default()).await;
    let all = r#"SELECT * FROM "demo/cells""#;

    // A guest sees every row but XS's: many more than k, so nothing is withheld.
    let view = e.view("demo/cells", &guest()).await.unwrap();
    assert_eq!(view.suppress_k, Some(5));
    let plan = datafusion::physical_plan::displayable(view.plan.as_ref())
        .indent(true)
        .to_string();
    assert!(plan.contains("SuppressExec: k=5"), "{plan}");
    assert!(plan.contains("GateExec: contract=demo/cells"), "{plan}");
    let viewed = run(view).await;
    let queried = answer(&e, all, &guest(), Path::Query).await.unwrap();
    assert_eq!(viewed.rows as i64, N - 3);
    assert_eq!(
        keyed_rows(&viewed.batches, &[]),
        keyed_rows(&queried.batches, &[])
    );

    // `tiny` sees three rows, fewer than k: the whole result is withheld, by both.
    let viewed = run(e.view("demo/cells", &tiny()).await.unwrap()).await;
    let queried = answer(&e, all, &tiny(), Path::Query).await.unwrap();
    assert_eq!((viewed.rows, queried.rows), (0, 0));
    // ... and by a filtered query through either path, where a larger one is not.
    for path in BOTH {
        let few = answer(
            &e,
            r#"SELECT id FROM "demo/cells" WHERE id < 8"#,
            &guest(),
            path,
        )
        .await
        .unwrap();
        assert_eq!(few.rows, 5, "{path:?}: ids 3..8 are five rows, k");
        let fewer = answer(
            &e,
            r#"SELECT id FROM "demo/cells" WHERE id < 7"#,
            &guest(),
            path,
        )
        .await
        .unwrap();
        assert_eq!(fewer.rows, 0, "{path:?}: four rows are fewer than k");
    }

    // The owner is exempt: every row, and no suppression in the plan.
    let view = e.view("demo/cells", &owner()).await.unwrap();
    assert_eq!(view.suppress_k, None);
    let plan = datafusion::physical_plan::displayable(view.plan.as_ref())
        .indent(true)
        .to_string();
    assert!(!plan.contains("SuppressExec"), "{plan}");
    assert_eq!(run(view).await.rows as i64, N);
}

#[tokio::test]
async fn grouped_suppression_is_in_the_plan() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), Arc::default()).await;
    let by_dept = r#"SELECT region, dept, COUNT(*) AS n FROM "demo/cells" GROUP BY region, dept ORDER BY region, dept"#;
    let union = r#"SELECT dept, COUNT(*) AS n FROM "demo/cells" GROUP BY dept
                   UNION ALL SELECT region, COUNT(*) FROM "demo/cells" WHERE id < 20 GROUP BY region"#;
    let probe = r#"SELECT id FROM "demo/cells"
                   WHERE (SELECT COUNT(*) FROM "demo/cells" WHERE id = 3) = 1"#;
    let mut answers: Vec<Answer> = Vec::new();
    for path in BOTH {
        let grouped = answer(&e, by_dept, &guest(), path).await.unwrap();
        let n = int_column(&grouped.batches, "n");
        assert!(n.iter().all(|n| *n >= 5), "{path:?}: a small cell leaked");
        // eng, ops and sales in EA and WA; WA's three hr rows are withheld.
        assert_eq!(n.len(), 6, "{path:?}");
        answers.push(grouped);

        // Every aggregate is suppressed, the second branch of a union included.
        let u = answer(&e, union, &guest(), path).await.unwrap();
        assert!(int_column(&u.batches, "n").iter().all(|n| *n >= 5));

        // A count below k inside a subquery answers no probe.
        let p = answer(&e, probe, &guest(), path).await.unwrap();
        assert_eq!(p.rows, 0, "{path:?}");
    }
    assert_eq!(
        int_column(&answers[0].batches, "n"),
        int_column(&answers[1].batches, "n")
    );
}

#[tokio::test]
async fn a_view_is_the_query_under_row_noise_and_is_charged_once() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), Arc::default()).await;
    e.budgets().set_limit("pay", 2.0).unwrap();
    let exact = run(e.view("demo/pay", &owner()).await.unwrap()).await;
    assert!(exact.charges.is_empty(), "the owner is exempt");

    // Planning charges the budget, once: the plan is run twice and nothing more is spent.
    let view = e.view("demo/pay", &guest()).await.unwrap();
    assert_eq!(view.charges["pay"], 1.0);
    assert_eq!(view.budgets["pay"], 1.0);
    let again = datafusion::physical_plan::collect(
        reset_plan_states(view.plan.clone()).unwrap(),
        view.ctx.task_ctx(),
    )
    .await
    .unwrap();
    let viewed = run(view).await;
    assert_eq!(
        again.iter().map(|b| b.num_rows()).sum::<usize>(),
        viewed.rows
    );
    let queried = answer(&e, r#"SELECT * FROM "demo/pay""#, &guest(), Path::Query)
        .await
        .unwrap();
    assert_eq!(
        queried.budgets["pay"], 0.0,
        "the query spent the other half"
    );

    // Same rows, same unnoised columns; the salaries differ from the truth by noise alone.
    assert_eq!(
        keyed_rows(&viewed.batches, &["salary"]),
        keyed_rows(&queried.batches, &["salary"])
    );
    let truth: std::collections::BTreeMap<i64, f64> = int_column(&exact.batches, "id")
        .into_iter()
        .zip(f64_column(&exact.batches, "salary"))
        .collect();
    for answer in [&viewed, &queried] {
        let ids = int_column(&answer.batches, "id");
        let salaries = f64_column(&answer.batches, "salary");
        let mut moved = 0;
        for (id, s) in ids.iter().zip(&salaries) {
            let d = (s - truth[id]).abs();
            assert!(d < 100.0 * 40.0, "Laplace(100) tail: {d}");
            moved += (d > 0.0) as usize;
        }
        assert!(moved > salaries.len() * 9 / 10, "salaries are noised");
    }

    // The budget is spent: a view is refused as the query is, before any row is read.
    let refused = e.view("demo/pay", &guest()).await;
    assert!(
        matches!(refused, Err(PeqlError::BudgetExhausted { ref budget }) if budget == "pay"),
        "{:?}",
        refused.err().map(|e| e.to_string())
    );
    let refused = answer(&e, r#"SELECT * FROM "demo/pay""#, &guest(), Path::Query).await;
    assert!(matches!(refused, Err(PeqlError::BudgetExhausted { .. })));
    // A view of columns that are not noised costs nothing.
    let free = answer(
        &e,
        r#"SELECT id, dept FROM "demo/pay""#,
        &guest(),
        Path::Plan,
    )
    .await
    .unwrap();
    assert!(free.charges.is_empty());
    assert_eq!(free.rows as i64, N);
}

#[tokio::test]
async fn a_planned_aggregate_is_the_query_up_to_noise() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), Arc::default()).await;
    e.budgets().set_limit("totals", 1.0).unwrap();
    let sql = r#"SELECT dept, SUM(salary) AS total, COUNT(*) AS n FROM "demo/totals" GROUP BY dept ORDER BY dept"#;
    let exact = answer(&e, sql, &owner(), Path::Plan).await.unwrap();
    let planned = answer(&e, sql, &guest(), Path::Plan).await.unwrap();
    let queried = answer(&e, sql, &guest(), Path::Query).await.unwrap();
    assert_eq!(planned.charges["totals"], 0.5);
    assert_eq!(planned.budgets["totals"], 0.5);
    assert_eq!(queried.budgets["totals"], 0.0);
    let scale = 100.0 / 0.5;
    let truth = f64_column(&exact.batches, "total");
    for a in [&planned, &queried] {
        assert_eq!(int_column(&a.batches, "n"), int_column(&exact.batches, "n"));
        let totals = f64_column(&a.batches, "total");
        assert_ne!(totals, truth, "noise was applied");
        for (t, n) in truth.iter().zip(&totals) {
            assert!((t - n).abs() < 40.0 * scale, "{t} vs {n}");
        }
    }
    let refused = e.plan(sql, &guest()).await;
    assert!(matches!(refused, Err(PeqlError::BudgetExhausted { .. })));
}

#[tokio::test]
async fn planning_is_audited_with_its_charges() {
    let dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(MemoryAudit::default());
    let e = engine(dir.path(), audit.clone()).await;
    e.budgets().set_limit("pay", 1.0).unwrap();
    e.view("demo/pay", &guest()).await.unwrap();
    assert!(e.view("demo/pay", &guest()).await.is_err());
    assert!(
        e.plan(r#"SELECT * FROM "demo/nothing""#, &guest())
            .await
            .is_err()
    );
    let records = audit.records.lock().unwrap();
    let n = records.len();
    let (planned, spent, refused) = (&records[n - 3], &records[n - 2], &records[n - 1]);
    assert_eq!(planned.outcome, Outcome::Planned);
    assert_eq!(planned.charges, vec![("pay".to_owned(), 1.0)]);
    assert_eq!(planned.contracts.len(), 1);
    assert!(planned.contracts[0].starts_with("demo/pay@1#"));
    assert!(matches!(spent.outcome, Outcome::Refused(ref r) if r.contains("budget")));
    assert!(spent.charges.is_empty());
    assert!(matches!(refused.outcome, Outcome::Refused(_)));
    assert!(refused.contracts.is_empty());
}

#[tokio::test]
async fn a_failed_plan_is_audited_as_failed() {
    let dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(MemoryAudit::default());
    let e = engine(dir.path(), audit.clone()).await;
    // Not a refusal by policy: the column does not exist.
    assert!(
        e.plan(r#"SELECT nothing FROM "demo/cells""#, &guest())
            .await
            .is_err()
    );
    let records = audit.records.lock().unwrap();
    assert!(matches!(
        records.last().unwrap().outcome,
        Outcome::Failed(_)
    ));
}

#[tokio::test]
async fn a_write_in_parts_refreshes_the_manifest_once() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), Arc::default()).await;
    let before = e.manifest("demo/cells").unwrap().unwrap();
    assert_eq!(before.row_count as i64, N);

    let w = e
        .begin_write("demo/cells", WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(w.contract(), "demo/cells");
    let (a, b) = futures::join!(
        e.write_part(&w, vec![cells(N, N + 100)]),
        e.write_part(&w, vec![cells(N + 100, N + 250)])
    );
    assert_eq!((a.unwrap(), b.unwrap()), (100, 150));
    assert_eq!(w.rows(), 250);
    // Until the write finishes, the manifest describes the data as it was.
    assert_eq!(
        e.manifest("demo/cells").unwrap().unwrap().written_at,
        before.written_at
    );
    let report = e.finish_write(w).await.unwrap();
    assert_eq!(report.rows_written, 250);
    assert!(report.verdict.valid);
    let after = e.manifest("demo/cells").unwrap().unwrap();
    assert_eq!(after.row_count as i64, N + 250);
    assert!(after.written_at > before.written_at);
    assert_eq!(report.files, after.files.len());

    // Overwrite removes what was there when the write begins.
    let w = e
        .begin_write("demo/cells", WriteMode::Overwrite)
        .await
        .unwrap();
    e.write_part(&w, vec![cells(0, 10)]).await.unwrap();
    let report = e.finish_write(w).await.unwrap();
    assert_eq!(report.verdict.row_count, 10);

    // A part that does not conform leaves an overwrite's old data where it was.
    let w = e
        .begin_write("demo/cells", WriteMode::Overwrite)
        .await
        .unwrap();
    let alien = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "nothing",
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    assert!(e.write_part(&w, vec![alien]).await.is_err());
    drop(w);
    assert_eq!(e.validate("demo/cells").await.unwrap().row_count, 10);
    // An overwrite with no parts at all empties the contract.
    let w = e
        .begin_write("demo/cells", WriteMode::Overwrite)
        .await
        .unwrap();
    assert_eq!(e.finish_write(w).await.unwrap().verdict.row_count, 0);

    // A contract bound to a table is not written.
    e.bind_batches("demo/pay", vec![pay()]).await.unwrap();
    assert!(e.begin_write("demo/pay", WriteMode::Append).await.is_err());
}
