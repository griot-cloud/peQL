//! The v0 definition of done: write under a contract, validate, query as different callers.
//! Every test runs on each binding: a Parquet directory, and (feature `iceberg`) an Iceberg
//! table in a catalog.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::pretty::pretty_format_batches;
use parcel_runtime::differential::differential;
use peql::{
    Caller, Engine, OnReadViolation, OnWriteViolation, PeqlError as EngineError, WriteMode,
};

const CONTRACT: &str = include_str!("fixtures/sales_orders.yaml");

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, true),
        Field::new("customer_id", DataType::Int64, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("msisdn", DataType::Utf8, true),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("unit_price_cents", DataType::Int64, true),
        Field::new("qty", DataType::Int32, true),
        Field::new("amount_cents", DataType::Int64, true),
        Field::new("amount", DataType::Decimal128(18, 2), true),
        Field::new(
            "tags",
            DataType::List(Field::new_list_field(DataType::Utf8, true).into()),
            true,
        ),
        Field::new("region", DataType::Utf8, false),
        Field::new("dt", DataType::Date32, false),
    ]))
}

#[derive(Clone, Debug)]
struct Order {
    order_id: Option<i64>,
    customer_id: Option<i64>,
    email: String,
    msisdn: String,
    tenant: &'static str,
    unit: i64,
    qty: i32,
    amount: i64,
    region: &'static str,
    day: i32,
}

/// 600 orders across three tenants and two regions. Every 25th amount is inconsistent,
/// every 20th phone number malformed, and one customer id in 200 missing.
fn orders() -> Vec<Order> {
    let tenants = ["acme", "globex", "initech"];
    let regions = ["EA", "WA"];
    (0..600)
        .map(|i: i64| {
            let unit = 100 + (i % 7) * 50;
            let qty = 1 + (i % 4) as i32;
            Order {
                order_id: Some(i),
                customer_id: if i % 200 == 7 {
                    None
                } else {
                    Some(1000 + i % 90)
                },
                email: format!("user{}@example.co.ke", i % 90),
                msisdn: if i % 20 == 3 {
                    format!("07{:08}", i)
                } else {
                    format!("2547{:08}", i)
                },
                tenant: tenants[(i % 3) as usize],
                unit,
                qty,
                amount: if i % 25 == 0 {
                    unit * qty as i64 + 1
                } else {
                    unit * qty as i64
                },
                region: regions[((i / 3) % 2) as usize],
                day: 19_800 + (i % 2) as i32,
            }
        })
        .collect()
}

fn batch(rows: &[Order]) -> RecordBatch {
    let mut tags = ListBuilder::new(StringBuilder::new());
    for o in rows {
        tags.values().append_value(o.region.to_lowercase());
        tags.values().append_value(o.tenant);
        tags.append(true);
    }
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|o| o.order_id).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|o| o.customer_id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|o| o.email.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|o| o.msisdn.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|o| o.tenant).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|o| o.unit).collect::<Vec<_>>(),
            )),
            Arc::new(Int32Array::from(
                rows.iter().map(|o| o.qty).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|o| o.amount).collect::<Vec<_>>(),
            )),
            Arc::new(
                Decimal128Array::from(rows.iter().map(|o| o.amount as i128).collect::<Vec<_>>())
                    .with_precision_and_scale(18, 2)
                    .unwrap(),
            ),
            Arc::new(tags.finish()),
            Arc::new(StringArray::from(
                rows.iter().map(|o| o.region).collect::<Vec<_>>(),
            )),
            Arc::new(Date32Array::from(
                rows.iter().map(|o| o.day).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Where the contract's data lives, as its document says.
#[derive(Clone, Copy, Debug)]
enum Binding {
    /// `binding: {parquet: orders/}`, the fixture as written.
    Parquet,
    /// `binding: {iceberg: sales.orders}`, read through a catalog the engine is given.
    #[cfg(feature = "iceberg")]
    Iceberg,
}

/// The contract's document bound to `binding`.
fn document(binding: Binding) -> String {
    match binding {
        Binding::Parquet => CONTRACT.to_owned(),
        #[cfg(feature = "iceberg")]
        Binding::Iceberg => iceberg_document(),
    }
}

fn iceberg_document() -> String {
    let doc = CONTRACT.replace("parquet: orders/", "iceberg: sales.orders");
    assert_ne!(doc, CONTRACT, "the fixture's binding is `parquet: orders/`");
    doc
}

async fn engine(dir: &std::path::Path, binding: Binding) -> Engine {
    let e = Engine::in_memory(dir);
    let e = match binding {
        Binding::Parquet => e,
        #[cfg(feature = "iceberg")]
        Binding::Iceberg => {
            let catalog = support::iceberg_catalog(&dir.join("warehouse"), "sales").await;
            e.with_bindings(Arc::new(peql::iceberg_table::IcebergTables::new(catalog)))
        }
    };
    e.register_contract(&document(binding), &schema())
        .unwrap_or_else(|err| panic!("{err}"));
    e
}

fn analyst(tenant: &str) -> Caller {
    Caller::new(&format!("{tenant}-analyst"), tenant, "analytics")
}

/// Rows as strings keyed by the first column, for comparisons.
fn keyed(batches: &[RecordBatch]) -> BTreeMap<String, Vec<String>> {
    let mut out = BTreeMap::new();
    for b in batches {
        for i in 0..b.num_rows() {
            let cells: Vec<String> = (0..b.num_columns())
                .map(|c| {
                    datafusion::arrow::util::display::array_value_to_string(b.column(c), i).unwrap()
                })
                .collect();
            out.insert(cells[0].clone(), cells[1..].to_vec());
        }
    }
    out
}

async fn end_to_end(binding: Binding) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), binding).await;
    let data = orders();

    // Write: flags, layout, manifest, verdict.
    let report = engine
        .write(
            "sales/orders",
            vec![batch(&data)],
            WriteMode::Overwrite,
            OnWriteViolation::Warn,
        )
        .await
        .unwrap();
    let v = &report.report.verdict;
    assert!(v.valid, "{v:#?}");
    assert_eq!(v.row_count, 600);
    assert_eq!(v.failures["pk_present"], 0);
    assert_eq!(v.failures["amount_consistent"], 24);
    assert_eq!(v.failures["msisdn_format"], 30);
    assert_eq!(
        v.stats["customer_id__null_rate"],
        serde_json::json!(3.0 / 600.0)
    );
    // A write's verdict is its row rules over its rows; guarantees are the whole data's.
    assert!(v.guarantees.is_empty(), "{:?}", v.guarantees);
    assert!(
        report.report.files.len() >= 4,
        "partitioned by region and dt: {} files",
        report.report.files.len()
    );

    let manifest = engine.manifest("sales/orders").unwrap().unwrap();
    assert!(manifest.valid);
    assert_eq!(manifest.files, report.report.files);
    assert_eq!(manifest.data_hash, v.data_hash);
    assert!(
        manifest.flags_current(
            &engine
                .get("sales/orders")
                .unwrap()
                .compilation
                .contract
                .contract_hash
        )
    );
    assert!(
        manifest
            .files
            .iter()
            .all(|f| f.flags.contains_key("_c_amount_consistent"))
    );

    // Validation of the whole data, on demand, finds what the write did: the first write's
    // rows are all the data.
    let again = engine.validate("sales/orders").await.unwrap();
    assert_eq!(again.data_hash, v.data_hash);
    assert_eq!(again.failures, v.failures);
    assert!(again.guarantees["ids_present"]);
    assert!(again.guarantees["msisdns_mostly_valid"]);

    // A verifier holding only the compiled bytes reproduces the verdict over the same data.
    let reg = engine.get("sales/orders").unwrap();
    use parcel_runtime::compiled::CompiledBytes;
    let back = parcel_core::compile::Compilation::from_bytes(&reg.compilation.to_bytes().unwrap())
        .unwrap();
    let verified = engine
        .validate_with("sales/orders", back.validation.plan.clone())
        .await
        .unwrap();
    assert_eq!(
        (verified.valid, &verified.failures, &verified.data_hash),
        (v.valid, &v.failures, &v.data_hash)
    );

    // globex sees only its own consistent rows, with suppression, as sums per region.
    let sql = r#"SELECT region, SUM(amount_cents) AS total, COUNT(*) AS n FROM "sales/orders" GROUP BY region ORDER BY region"#;
    let res = engine.query(sql, &analyst("globex")).await.unwrap();
    println!("{}", pretty_format_batches(&res.batches).unwrap());
    let mut want: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    for o in data
        .iter()
        .filter(|o| o.tenant == "globex" && o.amount == o.unit * o.qty as i64)
    {
        let e = want.entry(o.region.to_owned()).or_default();
        e.0 += o.amount;
        e.1 += 1;
    }
    let got = keyed(&res.batches);
    assert_eq!(got.len(), want.len());
    for (region, (total, n)) in &want {
        assert_eq!(
            got[region],
            vec![total.to_string(), n.to_string()],
            "region {region}"
        );
    }
    let env = &res.envelope;
    assert_eq!(env.contracts[0].decisions, ["analytics_only"]);
    assert_eq!(env.contracts[0].shapes, ["small_cells"]);
    assert_eq!(env.suppress_k, Some(5));
    assert!(env.contracts[0].flags_materialised);

    // globex sees hashed emails; acme sees them in clear; neither sees the other's rows.
    // Five rows, because globex's results are suppressed below k = 5.
    let emails = r#"SELECT order_id, email FROM "sales/orders" ORDER BY order_id LIMIT 5"#;
    let g = keyed(
        &engine
            .query(emails, &analyst("globex"))
            .await
            .unwrap()
            .batches,
    );
    let a = keyed(
        &engine
            .query(emails, &analyst("acme"))
            .await
            .unwrap()
            .batches,
    );
    assert_eq!(g.keys().collect::<Vec<_>>(), ["1", "10", "13", "4", "7"]);
    assert_eq!(a.keys().collect::<Vec<_>>(), ["12", "15", "3", "6", "9"]); // 0 is inconsistent and dropped
    assert_eq!(a["3"][0], "user3@example.co.ke");
    assert_eq!(g["1"][0].len(), 64);
    assert_ne!(g["1"][0], "user1@example.co.ke");

    // An admin sees every tenant's consistent rows.
    let count = r#"SELECT COUNT(*) FROM "sales/orders""#;
    let admin = Caller::new("root", "globex", "reporting").with_roles(&["admin"]);
    let n = keyed(&engine.query(count, &admin).await.unwrap().batches);
    assert_eq!(n.keys().next().unwrap(), "576");

    // Columns the contract does not expose do not exist for the caller.
    let hidden = engine
        .query(
            r#"SELECT tenant_id FROM "sales/orders""#,
            &analyst("globex"),
        )
        .await;
    assert!(hidden.is_err());
    let hidden = engine
        .query(
            r#"SELECT COUNT(*) FROM "sales/orders" WHERE unit_price_cents > 0"#,
            &analyst("globex"),
        )
        .await;
    assert!(hidden.is_err());

    // decide: a caller with the wrong purpose is refused before any file is opened.
    let m = engine
        .query(count, &Caller::new("m", "globex", "marketing"))
        .await;
    assert!(
        matches!(m, Err(EngineError::Denied { ref rule, .. }) if rule == "analytics_only"),
        "{:?}",
        m.err()
    );

    // Nor by DDL, DML, COPY, SET or EXPLAIN.
    for attack in [
        "CREATE EXTERNAL TABLE raw STORED AS PARQUET LOCATION 'orders/'",
        "COPY (SELECT * FROM \"sales/orders\") TO '/tmp/leak.parquet'",
        "INSERT INTO \"sales/orders\" VALUES (1)",
        "SET datafusion.execution.batch_size = 1",
        "EXPLAIN SELECT * FROM \"sales/orders\"",
        "EXPLAIN ANALYZE SELECT * FROM \"sales/orders\"",
    ] {
        assert!(
            engine.query(attack, &analyst("globex")).await.is_err(),
            "allowed: {attack}"
        );
    }

    // suppress applies to every aggregate: a UNION cannot leak small groups via its second branch.
    let union = r#"SELECT region, COUNT(*) AS n FROM "sales/orders" GROUP BY region
                   UNION ALL SELECT email, COUNT(*) FROM "sales/orders" GROUP BY email"#;
    let res = engine.query(union, &analyst("globex")).await.unwrap();
    for b in &res.batches {
        let n = datafusion::arrow::compute::cast(b.column(1), &DataType::Int64).unwrap();
        let n = n.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(
            n.iter().all(|v| v.unwrap() >= 5),
            "a group smaller than k leaked"
        );
    }

    // ... nor through an aggregate inside a scalar subquery (membership probing). Order 1 is
    // globex's and unique: probing "is there exactly one order 1?" must not answer.
    assert!(
        data.iter()
            .any(|o| o.order_id == Some(1) && o.tenant == "globex")
    );
    let probe = r#"SELECT order_id FROM "sales/orders"
                   WHERE (SELECT COUNT(*) FROM "sales/orders" WHERE order_id = 1) = 1"#
        .to_owned();
    let res = engine.query(&probe, &analyst("globex")).await.unwrap();
    assert_eq!(res.envelope.rows, 0, "a count below k answered a probe");

    // Raw files are not reachable by name.
    assert!(
        engine
            .query("SELECT * FROM __parcel_sales_orders", &analyst("globex"))
            .await
            .is_err()
    );

    // guarantee annotate: freshness fails for a caller living a week in the future.
    let later = analyst("globex").at(Utc::now() + chrono::Duration::days(7));
    let res = engine.query(count, &later).await.unwrap();
    assert_eq!(res.envelope.contracts[0].annotations, ["fresh_enough"]);

    // describe: what a caller would see.
    let described = engine.describe("sales/orders", &analyst("globex")).unwrap();
    assert_eq!(
        described
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        ["order_id", "customer_id", "email", "region", "amount_cents"]
    );

    // suppress without GROUP BY: a result smaller than k is withheld entirely.
    let few = engine
        .query(
            r#"SELECT order_id FROM "sales/orders" WHERE order_id < 10"#,
            &analyst("globex"),
        )
        .await
        .unwrap();
    assert_eq!(few.envelope.rows, 0);
    let few_acme = engine
        .query(
            r#"SELECT order_id FROM "sales/orders" WHERE order_id < 10"#,
            &analyst("acme"),
        )
        .await
        .unwrap();
    assert_eq!(few_acme.envelope.rows, 3, "acme is exempt from suppression");

    // Differential test: interpreter and DataFusion agree on every rule, row and caller.
    let callers = [analyst("globex"), analyst("acme"), admin.clone()];
    let comp = engine.get("sales/orders").unwrap().compilation.clone();
    let diff = differential(&comp, &batch(&data[..120]), &callers)
        .await
        .unwrap();
    assert!(
        diff.passed(),
        "{:#?}",
        &diff.mismatches[..diff.mismatches.len().min(10)]
    );
    assert!(diff.evaluations > 1000);
    assert_eq!(diff.both_errored, 0);
    let _ = Utc.timestamp_opt(0, 0);
}

/// Every tenant's rows, and no suppression: what the admit and the shapes let through.
fn admin() -> Caller {
    Caller::new("root", "acme", "reporting").with_roles(&["admin"])
}

/// The orders with order 5's id missing: it breaks `pk_present` (deny); every 25th order
/// breaks `amount_consistent` (drop), and every 20th phone number fails `msisdn_format`, which
/// only reports.
fn breaking() -> Vec<Order> {
    let mut data = orders();
    data[5].order_id = None;
    data
}

/// The orders that break no row rule.
fn passing() -> Vec<Order> {
    orders()
        .into_iter()
        .filter(|o| o.amount == o.unit * o.qty as i64)
        .collect()
}

const ALL_IDS: &str = r#"SELECT order_id FROM "sales/orders""#;

fn order_ids(batches: &[RecordBatch]) -> Vec<Option<i64>> {
    let mut ids: Vec<Option<i64>> = batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column_by_name("order_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            (0..a.len()).map(move |i| a.is_valid(i).then(|| a.value(i)))
        })
        .collect();
    ids.sort();
    ids
}

/// Every `.parquet` file under `dir`.
fn parquet_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "parquet") {
                out.push(p);
            }
        }
    }
    out
}

/// A write refuses rows that break a row rule unless the writer asks otherwise: the refusal
/// names each rule and its count, in the contract's order, and nothing is committed.
async fn a_write_refuses_rows_that_break_a_rule_by_default(binding: Binding) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), binding).await;
    assert_eq!(OnWriteViolation::default(), OnWriteViolation::Refuse);
    let refused = engine
        .write(
            "sales/orders",
            vec![batch(&breaking())],
            WriteMode::Overwrite,
            OnWriteViolation::default(),
        )
        .await;
    match refused {
        Err(EngineError::Violated {
            contract,
            violations,
        }) => {
            assert_eq!(contract, "sales/orders");
            assert_eq!(
                violations,
                [
                    ("pk_present".to_owned(), 1),
                    ("amount_consistent".to_owned(), 24)
                ]
            );
        }
        other => panic!("{:?}", other.map(|o| o.report)),
    }
    let e = engine
        .write(
            "sales/orders",
            vec![batch(&breaking())],
            WriteMode::Append,
            OnWriteViolation::Refuse,
        )
        .await
        .unwrap_err();
    assert!(e.is_refusal());
    assert!(
        e.to_string()
            .contains("pk_present (1 row), amount_consistent (24 rows)"),
        "{e}"
    );
    assert!(engine.manifest("sales/orders").unwrap().is_none());
    assert_eq!(parquet_files(dir.path()), Vec::<std::path::PathBuf>::new());
    let q = engine.query(ALL_IDS, &admin()).await;
    assert!(
        matches!(q, Err(EngineError::NotWritten { .. })),
        "{:?}",
        q.err()
    );

    // Rows that break no rule are written; a report-level assert's failures refuse nothing.
    let report = engine
        .write(
            "sales/orders",
            vec![batch(&passing())],
            WriteMode::Overwrite,
            OnWriteViolation::Refuse,
        )
        .await
        .unwrap()
        .report;
    assert_eq!(report.rows_written, 576);
    assert_eq!(report.on_violation, OnWriteViolation::Refuse);
    assert!(report.violations.is_empty());
    assert_eq!(report.rows_excluded, 0);
    assert_eq!(report.verdict.failures["msisdn_format"], 30);
    assert!(report.verdict.valid);
}

/// `exclude` commits only the rows that break no rule and hands the others back, each with the
/// rules it broke; the report counts them per rule.
async fn exclude_commits_only_the_rows_that_pass(binding: Binding) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), binding).await;
    let outcome = engine
        .write(
            "sales/orders",
            vec![batch(&breaking())],
            WriteMode::Overwrite,
            OnWriteViolation::Exclude,
        )
        .await
        .unwrap();
    let report = &outcome.report;
    assert_eq!(report.rows_written, 575);
    assert_eq!(report.rows_excluded, 25);
    assert_eq!(
        report.violations,
        BTreeMap::from([
            ("pk_present".to_owned(), 1),
            ("amount_consistent".to_owned(), 24)
        ])
    );
    assert!(report.verdict.valid, "{:?}", report.verdict.breached);
    assert_eq!(report.verdict.row_count, 575);
    assert_eq!(report.verdict.failures["pk_present"], 0);
    assert_eq!(report.verdict.failures["amount_consistent"], 0);
    assert_eq!(report.verdict.failures["msisdn_format"], 30);

    let excluded = outcome.excluded.as_ref().unwrap();
    assert_eq!(excluded.num_rows(), 25);
    let mut columns: Vec<String> = schema().fields().iter().map(|f| f.name().clone()).collect();
    columns.push(peql::violation::VIOLATIONS_COLUMN.to_owned());
    let names: Vec<String> = excluded
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, columns);
    let ids = excluded
        .column_by_name("order_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .clone();
    let reasons = excluded
        .column_by_name(peql::violation::VIOLATIONS_COLUMN)
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .clone();
    let mut set_aside = Vec::new();
    for i in 0..excluded.num_rows() {
        let rules = reasons.value(i);
        let rules = rules.as_any().downcast_ref::<StringArray>().unwrap();
        let rules: Vec<&str> = (0..rules.len()).map(|r| rules.value(r)).collect();
        set_aside.push((ids.is_valid(i).then(|| ids.value(i)), rules.join(",")));
    }
    set_aside.sort();
    let mut expected = vec![(None, "pk_present".to_owned())];
    expected.extend(
        (0..600)
            .step_by(25)
            .map(|i| (Some(i), "amount_consistent".to_owned())),
    );
    assert_eq!(set_aside, expected);

    // What was committed breaks nothing, so a read leaves nothing out, and a read that
    // refuses breaking rows is answered.
    let res = engine.query(ALL_IDS, &admin()).await.unwrap();
    assert_eq!(res.envelope.rows, 575);
    assert!(res.envelope.contracts[0].excluded.is_empty());
    assert!(!order_ids(&res.batches).contains(&None));
    let refusing = engine
        .query_as_of(
            ALL_IDS,
            &admin(),
            &peql::AsOf::current(),
            OnReadViolation::Refuse,
        )
        .await
        .unwrap();
    assert_eq!(refusing.envelope.rows, 575);

    // A write in parts hands each part's breaking rows back from that part, and holds none.
    let w = engine
        .begin_write("sales/orders", WriteMode::Append, OnWriteViolation::Exclude)
        .await
        .unwrap();
    let data = breaking();
    let first = engine
        .write_part(&w, vec![batch(&data[..300])])
        .await
        .unwrap();
    let second = engine
        .write_part(&w, vec![batch(&data[300..])])
        .await
        .unwrap();
    assert_eq!((first.rows_written, second.rows_written), (287, 288));
    assert_eq!(first.excluded.unwrap().num_rows(), 13);
    assert_eq!(second.excluded.unwrap().num_rows(), 12);
    let report = engine.finish_write(w).await.unwrap();
    assert_eq!((report.rows_written, report.rows_excluded), (575, 25));
    assert_eq!(report.violations["amount_consistent"], 24);
    assert_eq!(
        engine.query(ALL_IDS, &admin()).await.unwrap().envelope.rows,
        1150
    );

    // A write all of whose rows break a rule commits none of them.
    let all_breaking: Vec<Order> = breaking()
        .into_iter()
        .filter(|o| o.order_id.is_none() || o.amount != o.unit * o.qty as i64)
        .collect();
    let outcome = engine
        .write(
            "sales/orders",
            vec![batch(&all_breaking)],
            WriteMode::Append,
            OnWriteViolation::Exclude,
        )
        .await
        .unwrap();
    assert_eq!(
        (outcome.report.rows_written, outcome.report.rows_excluded),
        (0, 25)
    );
    assert_eq!(outcome.excluded.unwrap().num_rows(), 25);
    assert_eq!(
        engine.query(ALL_IDS, &admin()).await.unwrap().envelope.rows,
        1150
    );
}

/// `warn` commits every row and marks the write; a read then leaves the breaking rows out and
/// counts them, or, asked to refuse them, is refused naming each rule and its count.
async fn warn_commits_every_row_and_a_read_leaves_the_breaking_ones_out(binding: Binding) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), binding).await;
    let report = engine
        .write(
            "sales/orders",
            vec![batch(&breaking())],
            WriteMode::Overwrite,
            OnWriteViolation::Warn,
        )
        .await
        .unwrap()
        .report;
    assert_eq!(report.rows_written, 600);
    assert_eq!(report.rows_excluded, 0);
    assert_eq!(
        report.violations,
        BTreeMap::from([
            ("pk_present".to_owned(), 1),
            ("amount_consistent".to_owned(), 24)
        ])
    );
    assert!(!report.verdict.valid);
    assert_eq!(report.verdict.breached, ["pk_present"]);

    // By default the view leaves them out, and the envelope says how many per rule.
    let res = engine.query(ALL_IDS, &admin()).await.unwrap();
    assert_eq!(res.envelope.rows, 575);
    let ids = order_ids(&res.batches);
    assert!(!ids.contains(&None));
    assert!(!ids.contains(&Some(25)));
    assert_eq!(
        res.envelope.contracts[0].excluded,
        BTreeMap::from([
            ("pk_present".to_owned(), 1),
            ("amount_consistent".to_owned(), 24)
        ])
    );
    // Even a query written to find them does not.
    let probe = engine
        .query(
            r#"SELECT order_id FROM "sales/orders" WHERE order_id IS NULL OR order_id % 25 = 0"#,
            &admin(),
        )
        .await
        .unwrap();
    assert_eq!(probe.envelope.rows, 0);
    // An executor's plan is the same view.
    let planned = engine
        .view_for("sales/orders", &admin(), Some(1), OnReadViolation::Exclude)
        .await
        .unwrap();
    assert_eq!(planned.contracts[0].excluded["pk_present"], 1);

    // Asked to refuse them, every read path is refused, naming each rule and its count.
    let violated = |r: peql::Result<()>| match r {
        Err(EngineError::Violated {
            contract,
            violations,
        }) => {
            assert_eq!(contract, "sales/orders");
            assert_eq!(
                violations,
                [
                    ("pk_present".to_owned(), 1),
                    ("amount_consistent".to_owned(), 24)
                ]
            );
        }
        other => panic!("{other:?}"),
    };
    violated(
        engine
            .query_as_of(
                ALL_IDS,
                &admin(),
                &peql::AsOf::current(),
                OnReadViolation::Refuse,
            )
            .await
            .map(|_| ()),
    );
    violated(
        engine
            .view_for("sales/orders", &admin(), None, OnReadViolation::Refuse)
            .await
            .map(|_| ()),
    );
    violated(
        engine
            .check(ALL_IDS, &admin(), OnReadViolation::Refuse)
            .await
            .map(|_| ()),
    );

    // Data that breaks no rule is read either way.
    engine
        .write(
            "sales/orders",
            vec![batch(&passing())],
            WriteMode::Overwrite,
            OnWriteViolation::Refuse,
        )
        .await
        .unwrap();
    let res = engine
        .query_as_of(
            ALL_IDS,
            &admin(),
            &peql::AsOf::current(),
            OnReadViolation::Refuse,
        )
        .await
        .unwrap();
    assert_eq!(res.envelope.rows, 576);
    assert!(res.envelope.contracts[0].excluded.is_empty());
}

/// A contract's context terms refuse a read whatever it asks about breaking rows, before the
/// rows are looked at.
async fn a_context_breach_refuses_whatever_the_mode(binding: Binding) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), binding).await;
    engine
        .write(
            "sales/orders",
            vec![batch(&breaking())],
            WriteMode::Overwrite,
            OnWriteViolation::Warn,
        )
        .await
        .unwrap();
    let marketing = Caller::new("mo", "acme", "marketing").with_roles(&["admin"]);
    for mode in [OnReadViolation::Exclude, OnReadViolation::Refuse] {
        let denied = |r: peql::Result<()>| match r {
            Err(EngineError::Denied { contract, rule }) => {
                assert_eq!(
                    (contract.as_str(), rule.as_str()),
                    ("sales/orders", "analytics_only")
                )
            }
            other => panic!("{mode:?}: {other:?}"),
        };
        denied(
            engine
                .query_as_of(ALL_IDS, &marketing, &peql::AsOf::current(), mode)
                .await
                .map(|_| ()),
        );
        denied(
            engine
                .view_for("sales/orders", &marketing, None, mode)
                .await
                .map(|_| ()),
        );
        denied(engine.check(ALL_IDS, &marketing, mode).await.map(|_| ()));
    }
    let denied = engine.query(ALL_IDS, &marketing).await;
    assert!(
        matches!(denied, Err(EngineError::Denied { .. })),
        "{:?}",
        denied.err()
    );
}

/// The columns a contract withholds stay withheld, and masked columns masked, whatever a read
/// asks about breaking rows.
async fn withheld_columns_stay_withheld_in_every_mode(binding: Binding) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), binding).await;
    engine
        .write(
            "sales/orders",
            vec![batch(&passing())],
            WriteMode::Overwrite,
            OnWriteViolation::Refuse,
        )
        .await
        .unwrap();
    let globex = analyst("globex");
    for mode in [OnReadViolation::Exclude, OnReadViolation::Refuse] {
        let res = engine
            .query_as_of(
                r#"SELECT * FROM "sales/orders""#,
                &globex,
                &peql::AsOf::current(),
                mode,
            )
            .await
            .unwrap();
        let columns: Vec<String> = res
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(
            columns,
            ["order_id", "customer_id", "email", "region", "amount_cents"],
            "{mode:?}"
        );
        assert!(res.envelope.rows > 0);
        let emails = res
            .batches
            .iter()
            .flat_map(|b| {
                let a = b
                    .column_by_name("email")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .clone();
                (0..a.len()).map(move |i| a.value(i).to_owned())
            })
            .collect::<Vec<_>>();
        assert!(
            emails.iter().all(|e| !e.contains('@')),
            "{mode:?}: {emails:?}"
        );
        for probe in [
            r#"SELECT msisdn FROM "sales/orders""#,
            r#"SELECT tenant_id FROM "sales/orders""#,
        ] {
            assert!(
                engine
                    .query_as_of(probe, &globex, &peql::AsOf::current(), mode)
                    .await
                    .is_err(),
                "{mode:?}: {probe}"
            );
        }
    }
}

async fn data_guarantee_denies(binding: Binding) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), binding).await;
    let mut data = orders();
    for o in data.iter_mut().take(30) {
        o.customer_id = None; // 5% missing, above the 2% the contract guarantees
    }
    let report = engine
        .write(
            "sales/orders",
            vec![batch(&data)],
            WriteMode::Overwrite,
            OnWriteViolation::Warn,
        )
        .await
        .unwrap()
        .report;
    // The rows break no row rule, and a write decides no guarantee: the data is served.
    assert!(report.verdict.valid, "{:?}", report.verdict.breached);
    let facts = engine.manifest("sales/orders").unwrap().unwrap();
    assert!(facts.valid);
    assert!(facts.guarantees.is_empty());
    let acme = analyst("acme");
    let read = || engine.query(r#"SELECT COUNT(*) FROM "sales/orders""#, &acme);
    assert!(read().await.is_ok());
    // A validation of the whole data decides the guarantee, which does not hold.
    let whole = engine.validate("sales/orders").await.unwrap();
    assert_eq!(whole.breached, ["ids_present"]);
    assert!(!whole.guarantees["ids_present"]);
    let facts = engine.manifest("sales/orders").unwrap().unwrap();
    assert!(!facts.guarantees["ids_present"].holds);
    let q = read().await;
    assert!(
        matches!(q, Err(EngineError::NotServable { ref breached, .. }) if breached == &["ids_present"]),
        "{:?}",
        q.err()
    );
}

async fn append_accumulates(binding: Binding) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), binding).await;
    let data = orders();
    engine
        .write(
            "sales/orders",
            vec![batch(&data[..300])],
            WriteMode::Overwrite,
            OnWriteViolation::Warn,
        )
        .await
        .unwrap();
    let r = engine
        .write(
            "sales/orders",
            vec![batch(&data[300..])],
            WriteMode::Append,
            OnWriteViolation::Warn,
        )
        .await
        .unwrap()
        .report;
    // The write's verdict covers its own rows, and its hash its own files.
    let half = data[300..]
        .iter()
        .filter(|o| o.amount != o.unit * o.qty as i64)
        .count() as i64;
    assert_eq!(r.verdict.row_count, 300);
    assert_eq!(r.verdict.failures["amount_consistent"], half);
    assert_eq!(r.verdict.data_hash, peql::manifest::data_hash(&r.files));

    // The data's facts are the two writes' combined, and match a validation of the whole.
    let combined = engine.manifest("sales/orders").unwrap().unwrap();
    assert_eq!(combined.row_count, 600);
    assert_eq!(combined.failures["amount_consistent"], 24);
    assert!(r.files.iter().all(|f| combined.files.contains(f)));
    let whole = engine.validate("sales/orders").await.unwrap();
    let validated = engine.manifest("sales/orders").unwrap().unwrap();
    assert_eq!(combined.data_hash, whole.data_hash);
    assert_eq!(combined.files, validated.files);
    assert_eq!(combined.failures, validated.failures);
    assert_eq!(
        (combined.valid, &combined.breached),
        (validated.valid, &validated.breached)
    );
    for (k, v) in &combined.stats {
        assert_eq!(Some(v), validated.stats.get(k), "{k}");
    }
}

async fn unwritten_contract_is_reported(binding: Binding) {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path(), binding).await;
    let q = engine
        .query(r#"SELECT COUNT(*) FROM "sales/orders""#, &analyst("acme"))
        .await;
    assert!(
        matches!(q, Err(EngineError::NotWritten { .. })),
        "{:?}",
        q.err()
    );
}

/// An engine given no Iceberg catalog refuses a contract bound to an Iceberg table, naming the
/// contract and the table, whether it reads or writes.
#[tokio::test]
async fn an_iceberg_binding_without_a_catalog_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::in_memory(dir.path());
    engine
        .register_contract(&iceberg_document(), &schema())
        .unwrap_or_else(|err| panic!("{err}"));
    let want = "`sales/orders` is bound to the Iceberg table `sales.orders` \
                (`binding: {iceberg: sales.orders}`), and this engine has no Iceberg catalog";

    let w = engine
        .write(
            "sales/orders",
            vec![batch(&orders())],
            WriteMode::Append,
            OnWriteViolation::Warn,
        )
        .await;
    match w {
        Err(EngineError::Invalid(m)) => assert_eq!(m, want),
        other => panic!("{:?}", other.map(|r| r.report.rows_written)),
    }
    let q = engine
        .query(r#"SELECT COUNT(*) FROM "sales/orders""#, &analyst("acme"))
        .await;
    match q {
        Err(EngineError::Invalid(m)) => assert_eq!(m, want),
        other => panic!("{:?}", other.map(|r| r.envelope.rows)),
    }
    assert!(!dir.path().join("orders").exists());
}

/// Each test as `<test>::parquet` and, with feature `iceberg`, `<test>::iceberg`.
macro_rules! on_every_binding {
    ($($test:ident),* $(,)?) => {$(
        mod $test {
            #[tokio::test]
            async fn parquet() {
                super::$test(super::Binding::Parquet).await
            }
            #[cfg(feature = "iceberg")]
            #[tokio::test(flavor = "multi_thread")]
            async fn iceberg() {
                super::$test(super::Binding::Iceberg).await
            }
        }
    )*};
}

on_every_binding!(
    end_to_end,
    a_write_refuses_rows_that_break_a_rule_by_default,
    exclude_commits_only_the_rows_that_pass,
    warn_commits_every_row_and_a_read_leaves_the_breaking_ones_out,
    a_context_breach_refuses_whatever_the_mode,
    withheld_columns_stay_withheld_in_every_mode,
    data_guarantee_denies,
    append_accumulates,
    unwritten_contract_is_reported,
);
