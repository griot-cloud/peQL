//! The v0 definition of done: write under a contract, validate, query as different callers.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::pretty::pretty_format_batches;
use parcel_runtime::differential::differential;
use peql::{Caller, Engine, PeqlError as EngineError, WriteMode};

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

fn engine(dir: &std::path::Path) -> Engine {
    let e = Engine::in_memory(dir);
    let source = CONTRACT.replace("file:///data/orders/", "orders/");
    e.register_contract(&source, &schema())
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

#[tokio::test]
async fn end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let data = orders();

    // Write: flags, layout, manifest, verdict.
    let report = engine
        .write("sales/orders", vec![batch(&data)], WriteMode::Overwrite)
        .await
        .unwrap();
    let v = &report.verdict;
    assert!(v.valid, "{v:#?}");
    assert_eq!(v.row_count, 600);
    assert_eq!(v.failures["pk_present"], 0);
    assert_eq!(v.failures["amount_consistent"], 24);
    assert_eq!(v.failures["msisdn_format"], 30);
    assert_eq!(
        v.stats["customer_id__null_rate"],
        serde_json::json!(3.0 / 600.0)
    );
    assert!(v.guarantees["ids_present"]);
    assert!(v.guarantees["msisdns_mostly_valid"]);
    assert!(
        report.files >= 4,
        "partitioned by region and dt: {} files",
        report.files
    );

    let manifest = engine.manifest("sales/orders").unwrap().unwrap();
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

    // Validation is reproducible: same data, same verdict and data hash.
    let again = engine.validate("sales/orders").await.unwrap();
    assert_eq!(again.data_hash, v.data_hash);
    assert_eq!(again.failures, v.failures);

    // A verifier holding only the bundle reproduces the verdict over the same data.
    let reg = engine.get("sales/orders").unwrap();
    let bundle =
        parcel_runtime::bundle::Bundle::new(&reg.doc, &reg.ancestors, &schema(), &reg.compilation)
            .unwrap();
    let bundle = parcel_runtime::bundle::Bundle::from_json(&bundle.to_json().unwrap()).unwrap();
    bundle.verify().unwrap();
    let plan = bundle
        .validation_plan(&parcel_runtime::bundle::session().task_ctx())
        .unwrap();
    let verified = engine.validate_with("sales/orders", plan).await.unwrap();
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

#[tokio::test]
async fn deny_rules_make_data_unservable() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let mut data = orders();
    data[5].order_id = None;
    let report = engine
        .write("sales/orders", vec![batch(&data)], WriteMode::Overwrite)
        .await
        .unwrap();
    assert!(!report.verdict.valid);
    assert_eq!(report.verdict.breached, ["pk_present"]);
    let q = engine
        .query(r#"SELECT COUNT(*) FROM "sales/orders""#, &analyst("acme"))
        .await;
    assert!(
        matches!(q, Err(EngineError::NotServable { .. })),
        "{:?}",
        q.err()
    );

    // Fixing the data by overwriting makes it servable again.
    let report = engine
        .write("sales/orders", vec![batch(&orders())], WriteMode::Overwrite)
        .await
        .unwrap();
    assert!(report.verdict.valid);
    assert!(
        engine
            .query(r#"SELECT COUNT(*) FROM "sales/orders""#, &analyst("acme"))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn data_guarantee_denies() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let mut data = orders();
    for o in data.iter_mut().take(30) {
        o.customer_id = None; // 5% missing, above the 2% the contract guarantees
    }
    let report = engine
        .write("sales/orders", vec![batch(&data)], WriteMode::Overwrite)
        .await
        .unwrap();
    assert!(!report.verdict.valid);
    assert_eq!(report.verdict.breached, ["ids_present"]);
    assert!(!report.verdict.guarantees["ids_present"]);
}

#[tokio::test]
async fn append_accumulates() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let data = orders();
    engine
        .write(
            "sales/orders",
            vec![batch(&data[..300])],
            WriteMode::Overwrite,
        )
        .await
        .unwrap();
    let r = engine
        .write("sales/orders", vec![batch(&data[300..])], WriteMode::Append)
        .await
        .unwrap();
    assert_eq!(r.verdict.row_count, 600);
    assert_eq!(r.verdict.failures["amount_consistent"], 24);
}

#[tokio::test]
async fn unwritten_contract_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(dir.path());
    let q = engine
        .query(r#"SELECT COUNT(*) FROM "sales/orders""#, &analyst("acme"))
        .await;
    assert!(
        matches!(q, Err(EngineError::NotWritten { .. })),
        "{:?}",
        q.err()
    );
}
