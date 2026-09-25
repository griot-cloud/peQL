//! peQL 0.3's policy behaviours, written as a parcel contract and enforced by peQL 0.4:
//! purpose gating, owner-sees-raw, row filtering, projection hiding, every mask, noise on
//! row values with budget exhaustion, and deny without an existence oracle. Plus what 0.4
//! adds around them: pushdown, the gate barrier, publication, auditing and the bundle handoff.

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::display::array_value_to_string;
use peql::audit::{MemoryAudit, Outcome};
use peql::{Caller, Engine, PeqlError, WriteMode};

const ORDERS: &str = r#"
contract: sales/orders
version: 1
owner: acme
binding: {parquet: orders/, partitioned_by: [region]}
expose:
  - {name: order_id, type: int64}
  - {name: email, type: utf8}
  - {name: phone, type: utf8}
  - {name: name, type: utf8}
  - {name: notes, type: utf8}
  - {name: region, type: utf8}
  - {name: amount, type: utf8}
  - {name: salary, type: float64}
rules:
  - {id: purposes, op: decide, expr: "ctx.purpose in ['analytics']"}
  - {id: eu_only, op: admit, expr: "row.region == 'EU' || ctx.tenant == 'acme'"}
  - {id: hash_email, op: transform, column: email, expr: "ctx.tenant == 'acme' ? row.email : hash_sha256(row.email)"}
  - {id: partial_phone, op: transform, column: phone, expr: "ctx.tenant == 'acme' ? row.phone : partial(row.phone, 4)"}
  - {id: redact_name, op: transform, column: name, expr: "ctx.tenant == 'acme' ? row.name : redact(row.name)"}
  - {id: null_notes, op: transform, column: notes, expr: "ctx.tenant == 'acme' ? row.notes : null"}
  - {id: hash_amount, op: transform, column: amount, expr: "ctx.tenant == 'acme' ? string(row.amount) : hash_sha256(string(row.amount))"}
  - id: dp_salary
    op: shape
    operator: noise
    column: salary
    params: {sensitivity: 1000, epsilon: 1.0, budget: salary, at: row}
    unless: "ctx.tenant == 'acme'"
"#;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("phone", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("notes", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, true),
        Field::new("salary", DataType::Float64, true),
        Field::new("internal_cost", DataType::Int64, true),
    ]))
}

const REGIONS: [&str; 3] = ["EU", "US", "KE"];

fn batch() -> RecordBatch {
    let n = 30i64;
    let ids: Vec<i64> = (1..=n).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|i| format!("user{i}@x.io"))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|i| format!("07123456{i:02}"))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|i| format!("Person {i}"))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter().map(|i| format!("note {i}")).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|i| REGIONS[(*i % 3) as usize])
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                ids.iter().map(|i| i * 100).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                ids.iter().map(|i| 50_000.0 + *i as f64).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                ids.iter().map(|i| i * 7).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn region_of(id: i64) -> &'static str {
    REGIONS[(id % 3) as usize]
}

async fn engine(dir: &std::path::Path, audit: Arc<MemoryAudit>) -> Engine {
    let e = Engine::in_memory(dir).with_audit(audit);
    e.register_contract(ORDERS, &schema())
        .unwrap_or_else(|x| panic!("{x}"));
    e.publish("sales/orders", "globex").unwrap();
    e.write("sales/orders", vec![batch()], WriteMode::Overwrite)
        .await
        .unwrap();
    e
}

fn globex() -> Caller {
    Caller::new("user:bob", "globex", "analytics")
}
fn acme() -> Caller {
    Caller::new("user:alice", "acme", "analytics")
}

/// Every row as `column -> text`, ordered by order_id.
fn rows(batches: &[RecordBatch]) -> Vec<std::collections::BTreeMap<String, String>> {
    let mut out = Vec::new();
    for b in batches {
        for i in 0..b.num_rows() {
            out.push(
                b.schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(c, f)| {
                        let v = if b.column(c).is_null(i) {
                            "NULL".to_owned()
                        } else {
                            array_value_to_string(b.column(c), i).unwrap()
                        };
                        (f.name().clone(), v)
                    })
                    .collect(),
            );
        }
    }
    out
}

fn sha(s: &str) -> String {
    parcel_core::hash::sha256_hex(s.as_bytes())
}

#[tokio::test]
async fn outsiders_see_filtered_masked_noised_rows() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), Arc::default()).await;
    let sql = r#"SELECT order_id, email, phone, name, notes, region, amount, salary FROM "sales/orders" ORDER BY order_id"#;
    let res = e.query(sql, &globex()).await.unwrap();
    let got = rows(&res.batches);
    let eu: Vec<i64> = (1..=30).filter(|i| region_of(*i) == "EU").collect();
    assert_eq!(got.len(), eu.len(), "only EU rows");
    let mut noised = 0;
    for (r, id) in got.iter().zip(&eu) {
        assert_eq!(r["order_id"], id.to_string());
        assert_eq!(r["region"], "EU");
        assert_eq!(r["email"], sha(&format!("user{id}@x.io")), "hash_sha256");
        assert_eq!(
            r["phone"],
            format!("***56{id:02}"),
            "partial keeps the last four"
        );
        assert_eq!(r["name"], "***", "redact is a fixed token");
        assert_eq!(r["notes"], "NULL", "the null mask is a real null");
        assert_eq!(
            r["amount"],
            sha(&(id * 100).to_string()),
            "a number hashed as text"
        );
        if r["salary"] != format!("{}", 50_000.0 + *id as f64) {
            noised += 1;
        }
    }
    assert!(noised >= eu.len() - 1, "salary is noised row by row");
    assert!(res.envelope.budgets.contains_key("salary"));
}

#[tokio::test]
async fn the_owner_sees_raw_values_and_every_row() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), Arc::default()).await;
    let sql = r#"SELECT order_id, email, phone, name, notes, amount, salary FROM "sales/orders" ORDER BY order_id"#;
    let res = e.query(sql, &acme()).await.unwrap();
    let got = rows(&res.batches);
    assert_eq!(got.len(), 30);
    let r = &got[4];
    assert_eq!(r["email"], "user5@x.io");
    assert_eq!(r["phone"], "0712345605");
    assert_eq!(r["name"], "Person 5");
    assert_eq!(r["notes"], "note 5");
    assert_eq!(r["amount"], "500");
    assert_eq!(r["salary"], "50005.0");
    assert!(
        res.envelope.budgets.is_empty(),
        "exempt from noise, so nothing is charged"
    );
}

#[tokio::test]
async fn hidden_columns_disallowed_purposes_and_unpublished_tenants() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), Arc::default()).await;
    // A column the contract does not expose does not exist, not even in WHERE.
    for sql in [
        r#"SELECT internal_cost FROM "sales/orders""#,
        r#"SELECT COUNT(*) FROM "sales/orders" WHERE internal_cost > 0"#,
    ] {
        assert!(e.query(sql, &globex()).await.is_err(), "{sql}");
    }
    // Purpose gate.
    let m = e
        .query(
            r#"SELECT COUNT(*) FROM "sales/orders""#,
            &Caller::new("u", "globex", "marketing"),
        )
        .await;
    assert!(
        matches!(m, Err(PeqlError::Denied { ref rule, .. }) if rule == "purposes"),
        "{:?}",
        m.err()
    );
    // An unpublished tenant cannot tell the contract exists.
    let stranger = Caller::new("u", "initech", "analytics");
    let unknown = e
        .query(r#"SELECT 1 FROM "sales/nothing""#, &stranger)
        .await
        .err()
        .unwrap()
        .to_string();
    let hidden = e
        .query(r#"SELECT 1 FROM "sales/orders""#, &stranger)
        .await
        .err()
        .unwrap()
        .to_string();
    assert_eq!(
        unknown.replace("sales/nothing", "X"),
        hidden.replace("sales/orders", "X")
    );
    assert!(e.describe("sales/orders", &stranger).is_err());
    // Publishing to everyone makes it visible.
    e.publish("sales/orders", peql::store::PUBLIC).unwrap();
    assert!(e.describe("sales/orders", &stranger).is_ok());
}

#[tokio::test]
async fn budgets_run_out_and_only_readers_pay() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), Arc::default()).await;
    e.budgets().set_limit("salary", 2.0).unwrap();
    let with_salary = r#"SELECT AVG(salary) FROM "sales/orders""#;
    e.query(with_salary, &globex()).await.unwrap();
    e.query(with_salary, &globex()).await.unwrap();
    let third = e.query(with_salary, &globex()).await;
    assert!(
        matches!(third, Err(PeqlError::BudgetExhausted { .. })),
        "{:?}",
        third.err()
    );
    // A query that does not read salary costs nothing and still runs.
    e.query(r#"SELECT COUNT(*) FROM "sales/orders""#, &globex())
        .await
        .unwrap();
    // Budgets are per caller.
    e.query(
        with_salary,
        &Caller::new("user:carol", "globex", "analytics"),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn pushdown_prunes_and_the_gate_holds() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), Arc::default()).await;
    // The admit on the partition column reads only the EU partition.
    let res = e
        .query(r#"SELECT COUNT(*) FROM "sales/orders""#, &globex())
        .await
        .unwrap();
    assert_eq!(
        res.envelope.scan.rows_scanned, 10,
        "{:?}",
        res.envelope.scan
    );
    // A caller's safe predicate reaches the Parquet scan.
    let plan = e
        .explain(
            r#"SELECT order_id FROM "sales/orders" WHERE order_id > 20"#,
            &globex(),
        )
        .await
        .unwrap();
    assert!(plan.contains("GateExec: contract=sales/orders"), "{plan}");
    let scan = plan.lines().find(|l| l.contains("DataSourceExec")).unwrap();
    assert!(
        scan.contains("order_id@") && scan.contains("> 20"),
        "{plan}"
    );
    // An unselected transform is pruned: the hash of email is not computed.
    let plan = e
        .explain(r#"SELECT order_id FROM "sales/orders""#, &globex())
        .await
        .unwrap();
    assert!(!plan.contains("sha256"), "{plan}");
    // A predicate that can fail stays above the gate, so it never sees a hidden row.
    // Order 1 is in the US and hidden from globex: dividing by (order_id - 1) would fail on it.
    let res = e
        .query(
            r#"SELECT COUNT(*) FROM "sales/orders" WHERE 100 / (order_id - 1) <> 7"#,
            &globex(),
        )
        .await;
    assert!(res.is_ok(), "{:?}", res.err());
    let plan = e
        .explain(
            r#"SELECT order_id FROM "sales/orders" WHERE 100 / (order_id - 1) <> 7"#,
            &globex(),
        )
        .await
        .unwrap();
    let gate_line = plan.lines().position(|l| l.contains("GateExec")).unwrap();
    let div_line = plan.lines().position(|l| l.contains("100 /")).unwrap();
    assert!(
        div_line < gate_line,
        "the division is evaluated above the gate:\n{plan}"
    );
}

#[tokio::test]
async fn an_ungated_scan_is_refused() {
    use datafusion::physical_plan::empty::EmptyExec;
    let scan = Arc::new(peql::gate::ScanExec::new(
        "sales/orders".into(),
        Arc::new(EmptyExec::new(schema())),
    ));
    assert!(matches!(
        peql::gate::ensure_gated(scan.as_ref()),
        Err(PeqlError::Ungated(_))
    ));
    let gated = peql::gate::GateExec::new("sales/orders".into(), "h".into(), scan);
    peql::gate::ensure_gated(&gated).unwrap();
}

#[tokio::test]
async fn every_query_is_audited() {
    let dir = tempfile::tempdir().unwrap();
    let audit = Arc::new(MemoryAudit::default());
    let e = engine(dir.path(), audit.clone()).await;
    let ok = e
        .query(r#"SELECT COUNT(*) FROM "sales/orders""#, &globex())
        .await
        .unwrap();
    let _ = e
        .query(
            r#"SELECT COUNT(*) FROM "sales/orders""#,
            &Caller::new("u", "globex", "marketing"),
        )
        .await;
    let records = audit.records.lock().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].id, ok.envelope.audit_id);
    assert_eq!(records[0].outcome, Outcome::Answered);
    assert!(records[0].contracts[0].starts_with("sales/orders@1#"));
    assert!(matches!(records[1].outcome, Outcome::Refused(ref r) if r.contains("purposes")));
    assert_eq!(ok.envelope.attestation.query_sha256.len(), 64);
}

#[tokio::test]
async fn a_bundle_is_the_handoff() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), Arc::default()).await;
    let json = e
        .get("sales/orders")
        .unwrap()
        .bundle()
        .unwrap()
        .to_json()
        .unwrap();
    // A second engine over the same files, fed only the bundle.
    let other = Engine::in_memory(dir.path());
    other
        .register_bundle(&parcel_runtime::bundle::Bundle::from_json(&json).unwrap())
        .unwrap();
    other.publish("sales/orders", "globex").unwrap();
    let sql = r#"SELECT order_id, email FROM "sales/orders" ORDER BY order_id"#;
    let a = rows(&e.query(sql, &globex()).await.unwrap().batches);
    let b = rows(&other.query(sql, &globex()).await.unwrap().batches);
    assert_eq!(a, b);
    // A tampered bundle is refused: its artifacts no longer match its hash.
    let tampered = json.replace(
        "ctx.purpose in ['analytics']",
        "ctx.purpose in ['analytics', 'marketing']",
    );
    assert_ne!(tampered, json);
    let bad = parcel_runtime::bundle::Bundle::from_json(&tampered).unwrap();
    assert!(Engine::in_memory(dir.path()).register_bundle(&bad).is_err());
}

#[tokio::test]
async fn the_barrier_holds_for_a_row_level_admit() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), Arc::default()).await;
    // A second contract over the same files, admitting only large orders (ids 15..=30).
    let big = r#"
contract: sales/big
version: 1
binding: {parquet: orders/, partitioned_by: [region]}
expose: [{name: order_id, type: int64}, {name: region, type: utf8}]
rules:
  - {id: large, op: admit, expr: "row.amount >= 1500"}
"#;
    e.register_contract(big, &schema()).unwrap();
    let who = Caller::new("u", "globex", "analytics");
    let n = e
        .query(r#"SELECT COUNT(*) FROM "sales/big""#, &who)
        .await
        .unwrap();
    assert_eq!(n.envelope.rows, 1);
    assert_eq!(rows(&n.batches)[0]["count(*)"], "16");
    // Order 2 is hidden; a predicate that divides by zero on it must not run on it.
    let res = e
        .query(
            r#"SELECT COUNT(*) FROM "sales/big" WHERE 100 / (order_id - 2) <> 7"#,
            &who,
        )
        .await;
    assert!(res.is_ok(), "{:?}", res.err());
    // A safe predicate still reaches the scan alongside the admit.
    let plan = e
        .explain(
            r#"SELECT order_id FROM "sales/big" WHERE order_id < 20"#,
            &who,
        )
        .await
        .unwrap();
    let scan = plan.lines().find(|l| l.contains("DataSourceExec")).unwrap();
    assert!(
        scan.contains("predicate=") && scan.contains("< 20") && scan.contains("amount"),
        "{plan}"
    );
}

#[tokio::test]
async fn the_cache_never_crosses_callers_or_writes() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path(), Arc::default())
        .await
        .with_cache(Arc::new(peql::cache::QueryCache::new(Default::default())));
    let sql = r#"SELECT email FROM "sales/orders" ORDER BY order_id LIMIT 1"#;
    let bob = e.query(sql, &globex()).await.unwrap();
    assert!(!bob.envelope.cached);
    // Same contract context and data: served from the cache.
    let again = e.query(sql, &globex()).await.unwrap();
    assert!(again.envelope.cached);
    assert_eq!(rows(&bob.batches), rows(&again.batches));
    // The owner's answer is never the outsider's.
    let alice = e.query(sql, &acme()).await.unwrap();
    assert!(!alice.envelope.cached);
    assert_eq!(rows(&alice.batches)[0]["email"], "user1@x.io");
    // A write changes the data hash: the next answer is fresh.
    e.write("sales/orders", vec![batch()], WriteMode::Append)
        .await
        .unwrap();
    assert!(!e.query(sql, &globex()).await.unwrap().envelope.cached);
}
