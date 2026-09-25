//! The split pass: row-only subtrees of mixed rules are stored at write and read at query time.

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::pretty::pretty_format_batches;
use parcel_runtime::differential::differential;
use peql::{Caller, Engine, WriteMode};

const CONTRACT: &str = r#"
contract: telco/subscribers
version: 1
binding: {parquet: subs/}
expose:
  - {name: id, type: int64}
  - {name: name, type: utf8}
  - {name: phone, type: utf8}
rules:
  - id: valid_or_cleared
    op: admit
    expr: "is_msisdn(row.phone) || ctx.clearance > 3"
  - id: region_tags
    op: admit
    expr: "row.tags.exists(t, t.startsWith('ke-')) || 'admin' in ctx.roles"
  - id: mask_name
    op: transform
    column: name
    expr: "ctx.tenant == 'safcom' ? row.name : redact(row.name)"
"#;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("phone", DataType::Utf8, true),
        Field::new(
            "tags",
            DataType::List(Field::new_list_field(DataType::Utf8, true).into()),
            true,
        ),
    ]))
}

fn batch() -> RecordBatch {
    let n = 400;
    let mut tags = ListBuilder::new(StringBuilder::new());
    for i in 0..n {
        tags.values()
            .append_value(if i % 3 == 0 { "ke-nbo" } else { "ug-kla" });
        tags.append(true);
    }
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from((0..n).collect::<Vec<i64>>())),
            Arc::new(StringArray::from(
                (0..n)
                    .map(|i| {
                        if i % 11 == 0 {
                            None
                        } else {
                            Some(format!("Subscriber {i}"))
                        }
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (0..n)
                    .map(|i| {
                        if i % 4 == 0 {
                            format!("0{}", 700000000 + i)
                        } else {
                            format!("2547{:08}", i)
                        }
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(tags.finish()),
        ],
    )
    .unwrap()
}

fn rows(r: &peql::QueryResult) -> String {
    pretty_format_batches(&r.batches).unwrap().to_string()
}

#[tokio::test]
async fn stored_and_live_agree() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::in_memory(dir.path());
    engine.register_contract(CONTRACT, &schema()).unwrap();
    let c = engine.get("telco/subscribers").unwrap().compilation.clone();
    let derived: Vec<&str> = c.contract.derived.iter().map(|d| d.cel.as_str()).collect();
    assert_eq!(derived.len(), 3, "{derived:?}");
    assert!(derived.contains(&"is_msisdn(row.phone)"));
    assert!(derived.contains(&"redact(row.name)"));
    let report = &c.contract.report;
    assert!(
        report
            .iter()
            .find(|r| r.rule == "valid_or_cleared")
            .unwrap()
            .reason
            .contains("computed at write")
    );

    engine
        .write("telco/subscribers", vec![batch()], WriteMode::Overwrite)
        .await
        .unwrap();

    let callers = [
        Caller::new("a", "safcom", "analytics"),
        Caller::new("b", "airtel", "analytics").with_roles(&["admin"]),
        {
            let mut c = Caller::new("c", "airtel", "analytics");
            c.clearance = 5;
            c
        },
    ];
    let sql = r#"SELECT id, name, phone FROM "telco/subscribers" ORDER BY id"#;
    for caller in &callers {
        let stored = engine.query(sql, caller).await.unwrap();
        assert!(stored.envelope.contracts[0].flags_materialised);
        let plan = engine.explain(sql, caller).await.unwrap();
        assert!(plan.contains("_d_"), "stored columns are read:\n{plan}");
        engine.set_use_stored(false);
        let live = engine.query(sql, caller).await.unwrap();
        assert!(!live.envelope.contracts[0].flags_materialised);
        engine.set_use_stored(true);
        assert_eq!(rows(&stored), rows(&live), "caller {}", caller.tenant);
        assert!(stored.envelope.rows > 0);
    }

    let diff = differential(&c, &batch(), &callers).await.unwrap();
    assert!(
        diff.passed(),
        "{:?}",
        &diff.mismatches[..diff.mismatches.len().min(5)]
    );
}
