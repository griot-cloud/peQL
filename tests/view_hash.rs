//! A view's hash is what the view is: the contract compiled, the table's
//! columns and the unbound plan; never the data, never schema metadata.

use std::collections::HashMap;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use peql::{Engine, compiled_view_hash, view_hash};

const ORDERS: &str = r#"
contract: sales/orders
version: 1
binding: {parquet: orders/}
expose:
  - {name: order_id, type: int64}
  - {name: amount, type: float64}
rules:
  - {id: positive, op: assert, expr: "row.amount > 0.0", on_fail: drop}
"#;

fn table() -> Schema {
    Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, true),
    ])
}

fn bundle(doc: &str) -> parcel_runtime::bundle::Bundle {
    let dir = tempfile::tempdir().unwrap();
    Engine::in_memory(dir.path())
        .register_contract(doc, &table())
        .unwrap()
        .bundle()
        .unwrap()
}

#[test]
fn the_same_contract_over_the_same_columns_is_the_same_view() {
    let b = bundle(ORDERS);
    let first = view_hash(&b, &table()).unwrap();
    assert!(first.starts_with("sha256:") && first.len() == 71, "{first}");
    assert_eq!(view_hash(&bundle(ORDERS), &table()).unwrap(), first);
    // The registered compilation names the same view as its bundle.
    let dir = tempfile::tempdir().unwrap();
    let registered = Engine::in_memory(dir.path())
        .register_contract(ORDERS, &table())
        .unwrap();
    assert_eq!(
        compiled_view_hash(&registered.compilation, &table()).unwrap(),
        first
    );
}

#[test]
fn schema_metadata_is_not_the_view() {
    let b = bundle(ORDERS);
    let renumbered = table().with_metadata(HashMap::from([("schema-id".into(), "4".into())]));
    let fields: Vec<Field> = table()
        .fields()
        .iter()
        .map(|f| {
            f.as_ref()
                .clone()
                .with_metadata(HashMap::from([("field_id".into(), "9".into())]))
        })
        .collect();
    assert_eq!(
        view_hash(&b, &renumbered).unwrap(),
        view_hash(&b, &table()).unwrap()
    );
    assert_eq!(
        view_hash(&b, &Schema::new(fields)).unwrap(),
        view_hash(&b, &table()).unwrap()
    );
}

#[test]
fn another_column_type_rule_or_exposed_column_is_another_view() {
    let b = bundle(ORDERS);
    let same = view_hash(&b, &table()).unwrap();
    let retyped = Schema::new(vec![
        Field::new("order_id", DataType::Int32, false),
        Field::new("amount", DataType::Float64, true),
    ]);
    assert_ne!(view_hash(&b, &retyped).unwrap(), same, "a column's type");
    let widened = Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, true),
        Field::new("note", DataType::Utf8, true),
    ]);
    assert_ne!(view_hash(&b, &widened).unwrap(), same, "a column added");
    let rule = ORDERS.replace("row.amount > 0.0", "row.amount > 1.0");
    assert_ne!(view_hash(&bundle(&rule), &table()).unwrap(), same, "a rule");
    let exposed = ORDERS.replace("  - {name: amount, type: float64}\n", "");
    assert_ne!(
        view_hash(&bundle(&exposed), &table()).unwrap(),
        same,
        "the exposed columns"
    );
}
