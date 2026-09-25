//! Tenants' WebAssembly functions: registered, pinned, sandboxed, used by contracts.

use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parcel_core::registry::FunctionManifest;
use parcel_runtime::differential::differential;
use peql::{Caller, Engine, PeqlError as EngineError, WriteMode};

const MODULE: &[u8] = include_bytes!("fixtures/meter_serial.wasm");

fn manifest(name: &str, sig: &str) -> FunctionManifest {
    yaml_serde::from_str(&format!(
        "name: {name}\nversion: 1\nsignatures: [\"{sig}\"]\n"
    ))
    .unwrap()
}

fn register_all(engine: &Engine) {
    engine
        .register_function(
            MODULE,
            &manifest("is_meter_serial", "(string) -> bool"),
            "kplc",
        )
        .unwrap();
    engine
        .register_function(MODULE, &manifest("units", "(int, int) -> int"), "kplc")
        .unwrap();
    engine
        .register_function(MODULE, &manifest("county", "(string) -> string"), "kplc")
        .unwrap();
}

const TOKENS: &str = r#"
contract: kplc/tokens
version: 1
owner: kplc
binding: {parquet: tokens/}
expose:
  - {name: token_id, type: int64}
  - {name: meter, type: utf8}
  - {name: amount_cents, type: int64}
extensions:
  row: {units: int64}
enrich:
  - {field: units, expr: "units(row.amount_cents, row.tariff_cents)"}
rules:
  - {id: valid_meter, op: assert, expr: "is_meter_serial(row.meter)", on_fail: drop}
  - {id: some_units, op: assert, expr: "row.other.units > 0", on_fail: report}
  - {id: own_county, op: admit, expr: "county(row.meter) == ctx.tier || 'admin' in ctx.roles"}
  - {id: mask_meter, op: transform, column: meter, expr: "'admin' in ctx.roles ? row.meter : county(row.meter)"}
"#;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("token_id", DataType::Int64, false),
        Field::new("meter", DataType::Utf8, true),
        Field::new("amount_cents", DataType::Int64, true),
        Field::new("tariff_cents", DataType::Int64, true),
    ]))
}

fn batch() -> RecordBatch {
    let n = 240i64;
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from((0..n).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (0..n)
                    .map(|i| match i % 9 {
                        0 => None,
                        1 => Some("MK47ABC".to_owned()),
                        _ => Some(format!(
                            "MK{}{:08}",
                            ["47", "01", "30"][(i % 3) as usize],
                            i
                        )),
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..n).map(|i| Some(5_000 + i * 250)).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..n)
                    .map(|i| if i % 11 == 0 { Some(0) } else { Some(2_300) })
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn caller(county: &str) -> Caller {
    let mut c = Caller::new("u", "kplc", "analytics");
    c.tier = county.into();
    c
}

#[tokio::test]
async fn user_functions_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    register_all(&engine);
    engine
        .register_contract(TOKENS, &schema())
        .unwrap_or_else(|e| panic!("{e}"));
    let comp = engine.get("kplc/tokens").unwrap().compilation.clone();
    assert_eq!(comp.contract.functions.len(), 3);
    assert!(comp.contract.functions.iter().all(|p| p.hash.len() == 64));

    let report = engine
        .write("kplc/tokens", vec![batch()], WriteMode::Overwrite)
        .await
        .unwrap();
    let invalid_meters = (0..240).filter(|i| i % 9 <= 1).count() as i64;
    assert_eq!(report.verdict.failures["valid_meter"], invalid_meters);
    assert_eq!(
        report.verdict.failures["some_units"],
        (0..240).filter(|i| i % 11 == 0).count() as i64
    );

    // Each county's analyst sees its own meters, masked to the county code.
    let res = engine
        .query(
            r#"SELECT meter, COUNT(*) AS n FROM "kplc/tokens" GROUP BY meter ORDER BY meter"#,
            &caller("47"),
        )
        .await
        .unwrap();
    let pretty = datafusion::arrow::util::pretty::pretty_format_batches(&res.batches)
        .unwrap()
        .to_string();
    assert!(pretty.contains("| 47    |"), "{pretty}");
    assert_eq!(res.envelope.rows, 1);

    // Interpreter and DataFusion call the same module and agree, row by row.
    let admin = Caller::new("a", "kplc", "analytics").with_roles(&["admin"]);
    let diff = differential(&comp, &batch(), &[caller("47"), admin])
        .await
        .unwrap();
    assert!(
        diff.passed(),
        "{:#?}",
        &diff.mismatches[..diff.mismatches.len().min(5)]
    );
    assert!(diff.evaluations > 1500);

    // A bundle carries the modules; a verifier recompiles and checks it without the workspace.
    let bundle = engine.get("kplc/tokens").unwrap().bundle().unwrap();
    assert_eq!(bundle.functions.len(), 3);
    parcel_runtime::bundle::Bundle::from_json(&bundle.to_json().unwrap())
        .unwrap()
        .verify()
        .unwrap();

    // A reopened workspace reloads the functions from storage.
    let reopened = Engine::open(dir.path()).unwrap();
    assert_eq!(reopened.functions().list().len(), 3);
}

#[tokio::test]
async fn another_tenants_contract_cannot_call_them() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    register_all(&engine);
    let other = TOKENS
        .replace("owner: kplc", "owner: rival")
        .replace("contract: kplc/tokens", "contract: rival/tokens");
    match engine.register_contract(&other, &schema()) {
        Err(EngineError::Compile(d)) => assert!(
            d.iter()
                .any(|d| d.message.contains("not in the function registry")),
            "{d:?}"
        ),
        other => panic!("{:?}", other.err()),
    }
    // Nor can a tenant register, in the same workspace, a function name another tenant owns.
    let e = engine
        .register_function(MODULE, &manifest("county", "(string) -> string"), "rival")
        .err()
        .unwrap();
    assert!(
        e.to_string().contains("already registered by `kplc`"),
        "{e}"
    );
}

#[test]
fn bad_modules_and_manifests_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::open(dir.path()).unwrap();
    let err = |r: Result<_, EngineError>| r.err().map(|e| e.to_string()).unwrap_or_default();
    // Not WebAssembly.
    assert!(
        err(engine.register_function(b"nope", &manifest("x", "(int) -> int"), "t"))
            .contains("not a WebAssembly module")
    );
    // A name the module does not export.
    assert!(
        err(engine.register_function(MODULE, &manifest("absent", "(int) -> int"), "t"))
            .contains("parcel_fn_absent")
    );
    // A signature that does not match the function: the smoke batch catches it.
    assert!(
        err(engine.register_function(MODULE, &manifest("is_meter_serial", "(int) -> bool"), "t"))
            .contains("smoke batch")
    );
    // A module with an import: minimal hand-written module importing env.clock.
    let importing: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // magic, version
        0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // type section: () -> ()
        0x02, 0x0d, 0x01, 0x03, b'e', b'n', b'v', 0x05, b'c', b'l', b'o', b'c', b'k', 0x00,
        0x00, // import env.clock
    ];
    assert!(
        err(engine.register_function(importing, &manifest("x", "(int) -> int"), "t"))
            .contains("imports `env::clock`")
    );
}
