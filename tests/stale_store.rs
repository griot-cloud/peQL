//! Stored contracts that no longer load are set aside, reported and never served; the rest load.

use std::path::{Path, PathBuf};

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use peql::{Engine, PeqlError};

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ])
}

fn contract(name: &str) -> String {
    format!(
        "contract: {name}\nversion: 1\nowner: acme\nbinding: {{parquet: {}/}}\nexpose:\n  - {{name: id, type: int64}}\n  - {{name: amount, type: int64}}\n",
        name.replace('/', "_")
    )
}

fn stored_path(root: &Path, name: &str) -> PathBuf {
    root.join("_peql/contracts")
        .join(name.replace('/', "__"))
        .join("v0000000001.peql.json")
}

/// Rewrite a stored contract as if another version of parcel had compiled it.
fn spoil_version(path: &Path) {
    let mut v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let mut compiled: serde_json::Value =
        serde_json::from_str(v["compiled"].as_str().unwrap()).unwrap();
    compiled["parcel_version"] = "0.0.0".into();
    v["compiled"] = serde_json::to_string(&compiled).unwrap().into();
    std::fs::write(path, serde_json::to_string(&v).unwrap()).unwrap();
}

/// A workspace holding `acme/good`, `acme/old` (compiled by another parcel, published to
/// `beta`) and `acme/broken` (not a stored contract).
fn seeded() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(dir.path()).unwrap();
    e.register_contract(&contract("acme/good"), &schema())
        .unwrap();
    e.register_contract(&contract("acme/old"), &schema())
        .unwrap();
    e.publish("acme/old", "beta").unwrap();
    drop(e);
    spoil_version(&stored_path(dir.path(), "acme/old"));
    let broken = stored_path(dir.path(), "acme/broken");
    std::fs::create_dir_all(broken.parent().unwrap()).unwrap();
    std::fs::write(&broken, "{ not a bundle").unwrap();
    dir
}

#[test]
fn open_serves_what_verifies_and_reports_the_rest() {
    let dir = seeded();
    let e = Engine::open(dir.path()).unwrap();

    assert_eq!(e.get("acme/good").unwrap().name(), "acme/good");
    assert!(matches!(
        e.get("acme/old"),
        Err(PeqlError::UnknownContract(_))
    ));
    assert!(e.store().version("acme/old", 1).is_none());
    assert!(matches!(
        e.get("acme/broken"),
        Err(PeqlError::UnknownContract(_))
    ));
    let listed: Vec<String> = e
        .store()
        .list()
        .iter()
        .map(|r| r.name().to_owned())
        .collect();
    assert_eq!(listed, vec!["acme/good".to_owned()]);

    let mut stale = e.stale();
    stale.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(stale.len(), 2, "{stale:?}");
    assert_eq!(stale[0].name, "acme/broken");
    assert_eq!(stale[0].path, stored_path(dir.path(), "acme/broken"));
    assert_eq!(stale[0].version, Some(1));
    assert_eq!(stale[1].name, "acme/old");
    assert_eq!(stale[1].path, stored_path(dir.path(), "acme/old"));
    assert_eq!(stale[1].version, Some(1));
    assert!(
        stale[1].cause.contains("compiled by parcel 0.0.0"),
        "{}",
        stale[1].cause
    );
}

#[test]
fn registering_over_a_stale_contract_replaces_it() {
    let dir = seeded();
    let e = Engine::open(dir.path()).unwrap();
    // The publication survives the stale contract.
    assert!(e.store().audiences("acme/old").contains("beta"));

    e.register_contract(&contract("acme/old"), &schema())
        .unwrap();
    assert_eq!(e.get("acme/old").unwrap().name(), "acme/old");
    let names: Vec<String> = e.stale().into_iter().map(|s| s.name).collect();
    assert_eq!(names, vec!["acme/broken".to_owned()]);
    assert!(e.store().audiences("acme/old").contains("beta"));

    // The replacement is what the disk now holds.
    let reopened = Engine::open(dir.path()).unwrap();
    assert!(reopened.get("acme/old").is_ok());
    assert!(reopened.store().audiences("acme/old").contains("beta"));
    let names: Vec<String> = reopened.stale().into_iter().map(|s| s.name).collect();
    assert_eq!(names, vec!["acme/broken".to_owned()]);
}

#[test]
fn a_corrupt_stored_contract_is_reported_not_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let path = stored_path(dir.path(), "acme/broken");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, [0xff, 0xfe, 0x00]).unwrap();

    let e = Engine::open(dir.path()).unwrap();
    let stale = e.stale();
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].path, path);
    assert!(!stale[0].cause.is_empty());
    assert!(e.store().list().is_empty());
}

#[test]
fn an_engine_in_memory_has_nothing_stale() {
    let dir = tempfile::tempdir().unwrap();
    assert!(Engine::in_memory(dir.path()).stale().is_empty());
}
