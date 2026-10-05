//! Engines on one disk registering the same contract version at the same time each succeed:
//! several processes may open one workspace's disk and register the same contract at once.

use std::sync::{Arc, Barrier};

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use peql::Engine;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
    ])
}

const CONTRACT: &str = "contract: acme/orders\nversion: 1\nowner: acme\nbinding: {parquet: acme_orders/}\nexpose:\n  - {name: id, type: int64}\n  - {name: amount, type: int64}\n";

#[test]
fn engines_on_one_disk_register_the_same_version_at_once() {
    let dir = tempfile::tempdir().unwrap();
    for round in 0..20 {
        let writers = 8;
        let start = Arc::new(Barrier::new(writers));
        let handles: Vec<_> = (0..writers)
            .map(|_| {
                let root = dir.path().to_path_buf();
                let start = start.clone();
                std::thread::spawn(move || {
                    let e = Engine::open(&root).unwrap();
                    start.wait();
                    e.register_contract(CONTRACT, &schema()).map(|_| ())
                })
            })
            .collect();
        for h in handles {
            h.join()
                .unwrap()
                .unwrap_or_else(|e| panic!("round {round}: {e}"));
        }
    }
    let e = Engine::open(dir.path()).unwrap();
    assert_eq!(e.get("acme/orders").unwrap().name(), "acme/orders");
    let leftovers: Vec<_> = std::fs::read_dir(dir.path().join("_peql/contracts/acme__orders"))
        .unwrap()
        .map(|d| d.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| !n.ends_with(".peql.json"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "no temporary file is left: {leftovers:?}"
    );
}
