# Use peQL

peQL has three interfaces over the same engine: the `peql` command, the Rust crate, and a
Python package. Contracts are parcel documents in all three.

## The command line

```text
peql [--root DIR] <command>
  register  CONTRACT [--schema SAMPLE]     a parcel bundle, or a YAML/JSON contract and a sample
  write     CONTRACT --input FILE [--append]
  validate  NAME
  query     SQL --caller FILE [--format table|json|arrow|parquet] [--out FILE] [--envelope]
  describe  NAME --caller FILE
  list
  publish   NAME --to TENANT|public [--revoke]
  budget    NAME --limit EPSILON
  function  register MODULE --manifest FILE --owner TENANT | list
```

A workspace is a directory: contracts, functions, budgets and the audit log live under
`<root>/_peql/`, and relative bindings resolve under the root. Callers can be given as a YAML
file or with `--tenant`, `--purpose`, `--role`, `--clearance`, `--classification` and `--now`.
CSV inputs take `--type column=type` to override inferred types.

## Rust

```toml
[dependencies]
peql = { git = "https://github.com/griot-cloud/peql" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
use peql::{Caller, Engine, WriteMode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::open("./workspace")?;
    engine.register_contract(&std::fs::read_to_string("orders.yaml")?, &schema)?;
    engine.write("sales/orders", batches, WriteMode::Overwrite).await?;
    engine.publish("sales/orders", "globex")?;

    let caller = Caller::new("user:bob", "globex", "analytics");
    let result = engine
        .query(r#"SELECT region, COUNT(*) FROM "sales/orders" GROUP BY region"#, &caller)
        .await?;
    println!("{} rows; contracts {:?}", result.envelope.rows, result.envelope.contracts);
    Ok(())
}
```

`Engine::open` persists everything under the root; `Engine::in_memory(base)` keeps contracts,
budgets and the audit log in memory. Useful builders: `with_cache` (a result cache that never
crosses callers, see {doc}`concepts`), `with_budgets`, `with_audit`, `with_store`,
`with_bindings`. To serve a contract from data you already hold, call `bind_table` or
`bind_batches` instead of `write`.

Refusals are errors you can tell apart: `PeqlError::is_refusal()` is true for denials,
unservable data, failed guarantees, spent budgets, refused statements, and contracts the caller
cannot see.

## Python

```bash
pip install maturin && cd bindings/python && maturin develop --release
```

```python
import peql, pyarrow as pa

engine = peql.Engine.open("./workspace")
engine.register(open("orders.yaml").read(), schema=orders.schema)
engine.write("sales/orders", orders)
engine.publish("sales/orders", "globex")

table = engine.query(
    'SELECT region, COUNT(*) AS n FROM "sales/orders" GROUP BY region',
    peql.Caller("user:bob", "analytics", "globex"),
)
table, envelope = engine.query_with_envelope(sql, caller)
```

`peql.Caller(id, purpose, tenant, tier=None, classification=None, roles=None,
clearance=None)` keeps 0.3's argument order. A refusal raises `peql.Refused`.
