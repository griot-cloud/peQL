# Query from Rust and Python

Use `peql::engine::Engine` for contract-resolving Rust queries. The Python
binding wraps this API. `K04DEngine` is a separate lower-level Rust API;
{doc}`reference` describes its behavior.

## Rust: load contracts from a directory

Add the crate to your Cargo manifest:

```toml
[dependencies]
peql = { git = "https://github.com/griot-cloud/peQL" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Then create an engine and supply caller context for each query:

```rust
use peql::contract_source::Caller;
use peql::engine::Engine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::from_json_contracts_dir("./contracts")?;
    let caller = Caller::new("user:bob", "analytics", "globex");
    let batches = engine
        .query(r#"SELECT email FROM "sales/orders/v1""#, caller)
        .await?;
    println!("{batches:?}");
    Ok(())
}
```

`from_json_contracts_dir` loads the directory's `*.json` files at engine
construction. `from_json_contracts` accepts JSON strings instead. A quoted
dataset name is necessary when its name contains `/`.

## Rust: include scan statistics

```rust
let outcome = engine.query_with_stats(sql, caller).await?;
let batches = outcome.batches;
let bytes = outcome.stats.bytes_scanned;
let rows = outcome.stats.rows_scanned;
```

The counters measure raw Arrow batches before row filtering, masking, and
query `LIMIT`. `bytes_scanned` is Arrow in-memory batch size, not physical
bytes read from Parquet or storage. A query that scans no table reports zero.

## Python

Build the binding locally from `bindings/python` with maturin, or install a
wheel built by this repository's wheel workflow:

```bash
cd bindings/python
python -m pip install maturin
maturin develop --release
```

```python
import peql

engine = peql.Engine.from_json_contracts_dir("./contracts")
table = engine.query(
    'SELECT email FROM "sales/orders/v1"',
    peql.Caller("user:bob", "analytics", "globex"),
)
print(table)  # pyarrow.Table
```

The Python wrapper returns a `pyarrow.Table`. It currently exposes `query`,
not Rust's `query_with_stats`.

## Graphs

Use a contract with `binding.graph_snapshot` pointing to a local snapshot
directory, then call the SQL table functions:

```sql
SELECT name, kind
FROM graph_neighbors(
  'process-graphs/zijani-operations/v1',
  'Collection quarantined; jericans to segregated disposal'
)
```

Run `cargo run --example graph_query` for a working dataset and contract.
{doc}`GRAPH-QUERY` lists each function's positional arguments and explains
node and edge visibility.

## Platform contract source

Enable the `platform` Cargo feature to fetch T03 bundles over HTTP:

```toml
peql = { git = "https://github.com/griot-cloud/peQL", features = ["platform"] }
```

```rust
use std::sync::Arc;
use peql::engine::Engine;
use peql::platform::PlatformBundleSource;

let source = PlatformBundleSource::new("https://t03.internal")
    .with_verifying_key(t03_public_key)
    .with_auth("Authorization", "Bearer …");
let engine = Engine::new(Arc::new(source), binding);
```

The embedding application supplies `binding: Arc<dyn BindingResolver>` for
the physical data. `with_verifying_key` enables ECDSA P-256 verification;
without a key, the source does not verify bundle signatures. The adapter
derives a `ResolvedPolicy` from the bundle's SQL templates and Rego purpose
gate. See {doc}`ARCHITECTURE` for the mapping's limits.
