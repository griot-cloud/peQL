# Rust

Embed `peql::Engine` when your application needs to manage contracts, supply caller context or work directly with Arrow batches and DataFusion tables.

## Add the dependencies

peQL requires Rust 1.94 or newer. Add these dependencies to your application's `Cargo.toml`:

```toml
[dependencies]
peql = { git = "https://github.com/griot-cloud/peql", tag = "v0.4.0" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Query an existing workspace

After completing the {doc}`quickstart`, use this as `src/main.rs` and run your application from the directory containing `peql-demo`:

```rust
use peql::{Caller, Engine};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::open("peql-demo")?;
    // Rust uses the order: ID, tenant, purpose.
    let caller = Caller::new("globex-analyst", "globex", "reporting");
    let result = engine.query(
        r#"SELECT order_id, amount FROM "sales/orders" ORDER BY order_id"#,
        &caller,
    ).await?;

    println!("{} rows returned", result.envelope.rows);
    for batch in result.batches {
        println!("{batch:?}");
    }
    Ok(())
}
```

`QueryResult` contains Arrow record batches and an `Envelope` describing the execution. The example returns one row: order 2 with amount 80.

## Register or supply data

Call `register_contract(source, &schema)` with YAML/JSON text and an Arrow schema before using a new contract. Then choose how the data is supplied:

| Method | Use it when |
| --- | --- |
| `write(name, batches, WriteMode::Overwrite)` | You want peQL to replace the bound Parquet data. |
| `write(name, batches, WriteMode::Append)` | You want to add batches to the bound data. |
| `bind_batches(name, batches)` | The data is already in memory. |
| `bind_table(name, provider)` | You have a DataFusion `TableProvider`. |

The binding methods validate the supplied table and keep its manifest in memory. They do not write Parquet. `write` requires a file binding that points to a directory, rather than a single file or a bound table.

Use `register_bundle(&bundle)` for a compiled parcel bundle. Use `publish(name, tenant)` when callers from another tenant need to discover an owned contract.

## Configure the engine

`Engine::open(root)` persists contracts, functions, budgets and the audit log. `Engine::in_memory(base)` keeps that state in memory; it can still read and write data files under `base`.

Builder methods let an application replace individual components:

| Builder | Component |
| --- | --- |
| `with_store` | Contract storage and publication records. |
| `with_bindings` | Resolution of a contract's data binding. |
| `with_budgets` | Privacy budget ledger. |
| `with_audit` | Audit destination. |
| `with_cache` | Optional query result cache. |

The default engine has no result cache. See {doc}`execution` for what a cached result depends on.

## Handle refusals

Engine methods return `peql::Result<T>`. For a query error, `error.is_refusal()` distinguishes policy outcomes from execution failures. Match individual `PeqlError` variants when your application needs a specific response; see {doc}`reference`.

Authentication, endpoint access and permissions for management operations belong in your application. Supply a trusted `Caller` to `query`; expose registration, publication and writes only to the operators who should control them.

For the complete Rust API, generate local API documentation from the peQL repository:

```bash
cargo doc --no-deps --open
```
