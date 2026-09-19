# Run your first query

This tutorial uses the standalone JSON and Parquet path. It runs locally with
Rust 1.88 or newer.

## 1. Run the included example

```bash
git clone https://github.com/griot-cloud/peQL.git peql
cd peql
cargo run --example contract_query
```

The example creates a small Parquet file and a JSON contract in a temporary
directory. It sends the same SQL to peQL three times:

| Caller | Engine result |
| --- | --- |
| Tenant `globex`, purpose `analytics` | EU rows; `email` replaced with a SHA-256 digest. |
| Tenant `acme`, purpose `analytics` | All rows and original email values. |
| Tenant `globex`, purpose `marketing` | Query rejected because the purpose is not listed. |

Read [the example source](https://github.com/griot-cloud/peQL/blob/main/examples/contract_query.rs)
to see the complete setup, including Parquet creation.

## 2. Point a contract at your data

Create `contracts/orders.json`. Change the Parquet path and column names to
match your file.

```json
{
  "contract_id": "sales_orders_v1",
  "version": "1",
  "dataset": "sales/orders/v1",
  "binding": { "parquet": "/absolute/path/to/orders.parquet" },
  "owner_tenant": "acme",
  "purposes": ["analytics"],
  "columns": [
    { "name": "order_id" },
    { "name": "email", "mask": "hash_sha256" },
    { "name": "region" }
  ],
  "row_filter": "region = 'EU'"
}
```

The engine exposes only the listed columns. It applies `row_filter` and
`email` masking for non-owner tenants. It checks the purpose for every caller,
including the owner.

## 3. Query through the high-level Rust API

```rust
use peql::contract_source::Caller;
use peql::engine::Engine;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::from_json_contracts_dir("./contracts")?;
    let rows = engine
        .query(
            r#"SELECT order_id, email FROM "sales/orders/v1" ORDER BY order_id"#,
            Caller::new("user:bob", "analytics", "globex"),
        )
        .await?;
    println!("{rows:?}");
    Ok(())
}
```

The dataset name is a quoted SQL identifier because it contains `/`. The
result is `Vec<RecordBatch>`; the caller's application decides how to display
or serialize it.

Continue with {doc}`USAGE` for Python and graph queries, or
{doc}`CONTRACT-FORMAT` for every JSON field.
