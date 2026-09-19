# peQL

**Policy-Enforcing Query Engine for SQL tables and process graphs.**

peQL is a Rust engine built on Apache DataFusion. A query names a
contract-bound dataset; peQL evaluates that definition for the caller and
enforces the resulting policy inside the query plan.

It supports policy-driven access decisions, column visibility, row filtering,
value masking, optional differential-privacy noise, and governed SQL traversal
of compiled process graphs.

## Install

Rust 1.88 or newer is required.

```toml
[dependencies]
peql = { git = "https://github.com/griot-cloud/peQL" }
```

For the standalone path, create an `Engine` from JSON contracts whose bindings
point to local Parquet files:

```rust
use peql::contract_source::Caller;
use peql::engine::Engine;

let engine = Engine::from_json_contracts_dir("./contracts")?;
let rows = engine
    .query(
        r#"SELECT email, region FROM "sales/orders/v1""#,
        Caller::new("user:bob", "analytics", "globex"),
    )
    .await?;
```

Python bindings expose the same query path. Build a wheel from
`bindings/python` or use one produced by this repository's wheel workflow:

```python
import peql

engine = peql.Engine.from_json_contracts_dir("./contracts")
table = engine.query(
    'SELECT email, region FROM "sales/orders/v1"',
    peql.Caller("user:bob", "analytics", "globex"),
)
```

## Choose the right API

| API | Use it for |
| --- | --- |
| `peql::engine::Engine` | Contract-resolving Rust queries; this is the normal governed data and graph path. |
| Python `peql.Engine` | The same contract-resolving engine from Python. |
| `K04DEngine` | Lower-level Rust integrations that manage an injected bundle and direct DataFusion tables. It does not currently construct the policy-derived provider used by `Engine`. |

## Features

- `platform` — fetch and map signed T03 contract bundles; a storage-backed
  `BindingResolver` remains the embedding application's responsibility.
- `lance` — register Lance datasets through the storaged socket; requires
  `protoc` at build time.

## Documentation and development

The full documentation is organized by learning, how-to guides, reference, and
architecture explanation in [`docs/`](docs/). Build it locally with:

```bash
python -m pip install -r docs/requirements.txt
sphinx-build -W --keep-going -b html docs docs/_build/html
```

Run the Rust quality gates with:

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
```

## Status

peQL is currently version **0.3.0**. The new `peql` Rust and Python names are a
breaking change for downstream users of the predecessor API.

## License

License to be determined.
