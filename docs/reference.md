# Reference

The Rust crate and the Python package are both named `peql`, version 0.4.0.

## Engine

| Method | Does |
| --- | --- |
| `Engine::open(root)`, `Engine::in_memory(base)` | A workspace on disk, or in memory. |
| `register_contract(source, &schema)` | Compile a parcel document against the data's schema and store it. |
| `register_bundle(&bundle)` | Store a bundle after recompiling it to the same hash. |
| `register_function(module, &manifest, owner)` | Verify and store a tenant's WebAssembly function. |
| `publish(name, audience)`, `unpublish` | Share with a tenant, or `public`. |
| `write(name, batches, mode)` | The write path; returns a `WriteReport` with the verdict. |
| `bind_table(name, provider)`, `bind_batches(name, batches)` | Serve a contract from data you hold. |
| `validate(name)`, `validate_with(name, plan)` | The verdict and data hash; `validate_with` runs a plan from a bundle. |
| `query(sql, &caller)` | `QueryResult { batches, envelope }`. |
| `explain(sql, &caller)` | The physical plan (operators only; callers cannot `EXPLAIN`). |
| `describe(name, &caller)` | The schema a caller would see. |
| `resolve`, `view` | The resolver and view builder, for embedding. |
| `get`, `contracts`, `list_for(&caller)`, `manifest(name)` | Inspect the store. |

## Errors

`PeqlError`: `Compile`, `UnknownContract` (also for contracts the caller cannot see), `Denied`,
`NotWritten`, `NotServable`, `GuaranteeFailed`, `BudgetExhausted`, `Refused` (a statement that
is not a query), `Ungated`, `Invalid`, `DataFusion`, `Io`. `is_refusal()` separates policy
outcomes from failures.

## Envelope

| Field | Holds |
| --- | --- |
| `contracts` | Per contract: name, version, contract and compilation hashes, decisions that ran, annotated guarantees, shapes applied, whether stored flags were read. |
| `rows` | Rows returned. |
| `suppress_k` | The suppression threshold applied. |
| `budgets` | Budget left per budget charged. |
| `scan` | Rows scanned and released, bytes read, files and row groups pruned. |
| `attestation` | sha256 of the query and of the result (Arrow IPC), and the time. |
| `audit_id`, `cached` | The audit record, and whether the answer came from the cache. |

## Cargo features

| Feature | Default | Adds |
| --- | --- | --- |
| `platform` | off | Signed bundles from T03 over HTTP. |
| `lance` | off | Lance datasets, directly or through storaged. Building needs `protoc`. |

The storaged and T05 socket clients build on every Unix target.
