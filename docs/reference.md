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
| `query(sql, &caller)` | `QueryResult { schema, batches, envelope, signature }`; `signature` is the signer's JWS when the engine has one. |
| `check(sql, &caller)` | Every check `query` makes, without reading a row: the answer's schema and the resolutions, or the refusal. |
| `authorize_write(name, &caller)` | Whether a caller may write: the contract's owner tenant may. |
| `explain(sql, &caller)` | The physical plan (operators only; callers cannot `EXPLAIN`). |
| `describe(name, &caller)` | The schema a caller would see. |
| `resolve`, `view`, `session` | The resolver, and the gated plan an out-of-core executor reads: plan `view` in `session()` and refuse the physical plan unless `gate::ensure_gated` accepts it. Shapes apply to queries over a view, not to the view. |
| `with_bindings`, `with_signer`, `with_cache`, `with_store`, `with_budgets`, `with_audit` | Replace a part: where bindings resolve, who signs envelopes, and so on. |
| `get`, `contracts`, `list_for(&caller)`, `manifest(name)` | Inspect the store. |

## Errors

`PeqlError`: `Compile`, `UnknownContract` (also for contracts the caller cannot see), `Denied`,
`NotWritten`, `NotServable`, `GuaranteeFailed`, `BudgetExhausted`, `Refused` (a statement that
is not a query), `Ungated`, `Signing` (a signer is configured and did not sign), `Invalid`,
`DataFusion`, `Io`. `is_refusal()` separates policy
outcomes from failures.

## Envelope

| Field | Holds |
| --- | --- |
| `caller` | Who asked: id, tenant, purpose. |
| `contracts` | Per contract: name, version, contract and compilation hashes, decisions that ran, annotated guarantees, shapes applied, whether stored flags were read. |
| `rows` | Rows returned. |
| `suppress_k` | The suppression threshold applied. |
| `charges` | Epsilon charged, per budget. |
| `budgets` | Budget left per budget charged. |
| `scan` | Rows scanned and released, bytes read, files and row groups pruned. |
| `attestation` | sha256 of the query and of the result (Arrow IPC), and the time. |
| `audit_id`, `cached` | The audit record, and whether the answer came from the cache. |

## Bindings

| Resolver | Reads and writes |
| --- | --- |
| `LocalParquet { base }` | Directories on the local filesystem; relative bindings resolve under `base`. The default. |
| `ObjectStoreParquet::new("s3://bucket/prefix/", store)` | A prefix in an object store. Bindings that are URLs must name that bucket; relative ones resolve under the prefix. Manifests are kept at `<binding>/_peql/manifests/`. `s3_from_env` builds the S3 store from `AWS_*` variables (feature `s3`). |

## Cargo features

| Feature | Default | Adds |
| --- | --- | --- |
| `flight` | off | Flight SQL over tonic on a listener you supply ({doc}`platform`). |
| `signed-bundle` | off | Verification of bundles signed with ECDSA P-256 by their issuer. |
| `s3` | off | The S3 store for `ObjectStoreParquet`. Other `object_store` stores work without it. |
| `lance` | off | Lance datasets from a path or URI. Building needs `protoc`. |

`SocketSigner`, the envelope signer over a Unix socket, TCP or any stream you connect, is always
built.
