# Query architecture

This page follows the high-level `peql::engine::Engine` path. The source code
for each stage is named so a contributor can move between this explanation and
the implementation.

## 1. Create a caller-bound session

`Engine::query_with_stats(sql, caller)` in `src/engine.rs` rejects unsafe DDL,
creates a new DataFusion session, and registers a catalog bound to that
`Caller`. It also registers graph table functions bound to the same caller.
`Engine::query` calls this method and returns only its record batches.

The caller is supplied by the embedding application. Authentication is
outside this crate; the engine acts on the caller context it receives.

## 2. Resolve each table reference

For `FROM "sales/orders/v1"`, DataFusion asks `PeqlSchemaProvider::table` in
`src/catalog.rs` for the named table. The provider calls
`ContractSource::resolve(dataset, caller)`.

- On `Deny`, planning stops with an access error.
- On `Allow`, the provider calls `BindingResolver::resolve(dataset)` and wraps
  the returned raw table in `ContractTableProvider`.
- On an unknown name, the provider reports no table. Graph function names
  must remain available to DataFusion's function registry.

`ResolvedPolicy` in `src/policy.rs` is the data passed between resolution and
execution. It includes the decision, masks, row predicate, optional noise
parameters, exposed columns, and graph edge predicate. The engine serializes
the operator fields to the JSON format consumed by its physical operators.

## 3. Build a governed scan

`ContractTableProvider::scan` in `src/contract_table_provider.rs` asks the raw
provider for all columns and no pushed filter or limit. It then wraps that
plan in the following order:

```text
raw TableProvider scan
└─ ScanMetricsExec
   └─ ContractApprovedExec
      └─ RowFilterExec
         └─ MaskingExec
            └─ LaplaceNoiseExec, when dp_columns is set
               └─ contract projection
                  └─ query projection and limit
```

The diagram reads from input at the top to output at the bottom. The engine
needs the full raw schema because a contract row predicate can reference a
column absent from the query result. `ContractApprovedExec` supplies the
marker required by downstream enforcement operators.

`MaskingExec` can change a field's Arrow type. The provider computes its
governed schema before DataFusion plans expressions, then projects only the
contract's exposed columns. `governed_session_context` omits DataFusion 47's
physical `ProjectionPushdown` rule because that rule can move projections
through a type-changing mask and produce a schema mismatch.

## 4. Execute and account

The engine creates a physical plan, collects Arrow `RecordBatch`es, and walks
the plan for scan metrics. `QueryStats` reports rows scanned and Arrow
in-memory bytes from `ScanMetricsExec`, before contract filtering and result
limits. These counters are not storage I/O bytes.

The standalone Parquet binding (`src/binding.rs`) reads the entire file into a
DataFusion `MemTable`. There is no streaming local-file reader in this path.

## Alternative sources

`JsonContractSource` (`src/contract_source.rs`) supplies both contract
resolution and local binding. With the `platform` feature,
`PlatformBundleSource` fetches a T03 bundle over HTTP and maps its SQL and
Rego content to `ResolvedPolicy`. Signature verification occurs only when a
`VerifyingKey` is configured. This adapter does not provide a storage
`BindingResolver`; an application must supply one. The mapping recognizes
the current T03 template forms, so it is not a general SQL or Rego evaluator.

## Graph execution

`src/graph/functions.rs` registers SQL table functions per caller-bound
session. `GraphSession::governed` resolves the contract before opening the
snapshot, then loads and verifies the snapshot through `src/graph/bundle.rs`.
It caches immutable raw data and separately caches compiled visibility masks
by policy fingerprint. `src/graph/traverse.rs` consults those masks while
walking the graph. Output assembly applies masking before returning a
DataFusion table. The graph loader checks format, file digests, and structural
invariants; this standalone path does not verify a signed certificate.

## The other Rust entry point

`K04DEngine` in `src/lib.rs` creates a DataFusion session with direct table
registration methods. Its `query` method checks for an injected bundle handle
and rejects unsafe DDL. It does not resolve that bundle into a policy or wrap
directly registered tables with `ContractTableProvider`. Applications using
this API must account for that difference; the high-level enforcement path
described above is specific to `Engine`.
