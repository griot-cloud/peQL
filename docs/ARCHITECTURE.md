# Architecture

```text
                   caller (SQL + Caller)
                           │
                     guard (one query)
                           │
  contract store ──▶  resolver: decide · guarantee · shapes     ◀── manifests
  (parcel bundles)         │
  function store ──▶  view builder: scan · filter · project · bind ctx · Gate
  (tenant wasm)            │
                     DataFusion: plan · optimise · execute
                     + gate barrier  + shapes (parcel_runtime::shape)  + budgets
                           │
                     envelope · attestation · audit log
```

| Module | Job |
| --- | --- |
| `store` | Compiled contracts by name and version, and who they are published to. `DirStore` keeps parcel bundles on disk and verifies each by recompiling when it opens; `MemoryStore` is for tests and embedding. |
| `functions` | Tenants' WebAssembly functions, verified by parcel-runtime at registration and stored by owner. |
| `binding` | A contract's binding as a DataFusion table: a streaming listing table over Parquet with hive partitions. Also reads footers for the manifest. |
| `manifest` | What one contract knows about its data: statistics, the verdict, per-file flag status, contract hash, data hash. |
| `engine` | Registration, the write path, validation, resolution, views, queries. |
| `gate` | The `Gate` node, `GateExec`, the barrier rule, and `ScanExec`, which marks and counts every scan of contract data. |
| `guard` | The parse-level and plan-level statement checks. |
| `budget` | The privacy ledger, in memory or in a file. |
| `audit` | One record per query, in memory or as JSON lines. |
| `envelope` | Resolutions, scan statistics, attestation hashes. |
| `cache` | The result cache and its key. |
| `format` | Results as Arrow IPC, Parquet or JSON lines. |
| `k04d`, `pool` | The platform's tenant engine and its worker pool. |
| `platform` | Signed parcel bundles from T03 (feature `platform`). |
| `storaged_client`, `t05_client`, `lance_table` | Platform sockets and Lance datasets (feature `lance`). |

What peQL does not contain: a contract language, a rule evaluator, masking or filtering code of
its own. Those are parcel's; see {doc}`parcel-and-peql`.
