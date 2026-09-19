# API and behavior reference

The Rust crate is named `peql` and its current package version is `0.3.0`.
The Python package and import name are `peql`.

## Query entry points

| Entry point | Inputs | Output | Enforcement path |
| --- | --- | --- | --- |
| `peql::engine::Engine::query` | SQL and `Caller` | `Vec<RecordBatch>` | Resolves each dataset through `ContractSource`, then uses a governed provider. |
| `Engine::query_with_stats` | SQL and `Caller` | `QueryOutcome { batches, stats }` | Same query path, plus pre-enforcement scan counters. |
| Python `peql.Engine.query` | SQL and `Caller` | `pyarrow.Table` | Calls Rust `Engine::query` through PyO3 and Arrow IPC. |
| `peql::K04DEngine::query` | SQL after bundle injection | `Vec<RecordBatch>` | Checks bundle presence and DDL, then runs against tables registered directly in its session. |

`K04DEngine` does **not** resolve the injected bundle into a policy or install
the high-level row filter, mask, and noise stack for directly registered
tables. This matters when choosing a Rust integration path.

## Core Rust types

| Type or trait | Role |
| --- | --- |
| `Caller` | Identity and purpose supplied by the embedding application. |
| `ContractSource` | Resolves a dataset and caller into `ResolvedPolicy`. |
| `BindingResolver` | Returns a raw `TableProvider` or graph snapshot directory. |
| `ResolvedPolicy` | Allow/deny decision and requested transformations. |
| `ContractTableProvider` | Builds governed physical scans. |
| `QueryStats` | Raw rows and Arrow in-memory bytes scanned. |

## Cargo features

| Feature | Default | Effect |
| --- | --- | --- |
| `platform` | Off | Enables HTTP T03 bundle source and ECDSA P-256 verification when configured with a key. |
| `lance` | Off | Enables the storaged-backed Lance provider; building requires `protoc`. |

## Current boundaries

- The standalone Parquet resolver loads a whole file into memory.
- `Engine` uses a permissive tracker for `dp_columns`; it adds noise without
  enforcing a cross-query privacy budget.
- The standalone graph loader verifies digests and structure, but not a signed
  certificate.
- `AttestationExec` exists as a lower-level operator; `Engine::query` does
  not automatically attach signed attestation envelopes.
- Platform bundle mapping parses the SQL/Rego forms currently emitted by T03.
  A configured verifying key is required for signature verification.
