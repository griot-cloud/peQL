# Reference

Use this page to look up commands, caller fields, result metadata and common failures. For complete examples, see {doc}`quickstart`, {doc}`python` and {doc}`rust`.

## Commands

```text
peql [--root DIR] COMMAND
```

`--root` selects the workspace and defaults to the current directory. Use `peql COMMAND --help` for all options on a command.

| Command | Purpose |
| --- | --- |
| `register FILE --schema SAMPLE` | Register a YAML/JSON contract against a CSV or Parquet sample. |
| `register BUNDLE` | Register a parcel bundle; no schema argument needed. |
| `write NAME_OR_FILE --input FILE` | Write CSV/Parquet under a contract; replaces existing data by default. |
| `validate NAME` | Print the validation verdict as JSON. |
| `query SQL` | Execute one read-only SQL query. |
| `describe NAME` | Show the columns exposed to a caller. |
| `list` | Show registered contracts, publication and data status for the operator. |
| `publish NAME --to TENANT` | Share contract visibility; use `public` for every tenant. |
| `budget NAME --limit EPSILON` | Set a named privacy budget's limit per caller. |
| `function register MODULE --manifest FILE --owner TENANT` | Register a parcel WebAssembly function. |
| `function list` | List registered functions. |

### Write and registration options

- `--append` on `write` adds records instead of replacing data.
- `--type COLUMN=TYPE` on `register` or `write` overrides CSV type inference. Repeat it for multiple columns.
- `--revoke` on `publish` withdraws the specified audience.

### Query options

| Option | Effect |
| --- | --- |
| `--format table` | Human-readable table; the default. |
| `--format json` | One JSON object per row. |
| `--format arrow` | Arrow IPC file. |
| `--format parquet` | Parquet file. |
| `--out PATH` | Write results to a file instead of standard output. |
| `--envelope` | Print query metadata as JSON to standard error. |
| `--explain` | Show the physical plan without executing it; intended for operators. |

SQL `EXPLAIN` is refused through the ordinary query interface. `--explain` is a separate operator action and can expose implementation details.

### Caller options

`query` and `describe` accept `--caller FILE` for a YAML caller document. Explicit flags override file values.

| Flag | Context supplied |
| --- | --- |
| `--id` | Caller ID. |
| `--tenant` | Organisation or tenant name. |
| `--purpose` | Purpose of the query. |
| `--role` | Role; repeat for several roles. Supplied roles replace the file's role list. |
| `--clearance` | Integer clearance level. |
| `--tier` | Tier name. |
| `--classification` | Classification name. |
| `--now` | Query time in RFC 3339 format; defaults to the current time. |

Without a caller file or overrides, the CLI uses ID `cli`, an empty tenant and an empty purpose. Contracts that require specific values may refuse that caller.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | The command succeeded; writes and validation passed deny-level checks. |
| `1` | Query or describe refusal, or a write/validation verdict marking data unservable. |
| `2` | Other errors, such as invalid arguments, compilation, missing data or execution failures. |

## SQL behaviour

peQL uses DataFusion SQL and accepts one query at a time. Select from a registered contract by name, quoting names containing `/`, for example `"sales/orders"`.

Joins, aggregates and subqueries operate over the exposed contract views. DDL, DML, `COPY`, `SET`, SQL `EXPLAIN` and multiple statements are refused. Catalog/schema-qualified table references are not contract names; use the whole contract name as one quoted identifier.

## Validation and write reports

A write report contains `rows_written`, `files` and `verdict`. `files` counts the dataset's files after the write, including earlier files when appending.

The verdict includes `valid`, `row_count`, `failures`, `breached`, `guarantees`, `stats` and `data_hash`. `failures` maps assertion IDs to failing-row counts. `valid` can be true when drop-level or report-only checks fail.

## Query envelope

| Field | Meaning |
| --- | --- |
| `contracts` | Names, versions, hashes, decision rules, annotations, active result rules and use of stored calculations. |
| `rows` | Number of returned rows. |
| `suppress_k` | Active minimum group size, if any. |
| `budgets` | Remaining amounts for budgets charged by this query. An empty map is not a full ledger balance. |
| `scan` | Scan/release row counts, scanned Arrow bytes, file bytes read and pruning counters. |
| `attestation` | Query and result SHA-256 hashes plus a timestamp; not a signature. |
| `audit_id` | Identifier of the corresponding audit entry. |
| `cached` | Whether stored result batches were returned. |

## Common errors

| Error | What to check |
| --- | --- |
| `Compile` | Contract syntax, expressions, column names and types. |
| `UnknownContract` | Registration and publication to the caller's tenant. An invisible contract has the same error as an absent one. |
| `Denied` | The named caller-level rule and supplied context. |
| `NotWritten` | Whether the contract has data and a validation manifest. |
| `NotServable` | The dataset's deny-level failures; correct and rewrite the data. |
| `GuaranteeFailed` | The named dataset requirement, such as freshness or a missing-value limit. |
| `BudgetExhausted` | Spending and the limit for that caller's named budget. |
| `Refused` | Whether the SQL is a single supported read-only query. |
| `Invalid`, `DataFusion`, `Io` | The accompanying message: input, planning, execution or storage failed. |
| `Ungated` | A contract scan is missing its required execution gate; report this as an engine/integration issue. |

In Rust, `PeqlError::is_refusal()` includes `UnknownContract`, `Denied`, `NotServable`, `GuaranteeFailed`, `BudgetExhausted` and `Refused`. `NotWritten` is a separate failure. Python maps the refusal group to `peql.Refused`.

## Workspace files

| Location | Contents |
| --- | --- |
| `<root>/_peql/contracts/` | Versioned parcel bundles and publication records. |
| `<root>/_peql/functions/` | Registered WebAssembly modules and metadata. |
| `<root>/_peql/budgets.json` | Budget limits and spending. |
| `<root>/_peql/audit.jsonl` | Query-attempt records, one JSON object per line. |
| `<binding>/_peql/manifests/` | Per-contract dataset manifests beside the bound data. |

Keep the workspace state and dataset files together when moving or backing up a workspace. Single-file Parquet bindings can be read; peQL's write path requires a directory binding.

## Build features

The default Rust build has no optional features enabled. `platform` adds HTTP signed-bundle support. `lance` adds the Lance dependencies; the provider is exposed on Unix and needs `protoc` to build. The storage and notary Unix-socket clients are available on Unix targets.

See {doc}`platform` for integration behaviour.

```{toctree}
:hidden:

contributing
changelog
```
