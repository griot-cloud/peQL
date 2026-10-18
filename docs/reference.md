# Reference

Use these pages to look up the Python API, commands, caller fields, result metadata and common failures. For complete examples, see {doc}`quickstart`, {doc}`python` and {doc}`rust`.

[Python API reference](python-api.md) lists every public Python class and method with its parameters, return values and errors.

## Commands

```text
peql [--root DIR] COMMAND
```

`--root` selects the workspace and defaults to the current directory. Use `peql COMMAND --help` for all options on a command.

| Command | Purpose |
| --- | --- |
| `register FILE --schema SAMPLE` | Register a YAML/JSON contract against a CSV or Parquet sample. |
| `register FILE --compiled COMPILED` | Register a YAML/JSON contract as parcel compiled it (`parcel compile -o`), without compiling it. |
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
- `--on-violation refuse|exclude|warn` on `write` says what to do with rows that break a row rule (see [Rows that break a contract](#rows-that-break-a-contract)). The default, `refuse`, writes nothing when any row breaks a rule. `exclude` writes only the rows that pass and requires `--excluded PATH`, where the set-aside rows are written as Parquet with a `_violations` column. `warn` writes every row.
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
| `--on-violation exclude` | Leave out rows that break a row rule and note how many per rule on standard error; the default. |
| `--on-violation refuse` | Refuse the query when the data holds any row that breaks a row rule. |

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
| `1` | Query or describe refusal, a write refused for rows that break a row rule, or a write/validation verdict that is not valid. |
| `2` | Other errors, such as invalid arguments, compilation, missing data or execution failures. |

## SQL behaviour

peQL uses DataFusion SQL and accepts one query at a time. Select from a registered contract by name, quoting names containing `/`, for example `"sales/orders"`.

Joins, aggregates and subqueries operate over the exposed contract views. DDL, DML, `COPY`, `SET`, SQL `EXPLAIN` and multiple statements are refused. Catalog/schema-qualified table references are not contract names; use the whole contract name as one quoted identifier.

## Rows that break a contract

A row breaks its contract when it fails an `assert` whose `on_fail` is `drop` or `deny`. An assert with `on_fail: report` only counts its failures; no row breaks it. Guarantees are rules about the whole dataset, not rows, and are decided only by `validate`.

**Reads.** A read never returns a row that breaks its contract. The read's `on_violation` (`OnReadViolation` in Rust) chooses what happens when the data holds such rows:

| Mode | Effect |
| --- | --- |
| `exclude` | The default. The contract's view leaves the rows out, and the envelope's `excluded` reports how many rows it left out per rule. |
| `refuse` | The read is refused with `Violated`, naming each rule broken and how many rows broke it. |

There is no mode that returns breaking rows. The counts come from the dataset's manifest: they cover the whole dataset, not only the rows a query's own filters would have reached. A `refuse` read is therefore refused when the dataset holds any breaking row. Caller-level rules (`decide`) are checked first and refuse a read whatever its mode. Columns a contract withholds stay withheld, and masked columns stay masked, in either mode.

**Writes.** A write's `on_violation` (`OnWriteViolation` in Rust) chooses what happens to the rows it is given that break a row rule:

| Mode | Effect |
| --- | --- |
| `refuse` | The default. Nothing is committed; the write fails with `Violated`, naming each rule broken and how many rows broke it. |
| `exclude` | Only the rows that pass are committed. The others are returned to the writer in the contract's row schema plus a `_violations` column listing the rules each row broke. The report counts them per rule. |
| `warn` | Every row is committed. The verdict is not valid when a deny-level assert fails, and the report counts the breaking rows per rule. Reads leave those rows out. |

A write evaluates only row rules, over its own rows. `Engine::compact` rewrites rows as stored and neither refuses nor sets any aside.

## Validation and write reports

A write report contains `rows_written`, `on_violation`, `violations`, `rows_excluded`, `files`, `verdict`, `table` and, for an Iceberg table, `snapshot`. `violations` maps each row rule to the number of rows the write was given that broke it: set aside under `exclude`, committed under `warn`. A row that breaks two rules is counted under both. `rows_excluded` is the number of rows set aside rather than committed. `table` is the data's `row_count` and per-column `null_counts` after the write, from its manifest, or null while the manifest is not recorded. `files` lists the files the write added; `verdict` is the contract's row rules over the rows written, with a hash over those files. The dataset's manifest is updated from the write's facts without reading earlier files.

The verdict includes `valid`, `row_count`, `failures`, `breached`, `guarantees`, `stats` and `data_hash`. `failures` maps assertion IDs to failing-row counts. `valid` can be true when drop-level or report-only checks fail. A write's verdict covers only its rows and row rules, so its `guarantees` is empty; `validate` decides each dataset guarantee over the whole data, and the manifest keeps each one's last decision (`holds`, `snapshot_id`, `decided_at`) until the next `validate`.

## Query envelope

| Field | Meaning |
| --- | --- |
| `caller` | The caller's id, tenant and purpose, as the engine was told them. |
| `contracts` | Names, versions, hashes, decision rules, annotations, active result rules and use of stored calculations. Each contract's `excluded` maps a row rule to the number of the dataset's rows the view left out for breaking it; it is empty when nothing was left out. |
| `rows` | Number of returned rows. |
| `suppress_k` | Active minimum group size, if any. |
| `charges` | Epsilon this query charged, per budget. |
| `budgets` | Remaining amounts for budgets charged by this query. An empty map is not a full ledger balance. |
| `scan` | Scan/release row counts, scanned Arrow bytes, file bytes read and pruning counters. |
| `attestation` | Query and result SHA-256 hashes plus a timestamp; not a signature. With a signer configured, `QueryResult::signature` carries the signed envelope. |
| `audit_id` | Identifier of the corresponding audit entry. |
| `cached` | Whether stored result batches were returned. |

## Common errors

| Error | What to check |
| --- | --- |
| `Compile` | Contract syntax, expressions, column names and types. |
| `UnknownContract` | Registration and publication to the caller's tenant. An invisible contract has the same error as an absent one. |
| `Denied` | The named caller-level rule and supplied context. |
| `NotWritten` | Whether the contract has data and a validation manifest. |
| `NotServable` | The deny-level guarantees the dataset failed when `validate` last decided them; correct the data, then validate again. |
| `Violated` | Rows that break the named row rules, with how many broke each: a write refused them (`refuse`), or a read was asked to refuse them. Correct the rows, or write with `exclude`. |
| `GuaranteeFailed` | The named dataset requirement, such as freshness or a missing-value limit. |
| `BudgetExhausted` | Spending and the limit for that caller's named budget. |
| `Refused` | Whether the SQL is a single supported read-only query. |
| `Invalid`, `DataFusion`, `Io` | The accompanying message: input, planning, execution or storage failed. |
| `Signing` | The configured envelope signer: it refused or did not answer, so the query returns nothing. |
| `Ungated` | A contract scan is missing its required execution gate; report this as an engine/integration issue. |

In Rust, `PeqlError::is_refusal()` includes `UnknownContract`, `Denied`, `NotServable`, `Violated`, `GuaranteeFailed`, `BudgetExhausted` and `Refused`. `NotWritten` is a separate failure. Python maps the refusal group to `peql.Refused`.

## Workspace files

| Location | Contents |
| --- | --- |
| `<root>/_peql/contracts/` | Versioned contracts (document, functions and compiled bytes) and publication records. |
| `<root>/_peql/functions/` | Registered WebAssembly modules and metadata. |
| `<root>/_peql/budgets.json` | Budget limits and spending. |
| `<root>/_peql/audit.jsonl` | Query-attempt records, one JSON object per line. |
| `<binding>/_peql/manifests/` | Per-contract dataset manifests beside the bound data. |

Keep the workspace state and dataset files together when moving or backing up a workspace. Single-file Parquet bindings can be read; peQL's write path requires a directory binding.

## Rust write and read API

| Item | Shape |
| --- | --- |
| `Engine::write(name, batches, mode, on_violation)` | Returns `WriteOutcome { report: WriteReport, excluded: Option<RecordBatch> }`; `excluded` holds the rows set aside under `OnWriteViolation::Exclude`. |
| `Engine::begin_write(name, mode, on_violation)` | Starts a write delivered in parts. |
| `Engine::write_part(&writing, batches)` | Returns `Part { rows_written, excluded: Option<RecordBatch> }`. Under `Exclude`, each part returns its own set-aside rows; the write keeps none of them. |
| `Engine::finish_write(writing)` | Returns the `WriteReport`, or `Violated` under `Refuse`, in which case nothing the write wrote is published. |
| `OnWriteViolation` | `Refuse` (the `Default`), `Exclude` or `Warn`. |
| `OnReadViolation` | `Exclude` (the `Default`) or `Refuse`. A parameter of `query_as_of`, `plan_for`, `plan_as_of`, `view_for`, `view_as_of` and `check`; `query`, `plan`, `view` and `explain` use the default. |
| `Resolution::excluded` | `BTreeMap<String, u64>`: rows the contract's view left out, per row rule. |
| `PeqlError::Violated { contract, violations }` | `violations` is `Vec<(String, u64)>`: each rule broken, in the contract's order, and how many rows broke it. |
| `peql::violation::VIOLATIONS_COLUMN` | `_violations`, the `List<Utf8>` column of set-aside rows. |

## Build features

The default Rust build has no optional features enabled. `flight` adds the Flight SQL service and `s3` the S3 store for object-store bindings. `lance` adds the Lance dependencies; the provider is exposed on Unix and needs `protoc` to build.

See {doc}`platform` for integration behaviour.

```{toctree}
:hidden:
:maxdepth: 2

python-api
contributing
changelog
```
