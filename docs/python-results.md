# Results and errors

## Write report

`Engine.write` returns a dictionary with these keys:

| Key | Meaning |
| --- | --- |
| `rows_written` | Rows committed. |
| `on_violation` | `"refuse"`, `"exclude"` or `"warn"`, as the write was asked. |
| `violations` | Rows the write was given that broke each row rule, keyed by rule: set aside under `exclude`, committed under `warn`. A row that breaks two rules is counted under both. |
| `rows_excluded` | Rows set aside rather than committed. |
| `excluded` | Under `exclude`, a `pyarrow.Table` of the set-aside rows with a `_violations` column listing the rules each broke; otherwise `None`. |
| `files` | Files the write added. |
| `verdict` | The contract's row rules over the rows written. |
| `table` | The dataset's `row_count` and `null_counts` after the write, or `None` while its manifest is not recorded. |
| `snapshot` | For an Iceberg table, the snapshot committed. |

Under the default `refuse`, a write given any row that breaks a row rule raises `peql.Refused` and writes nothing.

## Validation verdict

`Engine.validate` returns the verdict directly; `Engine.write` puts it under `report["verdict"]`.

| Key | Meaning |
| --- | --- |
| `valid` | Whether deny-level quality checks passed. |
| `row_count` | Number of rows evaluated. |
| `failures` | Failure counts keyed by assertion id. |
| `breached` | Contract requirements that failed. |
| `guarantees` | Results of dataset-level guarantees, decided only by `validate`; empty in a write's verdict. |
| `stats` | Computed statistics used by rules. |
| `data_hash` | Hash of the validated data. |

A verdict can be valid even when a drop-level or report-only check fails.

## Query envelope

`Engine.query_with_envelope` returns `(pyarrow.Table, dict)`. The dictionary has these keys:

| Key | Meaning |
| --- | --- |
| `caller` | Caller id, tenant and purpose supplied to the engine. |
| `contracts` | Applied contract names, versions, hashes, rules and annotations. Each contract's `excluded` maps a row rule to the number of the dataset's rows left out for breaking it. |
| `rows` | Returned row count. |
| `suppress_k` | Active minimum group size, when applicable. |
| `charges` | Epsilon charged by this query, per budget. |
| `budgets` | Remaining amounts for budgets this query charged. |
| `scan` | Row, Arrow-byte, file-byte and pruning counters. |
| `attestation` | Query/result hashes and timestamp; a signature requires a configured signer. |
| `audit_id` | Matching audit entry identifier. |
| `cached` | Whether stored result batches were returned. |

## Exceptions

`peql.Refused` is the package's alias for Python `PermissionError`. It covers engine-classified refusals including invisible or unknown contracts, denied callers, unservable data, failed guarantees, rows that break a row rule (a write under `refuse`, or a query under `on_violation="refuse"`), exhausted privacy budgets and unsupported SQL. Other native engine failures map to `RuntimeError`. Ordinary Python argument mistakes can raise `TypeError` or `AttributeError` before the engine is called. These exception classes have no peQL-specific fields.

```python
try:
    table, envelope = engine.query_with_envelope(sql, caller)
except peql.Refused as error:
    print("Query refused:", error)
```
