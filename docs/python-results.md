# Results and errors

## Write report

`Engine.write` returns a dictionary with `rows_written: int`, `files: int` and `verdict: dict`. `files` counts the dataset's files after the write, including existing files when appending.

## Validation verdict

`Engine.validate` returns the verdict directly; `Engine.write` puts it under `report["verdict"]`.

| Key | Meaning |
| --- | --- |
| `valid` | Whether deny-level quality checks passed. |
| `row_count` | Number of rows evaluated. |
| `failures` | Failure counts keyed by assertion id. |
| `breached` | Contract requirements that failed. |
| `guarantees` | Results of dataset-level guarantees. |
| `stats` | Computed statistics used by rules. |
| `data_hash` | Hash of the validated data. |

A verdict can be valid even when a drop-level or report-only check fails.

## Query envelope

`Engine.query_with_envelope` returns `(pyarrow.Table, dict)`. The dictionary has these keys:

| Key | Meaning |
| --- | --- |
| `caller` | Caller id, tenant and purpose supplied to the engine. |
| `contracts` | Applied contract names, versions, hashes, rules and annotations. |
| `rows` | Returned row count. |
| `suppress_k` | Active minimum group size, when applicable. |
| `charges` | Epsilon charged by this query, per budget. |
| `budgets` | Remaining amounts for budgets this query charged. |
| `scan` | Row, Arrow-byte, file-byte and pruning counters. |
| `attestation` | Query/result hashes and timestamp; a signature requires a configured signer. |
| `audit_id` | Matching audit entry identifier. |
| `cached` | Whether stored result batches were returned. |

## Exceptions

`peql.Refused` is the package's alias for Python `PermissionError`. It covers engine-classified refusals including invisible or unknown contracts, denied callers, unservable data, failed guarantees, exhausted privacy budgets and unsupported SQL. Other native engine failures map to `RuntimeError`. Ordinary Python argument mistakes can raise `TypeError` or `AttributeError` before the engine is called. These exception classes have no peQL-specific fields.

```python
try:
    table, envelope = engine.query_with_envelope(sql, caller)
except peql.Refused as error:
    print("Query refused:", error)
```
