# How it works

peQL applies a contract before the caller's SQL can use its data. The SQL operates on the rows and columns allowed for that caller, then any result-level restrictions are applied.

## From a contract to a query

1. **Register.** Parcel checks the contract against the data schema and compiles its rules. peQL stores the compiled contract and its version.
2. **Check the request.** peQL accepts one read-only SQL query. It resolves each referenced contract, checks whether the caller can discover it, and evaluates its caller and dataset requirements.
3. **Build the caller's view.** Row rules select records, quality rules exclude failing rows where required, and column rules produce the exposed values. Caller context supplies values such as tenant and purpose.
4. **Run the SQL.** DataFusion plans and executes the query over those views. Active sampling, suppression and noise rules affect what can be returned; noise may also spend privacy budget.
5. **Return and record.** The engine returns Arrow batches and query metadata, and writes an audit record for the attempt.

Joins and subqueries use the same contract views. A query cannot introduce a raw file table with `CREATE EXTERNAL TABLE` or export around the rules with `COPY`; those statements are refused.

## What parcel does

Parcel owns the contract language and the meaning of its rules. It produces expressions for querying, a validation plan and a write plan. Its runtime library also supplies rule evaluation and result-shaping operations.

peQL uses those artifacts to manage a workspace, find the data, build caller-specific views, execute SQL, charge budgets and record results. It can compile a contract directly through parcel's libraries or receive a compiled bundle. A bundle is verified by recompiling its document and comparing its hash and executable artifacts.

The [parcel documentation](https://griot-cloud.github.io/parcel/) covers contract syntax, expressions, custom functions and compilation.

## Quality checks and stored calculations

When peQL writes data, it computes assertion flags and reusable calculations selected by parcel, writes Parquet, and validates the resulting dataset. It saves the verdict, dataset statistics and file information in a **manifest**.

Queries use the manifest to check whether the data can be served. They can reuse stored rule calculations when the files' contract hashes match the current contract. Otherwise, the view evaluates those row calculations live.

These are separate checks: evaluating a row expression live does not refresh a saved dataset verdict. Writes through peQL refresh manifests; external file edits and standalone `validate` calls do not. Contract changes that affect validation should be followed by a rewrite from the source data.

A write is not a transaction that rolls back on a failed quality check. Data can be stored and subsequently marked unservable. The write report tells you which checks failed.

## Keeping rules in the execution plan

Each contract view ends in a **gate**, an execution marker identifying the contract. Before running the physical plan, peQL checks that each marked contract scan sits beneath the matching gate.

The gate also restricts query optimisation. Predicates classified as safe can move closer to the scan, allowing DataFusion to skip irrelevant Parquet data. Expressions that can fail remain above the contract view so they are evaluated on rows the caller can see. This matters for expressions that might otherwise reveal a hidden row through an error.

## Result restrictions and budgets

| Rule | Behaviour |
| --- | --- |
| Sampling | Selects a stable fraction of rows using a key column. |
| Suppression | Removes groups smaller than the configured threshold. Without an aggregate, the entire result must meet that threshold. |
| Row noise | Adds noise to individual values before the caller's calculation. |
| Aggregate noise | Adds noise to aggregates over a protected column; direct reads and grouping by that column are refused. |

A rule's `unless` condition can exempt particular callers. Noise parameters, including sensitivity and epsilon, are supplied by the contract author.

peQL tracks spending per tenant and caller ID for each named budget. Applicable noise charges are collected per query, using the largest epsilon for each budget, and charged before execution. An exhausted budget refuses the query; charges are not automatically refunded if execution later fails.

The default limit is 10 epsilon per caller per budget. Operators can set a limit with `peql budget NAME --limit EPSILON`. Changing the limit does not reset spending.

## Cached answers

Caching is opt-in through the Rust engine's `with_cache` builder. Its key includes the SQL, contract compilation hashes, bound caller values, active result rules, and the data hash and write time recorded in the manifests.

The engine still prepares and checks the query before a cache lookup. A hit returns the stored batches, including any existing noise, without a new privacy charge. Its envelope has `cached: true`, no new budget charges and zero scan statistics.

Cache validity depends on the recorded manifests. Changing data outside peQL does not automatically update those records.

## Audit and result metadata

Each query attempt records caller details, a SQL hash, its outcome, elapsed time and row count. Successful attempts also record the contracts used and budget charges. The result's `audit_id` links to that record.

The envelope includes hashes of the SQL and returned Arrow data. These hashes are not a digital signature. Applications that need a signed envelope can configure the worker pool with a signer; see {doc}`platform`.
