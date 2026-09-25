# How it works

## Contracts are the only tables

Every name in a `FROM` clause is a contract. The contract's binding (where its files live) never
appears to a caller, in results or in errors. A contract the caller's tenant may not see
behaves exactly like one that does not exist, so a caller cannot probe for names.

A contract is visible to a caller when it has no owner, when the caller's tenant owns it, or
when the owner published it to that tenant or to `public`.

## A query, step by step

1. **Guard.** The SQL is parsed. Anything but exactly one query is refused: DDL, DML, `COPY`,
   `SET`, `EXPLAIN`, `INSTALL`, `LOAD`, `ATTACH`, a second statement. The planned query is checked
   again. Comments and string literals cannot hide a statement, because the check reads the
   parsed statement.
2. **Resolve.** For each contract the query names, peQL runs its `decide` rules against the
   caller (parcel-runtime's CEL interpreter). It then reads the dataset's manifest and runs
   `guarantee` rules against the stored statistics; a failing `deny` guarantee refuses the query,
   an `annotate` one is noted in the envelope. It also works out which shape rules apply.
3. **Build the view.** The view is ordinary DataFusion: a scan of the binding, a filter made of
   the contract's `admit` rules and drop-level `assert` flags, and a projection of the exposed
   columns with their `transform` rules. Caller context is bound as literals, so a rule that
   does not apply to this caller folds away. A `Gate` node goes on top.
4. **Plan.** The caller's SQL is planned over the views. `suppress` and aggregate `noise` are
   applied to the caller's aggregates (parcel_runtime::shape), and their budget charges are paid.
5. **Execute.** The engine refuses any physical plan in which a scan of contract data is not
   under that contract's gate. The rest is DataFusion.
6. **Envelope and audit.** The caller gets the rows and an envelope: what each contract decided,
   which shapes applied, budget left, what the scan read, and hashes of the query and the result.
   One audit record is written, answered or refused.

Everything a rule means was decided by parcel when the contract was compiled. peQL binds the
caller, finds the data, and runs what parcel compiled.

## The gate is a barrier

The optimiser pushes the contract's filter into the Parquet scan, so partitions and row groups
the contract excludes are never read. A caller's own predicates are pushed too, but only when
they cannot fail: comparisons, boolean logic, `IN` lists, `IS NULL`, `LIKE`. A predicate that can
raise an error, such as `100 / (id - 3) > 0`, stays above the gate. Otherwise it could fail on a
row the contract hides, and the error would reveal that the row exists.

Transforms are projections: a masked column the query does not read is never computed.

## Stored flags, or live rules

When peQL writes data under a contract, it stores a boolean column per assertion and a column
for each row-only subtree parcel chose to precompute. Views read those columns instead of
evaluating the rules, as long as every file was written under the current contract hash. Files
written under another contract, or not by peQL at all, are read with every rule evaluated live.
The results are the same either way.

## Shapes

| Shape | Where it runs | Effect |
| --- | --- | --- |
| `sample` | in the view's filter | A stable fraction of rows keyed on a column; repeated queries see the same rows. |
| `noise` at `row` | in the view's projection | Laplace noise on each value of the column. |
| `noise` at `aggregate` | on the caller's aggregates | Noise on each aggregate over the column; the column cannot be read otherwise or grouped by. |
| `suppress` | on the caller's aggregates | Groups under `k` rows are removed from every aggregate, including `UNION` branches and subqueries. A query without `GROUP BY` is one group. |

Each `unless` is evaluated per caller. Noise charges its named budget once per query, and only
when the query reads the noised column; a spent budget refuses the query. Budgets are kept per
caller and survive restarts in a workspace.

## The write path

`write` runs parcel's write plan: enrich (`row.other` fields, including tenant WebAssembly
functions), compute flags and precomputed columns, cluster by the flags queries filter on,
partition by the binding's partition columns, enable bloom filters where the contract compares
columns with caller context, stamp the contract hash into each Parquet file, run the validation
plan, and write the manifest. A deny-level assertion that fails marks the data as not servable
until it is fixed.

## The result cache

With `Engine::with_cache`, answers are cached under a key that covers the SQL and, for each
contract, its compilation hash, the caller's bound context, the shapes that apply, and the data
hash. Two callers share an answer only when the contracts would give them the same one; a write
or a new contract version never serves a stale answer. A cached answer with noise is served as
it was: seeing the same noisy answer twice spends no more privacy.
