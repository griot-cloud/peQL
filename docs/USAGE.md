# Using peQL

Use the command line to manage a workspace and run queries, or embed the engine in a Python or Rust application. All three interfaces use the same contracts and query rules.

The commands below continue from the {doc}`quickstart`. Run them from your `peql-demo` directory, or select it with `peql --root PATH`.

## Write or append data

Write a CSV or Parquet file under a registered contract:

```text
peql write sales/orders --input orders.csv
```

This **replaces the existing Parquet data** at the contract's binding. To add records instead:

```text
peql write sales/orders --input orders.csv --append
```

Append adds every input row; it does not deduplicate records or update rows with matching IDs. In this example, appending the same file twice duplicates the orders.

For CSV fields such as phone numbers or identifiers, override inferred types when needed:

```text
peql write sales/orders --input orders.csv --type order_id=int64
```

After a write, check the verdict. `valid` means the dataset passes its deny-level checks; it can still contain rows that a rule drops or reports. A `NOT SERVABLE` result means data was written but queries will be refused. Correct the input and write again.

## Register existing data

To query Parquet you already have, set the contract's `binding.parquet` to the file or directory, then register the contract against a representative file:

```text
peql register orders.yaml --schema data/orders/part-0.parquet
```

Replace the sample path with an actual file from your dataset. Registration checks and stores the contract without rewriting the data. On the first query, peQL creates a validation manifest if one is missing. The manifest stores the dataset's verdict and statistics.

Use peQL's write path for subsequent changes. In this version, existing manifests are not automatically refreshed when another program edits the files, and `validate` reports a verdict without saving a replacement manifest.

## Share and inspect a contract

```text
peql publish sales/orders --to globex
peql describe sales/orders --tenant globex --purpose reporting
peql list
```

Publishing allows a tenant to discover the contract; it does not exempt that tenant from its rules. `describe` shows the exposed columns after checking visibility and caller-level permission. It does not check whether the dataset currently meets every quality requirement.

To withdraw access:

```text
peql publish sales/orders --to globex --revoke
```

Use `--to public` to make a contract discoverable by every tenant. A contract with no `owner` is already visible to all tenants. The rules still determine which queries are allowed.

## Save caller context

Instead of repeating caller flags, save this as `analyst.yaml`:

```yaml
id: globex-analyst
tenant: globex
purpose: reporting
roles: [analyst]
```

Then run:

```bash
peql query 'SELECT SUM(amount) AS total FROM "sales/orders"' --caller analyst.yaml
```

Explicit flags override values from the file. For example, `--purpose marketing` changes the purpose for that query.

In an application, supply context from your authentication system. Keep registration, writes, sharing and other management operations under your application's control; these engine operations do not take a caller argument.

## Export and inspect results

Use `--format json` for one JSON object per row, or `--format arrow` or `--format parquet` for binary output. `--out` writes the result to a file:

```bash
peql query 'SELECT order_id, amount FROM "sales/orders"' --caller analyst.yaml --format parquet --out report.parquet
```

Add `--envelope` to print query metadata to standard error while leaving the result on standard output or in its output file. See {doc}`reference` for the envelope fields and exit codes.

To inspect data quality directly:

```text
peql validate sales/orders
```

This returns the validation verdict as JSON. It describes the underlying data, including rows excluded from query results.

## Update a contract

Edit the contract, increase its `version`, and register it again against the data's schema. peQL keeps registered versions and uses the highest version number for queries. Registering the same name and version replaces that stored version.

If the change affects quality checks, derived values or stored data layout, rewrite the data from the source through the updated contract. This refreshes the validation manifest and stored rule calculations.

## Use a compiled bundle

A parcel bundle packages a contract with its schema and compiled rules. If another tool or team provides one, register it directly:

```text
peql register orders.parcel.json
```

No `--schema` is needed because the bundle carries it. peQL verifies the bundle before registration. The bundle does not contain the dataset; its data binding must resolve in the workspace.

## Use peQL in an application

Choose the interface that fits your application:

```{toctree}
:maxdepth: 1

python
rust
platform
```
