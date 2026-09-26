# Quickstart

In this example, Acme stores orders for itself and its customer Globex. You will write a contract that lets each tenant query its own orders, excludes invalid amounts, and allows queries only for reporting.

You will create two small files and use the `peql` command. You do not need a running server, an account or a separate parcel installation.

## 1. Install peQL

On macOS or Linux:

```bash
curl -LsSf https://github.com/griot-cloud/peql/releases/latest/download/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
```

On Windows, in PowerShell:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://github.com/griot-cloud/peql/releases/latest/download/install.ps1 | iex"
```

Follow any PATH instructions printed by the installer, then check that the command is available:

```text
peql --version
```

If you prefer to install manually, download a binary for your operating system from [peQL releases](https://github.com/griot-cloud/peql/releases).

## 2. Create a workspace and some data

Create an empty directory and move into it. These commands work in a Unix shell or PowerShell:

```text
mkdir peql-demo
cd peql-demo
```

Use a text editor to save the following as `orders.csv` inside that directory:

```text
order_id,tenant_id,amount
1,acme,120
2,globex,80
3,globex,-10
4,acme,200
```

Each row is an order. `tenant_id` identifies who it belongs to. Order 3 has a negative amount; our contract will keep it out of query results.

## 3. Describe the rules in a contract

Save this as `orders.yaml` in the same directory:

```yaml
contract: sales/orders
version: 1
owner: acme

binding:
  parquet: data/orders/

expose:
  - {name: order_id, type: int64}
  - {name: amount, type: int64}

rules:
  - id: reporting_only
    op: decide
    expr: ctx.purpose == 'reporting'

  - id: own_orders
    op: admit
    expr: row.tenant_id == ctx.tenant

  - id: positive_amount
    op: assert
    expr: row.amount > 0
    on_fail: drop
```

The contract is called `sales/orders` and is owned by `acme`. Its `binding` points to the directory where peQL will write the data. Only `order_id` and `amount` are exposed to SQL; the rules can still read `tenant_id`.

The three rules do different jobs:

- `reporting_only` refuses callers whose purpose is not `reporting`.
- `own_orders` keeps only rows whose tenant matches the caller's tenant.
- `positive_amount` excludes rows whose amount is zero or negative.

`row` refers to a data record; `ctx` refers to the caller. See the [parcel documentation](https://griot-cloud.github.io/parcel/) for expressions and other kinds of rule.

## 4. Register the contract and write the data

Run this from `peql-demo`:

```text
peql write orders.yaml --input orders.csv
```

Because you supplied a contract file, this command registers the contract and writes the CSV data in one step. peQL uses the CSV's column types to check the contract, writes Parquet under `data/orders/`, and validates the data.

The write report should show four rows written and one row failing `positive_amount`. The dataset remains valid because this rule says to drop the failing row from results rather than deny the whole dataset.

The original CSV stays unchanged. peQL also creates `_peql/` for its registered contracts and state. Running the same write command again replaces the contract's Parquet data; use `--append` only when you want to add more rows.

## 5. Run the same query as two callers

First, query as an Acme analyst. The examples use single quotes around SQL and double quotes around the contract name in a Unix shell or PowerShell 7.3 and newer.

```bash
peql query 'SELECT order_id, amount FROM "sales/orders" ORDER BY order_id' --id acme-analyst --tenant acme --purpose reporting
```

You should receive:

```text
+----------+--------+
| order_id | amount |
+----------+--------+
| 1        | 120    |
| 4        | 200    |
+----------+--------+
```

The SQL has no tenant filter. The contract supplied it using `--tenant acme`.

Before Globex can query, Acme's operator must share the contract with that tenant:

```text
peql publish sales/orders --to globex
```

Publishing makes the contract visible to Globex. Its rules still apply. Run the same SQL with a different caller:

```bash
peql query 'SELECT order_id, amount FROM "sales/orders" ORDER BY order_id' --id globex-analyst --tenant globex --purpose reporting
```

You should receive:

```text
+----------+--------+
| order_id | amount |
+----------+--------+
| 2        | 80     |
+----------+--------+
```

Order 2 belongs to Globex and passes the quality check. Order 3 also belongs to Globex, but its negative amount excludes it. Acme's orders are not returned.

These flags let you try different caller contexts locally. In an application, the application authenticates users and supplies their context to peQL.

## 6. See a query refused

Keep the tenant the same but change the purpose to `marketing`:

```bash
peql query 'SELECT order_id, amount FROM "sales/orders" ORDER BY order_id' --id globex-analyst --tenant globex --purpose marketing
```

The `reporting_only` rule refuses the query:

```text
refused: denied by `sales/orders` rule `reporting_only`
```

This is an expected result. The command exits with status 1 and returns no rows.

## 7. Inspect the quality result

```text
peql validate sales/orders
```

The JSON verdict reports four stored rows and one failure of `positive_amount`. Validation checks the underlying data, so it includes the row that queries exclude.

Add `--envelope` to a successful query to see which contract and rules applied. The workspace's `_peql/audit.jsonl` file records your query attempts, including the refusal.

You have now used the same data and SQL with different callers, and seen both row filtering and a complete refusal. Continue to {doc}`USAGE` for Python and Rust integration, or read {doc}`execution` to understand how peQL applies the rules.
