# Concepts

peQL allows you to define a **data contract** that comprises the data quality rules and access policies for a dataset. The contract says where the data lives, which columns can be queried, and which rules peQL must apply.

## What is a data contract?

Acme wants to share order data with its suppliers. Its staff need to see all orders, while each supplier should see only orders assigned to them. Orders with zero or negative amounts should be excluded from everyone's results.

This contract expresses those requirements:

```yaml
contract: purchasing/orders
version: 1
owner: acme
binding:
  parquet: data/orders/
expose:
  - {name: order_id, type: int64}
  - {name: amount, type: int64}
rules:
  - id: supplier_orders
    op: admit
    expr: ctx.tenant == 'acme' || row.supplier_id == ctx.tenant

  - id: positive_amount
    op: assert
    expr: row.amount > 0
    on_fail: drop
```

The underlying data has `order_id`, `supplier_id` and `amount` columns. The first rule lets Acme see all orders and suppliers see their assigned orders. The second excludes non-positive amounts from both groups. Queries can return only `order_id` and `amount`; `supplier_id` is used by the rule but is not exposed to SQL.

Contracts are written in **parcel**, which uses YAML or JSON for the document and CEL expressions for its rules. For contract structure, rule types, expressions and namespaces, see the [parcel documentation](https://griot-cloud.github.io/parcel/). peQL uses parcel's libraries, so a separate parcel installation is not required.

## Workspaces and registration

A **workspace** is the directory peQL operates in. It contains registered contracts and engine state under `_peql/`. Relative data paths, such as `data/orders/`, are resolved from that directory. The CLI uses your current directory unless you select another with `--root`.

**Registration** makes a contract available to the engine. peQL checks its rules against the data's column names and types and stores the compiled result. You can register a contract document or a compiled parcel bundle. Registration alone does not copy or write the dataset.

The contract's name is what queries use in SQL. Its data can come from local Parquet or, in a Rust application, a table supplied by the application.

## Callers and sharing

A **caller** is the user, agent or service making a query. peQL receives its identity and attributes, such as tenant, roles and purpose, alongside the SQL. Your application authenticates the caller and supplies those values.

A **tenant** identifies an organisation or group. In this example, `acme` is the company and `globex` is one of its suppliers.

An owned contract must be shared with another tenant before that tenant can discover it. Once the example contract is registered, an operator can share it with Globex:

```text
peql publish purchasing/orders --to globex
```

Sharing makes the contract visible; its rules still control the results. Acme's access to all orders comes from the explicit rule in this example, not automatically from owning the contract.

## Queries

Once data is available, Globex can query it:

```bash
peql query 'SELECT order_id, amount FROM "purchasing/orders"' --id supplier-analyst --tenant globex
```

The SQL has no supplier filter or amount check. peQL applies both from the contract. Running the same SQL as `acme` returns positive-amount orders across suppliers.

Queries can select, join and aggregate the columns exposed by registered contracts. The rules still apply when the SQL changes. If a caller or dataset fails a rule that requires refusal, peQL returns an error instead of rows.

## Writes and validation

**Writing** saves data under a contract. The CLI reads CSV or Parquet and writes Parquet files. peQL also computes reusable rule calculations and validates the stored dataset.

**Validation** produces a verdict: which checks failed, how many rows failed, and whether the dataset can be queried. In the example, an invalid amount excludes that row from results without deleting it from storage. A contract can instead require a failure to block queries to the entire dataset.

A write can store data that fails validation. Inspect the write report to see whether it can be queried. peQL saves validation results and dataset statistics in a **manifest**, which later queries use to check dataset requirements.

You can also register a contract over existing Parquet without rewriting it. See {doc}`USAGE` for that workflow and how to keep validation results current.

## Results and audit records

A successful query returns rows. It also produces an **envelope** describing the contracts applied, row counts, scan statistics and any privacy budget charged. The CLI shows it with `--envelope`; Python and Rust applications can read it alongside the data.

The **audit log** records query attempts, including refusals and failures. Persistent workspaces store it in `_peql/audit.jsonl`.

Continue to the {doc}`quickstart` for a complete working example, or {doc}`execution` for how peQL applies rules during execution.
