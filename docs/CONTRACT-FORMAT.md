# JSON contract format

`JsonContractSource` reads one JSON object per dataset. The object defines
policy inputs and a data binding. The engine evaluates it for each caller and
enforces the resulting decision and transformations during query execution.

## Example

```json
{
  "contract_id": "sales_orders_v1",
  "version": "1",
  "dataset": "sales/orders/v1",
  "binding": { "parquet": "/data/orders.parquet" },
  "owner_tenant": "acme",
  "purposes": ["analytics"],
  "columns": [
    { "name": "order_id" },
    { "name": "email", "mask": "hash_sha256" },
    { "name": "region" }
  ],
  "row_filter": "region = 'EU'"
}
```

For a caller whose tenant is `acme`, peQL exposes the three listed columns
with original values and all rows. For another tenant with purpose
`analytics`, peQL filters to EU rows and hashes `email`. A caller with a
different purpose is denied before the binding is opened.

## Fields

| Field | Required | Engine interpretation |
| --- | --- | --- |
| `contract_id` | Yes | Identifier carried into the resolved policy and operator bundle. |
| `version` | Yes | Version string carried into the resolved policy. |
| `dataset` | Yes | Quoted table name in SQL; it is the lookup key for the JSON source. |
| `binding` | Yes | Physical location. Use `parquet` for tables or `graph_snapshot` for graphs. |
| `owner_tenant` | Yes | Tenant that receives unmasked, unfiltered values after the purpose check. |
| `purposes` | No | Allowed purpose strings. Empty or omitted means any purpose. |
| `columns` | No | Ordered names exposed by tabular scans to every caller. Omitted or empty exposes all source columns. |
| `columns[].name` | When a column is listed | Name of an exposed source column. |
| `columns[].mask` | No | Mask for non-owner callers. |
| `masks` | No | Additional column-to-mask map; it overrides a mask for the same name in `columns`. |
| `row_filter` | No | SQL predicate that restricts non-owner table rows. |
| `dp_columns` | No | Per-column `sensitivity` and `epsilon` for Laplace noise on non-owner scans. |
| `node_filter` | No | Graph alias for a node row predicate; ignored when `row_filter` is present. |
| `edge_filter` | No | Graph edge predicate that removes relationships from traversal. |

`columns[].type` and `columns[].sensitivity` can appear in JSON, but the
current parser does not use them. Arrow types come from the bound Parquet file.

## Column exposure and masks

For a tabular dataset, the engine reports only names listed in `columns` to
the SQL planner. A query for another source column fails resolution, including
for the owner. After scanning, peQL masks values and applies the
exposed-column projection. Graph table functions use their own result schemas;
this projection is not applied to graph function output.

| Mask | Current effect |
| --- | --- |
| `redact` | String becomes `***`; supported numeric values become zero and booleans become false. |
| `hash_sha256` | SHA-256 hex digest of the value's string representation. |
| `tokenize` | Currently the same deterministic SHA-256 digest as `hash_sha256`. |
| `partial` | `***` followed by the last four characters. |
| `null` | Empty string for a non-null string; zero or false for supported non-string types. Existing nulls remain null. |
| `noop` | Original value. |

The current `null` action does not turn a non-null input into an Arrow null.
An unknown mask name is an error. A string-producing mask on a non-string
column changes the output Arrow type to `Utf8`. Read the output schema rather
than assuming it matches the Parquet schema.

## Row filtering and differential privacy

`row_filter` is a SQL boolean expression over source columns. It applies to
non-owner callers before masking and before the query's own projection. A
filter may therefore use a column that the query does not select.

`dp_columns` has this shape:

```json
"dp_columns": {
  "amount": { "sensitivity": 1.0, "epsilon": 0.1 }
}
```

When present, `Engine` adds `LaplaceNoiseExec` to the governed scan using its
permissive budget tracker. This path adds noise but does **not** enforce a
cross-query privacy budget. The lower-level operator has a constructor that
accepts a budget tracker. Do not present `dp_columns` alone as a complete
privacy-budget system.

## Graph binding

For a graph, `binding.graph_snapshot` names a local directory containing
`manifest.json`, `nodes.parquet`, `edges.parquet`, and `edges_rev.parquet`:

```json
{
  "contract_id": "operations_graph",
  "version": "1",
  "dataset": "process-graphs/operations/v1",
  "binding": { "graph_snapshot": "/data/operations-v1" },
  "owner_tenant": "acme",
  "purposes": ["process_analysis"],
  "masks": { "owner": "redact" },
  "node_filter": "kind != 'system'",
  "edge_filter": "edge_type != 'depends_on'"
}
```

The standalone resolver accepts a `file://` prefix on the directory path.
Graph policy compilation evaluates node and edge predicates using DataFusion.
See {doc}`GRAPH-QUERY` for traversal behavior.
