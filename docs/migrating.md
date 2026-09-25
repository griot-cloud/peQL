# Migrating from 0.3

peQL 0.4 enforces parcel contracts instead of its own JSON policy format. Every 0.3 policy has a
parcel equivalent:

| 0.3 JSON | parcel rule |
| --- | --- |
| `"purposes": ["analytics"]` | `{id: purposes, op: decide, expr: "ctx.purpose in ['analytics']"}` |
| `"owner_tenant": "acme"` (owner sees raw) | `owner: acme`, and `ctx.tenant == 'acme' ? row.x : ...` in each transform |
| `"columns": [...]` | `expose: [{name, type}, ...]` |
| `"row_filter": "region = 'EU'"` | `{op: admit, expr: "row.region == 'EU' \|\| ctx.tenant == 'acme'"}` |
| `mask: redact` | `redact(row.x)`: a fixed `***` |
| `mask: hash_sha256` or `tokenize` | `hash_sha256(row.x)`; on a number, `hash_sha256(string(row.x))` with the column exposed as `utf8` |
| `mask: partial` | `partial(row.x, 4)`: `***` and the last four characters (fully masked when the value is four characters or fewer) |
| `mask: null` | `cond ? row.x : null`: a real null (0.3 wrote an empty string) |
| `"dp_columns": {"salary": {sensitivity, epsilon}}` | `{op: shape, operator: noise, column: salary, params: {sensitivity, epsilon, budget: salary, at: row}, unless: "ctx.tenant == 'acme'"}` |
| `node_filter`, `edge_filter` (graphs) | admits on a nodes contract and an edges contract; see {doc}`graphs` |

API changes:

| 0.3 | 0.4 |
| --- | --- |
| `peql::contract_source::Caller::new(id, purpose, tenant)` | `peql::Caller::new(id, tenant, purpose)` in Rust; Python keeps `Caller(id, purpose, tenant)` |
| `Engine::from_json_contracts_dir(dir)` | `Engine::open(root)` and `register_contract` or `register_bundle` |
| `Engine::query(sql, caller) -> Vec<RecordBatch>` | `Engine::query(sql, &caller) -> QueryResult { batches, envelope }` |
| `query_with_stats` | the envelope's `scan` |
| `K04DEngine::query(sql)` over raw registered tables | `K04DEngine::query(sql, &caller)`; registered data binds to a contract |
| `ResolvedPolicy`, `ContractSource`, optimiser rules, `RowFilterExec`, `MaskingExec`, `LaplaceNoiseExec` | removed: parcel compiles the rules, the view and gate enforce them |
| `QueryCache::get(tenant, sql)` | the engine's cache, keyed on everything an answer depends on |
| graph table functions | recursive SQL over two contracts |
| T03 bundles with SQL templates and Rego | signed parcel bundles |
