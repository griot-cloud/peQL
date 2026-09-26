# Python

The Python package accepts PyArrow data and returns a `pyarrow.Table`. It uses the same engine as the command line.

## Install

In your Python environment:

```bash
python -m pip install peql==0.4.0
```

PyArrow is installed as a dependency. The package requires Python 3.9 or newer and a compatible wheel for your platform. To build from source, see {doc}`contributing`.

## Query an existing workspace

After completing the {doc}`quickstart`, run this from the directory containing `peql-demo`:

```python
import peql

engine = peql.Engine.open("peql-demo")
caller = peql.Caller(
    id="globex-analyst",
    purpose="reporting",
    tenant="globex",
)

try:
    table, envelope = engine.query_with_envelope(
        'SELECT order_id, amount FROM "sales/orders" ORDER BY order_id',
        caller,
    )
    print(table.to_pylist())
    print(envelope["contracts"])
except peql.Refused as error:
    print(f"Query refused: {error}")
```

The result contains order 2 with amount 80. `query_with_envelope` returns the table and a dictionary of query metadata. Use `engine.query(sql, caller)` if you only need the table.

`peql.Refused` is an alias for `PermissionError`. It covers policy refusals, unavailable contracts and exhausted privacy budgets. Other failures, such as malformed SQL or I/O errors, use other exception types.

## Register and write data

This example uses the `orders.yaml` contract from the Quickstart and writes into a separate workspace:

```python
from pathlib import Path
import peql
import pyarrow as pa

orders = pa.table({
    "order_id": pa.array([1, 2, 3, 4], type=pa.int64()),
    "tenant_id": ["acme", "globex", "globex", "acme"],
    "amount": pa.array([120, 80, -10, 200], type=pa.int64()),
})

engine = peql.Engine.open("python-demo")
contract = Path("peql-demo/orders.yaml").read_text()
engine.register(contract, schema=orders.schema)
report = engine.write("sales/orders", orders)
print(report["verdict"])
engine.publish("sales/orders", "globex")
```

`register` takes contract text, not a filename. `write` replaces the bound Parquet data by default; pass `append=True` to add rows. Inspect `report["verdict"]["valid"]` before assuming the data can be queried.

## Common methods

| Method | Result or effect |
| --- | --- |
| `Engine.open(root)` | Open a persistent workspace. |
| `Engine.in_memory(base=".")` | Keep engine state in memory; relative data paths still resolve under `base`. |
| `register(source, schema)` | Register YAML/JSON contract text against an Arrow schema or table. |
| `register_bundle(bundle)` | Register a bundle supplied as JSON text. |
| `write(name, data, append=False)` | Write an Arrow table or batch; return a report dictionary. |
| `validate(name)` | Return a verdict dictionary. |
| `query(sql, caller)` | Return an Arrow table. |
| `query_with_envelope(sql, caller)` | Return `(table, metadata)`. |
| `describe(name, caller)` | Return exposed `(column, type)` pairs. |
| `contracts()` | List registered contract names, without a caller visibility check. |
| `publish(name, audience)` / `unpublish(name, audience)` | Share or withdraw visibility. |

The Python caller constructor is `Caller(id, purpose, tenant, tier=None, classification=None, roles=None, clearance=None)`. Prefer named arguments to keep purpose and tenant clear. The application is responsible for authenticating that caller.
