# peQL (Python)

Query data through [parcel](https://github.com/griot-cloud/parcel) contracts: every table is a
contract, and each caller gets what the contract allows. Python bindings for the
[peQL](../../README.md) engine.

```bash
pip install maturin && maturin develop --release   # from bindings/python
```

```python
import peql
import pyarrow as pa

engine = peql.Engine.open("./workspace")
engine.register(open("orders.yaml").read(), schema=orders.schema)  # a parcel contract
engine.write("sales/orders", orders)                               # a pyarrow Table
engine.publish("sales/orders", "globex")

table = engine.query(
    'SELECT region, COUNT(*) AS n FROM "sales/orders" GROUP BY region',
    peql.Caller("user:bob", "analytics", "globex"),
)
table, envelope = engine.query_with_envelope(sql, caller)   # what the contract did
```

A query the contract refuses raises `peql.Refused`. `engine.validate(name)` returns the
verdict; `engine.describe(name, caller)` the columns a caller would see;
`engine.register_bundle(json)` registers a bundle from `parcel compile -o`.
