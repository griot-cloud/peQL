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
report = engine.write("sales/orders", orders, on_violation="exclude")
report["excluded"]                     # the rows that broke a rule, with their _violations
engine.publish("sales/orders", "globex")

table = engine.query(
    'SELECT region, COUNT(*) AS n FROM "sales/orders" GROUP BY region',
    peql.Caller("user:bob", "analytics", "globex"),
)
table, envelope = engine.query_with_envelope(sql, caller)   # what the contract did
```

A write refuses rows that break a row rule by default (`on_violation="refuse"`, raising
`peql.Refused`); `"exclude"` writes the rows that pass and returns the others, and `"warn"`
writes every row. A query never returns such rows: it leaves them out and counts them in the
envelope, or, with `on_violation="refuse"`, raises `peql.Refused`.

A query the contract refuses raises `peql.Refused`. `engine.validate(name)` returns the
verdict; `engine.describe(name, caller)` the columns a caller would see;
`engine.register_compiled(source, compiled)` registers a contract as `parcel compile -o` wrote it.
