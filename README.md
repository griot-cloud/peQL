# peQL

**A query engine where every table is a data contract.**

peQL stores data contracts written in [parcel](https://github.com/griot-cloud/parcel), writes
data under them, and answers SQL through them. Every `FROM` names a contract, and the contract
decides what each caller gets: which rows, which columns, masked or in clear, noised or exact,
or nothing at all. Enforcement is part of the query plan, so the optimiser prunes with the
contract's rules instead of working around them.

```sh
peql write contracts/orders.yaml --input orders.csv
peql publish sales/orders --to globex
peql query 'SELECT region, SUM(amount_cents) FROM "sales/orders" GROUP BY region' \
  --caller callers/globex-analyst.yaml
```

## What a contract does in peQL

| parcel rule | In peQL |
| --- | --- |
| `decide` | Refuses a caller before any file is opened. |
| `admit` | A filter in the contract's view, pushed into the Parquet scan: excluded partitions and row groups are never read. |
| `assert` | Evaluated at write into a stored flag; failing rows are dropped, reported, or make the data unservable. |
| `transform` | A projection: masks, hashes and nulls per caller, never computed for columns a query does not read. |
| `guarantee` | Checked against the dataset's manifest: refuses or annotates the query. |
| `shape` | Sampling, noise on rows or on aggregates with privacy budgets, and small-group suppression. |

Every query returns an envelope (which rules applied, what was read, hashes of the question and
the answer) and leaves an audit record. A plan in which contract data is read outside its view is
refused before it runs.

## Documentation

- [Quickstart](docs/getting-started.md): write and query under a contract as three callers.
- [Use peQL](docs/USAGE.md): the command line, Rust, and Python.
- [How it works](docs/concepts.md): views, the gate, shapes, the write path.
- [parcel and peQL](docs/parcel-and-peql.md): who does what, and the bundle between them.
- [Migrating from 0.3](docs/migrating.md).

## Install

```sh
cargo install --git https://github.com/griot-cloud/peql peql
```

As a library, `peql = { git = "https://github.com/griot-cloud/peql" }`. The Python package is
in `bindings/python`. Rust 1.94 or newer.

## Status

Version 0.4.0: peQL is now the runtime for parcel contracts. See the
[changelog](CHANGELOG.md) for what changed from 0.3.

## License

Apache-2.0; see [LICENSE](LICENSE).
