# Examples

| Workspace | Shows |
|---|---|
| [`quickstart`](quickstart) | acme's orders contract shared with globex: writes, queries as three callers, a refusal, an inheriting child (`sales/orders_ea`), JSON output, and the parcel-to-peQL handoff. |
| [`utility`](utility) | A tenant's own WebAssembly functions (built in the parcel repository's `examples/udf-meter-serial`), called from rules and enrichers. |

Run everything from a clean state:

```sh
cargo build --bin peql
PEQL=$PWD/target/debug/peql examples/run-all.sh          # add PARCEL=... to include the handoff
```
