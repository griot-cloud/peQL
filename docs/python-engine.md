# `peql.Engine`

A Python wrapper around one native peQL engine. Construct it with `Engine.open` or `Engine.in_memory`; the `Engine(native)` initializer is an internal wrapper. Management methods do not take a `Caller`. Authenticate and authorize those operations in your application.

| Method | Result |
| --- | --- |
| [`open`](#open) / [`in_memory`](#in_memory) | Create an engine. |
| [`register`](#register) / [`register_compiled`](#register_compiled) | Register a contract. |
| [`write`](#write) / [`validate`](#validate) | Write or validate a dataset. |
| [`query`](#query) / [`query_with_envelope`](#query_with_envelope) | Execute a read-only SQL query. |
| [`describe`](#describe) / [`contracts`](#contracts) | Inspect exposed columns or registered names. |
| [`publish`](#publish) / [`unpublish`](#unpublish) | Change tenant visibility. |

## `open`

```python
peql.Engine.open(root) -> peql.Engine
```

`root` is a filesystem path or path-like object converted to `str`. Opens or creates a persistent workspace at that path. The returned engine keeps workspace state on disk.

## `in_memory`

```python
peql.Engine.in_memory(base=".") -> peql.Engine
```

Keeps engine registration state in memory. `base` is a path or path-like object, defaulting to the current directory; relative data bindings still resolve beneath it.

## `register`

```python
engine.register(contract: str, schema: pyarrow.Schema | object_with_schema) -> str
```

`contract` is YAML or JSON **text**, not a filename. `schema` is a `pyarrow.Schema`, `pyarrow.Table`, `pyarrow.RecordBatch`, or another object with a `.schema` attribute. peQL compiles the contract against that schema and registers it. Returns the contract name. Compilation or schema errors raise `RuntimeError`.

## `register_compiled`

```python
engine.register_compiled(contract: str, compiled: bytes) -> str
```

`contract` is the source YAML/JSON text. `compiled` is the byte sequence produced by `parcel compile -o`. Registers that compiled artifact as supplied; it does not compile again. Returns the contract name. A malformed document or incompatible artifact raises `RuntimeError`.

## `write`

```python
engine.write(name: str, data: pyarrow.Table | pyarrow.RecordBatch, append: bool = False) -> dict
```

`name` is a registered contract name. `data` supplies an Arrow schema and batches. With `append=False`, replaces the bound data; `append=True` adds rows. Returns `{"rows_written": int, "files": int, "verdict": dict}`. `files` counts all files after the write, including earlier files when appending. Inspect `verdict["valid"]` to determine whether deny-level checks passed; see [Results and errors](python-results.md). Write failures raise `RuntimeError` or `peql.Refused` when the engine classifies them as a refusal.

## `validate`

```python
engine.validate(name: str) -> dict
```

Runs the contract's validation plan over its bound data and returns a verdict dictionary. Validation itself does not save a replacement manifest. A false `valid` value is a result, not necessarily an exception. `name` is a registered contract name.

## `query`

```python
engine.query(sql: str, caller: peql.Caller) -> pyarrow.Table
```

Executes one read-only SQL query as the supplied caller and returns an Arrow table. The result contains only rows and columns exposed under that caller's policy. Raises `peql.Refused` for a policy or visibility refusal; malformed SQL and execution failures raise `RuntimeError`. See [`query_with_envelope`](#query_with_envelope) for metadata.

## `query_with_envelope`

```python
engine.query_with_envelope(sql: str, caller: peql.Caller) -> tuple[pyarrow.Table, dict]
```

Takes the same `sql` and `caller` as `query`. Returns `(table, envelope)`: the result table and a dictionary of applied contracts, scan statistics, charges, attestation and audit metadata. See [Results and errors](python-results.md) for the keys. Exceptions follow `query`.

## `describe`

```python
engine.describe(name: str, caller: peql.Caller) -> list[tuple[str, str]]
```

Returns `(column_name, Arrow_type_string)` pairs visible to the caller for the named contract. It checks visibility and caller-level permission. It does not validate that the underlying dataset currently passes all quality requirements.

## `contracts`

```python
engine.contracts() -> list[str]
```

Returns all names registered in this engine. It takes no caller and does not filter by tenant visibility; use it in operator code, not as a caller-facing discovery method.

## `publish`

```python
engine.publish(name: str, audience: str) -> None
```

Makes the named contract visible to `audience`, a tenant name or `"public"` for all tenants. Visibility does not bypass contract rules. Unknown names and storage failures raise `RuntimeError`.

## `unpublish`

```python
engine.unpublish(name: str, audience: str) -> None
```

Withdraws visibility from the tenant or `"public"` audience. This is an operator action and takes no caller parameter.
