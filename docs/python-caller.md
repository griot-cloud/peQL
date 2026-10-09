# `peql.Caller`

```python
peql.Caller(id, purpose, tenant, tier=None, classification=None, roles=None, clearance=None)
```

The authenticated context supplied to `Engine.query`, `Engine.query_with_envelope` and `Engine.describe`. Your application must verify these values; constructing `Caller` does not authenticate a user. The object is implemented by the native extension and exposes no public data methods or readable attributes beyond its representation.

| Parameter | Type and default | Meaning |
| --- | --- | --- |
| `id` | `str`, required | Stable caller identifier. |
| `purpose` | `str`, required | Declared purpose of this query. |
| `tenant` | `str`, required | Tenant used for visibility and rules. |
| `tier` | `str | None = None` | Caller tier; `None` becomes the empty string. |
| `classification` | `str | None = None` | Classification; `None` becomes the empty string. |
| `roles` | `list[str] | None = None` | Roles; `None` becomes an empty list. |
| `clearance` | `int | None = None` | Clearance level; `None` becomes `0`. |

```python
caller = peql.Caller(
    id="supplier:acme",
    purpose="fulfilment",
    tenant="acme",
    roles=["supplier"],
)
rows = engine.query('SELECT * FROM "sales/orders"', caller)
```

The argument order is `id, purpose, tenant`, so named arguments make their roles clear. The native `repr(caller)` shows `id`, `purpose` and `tenant`.
