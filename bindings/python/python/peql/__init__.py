"""peQL: query data through parcel contracts.

Every table is a contract. The contract decides which rows and columns each caller gets, and
how values are masked or noised; the engine enforces it inside the query plan.

    import peql, pyarrow as pa

    engine = peql.Engine.open("./workspace")
    engine.register(open("orders.yaml").read(), schema=orders.schema)
    engine.write("sales/orders", orders)
    engine.publish("sales/orders", "globex")
    table = engine.query(
        'SELECT region, SUM(amount_cents) FROM "sales/orders" GROUP BY region',
        peql.Caller("user:bob", "analytics", "globex"),
    )
"""

from __future__ import annotations

import json
from typing import Iterable, Optional

import pyarrow as _pa

from ._native import Caller, Engine as _NativeEngine

__all__ = ["Engine", "Caller", "Refused"]
__version__ = "0.4.0"

# A query refused by policy (denied, not servable, budget spent, unknown contract).
Refused = PermissionError


def _ipc(obj) -> bytes:
    """A pyarrow Table, RecordBatch or Schema as Arrow IPC stream bytes."""
    if isinstance(obj, _pa.Schema):
        schema, batches = obj, []
    elif isinstance(obj, _pa.RecordBatch):
        schema, batches = obj.schema, [obj]
    else:
        schema, batches = obj.schema, obj.to_batches()
    sink = _pa.BufferOutputStream()
    with _pa.ipc.new_stream(sink, schema) as w:
        for b in batches:
            w.write_batch(b)
    return sink.getvalue().to_pybytes()


class Engine:
    """A peQL engine: a workspace on disk (:meth:`open`) or in memory (:meth:`in_memory`)."""

    def __init__(self, native: _NativeEngine) -> None:
        self._native = native

    @classmethod
    def open(cls, root) -> "Engine":
        return cls(_NativeEngine.open(str(root)))

    @classmethod
    def in_memory(cls, base=".") -> "Engine":
        return cls(_NativeEngine.in_memory(str(base)))

    def register(self, contract: str, schema) -> str:
        """Compile a parcel contract (YAML or JSON text) against a pyarrow schema or table."""
        if not isinstance(schema, _pa.Schema):
            schema = schema.schema
        return self._native.register(contract, _ipc(schema))

    def register_bundle(self, bundle: str) -> str:
        """Register a bundle from `parcel compile -o` (its JSON text)."""
        return self._native.register_bundle(bundle)

    def write(self, name: str, data, append: bool = False) -> dict:
        """Write a pyarrow Table under a contract; returns the report with its verdict."""
        return json.loads(self._native.write(name, _ipc(data), append))

    def validate(self, name: str) -> dict:
        return json.loads(self._native.validate(name))

    def query(self, sql: str, caller: Caller) -> _pa.Table:
        """Run SQL as ``caller``. Raises :class:`Refused` when policy refuses it."""
        return self.query_with_envelope(sql, caller)[0]

    def query_with_envelope(self, sql: str, caller: Caller):
        """The result and its envelope: contracts applied, scan statistics, attestation."""
        ipc, envelope = self._native.query(sql, caller)
        table = _pa.ipc.open_file(_pa.py_buffer(ipc)).read_all() if ipc else _pa.table({})
        return table, json.loads(envelope)

    def describe(self, name: str, caller: Caller):
        """The columns a caller would see, as (name, type) pairs."""
        return self._native.describe(name, caller)

    def publish(self, name: str, audience: str) -> None:
        self._native.publish(name, audience)

    def unpublish(self, name: str, audience: str) -> None:
        self._native.unpublish(name, audience)

    def contracts(self):
        return self._native.contracts()
