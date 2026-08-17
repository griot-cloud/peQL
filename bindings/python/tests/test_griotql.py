"""Tests for the GriotQL Python wheel.

Run (from `bindings/python`, after `maturin develop`):
    pytest
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import griotql


def _engine(tmp_path):
    path = str(tmp_path / "orders.parquet")
    pq.write_table(
        pa.table(
            {
                "order_id": [1, 2, 3, 4, 5],
                "email": [
                    "alice@acme.com",
                    "bob@globex.com",
                    "carol@acme.com",
                    "dan@initech.com",
                    "erin@acme.com",
                ],
                "region": ["EU", "US", "EU", "APAC", "US"],
            }
        ),
        path,
    )
    contract = json.dumps(
        {
            "contract_id": "sales_orders_v1",
            "version": "1",
            "dataset": "sales/orders/v1",
            "binding": {"parquet": path},
            "owner_tenant": "acme",
            "purposes": ["analytics"],
            # `columns` is the contract's read projection (upstream 2.0 change):
            # declare everything we expose, not just the masked column.
            "columns": [
                {"name": "order_id", "type": "int"},
                {"name": "email", "type": "text", "mask": "hash_sha256"},
                {"name": "region", "type": "text"},
            ],
            "row_filter": "region = 'EU'",
        }
    )
    return griotql.Engine.from_json_contracts([contract])


SQL = 'SELECT order_id, email, region FROM "sales/orders/v1" ORDER BY order_id'


def test_outsider_is_masked_and_filtered(tmp_path):
    eng = _engine(tmp_path)
    t = eng.query(SQL, griotql.Caller("user:bob", "analytics", "globex"))
    assert t.num_rows == 2  # EU-only
    emails = t.column("email").to_pylist()
    assert all("@" not in e and len(e) == 64 for e in emails)  # SHA-256 hex


def test_owner_sees_raw(tmp_path):
    eng = _engine(tmp_path)
    t = eng.query(SQL, griotql.Caller("user:alice", "analytics", "acme"))
    assert t.num_rows == 5
    assert any("@" in e for e in t.column("email").to_pylist())


def test_disallowed_purpose_is_denied(tmp_path):
    eng = _engine(tmp_path)
    with pytest.raises(Exception) as exc:
        eng.query(SQL, griotql.Caller("u", "marketing", "globex"))
    assert "denied" in str(exc.value)


def test_returns_pyarrow_table(tmp_path):
    eng = _engine(tmp_path)
    t = eng.query(SQL, griotql.Caller("u", "analytics", "acme"))
    assert isinstance(t, pa.Table)


# ─── Graph functions (2.0, R10 parity) ────────────────────────────────────────

GRAPH_REF = "process-graphs/zijani-operations/v1"


def _graph_engine():
    import os

    fixture = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__), "..", "..", "..", "fixtures", "graph",
            "zijani-operations-v1",
        )
    )
    contract = json.dumps(
        {
            "contract_id": "zijani_ops_graph",
            "version": "1",
            "dataset": GRAPH_REF,
            "binding": {"graph_snapshot": fixture},
            "owner_tenant": "zijani",
            "purposes": ["process_analysis"],
            "masks": {"owner": "redact"},
        }
    )
    return griotql.Engine.from_json_contracts([contract])


def test_graph_nodes_scan_from_python():
    eng = _graph_engine()
    t = eng.query(
        f"SELECT name, kind, data_refs FROM graph_nodes('{GRAPH_REF}')",
        griotql.Caller("user:ops", "process_analysis", "zijani"),
    )
    assert t.num_rows == 271
    # list columns round-trip to pyarrow (R10).
    assert pa.types.is_list(t.schema.field("data_refs").type)


def test_graph_traversal_masked_for_outsider():
    eng = _graph_engine()
    t = eng.query(
        f"SELECT name, owner, min_depth FROM graph_reachable('{GRAPH_REF}', "
        "'Collection quarantined; jericans to segregated disposal', 'both', 3)",
        griotql.Caller("svc:x", "process_analysis", "globex"),
    )
    assert t.num_rows > 0
    assert all(o == "***" for o in t.column("owner").to_pylist())
