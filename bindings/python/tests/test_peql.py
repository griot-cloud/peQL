"""Tests for the peQL Python package. Run from `bindings/python` after `maturin develop`: pytest"""

import pyarrow as pa
import pytest

import peql

CONTRACT = """
contract: sales/orders
version: 1
owner: acme
binding: {parquet: orders/}
expose:
  - {name: order_id, type: int64}
  - {name: email, type: utf8}
  - {name: region, type: utf8}
rules:
  - {id: analytics_only, op: decide, expr: "ctx.purpose == 'analytics'"}
  - {id: eu_only, op: admit, expr: "row.region == 'EU' || ctx.tenant == 'acme'"}
  - {id: mask_email, op: transform, column: email, expr: "ctx.tenant == 'acme' ? row.email : hash_sha256(row.email)"}
"""

ORDERS = pa.table(
    {
        "order_id": pa.array([1, 2, 3, 4, 5], pa.int64()),
        "email": ["alice@acme.com", "bob@globex.com", "carol@acme.com", "dan@initech.com", "erin@acme.com"],
        "region": ["EU", "US", "EU", "APAC", "US"],
    }
)

SQL = 'SELECT order_id, email, region FROM "sales/orders" ORDER BY order_id'


def _engine(tmp_path):
    eng = peql.Engine.open(tmp_path)
    eng.register(CONTRACT, ORDERS)
    report = eng.write("sales/orders", ORDERS)
    assert report["rows_written"] == 5 and report["verdict"]["valid"]
    eng.publish("sales/orders", "globex")
    return eng


def test_outsider_is_masked_and_filtered(tmp_path):
    t = _engine(tmp_path).query(SQL, peql.Caller("user:bob", "analytics", "globex"))
    assert t.column("order_id").to_pylist() == [1, 3]
    assert all(len(e) == 64 for e in t.column("email").to_pylist())


def test_owner_sees_raw(tmp_path):
    t = _engine(tmp_path).query(SQL, peql.Caller("user:alice", "analytics", "acme"))
    assert t.num_rows == 5
    assert t.column("email").to_pylist()[0] == "alice@acme.com"


def test_disallowed_purpose_is_refused(tmp_path):
    with pytest.raises(peql.Refused, match="analytics_only"):
        _engine(tmp_path).query(SQL, peql.Caller("user:bob", "marketing", "globex"))


def test_unpublished_tenants_cannot_see_it(tmp_path):
    with pytest.raises(peql.Refused, match="no contract"):
        _engine(tmp_path).query(SQL, peql.Caller("u", "analytics", "initech"))


def test_envelope_and_describe(tmp_path):
    eng = _engine(tmp_path)
    t, env = eng.query_with_envelope(SQL, peql.Caller("user:bob", "analytics", "globex"))
    assert isinstance(t, pa.Table)
    assert env["contracts"][0]["decisions"] == ["analytics_only"]
    assert env["rows"] == 2
    assert eng.describe("sales/orders", peql.Caller("u", "analytics", "acme")) == [
        ("order_id", "Int64"),
        ("email", "Utf8"),
        ("region", "Utf8"),
    ]
    assert eng.validate("sales/orders")["valid"]


def test_free_threaded_import_keeps_the_gil_disabled():
    import sys
    import sysconfig

    if not sysconfig.get_config_var("Py_GIL_DISABLED"):
        pytest.skip("requires a free-threaded interpreter")
    assert not sys._is_gil_enabled()


def test_parallel_queries_keep_each_callers_view(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    eng = _engine(tmp_path)

    def read(tenant):
        table = eng.query(SQL, peql.Caller("parallel", "analytics", tenant))
        expected = 5 if tenant == "acme" else 2
        assert table.num_rows == expected
        if tenant == "globex":
            assert all(len(value) == 64 for value in table.column("email").to_pylist())

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(read, ["acme", "globex"] * 8))


READINGS_CONTRACT = """
contract: demo/readings
version: 1
owner: acme
binding: {parquet: readings/}
expose:
  - {name: id, type: int64}
  - {name: kwh, type: int64}
rules:
  - {id: non_negative, op: assert, expr: "row.kwh >= 0", on_fail: drop}
"""

READINGS = pa.table({"id": pa.array([1, 2, 3, 4], pa.int64()), "kwh": pa.array([5, -1, 7, -2], pa.int64())})


def test_rows_that_break_a_rule_are_refused_set_aside_or_left_out(tmp_path):
    eng = peql.Engine.open(tmp_path)
    eng.register(READINGS_CONTRACT, READINGS)
    owner = peql.Caller("user:alice", "analytics", "acme")
    with pytest.raises(peql.Refused, match=r"non_negative \(2 rows\)"):
        eng.write("demo/readings", READINGS)

    report = eng.write("demo/readings", READINGS, on_violation="exclude")
    assert (report["rows_written"], report["rows_excluded"]) == (2, 2)
    assert report["violations"] == {"non_negative": 2}
    assert report["excluded"].column("id").to_pylist() == [2, 4]
    assert report["excluded"].column("_violations").to_pylist() == [["non_negative"], ["non_negative"]]

    report = eng.write("demo/readings", READINGS, on_violation="warn")
    assert report["rows_written"] == 4 and report["excluded"] is None
    t, envelope = eng.query_with_envelope('SELECT id FROM "demo/readings" ORDER BY id', owner)
    assert t.column("id").to_pylist() == [1, 3]
    assert envelope["contracts"][0]["excluded"] == {"non_negative": 2}
    with pytest.raises(peql.Refused, match=r"non_negative \(2 rows\)"):
        eng.query('SELECT id FROM "demo/readings"', owner, on_violation="refuse")
