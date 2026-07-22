"""Unit tests for the SHARED relation-grouping CORE (#141) and the op-independent leaf transport.

These pin the Python grouping core (``litedbmodel_runtime.grouping``) and the ``pluck``/``group`` leaf
handlers (``litedbmodel_runtime.leaves.make_handlers`` — the bc handler ABI ``handler(ports, ctx)`` the
python ir-exec runner injects) behaviour-identical to the TS SSoT ``src/scp/grouping.ts`` /
``src/scp/leaves.ts`` and the Rust port ``rust/litedbmodel_runtime/src/{grouping,leaves}.rs`` —
null-drop, dedupe preserving insertion order, composite tuple keys, hasMany (list, ``[]`` when none),
and single (first child or ``None``). Python's native value model is the plain ``dict`` record.
"""

from __future__ import annotations

import sqlite3

from litedbmodel_runtime.driver import SqliteDriver
from litedbmodel_runtime.grouping import (
    attach_to_parent,
    dedupe_key_tuples,
    group_by_key,
    key_identity,
)
from litedbmodel_runtime.leaves import make_handlers

# The pluck/group leaves are pure in-memory grouping (no SQL), so any driver+dialect binds them; a
# bare in-memory sqlite driver supplies the ExecutionContext the handler factory closes over.
_HANDLERS = make_handlers(SqliteDriver(sqlite3.connect(":memory:")), "sqlite")
_pluck = _HANDLERS["pluck"]
_group = _HANDLERS["group"]
_CTX = {"nodeId": "n0", "component": "test"}


# ── key_identity — mirror of JS String(v) ──────────────────────────────────────


def test_key_identity_matches_js_string():
    # whole float → integer text (a scanned INT column arrives as a whole float), bool/string verbatim.
    assert key_identity([1.0]) == "1"
    assert key_identity([2]) == "2"
    assert key_identity(["x"]) == "x"
    assert key_identity([True]) == "true"
    assert key_identity([False]) == "false"
    assert key_identity([1.5]) == "1.5"
    # tuple → single-space joined.
    assert key_identity([1, "a"]) == "1 a"
    # null totality arm (never reached in a real grouping — nulls are dropped first).
    assert key_identity([None]) == "null"


# ── dedupe_key_tuples — null-drop + dedupe preserving order ─────────────────────


def test_dedupe_drops_null_and_absent_dedupes_preserving_order():
    rows = [
        {"id": 2},
        {"id": 1},
        {"id": 2},  # dup
        {"id": None},  # dropped (null)
        {"other": 9},  # dropped (absent id)
    ]
    keys = dedupe_key_tuples(rows, ["id"])
    assert [t[0] for t in keys] == [2, 1]  # insertion order, deduped, nulls/absent dropped


def test_dedupe_collapses_int_and_str_by_string_identity():
    # 1 and "1" collapse exactly as String(v) does (both stringify to "1").
    keys = dedupe_key_tuples([{"id": 1}, {"id": "1"}], ["id"])
    assert len(keys) == 1
    assert keys[0][0] == 1  # the FIRST occurrence is kept


def test_dedupe_composite_tuple():
    rows = [
        {"t": 1, "u": 9},
        {"t": 1, "u": 9},  # dup tuple
        {"t": 1, "u": 8},
        {"t": 1, "u": None},  # dropped (partial null)
    ]
    keys = dedupe_key_tuples(rows, ["t", "u"])
    assert len(keys) == 2
    assert key_identity(keys[0]) == "1 9"
    assert key_identity(keys[1]) == "1 8"


# ── group_by_key + attach_to_parent — hasMany / single ──────────────────────────


def test_group_and_attach_has_many():
    parents = [{"id": 1}, {"id": 2}]
    children = [
        {"author_id": 1, "t": "a"},
        {"author_id": 1, "t": "b"},
        {"author_id": 2, "t": "c"},
        {"author_id": None, "t": "x"},  # dropped (null fk)
    ]
    by_key = group_by_key(children, ["author_id"])
    # parent 1 → two children in input order
    a1 = attach_to_parent(parents[0], ["id"], by_key, False)
    assert [c["t"] for c in a1] == ["a", "b"]
    # parent 2 → one child (still a list for hasMany)
    a2 = attach_to_parent(parents[1], ["id"], by_key, False)
    assert [c["t"] for c in a2] == ["c"]
    # a parent with no matches → empty list
    a3 = attach_to_parent({"id": 3}, ["id"], by_key, False)
    assert a3 == []
    # a parent with a null key → empty list (matches nothing)
    a4 = attach_to_parent({"id": None}, ["id"], by_key, False)
    assert a4 == []


def test_attach_single_returns_first_or_none():
    children = [
        {"post_id": 5, "b": "first"},
        {"post_id": 5, "b": "second"},
    ]
    by_key = group_by_key(children, ["post_id"])
    # single → the FIRST matching child (input order)
    one = attach_to_parent({"id": 5}, ["id"], by_key, True)
    assert one["b"] == "first"
    # single, no match → None
    none = attach_to_parent({"id": 6}, ["id"], by_key, True)
    assert none is None
    # single, null parent key → None
    assert attach_to_parent({"id": None}, ["id"], by_key, True) is None


def test_group_by_key_composite_fk():
    children = [
        {"a": 1, "b": 2, "v": "x"},
        {"a": 1, "b": 2, "v": "y"},
        {"a": 1, "b": 3, "v": "z"},
    ]
    by_key = group_by_key(children, ["a", "b"])
    assert [c["v"] for c in by_key["1 2"]] == ["x", "y"]
    assert [c["v"] for c in by_key["1 3"]] == ["z"]


# ── leaf handler: pluck (ports dict → {"ok": key array}; col is the ordered key TUPLE) ──────────────


def test_pluck_deduped_non_null_key_array():
    rows = [{"id": 2}, {"id": 1}, {"id": 2}, {"id": None}, {"other": 9}]
    # single-key col → a flat scalar key array; deduped, insertion order, null/absent dropped.
    assert _pluck({"rows": rows, "col": ["id"]}, _CTX) == {"ok": [2, 1]}


def test_pluck_empty_when_no_keys():
    assert _pluck({"rows": [], "col": ["id"]}, _CTX) == {"ok": []}
    assert _pluck({"rows": [{"id": None}], "col": ["id"]}, _CTX) == {"ok": []}


def test_pluck_composite_tuple_emits_array_of_tuples():
    rows = [{"t": 1, "u": 9}, {"t": 1, "u": 9}, {"t": 1, "u": 8}]
    assert _pluck({"rows": rows, "col": ["t", "u"]}, _CTX) == {"ok": [[1, 9], [1, 8]]}


# ── leaf handler: group (ports dict → {"ok": nested parents}; delegates to the grouping core) ───────


def test_group_leaf_has_many_nests_list_under_into():
    parents = [{"id": 1}, {"id": 2}, {"id": 3}]
    children = [
        {"author_id": 1, "t": "a"},
        {"author_id": 1, "t": "b"},
        {"author_id": 2, "t": "c"},
    ]
    outcome = _group(
        {"parents": parents, "children": children, "pk": ["id"], "fk": ["author_id"], "into": "posts", "single": False},
        _CTX,
    )
    out = outcome["ok"]
    assert [p["id"] for p in out] == [1, 2, 3]  # parent order preserved
    assert [c["t"] for c in out[0]["posts"]] == ["a", "b"]
    assert [c["t"] for c in out[1]["posts"]] == ["c"]
    assert out[2]["posts"] == []  # no matches → empty list
    # parents are not mutated (new dicts)
    assert "posts" not in parents[0]


def test_group_leaf_single_nests_first_child_or_none():
    parents = [{"id": 5}, {"id": 6}]
    children = [{"post_id": 5, "b": "first"}, {"post_id": 5, "b": "second"}]
    outcome = _group(
        {"parents": parents, "children": children, "pk": ["id"], "fk": ["post_id"], "into": "author", "single": True},
        _CTX,
    )
    out = outcome["ok"]
    assert out[0]["author"]["b"] == "first"  # first matching child
    assert out[1]["author"] is None  # no match → None
