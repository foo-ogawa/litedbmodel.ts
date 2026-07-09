"""Render-axis unit tests (WS7b, #31) — the normative dynamic-expansion spec, ported.

These assert the Python `render_operation` reproduces the dynamic-expansion spec edge cases
(SKIP existence, empty-WHERE degeneration, IN-list expansion incl. the empty `1 = 0`, `?`→`$N`
for Postgres) directly — independent of the frozen corpus — so a render regression fails loudly
here too. They are the SAME semantics the vector corpus pins byte-for-byte.
"""

from __future__ import annotations

import pytest

from litedbmodel_runtime import (
    MYSQL,
    POSTGRES,
    SQLITE,
    dialect_for,
    render_operation,
    to_dollar_placeholders,
)


def _op(sql, where=None, params=None, component="Select"):
    return {"component": component, "sql": sql, "where": where, "params": params or [], "assembly": {"shape": "items"}}


def test_no_where_static_params_in_order():
    op = _op(
        "INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, title",
        params=[{"ref": ["authorId"]}, {"ref": ["title"]}],
        component="Insert",
    )
    r = render_operation(op, {"authorId": 7, "title": "Hello"}, SQLITE)
    assert r.sql == "INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, title"
    assert r.params == [7, "Hello"]


def test_skip_fragment_present_when_truthy():
    op = _op(
        "SELECT id, status FROM posts{where}",
        where={
            "connector": "AND",
            "fragments": [
                {"always": True, "sql": "author_id = ?", "params": [{"ref": ["authorId"]}]},
                {"when": {"ne": [{"refOpt": ["status"]}, None]}, "sql": "status = ?", "params": [{"ref": ["status"]}]},
            ],
        },
    )
    r = render_operation(op, {"authorId": 7, "status": "live"}, SQLITE)
    assert r.sql == "SELECT id, status FROM posts WHERE author_id = ? AND status = ?"
    assert r.params == [7, "live"]


def test_skip_fragment_dropped_when_null():
    op = _op(
        "SELECT id, status FROM posts{where}",
        where={
            "connector": "AND",
            "fragments": [
                {"always": True, "sql": "author_id = ?", "params": [{"ref": ["authorId"]}]},
                {"when": {"ne": [{"refOpt": ["status"]}, None]}, "sql": "status = ?", "params": [{"ref": ["status"]}]},
            ],
        },
    )
    r = render_operation(op, {"authorId": 7, "status": None}, SQLITE)
    assert r.sql == "SELECT id, status FROM posts WHERE author_id = ?"
    assert r.params == [7]


def test_empty_where_degeneration_drops_keyword():
    op = _op(
        "SELECT id FROM posts{where}",
        where={
            "connector": "AND",
            "fragments": [
                {"when": {"ne": [{"refOpt": ["status"]}, None]}, "sql": "status = ?", "params": [{"ref": ["status"]}]},
            ],
        },
    )
    r = render_operation(op, {"status": None}, SQLITE)
    assert r.sql == "SELECT id FROM posts"  # whole ` WHERE ` splice collapses
    assert r.params == []


def test_in_list_expansion_n():
    op = _op(
        "SELECT id FROM posts{where}",
        where={"connector": "AND", "fragments": [{"always": True, "sql": "id IN (?)", "params": [{"ref": ["ids"]}], "expand": 0}]},
    )
    r = render_operation(op, {"ids": [1, 2, 3]}, SQLITE)
    assert r.sql == "SELECT id FROM posts WHERE id IN (?, ?, ?)"
    assert r.params == [1, 2, 3]


def test_in_list_empty_degenerates_to_always_false():
    op = _op(
        "SELECT id FROM posts{where}",
        where={"connector": "AND", "fragments": [{"always": True, "sql": "id IN (?)", "params": [{"ref": ["ids"]}], "expand": 0}]},
    )
    r = render_operation(op, {"ids": []}, SQLITE)
    assert r.sql == "SELECT id FROM posts WHERE 1 = 0"
    assert r.params == []  # empty IN pushes NO params (byte-identical to v1)


def test_in_list_non_array_fails_closed():
    op = _op(
        "SELECT id FROM posts{where}",
        where={"connector": "AND", "fragments": [{"always": True, "sql": "id IN (?)", "params": [{"ref": ["ids"]}], "expand": 0}]},
    )
    with pytest.raises(ValueError, match="did not bind to an array"):
        render_operation(op, {"ids": 5}, SQLITE)


def test_postgres_dollar_placeholders_single_final_pass():
    op = _op(
        "SELECT id FROM posts{where}",
        where={
            "connector": "AND",
            "fragments": [
                {"always": True, "sql": "author_id = ?", "params": [{"ref": ["authorId"]}]},
                {"always": True, "sql": "id IN (?)", "params": [{"ref": ["ids"]}], "expand": 0},
            ],
        },
    )
    r = render_operation(op, {"authorId": 7, "ids": [1, 2, 3]}, POSTGRES)
    assert r.sql == "SELECT id FROM posts WHERE author_id = $1 AND id IN ($2, $3, $4)"
    assert r.params == [7, 1, 2, 3]


def test_mysql_and_sqlite_keep_question_marks():
    op = _op("SELECT id FROM posts WHERE id = ?", params=[{"ref": ["id"]}])
    assert render_operation(op, {"id": 3}, MYSQL).sql == "SELECT id FROM posts WHERE id = ?"
    assert render_operation(op, {"id": 3}, SQLITE).sql == "SELECT id FROM posts WHERE id = ?"


def test_to_dollar_placeholders_helper():
    assert to_dollar_placeholders("a ? b ? c ?") == "a $1 b $2 c $3"


def test_dialect_for_unknown_fails_closed():
    with pytest.raises(ValueError, match="unknown dialect"):
        dialect_for("oracle")
