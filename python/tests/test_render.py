"""Static-makeSQL render-axis unit tests (epic #43/#45).

These assert the Python static-makeSQL render path reproduces the render edge cases directly —
SKIP-fragment drop, WHERE/AND connector resolution from the present set, IN-list single-JSON
param, LIMIT deferral, and `?`→`$N` for Postgres — independent of the frozen corpus, so a render
regression fails loudly here too. They are the SAME semantics the vector corpus pins byte-for-byte.
The CLOSED Expression-IR evaluation is delegated to behavior-contracts (`evaluate_expression`).
"""

from __future__ import annotations

from litedbmodel_runtime import render_read_primary, render_statements
from litedbmodel_runtime.static_bundle import assemble_make_sql, compose_make_sql, render_placeholders


_FEED_STATEMENTS = [
    {"sql": "SELECT id, author_id, title, status FROM posts", "params": []},
    {"sql": "author_id = ?", "params": [{"ref": ["author_id"]}], "whereFragment": True},
    {
        "sql": "status = ?",
        "params": [{"ref": ["status"]}],
        "whereFragment": True,
        "skip": {"not": [{"ne": [{"refOpt": ["status"]}, None]}]},
    },
    {"sql": " ORDER BY id ASC", "params": []},
    {"sql": " LIMIT ?", "params": [{"coalesce": [{"refOpt": ["limit"]}, 20]}]},
]


def test_render_all_fragments_present():
    r = render_statements(_FEED_STATEMENTS, "sqlite", {"author_id": 7, "status": "live", "limit": 5})
    assert r["sql"] == "SELECT id, author_id, title, status FROM posts WHERE author_id = ? AND status = ? ORDER BY id ASC LIMIT ?"
    assert r["params"] == [7, "live", 5]


def test_render_skip_drops_status_and_defaults_limit():
    # status absent (present-as-null) → skip drops the fragment; coalesce defaults the limit.
    r = render_statements(_FEED_STATEMENTS, "sqlite", {"author_id": 7, "status": None, "limit": None})
    assert r["sql"] == "SELECT id, author_id, title, status FROM posts WHERE author_id = ? ORDER BY id ASC LIMIT ?"
    assert r["params"] == [7, 20]


def test_render_postgres_placeholder_rewrite():
    r = render_statements(_FEED_STATEMENTS, "postgres", {"author_id": 7, "status": "live", "limit": 5})
    assert r["sql"] == "SELECT id, author_id, title, status FROM posts WHERE author_id = $1 AND status = $2 ORDER BY id ASC LIMIT $3"
    assert r["params"] == [7, "live", 5]


def test_render_in_list_single_json_param_sqlite():
    stmts = [
        {"sql": "SELECT id FROM posts", "params": []},
        {
            "sql": "id IN (SELECT value FROM json_each(?))",
            "params": [{"__jsonArray": {"ref": ["ids"]}, "dialect": "sqlite"}],
            "whereFragment": True,
        },
    ]
    r = render_statements(stmts, "sqlite", {"ids": [1, 2, 3]})
    assert r["sql"] == "SELECT id FROM posts WHERE id IN (SELECT value FROM json_each(?))"
    assert r["params"] == ["[1,2,3]"]  # single JSON param (server-side expansion)


def test_render_in_list_postgres_binds_array():
    stmts = [
        {"sql": "SELECT id FROM posts", "params": []},
        {"sql": "id = ANY(?)", "params": [{"__jsonArray": {"ref": ["ids"]}, "dialect": "postgres"}], "whereFragment": True},
    ]
    r = render_statements(stmts, "postgres", {"ids": [1, 2, 3]})
    assert r["sql"] == "SELECT id FROM posts WHERE id = ANY($1)"
    assert r["params"] == [[1, 2, 3]]  # array bound as ONE text[] param


def test_placeholder_rewrite_quote_aware():
    # A `?` inside a string literal is NOT a placeholder (mirrors TS renderPlaceholders).
    assert render_placeholders("SELECT '?' AS q WHERE a = ?", "postgres") == "SELECT '?' AS q WHERE a = $1"
    assert render_placeholders("a = ? AND b = ?", "sqlite") == "a = ? AND b = ?"


def test_assemble_nested_makesql_splices():
    node = {"sql": "a = ? AND b IN (?)", "params": [1, {"sql": "SELECT id FROM t WHERE x = ?", "params": [2]}]}
    assembled = assemble_make_sql(node)
    assert assembled["sql"] == "a = ? AND b IN (SELECT id FROM t WHERE x = ?)"
    assert assembled["params"] == [1, 2]


def test_compose_concatenates_present_nodes():
    nodes = [{"sql": "SELECT * FROM t", "params": []}, {"sql": " WHERE a = ?", "params": [1]}]
    assembled = compose_make_sql(nodes)
    assert assembled["sql"] == "SELECT * FROM t WHERE a = ?"
    assert assembled["params"] == [1]


def test_render_read_primary_picks_first_body_node():
    graph = {
        "dialect": "sqlite",
        "name": "Feed",
        "statementsById": {"n0": _FEED_STATEMENTS},
        "optionalHeads": ["status", "limit"],
        "ir": {"irVersion": 1, "exprVersion": 2, "components": [{"name": "Feed", "body": [{"id": "n0"}]}]},
    }
    r = render_read_primary(graph, {"author_id": 7})
    # status + limit omitted → normalized present-as-null → skip drop + coalesce default.
    assert r["sql"] == "SELECT id, author_id, title, status FROM posts WHERE author_id = ? ORDER BY id ASC LIMIT ?"
    assert r["params"] == [7, 20]
