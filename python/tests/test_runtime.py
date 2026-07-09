"""Runtime unit tests (WS7b, #31): execute_bundle / execute_transaction_bundle end-to-end.

These exercise the SQL-backend runtime against a REAL in-proc sqlite3 driver, independent of the
frozen corpus, to prove:
  - read bundle → bc run_behavior (plan/map/wire/output) + SQL handlers → assembled Φ output;
  - write bundle → gate-first transaction (commit; requires short-circuit ROLLBACK; idempotent
    duplicate short-circuit) with `$.entity` RETURNING exposure + emit-payload JSON serialization;
  - bc-core is CONSUMED (the surrogate `__scope` port is evaluated by bc, the map orchestration is
    bc's) — not reimplemented.
"""

from __future__ import annotations

from litedbmodel_runtime import execute_bundle, execute_transaction_bundle
from litedbmodel_runtime.driver import SqliteDriver


# ── A minimal read bundle: one Select + a per-row map Select (mirrors the Feed shape) ──

READ_SCHEMA = [
    "CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT, created_at TEXT NOT NULL)",
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    "INSERT INTO posts VALUES (1, 7, 'Hello', 'live', '2026-02-01')",
    "INSERT INTO posts VALUES (2, 7, 'World', 'draft', '2026-03-01')",
    "INSERT INTO users VALUES (7, 'Ada')",
]


def _read_bundle():
    return {
        "irVersion": 1,
        "exprVersion": 1,
        "dialect": "sqlite",
        "optionalHeads": ["status"],
        "relations": {},
        "operations": {
            "n0": {
                "component": "Select",
                "sql": "SELECT id, author_id, title, status FROM posts{where} ORDER BY id ASC",
                "where": {
                    "connector": "AND",
                    "fragments": [
                        {"always": True, "sql": "author_id = ?", "params": [{"ref": ["author_id"]}]},
                        {"when": {"ne": [{"refOpt": ["status"]}, None]}, "sql": "status = ?", "params": [{"ref": ["status"]}]},
                    ],
                },
                "params": [],
                "assembly": {"shape": "items"},
            },
            "n1": {
                "component": "Select",
                "sql": "SELECT id, name FROM users{where}",
                "where": {"connector": "AND", "fragments": [{"always": True, "sql": "id = ?", "params": [{"ref": ["$e0", "author_id"]}]}]},
                "params": [],
                "assembly": {"shape": "items"},
            },
        },
        "component": {
            "name": "Feed",
            "inputPorts": {"author_id": {"required": True}, "status": {"required": True}},
            "body": [
                {"id": "n0", "component": "Select", "ports": {"__scope": {"obj": {"author_id": {"ref": ["author_id"]}, "status": {"ref": ["status"]}}}}},
                {
                    "id": "n1",
                    "map": {
                        "over": {"ref": ["n0"]},
                        "as": "$e0",
                        "component": "Select",
                        "parent": "n0",
                        "ports": {"__scope": {"obj": {"$e0": {"ref": ["$e0"]}}}},
                    },
                },
            ],
            "output": {"obj": {"posts": {"ref": ["n0"]}, "authors": {"ref": ["n1"]}}},
            "plan": {"concurrency": 16, "groups": [[0], [1]]},
        },
    }


def test_read_bundle_status_present():
    driver = SqliteDriver.in_memory(READ_SCHEMA)
    try:
        out = execute_bundle(_read_bundle(), {"author_id": 7, "status": "live"}, driver)
    finally:
        driver.close()
    assert out["posts"] == [{"id": 1, "author_id": 7, "title": "Hello", "status": "live"}]
    # `authors` is the per-post map result (a list per post) — bc's map orchestration, not ours.
    assert out["authors"] == [[{"id": 7, "name": "Ada"}]]


def test_read_bundle_status_absent_skips_fragment():
    driver = SqliteDriver.in_memory(READ_SCHEMA)
    try:
        # status omitted → normalized to present-as-null → SKIP guard drops the status fragment.
        out = execute_bundle(_read_bundle(), {"author_id": 7}, driver)
    finally:
        driver.close()
    assert [p["id"] for p in out["posts"]] == [1, 2]  # both posts, no status filter
    assert out["authors"] == [[{"id": 7, "name": "Ada"}], [{"id": 7, "name": "Ada"}]]


# ── A minimal write bundle: gate-first transaction plan (mirrors the Create shape) ──

WRITE_SCHEMA = [
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, post_count INTEGER NOT NULL DEFAULT 0)",
    "CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, author_id INTEGER NOT NULL REFERENCES users(id), title TEXT NOT NULL, created_at TEXT)",
    "CREATE TABLE idem (token TEXT PRIMARY KEY)",
    "CREATE TABLE outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL, payload TEXT NOT NULL)",
    "INSERT INTO users (id, name, post_count) VALUES (7, 'Ada', 2)",
]


def _write_bundle():
    return {
        "irVersion": 1,
        "exprVersion": 1,
        "dialect": "sqlite",
        "optionalHeads": [],
        "relations": {},
        "operations": {},
        "component": {"name": "Create", "inputPorts": {}, "body": [], "output": {"obj": {}}},
        "transaction": {
            "phase": "create",
            "entityFrom": "tx_body_2",
            "statements": [
                {"id": "tx_requires_0", "gate": "existsElseRollback", "op": {"component": "Select", "sql": "SELECT 1 FROM users WHERE id = ?", "where": None, "params": [{"ref": ["author_id"]}], "assembly": {"shape": "items"}}},
                {"id": "tx_idem_1", "gate": "insertedElseNoop", "op": {"component": "Insert", "sql": "INSERT INTO idem (token) VALUES (?) ON CONFLICT DO NOTHING", "where": None, "params": [{"ref": ["request_id"]}], "assembly": {"shape": "items"}}},
                {"id": "tx_body_2", "op": {"component": "Insert", "sql": "INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, author_id, title", "where": None, "params": [{"ref": ["author_id"]}, {"ref": ["title"]}], "assembly": {"shape": "items"}}},
                {"id": "tx_derive_3", "op": {"component": "Update", "sql": "UPDATE users SET post_count = post_count + ?{where}", "where": {"connector": "AND", "fragments": [{"always": True, "sql": "id = ?", "params": [{"ref": ["author_id"]}]}]}, "params": [1], "assembly": {"shape": "items"}}},
                {"id": "tx_emit_4", "op": {"component": "Insert", "sql": "INSERT INTO outbox (type, payload) VALUES (?, ?)", "where": None, "params": ["PostCreated", {"obj": {"postId": {"ref": ["__entity", "id"]}, "userId": {"ref": ["author_id"]}}}], "assembly": {"shape": "items"}}},
            ],
        },
    }


def test_write_tx_commits_gate_first():
    driver = SqliteDriver.in_memory(WRITE_SCHEMA)
    try:
        res = execute_transaction_bundle(_write_bundle(), {"author_id": 7, "title": "New Post", "request_id": "req-1"}, driver)
        assert res["committed"] is True
        assert res["entity"] == {"id": 1, "author_id": 7, "title": "New Post"}
        assert res["executed"] == ["tx_requires_0", "tx_idem_1", "tx_body_2", "tx_derive_3", "tx_emit_4"]
        post_count = driver.prepare("SELECT post_count FROM users WHERE id = 7").all([])[0]["post_count"]
        assert post_count == 3  # derive +1
        payload = driver.prepare("SELECT payload FROM outbox").all([])[0]["payload"]
        assert payload == '{"postId":1,"userId":7}'  # compact JSON, matching JS JSON.stringify
    finally:
        driver.close()


def test_write_tx_requires_gate_short_circuits():
    driver = SqliteDriver.in_memory(WRITE_SCHEMA)
    try:
        res = execute_transaction_bundle(_write_bundle(), {"author_id": 999, "title": "Orphan", "request_id": "req-2"}, driver)
        assert res["committed"] is False
        assert res["shortCircuit"] == {"statementId": "tx_requires_0", "reason": "requires_absent"}
        assert res["entity"] is None
        assert res["executed"] == ["tx_requires_0"]  # tail never ran (gate-first)
        # ROLLBACK left the DB unchanged.
        assert driver.prepare("SELECT COUNT(*) AS c FROM posts").all([])[0]["c"] == 0
    finally:
        driver.close()


def test_write_tx_idempotent_duplicate_short_circuits():
    driver = SqliteDriver.in_memory(WRITE_SCHEMA)
    try:
        first = execute_transaction_bundle(_write_bundle(), {"author_id": 7, "title": "P", "request_id": "dup"}, driver)
        assert first["committed"] is True
        # Second run with the SAME request_id: the idempotency INSERT affects 0 rows → no-op.
        second = execute_transaction_bundle(_write_bundle(), {"author_id": 7, "title": "P2", "request_id": "dup"}, driver)
        assert second["committed"] is False
        assert second["shortCircuit"]["reason"] == "idempotent_duplicate"
        # No double write: exactly one post, post_count incremented exactly once.
        assert driver.prepare("SELECT COUNT(*) AS c FROM posts").all([])[0]["c"] == 1
        assert driver.prepare("SELECT post_count FROM users WHERE id = 7").all([])[0]["post_count"] == 3
    finally:
        driver.close()
