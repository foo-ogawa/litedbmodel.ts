"""Runtime unit tests (epic #43/#45): execute_bundle / execute_transaction_bundle end-to-end.

These exercise the static-makeSQL SQL-backend runtime against a REAL in-proc sqlite3 driver,
independent of the frozen corpus, to prove:
  - read bundle → the NATIVE read-graph walker (plan/map/wire/output owned by litedbmodel; #12 —
    no bc run_behavior, no surrogate) → assembled Φ output;
  - write bundle → gate-first transaction (commit; requires short-circuit ROLLBACK; idempotent
    duplicate short-circuit) with `$.entity` RETURNING exposure + emit-payload JSON serialization;
  - bc-core is CONSUMED only for the deferred value-specs + skip (`evaluate_expression`) — the map
    orchestration is the walker's, not reimplemented per node.

The bundle is the LOCKED static-makeSQL shape: a read carries a `readGraph` (`compileBehaviors`'
REAL `Select`/map `ComponentGraphIR` + per-node STATIC `{sql, params, skip?, whereFragment?}`
statement templates keyed by node id); a write carries a `transaction` plan of `{sql, params}` ops.
"""

from __future__ import annotations

from litedbmodel_runtime import execute_bundle
from litedbmodel_runtime.runtime import _execute_transaction_bundle  # internal per-command auto-tx (guard opt-out)
from litedbmodel_runtime.driver import SqliteDriver

# ── A minimal read bundle: one Select + a per-row map Select (mirrors the Feed shape) ──

READ_SCHEMA = [
    "CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT, created_at TEXT NOT NULL)",
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
    "INSERT INTO posts VALUES (1, 7, 'Hello', 'live', '2026-02-01')",
    "INSERT INTO posts VALUES (2, 7, 'World', 'draft', '2026-03-01')",
    "INSERT INTO users VALUES (7, 'Ada')",
]

_N0_STATEMENTS = [
    {"sql": "SELECT id, author_id, title, status FROM posts", "params": []},
    {"sql": "author_id = ?", "params": [{"ref": ["author_id"]}], "whereFragment": True},
    {
        "sql": "status = ?",
        "params": [{"ref": ["status"]}],
        "whereFragment": True,
        "skip": {"not": [{"ne": [{"refOpt": ["status"]}, None]}]},
    },
    {"sql": " ORDER BY id ASC", "params": []},
]

_N1_STATEMENTS = [
    {"sql": "SELECT id, name FROM users", "params": []},
    {"sql": "id = ?", "params": [{"ref": ["$e0", "author_id"]}], "whereFragment": True},
]


def _read_bundle():
    return {
        "dialect": "sqlite",
        "name": "Feed",
        "readGraph": {
            "dialect": "sqlite",
            "name": "Feed",
            "statementsById": {"n0": _N0_STATEMENTS, "n1": _N1_STATEMENTS},
            "optionalHeads": ["status"],
            "ir": {
                "irVersion": 1,
                "exprVersion": 2,
                "components": [
                    {
                        "name": "Feed",
                        "inputPorts": {"author_id": {"required": True}, "status": {"required": True}},
                        "body": [
                            {"id": "n0"},
                            {
                                "id": "n1",
                                "map": {"over": {"ref": ["n0"]}, "as": "$e0", "parent": "n0"},
                            },
                        ],
                        "output": {"obj": {"posts": {"ref": ["n0"]}, "authors": {"ref": ["n1"]}}},
                        "plan": {"concurrency": 16, "groups": [[0], [1]]},
                    }
                ],
            },
        },
        "optionalHeads": ["status"],
        "relations": {},
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
        "dialect": "sqlite",
        "name": "Create",
        "optionalHeads": [],
        "relations": {},
        "transaction": {
            "phase": "create",
            "entityFrom": "tx_body_2",
            "onIdempotentHit": "rollback",
            "statements": [
                {"id": "tx_requires_0", "role": "gate:requires", "gate": "existsElseRollback", "label": "requires users", "op": {"sql": "SELECT 1 FROM users WHERE id = ?", "params": [{"ref": ["author_id"]}]}},
                {"id": "tx_idem_1", "role": "gate:idempotency", "gate": "insertedElseNoop", "label": "idempotency idem", "op": {"sql": "INSERT INTO idem (token) VALUES (?) ON CONFLICT DO NOTHING", "params": [{"ref": ["request_id"]}]}},
                {"id": "tx_body_2", "role": "body", "label": "Insert", "op": {"sql": "INSERT INTO posts (author_id, title) VALUES (?, ?) RETURNING id, author_id, title", "params": [{"ref": ["author_id"]}, {"ref": ["title"]}]}},
                {"id": "tx_derive_3", "role": "derive", "label": "derive users.post_count", "op": {"sql": "UPDATE users SET post_count = post_count + ? WHERE id = ?", "params": [1, {"ref": ["author_id"]}]}},
                {"id": "tx_emit_4", "role": "emit", "label": "emit PostCreated", "op": {"sql": "INSERT INTO outbox (type, payload) VALUES (?, ?)", "params": ["PostCreated", {"obj": {"postId": {"ref": ["__entity", "id"]}, "userId": {"ref": ["author_id"]}}}]}},
            ],
        },
    }


def test_write_tx_commits_gate_first():
    driver = SqliteDriver.in_memory(WRITE_SCHEMA)
    try:
        res = _execute_transaction_bundle(_write_bundle(), {"author_id": 7, "title": "New Post", "request_id": "req-1"}, driver, guard=False)
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
        res = _execute_transaction_bundle(_write_bundle(), {"author_id": 999, "title": "Orphan", "request_id": "req-2"}, driver, guard=False)
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
        first = _execute_transaction_bundle(_write_bundle(), {"author_id": 7, "title": "P", "request_id": "dup"}, driver, guard=False)
        assert first["committed"] is True
        # Second run with the SAME request_id: the idempotency INSERT affects 0 rows → no-op.
        second = _execute_transaction_bundle(_write_bundle(), {"author_id": 7, "title": "P2", "request_id": "dup"}, driver, guard=False)
        assert second["committed"] is False
        assert second["shortCircuit"]["reason"] == "idempotent_duplicate"
        # No double write: exactly one post, post_count incremented exactly once.
        assert driver.prepare("SELECT COUNT(*) AS c FROM posts").all([])[0]["c"] == 1
        assert driver.prepare("SELECT post_count FROM users WHERE id = 7").all([])[0]["post_count"] == 3
    finally:
        driver.close()


# ── M4 (re-audit): an unknown gate rule FAILS CLOSED (never a silent commit) ──
#
# Aligned across all 5 runtimes (Python + Rust + TS + Go + PHP): a corrupt / forward-incompatible
# gate string MUST abort the transaction (raise + ROLLBACK), NOT silently continue and COMMIT.
def _unknown_gate_bundle():
    b = _write_bundle()
    # Tag the requires gate with a bogus rule the runtime does not recognize.
    b["transaction"]["statements"][0]["gate"] = "someFutureGateRuleThatDoesNotExist"
    return b


def test_write_tx_unknown_gate_fails_closed():
    driver = SqliteDriver.in_memory(WRITE_SCHEMA)
    try:
        raised = False
        try:
            _execute_transaction_bundle(_unknown_gate_bundle(), {"author_id": 7, "title": "X", "request_id": "req-u"}, driver, guard=False)
        except Exception as e:  # noqa: BLE001 — any raise is fail-closed; assert the message survives
            raised = True
            assert "unknown gate rule" in str(e)
        assert raised, "an unknown gate rule must fail closed (raise), not silently commit"
        # FAIL-CLOSED: nothing committed.
        assert driver.prepare("SELECT COUNT(*) AS c FROM posts").all([])[0]["c"] == 0
    finally:
        driver.close()
