"""Unit tests for the ExecutionContext + central execute/run seam (Phase A / #78, python).

No live DB — an in-proc stdlib ``sqlite3`` is the driver. These pin: the seam funnel, empty-middleware
passthrough, middleware wrap+delegate order, per-execution owned-connection ownership (commit /
rollback / decided-rollback), the tx-scoped ``connection_for`` resolution, and the ``contextvars``
propagation of the pinned ctx. The live PG/MySQL concurrent-tx isolation + atomicity proof lives in
``test_tx_isolation.py``. Mirrors the go ``exec_context_test.go`` / rust ``exec_context.rs`` unit tests.
"""

from __future__ import annotations

import sqlite3

import pytest

from litedbmodel_runtime import RunInfo, SqliteDriver
from litedbmodel_runtime.driver import PreparedStatement, TxConnection
from litedbmodel_runtime.exec_context import (
    READ_INTENT,
    WRITE_INTENT,
    Connection,
    DriverConnection,
    ExecutionContext,
    MiddlewareChain,
    StatementIntent,
    commit,
    context_for_driver,
    current_context,
    execute,
    rollback,
    run,
    run_with_pinned_context,
    with_transaction,
    with_transaction_decided,
)


def _seed_driver() -> SqliteDriver:
    return SqliteDriver.in_memory(
        [
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            "INSERT INTO t VALUES (1, 'a')",
        ]
    )


# ── A recording driver (no DB) to assert the seam funnels SQL + tx ordering ─────


class _RecStmt:
    def __init__(self, log, sql, fail_on):
        self._log = log
        self._sql = sql
        self._fail_on = fail_on

    def all(self, params):
        self._log.append(self._sql)
        if self._fail_on == self._sql:
            raise RuntimeError("boom")
        return [{"sql": self._sql}]

    def run(self, params):
        self._log.append(self._sql)
        if self._fail_on == self._sql:
            raise RuntimeError("boom")
        return RunInfo(1, 0)


class _RecTx:
    """A recording TxConnection: forwards to the SAME log so BEGIN/…/COMMIT ordering is asserted."""

    def __init__(self, log, fail_on):
        self._log = log
        self._fail_on = fail_on
        self._log.append("BEGIN")

    def all(self, sql, params):
        return _RecStmt(self._log, sql, self._fail_on).all(params)

    def run(self, sql, params):
        return _RecStmt(self._log, sql, self._fail_on).run(params)

    def commit(self):
        self._log.append("COMMIT")

    def rollback(self):
        self._log.append("ROLLBACK")


class _RecDriver:
    def __init__(self, fail_on=None):
        self.log = []
        self._fail_on = fail_on

    def prepare(self, sql):
        return _RecStmt(self.log, sql, self._fail_on)

    def begin_tx(self):
        return _RecTx(self.log, self._fail_on)


# ── Seam funnel + middleware ────────────────────────────────────────────────────


def test_seam_execute_funnels_to_driver():
    d = _RecDriver()
    ctx = context_for_driver(d)
    rows = execute(ctx, "SELECT 1", [], READ_INTENT)
    assert len(rows) == 1
    assert d.log == ["SELECT 1"]


def test_seam_run_funnels_to_driver():
    d = _RecDriver()
    ctx = context_for_driver(d)
    info = run(ctx, "INSERT INTO t VALUES (1)", [], WRITE_INTENT)
    assert info.changes == 1
    assert d.log == ["INSERT INTO t VALUES (1)"]


def test_empty_middleware_is_passthrough():
    chain = MiddlewareChain()
    assert chain.is_empty
    r = chain.wrap("SELECT 1", [], lambda s, p: [{"n": 42}])
    assert r == [{"n": 42}]


def test_middleware_wraps_and_delegates():
    def mw(sql, params, next_):
        rows = next_(sql, params)
        return rows + [{"mw": True}]

    chain = MiddlewareChain([mw])
    assert not chain.is_empty
    r = chain.wrap("SELECT 1", [], lambda s, p: [{"n": 1}])
    assert r == [{"n": 1}, {"mw": True}]


def test_middleware_order_is_outermost_first():
    order = []

    def outer(sql, params, next_):
        order.append("outer-before")
        r = next_(sql, params)
        order.append("outer-after")
        return r

    def inner(sql, params, next_):
        order.append("inner-before")
        r = next_(sql, params)
        order.append("inner-after")
        return r

    chain = MiddlewareChain([outer, inner])
    chain.wrap("SELECT 1", [], lambda s, p: (order.append("terminal"), [])[1])
    assert order == ["outer-before", "inner-before", "terminal", "inner-after", "outer-after"]


# ── connection_for resolution ───────────────────────────────────────────────────


def test_connection_for_resolves_driver_outside_tx():
    d = _seed_driver()
    ctx = context_for_driver(d)
    assert not ctx.in_transaction()
    assert isinstance(ctx.connection_for(READ_INTENT), DriverConnection)


def test_with_connection_pins_only_when_tx():
    d = _seed_driver()
    ctx = context_for_driver(d)

    class _C(Connection):
        def execute(self, sql, params):
            return []

        def run(self, sql, params):
            return RunInfo(0, 0)

    pinned = _C()
    tx_ctx = ctx.with_connection(pinned, True)
    assert tx_ctx.in_transaction()
    assert tx_ctx.connection_for(WRITE_INTENT) is pinned
    # tx=False derives a NON-tx ctx (no pin) — the primary driver resolves.
    non_tx = ctx.with_connection(pinned, False)
    assert not non_tx.in_transaction()


# ── Per-execution owned-connection transaction ──────────────────────────────────


def test_with_transaction_commits_on_ok():
    d = _RecDriver()
    ctx = context_for_driver(d)
    out = with_transaction(ctx, lambda tx: (run(tx, "INSERT INTO t VALUES (1)", [], WRITE_INTENT), 7)[1])
    assert out == 7
    assert d.log == ["BEGIN", "INSERT INTO t VALUES (1)", "COMMIT"]


def test_with_transaction_rolls_back_on_err():
    d = _RecDriver(fail_on="BAD")
    ctx = context_for_driver(d)

    def body(tx):
        run(tx, "INSERT INTO t VALUES (1)", [], WRITE_INTENT)
        run(tx, "BAD", [], WRITE_INTENT)  # raises
        return None

    with pytest.raises(RuntimeError):
        with_transaction(ctx, body)
    assert d.log == ["BEGIN", "INSERT INTO t VALUES (1)", "BAD", "ROLLBACK"]


def test_with_transaction_decided_rollback_returns_value():
    d = _RecDriver()
    ctx = context_for_driver(d)

    def body(tx):
        run(tx, "INSERT INTO t VALUES (1)", [], WRITE_INTENT)
        return rollback("gated")

    out = with_transaction_decided(ctx, body)
    assert out == "gated"
    assert d.log == ["BEGIN", "INSERT INTO t VALUES (1)", "ROLLBACK"]


def test_tx_scoped_ctx_reports_in_transaction():
    d = _RecDriver()
    ctx = context_for_driver(d)
    assert not ctx.in_transaction()

    def body(tx):
        assert tx.in_transaction()
        return commit(None)

    with_transaction_decided(ctx, body)


# ── Real SQLite: tx commit persists, rollback undoes (owned-connection atomicity) ──


def test_sqlite_tx_commit_persists():
    d = _seed_driver()
    ctx = context_for_driver(d)
    with_transaction(ctx, lambda tx: run(tx, "INSERT INTO t VALUES (2, 'b')", [], WRITE_INTENT))
    rows = execute(ctx, "SELECT v FROM t WHERE id = 2", [], READ_INTENT)
    assert len(rows) == 1 and rows[0]["v"] == "b"


def test_sqlite_tx_rollback_undoes_first_stmt():
    d = _seed_driver()
    ctx = context_for_driver(d)

    def body(tx):
        run(tx, "INSERT INTO t VALUES (2, 'b')", [], WRITE_INTENT)
        # id=1 already exists → UNIQUE/PK violation → the whole tx rolls back (cross-stmt atomicity).
        run(tx, "INSERT INTO t VALUES (1, 'dup')", [], WRITE_INTENT)
        return None

    with pytest.raises(sqlite3.IntegrityError):
        with_transaction(ctx, body)
    rows = execute(ctx, "SELECT v FROM t WHERE id = 2", [], READ_INTENT)
    assert rows == []  # id=2 undone by the rollback


# ── contextvars propagation ─────────────────────────────────────────────────────


def test_current_context_is_none_outside_scope():
    assert current_context() is None


def test_run_with_pinned_context_sets_and_restores():
    d = _seed_driver()
    ctx = context_for_driver(d)
    seen = []
    run_with_pinned_context(ctx, lambda: seen.append(current_context()))
    assert seen == [ctx]
    # Restored to None after the scope.
    assert current_context() is None


def test_with_transaction_pins_ctx_in_contextvar():
    d = _seed_driver()
    ctx = context_for_driver(d)
    seen = []

    def body(tx):
        # Inside the tx body the ambient contextvar is the tx-scoped ctx (per-execution ownership).
        seen.append(current_context())
        assert current_context() is tx
        return commit(None)

    with_transaction_decided(ctx, body)
    assert seen and seen[0].in_transaction()
    assert current_context() is None  # restored


# ── StatementIntent shape ───────────────────────────────────────────────────────


def test_statement_intent_defaults():
    assert StatementIntent.read().write is False
    assert StatementIntent.write_().write is True
    assert StatementIntent().db is None
