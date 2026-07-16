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
    """A recording TxConnection: forwards to the SAME log so BEGIN/…/COMMIT/RELEASE ordering is
    asserted. ``fail_commit`` makes COMMIT raise (to drive the raising-commit release path)."""

    def __init__(self, log, fail_on, fail_commit=False):
        self._log = log
        self._fail_on = fail_on
        self._fail_commit = fail_commit
        self.released = None  # records the destroy flag the combinator releases with
        self._log.append("BEGIN")

    def all(self, sql, params):
        return _RecStmt(self._log, sql, self._fail_on).all(params)

    def run(self, sql, params):
        return _RecStmt(self._log, sql, self._fail_on).run(params)

    def commit(self):
        self._log.append("COMMIT")
        if self._fail_commit:
            raise RuntimeError("commit-boom")

    def rollback(self):
        self._log.append("ROLLBACK")

    def release(self, destroy):
        # The SINGLE release point (the combinator's finally). Record it exactly once for the leak guard.
        self.released = destroy
        self._log.append("RELEASE(destroy)" if destroy else "RELEASE")


class _RecDriver:
    def __init__(self, fail_on=None, fail_commit=False):
        self.log = []
        self._fail_on = fail_on
        self._fail_commit = fail_commit
        self.last_tx = None  # the most recent tx handle (to inspect its release)

    def prepare(self, sql):
        return _RecStmt(self.log, sql, self._fail_on)

    def begin_tx(self, before=(), after=()):
        # This recording driver models the SQLite single-conn tx (no per-tx isolation prelude); the
        # prelude args are accepted for the Phase B signature but must be empty here.
        assert not before and not after, "recording SQLite driver takes no isolation prelude"
        self.last_tx = _RecTx(self.log, self._fail_on, self._fail_commit)
        return self.last_tx


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
    # COMMIT then the combinator's single RELEASE (clean → back to the pool, destroy=False).
    assert d.log == ["BEGIN", "INSERT INTO t VALUES (1)", "COMMIT", "RELEASE"]
    assert d.last_tx.released is False


def test_with_transaction_rolls_back_on_err():
    d = _RecDriver(fail_on="BAD")
    ctx = context_for_driver(d)

    def body(tx):
        run(tx, "INSERT INTO t VALUES (1)", [], WRITE_INTENT)
        run(tx, "BAD", [], WRITE_INTENT)  # raises
        return None

    with pytest.raises(RuntimeError):
        with_transaction(ctx, body)
    # A clean ROLLBACK on a body error ⇒ the connection returns to the pool (destroy=False).
    assert d.log == ["BEGIN", "INSERT INTO t VALUES (1)", "BAD", "ROLLBACK", "RELEASE"]
    assert d.last_tx.released is False


def test_with_transaction_decided_rollback_returns_value():
    d = _RecDriver()
    ctx = context_for_driver(d)

    def body(tx):
        run(tx, "INSERT INTO t VALUES (1)", [], WRITE_INTENT)
        return rollback("gated")

    out = with_transaction_decided(ctx, body)
    assert out == "gated"
    assert d.log == ["BEGIN", "INSERT INTO t VALUES (1)", "ROLLBACK", "RELEASE"]
    assert d.last_tx.released is False


def test_release_runs_exactly_once_on_raising_commit():
    # LEAK REGRESSION GUARD (#78 audit): if COMMIT itself raises, the owned connection must STILL be
    # released — destroyed (poisoned), EXACTLY ONCE — via the combinator's finally. Before the fix the
    # release lived inside commit() (skipped when commit raised) ⇒ the connection leaked.
    d = _RecDriver(fail_commit=True)
    ctx = context_for_driver(d)

    def body(tx):
        run(tx, "INSERT INTO t VALUES (1)", [], WRITE_INTENT)
        return None

    with pytest.raises(RuntimeError, match="commit-boom"):
        with_transaction(ctx, body)
    # COMMIT raised → the connection is poisoned → RELEASE(destroy) runs once (not leaked).
    assert d.log == ["BEGIN", "INSERT INTO t VALUES (1)", "COMMIT", "RELEASE(destroy)"]
    assert d.last_tx.released is True
    assert d.log.count("RELEASE(destroy)") == 1  # exactly once (no double-release)


def test_sqlite_pool_not_leaked_on_raising_commit():
    """A faithful pool-level leak guard over a real pooled-shaped driver: a tx whose COMMIT raises
    must return the pool to its baseline free count (destroyed, not leaked). Uses a tiny fake pooled
    driver mirroring _PooledTxConnection's acquire/BEGIN + commit/rollback/release contract."""
    from litedbmodel_runtime.driver import RunInfo as _RunInfo

    class _FakePool:
        """Mirrors _ConnectionPool's opened-count accounting: acquire opens up to `size`; discard
        closes AND frees a slot (decrements opened) so a fresh conn can replace a poisoned one."""

        def __init__(self, size):
            self.size = size
            self.free = []
            self.opened = 0
            self.destroyed = []
            self._next = 0

        def acquire(self):
            if self.free:
                return self.free.pop()
            assert self.opened < self.size, "pool exhausted (leak!) — acquire would block forever"
            self.opened += 1
            self._next += 1
            return self._next

        def release(self, conn):
            self.free.append(conn)

        def discard(self, conn):
            self.destroyed.append(conn)
            self.opened -= 1  # free the slot so a fresh connection can be opened (no capacity shrink)

    class _FakeTx:
        def __init__(self, pool, fail_commit):
            self._pool = pool
            self._fail_commit = fail_commit
            self._conn = pool.acquire()  # BEGIN on the owned conn
            self._released = False

        def all(self, sql, params):
            return []

        def run(self, sql, params):
            return _RunInfo(1, 0)

        def commit(self):
            if self._fail_commit:
                raise RuntimeError("commit dropped the connection")

        def rollback(self):
            pass

        def release(self, destroy):
            if self._released:
                return
            self._released = True
            if destroy:
                self._pool.discard(self._conn)  # close + free the slot (no capacity shrink)
            else:
                self._pool.release(self._conn)

    class _FakePooledDriver:
        def __init__(self, pool, fail_commit):
            self._pool = pool
            self._fail_commit = fail_commit

        def prepare(self, sql):
            raise AssertionError("tx path must not hit prepare")

        def begin_tx(self, before=(), after=()):
            return _FakeTx(self._pool, self._fail_commit)

    # A SMALL pool (size 2) run through MANY raising-COMMIT txs: if a raising commit leaked its
    # connection (pre-fix) OR discard failed to free the pool slot (the deeper accounting bug), the
    # pool's opened-count would stick at the ceiling and the 3rd+ acquire would assert "pool exhausted".
    pool = _FakePool(2)
    driver = _FakePooledDriver(pool, fail_commit=True)
    ctx = context_for_driver(driver)
    for _ in range(20):
        with pytest.raises(RuntimeError, match="commit dropped"):
            with_transaction(ctx, lambda tx: run(tx, "INSERT", [], WRITE_INTENT))
    assert len(pool.destroyed) == 20  # every poisoned connection discarded, one per failing tx
    assert pool.opened == 0  # discard freed every slot — capacity fully restored (no leak, no shrink)
    # A subsequent CLEAN tx still acquires + commits + returns its connection — the pool functions.
    driver_ok = _FakePooledDriver(pool, fail_commit=False)
    ctx_ok = context_for_driver(driver_ok)
    with_transaction(ctx_ok, lambda tx: run(tx, "INSERT", [], WRITE_INTENT))
    assert pool.opened == 1 and len(pool.free) == 1  # one live, returned to free


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
