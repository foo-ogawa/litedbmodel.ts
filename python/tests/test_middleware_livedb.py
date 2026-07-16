"""Phase D (#95, python) — the SCP MIDDLEWARE layer on the LIVE seam (PG:5433).

The python mirror of the TS ``test/integration/ScpMiddleware.test.ts`` (#92). The unit test
(``test_middleware.py``) proves the hook mechanics on the in-proc ``sqlite3`` seam; THIS test proves the
SAME contract on the PRODUCTION path against REAL Postgres, so a registered middleware, ``run_method``,
``Logger``, and the raw ``execute``/``query`` API all compose with connection routing + the
per-execution-ownership transaction on live PG:

  D1 a registered SQL middleware intercepts EVERY statement funneled through the seam of a REAL
     ``transaction()`` — the runtime ``BEGIN`` + the body write + ``COMMIT`` (and ``ROLLBACK`` on a body
     error) — plus the read, with per-context isolation across two concurrent ``with_middleware_scope``
     bodies on distinct threads. RED: revert the seam-routing of tx-control ⇒ the BEGIN/COMMIT
     observation goes empty (proven with a driver double whose begin_tx self-issues tx-control).

     Owner DECISION (option A): runtime-issued tx-control IS middleware-visible in all 5 languages —
     ``with_transaction_decided`` issues BEGIN/COMMIT/ROLLBACK THROUGH the seam on the SAME pinned
     ``TxConnection`` (full TS parity), preserving ownership / guard-exempt / #78 discard.

  D3 raw ``raw_execute``/``raw_query`` go THROUGH the seam — a registered middleware sees them, and
     connection routing (writer pool for a write) still applies; the Logger records real SQL/params/
     timing for a live statement; a ``query`` method hook fires around ``raw_query``.

Namespaced to a py-unique table (``scp_mw_py``) so it never collides with the parallel rust/go/php ports
on the shared docker PG:5433. Gated behind LITEDBMODEL_TX_ISOLATION=1. Requires the dockerized PG:5433:

    docker compose -f docker-compose.test.yml -f docker-compose.livedb.yml up -d postgres
    LITEDBMODEL_TX_ISOLATION=1 TEST_DB_PORT=5433 \
        python3 -m pytest python/tests/test_middleware_livedb.py -v -s
"""

from __future__ import annotations

import contextvars
import os
import threading
import time

import pytest

from litedbmodel_runtime import (
    ConnectionRegistry,
    Logger,
    PostgresDriver,
    RoutingConfig,
    StatementIntent,
    WriterStickyClock,
    context_for_driver,
    create_middleware,
    execute as seam_execute,
    raw_execute,
    raw_query,
    reader_writer_pair,
    run as seam_run,
    transaction,
    use,
    with_middleware_scope,
)
from litedbmodel_runtime.connection_routing import ConnectionPool, RawConnectionPool
from litedbmodel_runtime.driver import _ConnectionPool, _dollar_to_pyformat
from litedbmodel_runtime.exec_context import ExecutionContext, MiddlewareChain, current_context
from litedbmodel_runtime.middleware import active_sql_middlewares


def _tx_write(sql, params):
    """Issue a WRITE on the AMBIENT pinned tx connection (the way a real op JOINs a ``transaction()``):
    the body reads the pinned tx ctx from the contextvar, so the statement runs on the SAME owned
    connection as the runtime BEGIN/COMMIT — transactional, not a fresh autocommit connection."""
    pinned = current_context()
    assert pinned is not None and pinned.in_transaction(), "expected an ambient pinned tx ctx"
    seam_run(pinned, sql, params)

TBL = "scp_mw_py"  # py-unique table on the shared docker PG


def _enabled() -> bool:
    return os.environ.get("LITEDBMODEL_TX_ISOLATION") == "1"


pytestmark = pytest.mark.skipif(not _enabled(), reason="set LITEDBMODEL_TX_ISOLATION=1 + docker up")


def _pg_cfg():
    return dict(
        host=os.environ.get("TEST_DB_HOST", "localhost"),
        port=int(os.environ.get("TEST_DB_PORT", "5433")),
        user=os.environ.get("TEST_DB_USER", "testuser"),
        password=os.environ.get("TEST_DB_PASSWORD", "testpass"),
        dbname=os.environ.get("TEST_DB_NAME", "testdb"),
    )


def _pg_raw_pool(size=8):
    import psycopg

    cfg = _pg_cfg()

    def factory():
        return psycopg.connect(autocommit=True, **cfg)

    return _ConnectionPool(factory, size)


def _pg_driver(raw):
    return PostgresDriver(raw, _dollar_to_pyformat, emulate_returning=False)


def _pg_pool(raw):
    return RawConnectionPool(raw, _dollar_to_pyformat, emulate_returning=False)


class _RecordingPool(ConnectionPool):
    """Delegate to a real ConnectionPool but record the ``label`` on every acquire (the python mirror of
    the TS ``recordingPool``) — so a test asserts WHICH pool (writer) served each statement."""

    def __init__(self, real, label, log):
        self._real = real
        self.label = label
        self.log = log
        self.xform = getattr(real, "xform", None)
        self.emulate_returning = getattr(real, "emulate_returning", None)

    def acquire(self):
        self.log.append(self.label)
        return self._real.acquire()

    def release(self, conn, destroy=False):
        return self._real.release(conn, destroy)


def _routing_ctx(routing) -> ExecutionContext:
    """A Phase-D routing ctx whose middleware chain sources the ACTIVE registry (so a registered
    middleware intercepts the routed live statement) — the Phase D analogue of ``context_for_driver``'s
    active-source chain, applied to a routing-only ctx."""
    return ExecutionContext(None, MiddlewareChain(active_sql_middlewares), routing=routing)


@pytest.fixture()
def raw_pool():
    raw = _pg_raw_pool()
    conn = raw.acquire()
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {TBL}")
    cur.execute(f"CREATE TABLE {TBL} (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
    cur.close()
    raw.release(conn)
    yield raw
    conn = raw.acquire()
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {TBL}")
    cur.close()
    raw.release(conn)
    raw.close()


# ── D1: SQL-level hook on the live seam ─────────────────────────────────────────


def test_d1_middleware_observes_runtime_begin_commit_of_real_transaction(raw_pool):
    """POSITIVE (owner decision A): a middleware observes the RUNTIME BEGIN + body write + COMMIT of a
    REAL live-PG ``transaction()`` — all funneled through the ONE seam on the SAME pinned connection."""
    driver = _pg_driver(raw_pool)
    ctx = context_for_driver(driver)
    seen = []

    def scope():
        use(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(sql), nxt(sql, params))[1]))
        transaction(ctx, lambda: _tx_write(f"INSERT INTO {TBL} (id, val) VALUES ($1, $2)", [1, "a"]), None, "postgres")
        rows = seam_execute(ctx, f"SELECT val FROM {TBL} WHERE id = $1", [1])
        assert [dict(r) for r in rows] == [{"val": "a"}]

    with_middleware_scope(scope)
    # The middleware saw the runtime BEGIN, the body INSERT, the runtime COMMIT, AND the SELECT — every
    # statement of a real transaction() funneled through the ONE seam (full TS parity).
    assert "BEGIN" in seen, seen
    assert f"INSERT INTO {TBL} (id, val) VALUES ($1, $2)" in seen
    assert "COMMIT" in seen, seen
    assert f"SELECT val FROM {TBL} WHERE id = $1" in seen
    # Ordering: BEGIN precedes the body write, which precedes COMMIT.
    assert seen.index("BEGIN") < seen.index(f"INSERT INTO {TBL} (id, val) VALUES ($1, $2)") < seen.index("COMMIT")


def test_d1_middleware_observes_runtime_rollback_on_body_error(raw_pool):
    """POSITIVE: a body error rolls back — the middleware observes the runtime BEGIN + ROLLBACK (no
    COMMIT). Proves the error path's tx-control is seam-visible too."""
    driver = _pg_driver(raw_pool)
    ctx = context_for_driver(driver)
    seen = []

    def scope():
        use(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(sql), nxt(sql, params))[1]))

        def body():
            _tx_write(f"INSERT INTO {TBL} (id, val) VALUES ($1, $2)", [7, "x"])
            raise RuntimeError("body boom")

        with pytest.raises(Exception):
            transaction(ctx, body, None, "postgres")

    with_middleware_scope(scope)
    assert "BEGIN" in seen, seen
    assert "ROLLBACK" in seen, seen
    assert "COMMIT" not in seen, seen
    # The row was rolled back — not visible after the tx.
    rows = _pg_driver(raw_pool).prepare(f"SELECT val FROM {TBL} WHERE id = $1").all([7])
    assert rows == []


def test_d1_red_tx_control_bypassing_seam_is_not_observed(raw_pool):
    """RED proof (faithful mutation): if the tx-control were issued DIRECTLY on the owned connection
    (the pre-decision path) instead of through the seam, a middleware would NOT observe BEGIN/COMMIT.

    We reproduce the OLD path by monkeypatching ``with_transaction_decided`` to a variant that issues
    BEGIN/COMMIT/ROLLBACK on ``tx`` DIRECTLY (``tx.run`` on the raw handle) WITHOUT going through the
    pinned-ctx ``run`` seam — everything else identical (same owned conn, same release). The middleware
    then sees ONLY the body write, NOT BEGIN/COMMIT — proving the seam-routing is load-bearing for the
    positive observation above."""
    import litedbmodel_runtime.exec_context as ec

    driver = _pg_driver(raw_pool)
    ctx = context_for_driver(driver)
    seen = []

    def bypass_with_transaction_decided(ctx, body, before=(), after=()):
        # The OLD divergent path: tx-control issued on the OWNED tx handle DIRECTLY (bypassing the seam).
        tx = ctx.begin_tx()
        tx_ctx = ctx.with_connection(ec._TxConnectionAdapter(tx), True)

        def scoped():
            destroy = True
            try:
                for stmt in before:
                    tx.run(stmt, [])  # DIRECT — not through run(tx_ctx, …), so NOT middleware-visible
                tx.run("BEGIN", [])
                for stmt in after:
                    tx.run(stmt, [])
                try:
                    decision = body(tx_ctx)
                except BaseException:
                    try:
                        tx.run("ROLLBACK", [])
                        destroy = False
                    except Exception:
                        pass
                    raise
                if decision.rollback:
                    tx.run("ROLLBACK", [])
                    destroy = False
                    return decision.value
                tx.run("COMMIT", [])
                destroy = False
                ctx.mark_sticky()
                return decision.value
            finally:
                tx.release(destroy)

        return ec.run_with_pinned_context(tx_ctx, scoped)

    orig = ec.with_transaction_decided
    ec.with_transaction_decided = bypass_with_transaction_decided
    try:
        def scope():
            use(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(sql), nxt(sql, params))[1]))
            transaction(ctx, lambda: _tx_write(f"INSERT INTO {TBL} (id, val) VALUES ($1, $2)", [9, "y"]), None, "postgres")

        with_middleware_scope(scope)
    finally:
        ec.with_transaction_decided = orig

    # The body write WAS seen (it goes through the seam), but the runtime BEGIN/COMMIT were NOT — the
    # divergent path made them invisible. This is exactly what the owner's decision A fixes.
    assert f"INSERT INTO {TBL} (id, val) VALUES ($1, $2)" in seen
    assert "BEGIN" not in seen, seen
    assert "COMMIT" not in seen, seen
    # Behavior preserved even on the bypass path: the row committed (id=9 present).
    rows = _pg_driver(raw_pool).prepare(f"SELECT val FROM {TBL} WHERE id = $1").all([9])
    assert [dict(r) for r in rows] == [{"val": "y"}]


def test_d1_red_without_registration_nothing_observed(raw_pool):
    driver = _pg_driver(raw_pool)
    ctx = context_for_driver(driver)
    seen = []
    transaction(ctx, lambda: seam_run(ctx, f"INSERT INTO {TBL} (id, val) VALUES ($1, $2)", [2, "b"]), None, "postgres")
    seam_execute(ctx, f"SELECT val FROM {TBL} WHERE id = $1", [2])
    assert seen == []  # byte-identical passthrough — nothing observed


def test_d1_per_context_isolation_concurrent(raw_pool):
    driver = _pg_driver(raw_pool)
    ctx = context_for_driver(driver)
    seen_a, seen_b = [], []
    barrier = threading.Barrier(2)

    def worker(seen, tag, delay):
        def scope():
            use(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(f"{tag}:{sql}"), nxt(sql, params))[1]))
            barrier.wait()
            time.sleep(delay)
            seam_execute(ctx, f"SELECT {1 if tag == 'A' else 2}", [])

        contextvars.copy_context().run(with_middleware_scope, scope)

    ta = threading.Thread(target=worker, args=(seen_a, "A", 0.03))
    tb = threading.Thread(target=worker, args=(seen_b, "B", 0.001))
    ta.start()
    tb.start()
    ta.join()
    tb.join()
    assert seen_a == ["A:SELECT 1"]
    assert seen_b == ["B:SELECT 2"]


# ── D3: Logger + raw execute/query through the live seam ────────────────────────


def test_d3_raw_execute_through_seam_and_writer_routing(raw_pool):
    log = []
    writer = _RecordingPool(_pg_pool(raw_pool), "writer", log)
    routing = RoutingConfig(
        ConnectionRegistry.from_default(reader_writer_pair(writer, writer)).build(),
        WriterStickyClock(use_writer_after_transaction=False),
    )
    ctx = _routing_ctx(routing)
    seen = []

    def scope():
        use(create_middleware(execute=lambda st, nxt, sql, params: (seen.append(sql), nxt(sql, params))[1]))
        ins = raw_execute(ctx, f"INSERT INTO {TBL} (id, val) VALUES ($1, $2)", [3, "c"], write=True)
        assert ins.row_count == 1
        rows = raw_query(ctx, f"SELECT val FROM {TBL} WHERE id = $1", [3])
        assert [dict(r) for r in rows] == [{"val": "c"}]

    with_middleware_scope(scope)
    assert seen == [f"INSERT INTO {TBL} (id, val) VALUES ($1, $2)", f"SELECT val FROM {TBL} WHERE id = $1"]
    # Both statements acquired a connection from the (writer) pool — routing applied through the seam.
    assert len(log) == 2


def test_d3_logger_records_live_sql_params_timing(raw_pool):
    driver = _pg_driver(raw_pool)
    ctx = context_for_driver(driver)
    logger = Logger()

    def scope():
        use(logger)
        transaction(ctx, lambda: seam_run(ctx, f"INSERT INTO {TBL} (id, val) VALUES ($1, $2)", [4, "d"]), None, "postgres")
        seam_execute(ctx, f"SELECT val FROM {TBL} WHERE id = $1", [4])
        entries = logger.state()["entries"]
        sqls = [e.sql for e in entries]
        assert f"INSERT INTO {TBL} (id, val) VALUES ($1, $2)" in sqls
        assert f"SELECT val FROM {TBL} WHERE id = $1" in sqls
        for e in entries:
            assert e.duration_ms >= 0

    with_middleware_scope(scope)


def test_d3_query_method_hook_around_raw_query(raw_pool):
    driver = _pg_driver(raw_pool)
    ctx = context_for_driver(driver)
    conn = raw_pool.acquire()
    cur = conn.cursor()
    cur.execute(f"INSERT INTO {TBL} (id, val) VALUES (5, 'e')")
    cur.close()
    raw_pool.release(conn)
    events = []

    def query_hook(st, model, nxt, *args):
        events.append("query:before")
        r = nxt(*args)
        events.append("query:after")
        return r

    def scope():
        use(create_middleware(query=query_hook))
        rows = raw_query(ctx, f"SELECT val FROM {TBL} WHERE id = $1", [5])
        assert [dict(r) for r in rows] == [{"val": "e"}]

    with_middleware_scope(scope)
    assert events == ["query:before", "query:after"]
