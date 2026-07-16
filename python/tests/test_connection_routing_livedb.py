"""Phase C (#90, python) — CONNECTION ROUTING + CONFIG on live PG (:5433) + MySQL (:3307).

The python mirror of the TS ``test/integration/ConnectionRouting.test.ts`` (#87). Proves the completion
of ``connection_for(intent)``'s resolution (design §3 steps 2-4) on the Phase A/B exec-context seam,
against REAL databases, with a faithful-mutation RED proof for every claim:

  C1 reader/writer separation + writer-sticky + withWriter
    - a READ → the READER pool; a WRITE → the WRITER pool (recording pools capture the split);
    - after a committed transaction, an in-window read → the WRITER (read-your-writes, injectable
      clock), then back to the READER after expiry;
    - with_writer(fn) forces the WRITER for reads in its scope, and REJECTS a write (read-only).
  C2 multi-DB connection registry + name→connection routing (PG = A, MySQL = B)
    - an untagged statement → DB A (default); a "B"-tagged statement → DB B — live cross-DB;
    - a missing connection name is a LOUD error;
    - a NAMED-DB transaction runs entirely on ONE pinned writer connection (tx-pin wins over routing).
  C3 set_config: queryTimeout fires a real SERVER statement timeout (PG statement_timeout + MySQL
    max_execution_time), pool sizing (max_pool is the SOLE cap, applied at construction — RED when
    deleted), searchPath/charset reset-on-release (no session leak), close_all_pools.

Namespaced to py-unique tables (a py-specific PG schema `phase_c_routing_py` + `_py` table suffixes) so
it never collides with the parallel rust/go/php ports on the shared docker PG:5433 + MySQL:3307.

Gated behind LITEDBMODEL_TX_ISOLATION=1 (same gate as the other live-DB tests). Requires the dockerized
PG (:5433) + MySQL (:3307):

    docker compose -f docker-compose.test.yml -f docker-compose.livedb.yml up -d postgres mysql
    LITEDBMODEL_TX_ISOLATION=1 TEST_DB_PORT=5433 TEST_MYSQL_PORT=3307 \
        python3 -m pytest python/tests/test_connection_routing_livedb.py -v -s
"""

from __future__ import annotations

import os
import threading
import time

import pytest

from litedbmodel_runtime import (
    ConnectionConfig,
    ConnectionRegistry,
    ConnectionSetup,
    RoutingConfig,
    TransactionOptions,
    WriteInReadOnlyContextError,
    WriterStickyClock,
    build_routing_config,
    check_write_allowed_ambient,
    configured_pool,
    execute,
    mysql_pool_factory,
    pg_pool_factory,
    reader_writer_pair,
    resolve_connection_config,
    run,
    run_guarded,
    transaction,
    with_writer,
)
from litedbmodel_runtime.connection_routing import ConnectionPool, RawConnectionPool
from litedbmodel_runtime.driver import _ConnectionPool, _dollar_to_pyformat, _qmark_to_pyformat
from litedbmodel_runtime.exec_context import ExecutionContext, MiddlewareChain, StatementIntent, current_context


def _enabled() -> bool:
    return os.environ.get("LITEDBMODEL_TX_ISOLATION") == "1"


pytestmark = pytest.mark.skipif(not _enabled(), reason="set LITEDBMODEL_TX_ISOLATION=1 + docker up")

PG_SCHEMA = "phase_c_routing_py"
TBL = "%s.scp_route_py" % PG_SCHEMA  # py-unique PG schema-qualified table
MY_TBL = "scp_route_py"  # py-unique MySQL table


def _pg_kwargs():
    return dict(
        host=os.environ.get("TEST_DB_HOST", "localhost"),
        port=int(os.environ.get("TEST_DB_PORT", "5433")),
        user=os.environ.get("TEST_DB_USER", "testuser"),
        password=os.environ.get("TEST_DB_PASSWORD", "testpass"),
        dbname=os.environ.get("TEST_DB_NAME", "testdb"),
    )


def _my_kwargs():
    return dict(
        host=os.environ.get("TEST_MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("TEST_MYSQL_PORT", "3307")),
        user=os.environ.get("TEST_MYSQL_USER", "testuser"),
        password=os.environ.get("TEST_MYSQL_PASSWORD", "testpass"),
        database=os.environ.get("TEST_MYSQL_DB", "testdb"),
    )


def _pg_conn_config():
    k = _pg_kwargs()
    return ConnectionConfig(driver="postgres", host=k["host"], port=k["port"], database=k["dbname"], user=k["user"], password=k["password"])


def _my_conn_config():
    k = _my_kwargs()
    return ConnectionConfig(driver="mysql", host=k["host"], port=k["port"], database=k["database"], user=k["user"], password=k["password"])


# ── A raw pg/mysql pool (the Phase A _ConnectionPool) wrapped as a routing ConnectionPool ──


def _pg_raw_pool(size=8):
    import psycopg

    kw = _pg_kwargs()

    def factory():
        return psycopg.connect(autocommit=True, **kw)

    return _ConnectionPool(factory, size)


def _my_raw_pool(size=8):
    import pymysql

    kw = _my_kwargs()

    def factory():
        return pymysql.connect(autocommit=True, **kw)

    return _ConnectionPool(factory, size)


def _pg_pool(raw):
    return RawConnectionPool(raw, _dollar_to_pyformat, emulate_returning=False)


def _my_pool(raw):
    return RawConnectionPool(raw, _qmark_to_pyformat, emulate_returning=True)


class _RecordingPool(ConnectionPool):
    """Delegate to a real ConnectionPool but record the ``label`` on every acquire, so a test can
    assert WHICH pool (reader vs writer vs DB-A vs DB-B) served each statement (the python mirror of the
    TS ``recordingPool``). The xform/emulate_returning of the delegate flow through so PoolConnection
    executes correctly."""

    def __init__(self, real, label, log):
        self._real = real
        self.label = label
        self.log = log
        # Surface the delegate's exec config so _pool_exec_config picks up the real driver rewrite.
        self.xform = getattr(real, "xform", None)
        self.emulate_returning = getattr(real, "emulate_returning", None)

    def acquire(self):
        self.log.append(self.label)
        return self._real.acquire()

    def release(self, conn, destroy=False):
        return self._real.release(conn, destroy)


def _ctx(routing):
    return ExecutionContext(None, MiddlewareChain(), routing=routing)


# ── Fixtures: bring up the py-unique schema/tables ──────────────────────────────


def _setup_pg():
    raw = _pg_raw_pool()
    conn = raw.acquire()
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS %s" % PG_SCHEMA)
    cur.execute("DROP TABLE IF EXISTS %s" % TBL)
    cur.execute("CREATE TABLE %s (id INTEGER PRIMARY KEY, val TEXT NOT NULL)" % TBL)
    cur.close()
    raw.release(conn)
    return raw


def _setup_my():
    raw = _my_raw_pool()
    conn = raw.acquire()
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS %s" % MY_TBL)
    cur.execute("CREATE TABLE %s (id INT PRIMARY KEY, val TEXT NOT NULL)" % MY_TBL)
    cur.close()
    raw.release(conn)
    return raw


# ════════════════════════════════════════════════════════════════════════════════
# C1 — reader/writer separation + writer-sticky + withWriter
# ════════════════════════════════════════════════════════════════════════════════


def test_c1_reader_writer_split():
    raw = _setup_pg()
    try:
        log = []
        reader = _RecordingPool(_pg_pool(raw), "reader", log)
        writer = _RecordingPool(_pg_pool(raw), "writer", log)
        routing = RoutingConfig(
            ConnectionRegistry.from_default(reader_writer_pair(reader, writer)).build(),
            WriterStickyClock(use_writer_after_transaction=False),
        )
        ctx = _ctx(routing)

        rows = execute(ctx, "SELECT 1 AS one", [], StatementIntent(write=False))
        assert int(rows[0]["one"]) == 1
        run(ctx, "INSERT INTO %s (id, val) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING" % TBL, [1, "a"], StatementIntent(write=True))
        assert log == ["reader", "writer"]

        # MUTATION (RED): collapse reader/writer to ONE pool → both statements land on it.
        mlog = []
        solo = _RecordingPool(_pg_pool(raw), "solo", mlog)
        mctx = _ctx(RoutingConfig(ConnectionRegistry.from_default(reader_writer_pair(solo, solo)).build(), WriterStickyClock(use_writer_after_transaction=False)))
        execute(mctx, "SELECT 1 AS one", [], StatementIntent(write=False))
        run(mctx, "INSERT INTO %s (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING" % TBL, [1, "a"], StatementIntent(write=True))
        assert mlog == ["solo", "solo"]  # split was load-bearing
    finally:
        raw.close()


def test_c1_writer_sticky_after_commit():
    raw = _setup_pg()
    try:
        log = []
        clock = {"t": 1_000_000}
        reader = _RecordingPool(_pg_pool(raw), "reader", log)
        writer = _RecordingPool(_pg_pool(raw), "writer", log)
        sticky = WriterStickyClock(use_writer_after_transaction=True, writer_sticky_duration=5000, now=lambda: clock["t"])
        ctx = _ctx(RoutingConfig(ConnectionRegistry.from_default(reader_writer_pair(reader, writer)).build(), sticky))

        execute(ctx, "SELECT 1", [], StatementIntent(write=False))
        assert log[-1] == "reader"  # sticky not armed

        # Commit a tx → arms the sticky clock.
        transaction(
            ctx,
            lambda: run_guarded(ctx, "INSERT INTO %s (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING" % TBL, [2, "b"], "INSERT"),
            TransactionOptions(),
            "postgres",
        )

        clock["t"] += 100  # within the 5s window → WRITER (read-your-writes)
        execute(ctx, "SELECT 1", [], StatementIntent(write=False))
        assert log[-1] == "writer"

        clock["t"] += 6000  # window elapsed → back to READER
        execute(ctx, "SELECT 1", [], StatementIntent(write=False))
        assert log[-1] == "reader"

        # MUTATION (RED): sticky OFF → the in-window post-commit read lands on the READER.
        mlog = []
        mclock = {"t": 2_000_000}
        mreader = _RecordingPool(_pg_pool(raw), "reader", mlog)
        mwriter = _RecordingPool(_pg_pool(raw), "writer", mlog)
        mctx = _ctx(RoutingConfig(ConnectionRegistry.from_default(reader_writer_pair(mreader, mwriter)).build(), WriterStickyClock(use_writer_after_transaction=False, now=lambda: mclock["t"])))
        transaction(mctx, lambda: run_guarded(mctx, "INSERT INTO %s (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING" % TBL, [3, "c"], "INSERT"), TransactionOptions(), "postgres")
        mlog.clear()  # ignore the tx's own writer acquisitions
        mclock["t"] += 100
        execute(mctx, "SELECT 1", [], StatementIntent(write=False))
        assert mlog == ["reader"]  # sticky off ⇒ read-your-writes lost
    finally:
        raw.close()


def test_c1_with_writer_scope():
    raw = _setup_pg()
    try:
        log = []
        reader = _RecordingPool(_pg_pool(raw), "reader", log)
        writer = _RecordingPool(_pg_pool(raw), "writer", log)
        ctx = _ctx(RoutingConfig(ConnectionRegistry.from_default(reader_writer_pair(reader, writer)).build(), WriterStickyClock(use_writer_after_transaction=False)))

        def read_in_scope():
            rows = execute(ctx, "SELECT 1 AS one", [], StatementIntent(write=False))
            assert int(rows[0]["one"]) == 1

        with_writer(read_in_scope)
        assert log == ["writer"]  # the read inside with_writer went to the WRITER

        # A read OUTSIDE with_writer → reader (proves the scope, not a permanent divert).
        execute(ctx, "SELECT 1", [], StatementIntent(write=False))
        assert log == ["writer", "reader"]

        # A write inside with_writer is REJECTED (read-your-writes scope is read-only) — v1 parity.
        def reject_body():
            check_write_allowed_ambient("INSERT", "X")

        with pytest.raises(WriteInReadOnlyContextError):
            with_writer(reject_body)

        # MUTATION (RED): the SAME read OUTSIDE the with_writer scope → the READER.
        log.clear()
        execute(ctx, "SELECT 1 AS one", [], StatementIntent(write=False))
        assert log == ["reader"]  # in-scope 'writer' was load-bearing
    finally:
        raw.close()


# ════════════════════════════════════════════════════════════════════════════════
# C2 — multi-DB name routing (PG = A, MySQL = B) + tx-pin precedence
# ════════════════════════════════════════════════════════════════════════════════


def test_c2_multi_db_name_routing():
    pg_raw = _setup_pg()
    my_raw = _setup_my()
    try:
        log = []
        a = _RecordingPool(_pg_pool(pg_raw), "A", log)
        b = _RecordingPool(_my_pool(my_raw), "B", log)
        registry = ConnectionRegistry({"default": reader_writer_pair(a, a), "B": reader_writer_pair(b, b)})
        ctx = _ctx(RoutingConfig(registry, WriterStickyClock(use_writer_after_transaction=False)))

        ra = execute(ctx, "SELECT 42 AS n", [], StatementIntent(write=False))
        assert int(ra[0]["n"]) == 42  # untagged → A (PG)
        rb = execute(ctx, "SELECT 7 AS n", [], StatementIntent(write=False, db="B"))
        assert int(rb[0]["n"]) == 7  # "B"-tagged → B (MySQL)
        assert log == ["A", "B"]

        # REAL cross-DB: write a distinct row into each DB via its tagged pool, read it back.
        run(ctx, "INSERT INTO %s (id, val) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING" % TBL, [100, "in-A"], StatementIntent(write=True))
        run(ctx, "INSERT INTO %s (id, val) VALUES (?,?) ON DUPLICATE KEY UPDATE val=val" % MY_TBL, [200, "in-B"], StatementIntent(write=True, db="B"))
        in_a = execute(ctx, "SELECT val FROM %s WHERE id=$1" % TBL, [100], StatementIntent(write=False))
        in_b = execute(ctx, "SELECT val FROM %s WHERE id=?" % MY_TBL, [200], StatementIntent(write=False, db="B"))
        assert in_a[0]["val"] == "in-A"
        assert in_b[0]["val"] == "in-B"
        miss = execute(ctx, "SELECT val FROM %s WHERE id=?" % MY_TBL, [100], StatementIntent(write=False, db="B"))
        assert len(miss) == 0  # the A-only row is NOT in B (separate databases)

        # MUTATION (RED): if routing IGNORED intent.db and used the default (A = PG), the MySQL-placeholder
        # `... WHERE id=?` query sent to PG (which uses $N) would THROW. Force it onto the default pool.
        forced = _ctx(RoutingConfig(ConnectionRegistry({"default": reader_writer_pair(a, a)}), WriterStickyClock(use_writer_after_transaction=False)))
        with pytest.raises(Exception):
            execute(forced, "SELECT val FROM %s WHERE id=?" % TBL, [200], StatementIntent(write=False))
    finally:
        pg_raw.close()
        my_raw.close()


def test_c2_missing_name_is_loud():
    pg_raw = _setup_pg()
    try:
        a = _pg_pool(pg_raw)
        ctx = _ctx(RoutingConfig(ConnectionRegistry.single_default(a), WriterStickyClock(use_writer_after_transaction=False)))
        with pytest.raises(ValueError, match=r"no connection registered under name 'ghost'"):
            execute(ctx, "SELECT 1", [], StatementIntent(write=False, db="ghost"))
    finally:
        pg_raw.close()


def test_c2_named_db_tx_pin_wins_over_routing():
    """A NAMED-DB transaction runs entirely on ONE pinned writer connection: every statement in the body
    resolves the SAME tx-owned connection (the active-tx pin wins over routing) — Phase B unbroken. A
    recording writer pool asserts EXACTLY ONE acquire (the tx's), and a distinct reader pool records
    ZERO (the in-tx read does NOT fan out to the reader)."""
    pg_raw = _setup_pg()
    my_raw = _setup_my()
    try:
        # Reset the B (MySQL) rows.
        c = my_raw.acquire()
        cur = c.cursor()
        cur.execute("DELETE FROM %s" % MY_TBL)
        cur.close()
        my_raw.release(c)

        rlog, wlog = [], []
        b_reader = _RecordingPool(_my_pool(my_raw), "B-reader", rlog)
        b_writer = _RecordingPool(_my_pool(my_raw), "B-writer", wlog)
        registry = ConnectionRegistry({"default": reader_writer_pair(_pg_pool(pg_raw), _pg_pool(pg_raw)), "B": reader_writer_pair(b_reader, b_writer)})
        ctx = _ctx(RoutingConfig(registry, WriterStickyClock(use_writer_after_transaction=False)))

        def body():
            # A write AND a read inside the tx — both must run on the ONE pinned tx connection (from the
            # B writer pool), NOT fan out to the B reader. Operations JOIN the ambient tx via the pinned
            # ctx in the contextvar (current_context()); a bare outer-ctx call would NOT see the pin.
            tx_ctx = current_context()
            run(tx_ctx, "INSERT INTO %s (id, val) VALUES (?, ?)" % MY_TBL, [1, "x"], StatementIntent(write=True, db="B"))
            rows = execute(tx_ctx, "SELECT val FROM %s WHERE id = ?" % MY_TBL, [1], StatementIntent(write=False, db="B"))
            assert rows[0]["val"] == "x"  # read-your-own-write inside the tx (same connection)
            return 0

        transaction(ctx, body, TransactionOptions(), "mysql", connection="B")

        # The tx acquired ONCE from the B WRITER pool (the pinned connection); the B READER saw ZERO
        # (the in-tx read did NOT route to the reader — the tx-pin won).
        assert wlog.count("B-writer") == 1, "the tx must acquire exactly ONE writer connection, got %r" % wlog
        assert rlog == [], "the in-tx read must NOT fan out to the reader (tx-pin wins), got %r" % rlog

        # The committed row is really in B.
        rows = execute(ctx, "SELECT val FROM %s WHERE id = ?" % MY_TBL, [1], StatementIntent(write=False, db="B"))
        assert rows[0]["val"] == "x"

        # MUTATION (RED): the SAME read issued through the OUTER ctx (no tx pin) inside a fresh tx would
        # route to the B READER (the routing steps, not the pin) — proving the tx-pin is what kept the
        # in-tx read on the ONE writer connection. Observe: a bare outer-ctx read (no pin) lands on the
        # reader, so rlog gains a "B-reader".
        rlog.clear()
        execute(ctx, "SELECT val FROM %s WHERE id = ?" % MY_TBL, [1], StatementIntent(write=False, db="B"))
        assert rlog == ["B-reader"], "a non-pinned read routes to the reader (the pin was load-bearing), got %r" % rlog
    finally:
        pg_raw.close()
        my_raw.close()


# ════════════════════════════════════════════════════════════════════════════════
# C3 — set_config: queryTimeout / pool sizing / searchPath reset / close_all_pools
# ════════════════════════════════════════════════════════════════════════════════


def test_c3_query_timeout_fires_pg():
    raw = _setup_pg()
    try:
        cfg = resolve_connection_config(ConnectionConfig(driver="postgres", query_timeout=200))
        pool = configured_pool(_pg_pool(raw), cfg)
        ctx = _ctx(RoutingConfig(ConnectionRegistry.single_default(pool), WriterStickyClock(use_writer_after_transaction=False)))

        # pg_sleep(2) (2s) exceeds the 200ms server statement_timeout → the SERVER aborts it.
        with pytest.raises(Exception, match=r"(?i)statement timeout|canceling statement"):
            execute(ctx, "SELECT pg_sleep(2)", [], StatementIntent(write=False))

        # MUTATION (RED): the SAME slow query on an UNCONFIGURED pool does NOT time out at 200ms.
        plain = _ctx(RoutingConfig(ConnectionRegistry.single_default(_pg_pool(raw)), WriterStickyClock(use_writer_after_transaction=False)))
        ok = execute(plain, "SELECT pg_sleep(0.3) AS done", [], StatementIntent(write=False))  # 300ms; no timeout set ⇒ succeeds
        assert len(ok) == 1
    finally:
        raw.close()


def test_c3_query_timeout_fires_mysql():
    raw = _setup_my()
    try:
        cfg = resolve_connection_config(ConnectionConfig(driver="mysql", query_timeout=200))
        pool = configured_pool(_my_pool(raw), cfg)
        ctx = _ctx(RoutingConfig(ConnectionRegistry.single_default(pool), WriterStickyClock(use_writer_after_transaction=False)))

        # A CPU-heavy SELECT (cross-join + SHA2) burns past 200ms; max_execution_time aborts it (NOT SLEEP,
        # which max_execution_time is documented to ignore). SELECT-only ⇒ read intent.
        heavy = (
            "SELECT COUNT(*) AS n FROM information_schema.COLLATIONS a, information_schema.COLLATIONS b, "
            "information_schema.COLLATIONS c WHERE SHA2(CONCAT(a.ID, b.ID, c.ID, RAND()), 256) > ''"
        )
        with pytest.raises(Exception, match=r"(?i)max_execution_time|execution was interrupted|3024|query execution"):
            execute(ctx, heavy, [], StatementIntent(write=False))

        # MUTATION (RED): the SAME (smaller) heavy query on an UNCONFIGURED pool COMPLETES.
        plain = _ctx(RoutingConfig(ConnectionRegistry.single_default(_my_pool(raw)), WriterStickyClock(use_writer_after_transaction=False)))
        small = (
            "SELECT COUNT(*) AS n FROM information_schema.COLLATIONS a, information_schema.COLLATIONS b "
            "WHERE SHA2(CONCAT(a.ID, b.ID), 256) > ''"
        )
        ok = execute(plain, small, [], StatementIntent(write=False))
        assert int(ok[0]["n"]) > 0  # uncapped ⇒ completes
    finally:
        raw.close()


def test_c3_max_pool_is_sole_cap_and_close():
    """max_pool is the SOLE cap, applied at CONSTRUCTION: build via the factory from config → the pool
    ceiling is max_pool; fire N>max_pool concurrent slow queries and prove no more than max_pool
    connections are ever open at once. DELETE max_pool → the default 10 lets all N open → RED."""
    _setup_pg()  # ensure schema exists (the queries below are schema-independent, but keep parity)

    # We capture the constructed _ConnectionPool (via a wrapping factory) to observe its `_opened` count.
    def capturing_factory(captured):
        base = pg_pool_factory()

        def factory(config, role):
            pool, close = base(config, role)
            # pool is a RawConnectionPool; its underlying _ConnectionPool is pool._pool.
            captured["raw"] = pool._pool
            return pool, close

        return factory

    captured = {}
    routing, close = build_routing_config(
        [ConnectionSetup(pool_factory=capturing_factory(captured), config=ConnectionConfig(driver="postgres", host=_pg_kwargs()["host"], port=_pg_kwargs()["port"], database=_pg_kwargs()["dbname"], user=_pg_kwargs()["user"], password=_pg_kwargs()["password"], max_pool=2))]
    )
    ctx = _ctx(routing)
    raw = captured["raw"]

    peak = {"v": 0}
    lock = threading.Lock()

    def q():
        execute(ctx, "SELECT pg_sleep(0.25) AS d", [], StatementIntent(write=False))

    threads = [threading.Thread(target=q) for _ in range(5)]
    for t in threads:
        t.start()
    time.sleep(0.12)
    mid = raw._opened  # connections opened mid-flight
    for t in threads:
        t.join()
    assert mid <= 2, "max_pool=2 must cap live connections at 2, saw %d" % mid
    close()

    # MUTATION (RED): DELETE max_pool ⇒ default 10 ⇒ all 5 connections open at once (> 2).
    capturedM = {}
    routingM, closeM = build_routing_config(
        [ConnectionSetup(pool_factory=capturing_factory(capturedM), config=ConnectionConfig(driver="postgres", host=_pg_kwargs()["host"], port=_pg_kwargs()["port"], database=_pg_kwargs()["dbname"], user=_pg_kwargs()["user"], password=_pg_kwargs()["password"]))]  # NO max_pool
    )
    ctxM = _ctx(routingM)
    rawM = capturedM["raw"]
    threadsM = [threading.Thread(target=lambda: execute(ctxM, "SELECT pg_sleep(0.25) AS d", [], StatementIntent(write=False))) for _ in range(5)]
    for t in threadsM:
        t.start()
    time.sleep(0.12)
    midM = rawM._opened
    for t in threadsM:
        t.join()
    assert midM > 2, "uncapped (no max_pool ⇒ default 10) must open >2 connections, saw %d — max_pool is the SOLE cap" % midM
    closeM()

    # close() closed the first pool: a query after close fails (the close is real).
    with pytest.raises(Exception):
        execute(ctx, "SELECT 1", [], StatementIntent(write=False))


def test_c3_search_path_reset_on_release_no_session_leak():
    """searchPath is applied on checkout AND RESET on release, so the SAME pooled connection does NOT
    leak the search_path to the next caller. Pool size 1 forces reuse of the ONE connection: a configured
    (search_path=phase_c_routing_py) checkout resolves the schema-unqualified table; the NEXT unconfigured
    checkout of that SAME connection must NOT still see the search_path (RESET on release)."""
    raw = _setup_pg()
    try:
        # Seed a row into the schema table.
        c = raw.acquire()
        cur = c.cursor()
        cur.execute("DELETE FROM %s" % TBL)
        cur.execute("INSERT INTO %s (id, val) VALUES (1, 'sp')" % TBL)
        cur.close()
        raw.release(c)

        # ONE shared connection so config-set state would LEAK without a reset.
        raw1 = _pg_raw_pool(size=1)
        try:
            cfg = resolve_connection_config(ConnectionConfig(driver="postgres", search_path=PG_SCHEMA))
            configured = configured_pool(_pg_pool(raw1), cfg)
            ctx_cfg = _ctx(RoutingConfig(ConnectionRegistry.single_default(configured), WriterStickyClock(use_writer_after_transaction=False)))
            # search_path=phase_c_routing_py ⇒ the UNQUALIFIED name resolves to phase_c_routing_py.scp_route_py.
            rows = execute(ctx_cfg, "SELECT val FROM scp_route_py WHERE id = $1", [1], StatementIntent(write=False))
            assert rows[0]["val"] == "sp"

            # NOW use the SAME underlying pool WITHOUT the config (plain pool over raw1). If the search_path
            # leaked (no reset), the unqualified name would STILL resolve; with the reset it must NOT (the
            # schema is not on the default search_path) ⇒ a "relation does not exist" error.
            plain = _pg_pool(raw1)
            ctx_plain = _ctx(RoutingConfig(ConnectionRegistry.single_default(plain), WriterStickyClock(use_writer_after_transaction=False)))
            with pytest.raises(Exception, match=r"(?i)does not exist|relation"):
                execute(ctx_plain, "SELECT val FROM scp_route_py WHERE id = $1", [1], StatementIntent(write=False))

            # MUTATION (RED): the schema-QUALIFIED name always resolves — proves the DB + row are fine, so
            # the failure above is specifically the search_path RESET (no session leak), not a missing row.
            ok = execute(ctx_plain, "SELECT val FROM %s WHERE id = $1" % TBL, [1], StatementIntent(write=False))
            assert ok[0]["val"] == "sp"
        finally:
            raw1.close()
    finally:
        raw.close()


def test_c3_pg_and_mysql_factories_end_to_end():
    """Exercise the SHIPPED factories (the ones the ports mirror) end-to-end: build via
    build_routing_config from config → query live → close, against real PG and MySQL."""
    _setup_pg()
    pg_built_routing, pg_close = build_routing_config([ConnectionSetup(pool_factory=pg_pool_factory(), config=_pg_conn_config())])
    pctx = _ctx(pg_built_routing)
    pr = execute(pctx, "SELECT 11 AS n", [], StatementIntent(write=False))
    assert int(pr[0]["n"]) == 11
    pg_close()

    my_built_routing, my_close = build_routing_config([ConnectionSetup(pool_factory=mysql_pool_factory(), config=_my_conn_config())])
    mctx = _ctx(my_built_routing)
    mr = execute(mctx, "SELECT 13 AS n", [], StatementIntent(write=False))
    assert int(mr[0]["n"]) == 13
    my_close()
