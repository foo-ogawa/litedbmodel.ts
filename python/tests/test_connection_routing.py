"""Phase C (#90, python) — connection routing + config UNIT tests (no live DB).

Pure-logic proofs of the completion of ``connection_for(intent)``'s resolution (design §3 steps 2-4)
and the C3 config surface, mirroring the pure blocks of the TS ``ConnectionRouting.test.ts`` (#87):

  - resolve_connection_config defaults (C3);
  - session_statements / session_reset_statements per-dialect mapping (C3);
  - resolve_pool routing order: named-DB → write=writer / read=reader → withWriter / writer-sticky (C1+C2);
  - ConnectionRegistry loud-fail on an unknown name (C2);
  - WriterStickyClock injectable-clock expiry (C1);
  - configured_pool session apply-on-acquire + reset-on-release with a fake pool (C3, no session leak);
  - build_routing_config CONSTRUCTS pools via the factory with the resolved sizing (C3).

Each behavioral assertion has a faithful-mutation RED proof (break the routing/sticky/config ⇒ the
assertion flips) so the test is not vacuous. The live-DB proofs (real PG:5433 + MySQL:3307, incl. the
maxPool-sole-cap RED + queryTimeout firing) are in test_connection_routing_livedb.py.
"""

from __future__ import annotations

from litedbmodel_runtime import (
    ConnectionConfig,
    ConnectionRegistry,
    ConnectionRegistryBuilder,
    ConnectionSetup,
    RoutingConfig,
    WriterStickyClock,
    build_routing_config,
    configured_pool,
    reader_writer_pair,
    resolve_connection_config,
    resolve_pool,
    session_reset_statements,
    session_statements,
    single_pool_pair,
    with_writer,
)
from litedbmodel_runtime.connection_routing import (
    ConnectionPool,
    ConfiguredPool,
    RawConnectionPool,
    PoolConnection,
)
from litedbmodel_runtime.exec_context import StatementIntent

import pytest


# ── A fake in-memory pool (label-recording; no real DB) ─────────────────────────


class _FakePool(ConnectionPool):
    """A ConnectionPool that hands out a labeled sentinel and records every acquire/release. Used to
    assert WHICH pool a routing decision selected without any DB contact."""

    def __init__(self, label):
        self.label = label
        self.acquired = []
        self.released = []  # (conn, destroy)

    def acquire(self):
        self.acquired.append(self.label)
        return {"_pool": self.label, "_cursors": []}

    def release(self, conn, destroy=False):
        self.released.append((conn.get("_pool"), destroy))


# ── C3: resolve defaults ────────────────────────────────────────────────────────


def test_resolve_connection_config_defaults():
    r = resolve_connection_config()
    assert r.driver == "postgres"
    assert r.query_timeout == 0
    assert r.keep_alive is False
    assert r.keep_alive_initial_delay_millis == 10000
    assert r.min_pool == 0
    assert r.max_pool == 10
    assert r.search_path is None and r.charset is None
    # Overrides pass through with the same field names.
    r2 = resolve_connection_config(
        ConnectionConfig(driver="mysql", max_pool=2, min_pool=1, query_timeout=250, keep_alive=True, search_path="app", charset="utf8mb4")
    )
    assert (r2.driver, r2.max_pool, r2.min_pool, r2.query_timeout, r2.keep_alive, r2.search_path, r2.charset) == (
        "mysql",
        2,
        1,
        250,
        True,
        "app",
        "utf8mb4",
    )


# ── C3: session statements per dialect ──────────────────────────────────────────


def test_session_statements_per_dialect():
    # PG
    assert session_statements(resolve_connection_config(ConnectionConfig(driver="postgres", query_timeout=250))) == [
        "SET statement_timeout = 250"
    ]
    assert session_statements(resolve_connection_config(ConnectionConfig(driver="postgres", search_path="app,public"))) == [
        "SET search_path TO app,public"
    ]
    assert session_statements(resolve_connection_config(ConnectionConfig(driver="postgres", charset="UTF8"))) == [
        "SET client_encoding TO UTF8"
    ]
    # MySQL
    assert session_statements(resolve_connection_config(ConnectionConfig(driver="mysql", query_timeout=250))) == [
        "SET SESSION max_execution_time = 250"
    ]
    assert session_statements(resolve_connection_config(ConnectionConfig(driver="mysql", charset="utf8mb4"))) == ["SET NAMES utf8mb4"]
    # MySQL has no search path → ignored
    assert session_statements(resolve_connection_config(ConnectionConfig(driver="mysql", search_path="x"))) == []
    # All-default → EMPTY (backward-compat: session untouched)
    assert session_statements(resolve_connection_config()) == []
    # sqlite → EMPTY
    assert session_statements(resolve_connection_config(ConnectionConfig(driver="sqlite", query_timeout=250))) == []


def test_session_reset_statements_per_dialect():
    assert session_reset_statements(resolve_connection_config(ConnectionConfig(driver="postgres", query_timeout=1, search_path="a", charset="UTF8"))) == [
        "RESET statement_timeout",
        "RESET search_path",
        "RESET client_encoding",
    ]
    assert session_reset_statements(resolve_connection_config(ConnectionConfig(driver="mysql", query_timeout=1, charset="utf8mb4"))) == [
        "SET SESSION max_execution_time = DEFAULT",
        "SET NAMES DEFAULT",
    ]
    assert session_reset_statements(resolve_connection_config()) == []


# ── C1+C2: resolve_pool routing order ───────────────────────────────────────────


def _routing(reader, writer, sticky=None, name_pairs=None):
    builder = ConnectionRegistry.from_default(reader_writer_pair(reader, writer))
    if name_pairs:
        for n, p in name_pairs.items():
            builder.add(n, p)
    return RoutingConfig(builder.build(), sticky or WriterStickyClock(use_writer_after_transaction=False))


def test_resolve_pool_read_reader_write_writer():
    reader, writer = _FakePool("reader"), _FakePool("writer")
    routing = _routing(reader, writer)
    assert resolve_pool(StatementIntent(write=False), routing) is reader
    assert resolve_pool(StatementIntent(write=True), routing) is writer

    # MUTATION (RED): collapse the split to ONE pool → read no longer distinguishable from write.
    solo = _FakePool("solo")
    mrouting = _routing(solo, solo)
    assert resolve_pool(StatementIntent(write=False), mrouting) is resolve_pool(StatementIntent(write=True), mrouting)


def test_resolve_pool_named_db():
    a_reader, a_writer = _FakePool("A"), _FakePool("A")
    b_reader, b_writer = _FakePool("B"), _FakePool("B")
    routing = _routing(a_reader, a_writer, name_pairs={"B": reader_writer_pair(b_reader, b_writer)})
    # Untagged → default (A); tagged "B" → B.
    assert resolve_pool(StatementIntent(write=False), routing) is a_reader
    assert resolve_pool(StatementIntent(write=False, db="B"), routing) is b_reader

    # MUTATION (RED): if routing IGNORED intent.db, the "B"-tagged read would land on A (a_reader), not b_reader.
    assert resolve_pool(StatementIntent(write=False, db="B"), routing) is not a_reader


def test_registry_unknown_name_is_loud():
    a = _FakePool("A")
    routing = _routing(a, a)
    with pytest.raises(ValueError, match=r"no connection registered under name 'ghost'"):
        resolve_pool(StatementIntent(write=False, db="ghost"), routing)


def test_resolve_pool_with_writer_scope_and_sticky():
    reader, writer = _FakePool("reader"), _FakePool("writer")
    routing = _routing(reader, writer)

    # Plain read → reader.
    assert resolve_pool(StatementIntent(write=False), routing) is reader

    # Inside with_writer → a read routes to the WRITER (read-your-writes).
    def inside():
        return resolve_pool(StatementIntent(write=False), routing)

    assert with_writer(inside) is writer

    # MUTATION (RED): OUTSIDE with_writer the same read lands on the reader (proves the scope diverts).
    assert resolve_pool(StatementIntent(write=False), routing) is reader

    # writer-sticky: after mark(), an in-window read → writer; after expiry → reader.
    clock = {"t": 1_000_000}
    sticky = WriterStickyClock(use_writer_after_transaction=True, writer_sticky_duration=5000, now=lambda: clock["t"])
    rs = _routing(reader, writer, sticky=sticky)
    assert resolve_pool(StatementIntent(write=False), rs) is reader  # not armed
    sticky.mark()
    clock["t"] += 100
    assert resolve_pool(StatementIntent(write=False), rs) is writer  # in window → writer
    clock["t"] += 6000
    assert resolve_pool(StatementIntent(write=False), rs) is reader  # elapsed → reader

    # MUTATION (RED): sticky OFF ⇒ even an in-window read stays on the reader (read-your-writes lost).
    sticky_off = WriterStickyClock(use_writer_after_transaction=False, now=lambda: clock["t"])
    rs_off = _routing(reader, writer, sticky=sticky_off)
    sticky_off.mark()
    assert resolve_pool(StatementIntent(write=False), rs_off) is reader


def test_writer_sticky_clock_expiry_deterministic():
    clock = {"t": 0}
    c = WriterStickyClock(use_writer_after_transaction=True, writer_sticky_duration=1000, now=lambda: clock["t"])
    assert c.is_sticky() is False  # never marked
    clock["t"] = 500
    c.mark()
    clock["t"] = 900
    assert c.is_sticky() is True  # 400ms < 1000ms window
    clock["t"] = 1600
    assert c.is_sticky() is False  # 1100ms ≥ window
    c.reset()
    clock["t"] = 1700
    assert c.is_sticky() is False


# ── C3: configured_pool session apply/reset (no session leak) — fake conn ────────


class _RecCursor:
    def __init__(self, sink):
        self._sink = sink

    def execute(self, sql, params=None):
        self._sink.append(sql)

    def close(self):
        pass


class _ConnObj:
    """A fake raw connection whose ``.cursor()`` records every executed SQL into ``_cursors`` (for
    asserting the session apply/reset statements a ConfiguredPool issues on checkout/release)."""

    def __init__(self, label):
        self._pool = label
        self._cursors = []

    def __getitem__(self, k):
        return {"_pool": self._pool, "_cursors": self._cursors}[k]

    def cursor(self):
        return _RecCursor(self._cursors)


class _CursorPool(ConnectionPool):
    """A pool whose raw connection has a recording ``.cursor()`` (for ConfiguredPool session tests)."""

    def __init__(self, label):
        self.label = label
        self.released = []

    def acquire(self):
        return _ConnObj(self.label)

    def release(self, conn, destroy=False):
        self.released.append((conn["_pool"], destroy))


def test_configured_pool_applies_and_resets_session():
    cfg = resolve_connection_config(ConnectionConfig(driver="postgres", query_timeout=200, search_path="app"))
    base2 = _CursorPool("cfg2")
    pool2 = configured_pool(base2, cfg)
    assert isinstance(pool2, ConfiguredPool)

    # acquire → the session statements run on the connection.
    conn2 = pool2.acquire()
    assert conn2["_cursors"] == ["SET statement_timeout = 200", "SET search_path TO app"]
    conn2["_cursors"].clear()
    # release → the reset statements run (reset-on-release: no session leak to the next caller).
    pool2.release(conn2, False)
    assert conn2["_cursors"] == ["RESET statement_timeout", "RESET search_path"]
    assert base2.released[-1] == ("cfg2", False)  # a clean connection goes back to the pool

    # MUTATION (RED): an all-default config ⇒ configured_pool is a transparent passthrough (unwrapped).
    plain = configured_pool(_CursorPool("plain"), resolve_connection_config())
    assert not isinstance(plain, ConfiguredPool)
    c = plain.acquire()
    assert c["_cursors"] == []  # no session statements — byte-identical passthrough


def test_configured_pool_destroys_on_session_failure():
    class _FailCursor:
        def execute(self, sql, params=None):
            raise RuntimeError("bad search_path")

        def close(self):
            pass

    class _FailConn(_ConnObj):
        def cursor(self):
            return _FailCursor()

    class _FailCursorPool(_CursorPool):
        def acquire(self):
            return _FailConn(self.label)

    base = _FailCursorPool("failcfg")
    cfg = resolve_connection_config(ConnectionConfig(driver="postgres", search_path="nonexistent"))
    pool = configured_pool(base, cfg)
    with pytest.raises(RuntimeError, match="bad search_path"):
        pool.acquire()
    # A failed session setup DESTROYS the connection (never re-enters the pool).
    assert base.released[-1][1] is True


# ── C3: build_routing_config constructs pools via the factory with resolved sizing ──


def test_build_routing_config_constructs_via_factory():
    captured = {}

    def fake_factory(config, role):
        captured["max_pool"] = config.max_pool
        captured["min_pool"] = config.min_pool
        captured["keep_alive"] = config.keep_alive
        captured["role"] = role
        closed = {"v": False}
        pool = _FakePool("built")

        def close():
            closed["v"] = True

        pool._closed = closed
        return pool, close

    routing, close = build_routing_config(
        [ConnectionSetup(pool_factory=fake_factory, config=ConnectionConfig(driver="postgres", max_pool=2, min_pool=1, keep_alive=True))]
    )
    # The factory was called with the RESOLVED sizing (config is the sole cap source).
    assert captured == {"max_pool": 2, "min_pool": 1, "keep_alive": True, "role": "reader"}
    # single default connection, reader IS writer (one constructed pool).
    pair = routing.registry.pair_for(None)
    assert pair.reader is pair.writer
    # close() closes the constructed pool.
    close()

    # MUTATION (RED): DELETE max_pool ⇒ the factory sees the DEFAULT cap (10), not 2 — so max_pool is
    # the load-bearing, sole cap source (a dead key would still show 2 here).
    captured.clear()
    build_routing_config([ConnectionSetup(pool_factory=fake_factory, config=ConnectionConfig(driver="postgres"))])
    assert captured["max_pool"] == 10  # default, not 2 ⇒ config max_pool genuinely drives the cap


def test_build_routing_config_separate_writer():
    calls = []

    def fake_factory(config, role):
        calls.append(role)
        return _FakePool(role), (lambda: None)

    routing, _close = build_routing_config(
        [ConnectionSetup(pool_factory=fake_factory, config=ConnectionConfig(driver="postgres"), separate_writer=True)]
    )
    assert calls == ["reader", "writer"]  # a distinct writer pool was constructed
    pair = routing.registry.pair_for(None)
    assert pair.reader is not pair.writer


def test_build_routing_config_empty_is_loud():
    with pytest.raises(ValueError, match="at least one connection setup"):
        build_routing_config([])


# ── PoolConnection runs on a routed pool (byte-identical acquire-run-release) ────


def test_pool_connection_acquire_release_per_statement():
    # A RawConnectionPool over a tiny in-memory sqlite exercises the real _conn_all / _conn_run path.
    import sqlite3
    from litedbmodel_runtime.driver import _ConnectionPool

    def factory():
        c = sqlite3.connect(":memory:")
        c.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        c.commit()
        return c

    # One shared connection (pool size 1) so the row persists across acquire/release.
    raw = _ConnectionPool(factory, 1)
    pool = RawConnectionPool(raw, lambda s: s, emulate_returning=False)  # sqlite uses ? natively
    conn = PoolConnection(pool)
    conn.run("INSERT INTO t (id, v) VALUES (?, ?)", [1, "a"])
    rows = conn.execute("SELECT v FROM t WHERE id = ?", [1])
    assert rows == [{"v": "a"}]
    raw.close()
