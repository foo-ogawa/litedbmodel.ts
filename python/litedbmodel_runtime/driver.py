"""litedbmodel v2 SCP — SQL driver seam (WS7b).

The minimal synchronous SQL-driver surface the runtime needs, mirroring the TS `SqliteDb`
seam (`prepare(sql).all(...) / .run(...)`). The conformance bar executes against an in-process
stdlib ``sqlite3`` connection (:class:`SqliteDriver`) — the sanctioned in-proc substitute for a
docker integration DB (#31 AC; live PG/MySQL is deferred to a coordinated cross-language docker
pass). A psycopg / mysql-connector driver plugs into this SAME abstract seam later: implement
:class:`Driver.prepare` returning a :class:`PreparedStatement` (`all` / `run`) over the
paramstyle the bundle's dialect emits (`$N` for Postgres, `?`/`%s` for MySQL) — no runtime change.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any, Dict, List, Protocol, Sequence


class RunInfo:
    """The summary of a non-returning write: affected-row count + last insert rowid."""

    __slots__ = ("changes", "last_insert_rowid")

    def __init__(self, changes: int, last_insert_rowid: int) -> None:
        self.changes = changes
        self.last_insert_rowid = last_insert_rowid


class PreparedStatement(Protocol):
    """A prepared statement: `all` returns the row list (SELECT/RETURNING); `run` a write summary."""

    def all(self, params: Sequence[Any]) -> List[Dict[str, Any]]: ...

    def run(self, params: Sequence[Any]) -> RunInfo: ...


class TxConnection(Protocol):
    """An OWNED transaction connection (Phase A / #78) — the Python analogue of v1 ``PoolTransaction``
    / go's ``*sql.Tx``. Acquired by :meth:`Driver.begin_tx`, it holds ONE connection for the
    transaction's whole duration: EVERY statement in the tx — the body (``all`` / ``run``) AND the
    tx-control (BEGIN / COMMIT / ROLLBACK / the isolation SET) — runs on it via ``run`` / ``all``. The
    tx-control is issued THROUGH the exec-context seam by ``with_transaction_decided`` (Phase D / #95,
    middleware-visible), NOT by this handle. The caller then :meth:`release` s the connection EXACTLY
    ONCE (back to the pool, or destroyed if poisoned).

    **Release ownership**: this handle is the connection OWNER, not the tx-control issuer. The seam
    combinator (``with_transaction_decided``) is the SOLE owner of :meth:`release`, calling it in a
    ``finally`` so the owned connection is returned/destroyed on EVERY path (success, BEGIN error, body
    error, AND a commit/rollback that itself raises — the leak the self-release model missed).
    :meth:`release` is idempotent (a second call is a no-op).

    Concurrent transactions each hold a DISTINCT handle over a DISTINCT pooled connection, so their
    writes never cross-talk — the isolation the removed driver-global ``_writer`` slot violated.
    """

    def all(self, sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]: ...

    def run(self, sql: str, params: Sequence[Any]) -> RunInfo: ...

    def release(self, destroy: bool) -> None:
        """Release the owned connection EXACTLY ONCE (idempotent): back to the pool, or dropped when
        ``destroy`` (a poisoned connection — a BEGIN/COMMIT/ROLLBACK that itself raised). Called by the
        seam combinator in a ``finally``; the tx-control SQL itself is issued through the seam."""
        ...


class Driver(Protocol):
    """The synchronous SQL-driver seam (mirrors the TS `SqliteDb`)."""

    def prepare(self, sql: str) -> PreparedStatement: ...

    def begin_tx(self) -> TxConnection:
        """Acquire + OWN a :class:`TxConnection` for a transaction (per-execution connection ownership,
        §3). The central seam's ``with_transaction`` pins the returned handle so every statement in the
        tx body runs on it, and issues the isolation SET + BEGIN/COMMIT/ROLLBACK THROUGH the seam on this
        connection (Phase D / #95, middleware-visible) — this method only acquires the owned connection.
        Empty prelude ⇒ a bare ``BEGIN`` (the Phase A behavior, byte-identical statements + connection)."""
        ...


class _SqlitePrepared:
    """A prepared statement over a stdlib ``sqlite3`` connection."""

    __slots__ = ("_conn", "_sql")

    def __init__(self, conn: "sqlite3.Connection", sql: str) -> None:
        self._conn = conn
        self._sql = sql

    def all(self, params: Sequence[Any]) -> List[Dict[str, Any]]:
        cur = self._conn.execute(self._sql, tuple(params))
        cols = [c[0] for c in cur.description] if cur.description is not None else []
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows

    def run(self, params: Sequence[Any]) -> RunInfo:
        cur = self._conn.execute(self._sql, tuple(params))
        changes = cur.rowcount if cur.rowcount is not None else 0
        last = cur.lastrowid if cur.lastrowid is not None else 0
        cur.close()
        return RunInfo(changes, last)


class SqliteDriver:
    """An in-process stdlib ``sqlite3`` driver implementing the :class:`Driver` seam.

    This is the runnable conformance seam: it binds `?` placeholders positionally, so a
    Postgres-tagged bundle's `$N` text is NOT what runs here — the exec/tx vectors run only the
    SQLite-tagged bundles (the §10 promise: same IR + input → same RESULT regardless of dialect
    text). PG/MySQL SQL-text conformance is proven on the render axis; live PG/MySQL execution is
    the coordinated docker pass.
    """

    __slots__ = ("conn",)

    def __init__(self, conn: "sqlite3.Connection") -> None:
        self.conn = conn

    @classmethod
    def in_memory(cls, schema: Sequence[str]) -> "SqliteDriver":
        conn = sqlite3.connect(":memory:")
        conn.execute("PRAGMA foreign_keys = ON")
        for stmt in schema:
            conn.execute(stmt)
        conn.commit()
        return cls(conn)

    def prepare(self, sql: str) -> _SqlitePrepared:
        return _SqlitePrepared(self.conn, sql)

    def begin_tx(self) -> "_SqliteTxConnection":
        """Own the OWNED tx connection (§3). SQLite is single-connection, so the tx owns THE conn: every
        tx statement runs on it. tx-control (BEGIN / COMMIT / ROLLBACK) is issued THROUGH the seam by the
        combinator on THIS connection (Phase D / #95, middleware-visible) — the SAME single-conn
        BEGIN…COMMIT bracket the pre-seam path ran, byte-identical (same literal statements, same conn).

        SQLite has NO per-transaction isolation level; the Phase B contract loud-rejects an isolation
        request for SQLite BEFORE it reaches here (:func:`isolation_prelude`), so the combinator issues a
        bare BEGIN with no prelude on this path."""
        return _SqliteTxConnection(self.conn)

    def close(self) -> None:
        self.conn.close()


class _SqliteTxConnection:
    """The OWNED tx handle over a stdlib ``sqlite3`` connection (single-conn; the tx owns THE conn).
    Both tx-body statements AND tx-control (BEGIN/COMMIT/ROLLBACK) run on THIS conn via :meth:`run`,
    routed THROUGH the seam by the combinator (Phase D / #95) — so a middleware observes them. This
    handle owns the conn (there is no pool to return to); it no longer issues tx-control itself."""

    __slots__ = ("_conn",)

    def __init__(self, conn: "sqlite3.Connection") -> None:
        self._conn = conn

    def all(self, sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
        return _SqlitePrepared(self._conn, sql).all(params)

    def run(self, sql: str, params: Sequence[Any]) -> RunInfo:
        # Serves tx-body writes AND tx-control (BEGIN/COMMIT/ROLLBACK) — the SAME literal statements the
        # pre-seam path ran on THIS conn, byte-identical.
        return _SqlitePrepared(self._conn, sql).run(params)

    def release(self, destroy: bool) -> None:
        # SQLite is single-connection (the driver owns THE conn); there is no pool to return to and
        # the shared conn is never dropped mid-life. A no-op — the combinator's finally still calls it
        # uniformly so the release contract is honored across drivers.
        pass


# ── Live PostgreSQL / MySQL drivers (WS7g #36; async/pooled #40) ────────────────
#
# The SAME synchronous `Driver` seam, now backed by a CONNECTION POOL over REAL psycopg (Postgres)
# / PyMySQL (MySQL) connections — proving the deferred live-DB execution axis (spec §10) AND turning
# the read plan's `concurrency` into REAL parallel DB I/O (#40). The Python bc `run_plan` dispatches
# the INDEPENDENT sibling relations of a plan stage on a `ThreadPoolExecutor` when
# `concurrency > 1` (bc#23); a single DB-API connection is NOT safe for concurrent use, so each
# `prepare().all()` CHECKS OUT ITS OWN pooled connection — distinct threads run on distinct
# connections in parallel. The runtime is UNCHANGED: it renders the dialect-tagged bundle
# (Postgres → `$N`, MySQL → `?`), binds params positionally, and calls `prepare(sql).all(...)` /
# `.run(...)`. Each live driver adapts the rendered placeholder text to its DB's native paramstyle
# (both DB-API drivers here use `%s`), and MySQL emulates the missing `RETURNING` at this seam
# (strip → execute → re-select the inserted PK) — the WS6 TS ScpDialect behavior-by-convention.
#
# WRITE-TX OWNS ITS CONNECTION (Phase A / #78): `begin_tx()` acquires ONE pooled connection, issues
# BEGIN on it, and returns an OWNED `_PooledTxConnection`; every tx-body statement runs on THAT
# connection (tx-DAG order, gate-first short-circuit), and the seam combinator ends the tx
# (COMMIT/ROLLBACK) then releases the connection EXACTLY ONCE in a finally — back to the pool, or
# destroyed if poisoned. There is NO driver-global writer slot, so concurrent transactions each own a
# DISTINCT connection ⇒ isolated. Reads (no active tx) each check out + return a pooled connection.
# The connections run with autocommit ON so the literal BEGIN…COMMIT bracket a REAL transaction.

# The read plan's default concurrency (spec) — the pool is sized to match so `concurrency` sibling
# relations can each hold a live connection without starving.
DEFAULT_POOL_SIZE = 16


class _ConnectionPool:
    """A minimal thread-safe, bounded pool of DB-API connections (dependency-free).

    A bounded ``queue`` of live connections created lazily up to ``max_size``. ``acquire`` blocks
    for a free connection (or opens a new one below the ceiling); ``release`` returns it. This keeps
    the parallel-read seam dependency-free (no psycopg_pool / DBUtils needed) while giving each
    concurrent sibling its own connection.
    """

    __slots__ = ("_factory", "_max", "_free", "_opened", "_lock", "_closed")

    def __init__(self, factory, max_size: int) -> None:
        import queue as _queue
        import threading as _threading

        self._factory = factory
        self._max = max_size
        self._free: "Any" = _queue.LifoQueue()
        self._opened = 0
        self._lock = _threading.Lock()
        # Fail-fast after close(): a post-close acquire must RAISE, not block forever on `_free.get()`
        # (the pool is drained and nothing will be released). Additive — the Phase A/B paths never
        # acquire after close, so behavior there is unchanged; Phase C's close_all_pools relies on it so
        # a query on a closed pool fails loudly (mirror the TS pool.end() → query rejects).
        self._closed = False

    def acquire(self) -> Any:
        import queue as _queue

        if self._closed:
            raise RuntimeError("scp connection pool: acquire after close (the pool has been closed)")
        # Fast path: reuse a free connection.
        try:
            return self._free.get_nowait()
        except _queue.Empty:
            pass
        # Open a new one if below the ceiling; else wait for a release.
        with self._lock:
            if self._closed:
                raise RuntimeError("scp connection pool: acquire after close (the pool has been closed)")
            if self._opened < self._max:
                self._opened += 1
                return self._factory()
        return self._free.get()  # block until a connection is released

    def release(self, conn: Any) -> None:
        self._free.put(conn)

    def discard(self, conn: Any) -> None:
        """Permanently drop a POISONED connection (a tx whose COMMIT/ROLLBACK itself raised): close it
        and DECREMENT the opened count so a fresh connection can be opened in its place. Without the
        decrement the pool's ``_opened < _max`` ceiling would count the dead connection forever and
        capacity would shrink by one per discard — eventual exhaustion under repeated commit failures
        (the deeper half of the #78 leak: releasing wasn't enough; the destroy path must re-open a slot).
        """
        try:
            conn.close()
        except Exception:
            pass
        with self._lock:
            if self._opened > 0:
                self._opened -= 1

    def close(self) -> None:
        import queue as _queue

        self._closed = True  # fail-fast: a subsequent acquire raises instead of blocking on a drained pool
        while True:
            try:
                conn = self._free.get_nowait()
            except _queue.Empty:
                break
            try:
                conn.close()
            except Exception:
                pass

# `$1`, `$2`, … (Postgres render output).
_DOLLAR_RE = re.compile(r"\$\d+")
# `INSERT INTO <table> (...) ... RETURNING <cols>` — MySQL RETURNING emulation parse.
_RETURNING_RE = re.compile(r"\s+RETURNING\s+(.+?)\s*$", re.IGNORECASE | re.DOTALL)
_INSERT_TABLE_RE = re.compile(r"^\s*INSERT\s+(?:IGNORE\s+)?INTO\s+([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)
# The INSERT column list `INSERT [IGNORE] INTO <t> (c1, c2, …)` — for extracting client-PK values.
_INSERT_COLS_RE = re.compile(r"^\s*INSERT\s+(?:IGNORE\s+)?INTO\s+[A-Za-z_][A-Za-z0-9_]*\s*\(([^)]*)\)", re.IGNORECASE)
# The strip-before-execute PK hint the mysql bundle appends to an INSERT…RETURNING (tx.ts mysqlPkHint):
#   ` /*scp:pk=col1,col2;ai=<autoIncCol|>*/`
_PK_HINT_RE = re.compile(r"\s*/\*scp:pk=([^;*]*);ai=([^*]*)\*/", re.IGNORECASE)


def _dollar_to_pyformat(sql: str) -> str:
    """Postgres `$N` → DB-API `%s` (positional). Render already numbers left-to-right 1..N, so a
    plain replace preserves order. Literal `%` is doubled so psycopg/pymysql don't treat it as a
    format directive (the rendered SQL never contains a literal `%`, but this keeps the seam safe).
    """
    return _DOLLAR_RE.sub("%s", sql.replace("%", "%%"))


def _qmark_to_pyformat(sql: str) -> str:
    """MySQL render keeps `?`; PyMySQL binds `%s`. Replace each `?` with `%s` (literal `%` doubled)."""
    return sql.replace("%", "%%").replace("?", "%s")


def _parse_pk_hint(returning_cols: str):
    """Parse the ` /*scp:pk=col1,col2;ai=<col|>*/` PK hint out of the RETURNING-cols text.

    Returns ``(pk_columns, auto_inc_or_None)``. Absent hint → ``([], None)`` (legacy path).
    """
    hm = _PK_HINT_RE.search(returning_cols)
    if hm is None:
        return [], None
    cols = [c.strip() for c in hm.group(1).split(",") if c.strip()]
    ai = hm.group(2).strip()
    return cols, (ai or None)


def _returning_reselect_where(insert_sql, pk_cols, auto_inc, params, last_id, affected):
    """Build the MySQL RETURNING re-select WHERE (SQL body with `?` + its params).

    - AUTO_INCREMENT single-column PK: a range on the identity column covering the ``affected`` rows
      just inserted (v1 `WHERE id >= ? AND id < ?` semantics, generalized to the real column name).
    - Client-supplied PK (UUID / composite, ``auto_inc`` is None): the PK value(s) are among the
      bound INSERT params — extract them by matching each PK column to its position in the INSERT
      column list, and key the re-select `WHERE pk1 = ? AND …` on those inserted values.
    - No hint (legacy): fall back to `id = ?` bound to LAST_INSERT_ID (the pre-fix auto-`id` path).
    """
    if not pk_cols:
        return "id = ?", [last_id]
    if auto_inc is not None and pk_cols == [auto_inc]:
        return f"{auto_inc} >= ? AND {auto_inc} < ?", [last_id, last_id + affected]
    # Client-supplied PK: pull each PK column's inserted value from the bound INSERT params by its
    # column position (single-row client-PK insert; the corpus UUID / composite cases are single-row).
    cm = _INSERT_COLS_RE.match(insert_sql)
    if cm is None:
        raise ValueError(f"scp mysql driver: cannot locate INSERT column list for PK re-select: {insert_sql!r}")
    insert_cols = [c.strip() for c in cm.group(1).split(",")]
    conds = []
    vals = []
    for pk in pk_cols:
        try:
            idx = insert_cols.index(pk)
        except ValueError:
            raise ValueError(f"scp mysql driver: PK column '{pk}' not in INSERT columns {insert_cols}")
        conds.append(f"{pk} = ?")
        vals.append(params[idx])
    return " AND ".join(conds), vals


# ── Per-connection execution primitives (shared by the pooled read/write path + the owned tx) ──
#
# These run one statement on a GIVEN DB-API connection — the SAME row-exec, MySQL-RETURNING-emulation,
# and cell-scalar logic whether the connection is a freshly-acquired pooled one (non-tx read/write) or
# the tx's OWNED connection (Phase A / #78). Factoring them out of the old `_PooledPrepared`/
# `_PooledDriver._writer` pair is what lets the tx path own its connection without a driver-global slot.


def _scalar(v: Any) -> Any:
    """Coerce a driver cell to a canonical bc scalar (int/float/bool/str/None).

    psycopg maps a PG ``uuid`` column to a Python ``uuid.UUID`` and other rich types (Decimal,
    date/datetime) to their own classes. The conformance row encoding — and the cross-language
    reference — are JSON scalars, so a non-native cell is stringified to its canonical text form,
    exactly as SQLite/MySQL return a uuid-as-text or the Rust PG driver falls back to ``String``.
    Native scalars pass through unchanged (bool before int, since ``bool`` is an ``int`` subclass).
    """
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    from decimal import Decimal

    if isinstance(v, Decimal):
        f = float(v)
        return int(f) if f.is_integer() else f
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).decode("utf-8", "replace")
    return str(v)


def _fetch_all(cur) -> List[Dict[str, Any]]:
    cols = [d[0] for d in cur.description] if cur.description is not None else []
    return [{c: _scalar(x) for c, x in zip(cols, r)} for r in cur.fetchall()]


def _conn_all(conn: Any, sql: str, params: Sequence[Any], xform, emulate_returning: bool) -> List[Dict[str, Any]]:
    """Run a SELECT/RETURNING statement on ``conn`` (with MySQL RETURNING emulation when configured).

    MySQL has no RETURNING: strip it, run the INSERT, re-select the inserted rows by the REAL primary
    key. The strip-before-execute PK hint (tx.ts mysqlPkHint) carries the PK columns + the
    AUTO_INCREMENT column so the re-select keys off the actual PK — an AUTO_INCREMENT range for an int
    identity, or the client-supplied PK values (UUID / composite) pulled from the bound INSERT params —
    NOT a hardcoded ``WHERE id = ?`` (which breaks for UUID / composite PKs).
    """
    if emulate_returning:
        m = _RETURNING_RE.search(sql)
        if m is not None:
            returning_cols = _PK_HINT_RE.sub("", m.group(1)).strip()
            pk_cols, auto_inc = _parse_pk_hint(m.group(1))
            write_sql = _PK_HINT_RE.sub("", sql[: m.start()])
            table_m = _INSERT_TABLE_RE.match(write_sql)
            if table_m is None:
                # A non-INSERT RETURNING (UPDATE/DELETE … RETURNING): MySQL has no native RETURNING and
                # the pre-image is gone, so v1 (`mysql.ts`) strips RETURNING, runs the write, and
                # returns NO rows. Byte-faithful: execute the stripped write, [].
                cur = conn.cursor()
                cur.execute(xform(write_sql), tuple(params))
                cur.close()
                return []
            # INSERT … RETURNING: run the INSERT, re-select the inserted rows by the REAL PK.
            table = table_m.group(1)
            cur = conn.cursor()
            cur.execute(xform(write_sql), tuple(params))
            last_id = cur.lastrowid
            affected = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 1
            cur.close()
            where_sql, where_params = _returning_reselect_where(write_sql, pk_cols, auto_inc, list(params), last_id, affected)
            sel = conn.cursor()
            sel.execute(xform(f"SELECT {returning_cols} FROM {table} WHERE {where_sql}"), tuple(where_params))
            rows = _fetch_all(sel)
            sel.close()
            return rows
    cur = conn.cursor()
    cur.execute(xform(sql), tuple(params))
    rows = _fetch_all(cur)
    cur.close()
    return rows


def _conn_run(conn: Any, sql: str, params: Sequence[Any], xform) -> RunInfo:
    """Run a non-returning write on ``conn`` and report the affected summary."""
    cur = conn.cursor()
    cur.execute(xform(sql), tuple(params))
    changes = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
    last = cur.lastrowid if getattr(cur, "lastrowid", None) is not None else 0
    cur.close()
    return RunInfo(changes, last)


class _PooledPrepared:
    """A prepared statement over a POOLED live DB-API driver (psycopg / PyMySQL) — the NON-TX path.

    It checks out a connection from the pool, runs the statement, and returns the connection — so
    concurrent siblings run on DISTINCT connections. The write-tx path no longer rides here: a tx runs
    on its OWN connection via :class:`_PooledTxConnection` (per-execution ownership, §3), NOT through a
    driver-global pinned writer.
    """

    __slots__ = ("_driver", "_sql")

    def __init__(self, driver: "_PooledDriver", sql: str) -> None:
        self._driver = driver
        self._sql = sql

    def all(self, params: Sequence[Any]) -> List[Dict[str, Any]]:
        return self._driver._with_conn(
            lambda conn: _conn_all(conn, self._sql, params, self._driver._xform, self._driver._emulate_returning)
        )

    def run(self, params: Sequence[Any]) -> RunInfo:
        return self._driver._with_conn(lambda conn: _conn_run(conn, self._sql, params, self._driver._xform))


class _PooledTxConnection:
    """The OWNED tx handle over a POOLED live DB-API connection (§3) — the Python analogue of v1
    ``PoolTransaction``. It acquires ONE connection from the pool and HOLDS it for the transaction's
    whole duration: every tx-body statement AND every tx-control statement (BEGIN / COMMIT / ROLLBACK /
    the isolation SET) runs on THIS connection via :meth:`run` — routed THROUGH the exec-context seam by
    the combinator, so a registered middleware observes the runtime tx-control (Phase D / #95, full TS
    parity). This handle no longer issues tx-control SQL itself; it is the connection OWNER (acquire /
    release / discard), not the tx-control issuer.

    **Release ownership**: :meth:`release` (idempotent) is the SOLE releaser, called by the seam
    combinator in a ``finally`` so the pooled connection is returned on EVERY path — including a
    COMMIT/ROLLBACK that itself raises (the leak the old self-in-``commit`` release missed — #78).
    ``destroy`` drops a poisoned connection instead of returning it to the pool.

    Concurrent transactions each hold a DISTINCT ``_PooledTxConnection`` over a DISTINCT pooled
    connection, so their writes never cross-talk — the isolation the removed driver-global ``_writer``
    slot violated.
    """

    __slots__ = ("_pool", "_xform", "_emulate_returning", "_conn", "_released")

    def __init__(
        self,
        pool: "_ConnectionPool",
        xform,
        emulate_returning: bool,
    ) -> None:
        self._pool = pool
        self._xform = xform
        self._emulate_returning = emulate_returning
        # Acquire + OWN one connection. tx-control (isolation SET / BEGIN / COMMIT / ROLLBACK) is issued
        # by the combinator THROUGH the seam on THIS pinned connection (Phase D / #95) — NOT here — so a
        # prelude/BEGIN failure is handled by the combinator's discard-on-poison finally (destroy=True),
        # exactly like a body-statement failure. `pool.acquire()` either returns an owned conn or raises
        # before ownership (nothing to discard).
        self._conn = pool.acquire()
        self._released = False

    def all(self, sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
        return _conn_all(self._conn, sql, params, self._xform, self._emulate_returning)

    def run(self, sql: str, params: Sequence[Any]) -> RunInfo:
        # Serves BOTH tx-body writes AND tx-control (BEGIN/COMMIT/ROLLBACK/SET). tx-control carries no
        # params, so `xform` is a no-op on it. A failure propagates so the combinator releases with
        # destroy=True (a raised COMMIT/BEGIN leaves the connection in an unknown state — it must not
        # re-enter the pool).
        return _conn_run(self._conn, sql, params, self._xform)

    def release(self, destroy: bool) -> None:
        if self._released:
            return  # idempotent — the combinator's finally is the single releaser, but guard anyway
        self._released = True
        if destroy:
            # A poisoned connection: DISCARD it (close + free a pool slot for a fresh one). Never
            # return it to the pool, and never leave the pool's opened-count stuck at the ceiling.
            self._pool.discard(self._conn)
        else:
            self._pool.release(self._conn)


class _PooledDriver:
    """Shared pooled live-driver base (Postgres / MySQL) — the parallel-read + per-execution-owned-tx
    seam (Phase A / #78). NO driver-global tx slot: a transaction owns its connection via
    :class:`_PooledTxConnection` (acquired by :meth:`begin_tx`), so concurrent transactions are
    isolated."""

    __slots__ = ("_pool", "_xform", "_emulate_returning")

    def __init__(self, pool: _ConnectionPool, xform, emulate_returning: bool) -> None:
        self._pool = pool
        self._xform = xform
        self._emulate_returning = emulate_returning

    def _with_conn(self, op):
        """Run ``op(conn)`` on a freshly checked-out pooled connection (the non-tx read/write path)."""
        conn = self._pool.acquire()
        try:
            return op(conn)
        finally:
            self._pool.release(conn)

    def exec_ddl(self, statements: Sequence[str]) -> None:
        conn = self._pool.acquire()
        try:
            cur = conn.cursor()
            for stmt in statements:
                cur.execute(stmt)
            cur.close()
        finally:
            self._pool.release(conn)

    def prepare(self, sql: str) -> _PooledPrepared:
        return _PooledPrepared(self, sql)

    def begin_tx(self) -> _PooledTxConnection:
        """Acquire + OWN one :class:`_PooledTxConnection` for a transaction (§3): ONE pooled connection,
        held for the tx's whole duration. Concurrent ``begin_tx`` calls (distinct threads) acquire
        DISTINCT connections ⇒ isolated — the concurrent-tx fix. tx-control (the isolation SET / BEGIN /
        COMMIT / ROLLBACK) is issued THROUGH the seam by the combinator on this pinned connection
        (Phase D / #95, middleware-visible), NOT here — so this method just acquires the owned conn."""
        return _PooledTxConnection(self._pool, self._xform, self._emulate_returning)

    def close(self) -> None:
        self._pool.close()


class PostgresDriver(_PooledDriver):
    """A live Postgres driver (psycopg 3, POOLED) implementing the :class:`Driver` seam.

    Renders a `postgres`-tagged bundle → `$N`; rewrites `$N`→`%s` for psycopg. A bounded pool of
    autocommit connections lets independent sibling relations run concurrently on distinct
    connections; the write-tx pins one connection for its BEGIN…COMMIT span.
    """

    @classmethod
    def connect(
        cls,
        *,
        host: str,
        port: int,
        user: str,
        password: str,
        dbname: str,
        pool_size: int = DEFAULT_POOL_SIZE,
    ) -> "PostgresDriver":
        import psycopg  # imported lazily so the SQLite conformance never needs the driver installed

        def factory():
            return psycopg.connect(
                host=host, port=port, user=user, password=password, dbname=dbname, autocommit=True
            )

        pool = _ConnectionPool(factory, pool_size)
        return cls(pool, _dollar_to_pyformat, emulate_returning=False)


class MysqlDriver(_PooledDriver):
    """A live MySQL driver (PyMySQL, POOLED) implementing the :class:`Driver` seam.

    Renders a `mysql`-tagged bundle → `?`; rewrites `?`→`%s` for PyMySQL. MySQL 8.0 has NO
    `RETURNING`, so an INSERT…RETURNING is emulated at this seam (strip → INSERT → re-select the
    AUTO_INCREMENT PK's columns) — the WS6 TS ScpDialect behavior-by-convention. A bounded pool of
    autocommit connections gives concurrent siblings distinct connections; the write-tx pins one.
    """

    @classmethod
    def connect(
        cls,
        *,
        host: str,
        port: int,
        user: str,
        password: str,
        dbname: str,
        pool_size: int = DEFAULT_POOL_SIZE,
    ) -> "MysqlDriver":
        import pymysql  # lazy import (conformance bar never needs it)

        def factory():
            return pymysql.connect(
                host=host, port=port, user=user, password=password, database=dbname, autocommit=True
            )

        pool = _ConnectionPool(factory, pool_size)
        return cls(pool, _qmark_to_pyformat, emulate_returning=True)
