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
from typing import Any, Dict, List, Optional, Protocol, Sequence


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


class Driver(Protocol):
    """The synchronous SQL-driver seam (mirrors the TS `SqliteDb`)."""

    def prepare(self, sql: str) -> PreparedStatement: ...


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

    def close(self) -> None:
        self.conn.close()


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
# WRITE-TX STAYS SERIAL: the runtime issues `prepare("BEGIN"|"COMMIT"|"ROLLBACK").run([])`. On
# `BEGIN` the driver PINS one pooled connection into a single writer slot and routes every
# subsequent statement to it until `COMMIT`/`ROLLBACK` releases it — one connection, tx-DAG order,
# gate-first short-circuit. Reads (no active tx) each check out + return a pooled connection. The
# connections run with autocommit ON so the literal BEGIN…COMMIT bracket a REAL transaction.

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

    __slots__ = ("_factory", "_max", "_free", "_opened", "_lock")

    def __init__(self, factory, max_size: int) -> None:
        import queue as _queue
        import threading as _threading

        self._factory = factory
        self._max = max_size
        self._free: "Any" = _queue.LifoQueue()
        self._opened = 0
        self._lock = _threading.Lock()

    def acquire(self) -> Any:
        import queue as _queue

        # Fast path: reuse a free connection.
        try:
            return self._free.get_nowait()
        except _queue.Empty:
            pass
        # Open a new one if below the ceiling; else wait for a release.
        with self._lock:
            if self._opened < self._max:
                self._opened += 1
                return self._factory()
        return self._free.get()  # block until a connection is released

    def release(self, conn: Any) -> None:
        self._free.put(conn)

    def close(self) -> None:
        import queue as _queue

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


_TXN_CONTROL = frozenset({"BEGIN", "COMMIT", "ROLLBACK", "START TRANSACTION"})


def _is_txn_control(sql: str) -> bool:
    return sql.strip().upper() in _TXN_CONTROL


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


class _PooledPrepared:
    """A prepared statement over a POOLED live DB-API driver (psycopg / PyMySQL).

    For a read (no active tx) it checks out a connection from the pool, runs the statement, and
    returns the connection — so concurrent siblings run on DISTINCT connections. Inside a write-tx
    it runs on the driver's PINNED writer connection (set on BEGIN, released on COMMIT/ROLLBACK).
    ``paramstyle_xform`` adapts the rendered placeholder text; ``emulate_returning`` toggles the
    MySQL RETURNING emulation.
    """

    __slots__ = ("_driver", "_sql", "_params")

    def __init__(self, driver: "_PooledDriver", sql: str) -> None:
        self._driver = driver
        self._sql = sql
        self._params: Sequence[Any] = ()

    @staticmethod
    def _scalar(v: Any) -> Any:
        """Coerce a driver cell to a canonical bc scalar (int/float/bool/str/None).

        psycopg maps a PG ``uuid`` column to a Python ``uuid.UUID`` and other rich types
        (Decimal, date/datetime) to their own classes. The conformance row encoding — and the
        cross-language reference — are JSON scalars, so a non-native cell is stringified to its
        canonical text form, exactly as SQLite/MySQL return a uuid-as-text or the Rust PG driver
        falls back to ``String``. Native scalars pass through unchanged (bool before int, since
        ``bool`` is an ``int`` subclass).
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

    @classmethod
    def _fetch_all(cls, cur) -> List[Dict[str, Any]]:
        cols = [d[0] for d in cur.description] if cur.description is not None else []
        return [{c: cls._scalar(x) for c, x in zip(cols, r)} for r in cur.fetchall()]

    def _run_all(self, conn: Any) -> List[Dict[str, Any]]:
        xform = self._driver._xform
        # MySQL has no RETURNING: strip it, run the INSERT, re-select the inserted rows by the REAL
        # primary key. The strip-before-execute PK hint (tx.ts mysqlPkHint) carries the PK columns +
        # the AUTO_INCREMENT column so the re-select keys off the actual PK — an AUTO_INCREMENT range
        # for an int identity, or the client-supplied PK values (UUID / composite) pulled from the
        # bound INSERT params — NOT a hardcoded `WHERE id = ?` (which breaks for UUID / composite PKs).
        if self._driver._emulate_returning:
            m = _RETURNING_RE.search(self._sql)
            if m is not None:
                returning_cols = m.group(1)
                pk_cols, auto_inc = _parse_pk_hint(returning_cols)
                # Strip the hint from the returning-cols text (it is NOT a real column list).
                returning_cols = _PK_HINT_RE.sub("", returning_cols).strip()
                insert_sql = _PK_HINT_RE.sub("", self._sql[: m.start()])
                table_m = _INSERT_TABLE_RE.match(insert_sql)
                if table_m is None:
                    raise ValueError(
                        f"scp mysql driver: cannot emulate RETURNING for non-INSERT statement: {self._sql!r}"
                    )
                table = table_m.group(1)
                cur = conn.cursor()
                cur.execute(xform(insert_sql), tuple(self._params))
                last_id = cur.lastrowid
                affected = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 1
                cur.close()
                where_sql, where_params = _returning_reselect_where(
                    insert_sql, pk_cols, auto_inc, list(self._params), last_id, affected
                )
                sel = conn.cursor()
                sel.execute(xform(f"SELECT {returning_cols} FROM {table} WHERE {where_sql}"), tuple(where_params))
                rows = self._fetch_all(sel)
                sel.close()
                return rows
        cur = conn.cursor()
        cur.execute(xform(self._sql), tuple(self._params))
        rows = self._fetch_all(cur)
        cur.close()
        return rows

    def all(self, params: Sequence[Any]) -> List[Dict[str, Any]]:
        self._params = params
        return self._driver._with_conn(self._run_all)

    def run(self, params: Sequence[Any]) -> RunInfo:
        # Transaction-control literals pin / release the single writer connection.
        if _is_txn_control(self._sql):
            self._driver._handle_txn_control(self._sql)
            return RunInfo(0, 0)

        def op(conn: Any) -> RunInfo:
            cur = conn.cursor()
            cur.execute(self._driver._xform(self._sql), tuple(params))
            changes = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
            last = cur.lastrowid if getattr(cur, "lastrowid", None) is not None else 0
            cur.close()
            return RunInfo(changes, last)

        return self._driver._with_conn(op)


class _PooledDriver:
    """Shared pooled live-driver base (Postgres / MySQL) — the parallel-read + serial-write seam."""

    __slots__ = ("_pool", "_xform", "_emulate_returning", "_writer")

    def __init__(self, pool: _ConnectionPool, xform, emulate_returning: bool) -> None:
        self._pool = pool
        self._xform = xform
        self._emulate_returning = emulate_returning
        self._writer: Any = None  # pinned connection for the active write-tx (single-threaded)

    def _with_conn(self, op):
        """Run ``op(conn)`` on the pinned writer (in a tx) or a freshly checked-out pooled conn."""
        if self._writer is not None:
            return op(self._writer)
        conn = self._pool.acquire()
        try:
            return op(conn)
        finally:
            self._pool.release(conn)

    def _handle_txn_control(self, sql: str) -> None:
        upper = sql.strip().upper()
        if upper in ("BEGIN", "START TRANSACTION"):
            conn = self._pool.acquire()
            cur = conn.cursor()
            cur.execute("BEGIN")
            cur.close()
            self._writer = conn
        else:  # COMMIT / ROLLBACK: run on the pinned writer, then return it to the pool.
            conn = self._writer
            self._writer = None
            if conn is not None:
                cur = conn.cursor()
                cur.execute(upper)
                cur.close()
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

    def close(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:
                pass
            self._writer = None
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
