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


# ── Live PostgreSQL / MySQL drivers (WS7g, #36) ────────────────────────────────
#
# The SAME synchronous `Driver` seam, now backed by REAL psycopg (Postgres) / PyMySQL (MySQL)
# connections — proving the deferred live-DB execution axis (spec §10 dialect axis). The runtime
# is UNCHANGED: it renders the dialect-tagged bundle (Postgres → `$N`, MySQL → `?`), binds the
# rendered params positionally, and calls `prepare(sql).all(...)` / `.run(...)`. Each live driver
# adapts the rendered placeholder text to its DB's native paramstyle (both DB-API drivers here use
# `%s`), and MySQL emulates the missing `RETURNING` at this seam (strip → execute → re-select the
# inserted PK) — the sanctioned dialect-behavior-by-convention (mirrors the WS6 TS ScpDialect).
#
# The transaction envelope: the runtime issues `prepare("BEGIN"|"COMMIT"|"ROLLBACK").run([])`.
# The live drivers run with autocommit ON so those literal statements control the transaction
# exactly like the SQLite seam's implicit-then-explicit tx — a real BEGIN…COMMIT on the live DB.

# `$1`, `$2`, … (Postgres render output).
_DOLLAR_RE = re.compile(r"\$\d+")
# `INSERT INTO <table> (...) ... RETURNING <cols>` — MySQL RETURNING emulation parse.
_RETURNING_RE = re.compile(r"\s+RETURNING\s+(.+?)\s*$", re.IGNORECASE | re.DOTALL)
_INSERT_TABLE_RE = re.compile(r"^\s*INSERT\s+(?:IGNORE\s+)?INTO\s+([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE)


def _dollar_to_pyformat(sql: str) -> str:
    """Postgres `$N` → DB-API `%s` (positional). Render already numbers left-to-right 1..N, so a
    plain replace preserves order. Literal `%` is doubled so psycopg/pymysql don't treat it as a
    format directive (the rendered SQL never contains a literal `%`, but this keeps the seam safe).
    """
    return _DOLLAR_RE.sub("%s", sql.replace("%", "%%"))


def _qmark_to_pyformat(sql: str) -> str:
    """MySQL render keeps `?`; PyMySQL binds `%s`. Replace each `?` with `%s` (literal `%` doubled)."""
    return sql.replace("%", "%%").replace("?", "%s")


class _LivePrepared:
    """A prepared statement over a live DB-API connection (psycopg / PyMySQL).

    `paramstyle_xform` adapts the rendered placeholder text; `emulate_returning` toggles the MySQL
    RETURNING emulation. Transaction-control literals (BEGIN/COMMIT/ROLLBACK) execute verbatim.
    """

    __slots__ = ("_conn", "_sql", "_xform", "_emulate_returning")

    def __init__(self, conn: Any, sql: str, xform, emulate_returning: bool) -> None:
        self._conn = conn
        self._sql = sql
        self._xform = xform
        self._emulate_returning = emulate_returning

    def _fetch_all(self, cur) -> List[Dict[str, Any]]:
        cols = [d[0] for d in cur.description] if cur.description is not None else []
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def all(self, params: Sequence[Any]) -> List[Dict[str, Any]]:
        # MySQL has no RETURNING: strip it, run the INSERT, re-select the inserted PK's columns.
        if self._emulate_returning:
            m = _RETURNING_RE.search(self._sql)
            if m is not None:
                returning_cols = m.group(1)
                insert_sql = self._sql[: m.start()]
                table_m = _INSERT_TABLE_RE.match(insert_sql)
                if table_m is None:
                    raise ValueError(
                        f"scp mysql driver: cannot emulate RETURNING for non-INSERT statement: {self._sql!r}"
                    )
                table = table_m.group(1)
                cur = self._conn.cursor()
                cur.execute(self._xform(insert_sql), tuple(params))
                last_id = cur.lastrowid
                cur.close()
                sel = self._conn.cursor()
                sel.execute(f"SELECT {returning_cols} FROM {table} WHERE id = %s", (last_id,))
                rows = self._fetch_all(sel)
                sel.close()
                return rows
        cur = self._conn.cursor()
        cur.execute(self._xform(self._sql), tuple(params))
        rows = self._fetch_all(cur)
        cur.close()
        return rows

    def run(self, params: Sequence[Any]) -> RunInfo:
        cur = self._conn.cursor()
        cur.execute(self._xform(self._sql), tuple(params))
        changes = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
        last = cur.lastrowid if getattr(cur, "lastrowid", None) is not None else 0
        cur.close()
        return RunInfo(changes, last)


class PostgresDriver:
    """A live Postgres driver (psycopg 3) implementing the :class:`Driver` seam.

    Renders a `postgres`-tagged bundle → `$N` text; this driver rewrites `$N`→`%s` for psycopg and
    executes REAL SQL over a live connection. Autocommit ON so the runtime's BEGIN/COMMIT/ROLLBACK
    literals control the transaction (a genuine PG transaction for the gate-first write-tx).
    """

    __slots__ = ("conn",)

    def __init__(self, conn: Any) -> None:
        self.conn = conn

    @classmethod
    def connect(cls, *, host: str, port: int, user: str, password: str, dbname: str) -> "PostgresDriver":
        import psycopg  # imported lazily so the SQLite conformance never needs the driver installed

        conn = psycopg.connect(host=host, port=port, user=user, password=password, dbname=dbname, autocommit=True)
        return cls(conn)

    def exec_ddl(self, statements: Sequence[str]) -> None:
        cur = self.conn.cursor()
        for stmt in statements:
            cur.execute(stmt)
        cur.close()

    def prepare(self, sql: str) -> _LivePrepared:
        return _LivePrepared(self.conn, sql, _dollar_to_pyformat, emulate_returning=False)

    def close(self) -> None:
        self.conn.close()


class MysqlDriver:
    """A live MySQL driver (PyMySQL) implementing the :class:`Driver` seam.

    Renders a `mysql`-tagged bundle → `?` text; this driver rewrites `?`→`%s` for PyMySQL. MySQL
    8.0 has NO `RETURNING`, so an INSERT…RETURNING is emulated at this seam (strip → INSERT →
    re-select the AUTO_INCREMENT PK's columns) — the dialect-behavior-by-convention the WS6 TS
    ScpDialect uses. Autocommit ON so the runtime's BEGIN/COMMIT/ROLLBACK literals bracket the tx.
    """

    __slots__ = ("conn",)

    def __init__(self, conn: Any) -> None:
        self.conn = conn

    @classmethod
    def connect(cls, *, host: str, port: int, user: str, password: str, dbname: str) -> "MysqlDriver":
        import pymysql  # lazy import (conformance bar never needs it)

        conn = pymysql.connect(host=host, port=port, user=user, password=password, database=dbname, autocommit=True)
        return cls(conn)

    def exec_ddl(self, statements: Sequence[str]) -> None:
        cur = self.conn.cursor()
        for stmt in statements:
            cur.execute(stmt)
        cur.close()

    def prepare(self, sql: str) -> _LivePrepared:
        return _LivePrepared(self.conn, sql, _qmark_to_pyformat, emulate_returning=True)

    def close(self) -> None:
        self.conn.close()
