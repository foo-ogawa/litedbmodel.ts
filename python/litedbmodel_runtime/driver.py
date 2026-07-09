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
