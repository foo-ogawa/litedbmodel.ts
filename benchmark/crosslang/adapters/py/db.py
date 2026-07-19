"""Raw DB-API driver seam for the python SDK baseline (hand-SQL) — the fair 1.0x denominator.

The python twin of the go SDK's raw `database/sql` use: connect the STDLIB driver for the dialect
(sqlite3 / psycopg / pymysql — no litedbmodel runtime), run hand-written SQL, and hand back dict rows
whose native cell values feed canon.canon_val directly (int / str / bool / datetime / None). Dialect
is detected from the one target string verify-cells passes (a sqlite file path, a libpq `key=val`
conninfo for postgres, or a `mysql://` URL for mysql), shared with the rust/go cells.
"""

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple


def dialect_of(target: str) -> str:
    if target.startswith("mysql://"):
        return "mysql"
    if "dbname=" in target or ("host=" in target and "port=" in target):
        return "postgres"
    return "sqlite"


def _parse_mysql_url(url: str) -> Dict[str, Any]:
    # mysql://user:pass@host:port/db
    rest = url[len("mysql://"):]
    cred, hostpart = rest.split("@", 1)
    user, password = cred.split(":", 1)
    hostport, db = hostpart.split("/", 1)
    host, port = hostport.split(":", 1)
    return dict(host=host, port=int(port), user=user, password=password, database=db, autocommit=True)


class RawDB:
    """A single-connection raw driver over the dialect's stdlib DB-API module."""

    def __init__(self, target: str) -> None:
        self.dialect = dialect_of(target)
        if self.dialect == "postgres":
            import psycopg

            self.conn = psycopg.connect(target, autocommit=True)
        elif self.dialect == "mysql":
            import pymysql

            self.conn = pymysql.connect(**_parse_mysql_url(target))
        else:
            import sqlite3

            self.conn = sqlite3.connect(target)

    def _bind(self, sql: str) -> str:
        """Rewrite the hand-SQL's `?` placeholders to the driver paramstyle: psycopg + PyMySQL both
        bind positional `%s`; sqlite3 keeps `?`. A python list bound to `= ANY(%s)` is adapted to a
        PG array by psycopg (the array-bind rule; mysql/sqlite use an `IN (?, …)` list instead)."""
        if self.dialect in ("postgres", "mysql"):
            return sql.replace("?", "%s")
        return sql

    def query(self, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(self._bind(sql), tuple(params))
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows

    def execute(self, sql: str, params: Sequence[Any] = ()) -> Tuple[int, int]:
        cur = self.conn.cursor()
        cur.execute(self._bind(sql), tuple(params))
        changes = cur.rowcount if cur.rowcount is not None else 0
        last = cur.lastrowid if getattr(cur, "lastrowid", None) is not None else 0
        cur.close()
        return changes, last

    def begin(self) -> None:
        # psycopg/pymysql run autocommit; issue an explicit BEGIN for the tx envelope. sqlite3
        # autocommits per statement unless in a transaction — a literal BEGIN opens one.
        self.conn.cursor().execute("BEGIN")

    def commit(self) -> None:
        self.conn.cursor().execute("COMMIT")

    def rollback(self) -> None:
        self.conn.cursor().execute("ROLLBACK")

    def close(self) -> None:
        self.conn.close()
