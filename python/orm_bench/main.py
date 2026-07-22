"""NATIVE-codegen ORM-bench cell (python leg, epic #123) — self-measure the covered ORM read ops
through the litedbmodel-GENERATED ir-exec module (``behaviors_generated.py``, verbatim ``bc generate
--lang python``) + ``litedbmodel_runtime``'s op-agnostic leaf transport, and print a flat CSV
(``cell,dialect,op,iter,us``) the TS collector aggregates.

This cell is a litedbmodel-CONSUMER: it binds the leaf transport (``make_handlers`` →
``executeSQL``/``pluck``/``group``) into the generated module's ``bind(handlers)`` (boundary injection
— the python literal/ir-exec path, epic #123: ts/go/rust = native de-box; py/php = literal) and calls
the resulting per-op callables. It holds NO hand-written exec seam — every DB access runs through the
runtime's central execute/run seam. Relations are N+1-free: ``parents → pluck → executeSQL(WHERE fk IN
…) → group`` runs 1 batched child query per level (nestedFindAll=2, nestedRelations=3, composite=3).

SLICE 1 covers the READ/relation ops. The single-write / batch-write / RETURNING-chained transaction
ops are slice 2 (the write-path leaf + the runtime tx boundary — see the slice-2 gap report).

Usage: ``python -m orm_bench.main <dialect> <spec> [reps] [warmup]`` or
``python -m orm_bench.main safety <dialect> <spec>``.
"""

from __future__ import annotations

import sqlite3
import sys
import time
from typing import Any, Callable, Dict, List

from litedbmodel_runtime import SqliteDriver, make_handlers

from .behaviors_generated import bind

# ── generated schema (the committed native fixture — identical to rust generated_setup.rs / the go
#    twin) + the canonical nested seed (identical to the rust bench cell seed). This is FIXTURE setup,
#    not covered code: the harness measures the GENERATED op callables, it does not hand-write them. ──
SCHEMA: List[str] = [
    "CREATE TABLE benchmark_users (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        email TEXT NOT NULL UNIQUE,\n        name TEXT,\n        created_at TEXT DEFAULT (datetime('now')),\n        updated_at TEXT DEFAULT (datetime('now'))\n      )",
    "CREATE TABLE benchmark_posts (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        title TEXT NOT NULL,\n        content TEXT,\n        published INTEGER DEFAULT 0,\n        author_id INTEGER,\n        created_at TEXT DEFAULT (datetime('now'))\n      )",
    "CREATE TABLE benchmark_comments (\n        id INTEGER PRIMARY KEY AUTOINCREMENT,\n        body TEXT NOT NULL,\n        post_id INTEGER,\n        created_at TEXT DEFAULT (datetime('now'))\n      )",
    "CREATE TABLE benchmark_tenant_users (\n        tenant_id INTEGER NOT NULL,\n        user_id INTEGER NOT NULL,\n        name TEXT,\n        PRIMARY KEY (tenant_id, user_id)\n      )",
    "CREATE TABLE benchmark_tenant_posts (\n        tenant_id INTEGER NOT NULL,\n        post_id INTEGER NOT NULL,\n        user_id INTEGER NOT NULL,\n        title TEXT NOT NULL,\n        PRIMARY KEY (tenant_id, post_id)\n      )",
    "CREATE TABLE benchmark_tenant_comments (\n        tenant_id INTEGER NOT NULL,\n        comment_id INTEGER NOT NULL,\n        post_id INTEGER NOT NULL,\n        body TEXT NOT NULL,\n        PRIMARY KEY (tenant_id, comment_id)\n      )",
]

SEED: List[str] = [
    "DELETE FROM benchmark_comments",
    "DELETE FROM benchmark_posts",
    "DELETE FROM benchmark_users",
    "DELETE FROM benchmark_tenant_comments",
    "DELETE FROM benchmark_tenant_posts",
    "DELETE FROM benchmark_tenant_users",
    "INSERT INTO benchmark_users (id, email, name) VALUES "
    "(1,'user1@example.com','User 1'),(2,'user2@example.com','User 2'),"
    "(3,'user3@example.com','User 3'),(4,'user4@example.com','User 4'),(5,'user5@example.com','User 5')",
    "INSERT INTO benchmark_posts (id, title, content, published, author_id) VALUES "
    "(1,'P1','c',1,1),(2,'P2','c',1,1),(3,'P3','c',1,2),(4,'P4','c',1,2),(5,'P5','c',1,3),(6,'P6','c',1,3)",
    "INSERT INTO benchmark_comments (id, body, post_id) VALUES (1,'b',1),(2,'b',1),(3,'b',2),(4,'b',3),(5,'b',5)",
    "INSERT INTO benchmark_tenant_users (tenant_id, user_id, name) VALUES (1,1,'TU1'),(1,2,'TU2'),(1,3,'TU3')",
    "INSERT INTO benchmark_tenant_posts (tenant_id, post_id, user_id, title) VALUES (1,10,1,'TP1'),(1,11,2,'TP2')",
    "INSERT INTO benchmark_tenant_comments (tenant_id, comment_id, post_id, body) VALUES "
    "(1,100,10,'tc'),(1,101,10,'tc'),(1,102,11,'tc')",
]

# The SLICE-1 covered ops (reads + relations). Write/batch/tx ops are slice 2.
READ_OPS: List[str] = [
    "findAll",
    "filterPaginateSort",
    "findFirst",
    "findUnique",
    "nestedFindAll",
    "nestedFindFirst",
    "nestedFindUnique",
    "nestedRelations",
    "compositeRelations",
]

# The batched-relation N+1 proof: 1 parent + 1 batched child per relation level, INDEPENDENT of the row
# count (never 1 + N). compositeRelations is a 3-level composite-key chain (grouped by the full tuple).
RELATION_QUERY_COUNTS: Dict[str, int] = {
    "nestedFindAll": 2,
    "nestedFindFirst": 2,
    "nestedFindUnique": 2,
    "nestedRelations": 3,
    "compositeRelations": 3,
}

# Per-op input scope (the emitter-declared `value` input ports; a read with no input ports gets `{}`).
_INPUTS: Dict[str, Dict[str, Any]] = {
    "filterPaginateSort": {"published": 1},
    "findFirst": {"name": "User%"},
    "findUnique": {"email": "user1@example.com"},
    "nestedFindFirst": {"name": "User%"},
    "nestedFindUnique": {"email": "user1@example.com"},
}


class CountingSqliteDriver(SqliteDriver):
    """A :class:`SqliteDriver` that counts each ``prepare`` (one per statement the runtime issues) — the
    consumer-side N+1 proof, riding the Driver seam (the runtime + generated runner stay unchanged)."""

    __slots__ = ("query_count",)

    def __init__(self, conn: "sqlite3.Connection") -> None:
        super().__init__(conn)
        self.query_count = 0

    def prepare(self, sql: str):
        self.query_count += 1
        return super().prepare(sql)


def open_driver(spec: str, counting: bool = False) -> SqliteDriver:
    """An in-memory sqlite DB seeded from the generated schema (the sqlite pilot; ``spec`` reserved for
    the live pg/mysql legs — slice 2)."""
    _ = spec
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    for stmt in SCHEMA:
        conn.execute(stmt)
    conn.commit()
    return CountingSqliteDriver(conn) if counting else SqliteDriver(conn)


def seed(driver: SqliteDriver) -> None:
    """DELETE + INSERT the canonical nested fixture so reads see a stable seed (runs on the driver
    directly — no leaf/ambient needed)."""
    for stmt in SEED:
        driver.prepare(stmt).run([])


def bound_ops(driver: SqliteDriver, dialect: str) -> Dict[str, Callable[..., Any]]:
    """Bind the op-agnostic leaf transport into the generated module — the per-op read callables."""
    return bind(make_handlers(driver, dialect))


def run_op(fns: Dict[str, Callable[..., Any]], op: str) -> Any:
    """Run ONE covered read op through its generated callable with its declared input scope."""
    return fns[op](_INPUTS.get(op, {}))


def _measure(dialect: str, spec: str, reps: int, warmup: int) -> None:
    driver = open_driver(spec)
    seed(driver)
    fns = bound_ops(driver, dialect)
    print("cell,dialect,op,iter,us")
    for op in READ_OPS:
        for _ in range(warmup):
            run_op(fns, op)
        for it in range(reps):
            t = time.perf_counter_ns()
            run_op(fns, op)
            us = (time.perf_counter_ns() - t) // 1000
            print(f"native,{dialect},{op},{it},{us}")


def _safety(dialect: str, spec: str) -> None:
    """N+1-avoidance proof: each relation op runs on its fixed batched query count (1 parent + 1 batched
    child per level), INDEPENDENT of the row count."""
    _ = dialect
    driver = open_driver(spec, counting=True)
    seed(driver)
    fns = bound_ops(driver, dialect)
    for op, expected in RELATION_QUERY_COUNTS.items():
        driver.query_count = 0
        run_op(fns, op)
        actual = driver.query_count
        assert actual == expected, f"{op} query-count regression: got {actual}, expect {expected}"
        print(f"{op} queries={actual} (expect {expected})")


def main(argv: List[str]) -> None:
    if len(argv) >= 2 and argv[0] == "safety":
        _safety(argv[1], argv[2] if len(argv) > 2 else "sqlite")
        return
    dialect = argv[0] if argv else "sqlite"
    spec = argv[1] if len(argv) > 1 else "sqlite"
    reps = int(argv[2]) if len(argv) > 2 else 300
    warmup = int(argv[3]) if len(argv) > 3 else 30
    _measure(dialect, spec, reps, warmup)


if __name__ == "__main__":
    main(sys.argv[1:])
