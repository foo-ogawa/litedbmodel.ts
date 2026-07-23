"""Raw-driver SDK-baseline ORM-bench cell (python leg) — the apples-to-apples twin of
``orm_bench.main`` (the native-codegen cell).

It runs the SAME 19 ORM ops over the SAME canonical fixture and the SAME in-memory sqlite storage the
native cell uses (``sqlite3.connect(":memory:")`` ↔ the native's ``SqliteDriver.in_memory``), but every
op is HAND-WRITTEN SQL issued straight at the ``sqlite3`` driver connection. ``litedbmodel_runtime`` and
the bc-generated ``behaviors_generated`` module are NOT imported and NOT in the path.

Fairness (a strawman SDK invalidates the comparison):
  - SAME storage: in-memory sqlite (no file → no fsync/WAL the native in-memory cell never pays).
  - Prepared-statement REUSE: ``sqlite3`` caches the compiled statement by SQL text at the connection
    level (``cached_statements``), so re-issuing the same op's SQL reuses the prepared statement across
    iterations — matching the native runtime's prepared-statement cache, not a re-parse-per-call strawman.
  - N+1-FREE relations: parent read → pluck keys → ONE batched child read (WHERE fk IN (…)) → group in
    memory, the SAME query counts the native cell proves (nestedFindAll=2, nestedRelations=3,
    compositeRelations=3, batch write=1, RETURNING-chained tx = BEGIN + body + COMMIT).
  - SAME seed + inputs as the native twin: the small canonical nested fixture (mirrored from
    ``orm_bench.main`` — the fixture each isolated cell carries), re-seeded before each op, and the SAME
    per-op inputs (findUnique=user1, update id=1, …).

Usage: ``python -m orm_bench_sdk.main <dialect> <spec> [reps] [warmup]`` or
``python -m orm_bench_sdk.main safety <dialect> <spec>``.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from typing import Any, List

# The shared seed-SSoT loader lives at the python/ root (one dir above this package) — anchor its import
# to this file so it resolves regardless of cwd.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import lm_bench_setup  # noqa: E402

# ── schema + seed from the ONE seed SSoT (benchmark/crosslang/.setup/sqlite.json, emitted from
#    orm-domain.ts) — the SAME fixture the native twin loads. Shared TEST DATA, not covered code. ──
_SETUP = lm_bench_setup.load("sqlite")
SCHEMA: List[str] = _SETUP["schema"]  # drop + create, applied once at open
SEED: List[str] = _SETUP["delete"] + _SETUP["insert"]  # empty + the canonical 110-user fixture, per op

OPS: List[str] = [
    "findAll", "filterPaginateSort", "findFirst", "findUnique",
    "nestedFindAll", "nestedFindFirst", "nestedFindUnique", "nestedRelations", "compositeRelations",
    "create", "update", "upsert", "createMany", "upsertMany", "updateMany",
    "nestedCreate", "nestedUpsert", "nestedUpdate", "delete",
]

# ── the ONE exec seam. All DB access rides these methods, so the statement counter (safety proof) lives
#    in one place. Prepared-statement reuse is the sqlite3 connection's own statement cache. ──────────
class Db:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.count = 0

    def query(self, sql: str, params: tuple = ()) -> List[tuple]:
        self.count += 1
        cur = self.conn.execute(sql, params)  # sqlite3 reuses the cached compiled statement by SQL text
        return cur.fetchall()

    def exec(self, sql: str, params: tuple = ()) -> None:
        self.count += 1
        self.conn.execute(sql, params)

    def exec_script(self, sql: str) -> None:
        # param-free control statement (BEGIN / COMMIT)
        self.count += 1
        self.conn.execute(sql)


def open_db(spec: str) -> Db:
    _ = spec  # sqlite pilot: an IN-MEMORY DB — SAME storage as the native cell.
    # isolation_level=None → autocommit, so the explicit BEGIN/COMMIT below bracket exactly one tx (the
    # native cell's SqliteDriver is autocommit for the same reason).
    conn = sqlite3.connect(":memory:", isolation_level=None, cached_statements=64)
    for stmt in SCHEMA:
        conn.execute(stmt)
    return Db(conn)


def seed(db: Db) -> None:
    for stmt in SEED:
        db.conn.execute(stmt)  # runs on the connection directly (off-seam) → never counted


# ── batch-write inputs (mirror ops.ts / the native cell) ───────────────────────────────────────────
def batch_rows(it: int, stable: bool) -> tuple:
    emails = [(f"many{i}@bench.com" if stable else f"many{it}_{i}@bench.com") for i in range(10)]
    names = [f"Many {i}" for i in range(10)]
    return emails, names


def _placeholders(n: int) -> str:
    return ",".join(["?"] * n)


def _tuple_in(rows: int, cols: int) -> str:
    one = "(" + _placeholders(cols) + ")"
    return "(VALUES " + ",".join([one] * rows) + ")"


def _group_by(rows: List[tuple], key_col: int) -> None:
    m: dict = {}
    for idx, r in enumerate(rows):
        m.setdefault(r[key_col], []).append(idx)


# ── read helpers: ONE batched child query per level, grouped in memory (N+1-free). ─────────────────
def _nested_posts_for(db: Db, users: List[tuple]) -> None:
    ids = [r[0] for r in users]
    if not ids:
        return
    sql = ("SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN (%s) ORDER BY id ASC"
           % _placeholders(len(ids)))
    posts = db.query(sql, tuple(ids))
    _group_by(posts, 2)


def _nested_posts_collect_ids(db: Db, users: List[tuple]) -> List[Any]:
    ids = [r[0] for r in users]
    if not ids:
        return []
    sql = ("SELECT id, title, author_id FROM benchmark_posts WHERE author_id IN (%s) ORDER BY id ASC"
           % _placeholders(len(ids)))
    posts = db.query(sql, tuple(ids))
    _group_by(posts, 2)
    return [r[0] for r in posts]


def _batched_comments(db: Db, post_ids: List[Any]) -> None:
    if not post_ids:
        return
    sql = ("SELECT id, body, post_id FROM benchmark_comments WHERE post_id IN (%s) ORDER BY id ASC"
           % _placeholders(len(post_ids)))
    comments = db.query(sql, tuple(post_ids))
    _group_by(comments, 2)


def _composite_relations(db: Db) -> None:
    tusers = db.query(
        "SELECT tenant_id, user_id, name FROM benchmark_tenant_users WHERE tenant_id = ? ORDER BY user_id ASC",
        (1,))
    if not tusers:
        return
    pbody = _tuple_in(len(tusers), 2)
    psql = ("SELECT tenant_id, post_id, user_id, title FROM benchmark_tenant_posts "
            "WHERE (tenant_id, user_id) IN " + pbody)
    pparams: list = []
    for r in tusers:
        pparams += [r[0], r[1]]
    tposts = db.query(psql, tuple(pparams))
    if not tposts:
        return
    cbody = _tuple_in(len(tposts), 2)
    csql = ("SELECT tenant_id, comment_id, post_id, body FROM benchmark_tenant_comments "
            "WHERE (tenant_id, post_id) IN " + cbody)
    cparams: list = []
    for r in tposts:
        cparams += [r[0], r[1]]
    db.query(csql, tuple(cparams))


def _update_many(db: Db) -> None:
    _, names = batch_rows(0, False)
    whens = ""
    params: list = []
    for k in range(10):
        whens += " WHEN ? THEN ?"
        params += [k + 1, names[k]]
    params += [k + 1 for k in range(10)]
    sql = "UPDATE benchmark_users SET name = CASE id%s END WHERE id IN (%s)" % (whens, _placeholders(10))
    db.exec(sql, tuple(params))


def _batch_insert(db: Db, emails: List[str], names: List[str], conflict: str) -> None:
    tuples = ",".join(["(?, ?)"] * 10)
    params: list = []
    for k in range(10):
        params += [emails[k], names[k]]
    db.exec("INSERT INTO benchmark_users (email, name) VALUES " + tuples + conflict, tuple(params))


# ── the 19 ops (native-cell order). Fixed inputs mirror the python native cell; mutating ops vary their
#    UNIQUE column by it. Read LIMIT/ORDER shapes match the ops SSoT (== the native generated SQL). ────
def run_op(db: Db, op: str, it: int) -> None:
    if op == "findAll":
        db.query("SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100")
    elif op == "filterPaginateSort":
        db.query("SELECT id, title, content, published, author_id, created_at FROM benchmark_posts "
                 "WHERE published = ? ORDER BY created_at DESC LIMIT 20 OFFSET 10", (1,))
    elif op == "findFirst":
        db.query("SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1", ("User%",))
    elif op == "findUnique":
        db.query("SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1", ("user1@example.com",))
    elif op == "nestedFindAll":
        _nested_posts_for(db, db.query("SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100"))
    elif op == "nestedFindFirst":
        _nested_posts_for(db, db.query("SELECT id, email, name FROM benchmark_users WHERE name LIKE ? LIMIT 1", ("User%",)))
    elif op == "nestedFindUnique":
        _nested_posts_for(db, db.query("SELECT id, email, name FROM benchmark_users WHERE email = ? LIMIT 1", ("user1@example.com",)))
    elif op == "nestedRelations":
        users = db.query("SELECT id, email, name FROM benchmark_users ORDER BY id ASC LIMIT 100")
        _batched_comments(db, _nested_posts_collect_ids(db, users))
    elif op == "compositeRelations":
        _composite_relations(db)
    elif op == "create":
        db.exec("INSERT INTO benchmark_users (email, name) VALUES (?, ?)", (f"new{it}@bench.com", "New"))
    elif op == "update":
        db.exec("UPDATE benchmark_users SET name = ? WHERE id = ?", ("Updated 1", 1))
    elif op == "upsert":
        db.exec("INSERT INTO benchmark_users (email, name) VALUES (?, ?) "
                "ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name",
                ("user1@example.com", "Upserted One"))
    elif op == "createMany":
        emails, names = batch_rows(it, False)
        _batch_insert(db, emails, names, "")
    elif op == "upsertMany":
        emails = ["user1@example.com", "user2@example.com"] + [f"many{k}@bench.com" for k in range(8)]
        _, names = batch_rows(it, True)
        _batch_insert(db, emails, names,
                      " ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name")
    elif op == "updateMany":
        _update_many(db)
    elif op == "nestedCreate":
        db.exec_script("BEGIN")
        cur = db.conn.execute("INSERT INTO benchmark_users (email, name) VALUES (?, ?)", (f"nc{it}@bench.com", "NC"))
        db.count += 1
        db.exec("INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)", (cur.lastrowid, "NC Post"))
        db.exec_script("COMMIT")
    elif op == "nestedUpsert":
        db.exec_script("BEGIN")
        db.exec("INSERT INTO benchmark_users (email, name) VALUES (?, ?) "
                "ON CONFLICT (email) DO UPDATE SET email = excluded.email, name = excluded.name",
                ("user1@example.com", "NUp"))
        rows = db.query("SELECT id FROM benchmark_users WHERE email = ?", ("user1@example.com",))
        db.exec("INSERT INTO benchmark_posts (author_id, title) VALUES (?, ?)", (rows[0][0], "NUp Post"))
        db.exec_script("COMMIT")
    elif op == "nestedUpdate":
        db.exec_script("BEGIN")
        db.exec("UPDATE benchmark_users SET name = ? WHERE id = ?", ("NU", 1))
        db.exec("UPDATE benchmark_posts SET title = ? WHERE author_id = ?", ("NU Post", 1))
        db.exec_script("COMMIT")
    elif op == "delete":
        db.exec_script("BEGIN")
        cur = db.conn.execute("INSERT INTO benchmark_users (email, name) VALUES (?, ?)", (f"del{it}@bench.com", "Del"))
        db.count += 1
        db.exec("DELETE FROM benchmark_users WHERE id = ?", (cur.lastrowid,))
        db.exec_script("COMMIT")
    else:
        raise ValueError(f"unknown op {op!r}")


# ── safety expectations (mirror the native cell) ──────────────────────────────────────────────────
RELATION_QUERY_COUNTS = {"nestedFindAll": 2, "nestedFindFirst": 2, "nestedFindUnique": 2,
                         "nestedRelations": 3, "compositeRelations": 3}
BATCH_QUERY_COUNTS = {"createMany": 1, "upsertMany": 1, "updateMany": 1}
# tx: BEGIN + body + COMMIT. nestedUpsert re-SELECTs the id (upsert has no portable RETURNING) → 5.
TX_STMT_COUNTS = {"nestedCreate": 4, "nestedUpsert": 5, "nestedUpdate": 4, "delete": 4}


def _measure(dialect: str, spec: str, reps: int, warmup: int) -> None:
    db = open_db(spec)
    print("cell,dialect,op,iter,us")
    for op in OPS:
        seed(db)  # re-seed before each op (matches the native cell)
        for it in range(warmup):
            run_op(db, op, it)
        for it in range(reps):
            g = it + warmup
            t = time.perf_counter_ns()
            run_op(db, op, g)
            us = (time.perf_counter_ns() - t) // 1000
            print(f"sdk,{dialect},{op},{it},{us}")


def _safety(dialect: str, spec: str) -> None:
    db = open_db(spec)
    expected = {**RELATION_QUERY_COUNTS, **BATCH_QUERY_COUNTS, **TX_STMT_COUNTS}
    for op, want in expected.items():
        seed(db)
        db.count = 0
        run_op(db, op, 0)
        got = db.count
        assert got == want, f"{op} statement-count regression: got {got}, expect {want}"
        kind = "queries" if op not in TX_STMT_COUNTS else "statements (BEGIN + body + COMMIT)"
        print(f"{op} {kind}={got} (expect {want})")


def main(argv: List[str]) -> None:
    if argv and argv[0] == "safety":
        _safety(argv[1] if len(argv) > 1 else "sqlite", argv[2] if len(argv) > 2 else "sqlite")
        return
    dialect = argv[0] if argv else "sqlite"
    spec = argv[1] if len(argv) > 1 else "sqlite"
    reps = int(argv[2]) if len(argv) > 2 else 300
    warmup = int(argv[3]) if len(argv) > 3 else 30
    _measure(dialect, spec, reps, warmup)


if __name__ == "__main__":
    main(sys.argv[1:])
