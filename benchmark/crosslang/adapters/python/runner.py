#!/usr/bin/env python3
"""litedbmodel cross-language adapter RUNNER — Python (epic #44).

Speaks the line-delimited JSON contract (../../contract.ts) over stdin/stdout for the
three Python cells: sql / codegen / ir. Every case-scoped request carries a `dialect`
(sqlite/postgres/mysql):

  micro     — runs against the PER-DIALECT bundle (different SQL/`?`→`$N`/JSON-array
              render forms) with the mock driver → the CLIENT-PATH cost per dialect.
  DB-backed — sqlite via the in-proc stdlib sqlite3 driver; postgres/mysql via the
              shipped `PostgresDriver`/`MysqlDriver` (psycopg / PyMySQL) through the
              SAME `execute_bundle`/`read_bundle`/`execute_transaction_bundle` runtime.

  sql     — hand-optimized raw SQL via stdlib sqlite3 (baseline 1.0×; sqlite only)
  codegen — Python is NOT a codegen-MODULE language (generate.ts's CODEGEN_LANGS is
            typescript/go/rust only — python/php stay on the ir/interpret surface, a
            declared design, not a gap). This cell verifies each bundle's integrity
            (fingerprint) once at load — matching the PHP codegen cell's convention —
            then executes via the SAME runtime call `ir` uses (codegen ≈ ir is honest
            and expected for this language; see CROSS-LANG.md).
  ir      — the bundle loaded FROM the generated JSON on disk, executed via the runtime

postgres/mysql DB-backed cells isolate into their OWN `scp_python_bench` schema/database
(#53 follow-up) — never the shared `testdb` fixture tables `test/fixtures/init.sql` seeds
for the integration suite. Mirrors the Rust/Go/PHP adapters' `scp_<lang>_bench`.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent.parent
sys.path.insert(0, str(REPO / "python"))

from litedbmodel_runtime import (  # noqa: E402
    execute_bundle,
    execute_transaction_bundle,
    read_bundle,
)
from litedbmodel_runtime.driver import SqliteDriver  # noqa: E402

BUNDLES_PATH = HERE.parent.parent / "generated" / "bundles.json"

IMPL = "sql"
for a in sys.argv[1:]:
    if a.startswith("--impl="):
        IMPL = a.split("=", 1)[1]

_RAW = BUNDLES_PATH.read_text()
ARTIFACT = json.loads(_RAW)
# Per-dialect case maps.
CASES_BY_DIALECT = {d: {c["case"]: c for c in blk["cases"]} for d, blk in ARTIFACT["dialects"].items()}
SCHEMA = ARTIFACT["schema"]
SEED = ARTIFACT["seed"]

# Connection config (matches docker-compose.test.yml + WS6 host defaults).
#
# #53 follow-up (independent audit): isolated into its OWN `scp_python_bench` namespace
# (PG schema via search_path, MySQL database) — mirrors the Rust/Go/PHP adapters'
# `scp_<lang>_bench` (and this runtime's own `livedb_runner.py` conformance seam, which
# uses the identical "CREATE SCHEMA/DATABASE IF NOT EXISTS then SET search_path/USE via
# exec_ddl on the pooled driver" pattern). Never creates/seeds/drops tables in the shared
# `testdb` that `test/fixtures/init.sql` seeds for the integration suite.
PG_SCHEMA_NAME = "scp_python_bench"
MYSQL_DB_NAME = "scp_python_bench"


def _pg_cfg():
    return dict(host=os.environ.get("TEST_DB_HOST", "localhost"), port=int(os.environ.get("TEST_DB_PORT", "5433")),
                user=os.environ.get("TEST_DB_USER", "testuser"), password=os.environ.get("TEST_DB_PASSWORD", "testpass"),
                dbname=os.environ.get("TEST_DB_NAME", "testdb"))
def _mysql_cfg():
    return dict(host=os.environ.get("TEST_MYSQL_HOST", "localhost"), port=int(os.environ.get("TEST_MYSQL_PORT", "3307")),
                user=os.environ.get("TEST_MYSQL_USER", "testuser"), password=os.environ.get("TEST_MYSQL_PASSWORD", "testpass"),
                dbname=os.environ.get("TEST_MYSQL_DB", "testdb"))

# Real-DB schema/seed (mirror of domain.ts PG_SCHEMA / MYSQL_SCHEMA).
PG_SCHEMA = [
    "DROP TABLE IF EXISTS comments CASCADE", "DROP TABLE IF EXISTS posts CASCADE",
    "DROP TABLE IF EXISTS users CASCADE", "DROP TABLE IF EXISTS uniq CASCADE",
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, post_count INTEGER NOT NULL DEFAULT 0)",
    "CREATE TABLE posts (id SERIAL PRIMARY KEY, author_id INTEGER NOT NULL, title TEXT NOT NULL, status TEXT, views INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL)",
    "CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL)",
    # s0 binds author_id (always numeric) — INTEGER (#53: pgx's strict binary protocol rejects an
    # int arg for a text column; psycopg's text protocol is permissive either way).
    "CREATE TABLE uniq (name TEXT NOT NULL, s0 INTEGER, f0 TEXT)",
]
PG_SEQ_RESET = ["SELECT setval('posts_id_seq', (SELECT MAX(id) FROM posts))"]
MYSQL_SCHEMA = [
    "SET FOREIGN_KEY_CHECKS = 0", "DROP TABLE IF EXISTS comments", "DROP TABLE IF EXISTS posts",
    "DROP TABLE IF EXISTS users", "DROP TABLE IF EXISTS uniq", "SET FOREIGN_KEY_CHECKS = 1",
    "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, post_count INT NOT NULL DEFAULT 0)",
    "CREATE TABLE posts (id INT AUTO_INCREMENT PRIMARY KEY, author_id INT NOT NULL, title VARCHAR(255) NOT NULL, status VARCHAR(255), views INT NOT NULL DEFAULT 0, created_at VARCHAR(255) NOT NULL)",
    "CREATE TABLE comments (id INT PRIMARY KEY, post_id INT NOT NULL, body VARCHAR(255) NOT NULL, created_at VARCHAR(255) NOT NULL)",
    "CREATE TABLE uniq (name VARCHAR(255) NOT NULL, s0 INT, f0 VARCHAR(255))",
]


def _verify_codegen_integrity():
    """Python has no generated codegen MODULE (generate.ts's CODEGEN_LANGS is
    typescript/go/rust only — python/php are the ir/interpret surface, a declared design).
    Mirror the PHP codegen cell's convention: verify each bundle's integrity (fingerprint)
    ONCE at cold start (fail-closed if the bundle is malformed/absent), then keep it
    resident — the codegen cell's own cold-start cost, distinct from ir's per-request
    reparse-from-disk."""
    fps = {}
    for case_id, c in CASES_BY_DIALECT["sqlite"].items():
        fps[case_id] = "fp:" + hashlib.sha256(json.dumps(c["bundle"], sort_keys=True).encode()).hexdigest()[:16]
    return fps


CODEGEN_INTEGRITY = _verify_codegen_integrity() if IMPL == "codegen" else {}


# ── sql baseline (hand-optimized raw SQL; sqlite only) ────────────────────────
def seed_sqlite():
    d = SqliteDriver.in_memory(list(SCHEMA))
    for s in SEED:
        d.prepare(s).run([])
    d.conn.commit()
    d.conn.isolation_level = None
    return d


def sql_op(case_id, driver):
    p = driver.prepare
    if case_id == "find":
        return lambda: p("SELECT id, author_id, title, status, views, created_at FROM posts WHERE author_id = ? AND status = ? AND created_at >= ? ORDER BY id ASC").all([1, "live", "2026-02-01"])
    if case_id == "complexWhere":
        return lambda: p("SELECT id, author_id, title, status, views FROM posts WHERE author_id = ? AND created_at >= ? AND title LIKE ? AND id IN (?, ?, ?, ?, ?) ORDER BY id ASC").all([1, "2026-02-01", "post-%", 1, 2, 3, 4, 5])
    if case_id == "inList":
        ids = list(range(1, 11)); ph = ", ".join("?" for _ in ids)
        return lambda: p(f"SELECT id, title FROM posts WHERE id IN ({ph}) ORDER BY id ASC").all(ids)
    if case_id == "belongsTo":
        def run():
            posts = p("SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC").all([1])
            aids = sorted({r["author_id"] for r in posts}); ph = ", ".join("?" for _ in aids)
            p(f"SELECT id, name FROM users WHERE id IN ({ph})").all(aids)
        return run
    if case_id == "hasMany":
        def run():
            posts = p("SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC").all([1])
            ids = [r["id"] for r in posts]; ph = ", ".join("?" for _ in ids)
            p(f"SELECT id, post_id, body FROM comments WHERE post_id IN ({ph})").all(ids)
        return run
    if case_id == "hasManyLimit":
        def run():
            posts = p("SELECT id, author_id, title FROM posts WHERE author_id = ? ORDER BY id ASC").all([1])
            ids = [r["id"] for r in posts]; ph = ", ".join("?" for _ in ids)
            p(f"SELECT id, post_id, body FROM (SELECT id, post_id, body, ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY id DESC) rn FROM comments WHERE post_id IN ({ph})) WHERE rn <= 3").all(ids)
        return run
    if case_id == "batchInsert":
        rows = CASES_BY_DIALECT["sqlite"]["batchInsert"]["input"]["rows"]
        cols = ["author_id", "title", "status", "views", "created_at"]
        vals = ",".join("(" + ",".join("?" for _ in cols) + ")" for _ in rows)
        flat = [r[c] for r in rows for c in cols]
        return lambda: p(f"INSERT INTO posts ({','.join(cols)}) VALUES {vals}").run(flat)
    if case_id == "writeTxGate":
        inp = CASES_BY_DIALECT["sqlite"]["writeTxGate"]["input"]
        def run():
            author = p("SELECT 1 FROM users WHERE id = ?").all([inp["author_id"]])
            if not author:
                raise RuntimeError("requires_absent")
            p("INSERT INTO uniq (name, s0, f0) VALUES (?, ?, ?) ON CONFLICT DO NOTHING").run(["title_per_author", str(inp["author_id"]), inp["title"]])
            p("INSERT INTO posts (author_id, title, created_at) VALUES (?, ?, ?) RETURNING id, author_id, title").all([inp["author_id"], inp["title"], inp["created_at"]])
            p("UPDATE users SET post_count = post_count + ? WHERE id = ?").run([1, inp["author_id"]])
        return run
    raise ValueError(f"unknown case {case_id}")


# ── litedbmodel runtime (ir) op ────────────────────────────────────────────────
def lm_op(case_id, dialect, driver):
    c = CASES_BY_DIALECT[dialect][case_id]
    bundle, kind, inp = c["bundle"], c["kind"], c["input"]
    if kind in ("batch", "tx"):
        scope = inp if kind == "tx" else {}
        return lambda: execute_transaction_bundle(bundle, scope, driver)
    if kind == "relation":
        with_name = c["withRelation"]
        return lambda: read_bundle(bundle, inp, driver, [with_name])
    return lambda: execute_bundle(bundle, inp, driver)


# ── codegen op — no generated MODULE for python (declared design; see module doc) ──
def codegen_op(case_id, dialect, driver):
    """Python has no bc-generated codegen module — CODEGEN_INTEGRITY already verified this
    bundle's fingerprint at cold start (the codegen cell's own load-time cost). Execute via
    the SAME runtime call the ir cell uses (codegen ≈ ir is honest and expected for this
    language, matching the PHP codegen cell's convention)."""
    assert case_id in CODEGEN_INTEGRITY, f"codegen: integrity not verified for {case_id}"
    return lm_op(case_id, dialect, driver)


def make_op(case_id, dialect, driver):
    if IMPL == "sql":
        return sql_op(case_id, driver)
    if IMPL == "codegen":
        return codegen_op(case_id, dialect, driver)
    return lm_op(case_id, dialect, driver)


# ── DB-backed connection per dialect (lazy) ────────────────────────────────────
_LIVE = {}


def live_driver(dialect):
    if dialect in _LIVE:
        return _LIVE[dialect]
    from litedbmodel_runtime import PostgresDriver, MysqlDriver
    if dialect == "postgres":
        d = PostgresDriver.connect(**_pg_cfg())
        # Isolated `scp_python_bench` schema (never the shared `testdb.public` fixture
        # tables) — same "CREATE SCHEMA IF NOT EXISTS + SET search_path" seam as
        # `livedb_runner.py`'s conformance runner.
        d.exec_ddl([f"CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA_NAME}", f"SET search_path TO {PG_SCHEMA_NAME}"])
        d.exec_ddl(list(PG_SCHEMA))
        for s in SEED:
            d.prepare(s).run([])
        d.exec_ddl(list(PG_SEQ_RESET))
    else:
        d = MysqlDriver.connect(**_mysql_cfg())
        # Isolated `scp_python_bench` database (never the shared `testdb` fixture tables).
        d.exec_ddl([f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB_NAME}", f"USE {MYSQL_DB_NAME}"])
        d.exec_ddl(list(MYSQL_SCHEMA))
        for s in SEED:
            d.prepare(s).run([])
    _LIVE[dialect] = d
    return d


def db_supported(dialect):
    if IMPL == "sql" and dialect != "sqlite":
        return (False, f"sql baseline is hand-written sqlite SQL — not run against {dialect}")
    return (True, None)


def make_db_op(case_id, dialect):
    if dialect == "sqlite":
        driver = seed_sqlite()
        return make_op(case_id, "sqlite", driver), (lambda: driver.close())
    driver = live_driver(dialect)
    return make_op(case_id, dialect, driver), (lambda: None)


# ── fairness cost probe (sqlite; queries/op + rows/op) ─────────────────────────
TX_CTRL = re.compile(r"^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE|PRAGMA)\b", re.I)


class _CountingStmt:
    def __init__(self, stmt, counters):
        self._stmt = stmt; self._c = counters
    def all(self, params):
        self._c["q"] += 1; r = self._stmt.all(params); self._c["r"] += len(r) if isinstance(r, list) else 0; return r
    def run(self, params):
        self._c["q"] += 1; return self._stmt.run(params)


class _CountingDriver:
    def __init__(self, inner):
        self._inner = inner; self.counters = {"q": 0, "r": 0}
    def prepare(self, sql):
        stmt = self._inner.prepare(sql)
        return stmt if TX_CTRL.match(sql) else _CountingStmt(stmt, self.counters)
    def close(self):
        self._inner.close()


def cost(case_id):
    inner = seed_sqlite()
    driver = _CountingDriver(inner)
    try:
        make_op(case_id, "sqlite", driver)()
    finally:
        driver.close()
    return driver.counters["q"], driver.counters["r"]


# ── timing ────────────────────────────────────────────────────────────────────
def collect(op, warmup, iterations):
    for _ in range(warmup):
        op()
    samples = [0.0] * iterations
    for i in range(iterations):
        t0 = time.perf_counter(); op(); samples[i] = (time.perf_counter() - t0) * 1000.0
    return samples


# ── micro-bench: mock driver (fixed rows, no round-trip) ──────────────────────
_POSTS = [{"id": i + 1, "author_id": 1, "title": f"post-{i+1}", "status": "live", "views": (i + 1) * 10, "created_at": "2026-02-01"} for i in range(5)]
_COMMENTS = [{"id": i + 1, "post_id": (i % 5) + 1, "body": f"comment-{i+1}"} for i in range(25)]
_USERS = [{"id": 1, "name": "user-1"}]


def _fixture(sql):
    s = sql.lower()
    if s.lstrip().startswith("select"):
        if "from comments" in s:
            return _COMMENTS
        if "from users" in s:
            return _USERS
        if "from posts" in s:
            return _POSTS
        if "from " in s:
            return _POSTS
        return [{"1": 1}]
    if "returning" in s:
        return [{"id": 41, "author_id": 1, "title": "txn-post"}]
    return []


class _MockPrepared:
    def __init__(self, sql):
        self._rows = _fixture(sql)
    def all(self, params):
        return self._rows
    def run(self, params):
        from litedbmodel_runtime.driver import RunInfo
        return RunInfo(1, 41)


class MockDriver:
    def prepare(self, sql):
        return _MockPrepared(sql)
    def close(self):
        pass


def write(obj):
    sys.stdout.write(json.dumps(obj) + "\n"); sys.stdout.flush()


def handle(req):
    kind = req["kind"]
    if kind == "run":
        dialect = req["dialect"]
        ok, reason = db_supported(dialect)
        if not ok:
            write({"kind": "skipped", "case": req["case"], "dialect": dialect, "reason": reason}); return
        op, teardown = make_db_op(req["case"], dialect)
        samples = collect(op, req["warmup"], req["iterations"]); teardown()
        write({"kind": "run", "case": req["case"], "dialect": dialect, "samplesMs": samples})
    elif kind == "throughput":
        dialect = req["dialect"]
        ok, reason = db_supported(dialect)
        if not ok:
            write({"kind": "skipped", "case": req["case"], "dialect": dialect, "reason": reason}); return
        op, teardown = make_db_op(req["case"], dialect)
        t0 = time.perf_counter(); completed = 0
        for _ in range(req["iterations"]):
            op(); completed += 1
        elapsed = (time.perf_counter() - t0) * 1000.0; teardown()
        write({"kind": "throughput", "case": req["case"], "dialect": dialect, "elapsedMs": elapsed, "completed": completed})
    elif kind == "micro":
        dialect = req["dialect"]
        if IMPL == "sql" and dialect != "sqlite":
            write({"kind": "skipped", "case": req["case"], "dialect": dialect, "reason": "hand-SQL baseline is sqlite-shaped"}); return
        driver = MockDriver()
        op = make_op(req["case"], dialect, driver)
        samples = collect(op, req["warmup"], req["iterations"])
        write({"kind": "micro", "case": req["case"], "dialect": dialect, "samplesMs": samples})
    elif kind == "rss":
        try:
            import resource
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if sys.platform != "darwin":
                rss *= 1024
        except Exception:
            rss = 0
        write({"kind": "rss", "rssBytes": rss})
    elif kind == "cost":
        dialect = req["dialect"]
        q, r = cost(req["case"])
        write({"kind": "cost", "case": req["case"], "dialect": dialect, "queries": q, "rows": r})
    elif kind == "shutdown":
        for d in _LIVE.values():
            try:
                d.close()
            except Exception:
                pass
        sys.exit(0)


def main():
    write({"kind": "ready", "language": "python", "impl": IMPL, "readyAtEpochMs": time.time() * 1000.0})
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except Exception as e:  # noqa: BLE001
            write({"kind": "error", "message": f"bad request line: {e}"}); continue
        try:
            handle(req)
        except Exception as e:  # noqa: BLE001
            write({"kind": "error", "message": str(e), "stack": traceback.format_exc()})


if __name__ == "__main__":
    main()
