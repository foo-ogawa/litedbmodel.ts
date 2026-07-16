"""Phase A-4 (#78, python) — CONCURRENT-TRANSACTION ISOLATION + ATOMICITY on live PG + MySQL.

The Python mirror of #75's ``test/integration/TxIsolation.test.ts``, the rust ``tests/tx_isolation.rs``
and the go ``tx_isolation_test.go``. It proves the per-execution connection ownership (ExecutionContext
+ ``with_transaction_decided`` over an OWNED pooled connection acquired by ``driver.begin_tx()``, §3):
N transactions run CONCURRENTLY IN THREADS in ONE process on the shared driver/pool, each acquiring its
OWN pooled connection. ``contextvars`` are per-thread, so each thread's pinned tx ctx is isolated. The
assertions run the UNMODIFIED PRODUCTION path (``execute_transaction_bundle`` → ``with_transaction_decided``
→ the tx-owned connection) — nothing is swapped:

  (1) ISOLATION — N concurrent workers each run TWO back-to-back single-INSERT transactions; the final
      table holds EXACTLY the 2·N rows, correctly paired per worker (assert real per-row membership,
      not counts). No cross-talk.
  (2) SINGLE-STATEMENT ATOMICITY — a tx whose sole INSERT collides on a pre-seeded PK ROLLBACKs (whole
      tx error); the concurrent committing transactions are unaffected (their rows present; the aborted
      one's absent).
  (3) MULTI-STATEMENT ATOMICITY (production path) — a 2-statement tx whose 2nd statement collides MUST
      roll back the 1st statement's already-executed INSERT (real cross-statement atomicity through
      ``with_transaction_decided`` on ONE owned connection), while a concurrently-committed tx is
      unaffected. This pins production ownership DIRECTLY — the faithful-mutation RED proof (documented
      at the bottom) reverts ownership to a fresh autocommit connection and this goes RED.

Gated behind LITEDBMODEL_TX_ISOLATION=1 so it never runs in the default ``pytest`` (which has no DBs).
Requires the dockerized PG (:5433) + MySQL (:3307):

    docker compose -f docker-compose.test.yml -f docker-compose.livedb.yml up -d postgres mysql
    LITEDBMODEL_TX_ISOLATION=1 TEST_DB_PORT=5433 TEST_MYSQL_PORT=3307 \
        python3 -m pytest python/tests/test_tx_isolation.py -v -s
"""

from __future__ import annotations

import os
import threading

import pytest

from litedbmodel_runtime import MysqlDriver, PostgresDriver
from litedbmodel_runtime.runtime import _execute_transaction_bundle  # internal per-command auto-tx (guard opt-out)

ISO_TBL = "scp_tx_iso_py"
ISO_N = 8


def _enabled() -> bool:
    return os.environ.get("LITEDBMODEL_TX_ISOLATION") == "1"


pytestmark = pytest.mark.skipif(not _enabled(), reason="set LITEDBMODEL_TX_ISOLATION=1 + docker up")


# ── connection config (matches docker-compose.livedb.yml / livedb_runner defaults) ──


def _pg_cfg():
    return dict(
        host=os.environ.get("TEST_DB_HOST", "localhost"),
        port=int(os.environ.get("TEST_DB_PORT", "5433")),
        user=os.environ.get("TEST_DB_USER", "testuser"),
        password=os.environ.get("TEST_DB_PASSWORD", "testpass"),
        dbname=os.environ.get("TEST_DB_NAME", "testdb"),
    )


def _mysql_cfg():
    return dict(
        host=os.environ.get("TEST_MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("TEST_MYSQL_PORT", "3307")),
        user=os.environ.get("TEST_MYSQL_USER", "testuser"),
        password=os.environ.get("TEST_MYSQL_PASSWORD", "testpass"),
        dbname=os.environ.get("TEST_MYSQL_DB", "testdb"),
    )


# ── Bundle authoring (the shape execute_transaction_bundle consumes) ────────────


def _insert_bundle(dialect):
    """A single-INSERT (no RETURNING, entityFrom null) tx bundle whose values come from the input
    scope. Committed via the production tx path on ONE owned connection."""
    return {
        "dialect": dialect,
        "transaction": {
            "phase": "create",
            "entityFrom": None,
            "statements": [
                {
                    "id": "tx_body_0",
                    "role": "body",
                    "op": {
                        "sql": f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (?, ?, ?)",
                        "params": [{"ref": ["id"]}, {"ref": ["worker"]}, {"ref": ["seq"]}],
                    },
                }
            ],
        },
    }


def _two_stmt_bundle(dialect, id1, id2, worker):
    """A TWO-statement tx bundle: stmt-1 inserts id1 (valid), stmt-2 inserts id2 (pre-seeded to
    collide). One logical transaction — stmt-2's PK violation must roll back stmt-1 (cross-statement
    atomicity on the owned connection)."""
    return {
        "dialect": dialect,
        "transaction": {
            "phase": "create",
            "entityFrom": None,
            "statements": [
                {"id": "tx_body_0", "role": "body", "op": {"sql": f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES ({id1}, {worker}, 0)", "params": []}},
                {"id": "tx_body_1", "role": "body", "op": {"sql": f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES ({id2}, {worker}, 0)", "params": []}},
            ],
        },
    }


def _tx_input(id_, worker, seq):
    return {"id": id_, "worker": worker, "seq": seq}


# ── Table lifecycle + read-back (through the driver's own DDL / read seam) ───────


def _reset(driver, int_type):
    driver.exec_ddl([f"DROP TABLE IF EXISTS {ISO_TBL}"])
    driver.exec_ddl([f"CREATE TABLE {ISO_TBL} (id {int_type} PRIMARY KEY, worker {int_type} NOT NULL, seq {int_type} NOT NULL)"])


def _read_iso_rows(driver):
    """Read (id, worker) rows (worker != 999 filters any pre-seed), sorted — for real per-row
    membership assertions (not counts)."""
    rows = driver.prepare(f"SELECT id, worker FROM {ISO_TBL} WHERE worker <> 999").all([])
    out = [(int(r["id"]), int(r["worker"])) for r in rows]
    out.sort()
    return out


def _run_insert_tx(driver, dialect, id_, worker, seq):
    return _execute_transaction_bundle(_insert_bundle(dialect), _tx_input(id_, worker, seq), driver, guard=False)


# ── (1) ISOLATION — N workers × 2 concurrent single-INSERT txs, no cross-talk ───


def _isolation(driver, dialect):
    errors = []

    def worker(k):
        try:
            r0 = _run_insert_tx(driver, dialect, 2 * k, k, 0)
            assert r0["committed"], f"worker {k} tx0 must commit"
            r1 = _run_insert_tx(driver, dialect, 2 * k + 1, k, 1)
            assert r1["committed"], f"worker {k} tx1 must commit"
        except Exception as e:  # collected so the assertion runs on the main thread
            errors.append((k, repr(e)))

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(ISO_N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, f"{dialect} worker errors: {errors}"

    got = _read_iso_rows(driver)
    want = sorted([(2 * k, k) for k in range(ISO_N)] + [(2 * k + 1, k) for k in range(ISO_N)])
    assert got == want, f"{dialect} isolation: every worker's rows present, no cross-talk\n got={got}\nwant={want}"


# ── (2) SINGLE-STATEMENT ATOMICITY — worker 0 collides, others commit ───────────


def _single_stmt_atomicity(driver, dialect):
    driver.exec_ddl([f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (0, 999, 9)"])  # pre-seed id=0
    outcomes = {}
    lock = threading.Lock()

    def worker(k):
        ok = True
        try:
            _run_insert_tx(driver, dialect, k, k, 0)
        except Exception:
            ok = False
        with lock:
            outcomes[k] = ok

    threads = [threading.Thread(target=worker, args=(k,)) for k in range(ISO_N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes[0] is False, f"{dialect}: worker 0 (PK collision) tx must FAIL"
    for k in range(1, ISO_N):
        assert outcomes[k] is True, f"{dialect}: worker {k} must commit"
    got = _read_iso_rows(driver)  # worker 999 pre-seed filtered out
    want = sorted([(i, i) for i in range(1, ISO_N)])
    assert got == want, f"{dialect}: aborted worker 0 row ABSENT; committed workers present\n got={got}\nwant={want}"


# ── (3) MULTI-STATEMENT ATOMICITY (production path) — cross-stmt rollback ────────


def _multi_stmt_atomicity(driver, dialect):
    driver.exec_ddl([f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (20, 999, 9)"])  # pre-seed id=20
    result = {}

    def fail_tx():
        try:
            _execute_transaction_bundle(_two_stmt_bundle(dialect, 10, 20, 1), _tx_input(0, 1, 0), driver, guard=False)
            result["fail_ok"] = True  # committed (BAD — stmt-2 should collide)
        except Exception:
            result["fail_ok"] = False  # raised → rolled back (GOOD)

    def commit_tx():
        try:
            r = _run_insert_tx(driver, dialect, 30, 2, 0)
            result["commit_ok"] = bool(r["committed"])
        except Exception:
            result["commit_ok"] = False

    threads = [threading.Thread(target=fail_tx), threading.Thread(target=commit_tx)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert result["fail_ok"] is False, f"{dialect}: the 2-statement tx must FAIL on stmt-2's collision"
    assert result["commit_ok"] is True, f"{dialect}: the concurrent single-INSERT tx must commit unaffected"
    got = _read_iso_rows(driver)  # id=20 pre-seed (worker 999) filtered out
    want = [(30, 2)]
    assert got == want, f"{dialect}: id=10 ROLLED BACK (cross-statement atomicity); id=30 present, unaffected\n got={got}\nwant={want}"


# ── (4) COMMIT-FAILURE POOL-LEAK GUARD (#78 audit) — a RAISING commit must not leak ──
#
# The realistic "commit itself raises" case on PG: a DEFERRABLE INITIALLY DEFERRED unique constraint
# whose violation surfaces at COMMIT (not at the INSERT). Run MANY such failing txs through the
# production path on a SMALL pool: if the owned connection leaked on each raising COMMIT (the pre-fix
# self-release-in-commit bug), the pool would exhaust and the run would hang / error long before the
# loop ends. After the fix, the combinator's finally releases (destroys) each poisoned connection, so
# the pool never shrinks below capacity and a final CLEAN tx still commits. (PG-only: MySQL has no
# deferred unique constraints; the unit test test_sqlite_pool_not_leaked_on_raising_commit covers the
# pool accounting portably.)


def _commit_failure_no_pool_leak_pg(driver):
    # A table with a DEFERRABLE INITIALLY DEFERRED unique constraint: two inserts of the same key in
    # one tx pass at INSERT time and only collide at COMMIT → COMMIT raises.
    driver.exec_ddl([f"DROP TABLE IF EXISTS {ISO_TBL}"])
    driver.exec_ddl(
        [
            f"CREATE TABLE {ISO_TBL} (id INTEGER PRIMARY KEY, worker INTEGER NOT NULL, seq INTEGER NOT NULL, "
            f"k INTEGER, CONSTRAINT {ISO_TBL}_k_uniq UNIQUE (k) DEFERRABLE INITIALLY DEFERRED)"
        ]
    )
    dup_bundle = {
        "dialect": "postgres",
        "transaction": {
            "phase": "create",
            "entityFrom": None,
            "statements": [
                {"id": "s0", "role": "body", "op": {"sql": f"INSERT INTO {ISO_TBL} (id, worker, seq, k) VALUES (?, 1, 0, 7)", "params": [{"ref": ["a"]}]}},
                {"id": "s1", "role": "body", "op": {"sql": f"INSERT INTO {ISO_TBL} (id, worker, seq, k) VALUES (?, 1, 0, 7)", "params": [{"ref": ["b"]}]}},  # same k=7 → COMMIT fails
            ],
        },
    }
    # Many more iterations than the pool size: a per-iteration leak would exhaust it and hang/raise.
    iterations = 40
    for i in range(iterations):
        raised = False
        try:
            _execute_transaction_bundle(dup_bundle, {"a": 2 * i, "b": 2 * i + 1}, driver, guard=False)
        except Exception:
            raised = True  # the deferred-unique violation surfaces at COMMIT → raises (GOOD)
        assert raised, "the deferred-unique tx must FAIL at COMMIT"
    # The pool survived (no leak) — a final CLEAN single-INSERT tx still commits.
    driver.exec_ddl([f"DELETE FROM {ISO_TBL}"])
    ok = _execute_transaction_bundle(
        {"dialect": "postgres", "transaction": {"phase": "create", "entityFrom": None, "statements": [
            {"id": "s0", "role": "body", "op": {"sql": f"INSERT INTO {ISO_TBL} (id, worker, seq, k) VALUES (?, 5, 0, 99)", "params": [{"ref": ["a"]}]}}]}},
        {"a": 500},
        driver,
        guard=False,
    )
    assert ok["committed"], "after 40 raising-COMMIT txs the pool must still serve a clean commit (no leak)"
    rows = driver.prepare(f"SELECT id FROM {ISO_TBL}").all([])
    assert [int(r["id"]) for r in rows] == [500]


# ── The two live-DB entry points ────────────────────────────────────────────────


def test_tx_isolation_postgres():
    driver = PostgresDriver.connect(pool_size=ISO_N + 4, **_pg_cfg())
    try:
        _reset(driver, "INTEGER")
        _isolation(driver, "postgres")
        _reset(driver, "INTEGER")
        _single_stmt_atomicity(driver, "postgres")
        _reset(driver, "INTEGER")
        _multi_stmt_atomicity(driver, "postgres")
    finally:
        driver.close()


def test_commit_failure_no_pool_leak_postgres():
    # A DELIBERATELY SMALL pool (size 2): a per-commit-failure connection leak would exhaust it within
    # the 40-iteration loop and hang/error, so a green run is a real no-leak proof of the finally-release.
    driver = PostgresDriver.connect(pool_size=2, **_pg_cfg())
    try:
        _commit_failure_no_pool_leak_pg(driver)
    finally:
        driver.close()


def test_tx_isolation_mysql():
    driver = MysqlDriver.connect(pool_size=ISO_N + 4, **_mysql_cfg())
    try:
        _reset(driver, "INT")
        _isolation(driver, "mysql")
        _reset(driver, "INT")
        _single_stmt_atomicity(driver, "mysql")
        _reset(driver, "INT")
        _multi_stmt_atomicity(driver, "mysql")
    finally:
        driver.close()


# ── FAITHFUL-MUTATION RED PROOF (performed during #78; reverted) ────────────────
#
# The gate's teeth were proven by a FAITHFUL mutation that reverts per-execution connection ownership
# and confirming this test goes RED. The mutation (applied to exec_context._TxConnectionAdapter): route
# the tx's write to a FRESH autocommit connection (a new pool checkout) instead of the tx-OWNED
# connection — i.e. the tx statements no longer run on the transaction's own connection, so a ROLLBACK
# cannot undo an already-autocommitted write. See the committed report / session log for the captured
# RED output (multi-statement atomicity fails: id=10 survives the rollback). With ownership restored the
# write runs on the tx's OWN connection and the ROLLBACK undoes it → GREEN. The mutation was applied,
# the RED captured, then FULLY reverted (the committed code owns the tx connection).
