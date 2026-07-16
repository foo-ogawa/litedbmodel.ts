"""Phase B (#84, python) — LIVE-DB integration tests for the public transaction() boundary + the
tx-completeness primitives on REAL Postgres (:5433) + MySQL (:3307). The python mirror of go's
tx_boundary_livedb_test.go + rust tests/tx_boundary.rs. These exercise the UNMODIFIED production path
(transaction → with_transaction_decided on an OWNED pooled connection, each op JOINing the ambient tx
via the contextvar) against real engines:

  (1) MULTI-OP ATOMICITY — transaction(lambda: [opA_insert; opB_insert]) → both commit; a RECORDING
      connection asserts EXACTLY ONE BEGIN / ONE COMMIT / ONE connection for the whole boundary. opB
      PK-collides → opA's row ALSO rolled back (ONE BEGIN + ONE ROLLBACK, zero COMMIT), read from real
      rows. (The teeth: the disable-join RED is proven portably in test_transaction_boundary.py.)
  (2) GUARD — write OUTSIDE transaction() → WriteOutsideTransactionError; read-only → WriteInReadOnly;
      inside → ok. All on the live path.
  (3) ISOLATION — REPEATABLE READ holds the snapshot vs READ COMMITTED sees the concurrent commit
      (behavioral, both dialects; threads).
  (4) RETRY — REAL contention: PG SERIALIZABLE write-skew → real 40001; MySQL deadlock → real 1213; the
      loser retries and both commit; a non-retryable error does NOT retry. Proven with the string
      fallback DISABLED so the TYPED-code path (psycopg .sqlstate / PyMySQL .args[0], reachable through
      the mapped SqlFailure's wrapped chain) is shown genuinely load-bearing (the go #83 audit lesson).
  (5) NESTED — one BEGIN/COMMIT; an inner error rolls back the whole tx.

Gated behind LITEDBMODEL_TX_ISOLATION=1 (same gate as test_tx_isolation.py). Requires the dockerized
PG (:5433) + MySQL (:3307):

    docker compose -f docker-compose.test.yml -f docker-compose.livedb.yml up -d postgres mysql
    LITEDBMODEL_TX_ISOLATION=1 TEST_DB_PORT=5433 TEST_MYSQL_PORT=3307 \
        python3 -m pytest python/tests/test_tx_boundary_livedb.py -v -s
"""

from __future__ import annotations

import os
import threading

import pytest

from litedbmodel_runtime import (
    IsolationLevel,
    MysqlDriver,
    PostgresDriver,
    TransactionOptions,
    WriteInReadOnlyContextError,
    WriteOutsideTransactionError,
    context_for_driver,
    execute,
    execute_transaction_bundle,
    transaction,
)
from litedbmodel_runtime import tx_options as T
from litedbmodel_runtime.exec_context import current_context, run_with_pinned_context

ISO_TBL = "scp_tx_boundary_livedb_py"


def _enabled() -> bool:
    return os.environ.get("LITEDBMODEL_TX_ISOLATION") == "1"


pytestmark = pytest.mark.skipif(not _enabled(), reason="set LITEDBMODEL_TX_ISOLATION=1 + docker up")


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


# ── A recording driver: counts BEGIN / COMMIT / ROLLBACK + distinct connections ──


class _Sink:
    def __init__(self):
        self.begins = 0
        self.commits = 0
        self.rolls = 0
        self.conns = set()
        self.lock = threading.Lock()

    def reset(self):
        with self.lock:
            self.begins = self.commits = self.rolls = 0
            self.conns = set()


class _RecCursor:
    """Wraps a DB-API cursor, counting BEGIN/COMMIT/ROLLBACK issued through it (the tx-control the
    owned-connection tx path emits). Everything else passes through verbatim."""

    def __init__(self, cur, sink, conn_id):
        self._cur = cur
        self._sink = sink
        self._conn_id = conn_id

    def execute(self, sql, params=None):
        head = sql.lstrip().split(" ", 1)[0].upper()
        if head == "BEGIN":
            with self._sink.lock:
                self._sink.begins += 1
                self._sink.conns.add(self._conn_id)
        elif head == "COMMIT":
            with self._sink.lock:
                self._sink.commits += 1
        elif head == "ROLLBACK":
            with self._sink.lock:
                self._sink.rolls += 1
        if params is None:
            return self._cur.execute(sql)
        return self._cur.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._cur, name)


class _RecConn:
    def __init__(self, conn, sink):
        self._conn = conn
        self._sink = sink
        self._id = id(conn)

    def cursor(self, *a, **k):
        return _RecCursor(self._conn.cursor(*a, **k), self._sink, self._id)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _rec_pg_driver(sink, pool_size):
    import psycopg

    cfg = _pg_cfg()

    def factory():
        return _RecConn(psycopg.connect(autocommit=True, **cfg), sink)

    from litedbmodel_runtime.driver import _ConnectionPool, _dollar_to_pyformat

    pool = _ConnectionPool(factory, pool_size)
    return PostgresDriver(pool, _dollar_to_pyformat, emulate_returning=False)


def _rec_mysql_driver(sink, pool_size):
    import pymysql

    cfg = _mysql_cfg()
    dbname = cfg.pop("dbname")

    def factory():
        return _RecConn(pymysql.connect(autocommit=True, database=dbname, **cfg), sink)

    from litedbmodel_runtime.driver import _ConnectionPool, _qmark_to_pyformat

    pool = _ConnectionPool(factory, pool_size)
    return MysqlDriver(pool, _qmark_to_pyformat, emulate_returning=True)


# ── bundle authoring + read-back ────────────────────────────────────────────────


def _insert_bundle(dialect, id_, worker, seq):
    # The makeSQL-neutral op carries `?` placeholders; render_placeholders converts to the dialect
    # (postgres → $N, mysql → ?), and each live driver rewrites to %s at the seam.
    sql = f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (?, ?, ?)"
    return {
        "dialect": dialect,
        "name": "InsertOne",
        "transaction": {
            "phase": "create",
            "entityFrom": None,
            "statements": [
                {"id": "tx_body_0", "role": "body", "op": {"sql": sql, "params": [{"ref": ["id"]}, {"ref": ["worker"]}, {"ref": ["seq"]}]}},
            ],
        },
    }


def _reset(driver, int_type):
    driver.exec_ddl([f"DROP TABLE IF EXISTS {ISO_TBL}"])
    driver.exec_ddl([f"CREATE TABLE {ISO_TBL} (id {int_type} PRIMARY KEY, worker {int_type} NOT NULL, seq {int_type} NOT NULL)"])


def _read_rows(driver):
    rows = driver.prepare(f"SELECT id, worker FROM {ISO_TBL} WHERE worker <> 999").all([])
    return sorted((int(r["id"]), int(r["worker"])) for r in rows)


def _op(driver, dialect, id_, worker, seq):
    return execute_transaction_bundle(_insert_bundle(dialect, id_, worker, seq), {"id": id_, "worker": worker, "seq": seq}, driver)


# ── (1) MULTI-OP ATOMICITY ──────────────────────────────────────────────────────


def _multi_op_commit(driver, sink, dialect):
    sink.reset()
    ctx = context_for_driver(driver)
    r = transaction(ctx, lambda: [_op(driver, dialect, 100, 1, 0), _op(driver, dialect, 101, 1, 1)], TransactionOptions(), dialect)
    assert [x["committed"] for x in r] == [True, True]
    assert sink.begins == 1, f"{dialect}: N ops in one boundary ⇒ ONE BEGIN, got {sink.begins}"
    assert sink.commits == 1, f"{dialect}: ONE COMMIT, got {sink.commits}"
    assert sink.rolls == 0
    assert len(sink.conns) == 1, f"{dialect}: ONE connection, got {len(sink.conns)}"
    assert _read_rows(driver) == [(100, 1), (101, 1)]


def _multi_op_rollback(driver, sink, dialect):
    driver.exec_ddl([f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (201, 999, 9)"])  # pre-seed collision
    sink.reset()
    ctx = context_for_driver(driver)
    raised = False
    try:
        transaction(ctx, lambda: [_op(driver, dialect, 200, 2, 0), _op(driver, dialect, 201, 2, 1)], TransactionOptions(retry_on_error=False), dialect)
    except Exception:
        raised = True
    assert raised, f"{dialect}: opB's PK collision must fail the whole boundary"
    assert sink.begins == 1 and sink.commits == 0 and sink.rolls == 1, f"{dialect}: ONE BEGIN + ONE ROLLBACK + zero COMMIT: {sink.begins}/{sink.commits}/{sink.rolls}"
    assert _read_rows(driver) == [], f"{dialect}: opA (id=200) must ALSO roll back when opB fails"


# ── (2) write=tx GUARD on the live path ─────────────────────────────────────────


def _guard_live(driver, dialect):
    ctx = context_for_driver(driver)
    # Outside any transaction() → WriteOutsideTransactionError.
    with pytest.raises(WriteOutsideTransactionError):
        _op(driver, dialect, 300, 3, 0)
    # Read-only-scoped write inside a transaction() → WriteInReadOnly (read-only first).
    def ro_body():
        ro = current_context().with_read_only()
        with pytest.raises(WriteInReadOnlyContextError):
            run_with_pinned_context(ro, lambda: _op(driver, dialect, 301, 3, 0))
        return 0
    transaction(ctx, ro_body, TransactionOptions(retry_on_error=False), dialect)
    # Inside a transaction() → succeeds.
    transaction(ctx, lambda: _op(driver, dialect, 302, 3, 0), TransactionOptions(), dialect)
    assert _read_rows(driver) == [(302, 3)], f"{dialect}: only the in-boundary write (id=302) persisted"


# ── (3) ISOLATION — REPEATABLE READ snapshot vs READ COMMITTED ──────────────────


def _isolation_behavior(driver, dialect):
    def reset_row():
        driver.exec_ddl([f"DELETE FROM {ISO_TBL}"])
        driver.exec_ddl([f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (1, 500, 10)"])

    def read_seq(tx_ctx):
        rows = execute(tx_ctx, f"SELECT seq FROM {ISO_TBL} WHERE id = 1", [])
        return int(rows[0]["seq"]) if rows else -1

    ctx = context_for_driver(driver)

    # REPEATABLE READ: the two in-tx reads MUST match despite the concurrent commit.
    reset_row()
    def rr_body():
        first = read_seq(current_context())
        driver.prepare(f"UPDATE {ISO_TBL} SET seq = 99 WHERE id = 1").run([])  # concurrent committed update
        second = read_seq(current_context())
        assert first == second, f"{dialect} REPEATABLE READ: snapshot must hold, first={first} second={second}"
        return 0
    transaction(ctx, rr_body, TransactionOptions(isolation=IsolationLevel.REPEATABLE_READ), dialect)

    # READ COMMITTED: the second in-tx read MUST see the concurrent commit.
    reset_row()
    def rc_body():
        first = read_seq(current_context())
        driver.prepare(f"UPDATE {ISO_TBL} SET seq = 77 WHERE id = 1").run([])
        second = read_seq(current_context())
        assert first != second, f"{dialect} READ COMMITTED: second read must see the concurrent commit, first={first} second={second}"
        return 0
    transaction(ctx, rc_body, TransactionOptions(isolation=IsolationLevel.READ_COMMITTED), dialect)


# ── (4) RETRY under REAL contention ─────────────────────────────────────────────


def _neuter_wrapped_chain():
    """Break the `.wrapped`/`__cause__` chain traversal in the typed classifier — yield ONLY the
    top-of-chain error. Returns a restore fn. Used to PROVE the mapped-`SqlFailure`/`.wrapped` envelope
    is load-bearing on the live retry path (the go #83 audit teeth): the error `transaction()` classifies
    is a mapped `SqlFailure` whose TOP level has no `.sqlstate` — so without the chain traversal (and
    with the string fallback OFF) it can't classify → the loser can't retry → RED."""
    orig = T._iter_error_chain

    def top_only(error):
        yield error  # NO traversal into .wrapped / __cause__ / __context__

    T._iter_error_chain = top_only
    return lambda: setattr(T, "_iter_error_chain", orig)


def _retry_pg_write_skew(driver, sink, typed_only, neuter_wrapped=False, expect_red=False):
    if typed_only:
        T.disable_retry_string_fallback = True
    restore = _neuter_wrapped_chain() if neuter_wrapped else (lambda: None)
    try:
        driver.exec_ddl([f"DELETE FROM {ISO_TBL}"])
        driver.exec_ddl([f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (1, 500, 10), (2, 500, 20)"])
        sink.reset()
        ctx = context_for_driver(driver)
        opts = TransactionOptions(isolation=IsolationLevel.SERIALIZABLE, retry_duration=5)
        barrier = threading.Barrier(2)
        errs = [None, None]

        def worker(i):
            def body():
                execute(current_context(), f"SELECT COALESCE(SUM(seq),0) AS s FROM {ISO_TBL}", [])  # write-skew read set
                try:
                    barrier.wait(timeout=10)
                except Exception:
                    pass
                from litedbmodel_runtime import run as seam_run
                seam_run(current_context(), f"UPDATE {ISO_TBL} SET seq = seq + 1 WHERE id = $1", [i + 1])
                return 0
            try:
                transaction(ctx, body, opts, "postgres")
            except Exception as e:
                errs[i] = e

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if expect_red:
            # TEETH: with the `.wrapped` chain neutered AND the string fallback off, the mapped 40001
            # cannot be classified retryable → the loser gives up (a worker carries the 40001 error).
            assert any(e is not None for e in errs), \
                "TEETH FAILED: PG retry still absorbed 40001 with .wrapped chain neutered + string fallback off (envelope NOT load-bearing)"
        else:
            for i, e in enumerate(errs):
                assert e is None, f"PG retry: worker {i} must eventually commit (retry absorbs 40001), got {e!r}"
            assert sink.begins > 2, f"PG retry: the retry must have fired (>2 BEGIN for 2 workers), got {sink.begins}"
    finally:
        restore()
        T.disable_retry_string_fallback = False


def _retry_mysql_deadlock(driver, sink, typed_only, neuter_wrapped=False, expect_red=False):
    if typed_only:
        T.disable_retry_string_fallback = True
    restore = _neuter_wrapped_chain() if neuter_wrapped else (lambda: None)
    try:
        deadlock_fired = False
        red_seen = False
        for _round in range(25):
            if deadlock_fired or red_seen:
                break
            driver.exec_ddl([f"DELETE FROM {ISO_TBL}"])
            driver.exec_ddl([f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (1, 500, 10), (2, 500, 20)"])
            sink.reset()
            ctx = context_for_driver(driver)
            opts = TransactionOptions(retry_duration=5)
            first_done = threading.Barrier(2)
            release = threading.Event()
            errs = [None, None]

            def worker(i):
                first, second = (1, 2) if i == 0 else (2, 1)  # opposite lock order → deadlock
                state = {"done": False}
                from litedbmodel_runtime import run as seam_run

                def body():
                    seam_run(current_context(), f"UPDATE {ISO_TBL} SET seq = seq + 1 WHERE id = ?", [first])
                    if not state["done"]:
                        state["done"] = True
                        try:
                            first_done.wait(timeout=10)
                        except Exception:
                            pass
                        release.set()
                        release.wait(timeout=10)
                    seam_run(current_context(), f"UPDATE {ISO_TBL} SET seq = seq + 1 WHERE id = ?", [second])
                    return 0
                try:
                    transaction(ctx, body, opts, "mysql")
                except Exception as e:
                    errs[i] = e

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            if expect_red:
                # TEETH: with the `.wrapped` chain neutered + string fallback off, a raced 1213 can't be
                # classified retryable → the loser gives up. A worker carrying an error IS the RED signal.
                if any(e is not None for e in errs):
                    red_seen = True
            else:
                for i, e in enumerate(errs):
                    assert e is None, f"MySQL retry: worker {i} must eventually commit (retry absorbs 1213), got {e!r}"
                if sink.begins > 2:
                    deadlock_fired = True
        if expect_red:
            assert red_seen, "TEETH inconclusive: no 1213 raced in 25 rounds to prove the neutered envelope goes RED"
        elif typed_only:
            assert deadlock_fired, "MySQL typed-path proof inconclusive: no deadlock raced in 25 rounds"
    finally:
        restore()
        T.disable_retry_string_fallback = False


def _non_retryable_does_not_retry(driver, sink, dialect):
    driver.exec_ddl([f"DELETE FROM {ISO_TBL}"])
    driver.exec_ddl([f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (400, 999, 9)"])
    sink.reset()
    ctx = context_for_driver(driver)
    raised = False
    try:
        transaction(ctx, lambda: _op(driver, dialect, 400, 4, 0), TransactionOptions(retry_duration=5), dialect)  # PK collision
    except Exception:
        raised = True
    assert raised, f"{dialect}: a unique collision must fail the boundary"
    assert sink.begins == 1, f"{dialect}: a non-retryable error must NOT retry (1 BEGIN), got {sink.begins}"


# ── (5) NESTED transaction ──────────────────────────────────────────────────────


def _nested(driver, sink, dialect):
    driver.exec_ddl([f"DELETE FROM {ISO_TBL}"])
    sink.reset()
    ctx = context_for_driver(driver)

    def outer():
        _op(driver, dialect, 500, 5, 0)
        return transaction(ctx, lambda: _op(driver, dialect, 501, 5, 1), TransactionOptions(), dialect)  # JOINs the outer
    transaction(ctx, outer, TransactionOptions(), dialect)
    assert sink.begins == 1 and sink.commits == 1 and sink.rolls == 0 and len(sink.conns) == 1, \
        f"{dialect} nested: ONE BEGIN + ONE COMMIT on ONE conn: {sink.begins}/{sink.commits}/{sink.rolls}/{len(sink.conns)}"

    # An inner error rolls back the WHOLE tx.
    driver.exec_ddl([f"DELETE FROM {ISO_TBL}"])
    driver.exec_ddl([f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (601, 999, 9)"])
    sink.reset()

    def outer2():
        _op(driver, dialect, 600, 6, 0)
        return transaction(ctx, lambda: _op(driver, dialect, 601, 6, 1), TransactionOptions(), dialect)  # inner PK collision
    raised = False
    try:
        transaction(ctx, outer2, TransactionOptions(retry_on_error=False), dialect)
    except Exception:
        raised = True
    assert raised, f"{dialect} nested inner error must fail the whole tx"
    assert _read_rows(driver) == [], f"{dialect} nested: an inner error rolls back the WHOLE tx (id=600 absent)"
    assert sink.commits == 0 and sink.rolls == 1, f"{dialect} nested inner error ⇒ ONE ROLLBACK zero COMMIT: {sink.commits}/{sink.rolls}"


# ── Entry points ─────────────────────────────────────────────────────────────────


def test_tx_boundary_postgres():
    sink = _Sink()
    driver = _rec_pg_driver(sink, pool_size=8)
    try:
        _reset(driver, "INTEGER")
        _multi_op_commit(driver, sink, "postgres")
        _reset(driver, "INTEGER")
        _multi_op_rollback(driver, sink, "postgres")
        _reset(driver, "INTEGER")
        _guard_live(driver, "postgres")
        _reset(driver, "INTEGER")
        _isolation_behavior(driver, "postgres")
        _reset(driver, "INTEGER")
        _retry_pg_write_skew(driver, sink, typed_only=False)
        _reset(driver, "INTEGER")
        # REGRESSION (audit lesson): the TYPED-code path alone (string fallback disabled) must classify
        # the live 40001 THROUGH the mapped SqlFailure/.wrapped envelope (go/rust parity — the raw
        # psycopg SerializationFailure is mapped in transaction()'s retry handler and classified via
        # .wrapped, exactly as go classifies via SqlFailure.Unwrap()).
        _retry_pg_write_skew(driver, sink, typed_only=True)
        _reset(driver, "INTEGER")
        # TEETH: neuter the .wrapped chain traversal (+ string fallback off) → the mapped 40001 can no
        # longer be classified → the loser gives up (RED). Proves the .wrapped envelope is genuinely
        # load-bearing on the LIVE retry path, not dead machinery.
        _retry_pg_write_skew(driver, sink, typed_only=True, neuter_wrapped=True, expect_red=True)
        _reset(driver, "INTEGER")
        _non_retryable_does_not_retry(driver, sink, "postgres")
        _reset(driver, "INTEGER")
        _nested(driver, sink, "postgres")
        print("PG TX-BOUNDARY PROOF: multi-op atomicity (1 BEGIN/COMMIT/conn + A-rolls-back-when-B-fails) + guard + isolation + real-contention retry (40001, typed-path load-bearing) + nested — all green")
    finally:
        driver.close()


def test_tx_boundary_mysql():
    sink = _Sink()
    driver = _rec_mysql_driver(sink, pool_size=8)
    try:
        _reset(driver, "INT")
        _multi_op_commit(driver, sink, "mysql")
        _reset(driver, "INT")
        _multi_op_rollback(driver, sink, "mysql")
        _reset(driver, "INT")
        _guard_live(driver, "mysql")
        _reset(driver, "INT")
        _isolation_behavior(driver, "mysql")
        _reset(driver, "INT")
        _retry_mysql_deadlock(driver, sink, typed_only=False)
        _reset(driver, "INT")
        # REGRESSION (audit lesson): the TYPED-code path alone must classify the live 1213 THROUGH the
        # mapped SqlFailure/.wrapped envelope (go/rust parity: the raw PyMySQL error is mapped in
        # transaction()'s retry handler and classified via .wrapped).
        _retry_mysql_deadlock(driver, sink, typed_only=True)
        _reset(driver, "INT")
        # TEETH: neuter the .wrapped chain traversal (+ string fallback off) → a raced 1213 can no longer
        # be classified → the loser gives up (RED). Proves the .wrapped envelope is load-bearing live.
        _retry_mysql_deadlock(driver, sink, typed_only=True, neuter_wrapped=True, expect_red=True)
        _reset(driver, "INT")
        _non_retryable_does_not_retry(driver, sink, "mysql")
        _reset(driver, "INT")
        _nested(driver, sink, "mysql")
        print("MYSQL TX-BOUNDARY PROOF: multi-op atomicity (1 BEGIN/COMMIT/conn + A-rolls-back-when-B-fails) + guard + isolation + real-contention retry (1213, typed-path load-bearing) + nested — all green")
    finally:
        driver.close()
