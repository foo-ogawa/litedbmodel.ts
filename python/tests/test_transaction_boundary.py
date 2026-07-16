"""Phase B-core (#84/#86, python) — UNIT tests for the public transaction() boundary (in-proc SQLite).

No live DB — an in-proc stdlib ``sqlite3`` driver (wrapped in a recording shim that counts begin_tx /
commit / rollback) proves the boundary mechanics that DON'T need PG/MySQL:

  (1) MULTI-OP ATOMICITY — transaction(lambda: [opA_insert(); opB_insert()]) → both commit; the
      recording driver asserts EXACTLY ONE begin_tx / ONE commit / ONE tx handle for the whole boundary
      (the ambient JOIN — opB opens no second BEGIN). opB PK-collides → opA's row ALSO rolls back (ONE
      begin_tx + ONE rollback, zero commit), verified by reading real rows.
  (2) MUTATION RED (teeth) — disabling the ambient-JOIN (opB opens its own auto-tx) makes the
      A-rolls-back-when-B-fails assertion go RED, proving the join is load-bearing.
  (3) GUARD — a write OUTSIDE transaction() → WriteOutsideTransactionError; a read-only write inside →
      WriteInReadOnlyContextError; inside a boundary → ok.
  (4) NESTED — one begin_tx/commit; an inner error rolls back the whole tx.
  (5) rollback_only — the body runs + returns its value, but NOTHING commits (dry-run).

The live-PG/MySQL isolation + real-contention-retry proof lives in test_tx_boundary_livedb.py.
"""

from __future__ import annotations

import sqlite3

import pytest

from litedbmodel_runtime import (
    IsolationLevel,
    TransactionOptions,
    WriteInReadOnlyContextError,
    WriteOutsideTransactionError,
    context_for_driver,
    execute_transaction_bundle,
    transaction,
)
from litedbmodel_runtime.runtime import _execute_transaction_bundle  # internal guard opt-out (disable-join mutation)
from litedbmodel_runtime.driver import SqliteDriver, _SqliteTxConnection


ISO_TBL = "scp_tx_boundary_py"


# ── A recording SQLite driver: counts begin_tx / commit / rollback / distinct handles ──


class _RecTx(_SqliteTxConnection):
    """New contract (Phase D / #95): tx-control flows through :meth:`run` (issued by the combinator via
    the seam), so COMMIT/ROLLBACK are counted HERE when their SQL passes through — not in removed
    ``commit``/``rollback`` methods. BEGIN is likewise a ``run`` call (counted in the driver's begin_tx
    sink for the ambient-JOIN count, and issued via the seam)."""

    def __init__(self, conn, sink):
        super().__init__(conn)
        self._sink = sink

    def run(self, sql, params):
        head = sql.strip().split(" ", 1)[0].upper()
        if head == "COMMIT":
            self._sink["commits"] += 1
        elif head == "ROLLBACK":
            self._sink["rolls"] += 1
        return super().run(sql, params)


class _RecSqliteDriver(SqliteDriver):
    """A SqliteDriver that records tx lifecycle into a shared sink dict (begin_tx / commit / rollback /
    the set of distinct tx handles). Single-conn SQLite, so the whole boundary shares ONE conn — the
    counts prove the ambient JOIN (N ops = ONE begin_tx)."""

    def __init__(self, conn, sink):
        super().__init__(conn)
        self._sink = sink

    def begin_tx(self):
        # New contract: acquire + OWN the conn only; BEGIN is issued by the combinator THROUGH the seam
        # (via _RecTx.run). The begin_tx COUNT still proves the ambient JOIN (N ops = ONE begin_tx).
        self._sink["begin_tx"] += 1
        tx = _RecTx(self.conn, self._sink)
        self._sink["distinct"].add(id(tx))
        return tx


def _fresh_sink():
    return {"begin_tx": 0, "commits": 0, "rolls": 0, "distinct": set()}


def _make_driver(sink):
    conn = sqlite3.connect(":memory:")
    conn.execute(f"CREATE TABLE {ISO_TBL} (id INTEGER PRIMARY KEY, worker INTEGER NOT NULL, seq INTEGER NOT NULL)")
    conn.commit()
    return _RecSqliteDriver(conn, sink)


def _insert_bundle(id_, worker, seq):
    return {
        "dialect": "sqlite",
        "name": "InsertOne",
        "transaction": {
            "phase": "create",
            "entityFrom": None,
            "statements": [
                {"id": "tx_body_0", "role": "body", "op": {
                    "sql": f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (?, ?, ?)",
                    "params": [{"ref": ["id"]}, {"ref": ["worker"]}, {"ref": ["seq"]}],
                }},
            ],
        },
    }


def _read_rows(driver):
    rows = driver.prepare(f"SELECT id, worker FROM {ISO_TBL} WHERE worker <> 999").all([])
    return sorted((int(r["id"]), int(r["worker"])) for r in rows)


# ── (1) MULTI-OP ATOMICITY — commit path ───────────────────────────────────────


def test_multi_op_boundary_one_begin_one_commit():
    sink = _fresh_sink()
    driver = _make_driver(sink)
    ctx = context_for_driver(driver)

    def do_op(id_, worker, seq):
        # A single op that JOINs the ambient boundary (guard ON — proving it's inside a tx).
        return execute_transaction_bundle(_insert_bundle(id_, worker, seq), {"id": id_, "worker": worker, "seq": seq}, driver)

    result = transaction(ctx, lambda: [do_op(100, 1, 0), do_op(101, 1, 1)], TransactionOptions(), "sqlite")

    assert [r["committed"] for r in result] == [True, True]
    # N ops in one boundary ⇒ ONE begin_tx + ONE commit on ONE tx handle (the ambient JOIN).
    assert sink["begin_tx"] == 1, f"expected 1 begin_tx, got {sink['begin_tx']}"
    assert sink["commits"] == 1, f"expected 1 commit, got {sink['commits']}"
    assert sink["rolls"] == 0
    assert len(sink["distinct"]) == 1, f"expected 1 distinct tx handle, got {len(sink['distinct'])}"
    assert _read_rows(driver) == [(100, 1), (101, 1)]


# ── (1) MULTI-OP ATOMICITY — rollback path (B fails ⇒ A also rolls back) ────────


def test_multi_op_boundary_opB_fail_rolls_back_opA():
    sink = _fresh_sink()
    driver = _make_driver(sink)
    driver.conn.execute(f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (201, 999, 9)")  # pre-seed collision
    driver.conn.commit()
    ctx = context_for_driver(driver)

    def do_op(id_, worker, seq):
        return execute_transaction_bundle(_insert_bundle(id_, worker, seq), {"id": id_, "worker": worker, "seq": seq}, driver)

    with pytest.raises(Exception):
        transaction(ctx, lambda: [do_op(200, 2, 0), do_op(201, 2, 1)], TransactionOptions(retry_on_error=False), "sqlite")

    assert sink["begin_tx"] == 1
    assert sink["commits"] == 0
    assert sink["rolls"] == 1, f"opB failure ⇒ ONE rollback, got {sink['rolls']}"
    # opA (id=200) must ALSO have rolled back — the whole boundary is atomic.
    assert _read_rows(driver) == [], "opA must roll back when opB fails (cross-op atomicity)"


# ── (2) MUTATION RED — disabling the ambient JOIN breaks A-rolls-back-when-B-fails ──


def test_disabling_ambient_join_goes_RED(monkeypatch):
    """The teeth: the A-rolls-back-when-B-fails guarantee depends ENTIRELY on each op JOINing the
    ambient boundary. FAITHFULLY DISABLE the join — monkeypatch the runtime's ambient-tx detection
    (``current_context``) so every op believes it is OUTSIDE a boundary and opens its OWN auto-tx — and
    confirm the green outcome (1 begin_tx + 1 rollback + rows==[]) NO LONGER holds. This proves the
    ambient JOIN is load-bearing, not decorative (mirrors go TestBoundaryJoinMutationRED)."""
    import litedbmodel_runtime.runtime as rt

    # Baseline GREEN outcome (join intact) for reference.
    sink_g = _fresh_sink()
    driver_g = _make_driver(sink_g)
    driver_g.conn.execute(f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (201, 999, 9)")
    driver_g.conn.commit()
    ctx_g = context_for_driver(driver_g)

    def do_op_g(id_, worker, seq):
        return _execute_transaction_bundle(_insert_bundle(id_, worker, seq), {"id": id_, "worker": worker, "seq": seq}, driver_g, guard=False)

    with pytest.raises(Exception):
        transaction(ctx_g, lambda: [do_op_g(200, 2, 0), do_op_g(201, 2, 1)], TransactionOptions(retry_on_error=False), "sqlite")
    green = (sink_g["begin_tx"] == 1 and sink_g["rolls"] == 1 and _read_rows(driver_g) == [])
    assert green, "sanity: with the JOIN intact, opA must roll back when opB fails"

    # MUTATION: force the runtime to NEVER detect an ambient tx ⇒ every op opens its own auto-tx.
    sink_m = _fresh_sink()
    driver_m = _make_driver(sink_m)
    driver_m.conn.execute(f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (201, 999, 9)")
    driver_m.conn.commit()
    ctx_m = context_for_driver(driver_m)
    monkeypatch.setattr(rt, "current_context", lambda: None)  # disable the ambient JOIN

    def do_op_m(id_, worker, seq):
        return _execute_transaction_bundle(_insert_bundle(id_, worker, seq), {"id": id_, "worker": worker, "seq": seq}, driver_m, guard=False)

    try:
        transaction(ctx_m, lambda: [do_op_m(200, 2, 0), do_op_m(201, 2, 1)], TransactionOptions(retry_on_error=False), "sqlite")
    except Exception:
        pass
    mutated = (sink_m["begin_tx"] == 1 and sink_m["rolls"] == 1 and _read_rows(driver_m) == [])
    assert not mutated, "disabling the ambient JOIN must break the atomic green outcome (teeth: JOIN is load-bearing)"


# ── (3) write=tx GUARD ──────────────────────────────────────────────────────────


def test_guard_outside_boundary_rejects_write():
    sink = _fresh_sink()
    driver = _make_driver(sink)
    # A bare write OUTSIDE any transaction() → WriteOutsideTransactionError, nothing written.
    with pytest.raises(WriteOutsideTransactionError):
        execute_transaction_bundle(_insert_bundle(300, 3, 0), {"id": 300, "worker": 3, "seq": 0}, driver)
    assert _read_rows(driver) == []


def test_guard_read_only_inside_boundary_rejects_write():
    sink = _fresh_sink()
    driver = _make_driver(sink)
    ctx = context_for_driver(driver)
    # Read-only-scoped write inside a boundary → WriteInReadOnlyContextError (read-only first).
    from litedbmodel_runtime.exec_context import current_context, run_with_pinned_context

    def body():
        ambient = current_context()  # the pinned tx ctx
        ro = ambient.with_read_only()
        return run_with_pinned_context(ro, lambda: execute_transaction_bundle(
            _insert_bundle(301, 3, 0), {"id": 301, "worker": 3, "seq": 0}, driver))

    with pytest.raises(WriteInReadOnlyContextError):
        transaction(ctx, body, TransactionOptions(retry_on_error=False), "sqlite")


def test_guard_inside_boundary_allows_write():
    sink = _fresh_sink()
    driver = _make_driver(sink)
    ctx = context_for_driver(driver)
    r = transaction(ctx, lambda: execute_transaction_bundle(
        _insert_bundle(302, 3, 0), {"id": 302, "worker": 3, "seq": 0}, driver), TransactionOptions(), "sqlite")
    assert r["committed"] is True
    assert _read_rows(driver) == [(302, 3)]


# ── (4) NESTED transaction = one begin/commit ──────────────────────────────────


def test_nested_transaction_one_begin_commit():
    sink = _fresh_sink()
    driver = _make_driver(sink)
    ctx = context_for_driver(driver)

    def outer():
        execute_transaction_bundle(_insert_bundle(500, 5, 0), {"id": 500, "worker": 5, "seq": 0}, driver)
        # A NESTED transaction() JOINs the outer — no new begin_tx/commit.
        return transaction(ctx, lambda: execute_transaction_bundle(
            _insert_bundle(501, 5, 1), {"id": 501, "worker": 5, "seq": 1}, driver), TransactionOptions(), "sqlite")

    transaction(ctx, outer, TransactionOptions(), "sqlite")
    assert sink["begin_tx"] == 1
    assert sink["commits"] == 1
    assert sink["rolls"] == 0
    assert _read_rows(driver) == [(500, 5), (501, 5)]


def test_nested_inner_error_rolls_back_whole_tx():
    sink = _fresh_sink()
    driver = _make_driver(sink)
    driver.conn.execute(f"INSERT INTO {ISO_TBL} (id, worker, seq) VALUES (601, 999, 9)")  # collision for inner
    driver.conn.commit()
    ctx = context_for_driver(driver)

    def outer():
        execute_transaction_bundle(_insert_bundle(600, 6, 0), {"id": 600, "worker": 6, "seq": 0}, driver)
        return transaction(ctx, lambda: execute_transaction_bundle(
            _insert_bundle(601, 6, 1), {"id": 601, "worker": 6, "seq": 1}, driver), TransactionOptions(), "sqlite")

    with pytest.raises(Exception):
        transaction(ctx, outer, TransactionOptions(retry_on_error=False), "sqlite")
    assert sink["commits"] == 0
    assert sink["rolls"] == 1
    assert _read_rows(driver) == [], "an inner error rolls back the WHOLE tx (id=600 absent)"


# ── (5) rollback_only (dry-run) ────────────────────────────────────────────────


def test_rollback_only_returns_value_but_commits_nothing():
    sink = _fresh_sink()
    driver = _make_driver(sink)
    ctx = context_for_driver(driver)
    r = transaction(ctx, lambda: execute_transaction_bundle(
        _insert_bundle(700, 7, 0), {"id": 700, "worker": 7, "seq": 0}, driver),
        TransactionOptions(rollback_only=True), "sqlite")
    assert r["committed"] is True  # the body's own view: its statement ran + returned
    # …but the boundary ROLLED BACK, so nothing persisted.
    assert sink["begin_tx"] == 1
    assert sink["commits"] == 0
    assert sink["rolls"] == 1
    assert _read_rows(driver) == [], "rollback_only must commit nothing"


# ── SQLite isolation is a hard error at the boundary ───────────────────────────


def test_sqlite_isolation_request_is_a_hard_error():
    sink = _fresh_sink()
    driver = _make_driver(sink)
    ctx = context_for_driver(driver)
    with pytest.raises(ValueError):
        transaction(ctx, lambda: None, TransactionOptions(isolation=IsolationLevel.SERIALIZABLE), "sqlite")
    # The hard-error fires BEFORE any connection is acquired.
    assert sink["begin_tx"] == 0
