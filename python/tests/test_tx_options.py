"""Phase B (#84, python) — UNIT tests for the tx-completeness contract (no live DB).

Mirrors the rust ``tx_options.rs`` #[cfg(test)] + go ``TestBeginStatements`` etc.: the option
defaults, the per-dialect isolation SQL (+ the SQLite hard-error), the retryable-error classifier
(with the TYPED-code path proven load-bearing via a synthetic psycopg/PyMySQL-shaped error reachable
through a mapped ``SqlFailure`` — the go #83 audit lesson), and the write=tx guard order.
"""

from __future__ import annotations

import pytest

from litedbmodel_runtime.errors import SqlFailure, map_sqlite_error
from litedbmodel_runtime import tx_options as T
from litedbmodel_runtime.tx_options import (
    IsolationLevel,
    TransactionOptions,
    WriteInReadOnlyContextError,
    WriteOutsideTransactionError,
    begin_statements,
    check_write_allowed,
    is_retryable_tx_error,
    isolation_prelude,
    retryable_by_typed_code,
)


# ── Defaults ───────────────────────────────────────────────────────────────────


def test_defaults_match_the_phase_b_contract():
    o = TransactionOptions()
    assert o.retry_on_error is True
    assert o.retry_limit == 3
    assert o.retry_duration == 200
    assert o.rollback_only is False
    assert o.isolation is None


# ── Per-dialect isolation SQL ──────────────────────────────────────────────────


def test_begin_statements_per_dialect():
    assert begin_statements("postgres", None) == ["BEGIN"]
    assert begin_statements("postgres", IsolationLevel.SERIALIZABLE) == ["BEGIN ISOLATION LEVEL SERIALIZABLE"]
    assert begin_statements("mysql", IsolationLevel.REPEATABLE_READ) == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "BEGIN",
    ]
    assert begin_statements("mysql", None) == ["BEGIN"]


def test_sqlite_isolation_is_a_hard_error():
    with pytest.raises(ValueError):
        begin_statements("sqlite", IsolationLevel.SERIALIZABLE)
    # …but a bare BEGIN with no isolation is fine (the Phase A path).
    assert begin_statements("sqlite", None) == ["BEGIN"]


def test_isolation_prelude_split():
    # PG: the SET runs AFTER BEGIN (first in-tx statement).
    assert isolation_prelude("postgres", IsolationLevel.SERIALIZABLE) == ([], ["SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"])
    # MySQL: the SET runs BEFORE BEGIN (it scopes the next tx).
    assert isolation_prelude("mysql", IsolationLevel.REPEATABLE_READ) == (["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"], [])
    # No isolation ⇒ both empty for every dialect.
    assert isolation_prelude("postgres", None) == ([], [])
    assert isolation_prelude("mysql", None) == ([], [])
    with pytest.raises(ValueError):
        isolation_prelude("sqlite", IsolationLevel.READ_COMMITTED)


def test_unknown_dialect_fails_closed():
    with pytest.raises(ValueError):
        begin_statements("oracle", IsolationLevel.SERIALIZABLE)


# ── Retryable classification — TYPED codes are load-bearing ────────────────────


class _FakePgError(Exception):
    """A psycopg-shaped error: carries a 5-char ``sqlstate`` (the TYPED PG classifier reads this)."""

    def __init__(self, sqlstate, message="pg error"):
        super().__init__(message)
        self.sqlstate = sqlstate


class _FakeMysqlError(Exception):
    """A PyMySQL-shaped error: ``args[0]`` is the numeric errno (the TYPED MySQL classifier reads this)."""

    def __init__(self, errno, message="mysql error"):
        super().__init__(errno, message)


def test_typed_pg_sqlstate_classifies_retryable():
    assert is_retryable_tx_error(_FakePgError("40001"))  # serialization_failure
    assert is_retryable_tx_error(_FakePgError("40P01"))  # deadlock_detected
    # A data conflict (unique) is NOT retryable.
    assert not is_retryable_tx_error(_FakePgError("23505"))


def test_typed_mysql_errno_classifies_retryable():
    assert is_retryable_tx_error(_FakeMysqlError(1213))  # ER_LOCK_DEADLOCK
    assert is_retryable_tx_error(_FakeMysqlError(1205))  # ER_LOCK_WAIT_TIMEOUT
    assert not is_retryable_tx_error(_FakeMysqlError(1062))  # ER_DUP_ENTRY — not retryable


def test_connection_error_is_retryable():
    assert is_retryable_tx_error(RuntimeError("server closed the connection unexpectedly"))
    assert is_retryable_tx_error(RuntimeError("Lost connection to MySQL server during query"))


def test_non_retryable_unrelated_error():
    assert not is_retryable_tx_error(RuntimeError("some unrelated driver error"))


def test_typed_path_reachable_through_mapped_sqlfailure_string_fallback_DISABLED():
    """The go #83 audit lesson, python: a live psycopg/PyMySQL error mapped into a ``SqlFailure`` at
    COMMIT time must STILL classify retryable via the TYPED code alone — with the string fallback
    DISABLED. This proves ``.sqlstate`` / ``.args[0]`` is reachable through the mapped failure's
    wrapped chain (``.wrapped`` / ``__cause__``), i.e. the typed path is NOT dead code behind the
    string match.
    """
    # map_sqlite_error flattens the concrete driver error's TEXT into the SqlFailure message but RETAINS
    # the concrete error on `.wrapped` (+ __cause__) — exactly what the classifier traverses.
    mapped_pg = map_sqlite_error(_FakePgError("40001", "could not serialize access (SQLSTATE 40001)"))
    mapped_my = map_sqlite_error(_FakeMysqlError(1213, "Deadlock found when trying to get lock"))
    assert isinstance(mapped_pg, SqlFailure) and mapped_pg.wrapped is not None
    assert isinstance(mapped_my, SqlFailure) and mapped_my.wrapped is not None

    # The TYPED classifier reaches the wrapped concrete error even through the mapped failure.
    assert retryable_by_typed_code(mapped_pg)
    assert retryable_by_typed_code(mapped_my)

    # Now DISABLE the string fallback and prove the mapped failures STILL classify retryable — the
    # typed path stands ALONE (if the wrapped chain were broken this would go False → the retry loop
    # would give up, the exact dead-code regression go #83 caught).
    T.disable_retry_string_fallback = True
    try:
        assert is_retryable_tx_error(mapped_pg), "typed PG 40001 must classify with string fallback OFF"
        assert is_retryable_tx_error(mapped_my), "typed MySQL 1213 must classify with string fallback OFF"
        # A mapped NON-retryable driver error (a unique collision) stays non-retryable under typed-only.
        mapped_dup = map_sqlite_error(_FakePgError("23505", "duplicate key value violates unique constraint"))
        assert not is_retryable_tx_error(mapped_dup)
    finally:
        T.disable_retry_string_fallback = False


def test_string_fallback_alone_catches_typeerased_error():
    """A type-ERASED failure (no ``.sqlstate`` / ``.args[0]`` — e.g. a re-constructed plain error whose
    only signal is the driver text) is still caught by the belt-and-suspenders string fallback."""
    e = RuntimeError("ERROR: could not serialize access due to concurrent update")
    assert is_retryable_tx_error(e)  # string fallback ON (default)
    T.disable_retry_string_fallback = True
    try:
        assert not is_retryable_tx_error(e)  # typed-only: no code to extract ⇒ not classified
    finally:
        T.disable_retry_string_fallback = False


# ── write=tx guard order ───────────────────────────────────────────────────────


def test_guard_order_read_only_first():
    # Read-only is rejected FIRST (more specific), even with no active tx.
    with pytest.raises(WriteInReadOnlyContextError):
        check_write_allowed("INSERT", "users", in_transaction=False, read_only=True)
    # No active tx (not read-only) → outside-transaction.
    with pytest.raises(WriteOutsideTransactionError):
        check_write_allowed("INSERT", "users", in_transaction=False, read_only=False)
    # Inside a tx (not read-only) → allowed (no raise).
    check_write_allowed("INSERT", "users", in_transaction=True, read_only=False)


def test_guard_errors_carry_kind():
    assert WriteOutsideTransactionError("INSERT", "users").kind == "write_outside_transaction"
    assert WriteInReadOnlyContextError("INSERT", "users").kind == "write_in_read_only_context"
