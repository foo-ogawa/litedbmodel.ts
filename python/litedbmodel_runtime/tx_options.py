"""litedbmodel v2 SCP — the **tx-completeness contract** (Phase B / #84, python port of
``src/scp/tx-options.ts``): the :class:`TransactionOptions` shape + defaults, the
:class:`IsolationLevel` enum + per-dialect isolation-level SQL, the retryable-error classifier, and
the write=tx guard errors (:class:`WriteOutsideTransactionError` / :class:`WriteInReadOnlyContextError`).

This is the Python mirror of the Phase B **API REFERENCE** (``tx-options.ts``, #81), matching the
rust ``tx_options.rs`` (#82) + go ``tx_options.go`` (#83): the option field names + defaults,
guard-error semantics, isolation-level→SQL mapping per dialect, retryable-error classification, and
retry-loop policy all match that contract exactly. It is dialect-neutral and driver-agnostic on
purpose: :func:`litedbmodel_runtime.exec_context.transaction` consumes it; nothing here touches a
connection. It layers the options/guards/retry/isolation on top of the Phase A
:class:`litedbmodel_runtime.exec_context.ExecutionContext` + owned-connection ownership
(``with_transaction_decided``); it does NOT re-implement connection ownership. It mirrors v1
``DBModel.ts`` (``transaction`` :2787, ``checkWriteAllowed`` :886, ``isRetryableError`` :2865) but on
the SCP seam.

## The typed-code retryable classifier is LOAD-BEARING (go #83 audit lesson)

The retryable classification extracts the driver error CODE **TYPED** — psycopg exposes the PG
``SQLSTATE`` on ``err.sqlstate``; PyMySQL exposes the MySQL errno on ``err.args[0]``. This is the
PRIMARY, driver-version-independent classifier. The go port's first attempt shipped the typed block
as DEAD CODE (the live driver error was flattened to a plain-string ``SqlFailure`` before it reached
the classifier, so only the string-substring fallback ever fired — esp. at COMMIT, where a PG 40001
write-skew or a MySQL 1213 deadlock actually surfaces).

For PARITY with rust/go the Python tx runtime maps a raw driver error into the :class:`SqlFailure`
envelope (``with_transaction_decided`` -> :func:`map_sqlite_error`, retaining the concrete error on
``.wrapped`` + ``__cause__`` -- the go ``SqlFailure.Unwrap()`` analogue) BEFORE the retry classifier
sees it. So the LIVE retry path classifies a PG 40001 / MySQL 1213 THROUGH the envelope, exactly like
go/rust: :func:`is_retryable_tx_error` TRAVERSES the wrapped chain (:func:`retryable_by_typed_code`)
to reach ``.sqlstate`` / ``.args[0]`` on the wrapped concrete error. The message-substring match is a
belt-and-suspenders FALLBACK for a type-erased error; it is EXERCISE-DISABLABLE
(``disable_retry_string_fallback``) so the live regression test can prove the typed path alone still
classifies live 40001 / 1213 (guarding against silent rot back to dead code -- the exact defect go
#83's audit caught). The chain traversal also reaches a raw driver error passed directly (the
top-of-chain case), so a mapped-envelope error and a raw error classify identically.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, List, Optional, Sequence


# ── Isolation level (the portable enum + per-dialect SQL) ─────────────────────


class IsolationLevel(Enum):
    """The three portable SQL isolation levels the tx API exposes (matching the v2 public surface).
    READ UNCOMMITTED is deliberately NOT offered — neither PG (which silently upgrades it to READ
    COMMITTED) nor a correctness-minded default wants it, and omitting it keeps the ports' enum
    identical. The value maps to the canonical SQL phrase via :meth:`phrase`.
    """

    READ_COMMITTED = "read committed"
    REPEATABLE_READ = "repeatable read"
    SERIALIZABLE = "serializable"

    def phrase(self) -> str:
        """The canonical SQL phrase for this level (e.g. ``SERIALIZABLE``). Mirrors `isolationPhrase`."""
        if self is IsolationLevel.READ_COMMITTED:
            return "READ COMMITTED"
        if self is IsolationLevel.REPEATABLE_READ:
            return "REPEATABLE READ"
        if self is IsolationLevel.SERIALIZABLE:
            return "SERIALIZABLE"
        # Fail-CLOSED on an unknown level (a corrupt/forward-incompatible value must NOT silently run
        # at the engine default — that would hide a mis-set isolation). Mirrors the tx gate's policy.
        raise ValueError(f"scp tx: unknown isolation level '{self!r}'")


def begin_statements(dialect_name: str, isolation: Optional[IsolationLevel] = None) -> List[str]:
    """The tx-start statements for ``dialect_name`` at ``isolation`` (in issue order). Per-dialect
    because the three engines express per-transaction isolation DIFFERENTLY (mirror `beginStatements`):

      - **postgres**: ``BEGIN ISOLATION LEVEL <phrase>`` — one statement, the level rides the BEGIN.
      - **mysql**: ``SET TRANSACTION ISOLATION LEVEL <phrase>`` MUST precede ``BEGIN`` (it scopes the
        very NEXT transaction only), so this returns TWO statements: the SET, then a bare ``BEGIN``.
      - **sqlite**: SQLite has NO per-transaction isolation-level knob — its isolation is a
        process/PRAGMA property (``journal_mode=WAL`` for snapshot reads), NOT a ``BEGIN`` clause. So
        an isolation request on SQLite is a HARD ERROR here (we do NOT silently drop it — that would
        fake honoring the level). Absent ``isolation`` SQLite emits a bare ``BEGIN``.

    With no ``isolation``, every dialect emits a single bare ``BEGIN`` (the Phase A behavior, byte-identical).

    NB: the actual ``BEGIN`` is issued by the driver's ``begin_tx`` (which owns the connection). The
    runtime uses :func:`isolation_prelude` (the driver-facing split) to bridge — MySQL's SET runs
    pre-BEGIN, PG's SET runs post-BEGIN as the first in-tx statement. This SQL-text form is retained
    for TS parity / the conformance-facing surface.
    """
    if isolation is None:
        return ["BEGIN"]
    phrase = isolation.phrase()
    if dialect_name == "postgres":
        return [f"BEGIN ISOLATION LEVEL {phrase}"]
    if dialect_name == "mysql":
        # The SET scopes ONLY the next tx; it must be issued before BEGIN, on the SAME connection.
        return [f"SET TRANSACTION ISOLATION LEVEL {phrase}", "BEGIN"]
    if dialect_name == "sqlite":
        raise ValueError(
            f"scp tx: SQLite does not support a per-transaction isolation level ('{phrase}'). "
            "SQLite isolation is a connection/PRAGMA property (e.g. journal_mode=WAL for snapshot "
            "reads), not a BEGIN clause — set it on the connection, and omit "
            "TransactionOptions.isolation for SQLite."
        )
    raise ValueError(f"scp tx: unknown dialect '{dialect_name}'")


def isolation_prelude(dialect_name: str, isolation: Optional[IsolationLevel] = None):
    """Split the isolation prelude into (before-BEGIN, after-BEGIN) statement lists the driver runs
    around its own ``BEGIN`` (Phase B / #84). This is the DRIVER-facing form of :func:`begin_statements`:
    because each driver's ``begin_tx`` issues the plain ``BEGIN`` itself, the isolation SET is
    delivered as prelude statements it runs BEFORE (MySQL — the SET scopes the next tx) or AFTER
    (Postgres — the SET is valid as the first in-tx statement) that ``BEGIN``.

      - **postgres**: ``([], ["SET TRANSACTION ISOLATION LEVEL <phrase>"])`` — runs post-BEGIN.
      - **mysql**: ``(["SET TRANSACTION ISOLATION LEVEL <phrase>"], [])`` — runs pre-BEGIN.
      - **sqlite** with isolation: a HARD ERROR (no per-tx level); no isolation ⇒ both empty.

    No isolation ⇒ ``([], [])`` for every dialect (the Phase A bare ``BEGIN``, byte-identical).
    Mirrors rust ``isolation_prelude`` / go ``isolationPrelude``.
    """
    if isolation is None:
        return [], []
    set_stmt = f"SET TRANSACTION ISOLATION LEVEL {isolation.phrase()}"
    if dialect_name == "postgres":
        return [], [set_stmt]
    if dialect_name == "mysql":
        return [set_stmt], []
    if dialect_name == "sqlite":
        raise ValueError(
            f"scp tx: SQLite does not support a per-transaction isolation level ('{isolation.phrase()}'). "
            "SQLite isolation is a connection/PRAGMA property (e.g. journal_mode=WAL for snapshot "
            "reads), not a BEGIN clause — set it on the connection, and omit "
            "TransactionOptions.isolation for SQLite."
        )
    raise ValueError(f"scp tx: unknown dialect '{dialect_name}'")


# ── TransactionOptions (the Phase B public option shape) ──────────────────────


class TransactionOptions:
    """Options for a transaction (the Phase B contract; mirrors v1 ``TransactionOptions`` plus the new
    ``isolation``, and the TS/rust/go ``TransactionOptions``). Every field is optional with a stable
    default; the field names + defaults match the other ports EXACTLY (retry_on_error=True,
    retry_limit=3, retry_duration=200 ms, rollback_only=False, isolation=None). NB this DIFFERS from
    the v1 defaults (retry OFF, 100 ms) — the v2 Phase B contract (``tx-options.ts``) is the SSoT the
    5 ports mirror, and it defaults retry ON with a 200 ms base.
    """

    __slots__ = ("isolation", "retry_on_error", "retry_limit", "retry_duration", "rollback_only")

    def __init__(
        self,
        isolation: Optional[IsolationLevel] = None,
        retry_on_error: bool = True,
        retry_limit: int = 3,
        retry_duration: int = 200,
        rollback_only: bool = False,
    ) -> None:
        #: Per-transaction isolation level. Issued via :func:`isolation_prelude` on the tx-owned
        #: connection (PG: post-BEGIN SET; MySQL: a preceding SET). ``None`` ⇒ the engine default.
        #: SQLite has no per-tx level ⇒ passing this on SQLite is an error.
        self.isolation = isolation
        #: Retry the whole tx on a retryable error (deadlock / serialization / connection). Default True.
        self.retry_on_error = retry_on_error
        #: Max attempts before giving up (the FIRST try counts as attempt 1). Default 3.
        self.retry_limit = retry_limit
        #: Backoff base in ms; attempt k waits ``retry_duration * 2^(k-1)`` (exponential). Default 200.
        self.retry_duration = retry_duration
        #: ROLLBACK instead of COMMIT at the end of a SUCCESSFUL body (dry-run / preview): the body
        #: runs and its result is returned, but NO change is committed. A body error still ROLLBACKs +
        #: re-raises as usual. Default False.
        self.rollback_only = rollback_only


# ── Retryable-error classification (per dialect) ──────────────────────────────

#: When True, :func:`is_retryable_tx_error` SKIPS the message-substring FALLBACK so the PRIMARY
#: typed-code path (``.sqlstate`` / ``.args[0]`` traversed through the wrapped driver error) must
#: stand on its own. It exists ONLY for the live regression test that proves the typed extraction is
#: genuinely load-bearing (guarding against the typed block silently rotting back to dead code — the
#: defect go #83's audit caught); production never sets it. Module-level (not concurrency-safe) — the
#: regression test flips it serially.
disable_retry_string_fallback = False


def _iter_error_chain(error: BaseException):
    """Yield ``error`` then every wrapped/caused error reachable via ``.wrapped`` / ``__cause__`` /
    ``__context__`` — so the typed classifier reaches a concrete psycopg / PyMySQL error even when it
    was mapped into a :class:`SqlFailure` at COMMIT time. Bounded + cycle-guarded."""
    seen = set()
    cur: Optional[BaseException] = error
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        yield cur
        # ``.wrapped`` is the explicit retained driver error (go Unwrap() analogue); __cause__ is set
        # by ``raise map(...) from e``; __context__ catches an implicit chain.
        nxt = getattr(cur, "wrapped", None)
        if nxt is None:
            nxt = cur.__cause__
        if nxt is None:
            nxt = cur.__context__
        cur = nxt if isinstance(nxt, BaseException) else None


def _pg_sqlstate(err: BaseException) -> Optional[str]:
    """The PG SQLSTATE for ``err`` if it is a psycopg error (``err.sqlstate``, a 5-char string), else
    ``None``. TYPED extraction — NOT a string match. psycopg attaches ``.sqlstate`` to every server
    error (including one raised at COMMIT, e.g. a SERIALIZABLE write-skew 40001)."""
    state = getattr(err, "sqlstate", None)
    return state if isinstance(state, str) and len(state) == 5 else None


def _mysql_errno(err: BaseException) -> Optional[int]:
    """The MySQL errno for ``err`` if it is a PyMySQL error (``err.args[0]``, an int), else ``None``.
    TYPED extraction — NOT a string match. PyMySQL raises ``MySQLError`` subclasses whose first arg is
    the numeric errno (e.g. 1213 ER_LOCK_DEADLOCK on an InnoDB deadlock at statement time)."""
    args = getattr(err, "args", None)
    if isinstance(args, tuple) and args and isinstance(args[0], int):
        # Guard against a bool (a bool is an int subclass) and out-of-range noise.
        if not isinstance(args[0], bool) and 1000 <= args[0] <= 65535:
            return args[0]
    return None


def retryable_by_typed_code(error: BaseException) -> bool:
    """Does any error in ``error``'s wrapped chain carry a retryable PG SQLSTATE (40001/40P01) or
    MySQL errno (1213/1205)? This is the PRIMARY, driver-version-independent classifier — it does NOT
    string-match. It is the load-bearing live path: a PG 40001 raised at COMMIT is mapped into a
    :class:`SqlFailure` whose ``.wrapped`` / ``__cause__`` re-exposes the concrete psycopg error, and
    this reaches its ``.sqlstate``. Mirrors go ``retryableByTypedCode`` (``errors.As`` on the concrete
    driver error via ``SqlFailure.Unwrap()``)."""
    for e in _iter_error_chain(error):
        state = _pg_sqlstate(e)
        if state == "40001" or state == "40P01":  # serialization_failure / deadlock_detected
            return True
        errno = _mysql_errno(e)
        if errno == 1213 or errno == 1205:  # ER_LOCK_DEADLOCK / ER_LOCK_WAIT_TIMEOUT
            return True
    return False


def is_connection_error(error: BaseException) -> bool:
    """Is ``error`` a broken/stale connection (retryable via reconnect)? A message/-code heuristic
    matching ``src/connection-errors.ts`` (``isConnectionError``) plus the psycopg/PyMySQL
    connection-closed phrasings. A dropped/reset/refused connection reconnects on the next attempt (a
    fresh pooled connection). Mirrors rust/go ``is_connection_error``."""
    m = str(error) or ""
    return (
        "Connection terminated" in m
        or "Client has encountered a connection error" in m
        or "ECONNRESET" in m
        or "ECONNREFUSED" in m
        or "Connection lost" in m
        or "This socket has been ended by the other party" in m
        or "EPIPE" in m
        or "PROTOCOL_CONNECTION_LOST" in m
        # psycopg / PyMySQL connection-closed phrasings.
        or "connection closed" in m
        or "connection was closed" in m
        or "server closed the connection" in m
        or "Lost connection to" in m
        or "MySQL server has gone away" in m
        or "the connection is closed" in m
    )


def is_retryable_tx_error(error: BaseException) -> bool:
    """Is ``error`` a RETRYABLE transaction failure — a deadlock, a serialization failure, or a broken
    connection — for which re-running the whole transaction can succeed? Classification is by the
    driver's stable error CODE first (PG SQLSTATE via psycopg ``.sqlstate``, MySQL errno via PyMySQL
    ``.args[0]``, traversed through the wrapped chain), with the v1 message substrings as a
    FALLBACK (mirrors TS ``isRetryableTxError`` / rust / go). A data conflict (unique/FK/check) is NOT
    retryable — re-running would fail identically.

    Codes (per dialect):
      - **postgres** SQLSTATE: ``40001`` serialization_failure, ``40P01`` deadlock_detected.
      - **mysql** errno: ``1213`` ER_LOCK_DEADLOCK, ``1205`` ER_LOCK_WAIT_TIMEOUT.
      - **connection errors** (either dialect): via :func:`is_connection_error`.

    The typed-code extraction (:func:`retryable_by_typed_code`, traversing the wrapped chain to the
    concrete psycopg/PyMySQL error) is the PRIMARY, load-bearing mechanism — NOT dead code behind the
    string match. The string fallback is EXERCISE-DISABLABLE (:data:`disable_retry_string_fallback`)
    so the live regression proves the typed path alone catches PG 40001 + MySQL 1213 (the go #83 audit
    lesson).
    """
    if error is None:
        return False
    if is_connection_error(error):
        return True
    # PRIMARY: stable CODE via the concrete driver error type (reachable through the wrapped chain).
    if retryable_by_typed_code(error):
        return True
    # FALLBACK: the v1 message substrings (driver-version-independent phrasing) — belt-and-suspenders
    # for a type-erased error. Disablable in tests to prove the typed path is load-bearing.
    if disable_retry_string_fallback:
        return False
    m = str(error) or ""
    return (
        "40001" in m
        or "40P01" in m
        or "1213" in m
        or "1205" in m
        or "The transaction might succeed if retried" in m
        or "try restarting transaction" in m
        or "could not serialize access due to concurrent update" in m
        or "could not serialize access" in m
        or "Deadlock found" in m
        or "deadlock detected" in m
        or "Lock wait timeout exceeded" in m
    )


# ── write=tx guards (mirror v1 `checkWriteAllowed`, DBModel.ts:886) ────────────


class WriteOutsideTransactionError(Exception):
    """The write=tx guard error: a write issued OUTSIDE a :func:`transaction` boundary. Mirrors v1
    ``WriteOutsideTransactionError`` (the TS / rust / go analogue). Carries a stable ``kind`` so the
    runtime surfaces it uniformly."""

    kind = "write_outside_transaction"

    def __init__(self, operation: str, model: Optional[str] = None) -> None:
        self.operation = operation
        self.model = model
        super().__init__(f'Write operation "{operation}" on {model or ""} requires a transaction')


class WriteInReadOnlyContextError(Exception):
    """The write=tx guard error: a write issued in a READ-ONLY scope. Mirrors v1
    ``WriteInReadOnlyContextError`` (the TS / rust / go analogue)."""

    kind = "write_in_read_only_context"

    def __init__(self, operation: str, model: Optional[str] = None) -> None:
        self.operation = operation
        self.model = model
        super().__init__(f'Write operation "{operation}" on {model or ""} is not allowed in read-only context')


def check_write_allowed(operation: str, model: Optional[str], in_transaction: bool, read_only: bool) -> None:
    """Enforce the write=tx guard (mirror v1 ``DBModel._checkWriteAllowed``, DBModel.ts:886-896, + the
    TS ``checkWriteAllowed`` / rust / go): a write in a read-only scope → :class:`WriteInReadOnlyContextError`;
    a write with NO active transaction → :class:`WriteOutsideTransactionError`. Called at every write
    ENTRY (create/update/delete/upsert/batch) BEFORE any SQL. The order matches v1: read-only is
    checked FIRST (the more specific rejection).

    ``in_transaction`` / ``read_only`` come from the AMBIENT contextvar-propagated
    :class:`ExecutionContext` (the Python analogue of the TS async-local markers): a write inside
    :func:`transaction` runs with a tx-scoped ctx (``in_transaction=True``), a bare write with no
    ambient tx ctx (``False``).
    """
    if read_only:
        raise WriteInReadOnlyContextError(operation, model)
    if not in_transaction:
        raise WriteOutsideTransactionError(operation, model)
