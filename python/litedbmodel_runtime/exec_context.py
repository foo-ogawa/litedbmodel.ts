"""litedbmodel v2 SCP — the **ExecutionContext + central execute/run seam** (Phase A / #78, python).

The Python port of the TS contract-defining artifact ``src/scp/exec-context.ts`` (#75), mirroring the
rust port ``rust/litedbmodel_runtime/src/exec_context.rs`` (#76) and the go port
``go/litedbmodel_runtime/exec_context.go`` (#77). It replaces the raw ``driver: Driver`` threaded
through ``execute_bundle`` / ``execute_read_graph`` / the relation walker / ``execute_transaction_bundle``
with an :class:`ExecutionContext` that carries:

  1. a **connection provider** — :meth:`ExecutionContext.connection_for` ``(intent)`` resolves WHICH
     connection a statement runs on (the tx-owned connection, else the primary driver; Phase A wires
     only the tx-owned + single-DB cases, reader/writer/named-DB are B/C/D on this seam);
  2. a **middleware chain** — :attr:`ExecutionContext.middleware`, wrapping every SQL (empty in
     Phase A = passthrough; the registration API is Phase D — this is only the hook point);
  3. a **pinned tx connection** — a tx-scoped ctx pins ONE owned connection so every statement in a
     transaction body runs on it (per-execution connection ownership, §3).

## The central seam (§2) — ALL SQL funnels through here

::

    execute(ctx, sql, params) -> Rows      # SELECT / RETURNING reads
    run(ctx, sql, params)     -> RunInfo   # INSERT/UPDATE/DELETE, BEGIN/COMMIT/ROLLBACK

Both do the SAME three things, in order:
  ① run the middleware chain (empty ⇒ passthrough, behavior unchanged);
  ② resolve the connection via ``ctx.connection_for(intent)``;
  ③ execute on that connection (the ONLY driver contact point).

Every direct ``driver.prepare(sql).all()/run()`` in the read / tx / relation path is replaced by a
call through this seam. A ``grep`` for ``.prepare(`` outside the connection adapters (this module's
:class:`DriverConnection` and the driver ``begin_tx`` handles) comes up empty in the runtime SQL
path — that is the AC.

## ONE interface, not sync/async-bifurcated (contract flag)

The TS reference bifurcates into ``ExecutionContext``/``execute`` (better-sqlite3, sync) and
``AsyncExecutionContext``/``executeAsync`` (pg/mysql2, async) because that split is TS-runtime
specific. Per the #78 contract flags the Python port **collapses to ONE interface**: Python's DB-API
is synchronous (stdlib ``sqlite3`` / psycopg / PyMySQL all block), so there is exactly ONE
:class:`ExecutionContext` / one :func:`execute` / one :func:`run`. There is likewise **no
``executeSafeIntegers``** — that is a better-sqlite3 #59 BIGINT toggle; bc-Python evaluates integers
to a plain unbounded ``int`` and PG/MySQL return BIGINT as ``int``/``str`` natively, so the read seam
is a plain execute.

## Per-execution connection ownership (§3) — the concurrent-tx fix

A transaction acquires ONE connection via :meth:`Driver.begin_tx` (a :class:`TxConnection` owned
handle — the Python analogue of v1 ``PoolTransaction`` / go's ``*sql.Tx``), pins it into a tx-scoped
:class:`ExecutionContext` **propagated via** :data:`contextvars.ContextVar` (§3), runs its body (every
statement resolves that connection via ``connection_for``), COMMITs/ROLLBACKs on the SAME owned
connection, and releases it (back to the pool, or destroyed if poisoned). Concurrent transactions —
on distinct threads, each with its OWN ``contextvars`` copy — acquire DISTINCT pooled connections ⇒
isolated. There is NO driver-global single-slot writer (the removed ``_PooledDriver._writer`` slot on
the PG/MySQL drivers was exactly the shared-slot model that corrupts concurrent transactions).
"""

from __future__ import annotations

import contextvars
import time
from typing import Any, Callable, List, Optional, Sequence, TypeVar, Union

from .driver import Driver, RunInfo, TxConnection
from .tx_options import (
    IsolationLevel,
    TransactionOptions,
    WriteInReadOnlyContextError,
    WriteOutsideTransactionError,
    check_write_allowed,
    is_retryable_tx_error,
    isolation_prelude,
)

Rows = List[Any]
_T = TypeVar("_T")


# ── Statement intent (§5) ──────────────────────────────────────────────────────


class StatementIntent:
    """What a statement needs from the connection provider (§3): whether it writes (so it must go to
    a writer / the tx-owned connection, never a read replica) and an optional named DB (multi-DB
    routing, Phase B). Phase A resolves only ``write`` (tx-owned vs. primary) and ignores ``db``
    (single DB); the field is in the contract now so B/C/D extend the resolver — not the seam.
    """

    __slots__ = ("write", "db")

    def __init__(self, write: bool = False, db: Optional[str] = None) -> None:
        self.write = write
        self.db = db

    @classmethod
    def read(cls) -> "StatementIntent":
        """A read intent (``write=False``, primary DB)."""
        return cls(write=False)

    @classmethod
    def write_(cls) -> "StatementIntent":
        """A write intent (``write=True``, primary DB)."""
        return cls(write=True)


# The shared, immutable Phase A intents (no per-call allocation).
READ_INTENT = StatementIntent.read()
WRITE_INTENT = StatementIntent.write_()


# ── The ONE driver contact point (§5) — a Connection ──────────────────────────


class Connection:
    """The ONE driver contact point (§5): a resolved connection a statement runs on. Outside a tx
    this is the primary :class:`Driver` (each ``prepare`` = one pooled connection per call for a live
    driver; the SAME in-proc connection for SQLite); inside a tx it is the tx-owned
    :class:`TxConnection` handle. The seam is the ONLY caller; the runtime SQL path never touches a
    ``driver.prepare(...)`` directly.
    """

    def execute(self, sql: str, params: Sequence[Any]) -> Rows:
        """Run a SELECT / RETURNING statement; return the raw rows."""
        raise NotImplementedError

    def run(self, sql: str, params: Sequence[Any]) -> RunInfo:
        """Run a non-returning write / DDL / tx-control statement; return the affected summary."""
        raise NotImplementedError


class DriverConnection(Connection):
    """Adapt a raw :class:`Driver` to the :class:`Connection` seam (the ONE driver contact for the
    non-tx path). ``execute``/``run`` are the SAME ``driver.prepare(sql).all()/run()`` the runtime
    used directly before the seam — so a ctx built via :func:`context_for_driver` is byte-identical to
    the old raw-driver path (the backward-compat wrapper, §6). It is the ONE place a
    ``driver.prepare()`` is issued on the non-tx path.
    """

    __slots__ = ("_driver",)

    def __init__(self, driver: Driver) -> None:
        self._driver = driver

    def execute(self, sql: str, params: Sequence[Any]) -> Rows:
        return self._driver.prepare(sql).all(params)

    def run(self, sql: str, params: Sequence[Any]) -> RunInfo:
        return self._driver.prepare(sql).run(params)


class _TxConnectionAdapter(Connection):
    """A :class:`Connection` view over a tx's OWNED :class:`TxConnection` handle. The seam resolves
    this (via ``connection_for``) for every statement inside a tx, so all of them run on the SAME
    owned connection. Concurrent transactions each hold a DISTINCT handle over a DISTINCT pooled
    connection, so their writes never cross-talk — the isolation the removed driver-global ``_writer``
    slot violated.
    """

    __slots__ = ("_tx",)

    def __init__(self, tx: TxConnection) -> None:
        self._tx = tx

    def execute(self, sql: str, params: Sequence[Any]) -> Rows:
        return self._tx.all(sql, params)

    def run(self, sql: str, params: Sequence[Any]) -> RunInfo:
        return self._tx.run(sql, params)


# ── Middleware chain (§4) — the hook point (empty in Phase A) ──────────────────

# The terminal of a middleware chain: resolve the connection + execute (the seam's ②③).
SeamNext = Callable[[str, Sequence[Any]], _T]
# One middleware: wrap a statement, delegating to `next` (Phase D supplies the registration API).
Middleware = Callable[[str, Sequence[Any], "SeamNext[Any]"], Any]


class MiddlewareChain:
    """The ordered middleware chain a ctx carries (§4). :meth:`wrap` folds the middlewares around
    ``next`` (the connection-resolve + execute terminal). An EMPTY chain is a pure passthrough —
    ``wrap`` returns ``next(sql, params)`` verbatim, so Phase A behavior is byte-identical. The chain
    is generic over the seam result type so ONE shape serves both the read (``Rows``) and write
    (``RunInfo``) seams. Phase A always constructs an empty chain; the registration API + native
    middleware entries are Phase D (this is only the hook point).
    """

    __slots__ = ("_stack",)

    def __init__(self, stack: Optional[Sequence[Middleware]] = None) -> None:
        self._stack: List[Middleware] = list(stack) if stack else []

    @property
    def is_empty(self) -> bool:
        """Is the chain empty (⇒ ``wrap`` is a guaranteed passthrough)?"""
        return len(self._stack) == 0

    def wrap(self, sql: str, params: Sequence[Any], next_: "SeamNext[_T]") -> _T:
        """Fold the chain around ``next_``, then invoke it. Empty ⇒ ``next_(sql, params)`` verbatim."""
        if not self._stack:
            return next_(sql, params)
        fn: SeamNext[Any] = next_
        for i in range(len(self._stack) - 1, -1, -1):
            mw = self._stack[i]
            inner = fn

            def wrapped(s: str, p: Sequence[Any], _mw: Middleware = mw, _inner: SeamNext[Any] = inner) -> Any:
                return _mw(s, p, _inner)

            fn = wrapped
        return fn(sql, params)


# The shared empty (Phase A) middleware chain — a passthrough so :func:`context_for_driver` can build
# a ctx without the caller owning a chain. Phase D swaps this for a per-ctx registered chain.
_EMPTY_CHAIN = MiddlewareChain()


# ── The ExecutionContext (§2 / §5) — ONE interface ────────────────────────────


class ExecutionContext:
    """The execution context threaded through ``execute_bundle`` / ``execute_read_graph`` / the
    relation walker / ``execute_transaction_bundle`` in place of a raw :class:`Driver`. It carries the
    connection provider (the primary driver + an optional pinned tx connection), the middleware chain,
    and derives a tx-scoped ctx via :meth:`with_connection`.

    ctx propagation (§3, Python-idiomatic) is via a module-level :data:`contextvars.ContextVar`: a
    transaction pins its owned connection into a derived ctx and runs the body inside
    :func:`run_with_pinned_context`, so a concurrent execution scope (a distinct thread) sees its OWN
    contextvar value — its own tx connection — never another's.
    """

    __slots__ = ("_driver", "middleware", "_pinned", "_read_only")

    def __init__(
        self,
        driver: Driver,
        middleware: MiddlewareChain,
        pinned: Optional[Connection] = None,
        read_only: bool = False,
    ) -> None:
        self._driver = driver
        self.middleware = middleware
        # The pinned tx connection (present ⇒ this is a tx-scoped ctx; every statement resolves it).
        self._pinned = pinned
        # The READ-ONLY marker (Phase B / #84 write=tx guard — mirror v1 `withWriter` / the TS
        # `withReadOnly` ALS marker / rust/go `read_only`): a write in a read-only-scoped ctx is
        # REJECTED (WriteInReadOnlyContextError). Derived via `with_read_only`.
        self._read_only = read_only

    @property
    def driver(self) -> Driver:
        """The primary driver (for the pooled read fan-out, which needs a thread-shared driver — the
        tx path never fans out, so this is the non-tx provider)."""
        return self._driver

    def in_transaction(self) -> bool:
        """Is this a tx-scoped ctx (a pinned connection is present)? This is the Python analogue of
        the TS async-local "inside a transaction" marker — the write=tx guard reads it (via the
        ambient contextvar), and the public :func:`transaction` boundary reads it for NESTED-tx join
        detection."""
        return self._pinned is not None

    def read_only(self) -> bool:
        """Is this a READ-ONLY-scoped ctx (Phase B / #84 write=tx guard)? A write here is REJECTED
        (:class:`WriteInReadOnlyContextError`). Derived via :meth:`with_read_only`."""
        return self._read_only

    def with_read_only(self) -> "ExecutionContext":
        """Derive a READ-ONLY-scoped ctx (mirror v1 ``withWriter`` / the TS ``withReadOnly`` / rust/go
        ``with_read_only``): reads are allowed, but ANY write funneled through the GUARDED write seam
        (:func:`run_guarded` / a guarded ``execute_transaction_bundle``) is rejected with
        :class:`WriteInReadOnlyContextError`. A tx-scoped ctx INHERITS its pinned connection + driver +
        middleware; a Transaction() opened inside a read-only scope stays read-only (v1 parity)."""
        return ExecutionContext(self._driver, self.middleware, self._pinned, read_only=True)

    def connection_for(self, intent: StatementIntent = READ_INTENT) -> Connection:
        """Resolve WHICH connection a statement runs on (§3). Phase A resolution: the tx-owned
        (pinned) connection wins; else the primary driver. Reader/writer split (§3-2/3) + named-DB
        routing (§3-4) extend HERE in B/C/D — the seam does not change.
        """
        if self._pinned is not None:
            return self._pinned
        return DriverConnection(self._driver)

    def with_connection(self, conn: Connection, tx: bool) -> "ExecutionContext":
        """Derive a tx-scoped ctx pinning ``conn`` (every statement resolves it while ``tx`` is True).
        The derived ctx shares the primary driver + middleware chain, and INHERITS the read-only
        marker (a tx opened inside a read-only scope stays read-only — v1 parity). This is the Python
        analogue of the TS ``withConnection(conn, tx)`` / go ``WithTxConnection`` / rust
        ``with_tx_connection``.
        """
        return ExecutionContext(self._driver, self.middleware, conn if tx else None, read_only=self._read_only)


# ── ctx propagation (§3) — the Python-idiomatic contextvars slot ──────────────

# Per-execution-scope ambient ctx: the ContextVar carrying the tx-scoped ExecutionContext (§3). A
# concurrent scope (a distinct thread) sees its OWN value; the base value (outside a tx) is None.
_ctx_var: "contextvars.ContextVar[Optional[ExecutionContext]]" = contextvars.ContextVar(
    "litedbmodel_scp_exec_context", default=None
)


def current_context() -> Optional[ExecutionContext]:
    """The ambient (contextvar-propagated) ExecutionContext of THIS execution scope, or ``None``
    outside a pinned tx scope. The seam consults it so a callee that only has the raw driver still
    resolves the tx-owned connection when it runs inside a :func:`run_with_pinned_context` scope."""
    return _ctx_var.get()


def run_with_pinned_context(ctx: ExecutionContext, fn: Callable[[], _T]) -> _T:
    """Run ``fn`` with ``ctx`` pinned as the ambient contextvar for THIS scope (§3). Every implicit
    ``current_context()`` inside ``fn`` returns ``ctx``. Restores the prior value on exit. This is the
    Python per-execution-ownership mechanism (v1 TS ``txContext.run`` analogue); it is thread-safe by
    construction — ``contextvars`` are per-thread, so a concurrent thread's pin never leaks here.
    """
    token = _ctx_var.set(ctx)
    try:
        return fn()
    finally:
        _ctx_var.reset(token)


# ── The central seam (§2) — the ONLY place SQL meets a connection ─────────────


def execute(ctx: ExecutionContext, sql: str, params: Sequence[Any], intent: StatementIntent = READ_INTENT) -> Rows:
    """Central READ seam: ① middleware chain, ② resolve the connection, ③ execute. Every read
    (primary read node, relation batch, tx-body SELECT/RETURNING) funnels through here."""
    conn = ctx.connection_for(intent)
    return ctx.middleware.wrap(sql, params, lambda s, p: conn.execute(s, p))


def run(ctx: ExecutionContext, sql: str, params: Sequence[Any], intent: StatementIntent = WRITE_INTENT) -> RunInfo:
    """Central WRITE seam: ① middleware chain, ② resolve the connection, ③ run. Every write and every
    tx-control statement (BEGIN/COMMIT/ROLLBACK on the non-tx driver path) funnels through here."""
    conn = ctx.connection_for(intent)
    return ctx.middleware.wrap(sql, params, lambda s, p: conn.run(s, p))


# ── Backward-compat wrappers (§6) ──────────────────────────────────────────────


def context_for_driver(driver: Driver) -> ExecutionContext:
    """**Backward-compat wrapper (§6).** Wrap a raw :class:`Driver` in a thin
    :class:`ExecutionContext`: reader = writer = the same driver, an EMPTY middleware chain, a single
    DB, no pinned tx connection. Existing callers (conformance / livedb / bench / unit that pass a raw
    driver) keep working **byte-identically** — the seam is a pure passthrough to
    ``driver.prepare(...).all()/run()``. This is the Python analogue of the TS ``contextForDriver`` /
    rust ``for_driver`` / go ``ContextForDB`` (§6).
    """
    return ExecutionContext(driver, _EMPTY_CHAIN, None)


def as_context(driver_or_ctx: Union[Driver, ExecutionContext]) -> ExecutionContext:
    """Accept EITHER a raw :class:`Driver` (wrap it via :func:`context_for_driver` — the byte-identical
    backward-compat path) OR an already-built :class:`ExecutionContext` (pass through). The public
    runtime entry points (``execute_bundle`` / ``execute_transaction_bundle`` / ``run_relation_op`` /
    ``read_bundle``) take this union so every existing caller that threads a raw driver keeps working
    while the ctx-threaded internals funnel every SQL through the seam.
    """
    if isinstance(driver_or_ctx, ExecutionContext):
        return driver_or_ctx
    return context_for_driver(driver_or_ctx)


# ── The per-execution-ownership transaction (§3) — the concurrent-tx fix ──────


class TxDecision:
    """The body's decision about how to end the transaction — so a body can legitimately ROLLBACK and
    STILL return a value (the gate short-circuit: a failed gate rolls back but is NOT an error, it
    returns ``committed:false``). A raised exception from the body always rolls back + re-raises.
    """

    __slots__ = ("rollback", "value")

    def __init__(self, rollback: bool, value: Any) -> None:
        self.rollback = rollback
        self.value = value


def commit(value: _T) -> TxDecision:
    """The COMMIT decision (the tx's owned connection commits, then ``value`` returns)."""
    return TxDecision(rollback=False, value=value)


def rollback(value: _T) -> TxDecision:
    """The (non-error) ROLLBACK decision (the tx's owned connection rolls back, then ``value`` returns
    — a legitimate gate short-circuit)."""
    return TxDecision(rollback=True, value=value)


def with_transaction_decided(
    ctx: ExecutionContext,
    body: Callable[["ExecutionContext"], TxDecision],
    before: Sequence[str] = (),
    after: Sequence[str] = (),
) -> Any:
    """Run ``body`` inside a transaction with **per-execution connection ownership** (§3, the
    concurrent-tx fix). This is the general form: ``body`` decides COMMIT vs ROLLBACK (see
    :class:`TxDecision`); a raised exception from ``body`` always rolls back and re-raises.

      1. acquire ONE connection via :meth:`Driver.begin_tx` — a :class:`TxConnection` (the tx's
         exclusive connection; BEGIN issued on it), the Python analogue of v1 ``PoolTransaction``;
      2. pin it into a tx-scoped :class:`ExecutionContext` (and the ambient contextvar) so EVERY
         statement ``body`` issues resolves THAT connection via the seam — never a fresh pooled one;
      3. run ``body(tx_ctx)`` → COMMIT / ROLLBACK on the OWNED connection per the returned decision;
         on any raised exception ROLLBACK (best-effort) and re-raise;
      4. **release the owned connection EXACTLY ONCE in a ``finally``** (the SOLE releaser — the
         :class:`TxConnection` never self-releases). It goes back to the pool on the clean paths, and
         is **destroyed** when the connection is poisoned — a body error, OR a COMMIT/ROLLBACK that
         itself raised (rare but real: a deferred-constraint violation or a dropped connection at
         COMMIT). Without this ``finally`` a raising COMMIT would leak the connection (the pool would
         shrink by one under repeated commit failures — the #78 audit defect).

    ``before`` / ``after`` carry the per-transaction isolation prelude (Phase B / #84 —
    :func:`litedbmodel_runtime.tx_options.isolation_prelude`): MySQL's ``SET`` runs pre-BEGIN, PG's
    post-BEGIN. Empty ⇒ a bare ``BEGIN`` (byte-identical to the Phase A path).

    Concurrent calls (on distinct threads) each acquire a DISTINCT connection and pin it in their OWN
    contextvar scope, so their writes never cross-talk — the isolation the shared-slot model (the
    removed driver-global ``_writer``) violated. This mirrors the TS ``withTransactionAsync`` (#75) /
    rust ``with_transaction_decided`` (#76) / go ``WithTransactionDecided`` (#77).
    """
    tx = ctx.driver.begin_tx(before, after)
    tx_ctx = ctx.with_connection(_TxConnectionAdapter(tx), True)

    def scoped() -> Any:
        # `destroy` starts True: the connection is only proven clean once a COMMIT/ROLLBACK completes
        # without raising. ANY failure below (body error, or a commit/rollback that itself throws)
        # leaves it True ⇒ the finally drops the poisoned connection instead of returning it.
        destroy = True
        try:
            try:
                decision = body(tx_ctx)
            except BaseException:
                # A body error rolls back (BEST-EFFORT) then re-raises the ORIGINAL failure. A rollback
                # that itself raises must NOT mask the body error — swallow it but keep destroy=True so
                # the finally drops the poisoned connection. A clean rollback ⇒ back to the pool.
                try:
                    tx.rollback()
                    destroy = False
                except Exception:
                    pass  # poisoned; destroy stays True. Original body error surfaces via `raise`.
                raise
            if decision.rollback:
                # A legitimate non-error rollback (e.g. a gate short-circuit): roll back, return value.
                tx.rollback()
                destroy = False
                return decision.value
            tx.commit()
            destroy = False
            return decision.value
        finally:
            # The SINGLE release point — runs on every path (success, body error, raising
            # commit/rollback). Idempotent on the TxConnection side, but this is the only caller.
            tx.release(destroy)

    return run_with_pinned_context(tx_ctx, scoped)


def with_transaction(
    ctx: ExecutionContext,
    body: Callable[["ExecutionContext"], _T],
) -> _T:
    """The simple form of :func:`with_transaction_decided`: ``body`` returns a value ⇒ COMMIT + return
    it; a raised exception ⇒ ROLLBACK + re-raise. For a body that never legitimately rolls back with a
    value."""
    return with_transaction_decided(ctx, lambda tx_ctx: commit(body(tx_ctx)))


# ── The write=tx GUARD seam (Phase B / #84) ────────────────────────────────────


def ambient_in_transaction() -> bool:
    """Is THIS execution scope inside an active transaction? Reads the AMBIENT contextvar-propagated
    :class:`ExecutionContext` (:func:`current_context`) — the Python analogue of the TS async-local
    "inside a transaction" marker. A bare write outside :func:`transaction` sees ``None`` ⇒ False."""
    ambient = current_context()
    return ambient is not None and ambient.in_transaction()


def ambient_read_only() -> bool:
    """Is THIS execution scope a READ-ONLY context? Reads the ambient contextvar-propagated ctx's
    read-only marker (the Python analogue of the TS ``withReadOnly`` async-local marker)."""
    ambient = current_context()
    return ambient is not None and ambient.read_only()


def check_write_allowed_ambient(operation: str, model: Optional[str] = None) -> None:
    """Enforce the write=tx guard against the AMBIENT (contextvar-propagated) tx/read-only markers
    (mirror v1 ``_checkWriteAllowed`` / the TS ``checkWriteAllowed``). A write in a read-only scope →
    :class:`WriteInReadOnlyContextError`; a write with NO active transaction →
    :class:`WriteOutsideTransactionError`. Read-only is checked FIRST (v1 order). The Python port reads
    the guard state from the ambient contextvar (not an explicit ctx arg) so a bare model-level write —
    which only has the raw driver — still sees the caller's :func:`transaction` scope."""
    check_write_allowed(operation, model, ambient_in_transaction(), ambient_read_only())


def run_guarded(
    ctx: ExecutionContext,
    sql: str,
    params: Sequence[Any],
    operation: str,
    model: Optional[str] = None,
) -> RunInfo:
    """GUARDED write seam (mirror the TS ``runGuarded`` / go ``RunGuarded``): enforce the write=tx
    guard (:func:`check_write_allowed_ambient`) for a DATA-mutating statement, then delegate to
    :func:`run`. A write issued OUTSIDE a :func:`transaction` throws
    :class:`WriteOutsideTransactionError`; a write in a read-only scope throws
    :class:`WriteInReadOnlyContextError`. Tx-control statements (BEGIN/COMMIT/ROLLBACK/SET) are NOT
    guarded — the tx runtime issues them to OPEN the very scope the guard checks."""
    check_write_allowed_ambient(operation, model)
    return run(ctx, sql, params, WRITE_INTENT)


# ── The PUBLIC user-controlled transaction boundary (Phase B-core / #86, python) ──


def transaction(
    ctx: ExecutionContext,
    fn: Callable[[], _T],
    options: Optional[TransactionOptions] = None,
    dialect_name: str = "postgres",
) -> _T:
    """**The public user-controlled transaction boundary** (#86, python port of the TS ``transaction``
    / rust ``transaction`` / go ``Transaction``) — the REAL transaction feature v2 was missing.
    ``transaction(ctx, fn, options?)`` opens ONE boundary the caller wraps around MULTIPLE arbitrary
    operations so they commit or roll back TOGETHER::

        transaction(ctx, lambda: [
            create_a(...),   # ← every op inside JOINS this ONE boundary:
            update_b(...),   #    one connection, one BEGIN…COMMIT, all-or-nothing.
        ], TransactionOptions(isolation=IsolationLevel.SERIALIZABLE))

    ## What it does (v1 ``DBModel.transaction`` :2787 parity, on the SCP seam)

    It acquires ONE owned connection (``driver.begin_tx`` with the isolation prelude), pins it into a
    tx-scoped :class:`ExecutionContext` **propagated via** :data:`contextvars` (:func:`run_with_pinned_context`),
    runs ``fn``, then COMMITs (or ROLLBACKs on a body error / ``options.rollback_only``), with the #81
    retry loop (deadlock / serialization / connection error) wrapped around the WHOLE boundary — a
    FRESH owned connection per attempt.

    ## The ambient-tx JOIN — how operations participate (the core #86 fix; python = contextvars)

    ``fn`` takes NO connection argument. Instead the pinned tx ctx lives in the ambient contextvar
    (:func:`current_context`). Every operation ``fn`` issues — a live-DB write via
    ``execute_transaction_bundle``, a read via ``execute_bundle`` — detects that ambient pinned ctx and
    runs its statements on THAT connection **without opening its own BEGIN/COMMIT** (the nested-join,
    below). So N operations inside one ``transaction(fn)`` produce exactly ONE BEGIN + ONE COMMIT on
    ONE connection. Outside a ``transaction(fn)`` the ambient pin is absent, so a bare guarded write's
    guard fires (:class:`WriteOutsideTransactionError`).

    NESTED ``transaction()`` joins the outer (one physical BEGIN/COMMIT; an inner error rolls back the
    WHOLE tx). Isolation/retry/rollback_only options on a nested call are IGNORED (the outer owns
    them). Mirrors v1 ``DBModel.transaction`` :2794-2797.
    """
    opts = options if options is not None else TransactionOptions()

    # NESTED-TX JOIN (mirror v1 :2794): already inside a tx on this contextvar scope ⇒ join the outer.
    # No new connection, no BEGIN/COMMIT — the inner body is part of the outer physical transaction.
    # Isolation/retry/rollback_only on a nested call are ignored: the outer owns the envelope.
    if ambient_in_transaction():
        return fn()

    # Validate + build the isolation prelude BEFORE acquiring a connection (fail-closed: SQLite + a
    # level is a hard error; an unsupported isolation must not open a tx it can't honor).
    before, after = isolation_prelude(dialect_name, opts.isolation)

    retry_limit = 1
    if opts.retry_on_error:
        retry_limit = opts.retry_limit if opts.retry_limit >= 1 else 1

    attempt = 0
    while True:
        attempt += 1
        # ONE attempt on a FRESH owned connection (a retry after a connection error thus RECONNECTS).
        # `fn` reads the pinned tx ctx from the ambient contextvar (run_with_pinned_context inside
        # with_transaction_decided), so every op JOINs this one connection.
        def body(_tx_ctx: ExecutionContext) -> TxDecision:
            value = fn()
            # rollback_only (dry-run): ROLLBACK but still return the body value — no committed change.
            return rollback(value) if opts.rollback_only else commit(value)

        try:
            return with_transaction_decided(ctx, body, before, after)
        except (WriteOutsideTransactionError, WriteInReadOnlyContextError):
            # A guard rejection is a programming error, never retryable — re-raise immediately.
            raise
        except Exception as error:
            if attempt < retry_limit and opts.retry_on_error and is_retryable_tx_error(error):
                # Exponential backoff before RETRYing the whole transaction on a fresh connection.
                backoff_ms = opts.retry_duration * (2 ** (attempt - 1))
                if backoff_ms > 0:
                    time.sleep(backoff_ms / 1000.0)
                continue
            raise
