"""litedbmodel v2 SCP — the **middleware layer** (Phase D / #95, python).

The Python port of the TS API-reference ``src/scp/middleware.ts`` (#92), mirroring the rust (#93) /
go (#94) / php (#96) ports: it makes REAL the empty middleware hook Phase A reserved on the
:class:`litedbmodel_runtime.exec_context.ExecutionContext` seam. It builds ON that seam — it does NOT
restructure it; it wires the reserved :class:`~litedbmodel_runtime.exec_context.MiddlewareChain` hook.

## The two hook levels (design §4)

  1. **SQL-level ``execute`` hook** — ``(sql, params, next) -> T``. Wraps EVERY statement that funnels
     through the central seam (:func:`~litedbmodel_runtime.exec_context.execute` /
     :func:`~litedbmodel_runtime.exec_context.run`), so read, write, relation-batch, AND tx-control —
     the isolation ``SET`` / ``BEGIN`` / ``COMMIT`` / ``ROLLBACK`` a :func:`~litedbmodel_runtime.exec_context.transaction`
     brackets around a body — are ALL intercepted (see the TX-CONTROL note below). A middleware can
     OBSERVE / REWRITE (``next(sql', params')``) / TIME / SHORT-CIRCUIT (return without calling
     ``next``). This is the seam's ``MiddlewareChain`` folded around the connection-resolve terminal.
  2. **method-level hook** — at the ORM operation boundary, keyed by the operation KIND
     (``find`` / ``findOne`` / ``findById`` / ``count`` / ``create`` / ``createMany`` / ``update`` /
     ``updateMany`` / ``delete`` / ``query``). :func:`run_method` folds the matching method hooks around
     the operation. The op kind is a TAG the operation boundary supplies — NEVER parsed from the SQL text.

## Runtime-issued tx-control IS middleware-visible (owner DECISION — full TS parity, all 5 languages)

The owner's decision (option A) is that runtime-issued tx-control MUST be middleware-visible in all 5
languages. So :func:`~litedbmodel_runtime.exec_context.with_transaction_decided` issues the isolation
``SET`` / ``BEGIN`` / ``COMMIT`` / ``ROLLBACK`` THROUGH the seam (:func:`~litedbmodel_runtime.exec_context.run`
on the PINNED tx ctx) — so a registered SQL middleware OBSERVES a real ``transaction()``'s BEGIN + COMMIT
(and ROLLBACK on error). The audited Phase A/B tx correctness is preserved: tx-control still runs on the
SAME owned/pinned ``TxConnection`` (never a new one), it is EXEMPT from the write=tx guard (issued via the
UNGUARDED ``run``, not ``run_guarded``), and the connection ownership / release-exactly-once /
destroy-on-poison (#78 pool-leak fix — now covering a failed BEGIN too) is unchanged. The ``TxConnection``
became the connection OWNER (acquire / release / discard); it no longer issues tx-control SQL itself.

## Registration + APPLIED ORDER (the 5-language contract — v1 ``DBModel.use`` parity)

:func:`use` / :func:`register_middleware` append to an ordered stack and return an un-register fn. The
stack is folded so the FIRST-registered middleware is the OUTERMOST wrapper: given ``use(A); use(B)`` a
statement runs ``A.before -> B.before -> «execute» -> B.after -> A.after``. This holds identically for
the SQL-level chain (:meth:`~litedbmodel_runtime.exec_context.MiddlewareChain.wrap`) and the
method-level chain (:func:`run_method`) — the fold walks the stack from LAST to FIRST building ``next``,
so index 0 ends up OUTERMOST. This ORDER is the normative contract the ports reproduce.

## Per-execution-scope isolation (NOT a serializing process global) — design §4 last line

A middleware binds to the EXECUTION SCOPE, so concurrent requests/contexts never see each other's
middleware or per-request state. The TS reference uses ``AsyncLocalStorage``; the Python port uses
:mod:`contextvars` (per-thread + task-local): :func:`with_middleware_scope` runs a callback with an
ISOLATED registry copy. The isolation has TWO coupled parts:

  (a) the **registry (stack)** is scope-local — :func:`with_middleware_scope` seeds a COPY of the
      currently-visible stack, so app-wide registrations remain in effect but a ``use`` inside the scope
      mutates ONLY this scope;
  (b) the **per-middleware state** (keyed per-middleware) is scope-local too and **starts EMPTY per
      scope** — a fresh scope's registry copy copies ONLY the stack, NOT the state map, so each scope
      lazily builds its OWN fresh state instances (isolated across concurrent scopes with no cross-talk).

Absent an explicit scope, :func:`use` / :func:`register_middleware` mutates a process-global default
stack (the app-startup registration path).

## Native registration (design §4 "native 側でも登録可")

:func:`register_middleware` appends a middleware to the current scope's ctx chain in the native runtime
— the CHAIN CONTRACT + ORDER above are shared across the 5 languages; the middleware BODY is that
language's closure/impl. TS is the reference for the shape.
"""

from __future__ import annotations

import contextvars
import copy as _copy
import time
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    TypeVar,
)

from .exec_context import (
    ExecutionContext,
    Middleware as SqlMiddleware,
    Rows,
    StatementIntent,
    execute as _seam_execute,
    run as _seam_run,
)

_R = TypeVar("_R")

__all__ = [
    "SqlHook",
    "MethodKind",
    "METHOD_KINDS",
    "MiddlewareDescriptor",
    "MiddlewareHandle",
    "MiddlewareConfig",
    "Registry",
    "current_registry",
    "with_middleware_scope",
    "active_sql_middlewares",
    "register",
    "use",
    "register_middleware",
    "create_middleware",
    "run_method",
    "LogEntry",
    "Logger",
    "RawResult",
    "raw_execute",
    "raw_query",
    "clear_middlewares",
]


# ── The SQL-level middleware (the seam's shape) ────────────────────────────────

# The SQL-level ``execute`` hook (design §4 level 1): ``(sql, params, next) -> T``. IDENTICAL to the
# seam's :data:`~litedbmodel_runtime.exec_context.Middleware` — aliased here so the middleware layer is
# the ONE import surface. ``next(sql, params)`` runs the rest of the chain + the connection-resolve
# terminal; a middleware may pass DIFFERENT sql/params (rewrite), skip ``next`` (short-circuit), or wrap
# the ``next`` call to time it.
SqlHook = SqlMiddleware


# ── Method-level hooks (design §4 level 2) — the ORM operation boundary ─────────

# The ORM operation KIND a method hook keys on. A read operation is tagged ``find``/``findOne``/
# ``findById``/``count``/``query``; a write operation ``create``/``createMany``/``update``/
# ``updateMany``/``delete``. :func:`run_method` dispatches to the hook of the matching kind — this is
# how a method hook DISTINGUISHES the op kind (the TAG the operation boundary supplies, NOT a guess
# from the SQL text).
MethodKind = str

# The op-kind method-hook keys (in a registration-independent, fixed order).
METHOD_KINDS: Sequence[MethodKind] = (
    "find",
    "findOne",
    "findById",
    "count",
    "create",
    "createMany",
    "update",
    "updateMany",
    "delete",
    "query",
)

# One method-level hook: ``(model, next, *args) -> R`` (v1 ``Middleware.find`` parity). ``model`` is the
# operation's model/target descriptor (opaque here — the runtime supplies it); ``*args`` are the
# operation's arguments; ``next(*args)`` runs the rest of the chain + the operation. A hook may rewrite
# ``args``, time the ``next`` call, or short-circuit by returning without calling ``next``.
MethodHook = Callable[..., Any]
MethodNext = Callable[..., Any]


# ── The middleware descriptor (registration unit) ──────────────────────────────


class MiddlewareDescriptor:
    """A registered middleware: its (optional) SQL-level :data:`SqlHook` and its per-kind
    :data:`MethodHook` s. :func:`use` registers ONE of these. Built by :func:`create_middleware` from
    the ergonomic (v1-shaped) config; a hand-built descriptor is also accepted."""

    __slots__ = ("sql", "methods")

    def __init__(
        self,
        sql: Optional[SqlHook] = None,
        methods: Optional[Mapping[MethodKind, MethodHook]] = None,
    ) -> None:
        self.sql = sql
        self.methods: Dict[MethodKind, MethodHook] = dict(methods) if methods else {}


class MiddlewareHandle:
    """The registration handle returned by :func:`create_middleware`. :func:`use` / :func:`register_middleware`
    bind its ``descriptor`` to the ambient/scope-local registry, and :meth:`state` reads the CURRENT
    execution scope's state instance (v1 ``getCurrentContext()`` — a fresh per-scope copy of the config's
    ``state``, contextvars-isolated). ``descriptor`` is the underlying :class:`MiddlewareDescriptor`."""

    __slots__ = ("descriptor", "_token", "_fresh")

    def __init__(self, descriptor: MiddlewareDescriptor, token: object, fresh: Callable[[], Any]) -> None:
        self.descriptor = descriptor
        self._token = token
        self._fresh = fresh

    def state(self) -> Any:
        """The CURRENT execution scope's state instance (fresh per scope, v1 ``getCurrentContext()``)."""
        return current_registry().state_for(self._token, self._fresh)

    def reset_state(self) -> None:
        """Reset the current scope's state to a fresh copy of the initial state (testing convenience)."""
        current_registry().reset_state_for(self._token, self._fresh)


# ── The middleware registry (the ordered stack + scope-local isolation) ─────────


class Registry:
    """The ordered middleware stack + per-scope state map. :meth:`use` appends (first-registered =
    outermost, §order), returning an un-register fn. :meth:`sql_hooks` / :meth:`method_hooks` return the
    folded-order slices the seam + :func:`run_method` consume. A :class:`Registry` is EITHER the
    process-global default (app-startup registration) OR a per-execution-scope COPY pushed by
    :func:`with_middleware_scope` (concurrent isolation) — the two share this class; only their lifetime
    differs."""

    __slots__ = ("_stack", "_states")

    def __init__(self) -> None:
        self._stack: List[MiddlewareDescriptor] = []
        # Per-scope STATE instances, keyed by the state-owning token a :func:`create_middleware`
        # descriptor carries. Because the Registry is itself per-execution-scope
        # (:func:`with_middleware_scope` pushes a COPY), the state map is scope-local too — so a
        # middleware's per-request state is isolated across concurrent scopes WITHOUT leaking. A fresh
        # scope's copy starts with an EMPTY state map (states are NOT copied — see :meth:`copy`), so each
        # scope lazily builds its OWN fresh state instances (the TS M5-isolation contract).
        self._states: Dict[object, Any] = {}

    def state_for(self, token: object, fresh: Callable[[], _R]) -> _R:
        """The current scope's state for ``token``, lazily created via ``fresh`` on first access."""
        if token not in self._states:
            self._states[token] = fresh()
        return self._states[token]

    def reset_state_for(self, token: object, fresh: Callable[[], _R]) -> None:
        """Reset ``token`` s state in this scope to a fresh instance (testing convenience)."""
        self._states[token] = fresh()

    def use(self, mw: MiddlewareDescriptor) -> Callable[[], None]:
        """Register ``mw`` (appended ⇒ outermost). Returns an idempotent un-register fn."""
        self._stack.append(mw)

        def unregister() -> None:
            try:
                self._stack.remove(mw)
            except ValueError:
                pass

        return unregister

    def remove(self, mw: MiddlewareDescriptor) -> bool:
        """Remove ``mw`` (v1 ``removeMiddleware``). Returns whether it was present."""
        try:
            self._stack.remove(mw)
            return True
        except ValueError:
            return False

    def clear(self) -> None:
        """Drop every registration (v1 ``clearMiddlewares`` — testing)."""
        self._stack = []
        self._states = {}

    def all(self) -> Sequence[MiddlewareDescriptor]:
        """The registered descriptors, registration order (index 0 = first = outermost)."""
        return self._stack

    def sql_hooks(self) -> List[SqlHook]:
        """The SQL-level hooks (registration order), for the ``MiddlewareChain`` fold."""
        return [mw.sql for mw in self._stack if mw.sql is not None]

    def method_hooks(self, kind: MethodKind) -> List[MethodHook]:
        """The method hooks for ``kind`` (registration order), for the :func:`run_method` fold."""
        out: List[MethodHook] = []
        for mw in self._stack:
            h = mw.methods.get(kind)
            if h is not None:
                out.append(h)
        return out

    def copy(self) -> "Registry":
        """A shallow COPY that seeds an isolated per-scope registry: copies ONLY the stack, NOT the
        state map (a fresh scope starts with an EMPTY state map so each scope lazily builds its OWN
        fresh state — the TS M5 concurrent-isolation contract). App-wide registrations remain visible;
        per-scope state never bleeds across concurrent scopes."""
        r = Registry()
        r._stack = list(self._stack)
        # r._states is intentionally left EMPTY — see the docstring (the isolation contract).
        return r


# The process-global default registry (app-startup registration with no explicit scope).
_global_registry = Registry()

# The per-execution-scope registry override (contextvars): present ⇒ ``use``/reads target THIS scope's
# copy. A concurrent scope (a distinct thread or asyncio task) sees its OWN value.
_registry_scope: "contextvars.ContextVar[Optional[Registry]]" = contextvars.ContextVar(
    "litedbmodel_scp_middleware_registry", default=None
)


def current_registry() -> Registry:
    """The registry the current execution scope resolves to: the contextvars override, else the global
    default."""
    r = _registry_scope.get()
    return r if r is not None else _global_registry


def with_middleware_scope(fn: Callable[[], _R], inherit: bool = True) -> _R:
    """Run ``fn`` with an ISOLATED middleware registry (concurrent-request isolation, design §4). The
    scope seeds a COPY of the currently-visible registry (so app-wide registrations remain in effect) —
    copying ONLY the stack, so per-scope STATE starts EMPTY (the concurrent-isolation contract). Any
    ``use`` / per-request state inside ``fn`` mutates ONLY this scope; two concurrent
    :func:`with_middleware_scope` bodies never see each other's middleware or state. ``inherit=False``
    seeds an EMPTY registry instead. Restores the prior scope on exit (contextvars token reset), so it is
    thread-safe by construction."""
    seed = Registry() if not inherit else current_registry().copy()
    token = _registry_scope.set(seed)
    try:
        return fn()
    finally:
        _registry_scope.reset(token)


def active_sql_middlewares() -> List[SqlHook]:
    """The LIVE SQL-level middleware stack of the current execution scope — the source the ctx factories
    give their ``MiddlewareChain``. Resolved at EACH ``wrap`` (via
    :func:`~litedbmodel_runtime.exec_context.context_for_driver`), so registration after ctx construction,
    and per-scope registries, are both honored. Empty ⇒ the seam is a byte-identical passthrough."""
    return current_registry().sql_hooks()


# ── Registration surface (v1 ``DBModel.use`` parity) ───────────────────────────


def register(mw: MiddlewareDescriptor) -> Callable[[], None]:
    """Register a middleware DESCRIPTOR on the CURRENT scope's registry (the ambient per-scope one inside
    :func:`with_middleware_scope`, else the process-global default). Returns an un-register fn. The
    low-level surface; app code usually registers a :func:`create_middleware` handle via :func:`use`."""
    return current_registry().use(mw)


def use(mw: Any) -> Callable[[], None]:
    """Register a :func:`create_middleware` handle (or a raw :class:`MiddlewareDescriptor`) — v1
    ``DBModel.use``. Returns an un-register fn."""
    descriptor = mw.descriptor if isinstance(mw, MiddlewareHandle) else mw
    return register(descriptor)


def register_middleware(mw: Any) -> Callable[[], None]:
    """**Native registration** (design §4 "native 側でも登録可"): append ``mw`` (a
    :func:`create_middleware` handle or a raw :class:`MiddlewareDescriptor`) to the CURRENT scope's ctx
    chain in the Python runtime. The CHAIN CONTRACT + ORDER are the shared 5-language contract; the
    middleware BODY is a Python closure. Alias of :func:`use` under the port's native name (rust/go/php
    expose the same-named entry)."""
    return use(mw)


class MiddlewareConfig:
    """The v1-shaped hook config :func:`create_middleware` consumes: per-kind method hooks + the
    ``execute`` SQL hook + an optional per-scope ``state``.

    ``state``: per-scope initial state — a FRESH deep copy is bound to each execution scope (v1
    ``structuredClone`` of ``state``). Read via the handle's :meth:`~MiddlewareHandle.state`.

    ``execute``: the SQL-level hook in the v1 form ``execute(state, next, sql, params)`` (adapted to the
    seam's ``(sql, params, next)``); the state object is passed as the FIRST argument (the Python
    analogue of the TS ``this``-bound state).

    Each method hook is ``(state, model, next, *args) -> R`` (state-first)."""

    __slots__ = ("state", "execute") + tuple(METHOD_KINDS)

    def __init__(
        self,
        state: Optional[Any] = None,
        execute: Optional[Callable[..., Any]] = None,
        **methods: Callable[..., Any],
    ) -> None:
        self.state = state
        self.execute = execute
        for kind in METHOD_KINDS:
            setattr(self, kind, methods.get(kind))
        unknown = set(methods) - set(METHOD_KINDS)
        if unknown:
            raise TypeError(f"create_middleware: unknown method-hook kind(s) {sorted(unknown)}")


def create_middleware(
    config: Optional[MiddlewareConfig] = None,
    *,
    state: Optional[Any] = None,
    execute: Optional[Callable[..., Any]] = None,
    **methods: Callable[..., Any],
) -> MiddlewareHandle:
    """Build a :class:`MiddlewareHandle` from a v1-shaped config (v1 ``createMiddleware`` parity). Accepts
    EITHER a :class:`MiddlewareConfig` OR keyword args (``state=`` / ``execute=`` / per-kind method
    hooks). Each hook is passed the CURRENT execution scope's state instance as its FIRST argument (a
    fresh :func:`copy.deepcopy` of ``state`` per scope, contextvars-isolated — the Python analogue of the
    TS ``this``-bound state). The ``execute`` hook is adapted from the config's ``(state, next, sql,
    params)`` shape to the seam's ``(sql, params, next)`` order. Method hooks pass through in the
    ``(state, model, next, *args)`` shape as ``(model, next, *args)`` with the state bound in."""
    if config is None:
        config = MiddlewareConfig(state=state, execute=execute, **methods)

    # Per-scope state: keyed on THIS unique token in the CURRENT scope's registry. Because the registry
    # is itself per-execution-scope (:func:`with_middleware_scope` pushes a fresh copy with an EMPTY state
    # map), each scope lazily builds its OWN state instance — isolated across concurrent scopes. First
    # access in a scope deep-copies the initial state (v1 ``structuredClone``).
    token = object()
    initial = config.state

    def fresh_state() -> Any:
        return _copy.deepcopy(initial) if initial is not None else {}

    def state_now() -> Any:
        return current_registry().state_for(token, fresh_state)

    # SQL-level hook: adapt config ``(state, next, sql, params)`` -> seam ``(sql, params, next)``; bind
    # the per-scope state in as the first arg.
    sql_hook: Optional[SqlHook] = None
    cfg_execute = config.execute
    if cfg_execute is not None:

        def sql_hook_impl(s: str, p: Sequence[Any], next_: Callable[..., Any]) -> Any:
            def adapted_next(ns: str, np: Optional[Sequence[Any]] = None) -> Any:
                return next_(ns, np if np is not None else [])

            return cfg_execute(state_now(), adapted_next, s, p)

        sql_hook = sql_hook_impl

    methods_map: Dict[MethodKind, MethodHook] = {}
    for kind in METHOD_KINDS:
        fn = getattr(config, kind)
        if fn is not None:

            def make(fn_: Callable[..., Any]) -> MethodHook:
                def method_hook(model: Any, next_: MethodNext, *args: Any) -> Any:
                    return fn_(state_now(), model, next_, *args)

                return method_hook

            methods_map[kind] = make(fn)

    descriptor = MiddlewareDescriptor(sql=sql_hook, methods=methods_map or None)
    return MiddlewareHandle(descriptor, token, fresh_state)


# ── Method-level dispatch (design §4 level 2) — the operation boundary fold ──────


def run_method(
    kind: MethodKind,
    model: Any,
    core: Callable[..., _R],
    args: Sequence[Any],
) -> _R:
    """Run an ORM operation of KIND ``kind`` through the current scope's method hooks, then execute
    ``core``. The hooks fold first-registered-OUTERMOST (§order), each getting ``(model, next, *args)``;
    a hook may rewrite ``args``, time ``next``, or short-circuit. Empty hooks for this kind ⇒
    ``core(*args)`` verbatim (byte-identical — no method registered ⇒ the operation runs untouched). The
    v2 equivalent of v1 ``DBModel._applyMiddleware``: the runtime calls it at the read/write boundary
    with the op TAGGED by its :data:`MethodKind` (NEVER parsed from the SQL).

    NB the op kind is the TAG the caller passes here; the SQL text is never inspected to pick the kind.
    """
    hooks = current_registry().method_hooks(kind)
    if not hooks:
        return core(*args)  # fast path: no method hook for this kind

    next_: MethodNext = core
    for i in range(len(hooks) - 1, -1, -1):
        hook = hooks[i]
        inner = next_

        def wrapped(*a: Any, _hook: MethodHook = hook, _inner: MethodNext = inner) -> Any:
            return _hook(model, _inner, *a)

        next_ = wrapped
    return next_(*args)


# ── D3: the standard Logger middleware (SQL / params / timing) ──────────────────


class LogEntry:
    """One logged statement: the SQL, its params, and the wall-clock ms ``next`` took (v1 Logger
    parity)."""

    __slots__ = ("sql", "params", "duration_ms")

    def __init__(self, sql: str, params: Sequence[Any], duration_ms: float) -> None:
        self.sql = sql
        self.params = params
        self.duration_ms = duration_ms

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"LogEntry(sql={self.sql!r}, params={self.params!r}, duration_ms={self.duration_ms})"


def Logger(
    sink: Optional[Callable[[LogEntry], None]] = None,
    console: bool = False,
    now: Optional[Callable[[], float]] = None,
) -> MiddlewareHandle:
    """The standard **Logger middleware** (design §4, v1 ``StatisticsMiddleware``/``Logger`` parity): a
    SQL-level hook that records the SQL, its params, and the wall-clock ms each statement takes. Every
    statement through the seam — read, write, relation-batch, AND tx-control (the runtime BEGIN / COMMIT
    / ROLLBACK / isolation SET a ``transaction()`` brackets) — is logged (it is an ``execute``-level
    hook). Register it with :func:`use`: ``use(Logger(sink=...))``. Timing brackets the
    ``next`` call, so it measures the connection execute (chain remainder included), NOT just the log call.

    The per-scope log history lives on the handle's ``state()['entries']`` (v1
    ``getCurrentContext().getLogs()``), so concurrent requests each collect their OWN entries
    (contextvars-isolated). ``now`` is an injectable clock (tests); defaults to a monotonic ms clock."""
    clock = now if now is not None else (lambda: time.perf_counter() * 1000.0)

    def execute_hook(st: Dict[str, Any], next_: Callable[..., Any], sql: str, params: Sequence[Any]) -> Any:
        entries: List[LogEntry] = st["entries"]
        started = clock()
        try:
            result = next_(sql, params)
        finally:
            # Record even if ``next`` raised — timing brackets the whole call. The Python seam is
            # synchronous (there is ONE interface, no async twin), so no thenable branch is needed.
            entry = LogEntry(sql, params, clock() - started)
            entries.append(entry)
            if sink is not None:
                sink(entry)
            if console:
                print(f"[scp] {sql} ({entry.duration_ms}ms) params={list(params)!r}")
        return result

    return create_middleware(state={"entries": []}, execute=execute_hook)


# ── D3: raw execute / query THROUGH the seam ────────────────────────────────────
#
# A public raw statement API that goes through the exec-context seam, so a registered SQL-level
# middleware sees it AND connection routing / an ambient transaction still apply (design §4 D3). It is a
# thin front over the seam's :func:`~litedbmodel_runtime.exec_context.execute` /
# :func:`~litedbmodel_runtime.exec_context.run` — the SAME central point every ORM-generated statement
# uses. :func:`raw_query` is :func:`raw_execute` TAGGED as a ``query`` operation kind (so a ``query``
# method hook fires) — the two-level D3 wiring (a method hook around, the ``execute`` hooks inside).

import re as _re

# Does ``sql`` return rows (SELECT / …RETURNING / WITH…SELECT / SHOW / PRAGMA / VALUES / EXPLAIN / TABLE)?
_RETURNS_ROWS_RE = _re.compile(r"^\s*(select|with|show|pragma|values|explain|table)\b", _re.IGNORECASE)
_RETURNING_RE = _re.compile(r"\breturning\b", _re.IGNORECASE)


def _returns_rows(sql: str) -> bool:
    return bool(_RETURNS_ROWS_RE.match(sql)) or bool(_RETURNING_RE.search(sql))


class RawResult:
    """The raw-statement result: a row list (for a row-returning statement) plus the affected-rows count
    (mirrors v1 ``ExecuteResult {rows, rowCount}``). A non-row statement resolves ``rows=[]``."""

    __slots__ = ("rows", "row_count")

    def __init__(self, rows: Rows, row_count: Optional[int]) -> None:
        self.rows = rows
        self.row_count = row_count


def raw_execute(
    ctx: ExecutionContext,
    sql: str,
    params: Sequence[Any] = (),
    write: bool = False,
) -> RawResult:
    """Raw ``execute(sql, params)`` THROUGH the seam (design §4 D3): a registered SQL-level middleware
    intercepts it, connection routing resolves the connection, and an ambient transaction (if the ctx is
    tx-scoped) applies — because it is the SAME ``execute``/``run`` seam the ORM uses, not a direct driver
    call. A row-returning statement runs the READ seam; a non-returning one runs the WRITE seam. ``write``
    forces the write intent (writer routing / tx connection) for a row-returning write."""
    if _returns_rows(sql):
        rows = _seam_execute(ctx, sql, params, StatementIntent(write=write))
        return RawResult(rows, len(rows))
    info = _seam_run(ctx, sql, params)
    return RawResult([], info.changes)


def raw_query(ctx: ExecutionContext, sql: str, params: Sequence[Any] = ()) -> Rows:
    """Raw ``query(sql, params)`` — :func:`raw_execute` TAGGED as a ``query`` operation, so a ``query``
    method hook fires (then its SQL flows through the same seam + ``execute`` hooks, exactly as v1
    ``DBModel.query`` calls ``DBModel.execute``). Returns the row list."""
    return run_method("query", None, lambda s, p: raw_execute(ctx, s, p).rows, [sql, params])


# ── Reset / testing helpers ─────────────────────────────────────────────────────


def clear_middlewares() -> None:
    """Clear the process-global registry (testing; a per-scope registry is dropped when its scope
    exits)."""
    _global_registry.clear()
