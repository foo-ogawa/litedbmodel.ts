"""litedbmodel v2 SCP — the **connection routing + config contract** (Phase C / #90, python).

The Python port of the TS **API REFERENCE** ``src/scp/connection-routing.ts`` (#87), mirroring the
rust port (#88) / go port (#89) / php port (#91). It builds ON the Phase A
:class:`litedbmodel_runtime.exec_context.ExecutionContext` seam and the Phase A/B owned-connection
transaction runtime; it does NOT re-implement the seam — it supplies the pieces
:meth:`litedbmodel_runtime.exec_context.ExecutionContext.connection_for` uses to complete its
resolution (steps 2-4).

## The ``connection_for(intent)`` resolution order (design §3, v1 ``DBModel.ts:313`` parity)

A statement's connection is resolved in THIS priority (first match wins):

  1. **active tx connection** — inside a transaction, always the tx-owned connection (Phase A;
     resolved by :class:`ExecutionContext` BEFORE the routing steps, since only it holds the pin).
  2. **writer scope / writer-sticky** — inside :func:`with_writer`, or within ``writer_sticky_duration``
     after a transaction (read-your-writes), a READ goes to the WRITER pool (Phase C — here).
  3. **read=reader / write=writer** — otherwise a read → the reader pool, a write → the writer pool
     (reader/writer separation; single-pool config ⇒ reader IS writer, Phase C).
  4. **named-DB routing** — the target pool is selected by ``intent.db`` (the connection NAME) against
     the :class:`ConnectionRegistry`; absent ⇒ the DEFAULT connection. Named-DB selection happens
     FIRST (it picks WHICH connection's reader/writer split steps 2-3 then apply to).

## The python model — synchronous DB-API pools, ONE Connection seam

The TS reference is async (``AsyncConnectionPool.acquire()`` returns a Promise) because pg/mysql2 are
async. Python's DB-API is synchronous, so a routing :class:`ConnectionPool` is the SYNCHRONOUS
acquire/release pair — exactly the shape of the Phase A :class:`litedbmodel_runtime.driver._ConnectionPool`
(``acquire()`` → a raw DB-API connection, ``release(conn)`` / ``discard(conn)``). A routed statement
runs on ONE pooled connection per statement (acquire-run-release), byte-identical to the non-tx
``_PooledDriver._with_conn`` path. There is ONE :class:`Connection` seam (no sync/async bifurcation),
matching ``exec_context.py``.

## Backward-compat (the hard constraint)

Single DB, reader IS writer (one pool), empty config, unnamed connection ⇒ BYTE-IDENTICAL to the
Phase A/B single-pool behavior. The existing ``context_for_driver(driver)`` path (no routing config)
is UNTOUCHED — routing is only consulted when an :class:`ExecutionContext` is built with a
:class:`RoutingConfig` (via :func:`build_routing_config`). A registry built from ONE pool routes every
intent to that ONE pool; the sticky clock only ever diverts to a pool that is the SAME object — so
nothing observable changes.
"""

from __future__ import annotations

import contextvars
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .driver import (
    RunInfo,
    TxConnection,
    _conn_all,
    _conn_run,
    _ConnectionPool,
    _dollar_to_pyformat,
    _PooledTxConnection,
    _qmark_to_pyformat,
)
from .exec_context import Connection, Rows, StatementIntent


# ── The runtime config (C3) — mirrors v1 DBConfig/DBConfigOptions ──────────────


_DEFAULT_DRIVER = "postgres"
_DEFAULT_QUERY_TIMEOUT = 0
_DEFAULT_KEEP_ALIVE = False
_DEFAULT_KEEP_ALIVE_INITIAL_DELAY_MS = 10000
_DEFAULT_MIN_POOL = 0
_DEFAULT_MAX_POOL = 10


class ConnectionConfig:
    """Per-connection database config (C3) — the knobs a pool is built with. Mirrors the TS
    ``ConnectionConfig`` (v1 ``DBConfig``/``DBHandler``): connection target + pool sizing +
    per-statement/keepalive/session knobs. Every field is optional with a documented default; the
    field names + defaults MATCH the other ports EXACTLY. This is a DATA contract — it describes how to
    BUILD a pool; the actual psycopg / PyMySQL pool construction lives in the pool factories
    (:func:`pg_pool_factory` / :func:`mysql_pool_factory`), which read these fields.

    Fields:
      - ``driver``: ``'postgres'`` | ``'mysql'`` | ``'sqlite'`` (@default ``'postgres'``).
      - ``host`` / ``port`` / ``database`` / ``user`` / ``password``: connection target.
      - ``query_timeout``: per-statement timeout in MILLISECONDS → a session ``statement_timeout`` (PG)
        / ``max_execution_time`` (MySQL) so a runaway query is aborted by the SERVER. ``0``/absent ⇒
        no timeout (@default 0). NB the v2 contract is server-side ms so it fires uniformly across
        drivers (v1's ``queryTimeout`` was pg-client seconds).
      - ``keep_alive``: enable TCP keepalive on pooled connections (@default False).
      - ``keep_alive_initial_delay_millis``: ms before the first keepalive probe (@default 10000).
      - ``min_pool``: minimum pooled connections kept warm (@default 0).
      - ``max_pool``: maximum pooled connections (@default 10).
      - ``search_path``: PG ``search_path`` set on each pooled connection at checkout (schema routing).
      - ``charset``: MySQL connection charset / PG ``client_encoding`` set on each pooled connection.
    """

    __slots__ = (
        "driver",
        "host",
        "port",
        "database",
        "user",
        "password",
        "query_timeout",
        "keep_alive",
        "keep_alive_initial_delay_millis",
        "min_pool",
        "max_pool",
        "search_path",
        "charset",
    )

    def __init__(
        self,
        driver: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        query_timeout: Optional[int] = None,
        keep_alive: Optional[bool] = None,
        keep_alive_initial_delay_millis: Optional[int] = None,
        min_pool: Optional[int] = None,
        max_pool: Optional[int] = None,
        search_path: Optional[str] = None,
        charset: Optional[str] = None,
    ) -> None:
        self.driver = driver
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.query_timeout = query_timeout
        self.keep_alive = keep_alive
        self.keep_alive_initial_delay_millis = keep_alive_initial_delay_millis
        self.min_pool = min_pool
        self.max_pool = max_pool
        self.search_path = search_path
        self.charset = charset


class ResolvedConnectionConfig:
    """The resolved (defaults-applied) config the pool builder consumes — no ``None`` holes on the
    knobs (``query_timeout`` / ``keep_alive`` / ``keep_alive_initial_delay_millis`` / ``min_pool`` /
    ``max_pool`` are always concrete). Connection-target fields (``host`` … ``password``) and the
    optional session knobs (``search_path`` / ``charset``) stay ``None`` when unset (so a factory omits
    them). Mirrors the TS ``ResolvedConnectionConfig``.
    """

    __slots__ = (
        "driver",
        "host",
        "port",
        "database",
        "user",
        "password",
        "query_timeout",
        "keep_alive",
        "keep_alive_initial_delay_millis",
        "min_pool",
        "max_pool",
        "search_path",
        "charset",
    )

    def __init__(
        self,
        driver: str,
        query_timeout: int,
        keep_alive: bool,
        keep_alive_initial_delay_millis: int,
        min_pool: int,
        max_pool: int,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        search_path: Optional[str] = None,
        charset: Optional[str] = None,
    ) -> None:
        self.driver = driver
        self.query_timeout = query_timeout
        self.keep_alive = keep_alive
        self.keep_alive_initial_delay_millis = keep_alive_initial_delay_millis
        self.min_pool = min_pool
        self.max_pool = max_pool
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.search_path = search_path
        self.charset = charset


def resolve_connection_config(config: Optional[ConnectionConfig] = None) -> ResolvedConnectionConfig:
    """Apply the C3 defaults (``query_timeout=0``, ``keep_alive=False``,
    ``keep_alive_initial_delay_millis=10000``, ``min_pool=0``, ``max_pool=10``, ``driver='postgres'``).
    Mirrors the TS ``resolveConnectionConfig`` — identical field names + defaults."""
    c = config if config is not None else ConnectionConfig()
    return ResolvedConnectionConfig(
        driver=c.driver if c.driver is not None else _DEFAULT_DRIVER,
        host=c.host,
        port=c.port,
        database=c.database,
        user=c.user,
        password=c.password,
        query_timeout=c.query_timeout if c.query_timeout is not None else _DEFAULT_QUERY_TIMEOUT,
        keep_alive=c.keep_alive if c.keep_alive is not None else _DEFAULT_KEEP_ALIVE,
        keep_alive_initial_delay_millis=(
            c.keep_alive_initial_delay_millis
            if c.keep_alive_initial_delay_millis is not None
            else _DEFAULT_KEEP_ALIVE_INITIAL_DELAY_MS
        ),
        min_pool=c.min_pool if c.min_pool is not None else _DEFAULT_MIN_POOL,
        max_pool=c.max_pool if c.max_pool is not None else _DEFAULT_MAX_POOL,
        search_path=c.search_path,
        charset=c.charset,
    )


def session_statements(config: ResolvedConnectionConfig) -> List[str]:
    """The SESSION statements a connection must run at checkout to honor ``config`` (issued once per
    acquired connection, in order). Pure (no connection contact) so it is testable in isolation.
    Mirrors the TS ``sessionStatements``:

      - **statement timeout** (``query_timeout`` > 0): PG ``SET statement_timeout = <ms>``; MySQL
        ``SET SESSION max_execution_time = <ms>`` (both server-side, ms).
      - **search_path**: PG ``SET search_path TO <path>``; MySQL has no schema search path ⇒ ignored.
      - **charset**: MySQL ``SET NAMES <charset>``; PG ``SET client_encoding TO <charset>``.

    A key with no value emits nothing (⇒ empty list for an all-default config ⇒ the session is
    untouched, backward-compatible). sqlite has no server session ⇒ empty.
    """
    out: List[str] = []
    dialect = config.driver
    if dialect == "sqlite":
        return out
    if config.query_timeout > 0:
        out.append(
            "SET statement_timeout = %d" % config.query_timeout
            if dialect == "postgres"
            else "SET SESSION max_execution_time = %d" % config.query_timeout
        )
    if config.search_path is not None and dialect == "postgres":
        out.append("SET search_path TO %s" % config.search_path)
    if config.charset is not None:
        out.append(
            "SET NAMES %s" % config.charset if dialect == "mysql" else "SET client_encoding TO %s" % config.charset
        )
    return out


def session_reset_statements(config: ResolvedConnectionConfig) -> List[str]:
    """The RESET statements that undo :func:`session_statements` on release (per dialect), so a session
    knob (``statement_timeout`` / ``search_path`` / ``client_encoding`` / ``max_execution_time`` /
    charset) set for THIS configured connection does NOT leak to the next caller that draws the SAME
    underlying pooled connection — psycopg/PyMySQL do NOT auto-reset session state on release.
    ``RESET`` / ``SET … DEFAULT`` restores the server default. Only the knobs ``config`` actually set
    are reset (an all-default config ⇒ nothing to reset). Mirrors the TS ``sessionResetStatements``.
    """
    out: List[str] = []
    dialect = config.driver
    if dialect == "sqlite":
        return out
    if config.query_timeout > 0:
        out.append("RESET statement_timeout" if dialect == "postgres" else "SET SESSION max_execution_time = DEFAULT")
    if config.search_path is not None and dialect == "postgres":
        out.append("RESET search_path")
    if config.charset is not None:
        out.append("SET NAMES DEFAULT" if dialect == "mysql" else "RESET client_encoding")
    return out


# ── The connection pool seam (the routing unit) ────────────────────────────────


class ConnectionPool:
    """The synchronous pool seam a routed statement runs on (the Python analogue of the TS
    ``AsyncConnectionPool``). ``acquire`` checks out one owned DB-API connection for the caller's
    exclusive use; ``release`` returns it (``destroy=True`` drops a poisoned/aborted connection). The
    Phase A :class:`litedbmodel_runtime.driver._ConnectionPool` fits this shape via :class:`RawConnectionPool`.
    """

    def acquire(self) -> Any:
        """Check out one owned raw DB-API connection."""
        raise NotImplementedError

    def release(self, conn: Any, destroy: bool = False) -> None:
        """Return ``conn`` to the pool. ``destroy`` ⇒ drop it (poisoned/aborted)."""
        raise NotImplementedError


class RawConnectionPool(ConnectionPool):
    """Adapt a Phase A :class:`litedbmodel_runtime.driver._ConnectionPool` (or any object with
    ``acquire`` / ``release`` / ``discard`` / ``close``) to the :class:`ConnectionPool` seam, carrying
    the driver's ``xform`` (``$N``/``?`` → ``%s`` rewrite) + ``emulate_returning`` flag so a routed
    statement executes byte-identically to the Phase A ``_PooledDriver`` non-tx path (SAME
    ``_conn_all`` / ``_conn_run`` primitives). ``release(destroy=True)`` calls the pool's ``discard``
    (the #78 leak fix: a poisoned connection is closed AND its slot freed).
    """

    __slots__ = ("_pool", "xform", "emulate_returning")

    def __init__(self, pool: Any, xform: Callable[[str], str], emulate_returning: bool) -> None:
        self._pool = pool
        self.xform = xform
        self.emulate_returning = emulate_returning

    def acquire(self) -> Any:
        return self._pool.acquire()

    def release(self, conn: Any, destroy: bool = False) -> None:
        if destroy:
            # discard = close + free a pool slot (the #78 leak fix). Fall back to release if the
            # underlying pool has no discard (defensive; _ConnectionPool always has it).
            discard = getattr(self._pool, "discard", None)
            if discard is not None:
                discard(conn)
                return
        self._pool.release(conn)

    def close(self) -> None:
        self._pool.close()


class PoolConnection(Connection):
    """A :class:`Connection` that acquires-runs-releases ONE pooled connection per statement (the
    routed non-tx path — the read fan-out where each concurrent sibling holds its own connection). This
    is the routing analogue of the TS ``PooledAsyncContext.connectionFor`` per-statement wrapper. It
    runs the SAME ``_conn_all`` / ``_conn_run`` primitives the Phase A ``_PooledDriver`` uses, so a
    routed statement is byte-identical to the pre-routing pooled path. On a statement error the pooled
    connection is released as DESTROYED (poisoned — e.g. a fired statement timeout aborted it), so a
    poisoned connection never re-enters the pool; a clean statement releases it back.
    """

    __slots__ = ("_pool",)

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def execute(self, sql: str, params: Sequence[Any]) -> Rows:
        conn = self._pool.acquire()
        poisoned = True
        try:
            xform, emulate = _pool_exec_config(self._pool)
            rows = _conn_all(conn, sql, params, xform, emulate)
            poisoned = False
            return rows
        finally:
            self._pool.release(conn, poisoned)

    def run(self, sql: str, params: Sequence[Any]) -> RunInfo:
        conn = self._pool.acquire()
        poisoned = True
        try:
            xform, _emulate = _pool_exec_config(self._pool)
            info = _conn_run(conn, sql, params, xform)
            poisoned = False
            return info
        finally:
            self._pool.release(conn, poisoned)


def _identity(sql: str) -> str:
    return sql


def _pool_exec_config(pool: ConnectionPool) -> Tuple[Callable[[str], str], bool]:
    """The (``xform``, ``emulate_returning``) a :class:`PoolConnection` runs a statement with. A
    :class:`RawConnectionPool` (and a :class:`ConfiguredPool` wrapping one) carries the driver's real
    rewrite + RETURNING-emulation flag; any other pool degrades to an identity xform + no emulation
    (e.g. a bare test recording pool wrapping SQLite/`%s`-native connections)."""
    inner: Any = pool
    while isinstance(inner, ConfiguredPool):
        inner = inner._pool
    if isinstance(inner, RawConnectionPool):
        return inner.xform, inner.emulate_returning
    xform = getattr(inner, "xform", None)
    emulate = getattr(inner, "emulate_returning", None)
    return (xform if callable(xform) else _identity), (bool(emulate) if emulate is not None else False)


# ── Reader/writer pool pair (C1) ───────────────────────────────────────────────


class ReaderWriterPools:
    """A reader/writer pool PAIR for ONE named connection (C1). ``reader`` serves read-intent
    statements; ``writer`` serves write-intent statements, :func:`with_writer` reads, and writer-sticky
    reads. When a connection has no separate replica, ``reader IS writer`` is the SAME pool object —
    reader/writer routing then always lands on that one pool (the single-pool backward-compat case).
    Mirrors the TS ``ReaderWriterPools``.
    """

    __slots__ = ("reader", "writer")

    def __init__(self, reader: ConnectionPool, writer: ConnectionPool) -> None:
        self.reader = reader
        self.writer = writer


def single_pool_pair(pool: ConnectionPool) -> ReaderWriterPools:
    """Build a :class:`ReaderWriterPools` where reader IS writer (single-pool, backward-compat)."""
    return ReaderWriterPools(pool, pool)


def reader_writer_pair(reader: ConnectionPool, writer: ConnectionPool) -> ReaderWriterPools:
    """Build a :class:`ReaderWriterPools` from a distinct reader + writer pool (reader/writer split)."""
    return ReaderWriterPools(reader, writer)


# ── The connection registry (C2) — name → reader/writer pools ──────────────────


DEFAULT_CONNECTION = "default"


class ConnectionRegistry:
    """The multi-DB connection registry (C2): a map from a connection NAME → its
    :class:`ReaderWriterPools`. :func:`resolve_pool` selects the pair by ``intent.db`` (the connection
    name), falling back to :data:`DEFAULT_CONNECTION` when unnamed. Selecting a name that was never
    registered is a LOUD error (a real wiring bug — never a silent default fallback, which would run a
    query on the wrong DB). Mirrors the TS ``ConnectionRegistry``.

    A single-DB deployment registers exactly one connection under :data:`DEFAULT_CONNECTION` with
    reader IS writer ⇒ every intent routes to that one pool ⇒ byte-identical to Phase A/B.
    """

    __slots__ = ("_connections",)

    def __init__(self, connections: Dict[str, ReaderWriterPools]) -> None:
        self._connections: Dict[str, ReaderWriterPools] = dict(connections)

    @classmethod
    def single_default(cls, pool: ConnectionPool) -> "ConnectionRegistry":
        """Build a registry from ONE pool as the default connection (reader IS writer). The
        backward-compat path: an :class:`ExecutionContext` built from a single pool wraps it here so its
        ``connection_for`` routes every intent to that one pool."""
        return cls({DEFAULT_CONNECTION: single_pool_pair(pool)})

    @classmethod
    def from_default(cls, pools: ReaderWriterPools) -> "ConnectionRegistryBuilder":
        """Fluent builder: start from a default connection's pools, then ``.add(name, pools)`` more."""
        return ConnectionRegistryBuilder().add(DEFAULT_CONNECTION, pools)

    def pair_for(self, name: Optional[str]) -> ReaderWriterPools:
        """The reader/writer pair for ``name`` (or :data:`DEFAULT_CONNECTION` when ``None``). LOUD on a
        missing name."""
        key = name if name is not None else DEFAULT_CONNECTION
        pair = self._connections.get(key)
        if pair is None:
            known = ", ".join("'%s'" % k for k in self._connections.keys())
            raise ValueError(
                "scp connection routing: no connection registered under name '%s' (known: %s). "
                "Register it via set_config/ConnectionRegistry, or drop the connection tag on the "
                "bundle/model." % (key, known or "<none>")
            )
        return pair

    def names(self) -> List[str]:
        """The registered connection names (for diagnostics / :func:`close_all_pools`)."""
        return list(self._connections.keys())

    def distinct_pools(self) -> List[ConnectionPool]:
        """Every DISTINCT pool object across all connections (a shared reader IS writer counts once)."""
        seen: List[ConnectionPool] = []
        seen_ids = set()
        for pair in self._connections.values():
            for p in (pair.reader, pair.writer):
                if id(p) not in seen_ids:
                    seen_ids.add(id(p))
                    seen.append(p)
        return seen


class ConnectionRegistryBuilder:
    """Incremental :class:`ConnectionRegistry` builder (name → pools). Mirrors the TS builder."""

    __slots__ = ("_connections",)

    def __init__(self) -> None:
        self._connections: Dict[str, ReaderWriterPools] = {}

    def add(self, name: str, pools: ReaderWriterPools) -> "ConnectionRegistryBuilder":
        """Register ``name`` → its reader/writer pools (chainable). Re-adding a name overwrites it."""
        self._connections[name] = pools
        return self

    def build(self) -> ConnectionRegistry:
        """Finalize into an immutable :class:`ConnectionRegistry`."""
        if not self._connections:
            raise ValueError(
                "scp connection routing: ConnectionRegistry must have at least the default connection"
            )
        return ConnectionRegistry(self._connections)


# ── Writer-sticky + with_writer (C1) ───────────────────────────────────────────


# Ambient "route reads to the writer" marker (mirror v1 withWriter writer context). A contextvar so a
# concurrent scope (a distinct thread) sees its OWN value — the Python analogue of the TS AsyncLocalStorage.
_writer_scope_var: "contextvars.ContextVar[bool]" = contextvars.ContextVar(
    "litedbmodel_scp_writer_scope", default=False
)

# A bare read-only marker for the `with_writer` write-reject half when routing is not ctx-threaded
# (there is no ambient ExecutionContext to derive a read-only view from). The guarded write seam
# consults this in addition to the ambient-ctx read-only marker.
_readonly_marker_var: "contextvars.ContextVar[bool]" = contextvars.ContextVar(
    "litedbmodel_scp_writer_readonly", default=False
)


def in_writer_scope() -> bool:
    """True if the current execution scope is inside a :func:`with_writer` scope."""
    return _writer_scope_var.get()


def in_writer_read_only_scope() -> bool:
    """True if the current scope is inside a :func:`with_writer` read-only (write-reject) marker. The
    guarded write seam ORs this with the ambient-ctx read-only marker so a write inside
    :func:`with_writer` is rejected even when routing is not ctx-threaded."""
    return _readonly_marker_var.get()


def with_writer(fn: Callable[[], Any]) -> Any:
    """Run ``fn`` with reads pinned to the WRITER pool (mirror v1 ``DBModel.withWriter``): every read
    ``fn`` issues resolves the writer pool (read-your-writes without replication lag), and — because
    this ALSO enters a READ-ONLY scope — ANY write inside ``fn`` funneled through the GUARDED write seam
    raises :class:`WriteInReadOnlyContextError`. Nested :func:`with_writer` is idempotent (already in a
    writer scope ⇒ just run ``fn``). Inside a transaction the tx-owned connection already wins in
    ``connection_for``, so a :func:`with_writer` there is a no-op on routing (matches v1).

    The read-only (write-reject) half is entered by a bare read-only marker the guarded write seam
    consults, AND — when an ambient routed :class:`ExecutionContext` is pinned in the current scope — by
    deriving its read-only view so ``ambient_read_only()`` (which reads the ctx) also sees the
    write-reject half. v1's single writerContext is BOTH the writer-routing and the read-only marker;
    both halves span the whole ``fn`` because the contextvars are per-scope.
    """
    if in_writer_scope():
        return fn()
    token = _writer_scope_var.set(True)
    ro_token = _readonly_marker_var.set(True)
    try:
        from .exec_context import current_context, run_with_pinned_context

        ambient = current_context()
        if ambient is not None and not ambient.read_only():
            return run_with_pinned_context(ambient.with_read_only(), fn)
        return fn()
    finally:
        _readonly_marker_var.reset(ro_token)
        _writer_scope_var.reset(token)


class WriterStickyClock:
    """A writer-sticky CLOCK (C1, read-your-writes; v1 ``_shouldUseWriterSticky`` + ``_lastTransactionTime``).
    After a transaction (or a bare write) COMMITs, reads within ``sticky_duration_ms`` route to the
    WRITER pool so a just-committed row is visible despite reader-replica lag. The ctx owns ONE clock
    instance; the tx runtime :meth:`mark` s it on every successful write/commit; :func:`resolve_pool`
    reads :meth:`is_sticky`.

    ``use_writer_after_transaction=False`` disables it entirely (:meth:`is_sticky` always False). A
    single-pool deployment (reader IS writer) is unaffected — the diverted pool is the same object.
    The ``now`` callable is INJECTABLE (tests advance it deterministically); defaults to
    ``time.monotonic``-based ms. Mirrors the TS ``WriterStickyClock``.
    """

    __slots__ = ("_last_write_at", "_enabled", "_sticky_duration_ms", "_now")

    def __init__(
        self,
        use_writer_after_transaction: bool = True,
        writer_sticky_duration: int = 5000,
        now: Optional[Callable[[], int]] = None,
    ) -> None:
        self._last_write_at = 0
        self._enabled = use_writer_after_transaction
        self._sticky_duration_ms = writer_sticky_duration
        # Default clock: monotonic ms (Date.now analogue). An injectable clock makes expiry deterministic.
        self._now: Callable[[], int] = now if now is not None else (lambda: int(time.monotonic() * 1000))

    def mark(self) -> None:
        """Record that a write/commit just happened (the tx runtime calls this on success)."""
        if self._enabled:
            self._last_write_at = self._now()

    def is_sticky(self) -> bool:
        """Is a read currently sticky-to-writer (within ``writer_sticky_duration`` of the last write)?"""
        if not self._enabled or self._last_write_at == 0:
            return False
        return (self._now() - self._last_write_at) < self._sticky_duration_ms

    def reset(self) -> None:
        """Reset the clock (e.g. between tests / on :func:`close_all_pools`)."""
        self._last_write_at = 0


# ── The routing config a ctx carries (C1+C2+C3) ────────────────────────────────


class RoutingConfig:
    """The routing configuration an :class:`ExecutionContext` carries to complete its
    ``connection_for(intent)`` resolution (steps 2-4): the multi-DB :class:`ConnectionRegistry` and the
    :class:`WriterStickyClock`. Absent ⇒ the ctx falls back to its single default driver with an
    always-false sticky clock — the byte-identical Phase A/B path. Mirrors the TS ``RoutingConfig``.
    """

    __slots__ = ("registry", "sticky")

    def __init__(self, registry: ConnectionRegistry, sticky: WriterStickyClock) -> None:
        self.registry = registry
        self.sticky = sticky


def resolve_pool(intent: StatementIntent, routing: RoutingConfig) -> ConnectionPool:
    """Resolve WHICH pool serves a statement given its ``intent`` and the routing config — the
    completion of ``connection_for``'s steps 2-4 (step 1, the tx-pin, is handled by the ctx BEFORE
    calling this, since only the ctx holds the pin). The order:

      1. **named-DB** (``intent.db``) selects the :class:`ReaderWriterPools` pair (loud on unknown name).
      2. within that pair: a WRITE ⇒ the writer pool.
      3. a READ in a :func:`with_writer` scope OR within writer-sticky ⇒ the writer pool (read-your-writes).
      4. otherwise a READ ⇒ the reader pool.

    Single-pool (reader IS writer) ⇒ every branch returns the same pool (backward-compat). Mirrors the
    TS ``resolvePool``.
    """
    pair = routing.registry.pair_for(intent.db)
    if intent.write:
        return pair.writer  # writes always to the writer
    if in_writer_scope() or routing.sticky.is_sticky():
        return pair.writer  # read-your-writes
    return pair.reader  # plain read → reader


# ── Session-config pool wrapper (C3) ───────────────────────────────────────────


class ConfiguredPool(ConnectionPool):
    """Wrap a :class:`ConnectionPool` so every acquired connection first runs the
    :func:`session_statements` for ``config`` (statement timeout / search_path / charset) and, on
    release, runs :func:`session_reset_statements` to restore the server defaults (so a pooled
    connection never leaks THIS config's session state to the next caller — psycopg/PyMySQL don't
    auto-reset on release). A config with no session knobs (all defaults) ⇒ ZERO extra statements ⇒
    :func:`configured_pool` returns the pool UNWRAPPED (a transparent passthrough, backward-compat).
    Mirrors the TS ``configuredPool``.

    If a session statement itself fails (e.g. an invalid search_path), the connection is released as
    DESTROYED so a mis-configured connection never re-enters the pool. On release, if the caller reports
    the connection as poisoned (``destroy=True`` — e.g. a fired statement timeout aborted it), the reset
    is SKIPPED and the connection is DESTROYED. A CLEAN connection is reset to defaults; if the reset
    itself fails the connection state is unknown ⇒ it is DESTROYED.
    """

    __slots__ = ("_pool", "_session", "_reset")

    def __init__(self, pool: ConnectionPool, config: ResolvedConnectionConfig) -> None:
        self._pool = pool
        self._session = session_statements(config)
        self._reset = session_reset_statements(config)

    def acquire(self) -> Any:
        conn = self._pool.acquire()
        try:
            _run_session_statements(conn, self._session)
        except Exception:
            self._pool.release(conn, True)  # a failed session setup poisons the connection — drop it
            raise
        return conn

    def release(self, conn: Any, destroy: bool = False) -> None:
        # A poisoned/aborted connection is dropped — no point resetting it, and a reset on an
        # aborted-by-timeout connection would itself fail.
        if destroy:
            self._pool.release(conn, True)
            return
        try:
            _run_session_statements(conn, self._reset)
        except Exception:
            self._pool.release(conn, True)  # reset failed ⇒ connection state unknown ⇒ drop it
            return
        self._pool.release(conn, False)


def _run_session_statements(conn: Any, statements: Sequence[str]) -> None:
    """Run each session/reset statement on ``conn`` (one cursor per statement, closed after)."""
    for stmt in statements:
        cur = conn.cursor()
        try:
            cur.execute(stmt)
        finally:
            cur.close()


def configured_pool(pool: ConnectionPool, config: ResolvedConnectionConfig) -> ConnectionPool:
    """Wrap ``pool`` with session-config apply/reset (C3), OR return it UNWRAPPED when ``config`` sets
    no session knobs (all defaults ⇒ ZERO extra statements ⇒ byte-identical passthrough). This is where
    C3's ``query_timeout`` / ``search_path`` / ``charset`` become REAL per-server effects. Mirrors the
    TS ``configuredPool``.
    """
    if not session_statements(config):
        return pool  # no knobs set ⇒ transparent passthrough (byte-identical)
    return ConfiguredPool(pool, config)


# ── set_config / close_all_pools (C3 public surface) ───────────────────────────


# A pool CLOSER — closes a pool's underlying connections (the Python analogue of pg `pool.end()`).
PoolCloser = Callable[[], None]

# A driver's pool factory: BUILD a pool from a ResolvedConnectionConfig, returning the ConnectionPool
# seam adapter plus a PoolCloser. `role` lets a factory build a distinct replica pool for the reader vs
# writer while sharing the same sizing config. The SIZING (min_pool/max_pool) + keep_alive must be
# applied at pool CONSTRUCTION (a pre-built pool can no longer accept them). Signature mirrors the TS
# `PoolFactory`: (config, role) -> { pool, close }, python idiom = returns a (pool, close) tuple.
PoolFactory = Callable[[ResolvedConnectionConfig, str], Tuple[ConnectionPool, PoolCloser]]


class ConnectionSetup:
    """One connection's inputs to :func:`build_routing_config`: its NAME (default when absent), its
    :class:`ConnectionConfig` (connection params + sizing + keepalive + session knobs), and a
    :data:`PoolFactory` that :func:`build_routing_config` CALLS with the resolved config to construct
    the pool(s) — so sizing/keepalive are applied at construction and the config is the sole cap source.

    ``separate_writer=True`` asks the factory for a DISTINCT writer pool (reader/writer replica split);
    otherwise the factory's reader pool is reused as the writer (single-pool, reader IS writer). Mirrors
    the TS ``ConnectionSetup``.
    """

    __slots__ = ("name", "config", "pool_factory", "separate_writer")

    def __init__(
        self,
        pool_factory: PoolFactory,
        name: Optional[str] = None,
        config: Optional[ConnectionConfig] = None,
        separate_writer: bool = False,
    ) -> None:
        self.name = name
        self.config = config
        self.pool_factory = pool_factory
        self.separate_writer = separate_writer


def build_routing_config(
    setups: Sequence[ConnectionSetup],
    use_writer_after_transaction: bool = True,
    writer_sticky_duration: int = 5000,
) -> Tuple[RoutingConfig, PoolCloser]:
    """The C3 ``set_config`` result: the :class:`RoutingConfig` an :class:`ExecutionContext` runs on,
    plus a ``close()`` that shuts every constructed pool (:func:`close_all_pools`). Build it from one or
    more :class:`ConnectionSetup` s (the one named ``default``, or the first unnamed, is the default
    connection). Mirrors the TS ``buildRoutingConfig``.

    For each setup: resolve the config, CALL its :data:`PoolFactory` to construct the pool(s) — so
    ``min_pool`` / ``max_pool`` / ``keep_alive`` / ``keep_alive_initial_delay_millis`` are applied at
    pool-construction time (the config is the SOLE source of the cap) — then wrap each pool with
    :func:`configured_pool` so the SESSION knobs (query_timeout/search_path/charset) apply on checkout.
    """
    if not setups:
        raise ValueError("scp set_config: at least one connection setup is required")
    builder = ConnectionRegistryBuilder()
    closers: List[PoolCloser] = []
    for s in setups:
        resolved = resolve_connection_config(s.config)
        # CONSTRUCT the reader pool from the resolved config (sizing/keepAlive land at pool build time).
        reader_pool, reader_close = s.pool_factory(resolved, "reader")
        closers.append(reader_close)
        reader = configured_pool(reader_pool, resolved)
        if s.separate_writer:
            writer_pool, writer_close = s.pool_factory(resolved, "writer")
            closers.append(writer_close)
            pair = reader_writer_pair(reader, configured_pool(writer_pool, resolved))
        else:
            pair = single_pool_pair(reader)  # reader IS writer (one constructed pool)
        builder.add(s.name if s.name is not None else DEFAULT_CONNECTION, pair)
    routing = RoutingConfig(
        registry=builder.build(),
        sticky=WriterStickyClock(
            use_writer_after_transaction=use_writer_after_transaction,
            writer_sticky_duration=writer_sticky_duration,
        ),
    )
    return routing, _close_all(closers)


def _close_all(closers: Sequence[PoolCloser]) -> PoolCloser:
    def closer() -> None:
        close_all_pools(closers)

    return closer


def close_all_pools(closers: Sequence[PoolCloser]) -> None:
    """Close every DISTINCT pool closer (deduped by identity), tolerating individual failures. Mirrors
    the TS ``closeRouting``."""
    seen = set()
    for c in closers:
        if id(c) in seen:
            continue
        seen.add(id(c))
        try:
            c()
        except Exception:
            pass


# ── The pg / mysql pool factories — sizing/keepAlive applied at CONSTRUCTION ────
#
# `build_routing_config` OWNS pool construction so the C3 config's CONSTRUCTION knobs — pool sizing
# (min_pool/max_pool) + keep_alive — reach the real pool at build time (a pre-built pool can no longer
# accept them). These factories map a ResolvedConnectionConfig to a psycopg / PyMySQL-backed
# `_ConnectionPool` sized to `max_pool` and return the ConnectionPool adapter + closer. The configured
# sizing is thus the SOLE source of the pool cap. Session knobs (query_timeout/search_path/charset) are
# layered separately by `configured_pool` on checkout.
#
# ── keep_alive honesty (per-driver, mirror the TS mysql2 minPool note) ──
#   - psycopg (PG): TCP keepalive is a libpq CONNECTION parameter, so `keep_alive=True` maps to
#     `keepalives=1` + `keepalives_idle=<seconds>` on `psycopg.connect(...)` (idle is SECONDS — the
#     ms `keep_alive_initial_delay_millis` is converted, min 1s). This is applied at construction (each
#     factory-built connection carries it), matching "keepAlive at construction".
#   - PyMySQL (MySQL): PyMySQL exposes NO TCP-keepalive connect option (unlike mysql2's
#     `enableKeepAlive`). So `keep_alive` is a documented NO-OP on MySQL here — we do NOT invent a bogus
#     option. (The analogue of the TS `mysqlPoolFactory` note that mysql2 has no `min` idle floor.)
#   - min_pool: the dependency-free `_ConnectionPool` opens connections LAZILY up to `max_pool`; it has
#     no warm-idle floor, so `min_pool` is a documented NO-OP for both drivers (same as mysql2's absent
#     `min`). The SIZING CAP that matters — `max_pool` → the pool ceiling — IS honored at construction.


def _pg_connect_kwargs(config: ResolvedConnectionConfig) -> Dict[str, Any]:
    """The psycopg.connect kwargs from ``config`` (connection target + keepalive). Autocommit ON so the
    literal BEGIN…COMMIT bracket a REAL transaction (same as the Phase A ``PostgresDriver``)."""
    kwargs: Dict[str, Any] = {"autocommit": True}
    if config.host is not None:
        kwargs["host"] = config.host
    if config.port is not None:
        kwargs["port"] = config.port
    if config.database is not None:
        kwargs["dbname"] = config.database
    if config.user is not None:
        kwargs["user"] = config.user
    if config.password is not None:
        kwargs["password"] = config.password
    if config.keep_alive:
        # libpq TCP keepalive (SECONDS for the idle delay; convert the ms knob, min 1s).
        kwargs["keepalives"] = 1
        idle_s = max(1, int(round(config.keep_alive_initial_delay_millis / 1000.0)))
        kwargs["keepalives_idle"] = idle_s
    return kwargs


def pg_pool_factory() -> PoolFactory:
    """A :data:`PoolFactory` for psycopg (PG): constructs a :class:`_ConnectionPool` of autocommit
    psycopg connections sized to ``max_pool`` (the SOLE cap source), applying ``keep_alive`` (libpq
    ``keepalives`` — see the keep_alive-honesty note above) AT CONSTRUCTION. Connection params flow from
    the config. Returns the :class:`ConnectionPool` adapter + closer. Mirrors the TS ``pgPoolFactory``.

    ``role`` is accepted for the reader/writer replica split; the caller may vary host per role via the
    config it passes.
    """

    def factory(config: ResolvedConnectionConfig, _role: str) -> Tuple[ConnectionPool, PoolCloser]:
        import psycopg  # lazy peer dep — the SQLite conformance never needs it

        kwargs = _pg_connect_kwargs(config)

        def conn_factory() -> Any:
            return psycopg.connect(**kwargs)

        raw = _ConnectionPool(conn_factory, config.max_pool)  # SIZING at construction: max_pool = the cap
        pool = RawConnectionPool(raw, _dollar_to_pyformat, emulate_returning=False)
        return pool, raw.close

    return factory


def _mysql_connect_kwargs(config: ResolvedConnectionConfig) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {"autocommit": True}
    if config.host is not None:
        kwargs["host"] = config.host
    if config.port is not None:
        kwargs["port"] = config.port
    if config.database is not None:
        kwargs["database"] = config.database
    if config.user is not None:
        kwargs["user"] = config.user
    if config.password is not None:
        kwargs["password"] = config.password
    # NB: PyMySQL exposes no TCP-keepalive connect option ⇒ keep_alive is a documented NO-OP on MySQL
    # (mirror the TS mysqlPoolFactory's "mysql2 has no min idle floor" per-driver deviation note).
    return kwargs


def mysql_pool_factory() -> PoolFactory:
    """A :data:`PoolFactory` for PyMySQL (MySQL): constructs a :class:`_ConnectionPool` of autocommit
    PyMySQL connections sized to ``max_pool`` (the SOLE cap source) AT CONSTRUCTION, with MySQL
    RETURNING emulation enabled (the Phase A ``MysqlDriver`` behavior). Connection params flow from the
    config. Returns the :class:`ConnectionPool` adapter + closer. Mirrors the TS ``mysqlPoolFactory``.

    NB: ``keep_alive`` / ``min_pool`` are documented NO-OPs on the MySQL path (PyMySQL has no keepalive
    connect option; the dependency-free ``_ConnectionPool`` has no warm-idle floor). The SIZING CAP —
    ``max_pool`` → the pool ceiling — is honored at construction.
    """

    def factory(config: ResolvedConnectionConfig, _role: str) -> Tuple[ConnectionPool, PoolCloser]:
        import pymysql  # lazy peer dep

        kwargs = _mysql_connect_kwargs(config)

        def conn_factory() -> Any:
            return pymysql.connect(**kwargs)

        raw = _ConnectionPool(conn_factory, config.max_pool)  # SIZING at construction: max_pool = the cap
        pool = RawConnectionPool(raw, _qmark_to_pyformat, emulate_returning=True)
        return pool, raw.close

    return factory


# ── Routed transaction acquisition (Phase C-2) — tx on the named connection's writer ──


class _TxPoolAdapter:
    """Present a routing :class:`ConnectionPool` (session-aware, ``release(conn, destroy)``) as the
    ``_ConnectionPool``-shaped interface the Phase A :class:`litedbmodel_runtime.driver._PooledTxConnection`
    drives (``acquire()`` / ``release(conn)`` / ``discard(conn)``). This lets a routed transaction acquire
    its OWNED connection from the target NAMED connection's WRITER pool while STILL honoring the C3
    session config (a :class:`ConfiguredPool` writer applies session statements on acquire and resets on
    release) — no session leak on the tx connection either. The Phase A owned-connection tx lifecycle
    (BEGIN/COMMIT/ROLLBACK on ONE connection, the single release in a ``finally``, the #78 discard) is
    UNCHANGED — only WHERE the connection comes from moves from the driver's own pool to the routed
    writer pool.
    """

    __slots__ = ("_pool",)

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def acquire(self) -> Any:
        return self._pool.acquire()

    def release(self, conn: Any) -> None:
        self._pool.release(conn, False)

    def discard(self, conn: Any) -> None:
        # A poisoned tx connection (a COMMIT/ROLLBACK that itself raised): drop it (close + free a pool
        # slot — the #78 leak fix), and SKIP the session reset (an aborted connection can't be reset).
        self._pool.release(conn, True)


def routed_begin_tx(
    routing: RoutingConfig,
    connection: Optional[str],
) -> TxConnection:
    """Acquire + OWN one :class:`litedbmodel_runtime.driver._PooledTxConnection` for a transaction on the
    NAMED connection ``connection``'s WRITER pool (Phase C-2; ``None`` ⇒ the default connection). The
    tx runs entirely on this ONE connection — the active-tx pin then wins over routing for every
    statement in the body (Phase B unbroken). tx-control (the isolation SET / BEGIN / COMMIT / ROLLBACK)
    is issued THROUGH the seam on this pinned connection by the combinator (Phase D / #95,
    middleware-visible), NOT here. The writer pool's ``xform`` (``$N``/``?`` → ``%s``) +
    ``emulate_returning`` flag drive the tx statements byte-identically to the Phase A
    ``_PooledTxConnection`` path; a :class:`ConfiguredPool` writer additionally applies/resets the C3
    session config on the tx connection (no session leak).
    """
    writer = routing.registry.pair_for(connection).writer
    xform, emulate = _pool_exec_config(writer)
    return _PooledTxConnection(_TxPoolAdapter(writer), xform, emulate)
