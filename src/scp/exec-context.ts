/**
 * litedbmodel v2 SCP — the **ExecutionContext + central execute/run seam** (Phase A / #75).
 *
 * This module is the CONTRACT-DEFINING artifact for Phase A: it fixes the shape every runtime
 * (TS here; rust #76 / go #77 / py #78 / php #79 port it) follows. It replaces the raw `driver`
 * parameter threaded through `executeBundle` / `executeReadGraph` / `executeTransaction` with an
 * {@link ExecutionContext} that carries:
 *
 *   1. a **connection provider** — {@link ExecutionContext.connectionFor}`(intent)` resolves WHICH
 *      connection a statement runs on (tx-owned / reader / writer / named-DB; Phase A only wires the
 *      tx-owned + single-DB cases, the rest are B/C/D on this same seam);
 *   2. a **middleware chain** — {@link ExecutionContext.middleware}, wrapping every SQL (empty in
 *      Phase A = passthrough; the registration API is Phase D — this is only the hook point);
 *   3. {@link ExecutionContext.withConnection}`(conn, tx)` — derive a tx-scoped ctx that pins ONE
 *      connection so every statement in a transaction body runs on it.
 *
 * ## The central seam (§2 of the design) — ALL SQL funnels through here
 *
 * ```
 *   execute(ctx, sql, params) -> Rows      // SELECT / RETURNING reads
 *   run(ctx, sql, params)     -> RunInfo   // INSERT/UPDATE/DELETE, BEGIN/COMMIT/ROLLBACK
 * ```
 *
 * Both do the SAME three things, in order:
 *   ① run the middleware chain (empty ⇒ passthrough, behavior unchanged);
 *   ② resolve the connection via `ctx.connectionFor(intent)`;
 *   ③ execute on that connection (the ONLY driver contact point).
 *
 * Every direct `driver.prepare(sql).all()/run()` in the read / tx / relation path is replaced by a
 * call through this seam. A `grep` for `.prepare(` outside this file's connection adapters must come
 * up empty in the runtime SQL path — that is the AC.
 *
 * ## Two flavors — sync (SQLite) and async (live PG / MySQL)
 *
 * The in-process better-sqlite3 path is **synchronous** (and must stay byte-identical to the frozen
 * conformance corpus), so it uses a {@link SyncConnection} and the {@link execute} / {@link run}
 * seam. The live PG / MySQL path is **async + pooled**, where per-execution connection ownership is
 * what actually fixes the concurrent-tx isolation bug; it uses an {@link AsyncConnection} and the
 * {@link executeAsync} / {@link runAsync} seam plus {@link withTransactionAsync}.
 *
 * Both flavors implement the SAME contract shape (`connectionFor` / `middleware` / `withConnection`
 * / a central seam / `withTransaction`); the native ports mirror whichever their runtime needs
 * (rust/go = the owned-connection async model; py = contextvars; php = the single-connection sync
 * model — same contract, no ALS since PHP is 1-request-1-process).
 *
 * ## Per-execution connection ownership (§3) — the concurrent-tx fix
 *
 * A transaction acquires ONE connection, scopes it into an {@link AsyncLocalStorage} ctx, runs its
 * body (every statement resolves that connection via `connectionFor`), COMMITs/ROLLBACKs, and
 * releases it. Concurrent transactions each run in their own ALS ctx with their own connection —
 * **isolated**. There is NO driver-global single-slot writer (the shared-slot model — `pool.query`
 * per statement, or a `Mutex<Option<writer>>` — is what corrupts concurrent transactions; see the
 * `test/integration/tx-isolation` proof).
 */

import { AsyncLocalStorage } from 'node:async_hooks';
import {
  type TransactionOptions,
  beginStatements,
  resolveTxOptions,
  isRetryableTxError,
  sleep,
  runInTransactionScope,
  checkWriteAllowed,
} from './tx-options';
import {
  type RoutingConfig,
  ConnectionRegistry,
  WriterStickyClock,
  resolvePool,
} from './connection-routing';
import type { Dialect } from './makesql/handler';
import { activeSqlMiddlewares } from './middleware';

// ── Statement intent & the driver contact (§5) ────────────────────────────────

/**
 * What a statement needs from the connection provider (§3): whether it writes (so it must go to a
 * writer / the tx-owned connection, never a read replica) and an optional named DB (multi-DB
 * routing, Phase B). Phase A resolves only `write` (tx-owned vs. primary) and ignores `db` (single
 * DB); the field is in the contract now so the native ports declare the SAME shape and B/C/D extend
 * the resolver — not the seam.
 */
export interface StatementIntent {
  /** The statement writes (INSERT/UPDATE/DELETE or BEGIN/COMMIT/ROLLBACK) ⇒ writer / tx connection. */
  readonly write: boolean;
  /** Named-DB routing key (multi-DB, Phase B). Absent ⇒ the primary DB. */
  readonly db?: string;
}

/** A SELECT / RETURNING result: the raw driver rows. */
export type Rows = Record<string, unknown>[];

/** A non-returning write summary (INSERT/UPDATE/DELETE affected + last insert id). */
export interface RunInfo {
  readonly changes: number;
  readonly lastInsertRowid: number | bigint;
}

/**
 * The ONE driver contact point (§5) — a single owned SQLite connection (better-sqlite3 `Database`,
 * or one pooled handle). The seam is the ONLY caller; the runtime SQL path never touches a driver
 * directly. `prepareRaw` is escape-hatched ONLY for the read path's `safeIntegers` toggle (a
 * better-sqlite3 statement-level knob the #59 BIGINT de-box needs) — it is NOT a general driver door.
 */
export interface SyncConnection {
  /** Run a SELECT / RETURNING statement; resolve the raw rows. */
  execute(sql: string, params: readonly unknown[]): Rows;
  /**
   * Run a SELECT in better-sqlite3 `safeIntegers` mode (the #59 BIGINT-exact read: int8 comes back
   * as an EXACT `bigint`, not a rounded JS number). A backend with no such toggle falls back to a
   * plain {@link SyncConnection.execute}. Kept BEHIND the seam so the read walker never touches a
   * raw prepared statement — the `safeIntegers` knob is a driver detail, not a runtime-path concern.
   */
  executeSafeIntegers(sql: string, params: readonly unknown[]): Rows;
  /** Run a non-returning write / DDL / tx-control statement; resolve the affected summary. */
  run(sql: string, params: readonly unknown[]): RunInfo;
}

/** The minimal better-sqlite3 prepared statement the read path drives (with the `safeIntegers` knob). */
export interface RawStmt {
  all(...params: unknown[]): unknown[];
  run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
  safeIntegers?(v: boolean): unknown;
}

/** The async twin: one OWNED pooled connection (PG client / mysql2 connection). */
export interface AsyncConnection {
  execute(sql: string, params: readonly unknown[]): Promise<Rows>;
  run(sql: string, params: readonly unknown[]): Promise<RunInfo>;
}

// ── Middleware chain (§4) — the hook point (empty in Phase A) ──────────────────

/** The terminal of a middleware chain: resolve the connection + execute (the seam's ②③). */
export type SeamNext<T> = (sql: string, params: readonly unknown[]) => T;

/** One middleware: wrap a statement, delegating to `next` (Phase D supplies the registration API). */
export type Middleware<T> = (sql: string, params: readonly unknown[], next: SeamNext<T>) => T;

/**
 * A source of the CURRENT SQL-level middleware stack, resolved at `wrap` time (§4, Phase D). A ctx
 * built by the backward-compat factories carries a source that reads the ambient/global
 * {@link import('./middleware').MiddlewareRegistry} — so a middleware registered AFTER the ctx was
 * built (the normal `DBModel.use(...)`-then-query order) is still seen, and a per-context registry
 * override (concurrent-request isolation) is honored. Absent ⇒ a fixed (possibly empty) stack.
 */
export type MiddlewareStackSource = () => readonly Middleware<unknown>[];

/**
 * The ordered middleware chain a ctx carries (§4). `wrap` folds the middlewares around `next`
 * (the connection-resolve + execute terminal). An EMPTY chain is a pure passthrough — `wrap`
 * returns `next(sql, params)` verbatim, so an unregistered chain is byte-identical. The chain is
 * generic over the seam result type `T` so ONE shape serves both the sync (`Rows`/`RunInfo`) and
 * async (`Promise<…>`) seams.
 *
 * The stack is resolved by a {@link MiddlewareStackSource} at EACH `wrap` (Phase D) rather than
 * captured at construction, so registration (`DBModel.use`) that happens after the ctx is built is
 * still honored and per-execution-scope registries (concurrent isolation) resolve correctly. A chain
 * constructed with an explicit fixed stack (or none) still works — that stack is its constant source.
 */
export class MiddlewareChain {
  /** Resolve the CURRENT registered SQL-level middlewares, outermost first, at `wrap` time. */
  private readonly source: MiddlewareStackSource;

  constructor(stackOrSource: readonly Middleware<unknown>[] | MiddlewareStackSource = []) {
    this.source = typeof stackOrSource === 'function' ? stackOrSource : () => stackOrSource;
  }

  /** Is the chain empty RIGHT NOW (⇒ this `wrap` is a guaranteed passthrough)? */
  get isEmpty(): boolean {
    return this.source().length === 0;
  }

  /** Fold the chain around `next`, then invoke it. Empty chain ⇒ `next(sql, params)` verbatim. */
  wrap<T>(sql: string, params: readonly unknown[], next: SeamNext<T>): T {
    const stack = this.source();
    if (stack.length === 0) return next(sql, params);
    let fn = next as SeamNext<unknown>;
    for (let i = stack.length - 1; i >= 0; i--) {
      const mw = stack[i];
      const inner = fn;
      fn = (s, p) => mw(s, p, inner);
    }
    return fn(sql, params) as T;
  }
}

// ── The ExecutionContext contract (§2 / §5) ───────────────────────────────────

/**
 * The SYNC execution context (better-sqlite3). Carries the connection provider, the middleware
 * chain, and `withConnection` to derive a tx-scoped ctx. The runtime passes THIS (not a raw driver)
 * to `executeBundle` / `executeReadGraph` / `executeTransaction`.
 */
export interface ExecutionContext {
  /** Resolve WHICH connection a statement runs on (§3). Phase A: the tx-owned conn, else the primary. */
  connectionFor(intent: StatementIntent): SyncConnection;
  /** The middleware chain wrapping every SQL (§4). Empty in Phase A. */
  readonly middleware: MiddlewareChain;
  /** Derive a tx-scoped ctx pinning `conn` (every statement resolves it while `tx` is true). */
  withConnection(conn: SyncConnection, tx: boolean): ExecutionContext;
}

/** The ASYNC execution context (live PG / MySQL) — the same contract, async connections. */
export interface AsyncExecutionContext {
  connectionFor(intent: StatementIntent): AsyncConnection;
  readonly middleware: MiddlewareChain;
  withConnection(conn: AsyncConnection, tx: boolean): AsyncExecutionContext;
}

// ── The central seam (§2) — the ONLY place SQL meets a connection ─────────────

/**
 * Central READ seam (sync): ① middleware chain, ② resolve the connection, ③ execute. Every read
 * (primary read node, relation batch) funnels through here.
 */
export function execute(ctx: ExecutionContext, sql: string, params: readonly unknown[], intent: StatementIntent = { write: false }): Rows {
  return ctx.middleware.wrap<Rows>(sql, params, (s, p) => ctx.connectionFor(intent).execute(s, p));
}

/**
 * Central READ seam (sync) in `safeIntegers` mode (§2 + the #59 BIGINT-exact read). Same
 * middleware → connectionFor → execute path, but the terminal runs the SELECT in exact-integer mode
 * so int8/BIGINT columns materialize without rounding. Used by the read walker for a BIGINT-
 * projecting node; a non-BIGINT node uses the plain {@link execute}.
 */
export function executeSafe(ctx: ExecutionContext, sql: string, params: readonly unknown[], intent: StatementIntent = { write: false }): Rows {
  return ctx.middleware.wrap<Rows>(sql, params, (s, p) => ctx.connectionFor(intent).executeSafeIntegers(s, p));
}

/**
 * Central WRITE seam (sync): ① middleware chain, ② resolve the connection, ③ run. Every write and
 * every tx-control statement (BEGIN/COMMIT/ROLLBACK) funnels through here (writes ⇒ `intent.write`).
 */
export function run(ctx: ExecutionContext, sql: string, params: readonly unknown[], intent: StatementIntent = { write: true }): RunInfo {
  return ctx.middleware.wrap<RunInfo>(sql, params, (s, p) => ctx.connectionFor(intent).run(s, p));
}

/** Central READ seam (async) — the live PG / MySQL twin of {@link execute}. */
export function executeAsync(ctx: AsyncExecutionContext, sql: string, params: readonly unknown[], intent: StatementIntent = { write: false }): Promise<Rows> {
  return ctx.middleware.wrap<Promise<Rows>>(sql, params, (s, p) => ctx.connectionFor(intent).execute(s, p));
}

/** Central WRITE seam (async) — the live PG / MySQL twin of {@link run}. */
export function runAsync(ctx: AsyncExecutionContext, sql: string, params: readonly unknown[], intent: StatementIntent = { write: true }): Promise<RunInfo> {
  return ctx.middleware.wrap<Promise<RunInfo>>(sql, params, (s, p) => ctx.connectionFor(intent).run(s, p));
}

/**
 * GUARDED write seam (sync): enforce the write=tx guard ({@link checkWriteAllowed}) for a
 * DATA-mutating statement, then delegate to {@link run}. A write issued OUTSIDE a transaction throws
 * {@link WriteOutsideTransactionError}; a write in a {@link withReadOnly} scope throws
 * {@link WriteInReadOnlyContextError}. Tx-control statements (BEGIN/COMMIT/ROLLBACK/SET) are NOT
 * guarded — the tx runtime issues them to OPEN the very scope the guard checks. This is the seam a
 * bare model-level write (create/update/delete/upsert/batch) goes through.
 */
export function runGuarded(ctx: ExecutionContext, sql: string, params: readonly unknown[], operation: string, modelName?: string): RunInfo {
  checkWriteAllowed(operation, modelName);
  return run(ctx, sql, params, { write: true });
}

/**
 * GUARDED write seam (async) — the live PG / MySQL twin of {@link runGuarded}. The guard is enforced
 * inside the returned promise (an async caller gets a REJECTED promise, never a synchronous throw).
 */
export async function runGuardedAsync(ctx: AsyncExecutionContext, sql: string, params: readonly unknown[], operation: string, modelName?: string): Promise<RunInfo> {
  checkWriteAllowed(operation, modelName);
  return runAsync(ctx, sql, params, { write: true });
}

// ── Sync ctx (SQLite) — the backward-compat driver wrapper (§6) ───────────────

/** The minimal synchronous SQLite driver surface (better-sqlite3 `Database`) the runtime accepts. */
export interface SqliteDriver {
  prepare(sql: string): {
    all(...params: unknown[]): unknown[];
    run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
    safeIntegers?(v: boolean): unknown;
  };
}

/** Adapt a better-sqlite3 `Database` to the {@link SyncConnection} seam (the ONE driver contact). */
export function connectionForDriver(driver: SqliteDriver): SyncConnection {
  return {
    execute(sql, params) {
      return driver.prepare(sql).all(...params) as Rows;
    },
    executeSafeIntegers(sql, params) {
      const stmt = driver.prepare(sql) as RawStmt;
      // better-sqlite3: run in exact-integer mode so int8/BIGINT arrives as a bigint (the #59 hole:
      // a JS number would round it). A driver without the toggle degrades to a plain `.all` (its
      // rows come back in whatever exact form it natively uses — pg/mysql string BIGINT).
      if (typeof stmt.safeIntegers === 'function') stmt.safeIntegers(true);
      return stmt.all(...params) as Rows;
    },
    run(sql, params) {
      return driver.prepare(sql).run(...params);
    },
  };
}

/**
 * A thin, single-DB, middleware-free {@link ExecutionContext} over ONE connection. A tx-scoped ctx
 * (`withConnection(conn, true)`) pins that connection for every `connectionFor` — this is the
 * per-execution connection ownership (§3). Absent a pinned tx connection, it returns the base
 * connection (the single-DB Phase A case; reader/writer/named-DB routing is B/C/D on this seam).
 */
class BasicContext implements ExecutionContext {
  readonly middleware: MiddlewareChain;
  private readonly base: SyncConnection;
  /** The pinned tx connection (present ⇒ this is a tx-scoped ctx; every statement uses it). */
  private readonly pinned: SyncConnection | null;

  constructor(base: SyncConnection, middleware: MiddlewareChain, pinned: SyncConnection | null) {
    this.base = base;
    this.middleware = middleware;
    this.pinned = pinned;
  }

  connectionFor(_intent: StatementIntent): SyncConnection {
    // Phase A resolution: the tx-owned (pinned) connection wins; else the single base connection.
    // Reader/writer split (§3-2/3) + named-DB routing (§3-4) extend HERE in B/C/D.
    return this.pinned ?? this.base;
  }

  withConnection(conn: SyncConnection, tx: boolean): ExecutionContext {
    return new BasicContext(this.base, this.middleware, tx ? conn : null);
  }
}

/**
 * **Backward-compat wrapper (§6).** Wrap a raw better-sqlite3 `Database` in a thin
 * {@link ExecutionContext}: reader = writer = the same driver, a single DB. Its middleware chain
 * resolves the ambient/global SQL-level {@link import('./middleware').MiddlewareRegistry} at wrap
 * time (Phase D), so a middleware registered via `DBModel.use(...)` intercepts the SQL — and, when
 * NO middleware is registered, the chain is empty and the seam is a pure passthrough to
 * `driver.prepare(...).all()/run()`, i.e. **byte-identical** (the conformance/livedb runners
 * register none, so they are unchanged).
 */
export function contextForDriver(driver: SqliteDriver): ExecutionContext {
  return new BasicContext(connectionForDriver(driver), new MiddlewareChain(activeSqlMiddlewares), null);
}

/**
 * Build a sync ctx directly over a {@link SyncConnection} (for a caller that already owns a
 * connection adapter — e.g. a non-better-sqlite3 sync backend). Same single-DB / ambient-middleware
 * shape as {@link contextForDriver}.
 */
export function contextForConnection(conn: SyncConnection): ExecutionContext {
  return new BasicContext(conn, new MiddlewareChain(activeSqlMiddlewares), null);
}

// ── Async ctx (live PG / MySQL) — per-execution connection ownership (§3) ──────

/**
 * A pool that hands out an OWNED connection per acquire and takes it back on release. This is the
 * substrate for per-execution connection ownership: a transaction `acquire()`s ONE connection,
 * runs its whole body on it, then `release()`s it. Concurrent transactions acquire DISTINCT
 * connections ⇒ isolation. (`pg.Pool.connect()` / `mysql2 pool.getConnection()` fit this shape.)
 */
export interface AsyncConnectionPool {
  /** Check out one owned connection for the caller's exclusive use. */
  acquire(): Promise<AsyncConnection>;
  /** Return a connection to the pool. `destroy` ⇒ drop it (a poisoned/aborted connection). */
  release(conn: AsyncConnection, destroy?: boolean): Promise<void>;
}

/** Per-async-execution ambient ctx: the ALS slot carrying the tx-scoped connection (§3). */
const asyncCtxStore = new AsyncLocalStorage<AsyncConnection>();

/**
 * A pooled async {@link AsyncExecutionContext}. Outside a transaction, `connectionFor` acquires a
 * fresh pooled connection per statement (the existing read fan-out model — each concurrent sibling
 * on its own connection). Inside a transaction, {@link withTransactionAsync} pins ONE acquired
 * connection into the ALS store; `connectionFor` returns THAT for every statement in the body, so
 * the whole tx runs on one owned connection — isolated from concurrent transactions.
 *
 * NB: outside a tx, `connectionFor` returns a **per-statement** owned connection wrapper that
 * acquires-runs-releases; the read walker issues one statement per `executeAsync`, matching the
 * existing `SqlExecutorAsync` "acquire per exec" semantics byte-for-byte.
 */
export class PooledAsyncContext implements AsyncExecutionContext {
  readonly middleware: MiddlewareChain;
  /**
   * The full routing config (§3 steps 2-4): the multi-DB {@link ConnectionRegistry} + the
   * {@link WriterStickyClock}. A ctx built from a single `pool` synthesizes a default-only registry
   * (reader === writer) + an always-false sticky clock, so its resolution is byte-identical to the
   * Phase A/B single-pool path. A ctx built with a {@link RoutingConfig} (via {@link setConfig})
   * gets reader/writer separation, writer-sticky, and named-DB routing.
   */
  private readonly routing: RoutingConfig;

  /**
   * Construct from EITHER a single {@link AsyncConnectionPool} (backward-compat: default connection,
   * reader === writer, sticky disabled — byte-identical to Phase A/B) OR a full {@link RoutingConfig}
   * (Phase C: reader/writer separation + writer-sticky + named-DB routing).
   */
  constructor(poolOrRouting: AsyncConnectionPool | RoutingConfig, middleware: MiddlewareChain = new MiddlewareChain(activeSqlMiddlewares)) {
    this.middleware = middleware;
    this.routing = isRoutingConfig(poolOrRouting)
      ? poolOrRouting
      : // Single-pool backward-compat: default-only registry (reader === writer) + a sticky clock
        // that is NEVER marked (so `isSticky()` stays false) ⇒ every intent lands on the one pool.
        { registry: ConnectionRegistry.singleDefault(poolOrRouting), sticky: new WriterStickyClock({ useWriterAfterTransaction: false }) };
  }

  connectionFor(intent: StatementIntent): AsyncConnection {
    // STEP 1 (§3): inside a tx, the ALS-pinned owned connection wins — every statement on the SAME
    // conn (the per-execution ownership the concurrent-tx fix depends on). The tx pin is a per-scope
    // fact only the ctx's ALS holds, so it is resolved HERE, before the pool-selection steps.
    const pinned = asyncCtxStore.getStore();
    if (pinned !== undefined) return pinned;
    // STEPS 2-4 (§3): named-DB → reader/writer split → writer-sticky/withWriter. `resolvePool`
    // returns WHICH pool serves this intent; the returned wrapper acquires/runs/releases one owned
    // connection per statement (the read fan-out: each concurrent sibling on its own connection).
    const pool = resolvePool(intent, this.routing);
    return {
      async execute(sql, params) {
        const c = await pool.acquire();
        try {
          return await c.execute(sql, params);
        } finally {
          await pool.release(c);
        }
      },
      async run(sql, params) {
        const c = await pool.acquire();
        try {
          return await c.run(sql, params);
        } finally {
          await pool.release(c);
        }
      },
    };
  }

  withConnection(_conn: AsyncConnection, _tx: boolean): AsyncExecutionContext {
    // Deriving a tx-scoped ctx pins the connection via the ALS run in withTransactionAsync (not by
    // mutating this object); the derived ctx shares the routing + middleware and is never used to
    // acquire (the ALS-pinned connection wins in `connectionFor`).
    return this;
  }

  /**
   * The WRITER pool of the connection a transaction runs against (so `withTransactionAsync` acquires
   * the tx's owned connection from the right pool). A transaction is a write ⇒ the writer pool of the
   * named connection (`intent.db`). Absent `db` ⇒ the default connection's writer.
   */
  connectionPoolFor(intent: StatementIntent = { write: true }): AsyncConnectionPool {
    return this.routing.registry.pairFor(intent.db).writer;
  }

  /** The default connection's writer pool (backward-compat accessor for the single-DB tx path). */
  get connectionPool(): AsyncConnectionPool {
    return this.routing.registry.pairFor(undefined).writer;
  }

  /** The writer-sticky clock (the tx runtime `.mark()`s it on a successful write/commit). */
  get stickyClock(): WriterStickyClock {
    return this.routing.sticky;
  }
}

/** Narrow the {@link PooledAsyncContext} ctor arg: is it a {@link RoutingConfig} (vs. a bare pool)? */
function isRoutingConfig(x: AsyncConnectionPool | RoutingConfig): x is RoutingConfig {
  return typeof x === 'object' && x !== null && 'registry' in x && 'sticky' in x;
}

/**
 * Run `fn` inside the ALS scope with `conn` pinned as the ambient tx connection (§3). Every
 * `connectionFor` inside `fn` (async boundaries included) returns `conn`. This is the TS
 * per-execution ownership mechanism (v1 `DBModel.ts` `txContext.run`); the native ports use
 * task-local (rust) / `context.Context` (go) / contextvars (py) / an explicit arg (php) for the
 * SAME effect.
 */
export function runWithPinnedAsyncConnection<R>(conn: AsyncConnection, fn: () => Promise<R>): Promise<R> {
  return asyncCtxStore.run(conn, fn);
}

/**
 * The tx-owned connection pinned in the CURRENT async scope, or `undefined` if not inside a
 * transaction. {@link withTransactionAsync} reads this to detect a NESTED transaction (an inner
 * `withTransactionAsync` running inside an outer's ALS scope joins the outer instead of opening a
 * new physical transaction). The native ports read their task-local / contextvar / `context.Context`
 * for the SAME nested detection.
 */
export function currentPinnedAsyncConnection(): AsyncConnection | undefined {
  return asyncCtxStore.getStore();
}

// ── The async per-execution-ownership transaction (§3) — the concurrent-tx fix ─

/**
 * Transaction options for {@link withTransactionAsync}. The full Phase B contract (isolation / retry
 * / rollbackOnly) lives in {@link import('./tx-options').TransactionOptions}; this re-exports it so
 * exec-context stays the single tx entry while the option shape + defaults + isolation-SQL mapping +
 * retryable-error classification are defined ONCE in `tx-options.ts` (the file the 4 native ports
 * mirror). `TxOptions` is the historical alias kept for callers; it IS `TransactionOptions`.
 */
export type TxOptions = TransactionOptions;

/**
 * Is `err` a broken/stale connection (retryable via reconnect)? Defaulted here to a message/-code
 * heuristic matching `src/connection-errors.ts` so exec-context needs no cross-module import; the
 * runtime passes the shared {@link import('../connection-errors').isConnectionError} in explicitly.
 */
function defaultIsConnectionError(err: Error): boolean {
  const message = err.message || '';
  const code = (err as NodeJS.ErrnoException).code || '';
  return (
    message.includes('Connection terminated') ||
    message.includes('Client has encountered a connection error') ||
    message.includes('ECONNRESET') ||
    message.includes('ECONNREFUSED') ||
    message.includes('Connection lost') ||
    message.includes('This socket has been ended by the other party') ||
    code === 'ECONNRESET' ||
    code === 'ECONNREFUSED' ||
    code === 'EPIPE' ||
    code === 'PROTOCOL_CONNECTION_LOST'
  );
}

/**
 * Run a transaction with **per-execution connection ownership** (§3, the concurrent-tx fix) plus the
 * full Phase B tx-completeness contract (#81): isolation level, nested-tx join, rollbackOnly, and
 * whole-tx retry with per-attempt connection acquisition.
 *
 * **Per attempt** (up to `retryLimit`):
 *   1. acquire ONE fresh connection from the pool (a retry after a connection error thus RECONNECTS);
 *   2. pin it into the ALS ctx (`runWithPinnedAsyncConnection`) so EVERY statement `fn` issues
 *      resolves THAT connection — never a fresh pooled one;
 *   3. issue the isolation-aware BEGIN ({@link beginStatements}: PG `BEGIN ISOLATION LEVEL …`;
 *      MySQL a preceding `SET TRANSACTION ISOLATION LEVEL …` then `BEGIN`);
 *   4. run `fn(txCtx)` → `COMMIT` (or `ROLLBACK` if `rollbackOnly`, still returning the body result);
 *   5. on a body/commit error `ROLLBACK` and either RETRY (retryable error + attempts remain, with
 *      exponential backoff `retryDuration·2^(k-1)`) or re-raise;
 *   6. release the connection back to the pool (DESTROYED if the ROLLBACK itself failed, or the whole
 *      tx errored non-cleanly — a poisoned connection must not re-enter the pool).
 *
 * **Nested**: if a tx-owned connection is ALREADY pinned in the ALS (an outer `withTransactionAsync`
 * on this async chain), the inner call JOINS the outer — it runs `fn` directly on the outer's ctx
 * with NO new BEGIN/COMMIT/acquire, so the whole nested body is one physical transaction (an inner
 * error propagates and rolls back the WHOLE tx). Mirrors v1 `DBModel.transaction` :2794-2797.
 *
 * Concurrent (non-nested) `withTransactionAsync` calls each acquire a DISTINCT connection and pin it
 * in their OWN ALS scope, so their writes never cross-talk — the isolation the shared-slot model
 * violates. This is the reference the native ports (#82-85) mirror.
 */
export async function withTransactionAsync<R>(
  ctx: PooledAsyncContext,
  fn: (txCtx: AsyncExecutionContext) => Promise<R>,
  opts: TransactionOptions = {},
  dialect: Dialect = 'postgres',
  isConnectionError: (e: Error) => boolean = defaultIsConnectionError,
  connection?: string,
): Promise<R> {
  // NESTED-TX JOIN (§ mirror v1 :2794): already inside a tx on this async chain ⇒ join the outer.
  // No new connection, no BEGIN/COMMIT — the inner body is part of the outer physical transaction.
  const outerConn = currentPinnedAsyncConnection();
  if (outerConn !== undefined) {
    // Reuse the outer's ctx (the pinned conn already wins in `connectionFor`). Isolation/retry/
    // rollbackOnly options on a NESTED call are ignored — the outer transaction owns them.
    return fn(ctx.withConnection(outerConn, true));
  }

  const resolved = resolveTxOptions(opts);
  const begins = beginStatements(dialect, resolved.isolation);
  // Named-DB routing (§3-4): a transaction is a write ⇒ acquire from the WRITER pool of the target
  // connection (`connection` name, or the default). Single-DB ⇒ the one default writer pool.
  const pool = ctx.connectionPoolFor({ write: true, ...(connection !== undefined ? { db: connection } : {}) });

  let attempt = 0;
  for (;;) {
    attempt++;
    const conn = await pool.acquire();
    const txCtx = ctx.withConnection(conn, true);

    // One attempt: BEGIN…body…COMMIT/ROLLBACK on the pinned conn. Returns `{ ok, value }` on success
    // or `{ ok:false, error }` on failure — so the release + retry decision happens OUTSIDE the ALS
    // run and the connection is released EXACTLY once per attempt.
    let poisoned = false;
    const attemptResult = await runWithPinnedAsyncConnection(conn, async (): Promise<{ ok: true; value: R } | { ok: false; error: unknown }> => {
      for (const begin of begins) await runAsync(txCtx, begin, []);
      try {
        // Mark the body as "inside a transaction" so a nested write's guard (`checkWriteAllowed`)
        // and nested-tx detection see an active tx. The connection pin (above) and this marker share
        // the SAME async scope.
        const r = await runInTransactionScope(() => fn(txCtx));
        // rollbackOnly (dry-run): ROLLBACK but still return the body result — no committed change.
        await runAsync(txCtx, resolved.rollbackOnly ? 'ROLLBACK' : 'COMMIT', []);
        return { ok: true, value: r };
      } catch (error) {
        try {
          await runAsync(txCtx, 'ROLLBACK', []);
        } catch {
          poisoned = true; // ROLLBACK failed ⇒ the connection is in an unknown state; drop it.
        }
        return { ok: false, error };
      }
    });

    // An errored attempt saw a failed statement — destroy the (possibly poisoned) conn so a retry
    // reconnects on a fresh one; a clean success returns the conn to the pool.
    await pool.release(conn, poisoned || !attemptResult.ok);

    if (attemptResult.ok) {
      // WRITER-STICKY (§3-2, read-your-writes): a committed tx marks the sticky clock so subsequent
      // reads within `writerStickyDuration` route to the writer pool (v1 `_lastTransactionTime`). A
      // rollbackOnly (dry-run) tx committed NOTHING ⇒ it does NOT arm stickiness.
      if (!resolved.rollbackOnly) ctx.stickyClock.mark();
      return attemptResult.value;
    }

    const { error } = attemptResult;
    if (resolved.retryOnError && attempt < resolved.retryLimit && isRetryableTxError(error, isConnectionError)) {
      await sleep(resolved.retryDuration * Math.pow(2, attempt - 1));
      continue; // RETRY the whole transaction on a fresh connection
    }
    throw error;
  }
}

// ── The PUBLIC user-controlled transaction boundary (§ Phase B-core / #86) ─────

/**
 * **The public user-controlled transaction boundary** (#86) — the REAL transaction feature v2 was
 * missing. `transaction(ctx, fn, options?)` opens ONE boundary the caller wraps around MULTIPLE
 * arbitrary operations so they commit or roll back TOGETHER:
 *
 * ```ts
 * await transaction(ctx, async () => {
 *   await A.create(...);   // ← every op inside joins this ONE boundary:
 *   await B.update(...);   //    one connection, one BEGIN…COMMIT, all-or-nothing.
 * }, { isolation: 'serializable' });
 * ```
 *
 * ## What it does (v1 `DBModel.transaction` :2787 parity, on the SCP seam)
 *
 * It acquires ONE pooled connection, issues the isolation-aware `BEGIN`
 * ({@link import('./tx-options').beginStatements}), PINS that connection into the ALS ctx, runs
 * `fn`, then `COMMIT` (or `ROLLBACK` on a body error / `options.rollbackOnly`), with the #81 retry
 * loop (deadlock / serialization / connection error) wrapped around the WHOLE boundary. It is a thin
 * façade over {@link withTransactionAsync}: the ONE mechanism — the same acquire → pin → BEGIN →
 * body → COMMIT/ROLLBACK → release + retry — powers both.
 *
 * ## The ambient-tx JOIN — how operations participate (the core #86 fix)
 *
 * `fn` takes NO connection argument. Instead the pinned connection lives in the ALS
 * ({@link currentPinnedAsyncConnection}) + the "inside a transaction" marker
 * ({@link import('./tx-options').runInTransactionScope}). Every operation `fn` issues — a live-DB
 * write via {@link import('./makesql/tx').executeTransactionAsync}, a read via the async read seam —
 * detects that ambient pinned connection and runs its statements on THAT connection **without opening
 * its own BEGIN/COMMIT** (`withTransactionAsync`'s nested-join, :495). So N operations inside one
 * `transaction(fn)` produce exactly ONE BEGIN + ONE COMMIT on ONE connection. Outside a
 * `transaction(fn)` the ambient pin is absent, so a bare write's own `executeTransactionAsync` sees
 * no ambient tx and the write=tx guard fires ({@link WriteOutsideTransactionError}).
 *
 * NESTED `transaction()` joins the outer (one physical BEGIN/COMMIT; an inner error rolls back the
 * WHOLE tx) — again via `withTransactionAsync`'s nested-join. Isolation/retry/rollbackOnly options on
 * a nested call are ignored (the outer owns them). This is the API REFERENCE the native ports
 * (#82-85) mirror.
 */
export function transaction<R>(
  ctx: PooledAsyncContext,
  fn: () => Promise<R>,
  options: TransactionOptions = {},
  dialect: Dialect = 'postgres',
  isConnectionError: (e: Error) => boolean = defaultIsConnectionError,
  connection?: string,
): Promise<R> {
  // The boundary is `withTransactionAsync` with a body that ignores the tx ctx — ambient operations
  // resolve the pinned connection through the ALS, not an explicit arg (v1 `txContext.run(func)`).
  // `connection` (Phase C-2) routes the tx to a NAMED connection's writer pool (default when absent).
  return withTransactionAsync(ctx, () => fn(), options, dialect, isConnectionError, connection);
}
