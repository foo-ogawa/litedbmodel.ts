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
 * The ordered middleware chain a ctx carries (§4). `wrap` folds the middlewares around `next`
 * (the connection-resolve + execute terminal). An EMPTY chain is a pure passthrough — `wrap`
 * returns `next(sql, params)` verbatim, so Phase A behavior is byte-identical. The chain is
 * generic over the seam result type `T` so ONE shape serves both the sync (`Rows`/`RunInfo`) and
 * async (`Promise<…>`) seams.
 */
export class MiddlewareChain {
  /** The registered middlewares, outermost first (Phase A: always empty). */
  private readonly stack: readonly Middleware<unknown>[];

  constructor(stack: readonly Middleware<unknown>[] = []) {
    this.stack = stack;
  }

  /** Is the chain empty (⇒ `wrap` is a guaranteed passthrough)? */
  get isEmpty(): boolean {
    return this.stack.length === 0;
  }

  /** Fold the chain around `next`, then invoke it. Empty chain ⇒ `next(sql, params)` verbatim. */
  wrap<T>(sql: string, params: readonly unknown[], next: SeamNext<T>): T {
    if (this.stack.length === 0) return next(sql, params);
    let fn = next as SeamNext<unknown>;
    for (let i = this.stack.length - 1; i >= 0; i--) {
      const mw = this.stack[i];
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
 * {@link ExecutionContext}: reader = writer = the same driver, an EMPTY middleware chain, a single
 * DB. Existing callers (conformance / livedb / bench / unit that pass a raw driver) keep working
 * **byte-identically** — the seam is a pure passthrough to `driver.prepare(...).all()/run()`.
 */
export function contextForDriver(driver: SqliteDriver): ExecutionContext {
  return new BasicContext(connectionForDriver(driver), new MiddlewareChain(), null);
}

/**
 * Build a sync ctx directly over a {@link SyncConnection} (for a caller that already owns a
 * connection adapter — e.g. a non-better-sqlite3 sync backend). Same single-DB / empty-middleware
 * shape as {@link contextForDriver}.
 */
export function contextForConnection(conn: SyncConnection): ExecutionContext {
  return new BasicContext(conn, new MiddlewareChain(), null);
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
  private readonly pool: AsyncConnectionPool;

  constructor(pool: AsyncConnectionPool, middleware: MiddlewareChain = new MiddlewareChain()) {
    this.pool = pool;
    this.middleware = middleware;
  }

  connectionFor(_intent: StatementIntent): AsyncConnection {
    // Inside a tx: the ALS-pinned owned connection (every statement on the SAME conn). Otherwise a
    // per-statement acquire/run/release wrapper (the read fan-out: each concurrent sibling gets its
    // own pooled connection). Phase A ignores reader/writer/db routing (single-DB).
    const pinned = asyncCtxStore.getStore();
    if (pinned !== undefined) return pinned;
    const pool = this.pool;
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
    // mutating this object); the derived ctx shares the pool + middleware and is never used to
    // acquire (the ALS-pinned connection wins in `connectionFor`).
    return this;
  }

  /** The pool (so `withTransactionAsync` can acquire the tx's owned connection). */
  get connectionPool(): AsyncConnectionPool {
    return this.pool;
  }
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

// ── The async per-execution-ownership transaction (§3) — the concurrent-tx fix ─

/** Transaction options (Phase A: none; isolation / retry are #69-B, on this same seam). */
export interface TxOptions {
  /** Optional isolation level SQL fragment (e.g. `SERIALIZABLE`) appended to BEGIN. Phase B. */
  readonly isolation?: string;
}

/**
 * Run a transaction with **per-execution connection ownership** (§3, the concurrent-tx fix):
 *
 *   1. acquire ONE connection from the pool (the tx's exclusive connection);
 *   2. pin it into the ALS ctx (`runWithPinnedAsyncConnection`) so EVERY statement `fn` issues
 *      resolves THAT connection via `ctx.connectionFor` — never a fresh pooled one;
 *   3. `BEGIN` → run `fn(txCtx)` → `COMMIT`; on any throw `ROLLBACK` and re-raise;
 *   4. release the connection back to the pool (destroyed if the ROLLBACK itself failed — a
 *      poisoned connection must not re-enter the pool).
 *
 * Concurrent `withTransactionAsync` calls each acquire a DISTINCT connection and pin it in their
 * OWN ALS scope, so their writes never cross-talk — the isolation the shared-slot model (one
 * `pool.query` per statement, a global `Mutex<Option<writer>>`) violates. This is the reference the
 * native ports (#76-79) mirror.
 */
export async function withTransactionAsync<R>(
  ctx: PooledAsyncContext,
  fn: (txCtx: AsyncExecutionContext) => Promise<R>,
  opts: TxOptions = {},
): Promise<R> {
  const pool = ctx.connectionPool;
  const conn = await pool.acquire();
  const txCtx = ctx.withConnection(conn, true);
  let poisoned = false;
  try {
    return await runWithPinnedAsyncConnection(conn, async () => {
      // BEGIN / COMMIT / ROLLBACK go through the seam too, on the SAME pinned connection.
      await runAsync(txCtx, opts.isolation ? `BEGIN ${opts.isolation}` : 'BEGIN', []);
      try {
        const r = await fn(txCtx);
        await runAsync(txCtx, 'COMMIT', []);
        return r;
      } catch (e) {
        try {
          await runAsync(txCtx, 'ROLLBACK', []);
        } catch {
          poisoned = true; // ROLLBACK failed ⇒ the connection is in an unknown state; drop it.
        }
        throw e;
      }
    });
  } finally {
    await pool.release(conn, poisoned);
  }
}
