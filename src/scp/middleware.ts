/**
 * litedbmodel v2 SCP — the **middleware layer** (Phase D / #92).
 *
 * This module is the **API REFERENCE** for Phase D — the 4 native ports (rust #93 / go #94 / py #95
 * / php #96) mirror THIS contract exactly: the registration surface (`use` / `createMiddleware`), the
 * SQL-level `execute(next, sql, params)` chain contract + APPLIED ORDER, the method-level hook shape
 * + op-kind dispatch, the standard {@link Logger} middleware, and the raw {@link rawExecute} /
 * {@link rawQuery} API that goes THROUGH the exec-context seam (so middleware + connection routing +
 * transaction all still apply). It builds ON the Phase A {@link import('./exec-context').ExecutionContext}
 * seam (the empty {@link import('./exec-context').MiddlewareChain} hook Phase A reserved) — it does
 * NOT restructure the seam; it makes the reserved hook real.
 *
 * ## The two hook levels (design §4)
 *
 *   1. **SQL-level `execute` hook** — `(sql, params, next) => T`. Wraps EVERY statement that funnels
 *      through the central seam ({@link import('./exec-context').execute} / `run` / their async +
 *      safeIntegers twins), so read, write, tx-control (BEGIN/COMMIT/ROLLBACK), and relation-batch
 *      SQL are ALL intercepted. A middleware can observe / rewrite (`next(sql', params')`) / time /
 *      short-circuit (return without calling `next`). This is the seam's `MiddlewareChain` folded
 *      around the connection-resolve terminal.
 *   2. **method-level hook** — at the ORM operation boundary, keyed by the operation KIND
 *      (`find`/`findOne`/`findById`/`count`/`create`/`createMany`/`update`/`updateMany`/`delete`/
 *      `query`). In v2 the "methods" map onto the read (`executeBundle`) / write (`executeTransactionBundle`)
 *      operations tagged with their {@link MethodKind}; {@link runMethod} folds the matching method
 *      hooks around the operation. A method hook runs before/after (around) the whole operation and
 *      can rewrite its args / short-circuit its result.
 *
 * ## Registration + APPLIED ORDER (the 5-language contract — v1 `DBModel.use` parity)
 *
 * `use(mw)` appends to an ordered stack and returns an un-register fn (`DBModel.use` :414). The stack
 * is folded so the FIRST-registered middleware is the OUTERMOST wrapper: given `use(A); use(B)`, a
 * statement runs `A.before → B.before → «execute» → B.after → A.after`. This holds identically for the
 * SQL-level chain ({@link MiddlewareChain.wrap}) and the method-level chain ({@link runMethod}) — the
 * fold walks the stack from LAST to FIRST building `next`, so index 0 ends up outermost. This ORDER is
 * the normative contract the ports reproduce.
 *
 * ## Per-execution-scope isolation (NOT a serializing process global) — design §4 last line
 *
 * A middleware binds to the EXECUTION SCOPE, so concurrent requests/contexts never see each other's
 * middleware or per-request state. TS uses `AsyncLocalStorage` (as v1 does): {@link withMiddlewareScope}
 * runs a callback with an ISOLATED registry copy; every middleware STATE object is likewise ALS-scoped
 * (a fresh per-scope instance via {@link MiddlewareHandle.state}). Two concurrent `withMiddlewareScope`
 * bodies each mutate their OWN registry + state, never a shared global slot — so N concurrent requests
 * do not serialize on one registry (the bug a process-global stack would cause). Absent an explicit
 * scope, `use` mutates a process-global default stack (the app-startup registration path). The native
 * ports translate the ALS: rust = `task_local!`, go = a value on `context.Context`, py = `contextvars`,
 * php = an explicit registry arg (1 request = 1 process ⇒ no concurrency to isolate).
 *
 * NB: native registration (design §4 "native 側でも登録可"): each runtime exposes its own
 * `register_middleware(mw)` appending to its ctx chain; the CHAIN CONTRACT + ORDER above is shared, the
 * middleware BODY is that language's closure/impl. TS is the reference for the shape.
 */

import { AsyncLocalStorage } from 'node:async_hooks';
import type { Middleware as SqlMiddleware } from './exec-context';

// ── The SQL-level middleware (re-export the seam's shape) ──────────────────────

/**
 * The SQL-level `execute` hook (design §4 level 1): `(sql, params, next) => T`. IDENTICAL to the seam's
 * {@link import('./exec-context').Middleware} — re-exported here so the middleware layer is the ONE
 * import surface. `next(sql, params)` runs the rest of the chain then the connection-resolve terminal;
 * a middleware may pass DIFFERENT `sql`/`params` (rewrite), skip `next` (short-circuit), or wrap the
 * `next` call to time it. Generic over `T` so the SAME middleware serves the sync (`Rows`/`RunInfo`)
 * and async (`Promise<…>`) seams.
 *
 * NB the argument order `(sql, params, next)` matches the seam; the v1 `DBModel` class-method form is
 * `execute(next, sql, params)`. {@link createMiddleware} accepts the v1 form and adapts it, so the v1
 * hook body ports over unchanged.
 */
export type SqlHook = SqlMiddleware<unknown>;

// ── Method-level hooks (design §4 level 2) — the ORM operation boundary ────────

/**
 * The ORM operation KIND a method hook keys on (v2 maps the v1 method names onto the read/write
 * operations). A read operation (`executeBundle`) is tagged `find`/`findOne`/`findById`/`count`/
 * `query`; a write operation (`executeTransactionBundle`) is tagged `create`/`createMany`/`update`/
 * `updateMany`/`delete`. {@link runMethod} dispatches to the hook of the matching kind — this is how a
 * method hook DISTINGUISHES the op kind (the tag the operation boundary supplies, NOT a guess from the
 * SQL text).
 */
export type MethodKind =
  | 'find'
  | 'findOne'
  | 'findById'
  | 'count'
  | 'create'
  | 'createMany'
  | 'update'
  | 'updateMany'
  | 'delete'
  | 'query';

/** The `next` of a method hook: run the rest of the method chain + the operation, resolving its result. */
export type MethodNext<R> = (...args: readonly unknown[]) => Promise<R>;

/**
 * One method-level hook: `(model, next, ...args) => Promise<R>` (v1 `Middleware.find` :196 parity).
 * `model` is the operation's model/target descriptor (opaque here — the runtime supplies it; a port
 * passes its own model handle); `...args` are the operation's arguments (conditions/values/options/…);
 * `next(...args)` runs the rest of the chain + the operation. A hook may rewrite `args`, time the
 * `next` call, or short-circuit by returning without calling `next`.
 */
export type MethodHook<R = unknown> = (model: unknown, next: MethodNext<R>, ...args: readonly unknown[]) => Promise<R>;

// ── The middleware descriptor + handle (registration unit) ─────────────────────

/**
 * A registered middleware: its (optional) SQL-level {@link SqlHook}, its per-kind {@link MethodHook}s,
 * and a per-scope STATE factory. `use` registers ONE of these. Built by {@link createMiddleware} from
 * the ergonomic (v1-shaped) config; a hand-built descriptor is also accepted.
 */
export interface MiddlewareDescriptor {
  /** The SQL-level `execute` hook (design §4 level 1), if any. */
  readonly sql?: SqlHook;
  /** The method-level hooks keyed by op kind (design §4 level 2), if any. */
  readonly methods?: Partial<Record<MethodKind, MethodHook>>;
}

/**
 * The registration handle returned by {@link createMiddleware}: `use()`/`unuse()` bind it to the
 * ambient/global registry, and `state()` reads the CURRENT execution scope's state instance (v1
 * `getCurrentContext()` — a fresh per-scope copy of the config's `state`, ALS-isolated). `descriptor`
 * is the underlying {@link MiddlewareDescriptor} (for a direct {@link Registry.use}).
 */
export interface MiddlewareHandle<S extends object = Record<string, never>> {
  /** The underlying descriptor (register via {@link register} / a {@link Registry}). */
  readonly descriptor: MiddlewareDescriptor;
  /** The CURRENT execution scope's state instance (fresh per scope, v1 `getCurrentContext()`). */
  state(): S;
  /** Reset the current scope's state to a fresh copy of the initial state (testing convenience). */
  resetState(): void;
}

// ── The middleware registry (the ordered stack + ambient isolation) ────────────

/**
 * The ordered middleware stack. `use` appends (first-registered = outermost, §order), returning an
 * un-register fn. `sqlHooks()` / `methodHooks(kind)` return the folded-order slices the seam +
 * {@link runMethod} consume. A {@link Registry} is EITHER the process-global default (app-startup
 * registration) OR a per-execution-scope copy pushed by {@link withMiddlewareScope} (concurrent
 * isolation) — the two share this class; only their lifetime differs.
 */
export class Registry {
  private stack: MiddlewareDescriptor[] = [];
  /**
   * Per-scope STATE instances, keyed by the state-owning token a {@link createMiddleware} descriptor
   * carries. Because the Registry is itself per-execution-scope ({@link withMiddlewareScope} pushes a
   * COPY), the state map is scope-local too — so a middleware's per-request state is isolated across
   * concurrent scopes WITHOUT leaking (the `enterWith`-leak trap). A fresh scope's copy starts with an
   * EMPTY state map (states are NOT copied), so each scope lazily builds its own fresh state instances.
   */
  private readonly states = new Map<object, unknown>();

  /** The current scope's state for `token`, lazily created via `fresh` on first access (v1 getCurrentContext). */
  stateFor<S>(token: object, fresh: () => S): S {
    let s = this.states.get(token) as S | undefined;
    if (s === undefined) {
      s = fresh();
      this.states.set(token, s);
    }
    return s;
  }

  /** Reset `token`'s state in this scope to a fresh instance (testing convenience). */
  resetStateFor<S>(token: object, fresh: () => S): void {
    this.states.set(token, fresh());
  }

  /** Register `mw` (appended ⇒ outermost). Returns an idempotent un-register fn. */
  use(mw: MiddlewareDescriptor): () => void {
    this.stack.push(mw);
    return () => {
      const i = this.stack.indexOf(mw);
      if (i !== -1) this.stack.splice(i, 1);
    };
  }

  /** Remove `mw` (v1 `removeMiddleware`). Returns whether it was present. */
  remove(mw: MiddlewareDescriptor): boolean {
    const i = this.stack.indexOf(mw);
    if (i === -1) return false;
    this.stack.splice(i, 1);
    return true;
  }

  /** Drop every registration (v1 `clearMiddlewares` — testing). */
  clear(): void {
    this.stack = [];
  }

  /** The registered descriptors, registration order (index 0 = first = outermost). */
  all(): readonly MiddlewareDescriptor[] {
    return this.stack;
  }

  /** The SQL-level hooks (registration order), for the {@link MiddlewareChain} fold. */
  sqlHooks(): readonly SqlHook[] {
    const out: SqlHook[] = [];
    for (const mw of this.stack) if (mw.sql !== undefined) out.push(mw.sql);
    return out;
  }

  /** The method hooks for `kind` (registration order), for the {@link runMethod} fold. */
  methodHooks(kind: MethodKind): readonly MethodHook[] {
    const out: MethodHook[] = [];
    for (const mw of this.stack) {
      const h = mw.methods?.[kind];
      if (h !== undefined) out.push(h);
    }
    return out;
  }

  /** A shallow COPY (used to seed an isolated per-scope registry that starts from the global set). */
  copy(): Registry {
    const r = new Registry();
    r.stack = [...this.stack];
    return r;
  }
}

/** The process-global default registry (app-startup `use` with no explicit scope). */
const globalRegistry = new Registry();

/** The per-execution-scope registry override (ALS): present ⇒ `use`/reads target THIS scope's copy. */
const registryScope = new AsyncLocalStorage<Registry>();

/** The registry the current execution scope resolves to: the ALS override, else the global default. */
export function currentRegistry(): Registry {
  return registryScope.getStore() ?? globalRegistry;
}

/**
 * Run `fn` with an ISOLATED middleware registry (concurrent-request isolation, design §4). The scope
 * seeds a COPY of the currently-visible registry (so app-wide registrations remain in effect), and any
 * `use` / per-request state inside `fn` mutates ONLY this scope — two concurrent `withMiddlewareScope`
 * bodies never see each other's middleware. `seed` lets a caller start from an EMPTY registry instead
 * (`inherit: false`). The native ports run their task-local / contextvar / `context.Context` /
 * explicit-arg equivalent.
 */
export function withMiddlewareScope<R>(fn: () => R, opts: { inherit?: boolean } = {}): R {
  const seed = opts.inherit === false ? new Registry() : currentRegistry().copy();
  return registryScope.run(seed, fn);
}

/**
 * The LIVE SQL-level middleware stack of the current execution scope — the
 * {@link import('./exec-context').MiddlewareStackSource} the ctx factories give their
 * {@link import('./exec-context').MiddlewareChain}. Resolved at EACH `wrap`, so registration after ctx
 * construction, and per-scope registries, are both honored. Empty ⇒ the seam is a byte-identical
 * passthrough.
 */
export function activeSqlMiddlewares(): readonly SqlMiddleware<unknown>[] {
  return currentRegistry().sqlHooks();
}

// ── Registration surface (v1 `DBModel.use` / `createMiddleware` parity) ────────

/**
 * Register a middleware descriptor on the CURRENT scope's registry (the ambient per-scope one inside
 * {@link withMiddlewareScope}, else the process-global default). Returns an un-register fn (v1
 * `DBModel.use` :414). This is the low-level surface; app code usually registers a
 * {@link createMiddleware} handle via {@link use}.
 */
export function register(mw: MiddlewareDescriptor): () => void {
  return currentRegistry().use(mw);
}

/** Register a {@link createMiddleware} handle (or a raw descriptor) — v1 `DBModel.use`. */
export function use(mw: MiddlewareHandle | MiddlewareDescriptor): () => void {
  const descriptor = 'descriptor' in mw ? mw.descriptor : mw;
  return register(descriptor);
}

/** The v1-shaped hook config {@link createMiddleware} consumes (method hooks + the `execute` SQL hook). */
export interface MiddlewareConfig<S extends object = Record<string, never>> {
  /**
   * Per-scope initial state — a FRESH deep copy is bound to each execution scope (v1 `structuredClone`
   * of `state`). Read via the handle's `state()` (v1 `getCurrentContext()`). Every hook body runs with
   * `this` bound to that per-scope state object.
   */
  readonly state?: S;
  /** SQL-level hook, v1 form `execute(next, sql, params)` (adapted to the seam's `(sql, params, next)`). */
  readonly execute?: (this: S, next: SqlNext, sql: string, params: readonly unknown[]) => unknown;
  readonly find?: MethodHookFn<S>;
  readonly findOne?: MethodHookFn<S>;
  readonly findById?: MethodHookFn<S>;
  readonly count?: MethodHookFn<S>;
  readonly create?: MethodHookFn<S>;
  readonly createMany?: MethodHookFn<S>;
  readonly update?: MethodHookFn<S>;
  readonly updateMany?: MethodHookFn<S>;
  readonly delete?: MethodHookFn<S>;
  readonly query?: MethodHookFn<S>;
}

/** The v1 `execute` hook's `next` (the seam terminal), in v1 argument order. */
export type SqlNext = (sql: string, params?: readonly unknown[]) => unknown;

/** A method hook in the config's v1 form: `(model, next, ...args)` with `this` bound to the state. */
export type MethodHookFn<S> = (this: S, model: unknown, next: MethodNext<unknown>, ...args: readonly unknown[]) => Promise<unknown>;

/** The op-kind method-hook config keys (in registration-independent order). */
const METHOD_KINDS: readonly MethodKind[] = [
  'find', 'findOne', 'findById', 'count', 'create', 'createMany', 'update', 'updateMany', 'delete', 'query',
];

/**
 * Build a {@link MiddlewareHandle} from a v1-shaped {@link MiddlewareConfig} (v1 `createMiddleware`
 * parity). Each hook body's `this` is the CURRENT execution scope's state instance (a fresh
 * `structuredClone` of `config.state` per scope, ALS-isolated). The `execute` hook is adapted from the
 * v1 `(next, sql, params)` order to the seam's `(sql, params, next)` order, so a v1 hook body ports
 * unchanged. Method hooks pass through in the v1 `(model, next, ...args)` shape.
 */
export function createMiddleware<S extends object = Record<string, never>>(
  config: MiddlewareConfig<S>,
): MiddlewareHandle<S> {
  // Per-scope state: keyed on THIS unique token in the CURRENT scope's registry. Because the registry
  // is itself per-execution-scope ({@link withMiddlewareScope} pushes a fresh copy with an empty state
  // map), each scope lazily builds its OWN state instance — isolated across concurrent scopes with no
  // `enterWith` leak (v1's per-subclass AsyncLocalStorage, but scope-tied to the registry). First
  // access in a scope deep-copies the initial state (v1 `structuredClone`).
  const token = {};
  const freshState = (): S => (config.state !== undefined ? (structuredClone(config.state) as S) : ({} as S));
  const state = (): S => currentRegistry().stateFor(token, freshState);

  // SQL-level hook: adapt v1 (next, sql, params) → seam (sql, params, next); bind `this` to the state.
  const sql: SqlHook | undefined = config.execute
    ? (s, p, next) => (config.execute as (this: S, next: SqlNext, sql: string, params: readonly unknown[]) => unknown)
        .call(state(), (ns, np) => next(ns, np ?? []), s, p)
    : undefined;

  const methods: Partial<Record<MethodKind, MethodHook>> = {};
  for (const kind of METHOD_KINDS) {
    const fn = config[kind] as MethodHookFn<S> | undefined;
    if (fn !== undefined) {
      methods[kind] = (model, next, ...args) => fn.call(state(), model, next, ...args);
    }
  }

  const descriptor: MiddlewareDescriptor = {
    ...(sql !== undefined ? { sql } : {}),
    ...(Object.keys(methods).length > 0 ? { methods } : {}),
  };

  return {
    descriptor,
    state,
    resetState: () => currentRegistry().resetStateFor(token, freshState),
  };
}

// ── Method-level dispatch (design §4 level 2) — the operation boundary fold ─────

/**
 * Run an ORM operation of KIND `kind` through the current scope's method hooks, then execute `core`.
 * The hooks fold first-registered-outermost (§order), each getting `(model, next, ...args)`; a hook may
 * rewrite `args`, time `next`, or short-circuit. Empty hooks for this kind ⇒ `core(...args)` verbatim
 * (byte-identical — no method registered = the operation runs untouched). This is the v2 equivalent of
 * v1 `DBModel._applyMiddleware`: the runtime calls it at the read (`executeBundle`) / write
 * (`executeTransactionBundle`) boundary with the op tagged by its {@link MethodKind}.
 *
 * @param kind the operation kind (how a method hook distinguishes read vs. write vs. find/create/…).
 * @param model the operation's model/target descriptor (opaque; passed to each hook).
 * @param core  the actual operation, taking the (possibly hook-rewritten) args.
 * @param args  the operation's arguments.
 */
export function runMethod<R>(
  kind: MethodKind,
  model: unknown,
  core: (...args: readonly unknown[]) => Promise<R>,
  args: readonly unknown[],
): Promise<R> {
  const hooks = currentRegistry().methodHooks(kind);
  if (hooks.length === 0) return core(...args); // fast path: no method hook for this kind
  let next: MethodNext<R> = (...a) => core(...a);
  for (let i = hooks.length - 1; i >= 0; i--) {
    const hook = hooks[i] as MethodHook<R>;
    const inner = next;
    next = (...a) => hook(model, inner, ...a);
  }
  return next(...args);
}

// ── D3: the standard Logger middleware (SQL / params / timing) ─────────────────

/** One logged statement: the SQL, its params, and the wall-clock ms `next` took (v1 Logger parity). */
export interface LogEntry {
  readonly sql: string;
  readonly params: readonly unknown[];
  /** Wall-clock milliseconds the wrapped `next` (chain remainder + connection execute) took. */
  readonly durationMs: number;
}

/** {@link Logger} options: a sink for each entry, and whether to also `console` it. */
export interface LoggerOptions {
  /** Called with each {@link LogEntry} as its statement completes (after `next`). */
  readonly sink?: (entry: LogEntry) => void;
  /** Also emit a one-line `[scp] SQL (Nms) params=…` to `console.log`. @default false */
  readonly console?: boolean;
  /** Injectable clock (tests). @default `Date.now` */
  readonly now?: () => number;
}

/**
 * The standard **Logger middleware** (design §4, v1 `StatisticsMiddleware`/`Logger` parity): a
 * SQL-level {@link SqlHook} that records the SQL, its params, and the wall-clock ms each statement
 * takes. Every statement through the seam — read, write, tx-control, relation-batch — is logged (it is
 * an `execute`-level hook). Register it with {@link use}: `use(Logger({ sink }))`. Timing brackets the
 * `next` call, so it measures the connection execute (chain remainder included), NOT just the log call.
 *
 * The per-scope log history lives on the handle's `state().entries` (v1 `getCurrentContext().getLogs()`),
 * so concurrent requests each collect their OWN entries (ALS-isolated). The port agents build the same
 * shape: a scope-local list + an `execute` hook timing `next`.
 */
export function Logger(options: LoggerOptions = {}): MiddlewareHandle<{ entries: LogEntry[] }> {
  const now = options.now ?? Date.now;
  return createMiddleware<{ entries: LogEntry[] }>({
    state: { entries: [] },
    // Works on BOTH the sync (`Rows`/`RunInfo`) and async (`Promise<…>`) seams: `next` may return a
    // value or a thenable, so timing/recording is done in a completion callback that handles either —
    // NOT an `async`/`await` (which would coerce the sync seam's value into a Promise, breaking the
    // synchronous conformance path). `this` is the per-scope state (isolated log history).
    execute: function (next, sql, params) {
      const entries = this.entries;
      const started = now();
      const record = (): void => {
        const entry: LogEntry = { sql, params, durationMs: now() - started };
        entries.push(entry);
        if (options.sink !== undefined) options.sink(entry);
        if (options.console === true) {
          console.log(`[scp] ${sql} (${entry.durationMs}ms) params=${JSON.stringify(params)}`);
        }
      };
      const result = next(sql, params);
      if (result !== null && typeof result === 'object' && typeof (result as { then?: unknown }).then === 'function') {
        return (result as Promise<unknown>).then(
          (v) => { record(); return v; },
          (e) => { record(); throw e; },
        );
      }
      record();
      return result;
    },
  });
}

// ── D3: raw execute / query THROUGH the seam ───────────────────────────────────
//
// A public raw statement API that goes through the exec-context seam, so a registered SQL-level
// middleware sees it AND connection routing / an ambient transaction still apply (design §4 D3). It is
// a thin front over the seam's `execute`/`run` (sync) or `executeAsync`/`runAsync` (async) — the SAME
// central point every ORM-generated statement uses. `rawQuery` is `rawExecute` tagged as a `query`
// operation kind (so a `query` method hook fires), matching v1 `DBModel.query` → `execute`.

import {
  type ExecutionContext,
  type AsyncExecutionContext,
  type Rows,
  type RunInfo,
  execute as seamExecute,
  run as seamRun,
  executeAsync as seamExecuteAsync,
  runAsync as seamRunAsync,
} from './exec-context';

/** Does `sql` return rows (SELECT / …RETURNING / WITH…SELECT / SHOW / PRAGMA / VALUES)? */
function returnsRows(sql: string): boolean {
  return /^\s*(select|with|show|pragma|values|explain|table)\b/i.test(sql) || /\breturning\b/i.test(sql);
}

/**
 * The raw-statement result: a row list (for a row-returning statement) plus the affected-rows count
 * (mirrors v1 `ExecuteResult { rows, rowCount }`). A non-row statement resolves `rows: []`.
 */
export interface RawResult {
  readonly rows: Rows;
  readonly rowCount: number | null;
}

/**
 * Raw **synchronous** `execute(sql, params)` THROUGH the seam (design §4 D3): a registered SQL-level
 * middleware intercepts it, connection routing resolves the connection, and an ambient transaction (if
 * the ctx is tx-scoped) applies — because it is the SAME `execute`/`run` seam the ORM uses, not a
 * direct driver call. A row-returning statement runs `execute`; a non-returning one runs `run`.
 * `write` forces the write intent (writer routing / tx connection) for a row-returning write.
 */
export function rawExecute(ctx: ExecutionContext, sql: string, params: readonly unknown[] = [], opts: { write?: boolean } = {}): RawResult {
  if (returnsRows(sql)) {
    const rows = seamExecute(ctx, sql, params, { write: opts.write ?? false });
    return { rows, rowCount: rows.length };
  }
  const info: RunInfo = seamRun(ctx, sql, params);
  return { rows: [], rowCount: info.changes };
}

/** Raw **async** `execute(sql, params)` THROUGH the async seam — the live PG / MySQL twin of {@link rawExecute}. */
export async function rawExecuteAsync(ctx: AsyncExecutionContext, sql: string, params: readonly unknown[] = [], opts: { write?: boolean } = {}): Promise<RawResult> {
  if (returnsRows(sql)) {
    const rows = await seamExecuteAsync(ctx, sql, params, { write: opts.write ?? false });
    return { rows, rowCount: rows.length };
  }
  const info = await seamRunAsync(ctx, sql, params);
  return { rows: [], rowCount: info.changes };
}

/**
 * Raw **synchronous** `query(sql, params)` — {@link rawExecute} tagged as a `query` operation, so a
 * `query` method hook fires (then its SQL flows through the same seam + `execute` hooks, exactly as v1
 * `DBModel.query` calls `DBModel.execute`). Returns the row list.
 */
export function rawQuery(ctx: ExecutionContext, sql: string, params: readonly unknown[] = []): Promise<Rows> {
  return runMethod('query', undefined, async () => rawExecute(ctx, sql, params).rows, [sql, params]);
}

/** Raw **async** `query(sql, params)` — the live PG / MySQL twin of {@link rawQuery}. */
export function rawQueryAsync(ctx: AsyncExecutionContext, sql: string, params: readonly unknown[] = []): Promise<Rows> {
  return runMethod('query', undefined, async () => (await rawExecuteAsync(ctx, sql, params)).rows, [sql, params]);
}

// ── Reset / testing helpers ────────────────────────────────────────────────────

/** Clear the process-global registry (testing; a per-scope registry is dropped when its scope exits). */
export function clearMiddlewares(): void {
  globalRegistry.clear();
}
