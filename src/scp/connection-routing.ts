/**
 * litedbmodel v2 SCP — the **connection routing + config contract** (Phase C / #87).
 *
 * This module is the **API REFERENCE** for Phase C — the 4 native ports (rust #88 / go #89 / py #90
 * / php #91) mirror THIS contract exactly (config field names + defaults, the connection-registry
 * shape, `withWriter` semantics, and the `connectionFor(intent)` resolution ORDER). It builds ON the
 * Phase A {@link import('./exec-context').ExecutionContext} seam and the Phase A/B owned-connection
 * transaction runtime; it does NOT re-implement the seam — it supplies the pieces
 * {@link import('./exec-context').PooledAsyncContext.connectionFor} uses to complete its resolution.
 *
 * ## The `connectionFor(intent)` resolution order (design §3, v1 `DBModel.ts:313` parity)
 *
 * A statement's connection is resolved in THIS priority (first match wins):
 *   1. **active tx connection** — inside a transaction, always the tx-owned connection (Phase A).
 *   2. **writer scope / writer-sticky** — inside {@link withWriter}, or within `writerStickyDuration`
 *      after a transaction (read-your-writes), a READ goes to the WRITER pool (Phase C — here).
 *   3. **read=reader / write=writer** — otherwise a read goes to the reader pool, a write to the
 *      writer pool (reader/writer separation; single-pool config ⇒ reader === writer, Phase C).
 *   4. **named-DB routing** — the target pool is selected by `intent.db` (the connection NAME carried
 *      by the bundle/model metadata) against the {@link ConnectionRegistry}; absent ⇒ the DEFAULT
 *      connection. Named-DB selection happens FIRST (it picks WHICH connection's reader/writer split
 *      steps 2-3 then apply to), so the order above is really: pick the named connection, then within
 *      it apply tx-pin / writer-sticky / reader-writer. (Phase C, decorator-free — the routing key is
 *      config + bundle/model metadata; decorator wiring is Phase F.)
 *
 * ## Backward-compat (the hard constraint)
 *
 * Single DB, `reader === writer` (one pool), empty config, unnamed connection ⇒ BYTE-IDENTICAL to the
 * Phase A/B single-pool behavior. A {@link ConnectionRegistry} built from ONE pool routes every
 * intent to that ONE pool, and the writer-sticky clock only ever diverts to a pool that is the SAME
 * object — so nothing observable changes. The existing `new PooledAsyncContext(pool)` /
 * `new PooledAsyncContext(pool, middleware)` constructors keep working unchanged.
 */

import type { AsyncConnectionPool } from './exec-context';

// ── The runtime config (C3) — mirrors v1 DBConfig/DBConfigOptions ──────────────

/**
 * Per-connection database config (C3) — the knobs a pool is built with. Mirrors v1
 * `DBConfig`/`DBHandler` (`src/DBHandler.ts`, `src/types.ts`): connection target + pool sizing +
 * per-statement/keepalive/session knobs. Every field is optional with a documented default; the 4
 * native ports MUST expose the SAME field names + defaults. This is a DATA contract — it describes
 * how to BUILD a pool; the actual `pg.Pool` / `mysql2.createPool` construction lives in the driver
 * adapters (`makesql/pool-executor.ts`), which read these fields.
 */
export interface ConnectionConfig {
  /** Driver dialect for this connection. @default 'postgres' */
  readonly driver?: 'postgres' | 'mysql' | 'sqlite';
  /** DB host (server-based dialects). */
  readonly host?: string;
  /** DB port. */
  readonly port?: number;
  /** DB name (or file path for sqlite). */
  readonly database?: string;
  /** Username. */
  readonly user?: string;
  /** Password. */
  readonly password?: string;
  /**
   * Per-statement timeout in MILLISECONDS. Applied as a session `statement_timeout` (PG) /
   * `max_execution_time` (MySQL) so a runaway query is aborted by the SERVER. `0`/absent ⇒ no
   * statement timeout (the engine default). v1 exposed this as `queryTimeout` in SECONDS on the pg
   * CLIENT (`query_timeout`); the v2 contract is server-side ms so it fires uniformly across drivers.
   * @default 0 (no timeout)
   */
  readonly queryTimeout?: number;
  /** Enable TCP keepalive on pooled connections (recommended for serverless). @default false */
  readonly keepAlive?: boolean;
  /** ms before the first keepalive probe. @default 10000 (when keepAlive) */
  readonly keepAliveInitialDelayMillis?: number;
  /** Minimum pooled connections kept warm. @default 0 */
  readonly minPool?: number;
  /** Maximum pooled connections. @default 10 */
  readonly maxPool?: number;
  /** PG `search_path` set on each pooled connection at checkout (schema routing). */
  readonly searchPath?: string;
  /** MySQL connection charset / PG client_encoding set on each pooled connection. */
  readonly charset?: string;
}

/** The resolved (defaults-applied) config the pool builder consumes — no `undefined` holes on the knobs. */
export interface ResolvedConnectionConfig {
  readonly driver: 'postgres' | 'mysql' | 'sqlite';
  readonly host?: string;
  readonly port?: number;
  readonly database?: string;
  readonly user?: string;
  readonly password?: string;
  readonly queryTimeout: number;
  readonly keepAlive: boolean;
  readonly keepAliveInitialDelayMillis: number;
  readonly minPool: number;
  readonly maxPool: number;
  readonly searchPath?: string;
  readonly charset?: string;
}

/** Apply the C3 defaults (queryTimeout=0, keepAlive=false, minPool=0, maxPool=10). */
export function resolveConnectionConfig(config: ConnectionConfig = {}): ResolvedConnectionConfig {
  return {
    driver: config.driver ?? 'postgres',
    ...(config.host !== undefined ? { host: config.host } : {}),
    ...(config.port !== undefined ? { port: config.port } : {}),
    ...(config.database !== undefined ? { database: config.database } : {}),
    ...(config.user !== undefined ? { user: config.user } : {}),
    ...(config.password !== undefined ? { password: config.password } : {}),
    queryTimeout: config.queryTimeout ?? 0,
    keepAlive: config.keepAlive ?? false,
    keepAliveInitialDelayMillis: config.keepAliveInitialDelayMillis ?? 10000,
    minPool: config.minPool ?? 0,
    maxPool: config.maxPool ?? 10,
    ...(config.searchPath !== undefined ? { searchPath: config.searchPath } : {}),
    ...(config.charset !== undefined ? { charset: config.charset } : {}),
  };
}

/**
 * The SESSION statements a connection must run at checkout to honor a {@link ResolvedConnectionConfig}
 * (issued once per acquired connection, in order). This is the per-dialect mapping the port agents
 * mirror; it is pure (no connection contact) so it is testable in isolation:
 *
 *   - **statement timeout** (`queryTimeout` > 0): PG `SET statement_timeout = <ms>`; MySQL
 *     `SET SESSION max_execution_time = <ms>` (both server-side, ms).
 *   - **searchPath**: PG `SET search_path TO <path>`; MySQL has no schema search path ⇒ ignored.
 *   - **charset**: MySQL `SET NAMES <charset>`; PG `SET client_encoding TO <charset>`.
 *
 * A key with no value emits nothing (⇒ empty array for an all-default config ⇒ the session is
 * untouched, backward-compatible). sqlite has no server session ⇒ empty.
 */
export function sessionStatements(config: ResolvedConnectionConfig): readonly string[] {
  const out: string[] = [];
  const dialect = config.driver;
  if (dialect === 'sqlite') return out;
  if (config.queryTimeout > 0) {
    out.push(
      dialect === 'postgres'
        ? `SET statement_timeout = ${config.queryTimeout}`
        : `SET SESSION max_execution_time = ${config.queryTimeout}`,
    );
  }
  if (config.searchPath !== undefined && dialect === 'postgres') {
    out.push(`SET search_path TO ${config.searchPath}`);
  }
  if (config.charset !== undefined) {
    out.push(dialect === 'mysql' ? `SET NAMES ${config.charset}` : `SET client_encoding TO ${config.charset}`);
  }
  return out;
}

// ── Reader/writer pool pair (C1) ───────────────────────────────────────────────

/**
 * A reader/writer pool PAIR for ONE named connection (C1). `reader` serves read-intent statements;
 * `writer` serves write-intent statements, `withWriter` reads, and writer-sticky reads. When a
 * connection has no separate replica, `reader === writer` is the SAME pool object — reader/writer
 * routing then always lands on that one pool (the single-pool backward-compat case).
 */
export interface ReaderWriterPools {
  readonly reader: AsyncConnectionPool;
  readonly writer: AsyncConnectionPool;
}

/** Build a {@link ReaderWriterPools} where reader === writer (single-pool, backward-compat). */
export function singlePoolPair(pool: AsyncConnectionPool): ReaderWriterPools {
  return { reader: pool, writer: pool };
}

/** Build a {@link ReaderWriterPools} from a distinct reader + writer pool (reader/writer separation). */
export function readerWriterPair(reader: AsyncConnectionPool, writer: AsyncConnectionPool): ReaderWriterPools {
  return { reader, writer };
}

// ── The connection registry (C2) — name → reader/writer pools ──────────────────

/** The reserved name of the DEFAULT (unnamed) connection. An `intent.db` of `undefined` uses this. */
export const DEFAULT_CONNECTION = 'default';

/**
 * The multi-DB connection registry (C2): a map from a connection NAME → its {@link ReaderWriterPools}.
 * `connectionFor(intent)` selects the pair by `intent.db` (the connection name the bundle/model
 * metadata carries — decorator-free, from config + metadata; decorator wiring is Phase F), falling
 * back to {@link DEFAULT_CONNECTION} when unnamed. Selecting a name that was never registered is a
 * LOUD error (a real wiring bug — never a silent default fallback, which would run a query on the
 * wrong DB; mirrors the V0 cross-DB relation registry's loud-fail policy).
 *
 * A single-DB deployment registers exactly one connection under {@link DEFAULT_CONNECTION} with
 * `reader === writer` ⇒ every intent routes to that one pool ⇒ byte-identical to Phase A/B.
 */
export class ConnectionRegistry {
  /** name → reader/writer pools. */
  private readonly connections: Map<string, ReaderWriterPools>;

  constructor(connections: ReadonlyMap<string, ReaderWriterPools>) {
    this.connections = new Map(connections);
  }

  /**
   * Build a registry from ONE pool as the default connection (reader === writer). This is the
   * backward-compat path: {@link import('./exec-context').PooledAsyncContext} built from a single
   * pool wraps it here so its `connectionFor` routes every intent to that one pool.
   */
  static singleDefault(pool: AsyncConnectionPool): ConnectionRegistry {
    return new ConnectionRegistry(new Map([[DEFAULT_CONNECTION, singlePoolPair(pool)]]));
  }

  /** Fluent builder: start from a default connection's pools, then `.add(name, pools)` more. */
  static fromDefault(pools: ReaderWriterPools): ConnectionRegistryBuilder {
    return new ConnectionRegistryBuilder().add(DEFAULT_CONNECTION, pools);
  }

  /** The reader/writer pair for `name` (or {@link DEFAULT_CONNECTION} when `undefined`). Loud on a missing name. */
  pairFor(name: string | undefined): ReaderWriterPools {
    const key = name ?? DEFAULT_CONNECTION;
    const pair = this.connections.get(key);
    if (pair === undefined) {
      const known = [...this.connections.keys()].map((k) => `'${k}'`).join(', ');
      throw new Error(
        `scp connection routing: no connection registered under name '${key}' ` +
          `(known: ${known || '<none>'}). Register it via setConfig/ConnectionRegistry, or drop the ` +
          `connection tag on the bundle/model.`,
      );
    }
    return pair;
  }

  /** The registered connection names (for diagnostics / `closeAllPools`). */
  names(): readonly string[] {
    return [...this.connections.keys()];
  }

  /** Every DISTINCT pool object across all connections (a shared reader===writer counts once). */
  distinctPools(): readonly AsyncConnectionPool[] {
    const seen = new Set<AsyncConnectionPool>();
    for (const pair of this.connections.values()) {
      seen.add(pair.reader);
      seen.add(pair.writer);
    }
    return [...seen];
  }
}

/** Incremental {@link ConnectionRegistry} builder (name → pools). */
export class ConnectionRegistryBuilder {
  private readonly connections = new Map<string, ReaderWriterPools>();

  /** Register `name` → its reader/writer pools (chainable). Re-adding a name overwrites it. */
  add(name: string, pools: ReaderWriterPools): this {
    this.connections.set(name, pools);
    return this;
  }

  /** Finalize into an immutable {@link ConnectionRegistry}. */
  build(): ConnectionRegistry {
    if (this.connections.size === 0) {
      throw new Error('scp connection routing: ConnectionRegistry must have at least the default connection');
    }
    return new ConnectionRegistry(this.connections);
  }
}

// ── Writer-sticky + withWriter (C1) ────────────────────────────────────────────

import { AsyncLocalStorage } from 'node:async_hooks';

/** Ambient "route reads to the writer" marker (mirror v1 `withWriter` writer context). */
const writerScopeStore = new AsyncLocalStorage<true>();

/** True if the current async scope is inside a {@link withWriter} scope. */
export function inWriterScope(): boolean {
  return writerScopeStore.getStore() === true;
}

/**
 * Run `fn` with reads pinned to the WRITER pool (mirror v1 `DBModel.withWriter`): every read `fn`
 * issues resolves the writer pool (read-your-writes without replication lag), and — because this ALSO
 * enters a {@link import('./tx-options').withReadOnly} scope — ANY write inside `fn` throws
 * {@link import('../types').WriteInReadOnlyContextError}. Nested `withWriter` is idempotent (already
 * in a writer scope ⇒ just run `fn`). Inside a transaction the tx-owned connection already wins in
 * `connectionFor`, so a `withWriter` there is a no-op on routing (matches v1 :2941).
 *
 * @param fn the read-your-writes scope body.
 */
export function withWriter<R>(fn: () => Promise<R>): Promise<R> {
  if (inWriterScope()) return fn();
  // Enter BOTH the writer-routing marker and the read-only (write-reject) marker — v1's single
  // writerContext is both. `withReadOnly` (tx-options) owns the write-reject half.
  return writerScopeStore.run(true, () => withReadOnlyAsync(fn));
}

/**
 * A writer-sticky CLOCK (C1, read-your-writes; v1 `_shouldUseWriterSticky` :344 + `_lastTransactionTime`).
 * After a transaction (or a bare write) COMMITs, reads within `stickyDurationMs` route to the WRITER
 * pool so a just-committed row is visible despite reader-replica lag. The ctx owns ONE clock instance;
 * the tx runtime `.mark()`s it on every successful write/commit; `connectionFor` reads `.isSticky()`.
 *
 * `useWriterAfterTransaction=false` disables it entirely (`.isSticky()` always false). A single-pool
 * deployment (reader === writer) is unaffected by stickiness — the diverted pool is the same object.
 */
export class WriterStickyClock {
  private lastWriteAt = 0;
  private readonly enabled: boolean;
  private readonly stickyDurationMs: number;
  /** Injectable clock (tests advance it); defaults to `Date.now`. */
  private readonly now: () => number;

  constructor(opts: { useWriterAfterTransaction?: boolean; writerStickyDuration?: number; now?: () => number } = {}) {
    this.enabled = opts.useWriterAfterTransaction ?? true;
    this.stickyDurationMs = opts.writerStickyDuration ?? 5000;
    this.now = opts.now ?? Date.now;
  }

  /** Record that a write/commit just happened (the tx runtime calls this on success). */
  mark(): void {
    if (this.enabled) this.lastWriteAt = this.now();
  }

  /** Is a read currently sticky-to-writer (within `writerStickyDuration` of the last write)? */
  isSticky(): boolean {
    if (!this.enabled || this.lastWriteAt === 0) return false;
    return this.now() - this.lastWriteAt < this.stickyDurationMs;
  }

  /** Reset the clock (e.g. between tests / on `closeAllPools`). */
  reset(): void {
    this.lastWriteAt = 0;
  }
}

// ── The routing options a PooledAsyncContext carries (C1+C2+C3) ────────────────

/**
 * The routing configuration a {@link import('./exec-context').PooledAsyncContext} carries to complete
 * its `connectionFor(intent)` resolution (steps 2-4): the multi-DB {@link ConnectionRegistry}, the
 * {@link WriterStickyClock}, and the resolved per-connection {@link ResolvedConnectionConfig} (for the
 * session statements the pool applies). Absent ⇒ the ctx falls back to its single default pool with
 * an always-false sticky clock — the byte-identical Phase A/B path.
 */
export interface RoutingConfig {
  readonly registry: ConnectionRegistry;
  readonly sticky: WriterStickyClock;
}

// ── The core resolution (steps 2-4) — the ONE routing function the ports mirror ─

/**
 * Resolve WHICH pool serves a statement given its {@link import('./exec-context').StatementIntent}
 * and the routing config — the completion of `connectionFor`'s steps 2-4 (step 1, the tx-pin, is
 * handled by the ctx BEFORE calling this, since only the ctx holds the ALS pin). The order:
 *
 *   1. **named-DB** (`intent.db`) selects the {@link ReaderWriterPools} pair (loud on unknown name).
 *   2. within that pair: a WRITE ⇒ the writer pool.
 *   3. a READ in a {@link withWriter} scope OR within writer-sticky ⇒ the writer pool (read-your-writes).
 *   4. otherwise a READ ⇒ the reader pool.
 *
 * Single-pool (reader === writer) ⇒ every branch returns the same pool (backward-compat).
 */
export function resolvePool(intent: { readonly write: boolean; readonly db?: string }, routing: RoutingConfig): AsyncConnectionPool {
  const pair = routing.registry.pairFor(intent.db);
  if (intent.write) return pair.writer; // writes always to the writer
  if (inWriterScope() || routing.sticky.isSticky()) return pair.writer; // read-your-writes
  return pair.reader; // plain read → reader
}

// ── Session-config pool wrapper (C3) — apply queryTimeout/searchPath/charset ───

/**
 * The RESET statements that undo {@link sessionStatements} on release (per dialect), so a session
 * knob (`statement_timeout` / `search_path` / `client_encoding` / `max_execution_time` / charset)
 * set for THIS configured connection does NOT leak to the next caller that draws the SAME underlying
 * pooled connection — pg/mysql2 do NOT auto-reset session state on release. `RESET`/`SET … DEFAULT`
 * restores the server default. Only the knobs `config` actually set are reset (an all-default config
 * ⇒ nothing to reset).
 */
export function sessionResetStatements(config: ResolvedConnectionConfig): readonly string[] {
  const out: string[] = [];
  const dialect = config.driver;
  if (dialect === 'sqlite') return out;
  if (config.queryTimeout > 0) {
    out.push(dialect === 'postgres' ? 'RESET statement_timeout' : 'SET SESSION max_execution_time = DEFAULT');
  }
  if (config.searchPath !== undefined && dialect === 'postgres') out.push('RESET search_path');
  if (config.charset !== undefined) out.push(dialect === 'mysql' ? 'SET NAMES DEFAULT' : 'RESET client_encoding');
  return out;
}

/**
 * Wrap an {@link AsyncConnectionPool} so every acquired connection first runs the
 * {@link sessionStatements} for `config` (statement timeout / search_path / charset) and, on release,
 * runs {@link sessionResetStatements} to restore the server defaults (so a pooled connection never
 * leaks THIS config's session state to the next caller — pg/mysql2 don't auto-reset on release). A
 * config with no session knobs (all defaults) ⇒ ZERO extra statements ⇒ the wrapper is a transparent
 * passthrough (backward-compat). This is where C3's `queryTimeout`/`searchPath`/`charset` become REAL
 * per-server effects — the acquired connection carries them for the statement(s) that run on it.
 *
 * If a session statement itself fails (e.g. an invalid search_path), the connection is released back
 * as DESTROYED so a mis-configured connection never re-enters the pool. On release, if a statement
 * ERRORED (the connection is possibly aborted — e.g. a fired statement timeout), the reset is SKIPPED
 * and the connection is DESTROYED (a poisoned connection must not re-enter the pool anyway).
 */
export function configuredPool(pool: AsyncConnectionPool, config: ResolvedConnectionConfig): AsyncConnectionPool {
  const session = sessionStatements(config);
  if (session.length === 0) return pool; // no knobs set ⇒ transparent passthrough (byte-identical)
  const reset = sessionResetStatements(config);
  return {
    async acquire() {
      const conn = await pool.acquire();
      try {
        for (const stmt of session) await conn.run(stmt, []);
      } catch (e) {
        await pool.release(conn, true); // a failed session setup poisons the connection — drop it
        throw e;
      }
      return conn;
    },
    async release(conn, destroy) {
      // A destroyed (poisoned/aborted) connection is dropped — no point resetting it, and a reset on
      // an aborted-by-timeout connection would itself fail. A CLEAN connection is reset to defaults.
      if (destroy === true) {
        await pool.release(conn, true);
        return;
      }
      try {
        for (const stmt of reset) await conn.run(stmt, []);
      } catch {
        await pool.release(conn, true); // reset failed ⇒ connection state unknown ⇒ drop it
        return;
      }
      await pool.release(conn, false);
    },
  };
}

// ── setConfig / closeAllPools (C3 public surface) ──────────────────────────────

/** A pool CLOSER — closes a pool's underlying connections (pg `pool.end()` / mysql2 `pool.end()`). */
export type PoolCloser = () => Promise<void>;

/**
 * A driver's pool factory: BUILD a pool from a {@link ResolvedConnectionConfig}, returning the
 * {@link AsyncConnectionPool} seam adapter plus a {@link PoolCloser}. This is where the CONSTRUCTION
 * knobs — pool sizing (`minPool`/`maxPool`) + `keepAlive`/`keepAliveInitialDelayMillis` — reach the
 * real driver (`new pg.Pool({ max, keepAlive, … })` / `mysql2.createPool({ connectionLimit, … })`),
 * because those are pool-CONSTRUCTION options a pre-built pool can no longer accept.
 *
 * `buildRoutingConfig` OWNS the call to this factory with the RESOLVED config, so the configured
 * sizing/keepAlive is the SOLE source of the pool's cap — there is no second raw `new Pool({max})`
 * path. `role` lets a factory build a distinct replica pool for the reader vs. the writer (e.g. a
 * different host) while sharing the same sizing config; a factory that returns the SAME pool for both
 * roles collapses to single-pool (reader === writer).
 *
 * The module stays driver-AGNOSTIC: the factory is supplied by the peer-dep-owning caller (this file
 * imports no driver). The pool-executor adapters (`makesql/pool-executor.ts`) provide the concrete
 * pg/mysql2 factories; the port agents (#88-91) supply their language's equivalent — the SAME shape.
 */
export type PoolFactory = (config: ResolvedConnectionConfig, role: 'reader' | 'writer') => { pool: AsyncConnectionPool; close: PoolCloser };

/**
 * One connection's inputs to {@link buildRoutingConfig}: its NAME (default when absent), its
 * {@link ConnectionConfig} (connection params + sizing + keepAlive + session knobs), and a
 * {@link PoolFactory} that `buildRoutingConfig` CALLS with the resolved config to construct the
 * pool(s) — so sizing/keepAlive are applied at construction and the config is the sole cap source.
 *
 * `separateWriter: true` asks the factory for a DISTINCT writer pool (reader/writer replica split);
 * otherwise the factory's reader pool is reused as the writer (single-pool, reader === writer).
 */
export interface ConnectionSetup {
  readonly name?: string; // default connection when absent
  readonly config?: ConnectionConfig;
  /** The driver pool factory (built by `buildRoutingConfig` with the resolved config — sizing/keepAlive land here). */
  readonly poolFactory: PoolFactory;
  /** Build a distinct writer pool via the factory (replica split). Default false ⇒ reader === writer. */
  readonly separateWriter?: boolean;
}

/**
 * The C3 `setConfig` result: the {@link RoutingConfig} a {@link import('./exec-context').PooledAsyncContext}
 * runs on, plus a `close()` that shuts every constructed pool ({@link closeAllPools}). Build it from
 * one or more {@link ConnectionSetup}s (the one named `default`, or the first unnamed, is the default
 * connection).
 *
 * For each setup: resolve the config, CALL its {@link PoolFactory} to construct the pool(s) — so
 * `minPool`/`maxPool`/`keepAlive`/`keepAliveInitialDelayMillis` are applied at `new Pool()` /
 * `createPool()` time (the config is the SOLE source of the cap) — then wrap each pool with
 * {@link configuredPool} so the SESSION knobs (queryTimeout/searchPath/charset) apply on checkout.
 */
export function buildRoutingConfig(
  setups: readonly ConnectionSetup[],
  stickyOpts: { useWriterAfterTransaction?: boolean; writerStickyDuration?: number } = {},
): { routing: RoutingConfig; close: PoolCloser } {
  if (setups.length === 0) throw new Error('scp setConfig: at least one connection setup is required');
  const builder = new ConnectionRegistryBuilder();
  const closers: PoolCloser[] = [];
  for (const s of setups) {
    const resolved = resolveConnectionConfig(s.config);
    // CONSTRUCT the reader pool from the resolved config (sizing/keepAlive land at `new Pool()`).
    const readerBuilt = s.poolFactory(resolved, 'reader');
    closers.push(readerBuilt.close);
    const reader = configuredPool(readerBuilt.pool, resolved);
    let pair: ReaderWriterPools;
    if (s.separateWriter === true) {
      const writerBuilt = s.poolFactory(resolved, 'writer');
      closers.push(writerBuilt.close);
      pair = readerWriterPair(reader, configuredPool(writerBuilt.pool, resolved));
    } else {
      pair = singlePoolPair(reader); // reader === writer (one constructed pool)
    }
    builder.add(s.name ?? DEFAULT_CONNECTION, pair);
  }
  const routing: RoutingConfig = { registry: builder.build(), sticky: new WriterStickyClock(stickyOpts) };
  return { routing, close: () => closeRouting(closers) };
}

/** Close every DISTINCT pool closer (deduped by identity), tolerating individual failures. */
async function closeRouting(closers: readonly PoolCloser[]): Promise<void> {
  const seen = new Set<PoolCloser>();
  for (const c of closers) {
    if (seen.has(c)) continue;
    seen.add(c);
    await c().catch(() => undefined);
  }
}

// ── read-only async scope (bridge to tx-options withReadOnly for the async body) ─

import { withReadOnly } from './tx-options';

/**
 * Run an ASYNC `fn` inside the tx-options read-only scope. `withReadOnly` is generic over its body's
 * return, so an async body's Promise is carried through the ALS run correctly (the marker stays set
 * across the awaits inside `fn` because ALS spans the async chain). Kept here so {@link withWriter}
 * composes the write-reject half without exec-context needing to know about it.
 */
function withReadOnlyAsync<R>(fn: () => Promise<R>): Promise<R> {
  return withReadOnly(fn);
}
