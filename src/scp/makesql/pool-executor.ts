/**
 * litedbmodel v2 SCP — pooled async `SqlExecutorAsync` factories for PG / MySQL (#40).
 *
 * The read-graph async execution model (`executeReadGraphAsync`) dispatches independent sibling
 * relations in bounded parallel via bc `runBehaviorAsync`. For that concurrency to become REAL
 * parallel DB I/O the driver seam must be an async CONNECTION POOL — each concurrent `exec` call
 * checks out a DISTINCT pooled connection. These factories adapt a `pg.Pool` / `mysql2` pool to the
 * {@link SqlExecutorAsync} seam.
 *
 * The read graph already renders the dialect placeholder form (`$N` for postgres, `?` for
 * MySQL/SQLite, via `renderPlaceholders`) and flattens params 1:1, so each executor is a THIN
 * adapter — it passes the already-rendered `{ sql, params }` straight to the pool. The pool sizing
 * should be aligned with the plan's `concurrency` (default 16) so `concurrency` sibling relations
 * can each hold a live connection without queueing.
 *
 * `pg` / `mysql2` are optional peer dependencies loaded dynamically — importing this module does not
 * force either into a consumer that only uses the sync better-sqlite3 conformance path.
 */

import type { SqlExecutorAsync } from './static-bundle';
import type { AsyncConnection, AsyncConnectionPool, Rows, RunInfo } from '../exec-context';
import type { ResolvedConnectionConfig, PoolFactory, PoolCloser } from '../connection-routing';

// ── TS read-path driver de-box config (issue #59) ─────────────────────────────
// The read-path materializer (`materializeCell`) coerces each driver cell to the JS form its SQL
// column type declares (BIGINT→bigint, DATE→string, …). For that to be LOSSLESS the driver must
// hand the value over in a coercible form:
//   - PG: int8 (OID 20) already arrives as a STRING; DATE/TIMESTAMP/TIMESTAMPTZ arrive as a JS
//     Date by default (TZ-shifted). `configurePgDeboxTypeParsers` registers `pg` type parsers so
//     the date family arrives as its NATIVE TEXTUAL form (carrying TZ for timestamptz) — the
//     value-preserving string the materializer expects. This is a GLOBAL `pg` setting (pg has no
//     per-pool parser), applied once by the consumer that owns the pg import.
//   - MySQL: BIGINT arrives as a rounded JS number by default (LOSSY). The pool MUST be built with
//     `mysqlDeboxPoolOptions` (`supportBigNumbers + bigNumberStrings` so BIGINT→string, and
//     `dateStrings` so the date family→string). `mysqlDeboxPoolOptions` is those options.

/** PG type OIDs for the date/time family + int8 (bigint). */
const PG_OID = { INT8: 20, DATE: 1082, TIMESTAMP: 1114, TIMESTAMPTZ: 1184, TIME: 1083, TIMETZ: 1266 } as const;

/** The minimal `pg.types` surface: register a per-OID text parser. */
export interface PgTypesLike {
  setTypeParser(oid: number, parser: (value: string) => unknown): void;
}

/**
 * Register `pg` type parsers so the read-path de-box (issue #59) gets coercible values: the
 * DATE/TIMESTAMP/TIMESTAMPTZ/TIME family is returned as its NATIVE TEXTUAL string (NOT a JS Date),
 * honoring the `date→string` outType and carrying the TZ for timestamptz. int8 already arrives as a
 * string (pg default), so it needs no parser. Idempotent. Call ONCE, before building the pool, from
 * the consumer that owns the `pg` module (`import { types } from 'pg'; configurePgDeboxTypeParsers(types)`).
 *
 * WHY the GLOBAL parser (a documented, deliberate exception): `pg`'s default DATE parser builds a JS
 * `Date` at LOCAL midnight, which shifts the UTC CALENDAR DAY (e.g. `2026-07-14` → a Date whose UTC
 * day is `2026-07-13`). That loss happens INSIDE the driver, BEFORE the materializer sees the value —
 * so materializer-side coercion CANNOT recover the correct calendar day from the already-shifted
 * Date. Reading the native text (this parser) is the only lossless option, and `pg` exposes type
 * parsers only at module (process-global) scope. It is set ONCE at pool creation, not per read. The
 * materializer's `Date → ISO string` path remains as a defensive fallback for any driver/config that
 * still slips a Date through, but the correct-day guarantee for pg DATE comes from this parser.
 */
export function configurePgDeboxTypeParsers(types: PgTypesLike): void {
  const asString = (v: string): string => v; // identity: keep the driver's native textual form
  types.setTypeParser(PG_OID.DATE, asString);
  types.setTypeParser(PG_OID.TIMESTAMP, asString);
  types.setTypeParser(PG_OID.TIMESTAMPTZ, asString);
  types.setTypeParser(PG_OID.TIME, asString);
  types.setTypeParser(PG_OID.TIMETZ, asString);
}

/**
 * The `mysql2` pool options the read-path de-box (issue #59) requires: `supportBigNumbers` +
 * `bigNumberStrings` so BIGINT arrives as an EXACT string (→ bigint; a plain number would already
 * have rounded it), and `dateStrings` so the DATE/TIMESTAMP/DATETIME family arrives as its native
 * textual string (→ the `date→string` outType, NOT a JS Date). Spread into `mysql.createPool(...)`.
 */
export const mysqlDeboxPoolOptions = {
  supportBigNumbers: true,
  bigNumberStrings: true,
  dateStrings: true,
} as const;

/** The minimal `pg.Pool` surface we need: `query(text, values)` resolving `{ rows }`. */
export interface PgPoolLike {
  query(text: string, values?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
}

/** The minimal `mysql2/promise` pool surface: `query(sql, values)` resolving `[rows, fields]`. */
export interface MysqlPoolLike {
  query(sql: string, values?: unknown[]): Promise<[Record<string, unknown>[], unknown]>;
}


/**
 * Adapt a `mysql2/promise` pool to the async executor seam. `pool.query` acquires + releases a
 * pooled connection per call, so concurrent siblings run in parallel. The graph renders `?`
 * placeholders for the `mysql` dialect — `mysql2`'s native form.
 *
 * Recommend `connectionLimit` ≥ the plan's `concurrency` (default 16).
 */
// ── Production de-box executor factories (issue #59) ──────────────────────────
// The read-path materializer needs the driver to hand over BIGINT as a string and (ideally) DATE
// as a string. These factories build a correctly-configured pool AND return its executor in ONE
// call, so the async production path (`executeBehaviorAsync`) is de-box-correct by construction —
// not reliant on a caller hand-spreading options. `pg` / `mysql2` are passed in (optional peer
// deps — this module imports neither), keeping the runtime driver-agnostic.

/** Minimal `pg` module surface: a `Pool` ctor + the global `types.setTypeParser`. */
export interface PgModuleLike {
  Pool: new (config: Record<string, unknown>) => PgPoolLike & { end?: () => Promise<void> };
  types: PgTypesLike;
}

/** Minimal `mysql2/promise` module surface: `createPool`. */
export interface Mysql2ModuleLike {
  createPool(config: Record<string, unknown>): MysqlPoolLike & { end?: () => Promise<void> };
}




// ── Phase A (#75): OWNED-connection pool adapters — the per-execution-ownership substrate ─────
//
// `pgPoolExecutor` / `mysqlPoolExecutor` above are the READ fan-out seam (acquire-run-release per
// statement). A TRANSACTION instead needs ONE owned connection held across BEGIN…COMMIT — the
// `AsyncConnectionPool` contract (`../exec-context`). These adapters expose a pg / mysql2 pool as
// that owned-connection pool so `withTransactionAsync` pins one connection per tx and concurrent
// transactions never share a connection (the isolation the shared-`pool.query`-per-statement model
// violates). Query results de-box exactly as the read executors above (same pool config).

/** A pg `Pool` that hands out an owned `PoolClient` (`connect()` → `release()`). */
export interface PgOwnedPoolLike {
  connect(): Promise<PgPoolClientLike>;
}
/** The pg `PoolClient`: parameterized `query`, and `release(destroy?)` back to the pool. */
export interface PgPoolClientLike {
  query(text: string, values?: unknown[]): Promise<{ rows: Record<string, unknown>[]; rowCount?: number | null }>;
  release(destroy?: boolean): void;
}

/** Adapt a pg `PoolClient` to the {@link AsyncConnection} seam (one owned connection). */
function pgConnection(client: PgPoolClientLike): AsyncConnection {
  return {
    async execute(sql, params) {
      const r = await client.query(sql, params as unknown[]);
      return r.rows as Rows;
    },
    async run(sql, params) {
      const r = await client.query(sql, params as unknown[]);
      return { changes: r.rowCount ?? 0, lastInsertRowid: 0 } as RunInfo;
    },
  };
}

/**
 * Adapt a pg `Pool` to the {@link AsyncConnectionPool} contract — `acquire` checks out an owned
 * `PoolClient`, `release` returns it (destroyed on a poisoned tx). Feed this to
 * `withTransactionAsync` for per-execution connection ownership.
 */
export function pgConnectionPool(pool: PgOwnedPoolLike): AsyncConnectionPool {
  const handles = new WeakMap<AsyncConnection, PgPoolClientLike>();
  return {
    async acquire() {
      const client = await pool.connect();
      const conn = pgConnection(client);
      handles.set(conn, client);
      return conn;
    },
    async release(conn, destroy) {
      handles.get(conn)?.release(destroy === true);
    },
  };
}

/** A mysql2/promise `Pool` that hands out an owned connection (`getConnection()` → `release()`). */
export interface MysqlOwnedPoolLike {
  getConnection(): Promise<MysqlPoolConnLike>;
}
/** A mysql2/promise pooled connection: `query`, and `release()` back to the pool. */
export interface MysqlPoolConnLike {
  query(sql: string, values?: unknown[]): Promise<[unknown, unknown]>;
  release(): void;
  destroy(): void;
}

/** Adapt a mysql2 pooled connection to the {@link AsyncConnection} seam (one owned connection). */
function mysqlConnection(conn: MysqlPoolConnLike): AsyncConnection {
  return {
    async execute(sql, params) {
      const [rows] = await conn.query(sql, params as unknown[]);
      return (Array.isArray(rows) ? rows : []) as Rows;
    },
    async run(sql, params) {
      const [res] = await conn.query(sql, params as unknown[]);
      const h = res as { affectedRows?: number; insertId?: number };
      return { changes: h?.affectedRows ?? 0, lastInsertRowid: h?.insertId ?? 0 } as RunInfo;
    },
  };
}

/**
 * Adapt a mysql2/promise `Pool` to the {@link AsyncConnectionPool} contract — `acquire` checks out
 * an owned connection, `release` returns it (or `destroy`s a poisoned one). Feed this to
 * `withTransactionAsync` for per-execution connection ownership.
 */
export function mysqlConnectionPool(pool: MysqlOwnedPoolLike): AsyncConnectionPool {
  const handles = new WeakMap<AsyncConnection, MysqlPoolConnLike>();
  return {
    async acquire() {
      const raw = await pool.getConnection();
      const conn = mysqlConnection(raw);
      handles.set(conn, raw);
      return conn;
    },
    async release(conn, destroy) {
      const raw = handles.get(conn);
      if (raw === undefined) return;
      if (destroy === true) raw.destroy();
      else raw.release();
    },
  };
}

// ── Phase C (#87): driver POOL FACTORIES — sizing/keepAlive applied at construction ───────────
//
// `buildRoutingConfig` (connection-routing.ts) OWNS pool construction so the C3 config's CONSTRUCTION
// knobs — pool sizing (`minPool`/`maxPool`) + `keepAlive`/`keepAliveInitialDelayMillis` — reach the
// real driver at `new pg.Pool({ max, … })` / `mysql2.createPool({ connectionLimit, … })` time (a
// pre-built pool can no longer accept them). These factories map a {@link ResolvedConnectionConfig}
// to the driver's pool options and return the owned-connection {@link AsyncConnectionPool} adapter +
// a closer. The configured sizing is thus the SOLE source of the pool cap. Session knobs
// (queryTimeout/searchPath/charset) are layered separately by `configuredPool` on checkout.

/** A pg `Pool` the factory constructs: owned-connection surface + `end()` (the closer). */
export type PgOwnedPoolWithEnd = PgOwnedPoolLike & { end(): Promise<void> };

/** The `pg` module surface the pool factory needs: an OWNED-connection `Pool` ctor + `types`. */
export interface PgFactoryModuleLike {
  Pool: new (config: Record<string, unknown>) => PgOwnedPoolWithEnd;
  types: PgTypesLike;
}

/**
 * A {@link PoolFactory} for `pg`: constructs a `new pg.Pool(...)` from a {@link ResolvedConnectionConfig},
 * applying the pool SIZING (`max` = `maxPool`, `min` = `minPool`) and `keepAlive` /
 * `keepAliveInitialDelayMillis` AT CONSTRUCTION — the config is the sole source of the cap. Connection
 * params (host/port/database/user/password) also flow from the config. Registers the read-path de-box
 * type parsers (issue #59, global, idempotent) once. Returns the owned-connection adapter + closer.
 *
 * `role` is accepted for the reader/writer replica split; the caller may vary host per role via the
 * config it passes. `Pool` is the `pg.Pool` ctor (peer dep, passed in — this module imports no driver).
 */
export function pgPoolFactory(pg: PgFactoryModuleLike): PoolFactory {
  return (config: ResolvedConnectionConfig, _role: 'reader' | 'writer'): { pool: AsyncConnectionPool; close: PoolCloser } => {
    configurePgDeboxTypeParsers(pg.types);
    const pool = new pg.Pool({
      ...(config.host !== undefined ? { host: config.host } : {}),
      ...(config.port !== undefined ? { port: config.port } : {}),
      ...(config.database !== undefined ? { database: config.database } : {}),
      ...(config.user !== undefined ? { user: config.user } : {}),
      ...(config.password !== undefined ? { password: config.password } : {}),
      // SIZING (construction knob): the config is the sole source of the cap.
      max: config.maxPool,
      min: config.minPool,
      // keepAlive (construction knob): TCP keepalive for serverless/long-idle connections.
      keepAlive: config.keepAlive,
      ...(config.keepAlive ? { keepAliveInitialDelayMillis: config.keepAliveInitialDelayMillis } : {}),
    });
    return { pool: pgConnectionPool(pool), close: () => pool.end() };
  };
}

/** A mysql2 `Pool` the factory constructs: owned-connection surface + `end()` (the closer). */
export type MysqlOwnedPoolWithEnd = MysqlOwnedPoolLike & { end(): Promise<void> };

/**
 * A {@link PoolFactory} for `mysql2/promise`: constructs a `mysql2.createPool(...)` from a
 * {@link ResolvedConnectionConfig}, applying the pool SIZING (`connectionLimit` = `maxPool`) +
 * `enableKeepAlive` / `keepAliveInitialDelay` AT CONSTRUCTION (the config is the sole source of the
 * cap) plus the read-path de-box options ({@link mysqlDeboxPoolOptions}). Connection params flow from
 * the config. Returns the owned-connection adapter + closer.
 *
 * NB: mysql2 has no `min` idle floor — `minPool` is a no-op there (a documented per-driver deviation;
 * the SIZING CAP that matters — `maxPool` → `connectionLimit` — is honored). `createPool` is the
 * `mysql2/promise` factory (peer dep, passed in).
 */
export function mysqlPoolFactory(mysql2: { createPool(config: Record<string, unknown>): MysqlOwnedPoolWithEnd }): PoolFactory {
  return (config: ResolvedConnectionConfig, _role: 'reader' | 'writer'): { pool: AsyncConnectionPool; close: PoolCloser } => {
    const pool = mysql2.createPool({
      ...(config.host !== undefined ? { host: config.host } : {}),
      ...(config.port !== undefined ? { port: config.port } : {}),
      ...(config.database !== undefined ? { database: config.database } : {}),
      ...(config.user !== undefined ? { user: config.user } : {}),
      ...(config.password !== undefined ? { password: config.password } : {}),
      // SIZING (construction knob): connectionLimit = maxPool (the sole source of the cap).
      connectionLimit: config.maxPool,
      // keepAlive (construction knob).
      enableKeepAlive: config.keepAlive,
      ...(config.keepAlive ? { keepAliveInitialDelay: config.keepAliveInitialDelayMillis } : {}),
      ...mysqlDeboxPoolOptions,
    });
    return { pool: mysqlConnectionPool(pool), close: () => pool.end() };
  };
}
