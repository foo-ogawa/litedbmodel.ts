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
 * Adapt a `pg.Pool` to the async executor seam. Each `exec` runs `pool.query`, which acquires a
 * pooled connection, runs the query, and releases it — so N concurrent siblings run on N pooled
 * connections in parallel. The graph renders `$N` placeholders for the `postgres` dialect, so the
 * `sql` is already in `pg`'s native form.
 *
 * Recommend constructing the pool with `max` ≥ the plan's `concurrency` (default 16).
 */
export function pgPoolExecutor(pool: PgPoolLike): SqlExecutorAsync {
  return async (sql: string, params: unknown[]): Promise<Record<string, unknown>[]> => {
    const result = await pool.query(sql, params);
    return result.rows;
  };
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

/**
 * Build a PG pool + executor with the read-path de-box wired IN (issue #59): registers the pg date
 * type parsers (DATE/TIMESTAMP family → native textual string) globally, then builds the pool.
 * int8 already arrives as a string on pg. Returns `{ pool, exec }` — pass `exec` to
 * `executeBehaviorAsync`. This is the production async path's de-box-correct entry (no hand-spread
 * options).
 */
export function pgDeboxExecutor(pg: PgModuleLike, config: Record<string, unknown>): { pool: PgPoolLike & { end?: () => Promise<void> }; exec: SqlExecutorAsync } {
  configurePgDeboxTypeParsers(pg.types);
  const pool = new pg.Pool(config);
  return { pool, exec: pgPoolExecutor(pool) };
}

/**
 * Build a MySQL pool + executor with the read-path de-box wired IN (issue #59): the pool carries
 * {@link mysqlDeboxPoolOptions} (`supportBigNumbers + bigNumberStrings` so BIGINT → exact string,
 * `dateStrings` so the date family → string) — options that MUST be set at pool construction (a
 * rounded BIGINT cannot be un-rounded post-hoc). Returns `{ pool, exec }` for `executeBehaviorAsync`.
 */
export function mysqlDeboxExecutor(mysql2: Mysql2ModuleLike, config: Record<string, unknown>): { pool: MysqlPoolLike & { end?: () => Promise<void> }; exec: SqlExecutorAsync } {
  const pool = mysql2.createPool({ ...config, ...mysqlDeboxPoolOptions });
  return { pool, exec: mysqlPoolExecutor(pool) };
}

export function mysqlPoolExecutor(pool: MysqlPoolLike): SqlExecutorAsync {
  return async (sql: string, params: unknown[]): Promise<Record<string, unknown>[]> => {
    const [rows] = await pool.query(sql, params);
    return rows;
  };
}
