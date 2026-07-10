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
export function mysqlPoolExecutor(pool: MysqlPoolLike): SqlExecutorAsync {
  return async (sql: string, params: unknown[]): Promise<Record<string, unknown>[]> => {
    const [rows] = await pool.query(sql, params);
    return rows;
  };
}
