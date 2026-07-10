/**
 * litedbmodel v2 SCP — the `makeSQL` HANDLER + dialect placeholder render.
 *
 * The handler is the ONE consumer implementation point behind the `makeSQL` catalog
 * leaf (spec §11 item 4): it receives the evaluated `{ sql, params, skip }` port
 * bundle from bc's `runBehavior`, assembles it (drop-on-skip, splice nested makeSQL),
 * renders the dialect placeholder form, binds params, and executes the SQL. Side
 * effects / driver / connection live entirely here (C4 — the IR carries no
 * implementation).
 *
 * bc supplies composition / value-eval / envelope / plan; litedbmodel supplies only
 * this handler + the compile that emits the SQL text.
 */

import type { Handler, HandlerCtx, Value } from 'behavior-contracts';
import { assembleMakeSQL, type MakeSQL, type AssembledSql } from './makesql';

/** A driver seam: run an already-rendered `{ sql, params }` and return result rows. */
export type SqlExecutor = (sql: string, params: unknown[]) => Promise<Record<string, unknown>[]>;
/** Synchronous driver seam (e.g. better-sqlite3) for the sync `runBehavior` path. */
export type SqlExecutorSync = (sql: string, params: unknown[]) => Record<string, unknown>[];

/** The dialect placeholder form: PostgreSQL uses `$N`, MySQL/SQLite keep `?`. */
export type Dialect = 'postgres' | 'mysql' | 'sqlite';

/**
 * Render `?` placeholders into the dialect form.
 *
 * - PostgreSQL: `?` → `$1, $2, …` (naive left-to-right, byte-identical to the original
 *   `src/drivers/postgres.ts` `convertPlaceholders`).
 * - MySQL / SQLite: `?` unchanged.
 *
 * There is NO array placeholder-count-expansion for ANY dialect (epic #43/#45): every
 * array/batch surface now binds the whole array/rows as ONE param against STATIC SQL
 * text — PG via `= ANY(?::t[])` / `UNNEST`, MySQL/SQLite via a single JSON param expanded
 * server-side (`MEMBER OF` / `JSON_TABLE` / `json_each`; see `json-array.ts`/`json-batch.ts`).
 * So `renderPlaceholders` is a pure `?`→`$N` mapping with a fixed placeholder count.
 */
export function renderPlaceholders(sql: string, dialect: Dialect): string {
  if (dialect !== 'postgres') return sql;
  let index = 0;
  return sql.replace(/\?/g, () => `$${++index}`);
}

/**
 * Coerce the evaluated `params` port back to a `MakeSQL['params']` list. bc evaluates
 * each port value; a nested `makeSQL` param arrives as an object `{ sql, params, skip? }`
 * and a bound value arrives as its opaque value. Nothing to transform — the handler
 * treats them structurally in {@link assembleMakeSQL}.
 */
function toMakeSQL(ports: Record<string, Value>): MakeSQL {
  return {
    sql: ports.sql as unknown as string,
    params: (ports.params as unknown as MakeSQL['params']) ?? [],
    skip: ports.skip === true,
  };
}

/** Assemble the evaluated ports (drop-on-skip, splice nested) and render the dialect form. */
export function renderPorts(ports: Record<string, Value>, dialect: Dialect): AssembledSql {
  const node = toMakeSQL(ports);
  const assembled = assembleMakeSQL(node);
  return { sql: renderPlaceholders(assembled.sql, dialect), params: assembled.params };
}

/**
 * Build the async `makeSQL` handler bound to a driver + dialect. This is what a
 * consumer registers under the catalog name `makeSQL` for `runBehaviorAsync`.
 *
 * A skipped component contributes nothing and never touches the driver — it returns
 * the empty result (`[]`) so downstream wiring reading its result sees "no rows".
 */
export function makeSqlHandler(exec: SqlExecutor, dialect: Dialect) {
  return async (ports: Record<string, Value>, _ctx: HandlerCtx) => {
    if (ports.skip === true) return { ok: [] as unknown as Value };
    const { sql, params } = renderPorts(ports, dialect);
    const rows = await exec(sql, params);
    return { ok: rows as unknown as Value };
  };
}

/**
 * Build the synchronous `makeSQL` handler (for `runBehavior` + a sync driver such as
 * better-sqlite3). Returns a bc {@link Handler}.
 */
export function makeSqlHandlerSync(exec: SqlExecutorSync, dialect: Dialect): Handler {
  return (ports: Record<string, Value>, _ctx: HandlerCtx) => {
    if (ports.skip === true) return { ok: [] as unknown as Value };
    const { sql, params } = renderPorts(ports, dialect);
    const rows = exec(sql, params);
    return { ok: rows as unknown as Value };
  };
}
