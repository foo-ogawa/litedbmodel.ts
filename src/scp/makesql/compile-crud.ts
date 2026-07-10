/**
 * litedbmodel v2 SCP — CRUD compile → `makeSQL` bundles, by driving the ORIGINAL
 * dialect SQL builders (`postgresSqlBuilder` / `mysqlSqlBuilder` / `sqliteSqlBuilder`)
 * and the original single-row UPDATE/DELETE text. The produced `{ sql, params }` is
 * byte-identical to what the original library sends, for every dialect and every
 * shape:
 *
 *   INSERT single (+per-col `?::sqlCast` on PG) & batch (PG UNNEST + type inference /
 *   MySQL·SQLite multi-VALUES) + ON CONFLICT (DO NOTHING / DO UPDATE incl. `= 'all'`
 *   → all columns) + RETURNING (bare / `t.col` alias / `table.col` / MySQL simulate);
 *   UPDATE single (`SET c = ?::sqlCast`) & batch (PG UNNEST / MySQL JOIN-VALUES /
 *   SQLite CASE-WHEN) + SKIP-column; DELETE; and the SELECT tail (LIMIT/OFFSET inline,
 *   FOR UPDATE, GROUP BY/HAVING) via `buildSelectSQL`.
 *
 * These delegate to the SAME builders the runtime uses, so parity is by construction.
 */

import { postgresSqlBuilder } from '../../drivers/PostgresSqlBuilder';
import { mysqlSqlBuilder } from '../../drivers/MysqlSqlBuilder';
import { sqliteSqlBuilder } from '../../drivers/SqliteSqlBuilder';
import type {
  SqlBuilder,
  InsertBuildOptions,
  UpdateManyBuildOptions,
  FindByPkeysOptions,
} from '../../drivers/types';
import { DBConditions, type ConditionObject } from '../../DBConditions';
import { DBToken, type SqlCastFormatter } from '../../DBValues';
import type { MakeSQL } from './makesql';
import { formatterFor } from './compile';
import type { Dialect } from './handler';

/** The original dialect builder for a dialect (the exact object the runtime uses). */
export function builderFor(dialect: Dialect): SqlBuilder {
  switch (dialect) {
    case 'postgres':
      return postgresSqlBuilder;
    case 'mysql':
      return mysqlSqlBuilder;
    case 'sqlite':
      return sqliteSqlBuilder;
  }
}

// ============================================================================
// INSERT (single & batch) — driven by the original `buildInsert`.
// ============================================================================

/**
 * Compile INSERT (single or batch) to a `makeSQL` bundle via the original
 * `buildInsert`. Single-row on PG carries per-col `?::sqlCast`; batch on PG uses UNNEST
 * with type inference; MySQL/SQLite use multi-VALUES. ON CONFLICT / RETURNING are the
 * original's exact verbs. `onConflictUpdate: 'all'` expands to every column (the
 * builder's fallback), so an empty DO-UPDATE never breaks.
 */
export function compileInsert(dialect: Dialect, options: InsertBuildOptions): MakeSQL {
  const { sql, params } = builderFor(dialect).buildInsert(options);
  return { sql, params };
}

// ============================================================================
// UPDATE batch — driven by the original `buildUpdateMany`.
// ============================================================================

/**
 * Compile batch UPDATE to a `makeSQL` bundle via the original `buildUpdateMany`: PG
 * UNNEST (`SET c = v.c FROM UNNEST(?::t[],…) AS v(cols) WHERE t.k = v.k`), MySQL
 * JOIN-VALUES (`JOIN (VALUES ROW(…)) AS v(…) ON … SET …`), SQLite CASE-WHEN
 * (`SET c = CASE WHEN k = ? THEN ? … END … WHERE k IN (…)`), with SKIP-column handling
 * (PG `CASE WHEN v._skip_c` / MySQL `IF(v._skip_c,…)` / SQLite `WHEN … THEN t.col`).
 */
export function compileUpdateMany(dialect: Dialect, options: UpdateManyBuildOptions): MakeSQL {
  const { sql, params } = builderFor(dialect).buildUpdateMany(options);
  return { sql, params };
}

// ============================================================================
// findByPkeys (single & composite) — driven by the original `buildFindByPkeys`.
// ============================================================================

/**
 * Compile findByPkeys to a `makeSQL` bundle: PG single = `= ANY(?::type[])`, PG
 * composite = `(cols) IN (SELECT * FROM UNNEST(?::t[],…))`; MySQL single = `IN (?,…)`,
 * composite = `JOIN (VALUES ROW(…)) AS v(…)`; SQLite single = `IN (?,…)`, composite =
 * `WITH v(cols) AS (VALUES …) … JOIN v ON …`.
 */
export function compileFindByPkeys(dialect: Dialect, options: FindByPkeysOptions): MakeSQL {
  const { sql, params } = builderFor(dialect).buildFindByPkeys(options);
  return { sql, params };
}

// ============================================================================
// UPDATE single — reproduces the ORIGINAL `_update` SET-clause + WHERE text exactly.
// ============================================================================

/**
 * Compile a single-row UPDATE to a `makeSQL` bundle, reproducing the original
 * `DBModel._update` text byte-for-byte: `UPDATE <t> SET <c = ?[::cast] | token>, … WHERE
 * <cond>[ RETURNING …]`. `serializedValues` is the already-serialized value map
 * (DBToken values compile via their own `.compile`, matching the original), and
 * `sqlCastMap` drives the per-column `?::sqlCast` on PG (skipping timestamp/date, as
 * the original does). Throws `UPDATE requires conditions` when the WHERE is empty
 * (v1 anchor: WHERE mandatory).
 */
export function compileUpdateSingle(opts: {
  dialect: Dialect;
  tableName: string;
  serializedValues: Record<string, unknown>;
  conditions: ConditionObject;
  sqlCastMap?: Map<string, string>;
  returning?: string;
}): MakeSQL {
  const params: unknown[] = [];
  const formatter: SqlCastFormatter | undefined =
    opts.dialect === 'postgres' ? formatterFor(opts.dialect) : undefined;
  const sqlCastMap = opts.sqlCastMap ?? new Map<string, string>();

  const setClauses: string[] = [];
  for (const [col, val] of Object.entries(opts.serializedValues)) {
    if (val instanceof DBToken) {
      setClauses.push(`${col} = ${val.compile(params, undefined, formatter)}`);
    } else {
      params.push(val);
      const sqlCast = sqlCastMap.get(col);
      if (sqlCast && formatter && sqlCast !== 'timestamp' && sqlCast !== 'date') {
        setClauses.push(`${col} = ${formatter('?', sqlCast)}`);
      } else {
        setClauses.push(`${col} = ?`);
      }
    }
  }

  const whereClause = new DBConditions(opts.conditions).compile(params, formatter);
  if (!whereClause) throw new Error('UPDATE requires conditions');

  let sql = `UPDATE ${opts.tableName} SET ${setClauses.join(', ')} WHERE ${whereClause}`;
  if (opts.returning) sql += ` RETURNING ${opts.returning}`;
  return { sql, params };
}

// ============================================================================
// DELETE single — reproduces the ORIGINAL `_delete` text exactly.
// ============================================================================

/**
 * Compile DELETE to a `makeSQL` bundle, reproducing the original `DBModel._delete`
 * text: `DELETE FROM <t> WHERE <cond>[ RETURNING …]`. Throws `DELETE requires
 * conditions` when the WHERE is empty (v1 anchor: WHERE mandatory — the .rs "delete
 * everything" behavior is NOT reproduced).
 */
export function compileDelete(opts: {
  dialect: Dialect;
  tableName: string;
  conditions: ConditionObject;
  returning?: string;
}): MakeSQL {
  const params: unknown[] = [];
  const formatter = opts.dialect === 'postgres' ? formatterFor(opts.dialect) : undefined;
  const whereClause = new DBConditions(opts.conditions).compile(params, formatter);
  if (!whereClause) throw new Error('DELETE requires conditions');

  let sql = `DELETE FROM ${opts.tableName} WHERE ${whereClause}`;
  if (opts.returning) sql += ` RETURNING ${opts.returning}`;
  return { sql, params };
}
