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
import { DBToken, DBImmediateValue, type SqlCastFormatter } from '../../DBValues';
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

/**
 * Options for a heterogeneous `createMany`, mirroring the inputs `DBModel._insert` has
 * AFTER serialization: the serialized `records`, their paired `rawRecords`, the shared
 * `sqlCastMap` / ON CONFLICT / RETURNING. The per-group `columns` are DERIVED here (not
 * supplied) — this is the whole point of the grouping.
 */
export interface InsertManyBuildOptions {
  tableName: string;
  /** Serialized records (as `DBModel._insert` holds them after `serializeRecord`). */
  records: Record<string, unknown>[];
  /** Raw records before serialization, paired 1:1 with `records` (PG UNNEST path). */
  rawRecords?: Record<string, unknown>[];
  sqlCastMap?: Map<string, string>;
  onConflict?: string[];
  onConflictIgnore?: boolean;
  onConflictUpdate?: 'all' | string[];
  returning?: string;
}

/**
 * Compile a (possibly heterogeneous) `createMany` into a COMPOSITION of `makeSQL`
 * INSERT components — one per column-set group, exactly as `DBModel._insert`
 * (`src/DBModel.ts:928-1020`) groups records and emits one `buildInsert` per group.
 *
 * A `createMany` whose rows carry DIFFERENT column subsets is not one INSERT: the
 * production write path groups rows by their sorted-column-set pattern
 * (`patternKey = recordColumns.sort().join(',')`) so each batch INSERT is homogeneous
 * (no DEFAULT keyword), and emits ONE statement per group. In the `makeSQL` model that
 * is precisely a composition of several INSERT components — no new vocabulary. This
 * function reproduces that grouping byte-for-byte:
 *
 *  - column detection per row drops `undefined` and DEFAULT immediates, exactly as
 *    `_insert:940-946 / 958-964`;
 *  - columns are CANONICAL sorted (`recordColumns.sort()`), the pattern key is the
 *    joined sorted columns, group order is first-seen insertion order (`Map`), and each
 *    group carries its serialized + raw records paired — mirroring `_insert:955-975`;
 *  - each group's SQL text comes from the SAME original `buildInsert` (`compileInsert`),
 *    so every component is byte-identical to the statement `_insert` sends for that
 *    group.
 *
 * The composition of the returned components (via `composeMakeSQL`) is the full
 * multi-statement `createMany` the production path executes.
 */
export function compileInsertMany(dialect: Dialect, options: InsertManyBuildOptions): MakeSQL[] {
  const { records, rawRecords, tableName, sqlCastMap, onConflict, onConflictIgnore, onConflictUpdate, returning } = options;

  // Group records by their sorted-column-set pattern — mirrors DBModel._insert:928-975
  // exactly (fast single-record path + multi-record Map grouping, same DEFAULT/undefined
  // filtering, same canonical sort, same first-seen group order, same raw/serialized pair).
  const grouped = new Map<string, {
    columns: string[];
    records: Record<string, unknown>[];
    rawRecords: Record<string, unknown>[];
  }>();

  if (records.length === 1) {
    const record = records[0];
    const recordColumns: string[] = [];
    for (const key of Object.keys(record)) {
      const val = record[key];
      if (val !== undefined && !(val instanceof DBImmediateValue && val.value === 'DEFAULT')) {
        recordColumns.push(key);
      }
    }
    recordColumns.sort();
    grouped.set('_', { columns: recordColumns, records: [record], rawRecords: [rawRecords ? rawRecords[0] : record] });
  } else {
    for (let i = 0; i < records.length; i++) {
      const record = records[i];
      const recordColumns: string[] = [];
      for (const key of Object.keys(record)) {
        const val = record[key];
        if (val !== undefined && !(val instanceof DBImmediateValue && val.value === 'DEFAULT')) {
          recordColumns.push(key);
        }
      }
      recordColumns.sort();
      const patternKey = recordColumns.join(',');
      if (!grouped.has(patternKey)) {
        grouped.set(patternKey, { columns: recordColumns, records: [], rawRecords: [] });
      }
      const group = grouped.get(patternKey)!;
      group.records.push(record);
      group.rawRecords.push(rawRecords ? rawRecords[i] : record);
    }
  }

  // One makeSQL INSERT component per group, via the SAME original buildInsert.
  const components: MakeSQL[] = [];
  for (const { columns, records: groupRecords, rawRecords: groupRawRecords } of grouped.values()) {
    components.push(
      compileInsert(dialect, {
        tableName,
        columns,
        records: groupRecords,
        rawRecords: groupRawRecords,
        sqlCastMap,
        onConflict,
        onConflictIgnore,
        onConflictUpdate,
        returning,
      })
    );
  }
  return components;
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
