/**
 * litedbmodel v2 SCP — RELATION batch-load compile → `makeSQL`, reproducing the
 * ORIGINAL `LazyRelation` SQL text byte-for-byte across all shapes and dialects:
 *
 *   single-key, unlimited:
 *     PG           `SELECT … FROM t WHERE t.key = ANY(?::type[])[ AND <filters>][ ORDER BY …]`
 *     MySQL/SQLite `SELECT … FROM t WHERE t.key IN (?, …)[ AND <filters>][ ORDER BY …]`
 *   single-key, per-parent limit:
 *     PG           `SELECT t.* FROM unnest(?::type[]) AS _keys(key) CROSS JOIN LATERAL
 *                    (SELECT * FROM t WHERE t.key = _keys.key[ AND <filters>] ORDER BY … LIMIT n) t`
 *     MySQL/SQLite `WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY …) AS _rn
 *                    FROM t WHERE key IN (?, …)[ AND <filters>]) SELECT * FROM ranked WHERE _rn <= n`
 *   composite-key, unlimited:
 *     PG           `SELECT … FROM t JOIN unnest(?::t1[], ?::t2[]) AS _u(a1, a2) ON t.k1=_u.a1 AND …`
 *     MySQL/SQLite `SELECT … FROM t WHERE (k1, k2) IN ((?, ?), …)`
 *   composite-key, per-parent limit: PG LATERAL composite / others ROW_NUMBER composite.
 *
 * The PG type text (`?::type[]`) comes from the ORIGINAL `inferPgArrayType` (sqlCast
 * wins, else element-type inference). The .rs regressions are NOT reproduced: PG
 * per-parent-limit is LATERAL (not ROW_NUMBER), and PG types are sqlCast-driven (not
 * text-folded).
 *
 * Inner SELECTs reuse {@link compileSelect} — the same text `buildSelectSQL` yields —
 * so the whole shape is byte-faithful to the original relation builder.
 */

import { DBConditions, type ConditionObject } from '../../DBConditions';
import type { MakeSQL } from './makesql';
import { compileSelect } from './compile-select';
import type { Dialect } from './handler';

/**
 * Reproduce the ORIGINAL `LazyRelation.inferPgArrayType`: sqlCast wins (`<cast>[]`);
 * otherwise infer the element type from the sample values. Byte-identical to the
 * original (which is the PG anchor — NOT the .rs coarse text-folding).
 */
export function inferPgArrayType(values: unknown[], sqlCast?: string): string {
  if (sqlCast) return `${sqlCast}[]`;
  if (values.length === 0) return 'text[]';
  const sample = values[0];
  if (typeof sample === 'number') {
    if (values.every((v) => Number.isInteger(v))) return 'int[]';
    return 'numeric[]';
  }
  if (typeof sample === 'bigint') return 'bigint[]';
  if (typeof sample === 'boolean') return 'boolean[]';
  if (sample instanceof Date) return 'timestamp[]';
  return 'text[]';
}

export interface RelationCompileBase {
  dialect: Dialect;
  tableName: string;
  /** SELECT column list (default `*`). */
  select?: string;
  /** Optional relation `where`-filter conditions (`config.conditions`), merged in. */
  conditions?: ConditionObject;
  /** ORDER BY clause (raw text), or undefined. */
  order?: string;
  /** Per-column sqlCast map (drives PG `?::type[]`). */
  sqlCastMap?: Map<string, string>;
}

// ============================================================================
// Single-key, unlimited.
// ============================================================================

/**
 * `= ANY(?::type[])` (PG) / `IN (?, …)` (MySQL/SQLite) single-key unlimited batch load.
 * Reproduces `batchLoadWithAnyArray` (PG) and `batchLoadWithIn` (others).
 *
 * The key array binds as ONE param on PG (`values` is a single array param); on
 * MySQL/SQLite the original passes the array to `DBConditions` which expands it to
 * `IN (?, ?, …)` with one param per element — reproduced here verbatim.
 */
export function compileSingleKeyUnlimited(
  opts: RelationCompileBase & { targetKey: string; values: unknown[] }
): MakeSQL {
  if (opts.dialect === 'postgres') {
    const sqlCast = opts.sqlCastMap?.get(opts.targetKey);
    const pgType = inferPgArrayType(opts.values, sqlCast);
    const conditions: ConditionObject = {
      __raw__: [`${opts.tableName}.${opts.targetKey} = ANY(?::${pgType})`, [opts.values]],
      ...opts.conditions,
    };
    return compileSelect({
      dialect: opts.dialect,
      tableName: opts.tableName,
      select: opts.select,
      conditions,
      order: opts.order,
    });
  }
  // MySQL/SQLite: `{ ...conditions, [targetKey]: values }` → `IN (?, …)` (array expand).
  const conditions: ConditionObject = { ...opts.conditions, [opts.targetKey]: opts.values };
  return compileSelect({
    dialect: opts.dialect,
    tableName: opts.tableName,
    select: opts.select,
    conditions,
    order: opts.order,
  });
}

// ============================================================================
// Single-key, per-parent limit.
// ============================================================================

/**
 * Per-parent-limit single-key batch load: PG `CROSS JOIN LATERAL` (the v1 anchor —
 * NOT the .rs ROW_NUMBER regression); MySQL/SQLite `ROW_NUMBER() OVER (PARTITION BY …)`.
 * Reproduces `batchLoadWithLateral` / `batchLoadWithRowNumber`.
 */
export function compileSingleKeyLimited(
  opts: RelationCompileBase & { targetKey: string; values: unknown[]; limit: number }
): MakeSQL {
  if (opts.dialect === 'postgres') {
    const sqlCast = opts.sqlCastMap?.get(opts.targetKey);
    const pgType = inferPgArrayType(opts.values, sqlCast);
    const lateralConditions: ConditionObject = {
      __raw__: `${opts.tableName}.${opts.targetKey} = _keys.key`,
      ...opts.conditions,
    };
    const inner = compileSelect({
      dialect: opts.dialect,
      tableName: opts.tableName,
      conditions: lateralConditions,
      order: opts.order,
      limit: opts.limit,
    });
    const sql =
      `SELECT ${opts.tableName}.* FROM unnest(?::${pgType}) AS _keys(key) ` +
      `CROSS JOIN LATERAL (${inner.sql}) ${opts.tableName}`;
    return { sql, params: [opts.values, ...inner.params] };
  }

  // MySQL/SQLite: ROW_NUMBER() CTE.
  const orderBy = opts.order || opts.targetKey;
  const cteConditions: ConditionObject = { [opts.targetKey]: opts.values, ...opts.conditions };
  const cte = compileSelect({
    dialect: opts.dialect,
    tableName: opts.tableName,
    select: `*, ROW_NUMBER() OVER (PARTITION BY ${opts.targetKey} ORDER BY ${orderBy}) AS _rn`,
    conditions: cteConditions,
  });
  return compileSelect({
    dialect: opts.dialect,
    tableName: 'ranked',
    conditions: { __raw__: `_rn <= ${opts.limit}` },
    cte: { name: 'ranked', sql: cte.sql, params: cte.params },
  });
}

// ============================================================================
// Composite-key, unlimited.
// ============================================================================

/** Transpose tuples to per-column arrays: `[[1,a],[2,b]] → [[1,2],[a,b]]`. */
function transpose(targetKeys: string[], tuples: unknown[][]): unknown[][] {
  return targetKeys.map((_, colIndex) => tuples.map((t) => t[colIndex]));
}

/**
 * Composite-key unlimited batch load: PG `JOIN unnest(?::t1[], ?::t2[]) AS _u(a,b) ON …`
 * (reproduces `batchLoadWithUnnestJoin`); MySQL/SQLite `(k1, k2) IN ((?, ?), …)`
 * (reproduces `batchLoadWithCompositeIn` via `DBTupleIn`).
 */
export function compileCompositeKeyUnlimited(
  opts: RelationCompileBase & { targetKeys: string[]; tuples: unknown[][] }
): MakeSQL {
  const { tableName, targetKeys, tuples } = opts;
  if (opts.dialect === 'postgres') {
    const columnArrays = transpose(targetKeys, tuples);
    const unnestParams = columnArrays
      .map((arr, i) => `?::${inferPgArrayType(arr, opts.sqlCastMap?.get(targetKeys[i]))}`)
      .join(', ');
    const unnestAlias = `_unnest_${tableName}`;
    const columnAliases = targetKeys.map((k) => `_unnest_${tableName}_${k}`).join(', ');
    const joinConditions = targetKeys
      .map((key) => `${tableName}.${key} = ${unnestAlias}._unnest_${tableName}_${key}`)
      .join(' AND ');
    const joinClause = `JOIN unnest(${unnestParams}) AS ${unnestAlias}(${columnAliases}) ON ${joinConditions}`;
    return compileSelect({
      dialect: opts.dialect,
      tableName,
      select: opts.select,
      join: joinClause,
      joinParams: columnArrays,
      conditions: opts.conditions,
      order: opts.order,
    });
  }

  // MySQL/SQLite: (k1, k2) IN ((?, ?), …) built exactly as `batchLoadWithCompositeIn`.
  const tuplePlaceholders = tuples
    .map(() => `(${targetKeys.map(() => '?').join(', ')})`)
    .join(', ');
  const inClause = `(${targetKeys.join(', ')}) IN (${tuplePlaceholders})`;
  // The original builds base conditions FIRST then the composite IN last; DBConditions
  // preserves object key insertion order, so replicate that order.
  const conditions: ConditionObject = { ...opts.conditions, __raw__: [inClause, tuples.flat()] };
  return compileSelect({
    dialect: opts.dialect,
    tableName,
    select: opts.select,
    conditions,
    order: opts.order,
  });
}

// ============================================================================
// Composite-key, per-parent limit.
// ============================================================================

/**
 * Composite-key per-parent-limit: PG LATERAL composite (reproduces
 * `batchLoadWithLateralComposite`); MySQL/SQLite ROW_NUMBER composite (reproduces
 * `batchLoadWithRowNumberComposite`).
 */
export function compileCompositeKeyLimited(
  opts: RelationCompileBase & { targetKeys: string[]; tuples: unknown[][]; limit: number }
): MakeSQL {
  const { tableName, targetKeys, tuples, limit } = opts;

  if (opts.dialect === 'postgres') {
    const columnArrays = transpose(targetKeys, tuples);
    const unnestParams = columnArrays
      .map((arr, i) => `?::${inferPgArrayType(arr, opts.sqlCastMap?.get(targetKeys[i]))}`)
      .join(', ');
    const keyAliases = targetKeys.map((_, i) => `key${i}`).join(', ');
    const keyConditions = targetKeys
      .map((key, i) => `${tableName}.${key} = _keys.key${i}`)
      .join(' AND ');
    const lateralConditions: ConditionObject = { __raw__: keyConditions, ...opts.conditions };
    const inner = compileSelect({
      dialect: opts.dialect,
      tableName,
      conditions: lateralConditions,
      order: opts.order,
      limit,
    });
    const sql =
      `SELECT ${tableName}.* FROM unnest(${unnestParams}) AS _keys(${keyAliases}) ` +
      `CROSS JOIN LATERAL (${inner.sql}) ${tableName}`;
    return { sql, params: [...columnArrays, ...inner.params] };
  }

  // MySQL/SQLite ROW_NUMBER composite.
  const orderBy = opts.order || targetKeys.join(', ');
  const partitionBy = targetKeys.join(', ');
  const tuplePlaceholders = tuples
    .map(() => `(${targetKeys.map(() => '?').join(', ')})`)
    .join(', ');
  const inClause = `(${targetKeys.join(', ')}) IN (${tuplePlaceholders})`;
  const cteParams = tuples.flat();
  const cteConditions: ConditionObject = { __raw__: [inClause, cteParams], ...opts.conditions };
  const cte = compileSelect({
    dialect: opts.dialect,
    tableName,
    select: `*, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${orderBy}) AS _rn`,
    conditions: cteConditions,
  });
  return compileSelect({
    dialect: opts.dialect,
    tableName: 'ranked',
    conditions: { __raw__: `_rn <= ${limit}` },
    cte: { name: 'ranked', sql: cte.sql, params: cte.params },
  });
}

// Silence unused import in builds where DBConditions is only referenced via ConditionObject.
void DBConditions;
