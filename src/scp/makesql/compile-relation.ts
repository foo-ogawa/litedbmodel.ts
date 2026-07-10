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

/**
 * The DEFERRED PG array-cast token (#46): a placeholder emitted in the STATIC SQL text where
 * the `= ANY(?::<T>[])` / `UNNEST(?::<T>[])` element type `<T>` cannot be known at symbolic
 * compile time (a schema-less `whereIn`, or a relation batch compiled without concrete keys).
 * The render/handler layer resolves it from the BOUND values via {@link inferPgArrayType} — a
 * mechanical dialect-render step, the same category as the `?`→`$N` placeholder render. This
 * reproduces v1's live-PG-correct cast (`::int[]` for int keys, etc.), which v1 got because
 * `inferPgArrayType` saw the real values at runtime.
 *
 * A byte sequence no legitimate SQL identifier/type text contains, so the resolve is a safe
 * literal substring replace (never a regex over user text).
 */
export const PG_ARRAY_CAST_TOKEN = '@@PG_ARRAY_CAST@@';

/**
 * Resolve the FIRST unresolved {@link PG_ARRAY_CAST_TOKEN} in a PG SQL fragment to the element
 * type inferred from the bound `values` (v1 `inferPgArrayType`). Called at render time, once per
 * array param, left-to-right — so each deferred array cast binds the type of its own value set.
 * SQL with no token is returned unchanged (every non-deferred cast is already concrete).
 */
export function resolvePgArrayCast(sql: string, values: unknown[]): string {
  const at = sql.indexOf(PG_ARRAY_CAST_TOKEN);
  if (at < 0) return sql;
  return sql.slice(0, at) + inferPgArrayType(values) + sql.slice(at + PG_ARRAY_CAST_TOKEN.length);
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
  /**
   * Emit the {@link PG_ARRAY_CAST_TOKEN} for the PG `?::<T>[]` element type instead of inferring
   * it from `values` NOW (#46). Set when the SQL text is compiled SYMBOLICALLY (placeholder keys),
   * so the element type is resolved at render from the REAL bound keys — never baked to `text[]`.
   * A `sqlCast` (concrete column type) still wins over the token.
   */
  deferPgArrayCast?: boolean;
}

/**
 * The PG array-cast element type for a batch cast, honoring `sqlCast` (concrete column type) →
 * the deferred {@link PG_ARRAY_CAST_TOKEN} (resolve at render from bound values) → inference from
 * the compile-time sample `values`. Centralizes the #43/#46 precedence for every relation shape.
 */
function pgArrayCastType(values: unknown[], sqlCast?: string, defer?: boolean): string {
  if (sqlCast) return `${sqlCast}[]`;
  if (defer) return PG_ARRAY_CAST_TOKEN;
  return inferPgArrayType(values);
}

/**
 * The STATIC composite-key batch forms (#47 item 1) — length-INDEPENDENT so the compiled `op.sql`
 * is fixed (one param per column on PG; ONE JSON param on MySQL/SQLite), the SAME static-op
 * property the single-key relation forms have. PG stays byte-identical to v1's `unnest`-JOIN
 * (`batchLoadWithUnnestJoin`), with the element-type cast DEFERRED (#46) to render from the real
 * keys. MySQL/SQLite use the single-JSON tuple form (the owner-approved deviation the single-key
 * IN-list and the batch UPDATE composite already use — RESULT parity, NOT byte-identity): a
 * `JSON_TABLE`/`json_each` subquery over one JSON array-of-tuples param. The v1 literal
 * `(k1,k2) IN ((?,?),…)` byte-form stays proven by the golden `compileCompositeKeyUnlimited`.
 *
 * The JSON tuple param is `[[k1a,k2a],[k1b,k2b],…]` (positional element arrays), read back by
 * ordinal path (`$[0]`, `$[1]`, …) so no per-column JSON key names are needed.
 */
export function compileCompositeKeyStaticUnlimited(
  opts: RelationCompileBase & { targetKeys: string[] },
): MakeSQL {
  const { tableName, targetKeys } = opts;
  if (opts.dialect === 'postgres') {
    // PG unnest-JOIN — ONE array param per key column (length-independent). Deferred cast (#46):
    // the element type is resolved at render from the real per-column key arrays.
    const unnestParams = targetKeys
      .map((k) => `?::${pgArrayCastType([], opts.sqlCastMap?.get(k), opts.deferPgArrayCast)}`)
      .join(', ');
    const unnestAlias = `_unnest_${tableName}`;
    const columnAliases = targetKeys.map((k) => `_unnest_${tableName}_${k}`).join(', ');
    const joinConditions = targetKeys
      .map((key) => `${tableName}.${key} = ${unnestAlias}._unnest_${tableName}_${key}`)
      .join(' AND ');
    const joinClause = `JOIN unnest(${unnestParams}) AS ${unnestAlias}(${columnAliases}) ON ${joinConditions}`;
    // One placeholder array PER column fixes the arity of the JOIN params (each binds one array).
    return compileSelect({
      dialect: opts.dialect,
      tableName,
      select: opts.select,
      join: joinClause,
      joinParams: targetKeys.map(() => [null]),
      conditions: opts.conditions,
      order: opts.order,
    });
  }
  // MySQL/SQLite: composite membership via ONE JSON array-of-tuples param, read by ORDINAL path.
  const jsonSubquery = compositeJsonMembership(opts.dialect, tableName, targetKeys);
  const conditions: ConditionObject = { ...opts.conditions, __raw__: [jsonSubquery, [[null]]] };
  return compileSelect({
    dialect: opts.dialect,
    tableName,
    select: opts.select,
    conditions,
    order: opts.order,
  });
}

/**
 * The MySQL/SQLite composite-membership predicate over ONE JSON array-of-tuples param (ordinal
 * paths). MySQL: `(k1,k2) IN (SELECT c0, c1 FROM JSON_TABLE(?, '$[*]' COLUMNS(c0 JSON PATH '$[0]',
 * …)))` — an IN-subquery so it inherits the SAME per-column type coercion `(k1,k2) IN ((?,?),…)`
 * would (the single-key rationale). SQLite: `EXISTS (SELECT 1 FROM json_each(?) je WHERE
 * json_extract(je.value,'$[0]') = t.k1 AND …)` — the composite json_each form the batch UPDATE uses.
 */
function compositeJsonMembership(dialect: Dialect, tableName: string, targetKeys: string[]): string {
  if (dialect === 'mysql') {
    const cols = targetKeys.map((_, i) => `c${i}`);
    const jtCols = cols.map((c, i) => `${c} JSON PATH '$[${i}]'`).join(', ');
    const selectCols = cols.map((c) => `JSON_UNQUOTE(${c})`).join(', ');
    const keyTuple = targetKeys.map((k) => `${tableName}.${k}`).join(', ');
    return `(${keyTuple}) IN (SELECT ${selectCols} FROM JSON_TABLE(?, '$[*]' COLUMNS(${jtCols})) jt)`;
  }
  // SQLite: EXISTS over json_each, matching every key column by ordinal element.
  const match = targetKeys
    .map((k, i) => `json_extract(je.value, '$[${i}]') = ${tableName}.${k}`)
    .join(' AND ');
  return `EXISTS (SELECT 1 FROM json_each(?) je WHERE ${match})`;
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
    const pgType = pgArrayCastType(opts.values, sqlCast, opts.deferPgArrayCast);
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
    const pgType = pgArrayCastType(opts.values, sqlCast, opts.deferPgArrayCast);
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
