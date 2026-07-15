/**
 * litedbmodel v2 SCP — the single-JSON-param BATCH forms (createMany INSERT,
 * updateMany UPDATE) for MySQL 8 + SQLite (epic #43 / #45).
 *
 * v1 emits a multi-VALUES INSERT / `VALUES ROW(…)`-join (MySQL) / `CASE WHEN` (SQLite)
 * UPDATE, binding one param per cell. These forms instead encode the whole group's rows
 * as ONE JSON array-of-objects param and expand it SERVER-side:
 *
 *   MySQL  INSERT  `INSERT INTO t (cols) SELECT <exprs> FROM JSON_TABLE(?, '$[*]'
 *                     COLUMNS(c <T> PATH '$.c', …)) jt`
 *   MySQL  UPDATE  `UPDATE t AS u JOIN JSON_TABLE(?, '$[*]' COLUMNS(…)) AS v(…)
 *                     ON u.k = v.k SET u.c = IF(v._skip_c, u.c, v.c), …`
 *   SQLite INSERT  `INSERT INTO t (cols) SELECT json_extract(value,'$.c'), … FROM json_each(?)`
 *   SQLite UPDATE  `UPDATE t SET c = CASE <WHEN key THEN …> END … WHERE k IN
 *                     (SELECT json_extract(value,'$.k') FROM json_each(?))` — one json param.
 *
 * ONE JSON param per group; the SQL text is STATIC (no per-row placeholder explosion).
 * Correctness bar is RESULT PARITY vs v1, proven on real MySQL 8 + SQLite. PostgreSQL is
 * untouched (UNNEST). A DBToken value (e.g. `NOW()`) cannot be JSON-encoded, so a group
 * carrying one falls back to v1's builder for that group (rare; keeps correctness).
 */

import { DBToken } from '../../DBValues';
import type { MakeSQL } from './makesql';

/** MySQL/SQLite batch dialects. */
export type JsonArrayDialect = 'mysql' | 'sqlite';

/** True if any row carries a DBToken value (can't be JSON-encoded → v1 fallback). */
export function rowsHaveDbToken(rows: Record<string, unknown>[], columns: string[]): boolean {
  for (const r of rows) {
    for (const c of columns) {
      if (r[c] instanceof DBToken) return true;
    }
  }
  return false;
}

/**
 * Infer the MySQL `JSON_TABLE` COLUMNS type + SELECT expression for a column, from the
 * first non-null serialized value in the group. Chosen so the extracted value coerces
 * to the target column identically to v1's bound `?`:
 *
 *  - boolean / integer / bigint → `BIGINT` (JSON true/false → 1/0; ints exact),
 *  - non-integer number         → `DECIMAL(65,30)` (lossless),
 *  - object / array             → `JSON` kept as JSON (json columns),
 *  - string / null / other      → `JSON`, then `JSON_UNQUOTE(...)` in the SELECT so
 *    arbitrary-length text (beyond CHAR's 255 cap) and JSON-null → SQL NULL work.
 *
 * Returns `{ colType, selectExpr }` where `selectExpr` is applied to the JSON_TABLE
 * output column reference (e.g. `jt.name`).
 */
export function mysqlJsonTableColumn(
  col: string,
  rows: Record<string, unknown>[]
): { colType: string; selectExpr: (ref: string) => string } {
  let sample: unknown;
  for (const r of rows) {
    const v = r[col];
    if (v !== null && v !== undefined) {
      sample = v;
      break;
    }
  }
  if (typeof sample === 'boolean' || typeof sample === 'bigint') {
    return { colType: 'BIGINT', selectExpr: (ref) => ref };
  }
  if (typeof sample === 'number') {
    return Number.isInteger(sample)
      ? { colType: 'BIGINT', selectExpr: (ref) => ref }
      : { colType: 'DECIMAL(65,30)', selectExpr: (ref) => ref };
  }
  if (sample !== null && typeof sample === 'object') {
    // Object/array serialized value (e.g. a json column) — keep as JSON.
    return { colType: 'JSON', selectExpr: (ref) => ref };
  }
  // string, null, date-as-string, json-string, or all-null column → text via JSON_UNQUOTE.
  return { colType: 'JSON', selectExpr: (ref) => `JSON_UNQUOTE(${ref})` };
}

/**
 * Serialize a group's rows to a JSON array-of-objects string, keeping only `columns`.
 * A JSON-string value produced by v1's serializer (e.g. a `@column.json()` column whose
 * serializer already `JSON.stringify`'d it) is passed through as-is — it becomes a JSON
 * string element, matching what v1 binds. `undefined` cells are dropped (the group is
 * homogeneous by construction, so every column is present in every row).
 */
function rowsToJson(rows: Record<string, unknown>[], columns: string[]): string {
  const objects = rows.map((r) => {
    const o: Record<string, unknown> = {};
    for (const c of columns) o[c] = r[c] === undefined ? null : r[c];
    return o;
  });
  return JSON.stringify(objects);
}

// ============================================================================
// Batch INSERT — one JSON param, server-side expansion.
// ============================================================================

export interface JsonInsertOptions {
  tableName: string;
  columns: string[];
  records: Record<string, unknown>[];
  onConflict?: string[];
  onConflictIgnore?: boolean;
  onConflictUpdate?: 'all' | string[];
  returning?: string;
}

/**
 * Build the MySQL single-JSON-param batch INSERT. `INSERT [IGNORE] INTO t (cols)
 * SELECT <typed exprs> FROM JSON_TABLE(?, '$[*]' COLUMNS(…)) jt [ON DUPLICATE KEY …]`.
 */
export function mysqlInsertJson(opts: JsonInsertOptions): MakeSQL {
  const { tableName, columns, records, onConflict, onConflictIgnore, onConflictUpdate, returning } = opts;
  const jtCols: string[] = [];
  const selectExprs: string[] = [];
  for (const col of columns) {
    const { colType, selectExpr } = mysqlJsonTableColumn(col, records);
    jtCols.push(`${col} ${colType} PATH '$.${col}'`);
    selectExprs.push(selectExpr(`jt.${col}`));
  }
  const jsonParam = rowsToJson(records, columns);
  const source =
    `SELECT ${selectExprs.join(', ')} ` +
    `FROM JSON_TABLE(?, '$[*]' COLUMNS(${jtCols.join(', ')})) jt`;

  let sql: string;
  if (onConflict && onConflictIgnore) {
    sql = `INSERT IGNORE INTO ${tableName} (${columns.join(', ')}) ${source}`;
  } else if (onConflict && onConflictUpdate) {
    const updateCols = onConflictUpdate === 'all' ? columns : onConflictUpdate;
    const updateClauses = updateCols.map((c) => `${c} = VALUES(${c})`);
    sql = `INSERT INTO ${tableName} (${columns.join(', ')}) ${source} ON DUPLICATE KEY UPDATE ${updateClauses.join(', ')}`;
  } else {
    sql = `INSERT INTO ${tableName} (${columns.join(', ')}) ${source}`;
  }
  if (returning) sql += ` RETURNING ${returning}`;
  return { sql, params: [jsonParam] };
}

/**
 * Build the SQLite single-JSON-param batch INSERT. `INSERT [OR IGNORE] INTO t (cols)
 * SELECT json_extract(value,'$.c'), … FROM json_each(?) [ON CONFLICT … DO …]`.
 * `json_extract` returns each cell's natural type; SQLite affinity coerces on insert.
 */
export function sqliteInsertJson(opts: JsonInsertOptions): MakeSQL {
  const { tableName, columns, records, onConflict, onConflictIgnore, onConflictUpdate, returning } = opts;
  const selectExprs = columns.map((c) => `json_extract(value, '$.${c}')`);
  const jsonParam = rowsToJson(records, columns);
  const source = `SELECT ${selectExprs.join(', ')} FROM json_each(?)`;

  let sql: string;
  if (onConflict && onConflictIgnore) {
    sql = `INSERT OR IGNORE INTO ${tableName} (${columns.join(', ')}) ${source}`;
  } else if (onConflict && onConflictUpdate) {
    const updateCols = onConflictUpdate === 'all' ? columns : onConflictUpdate;
    const updateClauses = updateCols.map((c) => `${c} = excluded.${c}`);
    // SQLite grammar disambiguation (#67): in an `INSERT … SELECT … ON CONFLICT … DO UPDATE`, the
    // parser cannot tell whether `ON CONFLICT` binds to the SELECT's source or is the upsert clause
    // unless a `WHERE` terminates the SELECT — without it SQLite raises `near "DO": syntax error`.
    // A `WHERE true` is the standard, semantically-neutral terminator (SQLite docs "Parsing Ambiguity").
    // Only this SELECT-sourced upsert path needs it; `INSERT OR IGNORE` and the plain INSERT do not.
    sql = `INSERT INTO ${tableName} (${columns.join(', ')}) ${source} WHERE true ON CONFLICT (${onConflict.join(', ')}) DO UPDATE SET ${updateClauses.join(', ')}`;
  } else {
    sql = `INSERT INTO ${tableName} (${columns.join(', ')}) ${source}`;
  }
  if (returning) sql += ` RETURNING ${returning}`;
  return { sql, params: [jsonParam] };
}

// ============================================================================
// Batch UPDATE — one JSON param, server-side expansion + SKIP-column preserved.
// ============================================================================

export interface JsonUpdateManyOptions {
  tableName: string;
  keyColumns: string[];
  updateColumns: string[];
  records: Record<string, unknown>[];
  /** record-index → set of SKIP column names (leave DB value unchanged for those). */
  skipMap?: Map<number, Set<string>>;
  returning?: string;
}

/**
 * Build the MySQL single-JSON-param batch UPDATE: JSON_TABLE join with the SKIP-column
 * `IF(v._skip_c, u.c, v.c)` logic preserved. Each row's `_skip_c` flag is carried inside
 * the ONE JSON param (as `0`/`1`), so the SQL text stays static.
 *
 *   UPDATE t AS u JOIN JSON_TABLE(?, '$[*]' COLUMNS(k <T> PATH '$.k', c <T> PATH '$.c',
 *     _skip_c BIGINT PATH '$._skip_c', …)) AS v ON u.k = v.k
 *     SET u.c = IF(v._skip_c, u.c, v.c), …
 */
export function mysqlUpdateManyJson(opts: JsonUpdateManyOptions): MakeSQL {
  const { tableName, keyColumns, updateColumns, records, skipMap = new Map(), returning } = opts;
  const allColumns = [...keyColumns, ...updateColumns];

  const skipColumnsNeeded = new Set<string>();
  for (const s of skipMap.values()) for (const c of s) skipColumnsNeeded.add(c);

  // Build the JSON payload: each row's data columns + the _skip_ flags (0/1).
  const skipCols = [...skipColumnsNeeded];
  const payload = records.map((r, i) => {
    const o: Record<string, unknown> = {};
    for (const c of allColumns) o[c] = r[c] === undefined ? null : r[c];
    for (const c of skipCols) o[`_skip_${c}`] = skipMap.get(i)?.has(c) ? 1 : 0;
    return o;
  });
  const jsonParam = JSON.stringify(payload);

  const jtCols: string[] = [];
  const selectExprOf = new Map<string, (ref: string) => string>();
  for (const col of allColumns) {
    const { colType, selectExpr } = mysqlJsonTableColumn(col, records);
    jtCols.push(`${col} ${colType} PATH '$.${col}'`);
    selectExprOf.set(col, selectExpr);
  }
  for (const c of skipCols) jtCols.push(`_skip_${c} BIGINT PATH '$._skip_${c}'`);

  const setClauses = updateColumns.map((col) => {
    const vExpr = selectExprOf.get(col)!(`v.${col}`);
    return skipColumnsNeeded.has(col)
      ? `u.${col} = IF(v._skip_${col}, u.${col}, ${vExpr})`
      : `u.${col} = ${vExpr}`;
  });
  const onConditions = keyColumns.map((k) => `u.${k} = ${selectExprOf.get(k)!(`v.${k}`)}`);

  // Target aliased `u`, JSON_TABLE derived rows aliased `v`; the two aliases only need to
  // differ from each other so `u.col` / `v.col` are unambiguous. (v1's MySQL builder used
  // `AS t`; `u` here just keeps the target distinct from the `v` derived table — not a
  // correctness requirement, purely disambiguation.)
  let sql =
    `UPDATE ${tableName} AS u ` +
    `JOIN JSON_TABLE(?, '$[*]' COLUMNS(${jtCols.join(', ')})) AS v ` +
    `ON ${onConditions.join(' AND ')} ` +
    `SET ${setClauses.join(', ')}`;
  if (returning) sql += ` RETURNING ${returning}`;
  return { sql, params: [jsonParam] };
}

/**
 * Build the SQLite batch UPDATE over `json_each(?)`, SKIP preserved. A single JSON
 * array-of-objects (keys + update values + `_skip_c` flags) carries every row.
 *
 * NOTE on params: the SAME one JSON string is bound to EACH `?` — one per update column's
 * correlated sub-SELECT plus one for the WHERE — so `params` has `updateColumns.length + 1`
 * entries, ALL equal to that single JSON value. The row COUNT never multiplies the param
 * count (the N-placeholder explosion is what this replaces); the repetition is only across
 * the (fixed) number of SET columns, and the SQL text stays static regardless of row count.
 *
 * Each update column becomes a correlated lookup into the json array by matching key(s):
 *   `c = (SELECT CASE WHEN json_extract(je.value,'$._skip_c') THEN <table>.c
 *                     ELSE json_extract(je.value,'$.c') END
 *         FROM json_each(?) je
 *         WHERE json_extract(je.value,'$.k') = <table>.k [AND …] LIMIT 1)`
 * and the WHERE clause restricts affected rows to the keys present in the json array.
 */
export function sqliteUpdateManyJson(opts: JsonUpdateManyOptions): MakeSQL {
  const { tableName, keyColumns, updateColumns, records, skipMap = new Map(), returning } = opts;
  const allColumns = [...keyColumns, ...updateColumns];
  const skipColumnsNeeded = new Set<string>();
  for (const s of skipMap.values()) for (const c of s) skipColumnsNeeded.add(c);
  const skipCols = [...skipColumnsNeeded];

  const payload = records.map((r, i) => {
    const o: Record<string, unknown> = {};
    for (const c of allColumns) o[c] = r[c] === undefined ? null : r[c];
    for (const c of skipCols) o[`_skip_${c}`] = skipMap.get(i)?.has(c) ? 1 : 0;
    return o;
  });
  const jsonParam = JSON.stringify(payload);

  const params: unknown[] = [];
  const keyMatch = keyColumns.map((k) => `json_extract(je.value, '$.${k}') = ${tableName}.${k}`).join(' AND ');

  const setClauses = updateColumns.map((col) => {
    params.push(jsonParam);
    const valueExpr = skipColumnsNeeded.has(col)
      ? `CASE WHEN json_extract(je.value, '$._skip_${col}') THEN ${tableName}.${col} ELSE json_extract(je.value, '$.${col}') END`
      : `json_extract(je.value, '$.${col}')`;
    return (
      `${col} = (SELECT ${valueExpr} FROM json_each(?) je ` +
      `WHERE ${keyMatch} LIMIT 1)`
    );
  });

  // WHERE: restrict to the affected keys. Single key → k IN (SELECT …); composite →
  // EXISTS over json_each matching all key columns.
  let whereClause: string;
  if (keyColumns.length === 1) {
    params.push(jsonParam);
    whereClause = `${keyColumns[0]} IN (SELECT json_extract(value, '$.${keyColumns[0]}') FROM json_each(?))`;
  } else {
    params.push(jsonParam);
    const match = keyColumns.map((k) => `json_extract(je.value, '$.${k}') = ${tableName}.${k}`).join(' AND ');
    whereClause = `EXISTS (SELECT 1 FROM json_each(?) je WHERE ${match})`;
  }

  let sql = `UPDATE ${tableName} SET ${setClauses.join(', ')} WHERE ${whereClause}`;
  if (returning) sql += ` RETURNING ${returning}`;
  return { sql, params };
}
