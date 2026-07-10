/**
 * litedbmodel v2 SCP — the single-JSON-param array forms for MySQL 8 + SQLite
 * (epic #43 / #45).
 *
 * ## Why this exists — the intentional deviation from v1 text
 *
 * v1's MySQL/SQLite builders expand an array into N placeholders — `col IN (?, ?, …)`
 * for an IN-list, `VALUES (?,?),(?,?),…` for a batch INSERT, `VALUES ROW(…)` /
 * `CASE WHEN` for a batch UPDATE — binding ONE param per element. PostgreSQL already
 * avoids that explosion (`= ANY(?::t[])` / `UNNEST(?::t[])` binds the whole array as a
 * SINGLE param). This module gives MySQL/SQLite the same one-param property by moving
 * the expansion SERVER-side: the array (or the batch rows) is encoded as ONE JSON
 * string param and expanded inside the engine with `MEMBER OF` / `JSON_TABLE`
 * (MySQL 8.0.17+) and `json_each` / `json_extract` (SQLite json1, always present via
 * better-sqlite3).
 *
 * This is an owner-approved improvement OVER v1 — so the emitted SQL TEXT for
 * MySQL/SQLite array/batch surfaces intentionally differs from v1. The correctness bar
 * is RESULT PARITY (same rows / same post-write state on a real MySQL 8 + SQLite),
 * proven by `test/scp/json-array-parity.test.ts`. **PostgreSQL is untouched** — it keeps
 * `= ANY` / `UNNEST` / LATERAL and stays byte-identical to v1.
 *
 * Everything here is still just TEXT inside a `makeSQL` `sql` port plus ONE JSON param —
 * no new IR kind, no new catalog leaf. The driver-side placeholder-count-expansion that
 * v1 relied on for MySQL/SQLite arrays is GONE: every dialect now binds an array as one
 * param with static SQL text.
 */

import { DBConditions, type ConditionObject, type ConditionValue } from '../../DBConditions';
import { DBToken } from '../../DBValues';
import type { SqlCastFormatter } from '../../DBValues';
import type { Dialect } from './handler';

/** Dialects that use the single-JSON-param array forms (everything except PostgreSQL). */
export type JsonArrayDialect = 'mysql' | 'sqlite';

/**
 * The MySQL/SQLite IN-list JSON form for a plain `col IN [values]` condition.
 *
 * - MySQL: `col MEMBER OF (CAST(? AS JSON))` — `MEMBER OF` (8.0.17+) auto-casts the
 *   left column to JSON and does a type-aware membership test, so it needs NO declared
 *   element type (unlike `JSON_TABLE`), which is exactly right here because the IN-list
 *   condition path (`DBConditions`) carries no column type. Verified type-correct on
 *   real MySQL 8 for int / bigint / decimal / string keys.
 * - SQLite: `col IN (SELECT value FROM json_each(?))` — `json_each` yields each element
 *   with its natural (dynamic) type; SQLite's own affinity handles the comparison.
 *
 * The single param is the JSON-encoded array string (`[1,2,3]`). Empty arrays are NOT
 * routed here — the caller keeps v1's `1 = 0` for the empty case (no param, exact v1
 * empty semantics).
 */
export function inListJson(dialect: JsonArrayDialect, col: string, values: unknown[]): { sql: string; param: string } {
  const param = JSON.stringify(values);
  if (dialect === 'mysql') {
    return { sql: `${col} MEMBER OF (CAST(? AS JSON))`, param };
  }
  return { sql: `${col} IN (SELECT value FROM json_each(?))`, param };
}

/**
 * A dialect-aware `DBConditions` for MySQL/SQLite that replaces the v1 N-placeholder
 * IN-list (`col IN (?, ?, …)`) with the single-JSON-param form (see {@link inListJson}).
 * Every OTHER construct — eq/ne/cmp, custom-op, IS NULL, boolean literal, raw, subquery,
 * EXISTS, cast, tuple-IN, nested AND/OR — is delegated UNCHANGED to the base
 * `DBConditions`, so only the plain-array membership case deviates from v1.
 *
 * The empty-array case is left to the base class (`1 = 0`), matching v1 exactly.
 */
export class JsonArrayConditions extends DBConditions {
  private readonly _dialect: JsonArrayDialect;

  constructor(conditions: ConditionObject, dialect: JsonArrayDialect, operator: 'AND' | 'OR' = 'AND') {
    super(conditions, operator);
    this._dialect = dialect;
  }

  protected compileCondition(
    key: string,
    value: ConditionValue,
    params: unknown[],
    formatter?: SqlCastFormatter
  ): string {
    // Intercept ONLY the plain-array IN-list case (non-empty, no custom-op `?` in key,
    // not a DBToken). Everything else — including empty array (`1 = 0`), custom-op
    // arrays, tuple-IN, raw — falls through to the base class untouched.
    if (
      Array.isArray(value) &&
      value.length > 0 &&
      !key.includes('?') &&
      !key.startsWith('__') &&
      !(value instanceof DBToken)
    ) {
      const { sql, param } = inListJson(this._dialect, key, value);
      params.push(param);
      return sql;
    }
    return super.compileCondition(key, value, params, formatter);
  }
}

/**
 * Build a `DBConditions` appropriate for the dialect: PostgreSQL gets the base class
 * (v1 byte-identical IN-list), MySQL/SQLite get {@link JsonArrayConditions} (single-JSON
 * IN-list). Nested `DBConditions` inside a condition object stay base-class (they are
 * authored by callers via `new DBConditions`), so only TOP-LEVEL plain-array IN-lists on
 * MySQL/SQLite take the JSON form — which is the surface v1 expanded.
 */
export function conditionsFor(conditions: ConditionObject, dialect: Dialect): DBConditions {
  if (dialect === 'postgres') return new DBConditions(conditions);
  return new JsonArrayConditions(conditions, dialect);
}
