/**
 * litedbmodel v2 SCP — the COMPILE step (build time, TS). Emits the tuned dialect SQL
 * TEXT for every surface as `makeSQL` port bundles, by REUSING the ORIGINAL tuned
 * builders (`DBConditions`/`DBValues`, `LazyRelationContext`, `Postgres/Mysql/Sqlite
 * SqlBuilder`, `DBModel.buildSelectSQL` + single INSERT/UPDATE/DELETE). Because each
 * bundle's `sql`/`params` come from driving the original, the assembled + rendered
 * output is byte-identical to what the original library sends over the wire.
 *
 * There is NO abstract IR here and NO reduced form. The produced artifact is ONLY
 * `makeSQL` port bundles (`{ sql, params, skip? }`) — SQL structure (`= ANY`,
 * `CROSS JOIN LATERAL`, `UNNEST`, cast, subquery, batch shapes) is TEXT inside `sql`.
 *
 * The three dialects share this compile: PG compiled text is the anchor; MySQL/SQLite
 * text comes from the respective original dialect builder. Array-param placeholder
 * expansion (MySQL/SQLite `IN (?, …)` / multi-VALUES / CASE-WHEN) is the DRIVER's job
 * for arrays bound as one param — but the original MySQL/SQLite builders already emit
 * expanded `IN (?, ?, …)` text with one param per element, so the bundle carries that
 * text verbatim.
 */

import { DBConditions, type ConditionObject } from '../../DBConditions';
import type { SqlCastFormatter } from '../../DBValues';
import type { MakeSQL } from './makesql';
import type { Dialect } from './handler';

// ============================================================================
// Per-dialect cast formatter (drives DBConditions/DBValues casting text).
// PG applies `?::type`; MySQL/SQLite leave the placeholder unchanged (dialect-gated —
// the .rs bug of leaking `::uuid` to MySQL/SQLite is NOT reproduced; v1's dialect-aware
// SqlCastFormatter is the anchor).
// ============================================================================

/** PostgreSQL cast formatter: `?` → `?::type` (matches `DBValues` default formatter). */
export const pgCastFormatter: SqlCastFormatter = (placeholder, sqlType) => `${placeholder}::${sqlType}`;
/** MySQL/SQLite cast formatter: no cast (placeholder unchanged). */
export const noCastFormatter: SqlCastFormatter = (placeholder) => placeholder;

/** The cast formatter the original builders use for a given dialect. */
export function formatterFor(dialect: Dialect): SqlCastFormatter {
  return dialect === 'postgres' ? pgCastFormatter : noCastFormatter;
}

// ============================================================================
// WHERE / conditions / values — compiled by the ORIGINAL DBConditions.
// ============================================================================

/**
 * Compile a WHERE clause (bare, without the `WHERE` keyword) from a condition object
 * to a `makeSQL` bundle. The text + params come STRAIGHT from the original
 * `DBConditions.compile(params, formatter)`, so every construct it supports —
 * eq/ne/cmp (custom-op key), IN-list, empty-IN (`1 = 0`), IS NULL, IS NOT NULL
 * (`dbNotNull`), boolean literal (`= TRUE`), LIKE/ILIKE/BETWEEN (custom-op/raw),
 * IN/NOT IN subquery (`DBSubquery`), EXISTS/NOT EXISTS (`DBExists`), correlated
 * parentRef, raw (`__raw__`), AND/OR grouping with parens (`__or__` / nested
 * DBConditions), cast (`dbCast`/`dbCastIn`, dialect-gated), immediate inline
 * (`dbImmediate`/`dbRaw`), dynamic (`dbDynamic`), tuple/composite IN (`dbTupleIn`) —
 * is reproduced byte-for-byte.
 *
 * Empty clause → `{ sql: '', params: [] }` (the original returns `''` when no part is
 * present; folded here into an empty bundle so composition drops it).
 */
export function compileWhere(conditions: ConditionObject, dialect: Dialect): MakeSQL {
  const params: unknown[] = [];
  const formatter = formatterFor(dialect);
  const core = new DBConditions(conditions).compile(params, formatter);
  return { sql: core, params };
}

/**
 * Compile an OPTIONAL (SKIP-guarded) single-column equality: `<col> = ?`, present only
 * when the runtime value at `presentWhen` is non-null. The core text is emitted by the
 * original `DBConditions` (byte-identical). At bind time the caller resolves `skip`
 * from `presentWhen`; here we only carry the compiled shape and the value.
 *
 * @param column     column name (the condition key).
 * @param value      bound value for the present case.
 * @param skip       resolved presence: true ⇒ omit this component entirely.
 */
export function compileOptionalEq(
  column: string,
  value: unknown,
  dialect: Dialect,
  skip: boolean
): MakeSQL {
  const params: unknown[] = [];
  const core = new DBConditions({ [column]: '__probe__' }).compile(params, formatterFor(dialect));
  // core === `<col> = ?`; replace the probe param with the real value.
  return { sql: core, params: [value], skip };
}

// ============================================================================
// Composition helpers: prefix a bare WHERE with the connector text the original uses.
// ============================================================================

/**
 * Wrap a bare WHERE core (from {@link compileWhere}) as the ` WHERE <core>` clause a
 * SELECT/UPDATE/DELETE appends — byte-identical to the originals which do
 * `sql += \` WHERE ${whereClause}\`` only when `whereClause` is non-empty. An empty
 * core yields an empty bundle (no ` WHERE`).
 */
export function whereClause(where: MakeSQL): MakeSQL {
  if (where.sql === '') return { sql: '', params: [] };
  return { sql: ` WHERE ${where.sql}`, params: where.params };
}

/**
 * Compose a leading condition with an ordered list of optional (SKIP-guarded) trailing
 * conditions joined by ` AND `, exactly as the original relation/select path appends
 * `config.conditions` after a mandatory key predicate. Each optional member carries
 * its own connector (` AND <core>`), so a skipped member drops its connector too.
 *
 * Returns the list of `makeSQL` components (head + guarded tails); the caller composes.
 */
export function andTrailing(head: MakeSQL, tails: MakeSQL[]): MakeSQL[] {
  const guardedTails = tails.map((t) => ({
    sql: ` AND ${t.sql}`,
    params: t.params,
    ...(t.skip !== undefined ? { skip: t.skip } : {}),
  }));
  return [head, ...guardedTails];
}
