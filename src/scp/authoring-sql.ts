/**
 * litedbmodel v2 SCP — SQL WHERE authoring helpers (WS3, #23).
 *
 * These are thin, closed-set wrappers over behavior-contracts' expression builders that
 * make a WHERE fragment tree authorable in the `SemanticBehavior` / eager surface while
 * emitting ONLY bc's closed-set Expression IR (no invented opcode — the hard rule). The
 * {@link import('./bridge').compileNode} Backend-Compile bridge decodes exactly these
 * closed-set encodings back into WS1's fragment `Condition[]`:
 *
 *   - {@link whereEq}   → `{eq:[<colRef>, <value>]}`                       → `col = ?`
 *   - {@link whereNe}/{@link whereLt}/… → `{ne|lt|le|gt|ge:[<colRef>, v]}` → `col <op> ?`
 *   - {@link whereIsNull} → `{eq:[<colRef>, null]}`                        → `col IS NULL`
 *   - {@link whereIn}   → `{eq:[{ref:[IN_SENTINEL, col]}, <arr>]}`         → `col IN (?, …)`
 *
 * A SKIP-optional condition is authored with bc's `when(cond, () => <whereX(...)>)`, which
 * bc lowers to `{cond:[cond, <member>, null]}` — the bridge reads that as a `skipWhen`
 * guard (dynamic-expansion spec §2). Nested AND/OR groups are authored with bc's `and` /
 * `or` over these members.
 *
 * The column argument is a RECORDED reference (`$.author_id`, or a wire field `$p.foo`):
 * its ref-path last segment is the physical column name (spec §7). The value argument is a
 * recorded reference, a literal, or any closed-set builder expression (`coalesce`, `add`, …).
 */

import { eq, ne, lt, le, gt, ge, type Recorded } from 'behavior-contracts';
import { IN_SENTINEL } from './makesql';

/** A recorded `$`-reference (column or value) or a literal value usable in a builder. */
type Operand = Recorded | unknown;

/**
 * The sentinel ref-path heads that mark the ADDITIVE where-primitives (V0 R2/R3) so the live
 * compile (`static-bundle.lowerWhereMember`) can decode them into the EXACT v1 `ConditionObject`
 * and drive the ORIGINAL `DBConditions.compile()` for byte-true text — never a hand-roll. Each is
 * encoded with the closed-set `eq(<sentinel ref>, <value>)` shape (the SAME trick {@link whereIn}
 * uses for IN-lists), so no opcode outside bc's closed set is invented. Structural metadata (the
 * column name, the SQL cast type, the LIKE keyword, the tuple column list, the subquery text)
 * rides as extra STRING segments in the sentinel ref path; the runtime value operands ride as the
 * `eq` RHS and are deferred as value-specs 1:1 with the placeholders in the v1-produced text.
 */
export const BETWEEN_SENTINEL = '@between';
export const LIKE_SENTINEL = '@like';
export const CAST_SENTINEL = '@cast';
export const DYNAMIC_SENTINEL = '@dynamic';
export const IMMEDIATE_SENTINEL = '@immediate';
export const TUPLE_SENTINEL = '@tuple';
export const SUBQUERY_SENTINEL = '@subquery';
export const EXISTS_SENTINEL = '@exists';
/**
 * A whole raw WHERE predicate carried verbatim (Phase F-2 / #105). Unlike the per-construct sentinels
 * above (each re-derives ONE v1 construct's text), this carries a COMPLETE predicate body (`sql`, with
 * its own `?` placeholders) + its bound value-specs, produced upstream by the ORIGINAL
 * `DBConditions.compile()` — so the v1 ActiveRecord condition surface (`find`'s `Conds` → one
 * `ConditionObject` → one compiled WHERE body) bridges onto the SCP where port in ONE member,
 * byte-true by construction, without re-decomposing every condition shape into per-member sugar. The
 * predicate rides as a nested makeSQL Fragment in the value slot (the SAME `{ sql, params }` shape the
 * subquery/EXISTS primitives use), spliced verbatim by the live compile.
 */
export const RAWPRED_SENTINEL = '@rawpred';

/** Build a sentinel column ref: `$[HEAD][seg0][seg1]…` → a recorded ref with that path. */
function sentinelRef($: Recorded, head: string, segs: readonly string[]): Recorded {
  let node = ($ as unknown as Record<string, Record<string, unknown>>)[head];
  for (const s of segs) node = (node as Record<string, unknown>)[s] as Record<string, unknown>;
  return node as unknown as Recorded;
}

// #141 WHERE model: a where fragment is a bc Expression (`eq`/`ne`/… over `$`-refs). It is passed as
// the `executeSQL` `where` PORT so bc RECORDS it (live recorder proxies → plain Expression IR); the
// post-compile pass in `authoring.ts` then lowers the RECORDED where IR to static `col = ?` SQL +
// param-refs via `lowerWherePort` (which decodes eq/ne/cmp + all sentinel sugar on plain IR). Lowering
// the recorded IR — never the live proxy — is why `$.col` cols and `whereRawPredicate` work (the
// authoring-time proxy-walk was the `NOT_RECORDABLE`/`column ref path` cause).

/** `col = value` — equality fragment. */
export function whereEq(col: Recorded, value: Operand): Recorded {
  return eq(col, value) as unknown as Recorded;
}

/** `col <> value` — inequality fragment. */
export function whereNe(col: Recorded, value: Operand): Recorded {
  return ne(col, value) as unknown as Recorded;
}

/** `col < value`. */
export function whereLt(col: Recorded, value: Operand): Recorded {
  return lt(col, value) as unknown as Recorded;
}

/** `col <= value`. */
export function whereLe(col: Recorded, value: Operand): Recorded {
  return le(col, value) as unknown as Recorded;
}

/** `col > value`. */
export function whereGt(col: Recorded, value: Operand): Recorded {
  return gt(col, value) as unknown as Recorded;
}

/** `col >= value`. */
export function whereGe(col: Recorded, value: Operand): Recorded {
  return ge(col, value) as unknown as Recorded;
}

/** `col IS NULL` — encoded as `eq(col, null)` (v1 parity; bridge maps null-RHS → IS NULL). */
export function whereIsNull(col: Recorded): Recorded {
  return eq(col, null) as unknown as Recorded;
}

/**
 * `col IN (?, …)` — encoded as `eq(<IN-sentinel col>, value)`. `col` MUST be the IN-sentinel
 * reference built by {@link inColumn} (its ref-path head is {@link IN_SENTINEL}); `value` is
 * the array-valued reference/expression whose `?` expands to `(?, ?, …)` at render time
 * (dynamic-expansion spec §5). Using the sentinel keeps IN expressible with only the
 * closed-set `eq` + `ref` operators.
 */
export function whereIn(col: Recorded, value: Operand): Recorded {
  return eq(col, value) as unknown as Recorded;
}

/**
 * Build the IN-list column reference for {@link whereIn}: `$[IN_SENTINEL][name]` — a
 * recorded ref whose path is `[IN_SENTINEL, name]`. The bridge strips the sentinel head and
 * treats `name` as the physical column (a membership fragment, not an equality).
 */
export function inColumn($: Recorded, name: string): Recorded {
  return ($ as unknown as Record<string, Record<string, Recorded>>)[IN_SENTINEL][name];
}

// ── Additive where-primitives (V0 R2/R3) — all v1-sourced at compile, live-reachable ──────────

/**
 * `col BETWEEN ? AND ?` (V0 R3). Encoded `eq($[@between][col], [lo, hi])`; the live compile builds
 * the v1 custom-op `ConditionObject` `{'<col> BETWEEN ? AND ?': [lo, hi]}` and defers both bounds.
 * `lo`/`hi` are recorded refs or literals.
 */
export function whereBetween($: Recorded, col: string, lo: Operand, hi: Operand): Recorded {
  return eq(sentinelRef($, BETWEEN_SENTINEL, [col]), [lo, hi]) as unknown as Recorded;
}

/**
 * `col LIKE ?` (V0 R3). Encoded `eq($[@like][col], pattern)` → v1 `{'<col> LIKE ?': pattern}`.
 * The pattern is a recorded ref or literal.
 */
export function whereLike($: Recorded, col: string, pattern: Operand): Recorded {
  return eq(sentinelRef($, LIKE_SENTINEL, [col, 'LIKE']), pattern) as unknown as Recorded;
}

/**
 * `col ILIKE ?` (V0 R3, PG case-insensitive; on MySQL/SQLite v1 emits the raw `ILIKE` keyword —
 * this reproduces v1 verbatim, not a portability rewrite). Encoded `eq($[@like][col][ILIKE], p)`.
 */
export function whereILike($: Recorded, col: string, pattern: Operand): Recorded {
  return eq(sentinelRef($, LIKE_SENTINEL, [col, 'ILIKE']), pattern) as unknown as Recorded;
}

/**
 * `col <op> ?::type` (V0 R3, dialect-gated cast — `dbCast`). Encoded
 * `eq($[@cast][col][type][op], value)`; the live compile drives v1 `dbCast(value, type, op)` so the
 * PG `::type` cast (and the MySQL/SQLite no-cast) is byte-true. `op` defaults to `=`.
 */
export function whereCast($: Recorded, col: string, sqlType: string, value: Operand, op: string = '='): Recorded {
  return eq(sentinelRef($, CAST_SENTINEL, [col, sqlType, op]), value) as unknown as Recorded;
}

/**
 * `col = fn(?)` (V0 R3, `dbDynamic`) — a function-call template with `?` placeholders bound to the
 * value list. Encoded `eq($[@dynamic][col][template], values)`; drives v1 `dbDynamic(template, vals)`.
 * `template` is the raw function SQL (e.g. `to_tsvector('en', ?)`).
 */
export function whereDynamic($: Recorded, col: string, template: string, values: Operand): Recorded {
  return eq(sentinelRef($, DYNAMIC_SENTINEL, [col, template]), values) as unknown as Recorded;
}

/**
 * `col = <sql>` (V0 R3, `dbImmediate` — an inline SQL expression, NO bound param, e.g. `NOW()`).
 * Encoded `eq($[@immediate][col][sql], null)`; drives v1 `dbImmediate(sql)`. No value-spec.
 */
export function whereImmediate($: Recorded, col: string, sql: string): Recorded {
  return eq(sentinelRef($, IMMEDIATE_SENTINEL, [col, sql]), null) as unknown as Recorded;
}

/**
 * `(a, b, …) IN ((?, ?), …)` (V0 R3, `dbTupleIn` composite membership). Encoded
 * `eq($[@tuple][a][b]…, tuples)` where `tuples` is an array-of-tuples value; drives v1
 * `dbTupleIn(columns, tuples)`. `columns` is the tuple column list.
 */
export function whereTupleIn($: Recorded, columns: readonly string[], tuples: Operand): Recorded {
  return eq(sentinelRef($, TUPLE_SENTINEL, columns), tuples) as unknown as Recorded;
}

/**
 * `lhs IN (SELECT …)` / `lhs NOT IN (SELECT …)` (V0 R2). The subquery is authored as a NESTED
 * makeSQL Fragment carried in the param slot (spec §2 補足): `sub` is a `{ sql, params? }` where
 * `sql` is the inner SELECT text (already dialect-tuned, v1-sourced) and `params` its bound
 * value-specs. Encoded `eq($[@subquery][lhsCol][kw], sub)`; the live compile splices the inner
 * `sql` into the v1 `DBSubquery`-shaped predicate `<lhs> <kw> (<sub.sql>)` and appends `sub.params`.
 */
export function whereInSubquery(
  $: Recorded,
  lhs: string,
  sub: { sql: string; params?: readonly Operand[] },
  not = false,
): Recorded {
  return eq(sentinelRef($, SUBQUERY_SENTINEL, [lhs, not ? 'NOT IN' : 'IN']), sub) as unknown as Recorded;
}

/**
 * `EXISTS (SELECT 1 …)` / `NOT EXISTS (…)` (V0 R2). Like {@link whereInSubquery}, the correlated
 * subquery rides as a nested makeSQL Fragment in the param slot: `sub.sql` is the inner
 * `SELECT 1 FROM … WHERE …` (correlation refs already rendered as literal column paths, v1-sourced).
 * Encoded `eq($[@exists][kw], sub)`; the live compile emits `<kw> (<sub.sql>)` + `sub.params`.
 */
export function whereExists(
  $: Recorded,
  sub: { sql: string; params?: readonly Operand[] },
  not = false,
): Recorded {
  return eq(sentinelRef($, EXISTS_SENTINEL, [not ? 'NOT EXISTS' : 'EXISTS']), sub) as unknown as Recorded;
}

/**
 * A COMPLETE raw WHERE predicate carried verbatim (Phase F-2 / #105). `pred.sql` is the whole
 * predicate body (with its own `?` placeholders) that the ORIGINAL `DBConditions.compile()` produced
 * for a v1 `ConditionObject`; `pred.params` its bound value-specs (recorded refs or literals), in `?`
 * order. Encoded `eq($[@rawpred], pred)` — the value slot carries the predicate as a nested makeSQL
 * Fragment (the SAME `{ sql, params }` shape {@link whereInSubquery}/{@link whereExists} use). The live
 * compile splices `pred.sql` verbatim and appends `pred.params`, so the emitted WHERE is byte-identical
 * to v1's (v1 IS the text source). This is the SINGLE-member bridge the F2 DBModel adapter uses to
 * lower an arbitrary `find`/`count` condition set onto the SCP where port without per-shape re-authoring.
 */
export function whereRawPredicate(
  $: Recorded,
  pred: { sql: string; params?: readonly Operand[] },
): Recorded {
  return eq(sentinelRef($, RAWPRED_SENTINEL, []), pred) as unknown as Recorded;
}

// ── Phase E-1 (#97): typed subquery / parentRef authoring SUGAR (TS-only ergonomics) ───────────
//
// These typed builders are a thin, PURE authoring layer over the existing {@link whereInSubquery} /
// {@link whereExists} primitives above — they add NO IR, NO sentinel, and NO runtime path. Each one
// RENDERS the inner subquery text (byte-for-byte the same shape v1's `DBSubquery`/`DBExists.compile`
// produce — fully `table.column`-qualified, `?` placeholders, params in encounter order) and then
// LOWERS to `whereInSubquery(lhs, {sql, params})` / `whereExists({sql, params})`. So the correlated
// subquery capability is unchanged; only the AUTHORING shape gets sugar that matches v1's typed API
// (`Model.inSubquery` / `.exists` / `parentRef`, DBModel.ts:1215-1352, DBValues.ts:574-821).

/**
 * A typed column reference — the v2 analogue of v1's `Column` (`{tableName, columnName}`). It renders
 * as `table.column` in every subquery position (unambiguous, v1 parity). Build one with {@link col}.
 */
export interface ColumnRef {
  readonly tableName: string;
  readonly columnName: string;
}

/** Build a typed {@link ColumnRef} (`table.column`). v2 has no `Column` class, so this is the seam. */
export function col(tableName: string, columnName: string): ColumnRef {
  return { tableName, columnName };
}

/** Brand marking the {@link parentRef} correlated outer-column marker in a condition VALUE slot. */
const PARENT_REF = Symbol('litedbmodel.parentRef');

/**
 * The correlated outer-column reference produced by {@link parentRef}. In a subquery condition value
 * position it renders as the OUTER `table.column` (NO bound param) — driving `col = outer.col` (v1
 * `DBParentRef.compile`, DBValues.ts:622-624). Anything not carrying {@link PARENT_REF} is a plain
 * value operand.
 */
export interface ParentRefValue {
  readonly [PARENT_REF]: true;
  readonly tableName: string;
  readonly columnName: string;
}

function isParentRef(v: unknown): v is ParentRefValue {
  return typeof v === 'object' && v !== null && (v as Record<PropertyKey, unknown>)[PARENT_REF] === true;
}

/**
 * Reference a column from the OUTER (parent) query inside a correlated subquery — v1's `parentRef`
 * (DBValues.ts:643-645). Renders as `table.column` with no bound param, so a subquery condition
 * `[Order.user_id, parentRef(User.id)]` becomes `orders.user_id = users.id`.
 */
export function parentRef(column: ColumnRef): ParentRefValue {
  return { [PARENT_REF]: true, tableName: column.tableName, columnName: column.columnName } as ParentRefValue;
}

/** A subquery WHERE condition: `[targetColumn, value]` — value may be a {@link parentRef}, an array
 *  (IN-list), `null` (IS NULL), or a scalar (`= ?`). Mirrors v1's `SubqueryCondition` shape. */
export type SubqueryCondition = readonly [ColumnRef, unknown];

function fmtCol(c: ColumnRef): string {
  return `${c.tableName}.${c.columnName}`;
}

/**
 * Render the WHERE fragment list for a subquery/EXISTS, byte-identical to v1's `DBSubquery`/
 * `DBExists.compile` inner loop (DBValues.ts:704-732, 786-814): parentRef → `col = outer.col` (no
 * param), null → `IS NULL`, array → `IN (?, …)` (empty → `1 = 0`), scalar → `= ?`. Placeholders are
 * always `?` (the makesql pass renumbers to the dialect's placeholder when it splices the fragment).
 */
function renderConditions(conditions: readonly SubqueryCondition[]): { where: string; params: Operand[] } {
  const parts: string[] = [];
  const params: Operand[] = [];
  for (const [column, value] of conditions) {
    const colRef = fmtCol(column);
    if (isParentRef(value)) {
      parts.push(`${colRef} = ${value.tableName}.${value.columnName}`);
    } else if (value === null || value === undefined) {
      parts.push(`${colRef} IS NULL`);
    } else if (Array.isArray(value)) {
      if (value.length === 0) {
        parts.push('1 = 0');
      } else {
        parts.push(`${colRef} IN (${value.map(() => '?').join(', ')})`);
        for (const v of value) params.push(v);
      }
    } else {
      parts.push(`${colRef} = ?`);
      params.push(value);
    }
  }
  return { where: parts.length ? ` WHERE ${parts.join(' AND ')}` : '', params };
}

/** A single key pair `[parentCol, targetCol]` or a list of them (composite key). Mirrors v1. */
export type KeyPair = readonly [ColumnRef, ColumnRef];
export type CompositeKeyPairs = readonly KeyPair[];

function normalizeKeyPairs(keyPairs: KeyPair | CompositeKeyPairs): KeyPair[] {
  // A single pair is `[ColumnRef, ColumnRef]`; a composite is `[[…],[…]]` — disambiguate on [0].
  return Array.isArray(keyPairs[0]) ? (keyPairs as CompositeKeyPairs).map((p) => p) : [keyPairs as KeyPair];
}

/** Shared IN / NOT IN typed builder — renders the inner SELECT and lowers to {@link whereInSubquery}. */
function inSubqueryImpl(
  $: Recorded,
  keyPairs: KeyPair | CompositeKeyPairs,
  conditions: readonly SubqueryCondition[],
  not: boolean,
): Recorded {
  const pairs = normalizeKeyPairs(keyPairs);
  const parentColumns = pairs.map((p) => p[0]);
  const selectColumns = pairs.map((p) => p[1]);
  const targetTable = selectColumns[0]?.tableName ?? '';
  const { where, params } = renderConditions(conditions);
  const selectClause = selectColumns.map(fmtCol).join(', ');
  const innerSql = `SELECT ${selectClause} FROM ${targetTable}${where}`;
  // Single key → `t.c`; composite → `(t.c1, t.c2)` — v1 `DBSubquery.compile` (DBValues.ts:739-745).
  const lhs =
    parentColumns.length === 1
      ? fmtCol(parentColumns[0])
      : `(${parentColumns.map(fmtCol).join(', ')})`;
  return whereInSubquery($, lhs, { sql: innerSql, params }, not);
}

/**
 * `parent.col IN (SELECT target.col FROM target WHERE …)` — typed sugar (v1 `Model.inSubquery`,
 * DBModel.ts:1215-1233). Single key `[parentCol, targetCol]` or composite `[[…],[…]]`. Lowers to
 * {@link whereInSubquery}; the rendered inner SELECT is v1-shape (`table.column`-qualified).
 */
export function inSubquery(
  $: Recorded,
  keyPairs: KeyPair | CompositeKeyPairs,
  conditions: readonly SubqueryCondition[] = [],
): Recorded {
  return inSubqueryImpl($, keyPairs, conditions, false);
}

/** `parent.col NOT IN (SELECT …)` — typed sugar (v1 `Model.notInSubquery`, DBModel.ts:1263-1281). */
export function notInSubquery(
  $: Recorded,
  keyPairs: KeyPair | CompositeKeyPairs,
  conditions: readonly SubqueryCondition[] = [],
): Recorded {
  return inSubqueryImpl($, keyPairs, conditions, true);
}

/** Shared EXISTS / NOT EXISTS typed builder — renders `SELECT 1 …` and lowers to {@link whereExists}. */
function existsImpl($: Recorded, conditions: readonly SubqueryCondition[], not: boolean): Recorded {
  const targetTable = conditions[0]?.[0]?.tableName ?? '';
  const { where, params } = renderConditions(conditions);
  const innerSql = `SELECT 1 FROM ${targetTable}${where}`;
  return whereExists($, { sql: innerSql, params }, not);
}

/**
 * `EXISTS (SELECT 1 FROM target WHERE …)` — typed sugar (v1 `Model.exists`, DBModel.ts:1306-1317).
 * The target table is inferred from the first condition's column; use {@link parentRef} for the
 * correlated outer reference. Lowers to {@link whereExists}.
 */
export function exists($: Recorded, conditions: readonly SubqueryCondition[]): Recorded {
  return existsImpl($, conditions, false);
}

/** `NOT EXISTS (SELECT 1 FROM target WHERE …)` — typed sugar (v1 `Model.notExists`, DBModel.ts:1341-1352). */
export function notExists($: Recorded, conditions: readonly SubqueryCondition[]): Recorded {
  return existsImpl($, conditions, true);
}

// ── QUERY view-model authoring (#98, Phase E-3) — lowers onto the EXISTING Select cte/cteParams ────

/**
 * A model's declared QUERY — either a raw SQL SELECT string, or an `{ sql, params }`
 * fragment carrying its own bound params (v1 `SqlFragment` shape; the SAME `{ sql, params? }`
 * the subquery helpers above accept). A QUERY makes the model a READ-ONLY VIEW over the
 * inner SELECT (v1 `DBModel.QUERY` — no `TABLE_NAME`; the read selects from the QUERY-as-CTE).
 */
export type QuerySource = string | { readonly sql: string; readonly params?: readonly Operand[] };

/**
 * Extra read ports for a QUERY view read — the SAME `Select` ports a normal read may carry
 * (`where` / `order` / `limit` / `offset` / `group`), plus a QUERY-only `params` slot for a
 * STRING query's bound values. It MUST NOT set `table` / `cte` / `cteParams` — {@link queryView}
 * owns those (it IS the QUERY→CTE lowering).
 */
export type QueryViewOptions = Omit<Record<string, unknown>, 'table' | 'cte' | 'cteParams'>;

/**
 * The `Select` ports for reading a QUERY-based (view-model) model, matching v1
 * (`DBModel._buildSelectSQL`, `:563-624`): a QUERY model has NO base table — the declared
 * QUERY becomes a `WITH <alias> AS (<QUERY sql>) SELECT … FROM <alias>` and the read selects
 * from that CTE. This lowers ONTO THE EXISTING `Select` `cte` / `cteParams` ports (no new IR,
 * no native work): `table` and the `cte.name` are BOTH the alias (v1 `getCTEAlias`), so the
 * emitted SQL is `WITH <alias> AS (<sql>) SELECT <select> FROM <alias> …` — the exact shape
 * the LIVE-tested `CteLive` vector already exercises.
 *
 * Param order matches v1 (`:574-589`): the QUERY's own params bind FIRST (they are the CTE
 * params — `cteParams` bind before JOIN/WHERE per the port contract, `catalog.ts:85-86`),
 * then any WHERE params (added by the fragment tree at render). A string QUERY takes its
 * params from `options.params`; a fragment QUERY prepends the fragment's own `params`, THEN
 * `options.params` (v1 `_resolveQuery`, `:1447-1457` — fragment params, then `_queryParams`).
 *
 * @param query   the declared QUERY: raw SQL string, or `{ sql, params }` fragment.
 * @param select  the projection over the QUERY's columns (the model's SELECT_COLUMN).
 * @param options extra read ports (`where` / `order` / `limit` / `offset` / `group`) and the
 *   optional QUERY-only `params` (string-query bound values); MUST NOT set
 *   `table` / `cte` / `cteParams` — this helper owns them.
 * @param alias   the CTE alias (v1 `getCTEAlias()` = `TABLE_NAME || 'derived'`); defaults to
 *   `'derived'` (a QUERY model has no `TABLE_NAME`).
 * @returns the `Select` ports object — pass to `L.Select(queryView(...))`.
 */
export function queryView(
  query: QuerySource,
  select: readonly string[],
  options: QueryViewOptions = {},
  alias = 'derived',
): Record<string, unknown> {
  const sql = typeof query === 'string' ? query : query.sql;
  // v1 param order: a fragment QUERY's own params come first, then the extra `params`
  // (v1 `_resolveQuery`: fragment.params, then `_queryParams`).
  const fragmentParams: readonly Operand[] = typeof query === 'string' ? [] : (query.params ?? []);
  const extra = ((options as { params?: readonly Operand[] }).params) ?? [];
  const cteParams = [...fragmentParams, ...extra];
  // The remaining read ports (drop the QUERY-only `params` key we consumed above).
  const rest = { ...options } as Record<string, unknown>;
  delete rest.params;
  return {
    table: alias,
    select: select as readonly string[],
    cte: { name: alias, sql },
    cteParams,
    ...rest,
  };
}
