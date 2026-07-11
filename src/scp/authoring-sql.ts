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

/** Build a sentinel column ref: `$[HEAD][seg0][seg1]…` → a recorded ref with that path. */
function sentinelRef($: Recorded, head: string, segs: readonly string[]): Recorded {
  let node = ($ as unknown as Record<string, Record<string, unknown>>)[head];
  for (const s of segs) node = (node as Record<string, unknown>)[s] as Record<string, unknown>;
  return node as unknown as Recorded;
}

/** `col = value` — equality fragment (`{eq:[colRef, value]}`). */
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
