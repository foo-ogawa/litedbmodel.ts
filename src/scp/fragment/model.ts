/**
 * litedbmodel v2 SCP — the MINIMAL fragment model (epic #43 reset).
 *
 * ## The locked vocabulary (owner-specified — nothing else exists here)
 *
 * The portable artifact is ONLY a combination of **fragments**. There is NO
 * abstract "Expression IR", NO relation-op / operator kinds for SQL structure,
 * NO `FragmentTree` connector algebra, NO `{where}` splice marker. SQL structure
 * (`= ANY`, `CROSS JOIN LATERAL`, `UNNEST`, subquery, cast, batch shapes) lives
 * ENTIRELY as TEXT inside `sql`.
 *
 *   fragment = { sql: string, params: Param[], skip?: PresenceCondition }
 *
 * - A query is an ordered list of fragments.
 * - Assembling = concatenate the `sql` of every PRESENT fragment, and concatenate
 *   their `params` in order. If a fragment's `skip` is satisfied, BOTH its `sql`
 *   and its `params` are omitted.
 *
 *   Param = EITHER
 *     (a) a **value-spec** — how to compute the bound value from input at runtime:
 *         an input-ref / wire-ref / a small closed value-expression
 *         (e.g. `coalesce(input.limit, 20)`). This computes the VALUE only.
 *     OR
 *     (b) a **nested fragment** — a raw SQL sub-piece with its own params. This is
 *         how subqueries / sub-expressions are represented: NOT as IR constructs,
 *         just a nested { sql, params }.
 *
 * Array-parameter placeholder expansion is DRIVER-side, not modeled here. On
 * PostgreSQL an array param binds directly (`= ANY(?::int[])`), so the text is
 * static and the array is ONE param. On MySQL/SQLite (no array binding) the DRIVER
 * expands an array param into `(?, ?, …)` at bind time. The bundle stays
 * "string + params" regardless.
 *
 * The whole model is plain JSON / serializable so a thin runtime in any language
 * reads it identically.
 */

// ============================================================================
// Value-spec (Param variant a) — computes the bound VALUE only.
// A tiny CLOSED set. No SQL structure. No relation/operator "kinds".
// ============================================================================

/** Read a value from runtime input by path, e.g. `{ input: ["authorId"] }`. */
export interface InputRef {
  input: string[];
}

/** Read a value produced by an upstream node/wire, e.g. `{ wire: ["node1", "id"] }`. */
export interface WireRef {
  wire: string[];
}

/** A literal constant value baked into the bundle (e.g. an array of keys, or 20). */
export interface LiteralValue {
  literal: unknown;
}

/**
 * A small CLOSED value-expression. It computes a value from other value-specs.
 * This is deliberately tiny: `coalesce` and `add` are enough for the LIMIT / key
 * shapes in this slice. It is NOT an SQL-structure IR — it never emits SQL.
 */
export interface ValueExpr {
  op: 'coalesce' | 'add';
  args: ValueSpec[];
}

/** Param variant (a): a value-spec. */
export type ValueSpec = InputRef | WireRef | LiteralValue | ValueExpr;

// ============================================================================
// Param = value-spec (a) OR nested fragment (b).
// ============================================================================

/**
 * Param variant (b): a nested fragment. A subquery / sub-expression is just a
 * `{ sql, params }` embedded as a param — its `sql` is spliced into the parent's
 * `?` position, its params flow into the parent's param stream in order. This is
 * how subqueries are represented WITHOUT any IR construct.
 */
export type Param = ValueSpec | Fragment;

// ============================================================================
// Fragment — the ONLY structural type.
// ============================================================================

/**
 * A presence condition for `skip`. When SATISFIED, the fragment (sql + params)
 * is OMITTED. Deliberately closed and tiny: "the input at this path is absent /
 * null / undefined". SKIP is not an operator — it is the ABSENCE of the fragment.
 */
export interface PresenceCondition {
  /** Skip the fragment when the input at this path is null/undefined. */
  absent: string[];
}

/**
 * The one and only structural type: `{ sql, params, skip? }`.
 *
 * `sql` is literal SQL text (leading connector included where relevant, e.g.
 * `" AND status = ?"`) with `?` placeholders. `params` are 1:1 with the `?`
 * (a value-spec fills one `?`; a nested-fragment param splices its own `sql`,
 * which itself may contain further `?`). `skip`, when present and satisfied,
 * omits the whole fragment.
 */
export interface Fragment {
  sql: string;
  params: Param[];
  skip?: PresenceCondition;
}

/**
 * A WHERE clause is an ordered list of condition fragments joined by the SAME text
 * glue the ORIGINAL `DBConditions.compile` uses: present members are joined with
 * `" AND "` and the whole is prefixed with `" WHERE "` — and if NO member is
 * present, the clause degenerates to the empty string (exactly the original's
 * `parts.length === 0 → ''`).
 *
 * This is the "tree" form of the fragment model (a query is an ordered list/tree of
 * fragments). It carries NO SQL-structure vocabulary — the join is pure text glue,
 * identical to the original builder's `parts.join(' AND ')`. Each member fragment's
 * `sql` is the bare condition core (`"status = ?"`, NO leading connector); the
 * connector belongs to the join, so it is SKIP-stable regardless of which members
 * survive.
 *
 * Members carry their own `skip`; a skipped member contributes neither text nor
 * params, and the join simply closes over the survivors (so the first survivor never
 * emits a stray leading `" AND "`).
 */
export interface WhereGroup {
  where: Fragment[];
}

/** A member of an assembled query: a plain fragment or a WHERE group. */
export type Node = Fragment | WhereGroup;

// ============================================================================
// Type guards (structural — a nested-fragment param has `sql`; value-specs don't)
// ============================================================================

export function isFragment(p: Param | Node): p is Fragment {
  return typeof (p as Fragment).sql === 'string';
}

export function isWhereGroup(n: Node): n is WhereGroup {
  return Array.isArray((n as WhereGroup).where);
}

export function isInputRef(v: ValueSpec): v is InputRef {
  return Array.isArray((v as InputRef).input);
}
export function isWireRef(v: ValueSpec): v is WireRef {
  return Array.isArray((v as WireRef).wire);
}
export function isLiteral(v: ValueSpec): v is LiteralValue {
  return 'literal' in (v as LiteralValue);
}
export function isValueExpr(v: ValueSpec): v is ValueExpr {
  return typeof (v as ValueExpr).op === 'string' && Array.isArray((v as ValueExpr).args);
}
