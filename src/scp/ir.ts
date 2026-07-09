/**
 * litedbmodel v2 SCP — Backend Compile IR shapes (WS1, #21).
 *
 * These are the litedbmodel-specific SQL IR structures a Component-graph CRUD node
 * carries AFTER Backend Compile (spec §8). They are the input a thin multi-language
 * runtime reads (WS3) to produce byte-identical SQL. The normative rules for how a
 * runtime expands them are `docs/proposal/sql-dynamic-expansion-spec.md`.
 *
 * ## Param slots (spec §8) — the closed Expression IR set only
 *
 * Every element of a `params` array is 1:1 with a `?` placeholder and is one of (using
 * bc's FLAT scope — input ports and node results are top-level scope roots, matching
 * `runBehavior`'s `{...input, ...results}`):
 *   - an **input reference** `{ ref: ["authorId"] }`
 *   - a **wire reference** `{ ref: ["<nodeId>", "field"] }`
 *   - an **Expression IR operator node** `{ coalesce: [{ refOpt: ["limit"] }, 20] }`
 *
 * All three are Expression IR nodes from bc's CLOSED operator set (`PORTABLE_EXPR_OPERATORS`
 * — `ref`/`refOpt`/`coalesce`/`add`/`eq`/… ~23 ops). NO litedbmodel-local opcodes are ever
 * emitted; an un-lowerable SQL construct is the Raw SQL escape hatch's concern (spec §13),
 * not a fake opcode. This is enforced by {@link assertParamsPortable}.
 */

/** An Expression IR node (bc closed set). Opaque here; validated by the guard. */
export type ExprNode = unknown;

/**
 * A dynamic WHERE/SET fragment with an existence rule (spec §8).
 *
 * - `always: true` — the fragment is unconditionally present.
 * - `when: <ExprNode>` — the fragment is present iff the Expression evaluates to a
 *   truthy (present) binding. A `cond ? [...] : SKIP` in authoring lowers to a fragment
 *   whose `when` tests the presence of the driving input (§8 / feasibility §4). SKIP is
 *   NOT an Expression opcode — it is the ABSENCE of the fragment when `when` is false.
 *
 * `sql` is the fragment's literal SQL text (leading connector included, e.g. `" AND "`),
 * with `?` placeholders. `params` are the fragment's param slots (1:1 with its `?`),
 * appended to the assembled params array in fragment order only when the fragment is
 * present. `expand` marks an IN-list slot whose single `?` expands to `(?, ?, …)` per the
 * bound array's length (dynamic-expansion spec §5).
 */
export interface Fragment {
  /** Unconditionally present. Exactly one of `always` / `when` is set. */
  always?: true;
  /** Presence guard (Expression IR). Present iff it evaluates truthy. */
  when?: ExprNode;
  /** Literal SQL text (includes the leading connector, e.g. `" AND status = ?"`). */
  sql: string;
  /** Param slots (Expression IR nodes), 1:1 with this fragment's `?`. */
  params: ExprNode[];
  /**
   * IN-list expansion: index into `params` whose bound value is an array; its single `?`
   * in `sql` (written `(?)`) expands to `(?, ?, …)` (dynamic-expansion spec §5).
   */
  expand?: number;
}

/**
 * A compiled fragment tree (spec §8): ordered fragments combined by a single connector
 * (`AND` / `OR`). Nested groups parenthesize (dynamic-expansion spec §4). A `null` tree
 * means "no WHERE" (empty-WHERE degeneration, §3 of the spec).
 */
export interface FragmentTree {
  connector: 'AND' | 'OR';
  fragments: (Fragment | FragmentTree)[];
}

/** The assembly spec (row → logical model). WS1 emits the shape only. */
export interface AssemblySpec {
  shape: string;
}

/**
 * The Backend Compile output of one CRUD node (spec §8). `sql` is the fully flattened
 * static SQL with a single `{where}` splice point where the fragment tree renders (or no
 * splice point at all for writes whose WHERE is static). `params` are the STATIC param
 * slots (those outside the fragment tree — e.g. Insert values, LIMIT); fragment params
 * are interleaved at render time per the dynamic-expansion spec.
 */
export interface CompiledOperation {
  /** Catalog component name (`Select` / `Insert` / `Update` / `Delete`). */
  component: string;
  /** SQLite dialect SQL text with a `{where}` splice marker if a fragment tree applies. */
  sql: string;
  /** The dynamic WHERE/SET fragment tree, or null when the WHERE is fully static/absent. */
  where: FragmentTree | null;
  /**
   * Static param slots outside the fragment tree, in SQL position order. Each is an
   * Expression IR node (input-ref / wire-ref / operator) from the closed set.
   */
  params: ExprNode[];
  /** Row → logical model assembly (shape only in WS1). */
  assembly: AssemblySpec;
}

/** The literal `{where}` splice marker inside `CompiledOperation.sql`. */
export const WHERE_SLOT = '{where}';
