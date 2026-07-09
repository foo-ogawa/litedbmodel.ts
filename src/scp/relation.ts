/**
 * litedbmodel v2 SCP — Relation ops (Read) + staged batch resolution (WS4, #24).
 *
 * Relations are NOT SQL JOINs by default (spec §5). They are the same
 * staged-batch query-composition + object-assembly shape graphddb and v1 `LazyRelation`
 * use: collect the parent key set of a result page, run ONE batched child SELECT
 * (`fk IN (…)` / per-parent-limited `ROW_NUMBER()` window), then distribute the child rows
 * back to their parents. This makes a relation load structurally bounded — one batched
 * query per relation edge, NEVER one-per-parent (no N+1).
 *
 * ## The relation op is a pre-compiled artifact (spec §8)
 *
 * A {@link RelationOp} is compiled ONCE from a model {@link RelationDecl}, dialect-aware
 * (SQLite for α), into the SAME {@link CompiledOperation} shape WS1 already emits for a
 * Select — so it renders through the SAME normative {@link renderOperation} (fragment tree
 * + IN-list `(?, ?, …)` expansion, spec §5). It carries NO functions and lands in the §8
 * bundle (`SqlBundle.relations`) as pure JSON, so a thin per-language runtime (WS7) gets
 * the relation batch SQL for free. The v1 `LazyRelation` batch-SQL assets (IN-list, the
 * `ROW_NUMBER() OVER (PARTITION BY … )` per-parent limit) are reused here as the compiled
 * content, not re-invented.
 *
 * ## ONE relation op, TWO surfaces (spec §5 / feasibility §9)
 *
 * Both read surfaces trigger the IDENTICAL compiled {@link RelationOp} via
 * {@link runRelationOp} — there is exactly one batch-execution code path:
 *
 *   - **Declarative select** (`with: { author: true }`): the planner resolves sibling
 *     relations eagerly, dedups the parent key set, prefetch-optimized (staged batch).
 *   - **Lazy** (`await post.author`): a prototype getter + a non-enumerable Symbol batch
 *     context (graphddb's `GRAPHDDB_KEY` technique) fires the SAME op at access time over
 *     the whole sibling set (batch, so still no N+1 — it just can't prefetch).
 *
 * `runRelationOp` renders `op.query` once for the deduped parent key set and returns the
 * child rows grouped by parent key. Both surfaces call it; a test asserts they resolve via
 * the same compiled op (same SQL text), not two parallel code paths.
 */

import { renderOperation, type RenderedSql } from './render';
import type { CompiledOperation, ExprNode, Fragment, FragmentTree } from './ir';
import { WHERE_SLOT } from './ir';

/** A read relation cardinality (v1 parity). `belongsTo`/`hasOne` are single; `hasMany` many. */
export type RelationKind = 'belongsTo' | 'hasMany' | 'hasOne';

/**
 * A model's read-relation declaration (the authored/decorated relation metadata, spec §4).
 * This is the litedbmodel-consumer input the relation compiler lowers to a {@link RelationOp}.
 * It mirrors v1's `@belongsTo`/`@hasMany`/`@hasOne` config (source/target key + optional
 * per-parent order/limit), reduced to the fields the batch SQL needs.
 */
export interface RelationDecl {
  /** Relation name (the property attached on the parent object). */
  readonly name: string;
  /** Cardinality. */
  readonly kind: RelationKind;
  /** The child (target) table name. */
  readonly targetTable: string;
  /** Child columns to project (the child typed-object own props). */
  readonly select: readonly string[];
  /**
   * The PARENT column whose value is the batch key (e.g. `belongsTo` → the parent FK
   * `author_id`; `hasMany`/`hasOne` → the parent PK `id`).
   */
  readonly parentKey: string;
  /**
   * The CHILD column matched against the parent key (e.g. `belongsTo` → child PK `id`;
   * `hasMany`/`hasOne` → child FK `post_id`).
   */
  readonly targetKey: string;
  /** Optional per-parent ORDER BY body (dialect-neutral text, e.g. `"created_at DESC"`). */
  readonly order?: string;
  /**
   * Optional per-parent row limit (`hasMany` only). When set, the batch query uses a
   * `ROW_NUMBER() OVER (PARTITION BY <targetKey> ORDER BY <order>)` window so each parent
   * gets at most `limit` children in ONE query (v1's SQLite `batchLoadWithRowNumber`).
   */
  readonly limit?: number;
}

/**
 * A pre-compiled relation batch op (spec §8 `relation ops`). Pure JSON — it carries the
 * compiled batch SELECT ({@link CompiledOperation}, rendered by {@link renderOperation})
 * plus the grouping metadata the runtime needs to distribute child rows to parents. No
 * functions, so it round-trips through `JSON.stringify` inside the bundle.
 */
export interface RelationOp {
  readonly name: string;
  readonly kind: RelationKind;
  /** Parent column supplying the batch key values (dedup key). */
  readonly parentKey: string;
  /** Child column the batch groups rows by (matches the parent key). */
  readonly targetKey: string;
  /**
   * The batched child SELECT as a {@link CompiledOperation}. Its WHERE is a single IN-list
   * fragment `targetKey IN (?)` whose one slot binds the deduped parent-key array and
   * expands to `(?, ?, …)` at render time (spec §5). A `limit` decl compiles to a
   * `ROW_NUMBER()` window CTE instead (per-parent limit).
   */
  readonly query: CompiledOperation;
}

/** The reserved input head the relation batch query binds its deduped key array to. */
export const RELATION_KEYS_HEAD = '__keys';

/**
 * Compile ONE {@link RelationDecl} into a {@link RelationOp} for SQLite (α dialect). The
 * batch query selects the child rows whose {@link RelationDecl.targetKey} is in the deduped
 * parent-key set. Reuses the existing IN-list fragment + `renderOperation` expansion (no new
 * SQL machinery). A `hasMany` with `limit` compiles to the v1 `ROW_NUMBER()` window form so
 * the per-parent cap holds in a single batched statement.
 */
export function compileRelationOp(decl: RelationDecl): RelationOp {
  if (decl.kind !== 'hasMany' && decl.limit !== undefined) {
    throw new Error(`relation '${decl.name}': a per-parent 'limit' is only valid for hasMany (got ${decl.kind})`);
  }
  const query = decl.limit !== undefined ? compileLimitedBatch(decl) : compilePlainBatch(decl);
  return {
    name: decl.name,
    kind: decl.kind,
    parentKey: decl.parentKey,
    targetKey: decl.targetKey,
    query,
  };
}

/** The IN-list membership slot binding the deduped key array (`{ref:[RELATION_KEYS_HEAD]}`). */
function keysRef(): ExprNode {
  return { ref: [RELATION_KEYS_HEAD] } as ExprNode;
}

/**
 * Plain batch (`belongsTo`/`hasOne`/unlimited `hasMany`):
 *   `SELECT <cols> FROM <target>{where}[ ORDER BY <o>]` with `{where}` = `targetKey IN (?)`.
 * The `(?)` expands to one `?` per deduped key at render time (dynamic-expansion spec §5).
 */
function compilePlainBatch(decl: RelationDecl): CompiledOperation {
  const cols = decl.select.join(', ');
  const inFragment: Fragment = {
    always: true,
    sql: `${decl.targetKey} IN (?)`,
    params: [keysRef()],
    expand: 0,
  };
  const where: FragmentTree = { connector: 'AND', fragments: [inFragment] };
  let sql = `SELECT ${cols} FROM ${decl.targetTable}${WHERE_SLOT}`;
  if (decl.order !== undefined) sql += ` ORDER BY ${decl.order}`;
  return { component: 'Select', sql, where, params: [], assembly: { shape: 'items' } };
}

/**
 * Per-parent-limited `hasMany` batch — v1's SQLite `ROW_NUMBER()` window form:
 *
 *   WITH ranked AS (
 *     SELECT <cols>, ROW_NUMBER() OVER (PARTITION BY <targetKey> ORDER BY <order>) AS _rn
 *     FROM <target>{where}                         -- {where} = targetKey IN (?)
 *   )
 *   SELECT <cols> FROM ranked WHERE _rn <= <limit>
 *
 * The `{where}` splice keeps the IN-list expansion (spec §5). `_rn` is a synthetic window
 * column dropped from the final projection, so the child typed-object own props stay exactly
 * `select`. `limit` is inlined as a literal integer (a compiled per-parent cap from the decl,
 * NOT a runtime-bound value — the decl is the SSoT), mirroring v1's `_rn <= ${limit}`.
 */
function compileLimitedBatch(decl: RelationDecl): CompiledOperation {
  const cols = decl.select.join(', ');
  const limit = decl.limit!;
  if (!Number.isInteger(limit) || limit < 0) {
    throw new Error(`relation '${decl.name}': per-parent limit must be a non-negative integer (got ${String(limit)})`);
  }
  // A per-parent limit REQUIRES a deterministic ORDER BY (the window decides WHICH `limit`
  // rows each parent keeps). Absent order = fail-closed authoring error, NOT a silent default
  // — the relation declaration is the SSoT for the per-parent ordering.
  if (decl.order === undefined) {
    throw new Error(
      `relation '${decl.name}': a per-parent 'limit' requires an explicit 'order' (the ROW_NUMBER() window needs a deterministic ordering to decide which ${limit} rows each parent keeps)`,
    );
  }
  const order = decl.order;
  const inFragment: Fragment = {
    always: true,
    sql: `${decl.targetKey} IN (?)`,
    params: [keysRef()],
    expand: 0,
  };
  const where: FragmentTree = { connector: 'AND', fragments: [inFragment] };
  const sql =
    `WITH ranked AS (SELECT ${cols}, ROW_NUMBER() OVER (PARTITION BY ${decl.targetKey} ORDER BY ${order}) AS _rn ` +
    `FROM ${decl.targetTable}${WHERE_SLOT}) ` +
    `SELECT ${cols} FROM ranked WHERE _rn <= ${limit}`;
  return { component: 'Select', sql, where, params: [], assembly: { shape: 'items' } };
}

// ── Batch execution (the SINGLE code path both read surfaces share) ────────────

/** A minimal read-only driver surface (`prepare(sql).all(...params)`), the SQLite `Database`. */
export interface RelationDriver {
  prepare(sql: string): { all(...params: unknown[]): unknown[] };
}

/** The child rows grouped for a batch: parent-key value (stringified) → child rows. */
export type RelationBatch = Map<string, Record<string, unknown>[]>;

/**
 * Run ONE {@link RelationOp} for a set of parent rows: dedup the parent keys, render + execute
 * the batched child SELECT ONCE (real SQLite), and group the child rows by their target key.
 * This is the single batch primitive BOTH the declarative-select and the lazy surface invoke —
 * proving the "same relation op" invariant structurally (there is no second path).
 *
 * Returns `{ sql, keys, batch }`: the rendered SQL (so a caller can assert both surfaces
 * produced the same text), the deduped keys bound, and the grouping. When there are no
 * non-null parent keys the query is NOT issued (an empty batch, matching v1) — this is not a
 * fallback default, it is the correct empty-set behavior (an IN over no keys selects nothing).
 */
export function runRelationOp(
  op: RelationOp,
  parents: readonly Record<string, unknown>[],
  db: RelationDriver,
): { sql: string; keys: unknown[]; batch: RelationBatch } {
  const keys = dedupeKeys(parents, op.parentKey);
  const batch: RelationBatch = new Map();
  if (keys.length === 0) {
    // No parent keys → no batched query issued, empty grouping. Still render the SQL (for
    // the same-op assertion) against the empty key set so the IN-list `1 = 0` degeneration
    // is observable, but do not touch the driver.
    const rendered = renderOperation(op.query, { [RELATION_KEYS_HEAD]: [] } as never);
    return { sql: rendered.sql, keys, batch };
  }
  const rendered: RenderedSql = renderOperation(op.query, { [RELATION_KEYS_HEAD]: keys } as never);
  const rows = db.prepare(rendered.sql).all(...rendered.params) as Record<string, unknown>[];
  for (const row of rows) {
    const k = String(row[op.targetKey]);
    const list = batch.get(k);
    if (list === undefined) batch.set(k, [row]);
    else list.push(row);
  }
  return { sql: rendered.sql, keys, batch };
}

/** The deduped, non-null parent-key values (insertion order preserved — deterministic). */
function dedupeKeys(parents: readonly Record<string, unknown>[], parentKey: string): unknown[] {
  const seen = new Set<string>();
  const out: unknown[] = [];
  for (const p of parents) {
    const v = p[parentKey];
    if (v === undefined || v === null) continue;
    const s = String(v);
    if (seen.has(s)) continue;
    seen.add(s);
    out.push(v);
  }
  return out;
}

/**
 * Distribute a resolved {@link RelationBatch} onto ONE parent per the relation cardinality:
 * `hasMany` → the child list (`[]` when none); `belongsTo`/`hasOne` → the single child (or
 * `null`). `null`/`undefined` is the correct absence value (an unresolved single relation),
 * NOT an ad-hoc default — it is the declared cardinality's empty representation.
 */
export function distributeToParent(
  op: RelationOp,
  parent: Record<string, unknown>,
  batch: RelationBatch,
): Record<string, unknown>[] | Record<string, unknown> | null {
  const key = parent[op.parentKey];
  const rows = key === undefined || key === null ? undefined : batch.get(String(key));
  if (op.kind === 'hasMany') return rows ?? [];
  return rows !== undefined && rows.length > 0 ? rows[0] : null;
}
