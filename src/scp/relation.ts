/**
 * litedbmodel v2 SCP — Read relations: pre-compiled batch op + staged batch resolution (WS4,
 * #24; makeSQL re-expression, epic #43/#45 Phase B).
 *
 * Relations are NOT SQL JOINs by default (spec §5). They are the staged-batch
 * query-composition + object-assembly shape v1's `LazyRelation` uses: collect the parent key
 * set of a result page, run ONE batched child SELECT keyed by the deduped parent keys, then
 * distribute the child rows back to their parents. One batched query per relation edge, NEVER
 * one-per-parent (no N+1).
 *
 * ## The relation op is a STATIC makeSQL artifact (design #45)
 *
 * A {@link RelationOp} is compiled ONCE from a model {@link RelationDecl} into a STATIC
 * `makeSQL` batch SELECT via the makeSQL relation builders (`./makesql/compile-relation`) —
 * byte-identical to the ORIGINAL `LazyRelation` SQL for PostgreSQL (`= ANY(?::type[])`,
 * `CROSS JOIN LATERAL`, `UNNEST`), and the single-JSON-param server-side form for MySQL/SQLite
 * (`json_each` / `JSON_TABLE`). The deduped key array binds as ONE param with STATIC text (no
 * placeholder-count expansion), so `op.sql` is fixed and value-independent — pure JSON, lands
 * in the bundle, a per-language runtime executes it directly. The REDUCED
 * `CompiledOperation`/`renderOperation`/`IN (?)`-expansion forms are GONE.
 *
 * ## ONE relation op, TWO surfaces (spec §5)
 *
 * Both read surfaces trigger the IDENTICAL compiled op via {@link runRelationOp}:
 *   - **Declarative select** (`with: { author: true }`): batch-prefetched over the page.
 *   - **Lazy** (`await post.author`): a prototype getter fires the SAME op over the sibling set.
 */

import { assembleMakeSQL, type MakeSQL } from './makesql/makesql';
import { renderPlaceholders, type Dialect } from './makesql/handler';
import {
  compileSingleKeyUnlimited,
  compileSingleKeyLimited,
  resolvePgArrayCast,
} from './makesql/compile-relation';

/** A read relation cardinality (v1 parity). `belongsTo`/`hasOne` are single; `hasMany` many. */
export type RelationKind = 'belongsTo' | 'hasMany' | 'hasOne';

/**
 * A model's read-relation declaration (the authored/decorated relation metadata, spec §4).
 * Mirrors v1's `@belongsTo`/`@hasMany`/`@hasOne` config (source/target key + optional
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
  /** The PARENT column whose value is the batch key. */
  readonly parentKey: string;
  /** The CHILD column matched against the parent key. */
  readonly targetKey: string;
  /** Optional per-parent ORDER BY body (dialect-neutral text). */
  readonly order?: string;
  /** Optional per-parent row limit (`hasMany` only). */
  readonly limit?: number;
  /** The target SQL dialect the batch SELECT is compiled for (default `'sqlite'`). */
  readonly dialect?: Dialect;
}

/**
 * A pre-compiled relation batch op (spec §8). Pure JSON — it carries the STATIC batch SELECT
 * `sql` (makeSQL text with ONE `?` for the deduped-key array param) plus the grouping metadata
 * the runtime needs to distribute child rows to parents. No functions, no reduced IR.
 */
export interface RelationOp {
  readonly name: string;
  readonly kind: RelationKind;
  /** Parent column supplying the batch key values (dedup key). */
  readonly parentKey: string;
  /** Child column the batch groups rows by (matches the parent key). */
  readonly targetKey: string;
  /** The target SQL dialect the batch SELECT text is compiled for. */
  readonly dialect: Dialect;
  /**
   * The batched child SELECT as STATIC makeSQL text: ONE `?` binds the deduped parent-key set
   * (PG `= ANY(?::t[])` / MySQL·SQLite single-JSON `json_each`/`JSON_TABLE`). Value-independent.
   */
  readonly sql: string;
}

/** The reserved input head the relation batch query binds its deduped key array to. */
export const RELATION_KEYS_HEAD = '__keys';

/**
 * Compile ONE {@link RelationDecl} into a STATIC {@link RelationOp} via the makeSQL relation
 * builders. The batch query selects the child rows whose {@link RelationDecl.targetKey} is in
 * the deduped parent-key set. PG stays byte-identical to v1's `LazyRelation`; MySQL/SQLite use
 * the single-JSON-param server-side form. A `hasMany` with `limit` compiles to the per-parent
 * LATERAL (PG) / ROW_NUMBER (MySQL·SQLite) form. The SQL text is FIXED (the array binds as one
 * param regardless of length), so it needs no per-input recompile.
 */
export function compileRelationOp(decl: RelationDecl): RelationOp {
  if (decl.kind !== 'hasMany' && decl.limit !== undefined) {
    throw new Error(`relation '${decl.name}': a per-parent 'limit' is only valid for hasMany (got ${decl.kind})`);
  }
  if (decl.limit !== undefined && (!Number.isInteger(decl.limit) || decl.limit < 0)) {
    throw new Error(`relation '${decl.name}': per-parent limit must be a non-negative integer (got ${String(decl.limit)})`);
  }
  if (decl.limit !== undefined && decl.order === undefined) {
    throw new Error(
      `relation '${decl.name}': a per-parent 'limit' requires an explicit 'order' (the per-parent window needs a deterministic ordering to decide which ${decl.limit} rows each parent keeps)`,
    );
  }
  const dialect: Dialect = decl.dialect ?? 'sqlite';
  const sql = compiledBatchSql(decl, dialect);
  return {
    name: decl.name,
    kind: decl.kind,
    parentKey: decl.parentKey,
    targetKey: decl.targetKey,
    dialect,
    sql,
  };
}

/**
 * Compile the STATIC batch SELECT text: the makeSQL relation builder emits complete tuned SQL
 * whose deduped-key array is ONE param. We compile against a single placeholder key array so
 * the text is fixed; the runtime re-binds the real deduped keys against the SAME text (the
 * single-JSON / `= ANY` forms are length-independent, so the text is stable).
 */
function compiledBatchSql(decl: RelationDecl, dialect: Dialect): string {
  // A one-element placeholder key set fixes the SQL text (single-JSON-param / `= ANY` forms are
  // value-length-independent). The concrete keys are bound at execute time.
  const placeholderKeys: unknown[] = [null];
  const base = {
    dialect,
    tableName: decl.targetTable,
    select: decl.select.join(', '),
    order: decl.order,
    targetKey: decl.targetKey,
    values: placeholderKeys,
    // The keys are UNKNOWN at symbolic compile (placeholder set), so the PG `= ANY(?::<T>[])`
    // element type is DEFERRED to render (#46) — resolved from the real deduped keys via
    // `inferPgArrayType`, reproducing v1's live-correct cast (`::int[]` for int keys). Baking a
    // compile-time `text[]` here was the #43 regression (`integer = text` on real PG).
    deferPgArrayCast: true,
  };
  const node: MakeSQL =
    decl.limit !== undefined
      ? compileSingleKeyLimited({ ...base, limit: decl.limit })
      : compileSingleKeyUnlimited(base);
  // The builder emits the SQL with ONE `?` (the whole key array). Assemble to the flat text.
  return assembleMakeSQL(node).sql;
}

// ── Batch execution (the SINGLE code path both read surfaces share) ────────────

/** A minimal read-only driver surface (`prepare(sql).all(...params)`), the SQLite `Database`. */
export interface RelationDriver {
  prepare(sql: string): { all(...params: unknown[]): unknown[] };
}

/** The child rows grouped for a batch: parent-key value (stringified) → child rows. */
export type RelationBatch = Map<string, Record<string, unknown>[]>;

/**
 * Bind the deduped key set to the batch op's single array param per dialect: PG binds the array
 * verbatim (`= ANY(?::t[])`); MySQL/SQLite bind the JSON-encoded array string (server-side
 * `json_each`/`JSON_TABLE` expansion). This is the ONE param the static batch `sql` expects.
 */
function bindKeys(op: RelationOp, keys: unknown[]): unknown {
  if (op.dialect === 'postgres') return keys;
  return JSON.stringify(keys);
}

/**
 * Run ONE {@link RelationOp} for a set of parent rows: dedup the parent keys, render the STATIC
 * batch SELECT once (dialect placeholder form) and execute it with the deduped keys bound as the
 * SINGLE array param, then group the child rows by their target key. The single batch primitive
 * BOTH the declarative-select and the lazy surface invoke.
 *
 * Returns `{ sql, keys, batch }`. An empty key set issues NO query (the correct empty-set
 * behavior — the `json_each`/`= ANY` over no keys selects nothing), matching v1.
 */
export function runRelationOp(
  op: RelationOp,
  parents: readonly Record<string, unknown>[],
  db: RelationDriver,
): { sql: string; keys: unknown[]; batch: RelationBatch } {
  const keys = dedupeKeys(parents, op.parentKey);
  const batch: RelationBatch = new Map();
  // Resolve the deferred PG array cast (#46) from the REAL deduped keys BEFORE the `?`→`$N`
  // render (both are render-layer dialect steps; the cast token carries no `?`). PG only — the
  // MySQL/SQLite forms bind the JSON array param and carry no cast token.
  const cast = op.dialect === 'postgres' ? resolvePgArrayCast(op.sql, keys) : op.sql;
  const sql = renderPlaceholders(cast, op.dialect);
  if (keys.length === 0) return { sql, keys, batch };
  const rows = db.prepare(sql).all(bindKeys(op, keys)) as Record<string, unknown>[];
  for (const row of rows) {
    const k = String(row[op.targetKey]);
    const list = batch.get(k);
    if (list === undefined) batch.set(k, [row]);
    else list.push(row);
  }
  return { sql, keys, batch };
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
 * `null`). `null`/`[]` is the declared cardinality's empty representation, not an ad-hoc default.
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
