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
import { materializeCell, type MaterializeClass, type MaterializeResolver } from './coltype';
import {
  compileSingleKeyUnlimited,
  compileSingleKeyLimited,
  compileCompositeKeyStaticUnlimited,
  compileCompositeKeyStaticLimited,
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
  /** The PARENT column whose value is the batch key (single-key relations). */
  readonly parentKey?: string;
  /** The CHILD column matched against the parent key (single-key relations). */
  readonly targetKey?: string;
  /**
   * COMPOSITE-key relations (#47 item 1): the ORDERED parent columns whose tuple is the batch key.
   * Mutually exclusive with {@link parentKey}. Pairs positionally with {@link targetKeys}.
   */
  readonly parentKeys?: readonly string[];
  /** COMPOSITE-key relations: the ORDERED child columns matched against the parent-key tuple. */
  readonly targetKeys?: readonly string[];
  /** Optional per-parent ORDER BY body (dialect-neutral text). */
  readonly order?: string;
  /** Optional per-parent row limit (`hasMany` only). */
  readonly limit?: number;
  /** The target SQL dialect the batch SELECT is compiled for (default `'sqlite'`). */
  readonly dialect?: Dialect;
  /**
   * CROSS-DB relations (V0 R1): the NAME of the connection the batch SELECT must execute against —
   * the TARGET model's DB, which may differ from the parent's (v1 `LazyRelation.ts:236` runs a
   * relation on `TargetClass.getDriverType()`'s driver/connection). Absent ⇒ the parent's own
   * connection (the same-DB default). The SQL is v1-identical either way; the tag only ROUTES the
   * statement — a per-language runtime with a connection registry picks the pooled driver by name.
   */
  readonly connection?: string;
}

/**
 * A pre-compiled relation batch op (spec §8). Pure JSON — it carries the STATIC batch SELECT
 * `sql` (makeSQL text with ONE `?` for the deduped-key array param) plus the grouping metadata
 * the runtime needs to distribute child rows to parents. No functions, no reduced IR.
 */
export interface RelationOp {
  readonly name: string;
  readonly kind: RelationKind;
  /** Parent column supplying the batch key values (dedup key) — single-key relations. */
  readonly parentKey?: string;
  /** Child column the batch groups rows by (matches the parent key) — single-key relations. */
  readonly targetKey?: string;
  /**
   * COMPOSITE-key relations (#47 item 1): the ORDERED parent columns whose tuple is the dedup key.
   * Present iff the op is composite (mutually exclusive with {@link parentKey}).
   */
  readonly parentKeys?: readonly string[];
  /** COMPOSITE-key relations: the ORDERED child columns the batch groups rows by (the key tuple). */
  readonly targetKeys?: readonly string[];
  /** The target SQL dialect the batch SELECT text is compiled for. */
  readonly dialect: Dialect;
  /**
   * CROSS-DB relations (V0 R1): the connection NAME the batch executes against (the target model's
   * DB). Present ONLY when it differs from the parent's connection (a same-DB relation omits it).
   * A per-language runtime routes the statement to the pooled driver of this name; the SQL text and
   * `dialect`-driven placeholder/bind are already correct for the target (v1 `LazyRelation` parity).
   */
  readonly connection?: string;
  /**
   * The batched child SELECT as STATIC makeSQL text. Single-key: ONE `?` binds the deduped
   * parent-key set (PG `= ANY(?::t[])` / MySQL·SQLite single-JSON). Composite: PG binds ONE array
   * param PER key column (`unnest(?::t1[], ?::t2[])`); MySQL·SQLite bind ONE JSON array-of-tuples
   * param. Value-length-independent either way, so `sql` is fixed.
   */
  readonly sql: string;
  /**
   * The child (target) table + projected columns (issue #59) — carried for diagnostics + as the
   * basis of the baked `materializers` map. Additive/optional.
   */
  readonly targetTable?: string;
  readonly select?: readonly string[];
  /**
   * The child columns' STATIC materialize classes (issue #59): `column → MaterializeClass`, baked
   * at compile from the model's DDL resolver (only non-passthrough entries). The relation batch runs
   * these over its child rows so a BIGINT/DATE/BOOLEAN child de-boxes identically to the primary
   * read — with ZERO per-read DB introspection. Absent ⇒ the child rows stay raw (no schema).
   */
  readonly materializers?: Readonly<Record<string, MaterializeClass>>;
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
export function compileRelationOp(decl: RelationDecl, materializeResolver?: MaterializeResolver): RelationOp {
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
  const composite = isCompositeDecl(decl);
  const sql = compiledBatchSql(decl, dialect);
  // CROSS-DB (V0 R1): carry the target connection tag ONLY when set (a same-DB relation stays
  // untagged, so existing bundles are byte-unchanged — the field is additive/optional).
  const conn = decl.connection !== undefined ? { connection: decl.connection } : {};
  // Carry the child table + projection AND bake the STATIC child materializers (issue #59) from the
  // model's DDL resolver — the batch's child rows then de-box with ZERO per-read introspection.
  const materializers: Record<string, MaterializeClass> = {};
  if (materializeResolver !== undefined) {
    for (const c of decl.select) {
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(c)) continue;
      const klass = materializeResolver(decl.targetTable, c);
      if (klass !== undefined && klass !== 'passthrough') materializers[c] = klass;
    }
  }
  const target = {
    targetTable: decl.targetTable,
    select: [...decl.select],
    ...(Object.keys(materializers).length > 0 ? { materializers } : {}),
  };
  if (composite) {
    return {
      name: decl.name,
      kind: decl.kind,
      parentKeys: [...(decl.parentKeys as readonly string[])],
      targetKeys: [...(decl.targetKeys as readonly string[])],
      dialect,
      ...conn,
      sql,
      ...target,
    };
  }
  return {
    name: decl.name,
    kind: decl.kind,
    parentKey: decl.parentKey,
    targetKey: decl.targetKey,
    dialect,
    ...conn,
    sql,
    ...target,
  };
}

/**
 * A relation decl is COMPOSITE iff it carries `parentKeys`/`targetKeys` arrays. Validates the two
 * are present together, equal-length (paired positionally), and non-empty; and that the single-key
 * `parentKey`/`targetKey` are NOT also set (mutually exclusive). Single-key iff both arrays absent.
 */
function isCompositeDecl(decl: RelationDecl): boolean {
  const hasArrays = decl.parentKeys !== undefined || decl.targetKeys !== undefined;
  if (!hasArrays) {
    if (decl.parentKey === undefined || decl.targetKey === undefined) {
      throw new Error(`relation '${decl.name}': a single-key relation requires 'parentKey' and 'targetKey'`);
    }
    return false;
  }
  if (decl.parentKeys === undefined || decl.targetKeys === undefined) {
    throw new Error(`relation '${decl.name}': a composite-key relation requires BOTH 'parentKeys' and 'targetKeys'`);
  }
  if (decl.parentKeys.length === 0 || decl.parentKeys.length !== decl.targetKeys.length) {
    throw new Error(`relation '${decl.name}': 'parentKeys' and 'targetKeys' must be non-empty and equal-length (got ${decl.parentKeys.length} vs ${decl.targetKeys.length})`);
  }
  if (decl.parentKey !== undefined || decl.targetKey !== undefined) {
    throw new Error(`relation '${decl.name}': cannot mix single-key ('parentKey'/'targetKey') with composite ('parentKeys'/'targetKeys')`);
  }
  return true;
}

/**
 * Compile the STATIC batch SELECT text: the makeSQL relation builder emits complete tuned SQL
 * whose deduped-key array is ONE param. We compile against a single placeholder key array so
 * the text is fixed; the runtime re-binds the real deduped keys against the SAME text (the
 * single-JSON / `= ANY` forms are length-independent, so the text is stable).
 */
function compiledBatchSql(decl: RelationDecl, dialect: Dialect): string {
  // A COMPOSITE decl compiles to the STATIC composite form (PG: one array param per key column;
  // MySQL/SQLite: one JSON array-of-tuples param) — length-independent, so the text is fixed. A
  // per-parent `limit` selects the STATIC composite-LIMITED builder (PG LATERAL / MySQL·SQLite
  // ROW_NUMBER window over the SAME static key-set predicate — #47 last completeness gap).
  if (decl.parentKeys !== undefined) {
    const compositeBase = {
      dialect,
      tableName: decl.targetTable,
      select: decl.select.join(', '),
      order: decl.order,
      targetKeys: [...(decl.targetKeys as readonly string[])],
      deferPgArrayCast: true,
    };
    const node =
      decl.limit !== undefined
        ? compileCompositeKeyStaticLimited({ ...compositeBase, limit: decl.limit })
        : compileCompositeKeyStaticUnlimited(compositeBase);
    return assembleMakeSQL(node).sql;
  }
  // A one-element placeholder key set fixes the SQL text (single-JSON-param / `= ANY` forms are
  // value-length-independent). The concrete keys are bound at execute time.
  const placeholderKeys: unknown[] = [null];
  const base = {
    dialect,
    tableName: decl.targetTable,
    select: decl.select.join(', '),
    order: decl.order,
    targetKey: decl.targetKey as string,
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

/** The ordered PARENT key columns of an op (single-key → 1-element list; composite → the tuple). */
function parentKeyCols(op: RelationOp): readonly string[] {
  return op.parentKeys ?? [op.parentKey as string];
}

/** The ordered CHILD key columns of an op (single-key → 1-element list; composite → the tuple). */
function targetKeyCols(op: RelationOp): readonly string[] {
  return op.targetKeys ?? [op.targetKey as string];
}

/** The stringified key identity for dedupe/grouping. Single scalar → `String(v)`; tuple → joined. */
function keyIdentity(values: readonly unknown[]): string {
  // A NUL separator no scalar `String(v)` contains, so distinct tuples never collide.
  return values.map((v) => String(v)).join(' ');
}

/**
 * Bind the deduped keys to the batch op's params per dialect + arity. Single-key: PG binds the
 * scalar array verbatim (`= ANY(?::t[])`); MySQL/SQLite bind the JSON-encoded array. Composite: PG
 * binds ONE array param PER key column (transposed tuples → `unnest(?::t1[], ?::t2[])`);
 * MySQL/SQLite bind ONE JSON array-of-tuples string. Returns the positional param list.
 */
function bindKeys(op: RelationOp, tuples: readonly unknown[][]): unknown[] {
  const composite = op.parentKeys !== undefined;
  if (op.dialect === 'postgres') {
    if (!composite) return [tuples.map((t) => t[0])]; // ONE scalar array param
    // Transpose tuples → per-column arrays: `[[1,a],[2,b]] → [[1,2],[a,b]]` — one array param each.
    return parentKeyCols(op).map((_, col) => tuples.map((t) => t[col]));
  }
  // MySQL/SQLite: single-key → JSON scalar array; composite → JSON array-of-tuples. ONE param.
  const payload = composite ? tuples.map((t) => [...t]) : tuples.map((t) => t[0]);
  return [JSON.stringify(payload)];
}

/**
 * Run ONE {@link RelationOp} for a set of parent rows: dedup the parent-key tuples, render the
 * STATIC batch SELECT once (dialect placeholder form) resolving the deferred PG array cast(s) from
 * the REAL keys, execute it with the keys bound (single array / per-column arrays / JSON tuples),
 * then group the child rows by their target-key identity. The single batch primitive BOTH the
 * declarative-select and the lazy surface invoke, single-key AND composite.
 *
 * Returns `{ sql, keys, batch }` (`keys` = the deduped parent-key tuples). An empty key set issues
 * NO query (the correct empty-set behavior — the membership over no keys selects nothing).
 */
export function runRelationOp(
  op: RelationOp,
  parents: readonly Record<string, unknown>[],
  db: RelationDriver,
): { sql: string; keys: unknown[][]; batch: RelationBatch } {
  const pCols = parentKeyCols(op);
  const keys = dedupeKeys(parents, pCols);
  const batch: RelationBatch = new Map();
  // Resolve the deferred PG array cast(s) (#46) from the REAL keys BEFORE the `?`→`$N` render. A
  // single-key op has ONE cast; a composite PG op has ONE cast PER key column — resolve each from
  // its own column's key values, left-to-right (resolvePgArrayCast resolves the first token each
  // call). MySQL/SQLite carry no cast token.
  let cast = op.sql;
  if (op.dialect === 'postgres') {
    for (let col = 0; col < pCols.length; col++) cast = resolvePgArrayCast(cast, keys.map((t) => t[col]));
  }
  const sql = renderPlaceholders(cast, op.dialect);
  if (keys.length === 0) return { sql, keys, batch };
  const tCols = targetKeyCols(op);
  // Materialize the child rows (issue #59) exactly like the primary read, using the STATIC
  // materializers baked onto the op at compile (from the model's DDL — ZERO per-read introspection).
  // Enable safeIntegers when the child projects a BIGINT so int8 arrives exact. A BIGINT/DATE/BOOL
  // child column de-boxes identically to a top-level read (INT→number / BIGINT→string / DATE→string
  // / bool). SQLite-shaped driver; the async PG/MySQL relation path materializes at its own seam.
  const childCols = op.materializers;
  const int64Child = childCols !== undefined && Object.values(childCols).includes('int64');
  const stmt = db.prepare(sql);
  if (int64Child && typeof (stmt as { safeIntegers?: unknown }).safeIntegers === 'function') {
    (stmt as unknown as { safeIntegers(v: boolean): unknown }).safeIntegers(true);
  }
  const rawRows = stmt.all(...bindKeys(op, keys)) as Record<string, unknown>[];
  const rows = materializeChildRows(rawRows, childCols, int64Child);
  for (const row of rows) {
    const k = keyIdentity(tCols.map((c) => row[c]));
    const list = batch.get(k);
    if (list === undefined) batch.set(k, [row]);
    else list.push(row);
  }
  return { sql, keys, batch };
}

/** Materialize relation child rows (issue #59): same coercion as the primary read. */
function materializeChildRows(
  rows: Record<string, unknown>[],
  cols: Record<string, MaterializeClass> | undefined,
  safeIntegersOn: boolean,
): Record<string, unknown>[] {
  if (cols === undefined && !safeIntegersOn) return rows;
  for (const row of rows) {
    for (const key of Object.keys(row)) {
      const klass = cols?.[key];
      if (klass !== undefined) { row[key] = materializeCell(row[key], klass); continue; }
      // Defensive: under safeIntegers, an untyped int child column arrives as bigint — narrow it.
      if (safeIntegersOn && typeof row[key] === 'bigint') {
        const v = row[key] as bigint;
        row[key] = v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : v.toString();
      }
    }
  }
  return rows;
}

/**
 * The deduped, non-null parent-key TUPLES (insertion order preserved — deterministic). A tuple is
 * dropped if ANY of its key columns is null/undefined (no partial keys). Deduped on the stringified
 * tuple identity (so `1` and `"1"` collapse exactly as `String(v)`).
 */
function dedupeKeys(parents: readonly Record<string, unknown>[], keyCols: readonly string[]): unknown[][] {
  const seen = new Set<string>();
  const out: unknown[][] = [];
  for (const p of parents) {
    const tuple = keyCols.map((c) => p[c]);
    if (tuple.some((v) => v === undefined || v === null)) continue;
    const s = keyIdentity(tuple);
    if (seen.has(s)) continue;
    seen.add(s);
    out.push(tuple);
  }
  return out;
}

/**
 * Distribute a resolved {@link RelationBatch} onto ONE parent per the relation cardinality:
 * `hasMany` → the child list (`[]` when none); `belongsTo`/`hasOne` → the single child (or
 * `null`). Keyed by the parent's key-tuple identity. `null`/`[]` is the declared cardinality's
 * empty representation, not an ad-hoc default.
 */
export function distributeToParent(
  op: RelationOp,
  parent: Record<string, unknown>,
  batch: RelationBatch,
): Record<string, unknown>[] | Record<string, unknown> | null {
  const tuple = parentKeyCols(op).map((c) => parent[c]);
  const rows = tuple.some((v) => v === undefined || v === null) ? undefined : batch.get(keyIdentity(tuple));
  if (op.kind === 'hasMany') return rows ?? [];
  return rows !== undefined && rows.length > 0 ? rows[0] : null;
}
