/**
 * litedbmodel v2 SCP — the decorator → SCP authoring ADAPTER (Phase F-1, epic #74, issue #104).
 *
 * v2's ActiveRecord surface (the README API: `@model` / `@column` / `@hasMany` + `find` / `create` /
 * `transaction`) currently lowers to an IMPERATIVE SQL-string path in `src/DBModel.ts` (zero
 * `src/scp/` imports). Phase F re-points that surface onto the v2 SCP stack. THIS module (F1) is the
 * standalone, unit-proven layer that translates the decorator METADATA into the SCP authoring it
 * lowers to — WITHOUT yet rewiring DBModel's public methods (that is F2). It is purely ADDITIVE.
 *
 * ## The pattern (mirrors graphddb `decorators/` collector → `define/` adapter → compile)
 *
 * litedbmodel's decorators are already metadata-only COLLECTORS: `@column` / `@hasMany` / … record
 * `ColumnMeta` / `RelationMeta` / `TABLE_NAME` on the class (`src/decorators.ts`, via `reflect-metadata`
 * + `_columnMeta` / `_relationMeta`). This module is the DEFINE/adapter layer graphddb's `define/`
 * is: it READS that registry off a decorated model class and calls the shared SCP authoring
 * (`compileEager` / `publishBehaviors`) and compile (`compileBundle` / `compileWriteBundle` / …). The
 * decorator NEVER emits IR; the adapter never re-implements composition — bc + the makeSQL compile own
 * both. So every op × every dialect the SCP stack already executes is reachable straight from the
 * decorator surface (litedbmodel: "declare-via-BC — no direct IR").
 *
 * ## What the adapter translates
 *
 *  1. **Columns** (the one real translation): `@column.*` `ColumnMeta` (the decorator already knows the
 *     type family — bigint / date / boolean / uuid / json / array / …) → the SCP `static columns`
 *     ({@link ModelColumns}: the SQL-type token per column, validated by `coltype.ts`). See
 *     {@link deriveModelColumns} + {@link COLUMN_FAMILY_SQL_TYPE}.
 *  2. **Reads**: `find` / `findOne` / `findById` / `count` → an eager `fn($, L)` producing
 *     `L.Select` / `L.Count`, compiled through `compileEager`. Conditions ride the EXISTING SCP where
 *     sugar (`whereEq` / `when`+`SKIP` / `inSubquery` / `queryView` / …) — the adapter's caller passes
 *     a where-builder, so no v1 `Conds` re-parse lives here.
 *  3. **Writes**: `create` / `createMany` / `update` / `updateMany` / `delete` → `compileWriteBundle` /
 *     `compileCreateManyBundle` / `compileUpdateManyBundle` / `compileDeleteManyBundle`. `onConflict`
 *     (upsert) rides the createMany path (v1 `create` and `createMany` share ONE `_insert` grouping
 *     path — see `DBModel._insert`), so it carries end-to-end with NO new SCP authoring port.
 *  4. **Relations**: `@hasMany` / `@belongsTo` / `@hasOne` `RelationMeta` → {@link RelationDecl} →
 *     `compileRelationOp` (single + composite keys, per-parent `limit` window + `hardLimit`).
 *
 * ## Byte-identity (the F1 acceptance)
 *
 * `authoring.ts` guarantees the eager path and an equivalent declaration method produce byte-identical
 * component IR (spec §9). This adapter builds the eager fn from metadata; so an adapter-generated
 * bundle is byte-identical to the hand-written SCP behavior for the same model (proven per README shape
 * in `test/scp/decorator-adapter.test.ts`). That proves the decorator surface lowers to the SAME SCP
 * the native runtimes already execute — the whole point of Phase F.
 */

import {
  compileEager,
  type BehaviorModelContract,
  type ComponentFns,
  type EagerBehavior,
  type ModelColumns,
} from './authoring';
import {
  compileBundle,
  compileWriteBundle,
  compileCreateManyBundle,
  compileUpdateManyBundle,
  compileDeleteManyBundle,
  type SqlBundle,
} from './runtime';
import { compileRelationOp, parentKeyCols, targetKeyCols, type RelationDecl, type RelationKind, type RelationOp } from './relation';
import { sqlTypeToMaterializeClass, keyArrayElemScalar, columnTypeResolverFromColumnMap, type ColumnTypeResolver } from './coltype';
import type { DialectName } from './dialect';
import { compileSelectNode, type StaticStatement } from './makesql/static-bundle';
import { compileWriteNode } from './makesql/tx';

// ── #141 op→leaf-graph lowering (read path) ────────────────────────────────────
//
// The op builders lower a SELECT/COUNT to the op-independent `executeSQL` leaf by running the SAME
// `compileSelectNode` (the SSoT op→sql+params lowering) at AUTHORING time over a synthetic catalog
// node, then assembling its statements. A BOUNDED op (no SKIP fragment) assembles to a LITERAL sql
// string (native-lowerable — the bench declared ops); a dynamic-WHERE/SKIP op is a ts-runtime-only
// concern that assembles per-input (not yet wired to the structured `executeSQL` path — see the
// throw below; bounded is this phase's scope).

/**
 * Assemble a BOUNDED read (no SKIP fragment) into ONE literal `{ sql, params }`: concatenate the head
 * + WHERE fragments (` WHERE `/` AND ` connectors resolved deterministically since all are present) +
 * tail. `params` are the deferred value-specs (bc `$`-ref Expression IR), 1:1 with the `?`.
 */
function assembleBoundedRead(statements: readonly StaticStatement[]): { sql: string; params: unknown[] } {
  let sql = '';
  const params: unknown[] = [];
  let whereCount = 0;
  for (const st of statements) {
    if (st.skip !== undefined) {
      throw new Error(
        'scp read (#141): a dynamic-WHERE/SKIP read is not yet wired to the structured `executeSQL` runtime-assemble path ' +
          '(bounded ops only in this phase). The SKIP fragment must ride a ts-runtime structured `executeSQL` input.',
      );
    }
    sql += st.whereFragment === true ? (whereCount++ === 0 ? ` WHERE ${st.sql}` : ` AND ${st.sql}`) : st.sql;
    params.push(...st.params);
  }
  return { sql, params };
}

/**
 * Wrap the raw authoring ports into the IR-form `compileSelectNode` reads — the SAME shape bc's
 * recording produces for a hand-authored `L.Select({…})`, so `emitRead` (which bypasses bc recording
 * for the base ports and drives `compileSelectNode` directly) feeds it an equivalent node. Array ports
 * (`select`/`where`/`cteParams`/`joinParams`) become `{arr:[…]}`; the `cte` map port becomes
 * `{obj:{name,sql}}`; string/expr ports (`table`/`join`/`order`/`group`/`limit`/`offset`) pass through.
 * The `where`/`cteParams`/`joinParams` MEMBERS are bc expression nodes (`eq(col,val)` from the
 * where-sugar / `$`-ref value-specs) — the SAME nodes `compileSelectNode`/`lowerWherePort` decode.
 */
function toIrPorts(ports: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = { ...ports };
  if (Array.isArray(out.select)) out.select = { arr: out.select };
  if (Array.isArray(out.where)) out.where = { arr: out.where };
  if (Array.isArray(out.cteParams)) out.cteParams = { arr: out.cteParams };
  if (Array.isArray(out.joinParams)) out.joinParams = { arr: out.joinParams };
  if (out.cte !== null && typeof out.cte === 'object' && !Array.isArray(out.cte) && !('obj' in (out.cte as object))) {
    out.cte = { obj: out.cte };
  }
  return out;
}

/**
 * Emit an `executeSQL` read node from a synthetic Select/Count catalog node. The base SQL (head +
 * ORDER/LIMIT tail, NO WHERE) is lowered at authoring via {@link compileSelectNode} (literal sql —
 * native-lowerable). The WHERE is passed as the transient `where` PORT: bc RECORDS the where sugar
 * (live `$`-ref/`whereRawPredicate`/sentinel proxies → plain Expression IR), and the post-compile
 * pass in `authoring.ts` ({@link import('./authoring').lowerRecordedWhere}) lowers the RECORDED IR
 * into the node's static `sql`/`params` — never walking a live proxy (the `NOT_RECORDABLE` cause).
 */
export function emitRead(L: ComponentFns, component: 'Select' | 'Count', ports: Record<string, unknown>, dialect: DialectName): Recorded {
  const where = ports.where;
  const base: Record<string, unknown> = { ...ports };
  delete base.where;
  // Inline a LITERAL LIMIT/OFFSET into the SQL text instead of deferring it as a `?` param (v1 inlined
  // them too). This keeps the executeSQL `params` a UNIFORM `value[]` of value-specs — the native single
  // transport requires every param element to spread as `&[WireValue]` (bc#156/#160); a mixed
  // value-ref + i64-literal params array is not natively coverable. A value-ref (dynamic) limit stays a
  // deferred param (it spreads as a WireValue like any bound value).
  const litLimit = typeof base.limit === 'number' ? base.limit : undefined;
  const litOffset = typeof base.offset === 'number' ? base.offset : undefined;
  if (litLimit !== undefined) delete base.limit;
  if (litOffset !== undefined) delete base.offset;
  const assembled = assembleBoundedRead(compileSelectNode({ id: 'r', component, ports: toIrPorts(base) } as never, dialect));
  const params = assembled.params;
  const sql = assembled.sql + (litLimit !== undefined ? ` LIMIT ${litLimit}` : '') + (litOffset !== undefined ? ` OFFSET ${litOffset}` : '');
  // `params` is the opaque value list (bc#156); the leaf's `params` port element type is a nominal
  // placeholder the ir-exec runtime does not enforce — the value-specs flow through as bc `$`-refs.
  const portObj: Record<string, unknown> = { sql, params, write: false, returning: false, bigint: false };
  if (Array.isArray(where) && where.length > 0) portObj.where = where; // bc records → plain IR; lowered post-compile
  // #59 read de-box: carry the base table + explicit projection as the transient `readColumns` port so
  // the post-compile pass (`authoring.lowerReadColumns`, which holds the model's column resolver) can
  // resolve each column, fail-close on `*`/undeclared (the coverage guard), and stamp the de-box
  // `materializers` map. A `Select` projects `select` (default `['*']` → the guard rejects at lowering);
  // a `Count` returns a scalar `int` (no per-column de-box), so it carries no `readColumns`.
  if (component === 'Select' && typeof base.table === 'string' && Array.isArray(base.select)) {
    portObj.readColumns = { table: base.table, cols: base.select };
  }
  return (L.executeSQL as (p: Record<string, unknown>) => Recorded)(portObj);
}

/**
 * Emit an `executeSQL` write node (write intent) from a synthetic Insert/Update/Delete catalog node.
 * The op→sql+params lowering is the SSoT {@link compileWriteNode} (complete tuned SQL incl. the
 * RETURNING tail + deferred `$`-ref params). `returning` is set from the authored `returning` port so
 * the leaf runs a RETURNING write through the row-returning seam (`execute`) and a plain write through
 * `run` (the affected summary) — RETURNING is preserved, never gutted.
 */
export function emitWrite(L: ComponentFns, component: 'Insert' | 'Update' | 'Delete', ports: Record<string, unknown>, dialect: DialectName): Recorded {
  // The WHERE (Update/Delete) is DEFERRED to the transient `where` port and lowered post-compile from
  // the RECORDED IR (the SAME path as reads — `lowerRecordedWhere` in authoring.ts) rather than walked
  // as a live proxy at authoring (the NOT_RECORDABLE cause). `compileWriteNode` emits the base
  // INSERT/UPDATE SET/DELETE (no WHERE) from the where-less ports; `lowerRecordedWhere` appends it.
  const where = ports.where;
  if ((component === 'Update' || component === 'Delete') && !(Array.isArray(where) && where.length > 0)) {
    throw new Error(`decorator-adapter: ${component} requires a WHERE (no unfiltered ${component}). Provide the read/where spec.`);
  }
  const base: Record<string, unknown> = { ...ports };
  delete base.where;
  const op = compileWriteNode({ component, ports: toIrPorts(base) } as never, dialect);
  const returning = ports.returning !== undefined;
  const portObj: Record<string, unknown> = { sql: op.sql, params: [...op.params], write: true, returning, bigint: false };
  if (Array.isArray(where) && where.length > 0) portObj.where = where; // bc records → plain IR; appended post-compile
  return (L.executeSQL as (p: Record<string, unknown>) => Recorded)(portObj);
}

/**
 * The batch-write authoring spec (createMany / upsertMany / updateMany). `rows` is the ONE opaque
 * input value (bc#156) the consumer binds to the whole record set — an `{arr:'value'}` array of row
 * objects the transport json-encodes (`json_each(?)` / `JSON_TABLE(?)`). `columns` are the write
 * columns (INSERT `values.*` / UPDATE `set.*`); `keyColumns` the UPDATE match keys.
 */
export interface BatchWriteAuthoringSpec {
  readonly table: string;
  /** INSERT value columns / UPDATE set columns. */
  readonly columns: readonly string[];
  /** UPDATE match-key columns (`updateMany` — omitted for INSERT). */
  readonly keyColumns?: readonly string[];
  /** The ONE opaque rows input (a `$`-ref to the record-set array value the consumer binds). */
  readonly rows: Recorded;
  /** Conflict-target column list (upsertMany) — comma-joined, as the single-write `onConflict` port. */
  readonly onConflict?: string;
  /** Conflict action (upsertMany) — `'update'` (default) or `'ignore'`. */
  readonly onConflictAction?: 'update' | 'ignore';
}

/**
 * Emit an `executeSQL` batch-write node (createMany / upsertMany / updateMany) as ONE statement whose
 * `?`(s) all bind the ONE opaque `rows` array value (bc#156). The static batch SQL is the SSoT
 * {@link compileWriteNode} json-batch form (`json_each`/`JSON_TABLE`) — its text is record-count
 * INDEPENDENT (it expands the JSON server-side), so the native path lowers it to ONE `execute_sql`
 * with `params = [rows, …]` (nQ copies of the SAME opaque input — the json-batch SSoT binds every `?`
 * to the same records-JSON). NO `__batchRows` marker, NO per-column parallel arrays: the marker/refs
 * are the TS ir-exec build shape, replaced HERE by the single opaque `rows` value the transport
 * json-encodes. Native-clean: ONE statement for N records (safety: 1 query).
 */
export function emitBatchWrite(L: ComponentFns, component: 'Insert' | 'Update', spec: BatchWriteAuthoringSpec, dialect: DialectName): Recorded {
  // Derive the static batch SQL + the `?` count from the SSoT compile. Only the COLUMN NAMES shape the
  // SQL text; the parallel-array value refs feed the ir-exec `__batchRows` marker, which the native
  // path discards — so bind placeholder `0` refs to enumerate columns, then replace the params below.
  const base: Record<string, unknown> = { table: spec.table, batch: 'true' };
  if (component === 'Insert') {
    for (const c of spec.columns) base[`values.${c}`] = 0;
    if (spec.onConflict !== undefined) base.onConflict = spec.onConflict;
    if (spec.onConflictAction !== undefined) base.onConflictAction = spec.onConflictAction;
  } else {
    for (const c of spec.keyColumns ?? []) base[`key.${c}`] = 0;
    for (const c of spec.columns) base[`set.${c}`] = 0;
  }
  const op = compileWriteNode({ component, ports: toIrPorts(base) } as never, dialect);
  // The json-batch SSoT binds the SAME records-JSON to every `?` (updateMany: one per SET sub-select +
  // the WHERE). `op.params.length` is that `?` count — bind the ONE opaque `rows` value that many times.
  const params = Array.from({ length: op.params.length }, () => spec.rows);
  // Batch writes are non-RETURNING (v1: createMany/upsertMany/updateMany return null — see ops.ts).
  return (L.executeSQL as (p: Record<string, unknown>) => Recorded)({ sql: op.sql, params, write: true, returning: false, bigint: false });
}
import type { InsertManyBuildOptions } from './makesql/compile-crud';
import type { UpdateManyBuildOptions } from '../drivers/types';
import type { EntityWritesDefinition, WriteLifecyclePhase } from './writes';
import { getColumnMeta, getRelationMeta, type ColumnMeta, type RelationMeta } from '../decorators';
import { orderToString, type OrderSpec } from '../Column';
import type { Recorded } from 'behavior-contracts';

// ── 1. Column-type mapping (decorator ColumnMeta → SCP `static columns` SQL type) ──────────────

/**
 * The decorator's `ColumnMeta.sqlCast` type-family token → the canonical §4.1 SQL-type token the SCP
 * `static columns` SoT carries (validated by `coltype.ts`). The decorator ALREADY knows the family
 * (it set `sqlCast` from the `@column.*` variant / the `design:type` inference — `src/decorators.ts`),
 * so this is a pure token normalization, no inference. `coltype.ts` accepts every RHS here:
 * BOOLEAN/BIGINT/TIMESTAMP/DATE/UUID/JSONB are scalar; the array tokens de-box as `passthrough`
 * (`sqlTypeToMaterializeClass`).
 *
 * The RHS matches the decorator families 1:1:
 *  - `boolean`    → `BOOLEAN`     (`@column.boolean()` / auto `Boolean`)
 *  - `bigint`     → `BIGINT`      (`@column.bigint()` / auto `BigInt` — read de-box: exact string)
 *  - `timestamp`  → `TIMESTAMP`   (`@column.datetime()` / auto `Date` — read de-box: TZ string)
 *  - `date`       → `DATE`        (`@column.date()` — read de-box: YYYY-MM-DD string)
 *  - `uuid`       → `UUID`        (`@column.uuid()`)
 *  - `jsonb`      → `JSONB`       (`@column.json()`)
 *  - `text[]` / `int[]` / `numeric[]` / `boolean[]` → the array token (`@column.*Array()`)
 */
export const COLUMN_FAMILY_SQL_TYPE: Readonly<Record<string, string>> = {
  boolean: 'BOOLEAN',
  bigint: 'BIGINT',
  timestamp: 'TIMESTAMP',
  date: 'DATE',
  uuid: 'UUID',
  jsonb: 'JSONB',
  'text[]': 'TEXT[]',
  'int[]': 'INT[]',
  'numeric[]': 'NUMERIC[]',
  'boolean[]': 'BOOLEAN[]',
};

/**
 * The LAST-RESORT SQL type for a column that carries NO `sqlCast` family, NO `baseSqlType` (its
 * `design:type` is absent — no `emitDecoratorMetadata` — or is an Array/Object family), and NO
 * `columnTypes` override. Phase F-2 (#105 option B) made {@link columnSqlType} consult the decorator's
 * `baseSqlType` (derived from the field's TS `design:type`: String→TEXT / Number→INTEGER / Boolean→
 * BOOLEAN / Date→TIMESTAMP / BigInt→BIGINT) BEFORE this default, so a bare `@column() name: string`
 * types as `TEXT` (not `INTEGER`) — the fix for F1's blanket-INTEGER read-de-box defect (it threw
 * `materialize int32` on a live string column). This default now only applies when `design:type` is
 * genuinely unavailable; a REAL/DECIMAL column (a `Number` that is not INT) is pinned via
 * {@link DeriveColumnsOptions.columnTypes} (the escape hatch, unchanged from F1).
 */
export const DEFAULT_UNCAST_SQL_TYPE = 'INTEGER';

/** Options for {@link deriveModelColumns}. */
export interface DeriveColumnsOptions {
  /**
   * Per-column SQL-type OVERRIDES (property name → §4.1 SQL type token), for columns whose decorator
   * family is ambiguous (a bare `Number` that is actually REAL/DECIMAL) or that need a non-default
   * width. Takes precedence over the family-derived token. The escape hatch for the no-INT-vs-REAL
   * decorator gap — no assumption is baked into the engine.
   */
  readonly columnTypes?: Readonly<Record<string, string>>;
}

/**
 * Translate ONE column's {@link ColumnMeta} to its §4.1 SQL-type token. Precedence (Phase F-2 / #105
 * option B):
 *   1. an explicit `columnTypes` override (the REAL/DECIMAL/width escape hatch) wins;
 *   2. else the family token from {@link COLUMN_FAMILY_SQL_TYPE} (an explicit `@column.*` `sqlCast`);
 *   3. else the decorator's `baseSqlType` — derived from the field's TS `design:type` (String→TEXT /
 *      Number→INTEGER / Boolean→BOOLEAN / Date→TIMESTAMP / BigInt→BIGINT), so a bare `@column()` types
 *      correctly for the SCP typed-read de-box (the fix for F1's blanket-INTEGER default);
 *   4. else {@link DEFAULT_UNCAST_SQL_TYPE} (only when `design:type` is unavailable).
 * Fail-closed: a family token the mapping does not know, or a produced token `coltype.ts` rejects,
 * THROWS (naming the column) — never a silent skip (no-assume, no-fallback).
 */
export function columnSqlType(propKey: string, meta: ColumnMeta, override?: string): string {
  const sqlType =
    override ??
    (meta.sqlCast !== undefined ? COLUMN_FAMILY_SQL_TYPE[meta.sqlCast] : undefined) ??
    meta.baseSqlType ??
    DEFAULT_UNCAST_SQL_TYPE;
  if (meta.sqlCast !== undefined && override === undefined && COLUMN_FAMILY_SQL_TYPE[meta.sqlCast] === undefined) {
    throw new Error(
      `decorator-adapter: column '${propKey}' has decorator sqlCast family '${meta.sqlCast}' with no ` +
        `SCP SQL-type mapping. Add it to COLUMN_FAMILY_SQL_TYPE or pin the column via ` +
        `options.columnTypes['${propKey}']. No-assume, no-fallback.`,
    );
  }
  // Validate the produced token is in the §4.1 vocabulary (throws for an unknown/ambiguous type).
  sqlTypeToMaterializeClass(sqlType);
  return sqlType;
}

/**
 * Derive the SCP `static columns` ({@link ModelColumns}) for a decorated model class from its
 * `@column` `ColumnMeta` registry (the decorator IS the type SoT for v1/v2's decorator surface). The
 * table is the model's `TABLE_NAME` (or the model-name lowercased, matching the decorator's
 * `effectiveTableName`). Each column keys by its DB `columnName` (what a SELECT projects), mapping to
 * its §4.1 SQL type. A model with no `@column` declarations yields no table entry.
 *
 * This is the ONE real translation in the adapter — the read-path de-box (INT→number / BIGINT→string /
 * DATE→string / BOOLEAN→boolean) and the codegen `outType` both consult exactly this SoT.
 */
export function deriveModelColumns(modelClass: ModelClassLike, options: DeriveColumnsOptions = {}): ModelColumns {
  const meta = getColumnMeta(modelClass);
  if (meta === undefined || meta.size === 0) return {};
  const table = tableNameOf(modelClass);
  const cols: Record<string, string> = {};
  for (const [propKey, m] of meta) {
    cols[m.columnName] = columnSqlType(propKey, m, options.columnTypes?.[propKey]);
  }
  return { [table]: cols };
}

/** The decorated model class shape the adapter reads (a `@model`-decorated `DBModel` subclass). */
export interface ModelClassLike {
  readonly name: string;
  readonly TABLE_NAME?: string;
}

/** The effective table name (v1 `@model` rule): explicit `TABLE_NAME`, else the model name lowercased. */
export function tableNameOf(modelClass: ModelClassLike): string {
  return modelClass.TABLE_NAME ?? modelClass.name.toLowerCase();
}

// ── 2. Read authoring generation (find / findOne / findById / count) ────────────────────────────

/**
 * The read ports a `find`/`findOne`/`findById` lowers to (the model's `@model` table + the caller's
 * conditions / order / limit / offset / select / group). A WHERE fragment list is authored with the
 * EXISTING SCP where sugar (`whereEq` / `when(cond, () => …)` for SKIP / `inSubquery` / `queryView`
 * ports), passed as {@link where} — the adapter does not re-parse v1 `Conds` (F1 is a lowering layer).
 */
export interface ReadAuthoringSpec {
  /** The projection column list (the model's `SELECT_COLUMN`, split; defaults to `['*']`). */
  readonly select?: readonly string[];
  /** A WHERE fragment builder over the recorder `$` (uses the SCP where sugar). */
  readonly where?: (($: Recorded) => readonly unknown[]) | undefined;
  /** ORDER BY body (dialect-neutral text; the model's `DEFAULT_ORDER` rendered, or an explicit order). */
  readonly order?: string;
  /** GROUP BY body (the model's `DEFAULT_GROUP` rendered). */
  readonly group?: string;
  /** LIMIT — a literal number, or a `$`-ref/`coalesce` expression built by the caller. */
  readonly limit?: (($: Recorded) => unknown) | number;
  /** OFFSET — a literal number, or a `$`-ref/`coalesce` expression. */
  readonly offset?: (($: Recorded) => unknown) | number;
}

/**
 * Build the eager `fn($, L)` a `find`/`findOne`/`findById` read lowers to: `L.Select({ table, select,
 * where?, order?, group?, limit?, offset? })`. Mirrors `DBModel._buildSelectSQL`'s inputs, but emits
 * SCP authoring instead of an imperative SQL string. `findOne`/`findById` are a `find` with the
 * identity/limit-1 shape decided by the CALLER's `where`/`limit` — the authoring is identical (the
 * single-row collapse is a runtime cardinality concern, spec §5), so ONE generator serves all three.
 */
export function findAuthoring(table: string, spec: ReadAuthoringSpec = {}, dialect: DialectName = 'sqlite'): EagerBehavior {
  // The returned closure's SOURCE is what bc's native-control-syntax scan reads (`authoring.ts`
  // `makeEagerClass`); it MUST contain no native control flow. Port assembly (`selectPorts`) and the
  // op→sql+params lowering (`emitRead` → `compileSelectNode`) are EXTERNAL helpers (their `if`s are
  // over compile-time spec fields / the recorded ports, not the scanned frame). The closure is a bare
  // `emitRead(...)` call that records ONE op-independent `executeSQL` node (#141).
  return ($: Recorded, L: ComponentFns) => emitRead(L, 'Select', selectPorts(table, spec, $), dialect);
}

/**
 * Assemble the `Select` ports object in the CANONICAL author order (`table`, `select`, `where`,
 * `order`, `group`, `limit`, `offset`) — the SAME insertion order a hand-written `L.Select({…})`
 * produces, so the recorded IR is byte-identical (bc records port keys in insertion order → the JSON
 * bytes depend on it). Runs at recording time with the live `$`. NOT scanned by bc (it is external to
 * the authored method), so its build-time `if`s are legitimate.
 */
function selectPorts(table: string, spec: ReadAuthoringSpec, $: Recorded): Record<string, unknown> {
  const ports: Record<string, unknown> = { table, select: spec.select ?? ['*'] };
  if (spec.where !== undefined) ports.where = spec.where($);
  if (spec.order !== undefined) ports.order = spec.order;
  if (spec.group !== undefined) ports.group = spec.group;
  if (spec.limit !== undefined) ports.limit = typeof spec.limit === 'function' ? spec.limit($) : spec.limit;
  if (spec.offset !== undefined) ports.offset = typeof spec.offset === 'function' ? spec.offset($) : spec.offset;
  return ports;
}

/**
 * Build the eager `fn($, L)` a `count` lowers to: `L.Count({ table, where? })` (v1 `DBModel._count` —
 * `SELECT COUNT(*) FROM t [WHERE …]`, no projection/order/limit). The one-row `[{count}]` shape is the
 * `items` output; the consumer reads `count` (v1 `parseInt`).
 */
export function countAuthoring(table: string, where?: ($: Recorded) => readonly unknown[], dialect: DialectName = 'sqlite'): EagerBehavior {
  return ($: Recorded, L: ComponentFns) => emitRead(L, 'Count', countPorts(table, $, where), dialect);
}

// ── #141 read relations as a pluck/group graph (N+1-free: one child query per level) ──────────────
//
// A read WITH relations authors the FULL graph `parents = executeSQL(select) → ids = pluck(parents,fk)
// → children = executeSQL(WHERE fk = ANY(ids)) → group(parents, children)`. bc orchestrates it; the
// child fetch is ONE query per level (nestedFindAll = 2 queries, nestedRelations = 3). The child SQL
// is the SSoT `compileRelationOp` (byte-identical to v1 `LazyRelation`); grouping/dedupe is the
// `pluck`/`group` op-independent leaves (relocated `relation.ts` logic). This REPLACES the imperative
// `runRelationOp`/`distributeToParent` post-read grouping (step 5 deletes those).

/**
 * The eager `fn($, L)` a read WITH relations lowers to: the parent `executeSQL` + a `pluck`/`group`
 * subgraph per relation. `relations` are the model's `RelationDecl`s (compiled to the static child
 * batch SQL by {@link compileRelationOp}). The graph build (loops/recursion) lives in the EXTERNAL
 * {@link buildRelationGraph} so the scanned closure stays a bare call (no native control syntax).
 */
export function relationReadAuthoring(
  table: string,
  spec: ReadAuthoringSpec,
  relations: readonly RelationDecl[],
  dialect: DialectName = 'sqlite',
  keyType?: KeyTypeResolver,
): EagerBehavior {
  return ($: Recorded, L: ComponentFns) =>
    buildRelationGraph(L, emitRead(L, 'Select', selectPorts(table, spec, $), dialect), table, relations, dialect, keyType);
}

/**
 * Resolve the de-box bc element scalar of a relation's parent-key column ((table, column) → bc scalar),
 * so the `pluck` key array is stamped with the type the READ-materialized key value actually carries
 * (INT→`float` / BIGINT→`string` / text/uuid→`string`) — see {@link import('./coltype').keyArrayElemScalar}.
 */
export type KeyTypeResolver = (table: string, column: string) => import('./coltype').BcScalar;

/**
 * Attach each relation's `pluck`/`group` subgraph onto the parent read result (N+1-free per level).
 * `parentTable` is the PRIMARY read table — the parent-key column each relation plucks lives on it.
 */
function buildRelationGraph(L: ComponentFns, parents: Recorded, parentTable: string, relations: readonly RelationDecl[], dialect: DialectName, keyType?: KeyTypeResolver): Recorded {
  let result = parents;
  for (const decl of relations) result = attachRelation(L, result, parents, compileRelationOp({ ...decl, dialect }), parentTable, dialect, keyType);
  return result;
}

/**
 * Attach ONE relation: pluck the parent keys → one child `executeSQL(WHERE fk = ANY(keys))` →
 * (recurse into grandchildren, keyed off the child rows) → `group` the children onto the parents.
 * `parentsResult` is the accumulating parent node (nested fields stack); `parentsForKeys` supplies the
 * dedup key column; `parentTable` names the table the parent-key column lives on (for its de-box type).
 * Single-key only for now (composite `parentKeys` is a follow-up).
 */
function attachRelation(L: ComponentFns, parentsResult: Recorded, parentsForKeys: Recorded, op: RelationOp, parentTable: string, dialect: DialectName, keyType?: KeyTypeResolver): Recorded {
  // The ordered parent/child key-column TUPLES (single-key → 1 column; composite → the tuple) — the
  // SSoT {@link parentKeyCols}/{@link targetKeyCols} (relation.ts). Passing the WHOLE tuple to the
  // pluck/group leaves is what makes a composite relation group by the full key identity (not the old
  // `op.parentKey ?? ''` scalar collapse → cartesian). B-2 (NATIVE_RELATION_PLAN.md).
  const parentKeys = parentKeyCols(op);
  const targetKeys = targetKeyCols(op);
  const pluck = L.pluck as (p: Record<string, unknown>) => Recorded & { as(t: unknown): Recorded };
  const executeSQL = L.executeSQL as (p: Record<string, unknown>) => Recorded;
  const group = L.group as (p: Record<string, unknown>) => Recorded;
  // Key array is an opaque transport value (bc#156): stamp its element type via `.as` from the parent
  // key column's de-box scalar (single-key → the scalar array; composite → an array of tuples). Without
  // a resolver the element type defaults to `float` (a JS-number key) — the common INT-key shape.
  const elem = keyType !== undefined ? keyType(parentTable, parentKeys[0]) : 'float';
  const asType = parentKeys.length === 1 ? { arr: elem } : { arr: { arr: elem } };
  const ids = pluck({ rows: parentsForKeys, col: [...parentKeys] }).as(asType);
  // A relation child fetch is a READ (`write:false`, non-RETURNING) — the executeSQL leaf requires the
  // full read intent port set, same as `emitRead`.
  // The child fetch is a READ: carry `readColumns` (child table + projection) so `lowerReadColumns`
  // stamps its outType — the SAME #59 mechanism as the primary `emitRead`, applied to the relation
  // child, so the native de-box types the child rows (and `group` nests a typed child list, not empty).
  const childPortObj: Record<string, unknown> = { sql: op.sql, params: [ids], write: false, returning: false, bigint: false };
  if (op.targetTable !== undefined && op.select !== undefined) childPortObj.readColumns = { table: op.targetTable, cols: [...op.select] };
  let children = executeSQL(childPortObj);
  // The grandchild's parent-key column lives on THIS relation's child table (`op.targetTable`).
  for (const gc of op.childRelations ?? []) children = attachRelation(L, children, children, gc, op.targetTable ?? parentTable, dialect, keyType);
  return group({ parents: parentsResult, children, pk: [...parentKeys], fk: [...targetKeys], into: op.name, single: op.kind !== 'hasMany' });
}

/** Assemble the `Count` ports (`table`, then optional `where`) — external to the scanned frame. */
function countPorts(table: string, $: Recorded, where?: ($: Recorded) => readonly unknown[]): Record<string, unknown> {
  const ports: Record<string, unknown> = { table };
  if (where !== undefined) ports.where = where($);
  return ports;
}

/**
 * Compile a decorated model's read (`find`/`findOne`/`findById`/`count`) into the published
 * {@link SqlBundle} straight from its metadata: derive the `static columns`, build the eager fn, run it
 * through `compileEager` (the SINGLE compile path — byte-identical to a hand-written declaration), then
 * `compileBundle`. The `entry`/method `name` names the emitted component (byte-identity requires equal
 * names, so the caller passes the method name it is standing in for, e.g. `'find'`).
 */
export function compileReadBundle(
  modelClass: ModelClassLike,
  name: string,
  fn: EagerBehavior,
  dialect: DialectName = 'sqlite',
  columnsOptions?: DeriveColumnsOptions,
  relations?: readonly RelationDecl[],
): SqlBundle {
  const contract = compileReadContract(modelClass, name, fn, dialect, columnsOptions);
  return compileBundle(contract, name, relations, dialect, undefined, contract.resolveColumnType);
}

/**
 * Compile a decorated model's read into the {@link BehaviorModelContract} (the authored op-independent
 * leaf-graph IR) — the SSoT the new sync ({@link import('./runtime').executeBehavior}) and async
 * ({@link import('./runtime').executeBehaviorAsync}) read paths run via `bindBehaviors`. Derives the
 * model's `static columns` and lowers `fn` through `compileEager` (the single compile path). Shared by
 * {@link compileReadBundle} (legacy bundle surface) so there is one read-authoring derivation.
 */
export function compileReadContract(
  modelClass: ModelClassLike,
  name: string,
  fn: EagerBehavior,
  dialect: DialectName = 'sqlite',
  columnsOptions?: DeriveColumnsOptions,
): BehaviorModelContract {
  return compileEager(name, fn, { columns: deriveModelColumns(modelClass, columnsOptions), dialect });
}

// ── 3. Write authoring generation (create / createMany / update / updateMany / delete) ──────────

/**
 * The `create` insert spec: the per-column VALUE fragments (`values.<col>` — a `$`-ref/literal each),
 * an optional RETURNING projection, and optional per-column PG casts (`sqlCast.<col>`). A plain create
 * with NO `onConflict` lowers to `compileWriteBundle` (single INSERT via `compileWriteNode`); an
 * upsert (`onConflict*`) routes through {@link compileCreateBundle}'s createMany path (see there).
 */
export interface InsertAuthoringSpec {
  /** `col → value fragment` (a `$`-ref / literal / builder expr). Keys are DB column names. */
  readonly values: ($: Recorded) => Record<string, unknown>;
  /** RETURNING projection (bare / `t.col`), or absent for a non-returning insert. */
  readonly returning?: string;
  /** `col → PG cast type` (drives `?::<cast>` on Postgres; inert on MySQL/SQLite). */
  readonly sqlCast?: Readonly<Record<string, string>>;
}

/** Build the eager `fn($, L)` a plain `create` lowers to: `L.Insert({ table, values.*, sqlCast.*?, returning? })`. */
export function createAuthoring(table: string, spec: InsertAuthoringSpec, dialect: DialectName = 'sqlite'): EagerBehavior {
  // Port assembly (branching on spec presence) lives in the external {@link insertPorts} helper so the
  // scanned closure body stays free of native control syntax — see {@link findAuthoring}. The op lowers
  // to ONE op-independent `executeSQL` write node via {@link emitWrite} (#141).
  return ($: Recorded, L: ComponentFns) => emitWrite(L, 'Insert', insertPorts(table, spec, $), dialect);
}

function insertPorts(table: string, spec: InsertAuthoringSpec, $: Recorded): Record<string, unknown> {
  const ports: Record<string, unknown> = { table };
  for (const [col, frag] of Object.entries(spec.values($))) ports[`values.${col}`] = frag;
  if (spec.sqlCast !== undefined) for (const [col, t] of Object.entries(spec.sqlCast)) ports[`sqlCast.${col}`] = t;
  if (spec.returning !== undefined) ports.returning = spec.returning;
  return ports;
}

/** Build the eager `fn($, L)` an `update` lowers to: `L.Update({ table, set.*, where, sqlCast.*?, returning? })`. */
export function updateAuthoring(
  table: string,
  set: ($: Recorded) => Record<string, unknown>,
  where: ($: Recorded) => readonly unknown[],
  opts: { returning?: string; sqlCast?: Readonly<Record<string, string>> } = {},
  dialect: DialectName = 'sqlite',
): EagerBehavior {
  return ($: Recorded, L: ComponentFns) => emitWrite(L, 'Update', updatePorts(table, set, where, opts, $), dialect);
}

function updatePorts(
  table: string,
  set: ($: Recorded) => Record<string, unknown>,
  where: ($: Recorded) => readonly unknown[],
  opts: { returning?: string; sqlCast?: Readonly<Record<string, string>> },
  $: Recorded,
): Record<string, unknown> {
  const ports: Record<string, unknown> = { table, where: where($) };
  for (const [col, frag] of Object.entries(set($))) ports[`set.${col}`] = frag;
  if (opts.sqlCast !== undefined) for (const [col, t] of Object.entries(opts.sqlCast)) ports[`sqlCast.${col}`] = t;
  if (opts.returning !== undefined) ports.returning = opts.returning;
  return ports;
}

/** Build the eager `fn($, L)` a `delete` lowers to: `L.Delete({ table, where, returning? })`. */
export function deleteAuthoring(
  table: string,
  where: ($: Recorded) => readonly unknown[],
  returning?: string,
  dialect: DialectName = 'sqlite',
): EagerBehavior {
  return ($: Recorded, L: ComponentFns) => emitWrite(L, 'Delete', deletePorts(table, where, $, returning), dialect);
}

function deletePorts(
  table: string,
  where: ($: Recorded) => readonly unknown[],
  $: Recorded,
  returning?: string,
): Record<string, unknown> {
  const ports: Record<string, unknown> = { table, where: where($) };
  if (returning !== undefined) ports.returning = returning;
  return ports;
}

/**
 * Compile a decorated model's `create` (or `update` / `delete`) command into a write {@link SqlBundle}
 * via `compileWriteBundle` — the single-statement Command path (gate-first tx plan). The eager fn is
 * built by {@link createAuthoring} / {@link updateAuthoring} / {@link deleteAuthoring}. The model's
 * `static columns` types the RETURNING de-box (post-compile). See {@link compileUpsertBundle} for
 * `onConflict` (upsert) — a single INSERT `compileWriteNode` does NOT emit ON CONFLICT (by design; v1
 * routes upsert through the `_insert` grouping path = the createMany bundle).
 */
export function compileCommandBundle(
  modelClass: ModelClassLike,
  name: string,
  fn: EagerBehavior,
  writes: EntityWritesDefinition,
  phase: WriteLifecyclePhase,
  dialect: DialectName = 'sqlite',
  columnsOptions?: DeriveColumnsOptions,
): SqlBundle {
  const columns = deriveModelColumns(modelClass, columnsOptions);
  const contract = compileEager(name, fn, { columns, dialect });
  return compileWriteBundle(contract, name, writes, phase, dialect, contract.resolveColumnType);
}

/**
 * Compile a decorated model's `createMany` — and a `create` WITH `onConflict` (upsert) — into a batch
 * write {@link SqlBundle} via `compileCreateManyBundle`. This is the UPSERT carry: `onConflict` /
 * `onConflictUpdate` / `onConflictIgnore` are `compileInsertMany` BUILD options (not authored ports),
 * so they carry end-to-end here with NO SCP authoring addition — exactly as v1's `create` and
 * `createMany` share the ONE `DBModel._insert` grouping path (`buildInsert` handles ON CONFLICT for
 * both a single record and a batch). A single-record `records` array is a one-group createMany that
 * emits the SAME statement `_insert` does for a single upsert.
 *
 * Parity scope (the v1/v2 SQL-parity rule, per dialect — NOT a blanket "byte-identical to v1"): on
 * **Postgres** the emitted upsert INSERT is byte-identical to v1 (`compileInsertMany` copies the v1
 * `buildInsert` verbatim; PG stays base-class tuple/placeholder form). On **MySQL / SQLite** the v2
 * form is the JSON-array single-param shape (`json_each` / `JSON_TABLE`), which is DELIBERATELY NOT
 * byte-identical to v1's per-row placeholder expansion — it is the established v2 shape the conformance
 * corpus freezes (#64/#65), executing to the same rows. So: pg = v1-byte-identical; mysql/sqlite = the
 * v2 JSON-array form (equivalent result, distinct text).
 *
 * @param options the `compileCreateManyBundle` options (records / rawRecords / sqlCastMap / onConflict /
 *   onConflictUpdate / onConflictIgnore / returning / pk) — already serialized as `DBModel._insert`
 *   holds them.
 */
export function compileCreateBundle(
  modelClass: ModelClassLike,
  name: string,
  options: InsertManyBuildOptions & { pk?: { columns: readonly string[]; autoInc: string | null } },
  dialect: DialectName = 'sqlite',
  columnsOptions?: DeriveColumnsOptions,
): SqlBundle {
  const resolveColumnType = modelColumnResolver(modelClass, columnsOptions);
  return compileCreateManyBundle(name, options, dialect, resolveColumnType);
}

/** Compile a decorated model's `updateMany` into a batch write {@link SqlBundle} (`compileUpdateManyBundle`). */
export function compileUpdateBundle(
  name: string,
  options: UpdateManyBuildOptions,
  dialect: DialectName = 'sqlite',
): SqlBundle {
  return compileUpdateManyBundle(name, options, dialect);
}

/**
 * Compile a decorated model's `deleteMany` into a batch write {@link SqlBundle} (`compileDeleteManyBundle`):
 * a PK-set IN-list DELETE (single key) or one DELETE per composite-key group. `keyColumns` +
 * `returning` carry straight through.
 */
export function compileDeleteBundle(
  name: string,
  options: { tableName: string; keyColumns: string[]; keys: Record<string, unknown>[]; returning?: string },
  dialect: DialectName = 'sqlite',
): SqlBundle {
  return compileDeleteManyBundle(name, options, dialect);
}

/**
 * Build a {@link KeyTypeResolver} from a decorated model's `static columns` SoT — `(table, column) →`
 * the de-box bc scalar of that key column ({@link import('./coltype').keyArrayElemScalar}). Threaded into
 * {@link relationReadAuthoring} so each relation's `pluck` key array is stamped with the type its
 * READ-materialized key value carries. Returns `undefined` when the model declares no columns.
 */
export function relationKeyTypeResolver(
  modelClass: ModelClassLike,
  columnsOptions?: DeriveColumnsOptions,
): KeyTypeResolver | undefined {
  const columns = deriveModelColumns(modelClass, columnsOptions);
  if (Object.keys(columns).length === 0) return undefined;
  return (table: string, column: string) => {
    const sqlType = columns[table]?.[column];
    if (sqlType === undefined) {
      throw new Error(
        `decorator-adapter: relation key column '${table}.${column}' has no declared type in the model's ` +
          `static columns — cannot stamp the key array's de-box element type (no-assume, no-fallback).`,
      );
    }
    return keyArrayElemScalar(sqlType);
  };
}

/** The fail-closed column-type resolver for a decorated model (its `static columns` SoT), or `undefined` if it has no columns. */
export function modelColumnResolver(
  modelClass: ModelClassLike,
  columnsOptions?: DeriveColumnsOptions,
): ColumnTypeResolver | undefined {
  const columns = deriveModelColumns(modelClass, columnsOptions);
  const tables = Object.keys(columns);
  if (tables.length === 0) return undefined;
  const map = new Map<string, Map<string, string>>();
  for (const [t, cols] of Object.entries(columns)) map.set(t, new Map(Object.entries(cols)));
  return columnTypeResolverFromColumnMap(map);
}

// ── 4. Relation authoring generation (@hasMany / @belongsTo / @hasOne → RelationDecl → RelationOp) ──

/**
 * Translate a decorated model's `@hasMany` / `@belongsTo` / `@hasOne` {@link RelationMeta} registry
 * into SCP {@link RelationDecl}s. Single AND composite keys are supported: the decorator's
 * `keysFactory` resolves lazily (forward refs) to `[srcCol, tgtCol]` (single) or `[[…],[…]]`
 * (composite); the target table + projection come from the target model's `@column` metadata (the
 * relation projects the child's OWN columns). Per-parent `order` / `limit` (hasMany window) and
 * `hardLimit` carry from the decorator `options`.
 *
 * @param modelClass the parent (source) `@model` class.
 * @param resolveTargetModel model NAME → the target `@model` class (the decorator records the target
 *   model NAME on the key column; the caller supplies the registry — same lazy-resolution shape v1's
 *   `_loadRelation` uses).
 */
export function deriveRelationDecls(
  modelClass: ModelClassLike,
  resolveTargetModel: (modelName: string) => ModelClassLike,
  dialect: DialectName = 'sqlite',
): RelationDecl[] {
  return getRelationMeta(modelClass).map((rel) => relationDeclOf(rel, resolveTargetModel, dialect).decl);
}

/**
 * Translate ONE {@link RelationMeta} → a {@link RelationDecl} (single or composite key) AND the
 * resolved target model (so a caller can bake the child's de-box materializers from the TARGET
 * model's `static columns` — the relation projects the child's own columns).
 */
export function relationDeclOf(
  rel: RelationMeta,
  resolveTargetModel: (modelName: string) => ModelClassLike,
  dialect: DialectName = 'sqlite',
): { decl: RelationDecl; targetModel: ModelClassLike } {
  const parsed = parseKeys(rel.keysFactory());
  const targetModel = resolveTargetModel(parsed.targetModelName);
  const targetTable = tableNameOf(targetModel);
  const select = targetProjection(targetModel);
  const order = rel.options?.order ? orderToString(rel.options.order() as OrderSpec) : undefined;

  const base = {
    name: rel.propertyKey,
    kind: rel.type as RelationKind,
    targetTable,
    select,
    dialect,
    ...(order !== undefined ? { order } : {}),
    ...(rel.options?.limit !== undefined ? { limit: rel.options.limit } : {}),
    ...(rel.options?.hardLimit !== undefined ? { hardLimit: rel.options.hardLimit } : {}),
  };
  const decl: RelationDecl = parsed.composite
    ? { ...base, parentKeys: parsed.sourceKeys, targetKeys: parsed.targetKeys }
    : { ...base, parentKey: parsed.sourceKeys[0], targetKey: parsed.targetKeys[0] };
  return { decl, targetModel };
}

/**
 * Compile a decorated model's relation registry into ready {@link RelationOp}s (one per relation). Each
 * op's child-column de-box materializers are baked from the TARGET model's `static columns` (the same
 * resolution the primary read uses) — ZERO per-read introspection.
 */
export function compileRelationOps(
  modelClass: ModelClassLike,
  resolveTargetModel: (modelName: string) => ModelClassLike,
  dialect: DialectName = 'sqlite',
  columnsOptions?: DeriveColumnsOptions,
): Record<string, RelationOp> {
  const ops: Record<string, RelationOp> = {};
  for (const rel of getRelationMeta(modelClass)) {
    const { decl, targetModel } = relationDeclOf(rel, resolveTargetModel, dialect);
    ops[decl.name] = compileRelationOp(decl, modelColumnResolver(targetModel, columnsOptions));
  }
  return ops;
}

// ── internal helpers ───────────────────────────────────────────────────────────────────────────

/** A relation Column marker as the decorator records it (`{ columnName, modelName }`, `src/Column.ts`). */
interface RelColumn {
  readonly columnName: string;
  readonly modelName: string;
}
type RelKeyPair = readonly [RelColumn, RelColumn];

/** Parse the decorator `keysFactory()` result (single pair or composite) into src/target column lists. */
function parseKeys(keys: unknown): {
  composite: boolean;
  sourceKeys: string[];
  targetKeys: string[];
  targetModelName: string;
} {
  const arr = keys as readonly unknown[];
  const composite = Array.isArray(arr[0]);
  if (composite) {
    const pairs = arr as readonly RelKeyPair[];
    return {
      composite: true,
      sourceKeys: pairs.map((p) => p[0].columnName),
      targetKeys: pairs.map((p) => p[1].columnName),
      targetModelName: pairs[0][1].modelName,
    };
  }
  const [src, tgt] = arr as unknown as RelKeyPair;
  return { composite: false, sourceKeys: [src.columnName], targetKeys: [tgt.columnName], targetModelName: tgt.modelName };
}

/** The target model's projected columns (its own `@column` DB column names — the relation child props). */
function targetProjection(targetModel: ModelClassLike): string[] {
  const meta = getColumnMeta(targetModel);
  if (meta === undefined) return [];
  return Array.from(meta.values()).map((m) => m.columnName);
}
