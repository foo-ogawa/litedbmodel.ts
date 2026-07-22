/**
 * litedbmodel v2 SCP — the op-INDEPENDENT runtime leaves (#141).
 *
 * The whole per-DSL execution surface is THREE op-agnostic leaves, defined once via bc's
 * `defineLeaf` and exposed as authoring components via `behaviorComponents` (SA8 / bc#126).
 * There is NO per-op catalog (the retired 8-leaf `Select`/`Insert`/… catalog); a query is a
 * graph of these three leaves over the tuned SQL text the `makesql/compile-*` builders emit:
 *
 *   - {@link executeSQL} — the SOLE SQL transport. `fn(ports, ctx)` binds params and runs one
 *     statement through the central {@link import('./exec-context') execute/run seam} (the ONLY
 *     driver contact). Read (`write:false`) → rows; write (`write:true`) → a one-row
 *     `[{changes,lastInsertRowid}]` summary (RETURNING writes return their rows via `execute`).
 *     It owns the transport-level param shaping a relation key-set needs — the dialect array
 *     encoding + deferred PG cast resolution + `?`→`$N` render relocated here from the retired
 *     `relation.ts` `bindKeys`/`runRelationOp` tail (op-independent: it is a value-list concern,
 *     not a relation concern).
 *   - {@link pluck} — rows + a column name → the deduped, non-null key array (the `= ANY($1)`
 *     batch key set). Relocated from `relation.ts` `dedupeKeys`. bc represents `int` as `BigInt`
 *     (expr-eval `typeName`: `number`→float / `bigint`→int), so an integer key is emitted as a
 *     `BigInt`; the authoring stamps the key array's element type via `.as` from the model's
 *     `static columns` (int → `{arr:'int'}`, text/uuid → `{arr:'string'}`).
 *   - {@link group} — parents + a flat child list + `pk`/`fk`/`into` → each parent with its
 *     matching children nested under `into` (`hasMany` → list, single → the one child or null).
 *     Relocated from `relation.ts` `keyIdentity`/`distributeToParent`. This is the in-memory
 *     grouping that makes the child fetch ONE query (N+1-free): `parents → pluck → children
 *     (WHERE fk = ANY($1)) → group`.
 *
 * `ctx` is the environment boundary bc injects at `bindBehaviors` time (never on the IR — C4):
 * the {@link ExecutionContext} (connection provider + middleware + tx pin) and the target
 * {@link Dialect}. The three leaves are the op-independent transport symbols the native codegen
 * calls directly (`generateModule` `leafTransport.symbols`): `execute_sql` / `pluck_keys` /
 * `group_children`.
 */

import { defineLeaf, behaviorComponents } from 'behavior-contracts';
import {
  type ExecutionContext,
  type AsyncExecutionContext,
  execute as seamExecute,
  executeSafe as seamExecuteSafe,
  run as seamRun,
  executeAsync as seamExecuteAsync,
  runAsync as seamRunAsync,
  type StatementIntent,
  type RunInfo,
} from './exec-context';
import { renderPlaceholders, type Dialect } from './makesql/handler';
import { resolvePgArrayCast } from './makesql/compile-relation';
import { dedupeKeyTuples, groupByKey, attachToParent } from './grouping';
import { materializeCell, type MaterializeClass } from './coltype';

/**
 * The environment boundary the runtime injects at {@link import('behavior-contracts').bindBehaviors}
 * time (C4 — never on the IR). `exec` is the SYNC connection/middleware/tx seam (better-sqlite3, run
 * via `bindBehaviors().run`); `dialect` selects the transport-level param encoding + placeholder
 * render. The native ports carry the SAME facts.
 */
export interface LeafContext {
  /** The central sync execute/run seam (connection provider + middleware + tx pin). */
  readonly exec: ExecutionContext;
  /** The target SQL dialect (drives array-param encoding + `?`→`$N` render). */
  readonly dialect: Dialect;
}

/**
 * The ASYNC environment boundary (live PG / MySQL, run via `bindBehaviors().runAsync`). Carries the
 * async execute/run seam ({@link AsyncExecutionContext}) instead of the sync one — the `executeSQL`
 * leaf branches on the presence of `execAsync` and returns a Promise (bc `runAsync` awaits it).
 */
export interface AsyncLeafContext {
  /** The central async execute/run seam (pooled per-execution connection ownership). */
  readonly execAsync: AsyncExecutionContext;
  /** The target SQL dialect. */
  readonly dialect: Dialect;
}

/** Normalize a driver `lastInsertRowid` (number|bigint) to bc's `int` value model (BigInt). */
function toRowid(v: number | bigint): bigint {
  return typeof v === 'bigint' ? v : BigInt(v);
}

// ── executeSQL — the sole op-independent SQL transport ─────────────────────────

/**
 * Encode a value list for the driver: a bound scalar passes through; an ARRAY element (a relation
 * key set bound as ONE param — `= ANY($1)` / `json_each(?)`) binds as the raw array on PostgreSQL
 * and as a single JSON string on MySQL/SQLite (the `makesql` locked model + the retired
 * `relation.ts` `bindKeys` encoding, relocated to the transport seam).
 *
 * bc's expression eval models `int` as `BigInt`, so an IN-list / key-set value can arrive as a
 * `BigInt` (a `$`-ref to a BIGINT column, a bc int literal). PostgreSQL binds the raw array (the
 * driver accepts BigInt); for the MySQL/SQLite JSON form the array must serialize to JSON, where a
 * `BigInt` element is coerced to a JSON number ({@link jsonNumber}) — `JSON.stringify` cannot emit a
 * `BigInt`, and `json_each`/`JSON_TABLE` compare numerically, so a numeric literal is the correct
 * (and v1-parity) encoding for every in-range key. This is the SOLE transport-level array encode.
 */
function encodeParams(params: readonly unknown[], dialect: Dialect): unknown[] {
  return params.map((p) => (Array.isArray(p) ? (dialect === 'postgres' ? p : JSON.stringify(p, jsonNumber)) : p));
}

/** `JSON.stringify` replacer: coerce a bc `int` (`BigInt`) element to a JSON number for the JSON IN-list form. */
function jsonNumber(_key: string, value: unknown): unknown {
  return typeof value === 'bigint' ? Number(value) : value;
}

/** Prepare a statement for the seam: resolve deferred PG cast(s), render `?`→`$N`, encode params. */
function prepareSql(p: { sql: string; params: unknown[]; write: boolean; connection?: string | null }, dialect: Dialect): { sql: string; bound: unknown[]; intent: StatementIntent } {
  let sql = p.sql;
  if (dialect === 'postgres') {
    for (const param of p.params) if (Array.isArray(param)) sql = resolvePgArrayCast(sql, param);
  }
  sql = renderPlaceholders(sql, dialect);
  const bound = encodeParams(p.params, dialect);
  const intent: StatementIntent = { write: p.write === true, ...(p.connection != null ? { db: p.connection } : {}) };
  return { sql, bound, intent };
}

/** The affected-write summary row a non-returning write yields (uniform `items` output shape). */
function writeSummary(info: RunInfo): Array<Record<string, unknown>> {
  return [{ changes: info.changes, lastInsertRowid: toRowid(info.lastInsertRowid) }];
}

/**
 * Apply the #59 read de-box map to the fetched rows IN PLACE: coerce each `outputKey`'s cell through
 * {@link materializeCell} (BIGINT→exact string / DATE→string / BOOLEAN→boolean). This is the transport
 * OUTPUT counterpart of {@link encodeParams} (the sole driver contact owns both param encode + row
 * de-box); the map is the model's `static columns` resolution stamped at compile by `lowerReadColumns`
 * (`authoring.ts`). The coercion SSoT is {@link materializeCell} — consumed here, never re-implemented.
 */
function materializeRows(rows: Array<Record<string, unknown>>, materializers: Record<string, MaterializeClass>): Array<Record<string, unknown>> {
  const cols = Object.keys(materializers);
  if (cols.length === 0) return rows;
  for (const row of rows) for (const col of cols) if (col in row) row[col] = materializeCell(row[col], materializers[col]);
  return rows;
}

/**
 * The ASYNC (live PG / MySQL) execution body — the async twin of the sync branch below, over the
 * exec-context async seam ({@link seamExecuteAsync}/{@link seamRunAsync}). Returns a Promise bc
 * `runAsync` awaits. Async de-box is a per-connection driver-config concern (not `safeIntegers`), so
 * there is no async `bigint` branch — reads go through `executeAsync`.
 */
async function executeSqlAsync(
  execAsync: AsyncExecutionContext,
  prepared: { sql: string; bound: unknown[]; intent: StatementIntent },
  nonReturningWrite: boolean,
  materializers: Record<string, MaterializeClass> | undefined,
): Promise<Array<Record<string, unknown>>> {
  if (nonReturningWrite) return writeSummary(await seamRunAsync(execAsync, prepared.sql, prepared.bound, prepared.intent));
  const rows = (await seamExecuteAsync(execAsync, prepared.sql, prepared.bound, prepared.intent)) as Array<Record<string, unknown>>;
  // #59 read de-box (async PG/MySQL): the SAME stamped map + coercion SSoT as the sync branch.
  return materializers !== undefined ? materializeRows(rows, materializers) : rows;
}

/**
 * The SOLE SQL transport leaf. Binds `params` and runs `sql` through the central seam. `write`
 * selects `run` (INSERT/UPDATE/DELETE, or BEGIN/COMMIT via the tx path) vs `execute` (SELECT /
 * RETURNING). A non-returning write returns a one-row `[{changes,lastInsertRowid}]` summary so the
 * leaf output shape is uniform (`items`). `connection` routes to a named DB (Phase C). `bigint`
 * runs the read in exact-integer mode (the #59 BIGINT-exact read; sync sqlite only). Before executing
 * it resolves any deferred PG array cast(s) from the real key arrays and renders `?`→`$N` for PostgreSQL.
 *
 * ONE leaf, sync + async: it branches on the injected ctx — a sync {@link LeafContext} (better-sqlite3,
 * via `bindBehaviors().run`) runs the sync seam and returns rows; an {@link AsyncLeafContext} (pooled
 * PG/MySQL, via `bindBehaviors().runAsync`) runs the async seam and returns a Promise. Not a parallel
 * leaf — the SAME prepare/encode SSoT feeds both. The async branch returns a Promise cast to the sync
 * return type to satisfy `defineLeaf`'s sync-typed gate (bc types leaves sync; `runAsync` awaits the
 * Promise impl at runtime — the sanctioned async-leaf shape).
 */
export const executeSQL = defineLeaf(
  'executeSQL',
  {
    cardinality: 'many' as const,
    ports: {
      sql: 'string' as const,
      // bc#156 (0.8.13): `value[]` is the input-only heterogeneous bound-value list — the native emitter
      // spreads it into the transport as `&[WireValue]` (a single `execute_sql(sql, params, …)` covers
      // every op with heterogeneous / empty params). For the ts ir-exec runtime `value` is opaque (bc
      // evaluates each element and passes it through); output de-box is BC-generated (#154), not here.
      params: { arr: 'value' as const },
      write: 'bool' as const,
      returning: 'bool' as const,
      bigint: 'bool' as const,
      connection: { opt: 'string' as const },
      // TRANSIENT authoring input: the WHERE fragment tree (bc records the where sugar → plain
      // Expression IR). The post-compile pass in `authoring.ts` lowers it from the RECORDED IR into
      // the static `sql`/`params` and STRIPS it, so a bounded op reaches the runtime as literal sql +
      // param-refs (native-lowerable). It must never survive to runtime (see the fn's fail-closed guard).
      where: { opt: { arr: 'string' as const } },
      // TRANSIENT authoring input (#59): `{obj:{table, cols}}` — the read's base table + explicit
      // projection. The post-compile pass in `authoring.ts` (`lowerReadColumns`) resolves each column
      // against the model's `static columns` (fail-closed: `*`/computed→raw, undeclared→throw — the
      // #59 read-column coverage guard), derives the `materializers` de-box map, stamps it below, and
      // STRIPS this port. It must never survive to runtime (see the fn's fail-closed guard).
      readColumns: { opt: { obj: {} } as const },
      // The TS read-path de-box map (#59): `outputKey → MaterializeClass` (BIGINT→string / DATE→string
      // / BOOLEAN→boolean; INT32/text/etc. omitted = no coercion). Stamped by `lowerReadColumns` from
      // the model's `static columns`; the fn coerces each output row cell through {@link materializeCell}.
      materializers: { opt: { obj: {} } as const },
    },
    output: { obj: {} } as const,
    // `params` carries a heterogeneous value list (the bc#156 opaque-value gap); the element type is
    // a nominal placeholder the ir-exec runtime does not enforce (values flow through opaquely).
    additionalPorts: false,
  },
  (p: { sql: string; params: unknown[]; write: boolean; returning: boolean; bigint: boolean; connection?: string | null; where?: unknown[] | null; readColumns?: unknown; materializers?: Record<string, MaterializeClass> | null }, ctx: LeafContext | AsyncLeafContext): Array<Record<string, unknown>> => {
    // The `where`/`readColumns` ports are compile-time-only inputs the post-compile passes lower/strip.
    // If either reaches runtime, that pass did not run — fail closed, never silently drop the predicate
    // (WHERE) or the read-column de-box (readColumns).
    if (p.where != null) {
      throw new Error('scp executeSQL: an unlowered `where` port reached runtime — the post-compile WHERE lowering did not run (bounded WHERE must lower to sql+params at compile; dynamic-WHERE runtime assembly is not yet wired).');
    }
    if (p.readColumns != null) {
      throw new Error('scp executeSQL: an unlowered `readColumns` port reached runtime — the post-compile read-column lowering (#59 de-box) did not run.');
    }
    const prepared = prepareSql(p, ctx.dialect);
    const materializers = p.materializers ?? undefined;
    // A NON-returning write (`write && !returning`) uses the `run` seam and yields the affected summary;
    // a RETURNING write (`write && returning`) and every read use the `execute` seam (it yields rows —
    // dropping to `run` would discard the RETURNING rows).
    const nonReturningWrite = p.write === true && p.returning !== true;
    if ('execAsync' in ctx) {
      // ASYNC (PG/MySQL) — return the Promise; `runAsync` awaits it (cast to the sync gate type).
      return executeSqlAsync(ctx.execAsync, prepared, nonReturningWrite, materializers) as unknown as Array<Record<string, unknown>>;
    }
    if (nonReturningWrite) return writeSummary(seamRun(ctx.exec, prepared.sql, prepared.bound, prepared.intent));
    // `bigint` runs the sync read in exact-integer mode (the #59 BIGINT-exact read).
    const rows = (p.bigint === true ? seamExecuteSafe(ctx.exec, prepared.sql, prepared.bound, prepared.intent) : seamExecute(ctx.exec, prepared.sql, prepared.bound, prepared.intent)) as Array<Record<string, unknown>>;
    // #59 read de-box: coerce each cell per the stamped `materializers` map (no-op when absent/empty).
    return materializers !== undefined ? materializeRows(rows, materializers) : rows;
  },
);

// ── pluck — rows + column → the deduped key array (the `= ANY($1)` batch key set) ──

/**
 * Extract the deduped, non-null key array from `rows[col]` — the batch key set a relation child
 * fetch binds to `WHERE fk = ANY($1)`. Insertion order preserved; a null/undefined key is dropped
 * (no partial keys). Dedupe is the shared grouping core ({@link dedupeKeyTuples}) — the SAME SSoT the
 * runtime lazy/relation path uses (no duplicated grouping).
 *
 * Keys pass through RAW (an INT key stays a JS `number` — so the PG cast infers `::int[]` byte-
 * identically to v1's `LazyRelation`, and the MySQL/SQLite JSON encode is a plain number array).
 * The authoring stamps the array's bc element type via `.as` from the key column (`{arr:'float'}`
 * for a JS-number key, `{arr:'string'}` for text/uuid) — the key array is an opaque transport value
 * bound to `= ANY($1)`, never a typed row, so its bc tag is the runtime JS value's tag, not the SQL
 * column type. The nominal `meta.output` here is the bc#156 opaque-value placeholder.
 */
export const pluck = defineLeaf(
  'pluck',
  { cardinality: 'one' as const, ports: { rows: { arr: 'value' } as const, col: 'string' as const }, output: { arr: 'string' as const } },
  (p: { rows: Array<Record<string, unknown>>; col: string }): string[] => {
    // bc#156 bridge: the key array is an opaque value list; its concrete element type is stamped by
    // the authoring `.as`. The cast satisfies the `defineLeaf` return-type gate until #156 lands an
    // opaque-value port type.
    return dedupeKeyTuples(p.rows, [p.col]).map((t) => t[0]) as unknown as string[];
  },
);

// ── group — parents + flat children → each parent with its children nested ─────

/**
 * Distribute a flat `children` list onto `parents` by matching `child[fk]` to `parent[pk]`, nesting
 * the result under `into`. `single:true` (belongsTo/hasOne) nests the one matching child (or `null`);
 * otherwise (hasMany) nests the child list (`[]` when none). Grouping is the shared core
 * ({@link groupByKey}/{@link attachToParent}) — the SAME SSoT the runtime lazy/relation path uses.
 */
export const group = defineLeaf(
  'group',
  {
    cardinality: 'many' as const,
    ports: {
      parents: { arr: 'value' } as const,
      children: { arr: 'value' } as const,
      pk: 'string' as const,
      fk: 'string' as const,
      into: 'string' as const,
      single: 'bool' as const,
    },
    output: { obj: {} } as const,
  },
  (p: { parents: Array<Record<string, unknown>>; children: Array<Record<string, unknown>>; pk: string; fk: string; into: string; single: boolean }): Array<Record<string, unknown>> => {
    const byKey = groupByKey(p.children, [p.fk]);
    return p.parents.map((par) => ({ ...par, [p.into]: attachToParent(par, [p.pk], byKey, p.single === true) }));
  },
);

/**
 * The op-independent leaf component functions for the authoring surface — `behaviorComponents`
 * derives the portable catalog from the `defineLeaf` annotations (no hand-written catalog; SA8 /
 * bc#126). Authoring calls `L.executeSQL({...})` / `L.pluck({...})` / `L.group({...})`.
 */
export const LEAVES = { executeSQL, pluck, group } as const;

/** Build the authoring leaf-component map (memoized at module scope — bound once). */
export const leafComponents = behaviorComponents(LEAVES);

/**
 * The native-codegen transport symbol table (`generateModule` `leafTransport.symbols`): each
 * op-independent leaf → the runtime symbol the covered native module calls directly. The consumer
 * supplies these (litedbmodel = a fixed set of ops, not one method per node — bc `LeafTransportOptions`).
 */
export const LEAF_TRANSPORT_SYMBOLS: Readonly<Record<string, string>> = {
  executeSQL: 'execute_sql',
  pluck: 'pluck_keys',
  group: 'group_children',
};
