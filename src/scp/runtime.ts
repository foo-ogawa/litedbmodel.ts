/**
 * litedbmodel v2 SCP — the thin TS runtime on the STATIC makeSQL bundle (WS3, #23; makeSQL
 * flip, epic #43/#45 Phase B; spec §3 / §10 / §11).
 *
 * makeSQL is the SOLE SCP compile/execute path. A behavior method compiles (SYMBOLICALLY — no
 * concrete input) to a {@link SqlBundle}:
 *   - a READ bundle carries a {@link ReadGraph} — the surrogate bc `ComponentGraphIR` (each SQL
 *     node → a `makeSQL` node) + per-node static statement templates. bc `runBehavior` drives
 *     map iteration / wire binding / Φ output; the makeSQL handler renders each node's statements
 *     (value-specs + skip evaluated per-input) and executes real SQL ("bc composes, makeSQL
 *     executes"). Read-relation batch ops ride the bundle too.
 *   - a WRITE bundle carries a single `makeSQL` statement (the base write) + the derived gate-first
 *     {@link TransactionPlan} for a Command with write-time relations.
 *
 * The reduced spine (`CompiledOperation` + `FragmentTree` + `renderOperation`) is RETIRED — this
 * module carries no reduced IR. Reads execute via {@link import('./makesql/static-bundle')} (bc
 * `runBehavior` + makeSQL handler); writes via {@link import('./makesql/tx')} (gate-first tx-DAG).
 *
 * ## Input normalization (SSoT — defaults live in the schema, not code)
 *
 * An OPTIONAL input head the caller omits is normalized to `null` (present-as-null) so a SKIP
 * presence expression drops its fragment (absent-key SKIP). "Optional" comes from the bundle's
 * `optionalHeads` (schema-optional + SKIP-guarded + every `refOpt` head) — the SSoT.
 */

import { type Scope, type Value } from 'behavior-contracts';
import type { BehaviorModelContract, Component } from './authoring';
import { SqlFailure } from './errors';
import {
  compileRelationOp,
  type RelationDecl,
  type RelationOp,
} from './relation';
import { buildResultSet, type ReadOptions } from './typed-object';
import type { DialectName } from './dialect';
import type { FindFilterSource } from './find-filter-guard';
import type { ColumnTypeResolver } from './coltype';
import {
  compileReadGraph,
  executeReadGraph,
  executeReadGraphAsync,
  executeStaticWrite,
  type ReadGraph,
  type StaticBundle,
  type StaticStatement,
  type SqliteDb as StaticSqliteDb,
  type SqlExecutorAsync,
} from './makesql/static-bundle';
import {
  compileWriteNode,
  deriveTransactionPlan,
  deriveBatchPlan,
  mysqlPkHint,
  executeTransaction,
  type BaseWrite,
  type TransactionPlan,
  type TransactionResult,
} from './makesql/tx';
import {
  compileInsertMany,
  compileUpdateMany,
  compileDeleteMany,
  type InsertManyBuildOptions,
} from './makesql/compile-crud';
import { assembleMakeSQL, type MakeSQL } from './makesql/makesql';
import { annotateWriteBundleOutType } from './makesql/writeouttype';
import type { UpdateManyBuildOptions } from '../drivers/types';
import {
  lifecycleFor,
  type EntityWritesDefinition,
  type LifecycleContract,
  type WriteLifecyclePhase,
} from './writes';

/** The minimal synchronous SQLite driver surface the runtime needs (better-sqlite3 `Database`). */
export type SqliteDb = StaticSqliteDb;

// ── Public runtime entrypoint ─────────────────────────────────────────────────

/** Execute options: the driver and the entry component (method) to run. */
export interface ExecuteOptions {
  /** The synchronous SQLite driver (better-sqlite3 `Database`). */
  readonly db: SqliteDb;
  /** The behavior method (component) name to run (default: the first component). */
  readonly entry?: string;
  /** The model's read-relation declarations (spec §4/§5), compiled ONCE into the bundle. */
  readonly relations?: readonly RelationDecl[];
  /** The target SQL dialect (spec §4/§10). Defaults to `'sqlite'` (the in-process runtime seam). */
  readonly dialect?: DialectName;
}

// ── The STATIC makeSQL bundle (§8 published artifact — the WS7 multi-language target) ──

/**
 * The compiled bundle of ONE behavior method (spec §8) — pure serializable JSON a thin
 * per-language runtime (bc + a SQL handler) can execute WITHOUT re-implementing litedbmodel's
 * compile.
 *
 *  - READ (`readGraph` present): the REAL Select-node `ComponentGraphIR` + per-node makeSQL statement
 *    templates; a native read-graph walker owns orchestration (never bc `runBehavior`), rendering +
 *    executing each node's statements.
 *  - WRITE (`statement` present): the single base-write makeSQL template; for a Command with
 *    write-time relations, `transaction` carries the derived gate-first plan.
 *  - `relations` — STATIC read-relation batch ops (spec §8 relation ops), keyed by name.
 *
 * `dialect` is the target SQL dialect (compiled ONCE, TS-side); a PG bundle's `?`→`$N` conversion
 * is the render-time final pass, so the bundle stays uniform and dialect-tagged.
 */
export interface SqlBundle {
  readonly dialect: DialectName;
  /** The behavior (component) name. */
  readonly name: string;
  /** READ bundles: the portable read graph (surrogate IR + per-node makeSQL statements). */
  readonly readGraph?: ReadGraph;
  /** WRITE bundles: the single base-write makeSQL statement template. */
  readonly statement?: StaticStatement;
  /** Optional input heads normalized to present-as-null (absent-key SKIP). */
  readonly optionalHeads: readonly string[];
  /** Pre-compiled STATIC read-relation batch ops, keyed by relation name (pure JSON). */
  readonly relations: Record<string, RelationOp>;
  /** The derived write-time-relations transaction plan (present ONLY for a write Command bundle). */
  readonly transaction?: TransactionPlan;
  /**
   * Codegen typed-de-box `outputType` for a WRITE bundle (spec §4.1 / §9): the bc portable type of
   * the write's {@link TransactionResult} (entity / returnedRows rows typed via the schema SoT). A
   * READ bundle carries its outType/outputType inside `readGraph.ir` instead; a write bundle has no
   * component-graph IR (#12: the makeSQL write surrogate is eliminated — writes ride the write/tx exec
   * path, not a codegen module), so its output type rides HERE on the bundle/companion. Present ONLY
   * when a column-type resolver was supplied at compile (additive/back-compat: absent → un-annotated).
   */
  readonly outputType?: unknown;
}

/** The primary catalog node of a component (the first non-cond, non-map body node). */
function primaryNodeOf(component: Component): Component['body'][number] | undefined {
  for (const n of component.body) {
    if ('cond' in n) continue;
    if ('map' in n) continue;
    return n;
  }
  return undefined;
}

/** Is a component's primary catalog node a write (Insert/Update/Delete)? */
function isWriteComponent(component: Component): boolean {
  const p = primaryNodeOf(component);
  if (p === undefined || 'map' in p) return false;
  const c = (p as { component: string }).component;
  return c === 'Insert' || c === 'Update' || c === 'Delete';
}

/**
 * Execute a published behavior contract (WS2 {@link BehaviorModelContract}) end-to-end via the
 * static makeSQL bundle. Returns the component's output (a read Φ output / row list, or a write's
 * RETURNING rows / `[{changes, lastInsertRowid}]` summary).
 *
 * @throws {SqlFailure} a mapped driver failure re-surfaced at the boundary.
 */
export function executeBehavior(
  contract: BehaviorModelContract,
  input: Scope,
  options: ExecuteOptions,
): Value {
  // ALWAYS-ON read de-box (issue #59), STATIC: the FAIL-CLOSED materialize resolver is precomputed
  // ONCE from the model's INLINE `static columns` declaration at registration and carried on the
  // contract — pure in-memory map lookups, ZERO per-read DB introspection. Coverage is enforced at
  // registration (a typed read whose projected columns aren't declared cannot be published), so
  // INT→number / BIGINT→string / DATE→string / BOOLEAN→boolean fires for every read — never a silent
  // raw (rounded i64) result from an undeclared column.
  return executeBundle(
    compileBundle(contract, options.entry, options.relations, options.dialect, undefined, contract.resolveColumnType),
    input,
    options,
  );
}

/**
 * Compile ONE behavior method into the serializable STATIC {@link SqlBundle} (spec §8) — the
 * published multi-language artifact. SYMBOLIC (no concrete input). A read method compiles to a
 * {@link ReadGraph}; a single-write method to a `makeSQL` statement. Pure JSON throughout.
 */
export function compileBundle(
  contract: BehaviorModelContract,
  entry?: string,
  relations: readonly RelationDecl[] = [],
  dialectName: DialectName = 'sqlite',
  findFilterModel?: FindFilterSource,
  resolveColumnType?: ColumnTypeResolver,
): SqlBundle {
  const component = entry ? contract.components.find((c) => c.name === entry) : contract.components[0];
  if (component === undefined) throw new Error(`scp runtime: entry component '${entry ?? '<first>'}' not found in contract`);

  const relationOps: Record<string, RelationOp> = {};
  for (const decl of relations) {
    if (relationOps[decl.name] !== undefined) {
      throw new Error(`scp runtime: duplicate relation declaration '${decl.name}'`);
    }
    // Bake the child column materializers (issue #59) from the SINGLE static column-type resolver at
    // compile — the relation batch's child rows then de-box with ZERO introspection, via the SAME
    // resolution the primary read uses (no separate materializer pass).
    relationOps[decl.name] = compileRelationOp({ ...decl, dialect: decl.dialect ?? dialectName }, resolveColumnType);
  }

  if (isWriteComponent(component)) {
    const writeNode = primaryNodeOf(component)!;
    const op = compileWriteNode(writeNode as never, dialectName);
    // The write's component GRAPH survives alongside its single compiled `statement`. Read and write
    // are ONE flow (they must not be split): the graph is what a codegen lowering needs in order to
    // bake the write's SQL into a native module, exactly as a read's does. It is ADDITIVE — mode-2
    // execution still dispatches on `statement` FIRST (see `executeBundle`), so the write's runtime
    // path is byte-unchanged; the graph is inert unless a codegen consumer asks for it.
    const graph = compileReadGraph(contract, dialectName, entry, findFilterModel, resolveColumnType);
    return {
      dialect: dialectName,
      name: component.name,
      statement: { sql: op.sql, params: op.params },
      readGraph: graph,
      optionalHeads: [],
      relations: relationOps,
    };
  }

  const readGraph = compileReadGraph(contract, dialectName, entry, findFilterModel, resolveColumnType);
  return {
    dialect: dialectName,
    name: readGraph.name,
    readGraph,
    optionalHeads: readGraph.optionalHeads,
    relations: relationOps,
  };
}

/**
 * Execute a {@link SqlBundle} (the §8 published artifact) end-to-end via the static makeSQL
 * runtime + REAL SQLite. Read bundles run the read graph via bc `runBehavior`; single-statement
 * write bundles run the write (RETURNING rows / summary). The SAME code path a thin per-language
 * runtime follows — it consumes ONLY the serialized bundle + bc, never re-running the compile.
 *
 * @throws {SqlFailure} a mapped driver failure re-surfaced at the boundary.
 */
export function executeBundle(bundle: SqlBundle, input: Scope, options: ExecuteOptions): Value {
  if (bundle.statement !== undefined && bundle.transaction === undefined) {
    const view: StaticBundle = {
      dialect: bundle.dialect,
      name: bundle.name,
      statements: [bundle.statement],
      optionalHeads: bundle.optionalHeads,
    };
    return executeStaticWrite(view, input, options.db) as unknown as Value;
  }
  if (bundle.readGraph === undefined) {
    throw new Error(`scp runtime: bundle '${bundle.name}' carries neither a read graph nor a write statement`);
  }
  return executeReadGraph(bundle.readGraph, input, options.db);
}

// ── Async read execution model — the PRODUCTION PG / MySQL read path (#40) ──────
//
// The sync `executeBundle` above rides the in-proc better-sqlite3 conformance path
// (`executeReadGraph` → bc `runBehavior`, SERIAL). The LIVE PG / MySQL execution model is
// ASYNC + pooled: `executeReadGraphAsync` → bc `runBehaviorAsync`, whose `runPlanAsync` stage
// dispatches the INDEPENDENT sibling read nodes of a plan stage in bounded parallel (bc#23,
// bounded by the plan's `concurrency`, default 16). Against a pooled async executor (built by
// `pgPoolExecutor` / `mysqlPoolExecutor`, each `exec` resolving on its own pooled connection),
// that becomes REAL parallel read-relation DB I/O — the same production fan-out the Go
// (`*sql.DB` pool + goroutine-parallel `RunPlan`) and Python (`ThreadPoolExecutor` `run_plan` +
// pooled driver) read paths already have. The result is deterministic (bc commits stage
// outcomes in declaration order), so it is byte-identical to the serial `executeBundle` output;
// only the wall-clock changes. Writes are UNCHANGED — the write-tx path stays sync + serial.

/** Async execute options: the pooled async SQL executor + the entry component (method) to run. */
export interface AsyncExecuteOptions {
  /** The pooled async SQL executor (build via `pgPoolExecutor` / `mysqlPoolExecutor`). */
  readonly exec: SqlExecutorAsync;
  /** The behavior method (component) name to run (default: the first component). */
  readonly entry?: string;
  /** The model's read-relation declarations (spec §4/§5), compiled ONCE into the bundle. */
  readonly relations?: readonly RelationDecl[];
  /** The target SQL dialect (spec §4/§10). For the live async path this is `'postgres'`/`'mysql'`. */
  readonly dialect?: DialectName;
}

/**
 * Execute a READ {@link SqlBundle} end-to-end via the ASYNC PG / MySQL execution model (#40): bc
 * `runBehaviorAsync` drives map iteration / wire binding / Φ output and dispatches independent
 * sibling read nodes in bounded parallel against the pooled async executor. This is the PRODUCTION
 * live-DB read entry point — the async twin of {@link executeBundle} for read bundles.
 *
 * Byte-identical composition (SAME IR, SAME per-node SQL text + params, SAME Φ output) to the
 * serial `executeBundle`; concurrency changes only the wall-clock. A single-relation (zero-sibling)
 * read graph runs exactly one query, so it is identical to the serial path either way.
 *
 * A single-statement WRITE bundle is NOT accepted here — the write-tx path stays sync + serial
 * (`executeBundle` / `executeTransactionBundle`); this async path is READ-only.
 *
 * @throws {SqlFailure} a mapped driver failure re-surfaced at the boundary.
 */
export async function executeBundleAsync(
  bundle: SqlBundle,
  input: Scope,
  options: AsyncExecuteOptions,
): Promise<Value> {
  if (bundle.readGraph === undefined) {
    throw new Error(
      `scp runtime: executeBundleAsync requires a READ bundle ('${bundle.name}' carries no read graph); ` +
        `the write-tx path stays synchronous + serial (use executeBundle / executeTransactionBundle).`,
    );
  }
  return executeReadGraphAsync(bundle.readGraph, input, options.exec);
}

/**
 * Compile + execute a READ behavior method via the ASYNC PG / MySQL execution model (#40). The
 * async twin of {@link executeBehavior} for reads: compile the contract to a {@link SqlBundle}
 * (SYMBOLIC), then run its read graph through {@link executeBundleAsync}.
 *
 * @throws {SqlFailure} a mapped driver failure re-surfaced at the boundary.
 */
export async function executeBehaviorAsync(
  contract: BehaviorModelContract,
  input: Scope,
  options: AsyncExecuteOptions,
): Promise<Value> {
  // ALWAYS-ON read de-box (issue #59), live PG/MySQL, STATIC: the materialize resolver is precomputed
  // from the model's DDL at registration and carried on the contract — ZERO per-read introspection
  // (no information_schema round-trips). PAIR with the per-CONNECTION driver de-box config, set ONCE
  // at pool creation (mysql2 supportBigNumbers+bigNumberStrings+dateStrings; pg date type parsers) —
  // see `pgDeboxExecutor` / `mysqlDeboxExecutor`.
  return executeBundleAsync(
    compileBundle(contract, options.entry, options.relations, options.dialect, undefined, contract.resolveColumnType),
    input,
    options,
  );
}

// ── Write-time relations: Command bundle + 1-tx execution (WS5, #25 — spec §6) ──

/**
 * Find the single base-write catalog node (`Insert`/`Update`/`Delete`) of a Command component.
 * WS5 initial scope is a single-statement Command (spec §6/§13). More than one write node ⇒ the
 * multi-write DAG derivation (WS8, rejected loudly here).
 */
function baseWriteNodeOf(component: Component): Component['body'][number] {
  const writes = component.body.filter(
    // `fanout` (bc 0.7.3+ FanoutNode) is excluded from narrowing here: litedbmodel never emits
    // it, and it carries no base write op, so it can never be a write node.
    (n) =>
      !('cond' in n) &&
      !('map' in n) &&
      !('fanout' in n) &&
      (n.component === 'Insert' || n.component === 'Update' || n.component === 'Delete'),
  );
  if (writes.length === 0) {
    throw new Error(`scp write: Command '${component.name}' has no base write (Insert/Update/Delete) node`);
  }
  if (writes.length > 1) {
    throw new Error(
      `scp write: Command '${component.name}' has ${writes.length} base write nodes; WS5 initial ` +
        `scope is a SINGLE-statement Command + write-time relations (multi-write DAG is WS8, spec §6/§13).`,
    );
  }
  return writes[0];
}

/**
 * Compile a Command method + its `entityWrites` save contract into a {@link SqlBundle} carrying
 * the derived, gate-first write-time-relations {@link TransactionPlan} (spec §6/§8). The base
 * write op is compiled ONCE (the SAME `compileWriteNode` — complete tuned SQL + deferred
 * Expression-IR params); {@link deriveTransactionPlan} lowers the lifecycle's §6 effect arrays
 * around it into the ordered statement list. Pure JSON; a WS7 runtime honors the SAME plan.
 */
export function compileWriteBundle(
  contract: BehaviorModelContract,
  entry: string,
  writes: EntityWritesDefinition,
  phase: WriteLifecyclePhase,
  dialectName: DialectName = 'sqlite',
  resolveColumnType?: ColumnTypeResolver,
): SqlBundle {
  const component = contract.components.find((c) => c.name === entry);
  if (component === undefined) throw new Error(`scp write: entry component '${entry}' not found in contract`);
  const lifecycle = lifecycleFor(writes, phase);
  if (lifecycle === undefined) {
    throw new Error(`scp write: the '${phase}' lifecycle is not declared in the entityWrites save contract`);
  }
  const writeNode = baseWriteNodeOf(component);
  // MySQL has no native RETURNING; when the base write is an INSERT…RETURNING carrying a PK
  // descriptor, annotate it with the strip-before-execute PK hint so the mysql emulation re-selects
  // by the REAL PK (not a hardcoded `id`). PG/SQLite keep the op verbatim (native RETURNING).
  const baseOp = dialectName === 'mysql' ? mysqlPkHint(compileWriteNode(writeNode as never, dialectName)) : compileWriteNode(writeNode as never, dialectName);
  const base: BaseWrite = { op: baseOp, label: `${(writeNode as { component: string }).component}` };
  const plan = deriveTransactionPlan(phase, [base], lifecycle, dialectName);

  const bundle: SqlBundle = {
    dialect: dialectName,
    name: component.name,
    statement: { sql: baseOp.sql, params: baseOp.params },
    optionalHeads: [],
    relations: {},
    transaction: plan,
  };
  // Codegen typed de-box (spec §4.1/§9): when the schema/DDL column-type SoT is supplied, annotate
  // the write bundle with the TransactionResult `outputType` (entity/returnedRows rows typed via the
  // resolver + the write's target/RETURNING) so bc's typed(-raw) emitters materialize a concrete
  // struct for the result. Fail-closed inside `annotateWriteBundleOutType`. Absent resolver → the
  // bundle stays un-annotated (interpret/boxed only, back-compat).
  return resolveColumnType === undefined ? bundle : annotateWriteBundleOutType(bundle, resolveColumnType);
}

/**
 * One member of a COMPOSITE (multi-write) Command (WS8a, #28 — spec §6 nested write / §14 tx-DAG):
 * a named base write (its sole Insert/Update/Delete node in `entry`) carrying its OWN save-contract
 * `effects`. A later member references an earlier member's RETURNING row via `$.ref.<name>.<field>`.
 */
export interface CompositeWriteEntry {
  readonly entry: string;
  readonly name: string;
  readonly lifecycle: LifecycleContract;
}

/**
 * Compile a COMPOSITE (multi-write) Command into a {@link SqlBundle} carrying ONE derived,
 * gate-first, TOPOLOGICALLY-ORDERED transaction plan (spec §6 nested write / §14). Each entry
 * contributes a named base write + its effects; a later member depends on an earlier via
 * `$.ref.<name>.*`. {@link deriveTransactionPlan} builds the DAG + gate-first constraint and
 * derives the ordered plan; a cycle / dangling ref is ESCALATED. Pure JSON.
 */
export function compileCompositeWriteBundle(
  contract: BehaviorModelContract,
  entries: readonly CompositeWriteEntry[],
  phase: WriteLifecyclePhase,
  dialectName: DialectName = 'sqlite',
): SqlBundle {
  if (entries.length < 2) {
    throw new Error('scp write: a composite write bundle needs at least 2 named write members (use compileWriteBundle for a single write).');
  }
  const bases: BaseWrite[] = [];
  let firstBaseOp: { sql: string; params: readonly unknown[] } | undefined;
  let firstName = '';
  for (const e of entries) {
    const component = contract.components.find((c) => c.name === e.entry);
    if (component === undefined) throw new Error(`scp write: entry component '${e.entry}' not found in contract`);
    if (firstBaseOp === undefined) firstName = component.name;
    const writeNode = baseWriteNodeOf(component);
    const baseOp = compileWriteNode(writeNode as never, dialectName);
    if (firstBaseOp === undefined) firstBaseOp = baseOp;
    bases.push({ op: baseOp, label: `${(writeNode as { component: string }).component} ${e.name}`, name: e.name, effects: e.lifecycle.effects });
  }
  const plan = deriveTransactionPlan(phase, bases, { effects: {} }, dialectName);

  return {
    dialect: dialectName,
    name: firstName,
    statement: { sql: firstBaseOp!.sql, params: firstBaseOp!.params },
    optionalHeads: [],
    relations: {},
    transaction: plan,
  };
}

// ── Batch writes: createMany / updateMany / deleteMany (a v1→v2 regression fix) ──
//
// A batch write is ONE LOGICAL operation that produces N grouped SQL statements (createMany with a
// heterogeneous column-set groups records into one INSERT per group, mirroring `DBModel._insert`;
// updateMany is one UNNEST/JSON/CASE statement; deleteMany is a PK-set IN-list DELETE). This is
// DISTINCT from the deferred composite multi-write DAG (`baseWriteNodeOf` still rejects >1 authored
// base write). The batch compilers (`compileInsertMany`/`compileUpdateMany`/`compileDeleteMany`) copy
// the v1 builders byte-for-byte; here they are lowered into a gate-free {@link TransactionPlan} of
// body statements ({@link deriveBatchPlan}), so ALL FIVE runtimes execute the multi-statement batch
// through the SAME per-statement tx loop with no runtime change (concrete params are literalized to
// bc IR so they survive the render pass — see `deriveBatchPlan`/`literalize`).

/** Flatten a batch compiler's `MakeSQL` component to a concrete `{ sql, params }` op. */
function flattenBatchOp(node: MakeSQL, label: string): { sql: string; params: readonly unknown[]; label: string } {
  const assembled = assembleMakeSQL(node);
  return { sql: assembled.sql, params: assembled.params, label };
}

/**
 * Compile a `createMany` into a batch write {@link SqlBundle} carrying a gate-free
 * {@link TransactionPlan}. Heterogeneous column-set groups become MULTIPLE ordered INSERT
 * statements — byte-identical to what `DBModel._insert` emits per group (via `compileInsertMany`).
 *
 * `pk` (the target PK descriptor) is REQUIRED when the createMany carries a RETURNING clause on the
 * MySQL dialect: a batch INSERT persists N rows, so the MySQL RETURNING emulation must re-select ALL
 * N (a range on the AUTO_INCREMENT column, or the client-supplied PK values), not a single `id`.
 */
export function compileCreateManyBundle(
  name: string,
  options: InsertManyBuildOptions & { pk?: { columns: readonly string[]; autoInc: string | null } },
  dialectName: DialectName = 'sqlite',
  resolveColumnType?: ColumnTypeResolver,
): SqlBundle {
  const components = compileInsertMany(dialectName, options);
  const ops = components.map((c, i) => {
    const flat = flattenBatchOp(c, `createMany group ${i}`);
    // On MySQL, annotate a RETURNING batch INSERT with the PK hint so the driver emulation
    // re-selects every inserted row of the group by the real PK.
    if (dialectName === 'mysql' && options.pk !== undefined && options.returning !== undefined) {
      return { ...flat, ...mysqlPkHint({ sql: flat.sql, params: flat.params, pk: options.pk }) };
    }
    return flat;
  });
  const plan = deriveBatchPlan('create', ops);
  // `ops` is never empty here: `deriveBatchPlan` throws on zero ops, and a `createMany` always groups
  // into >=1 INSERT. Fail LOUDLY rather than emitting an empty `sql` fallback (an empty-ops bundle is a
  // compile bug, not a default — 'defaults in schema, not code'). The `statement` mirrors the batch plan's
  // first (group-0) statement, matching the updateMany/deleteMany bundle shape below.
  const head = ops[0];
  if (head === undefined) {
    throw new Error(`scp write: compileCreateManyBundle('${name}') produced no INSERT statements (empty createMany grouping) — an empty-ops batch bundle is a compile bug, not a silent empty-SQL default.`);
  }
  const bundle: SqlBundle = { dialect: dialectName, name, statement: { sql: head.sql, params: head.params as unknown[] }, optionalHeads: [], relations: {}, transaction: plan };
  // Codegen typed de-box (spec §4.1/§9): annotate the batch write with the TransactionResult
  // `outputType` when the schema SoT resolver is supplied (returnedRows rows typed via the RETURNING
  // + target table). Fail-closed. Absent resolver → un-annotated (back-compat).
  return resolveColumnType === undefined ? bundle : annotateWriteBundleOutType(bundle, resolveColumnType);
}

/**
 * Compile an `updateMany` into a batch write {@link SqlBundle} (one UNNEST/JSON/CASE statement,
 * byte-identical to `compileUpdateMany` driving the v1 `buildUpdateMany` / JSON-batch builder).
 */
export function compileUpdateManyBundle(
  name: string,
  options: UpdateManyBuildOptions,
  dialectName: DialectName = 'sqlite',
): SqlBundle {
  const op = flattenBatchOp(compileUpdateMany(dialectName, options), 'updateMany');
  const plan = deriveBatchPlan('update', [op]);
  return { dialect: dialectName, name, statement: { sql: op.sql, params: op.params as unknown[] }, optionalHeads: [], relations: {}, transaction: plan };
}

/**
 * Compile a `deleteMany` into a batch write {@link SqlBundle} — a PK-set IN-list DELETE (single-key)
 * or one DELETE per composite-key group, driven by the v1 `DBConditions` builder (`compileDeleteMany`).
 */
export function compileDeleteManyBundle(
  name: string,
  options: { tableName: string; keyColumns: string[]; keys: Record<string, unknown>[]; returning?: string },
  dialectName: DialectName = 'sqlite',
): SqlBundle {
  const components = compileDeleteMany({ dialect: dialectName, ...options });
  const ops = components.map((c, i) => flattenBatchOp(c, `deleteMany group ${i}`));
  if (ops.length === 0) {
    // Empty key set: nothing to delete. Emit a no-op plan (zero statements is rejected by
    // deriveBatchPlan, so surface a single always-false DELETE keyed by the v1 empty IN-list is
    // NOT needed — an empty deleteMany simply has no statements). Represent as an empty transaction.
    return { dialect: dialectName, name, statement: { sql: '', params: [] }, optionalHeads: [], relations: {}, transaction: { phase: 'remove', entityFrom: null, statements: [], onIdempotentHit: 'rollback' } };
  }
  const plan = deriveBatchPlan('remove', ops);
  return { dialect: dialectName, name, statement: { sql: ops[0].sql, params: ops[0].params as unknown[] }, optionalHeads: [], relations: {}, transaction: plan };
}

/**
 * Execute a COMPOSITE (multi-write) Command end-to-end as ONE real SQLite transaction with gate-
 * first short-circuit + DAG-ordered statements (spec §6 / §14).
 */
export function executeCompositeCommand(
  contract: BehaviorModelContract,
  entries: readonly CompositeWriteEntry[],
  phase: WriteLifecyclePhase,
  input: Scope,
  options: ExecuteOptions,
): TransactionResult {
  const bundle = compileCompositeWriteBundle(contract, entries, phase, options.dialect);
  return executeTransactionBundle(bundle, input, options);
}

/**
 * Execute a Command end-to-end as ONE real SQLite transaction with gate-first short-circuit
 * (spec §6): compile the Command + its save contract into a {@link SqlBundle} with a transaction
 * plan, then run {@link executeTransactionBundle}. The v2 public write path.
 *
 * @throws {SqlFailure} a mapped driver failure (the transaction ROLLBACKs first).
 */
export function executeCommand(
  contract: BehaviorModelContract,
  writes: EntityWritesDefinition,
  phase: WriteLifecyclePhase,
  input: Scope,
  options: ExecuteOptions & { readonly entry: string },
): TransactionResult {
  const bundle = compileWriteBundle(contract, options.entry, writes, phase, options.dialect);
  return executeTransactionBundle(bundle, input, options);
}

/**
 * Execute a {@link SqlBundle}'s derived transaction plan (spec §6/§8) as ONE real SQLite
 * transaction. The SAME code path a thin per-language runtime follows: it consumes ONLY the
 * serialized {@link TransactionPlan} (pure JSON) + a SQL driver, never re-deriving the plan.
 *
 * @throws if the bundle carries no transaction plan, or {@link SqlFailure} on a driver failure.
 */
export function executeTransactionBundle(bundle: SqlBundle, input: Scope, options: ExecuteOptions): TransactionResult {
  if (bundle.transaction === undefined) {
    throw new Error('scp write: this bundle carries no transaction plan (not a write-time-relations Command bundle)');
  }
  return executeTransaction(options.db, bundle.transaction, input, bundle.dialect);
}

// ── typed-object read surface (WS4, #24 — result + Read relations) ────────────

/** Options for the typed-object {@link read} surface: base execution + relation read opts. */
export interface ReadRuntimeOptions<R = Record<string, unknown>> extends ExecuteOptions, ReadOptions<R> {}

/**
 * The v2 typed-object read surface (spec §4/§5/§12): run a read behavior whose output is a row
 * list (a Select), then wrap each raw row in a plain TYPED-OBJECT with read relations attached.
 * A relation named in `options.with` is declaratively batch-prefetched; every other declared
 * relation is lazy (a prototype getter firing the SAME compiled op over the sibling set). Both
 * surfaces share the ONE compiled relation op in the bundle.
 *
 * @throws if the behavior output is not a row list (the typed-object read surface is for reads).
 */
export function read<R = Record<string, unknown>>(
  contract: BehaviorModelContract,
  input: Scope,
  options: ReadRuntimeOptions<R>,
): R[] {
  // ALWAYS-ON read de-box (issue #59), STATIC: the resolver is precomputed from the model's DDL at
  // registration and carried on the contract — ZERO per-read introspection.
  return readBundle(
    compileBundle(contract, options.entry, options.relations, options.dialect, undefined, contract.resolveColumnType),
    input,
    options,
  );
}

/** {@link read} against an already-compiled {@link SqlBundle} (the published §8 artifact). */
export function readBundle<R = Record<string, unknown>>(
  bundle: SqlBundle,
  input: Scope,
  options: ReadRuntimeOptions<R>,
): R[] {
  const out = executeBundle(bundle, input, options);
  if (!Array.isArray(out)) {
    throw new Error(
      `scp read: the read behavior output is not a row list (got ${out === null ? 'null' : typeof out}); ` +
        `the typed-object read surface expects a Select-shaped output`,
    );
  }
  const rawRows = out as unknown as Record<string, unknown>[];
  const readOpts: ReadOptions<R> = {
    ...(options.with !== undefined ? { with: options.with } : {}),
    ...(options.hydrate !== undefined ? { hydrate: options.hydrate } : {}),
    // CROSS-DB relations (V0 R1): forward the connection registry so a tagged relation routes to
    // its target DB. Absent for a single-DB read (every relation runs on the primary `db`).
    ...(options.connections !== undefined ? { connections: options.connections } : {}),
  };
  return buildResultSet<R>(rawRows, bundle.relations, options.db, readOpts);
}

export { SqlFailure };
