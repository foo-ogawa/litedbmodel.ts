/**
 * litedbmodel v2 SCP — the thin TS runtime (WS3, #23; spec §3 / §10 / §11).
 *
 * Consumes bc's runtime-core (`runBehavior` = plan stage execution + map/wire/skip
 * propagation + Φ output assembly) and adds ONLY the SQL-backend concerns (spec §11):
 * Backend-Compile bridge (WS1↔WS3), per-Catalog Handlers (render → driver execute →
 * row→model assembly), and Error Mapping. It re-implements NO generic execution.
 *
 * ## Execution pipeline (spec §3)
 *
 *   validate → fragment select (SKIP) → array expand → Expression eval → bind → SQL execute
 *   → assembly
 *
 * bc's `runBehavior` owns the orchestration (which node runs when, map iteration, wire
 * binding, output merge). But the SQL-structural ports (`where` fragment tree, `limit`, …)
 * MUST NOT go through bc's generic port evaluation — a `where` `{arr:[…]}` of comparison
 * Expressions would evaluate to bare booleans (losing the SQL structure), and a SKIP guard
 * over an ABSENT optional input would fail-closed with `UNKNOWN_BINDING`. So the runtime:
 *
 *  1. Backend-Compiles every SQL node up front ({@link import('./bridge').compileNode}) to a
 *     WS1 `CompiledOperation` (SQL text + fragment tree + closed-set param slots).
 *  2. Rewrites the IR into a SURROGATE graph: each SQL node keeps its `id` / wiring / `map`
 *     shape, but its ports collapse to ONE synthetic `__scope` port — a bc `{obj:…}` that
 *     re-exports every binding head the op references (`input` names, wire node ids, the
 *     `map` element `as`). bc evaluates `__scope` in ITS scope (so `input`, sibling results
 *     and the map element all resolve through bc's own machinery) and hands the plain scope
 *     to the handler. No SQL-structural port is ever evaluated by bc.
 *  3. Each handler renders its pre-compiled op against `ports.__scope` via WS1's normative
 *     {@link renderOperation} (fragment select + array expand + Expression eval), binds the
 *     params through the driver, executes REAL SQL, and returns the row list (assembly).
 *
 * ## Input normalization (SSoT — defaults live in the schema, not code)
 *
 * An OPTIONAL Input Port that the caller omits is normalized to `null` (present-as-null) so
 * a SKIP guard `{ne:[{refOpt:[port]}, null]}` evaluates to `false` and drops its fragment —
 * absent-key SKIP via `refOpt`. This is driven by the component's `inputPorts` schema
 * (`required !== true` ⇒ optional), NOT an ad-hoc `?? null` in engine code: the schema is
 * the single source of truth for which ports are optional.
 */

import {
  runBehavior,
  type Component,
  type ComponentGraphIR,
  type Handlers,
  type HandlerCtx,
  type Scope,
  type Value,
  type ExecOutcome,
} from 'behavior-contracts';
import type { BehaviorModelContract } from './authoring';
import { compileComponentNodes } from './bridge';
import { renderOperation } from './render';
import type { CompiledOperation, ExprNode, Fragment, FragmentTree } from './ir';
import { mapSqliteError, SqlFailure } from './errors';
import { compileRelationOp, runRelationOp, distributeToParent, type RelationDecl, type RelationOp, type RelationDriver } from './relation';
import { buildResultSet, type ReadOptions } from './typed-object';

/** The synthetic port that carries a SQL node's render scope (see module doc). */
const SCOPE_PORT = '__scope';

/**
 * The minimal synchronous SQLite driver surface the runtime needs (better-sqlite3
 * `Database`). `all` returns the row list of a SELECT / RETURNING statement; `run` executes
 * a non-returning write and reports `changes` + `lastInsertRowid`. In-process, synchronous
 * — the sanctioned in-proc substitute for a docker integration DB (#23 AC).
 */
export interface SqliteDb {
  prepare(sql: string): {
    all(...params: unknown[]): unknown[];
    run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
    reader?: boolean;
  };
}

// ── Value normalization at the SQL boundary ───────────────────────────────────

/**
 * bc evaluates integers to `bigint`. Convert a rendered param to a driver-bindable value:
 * a safe-range `bigint` → JS `number` (v1 uses numbers; keeps result parity), an
 * out-of-safe-range `bigint` stays `bigint` (better-sqlite3 binds it losslessly). Arrays /
 * objects are IN-list elements / already-flat scalars; strings / booleans / null pass
 * through. This mirrors graphddb's `toPlain` boundary conversion.
 */
function toDriverParam(v: Value): unknown {
  if (typeof v === 'bigint') {
    if (v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(v);
    return v; // better-sqlite3 binds a BigInt losslessly for i64-range values
  }
  return v;
}

// ── Ref-head collection (build the surrogate `__scope` port) ──────────────────

/** Collect every ref/refOpt path HEAD used inside an Expression IR node. */
function collectHeads(node: unknown, heads: Set<string>): void {
  if (node === null || typeof node !== 'object') return;
  if (Array.isArray(node)) {
    for (const el of node) collectHeads(el, heads);
    return;
  }
  const keys = Object.keys(node);
  if (keys.length === 1 && (keys[0] === 'ref' || keys[0] === 'refOpt')) {
    const path = (node as Record<string, unknown>)[keys[0]];
    if (Array.isArray(path) && typeof path[0] === 'string') heads.add(path[0]);
    return;
  }
  for (const k of keys) collectHeads((node as Record<string, unknown>)[k], heads);
}

/**
 * Collect every path HEAD accessed via `refOpt` in an Expression IR node. A `refOpt` head
 * is OPTIONAL by construction — the author reached for `opt(…)` / `refOpt` precisely because
 * the binding may be absent (a `coalesce`-defaulted LIMIT, a SKIP presence guard). It is
 * therefore normalized to present-as-null (SSoT: the `refOpt` is the optionality
 * declaration, not an ad-hoc code default).
 */
function collectRefOptHeads(node: unknown, heads: Set<string>): void {
  if (node === null || typeof node !== 'object') return;
  if (Array.isArray(node)) {
    for (const el of node) collectRefOptHeads(el, heads);
    return;
  }
  const keys = Object.keys(node);
  if (keys.length === 1 && keys[0] === 'refOpt') {
    const path = (node as Record<string, unknown>).refOpt;
    if (Array.isArray(path) && typeof path[0] === 'string') heads.add(path[0] as string);
    return;
  }
  for (const k of keys) collectRefOptHeads((node as Record<string, unknown>)[k], heads);
}

/** Collect ref heads from a whole fragment tree (its `when` guards + param slots). */
function collectFragmentHeads(node: Fragment | FragmentTree, heads: Set<string>): void {
  if ('connector' in node) {
    for (const f of node.fragments) collectFragmentHeads(f, heads);
    return;
  }
  if (node.when !== undefined) collectHeads(node.when, heads);
  for (const p of node.params) collectHeads(p as ExprNode, heads);
}

/** Every binding head a compiled op references (static params + fragment tree). */
function operationHeads(op: CompiledOperation): Set<string> {
  const heads = new Set<string>();
  for (const p of op.params) collectHeads(p as ExprNode, heads);
  if (op.where !== null) collectFragmentHeads(op.where, heads);
  return heads;
}

/**
 * Collect the binding heads that live UNDER a SKIP guard: every head in a `when`-guarded
 * fragment's guard OR its params. A SKIP guard is the structural declaration that its
 * driving input is OPTIONAL (spec §7: `cond ? [...] : SKIP`) — so an absent such head is
 * normalized to null (present-as-null), letting `refOpt` drop the fragment. This is
 * schema/structure-driven, not an ad-hoc code default: the SKIP fragment IS the SSoT that
 * the input is optional.
 */
function skipGuardedHeads(node: Fragment | FragmentTree, heads: Set<string>): void {
  if ('connector' in node) {
    for (const f of node.fragments) skipGuardedHeads(f, heads);
    return;
  }
  if (node.when !== undefined) {
    collectHeads(node.when, heads);
    for (const p of node.params) collectHeads(p as ExprNode, heads);
  }
}

/** Collect every `refOpt` head across a compiled op (static params + fragment tree). */
function operationRefOptHeads(op: CompiledOperation, heads: Set<string>): void {
  for (const p of op.params) collectRefOptHeads(p as ExprNode, heads);
  if (op.where !== null) collectFragmentRefOptHeads(op.where, heads);
}

/** Collect `refOpt` heads from a whole fragment tree (guards + param slots). */
function collectFragmentRefOptHeads(node: Fragment | FragmentTree, heads: Set<string>): void {
  if ('connector' in node) {
    for (const f of node.fragments) collectFragmentRefOptHeads(f, heads);
    return;
  }
  if (node.when !== undefined) collectRefOptHeads(node.when, heads);
  for (const p of node.params) collectRefOptHeads(p as ExprNode, heads);
}

/**
 * All OPTIONAL heads across a set of compiled ops: the SKIP-guarded heads (a `when`-guarded
 * fragment declares its driving input optional) plus every `refOpt`-accessed head (the
 * author declared it optional via `opt(…)`). These are normalized to present-as-null so
 * `refOpt` drops/defaults cleanly instead of failing on an absent head.
 */
function optionalHeadsOf(compiled: Record<string, CompiledOperation>): Set<string> {
  const heads = new Set<string>();
  for (const op of Object.values(compiled)) {
    if (op.where !== null) skipGuardedHeads(op.where, heads);
    operationRefOptHeads(op, heads);
  }
  return heads;
}

/**
 * Build the surrogate `__scope` port for a SQL node: a bc `{obj:{…}}` re-exporting each
 * referenced head as `{ref:[head]}`. Excludes the IN-sentinel head (`@in` is a column
 * marker, never a real binding). bc evaluates this in its own scope, so `input` names,
 * wire node ids, and the `map` element `as` all resolve; the handler then renders the op
 * against the resulting plain scope. A head that is genuinely absent surfaces as bc's
 * `UNKNOWN_BINDING` — the runtime pre-normalizes OPTIONAL input heads to null so only a
 * real wiring bug reaches that fail-closed path.
 */
function scopePort(op: CompiledOperation): unknown {
  const obj: Record<string, unknown> = {};
  for (const head of operationHeads(op)) {
    if (head === '@in') continue; // IN-list sentinel column marker, not a binding
    obj[head] = { ref: [head] };
  }
  return { obj };
}

/** Rewrite one body node to its surrogate (SQL nodes → single `__scope` port). */
function surrogateNode(node: Component['body'][number], compiled: Record<string, CompiledOperation>): Component['body'][number] {
  if ('cond' in node) return node; // pure Expression — bc evaluates it unchanged
  if ('map' in node) {
    const op = compiled[node.id];
    if (op === undefined) return node;
    return { ...node, map: { ...node.map, ports: { [SCOPE_PORT]: scopePort(op) } } };
  }
  const op = compiled[node.id];
  if (op === undefined) return node;
  return { ...node, ports: { [SCOPE_PORT]: scopePort(op) } };
}

/** Rewrite a whole component to its surrogate graph (structure + wiring preserved). */
function surrogateComponent(component: Component, compiled: Record<string, CompiledOperation>): Component {
  return { ...component, body: component.body.map((n) => surrogateNode(n, compiled)) };
}

// ── Handlers (render → execute → assembly) ────────────────────────────────────

/**
 * Execute one rendered SQL statement against the driver and return the row list (assembly).
 * A SELECT / RETURNING statement returns its rows; a non-returning write returns a
 * single-row summary `[{ changes, lastInsertRowid }]` (the RETURNING-less write shape). A
 * driver error is mapped ({@link mapSqliteError}) and returned as bc `{ error }` so the
 * node's Policy Kind governs propagation.
 */
function executeRendered(db: SqliteDb, op: CompiledOperation, scope: Scope): ExecOutcome {
  const rendered = renderOperation(op, scope);
  const params = rendered.params.map(toDriverParam);
  let stmt: ReturnType<SqliteDb['prepare']>;
  try {
    stmt = db.prepare(rendered.sql);
  } catch (e) {
    return { error: mapSqliteError(e).message };
  }
  const hasReturn = op.component === 'Select' || /\breturning\b/i.test(rendered.sql);
  try {
    if (hasReturn) {
      const rows = stmt.all(...params) as Record<string, unknown>[];
      return { ok: rows as unknown as Value };
    }
    const info = stmt.run(...params);
    return {
      ok: [{ changes: info.changes, lastInsertRowid: toDriverParam(BigInt(info.lastInsertRowid)) }] as unknown as Value,
    };
  } catch (e) {
    return { error: mapSqliteError(e).message };
  }
}

/** Build the SQL handler registry: one handler per SQL Catalog name (spec §11 item 4). */
function buildHandlers(db: SqliteDb, compiled: Record<string, CompiledOperation>): Handlers {
  const handle = (ports: Record<string, Value>, ctx: HandlerCtx): ExecOutcome => {
    const op = compiled[ctx.nodeId];
    if (op === undefined) {
      return { error: `scp runtime: no compiled operation for node '${ctx.nodeId}' (${ctx.component})` };
    }
    const scope = ports[SCOPE_PORT];
    if (scope === null || typeof scope !== 'object' || Array.isArray(scope)) {
      return { error: `scp runtime: node '${ctx.nodeId}' surrogate scope did not evaluate to an object` };
    }
    return executeRendered(db, op, scope as Scope);
  };
  // One binding per SQL CRUD Catalog name (all share the render→execute handler; the
  // pre-compiled op keyed by nodeId already encodes the per-node operation).
  return { Select: handle, Insert: handle, Update: handle, Delete: handle };
}

// ── Input normalization (schema-driven — SSoT) ────────────────────────────────

/**
 * Normalize the caller input to `null` (present-as-null) for every OPTIONAL binding the
 * caller omitted, so a SKIP guard using `refOpt` evaluates to `false` and drops its
 * fragment (absent-key SKIP). "Optional" is determined from the SSoT, NOT an ad-hoc code
 * default — a head is optional iff EITHER (a) the component's Input Port schema marks the
 * port `required !== true`, OR (b) the head is SKIP-guarded in a compiled fragment (the
 * SKIP fragment declares its driving input optional — spec §7). A REQUIRED, non-SKIP head
 * that is missing is left absent so a real wiring bug surfaces loudly as bc's
 * `UNKNOWN_BINDING` rather than being silently defaulted.
 */
function normalizeInput(component: Component, optionalHeads: Set<string>, input: Scope): Scope {
  const out: Scope = { ...input };
  for (const [port, schema] of Object.entries(component.inputPorts)) {
    if (schema.required !== true && !(port in out)) out[port] = null;
  }
  for (const head of optionalHeads) {
    if (!(head in out)) out[head] = null;
  }
  return out;
}

// ── Public runtime entrypoint ─────────────────────────────────────────────────

/** Execute options: the driver and the entry component (method) to run. */
export interface ExecuteOptions {
  /** The synchronous SQLite driver (better-sqlite3 `Database`). */
  readonly db: SqliteDb;
  /** The behavior method (component) name to run (default: the first component). */
  readonly entry?: string;
  /**
   * The model's read-relation declarations (spec §4/§5). Compiled ONCE into the bundle's
   * relation ops so the typed-object read surface ({@link read}) can batch-resolve both
   * declarative-select and lazy relations via the SAME compiled op.
   */
  readonly relations?: readonly RelationDecl[];
}

// ── Backend-Compiled bundle (§8 published artifact — the WS7 multi-language target) ──

/**
 * The Backend-Compiled bundle of ONE behavior method (spec §8): the fully compiled SQL IR a
 * thin per-language runtime (bc runtime-core + a SQL handler) can execute WITHOUT
 * re-implementing litedbmodel's Backend-Compile. It is pure serializable JSON:
 *
 *  - `component` — the bc component's WIRING ONLY (body node ids / parent / map `over`+`as` /
 *    `cond` / output / plan / inputPorts). The catalog nodes' original Expression-IR ports
 *    are replaced by the surrogate `__scope` port so a consumer runtime feeds bc's
 *    `runBehavior` the plan/map/wire orchestration with NO SQL-structural port to mis-evaluate.
 *  - `operations` — `nodeId → CompiledOperation` (§8: `sql` with the `{where}` splice,
 *    `where` fragment tree with existence rules, `params` static slots, `assembly`). Param
 *    slots stay as Expression IR (`{coalesce:[…]}`, `{ref:[…]}`) so bc runtime-core owns
 *    value/SKIP evaluation per language (they are NOT pre-evaluated to literals).
 *  - `optionalHeads` — the input heads normalized to present-as-null (absent-key SKIP), so a
 *    consumer runtime reproduces the same normalization from the bundle (not from TS state).
 *
 * `dialect` is `'sqlite'` for α (the dialect axis is compiled once, TS-side — spec §10).
 */
export interface SqlBundle {
  readonly irVersion: number;
  readonly exprVersion: number;
  readonly dialect: 'sqlite';
  /** The surrogate component (wiring/plan/output only; catalog ports → `__scope`). */
  readonly component: Component;
  /** Backend-Compiled SQL IR per catalog node id (§8 shape). */
  readonly operations: Record<string, CompiledOperation>;
  /** Optional input heads normalized to present-as-null (absent-key SKIP). */
  readonly optionalHeads: string[];
  /**
   * Pre-compiled read-relation batch ops (spec §8 `relation ops`), keyed by relation name.
   * Derived ONCE (TS-side) from the model relation declarations, dialect-aware (SQLite).
   * Pure JSON (each op's `query` is a {@link CompiledOperation}) so a thin per-language
   * runtime gets the relation batch SQL for free (WS7). BOTH read surfaces (declarative
   * select + lazy) resolve through these ops — the identical compiled op.
   */
  readonly relations: Record<string, RelationOp>;
}

/**
 * Execute a published behavior contract (WS2 {@link BehaviorModelContract}) end-to-end:
 * Backend-Compile every SQL node → surrogate IR → bc `runBehavior` (plan / map / wire /
 * output) with SQL handlers → REAL SQLite execution → assembled Φ output.
 *
 * This is α's vertical slice: authoring → IR → Backend-Compile → thin runtime → real
 * SQLite → assembly. The returned Value is the component's `output` (Φ merge) with each SQL
 * node's slot filled by its executed row list.
 *
 * @throws {SqlFailure} a mapped driver failure re-surfaced at the boundary (a `fail`-policy
 *   node re-throws through `runPlan` as `OP_FAILED`; the runtime unwraps it back to the
 *   structured {@link SqlFailure} so the caller sees `kind` / `policy` / `sqliteCode`).
 */
export function executeBehavior(
  contract: BehaviorModelContract,
  input: Scope,
  options: ExecuteOptions,
): Value {
  return executeBundle(compileBundle(contract, options.entry, options.relations), input, options);
}

/**
 * Backend-Compile ONE behavior method of a contract into the serializable {@link SqlBundle}
 * (spec §8) — the published multi-language artifact. This runs the WS1↔WS3 bridge and the
 * surrogate rewrite ONCE, on the TS side (spec §10: the dialect axis is compiled once), and
 * emits pure JSON: no TS runtime state is captured, so `JSON.stringify(bundle)` round-trips
 * losslessly and a thin per-language runtime can execute it with bc runtime-core + a SQL
 * handler alone (proven by the bundle round-trip test).
 */
export function compileBundle(
  contract: BehaviorModelContract,
  entry?: string,
  relations: readonly RelationDecl[] = [],
): SqlBundle {
  const component = entry
    ? contract.components.find((c) => c.name === entry)
    : contract.components[0];
  if (component === undefined) {
    throw new Error(`scp runtime: entry component '${entry ?? '<first>'}' not found in contract`);
  }
  const operations: Record<string, CompiledOperation> = {};
  for (const [id, op] of compileComponentNodes(component)) operations[id] = op;
  const surrogate = surrogateComponent(component, operations);
  // Backend-Compile every relation declaration ONCE into a pure-JSON relation op (spec §8).
  const relationOps: Record<string, RelationOp> = {};
  for (const decl of relations) {
    if (relationOps[decl.name] !== undefined) {
      throw new Error(`scp runtime: duplicate relation declaration '${decl.name}'`);
    }
    relationOps[decl.name] = compileRelationOp(decl);
  }
  return {
    irVersion: contract.ir.irVersion,
    exprVersion: contract.ir.exprVersion,
    dialect: 'sqlite',
    component: surrogate,
    operations,
    optionalHeads: [...optionalHeadsOf(operations)],
    relations: relationOps,
  };
}

/**
 * Execute a {@link SqlBundle} (the §8 published artifact) end-to-end: feed bc `runBehavior`
 * the bundle's surrogate component (plan / map / wire / output orchestration) with SQL
 * handlers that render the bundle's `CompiledOperation`s and run REAL SQLite. This is the
 * SAME code path a thin per-language runtime follows — it consumes ONLY the serialized
 * bundle + bc runtime-core, never re-running litedbmodel's Backend-Compile. The bundle
 * round-trip test parses this from JSON (no TS state) to prove self-sufficiency (WS7).
 *
 * @throws {SqlFailure} a mapped driver failure re-surfaced at the boundary.
 */
export function executeBundle(bundle: SqlBundle, input: Scope, options: ExecuteOptions): Value {
  const surrogate = bundle.component;
  const ir: ComponentGraphIR = {
    irVersion: bundle.irVersion as 1,
    exprVersion: bundle.exprVersion,
    components: [surrogate],
  };
  const handlers = buildHandlers(options.db, bundle.operations);
  const optionalHeads = new Set(bundle.optionalHeads);
  const normalized = normalizeInput(surrogate, optionalHeads, input);

  try {
    return runBehavior(ir, handlers, normalized, surrogate.name);
  } catch (e) {
    // A `fail`-policy node's `{error}` re-throws through runPlan as `OP_FAILED` carrying the
    // mapped-failure message. Re-surface the structured SqlFailure from the message-embedded
    // code if present; otherwise re-throw verbatim.
    throw reErrorToSqlFailure(e);
  }
}

// ── typed-object read surface (WS4, #24 — result + Read relations) ────────────

/** Options for the typed-object {@link read} surface: base execution + relation read opts. */
export interface ReadRuntimeOptions<R = Record<string, unknown>> extends ExecuteOptions, ReadOptions<R> {}

/**
 * The v2 typed-object read surface (spec §4/§5/§12): run a read behavior whose output is a
 * row list (a Select), then wrap each raw row in a plain TYPED-OBJECT (own props = data only,
 * NOT a DBModel instance) with read relations attached.
 *
 * - A relation named in `options.with` is DECLARATIVELY selected: batch-prefetched ONCE over
 *   the whole page (staged, no N+1) and attached as an OWN prop.
 * - Every OTHER declared relation is LAZY: a prototype getter that, on `await result.author`,
 *   fires the SAME compiled relation op over the sibling set (batched — still no N+1).
 * - `options.hydrate` recovers host objects (`(raw) => new Domain(raw)`) — the consumer-side
 *   method-UX recovery. It stays in the runtime and never enters the bundle.
 *
 * Both surfaces share the ONE compiled relation op in the bundle (spec §5). The base row list
 * is produced by the SAME {@link executeBundle} path (bc runtime-core + SQL handlers), so the
 * read query itself is fully IR-driven; relations are the runtime's staged-batch concern.
 *
 * @throws if the behavior output is not a row list (the typed-object surface is for reads).
 */
export function read<R = Record<string, unknown>>(
  contract: BehaviorModelContract,
  input: Scope,
  options: ReadRuntimeOptions<R>,
): R[] {
  return readBundle(compileBundle(contract, options.entry, options.relations), input, options);
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
  };
  return buildResultSet<R>(rawRows, bundle.relations, options.db, readOpts);
}

// ── Staged-batch relation resolution THROUGH bc's plan (spec §5) ──────────────

/** The synthetic catalog name of a batched-relation map node (runtime-internal, not authored). */
const RELATION_BATCH_COMPONENT = '__RelationBatch';
/** The port a batched-relation map element evaluates to: the parent's batch-key value. */
const RELATION_KEY_PORT = '__key';

/**
 * Resolve ONE relation for a page of parent rows by driving bc's PLAN mechanism (spec §5:
 * `deriveExecutionPlan` — result path → stage groups + concurrency). This is the staged-batch
 * path: it builds a bc component with a single `map.batched` node over the parent rows and
 * runs it through {@link runBehavior} (the SAME multi-language execution core WS7 languages
 * use). Because the map is `batched`, bc evaluates every parent's key port and invokes the
 * relation handler EXACTLY ONCE with `{items:[…]}` — so the relation op fires ONE batched
 * query for the whole page (structurally no N+1, enforced by bc's batch contract). The
 * handler runs the IDENTICAL compiled {@link RelationOp} the declarative-select and lazy
 * surfaces use, and `into` attaches each parent's resolved child(ren).
 *
 * Returns the parent rows augmented with the relation under `op.name` (per cardinality).
 */
export function resolveRelationViaPlan(
  op: RelationOp,
  parents: readonly Record<string, unknown>[],
  db: RelationDriver,
): Record<string, unknown>[] {
  // One batched map node over the parent list. Each element evaluates its batch-key port
  // (`{ref:[<elemAs>, parentKey]}`); the batched handler receives all keys at once.
  const elemAs = '$p';
  const node: Component['body'][number] = {
    id: 'rel',
    map: {
      over: { ref: ['__parents'] },
      as: elemAs,
      component: RELATION_BATCH_COMPONENT,
      ports: { [RELATION_KEY_PORT]: { ref: [elemAs, op.parentKey] } },
      into: op.name,
      batched: true,
    },
  } as Component['body'][number];
  const component: Component = {
    name: '__resolveRelation',
    inputPorts: { __parents: { required: true } },
    body: [node],
    output: { ref: ['rel'] },
  } as unknown as Component;
  const ir: ComponentGraphIR = {
    irVersion: 1,
    exprVersion: 1,
    components: [component],
  } as ComponentGraphIR;

  // The batched-relation handler: bc calls it ONCE with every parent's evaluated key port.
  // It runs the relation op's batch query a SINGLE time and returns per-parent results aligned
  // to `items` (bc's MAP_BATCH_RESULT_MISMATCH enforces the 1:1 alignment).
  const handlers: Handlers = {
    [RELATION_BATCH_COMPONENT]: (portsOrItems: Record<string, Value>): ExecOutcome => {
      const items = (portsOrItems as { items?: unknown }).items;
      if (!Array.isArray(items)) {
        return { error: `${RELATION_BATCH_COMPONENT}: expected a batched {items:[…]} invocation` };
      }
      // Reconstruct the parent-key-bearing rows from the evaluated key ports, run the op ONCE.
      const keyRows = items.map((it) => ({ [op.parentKey]: (it as Record<string, unknown>)[RELATION_KEY_PORT] }));
      const { batch } = runRelationOp(op, keyRows, db);
      const aligned = keyRows.map((r) => distributeToParent(op, r, batch) as Value);
      return { ok: aligned as unknown as Value };
    },
  };

  const out = runBehavior(ir, handlers, { __parents: parents as unknown as Value }, '__resolveRelation');
  return out as unknown as Record<string, unknown>[];
}

/**
 * If a `runPlan` `OP_FAILED` carries a mapped-failure message, re-surface the structured
 * {@link SqlFailure} (the message embeds the original SQLite code). Non-driver errors are
 * re-thrown verbatim.
 */
function reErrorToSqlFailure(e: unknown): unknown {
  const message = e instanceof Error ? e.message : String(e);
  const m = /(SQLITE_[A-Z_]+)/.exec(message);
  if (m) {
    // Reconstruct a minimal driver-error shape and re-map to preserve kind/policy/code.
    return mapSqliteError({ code: m[1], message });
  }
  return e;
}

export { SqlFailure };
