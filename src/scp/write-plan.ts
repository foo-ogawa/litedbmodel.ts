/**
 * litedbmodel v2 SCP — write-time relations → ordered SQL transaction plan (WS5, #25; spec §6).
 *
 * Derives a declarative {@link LifecycleContract} (the §6 effect arrays) + a base write op
 * (the Command's `Insert`/`Update`/`Delete`, e.g. `Insert(Post, {onWrite: Post.writes.create})`)
 * into ONE ordered, gate-first multi-statement transaction plan — exactly the §6 example:
 *
 * ```
 * BEGIN;
 *   -- Gate First (short-circuit + ROLLBACK before any body write)
 *   requires:    SELECT 1 FROM users WHERE id = :author_id;        -- absent ⇒ fail
 *   idempotency: INSERT INTO idem(token) VALUES(:request_id);      -- duplicate ⇒ short-circuit
 *   unique:      INSERT INTO uniq(...) ON CONFLICT DO NOTHING; …   -- collision ⇒ fail
 *   -- Body
 *   INSERT INTO posts(author_id,title) VALUES(:author_id,:title) RETURNING id;
 *   derive:  UPDATE users SET post_count = post_count + 1 WHERE id = :author_id;
 *   edges:   … (M:N intermediate INSERT/DELETE  or  1:N FK UPDATE) …
 *   emits:   INSERT INTO outbox(type,payload) VALUES('PostCreated', :payload);
 * COMMIT;
 * ```
 *
 * The plan is PURE JSON (each statement is a WS1 {@link CompiledOperation} rendered by the
 * SAME {@link import('./render').renderOperation}), so it serializes into the §8 bundle and a
 * thin per-language runtime honors the SAME ordering + gate-first (no re-derivation, spec §6:
 * "多言語 runtime は同一計画を honor"). `Transaction` は公開仕様にしない — this plan is DERIVED
 * from the Command's declared intent, never authored.
 *
 * ## Gate-first is a real execution behavior, not a text ordering (spec §6 "Gate First")
 *
 * Each gate statement carries a {@link GateRule} the runtime evaluates AFTER executing it: a
 * failing gate short-circuits — the remaining statements (body + derive + edges + emits) never
 * execute and the transaction ROLLBACKs. This is semantically-invariant early termination
 * ({@link import('./write-runtime').executeTransaction} proves it with real query counts + DB state).
 *
 * ## Path lowering (spec §6) — `$.input.*` / `$.entity.*` → closed-set Expression IR
 *
 * A `$.input.<field>` value lowers to `{ref:['<field>']}` (bc flat input scope); a
 * `$.entity.<field>` value lowers to `{ref:['__entity','<field>']}` (the body write's
 * RETURNING row, exposed to the derive/edges/emits stages under {@link ENTITY_ROOT}). Only
 * bc's closed operator set (`ref`) is emitted — no invented opcode (hard rule).
 *
 * ## INITIAL scope (spec §6 / §13) — single-statement Command + fixed-order relations
 *
 * The ordering is FIXED (requires → idempotency → unique → body → derive → edges → emits), not
 * a derived dependency DAG. A case needing full DAG derivation (cross-fragment dependency graph
 * → tx-DAG) is DEFERRED to WS8 — see {@link assertInitialScope}; this module never half-builds it.
 */

import type { CompiledOperation, ExprNode } from './ir';
import { WHERE_SLOT } from './ir';
import { assertOperationPortable } from './guard';
import {
  ENTITY_ROOT,
  parseEffectPath,
  type DeriveEffect,
  type EdgeEffect,
  type EmitEffect,
  type IdempotencyEffect,
  type LifecycleContract,
  type RequiresEffect,
  type UniqueEffect,
  type WriteLifecyclePhase,
} from './writes';

/** The role a transaction statement plays (drives the runtime's gate-first interpretation). */
export type StatementRole =
  | 'gate:requires'
  | 'gate:idempotency'
  | 'gate:unique'
  | 'body'
  | 'derive'
  | 'edge'
  | 'emit';

/**
 * The gate rule the runtime evaluates on a gate statement's result to decide short-circuit
 * (spec §6 "Gate First"):
 *   - `existsElseRollback` — the statement is a `SELECT 1 …` existence probe; ZERO rows ⇒
 *     the required row is absent ⇒ short-circuit + ROLLBACK (`requires`).
 *   - `insertedElseRollback` — the statement is a guard `INSERT … ON CONFLICT DO NOTHING`;
 *     ZERO affected rows ⇒ a collision ⇒ short-circuit + ROLLBACK (`unique`).
 *   - `insertedElseNoop` — the statement is an idempotency-token `INSERT … ON CONFLICT DO
 *     NOTHING`; ZERO affected rows ⇒ a DUPLICATE request ⇒ short-circuit WITHOUT error
 *     (idempotent: the prior write already happened; the tx commits the no-op / rolls back
 *     the empty body per {@link TransactionPlan.onIdempotentHit}).
 */
export type GateRule = 'existsElseRollback' | 'insertedElseRollback' | 'insertedElseNoop';

/** One ordered statement of a transaction plan (pure JSON — a WS1 compiled op + its role). */
export interface TxStatement {
  /** Stable statement id (ordering key + diagnostics). */
  readonly id: string;
  /** The statement's role in the §6 derivation order. */
  readonly role: StatementRole;
  /** The compiled SQL op (rendered by the normative {@link import('./render').renderOperation}). */
  readonly op: CompiledOperation;
  /** For a gate statement: the short-circuit rule. Absent for body/derive/edge/emit. */
  readonly gate?: GateRule;
  /** Human label (diagnostics; e.g. `requires users`, `derive users.post_count`). */
  readonly label: string;
}

/** How the runtime resolves an idempotency short-circuit hit (a duplicate request). */
export type IdempotentHitPolicy = 'rollback';

/**
 * A derived write-time-relations transaction plan (spec §6 / §8 `transaction plan`). Pure JSON:
 * ordered statements + gate-first rules + the body-write's `entity` exposure. Serializes into
 * the §8 bundle so a WS7 runtime executes the SAME plan.
 */
export interface TransactionPlan {
  /** The lifecycle phase this plan realizes (`create` / `update` / `remove`). */
  readonly phase: WriteLifecyclePhase;
  /**
   * The statement id whose RETURNING row is exposed to later stages under `$.entity.*`
   * ({@link ENTITY_ROOT}). Always the body write. `null` when the body has no RETURNING
   * (then no later stage may reference `$.entity.*` — validated at derivation time).
   */
  readonly entityFrom: string | null;
  /** The ordered statements (gate-first, then body, then derive/edges/emits). */
  readonly statements: readonly TxStatement[];
  /** What a duplicate-idempotency-token short-circuit does (α: rollback the empty tx). */
  readonly onIdempotentHit: IdempotentHitPolicy;
}

// ── Path → Expression IR ref (closed-set only) ────────────────────────────────

/**
 * Lower a path-rooted write-relation value (`$.input.<f>` / `$.entity.<f>`) to a bc closed-set
 * `ref` Expression IR node. `$.input.<f>` → `{ref:['<f>']}` (bc flat input scope); `$.entity.<f>`
 * → `{ref:['__entity','<f>']}` (the body RETURNING row bound under {@link ENTITY_ROOT}).
 */
function pathToRef(value: string): ExprNode {
  const p = parseEffectPath(value);
  return p.root === 'input' ? ({ ref: [p.field] } as ExprNode) : ({ ref: [ENTITY_ROOT, p.field] } as ExprNode);
}

// ── Per-effect statement compilers (all emit WS1 CompiledOperation shapes) ─────

/**
 * A per-derivation monotonic id allocator. Ids are deterministic within ONE
 * {@link deriveTransactionPlan} call (`<role>_<n>` starting at 0), so the SAME Command +
 * save contract derives a byte-identical plan every time — the golden bar (same input →
 * same ordered SQL group). A module-global counter would make ids depend on prior calls.
 */
type IdGen = (role: string) => string;
function makeIdGen(): IdGen {
  let n = 0;
  return (role: string) => `tx_${role}_${n++}`;
}

/**
 * `requires` → a gate-first existence probe: `SELECT 1 FROM <table> WHERE k1 = ? AND …`.
 * ZERO rows ⇒ ROLLBACK. Key columns are ordered deterministically (declaration key order).
 */
function compileRequires(e: RequiresEffect, nextId: IdGen): TxStatement {
  const cols = Object.keys(e.keys);
  if (cols.length === 0) throw new Error(`write-plan: requires on '${e.table}' declares no keys`);
  const whereSql = cols.map((c) => `${c} = ?`).join(' AND ');
  const params = cols.map((c) => pathToRef(e.keys[c]));
  const op: CompiledOperation = {
    component: 'Select',
    sql: `SELECT 1 FROM ${e.table} WHERE ${whereSql}`,
    where: null,
    params,
    assembly: { shape: 'items' },
  };
  return { id: nextId('requires'), role: 'gate:requires', op, gate: 'existsElseRollback', label: `requires ${e.table}` };
}

/**
 * `idempotency` → a gate-first token INSERT: `INSERT INTO <table>(<col>) VALUES(?) ON CONFLICT
 * DO NOTHING`. ZERO affected ⇒ DUPLICATE ⇒ short-circuit (no double write). Runs FIRST-among-
 * gates after requires, per the §6 example ordering.
 */
function compileIdempotency(e: IdempotencyEffect, nextId: IdGen): TxStatement {
  const op: CompiledOperation = {
    component: 'Insert',
    sql: `INSERT INTO ${e.table} (${e.column}) VALUES (?) ON CONFLICT DO NOTHING`,
    where: null,
    params: [pathToRef(e.token)],
    assembly: { shape: 'items' },
  };
  return { id: nextId('idem'), role: 'gate:idempotency', op, gate: 'insertedElseNoop', label: `idempotency ${e.table}` };
}

/**
 * `unique` → a gate-first guard-row INSERT: `INSERT INTO <guardTable>(name, s1, …, f1, …)
 * VALUES(?, …) ON CONFLICT DO NOTHING`. ZERO affected ⇒ COLLISION ⇒ ROLLBACK. The guard row
 * columns are `[name] + scope + fields` (deterministic order); `name` is a literal discriminator
 * bound as a param via a closed-set string literal (not SQL-inlined) to keep the render path uniform.
 */
function compileUnique(e: UniqueEffect, nextId: IdGen): TxStatement {
  const scopeCols = e.scope.map((_, i) => `s${i}`);
  const fieldCols = e.fields.map((_, i) => `f${i}`);
  const cols = ['name', ...scopeCols, ...fieldCols];
  const placeholders = cols.map(() => '?').join(', ');
  const params: ExprNode[] = [
    e.name as unknown as ExprNode, // literal discriminator (a closed-set string literal)
    ...e.scope.map(pathToRef),
    ...e.fields.map(pathToRef),
  ];
  const op: CompiledOperation = {
    component: 'Insert',
    sql: `INSERT INTO ${e.guardTable} (${cols.join(', ')}) VALUES (${placeholders}) ON CONFLICT DO NOTHING`,
    where: null,
    params,
    assembly: { shape: 'items' },
  };
  return { id: nextId('unique'), role: 'gate:unique', op, gate: 'insertedElseRollback', label: `unique ${e.name}` };
}

/**
 * `derive` → a cascade counter update: `UPDATE <table> SET <attr> = <attr> + ? WHERE k = ?`.
 * The increment amount is bound as a closed-set literal param (the declaration IS the SSoT —
 * no hardcoded default). A negative amount renders `attr = attr + ?` with a negative bound value
 * (byte-identical text regardless of sign; matches v1's parameterized increment).
 */
function compileDerive(e: DeriveEffect, nextId: IdGen): TxStatement {
  const keyCols = Object.keys(e.keys);
  if (keyCols.length === 0) throw new Error(`write-plan: derive on '${e.table}' declares no keys`);
  // SET clause `?` (the increment amount) is the single pre-WHERE static param; the key `?`s
  // live on the WHERE fragment tree (so render's pre/post `{where}` partition stays correct).
  const op: CompiledOperation = {
    component: 'Update',
    sql: `UPDATE ${e.table} SET ${e.attribute} = ${e.attribute} + ?${WHERE_SLOT}`,
    where: {
      connector: 'AND',
      fragments: keyCols.map((c) => ({ always: true, sql: `${c} = ?`, params: [pathToRef(e.keys[c])] })),
    },
    params: [e.amount as unknown as ExprNode],
    assembly: { shape: 'items' },
  };
  return { id: nextId('derive'), role: 'derive', op, label: `derive ${e.table}.${e.attribute}` };
}

/**
 * `edges` → M:N intermediate INSERT/DELETE or 1:N FK UPDATE (spec §6 table row `edges`).
 *   - `m2m` + `set`   → `INSERT INTO <join>(c1,…) VALUES(?,…)` (link).
 *   - `m2m` + `unset` → `DELETE FROM <join> WHERE c1 = ? AND …` (unlink).
 *   - `fk`  + `set`   → `UPDATE <related> SET <fkCol> = ? WHERE <keyCols…>` (attach).
 *   - `fk`  + `unset` → `UPDATE <related> SET <fkCol> = NULL WHERE <keyCols…>` (detach).
 */
function compileEdge(e: EdgeEffect, nextId: IdGen): TxStatement {
  if (e.relation === 'm2m') {
    const cols = Object.keys(e.columns);
    if (e.action === 'set') {
      const placeholders = cols.map(() => '?').join(', ');
      const op: CompiledOperation = {
        component: 'Insert',
        sql: `INSERT INTO ${e.table} (${cols.join(', ')}) VALUES (${placeholders})`,
        where: null,
        params: cols.map((c) => pathToRef(e.columns[c])),
        assembly: { shape: 'items' },
      };
      return { id: nextId('edge'), role: 'edge', op, label: `edge m2m link ${e.table}` };
    }
    // unset → DELETE FROM join WHERE all columns match
    const op: CompiledOperation = {
      component: 'Delete',
      sql: `DELETE FROM ${e.table}${WHERE_SLOT}`,
      where: { connector: 'AND', fragments: cols.map((c) => ({ always: true, sql: `${c} = ?`, params: [pathToRef(e.columns[c])] })) },
      params: [],
      assembly: { shape: 'items' },
    };
    return { id: nextId('edge'), role: 'edge', op, label: `edge m2m unlink ${e.table}` };
  }
  // fk: UPDATE <related> SET <fkCol> = ? (or NULL) WHERE <where keys…>
  const setCols = Object.keys(e.columns);
  const whereCols = Object.keys(e.where!);
  const setClauses = setCols.map((c) => (e.action === 'set' ? `${c} = ?` : `${c} = NULL`));
  const setParams: ExprNode[] = e.action === 'set' ? setCols.map((c) => pathToRef(e.columns[c])) : [];
  const op: CompiledOperation = {
    component: 'Update',
    sql: `UPDATE ${e.table} SET ${setClauses.join(', ')}${WHERE_SLOT}`,
    where: { connector: 'AND', fragments: whereCols.map((c) => ({ always: true, sql: `${c} = ?`, params: [pathToRef(e.where![c])] })) },
    params: setParams,
    assembly: { shape: 'items' },
  };
  return { id: nextId('edge'), role: 'edge', op, label: `edge fk ${e.action} ${e.table}` };
}

/**
 * `emits` → an outbox INSERT (same tx): `INSERT INTO <outbox>(type, payload) VALUES(?, ?)`.
 * `type` is the literal event name; `payload` is a bc `{obj:{…}}` of the path-rooted values,
 * serialized to a JSON text column by the runtime (the outbox `payload` column). Emitting the
 * payload as a single `obj` param keeps the SQL text stable (two `?` slots) regardless of the
 * payload's field count.
 */
function compileEmit(e: EmitEffect, nextId: IdGen): TxStatement {
  const payloadObj: Record<string, ExprNode> = {};
  for (const [k, v] of Object.entries(e.payload)) payloadObj[k] = pathToRef(v);
  const op: CompiledOperation = {
    component: 'Insert',
    sql: `INSERT INTO ${e.outboxTable} (type, payload) VALUES (?, ?)`,
    where: null,
    params: [e.name as unknown as ExprNode, { obj: payloadObj } as unknown as ExprNode],
    assembly: { shape: 'items' },
  };
  return { id: nextId('emit'), role: 'emit', op, label: `emit ${e.name}` };
}

// ── Derivation entrypoint ──────────────────────────────────────────────────────

/**
 * The base write op the Command declares (`Insert`/`Update`/`Delete` with `onWrite`). This is
 * the single-statement Command body the plan wraps in a transaction; WS5 initial scope is a
 * SINGLE base write (not a multi-fragment DAG — WS8).
 */
export interface BaseWrite {
  /** The compiled base write op (from the bridge, e.g. `compileNode` of the Insert node). */
  readonly op: CompiledOperation;
  /** The body statement's role (always `body`). */
  readonly label: string;
}

/**
 * Assert the derivation stays inside WS5 INITIAL scope (spec §6 / §13): a SINGLE base write.
 * A multi-write / cross-fragment DAG is DEFERRED to WS8 — rejected loudly here so we never
 * half-build the dependency-graph→tx-DAG derivation.
 */
function assertInitialScope(bases: readonly BaseWrite[]): void {
  if (bases.length !== 1) {
    throw new Error(
      `write-plan: WS5 initial scope is a SINGLE base-write Command + fixed-order write-time ` +
        `relations (got ${bases.length} base writes). Multi-write / cross-fragment DAG derivation ` +
        `is deferred to WS8 (spec §6/§13); it is intentionally not half-built here.`,
    );
  }
}

/**
 * Derive the ORDERED, gate-first {@link TransactionPlan} from a Command's base write + its
 * lifecycle save contract (spec §6). The statement order is FIXED (initial scope):
 *
 *   requires (gate) → idempotency (gate) → unique (gate) → BODY → derive → edges → emits
 *
 * `$.entity.*` references in derive/edges/emits require the body write to RETURN the entity
 * row; if any later stage references `$.entity.*` but the body has no RETURNING, that is a
 * loud build error (fail-closed — no silent absent-entity default).
 *
 * @throws if not single-base-write scope, or a `$.entity.*` reference lacks a RETURNING body.
 */
export function deriveTransactionPlan(
  phase: WriteLifecyclePhase,
  bases: readonly BaseWrite[],
  lifecycle: LifecycleContract,
): TransactionPlan {
  assertInitialScope(bases);
  const base = bases[0];
  const e = lifecycle.effects;
  const nextId = makeIdGen();

  const gates: TxStatement[] = [];
  for (const r of e.requires ?? []) gates.push(compileRequires(r, nextId));
  if (e.idempotency !== undefined) gates.push(compileIdempotency(e.idempotency, nextId));
  for (const u of e.unique ?? []) gates.push(compileUnique(u, nextId));

  const bodyId = nextId('body');
  const body: TxStatement = { id: bodyId, role: 'body', op: base.op, label: base.label };

  const after: TxStatement[] = [];
  for (const d of e.derive ?? []) after.push(compileDerive(d, nextId));
  for (const ed of e.edges ?? []) after.push(compileEdge(ed, nextId));
  for (const em of e.emits ?? []) after.push(compileEmit(em, nextId));

  const statements = [...gates, body, ...after];

  // If any after-body statement references `$.entity.*`, the body MUST RETURN the entity row.
  const usesEntity = referencesEntity(after);
  const bodyReturns = /\breturning\b/i.test(base.op.sql);
  const entityFrom = usesEntity ? bodyId : bodyReturns ? bodyId : null;
  if (usesEntity && !bodyReturns) {
    throw new Error(
      `write-plan: a derive/edges/emits stage references '$.entity.*' but the body write ` +
        `('${base.label}') has no RETURNING clause — the written row cannot be exposed to later ` +
        `stages. Add a RETURNING to the base write (fail-closed; no absent-entity default).`,
    );
  }

  // Hard rule: every param slot in the plan (gates, body, derive, edges, emits) is closed-set
  // Expression IR only. Assert fail-closed — an invented opcode is a build error, never bodged.
  for (const s of statements) assertOperationPortable(s.op);

  return { phase, entityFrom, statements, onIdempotentHit: 'rollback' };
}

/** True if any statement's compiled op references a `{ref:['__entity', …]}` head. */
function referencesEntity(statements: readonly TxStatement[]): boolean {
  const heads = new Set<string>();
  for (const s of statements) {
    collectRefHeads(s.op.params, heads);
    if (s.op.where !== null) collectWhereHeads(s.op.where, heads);
  }
  return heads.has(ENTITY_ROOT);
}

function collectRefHeads(node: unknown, heads: Set<string>): void {
  if (node === null || typeof node !== 'object') return;
  if (Array.isArray(node)) {
    for (const el of node) collectRefHeads(el, heads);
    return;
  }
  const keys = Object.keys(node);
  if (keys.length === 1 && keys[0] === 'ref') {
    const path = (node as Record<string, unknown>).ref;
    if (Array.isArray(path) && typeof path[0] === 'string') heads.add(path[0]);
    return;
  }
  for (const k of keys) collectRefHeads((node as Record<string, unknown>)[k], heads);
}

function collectWhereHeads(node: { fragments?: unknown[]; params?: unknown[] } | unknown, heads: Set<string>): void {
  if (node === null || typeof node !== 'object') return;
  const n = node as { connector?: string; fragments?: unknown[]; params?: unknown[] };
  if ('connector' in n && Array.isArray(n.fragments)) {
    for (const f of n.fragments) collectWhereHeads(f, heads);
    return;
  }
  if (Array.isArray(n.params)) collectRefHeads(n.params, heads);
}
