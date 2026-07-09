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
 * ## Transaction derivation (spec §6 / §14) — tx-DAG + gate-first
 *
 * A Command carries one or more named base writes, each with its §6 effects. Statements form a
 * data-dependency DAG (`$.ref.<writeName>.<field>` consumes an earlier write's RETURNING row,
 * exposed via `TxStatement.binds`), with a gate-first constraint (every gate precedes every
 * body/derive/edge/emit). The DAG is topologically ordered (Kahn, stable ascending declaration
 * `seq` tie-break) into a single-transaction statement plan. Underivable shapes (dependency
 * cycle, dangling `$.ref`, referenced write without RETURNING, duplicate bind, composite
 * `$.entity`) are LOUD rejects — never a silently mis-ordered plan.
 */

import type { CompiledOperation, ExprNode } from './ir';
import { WHERE_SLOT } from './ir';
import { assertOperationPortable } from './guard';
import { SQLITE, type Dialect } from './dialect';
import {
  ENTITY_ROOT,
  parseEffectPath,
  type DeriveEffect,
  type EdgeEffect,
  type EmitEffect,
  type IdempotencyEffect,
  type LifecycleContract,
  type LifecycleEffects,
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
  /**
   * For a composite (multi-write) body statement: the scope binding under which THIS statement's
   * RETURNING row is exposed to LATER statements — a downstream `$.ref.<binds>.<field>` resolves
   * to `scope[<binds>][<field>]` (spec §6 nested write). This is the DAG's data-dependency edge,
   * self-describing so the 5 runtimes just bind the row and continue (no re-derivation). Absent for
   * a statement no later statement depends on; the WS5 single-base write additionally exposes its
   * row under {@link TransactionPlan.entityFrom} / {@link ENTITY_ROOT} for `$.entity.*` back-compat.
   */
  readonly binds?: string;
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
 * Lower a path-rooted write-relation value to a bc closed-set `ref` Expression IR node:
 *   - `$.input.<f>`          → `{ref:['<f>']}`                 (bc flat input scope).
 *   - `$.entity.<f>`         → `{ref:['__entity','<f>']}`      (the sole body RETURNING row).
 *   - `$.ref.<write>.<f>`    → `{ref:['<write>','<f>']}`       (a named upstream write's RETURNING
 *                                                              row, bound under `<write>` via
 *                                                              {@link TxStatement.binds}).
 * Only bc's closed `ref` operator is emitted — no invented opcode (hard rule).
 */
function pathToRef(value: string): ExprNode {
  const p = parseEffectPath(value);
  if (p.root === 'input') return { ref: [p.field] } as ExprNode;
  if (p.root === 'entity') return { ref: [ENTITY_ROOT, p.field] } as ExprNode;
  return { ref: [p.writeName!, p.field] } as ExprNode; // 'ref' → named upstream write row
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
function compileIdempotency(e: IdempotencyEffect, nextId: IdGen, dialect: Dialect): TxStatement {
  // A bare do-nothing guard INSERT (no conflict-column target): SQLite/Postgres emit
  // `… ON CONFLICT DO NOTHING`; MySQL emits `INSERT IGNORE INTO …`. Routed through the SSoT.
  const op: CompiledOperation = {
    component: 'Insert',
    sql: dialect.guardInsert(e.table, [e.column], '?'),
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
function compileUnique(e: UniqueEffect, nextId: IdGen, dialect: Dialect): TxStatement {
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
    sql: dialect.guardInsert(e.guardTable, cols, placeholders),
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
 * payload as a single `obj` param keeps the SQL text stable (two `?` slots).
 *
 * Single-field payloads (`{obj:{one:…}}`) are fully supported: the portability guard treats
 * the `obj` arg as a FIELD MAP (its keys are data field names, not opcodes) and recurses into
 * the field values only — the same semantics as bc's guard/evaluator. (Fixed in #38; the guard
 * previously misread the lone field name as an unknown opcode.)
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
 * the Command body statement the plan wraps in a transaction. A Command may declare ONE base
 * write (WS5 single-statement scope) OR several (WS8a composite / nested write, spec §6:
 * "多対多や nested write（親作成と同時に子作成）は edges/追加 write で表現し、同一 tx にまとめる").
 */
export interface BaseWrite {
  /** The compiled base write op (from the bridge, e.g. `compileNode` of the Insert node). */
  readonly op: CompiledOperation;
  /** The body statement's role (always `body`). */
  readonly label: string;
  /**
   * The write's stable NAME (WS8a composite scope) — later statements reference its RETURNING row
   * as `$.ref.<name>.<field>`, and the derived body statement `binds` this name. Omitted for the
   * WS5 single-base write (its row is addressed as `$.entity.*` under {@link ENTITY_ROOT}).
   */
  readonly name?: string;
  /**
   * The write's OWN save-contract effects (WS8a composite scope): its gates/derive/edges/emits.
   * For the WS5 single-base write, the lifecycle is passed as the top-level `lifecycle` arg and
   * this is omitted. When a base write carries `effects`, the derivation treats it as one DAG
   * node group (its gates → its body → its after-effects), then topologically orders ALL groups.
   */
  readonly effects?: LifecycleEffects;
}

/**
 * A DAG node: one compiled statement plus the ref-heads it CONSUMES (upstream write names it
 * depends on) and the name it PRODUCES (its `binds`, if any). The topological sort orders nodes
 * so every producer precedes its consumers, with gate-first + a stable declaration-order tie-break.
 */
interface DagNode {
  readonly stmt: TxStatement;
  /** Declaration index (the monotonic id-allocation order) — the deterministic tie-break key. */
  readonly seq: number;
  /** The upstream write names this statement's params/where reference (its data-dependency edges). */
  readonly consumes: readonly string[];
  /** The name this statement produces for downstream consumers (its `binds`), or null. */
  readonly produces: string | null;
  /** Whether this node is a gate (drives the gate-first ordering constraint). */
  readonly isGate: boolean;
}

/**
 * Compile ONE base write + its effects into an ordered node group: its gate statements, its body
 * (which `binds` the write name for composite scope), then its derive/edges/emits. `seqBase` is
 * the group's starting declaration index; the returned nodes carry contiguous `seq` values so the
 * topo tie-break is a stable, insertion-independent total order.
 */
function compileWriteGroup(
  base: BaseWrite,
  lifecycle: LifecycleContract,
  nextId: IdGen,
  dialect: Dialect,
  seqRef: { n: number },
): { nodes: DagNode[]; bodyId: string } {
  const e = lifecycle.effects;
  const nodes: DagNode[] = [];
  const mk = (stmt: TxStatement, isGate: boolean): DagNode => {
    const consumes = refHeadsOf(stmt).filter((h) => h !== ENTITY_ROOT && !isInputHead(h, stmt));
    return { stmt, seq: seqRef.n++, consumes, produces: stmt.binds ?? null, isGate };
  };

  for (const r of e.requires ?? []) nodes.push(mk(compileRequires(r, nextId), true));
  if (e.idempotency !== undefined) nodes.push(mk(compileIdempotency(e.idempotency, nextId, dialect), true));
  for (const u of e.unique ?? []) nodes.push(mk(compileUnique(u, nextId, dialect), true));

  const bodyId = nextId('body');
  const bodyStmt: TxStatement = {
    id: bodyId,
    role: 'body',
    op: base.op,
    label: base.label,
    ...(base.name !== undefined ? { binds: base.name } : {}),
  };
  nodes.push(mk(bodyStmt, false));

  for (const d of e.derive ?? []) nodes.push(mk(compileDerive(d, nextId), false));
  for (const ed of e.edges ?? []) nodes.push(mk(compileEdge(ed, nextId), false));
  for (const em of e.emits ?? []) nodes.push(mk(compileEmit(em, nextId), false));

  return { nodes, bodyId };
}

/**
 * Topologically order the DAG nodes into the executable statement sequence (spec §6 / §14 "write-
 * time tx DAG 導出"). Edges:
 *   1. DATA dependency — a node consuming `$.ref.<w>.*` runs AFTER the node that produces `<w>`.
 *   2. GATE-FIRST — every gate runs before every non-gate (a semantic invariant: a gate must be
 *      able to short-circuit BEFORE any dependent body/derive/edge/emit work; spec §6 "Gate First").
 * Determinism: a Kahn topo sort whose ready-set is drained in ascending `seq` (declaration order),
 * so the SAME input always yields the SAME order → byte-identical SQL (the golden bar). A cycle
 * (data dependency vs gate-first, or a mutual `$.ref` cycle) is UNRESOLVABLE — ESCALATE loudly
 * rather than pick a silent order (hard rule: no guessed default on a genuine ambiguity).
 */
function topoOrder(nodes: readonly DagNode[]): TxStatement[] {
  const byName = new Map<string, DagNode>();
  for (const n of nodes) {
    if (n.produces !== null) {
      if (byName.has(n.produces)) {
        throw new Error(`write-plan: two writes both bind the name '${n.produces}' — write names must be unique in a composite Command.`);
      }
      byName.set(n.produces, n);
    }
  }

  // Build the edge set producer→consumer + gate→non-gate. `preds` counts in-edges per node.
  const idOf = new Map<DagNode, string>();
  for (const n of nodes) idOf.set(n, n.stmt.id);
  const succ = new Map<string, Set<string>>(); // stmt id → dependent stmt ids
  const indeg = new Map<string, number>();
  const nodeById = new Map<string, DagNode>();
  for (const n of nodes) {
    nodeById.set(n.stmt.id, n);
    succ.set(n.stmt.id, new Set());
    indeg.set(n.stmt.id, 0);
  }
  const addEdge = (fromId: string, toId: string): void => {
    if (fromId === toId) return;
    const s = succ.get(fromId)!;
    if (!s.has(toId)) {
      s.add(toId);
      indeg.set(toId, indeg.get(toId)! + 1);
    }
  };

  // 1. data-dependency edges (producer → each consumer).
  for (const n of nodes) {
    for (const dep of n.consumes) {
      const producer = byName.get(dep);
      if (producer === undefined) {
        throw new Error(
          `write-plan: statement '${n.stmt.label}' references '$.ref.${dep}.*' but no write in this ` +
            `Command binds the name '${dep}' — a dangling write reference (fail-closed; no silent skip).`,
        );
      }
      addEdge(producer.stmt.id, n.stmt.id);
    }
  }
  // 2. gate-first edges (every gate → every non-gate).
  const gates = nodes.filter((n) => n.isGate);
  const nonGates = nodes.filter((n) => !n.isGate);
  for (const g of gates) for (const b of nonGates) addEdge(g.stmt.id, b.stmt.id);

  // Kahn's algorithm; the ready frontier is drained in ascending declaration `seq` (stable).
  const ordered: TxStatement[] = [];
  const ready: DagNode[] = nodes.filter((n) => indeg.get(n.stmt.id) === 0).sort((a, b) => a.seq - b.seq);
  while (ready.length > 0) {
    const n = ready.shift()!;
    ordered.push(n.stmt);
    for (const depId of succ.get(n.stmt.id)!) {
      indeg.set(depId, indeg.get(depId)! - 1);
      if (indeg.get(depId) === 0) {
        const dn = nodeById.get(depId)!;
        // insert keeping the frontier sorted by seq (stable deterministic drain)
        let lo = 0;
        while (lo < ready.length && ready[lo].seq < dn.seq) lo++;
        ready.splice(lo, 0, dn);
      }
    }
  }

  if (ordered.length !== nodes.length) {
    const stuck = nodes.filter((n) => !ordered.includes(n.stmt)).map((n) => n.stmt.label);
    throw new Error(
      `write-plan: the write-time transaction DAG has a CYCLE (unresolvable ordering among ` +
        `[${stuck.join(', ')}]). This is either a mutual '$.ref' data dependency or a gate that ` +
        `depends on a body row (a gate cannot depend on a not-yet-written row). ESCALATE: the ` +
        `Command's write dependencies are contradictory — no order satisfies both the data ` +
        `dependencies and gate-first. (Fail-closed; the derivation never guesses an order.)`,
    );
  }
  return ordered;
}

/**
 * Derive the ORDERED, gate-first {@link TransactionPlan} from a Command's base write(s) + its
 * lifecycle save contract (spec §6 / §14).
 *
 * ## Single base write (WS5 scope)
 *
 * One base write + a top-level `lifecycle`; the order is the fixed §6 group
 * (requires → idempotency → unique → BODY → derive → edges → emits). `$.entity.*` references in
 * derive/edges/emits require the body write to RETURN the entity row (else a loud build error).
 *
 * ## Composite / nested write (WS8a scope) — real DAG derivation
 *
 * Multiple base writes, each carrying its OWN {@link BaseWrite.effects} and a `name`. A later
 * write references an earlier write's RETURNING row via `$.ref.<name>.<field>` (e.g. a child
 * INSERT keyed by the parent's returned id). The derivation builds the data-dependency graph
 * (statement → the writes it references) + the gate-first constraint, TOPOLOGICALLY orders it
 * (deterministic, stable tie-break), and emits ONE ordered gate-first transaction plan. A cycle
 * or dangling reference is ESCALATED (no silent mis-derivation).
 *
 * @throws if a `$.entity.*` reference lacks a RETURNING body, a `$.ref.*` is dangling, two writes
 *   bind the same name, or the dependency graph has a cycle.
 */
export function deriveTransactionPlan(
  phase: WriteLifecyclePhase,
  bases: readonly BaseWrite[],
  lifecycle: LifecycleContract,
  dialect: Dialect = SQLITE,
): TransactionPlan {
  if (bases.length === 0) {
    throw new Error('write-plan: a Command must declare at least one base write (Insert/Update/Delete).');
  }
  const nextId = makeIdGen();
  const seqRef = { n: 0 };
  const composite = bases.length > 1;

  // Each base write contributes a node group. In single-base WS5 scope the lifecycle is the
  // top-level arg; in composite scope each base carries its own `effects` (the shared top-level
  // `lifecycle` still applies to the FIRST base — the Command body — for backward-compatible
  // single-write callers that pass effects there).
  const allNodes: DagNode[] = [];
  const bodyIds: string[] = [];
  bases.forEach((base, i) => {
    const lc: LifecycleContract = base.effects !== undefined ? { effects: base.effects } : i === 0 ? lifecycle : { effects: {} };
    const { nodes, bodyId } = compileWriteGroup(base, lc, nextId, dialect, seqRef);
    allNodes.push(...nodes);
    bodyIds.push(bodyId);
  });

  const statements = topoOrder(allNodes);

  // `$.entity.*` back-compat: the SOLE base write's RETURNING row is exposed under __entity.
  const soleBody = bases[0];
  const soleBodyId = bodyIds[0];
  const usesEntity = referencesHead(statements, ENTITY_ROOT);
  const soleBodyReturns = /\breturning\b/i.test(soleBody.op.sql);
  if (usesEntity && composite) {
    throw new Error(
      `write-plan: a composite (multi-write) Command uses '$.entity.*' — ambiguous which write it ` +
        `denotes. Address each write's row by name via '$.ref.<writeName>.<field>' (fail-closed).`,
    );
  }
  if (usesEntity && !soleBodyReturns) {
    throw new Error(
      `write-plan: a derive/edges/emits stage references '$.entity.*' but the body write ` +
        `('${soleBody.label}') has no RETURNING clause — the written row cannot be exposed to later ` +
        `stages. Add a RETURNING to the base write (fail-closed; no absent-entity default).`,
    );
  }
  const entityFrom = usesEntity || (!composite && soleBodyReturns) ? soleBodyId : null;

  // Every write that a later statement references via `$.ref.<name>.*` MUST RETURN its row.
  for (const base of bases) {
    if (base.name === undefined) continue;
    if (referencesHead(statements, base.name) && !/\breturning\b/i.test(base.op.sql)) {
      throw new Error(
        `write-plan: write '${base.name}' is referenced as '$.ref.${base.name}.*' by a later ` +
          `statement but its op has no RETURNING clause — its row cannot be bound for downstream ` +
          `writes. Add a RETURNING to write '${base.name}' (fail-closed; no absent-row default).`,
      );
    }
  }

  // Hard rule: every param slot in the plan is closed-set Expression IR only. Fail-closed.
  for (const s of statements) assertOperationPortable(s.op);

  return { phase, entityFrom, statements, onIdempotentHit: 'rollback' };
}

/** Collect the ref-heads (`ref[0]`) a statement's params + where reference. */
function refHeadsOf(stmt: TxStatement): string[] {
  const heads = new Set<string>();
  collectRefHeads(stmt.op.params, heads);
  if (stmt.op.where !== null) collectWhereHeads(stmt.op.where, heads);
  return [...heads];
}

/**
 * True if a ref-head is an INPUT head (a bare `$.input.<f>` → `{ref:['<f>']}`), i.e. NOT a
 * cross-statement dependency. An input head is a single-element `ref` path whose head is a plain
 * field. We distinguish it from a write-name head (`$.ref.<w>.<f>` → `{ref:['<w>','<f>']}`, a
 * 2-element path) and the sole-entity head (`__entity`). A 2-element `ref` whose head is neither
 * `__entity` nor a declared write name would be a dangling reference (caught in {@link topoOrder}).
 */
function isInputHead(head: string, stmt: TxStatement): boolean {
  // An input ref is a 1-element path `{ref:[head]}`; a dependency ref is 2-element `{ref:[w,f]}`.
  return refPathLengthFor(stmt, head) === 1;
}

/** The (max) path length of a `ref` node whose head equals `head`, within a statement's op. */
function refPathLengthFor(stmt: TxStatement, head: string): number {
  let maxLen = 0;
  const walk = (node: unknown): void => {
    if (node === null || typeof node !== 'object') return;
    if (Array.isArray(node)) {
      for (const el of node) walk(el);
      return;
    }
    const keys = Object.keys(node);
    if (keys.length === 1 && keys[0] === 'ref') {
      const path = (node as Record<string, unknown>).ref;
      if (Array.isArray(path) && path[0] === head) maxLen = Math.max(maxLen, path.length);
      return;
    }
    for (const k of keys) walk((node as Record<string, unknown>)[k]);
  };
  walk(stmt.op.params);
  if (stmt.op.where !== null) {
    const collectWhere = (node: unknown): void => {
      if (node === null || typeof node !== 'object') return;
      const n = node as { connector?: string; fragments?: unknown[]; params?: unknown[] };
      if ('connector' in n && Array.isArray(n.fragments)) {
        for (const f of n.fragments) collectWhere(f);
        return;
      }
      if (Array.isArray(n.params)) walk(n.params);
    };
    collectWhere(stmt.op.where);
  }
  return maxLen;
}

/** True if any statement's compiled op references a `{ref:[head, …]}` head. */
function referencesHead(statements: readonly TxStatement[], head: string): boolean {
  const heads = new Set<string>();
  for (const s of statements) {
    collectRefHeads(s.op.params, heads);
    if (s.op.where !== null) collectWhereHeads(s.op.where, heads);
  }
  return heads.has(head);
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
