/**
 * litedbmodel v2 SCP — write-time relations → ordered SQL transaction plan, re-expressed on
 * the `makeSQL` model (epic #43/#45 Phase B; spec §6 / §14). This REPLACES the reduced-spine
 * `../write-plan.ts` + `../write-runtime.ts` (`CompiledOperation` + `renderOperation` +
 * FragmentTree). No reduced IR is emitted anywhere: every statement's SQL is COMPLETE tuned
 * text (byte-identical to what the v1 write path sends — `SELECT 1 …`, `INSERT … ON CONFLICT
 * DO NOTHING`, `UPDATE … SET c = c + ?`, the outbox INSERT), and its `params` are closed-set
 * bc Expression IR refs resolved at execute time against the accumulated transaction scope.
 *
 * A statement op is exactly a `makeSQL` template: `{ sql, params }` where `sql` carries `?`
 * placeholders and `params` are Expression IR (`{ref:[…]}` / a literal / a `{obj:{…}}` payload)
 * that bc evaluates per statement — then the concrete values assemble + render + bind through
 * the SAME `assembleMakeSQL` / `renderPlaceholders` the read path uses.
 *
 * ## Gate-first is a real execution behavior (spec §6 "Gate First")
 *
 * Each gate statement carries a {@link GateRule} the runtime evaluates AFTER executing it: a
 * failing gate short-circuits — the remaining statements never execute and the tx ROLLBACKs.
 *
 * ## Path lowering (spec §6) — `$.input.*` / `$.entity.*` / `$.ref.<w>.*` → closed-set refs
 *
 *   `$.input.<f>`       → `{ref:['<f>']}`             (bc flat input scope)
 *   `$.entity.<f>`      → `{ref:['__entity','<f>']}`  (the sole body write's RETURNING row)
 *   `$.ref.<w>.<f>`     → `{ref:['<w>','<f>']}`        (a named upstream write's RETURNING row)
 *
 * ## tx-DAG derivation (spec §6 / §14) — gate-first + data-dependency topo order
 *
 * Statements form a data-dependency DAG (`$.ref.<w>.*` consumes an earlier write's RETURNING
 * row via `TxStatement.binds`) with a gate-first constraint. A deterministic Kahn topo sort
 * (stable ascending declaration `seq` tie-break) orders them into a single-transaction plan.
 * Underivable shapes (cycle, dangling `$.ref`, referenced write without RETURNING, duplicate
 * bind, composite `$.entity`) are LOUD rejects — never a silently mis-ordered plan.
 */

import { evaluateExpression, type Scope, type Value } from 'behavior-contracts';
import { assembleMakeSQL, type MakeSQL } from './makesql';
import { renderPlaceholders, type Dialect as MakeSQLDialect } from './handler';
import { mapSqliteError } from '../errors';
import { DBConditions, type ConditionObject } from '../../DBConditions';
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
} from '../writes';

// ── Expression IR alias (a statement param is a closed-set bc Expression node) ──

/** A closed-set bc Expression IR node used as a `makeSQL` deferred param (ref / literal / obj). */
export type TxExpr = unknown;

/**
 * Encode a CONCRETE value into a bc Expression IR node that `evaluateExpression` returns VERBATIM.
 *
 * A batch-write op (createMany / updateMany / deleteMany) is compiled by driving the v1 builders
 * (compile-crud), so its `params` are already CONCRETE grouped values — NOT deferred Expression IR.
 * But the tx runtime renders every statement param through bc `evaluateExpression` (spec §6 /
 * `renderStatement`), which fail-closes on a BARE array ("bare array is not an expression") and on a
 * multi-key plain object. The batch INSERT on Postgres binds REAL arrays as single params
 * (`UNNEST($1::int[], …)` → `[[7,8], …]`), so those concrete params must be wrapped in the bc
 * literal-carrier ops so they survive evaluation unchanged:
 *   - array   → `{arr:[literalize(e)…]}`   (bc evaluates each element, so wrap recursively)
 *   - object  → `{obj:{k:literalize(v)…}}`  (a plain map, e.g. a JSON payload object)
 *   - scalar  → the value itself (null/bool/string/number pass through evaluateExpression verbatim;
 *               an integral number becomes a bigint, normalized back at the driver boundary).
 * This reuses ONLY vocabulary every language runtime's bc already implements (`{arr}`/`{obj}` — the
 * emit outbox payload already rides `{obj}` on live PG+MySQL), so a batch write executes through the
 * SAME per-statement tx loop in all five runtimes with NO runtime change.
 */
export function literalize(value: unknown): TxExpr {
  if (value === null || value === undefined) return null;
  if (Array.isArray(value)) return { arr: value.map((e) => literalize(e)) };
  if (typeof value === 'object') {
    const obj: Record<string, TxExpr> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) obj[k] = literalize(v);
    return { obj };
  }
  return value;
}

/**
 * The compiled op of a transaction statement — a `makeSQL` template. `sql` is the COMPLETE
 * tuned SQL text (byte-parity with the v1 write path) with `?` placeholders; `params` are
 * closed-set Expression IR resolved at execute time against the tx scope. This is the makeSQL
 * re-expression of the reduced `CompiledOperation` for the write path (no fragment tree, no
 * `{where}` splice — the WHERE text is already complete).
 */
export interface TxOp {
  /** Complete tuned SQL text (`?` placeholders). */
  readonly sql: string;
  /** Deferred param slots — closed-set Expression IR (`{ref:…}` / literal / `{obj:…}`). */
  readonly params: readonly TxExpr[];
  /**
   * The target table's PRIMARY KEY descriptor, for the MySQL RETURNING emulation (MySQL has no
   * native RETURNING). Present ONLY on an INSERT…RETURNING op. `columns` are the real PK column(s);
   * `autoInc` is the single AUTO_INCREMENT column name (int identity), or null for a client-supplied
   * PK (UUID / composite / natural key). The mysql-dialect bundle serializes this into a strip-
   * before-execute SQL comment ({@link mysqlPkHint}) the driver emulation reads so it re-selects by
   * the REAL PK — not a hardcoded `WHERE id = ?` (which breaks for UUID / composite PKs).
   */
  readonly pk?: { readonly columns: readonly string[]; readonly autoInc: string | null };
}

/** The role a transaction statement plays (drives the runtime's gate-first interpretation). */
export type StatementRole =
  | 'gate:requires'
  | 'gate:idempotency'
  | 'gate:unique'
  | 'body'
  | 'derive'
  | 'edge'
  | 'emit';

/** The gate rule the runtime evaluates on a gate statement's result to decide short-circuit. */
export type GateRule = 'existsElseRollback' | 'insertedElseRollback' | 'insertedElseNoop';

/** One ordered statement of a transaction plan (pure JSON — a makeSQL template + its role). */
export interface TxStatement {
  /** Stable statement id (ordering key + diagnostics). */
  readonly id: string;
  /** The statement's role in the §6 derivation order. */
  readonly role: StatementRole;
  /** The compiled makeSQL op (complete `sql` + deferred Expression-IR `params`). */
  readonly op: TxOp;
  /** For a gate statement: the short-circuit rule. Absent for body/derive/edge/emit. */
  readonly gate?: GateRule;
  /** For a composite body statement: the name under which this RETURNING row is exposed. */
  readonly binds?: string;
  /** Human label (diagnostics; e.g. `requires users`, `derive users.post_count`). */
  readonly label: string;
}

/** How the runtime resolves an idempotency short-circuit hit (a duplicate request). */
export type IdempotentHitPolicy = 'rollback';

/** A derived write-time-relations transaction plan (pure JSON — ordered gate-first statements). */
export interface TransactionPlan {
  readonly phase: WriteLifecyclePhase;
  readonly entityFrom: string | null;
  readonly statements: readonly TxStatement[];
  readonly onIdempotentHit: IdempotentHitPolicy;
}

// ── Path → Expression IR ref (closed-set only) ────────────────────────────────

function pathToRef(value: string): TxExpr {
  const p = parseEffectPath(value);
  if (p.root === 'input') return { ref: [p.field] };
  if (p.root === 'entity') return { ref: [ENTITY_ROOT, p.field] };
  return { ref: [p.writeName!, p.field] };
}

// ── Per-dialect guard INSERT text (the ON CONFLICT DO NOTHING / INSERT IGNORE SSoT) ──

/**
 * The tuned guard INSERT text for a gate-first `idempotency`/`unique` statement — byte-identical
 * to the v1 write path: SQLite/Postgres emit `INSERT INTO … VALUES (…) ON CONFLICT DO NOTHING`;
 * MySQL emits `INSERT IGNORE INTO … VALUES (…)`.
 */
function guardInsert(dialect: MakeSQLDialect, table: string, columns: readonly string[], placeholders: string): string {
  const cols = `(${columns.join(', ')})`;
  if (dialect === 'mysql') return `INSERT IGNORE INTO ${table} ${cols} VALUES (${placeholders})`;
  return `INSERT INTO ${table} ${cols} VALUES (${placeholders}) ON CONFLICT DO NOTHING`;
}

// ── Per-effect statement compilers (all emit makeSQL TxOp shapes) ──────────────

type IdGen = (role: string) => string;
function makeIdGen(): IdGen {
  let n = 0;
  return (role: string) => `tx_${role}_${n++}`;
}

/** `requires` → a gate-first existence probe: `SELECT 1 FROM <table> WHERE k1 = ? AND …`. */
function compileRequires(e: RequiresEffect, nextId: IdGen): TxStatement {
  const cols = Object.keys(e.keys);
  if (cols.length === 0) throw new Error(`write-plan: requires on '${e.table}' declares no keys`);
  const whereSql = v1EqualityWhereText(cols);
  const op: TxOp = {
    sql: `SELECT 1 FROM ${e.table} WHERE ${whereSql}`,
    params: cols.map((c) => pathToRef(e.keys[c])),
  };
  return { id: nextId('requires'), role: 'gate:requires', op, gate: 'existsElseRollback', label: `requires ${e.table}` };
}

/** `idempotency` → a gate-first token INSERT ON CONFLICT DO NOTHING (duplicate short-circuits). */
function compileIdempotency(e: IdempotencyEffect, nextId: IdGen, dialect: MakeSQLDialect): TxStatement {
  const op: TxOp = {
    sql: guardInsert(dialect, e.table, [e.column], '?'),
    params: [pathToRef(e.token)],
  };
  return { id: nextId('idem'), role: 'gate:idempotency', op, gate: 'insertedElseNoop', label: `idempotency ${e.table}` };
}

/** `unique` → a gate-first guard-row INSERT ON CONFLICT DO NOTHING (collision → ROLLBACK). */
function compileUnique(e: UniqueEffect, nextId: IdGen, dialect: MakeSQLDialect): TxStatement {
  const scopeCols = e.scope.map((_, i) => `s${i}`);
  const fieldCols = e.fields.map((_, i) => `f${i}`);
  const cols = ['name', ...scopeCols, ...fieldCols];
  const placeholders = cols.map(() => '?').join(', ');
  const params: TxExpr[] = [
    e.name, // literal discriminator (a closed-set string literal)
    ...e.scope.map(pathToRef),
    ...e.fields.map(pathToRef),
  ];
  const op: TxOp = { sql: guardInsert(dialect, e.guardTable, cols, placeholders), params };
  return { id: nextId('unique'), role: 'gate:unique', op, gate: 'insertedElseRollback', label: `unique ${e.name}` };
}

/** `derive` → a cascade counter update: `UPDATE <table> SET <attr> = <attr> + ? WHERE k = ?`. */
function compileDerive(e: DeriveEffect, nextId: IdGen): TxStatement {
  const keyCols = Object.keys(e.keys);
  if (keyCols.length === 0) throw new Error(`write-plan: derive on '${e.table}' declares no keys`);
  const whereSql = v1EqualityWhereText(keyCols);
  const op: TxOp = {
    sql: `UPDATE ${e.table} SET ${e.attribute} = ${e.attribute} + ? WHERE ${whereSql}`,
    // SET amount is the first param (before the WHERE keys), matching the v1 param order.
    params: [e.amount, ...keyCols.map((c) => pathToRef(e.keys[c]))],
  };
  return { id: nextId('derive'), role: 'derive', op, label: `derive ${e.table}.${e.attribute}` };
}

/** `edges` → M:N intermediate INSERT/DELETE or 1:N FK UPDATE (spec §6 table row `edges`). */
function compileEdge(e: EdgeEffect, nextId: IdGen): TxStatement {
  if (e.relation === 'm2m') {
    const cols = Object.keys(e.columns);
    if (e.action === 'set') {
      const placeholders = cols.map(() => '?').join(', ');
      const op: TxOp = {
        sql: `INSERT INTO ${e.table} (${cols.join(', ')}) VALUES (${placeholders})`,
        params: cols.map((c) => pathToRef(e.columns[c])),
      };
      return { id: nextId('edge'), role: 'edge', op, label: `edge m2m link ${e.table}` };
    }
    const whereSql = v1EqualityWhereText(cols);
    const op: TxOp = {
      sql: `DELETE FROM ${e.table} WHERE ${whereSql}`,
      params: cols.map((c) => pathToRef(e.columns[c])),
    };
    return { id: nextId('edge'), role: 'edge', op, label: `edge m2m unlink ${e.table}` };
  }
  // fk: UPDATE <related> SET <fkCol> = ? (or NULL) WHERE <where keys…>
  const setCols = Object.keys(e.columns);
  const whereCols = Object.keys(e.where!);
  const setClauses = setCols.map((c) => (e.action === 'set' ? `${c} = ?` : `${c} = NULL`));
  const setParams: TxExpr[] = e.action === 'set' ? setCols.map((c) => pathToRef(e.columns[c])) : [];
  const whereSql = v1EqualityWhereText(whereCols);
  const op: TxOp = {
    sql: `UPDATE ${e.table} SET ${setClauses.join(', ')} WHERE ${whereSql}`,
    params: [...setParams, ...whereCols.map((c) => pathToRef(e.where![c]))],
  };
  return { id: nextId('edge'), role: 'edge', op, label: `edge fk ${e.action} ${e.table}` };
}

/** `emits` → an outbox INSERT (same tx): `INSERT INTO <outbox>(type, payload) VALUES(?, ?)`. */
function compileEmit(e: EmitEffect, nextId: IdGen): TxStatement {
  const payloadObj: Record<string, TxExpr> = {};
  for (const [k, v] of Object.entries(e.payload)) payloadObj[k] = pathToRef(v);
  const op: TxOp = {
    sql: `INSERT INTO ${e.outboxTable} (type, payload) VALUES (?, ?)`,
    params: [e.name, { obj: payloadObj }],
  };
  return { id: nextId('emit'), role: 'emit', op, label: `emit ${e.name}` };
}

// ============================================================================
// Authored write node → makeSQL TxOp (the makeSQL re-expression of the bridge's
// `compileNode` for the write path — reads ports STRUCTURALLY, emits complete SQL
// text + DEFERRED Expression-IR params resolved at tx execute time).
// ============================================================================

/** The reserved column-ref path head that marks an IN-list membership (mirrors the bridge). */
export const IN_SENTINEL = '@in';

type WriteComponent = 'Insert' | 'Update' | 'Delete' | 'Select';
interface WriteNodeLike {
  readonly id?: string;
  readonly component: WriteComponent;
  readonly ports: Record<string, unknown>;
}

function stringPort(ports: Record<string, unknown>, name: string): string | undefined {
  const v = ports[name];
  if (v === undefined) return undefined;
  if (typeof v !== 'string') throw new Error(`compileWriteNode: port '${name}' must be a literal string in the IR (got ${JSON.stringify(v)})`);
  return v;
}

/** `<prefix>.<field>` ports → an ordered `Record<field, ExprNode>` (declaration order). */
function collectFamily(ports: Record<string, unknown>, prefix: string): Record<string, TxExpr> {
  const out: Record<string, TxExpr> = {};
  for (const k of Object.keys(ports)) {
    if (k.startsWith(`${prefix}.`)) out[k.slice(prefix.length + 1)] = ports[k];
  }
  return out;
}

function opKey(node: unknown): string | undefined {
  if (node === null || typeof node !== 'object' || Array.isArray(node)) return undefined;
  const keys = Object.keys(node as object);
  return keys.length === 1 ? keys[0] : undefined;
}

function columnOf(node: unknown, ctx: string): string {
  const op = opKey(node);
  if (op !== 'ref' && op !== 'refOpt') throw new Error(`compileWriteNode: ${ctx}: the column operand must be a {ref:[…]} path`);
  const path = (node as Record<string, unknown[]>)[op];
  if (!Array.isArray(path) || path.length === 0 || typeof path[path.length - 1] !== 'string') {
    throw new Error(`compileWriteNode: ${ctx}: column ref path must be a non-empty string path`);
  }
  return path[path.length - 1] as string;
}

function binOperands(node: unknown, op: string, at: string): [unknown, unknown] {
  const args = (node as Record<string, unknown[]>)[op];
  if (!Array.isArray(args) || args.length !== 2) throw new Error(`compileWriteNode: ${at}: '${op}' expects exactly 2 operands`);
  return [args[0], args[1]];
}

/** The comparison-operator SQL symbols, keyed by the bc operator name (drives the v1 custom-op key). */
const CMP_OPS: Record<string, string> = { lt: '<', le: '<=', gt: '>', ge: '>=', ne: '<>' };

/** A probe placeholder value fed to the v1 builder so it emits one `?` per bound slot. */
const PROBE = '__probe__';

/**
 * Produce a bare condition body's TEXT by driving the ORIGINAL v1 `DBConditions.compile()` — the
 * SAME builder the eager write path and `compile-crud` use, so the WHERE text is byte-identical
 * to v1 by construction (a v2 hand-roll would make the corpus tautological). The throwaway
 * `probe` array absorbs the probe values; the real runtime values are the caller's deferred
 * Expression-IR params. The write path targets a single dialect at render but the WHERE `= ?` /
 * `<op> ?` / `IS NULL` / `IS NOT NULL` forms are dialect-invariant in `DBConditions`.
 */
function v1ConditionText(conditions: ConditionObject): string {
  const probe: unknown[] = [];
  return new DBConditions(conditions).compile(probe);
}

/**
 * The tuned WHERE-body TEXT for a set of PK-equality columns (`k1 = ? AND k2 = ?`), produced by
 * driving the ORIGINAL v1 `DBConditions.compile()` (via {@link v1ConditionText}) — the SAME
 * builder the read path uses — so the write-tx predicate text is byte-identical to v1 by
 * construction (a v1 predicate regression now moves a tx golden). The throwaway probe params are
 * discarded; the real runtime values are the caller's deferred Expression-IR params, bound 1:1
 * with the `?` placeholders. `DBConditions.compile` iterates the condition object in insertion
 * order and joins with ` AND `, so the `?`/param order matches the caller's column order.
 */
function v1EqualityWhereText(columns: readonly string[]): string {
  const conditions: ConditionObject = {};
  for (const c of columns) conditions[c] = PROBE;
  return v1ConditionText(conditions);
}

/** Lower one where-member Expression node → a `<sql, params>` WHERE fragment (deferred params). */
function lowerWhereMember(node: unknown, at: string): { sql: string; params: TxExpr[] } {
  const op = opKey(node);
  if (op === undefined) throw new Error(`compileWriteNode: ${at}: a where member must be a single-operator Expression node`);
  if (op === 'eq') {
    const [col, val] = binOperands(node, op, at);
    const column = columnOf(col, at);
    if (val === null) return { sql: v1ConditionText({ [column]: null }), params: [] };
    return { sql: v1ConditionText({ [column]: PROBE }), params: [val] };
  }
  if (op in CMP_OPS) {
    const [col, val] = binOperands(node, op, at);
    const column = columnOf(col, at);
    if (op === 'ne' && val === null) return { sql: v1ConditionText({ [`${column} IS NOT NULL`]: true }), params: [] };
    return { sql: v1ConditionText({ [`${column} ${CMP_OPS[op]} ?`]: PROBE }), params: [val] };
  }
  throw new Error(`compileWriteNode: ${at}: unsupported where operator '${op}' (write path supports eq/ne/lt/le/gt/ge)`);
}

function lowerWherePort(ports: Record<string, unknown>, at: string): { sql: string; params: TxExpr[] } {
  const v = ports.where;
  if (v === undefined) return { sql: '', params: [] };
  if (typeof v !== 'object' || v === null || !('arr' in v) || !Array.isArray((v as { arr: unknown }).arr)) {
    throw new Error(`compileWriteNode: ${at}: 'where' must be an {arr:[…]} literal`);
  }
  const members = (v as { arr: unknown[] }).arr;
  const parts: string[] = [];
  const params: TxExpr[] = [];
  members.forEach((m, i) => {
    const f = lowerWhereMember(m, `${at}.where[${i}]`);
    parts.push(f.sql);
    params.push(...f.params);
  });
  // Join with the SAME ` AND ` connector `DBConditions.compile` uses (parts.join(' AND ')).
  return { sql: parts.join(' AND '), params };
}

function returningTail(ports: Record<string, unknown>): string {
  const r = stringPort(ports, 'returning');
  return r === undefined ? '' : ` RETURNING ${r}`;
}

/**
 * Read the optional PRIMARY KEY descriptor ports of an Insert node (for the MySQL RETURNING
 * emulation). `pk` is a comma-separated column list (`'doc_id'` / `'order_id,line_no'`); `autoInc`
 * names the single AUTO_INCREMENT column, or is absent for a client-supplied PK. Absent `pk`
 * defaults to null (the emulation then keeps its legacy `WHERE id`/`LAST_INSERT_ID` path, so the
 * existing auto-increment-`id` corpus is unchanged).
 */
function pkPort(ports: Record<string, unknown>): { columns: readonly string[]; autoInc: string | null } | undefined {
  const pk = stringPort(ports, 'pk');
  if (pk === undefined) return undefined;
  const columns = pk.split(',').map((c) => c.trim()).filter((c) => c.length > 0);
  if (columns.length === 0) return undefined;
  const ai = stringPort(ports, 'autoInc');
  return { columns, autoInc: ai ?? null };
}

/** The strip-before-execute PK-hint comment marker the MySQL RETURNING emulation reads. */
const MYSQL_PK_HINT_RE = /\s*\/\*scp:pk=[^*]*\*\//;

/**
 * Serialize a {@link TxOp.pk} descriptor into a strip-before-execute SQL comment appended to an
 * INSERT…RETURNING op, so the MySQL driver emulation can re-select by the REAL primary key. The
 * comment is STRIPPED (with the RETURNING clause) before the INSERT executes, so the executed SQL
 * stays byte-clean; it is emitted ONLY into the mysql-dialect bundle (PG/SQLite keep native
 * RETURNING and never see it). Format: ` /*scp:pk=col1,col2;ai=<autoIncCol|>* /`.
 */
export function mysqlPkHint(op: TxOp): TxOp {
  if (op.pk === undefined) return op;
  if (!/\breturning\b/i.test(op.sql)) return op;
  const hint = ` /*scp:pk=${op.pk.columns.join(',')};ai=${op.pk.autoInc ?? ''}*/`;
  return { ...op, sql: op.sql + hint };
}

/** Strip a trailing MySQL PK-hint comment from a rendered SQL (defensive; runtimes strip too). */
export function stripMysqlPkHint(sql: string): string {
  return sql.replace(MYSQL_PK_HINT_RE, '');
}

/**
 * Compile ONE authored catalog write node (`Insert`/`Update`/`Delete`) into a makeSQL {@link TxOp}
 * — complete tuned SQL text + DEFERRED Expression-IR params. This is the makeSQL re-expression of
 * the reduced bridge's `compileNode` for the write path (the tx-DAG base writes). INSERT columns
 * are CANONICAL (alphabetical) sorted — the v2 write-path SSoT (matches `DBModel._insert`).
 */
export function compileWriteNode(node: WriteNodeLike): TxOp {
  const { component, ports } = node;
  const table = stringPort(ports, 'table');
  if (table === undefined) throw new Error(`compileWriteNode: ${component} node requires a literal 'table' port`);

  switch (component) {
    case 'Insert': {
      const values = collectFamily(ports, 'values');
      const cols = Object.keys(values);
      if (cols.length === 0) throw new Error(`compileWriteNode: Insert requires at least one 'values.<field>' port`);
      const sorted = [...cols].sort();
      const placeholders = sorted.map(() => '?').join(', ');
      const sql = `INSERT INTO ${table} (${sorted.join(', ')}) VALUES (${placeholders})${returningTail(ports)}`;
      const pk = pkPort(ports);
      return { sql, params: sorted.map((c) => values[c]), ...(pk !== undefined ? { pk } : {}) };
    }
    case 'Update': {
      const set = collectFamily(ports, 'set');
      const setCols = Object.keys(set);
      if (setCols.length === 0) throw new Error(`compileWriteNode: Update requires at least one 'set.<field>' port`);
      const where = lowerWherePort(ports, 'Update');
      if (where.sql === '') throw new Error(`compileWriteNode: Update requires a 'where' port`);
      const setClauses = setCols.map((c) => `${c} = ?`).join(', ');
      const sql = `UPDATE ${table} SET ${setClauses} WHERE ${where.sql}${returningTail(ports)}`;
      return { sql, params: [...setCols.map((c) => set[c]), ...where.params] };
    }
    case 'Delete': {
      const where = lowerWherePort(ports, 'Delete');
      if (where.sql === '') throw new Error(`compileWriteNode: Delete requires a 'where' port`);
      const sql = `DELETE FROM ${table} WHERE ${where.sql}${returningTail(ports)}`;
      return { sql, params: where.params };
    }
    default:
      throw new Error(`compileWriteNode: catalog component '${component}' has no write compile (SQL writes: Insert/Update/Delete)`);
  }
}

// ── Derivation entrypoint ──────────────────────────────────────────────────────

/** The base write op the Command declares (`Insert`/`Update`/`Delete` with `onWrite`). */
export interface BaseWrite {
  /** The compiled base write makeSQL op (complete `sql` + deferred Expression-IR `params`). */
  readonly op: TxOp;
  readonly label: string;
  /** The write's stable NAME (composite scope) — referenced downstream as `$.ref.<name>.*`. */
  readonly name?: string;
  /** The write's OWN save-contract effects (composite scope). */
  readonly effects?: LifecycleEffects;
}

interface DagNode {
  readonly stmt: TxStatement;
  readonly seq: number;
  readonly consumes: readonly string[];
  readonly produces: string | null;
  readonly isGate: boolean;
}

function compileWriteGroup(
  base: BaseWrite,
  lifecycle: LifecycleContract,
  nextId: IdGen,
  dialect: MakeSQLDialect,
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

  const succ = new Map<string, Set<string>>();
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
  const gates = nodes.filter((n) => n.isGate);
  const nonGates = nodes.filter((n) => !n.isGate);
  for (const g of gates) for (const b of nonGates) addEdge(g.stmt.id, b.stmt.id);

  const ordered: TxStatement[] = [];
  const ready: DagNode[] = nodes.filter((n) => indeg.get(n.stmt.id) === 0).sort((a, b) => a.seq - b.seq);
  while (ready.length > 0) {
    const n = ready.shift()!;
    ordered.push(n.stmt);
    for (const depId of succ.get(n.stmt.id)!) {
      indeg.set(depId, indeg.get(depId)! - 1);
      if (indeg.get(depId) === 0) {
        const dn = nodeById.get(depId)!;
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
 * lifecycle save contract (spec §6 / §14). Single base write = fixed §6 group; multiple named
 * base writes = a topologically-ordered gate-first tx DAG (`$.ref.<name>.*` data dependencies).
 */
export function deriveTransactionPlan(
  phase: WriteLifecyclePhase,
  bases: readonly BaseWrite[],
  lifecycle: LifecycleContract,
  dialect: MakeSQLDialect = 'sqlite',
): TransactionPlan {
  if (bases.length === 0) {
    throw new Error('write-plan: a Command must declare at least one base write (Insert/Update/Delete).');
  }
  const nextId = makeIdGen();
  const seqRef = { n: 0 };
  const composite = bases.length > 1;

  const allNodes: DagNode[] = [];
  const bodyIds: string[] = [];
  bases.forEach((base, i) => {
    const lc: LifecycleContract = base.effects !== undefined ? { effects: base.effects } : i === 0 ? lifecycle : { effects: {} };
    const { nodes, bodyId } = compileWriteGroup(base, lc, nextId, dialect, seqRef);
    allNodes.push(...nodes);
    bodyIds.push(bodyId);
  });

  const statements = topoOrder(allNodes);

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

  return { phase, entityFrom, statements, onIdempotentHit: 'rollback' };
}

/**
 * Lower a list of CONCRETE batch-write ops (createMany / updateMany / deleteMany — each already
 * compiled by driving the v1 builders in `compile-crud`, so its `sql` is byte-identical to v1 and
 * its `params` are concrete grouped values) into a gate-free {@link TransactionPlan} of `body`
 * statements, run IN THE DECLARED ORDER as ONE transaction. This is the makeSQL re-expression of a
 * BATCH write: createMany with heterogeneous column-set groups is exactly a composition of several
 * INSERT statements (`_insert:928-975` grouping), and each group's SQL is one v1-copied statement.
 *
 * Each op's concrete params are {@link literalize}d into bc literal-carrier IR so they survive the
 * tx runtime's `evaluateExpression` render pass unchanged (a PG batch binds real arrays). The plan
 * carries NO gates and NO `$.entity`/`$.ref` bindings — a batch write is N independent grouped
 * statements, executed in order (the last statement's RETURNING rows, if any, are the `entity`).
 */
export function deriveBatchPlan(
  phase: WriteLifecyclePhase,
  ops: readonly { sql: string; params: readonly unknown[]; label?: string }[],
): TransactionPlan {
  if (ops.length === 0) {
    throw new Error('write-plan: a batch write must declare at least one statement (createMany/updateMany/deleteMany produced none).');
  }
  const nextId = makeIdGen();
  const statements: TxStatement[] = ops.map((op, i) => ({
    id: nextId('body'),
    role: 'body' as const,
    op: { sql: op.sql, params: op.params.map((p) => literalize(p)) },
    label: op.label ?? `batch[${i}]`,
  }));
  // A batch write has NO single `$.entity` row — it is N grouped statements, each possibly RETURNING
  // its own rows. `entityFrom` stays null; the runtime accumulates EVERY body statement's RETURNING
  // rows into `TransactionResult.returnedRows` (ordered by group), reproducing v1 `createMany`'s
  // "all created rows" result while `expectedDbState` proves the persisted state.
  return { phase, entityFrom: null, statements, onIdempotentHit: 'rollback' };
}

/** Collect the ref-heads (`ref[0]`) a statement's params reference. */
function refHeadsOf(stmt: TxStatement): string[] {
  const heads = new Set<string>();
  collectRefHeads(stmt.op.params, heads);
  return [...heads];
}

/** True if a ref-head is an INPUT head (a bare `$.input.<f>` → 1-element `{ref:['<f>']}`). */
function isInputHead(head: string, stmt: TxStatement): boolean {
  return refPathLengthFor(stmt.op.params, head) === 1;
}

function refPathLengthFor(node: unknown, head: string): number {
  let maxLen = 0;
  const walk = (n: unknown): void => {
    if (n === null || typeof n !== 'object') return;
    if (Array.isArray(n)) {
      for (const el of n) walk(el);
      return;
    }
    const keys = Object.keys(n);
    if (keys.length === 1 && keys[0] === 'ref') {
      const path = (n as Record<string, unknown>).ref;
      if (Array.isArray(path) && path[0] === head) maxLen = Math.max(maxLen, path.length);
      return;
    }
    for (const k of keys) walk((n as Record<string, unknown>)[k]);
  };
  walk(node);
  return maxLen;
}

function referencesHead(statements: readonly TxStatement[], head: string): boolean {
  const heads = new Set<string>();
  for (const s of statements) collectRefHeads(s.op.params, heads);
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

// ============================================================================
// Runtime — execute a TransactionPlan against real SQLite as ONE transaction.
// ============================================================================

/** The minimal synchronous SQLite driver surface the tx runtime needs (better-sqlite3). */
export interface SqliteDb {
  prepare(sql: string): {
    all(...params: unknown[]): unknown[];
    run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
  };
}

/** Why a transaction did not commit (a gate short-circuit outcome; not a driver error). */
export type ShortCircuitReason = 'requires_absent' | 'unique_collision' | 'idempotent_duplicate';

/** The structured outcome of executing a {@link TransactionPlan}. */
export interface TransactionResult {
  readonly committed: boolean;
  readonly shortCircuit?: { readonly statementId: string; readonly reason: ShortCircuitReason };
  readonly entity: Record<string, unknown> | null;
  readonly executed: readonly string[];
  /**
   * For a BATCH write (createMany/updateMany/deleteMany — a gate-free plan with `entityFrom:null`):
   * the RETURNING rows of every body statement, ordered by statement. Present ONLY when a body
   * statement RETURNED rows and the plan has no `$.entity` (batch mode); absent for a gate-first
   * single/composite Command (which exposes its written row via `entity`). This carries v1
   * `createMany`'s "all created rows" result across the multi-statement batch.
   */
  readonly returnedRows?: readonly (readonly Record<string, unknown>[])[];
}

/** bc evaluates ints to bigint; convert a rendered param to a driver-bindable value. */
function toDriverParam(v: Value): unknown {
  if (typeof v === 'bigint') {
    if (v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(v);
    return v;
  }
  // An emit payload evaluates to a plain object (`{obj:{…}}`); serialize it to the outbox JSON text.
  if (v !== null && typeof v === 'object' && !Array.isArray(v)) return JSON.stringify(v);
  return v;
}

/**
 * Render a statement's makeSQL op against the tx scope: evaluate each deferred Expression-IR
 * param to a concrete value (bc `evaluateExpression`), build a concrete `makeSQL`, then
 * assemble + render to the dialect placeholder form. This is the SAME assemble/render the read
 * path uses — only the param values come from the tx scope, not a compile-time input.
 */
function renderStatement(op: TxOp, scope: Scope, dialect: MakeSQLDialect): { sql: string; params: unknown[] } {
  const concrete: unknown[] = op.params.map((p) => evaluateExpression(p, scope));
  const node: MakeSQL = { sql: op.sql, params: concrete };
  const assembled = assembleMakeSQL(node);
  return { sql: renderPlaceholders(assembled.sql, dialect), params: assembled.params.map((p) => toDriverParam(p as Value)) };
}

function execStatement(
  db: SqliteDb,
  op: TxOp,
  scope: Scope,
  dialect: MakeSQLDialect,
): { rows: Record<string, unknown>[]; changes: number } {
  const { sql, params } = renderStatement(op, scope, dialect);
  const stmt = db.prepare(sql);
  const hasReturn = /\bselect\b/i.test(sql.slice(0, 8)) || /\breturning\b/i.test(sql);
  if (hasReturn) {
    const rows = stmt.all(...params) as Record<string, unknown>[];
    return { rows, changes: rows.length };
  }
  const info = stmt.run(...params);
  return { rows: [], changes: info.changes };
}

function gateShortCircuit(gate: GateRule, result: { rows: Record<string, unknown>[]; changes: number }): ShortCircuitReason | null {
  switch (gate) {
    case 'existsElseRollback':
      return result.rows.length === 0 ? 'requires_absent' : null;
    case 'insertedElseRollback':
      return result.changes === 0 ? 'unique_collision' : null;
    case 'insertedElseNoop':
      return result.changes === 0 ? 'idempotent_duplicate' : null;
  }
}

/** Execute a derived {@link TransactionPlan} as ONE real SQLite transaction with gate-first. */
export function executeTransaction(db: SqliteDb, plan: TransactionPlan, input: Scope, dialect: MakeSQLDialect = 'sqlite'): TransactionResult {
  db.prepare('BEGIN').run();
  const executed: string[] = [];
  const scope: Scope = { ...input };
  let entity: Record<string, unknown> | null = null;
  // Batch mode (createMany/updateMany/deleteMany): a gate-free, ref-free plan (no `$.entity`, no
  // gates, no `$.ref` binds) — a pure list of body statements. Only THEN accumulate each body
  // statement's RETURNING rows in order (a composite Command also has `entityFrom:null` but carries
  // `binds`/gates and is NOT batch — its written rows flow via scope refs, not `returnedRows`).
  const isBatch =
    plan.entityFrom === null &&
    plan.statements.every((s) => s.gate === undefined && s.binds === undefined && s.role === 'body');
  const returnedRows: Record<string, unknown>[][] = [];

  try {
    for (const stmt of plan.statements) {
      const result = execStatement(db, stmt.op, scope, dialect);
      executed.push(stmt.id);

      if (stmt.gate !== undefined) {
        const reason = gateShortCircuit(stmt.gate, result);
        if (reason !== null) {
          db.prepare('ROLLBACK').run();
          return { committed: false, shortCircuit: { statementId: stmt.id, reason }, entity: null, executed };
        }
      }

      if (stmt.id === plan.entityFrom) {
        entity = result.rows.length > 0 ? result.rows[0] : null;
        if (entity !== null) scope[ENTITY_ROOT] = entity as unknown as Value;
      }
      if (stmt.binds !== undefined && result.rows.length > 0) {
        scope[stmt.binds] = result.rows[0] as unknown as Value;
      }
      if (isBatch && stmt.role === 'body' && result.rows.length > 0) returnedRows.push(result.rows);
    }
    db.prepare('COMMIT').run();
    return { committed: true, entity, executed, ...(returnedRows.length > 0 ? { returnedRows } : {}) };
  } catch (e) {
    try {
      db.prepare('ROLLBACK').run();
    } catch {
      /* ROLLBACK best-effort; surface the original failure below */
    }
    throw mapSqliteError(e);
  }
}

/**
 * A counting {@link SqliteDb} wrapper: forwards every call to the wrapped driver but records
 * each PREPARED SQL string. Tests use it to PROVE gate-first short-circuit.
 */
export function countingDriver(db: SqliteDb): { db: SqliteDb; prepared: string[] } {
  const prepared: string[] = [];
  const wrapped: SqliteDb = {
    prepare(sql: string) {
      prepared.push(sql);
      return db.prepare(sql);
    },
  };
  return { db: wrapped, prepared };
}

/** Render one statement op to its dialect SQL text + params (exposed for golden tests). */
export function renderTxStatement(op: TxOp, scope: Scope, dialect: MakeSQLDialect = 'sqlite'): { sql: string; params: unknown[] } {
  return renderStatement(op, scope, dialect);
}
