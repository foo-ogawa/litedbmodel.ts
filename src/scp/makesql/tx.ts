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
import { sqliteInsertJson, mysqlInsertJson, sqliteUpdateManyJson, mysqlUpdateManyJson } from './json-batch';
import { postgresSqlBuilder } from '../../drivers/PostgresSqlBuilder';
import { sqlTypeToBcScalar, sqlTypeToMaterializeClass, type ColumnTypeResolver } from '../coltype';
import { renderPlaceholders, type Dialect as MakeSQLDialect } from './handler';
import { formatterFor } from './compile';
import { mapSqliteError } from '../errors';
import {
  type ExecutionContext,
  type AsyncExecutionContext,
  type PooledAsyncContext,
  type SqliteDriver,
  execute as seamExecute,
  run as seamRun,
  executeAsync as seamExecuteAsync,
  runAsync as seamRunAsync,
  contextForDriver,
  withTransactionAsync,
} from '../exec-context';
import { type TransactionOptions, runInTransactionScope, checkWriteAllowed } from '../tx-options';
import { isConnectionError } from '../../connection-errors';
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
  /**
   * NATIVE-CODEGEN typing metadata (E5/#120 — the RETURNING-chained tx chain). Additive: the runtime
   * tx ({@link executeTransaction}) IGNORES it; the codegen chain lowering
   * ({@link import('../codegen').lowerTransactionForNativeChain}) reads it to type each statement's
   * native param ports + its produced-row struct WITHOUT re-parsing the rendered SQL. The SHARED
   * {@link compileWriteNode} emits it from the structured ports it already has (one compiler feeds both
   * the runtime and the codegen chain). Present on a single-statement Insert/Update/Delete op; absent
   * on a batch op (its `?` binds a `{__batchRows}` marker, not a column). `bindColumns[i]` is the table
   * column the i-th `?` binds (parallel to `params`; `null` when the param is not a plain column value).
   */
  readonly writeMeta?: {
    readonly table: string;
    readonly bindColumns: readonly (string | null)[];
    readonly returning: readonly string[];
  };
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

/**
 * Collect the `sqlCast.<field>` port family → `Map<column, sqlCastType>` (the PG per-column cast
 * types, e.g. `jsonb`/`uuid`/`int[]`). This mirrors the `sqlCastMap` v1 `DBModel._insert`/`_update`
 * read from the column metadata (`getSqlCastMap`) to emit `?::<sqlCast>` on Postgres. A write node
 * that declares no cast ports yields an empty map (no cast columns — the common case).
 */
function collectSqlCast(ports: Record<string, unknown>): Map<string, string> {
  const map = new Map<string, string>();
  for (const k of Object.keys(ports)) {
    if (!k.startsWith('sqlCast.')) continue;
    const v = ports[k];
    if (typeof v !== 'string') {
      throw new Error(`compileWriteNode: port '${k}' (a sqlCast type) must be a literal string in the IR (got ${JSON.stringify(v)})`);
    }
    map.set(k.slice('sqlCast.'.length), v);
  }
  return map;
}

/**
 * The placeholder text for one written column's value, applying the v1 PER-COLUMN cast on Postgres.
 * Byte-identical to v1 `DBModel._insert` (`src/drivers/PostgresSqlBuilder.ts:289-296`) and `_update`
 * (`src/DBModel.ts:1058-1063`): a PG cast column emits `?::<sqlCast>` via the SAME dialect cast
 * formatter (`formatterFor('postgres')`), SKIPPING `timestamp`/`date` (the pg driver serializes Date
 * objects itself — an explicit cast interferes). MySQL/SQLite emit a bare `?` (v1's dialect-aware
 * `SqlCastFormatter` is identity there — the .rs `::type` leak is NOT reproduced). The tx-write path
 * targets a single dialect at compile, so the cast is resolved here, not deferred.
 */
function castPlaceholder(dialect: MakeSQLDialect, sqlCastMap: Map<string, string>, column: string): string {
  const sqlCast = sqlCastMap.get(column);
  if (dialect !== 'postgres' || sqlCast === undefined || sqlCast === 'timestamp' || sqlCast === 'date') return '?';
  return formatterFor('postgres')('?', sqlCast);
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

/**
 * Lower one where-member Expression node → a `<sql, params, columns>` WHERE fragment (deferred
 * params). `columns[i]` is the table column the i-th emitted param binds — parallel to `params` — so
 * the native-codegen chain types each WHERE-bound `?` from its column (see {@link TxOp.writeMeta}).
 */
function lowerWhereMember(node: unknown, at: string): { sql: string; params: TxExpr[]; columns: string[] } {
  const op = opKey(node);
  if (op === undefined) throw new Error(`compileWriteNode: ${at}: a where member must be a single-operator Expression node`);
  if (op === 'eq') {
    const [col, val] = binOperands(node, op, at);
    const column = columnOf(col, at);
    if (val === null) return { sql: v1ConditionText({ [column]: null }), params: [], columns: [] };
    return { sql: v1ConditionText({ [column]: PROBE }), params: [val], columns: [column] };
  }
  if (op in CMP_OPS) {
    const [col, val] = binOperands(node, op, at);
    const column = columnOf(col, at);
    if (op === 'ne' && val === null) return { sql: v1ConditionText({ [`${column} IS NOT NULL`]: true }), params: [], columns: [] };
    return { sql: v1ConditionText({ [`${column} ${CMP_OPS[op]} ?`]: PROBE }), params: [val], columns: [column] };
  }
  throw new Error(`compileWriteNode: ${at}: unsupported where operator '${op}' (write path supports eq/ne/lt/le/gt/ge)`);
}

function lowerWherePort(ports: Record<string, unknown>, at: string): { sql: string; params: TxExpr[]; columns: string[] } {
  const v = ports.where;
  if (v === undefined) return { sql: '', params: [], columns: [] };
  if (typeof v !== 'object' || v === null || !('arr' in v) || !Array.isArray((v as { arr: unknown }).arr)) {
    throw new Error(`compileWriteNode: ${at}: 'where' must be an {arr:[…]} literal`);
  }
  const members = (v as { arr: unknown[] }).arr;
  const parts: string[] = [];
  const params: TxExpr[] = [];
  const columns: string[] = [];
  members.forEach((m, i) => {
    const f = lowerWhereMember(m, `${at}.where[${i}]`);
    parts.push(f.sql);
    params.push(...f.params);
    columns.push(...f.columns);
  });
  // Join with the SAME ` AND ` connector `DBConditions.compile` uses (parts.join(' AND ')).
  return { sql: parts.join(' AND '), params, columns };
}

/** The RETURNING column names (`['id','author_id']`), or `[]` when the op has no RETURNING clause. */
function returningColumns(ports: Record<string, unknown>): string[] {
  const r = stringPort(ports, 'returning');
  return r === undefined ? [] : r.split(',').map((c) => c.trim()).filter((c) => c.length > 0);
}

function returningTail(ports: Record<string, unknown>): string {
  const r = stringPort(ports, 'returning');
  return r === undefined ? '' : ` RETURNING ${r}`;
}

/**
 * The upsert `ON CONFLICT` / `ON DUPLICATE KEY` tail of an `Insert`, from the `onConflict` (the
 * conflict-target column list) + `onConflictAction` (`'update'` default / `'ignore'`) ports. Absent
 * `onConflict` ⇒ a plain INSERT (no tail). Per-dialect verbs, byte-matching the JSON-batch upsert
 * form (`sqliteInsertJson`): pg/sqlite `ON CONFLICT (k) DO UPDATE SET c = excluded.c` / `DO NOTHING`;
 * mysql `ON DUPLICATE KEY UPDATE c = VALUES(c)` (mysql ignores the target list). The DO-UPDATE sets
 * every inserted column to its excluded value (`onConflictUpdate:'all'` — the v1 builder fallback);
 * setting the key column to itself is a harmless no-op. This shared compiler is what BOTH the runtime
 * (`executeStaticWrite`) and codegen read, so an authored upsert executes AND bakes identically.
 */
/** Map the `onConflict`/`onConflictAction` ports to the {@link JsonInsertOptions} upsert fields (the
 * batch json_each builder's own shape) — so a batch UPSERT (upsertMany) reuses the SAME conflict verbs
 * as the single upsert (E2). Absent `onConflict` ⇒ a plain batch INSERT. */
function onConflictJsonOpts(ports: Record<string, unknown>, cols: readonly string[]): { onConflict?: string[]; onConflictUpdate?: 'all'; onConflictIgnore?: boolean } {
  const conflict = stringPort(ports, 'onConflict');
  if (conflict === undefined) return {};
  const onConflict = conflict.split(',').map((c) => c.trim()).filter((c) => c.length > 0);
  const action = stringPort(ports, 'onConflictAction') ?? 'update';
  void cols;
  return action === 'ignore' ? { onConflict, onConflictIgnore: true } : { onConflict, onConflictUpdate: 'all' };
}

function onConflictTail(dialect: MakeSQLDialect, ports: Record<string, unknown>, cols: readonly string[]): string {
  const conflict = stringPort(ports, 'onConflict');
  if (conflict === undefined) return '';
  const action = stringPort(ports, 'onConflictAction') ?? 'update';
  const conflictCols = conflict.split(',').map((c) => c.trim()).filter((c) => c.length > 0);
  if (action === 'ignore') {
    return dialect === 'mysql'
      ? ` ON DUPLICATE KEY UPDATE ${conflictCols[0]} = ${conflictCols[0]}` // mysql no-op update = IGNORE-equivalent
      : ` ON CONFLICT (${conflictCols.join(', ')}) DO NOTHING`;
  }
  if (dialect === 'mysql') {
    return ` ON DUPLICATE KEY UPDATE ${cols.map((c) => `${c} = VALUES(${c})`).join(', ')}`;
  }
  return ` ON CONFLICT (${conflictCols.join(', ')}) DO UPDATE SET ${cols.map((c) => `${c} = excluded.${c}`).join(', ')}`;
}

/**
 * A per-column raw VALUE SPECIMEN whose {@link import('../../drivers/PostgresSqlBuilder').inferPgType}
 * matches the column's SCHEMA SQL type, so the PG batch UNNEST cast (`?::<pgType>[]`) the original
 * `buildInsert`/`buildUpdateMany` emits is derived from the schema SoT — NOT from the runtime values
 * the symbolic codegen path never sees. int32→number / int64→bigint / bool→boolean / date→Date /
 * float→non-integer number / string(+decimal/uuid/json)→string, reproducing each `inferPgType` branch.
 * Ambiguous only for a REAL column whose live value is integer-valued (v1 would infer `int`, not
 * `numeric`); the bench columns (text/int/bigint) are unambiguous. Unknown SQL types are a hard error
 * (fail-closed) via the §4.1 classifiers.
 */
function pgTypeSpecimen(sqlType: string): unknown {
  const klass = sqlTypeToMaterializeClass(sqlType);
  if (klass === 'int32') return 0;
  if (klass === 'int64') return 0n;
  if (klass === 'bool') return false;
  if (klass === 'date') return new Date(0);
  // passthrough: float / decimal(→string) / text / uuid / json — split by the bc scalar.
  const scalar = sqlTypeToBcScalar(sqlType);
  if (scalar === 'float') return 0.5; // a non-integer number ⇒ inferPgType 'numeric'
  return ''; // string family (text / varchar / uuid / decimal / json) ⇒ inferPgType 'text'
}

/**
 * Compile a PG BATCH Insert/Update to its byte-identical v1 UNNEST form for the NATIVE codegen path,
 * by driving the ORIGINAL `postgresSqlBuilder` (never a re-roll) with schema-typed specimen records
 * (so the emitted `?::<pgType>[]` casts come from the schema SoT). Each `?` binds a per-column
 * {@link BatchArrayMarker} — the PG (v1) twin of the sqlite/mysql (v2) `{__batchRows}` JSON marker:
 * `refs[i]` is the WHOLE array for column `columns[i]`. codegen types the SAME array-input head off
 * both markers (one shared path); the per-driver seam binds each PG marker as a `<elem>[]` array.
 * Length-independent (the UNNEST text depends only on columns + types + onConflict/returning), so
 * FIXED and bakeable.
 */
function pgBatchArrayParams(cols: string[], refFor: (c: string) => TxExpr, dialect: MakeSQLDialect): TxExpr[] {
  return cols.map((c) => ({ __batchArray: { column: c, ref: refFor(c), dialect } }) as unknown as TxExpr);
}

function pgBatchInsert(table: string, sorted: string[], values: Record<string, TxExpr>, ports: Record<string, unknown>, resolve: ColumnTypeResolver, dialect: MakeSQLDialect): TxOp {
  const specimen = Object.fromEntries(sorted.map((c) => [c, pgTypeSpecimen(resolve(table, c))]));
  const records = [specimen, specimen]; // 2 rows ⇒ UNNEST branch (records.length > 1)
  const { sql } = postgresSqlBuilder.buildInsert({
    tableName: table, columns: sorted, records, rawRecords: records,
    ...onConflictJsonOpts(ports, sorted),
    ...(stringPort(ports, 'returning') !== undefined ? { returning: stringPort(ports, 'returning') } : {}),
  });
  return { sql, params: pgBatchArrayParams(sorted, (c) => values[c], dialect) };
}

function pgBatchUpdate(table: string, keyCols: string[], updateCols: string[], key: Record<string, TxExpr>, set: Record<string, TxExpr>, ports: Record<string, unknown>, resolve: ColumnTypeResolver, dialect: MakeSQLDialect): TxOp {
  const allCols = [...keyCols, ...updateCols];
  const specimen = Object.fromEntries(allCols.map((c) => [c, pgTypeSpecimen(resolve(table, c))]));
  const records = [specimen, specimen];
  // The pg batch UPDATE aliases the table `AS t` and the value source `AS v(keyCols…)`, so a BARE
  // RETURNING column that is also a key column (in `v`) is ambiguous. v1's `DBModel.updateMany`
  // qualifies RETURNING with the `t` alias via `buildReturning(table, cols, 't')` — reuse the SAME
  // builder so the qualified RETURNING is byte-identical to v1 (never a hand-roll).
  const returningPort = stringPort(ports, 'returning');
  const returning = returningPort === undefined
    ? undefined
    : postgresSqlBuilder.buildReturning(table, returningPort.split(',').map((c) => c.trim()).filter((c) => c.length > 0), 't');
  const { sql } = postgresSqlBuilder.buildUpdateMany({
    tableName: table, keyColumns: keyCols, updateColumns: updateCols, records, rawRecords: records,
    ...(returning !== undefined ? { returning } : {}),
  });
  // One array param per UNNEST column in [keyCols…, updateCols…] order (matches buildUpdateMany).
  const refFor = (c: string): TxExpr => (keyCols.includes(c) ? key[c] : set[c]);
  return { sql, params: pgBatchArrayParams(allCols, refFor, dialect) };
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
export function compileWriteNode(node: WriteNodeLike, dialect: MakeSQLDialect = 'sqlite', resolveColumnType?: ColumnTypeResolver): TxOp {
  const { component, ports } = node;
  const table = stringPort(ports, 'table');
  if (table === undefined) throw new Error(`compileWriteNode: ${component} node requires a literal 'table' port`);
  // Per-column PG cast types (`sqlCast.<field>` ports) — drive the v1 `?::<sqlCast>` on Postgres.
  const sqlCastMap = collectSqlCast(ports);

  switch (component) {
    case 'Insert': {
      const values = collectFamily(ports, 'values');
      const cols = Object.keys(values);
      if (cols.length === 0) throw new Error(`compileWriteNode: Insert requires at least one 'values.<field>' port`);
      const sorted = [...cols].sort();
      // E3 (#118) BATCH insert (createMany / upsertMany): a `batch:'true'` marker means each
      // `values.<col>` port is a PARALLEL ARRAY of that column's values (bc has no Vec<struct>, so the
      // records ride as one scalar array per column). Reuse the EXISTING json_each batch f_sql (its
      // text depends only on the columns + onConflict/returning — value-length-independent, so FIXED
      // and bakeable); the ONE JSON `?` binds a `{__batchRows}` marker the runtime/codegen build from
      // the parallel arrays at execute time (NOT literalized). One statement for N records.
      if (stringPort(ports, 'batch') === 'true') {
        if (dialect === 'postgres') {
          if (resolveColumnType === undefined) throw new Error(`compileWriteNode: batch insert on postgres needs the column-type resolver (schema SoT) to derive the UNNEST element casts — pass it through compileBundle.`);
          return pgBatchInsert(table, sorted, values, ports, resolveColumnType, dialect);
        }
        const shapeOpts = { tableName: table, columns: sorted, records: [] as Record<string, unknown>[], ...onConflictJsonOpts(ports, sorted), ...(stringPort(ports, 'returning') !== undefined ? { returning: stringPort(ports, 'returning') } : {}) };
        const shape = dialect === 'mysql' ? mysqlInsertJson(shapeOpts) : sqliteInsertJson(shapeOpts);
        // The ONE json param = a deferred marker carrying the columns + their parallel array refs.
        const marker = { __batchRows: { columns: sorted, refs: sorted.map((c) => values[c]), dialect } };
        return { sql: shape.sql, params: [marker] };
      }
      // v1 `DBModel._insert` emits `?::<sqlCast>` PER COLUMN on Postgres (skipping timestamp/date);
      // the placeholder list is thus per-column, NOT a uniform `?` join (the latent H1 divergence).
      const placeholders = sorted.map((c) => castPlaceholder(dialect, sqlCastMap, c)).join(', ');
      const sql = `INSERT INTO ${table} (${sorted.join(', ')}) VALUES (${placeholders})${onConflictTail(dialect, ports, sorted)}${returningTail(ports)}`;
      const pk = pkPort(ports);
      // The `?`s bind the value columns in sorted order (the ON CONFLICT / RETURNING tails add no `?`).
      const writeMeta = { table, bindColumns: sorted, returning: returningColumns(ports) };
      return { sql, params: sorted.map((c) => values[c]), ...(pk !== undefined ? { pk } : {}), writeMeta };
    }
    case 'Update': {
      const set = collectFamily(ports, 'set');
      const setCols = Object.keys(set);
      if (setCols.length === 0) throw new Error(`compileWriteNode: Update requires at least one 'set.<field>' port`);
      // E3 (#118) BATCH update (updateMany): `batch:'true'` — the `key.<col>` family names the match
      // key(s) (parallel arrays), the `set.<col>` family the columns to set (parallel arrays). Reuse
      // the EXISTING json_each/JSON_TABLE batch UPDATE (`sqliteUpdateManyJson`): its text depends only
      // on the key + update columns, so it's FIXED and bakeable. It binds the ONE records-JSON to
      // MULTIPLE `?` (one per SET clause + the WHERE) — each is the SAME `__batchRows` marker, so the
      // runtime evalSpec (and the codegen seam) build the SAME JSON per `?`. ONE statement for N rows.
      if (stringPort(ports, 'batch') === 'true') {
        const key = collectFamily(ports, 'key');
        const keyCols = Object.keys(key).sort();
        if (keyCols.length === 0) throw new Error(`compileWriteNode: batch Update requires at least one 'key.<field>' port`);
        const updateCols = [...setCols].sort();
        if (dialect === 'postgres') {
          if (resolveColumnType === undefined) throw new Error(`compileWriteNode: batch update on postgres needs the column-type resolver (schema SoT) to derive the UNNEST element casts — pass it through compileBundle.`);
          return pgBatchUpdate(table, keyCols, updateCols, key, set, ports, resolveColumnType, dialect);
        }
        const shapeOpts = { tableName: table, keyColumns: keyCols, updateColumns: updateCols, records: [] as Record<string, unknown>[], ...(stringPort(ports, 'returning') !== undefined ? { returning: stringPort(ports, 'returning') } : {}) };
        const shape = dialect === 'mysql' ? mysqlUpdateManyJson(shapeOpts) : sqliteUpdateManyJson(shapeOpts);
        // The JSON carries BOTH the key + update columns (in that order); one marker per `?`.
        const columns = [...keyCols, ...updateCols];
        const refs = [...keyCols.map((c) => key[c]), ...updateCols.map((c) => set[c])];
        const nQ = (shape.sql.match(/\?/g) ?? []).length;
        const marker = { __batchRows: { columns, refs, dialect } };
        return { sql: shape.sql, params: Array.from({ length: nQ }, () => marker) };
      }
      const where = lowerWherePort(ports, 'Update');
      if (where.sql === '') throw new Error(`compileWriteNode: Update requires a 'where' port`);
      // v1 `DBModel._update` emits `<c> = ?::<sqlCast>` PER COLUMN on Postgres (skipping timestamp/date).
      const setClauses = setCols.map((c) => `${c} = ${castPlaceholder(dialect, sqlCastMap, c)}`).join(', ');
      const sql = `UPDATE ${table} SET ${setClauses} WHERE ${where.sql}${returningTail(ports)}`;
      // The `?`s bind the SET columns (in setCols order) then the WHERE columns (`where.columns`).
      const writeMeta = { table, bindColumns: [...setCols, ...where.columns], returning: returningColumns(ports) };
      return { sql, params: [...setCols.map((c) => set[c]), ...where.params], writeMeta };
    }
    case 'Delete': {
      const where = lowerWherePort(ports, 'Delete');
      if (where.sql === '') throw new Error(`compileWriteNode: Delete requires a 'where' port`);
      const sql = `DELETE FROM ${table} WHERE ${where.sql}${returningTail(ports)}`;
      // The `?`s bind the WHERE columns; a DELETE has no SET/VALUES params.
      const writeMeta = { table, bindColumns: where.columns, returning: returningColumns(ports) };
      return { sql, params: where.params, writeMeta };
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

/**
 * The minimal synchronous SQLite driver surface the tx runtime needs (better-sqlite3) — the
 * backward-compat public seam. Internally the tx runs through the {@link ExecutionContext} seam
 * (`../exec-context`); a raw driver passed here is wrapped via {@link contextForDriver}. Aliased to
 * {@link SqliteDriver}.
 */
export type SqliteDb = SqliteDriver;

/** A tx entry accepts either a raw {@link SqliteDb} or a full {@link ExecutionContext}. */
export type DbOrContext = SqliteDb | ExecutionContext;

/** Coerce a `SqliteDb | ExecutionContext` argument to a ctx (raw driver ⇒ backward-compat wrapper). */
function asContext(dbOrCtx: DbOrContext): ExecutionContext {
  return 'connectionFor' in dbOrCtx ? dbOrCtx : contextForDriver(dbOrCtx);
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
  ctx: ExecutionContext,
  op: TxOp,
  scope: Scope,
  dialect: MakeSQLDialect,
): { rows: Record<string, unknown>[]; changes: number } {
  const { sql, params } = renderStatement(op, scope, dialect);
  const hasReturn = /\bselect\b/i.test(sql.slice(0, 8)) || /\breturning\b/i.test(sql);
  // Every tx body statement funnels through the central seam (middleware → connectionFor → exec).
  // A SELECT / RETURNING statement reads rows (execute); a bare write runs (run). The tx's `intent`
  // is write throughout, so `connectionFor` resolves the tx-owned connection (§3).
  if (hasReturn) {
    const rows = seamExecute(ctx, sql, params, { write: true });
    return { rows, changes: rows.length };
  }
  const info = seamRun(ctx, sql, params);
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
    default:
      // Fail-CLOSED on an unknown / forward-incompatible gate rule (aligned with Python + Rust): a
      // corrupt or unrecognized gate MUST NOT silently continue (fail-open would let a malformed gate
      // be skipped and the write COMMIT). Throwing here aborts the tx (the caller's catch ROLLBACKs).
      throw new Error(`scp write: unknown gate rule '${String(gate)}'`);
  }
}

/**
 * Execute a derived {@link TransactionPlan} as ONE real SQLite transaction with gate-first
 * short-circuit. Accepts a raw {@link SqliteDb} (wrapped via {@link contextForDriver}) or a full
 * {@link ExecutionContext}. The transaction derives a tx-scoped ctx (`withConnection(conn, true)`)
 * that PINS one connection so BEGIN, every body statement, and COMMIT/ROLLBACK all run on the SAME
 * connection (§3, per-execution ownership). For the single-DB SQLite driver the pinned connection
 * is the sole connection; the ownership shows its teeth on the pooled async path
 * ({@link import('../exec-context').withTransactionAsync}), which this mirrors.
 */
export function executeTransaction(db: DbOrContext, plan: TransactionPlan, input: Scope, dialect: MakeSQLDialect = 'sqlite'): TransactionResult {
  const outer = asContext(db);
  // Pin ONE connection for the whole transaction (BEGIN…COMMIT on the same conn). For SQLite the
  // base ctx already owns a single connection; deriving the tx-scoped ctx keeps the contract uniform
  // with the async per-execution-ownership path so the native ports mirror ONE shape.
  const ctx = outer.withConnection(outer.connectionFor({ write: true }), true);
  seamRun(ctx, 'BEGIN', []);
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
    // Mark the body "inside a transaction" (guard/nested detection) — the SAME async marker the
    // async path sets; synchronous `run` keeps it on the current tick.
    const shortCircuited = runInTransactionScope((): TransactionResult | null => {
      for (const stmt of plan.statements) {
        const result = execStatement(ctx, stmt.op, scope, dialect);
        executed.push(stmt.id);

        if (stmt.gate !== undefined) {
          const reason = gateShortCircuit(stmt.gate, result);
          if (reason !== null) {
            seamRun(ctx, 'ROLLBACK', []);
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
      return null;
    });
    if (shortCircuited !== null) return shortCircuited;
    seamRun(ctx, 'COMMIT', []);
    return { committed: true, entity, executed, ...(returnedRows.length > 0 ? { returnedRows } : {}) };
  } catch (e) {
    try {
      seamRun(ctx, 'ROLLBACK', []);
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

// ============================================================================
// Phase A (#75) — ASYNC transaction runtime (live PG / MySQL) with PER-EXECUTION
// CONNECTION OWNERSHIP. The async twin of `executeTransaction`: it runs the derived
// TransactionPlan through `withTransactionAsync`, which acquires ONE pooled connection,
// pins it in the ALS ctx, and runs BEGIN…COMMIT on it. Concurrent transactions each own a
// DISTINCT connection ⇒ isolated (no shared-slot cross-talk). This is the production async
// write-tx path the concurrent-tx isolation test exercises.
// ============================================================================

/** Parse the `scp:pk=cols;ai=col` block-comment hint the MySQL RETURNING emulation reads. */
function parsePkHint(sql: string): { cols: string[]; autoInc: string } | null {
  const m = /\/\*scp:pk=([^;*]*);ai=([^*]*)\*\//i.exec(sql);
  if (!m) return null;
  return { cols: m[1].split(',').map((c) => c.trim()).filter(Boolean), autoInc: m[2].trim() };
}

/** The INSERT/UPDATE/DELETE target table (for the MySQL re-select). */
function insertTable(sql: string): string {
  const m = /\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+([A-Za-z0-9_."`]+)/i.exec(sql);
  if (m === null) throw new Error(`scp write(mysql): cannot parse target table from '${sql.slice(0, 60)}…'`);
  return m[1];
}

/** The INSERT column list (for re-selecting a client-supplied composite PK). */
function insertCols(sql: string): string[] {
  const m = /\bINSERT\s+INTO\s+[A-Za-z0-9_."`]+\s*\(([^)]*)\)/i.exec(sql);
  if (m === null) return [];
  return m[1].split(',').map((c) => c.trim());
}

/**
 * Run ONE rendered tx statement through the async seam. On PG the RETURNING clause is native. On
 * MySQL (no native RETURNING) the emulation strips RETURNING, runs the write, and re-selects the
 * written row(s) by the REAL PK (auto-inc range, client PK, or composite) — mirroring the sync
 * driver + the 4 native runtimes. Returns `{ rows, changes }`.
 */
async function execStatementAsync(
  ctx: AsyncExecutionContext,
  op: TxOp,
  scope: Scope,
  dialect: MakeSQLDialect,
): Promise<{ rows: Record<string, unknown>[]; changes: number }> {
  const { sql, params } = renderStatement(op, scope, dialect);
  const hasReturn = /\bselect\b/i.test(sql.slice(0, 8)) || /\breturning\b/i.test(sql);

  if (dialect === 'mysql' && /\breturning\b/i.test(sql)) {
    const retMatch = /\s+RETURNING\s+(.+?)\s*$/is.exec(sql)!;
    const cols = stripMysqlPkHint(retMatch[1]).trim();
    const writeSql = stripMysqlPkHint(sql.slice(0, retMatch.index));
    const pk = parsePkHint(sql);
    const isInsert = /^\s*INSERT\b/i.test(writeSql);
    const info = await seamRunAsync(ctx, writeSql, params);
    if (!isInsert) return { rows: [], changes: info.changes };
    const insertId = Number(info.lastInsertRowid);
    let rows: Record<string, unknown>[];
    if (pk === null) {
      // Legacy auto-increment-`id` path (no PK hint). MySQL's insertId is the FIRST auto-inc id of
      // the batch; a multi-row `createMany` inserts `changes` consecutive ids [insertId, insertId +
      // changes). Re-select the whole range (mirrors the `pk.autoInc` branch below) so createMany
      // returns ALL affected PKs, not just the first row's. A single-row insert (changes = 1)
      // reduces to `id >= insertId AND id < insertId + 1`, i.e. the original single-row select.
      const count = Math.max(1, info.changes);
      rows = await seamExecuteAsync(ctx, `SELECT ${cols} FROM ${insertTable(writeSql)} WHERE id >= ? AND id < ?`, [insertId, insertId + count], { write: true });
    } else if (pk.autoInc && pk.cols.length === 1 && pk.cols[0] === pk.autoInc) {
      rows = await seamExecuteAsync(ctx, `SELECT ${cols} FROM ${insertTable(writeSql)} WHERE ${pk.autoInc} >= ? AND ${pk.autoInc} < ?`, [insertId, insertId + Math.max(1, info.changes)], { write: true });
    } else {
      const insCols = insertCols(writeSql);
      const where = pk.cols.map((c) => `${c} = ?`).join(' AND ');
      const vals = pk.cols.map((c) => params[insCols.indexOf(c)]);
      rows = await seamExecuteAsync(ctx, `SELECT ${cols} FROM ${insertTable(writeSql)} WHERE ${where}`, vals, { write: true });
    }
    return { rows, changes: rows.length };
  }

  if (hasReturn) {
    const rows = await seamExecuteAsync(ctx, sql, params, { write: true });
    return { rows, changes: rows.length };
  }
  const info = await seamRunAsync(ctx, sql, params);
  return { rows: [], changes: info.changes };
}

/**
 * Options for the live async write entry {@link executeTransactionAsync} — the tx {@link
 * TransactionOptions} plus the write=tx `guard` policy (#86).
 */
export interface WriteExecOptions extends TransactionOptions {
  /**
   * Enforce the write=tx guard (#86 / #81 `checkWriteAllowed`): a write issued OUTSIDE a user
   * `transaction(fn)` throws {@link WriteOutsideTransactionError}; a write in a {@link
   * import('../tx-options').withReadOnly} scope throws {@link WriteInReadOnlyContextError}. This is
   * the DEFAULT for the public write path — writes require an explicit transaction (v1 parity,
   * `DBModel.ts:886`). Set `false` ONLY for the internal per-execution-ownership plane (the Phase A
   * ownership proofs that drive the plan executor as its OWN auto-tx). @default true
   */
  readonly guard?: boolean;
}

/**
 * Execute a derived {@link TransactionPlan} on a live PG / MySQL connection with gate-first
 * short-circuit and **per-execution connection ownership** (§3). The live-DB WRITE entry (#86).
 *
 * ## Ambient-tx JOIN vs. its own envelope (the #86 core)
 *
 * `withTransactionAsync` (:495) decides the envelope:
 *   - **inside a user `transaction(fn)`** (an outer connection is pinned in the ALS) → the write
 *     JOINS the outer: its statements run on the outer's owned connection with NO new BEGIN/COMMIT,
 *     so N writes in one boundary are ONE physical transaction (one BEGIN, one COMMIT, one conn);
 *   - **outside any transaction** → it opens its OWN BEGIN…COMMIT on a freshly-acquired owned
 *     connection (the per-execution auto-tx; concurrent calls each own a DISTINCT connection ⇒
 *     isolated).
 *
 * ## write=tx guard (#86, wired here — fires at runtime, not a standalone helper)
 *
 * With `options.guard` (DEFAULT true), a write with NO ambient user tx is REJECTED via {@link
 * checkWriteAllowed} BEFORE any SQL: `WriteOutsideTransactionError` (no active tx) /
 * `WriteInReadOnlyContextError` (read-only scope). The check runs at ENTRY — before
 * `withTransactionAsync` would open the write's own envelope — so it sees the CALLER's scope, exactly
 * mirroring v1 `DBModel._checkWriteAllowed` (:886, called at the public write entry, not the plan
 * executor). Inside a `transaction(fn)` the ambient marker is set ⇒ the guard passes and the write
 * joins. The structured {@link TransactionResult} is identical to the sync {@link executeTransaction}.
 */
export function executeTransactionAsync(
  ctx: PooledAsyncContext,
  plan: TransactionPlan,
  input: Scope,
  dialect: MakeSQLDialect = 'sqlite',
  options: WriteExecOptions = {},
): Promise<TransactionResult> {
  // write=tx guard (#86), enforced at ENTRY so it sees the CALLER's scope — a write inside a user
  // `transaction(fn)` has the ambient "inside a tx" marker set (⇒ passes + JOINS the outer); a bare
  // write outside any boundary has no marker (⇒ WriteOutsideTransactionError). Reject as a REJECTED
  // promise (never a synchronous throw) since this entry is async. Tx-control statements the runtime
  // itself issues (BEGIN/COMMIT) never pass through here — only data-write plans do.
  if (options.guard !== false) {
    // Run the guard, then the plan, as a REJECTED promise on failure (never a synchronous throw).
    // `checkWriteAllowed` mirrors v1 ordering (:886): read-only is rejected FIRST
    // (`WriteInReadOnlyContextError`), then a missing active tx (`WriteOutsideTransactionError`).
    // Inside a user `transaction(fn)` the ambient marker is set ⇒ neither fires and the write JOINS
    // the outer; outside any boundary the no-active-tx branch throws.
    return Promise.resolve().then(() => {
      checkWriteAllowed('WRITE', plan.statements[0]?.id);
      return runTransactionPlanAsync(ctx, plan, input, dialect, options);
    });
  }
  return runTransactionPlanAsync(ctx, plan, input, dialect, options);
}

/** The plan-executor body of {@link executeTransactionAsync}, split so the guard runs at entry. */
function runTransactionPlanAsync(
  ctx: PooledAsyncContext,
  plan: TransactionPlan,
  input: Scope,
  dialect: MakeSQLDialect,
  options: TransactionOptions,
): Promise<TransactionResult> {
  const isBatch =
    plan.entityFrom === null &&
    plan.statements.every((s) => s.gate === undefined && s.binds === undefined && s.role === 'body');

  return withTransactionAsync(ctx, async (txCtx) => {
    const executed: string[] = [];
    const scope: Scope = { ...input };
    let entity: Record<string, unknown> | null = null;
    const returnedRows: Record<string, unknown>[][] = [];

    for (const stmt of plan.statements) {
      const result = await execStatementAsync(txCtx, stmt.op, scope, dialect);
      executed.push(stmt.id);

      if (stmt.gate !== undefined) {
        const reason = gateShortCircuit(stmt.gate, result);
        if (reason !== null) {
          // A gate short-circuit ROLLBACKs the whole tx (atomicity): throw the sentinel so
          // `withTransactionAsync` runs ROLLBACK on the owned connection, then translate to the
          // structured non-committed result at the boundary.
          throw new GateShortCircuit(stmt.id, reason, executed);
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
    return { committed: true, entity, executed, ...(returnedRows.length > 0 ? { returnedRows } : {}) } as TransactionResult;
  }, options, dialect === 'sqlite' ? 'postgres' : dialect, isConnectionError).catch((e: unknown) => {
    // A gate short-circuit is NOT a failure — it ROLLBACKs and reports `committed:false`. Any other
    // error is a real driver failure (already rolled back by withTransactionAsync) → re-surface.
    if (e instanceof GateShortCircuit) {
      return { committed: false, shortCircuit: { statementId: e.statementId, reason: e.reason }, entity: null, executed: e.executed };
    }
    throw mapSqliteError(e);
  });
}

/** Internal sentinel: a gate short-circuit inside an async tx (ROLLBACK, then report non-committed). */
class GateShortCircuit extends Error {
  constructor(
    readonly statementId: string,
    readonly reason: ShortCircuitReason,
    readonly executed: readonly string[],
  ) {
    super(`scp write: gate short-circuit at '${statementId}' (${reason})`);
    this.name = 'GateShortCircuit';
  }
}
