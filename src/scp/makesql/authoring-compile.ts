/**
 * litedbmodel v2 SCP — the AUTHORING → `makeSQL` bundle path (Phase A of epic #43/#45).
 *
 * WS2 (`../authoring.ts`) lowers BOTH the SCP declaration blocks and the eager public
 * API through ONE bc compile path (`publishBehaviors` / `compileEager`) into ONE
 * portable `ComponentGraphIR` — a Component's body carries `componentRef` / `map` nodes
 * whose SQL-structural ports (`table`, `select`, `where`, `values.<field>`, `set.<field>`,
 * `order`, `limit`, …) are closed-set bc Expression IR (spec §7). Until now the ONLY
 * consumer of that authored IR was the REDUCED spine (`../bridge.ts` → `../compile-sqlite.ts`
 * → `CompiledOperation`).
 *
 * This module is the ADDITIVE makeSQL producer for the SAME authored IR: it reads a
 * node's ports STRUCTURALLY (exactly the closed-set decoding `../bridge.ts` performs —
 * same operators, same IN-sentinel, same SKIP-`cond` collapse, no invented opcode) and,
 * given the concrete Input Port values, routes WHERE / CRUD / relation through
 * `./compile*` + `./json-array` / `./json-batch` to emit a **`makeSQL` bundle** (composed
 * `makeSQL` components). The value slots are resolved HANDLER-SIDE (spec §11 item 4): a bc
 * `makeSQL` catalog leaf whose handler holds the concrete port values from bc's
 * `runBehavior` and calls `./compile*` to build the byte-tuned SQL — so the emitted SQL is
 * v1-tuned by construction (PG byte-match; MySQL/SQLite single-JSON-param parity).
 *
 * ## Single compile path (spec §9 — preserved)
 *
 * Both the eager public-API path (`compileEager`) and the declaration path
 * (`publishBehaviors`) produce byte-identical authored component IR (pinned by
 * `test/scp/authoring.test.ts`). Because {@link compileAuthoredBehavior} is a pure function
 * of that IR + the concrete input, feeding EITHER path's contract through it yields a
 * byte-identical `makeSQL` bundle — the single-compile-path invariant carries over to the
 * makeSQL target with no second interpreter.
 *
 * Coverage (Phase A): a read behavior (Select + belongsTo `.map` + hasMany-limit `.map`),
 * a write behavior (Insert), and a SKIP-optional condition. The three dialects share this
 * routing; PG stays byte-identical to v1, MySQL/SQLite use the single-JSON-param forms.
 */

import { evaluateExpression, type Scope, type Value } from 'behavior-contracts';
import type { Component, ComponentRefNode, MapNode, FanoutNode } from '../authoring';
import type { BehaviorModelContract } from '../authoring';
import { IN_SENTINEL } from './tx';
import type { ConditionObject, ConditionValue } from '../../DBConditions';
import type { MakeSQL } from './makesql';
import { assembleMakeSQL } from './makesql';
import { compileSelect } from './compile-select';
import { compileInsert, compileUpdateSingle, compileDelete } from './compile-crud';
import {
  compileSingleKeyUnlimited,
  compileSingleKeyLimited,
} from './compile-relation';
import type { Dialect } from './handler';

// ============================================================================
// Structural port readers — the SAME closed-set decoding `../bridge.ts` performs,
// specialized to keep the RAW value Expression IR (so it can be evaluated against a
// concrete input scope, handler-side). No evaluation happens here.
// ============================================================================

/** A bc body node that references a catalog component (`componentRef` or `map`). */
type RefLike = ComponentRefNode | MapNode;

function isMap(n: Component['body'][number]): n is MapNode {
  return 'map' in n;
}

/** A bc 0.7.3+ `FanoutNode` (connection fan-out). litedbmodel never emits these. */
function isFanout(n: Component['body'][number]): n is FanoutNode {
  return 'fanout' in n;
}

/** The catalog-name + ports of a `componentRef`/`map` body node (uniform view). */
function nodeRef(n: RefLike): { component: string; ports: Record<string, unknown> } {
  return isMap(n) ? { component: n.map.component, ports: n.map.ports } : { component: n.component, ports: n.ports };
}

/** A port that must be a literal string (`table`, `order`, `group`, `returning`). */
function stringPort(ports: Record<string, unknown>, name: string): string | undefined {
  const v = ports[name];
  if (v === undefined) return undefined;
  if (typeof v !== 'string') {
    throw new Error(`authoring-compile: port '${name}' must be a literal string in the IR (got ${JSON.stringify(v)})`);
  }
  return v;
}

/** A `{arr:[...]}` port → its element array (a bc-lowered literal array). */
function arrPort(ports: Record<string, unknown>, name: string): unknown[] | undefined {
  const v = ports[name];
  if (v === undefined) return undefined;
  if (typeof v === 'object' && v !== null && 'arr' in v && Array.isArray((v as { arr: unknown }).arr)) {
    return (v as { arr: unknown[] }).arr;
  }
  throw new Error(`authoring-compile: port '${name}' must be an {arr:[...]} literal in the IR (got ${JSON.stringify(v)})`);
}

/** `select` string-array port → `string[]` (elements must be literals). */
function stringArrayPort(ports: Record<string, unknown>, name: string): string[] | undefined {
  const arr = arrPort(ports, name);
  if (arr === undefined) return undefined;
  return arr.map((e) => {
    if (typeof e !== 'string') throw new Error(`authoring-compile: '${name}' entries must be literal strings (got ${JSON.stringify(e)})`);
    return e;
  });
}

/** The single-key operator name of an Expression node, or undefined if not a 1-key object. */
function opKey(node: unknown): string | undefined {
  if (node === null || typeof node !== 'object' || Array.isArray(node)) return undefined;
  const keys = Object.keys(node as object);
  return keys.length === 1 ? keys[0] : undefined;
}

/** The column name carried by a `ref`/`refOpt` path (its LAST segment — spec §7). */
function columnOf(node: unknown, ctx: string): string {
  const op = opKey(node);
  if (op !== 'ref' && op !== 'refOpt') {
    throw new Error(`authoring-compile: ${ctx}: the column operand must be a {ref:[...]} / {refOpt:[...]} path, got ${JSON.stringify(node)}`);
  }
  const path = (node as Record<string, unknown[]>)[op];
  if (!Array.isArray(path) || path.length === 0 || typeof path[path.length - 1] !== 'string') {
    throw new Error(`authoring-compile: ${ctx}: column ref path must be a non-empty string path`);
  }
  return path[path.length - 1] as string;
}

/**
 * If a column operand is an IN-list membership marker (its `ref`/`refOpt` path head is
 * {@link IN_SENTINEL}), return the real column name; otherwise undefined. Same rule as
 * `../bridge.ts` — the sentinel keeps IN expressible with only `eq` + `ref`.
 */
function inSentinelColumn(node: unknown): string | undefined {
  const op = opKey(node);
  if (op !== 'ref' && op !== 'refOpt') return undefined;
  const path = (node as Record<string, unknown[]>)[op];
  if (Array.isArray(path) && path.length >= 2 && path[0] === IN_SENTINEL && typeof path[path.length - 1] === 'string') {
    return path[path.length - 1] as string;
  }
  return undefined;
}

/**
 * Coerce a bc-evaluated integer Value to a JS `number` for INLINE numeric SQL text
 * (`LIMIT n` / `OFFSET n`, which the original `_buildSelectSQL` inlines, not binds). bc's
 * value model encodes an integer LITERAL as `bigint`; a `{ref}` reading a JS number stays
 * a number. Both normalize to a JS number for the inline text.
 */
function asInt(v: Value): number {
  if (typeof v === 'bigint') return Number(v);
  if (typeof v === 'number') return v;
  throw new Error(`authoring-compile: limit/offset must evaluate to an integer, got ${JSON.stringify(v as unknown)}`);
}

/** Read the two operands of a binary comparison node, fail-closed on arity. */
function binOperands(node: unknown, op: string, at: string): [unknown, unknown] {
  const args = (node as Record<string, unknown[]>)[op];
  if (!Array.isArray(args) || args.length !== 2) {
    throw new Error(`authoring-compile: ${at}: '${op}' expects exactly 2 operands`);
  }
  return [args[0], args[1]];
}

/**
 * bc comparison operator → the SQL operator text. v1 `DBConditions` expresses a
 * non-equality comparison as a CUSTOM-OP KEY carrying its own `?` placeholder
 * (`{ '<col> <op> ?': value }` → emits `<col> <op> ?` with the value pushed) — there is
 * NO `{ col: { '<op>': v } }` object form. `ne` against a non-null value maps to `<>`.
 */
const CMP_OPS: Record<string, string> = { lt: '<', le: '<=', gt: '>', ge: '>=', ne: '<>' };

// ============================================================================
// WHERE member → v1 ConditionObject entry (value resolved against the concrete scope).
//
// A `{cond:[c, member, null]}` node is the SKIP-optional collapse: the inner `member`
// contributes to the ConditionObject ONLY when `c` evaluates to a present binding
// (non-null / true), byte-identical to the render.ts existence rule (dynamic-expansion
// spec §2). This is where the makeSQL path realizes SKIP: an absent optional member adds
// no column key at all (so v1 `DBConditions` emits neither its text nor its param).
// ============================================================================

/**
 * Fold ONE authored where-member Expression node into a v1 {@link ConditionObject},
 * evaluating its value operand against `scope`. Mutates `into` (the accumulator) so
 * declaration order (= SQL AND order) is preserved.
 */
function foldWhereMember(node: unknown, scope: Scope, into: ConditionObject, at: string): void {
  const op = opKey(node);
  if (op === undefined) {
    throw new Error(`authoring-compile: ${at}: a where member must be a single-operator Expression node, got ${JSON.stringify(node)}`);
  }

  if (op === 'cond') {
    // SKIP-optional: {cond:[c, <member>, null]}. Present iff `c` is a present binding.
    const args = (node as Record<string, unknown[]>).cond;
    if (!Array.isArray(args) || args.length !== 3 || args[2] !== null) {
      throw new Error(`authoring-compile: ${at}: a SKIP-optional condition must be {cond:[c, <member>, null]} (else = null)`);
    }
    const present = evaluateExpression(args[0], scope);
    if (present !== null && present !== false) foldWhereMember(args[1], scope, into, `${at}.cond.then`);
    return;
  }

  if (op === 'and') {
    // A flat AND group folds its members into the same ConditionObject (v1 default AND).
    const args = (node as Record<string, unknown[]>).and;
    if (!Array.isArray(args) || args.length < 2) throw new Error(`authoring-compile: ${at}: 'and' group expects >= 2 members`);
    args.forEach((m, i) => foldWhereMember(m, scope, into, `${at}.and[${i}]`));
    return;
  }

  if (op === 'or') {
    // An OR group is v1's `__or__` list of sub-condition objects.
    const args = (node as Record<string, unknown[]>).or;
    if (!Array.isArray(args) || args.length < 2) throw new Error(`authoring-compile: ${at}: 'or' group expects >= 2 members`);
    const branches = args.map((m, i) => {
      const sub: ConditionObject = {};
      foldWhereMember(m, scope, sub, `${at}.or[${i}]`);
      return sub;
    });
    const existing = (into.__or__ as unknown as ConditionObject[] | undefined) ?? [];
    into.__or__ = [...existing, ...branches] as unknown as ConditionValue;
    return;
  }

  if (op === 'eq') {
    const [col, val] = binOperands(node, op, at);
    const inCol = inSentinelColumn(col);
    if (inCol !== undefined) {
      // IN-list membership: v1 reads an array value as `IN (?, …)` (or the JSON single-param
      // form on MySQL/SQLite via `conditionsFor`); empty array degenerates to `1 = 0`.
      into[inCol] = evaluateExpression(val, scope) as ConditionValue;
      return;
    }
    const value = evaluateExpression(val, scope);
    // IS NULL: `eq(col, null)` → v1 emits `col IS NULL` for a null value.
    into[columnOf(col, at)] = value as ConditionValue;
    return;
  }

  if (op in CMP_OPS) {
    const [col, val] = binOperands(node, op, at);
    const column = columnOf(col, at);
    const value = evaluateExpression(val, scope);
    // `ne(col, null)` ⇒ v1 `col IS NOT NULL` (custom-op key with no placeholder).
    if (op === 'ne' && value === null) {
      into[`${column} IS NOT NULL`] = true as ConditionValue;
      return;
    }
    // v1 custom-op form: the KEY carries its own `?` (`{ '<col> <op> ?': value }`).
    into[`${column} ${CMP_OPS[op]} ?`] = value as ConditionValue;
    return;
  }

  throw new Error(`authoring-compile: ${at}: unsupported where operator '${op}' (supported: eq/ne/lt/le/gt/ge/and/or/cond; IN via ${IN_SENTINEL} column head; IS NULL via eq(col,null))`);
}

/** Fold the `where` port (`{arr:[...]}` of member nodes) into a v1 `ConditionObject`. */
function foldWherePort(ports: Record<string, unknown>, scope: Scope, at: string): ConditionObject {
  const arr = arrPort(ports, 'where');
  const conditions: ConditionObject = {};
  if (arr === undefined) return conditions;
  arr.forEach((m, i) => foldWhereMember(m, scope, conditions, `${at}.where[${i}]`));
  return conditions;
}

/**
 * Collect a flattened write record family (`<prefix>.<field>` ports) → an ordered
 * `Record<field, Value>` with each value slot evaluated against `scope`. Field order =
 * declaration (insertion) order of the ports — the SQL column order.
 */
function collectFamily(ports: Record<string, unknown>, prefix: string, scope: Scope): Record<string, Value> {
  const out: Record<string, Value> = {};
  for (const k of Object.keys(ports)) {
    if (k.startsWith(`${prefix}.`)) out[k.slice(prefix.length + 1)] = evaluateExpression(ports[k], scope);
  }
  return out;
}

// ============================================================================
// Node → makeSQL bundle (handler-side compile: concrete port values → ./compile*).
// ============================================================================

/**
 * Compile ONE authored catalog `componentRef`/`map` node into a `makeSQL` bundle, given
 * the concrete input `scope`. Routes WHERE / CRUD through `./compile*`. The value slots
 * are the bc-evaluated concrete values (handler-side), so the emitted SQL text/params
 * come STRAIGHT from the v1 builders (`compileSelect` / `compileInsert` / …) —
 * byte-parity by construction.
 */
export function compileAuthoredNode(node: RefLike, scope: Scope, dialect: Dialect): MakeSQL {
  const { component, ports } = nodeRef(node);
  const table = stringPort(ports, 'table');
  if (table === undefined) throw new Error(`authoring-compile: ${component} node requires a literal 'table' port`);
  const at = `${component}`;

  switch (component) {
    case 'Select': {
      const select = stringArrayPort(ports, 'select');
      const conditions = foldWherePort(ports, scope, at);
      return compileSelect({
        dialect,
        tableName: table,
        select: select ? select.join(', ') : undefined,
        conditions,
        order: stringPort(ports, 'order'),
        group: stringPort(ports, 'group'),
        limit: ports.limit !== undefined ? asInt(evaluateExpression(ports.limit, scope)) : undefined,
        offset: ports.offset !== undefined ? asInt(evaluateExpression(ports.offset, scope)) : undefined,
      });
    }
    case 'Insert': {
      const values = collectFamily(ports, 'values', scope);
      if (Object.keys(values).length === 0) throw new Error(`authoring-compile: Insert requires at least one 'values.<field>' port`);
      const returning = stringPort(ports, 'returning');
      // A single authored Insert is ONE row → the ORIGINAL single-row `buildInsert`
      // (`VALUES (?, …)`, byte-identical to v1). The JSON single-param batch form is
      // reserved for multi-row `createMany` (spec §43/§45), not the authoring single
      // Insert. Columns are CANONICAL (alphabetical) sorted — the v2 write-path SSoT
      // (`DBModel._insert` single-record fast path aligns to the same order).
      const columns = Object.keys(values).sort();
      return compileInsert(dialect, { tableName: table, columns, records: [values], returning });
    }
    case 'Update': {
      const set = collectFamily(ports, 'set', scope);
      if (Object.keys(set).length === 0) throw new Error(`authoring-compile: Update requires at least one 'set.<field>' port`);
      const conditions = foldWherePort(ports, scope, at);
      return compileUpdateSingle({
        dialect,
        tableName: table,
        serializedValues: set,
        conditions,
        returning: stringPort(ports, 'returning'),
      });
    }
    case 'Delete': {
      const conditions = foldWherePort(ports, scope, at);
      return compileDelete({ dialect, tableName: table, conditions, returning: stringPort(ports, 'returning') });
    }
    default:
      throw new Error(`authoring-compile: catalog component '${component}' has no makeSQL compile (SQL CRUD only: Select/Insert/Update/Delete)`);
  }
}

// ============================================================================
// Relation `.map` node → batch-load makeSQL (belongsTo / hasMany, ±per-parent limit).
//
// The authored relation is a bc `map` node: `over` is the parent Select result (a wire),
// `as` binds each parent row, and its `where` is `[ eq($p.<parentKey>, $p.<parentKey>) ]`
// (belongsTo/hasMany key match). In the batch (makeSQL) world this is ONE relation query
// keyed by the DISTINCT parent-key values — exactly `LazyRelation`'s batch load. We read
// the target column from the map's `where` (the IN/eq column) and the key values from the
// parent rows, then route to `./compile-relation`.
// ============================================================================

/** A relation `.map` compiled against the resolved PARENT rows (the batch key source). */
export interface RelationMapInput {
  /** The parent result rows (from executing the parent Select node). */
  parentRows: Record<string, unknown>[];
  /** Per-parent limit (undefined ⇒ unlimited batch load). */
  limit?: number;
}

/**
 * Compile a relation `.map` node into a batch-load `makeSQL` bundle. The map's inner
 * `where` names the target key column (`eq($p.<k>, …)`); the batch key VALUES are the
 * distinct `parentKey` values across `parentRows`. Extra (non-key) where members become
 * the relation's merged `conditions` filter.
 *
 * @param parentKey the parent-row field whose values key the batch (e.g. `author_id`).
 */
export function compileRelationMap(
  node: MapNode,
  input: RelationMapInput,
  dialect: Dialect,
  parentKey: string,
): MakeSQL {
  const ports = node.map.ports;
  const table = stringPort(ports, 'table');
  if (table === undefined) throw new Error(`authoring-compile: relation map requires a literal 'table' port`);
  const select = stringArrayPort(ports, 'select');
  const order = stringPort(ports, 'order');

  // The target key column = the single eq-member column of the relation `where`.
  const whereArr = arrPort(ports, 'where') ?? [];
  let targetKey: string | undefined;
  const filters: ConditionObject = {};
  whereArr.forEach((m, i) => {
    const op = opKey(m);
    if (op === 'eq') {
      const [col] = binOperands(m, 'eq', `relationMap.where[${i}]`);
      const inCol = inSentinelColumn(col);
      const name = inCol ?? columnOf(col, `relationMap.where[${i}]`);
      if (targetKey === undefined) {
        targetKey = name; // first eq is the join key
        return;
      }
    }
    // Any further member is a relation filter (folded against an empty scope — filters
    // here are static; dynamic relation filters are out of Phase A scope).
    foldWhereMember(m, {}, filters, `relationMap.where[${i}]`);
  });
  if (targetKey === undefined) throw new Error(`authoring-compile: relation map 'where' must carry a key eq(...) member`);

  const values = distinctKeys(input.parentRows, parentKey);
  const base = {
    dialect,
    tableName: table,
    conditions: Object.keys(filters).length > 0 ? filters : undefined,
    order,
    targetKey,
    values,
  };
  // Unlimited honors the per-column projection (v1 `WHERE key IN … [ORDER BY]`); the
  // per-parent-limit ROW_NUMBER/LATERAL shape projects the outer `*` (the compile-relation
  // anchor — v1 `SELECT * FROM ranked`), so `select` is not forwarded to the limited path.
  return input.limit !== undefined
    ? compileSingleKeyLimited({ ...base, limit: input.limit })
    : compileSingleKeyUnlimited({ ...base, select: select ? select.join(', ') : undefined });
}

/** Distinct, non-null parent-key values in first-seen order (the batch IN-list source). */
function distinctKeys(rows: Record<string, unknown>[], key: string): unknown[] {
  const seen = new Set<unknown>();
  const out: unknown[] = [];
  for (const r of rows) {
    const v = r[key];
    if (v === null || v === undefined) continue;
    if (!seen.has(v)) {
      seen.add(v);
      out.push(v);
    }
  }
  return out;
}

// ============================================================================
// Behavior → makeSQL bundle(s) — the public authoring→makeSQL entrypoint.
// ============================================================================

/** The compiled makeSQL artifact of one authored behavior method. */
export interface AuthoredMakeSQL {
  /** The behavior (component) name. */
  readonly name: string;
  /** The PRIMARY read/write query bundle (the root catalog node of the method). */
  readonly primary: MakeSQL;
  /** The relation `.map` nodes of the method (batch loads), in body order (unresolved). */
  readonly relations: MapNode[];
}

/**
 * Compile the PRIMARY read/write query of an authored behavior method into a `makeSQL`
 * bundle, given the concrete Input Port `scope`. The "primary" node is the method's root
 * catalog `componentRef` (the Select the relations map over, or the write). Relation
 * `.map` nodes are returned unresolved (their batch keys come from executing `primary`
 * first — {@link compileRelationMap}).
 *
 * This is a PURE function of the authored component IR + the input, so `publishBehaviors`
 * (declaration) and `compileEager` (eager) — which produce byte-identical component IR —
 * yield a byte-identical `primary` bundle (single-compile-path invariant, spec §9).
 */
export function compileAuthoredBehavior(
  contract: BehaviorModelContract,
  scope: Scope,
  dialect: Dialect,
  entry?: string,
): AuthoredMakeSQL {
  const component = entry
    ? contract.components.find((c) => c.name === entry)
    : contract.components[0];
  if (component === undefined) {
    throw new Error(`authoring-compile: entry component '${entry ?? '<first>'}' not found in contract`);
  }

  // The primary node is the first catalog `componentRef` (root); `map` nodes are relations.
  const relations: MapNode[] = [];
  let primaryNode: ComponentRefNode | undefined;
  for (const n of component.body) {
    if ('cond' in n) continue; // pure Expression node (a shared SKIP guard), no SQL.
    if (isFanout(n)) {
      // bc 0.7.3+ `FanoutNode`. litedbmodel never emits fanout — reject fail-closed rather
      // than mistake it for the primary catalog `componentRef`.
      throw new Error(`authoring-compile: behavior '${component.name}' node '${n.id}' is a fanout node, not supported by litedbmodel (bc FanoutNode)`);
    }
    if (isMap(n)) {
      relations.push(n);
      continue;
    }
    if (primaryNode === undefined) primaryNode = n;
  }
  if (primaryNode === undefined) {
    throw new Error(`authoring-compile: behavior '${component.name}' has no primary catalog node`);
  }

  const normalized = normalizeInput(component, scope);
  return {
    name: component.name,
    primary: compileAuthoredNode(primaryNode, normalized, dialect),
    relations,
  };
}

/**
 * Normalize the caller input to present-as-null for every OPTIONAL Input Port the caller
 * omitted, so a SKIP-optional guard reading that port (`ne($.status, null)`) evaluates
 * against a present `null` (→ the guard is false → the member drops) rather than throwing
 * `UNKNOWN_BINDING`. This is the SAME absent-key SKIP rule the reduced runtime applies
 * (`../runtime.ts` `normalizeInput`), driven by the SSoT — never an ad-hoc code default. A
 * head is OPTIONAL iff EITHER (a) the component's Input Port schema marks it `required !==
 * true`, OR (b) it drives a SKIP `cond` guard (bc infers erased-type ports as `required`,
 * so a SKIP-guarded head declares its driving input optional — spec §7). A REQUIRED,
 * non-SKIP head that is missing is left ABSENT so a real wiring bug surfaces loudly as
 * bc's `UNKNOWN_BINDING`, not a silent default.
 */
function normalizeInput(component: Component, input: Scope): Scope {
  const optional = new Set<string>();
  for (const [port, schema] of Object.entries(component.inputPorts)) {
    if (schema.required !== true) optional.add(port);
  }
  for (const n of component.body) skipGuardHeads(n, optional);

  const out: Scope = { ...input };
  for (const head of optional) {
    if (!(head in out)) out[head] = null;
  }
  return out;
}

/**
 * Collect the input heads that DRIVE a SKIP `cond` guard anywhere in a body node's ports
 * (the `c` of a `{cond:[c, member, null]}` where-member). Those heads are optional (their
 * absence must present as `null` so the guard drops the member). Reads the guard condition
 * expression's `ref`/`refOpt` PATH HEADS structurally — no evaluation.
 */
function skipGuardHeads(n: Component['body'][number], into: Set<string>): void {
  if ('cond' in n) return; // a shared pure Expression node, not a catalog SQL node
  if (isFanout(n)) return; // bc FanoutNode carries no where-ports SKIP guard (litedbmodel never emits it)
  const ports = isMap(n) ? n.map.ports : n.ports;
  const where = ports.where;
  if (typeof where !== 'object' || where === null || !('arr' in where)) return;
  const arr = (where as { arr: unknown[] }).arr;
  if (!Array.isArray(arr)) return;
  for (const m of arr) {
    if (opKey(m) === 'cond') {
      const cond = (m as Record<string, unknown[]>).cond;
      if (Array.isArray(cond) && cond.length === 3) collectRefHeads(cond[0], into);
    }
  }
}

/** Collect every `ref`/`refOpt` path HEAD reachable in an Expression IR node. */
function collectRefHeads(node: unknown, into: Set<string>): void {
  if (node === null || typeof node !== 'object') return;
  if (Array.isArray(node)) {
    for (const e of node) collectRefHeads(e, into);
    return;
  }
  const op = opKey(node);
  if (op === 'ref' || op === 'refOpt') {
    const path = (node as Record<string, unknown[]>)[op];
    if (Array.isArray(path) && path.length > 0 && typeof path[0] === 'string') into.add(path[0] as string);
    return;
  }
  for (const v of Object.values(node as Record<string, unknown>)) collectRefHeads(v, into);
}

/** Assemble + dialect-render a compiled `makeSQL` bundle (thin convenience for callers). */
export { assembleMakeSQL };
