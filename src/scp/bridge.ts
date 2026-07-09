/**
 * litedbmodel v2 SCP — the WS1↔WS3 Backend-Compile bridge (WS3, #23).
 *
 * WS2 (`authoring.ts`) produces the REAL behavior-contracts `ComponentGraphIR`: a
 * Component's body carries `componentRef` / `map` nodes whose `ports` are Expression IR.
 * The SQL-structural ports (`where`, `select`, `values.<field>`, `set.<field>`, `limit`,
 * `order`, …) are NOT plain scalars — the `where` port is `{arr:[ <cond-expr>, … ]}` where
 * a SKIP-optional condition is a pure `{cond:[c, <frag-expr>, null]}` node (WS2), and a
 * comparison condition is a closed-set Expression `{eq:[<colRef>, <valueExpr>]}` (spec §7).
 *
 * WS1's Backend-Compile (`compile-sqlite.ts`) consumes a structured `SelectDesc` /
 * `Condition[]`. NOTHING produced that shape from the real IR — this module closes that
 * gap. It reads a raw `componentRef`/`map` node's ports directly and lowers them to WS1's
 * `CompiledOperation` (SQL text + fragment tree + static param slots), performing the
 * **SKIP → fragment-existence collapse**: a `{cond:[c, frag, null]}` where-member becomes a
 * fragment guarded by `when: c` (present iff `c` is present), exactly the existence rule
 * `render.ts` implements (dynamic-expansion spec §2). This is the single place that maps
 * the real bc port shape → WS1 compile input; WS1's compiler is unchanged (its golden
 * SQL text is the pinned bar) and now has a producer for its `SelectDesc`/`Condition`.
 *
 * ## WHERE condition interpretation (spec §7) — closed-set ONLY
 *
 * SKIP existence, IN-list membership and IS NULL are **fragment structure, NOT Expression
 * IR opcodes** (catalog `where` port type is `fragment`; bc `expression-ir.md` §4: SKIP is
 * OUTSIDE Expression IR). But bc's authoring lowering (`compileBehaviors`) only emits
 * closed-set Expression IR into a port, so every where-member arrives as a closed-set node.
 * This bridge therefore reads them STRUCTURALLY into fragment kinds WITHOUT inventing any
 * opcode (the hard rule): every member is one of bc's closed operators, decoded here.
 *
 *   - `{eq:[<colRef>, null]}`          → `{ kind:'isNull', column }`  (SQL `col IS NULL`)
 *   - `{eq:[<colRef>, <valueExpr>]}`   → `{ kind:'eq',  column, value:<valueExpr> }`
 *   - `{eq:[{ref:[IN_SENTINEL, col]}, <arrExpr>]}` → `{ kind:'in', column, value:<arrExpr> }`
 *   - `{lt|le|gt|ge|ne:[<colRef>, v]}` → `{ kind:'cmp', column, op, value:v }`
 *   - `{and|or:[m1, m2, …]}`           → a nested `{ kind:'group', connector, conditions }`
 *   - `{cond:[c, <member>, null]}`     → the lowered `<member>` with `skipWhen: c` (SKIP)
 *
 * The LEFT operand of a comparison names the column (spec §7 `{ref:["author_id"]}`): its
 * `ref` path's LAST segment is the column name. The RIGHT operand is the value slot — a
 * closed-set Expression IR node bound at render time. The litedbmodel authoring helpers
 * ({@link import('./authoring-sql')}) produce exactly these closed-set encodings, so the
 * WS2 portability guard passes and this decoder round-trips them to the WS1 fragment tree.
 */

/** The reserved column-ref path head that marks an IN-list membership (see module doc). */
export const IN_SENTINEL = '@in';

import type { Component, ComponentRefNode, MapNode } from './authoring';
import {
  compileSelect,
  compileInsert,
  compileUpdate,
  compileDelete,
  type Condition,
  type SelectDesc,
  type InsertDesc,
  type UpdateDesc,
  type DeleteDesc,
} from './compile-sqlite';
import type { CompiledOperation, ExprNode } from './ir';

/** A bc body node that references a catalog component (not a `cond` node). */
type RefLike = ComponentRefNode | MapNode;

function isMap(n: Component['body'][number]): n is MapNode {
  return 'map' in n;
}

/** The catalog-name + ports of a `componentRef`/`map` body node (uniform view). */
function nodeRef(n: RefLike): { component: string; ports: Record<string, unknown> } {
  return isMap(n) ? { component: n.map.component, ports: n.map.ports } : { component: n.component, ports: n.ports };
}

// ── Port readers (structural — no evaluation) ─────────────────────────────────

/** A port that must be a literal string (`table`, `order`, `group`, `returning`). */
function stringPort(ports: Record<string, unknown>, name: string): string | undefined {
  const v = ports[name];
  if (v === undefined) return undefined;
  if (typeof v !== 'string') {
    throw new Error(`bridge: port '${name}' must be a literal string in the IR (got ${JSON.stringify(v)})`);
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
  throw new Error(`bridge: port '${name}' must be an {arr:[...]} literal in the IR (got ${JSON.stringify(v)})`);
}

/** `select` / `returning` string-array port → `string[]` (elements must be literals). */
function stringArrayPort(ports: Record<string, unknown>, name: string): string[] | undefined {
  const arr = arrPort(ports, name);
  if (arr === undefined) return undefined;
  return arr.map((e) => {
    if (typeof e !== 'string') throw new Error(`bridge: '${name}' entries must be literal strings (got ${JSON.stringify(e)})`);
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
    throw new Error(`bridge: ${ctx}: the column operand must be a {ref:[...]} / {refOpt:[...]} path, got ${JSON.stringify(node)}`);
  }
  const path = (node as Record<string, unknown[]>)[op];
  if (!Array.isArray(path) || path.length === 0 || typeof path[path.length - 1] !== 'string') {
    throw new Error(`bridge: ${ctx}: column ref path must be a non-empty string path`);
  }
  return path[path.length - 1] as string;
}

/** bc comparison operator → SQL operator (spec §7 comparison fragment). */
type CmpOp = '<' | '<=' | '>' | '>=' | '<>';
const CMP_OPS: Record<string, CmpOp> = {
  lt: '<',
  le: '<=',
  gt: '>',
  ge: '>=',
  ne: '<>',
};

/**
 * If a column operand is an IN-list membership marker (its `ref`/`refOpt` path head is
 * {@link IN_SENTINEL}), return the real column name (the segment AFTER the sentinel);
 * otherwise undefined. The sentinel keeps IN-list expressible with only the closed-set
 * `eq` + `ref` operators (no invented `in` opcode).
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
 * Lower ONE where-member Expression node to a WS1 {@link Condition} (structural read).
 * A `{cond:[c, member, null]}` node is the SKIP-optional collapse: the inner `member` is
 * lowered and its `skipWhen` set to `c` (present iff `c` is present — dynamic-expansion §2).
 */
function lowerWhereMember(node: unknown, at: string): Condition {
  const op = opKey(node);
  if (op === undefined) {
    throw new Error(`bridge: ${at}: a where member must be a single-operator Expression node, got ${JSON.stringify(node)}`);
  }

  if (op === 'cond') {
    // SKIP-optional: {cond:[c, <member>, null]}. The else branch MUST be null (absence).
    const args = (node as Record<string, unknown[]>).cond;
    if (!Array.isArray(args) || args.length !== 3 || args[2] !== null) {
      throw new Error(`bridge: ${at}: a SKIP-optional condition must be {cond:[c, <member>, null]} (else = null)`);
    }
    const inner = lowerWhereMember(args[1], `${at}.cond.then`);
    if (inner.skipWhen !== undefined) {
      throw new Error(`bridge: ${at}: nested SKIP guards are not supported (one cond per member)`);
    }
    return { ...inner, skipWhen: args[0] as ExprNode };
  }

  if (op === 'and' || op === 'or') {
    // A nested group: {and|or:[m1, m2, ...]} → a Condition group.
    const args = (node as Record<string, unknown[]>)[op];
    if (!Array.isArray(args) || args.length < 2) {
      throw new Error(`bridge: ${at}: '${op}' group expects >= 2 members`);
    }
    return {
      kind: 'group',
      connector: op === 'and' ? 'AND' : 'OR',
      conditions: args.map((m, i) => lowerWhereMember(m, `${at}.${op}[${i}]`)),
    };
  }

  if (op === 'eq') {
    const [col, val] = binOperands(node, op, at);
    // IN-list membership: the column ref path head is the reserved IN_SENTINEL.
    const inCol = inSentinelColumn(col);
    if (inCol !== undefined) {
      return { kind: 'in', column: inCol, value: val as ExprNode };
    }
    // IS NULL: `eq(col, null)` (a null literal RHS) → SQL `col IS NULL` (v1 parity).
    if (val === null) {
      return { kind: 'isNull', column: columnOf(col, at) };
    }
    return { kind: 'eq', column: columnOf(col, at), value: val as ExprNode };
  }

  if (op in CMP_OPS) {
    const [col, val] = binOperands(node, op, at);
    return { kind: 'cmp', column: columnOf(col, at), op: CMP_OPS[op], value: val as ExprNode };
  }

  throw new Error(`bridge: ${at}: unsupported where operator '${op}' (supported: eq/ne/lt/le/gt/ge/and/or/cond; IN via ${IN_SENTINEL} column head; IS NULL via eq(col,null))`);
}

/** Read the two operands of a binary comparison node, fail-closed on arity. */
function binOperands(node: unknown, op: string, at: string): [unknown, unknown] {
  const args = (node as Record<string, unknown[]>)[op];
  if (!Array.isArray(args) || args.length !== 2) {
    throw new Error(`bridge: ${at}: '${op}' expects exactly 2 operands`);
  }
  return [args[0], args[1]];
}

/** Lower the `where` port (`{arr:[...]}` of member nodes) to a WS1 `Condition[]`. */
function lowerWherePort(ports: Record<string, unknown>, at: string): Condition[] | undefined {
  const arr = arrPort(ports, 'where');
  if (arr === undefined) return undefined;
  return arr.map((m, i) => lowerWhereMember(m, `${at}.where[${i}]`));
}

// ── The write value/set record families (`values.<field>` / `set.<field>`) ────

/**
 * Collect a flattened write record family (`<prefix>.<field>` ports) → an ordered
 * `Record<field, ExprNode>`. Field order = declaration (insertion) order of the ports,
 * which is the SQL column order the golden pins.
 */
function collectFamily(ports: Record<string, unknown>, prefix: string): Record<string, ExprNode> {
  const out: Record<string, ExprNode> = {};
  for (const k of Object.keys(ports)) {
    if (k.startsWith(`${prefix}.`)) out[k.slice(prefix.length + 1)] = ports[k] as ExprNode;
  }
  return out;
}

/** Parse the Insert conflict ports into WS1's `onConflict` / `onConflictAction`. */
function conflictOf(ports: Record<string, unknown>): Pick<InsertDesc, 'onConflict' | 'onConflictAction'> {
  const onConflict = stringPort(ports, 'onConflict');
  const action = stringPort(ports, 'onConflictAction');
  if (onConflict === undefined) return {};
  const keys = onConflict.split(',').map((s) => s.trim()).filter((s) => s.length > 0);
  if (action === undefined || action === 'ignore') {
    return { onConflict: keys, onConflictAction: 'ignore' };
  }
  if (action === 'update') {
    // Update all inserted columns except the conflict keys (SQLite `excluded.*` upsert).
    return { onConflict: keys, onConflictAction: { updateColumns: 'all' } };
  }
  // `update:col1,col2` — explicit update column list.
  const m = /^update:(.+)$/.exec(action);
  if (m) {
    return { onConflict: keys, onConflictAction: { updateColumns: m[1].split(',').map((s) => s.trim()) } };
  }
  throw new Error(`bridge: unsupported onConflictAction '${action}' (expected 'ignore' / 'update' / 'update:col,col')`);
}

/** RETURNING port (`"id, title"` literal) → `string[]`. */
function returningOf(ports: Record<string, unknown>): string[] | undefined {
  const r = stringPort(ports, 'returning');
  if (r === undefined) return undefined;
  return r.split(',').map((s) => s.trim()).filter((s) => s.length > 0);
}

// ── Node → CompiledOperation ──────────────────────────────────────────────────

/**
 * Backend-Compile ONE catalog `componentRef`/`map` body node into a WS1
 * {@link CompiledOperation}. This is the real IR → WS1 compile-input bridge for the
 * SQL CRUD primitives.
 */
export function compileNode(node: RefLike): CompiledOperation {
  const { component, ports } = nodeRef(node);
  const table = stringPort(ports, 'table');
  if (table === undefined) throw new Error(`bridge: ${component} node requires a literal 'table' port`);
  const at = `${component}`;

  switch (component) {
    case 'Select': {
      const select = stringArrayPort(ports, 'select');
      if (select === undefined) throw new Error(`bridge: Select requires a 'select' {arr:[...]} port`);
      const desc: SelectDesc = { table, select };
      const where = lowerWherePort(ports, at);
      if (where !== undefined) desc.where = where;
      const order = stringPort(ports, 'order');
      if (order !== undefined) desc.order = order;
      const group = stringPort(ports, 'group');
      if (group !== undefined) desc.group = group;
      if (ports.limit !== undefined) desc.limit = ports.limit as ExprNode;
      if (ports.offset !== undefined) desc.offset = ports.offset as ExprNode;
      return compileSelect(desc);
    }
    case 'Insert': {
      const values = collectFamily(ports, 'values');
      if (Object.keys(values).length === 0) throw new Error(`bridge: Insert requires at least one 'values.<field>' port`);
      const desc: InsertDesc = { table, values, ...conflictOf(ports) };
      const returning = returningOf(ports);
      if (returning !== undefined) desc.returning = returning;
      return compileInsert(desc);
    }
    case 'Update': {
      const set = collectFamily(ports, 'set');
      if (Object.keys(set).length === 0) throw new Error(`bridge: Update requires at least one 'set.<field>' port`);
      const where = lowerWherePort(ports, at);
      if (where === undefined) throw new Error(`bridge: Update requires a 'where' port`);
      const desc: UpdateDesc = { table, set, where };
      const returning = returningOf(ports);
      if (returning !== undefined) desc.returning = returning;
      return compileUpdate(desc);
    }
    case 'Delete': {
      const where = lowerWherePort(ports, at);
      if (where === undefined) throw new Error(`bridge: Delete requires a 'where' port`);
      const desc: DeleteDesc = { table, where };
      const returning = returningOf(ports);
      if (returning !== undefined) desc.returning = returning;
      return compileDelete(desc);
    }
    default:
      throw new Error(`bridge: catalog component '${component}' has no Backend-Compile (SQL CRUD only: Select/Insert/Update/Delete)`);
  }
}

/**
 * Backend-Compile every catalog node of a component into a `nodeId → CompiledOperation`
 * map. `cond` nodes carry no catalog reference and are skipped (they are pure Expression,
 * evaluated by bc's `runBehavior`). The thin runtime looks up a node's `CompiledOperation`
 * by `ctx.nodeId` when its handler fires.
 */
export function compileComponentNodes(component: Component): Map<string, CompiledOperation> {
  const out = new Map<string, CompiledOperation>();
  for (const n of component.body) {
    if ('cond' in n) continue;
    out.set(n.id, compileNode(n));
  }
  return out;
}
