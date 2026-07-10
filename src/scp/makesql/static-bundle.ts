/**
 * litedbmodel v2 SCP — the STATIC, PORTABLE `makeSQL` bundle + its runtime (epic #43/#45
 * Phase B; design #45 owner-confirmed static-bundle decision).
 *
 * This is the SOLE read/compile path of the makeSQL SCP model, expressed as a compile step
 * that is **symbolic** (no concrete input needed) and a runtime that evaluates value-specs +
 * skip PER-INPUT via behavior-contracts. It REPLACES the reduced-spine `../runtime.ts`
 * `SqlBundle`/`compileBundle`/`executeBundle` for the read path.
 *
 * ## The static bundle shape (design #45)
 *
 * A compiled behavior method is a bundle of ordered {@link StaticStatement} — each a
 * `makeSQL` TEMPLATE:
 *   - `sql`    — tuned dialect SQL text (`?` placeholders), COMPLETE and value-independent
 *                (compiled ONCE; the single-JSON-param array forms + PG `= ANY`/UNNEST keep
 *                the text fixed regardless of the runtime array length — no per-input text).
 *   - `params` — value-specs = bc Expression IR (`{ref:[…]}` / small ops / literals), 1:1 with
 *                the top-level `?`, evaluated AT RUNTIME by bc `evaluateExpression`.
 *   - `skip?`  — a bc presence expression, evaluated at runtime; when truthy the whole
 *                statement (its `sql` and `params`) drops (contributes NOTHING).
 *
 * So the compile step emits a reusable portable artifact; the runtime (this module's
 * {@link executeStaticBundle}) evaluates skip + params per-input via bc, assembles present
 * fragments, renders placeholders, and executes. This unifies read/write/codegen on ONE
 * `SqlBundle` shape and enables multi-language IR-reference execution.
 *
 * ## Symbolic compile — reuses `authoring-compile`'s port decoding, defers evaluation
 *
 * Phase A's `authoring-compile.ts` compiled per-input EAGERLY (it called `evaluateExpression`
 * on every value slot to produce concrete `{sql, params}`). Here we keep the SAME closed-set
 * port decoding (`../bridge.ts` operators, IN-sentinel, SKIP-`cond` collapse) but DEFER the
 * value/skip evaluation to the runtime: a where-member's value operand stays as its raw bc
 * Expression IR, a SKIP-`cond` guard becomes the statement's `skip` expression, and the SQL
 * text is emitted once.
 */

import {
  evaluateExpression,
  runBehavior,
  runBehaviorAsync,
  type Scope,
  type Value,
  type Component as BcComponent,
  type ComponentGraphIR,
  type Handlers,
  type AsyncHandlers,
  type HandlerCtx,
  type ExecOutcome,
} from 'behavior-contracts';
import type { Component, ComponentRefNode, MapNode, BehaviorModelContract } from '../authoring';
import { IN_SENTINEL } from './tx';
import { composeMakeSQL, type MakeSQL, type SqlParam } from './makesql';
import { renderPlaceholders, type Dialect } from './handler';
import { compileWriteNode } from './tx';
import { mapSqliteError } from '../errors';
import { type ConditionObject } from '../../DBConditions';
import { conditionsFor } from './json-array';
import { compileSelect } from './compile-select';

// ── Expression IR alias (a value-spec / skip expression is a closed-set bc node) ──

/** A closed-set bc Expression IR node used as a deferred value-spec or a skip expression. */
export type ValueSpec = unknown;

/**
 * One statement of a static bundle — a `makeSQL` TEMPLATE. `sql` is the complete tuned SQL
 * text (`?` placeholders, value-independent); `params` are deferred value-specs (bc
 * Expression IR) evaluated at runtime; `skip`, when present, is a bc presence expression
 * evaluated at runtime (truthy ⇒ drop the whole statement).
 */
export interface StaticStatement {
  /** Complete tuned SQL text (`?` placeholders). */
  readonly sql: string;
  /** Deferred value-specs — closed-set bc Expression IR, 1:1 with the top-level `?`. */
  readonly params: readonly ValueSpec[];
  /** Optional bc presence expression; truthy ⇒ the whole statement drops. */
  readonly skip?: ValueSpec;
  /**
   * A WHERE-clause fragment: its `sql` is a bare predicate body (no leading connector). The
   * runtime prepends ` WHERE ` to the FIRST present fragment and ` AND ` to each later one, so
   * a skipped fragment never leaves a dangling connector. A statement WITHOUT this flag emits
   * its `sql` verbatim.
   */
  readonly whereFragment?: true;
}

/**
 * The static, portable compiled artifact of ONE authored behavior method (the read path).
 * Pure JSON — every statement's `sql` is fixed text and its `params`/`skip` are bc Expression
 * IR, so `JSON.stringify` round-trips losslessly and a per-language runtime can execute it
 * with bc + a SQL driver alone.
 */
export interface StaticBundle {
  readonly dialect: Dialect;
  /** The behavior (component) name. */
  readonly name: string;
  /** Ordered `makeSQL` statement templates composed into the read query. */
  readonly statements: readonly StaticStatement[];
  /** Input heads normalized to present-as-null (absent-key SKIP; SSoT-driven). */
  readonly optionalHeads: readonly string[];
}

// ── Structural port readers (the SAME closed-set decoding the bridge performs) ──

type RefLike = ComponentRefNode | MapNode;

function isMap(n: Component['body'][number]): n is MapNode {
  return 'map' in n;
}

function nodeRef(n: RefLike): { component: string; ports: Record<string, unknown> } {
  return isMap(n) ? { component: n.map.component, ports: n.map.ports } : { component: n.component, ports: n.ports };
}

function stringPort(ports: Record<string, unknown>, name: string): string | undefined {
  const v = ports[name];
  if (v === undefined) return undefined;
  if (typeof v !== 'string') throw new Error(`static-bundle: port '${name}' must be a literal string in the IR (got ${JSON.stringify(v)})`);
  return v;
}

function arrPort(ports: Record<string, unknown>, name: string): unknown[] | undefined {
  const v = ports[name];
  if (v === undefined) return undefined;
  if (typeof v === 'object' && v !== null && 'arr' in v && Array.isArray((v as { arr: unknown }).arr)) {
    return (v as { arr: unknown[] }).arr;
  }
  throw new Error(`static-bundle: port '${name}' must be an {arr:[...]} literal in the IR (got ${JSON.stringify(v)})`);
}

function stringArrayPort(ports: Record<string, unknown>, name: string): string[] | undefined {
  const arr = arrPort(ports, name);
  if (arr === undefined) return undefined;
  return arr.map((e) => {
    if (typeof e !== 'string') throw new Error(`static-bundle: '${name}' entries must be literal strings (got ${JSON.stringify(e)})`);
    return e;
  });
}

function opKey(node: unknown): string | undefined {
  if (node === null || typeof node !== 'object' || Array.isArray(node)) return undefined;
  const keys = Object.keys(node as object);
  return keys.length === 1 ? keys[0] : undefined;
}

function columnOf(node: unknown, ctx: string): string {
  const op = opKey(node);
  if (op !== 'ref' && op !== 'refOpt') throw new Error(`static-bundle: ${ctx}: the column operand must be a {ref:[…]} / {refOpt:[…]} path`);
  const path = (node as Record<string, unknown[]>)[op];
  if (!Array.isArray(path) || path.length === 0 || typeof path[path.length - 1] !== 'string') {
    throw new Error(`static-bundle: ${ctx}: column ref path must be a non-empty string path`);
  }
  return path[path.length - 1] as string;
}

function inSentinelColumn(node: unknown): string | undefined {
  const op = opKey(node);
  if (op !== 'ref' && op !== 'refOpt') return undefined;
  const path = (node as Record<string, unknown[]>)[op];
  if (Array.isArray(path) && path.length >= 2 && path[0] === IN_SENTINEL && typeof path[path.length - 1] === 'string') {
    return path[path.length - 1] as string;
  }
  return undefined;
}

function binOperands(node: unknown, op: string, at: string): [unknown, unknown] {
  const args = (node as Record<string, unknown[]>)[op];
  if (!Array.isArray(args) || args.length !== 2) throw new Error(`static-bundle: ${at}: '${op}' expects exactly 2 operands`);
  return [args[0], args[1]];
}

/** The comparison-operator SQL symbols, keyed by the bc operator name (drives the v1 custom-op key). */
const CMP_OPS: Record<string, string> = { lt: '<', le: '<=', gt: '>', ge: '>=', ne: '<>' };

// ── WHERE member → static SQL fragment + deferred value-specs ──────────────────
//
// The fragment TEXT is ALWAYS produced by driving the ORIGINAL v1 builder
// (`DBConditions.compile()` via `conditionsFor` — the SAME path `compile.ts` /
// `compile-select.ts` / `compile-relation.ts` use), so it is byte-identical to v1 by
// construction. A hand-rolled string template would make the corpus v2-to-v2 (tautological)
// and would NOT catch a v1-original regression — hence NONE remain here. Only the DEFERRED
// value-specs (bc Expression IR evaluated per-input at runtime) are computed structurally;
// the placeholder COUNT in the v1-produced text is kept 1:1 with the value-spec list.

/** A lowered WHERE fragment: complete text (`?`) + its deferred value-specs, in `?` order. */
interface WhereFragment {
  readonly sql: string;
  readonly params: readonly ValueSpec[];
}

/** A probe placeholder value fed to the v1 builder so it emits one `?` per bound slot. */
const PROBE = '__probe__';

/**
 * Produce a bare condition body's TEXT by driving the ORIGINAL v1 builder
 * `DBConditions.compile()` (through `conditionsFor`, so MySQL/SQLite IN-lists take the
 * single-JSON server-side form and PG stays base-class). The `conditions` object is built to
 * be byte-identical to what the v1 eager path would pass for the same construct; the throwaway
 * `probe` params array absorbs the probe values (the real runtime values are the caller's
 * deferred value-specs). Returns the v1 text — no v2 hand-roll.
 */
function v1ConditionText(conditions: ConditionObject, dialect: Dialect): string {
  const probe: unknown[] = [];
  return conditionsFor(conditions, dialect).compile(probe);
}

/** A deferred value-spec that JSON-encodes an array value-spec at runtime (single-param IN). */
function jsonArraySpec(dialect: Dialect, valueSpec: ValueSpec): ValueSpec {
  // Marker op the runtime interprets: `{ __jsonArray: <valueSpec>, dialect }`. Postgres keeps
  // the array as-is (bound as a t[] param via node-postgres); mysql/sqlite JSON-encode it.
  return { __jsonArray: valueSpec, dialect };
}

/**
 * The IN-list membership fragment, driven by the ORIGINAL builders (never hand-rolled):
 *   - MySQL/SQLite: `conditionsFor({col:[…]})` → the single-JSON `JsonArrayConditions` form
 *     (`JSON_TABLE`/`json_each`) — the SAME text the eager path emits; ONE `?`, one JSON param.
 *   - PostgreSQL: the single-array-param form `col = ANY(?)` with NO element-type cast. This
 *     authored `whereIn` surface carries NO column type (spec §7 `inColumn`), so there is nothing
 *     to cast the array TO at compile time — and value-inference (v1's `inferPgArrayType`) cannot
 *     recover it either: `[]` is indistinguishable from any element type, and a uuid value is
 *     indistinguishable-from-text by value. Emitting a cast therefore RE-BROKE #46 twice on live
 *     PG (empty int → `integer = text`; uuid → `uuid = text`). Instead we cast NOTHING and let
 *     PostgreSQL infer the array element type from the column context (`id` int → `int[]`, a uuid
 *     column → `uuid[]`, empty → the column's type → zero rows, no error) — proven on live PG16
 *     for int/bigint/uuid/bool/numeric/text and all three empty cases. This is v1-RESULT-parity
 *     (same rows, incl. empty → zero rows via `1 = 0` in v1): v1's authored surface had no
 *     `= ANY` form (it expanded `IN(?,?,…)`), so byte-identity to a v1 `= ANY(?::T[])` is not the
 *     requirement here — only that the rows match. The array binds verbatim as ONE param.
 */
function inListFragment(dialect: Dialect, column: string, valueSpec: ValueSpec): WhereFragment {
  if (dialect === 'postgres') {
    // `col = ANY(?)` — no cast. PG infers the array element type from the column, which is correct
    // for every type INCLUDING empty and uuid (a value-inferred cast cannot, and re-broke #46).
    const conditions: ConditionObject = { __raw__: [`${column} = ANY(?)`, [PROBE]] };
    return { sql: v1ConditionText(conditions, dialect), params: [jsonArraySpec(dialect, valueSpec)] };
  }
  // MySQL/SQLite: the single-JSON IN-list is the JsonArrayConditions form (v1/epic builder).
  const conditions: ConditionObject = { [column]: [PROBE] };
  return { sql: v1ConditionText(conditions, dialect), params: [jsonArraySpec(dialect, valueSpec)] };
}

/**
 * Lower ONE where-member Expression node to a static fragment. The TEXT of every leaf is
 * emitted by the ORIGINAL `DBConditions`; AND/OR groups join the v1-produced leaf texts with
 * the EXACT connector `DBConditions.compile` uses (`parts.join(' AND '|' OR ')` + wrapping
 * parens) — pure text glue, the original's own algorithm (mirrors `compile-pg.compileBaseSelect`).
 */
function lowerWhereMember(node: unknown, dialect: Dialect, at: string): WhereFragment {
  const op = opKey(node);
  if (op === undefined) throw new Error(`static-bundle: ${at}: a where member must be a single-operator Expression node`);

  if (op === 'and' || op === 'or') {
    const args = (node as Record<string, unknown[]>)[op];
    if (!Array.isArray(args) || args.length < 2) throw new Error(`static-bundle: ${at}: '${op}' group expects >= 2 members`);
    const parts: string[] = [];
    const params: ValueSpec[] = [];
    args.forEach((m, i) => {
      const f = lowerWhereMember(m, dialect, `${at}.${op}[${i}]`);
      parts.push(f.sql);
      params.push(...f.params);
    });
    const connector = op === 'and' ? ' AND ' : ' OR ';
    return { sql: `(${parts.join(connector)})`, params };
  }

  if (op === 'eq') {
    const [col, val] = binOperands(node, op, at);
    const inCol = inSentinelColumn(col);
    if (inCol !== undefined) return inListFragment(dialect, inCol, val);
    const column = columnOf(col, at);
    if (val === null) return { sql: v1ConditionText({ [column]: null }, dialect), params: [] };
    // v1 default equality: `{col: value}` → `col = ?`.
    return { sql: v1ConditionText({ [column]: PROBE }, dialect), params: [val] };
  }

  if (op in CMP_OPS) {
    const [col, val] = binOperands(node, op, at);
    const column = columnOf(col, at);
    if (op === 'ne' && val === null) {
      // v1 `IS NOT NULL`: a boolean-true key that is a SQL expression returns the key verbatim.
      return { sql: v1ConditionText({ [`${column} IS NOT NULL`]: true }, dialect), params: [] };
    }
    // v1 comparison: a custom-operator key (`col <op> ?`) returns the key + pushes the value.
    return { sql: v1ConditionText({ [`${column} ${CMP_OPS[op]} ?`]: PROBE }, dialect), params: [val] };
  }

  throw new Error(`static-bundle: ${at}: unsupported where operator '${op}' (supported: eq/ne/lt/le/gt/ge/and/or; IN via ${IN_SENTINEL}; IS NULL via eq(col,null))`);
}

/**
 * Lower the `where` port to a list of `{ sql, params, skip? }` fragment statements. A
 * `{cond:[c, member, null]}` SKIP-optional member lowers its inner member and attaches `c`
 * as the fragment's `skip` presence expression (dropped at runtime when `c` is null/false).
 * The FIRST present fragment carries no leading connector; every later one carries ` AND `.
 */
function lowerWherePort(ports: Record<string, unknown>, dialect: Dialect, at: string): StaticStatement[] {
  const arr = arrPort(ports, 'where');
  if (arr === undefined || arr.length === 0) return [];
  // Each member becomes a fragment statement. The leading ` WHERE `/` AND ` connectors are
  // resolved at runtime from the PRESENT set (a compile-time connector would be wrong when an
  // earlier fragment skips). We therefore mark each fragment and let the assembler join them.
  const fragments: { sql: string; params: readonly ValueSpec[]; skip?: ValueSpec }[] = [];
  arr.forEach((m, i) => {
    const memberAt = `${at}.where[${i}]`;
    if (opKey(m) === 'cond') {
      const cargs = (m as Record<string, unknown[]>).cond;
      if (!Array.isArray(cargs) || cargs.length !== 3 || cargs[2] !== null) {
        throw new Error(`static-bundle: ${memberAt}: a SKIP-optional condition must be {cond:[c, <member>, null]} (else = null)`);
      }
      const inner = lowerWhereMember(cargs[1], dialect, `${memberAt}.cond.then`);
      fragments.push({ sql: inner.sql, params: inner.params, skip: skipFrom(cargs[0]) });
      return;
    }
    const f = lowerWhereMember(m, dialect, memberAt);
    fragments.push({ sql: f.sql, params: f.params });
  });
  return fragments.map((f) => ({ sql: f.sql, params: f.params, ...(f.skip !== undefined ? { skip: f.skip } : {}) }));
}

/**
 * Turn a SKIP-`cond` guard condition `c` (present iff `c` is a present binding) into a bc
 * `skip` expression: the statement is SKIPPED when the guard is ABSENT, i.e. `skip = not(c)`.
 * `c` is a closed-set bc Expression (e.g. `{ne:[{refOpt:['status']}, null]}`).
 */
function skipFrom(guard: ValueSpec): ValueSpec {
  return { not: [guard] };
}

// ── Node → static statements (SELECT read path) ────────────────────────────────

/**
 * The ORIGINAL `_buildSelectSQL` GROUP BY / ORDER BY append text for a Select tail fragment.
 * Drives `compileSelect` (a bare `SELECT * FROM <t>` head + the one tail field) and slices off
 * the shared head, so the remaining ` GROUP BY …` / ` ORDER BY …` is v1's exact append — never
 * a v2 hand-roll. The head is always `SELECT * FROM <table>` here (no `select`/conditions given).
 */
function selectTail(desc: Parameters<typeof compileSelect>[0], table: string): string {
  const full = compileSelect(desc).sql;
  const head = `SELECT * FROM ${table}`;
  return full.startsWith(head) ? full.slice(head.length) : full;
}

/**
 * Compile ONE authored `Select` node into its static `makeSQL` statement templates:
 * the head `SELECT … FROM t` fragment plus one guarded WHERE fragment per member, then the
 * trailing GROUP BY / ORDER BY / LIMIT / OFFSET tail. The head + GROUP BY + ORDER BY TEXT is
 * produced by the ORIGINAL `compileSelect` (which reproduces `_buildSelectSQL` byte-for-byte),
 * never a v2 hand-roll. LIMIT/OFFSET are INLINE literals in v1; for the static bundle they are
 * deferred value-specs bound as `?` (a portable equivalent a per-language runtime renders
 * identically) — evaluated at runtime.
 */
export function compileSelectNode(node: RefLike, dialect: Dialect): StaticStatement[] {
  const { component, ports } = nodeRef(node);
  if (component !== 'Select') {
    throw new Error(`static-bundle: compileSelectNode only compiles Select nodes (got '${component}')`);
  }
  const table = stringPort(ports, 'table');
  if (table === undefined) throw new Error(`static-bundle: Select node requires a literal 'table' port`);
  const select = stringArrayPort(ports, 'select');
  const cols = select && select.length > 0 ? select.join(', ') : '*';

  const statements: StaticStatement[] = [];
  // Head `SELECT <cols> FROM <t>` — driven by the ORIGINAL `_buildSelectSQL` (via compileSelect,
  // no WHERE/tail) so the skeleton text is byte-identical to v1.
  statements.push({ sql: compileSelect({ dialect, tableName: table, select: cols }).sql, params: [] });

  const whereFrags = lowerWherePort(ports, dialect, 'Select');
  for (const frag of whereFrags) {
    // A bare predicate body flagged `whereFragment`; the runtime resolves the ` WHERE `/` AND `
    // connector from the present set (a compile-time connector would be wrong when an earlier
    // fragment skips).
    statements.push({ sql: frag.sql, params: frag.params, whereFragment: true, ...(frag.skip !== undefined ? { skip: frag.skip } : {}) });
  }

  // GROUP BY / ORDER BY tail — the ORIGINAL `_buildSelectSQL` append text (via compileSelect over
  // a bare table, then slicing off the leading `SELECT * FROM <t>` head it shares).
  const group = stringPort(ports, 'group');
  if (group !== undefined) statements.push({ sql: selectTail({ dialect, tableName: table, group }, table), params: [] });
  const order = stringPort(ports, 'order');
  if (order !== undefined) statements.push({ sql: selectTail({ dialect, tableName: table, order }, table), params: [] });
  if (ports.limit !== undefined) statements.push({ sql: ` LIMIT ?`, params: [ports.limit as ValueSpec] });
  if (ports.offset !== undefined) statements.push({ sql: ` OFFSET ?`, params: [ports.offset as ValueSpec] });

  return statements;
}

// ── Compile a behavior method → static bundle (CRUD primary node) ──────────────

/**
 * Compile ONE authored primary catalog node into its static `makeSQL` statement templates. A
 * `Select` lowers to the fragment/tail statements ({@link compileSelectNode}); a single
 * `Insert`/`Update`/`Delete` lowers via the SAME symbolic write compile the write-tx spine uses
 * ({@link compileWriteNode} in `./tx` — complete tuned SQL text + deferred Expression-IR params),
 * yielding ONE statement. This is the makeSQL SCP path for a standalone CRUD query (a Command
 * with write-time relations rides the tx-DAG plan, not this single-statement path).
 */
function compilePrimaryNode(node: ComponentRefNode, dialect: Dialect): StaticStatement[] {
  const component = 'map' in node ? (node as MapNode).map.component : node.component;
  if (component === 'Select') return compileSelectNode(node, dialect);
  if (component === 'Insert' || component === 'Update' || component === 'Delete') {
    const op = compileWriteNode(node as { component: 'Insert' | 'Update' | 'Delete'; ports: Record<string, unknown> });
    return [{ sql: op.sql, params: op.params }];
  }
  throw new Error(`static-bundle: catalog component '${component}' has no makeSQL compile (SQL CRUD only: Select/Insert/Update/Delete)`);
}

/**
 * Compile the primary CRUD query of an authored behavior method into a {@link StaticBundle}.
 * Relation `.map` nodes are NOT part of the primary bundle (they are the read-relations concern
 * — step 3, still on the reduced spine). The compile is SYMBOLIC (no concrete input); the runtime
 * ({@link executeStaticBundle} for reads / {@link executeStaticWrite} for writes) evaluates skip +
 * value-specs per-input.
 */
export function compileStaticBundle(
  contract: BehaviorModelContract,
  dialect: Dialect,
  entry?: string,
): StaticBundle {
  const component = entry ? contract.components.find((c) => c.name === entry) : contract.components[0];
  if (component === undefined) throw new Error(`static-bundle: entry component '${entry ?? '<first>'}' not found in contract`);

  let primary: ComponentRefNode | undefined;
  for (const n of component.body) {
    if ('cond' in n) continue;
    if (isMap(n)) continue;
    if (primary === undefined) primary = n;
  }
  if (primary === undefined) throw new Error(`static-bundle: behavior '${component.name}' has no primary catalog node`);

  const statements = compilePrimaryNode(primary, dialect);
  return {
    dialect,
    name: component.name,
    statements,
    optionalHeads: [...optionalHeadsOf(component, statements)],
  };
}

// ── Optional-head detection (SSoT-driven, mirrors ../runtime.optionalHeadsOf) ──

/**
 * The OPTIONAL input heads to normalize to present-as-null (absent-key SKIP): the
 * schema-optional ports, the SKIP-`cond`-guarded heads (a guard declares its driving input
 * optional — spec §7), AND every head accessed via `refOpt` in a value-spec or skip expression
 * (the author reached for `opt(…)`/`refOpt` precisely because the binding may be absent — e.g. a
 * `coalesce`-defaulted LIMIT). bc's `evaluateExpression` fail-closes on an absent head even
 * under `refOpt`, so these must be pre-normalized to null. A REQUIRED, non-refOpt head that is
 * missing is left absent so a real wiring bug surfaces loudly. This is SSoT-driven (the
 * schema/guard/refOpt IS the optionality declaration), never an ad-hoc code default.
 */
function optionalHeadsOf(component: Component, statements: readonly StaticStatement[]): Set<string> {
  const optional = new Set<string>();
  for (const [port, schema] of Object.entries(component.inputPorts)) {
    if (schema.required !== true) optional.add(port);
  }
  for (const n of component.body) skipGuardHeads(n, optional);
  for (const stmt of statements) {
    for (const p of stmt.params) collectRefOptHeads(p, optional);
    if (stmt.skip !== undefined) collectRefOptHeads(stmt.skip, optional);
  }
  return optional;
}

/** Collect every path HEAD accessed via `refOpt` in an Expression IR node. */
function collectRefOptHeads(node: unknown, into: Set<string>): void {
  if (node === null || typeof node !== 'object') return;
  if (Array.isArray(node)) {
    for (const e of node) collectRefOptHeads(e, into);
    return;
  }
  const keys = Object.keys(node as object);
  if (keys.length === 1 && keys[0] === 'refOpt') {
    const path = (node as Record<string, unknown[]>).refOpt;
    if (Array.isArray(path) && path.length > 0 && typeof path[0] === 'string') into.add(path[0] as string);
    return;
  }
  for (const v of Object.values(node as Record<string, unknown>)) collectRefOptHeads(v, into);
}

function skipGuardHeads(n: Component['body'][number], into: Set<string>): void {
  if ('cond' in n) return;
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

// ── Runtime: evaluate value-specs + skip per-input, assemble, render, execute ──


/** bc evaluates ints to bigint; convert a rendered param to a driver-bindable value. */
function toDriverParam(v: Value): unknown {
  if (typeof v === 'bigint') {
    if (v >= BigInt(Number.MIN_SAFE_INTEGER) && v <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(v);
    return v;
  }
  return v;
}

/** Evaluate one deferred value-spec against the input scope (handling the JSON-array marker). */
function evalSpec(spec: ValueSpec, scope: Scope): unknown {
  if (spec !== null && typeof spec === 'object' && !Array.isArray(spec) && '__jsonArray' in (spec as object)) {
    const marker = spec as { __jsonArray: ValueSpec; dialect: Dialect };
    const arr = evaluateExpression(marker.__jsonArray, scope);
    if (!Array.isArray(arr)) throw new Error('static-bundle: IN-list value-spec did not evaluate to an array');
    if (marker.dialect === 'postgres') return arr.map((e) => toDriverParam(e as Value));
    return JSON.stringify((arr as Value[]).map((e) => toDriverParam(e)));
  }
  return toDriverParam(evaluateExpression(spec, scope));
}

/**
 * Evaluate a list of statement templates against a scope: drop skipped statements (skip
 * expression truthy), resolve each surviving statement's WHERE-role connector from the present
 * set, build concrete `makeSQL` nodes, assemble + render to the dialect placeholder form.
 */
function renderStatements(statements: readonly StaticStatement[], dialect: Dialect, scope: Scope): { sql: string; params: unknown[] } {
  const nodes: MakeSQL[] = [];
  let whereSeen = false;
  for (const stmt of statements) {
    if (stmt.skip !== undefined) {
      const drop = evaluateExpression(stmt.skip, scope);
      if (drop !== null && drop !== false) continue;
    }
    let sql = stmt.sql;
    if (stmt.whereFragment === true) {
      sql = (whereSeen ? ' AND ' : ' WHERE ') + stmt.sql;
      whereSeen = true;
    }
    const params: SqlParam[] = stmt.params.map((p) => evalSpec(p, scope) as SqlParam);
    // The authored PG IN-list emits `col = ANY(?)` with NO cast token (#46 — PG infers the array
    // element type from the column, correct for empty/uuid where value-inference cannot). No
    // render-time cast resolution is needed here; the relation-batch path resolves its own v1
    // `::T[]` cast in `runRelationOp`, not on this authored-statement surface.
    nodes.push({ sql, params });
  }
  const assembled = composeMakeSQL(nodes);
  return { sql: renderPlaceholders(assembled.sql, dialect), params: assembled.params };
}

/** Evaluate a whole bundle's statements against the input scope (convenience over the list). */
function renderBundle(bundle: StaticBundle, scope: Scope): { sql: string; params: unknown[] } {
  return renderStatements(bundle.statements, bundle.dialect, scope);
}

/** The minimal synchronous SQLite driver surface (better-sqlite3 `Database`). */
export interface SqliteDb {
  prepare(sql: string): {
    all(...params: unknown[]): unknown[];
    run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
  };
}

/** Normalize omitted OPTIONAL heads to present-as-null (absent-key SKIP; SSoT-driven). */
function normalizeInput(bundle: StaticBundle, input: Scope): Scope {
  const out: Scope = { ...input };
  for (const head of bundle.optionalHeads) {
    if (!(head in out)) out[head] = null;
  }
  return out;
}

/**
 * Execute a {@link StaticBundle} read query end-to-end against real SQLite: normalize optional
 * heads, evaluate skip + value-specs per-input, assemble present fragments, render, and run the
 * SELECT. Returns the row list.
 */
export function executeStaticBundle(bundle: StaticBundle, input: Scope, db: SqliteDb): Record<string, unknown>[] {
  const scope = normalizeInput(bundle, input);
  const { sql, params } = renderBundle(bundle, scope);
  let stmt: ReturnType<SqliteDb['prepare']>;
  try {
    stmt = db.prepare(sql);
    return stmt.all(...params) as Record<string, unknown>[];
  } catch (e) {
    throw mapSqliteError(e);
  }
}

/**
 * Execute a single-statement WRITE {@link StaticBundle} (Insert/Update/Delete) against real
 * SQLite: evaluate the deferred value-specs per-input, render, and run the statement. A statement
 * with a RETURNING tail returns its rows; a non-returning write returns a single-row summary
 * `[{ changes, lastInsertRowid }]`. A driver error maps to a {@link SqlFailure} at the boundary.
 * (A Command with write-time relations rides the tx-DAG plan of `./tx`, not this path.)
 */
export function executeStaticWrite(bundle: StaticBundle, input: Scope, db: SqliteDb): Record<string, unknown>[] {
  const scope = normalizeInput(bundle, input);
  const { sql, params } = renderBundle(bundle, scope);
  const hasReturn = /\breturning\b/i.test(sql);
  try {
    const stmt = db.prepare(sql);
    if (hasReturn) return stmt.all(...params) as Record<string, unknown>[];
    const info = stmt.run(...params);
    return [{ changes: info.changes, lastInsertRowid: toDriverParam(BigInt(info.lastInsertRowid)) }];
  } catch (e) {
    throw mapSqliteError(e);
  }
}

// ============================================================================
// Step 0 — the bc-runBehavior read executor: bc composes (map / Φ-merge / wiring),
// makeSQL executes. A read behavior method whose output is a Φ-merge of SQL nodes
// (`{obj:{posts:ref[n0], authors:map(n0)}}`) is executed by rewriting each SQL node
// to a `makeSQL` node carrying a synthetic `__scope` port and running bc `runBehavior`
// with a handler that renders that node's static statements against the evaluated scope.
// This gives map iteration + Φ output + wiring FOR FREE (bc owns orchestration).
// ============================================================================

/** The synthetic port that carries a SQL node's render scope (bc evaluates it in-scope). */
const SCOPE_PORT = '__scope';
/** The makeSQL catalog leaf name every rewritten SQL node references. */
const NODE_COMPONENT = '__makeSqlNode';

/** Collect every ref/refOpt path HEAD used inside an Expression IR node. */
function collectAllHeads(node: unknown, heads: Set<string>): void {
  if (node === null || typeof node !== 'object') return;
  if (Array.isArray(node)) {
    for (const el of node) collectAllHeads(el, heads);
    return;
  }
  const keys = Object.keys(node);
  if (keys.length === 1 && (keys[0] === 'ref' || keys[0] === 'refOpt')) {
    const path = (node as Record<string, unknown>)[keys[0]];
    if (Array.isArray(path) && typeof path[0] === 'string') heads.add(path[0] as string);
    return;
  }
  for (const k of keys) collectAllHeads((node as Record<string, unknown>)[k], heads);
}

/** Every binding head a node's compiled statements reference (params + skip). */
function statementHeads(statements: readonly StaticStatement[]): Set<string> {
  const heads = new Set<string>();
  for (const stmt of statements) {
    for (const p of stmt.params) collectAllHeads(p, heads);
    if (stmt.skip !== undefined) collectAllHeads(stmt.skip, heads);
  }
  return heads;
}

/**
 * Build the surrogate `__scope` port for a node: a bc `{obj:{…}}` re-exporting each referenced
 * head as `{ref:[head]}` (excluding the IN-sentinel column marker). bc evaluates this in ITS
 * scope (so `input`, sibling wire results and the `map` element `as` all resolve), then the
 * handler renders the node's static statements against the resulting plain scope.
 */
function scopePort(statements: readonly StaticStatement[]): unknown {
  const obj: Record<string, unknown> = {};
  for (const head of statementHeads(statements)) {
    if (head === IN_SENTINEL) continue;
    obj[head] = { ref: [head] };
  }
  return { obj };
}

/** Compile ONE authored SQL node (Select or CRUD write) into its static statements. */
function compileNodeStatements(node: RefLike, dialect: Dialect): StaticStatement[] {
  const component = 'map' in node ? (node as MapNode).map.component : node.component;
  if (component === 'Select') return compileSelectNode(node, dialect);
  const op = compileWriteNode(node as { component: 'Insert' | 'Update' | 'Delete'; ports: Record<string, unknown> });
  return [{ sql: op.sql, params: op.params }];
}

/**
 * The compiled, portable READ graph of a behavior method: the surrogate bc `ComponentGraphIR`
 * (each SQL node → a `makeSQL` node with one `__scope` port; wiring / map / Φ output preserved)
 * plus the per-node static statement templates and the optional heads. Pure JSON —
 * `JSON.stringify` round-trips, so a per-language runtime executes it with bc + a SQL driver.
 */
export interface ReadGraph {
  readonly dialect: Dialect;
  readonly name: string;
  /** The surrogate bc IR (catalog nodes → `makeSQL` `__scope`-port nodes). */
  readonly ir: ComponentGraphIR;
  /** Per-node static `makeSQL` statement templates, keyed by body node id. */
  readonly statementsById: Record<string, readonly StaticStatement[]>;
  /** Input heads normalized to present-as-null (absent-key SKIP; SSoT-driven). */
  readonly optionalHeads: readonly string[];
}

/**
 * Compile a READ behavior method into a portable {@link ReadGraph}: rewrite each authored SQL
 * body node to a `makeSQL` node carrying a synthetic `__scope` port (bc evaluates it in-scope),
 * and compile each node's static statements ONCE. SYMBOLIC — no concrete input. bc will own map
 * iteration / wire binding / Φ output at execute time; the handler renders each node's statements.
 */
export function compileReadGraph(
  contract: BehaviorModelContract,
  dialect: Dialect,
  entry?: string,
): ReadGraph {
  const component = entry ? contract.components.find((c) => c.name === entry) : contract.components[0];
  if (component === undefined) throw new Error(`static-bundle: entry component '${entry ?? '<first>'}' not found in contract`);

  const statementsById: Record<string, readonly StaticStatement[]> = {};
  for (const n of component.body) {
    if ('cond' in n) continue;
    statementsById[n.id] = compileNodeStatements(n as RefLike, dialect);
  }

  const surrogateBody = component.body.map((n) => {
    if ('cond' in n) return n;
    const stmts = statementsById[n.id];
    if ('map' in n) {
      return { ...n, map: { ...n.map, component: NODE_COMPONENT, ports: { [SCOPE_PORT]: scopePort(stmts) } } };
    }
    return { ...n, component: NODE_COMPONENT, ports: { [SCOPE_PORT]: scopePort(stmts) } };
  });
  const surrogate = { ...component, body: surrogateBody } as unknown as BcComponent;
  const ir: ComponentGraphIR = { irVersion: 1, exprVersion: 2, components: [surrogate] };

  return {
    dialect,
    name: component.name,
    ir,
    statementsById,
    optionalHeads: [...optionalHeadsOfComponent(component, dialect)],
  };
}

/**
 * Execute a compiled {@link ReadGraph} via bc `runBehavior` + a makeSQL handler: bc drives map
 * iteration / wire binding / Φ output; the handler renders each node's static statements against
 * the evaluated `__scope` and runs REAL SQLite. Returns the component's Φ output. This is the
 * design's "bc composes, makeSQL executes" — the SAME path a per-language runtime follows.
 */
export function executeReadGraph(graph: ReadGraph, input: Scope, db: SqliteDb): Value {
  const handle = (ports: Record<string, Value>, ctx: HandlerCtx): ExecOutcome => {
    const stmts = graph.statementsById[ctx.nodeId];
    if (stmts === undefined) return { error: `static-bundle: no statements for node '${ctx.nodeId}'` };
    const scope = ports[SCOPE_PORT];
    if (scope === null || typeof scope !== 'object' || Array.isArray(scope)) {
      return { error: `static-bundle: node '${ctx.nodeId}' surrogate scope did not evaluate to an object` };
    }
    const { sql, params } = renderStatements(stmts, graph.dialect, scope as Scope);
    try {
      const rows = db.prepare(sql).all(...params) as Record<string, unknown>[];
      return { ok: rows as unknown as Value };
    } catch (e) {
      return { error: mapSqliteError(e).message };
    }
  };
  const handlers: Handlers = { [NODE_COMPONENT]: handle };

  const normalized: Scope = { ...input };
  for (const head of graph.optionalHeads) if (!(head in normalized)) normalized[head] = null;

  try {
    return runBehavior(graph.ir, handlers, normalized, graph.name);
  } catch (e) {
    throw reErrorToSqlFailure(e);
  }
}

/** An async driver seam: run a rendered `{ sql, params }` and resolve to result rows (#40). */
export type SqlExecutorAsync = (sql: string, params: unknown[]) => Promise<Record<string, unknown>[]>;

/**
 * Async twin of {@link executeReadGraph} — the PG / MySQL execution model (#40).
 *
 * Byte-identical composition (SAME IR, SAME per-node SQL text + params, SAME Φ output), but bc's
 * `runBehaviorAsync` drives it: the makeSQL handler returns a Promise, and bc dispatches the
 * INDEPENDENT sibling nodes of a plan stage on `runPlanAsync`'s bounded-parallel path (bc#23),
 * bounded by the plan's `concurrency` (default 16). Against a pooled async driver (`pg` / `mysql2`,
 * each `exec` call resolving on its own pooled connection), that becomes REAL parallel read-relation
 * DB I/O — the plan's declared concurrency finally cashes out as concurrent dispatch. The result is
 * deterministic (bc commits stage outcomes in declaration order), so it equals the serial
 * `executeReadGraph` output exactly; only the wall-clock changes.
 *
 * The conformance bar stays on the SYNC in-proc better-sqlite3 path (`executeReadGraph`); this async
 * path is the live PG/MySQL execution model and is proven by the latency-injecting concurrency test.
 */
export async function executeReadGraphAsync(
  graph: ReadGraph,
  input: Scope,
  exec: SqlExecutorAsync,
): Promise<Value> {
  const handle = async (ports: Record<string, Value>, ctx: HandlerCtx): Promise<ExecOutcome> => {
    const stmts = graph.statementsById[ctx.nodeId];
    if (stmts === undefined) return { error: `static-bundle: no statements for node '${ctx.nodeId}'` };
    const scope = ports[SCOPE_PORT];
    if (scope === null || typeof scope !== 'object' || Array.isArray(scope)) {
      return { error: `static-bundle: node '${ctx.nodeId}' surrogate scope did not evaluate to an object` };
    }
    const { sql, params } = renderStatements(stmts, graph.dialect, scope as Scope);
    try {
      const rows = await exec(sql, params);
      return { ok: rows as unknown as Value };
    } catch (e) {
      return { error: mapSqliteError(e).message };
    }
  };
  const handlers: AsyncHandlers = { [NODE_COMPONENT]: handle };

  const normalized: Scope = { ...input };
  for (const head of graph.optionalHeads) if (!(head in normalized)) normalized[head] = null;

  try {
    return await runBehaviorAsync(graph.ir, handlers, normalized, graph.name);
  } catch (e) {
    throw reErrorToSqlFailure(e);
  }
}

/**
 * Convenience: compile + execute a READ behavior method whose output may be a Φ-merge of SQL
 * nodes (including relation `.map` nodes). Equivalent to `executeReadGraph(compileReadGraph(...))`.
 */
export function executeReadBehavior(
  contract: BehaviorModelContract,
  input: Scope,
  dialect: Dialect,
  db: SqliteDb,
  entry?: string,
): Value {
  return executeReadGraph(compileReadGraph(contract, dialect, entry), input, db);
}

/**
 * Render the PRIMARY read node's statements of a {@link ReadGraph} against an input scope to its
 * dialect SQL text + bound params (the render axis for conformance golden). The primary node is
 * the first non-map body node (the root SELECT the relations map over). Optional heads are
 * normalized to present-as-null first (absent-key SKIP), so an omitted optional head renders the
 * SAME text a runtime would produce.
 */
export function renderReadPrimary(graph: ReadGraph, input: Scope): { sql: string; params: unknown[] } {
  const ids = Object.keys(graph.statementsById);
  // The primary node is the first body node in the surrogate IR order (map nodes reference it).
  const bodyIds = graph.ir.components[0].body.map((n) => n.id).filter((id) => ids.includes(id));
  const primaryId = bodyIds[0];
  if (primaryId === undefined) throw new Error('static-bundle: read graph has no primary node to render');
  const scope: Scope = { ...input };
  for (const head of graph.optionalHeads) if (!(head in scope)) scope[head] = null;
  return renderStatements(graph.statementsById[primaryId], graph.dialect, scope);
}

/** Optional heads across ALL SQL nodes of a component (schema + SKIP-guard + refOpt). */
function optionalHeadsOfComponent(component: Component, dialect: Dialect): Set<string> {
  const optional = new Set<string>();
  for (const [port, schema] of Object.entries(component.inputPorts)) {
    if (schema.required !== true) optional.add(port);
  }
  for (const n of component.body) skipGuardHeads(n, optional);
  for (const n of component.body) {
    if ('cond' in n) continue;
    const stmts = compileNodeStatements(n as RefLike, dialect);
    for (const stmt of stmts) {
      for (const p of stmt.params) collectRefOptHeads(p, optional);
      if (stmt.skip !== undefined) collectRefOptHeads(stmt.skip, optional);
    }
  }
  return optional;
}

/** Re-surface a mapped SqlFailure from a runBehavior OP_FAILED message (SQLITE_* code embedded). */
function reErrorToSqlFailure(e: unknown): unknown {
  const message = e instanceof Error ? e.message : String(e);
  const m = /(SQLITE_[A-Z_]+)/.exec(message);
  if (m) return mapSqliteError({ code: m[1], message });
  return e;
}
