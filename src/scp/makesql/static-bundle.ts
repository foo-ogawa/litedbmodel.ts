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

import { evaluateExpression, type Scope, type Value } from 'behavior-contracts';
import type { Component, ComponentRefNode, MapNode, BehaviorModelContract } from '../authoring';
import { IN_SENTINEL } from '../bridge';
import { composeMakeSQL, type MakeSQL, type SqlParam } from './makesql';
import { renderPlaceholders, type Dialect } from './handler';
import { mapSqliteError } from '../errors';

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

const CMP_OPS: Record<string, string> = { lt: '<', le: '<=', gt: '>', ge: '>=', ne: '<>' };

// ── WHERE member → static SQL fragment + deferred value-specs ──────────────────

/** A lowered WHERE fragment: complete text (`?`) + its deferred value-specs, in `?` order. */
interface WhereFragment {
  readonly sql: string;
  readonly params: readonly ValueSpec[];
}

/**
 * The dialect single-JSON-param IN-list text (server-side expansion; epic #43/#45). The array
 * binds as ONE JSON param regardless of length (static text). Postgres keeps `= ANY(?::text[])`
 * — but for a STATIC bundle whose element type is unknown at compile time we anchor on the
 * value-driven builders at render for PG; the static read path here targets sqlite/mysql
 * single-JSON forms and PG `= ANY` with the JSONB text array. The concrete PG type-inference
 * parity leg remains the value-driven `compile-relation` path (relations), not this member.
 */
function inListText(dialect: Dialect, column: string): string {
  if (dialect === 'mysql') {
    return `${column} IN (SELECT JSON_UNQUOTE(v) FROM JSON_TABLE(?, '$[*]' COLUMNS(v JSON PATH '$')) jt)`;
  }
  if (dialect === 'sqlite') {
    return `${column} IN (SELECT value FROM json_each(?))`;
  }
  // postgres: array as one text[] param, server-side membership.
  return `${column} = ANY(?)`;
}

/** A deferred value-spec that JSON-encodes an array value-spec at runtime (single-param IN). */
function jsonArraySpec(dialect: Dialect, valueSpec: ValueSpec): ValueSpec {
  // Marker op the runtime interprets: `{ __jsonArray: <valueSpec>, dialect }`. Postgres keeps
  // the array as-is (bound as a text[] param via node-postgres); mysql/sqlite JSON-encode it.
  return { __jsonArray: valueSpec, dialect };
}

/** Lower ONE where-member Expression node to a static fragment (deferred value-specs). */
function lowerWhereMember(node: unknown, dialect: Dialect, at: string): WhereFragment {
  const op = opKey(node);
  if (op === undefined) throw new Error(`static-bundle: ${at}: a where member must be a single-operator Expression node`);

  if (op === 'and') {
    const args = (node as Record<string, unknown[]>).and;
    if (!Array.isArray(args) || args.length < 2) throw new Error(`static-bundle: ${at}: 'and' group expects >= 2 members`);
    const parts: string[] = [];
    const params: ValueSpec[] = [];
    args.forEach((m, i) => {
      const f = lowerWhereMember(m, dialect, `${at}.and[${i}]`);
      parts.push(f.sql);
      params.push(...f.params);
    });
    return { sql: `(${parts.join(' AND ')})`, params };
  }

  if (op === 'or') {
    const args = (node as Record<string, unknown[]>).or;
    if (!Array.isArray(args) || args.length < 2) throw new Error(`static-bundle: ${at}: 'or' group expects >= 2 members`);
    const parts: string[] = [];
    const params: ValueSpec[] = [];
    args.forEach((m, i) => {
      const f = lowerWhereMember(m, dialect, `${at}.or[${i}]`);
      parts.push(f.sql);
      params.push(...f.params);
    });
    return { sql: `(${parts.join(' OR ')})`, params };
  }

  if (op === 'eq') {
    const [col, val] = binOperands(node, op, at);
    const inCol = inSentinelColumn(col);
    if (inCol !== undefined) {
      // IN-list membership: single-JSON-param (or PG text[]) — one `?`, static text.
      return { sql: inListText(dialect, inCol), params: [jsonArraySpec(dialect, val)] };
    }
    if (val === null) return { sql: `${columnOf(col, at)} IS NULL`, params: [] };
    return { sql: `${columnOf(col, at)} = ?`, params: [val] };
  }

  if (op in CMP_OPS) {
    const [col, val] = binOperands(node, op, at);
    if (op === 'ne' && val === null) return { sql: `${columnOf(col, at)} IS NOT NULL`, params: [] };
    return { sql: `${columnOf(col, at)} ${CMP_OPS[op]} ?`, params: [val] };
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
 * Compile ONE authored `Select` node into its static `makeSQL` statement templates:
 * the head `SELECT … FROM t` fragment plus one guarded WHERE fragment per member, then the
 * trailing GROUP BY / ORDER BY / LIMIT / OFFSET tail. LIMIT/OFFSET are INLINE literals in v1;
 * for the static bundle they are deferred value-specs bound as `?` (a portable equivalent that
 * a per-language runtime renders identically) — evaluated at runtime.
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
  statements.push({ sql: `SELECT ${cols} FROM ${table}`, params: [] });

  const whereFrags = lowerWherePort(ports, dialect, 'Select');
  for (const frag of whereFrags) {
    // A bare predicate body flagged `whereFragment`; the runtime resolves the ` WHERE `/` AND `
    // connector from the present set (a compile-time connector would be wrong when an earlier
    // fragment skips).
    statements.push({ sql: frag.sql, params: frag.params, whereFragment: true, ...(frag.skip !== undefined ? { skip: frag.skip } : {}) });
  }

  const group = stringPort(ports, 'group');
  if (group !== undefined) statements.push({ sql: ` GROUP BY ${group}`, params: [] });
  const order = stringPort(ports, 'order');
  if (order !== undefined) statements.push({ sql: ` ORDER BY ${order}`, params: [] });
  if (ports.limit !== undefined) statements.push({ sql: ` LIMIT ?`, params: [ports.limit as ValueSpec] });
  if (ports.offset !== undefined) statements.push({ sql: ` OFFSET ?`, params: [ports.offset as ValueSpec] });

  return statements;
}

// ── Compile a behavior method → static bundle (the SELECT primary of a read method) ──

/**
 * Compile the primary read `Select` of an authored behavior method into a {@link StaticBundle}.
 * Relation `.map` nodes are NOT part of the primary bundle (they are the read-relations concern
 * — step 3); a read method whose primary is not a Select is rejected loudly.
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

  const statements = compileSelectNode(primary, dialect);
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
 * Evaluate a bundle's statements against an input scope: drop skipped statements (skip
 * expression truthy), resolve each surviving statement's WHERE-role connector from the present
 * set, build concrete `makeSQL` nodes, assemble + render to the dialect placeholder form.
 */
function renderBundle(bundle: StaticBundle, scope: Scope): { sql: string; params: unknown[] } {
  const nodes: MakeSQL[] = [];
  let whereSeen = false;
  for (const stmt of bundle.statements) {
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
    nodes.push({ sql, params });
  }
  const assembled = composeMakeSQL(nodes);
  return { sql: renderPlaceholders(assembled.sql, bundle.dialect), params: assembled.params };
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
