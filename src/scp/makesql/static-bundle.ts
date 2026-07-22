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

// bc 0.8.0: the read-graph IR litedbmodel carries is DERIVED (spread + additive outType annotation)
// from `compileBehaviors`' output — an UNBRANDED structural doc. Use the unbranded shapes from
// `../authoring` (`BcComponent` = the unbranded component shape). The branded compile-seam handle is
// re-adopted only at the bc `generateModule` boundary, never here (the native walker
// reads structural nodes directly, no bc provenance gate).
import type { Component, Component as BcComponent, ComponentGraphIR, ComponentRefNode, MapNode, FanoutNode, BehaviorModelContract } from '../authoring';
import { IN_SENTINEL } from './tx';
import { type Dialect } from './handler';
import { compileWriteNode, mysqlPkHint } from './tx';
import { assertFindFilterFolded, type FindFilterSource } from '../find-filter-guard';
import { deriveReadOutTypes } from './outtype';
import { type ColumnTypeResolver, type MaterializeClass } from '../coltype';
import { resolveFindHardLimit } from '../limit-config';
import { DBConditions, type ConditionObject } from '../../DBConditions';
import { dbCast, dbDynamic, dbImmediate, dbTupleIn } from '../../DBValues';
import { conditionsFor } from './json-array';
import { compileSelect } from './compile-select';
import { formatterFor } from './compile';
import {
  type ExecutionContext,
  type SqliteDriver,
  contextForDriver,
} from '../exec-context';
import {
  BETWEEN_SENTINEL,
  LIKE_SENTINEL,
  CAST_SENTINEL,
  DYNAMIC_SENTINEL,
  IMMEDIATE_SENTINEL,
  TUPLE_SENTINEL,
  SUBQUERY_SENTINEL,
  EXISTS_SENTINEL,
  RAWPRED_SENTINEL,
} from '../authoring-sql';

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

/** A bc 0.7.3+ `FanoutNode` (connection fan-out). litedbmodel never emits these. */
function isFanout(n: Component['body'][number]): n is FanoutNode {
  return 'fanout' in n;
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

/** Read a `{name, sql}` CTE map port (V0 R4). Both fields must be literal strings in the IR. */
function mapPort(ports: Record<string, unknown>, name: string): { name: string; sql: string } | undefined {
  const v = ports[name];
  if (v === undefined) return undefined;
  if (typeof v === 'object' && v !== null && 'obj' in v && typeof (v as { obj: unknown }).obj === 'object') {
    const obj = (v as { obj: Record<string, unknown> }).obj;
    if (typeof obj.name !== 'string' || typeof obj.sql !== 'string') {
      throw new Error(`static-bundle: port '${name}' must be a {name, sql} literal map (got ${JSON.stringify(v)})`);
    }
    return { name: obj.name, sql: obj.sql };
  }
  throw new Error(`static-bundle: port '${name}' must be an {obj:{name, sql}} literal (got ${JSON.stringify(v)})`);
}

/** Read an `{arr:[…]}` list of DEFERRED value-specs (bc Expression IR), e.g. cte/join params. */
function exprArrayPort(ports: Record<string, unknown>, name: string): ValueSpec[] | undefined {
  const arr = arrPort(ports, name);
  if (arr === undefined) return undefined;
  return arr as ValueSpec[];
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

// ── Additive where-primitive decode (V0 R2/R3) ─────────────────────────────────
//
// The authoring layer (`authoring-sql.ts`) encodes each additive where-primitive as
// `eq(<sentinel ref>, <value>)` (the SAME closed-set trick `whereIn` uses). Here we detect the
// sentinel ref-path head, rebuild the EXACT v1 `ConditionObject` (or `DBSubquery`/`DBExists`) it
// stands for, drive the ORIGINAL `DBConditions.compile()` for byte-true text, and defer the value
// operands as value-specs 1:1 with the produced `?` placeholders — never a hand-rolled string.

/** The sentinel ref-path segments (`[head, seg0, …]`) if `node` is a sentinel column ref. */
function sentinelPath(node: unknown): string[] | undefined {
  const op = opKey(node);
  if (op !== 'ref' && op !== 'refOpt') return undefined;
  const path = (node as Record<string, unknown[]>)[op];
  if (!Array.isArray(path) || path.length === 0) return undefined;
  return path.every((s) => typeof s === 'string') ? (path as string[]) : undefined;
}

/** Extract a literal `{arr:[…]}` value-spec list from a value operand (else undefined). */
function litArr(node: unknown): unknown[] | undefined {
  if (node !== null && typeof node === 'object' && !Array.isArray(node) && 'arr' in (node as object)) {
    const a = (node as { arr: unknown }).arr;
    if (Array.isArray(a)) return a;
  }
  return undefined;
}

/** A nested makeSQL Fragment carried in a param slot: `{obj:{sql:<str>, params?:{arr:[…]}}}`. */
interface NestedSub {
  readonly sql: string;
  readonly params: readonly ValueSpec[];
}

/** Decode the nested-makeSQL Fragment param operand of a subquery/EXISTS primitive. */
function nestedSub(node: unknown, at: string): NestedSub {
  if (node === null || typeof node !== 'object' || Array.isArray(node) || !('obj' in (node as object))) {
    throw new Error(`static-bundle: ${at}: subquery operand must be a nested makeSQL Fragment {obj:{sql, params?}}`);
  }
  const obj = (node as { obj: Record<string, unknown> }).obj;
  const sqlNode = obj.sql;
  if (typeof sqlNode !== 'string') throw new Error(`static-bundle: ${at}: nested subquery 'sql' must be a literal string`);
  const params = obj.params === undefined ? [] : (litArr(obj.params) ?? undefinedThrow(at));
  return { sql: sqlNode, params };
}
function undefinedThrow(at: string): never {
  throw new Error(`static-bundle: ${at}: nested subquery 'params' must be an {arr:[…]} literal`);
}

/**
 * Decode a sentinel-encoded additive where-primitive into its v1-sourced fragment. Returns
 * undefined when the LHS is NOT one of the additive sentinels (the caller falls through to the
 * eq/cmp/IN handling). The TEXT is always produced by the ORIGINAL builders (`DBConditions` /
 * `dbCast` / `dbDynamic` / `dbImmediate` / `dbTupleIn` / `DBSubquery` / `DBExists`).
 */
function decodeSentinel(col: unknown, val: unknown, dialect: Dialect, at: string): WhereFragment | undefined {
  const path = sentinelPath(col);
  if (path === undefined) return undefined;
  const head = path[0];

  if (head === BETWEEN_SENTINEL) {
    const [column] = [path[1]];
    const bounds = litArr(val);
    if (bounds === undefined || bounds.length !== 2) throw new Error(`static-bundle: ${at}: BETWEEN expects a 2-element bounds array`);
    // v1 custom-op key `col BETWEEN ? AND ?` → 2 placeholders, both deferred.
    return { sql: v1ConditionText({ [`${column} BETWEEN ? AND ?`]: [PROBE, PROBE] }, dialect), params: bounds as ValueSpec[] };
  }

  if (head === LIKE_SENTINEL) {
    const [column, keyword] = [path[1], path[2]];
    // v1 custom-op key `col LIKE ?` / `col ILIKE ?` → 1 placeholder, deferred.
    return { sql: v1ConditionText({ [`${column} ${keyword} ?`]: PROBE }, dialect), params: [val] };
  }

  if (head === CAST_SENTINEL) {
    const [column, sqlType, cmpOp] = [path[1], path[2], path[3]];
    // Drive the ORIGINAL `dbCast(value, type, op)` so the PG `::type` cast (and MySQL/SQLite
    // no-cast) is byte-true and dialect-gated by the formatter `v1ConditionText` passes.
    return { sql: v1ConditionText({ [column]: dbCast(PROBE, sqlType, cmpOp) }, dialect), params: [val] };
  }

  if (head === DYNAMIC_SENTINEL) {
    const [column, template] = [path[1], path[2]];
    const values = litArr(val);
    // `dbDynamic(template, values)` → `col = <template>` with one `?` per template placeholder.
    const nPlaceholders = (template.match(/\?/g) ?? []).length;
    const probeVals = Array.from({ length: nPlaceholders }, () => PROBE);
    const deferred = values ?? [val];
    if (deferred.length !== nPlaceholders) {
      throw new Error(`static-bundle: ${at}: dbDynamic template has ${nPlaceholders} '?' but ${deferred.length} value-specs`);
    }
    return { sql: v1ConditionText({ [column]: dbDynamic(template, probeVals) }, dialect), params: deferred as ValueSpec[] };
  }

  if (head === IMMEDIATE_SENTINEL) {
    const [column, sql] = [path[1], path[2]];
    // `dbImmediate(sql)` → `col = <sql>` inline, NO bound param.
    return { sql: v1ConditionText({ [column]: dbImmediate(sql) }, dialect), params: [] };
  }

  if (head === TUPLE_SENTINEL) {
    const columns = path.slice(1);
    const tuples = litArr(val);
    if (tuples === undefined) throw new Error(`static-bundle: ${at}: tuple-IN expects an array-of-tuples value`);
    // Each tuple is itself a literal `{arr:[…]}` of value-specs. The v1 `dbTupleIn` text is
    // `(a, b) IN ((?, ?), …)` — one `?` per tuple element; defer them in row-major order.
    const rows = tuples.map((t) => litArr(t) ?? throwTuple(at));
    const deferred: ValueSpec[] = [];
    for (const row of rows) for (const el of row) deferred.push(el as ValueSpec);
    // Build the v1 text with PROBE tuples of the same shape (all-int probes → identical text).
    const probeRows = rows.map((r) => r.map(() => PROBE));
    return { sql: tupleInText(columns, probeRows, dialect), params: deferred };
  }

  if (head === SUBQUERY_SENTINEL) {
    const [lhs, keyword] = [path[1], path[2]];
    const sub = nestedSub(val, at);
    // v1 `DBSubquery` renders `<lhs> <IN|NOT IN> (<inner SELECT>)`. The inner SELECT text is
    // v1-sourced by the AUTHOR (dialect-tuned) and spliced verbatim; its bound value-specs follow.
    return { sql: `${lhs} ${keyword} (${sub.sql})`, params: sub.params };
  }

  if (head === EXISTS_SENTINEL) {
    const [keyword] = [path[1]];
    const sub = nestedSub(val, at);
    // v1 `DBExists` renders `<EXISTS|NOT EXISTS> (<inner SELECT 1 …>)`. Inner text v1-sourced.
    return { sql: `${keyword} (${sub.sql})`, params: sub.params };
  }

  if (head === RAWPRED_SENTINEL) {
    // Phase F-2 (#105): a COMPLETE raw WHERE predicate carried verbatim (the whole `ConditionObject`
    // compiled by v1 `DBConditions.compile()` upstream). Splice `sub.sql` verbatim (it already carries
    // its own `?` placeholders) and defer `sub.params` 1:1 — no re-derivation, byte-true because the
    // text IS v1's. The `?`→`$N` dialect renumber is the final render pass, so the spliced text stays `?`.
    const sub = nestedSub(val, at);
    return { sql: sub.sql, params: sub.params };
  }

  return undefined;
}
function throwTuple(at: string): never {
  throw new Error(`static-bundle: ${at}: each tuple-IN row must be an {arr:[…]} literal`);
}

/** The v1 `dbTupleIn` text via a nested `DBConditions.__tuple__` (byte-true; probe tuples). */
function tupleInText(columns: readonly string[], probeRows: unknown[][], dialect: Dialect): string {
  // Reuse the ORIGINAL dbTupleIn so the `(cols) IN ((?, …), …)` text is v1-produced, not hand-rolled.
  const cond: ConditionObject = { __tuple__: dbTupleIn([...columns], probeRows) };
  return v1ConditionText(cond, dialect);
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
  // Pass the dialect cast formatter so `dbCast` gates the `::type` cast per-dialect (PG applies it,
  // MySQL/SQLite drop it — v1 parity). Inert for every non-cast construct (the formatter is only
  // consulted by DBCast/DBCastArray), so it stays byte-identical for eq/cmp/BETWEEN/LIKE/etc.
  return conditionsFor(conditions, dialect).compile(probe, formatterFor(dialect));
}

/**
 * Assemble an AND/OR GROUP's text by driving the ORIGINAL `DBConditions.compile()` — the SAME join
 * + paren-wrapping the v1 eager path performs — NOT a v2 hand-roll (#47 item 5). The already-v1-
 * produced leaf texts are fed as `__raw__` members of a nested `DBConditions` under the group's
 * operator; a parent references that nested group, so v1's own `compileCondition` emits the wrapping
 * `(${nested.compile()})` and `nested.compile()` joins the leaves with ` AND `/` OR ` — i.e. the
 * `(A AND B)` / `(A OR B)` text is byte-produced by v1's builder, not concatenated here.
 *
 * `DBConditions.compile` appends probe values per `__raw__` in encounter order, so the parent probe
 * list length equals the group's total placeholder count; the leaf value-specs (kept 1:1 with those
 * placeholders by the caller) remain the runtime binds — the probe array is throwaway.
 */
function v1GroupText(op: 'and' | 'or', leafSqls: readonly string[]): string {
  const operator: 'AND' | 'OR' = op === 'and' ? 'AND' : 'OR';
  // Each leaf is a v1-produced predicate string → a `__raw__` member. `add` preserves order and the
  // nested group's `compile()` joins them with ` ${operator} ` (v1's exact algorithm).
  const inner = new DBConditions({}, operator);
  for (const sql of leafSqls) inner.add({ __raw__: sql }, operator);
  // A parent whose sole member is the nested group → v1 wraps it as `(${inner.compile()})`
  // (DBConditions.compileCondition, the `value instanceof DBConditions` branch) — the group parens.
  const parent = new DBConditions({});
  parent.add(inner);
  const probe: unknown[] = [];
  return parent.compile(probe);
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
  // The array param is bound as ONE value; the `executeSQL` leaf's `encodeParams` is the SSoT that
  // dialect-encodes it (PostgreSQL keeps the raw array for `= ANY(?)`; MySQL/SQLite JSON-encode it for
  // the `json_each(?)` form). So the fragment carries the plain array `valueSpec` — no separate marker.
  if (dialect === 'postgres') {
    // `col = ANY(?)` — no cast. PG infers the array element type from the column, which is correct
    // for every type INCLUDING empty and uuid (a value-inferred cast cannot, and re-broke #46).
    const conditions: ConditionObject = { __raw__: [`${column} = ANY(?)`, [PROBE]] };
    return { sql: v1ConditionText(conditions, dialect), params: [valueSpec] };
  }
  // MySQL/SQLite: the single-JSON IN-list is the JsonArrayConditions form (v1/epic builder).
  const conditions: ConditionObject = { [column]: [PROBE] };
  return { sql: v1ConditionText(conditions, dialect), params: [valueSpec] };
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
    // Assemble the group text through the ORIGINAL `DBConditions` join + paren-wrap (item 5) — NOT
    // a v2 `parts.join(connector)` hand-roll. The v1-produced leaf texts keep their `?` order, so
    // the deferred value-specs (`params`) still bind 1:1 with the placeholders in the assembled sql.
    return { sql: v1GroupText(op, parts), params };
  }

  if (op === 'eq') {
    const [col, val] = binOperands(node, op, at);
    // Additive where-primitives (V0 R2/R3) — decoded FIRST (an IMMEDIATE/EXISTS primitive carries a
    // `null` RHS that would otherwise be mis-read as IS NULL).
    const inCol = inSentinelColumn(col);
    if (inCol !== undefined) return inListFragment(dialect, inCol, val);
    const additive = decodeSentinel(col, val, dialect, at);
    if (additive !== undefined) return additive;
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
export function lowerWherePort(ports: Record<string, unknown>, dialect: Dialect, at: string): StaticStatement[] {
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
 * The ` LIMIT `/` OFFSET ` KEYWORD text sourced from the ORIGINAL `_buildSelectSQL` (via
 * `compileSelect`) — item 5. v1 INLINES the count as a literal (` LIMIT 10`); the static bundle
 * keeps the INTENTIONAL divergence of binding it as a `?` deferred value-spec (a portable equivalent
 * every language runtime renders identically, evaluated per-input). So we drive v1 with a sentinel
 * count, take v1's exact ` LIMIT <n>`/` OFFSET <n>` append, and swap the sentinel for `?` — the
 * keyword text is v1's, only the literal becomes a placeholder. Documented, all-dialect parity-checked.
 */
function limitOffsetTail(kind: 'limit' | 'offset', dialect: Dialect, table: string): string {
  const SENTINEL = 987654321; // a distinctive count with no substring clash in the keyword text
  const desc = kind === 'limit' ? { dialect, tableName: table, limit: SENTINEL } : { dialect, tableName: table, offset: SENTINEL };
  const tail = selectTail(desc, table); // v1's ` LIMIT 987654321` / ` OFFSET 987654321`
  return tail.replace(String(SENTINEL), '?'); // keep the bound-param behavior; keyword text is v1's
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
  if (component !== 'Select' && component !== 'Count' && component !== 'RelationBatch') {
    throw new Error(`static-bundle: compileSelectNode only compiles Select/Count/RelationBatch nodes (got '${component}')`);
  }
  const table = stringPort(ports, 'table');
  if (table === undefined) throw new Error(`static-bundle: ${component} node requires a literal 'table' port`);
  if (component === 'RelationBatch') {
    const sql = stringPort(ports, 'sql');
    if (sql === undefined) throw new Error("static-bundle: RelationBatch requires compiled 'sql'");
    const keys = Object.keys(ports)
      .filter((p) => /^key\.\d+$/.test(p))
      .sort((a, b) => Number(a.slice(4)) - Number(b.slice(4)))
      .map((p) => ports[p] as ValueSpec);
    return [{ sql, params: keys }];
  }
  // A `Count` node projects `COUNT(*) as count` (v1 `DBModel._count`: `SELECT COUNT(*) as count
  // FROM t`), a `Select` its own column list (default `*`). Both heads go through the ORIGINAL
  // `compileSelect`, so the text is v1-sourced (count head byte-identical to `_count`).
  const isCount = component === 'Count';
  const select = stringArrayPort(ports, 'select');
  const cols = isCount ? 'COUNT(*) as count' : select && select.length > 0 ? select.join(', ') : '*';

  const statements: StaticStatement[] = [];
  // Head `[WITH …] SELECT <cols> FROM <t>[ JOIN …]` — driven by the ORIGINAL `_buildSelectSQL` (via
  // compileSelect, no WHERE/tail) so the skeleton text is byte-identical to v1. The optional
  // `cte`/`join` head fields (V0 R4/R5) are v1-sourced from the SAME `SelectDesc`; the `?`
  // placeholders in their raw text bind to the deferred `cteParams`/`joinParams` value-specs (v1
  // param order: CTE → JOIN → WHERE). `select` carries the joined-column projection as authored.
  const cte = mapPort(ports, 'cte');
  const join = stringPort(ports, 'join');
  const cteParams = exprArrayPort(ports, 'cteParams');
  const joinParams = exprArrayPort(ports, 'joinParams');
  const headDesc: Parameters<typeof compileSelect>[0] = { dialect, tableName: table, select: cols };
  if (cte !== undefined) headDesc.cte = { name: cte.name, sql: cte.sql, params: [] };
  if (join !== undefined) headDesc.join = join;
  const headParams: ValueSpec[] = [...(cteParams ?? []), ...(joinParams ?? [])];
  statements.push({ sql: compileSelect(headDesc).sql, params: headParams });

  const whereFrags = lowerWherePort(ports, dialect, component);
  for (const frag of whereFrags) {
    // A bare predicate body flagged `whereFragment`; the runtime resolves the ` WHERE `/` AND `
    // connector from the present set (a compile-time connector would be wrong when an earlier
    // fragment skips).
    statements.push({ sql: frag.sql, params: frag.params, whereFragment: true, ...(frag.skip !== undefined ? { skip: frag.skip } : {}) });
  }

  // A Count carries no projection/order/limit/offset tail (v1 `_count` is `COUNT(*)` + WHERE only).
  if (isCount) return statements;

  // GROUP BY / ORDER BY tail — the ORIGINAL `_buildSelectSQL` append text (via compileSelect over
  // a bare table, then slicing off the leading `SELECT * FROM <t>` head it shares).
  const group = stringPort(ports, 'group');
  if (group !== undefined) statements.push({ sql: selectTail({ dialect, tableName: table, group }, table), params: [] });
  const order = stringPort(ports, 'order');
  if (order !== undefined) statements.push({ sql: selectTail({ dialect, tableName: table, order }, table), params: [] });
  // LIMIT/OFFSET: the ` LIMIT `/` OFFSET ` keyword text is v1-sourced (via compileSelect), the count
  // stays a bound `?` value-spec (the intentional portable divergence from v1's literal inlining).
  if (ports.limit !== undefined) statements.push({ sql: limitOffsetTail('limit', dialect, table), params: [ports.limit as ValueSpec] });
  if (ports.offset !== undefined) statements.push({ sql: limitOffsetTail('offset', dialect, table), params: [ports.offset as ValueSpec] });

  // FOR UPDATE / append (V0 R3/R6) — v1 `_buildSelectSQL` tail order is `… FOR UPDATE <append>`.
  // Both are v1-sourced via `selectTail` (a bare-head compileSelect, head sliced off). `append`
  // (e.g. HAVING) is raw trailing text with no bound param on this authored surface.
  const forUpdate = stringPort(ports, 'forUpdate');
  if (forUpdate === 'true') statements.push({ sql: selectTail({ dialect, tableName: table, forUpdate: true }, table), params: [] });
  const append = stringPort(ports, 'append');
  if (append !== undefined) statements.push({ sql: selectTail({ dialect, tableName: table, append }, table), params: [] });

  return statements;
}

// ── Compile a behavior method → static bundle (CRUD primary node) ──────────────



// ── Optional-head detection (SSoT-driven, mirrors ../runtime.optionalHeadsOf) ──


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

// ── FIND_FILTER leak guard: authored WHERE column keys (fail-closed model-scope enforcement) ──

/**
 * Collect the top-level COLUMN names an authored `where` port constrains — the LHS column of each
 * eq/ne/cmp member, the IN-sentinel column, and (recursively) the columns of `and`/`or` groups and
 * SKIP-`cond` members. This is exactly the key set v1 compares against when it folds a model's
 * `FIND_FILTER` into the WHERE (`condsToRecord(FIND_FILTER)` keys vs the compiled predicate), so the
 * {@link assertFindFilterFolded} guard can fail-closed when a FIND_FILTER model is SCP-compiled
 * without its scope predicates folded in. Additive sentinel primitives (immediate/dynamic/exists/…)
 * carry no plain column key and are skipped — a scope key must be an ordinary eq/cmp/in the author
 * expressed, matching how a FIND_FILTER predicate manifests.
 */
function whereMemberColumns(node: unknown, into: Set<string>): void {
  const op = opKey(node);
  if (op === undefined) return;
  if (op === 'and' || op === 'or') {
    const args = (node as Record<string, unknown[]>)[op];
    if (Array.isArray(args)) for (const m of args) whereMemberColumns(m, into);
    return;
  }
  if (op === 'cond') {
    const cargs = (node as Record<string, unknown[]>).cond;
    if (Array.isArray(cargs) && cargs.length === 3) whereMemberColumns(cargs[1], into);
    return;
  }
  if (op === 'eq' || op in CMP_OPS) {
    const args = (node as Record<string, unknown[]>)[op];
    if (!Array.isArray(args) || args.length !== 2) return;
    const inCol = inSentinelColumn(args[0]);
    if (inCol !== undefined) { into.add(inCol); return; }
    // A plain column ref LHS (a bare eq/cmp). An additive sentinel LHS (immediate/dynamic/exists/…)
    // is NOT a plain column — its column key would be encoded in the primitive, so it is skipped
    // (a FIND_FILTER scope key is always an ordinary eq/cmp/IS NULL the author must fold explicitly).
    const lhsOp = opKey(args[0]);
    if (lhsOp === 'ref' || lhsOp === 'refOpt') {
      const path = (args[0] as Record<string, unknown[]>)[lhsOp];
      if (Array.isArray(path) && path.length > 0 && typeof path[path.length - 1] === 'string' && path[0] !== IN_SENTINEL) {
        into.add(path[path.length - 1] as string);
      }
    }
  }
}

/** The authored WHERE column-key set of a read node's `where` port (for the FIND_FILTER guard). */
function authoredWhereKeysOf(ports: Record<string, unknown>): Set<string> {
  const keys = new Set<string>();
  const arr = arrPort(ports, 'where');
  if (arr === undefined) return keys;
  for (const m of arr) whereMemberColumns(m, keys);
  return keys;
}

// ── Runtime: evaluate value-specs + skip per-input, assemble, render, execute ──






/**
 * The minimal synchronous SQLite driver surface (better-sqlite3 `Database`) — the backward-compat
 * public seam. Internally the read/write path runs through the {@link ExecutionContext} seam
 * (`../exec-context`); a raw `SqliteDb` a caller passes is wrapped via {@link contextForDriver} at
 * the entry so existing callers stay byte-identical. Aliased to {@link SqliteDriver} (the seam's
 * driver shape) so the two are interchangeable.
 */
export type SqliteDb = SqliteDriver;

/** A read/write entry accepts either a raw {@link SqliteDb} or a full {@link ExecutionContext}. */
export type DbOrContext = SqliteDb | ExecutionContext;





// ============================================================================
// #12 — the NATIVE read-graph walker (interpreter-free): litedbmodel composes map /
// Φ-merge / wiring ITSELF over `compileBehaviors`' REAL `Select`/`Count`/map node IR
// (no `__makeSqlNode`/`__scope` surrogate, no bc `runBehavior`). Each body node's SQL
// comes from its `statementsById[id]` fragment templates (rendered against the walk
// scope); topology (`map.over`/`as`, `plan.groups`, `output` Φ) is read off the real
// nodes. This mirrors the rust/go/py/php native walkers — one shared orchestration model
// across all 5 runtimes, `runBehavior` invoked on NONE of them.
// ============================================================================

/** Compile ONE authored SQL node (Select or CRUD write) into its static statements. */
function compileNodeStatements(node: RefLike, dialect: Dialect, resolveColumnType?: ColumnTypeResolver): StaticStatement[] {
  const component = 'map' in node ? (node as MapNode).map.component : node.component;
  if (component === 'Select' || component === 'Count' || component === 'RelationBatch') return compileSelectNode(node, dialect);
  const op = compileWriteNode(node as { component: 'Insert' | 'Update' | 'Delete'; ports: Record<string, unknown> }, dialect, resolveColumnType);
  // MySQL has no native RETURNING: bake the pk-hint into the readGraph write statement (the SAME
  // `mysqlPkHint` SSoT compileSaveBundle/compileCreateManyBundle/codegen use) so the interpreter path
  // (execute_bundle over this readGraph) honors a declared RETURNING via the driver re-select — matching
  // the native-codegen path. No-op for pg/sqlite (native RETURNING) or a non-RETURNING write.
  const hinted = dialect === 'mysql' ? mysqlPkHint(op) : op;
  return [{ sql: hinted.sql, params: hinted.params }];
}

/**
 * The compiled, portable READ graph of a behavior method (#12 de-surrogated): `compileBehaviors`'
 * REAL `ComponentGraphIR` — the authored `Select`/`Count`/map nodes with their own ports (only
 * additively annotated with read `outType`/`outputType`), NOT a hand-built `__makeSqlNode`/`__scope`
 * surrogate — plus the per-node static `makeSQL` statement templates (opaque, v1-`DBConditions`-
 * sourced SQL TEXT) keyed by the real body-node id, and the optional heads. Pure JSON —
 * `JSON.stringify` round-trips, so every language runtime walks this real-node IR natively (topology
 * from the real nodes, SQL from `statementsById`) with a SQL driver — never bc `runBehavior`.
 */
export interface ReadGraph {
  readonly dialect: Dialect;
  readonly name: string;
  /** `compileBehaviors`' real `ComponentGraphIR` (real `Select`/`Count`/map nodes; outType-annotated). */
  readonly ir: ComponentGraphIR;
  /** Per-node static `makeSQL` statement templates (v1-sourced SQL text), keyed by real body node id. */
  readonly statementsById: Record<string, readonly StaticStatement[]>;
  /** Input heads normalized to present-as-null (absent-key SKIP; SSoT-driven). */
  readonly optionalHeads: readonly string[];
  /**
   * TS read-path MATERIALIZERS (issue #59), per Select node: `column → MaterializeClass`. Present
   * ONLY when a column-type resolver was supplied at compile (same gate as the `outType`
   * annotations) — without it the read stays un-materialized (raw driver values), identical to the
   * pre-#59 behavior. When present, the read handler coerces each raw row cell to the JS form the
   * SQL column type declares (BIGINT→bigint, INT→number, DATE→string, BOOLEAN→boolean), consistent
   * across sqlite/pg/mysql.
   */
  readonly materializersByNode?: Record<string, Record<string, MaterializeClass>>;
  /**
   * Hard-limit runaway guard for the top-level read (Phase E-2, epic #74; v1 `DBModel` find
   * hard-limit). Present ONLY when a `findHardLimit` is configured AND the PRIMARY read node (the
   * first non-cond/non-map body node — the "find" row list) had NO authored `limit` port (v1: the
   * cap applies only when the caller set no explicit limit). At compile the read injects `LIMIT
   * hardLimit + 1` into that node's statements; the native walker checks the primary node's fetched
   * row count against `hardLimit` AFTER fetch and throws {@link import('../errors').LimitExceededError}
   * (`context: 'find'`) when it exceeds. Pure JSON — the native ports (#100-103) run the SAME check
   * off this field with no config surface of their own.
   */
  readonly findGuard?: {
    /** The row cap. A primary-node fetch of MORE than this throws (`context: 'find'`). */
    readonly hardLimit: number;
    /** The real body-node id of the primary read node whose row count is checked. */
    readonly nodeId: string;
    /** The read model (behavior) name — the error's `model` field. */
    readonly model: string;
  };
}

/**
 * Compile a READ behavior method into a portable {@link ReadGraph} (#12 de-surrogated): keep
 * `compileBehaviors`' REAL `Select`/`Count`/map body nodes (only additively annotate each with its
 * read `outType`), and compile each node's static `makeSQL` statements ONCE into the `statementsById`
 * sidecar (keyed by the real body-node id). SYMBOLIC — no concrete input. The native walker
 * ({@link executeReadGraph}) owns map iteration / wire binding / Φ output at execute time, rendering
 * each node's statements against the walk scope — never bc `runBehavior`, never a `__scope` surrogate.
 */
export function compileReadGraph(
  contract: BehaviorModelContract,
  dialect: Dialect,
  entry?: string,
  findFilterModel?: FindFilterSource,
  resolveColumnType?: ColumnTypeResolver,
): ReadGraph {
  const component = entry ? contract.components.find((c) => c.name === entry) : contract.components[0];
  if (component === undefined) throw new Error(`static-bundle: entry component '${entry ?? '<first>'}' not found in contract`);

  // FIND_FILTER fail-closed guard (M2): when a `DBModel`-shaped source that DECLARES a `FIND_FILTER`
  // (soft-delete / tenant scope) is routed through the SCP read compile, its scope predicates MUST be
  // folded into the authored `where` of every read node — the SCP compile has no model context to
  // auto-apply them (see find-filter-guard.ts). If any read node's authored WHERE omits a scope key,
  // `assertFindFilterFolded` throws (never a silent cross-tenant / soft-deleted leak). No model
  // supplied ⇒ nothing to enforce (the guard is a no-op for a model with no FIND_FILTER either way).
  if (findFilterModel !== undefined) {
    for (const n of component.body) {
      if ('cond' in n) continue;
      if (isFanout(n)) {
        // bc 0.7.3+ `FanoutNode`. litedbmodel never emits fanout — reject fail-closed rather
        // than mis-read it as a Select/Count ref and skip the FIND_FILTER fold check.
        throw new Error(`static-bundle: read component '${component.name}' node '${n.id}' is a fanout node, not supported by litedbmodel (bc FanoutNode)`);
      }
      const ref = 'map' in n ? n.map : n;
      if (ref.component !== 'Select' && ref.component !== 'Count') continue;
      assertFindFilterFolded(findFilterModel, authoredWhereKeysOf(ref.ports as Record<string, unknown>));
    }
  }

  const statementsById: Record<string, readonly StaticStatement[]> = {};
  for (const n of component.body) {
    if ('cond' in n) continue;
    if (isFanout(n)) {
      // bc 0.7.3+ `FanoutNode`. litedbmodel never emits fanout — reject fail-closed rather
      // than feed it to `compileNodeStatements` as a catalog ref.
      throw new Error(`static-bundle: read component '${component.name}' node '${n.id}' is a fanout node, not supported by litedbmodel (bc FanoutNode)`);
    }
    statementsById[n.id] = compileNodeStatements(n, dialect, resolveColumnType);
  }

  // Hard-limit runaway guard for the top-level read (Phase E-2, epic #74; v1 `DBModel` find
  // hard-limit). The PRIMARY read node is the first non-cond/non-map body `Select` — the "find" row
  // list. When a `findHardLimit` is configured AND the author set NO explicit `limit` on it (v1:
  // `!opts?.limit`), inject `LIMIT hardLimit + 1` (an N+1 fetch — enough to KNOW the total exceeds
  // the cap without loading the whole runaway set) into that node's statements, and carry the guard
  // metadata so the native walker checks the primary node's row count post-fetch. A node that already
  // carries an authored `limit` is left untouched (no guard, no injection — byte-identical SQL).
  const findGuard = resolveFindGuard(component, statementsById, dialect);

  // SINGLE column-type resolution (issue #59 audit — unified read+codegen): `deriveReadOutTypes`
  // resolves every projected column of the read ONCE (via the SHARED projection parser, all shapes:
  // bare / qualified / aliased; `*` / computed / undeclared → HARD ERROR, spec §4.1) and returns BOTH
  // the codegen `outType`/`outputType` annotations AND the TS read-path `materializersByNode` — two
  // projections of the ONE resolution, so they cannot diverge (there is no second, weaker read-path
  // pass). The column types come from the model's inline `static columns` (carried on the contract as
  // `resolveColumnType`, always present for a read model), so this runs for EVERY read: codegen writes
  // the outType IR annotations, the runtime read path consumes the materializers. Any ambiguity /
  // undeclared column / `*` / computed projection throws here.
  const readTypes = resolveColumnType !== undefined ? deriveReadOutTypes(component as never, resolveColumnType) : undefined;
  const materializers = readTypes?.materializersByNode;

  // #12 (de-surrogation): the read-graph IR is `compileBehaviors`' REAL component — the authored
  // `Select`/`Count`/map nodes with their own `table`/`select`/`where`/… ports, NOT a hand-built
  // `__makeSqlNode` + `__scope` surrogate. litedbmodel constructs NO `ComponentGraphIR`/`BodyNode`
  // literal here: it only ADDITIVELY annotates each node with its read `outType` (issue #59) and the
  // component with its `outputType` — the node SHAPE/ports are `compileBehaviors`' output untouched.
  // The compiled SQL fragment templates ride the sidecar `statementsById` (opaque, v1-`DBConditions`-
  // sourced TEXT), keyed by the SAME real body-node id; every language runtime walks this real-node
  // IR natively (topology from the real nodes; SQL from `statementsById[id]`), never bc `runBehavior`.
  const annotatedBody = component.body.map((n) => {
    if ('cond' in n) return n;
    const outType = readTypes?.byNode.get(n.id);
    return outType !== undefined ? { ...n, outType } : n;
  });
  const irComponent = {
    ...component,
    body: annotatedBody,
    ...(readTypes !== undefined ? { outputType: readTypes.outputType } : {}),
  } as unknown as BcComponent;
  // Preserve `compileBehaviors`' OWN IR envelope (irVersion/exprVersion + any other metadata) — the
  // read graph IR is the compiler's output with ONLY the single entry component swapped for its
  // outType-annotated twin (additive). litedbmodel fabricates no `ComponentGraphIR`/`BodyNode`.
  const ir: ComponentGraphIR = { ...contract.ir, components: [irComponent] };

  return {
    dialect,
    name: component.name,
    ir,
    statementsById,
    optionalHeads: [...optionalHeadsOfComponent(component, dialect, resolveColumnType)],
    ...(materializers !== undefined && materializers.size > 0 ? { materializersByNode: Object.fromEntries(materializers) } : {}),
    ...(findGuard !== undefined ? { findGuard } : {}),
  };
}

/**
 * Resolve the top-level-read hard-limit guard (Phase E-2). Returns the guard metadata (and MUTATES
 * `statementsById` to inject `LIMIT hardLimit + 1` into the primary node's statements) when a
 * `findHardLimit` is configured AND the primary read node had NO authored `limit`; else `undefined`
 * (no guard, statements untouched — byte-identical SQL for the uncapped path).
 *
 * The PRIMARY read node is the first non-cond/non-map body `Select` (the "find" row list). A `Count`
 * primary is never capped (it returns a scalar, not a runaway row list). The injection reuses the
 * SAME `limitOffsetTail` + literal value-spec the authored `limit` port lowers to, so the injected
 * ` LIMIT ?` renders byte-identically to an authored limit — only the value is the baked `hardLimit
 * + 1`.
 */
function resolveFindGuard(
  component: Component,
  statementsById: Record<string, readonly StaticStatement[]>,
  dialect: Dialect,
): ReadGraph['findGuard'] {
  const hardLimit = resolveFindHardLimit();
  if (hardLimit === null) return undefined; // disabled ⇒ no guard, no injection
  // The primary read node: first non-cond, non-map, non-fanout body node that is a `Select`.
  const primary = component.body.find(
    (n) => !('cond' in n) && !('map' in n) && !isFanout(n) && (n as { component?: string }).component === 'Select',
  ) as (ComponentRefNode | undefined);
  if (primary === undefined) return undefined; // no plain Select primary (e.g. a Count-only read)
  // v1 `!opts?.limit`: only cap a read whose primary carries NO authored `limit` port.
  if ((primary.ports as Record<string, unknown>).limit !== undefined) return undefined;
  const table = (primary.ports as Record<string, unknown>).table;
  if (typeof table !== 'string') return undefined; // a Select without a literal table can't be capped
  // Inject `LIMIT hardLimit + 1` as a literal value-spec (bc IR literal = the bare number) into the
  // primary node's statements — the SAME shape an authored `limit` lowers to, so it renders identically.
  const injected: StaticStatement = { sql: limitOffsetTail('limit', dialect, table), params: [hardLimit + 1] };
  statementsById[primary.id] = [...statementsById[primary.id], injected];
  return { hardLimit, nodeId: primary.id, model: component.name };
}



// ── Native read-graph orchestration primitives (shared by sync/async walkers) ──










/** An async driver seam: run a rendered `{ sql, params }` and resolve to result rows (#40). */
export type SqlExecutorAsync = (sql: string, params: unknown[]) => Promise<Record<string, unknown>[]>;






/** Optional heads across ALL SQL nodes of a component (schema + SKIP-guard + refOpt). */
function optionalHeadsOfComponent(component: Component, dialect: Dialect, resolveColumnType?: ColumnTypeResolver): Set<string> {
  const optional = new Set<string>();
  for (const [port, schema] of Object.entries(component.inputPorts)) {
    if (schema.required !== true) optional.add(port);
  }
  for (const n of component.body) skipGuardHeads(n, optional);
  for (const n of component.body) {
    if ('cond' in n) continue;
    if (isFanout(n)) continue; // bc FanoutNode: litedbmodel never emits it; carries no ref-opt heads.
    const stmts = compileNodeStatements(n, dialect, resolveColumnType);
    for (const stmt of stmts) {
      for (const p of stmt.params) collectRefOptHeads(p, optional);
      if (stmt.skip !== undefined) collectRefOptHeads(stmt.skip, optional);
    }
  }
  return optional;
}

