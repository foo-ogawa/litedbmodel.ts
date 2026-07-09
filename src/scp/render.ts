/**
 * litedbmodel v2 SCP — fragment-tree render + param assembly (WS1, #21).
 *
 * The NORMATIVE reference implementation of `docs/proposal/sql-dynamic-expansion-spec.md`.
 * Given a compiled operation and a bound input scope, it deterministically produces the
 * final SQLite SQL text (`?` placeholders) and the flat params array. Every multi-language
 * runtime (WS3) must reproduce this byte-for-byte; this TS implementation is the golden
 * reference the conformance harness pins.
 *
 * The three moving parts (dynamic-expansion spec):
 *   §2 SKIP → fragment existence: a fragment with `when` is present iff `when` evaluates
 *       to a present (non-null / true) binding; an absent fragment contributes NO SQL and
 *       NO params.
 *   §3 empty-WHERE degeneration: if no fragment is present the whole `{where}` splice
 *       (including the ` WHERE ` keyword) collapses to the empty string.
 *   §4 AND/OR structure + parenthesization: a nested tree renders `(… <connector> …)`.
 *   §5 IN-list array expansion: a fragment slot marked `expand` turns its `(?)` into
 *       `(?, ?, …)` for each element of the bound array (0 elements → `(NULL)` sentinel,
 *       matching litedbmodel v1's `1 = 0` degeneration handled at compile time instead).
 */

import { evaluateExpression, type Scope, type Value } from 'behavior-contracts';
import type { CompiledOperation, Fragment, FragmentTree } from './ir';
import { WHERE_SLOT } from './ir';

/** The result of rendering: final SQL text + flat params (1:1 with `?`). */
export interface RenderedSql {
  sql: string;
  params: Value[];
}

/** A fragment tree carries a `connector`; a fragment carries `sql`. */
function isTree(node: Fragment | FragmentTree): node is FragmentTree {
  return 'connector' in node;
}

/**
 * SKIP existence rule (dynamic-expansion spec §2): a fragment is present iff it is
 * `always`, or its `when` Expression evaluates to a PRESENT binding — `null` and `false`
 * are absent; everything else (including `0`, `""`) is present. This mirrors bc's
 * strict-bool discipline: `when` is expected to be an explicit presence/bool Expression
 * (e.g. `{ne:[{refOpt:["status"]}, null]}` in bc's flat scope), evaluated fail-closed by
 * evaluateExpression.
 */
function fragmentPresent(f: Fragment, scope: Scope): boolean {
  if (f.always === true) return true;
  if (f.when === undefined) return false; // fail-closed: neither always nor when
  const v = evaluateExpression(f.when, scope);
  return v !== null && v !== false;
}

/**
 * Render one leaf fragment's SQL + params into the accumulators. Handles IN-list
 * expansion (spec §5): the slot at `expand` is an array; its `(?)` in `sql` becomes
 * `(?, ?, …)` and each element is pushed as its own param.
 */
function renderFragment(f: Fragment, scope: Scope, params: Value[]): string {
  if (f.expand === undefined) {
    for (const slot of f.params) params.push(evaluateExpression(slot, scope));
    return f.sql;
  }
  // IN-list expansion. Evaluate all slots; the `expand` slot must be an array.
  let sql = f.sql;
  for (let i = 0; i < f.params.length; i++) {
    const v = evaluateExpression(f.params[i], scope);
    if (i === f.expand) {
      if (!Array.isArray(v)) {
        throw new Error(`IN-list expansion slot ${i} did not bind to an array (got ${v === null ? 'null' : typeof v})`);
      }
      if (v.length === 0) {
        // Empty-array degeneration (dynamic-expansion spec §5): the whole `col IN (?)`
        // fragment collapses to the always-false sentinel `1 = 0` — byte-identical to
        // litedbmodel v1's DBConditions (an empty IN pushes NO params). The `col IN`
        // prefix and the `(?)` slot are BOTH replaced.
        sql = '1 = 0';
        // no params pushed for this slot
      } else {
        // Replace the single `(?)` with `(?, ?, …)` sized to the array; push each element.
        sql = sql.replace('(?)', `(${v.map(() => '?').join(', ')})`);
        for (const el of v) params.push(el);
      }
    } else {
      params.push(v);
    }
  }
  return sql;
}

/**
 * Render a fragment tree into a WHERE clause body (no leading ` WHERE ` keyword).
 * Present fragments are joined by ` <connector> `; a nested tree is parenthesized
 * (spec §4). Returns the empty string when NO fragment is present (degeneration, §3).
 *
 * Each fragment's `sql` already carries its own leading connector prefix when authored as
 * a chained condition (e.g. `" AND status = ?"`), so this function STRIPS the accumulated
 * connector from the first present fragment to avoid a dangling leading `AND`/`OR`, then
 * joins the rest as-authored. To keep the golden output identical to litedbmodel v1 (which
 * builds `parts.join(' AND ')` with no leading connector), fragment `sql` at the top level
 * is authored WITHOUT a leading connector and joined here.
 */
function renderTree(tree: FragmentTree, scope: Scope, params: Value[]): string {
  const parts: string[] = [];
  for (const node of tree.fragments) {
    if (isTree(node)) {
      const inner = renderTree(node, scope, params);
      if (inner !== '') parts.push(`(${inner})`);
    } else if (fragmentPresent(node, scope)) {
      parts.push(renderFragment(node, scope, params));
    }
  }
  if (parts.length === 0) return '';
  return parts.join(` ${tree.connector} `);
}

/**
 * Render a compiled operation to final SQLite SQL + params for a bound input scope.
 *
 * Param order (matches SQL text order, dynamic-expansion spec §6): the compiled `sql`
 * splices the WHERE tree at `{where}`; static params before the marker are emitted first,
 * then the fragment params in tree order, then static params after the marker. WS1's CRUD
 * shapes place all pre-WHERE statics (Insert values, Update SET) before `{where}` and all
 * post-WHERE statics (LIMIT / OFFSET) after — so a single left-to-right walk of the
 * spliced SQL yields the canonical `?` order.
 */
export function renderOperation(op: CompiledOperation, input: Scope): RenderedSql {
  const params: Value[] = [];
  const markerIdx = op.sql.indexOf(WHERE_SLOT);

  if (markerIdx === -1) {
    // No dynamic WHERE: all params are static, in position order.
    for (const slot of op.params) params.push(evaluateExpression(slot, input));
    return { sql: op.sql, params };
  }

  const before = op.sql.slice(0, markerIdx);
  const after = op.sql.slice(markerIdx + WHERE_SLOT.length);

  // Static params are partitioned by whether their `?` sits before or after the marker.
  const beforeQ = countPlaceholders(before);
  const preStatics = op.params.slice(0, beforeQ);
  const postStatics = op.params.slice(beforeQ);

  for (const slot of preStatics) params.push(evaluateExpression(slot, input));

  let whereSql = '';
  if (op.where !== null) {
    const body = renderTree(op.where, input, params);
    if (body !== '') whereSql = ` WHERE ${body}`; // degeneration §3: drop keyword when empty
  }

  for (const slot of postStatics) params.push(evaluateExpression(slot, input));

  return { sql: before + whereSql + after, params };
}

/** Count `?` placeholders in a static SQL segment (no fragment markers present). */
function countPlaceholders(sql: string): number {
  let n = 0;
  for (const ch of sql) if (ch === '?') n++;
  return n;
}
