/**
 * litedbmodel v2 SCP — fragment RUNTIME (assemble + bind).
 *
 * This is the thin runtime a bundle drives. It does exactly three things:
 *
 *   1. ASSEMBLE — walk the ordered fragment list; drop any fragment whose `skip`
 *      is satisfied; concatenate the present fragments' `sql`, and their `params`
 *      in order. Nested-fragment params splice their own `sql` at the `?` and
 *      contribute their own params in order.
 *   2. EVALUATE — turn each value-spec param into a concrete bound value using the
 *      runtime `input` (and upstream `wire` results).
 *   3. RENDER — the produced `{ sql, params }` uses `?`; a dialect step turns `?`
 *      into `$N` for PostgreSQL (identical to the original driver's naive replace).
 *
 * NO SQL structure is generated here. The `sql` text is already complete; the
 * runtime only splices nested-fragment text and fills `?`.
 */

import {
  type Fragment,
  type Node,
  type WhereGroup,
  type Param,
  type ValueSpec,
  type PresenceCondition,
  isFragment,
  isWhereGroup,
  isInputRef,
  isWireRef,
  isLiteral,
  isValueExpr,
} from './model';

/** Runtime binding scope: FLAT — input keys and wire results are top-level roots. */
export interface Scope {
  input: Record<string, unknown>;
  wire?: Record<string, Record<string, unknown>>;
}

function readPath(root: Record<string, unknown>, path: string[]): unknown {
  let cur: unknown = root;
  for (const seg of path) {
    if (cur == null) return undefined;
    cur = (cur as Record<string, unknown>)[seg];
  }
  return cur;
}

/** Is a fragment's `skip` satisfied (⇒ omit the fragment)? */
export function isSkipped(cond: PresenceCondition | undefined, scope: Scope): boolean {
  if (!cond) return false;
  const v = readPath(scope.input, cond.absent);
  return v == null;
}

/** Evaluate a value-spec to its bound value against the runtime scope. */
export function evalValueSpec(spec: ValueSpec, scope: Scope): unknown {
  if (isInputRef(spec)) return readPath(scope.input, spec.input);
  if (isWireRef(spec)) {
    const [node, ...rest] = spec.wire;
    const nodeResults = scope.wire?.[node] ?? {};
    return readPath(nodeResults, rest);
  }
  if (isLiteral(spec)) return spec.literal;
  if (isValueExpr(spec)) {
    if (spec.op === 'coalesce') {
      for (const a of spec.args) {
        const v = evalValueSpec(a, scope);
        if (v != null) return v;
      }
      return null;
    }
    if (spec.op === 'add') {
      return spec.args.reduce((acc, a) => (acc as number) + (evalValueSpec(a, scope) as number), 0);
    }
  }
  throw new Error(`unknown value-spec: ${JSON.stringify(spec)}`);
}

/** Flatten one param (value-spec OR nested fragment) into sql text + bound params. */
function assembleParam(param: Param, scope: Scope): { sql: string; params: unknown[] } {
  if (isFragment(param)) {
    // Nested fragment: splice its own sql at the `?`, contribute its own params.
    return assembleFragment(param, scope);
  }
  // Value-spec: this `?` is filled by a single bound value.
  return { sql: '?', params: [evalValueSpec(param, scope)] };
}

/**
 * Assemble a single fragment: interleave its literal `sql` (split on `?`) with the
 * rendered text of each param, concatenating params in order.
 */
export function assembleFragment(frag: Fragment, scope: Scope): { sql: string; params: unknown[] } {
  const chunks = frag.sql.split('?');
  if (chunks.length - 1 !== frag.params.length) {
    throw new Error(
      `fragment placeholder/param mismatch: ${chunks.length - 1} '?' vs ${frag.params.length} params in ${JSON.stringify(frag.sql)}`
    );
  }
  let sql = chunks[0];
  const params: unknown[] = [];
  for (let i = 0; i < frag.params.length; i++) {
    const rendered = assembleParam(frag.params[i], scope);
    sql += rendered.sql + chunks[i + 1];
    params.push(...rendered.params);
  }
  return { sql, params };
}

/**
 * Assemble a WHERE group: join present members with `" AND "`, prefix `" WHERE "`.
 * If no member is present, produce the empty string (identical to the ORIGINAL
 * `DBConditions.compile` → `parts.length === 0 ? '' : parts.join(' AND ')`, with the
 * `" WHERE "` prefix owned by the caller/select in the original — here folded in).
 */
export function assembleWhereGroup(group: WhereGroup, scope: Scope): { sql: string; params: unknown[] } {
  const parts: string[] = [];
  const params: unknown[] = [];
  for (const member of group.where) {
    if (isSkipped(member.skip, scope)) continue;
    const r = assembleFragment(member, scope);
    parts.push(r.sql);
    params.push(...r.params);
  }
  if (parts.length === 0) return { sql: '', params: [] };
  return { sql: ` WHERE ${parts.join(' AND ')}`, params };
}

/**
 * Assemble an ordered fragment/WHERE-group list into a single `{ sql, params }`
 * (with `?`). Fragments whose `skip` is satisfied are omitted entirely (sql AND
 * params). A WHERE group joins its present members with the original's text glue.
 */
export function assemble(nodes: Node[], scope: Scope): { sql: string; params: unknown[] } {
  let sql = '';
  const params: unknown[] = [];
  for (const node of nodes) {
    if (isWhereGroup(node)) {
      const r = assembleWhereGroup(node, scope);
      sql += r.sql;
      params.push(...r.params);
      continue;
    }
    if (isSkipped(node.skip, scope)) continue;
    const r = assembleFragment(node, scope);
    sql += r.sql;
    params.push(...r.params);
  }
  return { sql, params };
}

/**
 * PostgreSQL dialect render: `?` → `$1, $2, …`. Byte-identical to the original
 * driver's `convertPlaceholders` (naive left-to-right replace; see
 * `src/drivers/postgres.ts`).
 */
export function toPostgres(sql: string): string {
  let index = 0;
  return sql.replace(/\?/g, () => `$${++index}`);
}
