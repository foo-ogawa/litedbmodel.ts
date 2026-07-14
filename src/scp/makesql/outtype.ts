/**
 * litedbmodel v2 SCP — codegen `outType` derivation (spec §4.1; issues #58/#56).
 *
 * bc's TYPED-RAW de-box emitters (`rust-typed-raw` / `go-typed-raw` / `typescript-typed`) require
 * each read node to carry an `outType` and the component an `outputType` (bc portable type
 * notation). WITHOUT them the emitter hard-fails ("nothing to de-box") — the correct fail-closed
 * behavior. This module derives those annotations for a READ component from:
 *
 *   1. the per-node SELECT projection (`table` + `select` ports) → a row `obj` type, each column's
 *      bc scalar resolved from its SQL type (the schema/DDL SoT) via {@link sqlTypeToBcScalar};
 *   2. the node cardinality (a Select node returns a row LIST → `{arr:rowObj}`; a Count → `int`; a
 *      `.map` node wraps its per-element handler result → `{arr:<elem>}`);
 *   3. the component `output` Φ-expression, walked to compose the `outputType` from the node types
 *      (`{ref:[nodeId]}` → that node's outType; `{obj:{…}}`/`{arr:…}`/`{opt:…}` recurse).
 *
 * Spec §4.1 discipline: an ambiguous/unmappable projection is a HARD ERROR (no-assume,
 * no-fallback) — a `*` projection, a computed/aliased column, or a column with no schema type
 * THROWS. It is NEVER degraded to a boxed/untyped output; that would silently defeat the de-box.
 */

import type { PortableType, PortableScalarType } from 'behavior-contracts';
import type { Component, ComponentRefNode, MapNode } from '../authoring';
import { sqlTypeToBcScalar, sqlTypeToMaterializeClass, type BcScalar, type MaterializeClass, type ColumnTypeResolver } from '../coltype';
import { IN_SENTINEL } from './tx';

/**
 * Narrow a {@link BcScalar} to a bc {@link PortableScalarType}. `BcScalar`'s members
 * (`string|int|float|bool|null`) are exactly bc 0.3.0's portable scalar notation, so this is an
 * identity narrow. DATE/TIMESTAMP columns map to `string` at the `sqlTypeToBcScalar` layer (spec
 * §4.1 owner decision; bc has no `date` scalar — behavior-contracts#84 deferred), so no `date`
 * scalar ever reaches here.
 */
function toPortableScalar(scalar: BcScalar, _table: string, _column: string): PortableScalarType {
  return scalar;
}

/** A body node that is a component ref or a map (a cond node has no SELECT projection). */
type RefLike = ComponentRefNode | MapNode;

function isMap(n: Component['body'][number]): n is MapNode {
  return 'map' in n;
}

/** The `{component, ports}` of a body node (unwrapping a `.map`). */
function nodeRef(n: RefLike): { component: string; ports: Record<string, unknown> } {
  return isMap(n) ? { component: n.map.component, ports: n.map.ports } : { component: n.component, ports: n.ports };
}

function opKey(node: unknown): string | undefined {
  if (node === null || typeof node !== 'object' || Array.isArray(node)) return undefined;
  const keys = Object.keys(node as object);
  return keys.length === 1 ? keys[0] : undefined;
}

/** Read a required literal string port, or throw (the projection SoT must be a literal in the IR). */
function stringPort(ports: Record<string, unknown>, name: string): string | undefined {
  const v = ports[name];
  if (v === undefined) return undefined;
  if (typeof v !== 'string') {
    throw new Error(`outtype: port '${name}' must be a literal string in the IR (got ${JSON.stringify(v)})`);
  }
  return v;
}

/** Read a literal `{arr:[str,…]}` string-list port (the SELECT projection). */
function stringArrayPort(ports: Record<string, unknown>, name: string): string[] | undefined {
  const v = ports[name];
  if (v === undefined) return undefined;
  if (typeof v === 'object' && v !== null && 'arr' in v && Array.isArray((v as { arr: unknown }).arr)) {
    return (v as { arr: unknown[] }).arr.map((e) => {
      if (typeof e !== 'string') throw new Error(`outtype: '${name}' entries must be literal strings (got ${JSON.stringify(e)})`);
      return e;
    });
  }
  throw new Error(`outtype: port '${name}' must be an {arr:[…]} literal in the IR (got ${JSON.stringify(v)})`);
}

/**
 * The row `obj` type of a SELECT projection: `{obj:{col: <bcScalar>, …}}`. Each column is typed by
 * resolving its SQL type from the schema SoT and mapping via {@link sqlTypeToBcScalar}. A `*` or
 * absent projection, a computed/aliased/qualified column, or a duplicate column is a HARD ERROR —
 * such a projection cannot be de-boxed into a concrete struct (no-assume, no-fallback).
 */
function rowObjType(table: string, projection: readonly string[] | undefined, resolveColumnType: ColumnTypeResolver, at: string): PortableType {
  if (projection === undefined || projection.length === 0) {
    throw new Error(
      `outtype: ${at}: SELECT on '${table}' has no explicit column projection ('*' / empty). A typed ` +
        `de-box needs the concrete column list to build the row struct — spec §4.1 is column-typed. ` +
        `Project explicit columns (no-assume, no-fallback).`,
    );
  }
  const obj: Record<string, PortableType> = {};
  for (const col of projection) {
    // Reject anything that is not a bare column name — an alias (`x AS y`), a qualified name
    // (`t.c`), or a computed expression cannot be typed from the schema alone.
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(col)) {
      throw new Error(
        `outtype: ${at}: projected column '${col}' on '${table}' is not a bare column name (alias / ` +
          `qualified / computed). Typed de-box (spec §4.1) requires bare schema columns so each maps ` +
          `to a SQL type. No-assume, no-fallback.`,
      );
    }
    if (col in obj) throw new Error(`outtype: ${at}: duplicate projected column '${col}' on '${table}'`);
    // sqlTypeToBcScalar throws on an unknown/ambiguous SQL type — fail-closed by construction.
    obj[col] = toPortableScalar(sqlTypeToBcScalar(resolveColumnType(table, col)), table, col);
  }
  return { obj };
}

/**
 * The `outType` of ONE read body node. A `Select` returns a ROW LIST → `{arr:rowObj}`. A `Count`
 * returns a single `int`. A `.map` node runs its inner component PER element of the iterated list,
 * so its result is `{arr:<per-element outType>}` (the per-element type being the inner node's own
 * outType — for a `.map` over a Select-per-element that is `{arr:{arr:rowObj}}`). An unrecognized
 * component throws (typed codegen covers only the SQL read catalog).
 */
export function nodeOutType(node: RefLike, resolveColumnType: ColumnTypeResolver): PortableType {
  const { component, ports } = nodeRef(node);
  const at = `node '${node.id}'`;
  if (component === 'Count') return 'int';
  if (component === 'Select') {
    const table = stringPort(ports, 'table');
    if (table === undefined) throw new Error(`outtype: ${at}: Select node requires a literal 'table' port`);
    const projection = stringArrayPort(ports, 'select');
    const row = rowObjType(table, projection, resolveColumnType, at);
    // A `.map` node iterates a list and invokes the inner component per element; the node result is
    // the list of per-element results. Here the inner is a Select → per element a row LIST, so the
    // map node's outType is `{arr:{arr:rowObj}}`. A non-map Select node returns the row list `{arr:rowObj}`.
    if (isMap(node)) return { arr: { arr: row } };
    return { arr: row };
  }
  throw new Error(
    `outtype: ${at}: component '${component}' has no typed outType (typed read codegen covers Select/Count only). ` +
      `A write / unknown node cannot be de-boxed here — no-assume, no-fallback.`,
  );
}

/**
 * Derive the component `outputType` by walking the `output` Φ-expression against a `nodeId →
 * outType` map. `{ref:[id]}` resolves to that node's outType; `{obj:{k:expr,…}}` /
 * `{arr:expr}` / `{opt:expr}` recurse structurally. A `ref` to an unknown id, or an operator the
 * typed output path does not cover, is a HARD ERROR (no-assume, no-fallback) — the output must be
 * a pure composition of typed node results, never a boxed dynamic tree.
 */
function outputType(output: unknown, byNode: Map<string, PortableType>, at: string): PortableType {
  const op = opKey(output);
  if (op === 'ref' || op === 'refOpt') {
    const path = (output as Record<string, unknown[]>)[op];
    if (!Array.isArray(path) || path.length === 0 || typeof path[0] !== 'string') {
      throw new Error(`outtype: ${at}: a '${op}' output must reference a node id`);
    }
    // A node-result ref is a single-segment path `[nodeId]`. A deeper path (field access on a node
    // result) cannot be typed here without walking into the row struct — out of the bench's scope.
    if (path.length !== 1 || (path[0] as string).startsWith('$') || path[0] === IN_SENTINEL) {
      throw new Error(
        `outtype: ${at}: output ref path ${JSON.stringify(path)} is not a bare node-result reference; ` +
          `field access / input refs at the output position are not supported by the typed de-box path. No-assume, no-fallback.`,
      );
    }
    const t = byNode.get(path[0] as string);
    if (t === undefined) throw new Error(`outtype: ${at}: output references node '${path[0]}' which has no derived outType`);
    return t;
  }
  if (op === 'obj') {
    const obj = (output as { obj: unknown }).obj;
    if (obj === null || typeof obj !== 'object' || Array.isArray(obj)) throw new Error(`outtype: ${at}: 'obj' output must be a record`);
    const out: Record<string, PortableType> = {};
    for (const [k, expr] of Object.entries(obj as Record<string, unknown>)) {
      if (k === '__proto__') throw new Error(`outtype: ${at}: '__proto__' key forbidden in output obj`);
      out[k] = outputType(expr, byNode, `${at}.obj.${k}`);
    }
    return { obj: out };
  }
  if (op === 'arr') {
    const arr = (output as { arr: unknown }).arr;
    // An `{arr:<expr>}` output whose single element expr types the element (the typed corpus uses
    // homogeneous arrays). An `{arr:[…]}` literal list is not a typed homogeneous array here.
    if (Array.isArray(arr)) throw new Error(`outtype: ${at}: an '{arr:[…]}' literal list output cannot be typed as a homogeneous array. No-assume, no-fallback.`);
    return { arr: outputType(arr, byNode, `${at}.arr`) };
  }
  if (op === 'opt') {
    const inner = (output as { opt: unknown }).opt;
    return { opt: outputType(inner, byNode, `${at}.opt`) };
  }
  throw new Error(
    `outtype: ${at}: output expression ${JSON.stringify(output)} is not a typed composition ` +
      `(ref / obj / arr / opt of node results). The typed de-box path does not build a dynamic ` +
      `Value output tree — no-assume, no-fallback.`,
  );
}

/**
 * Per-node `outType` (keyed by body node id) + the component `outputType`, derived for a READ
 * component (spec §4.1). `cond` nodes are skipped for the per-node map but a `cond` at the OUTPUT
 * position (a shared-branch merge) would be handled by {@link outputType} if reached — the bench's
 * read outputs are ref / obj compositions of Select nodes. Any ambiguity throws (fail-closed).
 */
export function deriveReadOutTypes(
  component: Component,
  resolveColumnType: ColumnTypeResolver,
): { byNode: Map<string, PortableType>; outputType: PortableType } {
  const byNode = new Map<string, PortableType>();
  for (const n of component.body) {
    if ('cond' in n) continue; // a cond node carries no SELECT projection; it is typed by its branch
    byNode.set(n.id, nodeOutType(n as RefLike, resolveColumnType));
  }
  return { byNode, outputType: outputType(component.output, byNode, `component '${component.name}' output`) };
}

/**
 * Per-node TS read-path MATERIALIZER map (issue #59, owner-approved type-honoring de-box): for each
 * read node, `column → MaterializeClass` (int32/int64/date/bool/passthrough), derived from the SAME
 * projection + column-type SoT `deriveReadOutTypes` uses. The read handler applies these to each raw
 * driver row so a BIGINT column returns `bigint` (exact), an INT column stays `number`, a DATE column
 * returns a TZ-attached string, a BOOLEAN returns a JS boolean — consistently across sqlite/pg/mysql.
 *
 * Only Select nodes have a projection; a Count node returns a single scalar `int` (never > i64 in
 * practice, and always a 32-bit-safe COUNT) so it needs no per-column map (its outType `int` stays a
 * JS number). A node whose projection cannot be typed throws here exactly as the outType derivation
 * does (fail-closed) — so an un-typeable read never silently skips materialization.
 */
export function deriveReadMaterializers(
  component: Component,
  resolveColumnType: ColumnTypeResolver,
): Map<string, Record<string, MaterializeClass>> {
  const byNode = new Map<string, Record<string, MaterializeClass>>();
  for (const n of component.body) {
    if ('cond' in n) continue;
    const { component: comp, ports } = nodeRef(n as RefLike);
    if (comp !== 'Select') continue; // Count → scalar int (JS number); no per-column materialization
    const table = stringPort(ports, 'table');
    if (table === undefined) throw new Error(`materializers: node '${n.id}': Select requires a literal 'table' port`);
    const projection = stringArrayPort(ports, 'select');
    if (projection === undefined || projection.length === 0) {
      throw new Error(`materializers: node '${n.id}': Select on '${table}' has no explicit projection — cannot type each column for materialization (no-assume, no-fallback)`);
    }
    const cols: Record<string, MaterializeClass> = {};
    for (const col of projection) {
      // sqlTypeToMaterializeClass throws on unknown — fail-closed, mirroring the outType derivation.
      cols[col] = sqlTypeToMaterializeClass(resolveColumnType(table, col));
    }
    byNode.set(n.id, cols);
  }
  return byNode;
}
