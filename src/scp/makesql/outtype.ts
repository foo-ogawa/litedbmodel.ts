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

/**
 * The bc {@link PortableType} for a projected column's SQL type, ARRAY-aware (Phase F-2 / #105). A
 * scalar column maps to its {@link sqlTypeToBcScalar} scalar; an ARRAY column (`TEXT[]` / `INT[]` /
 * `NUMERIC[]` / `BOOLEAN[]` / …) maps to a `{ arr: <element scalar> }` list type — the honest output
 * type of a projected array column (the driver hands over a JS array whose elements match the element
 * outType; the read path passes it through, `sqlTypeToMaterializeClass` = passthrough). Without this,
 * a projected array column would throw at `sqlTypeToBcScalar` (a scalar-only mapping) — which is why
 * F1's `TEXT[]` support never surfaced (F1 reads projected no array column). Fail-closed: the element
 * base type is validated by `sqlTypeToBcScalar` (an unknown element throws).
 */
function columnSqlTypeToPortable(sqlType: string, table: string, column: string): PortableType {
  const arrayMatch = /^(.+?)\s*\[\s*\]$/.exec(sqlType.trim());
  if (arrayMatch !== null) {
    const element = toPortableScalar(sqlTypeToBcScalar(arrayMatch[1].trim()), table, column);
    return { arr: element };
  }
  return toPortableScalar(sqlTypeToBcScalar(sqlType), table, column);
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
 * A parsed `select`-list entry (issue #59). Either a SCHEMA-COLUMN reference (bare / qualified /
 * aliased — resolvable to a declared column type + a driver-row output key) OR a COMPUTED expression
 * (a function / aggregate / literal / cast — no underlying schema column). `*` is neither; it throws
 * (the concrete column list is unknown, so nothing can be typed).
 */
export type ProjectionEntry =
  | {
      readonly kind: 'column';
      /** The bare column name (qualifier + alias stripped) — what the type resolver is keyed on. */
      readonly underlying: string;
      /** The property name the driver row carries (the alias, else the bare column). */
      readonly outputKey: string;
      /** The qualifier table if the projection was `qual.col` (a JOIN column), else `undefined` — the
       *  column's type resolves against THIS table, not the node's base table. */
      readonly qualifier?: string;
    }
  | { readonly kind: 'computed'; readonly text: string };

/**
 * The SINGLE projection-column parser (issue #59) — shared by BOTH the codegen `outType` derivation
 * and the TS read-path materializer derivation so the two CANNOT diverge (the recurring source of
 * silent-rounding leaks). Classifies ONE `select`-list entry:
 *   - bare `col`                        → `{ kind:'column', underlying:'col', outputKey:'col' }`.
 *   - qualified `t.col`                 → strip qualifier → `underlying:'col', outputKey:'col'`.
 *   - aliased `col AS b` / `t.col AS b` → `underlying:'col', outputKey:'b'` (the driver row key).
 *   - computed `COUNT(*)`, `NOW()`, `x::uuid`, `x+1`, aliased or not → `{ kind:'computed', text }`.
 *   - `*` / `t.*`                       → HARD ERROR (the concrete column list is unknown).
 * A SCHEMA-column entry MUST be typed against the declaration (a caller resolves + fail-closes on an
 * undeclared column → never a silent rounded-i64 leak). A COMPUTED entry has no schema column to
 * round: the read path leaves it raw; the codegen path (which needs every struct field typed) rejects
 * it. This ONE parser guarantees every shape is classified identically for both paths.
 */
export function parseProjectionColumn(col: string, table: string, at: string): ProjectionEntry {
  const raw = col.trim();
  if (raw === '*' || raw.endsWith('.*')) {
    throw new Error(
      `outtype: ${at}: SELECT on '${table}' projects '${raw}' (a wildcard). A typed de-box needs the ` +
        `concrete column list to build the row struct + type each column — spec §4.1 is column-typed. ` +
        `Project explicit columns (no-assume, no-fallback).`,
    );
  }
  // Split off an `AS <alias>` (case-insensitive; alias must be a bare identifier).
  let expr = raw;
  let alias: string | undefined;
  const asMatch = /^(.*?)\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)$/i.exec(raw);
  if (asMatch !== null) {
    expr = asMatch[1].trim();
    alias = asMatch[2];
  }
  // A bare or qualified column reference — `col` or `qual.col` (identifiers only) — is a SCHEMA
  // column. The qualifier (if present) names the column's OWNER table (a JOIN column), against which
  // its type resolves; without a qualifier the column belongs to the node's base table.
  const refMatch = /^(?:([A-Za-z_][A-Za-z0-9_]*)\.)?([A-Za-z_][A-Za-z0-9_]*)$/.exec(expr);
  if (refMatch !== null) {
    const qualifier = refMatch[1];
    const underlying = refMatch[2];
    return { kind: 'column', underlying, outputKey: alias ?? underlying, ...(qualifier !== undefined ? { qualifier } : {}) };
  }
  // Anything else (a function call `f(x)`, an aggregate `COUNT(*)`, a cast `x::uuid`, arithmetic, a
  // literal) is a COMPUTED expression — no underlying schema column, so it cannot round an i64. The
  // read path leaves it raw; the codegen path rejects it (it needs a concrete field type).
  return { kind: 'computed', text: raw };
}

/**
 * The row `obj` type of a SELECT projection: `{obj:{outputKey: <bcScalar>, …}}`. Each column is
 * resolved ONCE via the SHARED {@link parseProjectionColumn} (bare / qualified / aliased) and its SQL
 * type (from the schema SoT) yields BOTH the bc scalar (this outType, for codegen) AND — written into
 * the `materializers` accumulator — the TS read-path {@link MaterializeClass} (INT32/INT64/date/bool).
 * This is the SINGLE column-type resolution the whole read pipeline consumes: codegen reads the
 * `obj`; the runtime read path reads `materializers`. They cannot diverge — they are two projections
 * of the ONE resolution. A `*` / absent projection, a computed column, a duplicate OUTPUT key, or an
 * undeclared underlying column is a HARD ERROR (no-assume, no-fallback) — for BOTH consumers.
 */
function rowObjType(
  table: string,
  projection: readonly string[] | undefined,
  resolveColumnType: ColumnTypeResolver,
  at: string,
  materializers: Record<string, MaterializeClass>,
): PortableType {
  if (projection === undefined || projection.length === 0) {
    throw new Error(
      `outtype: ${at}: SELECT on '${table}' has no explicit column projection ('*' / empty). A typed ` +
        `de-box needs the concrete column list to build the row struct — spec §4.1 is column-typed. ` +
        `Project explicit columns (no-assume, no-fallback).`,
    );
  }
  const obj: Record<string, PortableType> = {};
  for (const col of projection) {
    const entry = parseProjectionColumn(col, table, at);
    // A computed expression has no concrete struct field type — HARD ERROR (spec §4.1 no-assume).
    // The read path never reaches a separate lenient pass now; `*`/computed fail here for BOTH.
    if (entry.kind === 'computed') {
      throw new Error(
        `outtype: ${at}: projected expression '${entry.text}' on '${table}' is computed (function / ` +
          `aggregate / cast / literal) and cannot be typed into a concrete row struct field. Typed ` +
          `de-box (spec §4.1) requires bare/qualified/aliased schema columns. No-assume, no-fallback.`,
      );
    }
    const { underlying, outputKey, qualifier } = entry;
    if (outputKey in obj) throw new Error(`outtype: ${at}: duplicate projected column key '${outputKey}' on '${table}'`);
    // Resolve against the qualifier's table (a JOIN column) if present, else the node's base table.
    const owner = qualifier ?? table;
    // ONE resolution of the SQL type → both the bc scalar AND the materialize class. resolveColumnType
    // throws on an undeclared column, sqlType* throw on an unknown SQL type — fail-closed by
    // construction on the UNDERLYING column, keyed by the OUTPUT column (the driver's row key).
    const sqlType = resolveColumnType(owner, underlying);
    obj[outputKey] = columnSqlTypeToPortable(sqlType, owner, underlying); // array-aware (F2 #105)
    const klass = sqlTypeToMaterializeClass(sqlType);
    if (klass !== 'passthrough') materializers[outputKey] = klass; // omit passthrough (no-op coercion)
  }
  return { obj };
}

/** A Select/Count node's outType + (for Select) the per-output-column read-path materializer map. */
interface NodeTypes {
  readonly outType: PortableType;
  /** `outputKey → MaterializeClass` (non-passthrough only). Empty for a Count / passthrough-only row. */
  readonly materializers: Record<string, MaterializeClass>;
}

/**
 * The outType + read-path materializers of ONE read body node — derived from the SINGLE column-type
 * resolution ({@link rowObjType}). A `Select` returns a ROW LIST → `{arr:rowObj}`; a `Count` a single
 * `int` (a scalar, no per-column map). A `.map` node wraps its per-element Select result. An
 * unrecognized component throws (typed read de-box covers only the SQL read catalog).
 */
function nodeTypes(node: RefLike, resolveColumnType: ColumnTypeResolver): NodeTypes {
  const { component, ports } = nodeRef(node);
  const at = `node '${node.id}'`;
  if (component === 'Count') return { outType: 'int', materializers: {} };
  if (component === 'Select' || component === 'RelationBatch') {
    const table = stringPort(ports, 'table');
    if (table === undefined) throw new Error(`outtype: ${at}: Select node requires a literal 'table' port`);
    const projection = stringArrayPort(ports, 'select');
    const materializers: Record<string, MaterializeClass> = {};
    const row = rowObjType(table, projection, resolveColumnType, at, materializers);
    // A `.map` node iterates a list and invokes the inner component per element (row LIST per element
    // → `{arr:{arr:rowObj}}`); a plain Select node returns the row list `{arr:rowObj}`.
    const outType: PortableType = isMap(node) ? { arr: { arr: row } } : { arr: row };
    return { outType, materializers };
  }
  // A WRITE node (Insert/Update/Delete). Read and write are ONE flow — both make SQL and execute it,
  // and both hand back ROWS (`executeStaticWrite` returns the RETURNING rows, else the single
  // `[{changes, lastInsertRowid}]` summary row) — so a write node is typed here rather than being a
  // separate path. The type comes from {@link crudNodeAsType}, the SAME derivation the authoring
  // layer's `.as` stamp uses (single SoT: the two can never diverge). A write with no RETURNING types
  // as the determined empty-row list `{arr:{obj:{}}}`; its executed summary-row shape is supplied by
  // the codegen lowering, which is where the row struct must match what the exec seam returns.
  if (component === 'Insert' || component === 'Update' || component === 'Delete') {
    const materializers: Record<string, MaterializeClass> = {};
    const outType = crudNodeAsType(component, ports, resolveColumnType, at, materializers);
    return { outType, materializers };
  }
  throw new Error(
    `outtype: ${at}: component '${component}' has no typed outType (typed de-box covers Select/Count + ` +
      `Insert/Update/Delete). An unknown node cannot be de-boxed here — no-assume, no-fallback.`,
  );
}

/** The outType of ONE read body node (thin wrapper over {@link nodeTypes} for external callers). */
export function nodeOutType(node: RefLike, resolveColumnType: ColumnTypeResolver): PortableType {
  return nodeTypes(node, resolveColumnType).outType;
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
 * PRE-COMPILE node-output type (bc 0.8.0 SA5 / #12): the determined `PortableType` a litedbmodel CRUD
 * leaf annotates its authored node with, via bc's `.as(t)` recording API, so `compileBehaviors`'
 * all-nodes-typed gate (`UNTYPED_NODE`) passes. Computed from the RAW authored ports (a plain `table`
 * string + a plain `select` / `returning` column list — the same projection SoT `deriveReadOutTypes`
 * reads post-compile, just moved to authoring time) + the model's inline `static columns` resolver.
 *
 *  - `Select` → the row LIST `{arr:{obj:{outKey:<scalar>,…}}}` from the `select` projection. This is
 *    the value `.as` takes in BOTH positions: a top-level Select node's `outType` is this list; a
 *    mapped Select's `.as` records it as the map ELEMENT type (bc convention — the per-parent row list).
 *  - `Count` → NOT handled here: its output is the static `int` catalog `elemType` (no `.as`).
 *  - `Insert`/`Update`/`Delete` → the RETURNING row list `{arr:{obj:…}}` from the `returning` port
 *    (bare-column list). No `returning` → an EMPTY row list `{arr:{obj:{}}}` (the write surfaces rows
 *    only via RETURNING; a no-RETURNING write yields empty rows — mirrors `writeouttype.ts`).
 *
 * Schema columns are parsed by the SAME {@link parseProjectionColumn} the read/codegen paths use and
 * typed via the resolver (an undeclared column fails closed here at authoring — earlier than the
 * post-compile derivation). A COMPUTED projected column (`COUNT(*) as n`, a cast, a literal) is OMITTED
 * from the typed row struct — the read path leaves a computed column RAW (#59: "the read path leaves it
 * raw; the codegen path rejects it"), so its field is absent from the de-box row type. This keeps the
 * node's outType DETERMINED (gate passes) while preserving #59's raw-computed read behavior; the typed
 * codegen path independently rejects a computed projection later (it needs every field typed). The
 * value is the OUTPUT-row type only; SQL stays opaque text on the makeSQL leaf (types the node OUTPUT,
 * never the SQL).
 */
export function crudNodeAsType(
  component: string,
  ports: Record<string, unknown>,
  resolveColumnType: ColumnTypeResolver,
  at: string,
  materializers: Record<string, MaterializeClass> = {},
): PortableType {
  const table = ports['table'];
  if (typeof table !== 'string') {
    throw new Error(`outtype: ${at}: ${component} node requires a literal 'table' string port to type its output (got ${JSON.stringify(table)})`);
  }
  if (component === 'Select' || component === 'RelationBatch') {
    const select = ports['select'];
    if (!Array.isArray(select) || select.length === 0) {
      throw new Error(
        `outtype: ${at}: Select has no explicit column projection ('*' / empty). A typed de-box needs the ` +
          `concrete column list to build the row struct — spec §4.1 is column-typed. Project explicit columns.`,
      );
    }
    const obj: Record<string, PortableType> = {};
    for (const raw of select) {
      if (typeof raw !== 'string') throw new Error(`outtype: ${at}: Select 'select' entries must be literal strings (got ${JSON.stringify(raw)})`);
      const parsed = parseProjectionColumn(raw, table, at);
      if (parsed.kind === 'computed') continue; // computed column: read raw, omitted from the typed row (SA5-determined without it)
      const { underlying, outputKey, qualifier } = parsed;
      if (outputKey in obj) throw new Error(`outtype: ${at}: duplicate projected column key '${outputKey}' on '${table}'`);
      obj[outputKey] = columnSqlTypeToPortable(resolveColumnType(qualifier ?? table, underlying), qualifier ?? table, underlying);
    }
    return { arr: { obj } };
  }
  if (component === 'Insert' || component === 'Update' || component === 'Delete') {
    // The RETURNING port is a single comma-separated bare-column string (`'id, author_id'`) or absent.
    const returning = ports['returning'];
    if (returning === undefined) return { arr: { obj: {} } };
    if (typeof returning !== 'string') {
      throw new Error(`outtype: ${at}: ${component} 'returning' port must be a literal string (got ${JSON.stringify(returning)})`);
    }
    const cols = returning.split(',').map((c) => c.trim()).filter((c) => c.length > 0);
    if (cols.length === 0) return { arr: { obj: {} } };
    return { arr: rowObjType(table, cols, resolveColumnType, at, materializers) };
  }
  throw new Error(`outtype: ${at}: no pre-compile output type for component '${component}' (typed CRUD covers Select/Insert/Update/Delete; Count is static via catalog elemType).`);
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
): { byNode: Map<string, PortableType>; outputType: PortableType; materializersByNode: Map<string, Record<string, MaterializeClass>> } {
  const byNode = new Map<string, PortableType>();
  const materializersByNode = new Map<string, Record<string, MaterializeClass>>();
  for (const n of component.body) {
    if ('cond' in n) continue; // a cond node carries no SELECT projection; it is typed by its branch
    // ONE resolution per node → the outType (codegen) AND the read-path materializers (runtime).
    const t = nodeTypes(n as RefLike, resolveColumnType);
    byNode.set(n.id, t.outType);
    if (Object.keys(t.materializers).length > 0) materializersByNode.set(n.id, t.materializers);
  }
  return { byNode, outputType: outputType(component.output, byNode, `component '${component.name}' output`), materializersByNode };
}
