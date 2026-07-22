/**
 * litedbmodel v2 SCP — read-row de-box type derivation (spec §4.1; issues #58/#59).
 *
 * The read-column coverage + de-box SSoT for the op-independent leaf path. It resolves a read's
 * projection ONCE into two projections of the SAME column-type resolution:
 *
 *   - {@link deriveReadRow} — the read ROW `{arr:{obj:{outputKey: <read-de-boxed scalar>}}}` (the
 *     bc `outType` bc's native emitter reads for the typed de-box, #154) PLUS the TS-leaf
 *     `materializers` coercion map. Array columns read as `{arr:<element>}` (the SSoT array split
 *     {@link import('../coltype').arrayElementType}); the element rides the SAME de-box scalar.
 *   - {@link outputType} — composes the component `outputType` by walking the `output` Φ-expression
 *     against a `nodeId → outType` map (consumed by `authoring.lowerReadColumns`).
 *
 * Spec §4.1 discipline: an ambiguous/unmappable projection is a HARD ERROR (no-assume, no-fallback)
 * — a `*` projection, a computed column (typed reads only), or an undeclared/unknown-type column
 * THROWS via {@link parseProjectionColumn} + the coltype SSoT; never a boxed/untyped output.
 */

import type { PortableType } from 'behavior-contracts';
import { sqlTypeToMaterializeClass, keyArrayElemScalar, arrayElementType, sqlTypeIsNotNull, type MaterializeClass, type ColumnTypeResolver } from '../coltype';
import { IN_SENTINEL } from './tx';


function opKey(node: unknown): string | undefined {
  if (node === null || typeof node !== 'object' || Array.isArray(node)) return undefined;
  const keys = Object.keys(node as object);
  return keys.length === 1 ? keys[0] : undefined;
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


/** The read row TYPE (SSoT) + its TS-leaf coercion map — two projections of ONE column resolution. */
export interface ReadRow {
  /** The read row `{arr:{obj:{outputKey: readScalar}}}` — the SINGLE row-type representation (#141). */
  readonly outType: PortableType;
  /** `outputKey → MaterializeClass` (non-passthrough only) — the TS-leaf coercion derived from the SAME row. */
  readonly materializers: Record<string, MaterializeClass>;
}

/**
 * Derive the read ROW from a projection (#141 SSoT + #59 op-builder guard). Resolves each projected
 * column ONCE via the SHARED {@link parseProjectionColumn} + the coltype SSoT, producing BOTH:
 *   - `outType` — `{arr:{obj:{outputKey: keyArrayElemScalar(sqlType)}}}`, the READ-DE-BOXED scalar
 *     (int32→`float`(number) / BIGINT→`string` / BOOLEAN→`bool` / DATE→`string` / text/uuid→`string`).
 *     This is the ONE row-type representation: bc `generateModule` reads it for the NATIVE typed de-box
 *     (#154), and it is the conform target the TS leaf coercion below produces.
 *   - `materializers` — the TS-leaf coercion map ({@link sqlTypeToMaterializeClass}); the leaf coerces
 *     each driver cell (BIGINT→exact string / DATE→string / BOOLEAN→boolean) so the row MATCHES `outType`
 *     before bc conforms it (bc conform VALIDATES, does not coerce). Derived from the SAME resolution as
 *     `outType`, so the coerced JS form always equals the declared read scalar — no second row type.
 * This IS the #59 read-column coverage guard: `*`/`t.*` throws (unknown column list), an undeclared
 * column or unknown SQL type throws. A COMPUTED projection (`COUNT(*)`, cast, literal) is left RAW
 * (omitted from BOTH) — #59: "the read path leaves it raw".
 */
export function deriveReadRow(
  table: string,
  projection: readonly string[],
  resolveColumnType: ColumnTypeResolver,
  at: string,
): ReadRow {
  const obj: Record<string, PortableType> = {};
  const materializers: Record<string, MaterializeClass> = {};
  for (const col of projection) {
    const entry = parseProjectionColumn(col, table, at); // throws on `*` / `t.*`
    if (entry.kind === 'computed') continue; // computed column: read raw (no de-box / no typed field), #59
    const { underlying, outputKey, qualifier } = entry;
    const owner = qualifier ?? table;
    const sqlType = resolveColumnType(owner, underlying); // undeclared → throw
    // A read cell is nullable UNLESS `static columns` declares the column `NOT NULL` — a driver column
    // may be NULL, so the read scalar rides under `opt` (bc conforms `null`/absent OR the de-boxed
    // scalar). A `NOT NULL` column is non-null by SQL contract → the raw scalar (no `opt`), so a
    // RETURNING primary key can be consumed in a value position (a `.map` param) without an
    // optional-narrowing coalesce (spec §4.1 nullability — the SSoT is the declared column constraint).
    // Array-aware (F2 #105): a projected ARRAY column reads as a `{arr:<de-boxed element>}` list (the
    // driver hands over a JS array; the whole value is passthrough — see `sqlTypeToMaterializeClass`),
    // a scalar column as its read-de-boxed scalar. SSoT array split ({@link arrayElementType}); the
    // element rides the SAME `keyArrayElemScalar` de-box as a scalar column (throws on unknown type).
    const element = arrayElementType(sqlType);
    const scalar: PortableType = element !== null ? { arr: keyArrayElemScalar(element) } : keyArrayElemScalar(sqlType);
    obj[outputKey] = sqlTypeIsNotNull(sqlType) ? scalar : { opt: scalar };
    const klass = sqlTypeToMaterializeClass(sqlType);
    if (klass !== 'passthrough') materializers[outputKey] = klass; // omit passthrough (no-op coercion)
  }
  return { outType: { arr: { obj } }, materializers };
}

/**
 * Derive the component `outputType` by walking the `output` Φ-expression against a `nodeId →
 * outType` map. `{ref:[id]}` resolves to that node's outType; `{obj:{k:expr,…}}` /
 * `{arr:expr}` / `{opt:expr}` recurse structurally. A `ref` to an unknown id, or an operator the
 * typed output path does not cover, is a HARD ERROR (no-assume, no-fallback) — the output must be
 * a pure composition of typed node results, never a boxed dynamic tree.
 */
export function outputType(output: unknown, byNode: Map<string, PortableType>, at: string): PortableType {
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
