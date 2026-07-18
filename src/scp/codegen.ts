/**
 * litedbmodel v2 SCP — mode-3 codegen (spec §9 exec-mode 3 / §10 / §11; makeSQL flip, epic #43/#45).
 *
 * The static-codegen execution mode ("Codegen・静的（全言語）— IR → 各言語ソース生成 runtime≈0" —
 * spec §9). It takes a §8 STATIC makeSQL {@link SqlBundle} and, per target language, emits:
 *
 *  1. **Behavior module** — bc's SHARED generator run over the ONE SQL-baking lowering
 *     ({@link lowerReadGraphForNativeSql}): each read/write node's rendered per-dialect SQL is baked
 *     into the module as a native string LITERAL (`f_sql`) with typed param ports (scalar / optional /
 *     coalesce-default / IN-list array / skip-fragment / map element-field). The module carries its
 *     own query — it needs NO runtime JSON read. Covered by bc's typed-NATIVE endpoint
 *     (`rust-typed-native` / `go-typed-native`) for go/rust and `typescript-typed` for ts; every
 *     language consumes the SAME baked-SQL ports. There is ONE lowering and NO opt-in flag.
 *  2. **Runtime-stitch sidecar** — the pure-JSON `relations` batch ops (a belongsTo/hasMany prefetch
 *     is ONE batched IN query the runtime stitches, not baked into the read module) + the
 *     write-Command `transaction` plan. The read/write PRIMARY SQL is NOT here (baked in the module);
 *     the JSON `SqlCatalogCompanion` is RETIRED for reads.
 *
 * ## Codegen-path only; behavior-identical to the thin-runtime (mode-2), by construction
 *
 * The lowering builds a NEW, separate `ComponentGraphIR` from the bundle's `readGraph`; it never
 * mutates `SqlBundle`/`ReadGraph` and never touches the shared `executeReadGraph` (or the frozen
 * conformance corpus). The baked SQL is rendered through the SAME `composeMakeSQL`/`renderPlaceholders`
 * assembly the mode-2 runtime uses, so the module's query text is byte-identical to what
 * {@link executeBundle} renders. The TS leg proves result-equality vs {@link executeBundle}.
 *
 * ## Fail-closed, no boxed fallback
 *
 * A shape the lowering cannot bake natively (a `cond` node, a SKIP-guarded `map` child) THROWS
 * {@link TypedNativeCoverageError}, naming the exact gap — never a silent boxed escape.
 */

import { generateModule, loadCompiledIR, type GeneratedModule, type Scope, type Value, type PortSchema } from 'behavior-contracts';
// bc 0.8.0: litedbmodel's codegen IR is DERIVED (lowered from a compiled read graph) — an UNBRANDED
// structural doc. It is re-adopted into the branded compile-seam handle via `loadCompiledIR` right
// before `generateModule` (which fail-closes on un-tokened IR, SA3/SA7). The node/component types
// here are the unbranded structural shapes (see `./authoring`).
import type { Component, ComponentGraphIR } from './authoring';
import type { SqlBundle, SqliteDb } from './runtime';
import { executeBundle } from './runtime';
import type { DialectName } from './dialect';
import type { RelationOp } from './relation';
import type { ReadGraph, StaticStatement } from './makesql/static-bundle';
import type { TransactionPlan } from './makesql/tx';
import { IN_SENTINEL } from './makesql/tx';
import { composeMakeSQL, type MakeSQL, type SqlParam } from './makesql/makesql';
import { renderPlaceholders } from './makesql/handler';
import { sqlTypeToBcScalar, type BcScalar, type ColumnTypeResolver } from './coltype';

/**
 * The languages litedbmodel mode-3 codegen supports — exactly the set bc's shared generator
 * registers (bc#13 SP1 typescript/python + SP2 go/rust/php). {@link codegenEmitterFor}
 * verifies against the LIVE registry so a bc capability drift is caught loudly.
 */
export const CODEGEN_LANGUAGES = ['typescript', 'python', 'go', 'rust', 'php'] as const;
export type CodegenLanguage = (typeof CODEGEN_LANGUAGES)[number];

/**
 * The bc READ emitter litedbmodel drives for each codegen target (#60 milestone 1): the
 * **de-interpreted, RUNTIME-FREE typed-NATIVE** endpoint (bc#77/#90) for go/rust — ZERO boxed
 * `Value`/`RawValue`, ZERO IR baked into the module, concrete `HandlerNR`/`InNR`/`PortsNR`/
 * `RawRowNR` structs. There is **NO literal / straight-line / boxed-typed fallback** for a read: an
 * unspec'd fallback is INVALID (it silently swallows the "this shape can't be natively codegen'd"
 * signal) — a read shape typed-native does not cover MUST fail loudly at generation, naming the gap
 * (report it as a bc#86 coverage gap), never silently regenerate on `-typed`/`-typed-raw`.
 *
 * `-typed-raw` (bc#76, the boxed-wire intermediate) and `-typed` (the boxed-Value intermediate) are
 * RETIRED from litedbmodel's codegen surface entirely (#60 milestone 1) — typed-native supersedes
 * both for every read shape it covers (bc#77/#90: single-componentRef reads AND the `map`
 * relationKind:single|connection shape, bc#86 part 2).
 *
 * - ts: **no typed-native endpoint registered yet** (bc has not shipped `typescript-typed-native`).
 *   TS stays on its existing `typescript-typed` (boxed) endpoint until bc adds one — a DECLARED,
 *   reported gap (not silently mirrored to go/rust's retirement), tracked as a bc#77 TS follow-up.
 * - **python/php: NOT present.** bc registers no de-boxed typed endpoint for them (capability limit).
 *   litedbmodel does NOT substitute the literal (≈ir) emitter — that would be an unspec'd fallback.
 *   Codegen for py/php is a bc capability gap that ERRORS (ESCALATE to bc), never a silent literal.
 *
 * WRITES (batchInsert / gate-first tx) are NOT reads — they are NOT routed through this table at
 * all (#60 milestone 1): a write bundle stays on the existing write/tx execution path
 * (`executeTransactionBundle` / the native adapter's hand-written tx mirror), never a codegen
 * module, boxed or typed-raw. {@link generateCodegenArtifact} throws if asked to codegen a write.
 */
export const CODEGEN_EMITTER: Partial<Record<CodegenLanguage, string>> = {
  typescript: 'typescript-typed',
  go: 'go-typed-native',
  rust: 'rust-typed-native',
};

/**
 * The SQL catalog companion sidecar. The read/write PRIMARY SQL is now BAKED into the generated
 * module (the single SQL-baking lowering), so the companion NO LONGER carries the read graph — the
 * read path is companion-free (owner: "retire the JSON SqlCatalogCompanion for reads"). What remains
 * is the runtime-stitched surface the module does NOT bake: the `relations` batch ops (a RelationDecl
 * belongsTo/hasMany prefetch is ONE batched IN query resolved by the runtime, not part of the read
 * module) and the write-Command `transaction` plan (the gate-first tx-DAG, not codegen'd). Pure JSON.
 */
export interface SqlCatalogCompanion {
  /** Target SQL dialect (`sqlite`/`postgres`/`mysql`) — compiled once, TS-side (spec §10). */
  readonly dialect: DialectName;
  /** Input heads normalized to present-as-null (absent-key SKIP) — mirrors the bundle. */
  readonly optionalHeads: readonly string[];
  /** Pre-compiled STATIC read-relation batch ops (spec §5/§8), keyed by relation name — the ONE
   * batched IN query per relation the runtime stitches (not baked into the read module). */
  readonly relations: Record<string, RelationOp>;
  /** Derived gate-first write-time-relations transaction plan (spec §6/§8), for a Command bundle. */
  readonly transaction?: TransactionPlan;
  /**
   * WRITE bundles: the codegen typed-de-box `outputType` (the TransactionResult typed shape). Rides
   * the companion so a codegen consumer that reassembles the bundle round-trips it losslessly.
   */
  readonly outputType?: unknown;
}

/** The full codegen artifact for one bundle × one language. */
export interface CodegenArtifact {
  /** The target language. */
  readonly language: CodegenLanguage;
  /** The generated behavior module (bc's SHARED STRAIGHT-LINE generator output — real static code). */
  readonly module: GeneratedModule;
  /** The SQL catalog companion sidecar (the STATIC makeSQL execution catalog). */
  readonly companion: SqlCatalogCompanion;
  /** The originating bundle (so the equivalence leg re-executes the SAME static bundle). */
  readonly bundle: SqlBundle;
}


/** The runtime-stitched sidecar carried alongside the generated module — NO read/write SQL (baked in
 * the module now), only the relation batch ops + the write-Command tx plan the module does not bake. */
function companionOf(bundle: SqlBundle): SqlCatalogCompanion {
  const base: SqlCatalogCompanion = {
    dialect: bundle.dialect,
    optionalHeads: bundle.optionalHeads,
    relations: bundle.relations,
    ...(bundle.outputType !== undefined ? { outputType: bundle.outputType } : {}),
  };
  return bundle.transaction === undefined ? base : { ...base, transaction: bundle.transaction };
}

// ═══════════════════════════════════════════════════════════════════════════════════════════
// #60 milestone 1 — CODEGEN-ONLY typed-native lowering (#12: from the real-node read graph).
//
// bc's typed-native coverage predicate (`coverageRejectReason`, bc#86/#89) requires EVERY
// componentRef port to be STATICALLY resolvable to a native scalar/string-array/number-literal.
// The de-surrogated read graph's real `Select`/`Count`/map nodes carry boxed authoring ports
// (`where:{arr:[…]}`, `table`, `select`) that are not that shape, so bc would hard-fail on them.
// This lowering rebuilds a NEW, CODEGEN-ONLY component whose body nodes carry ONLY individual
// native-scalar `{ref:[head]}` ports (one per input head the node's compiled statements bind,
// derived from `statementsById`) + typed `inputPorts` — the shape bc's typed-native emitter needs.
//
// This is a NEW, throwaway `ComponentGraphIR` built FROM the bundle's `readGraph` fields
// (`ir` topology + `statementsById`, both already computed by the EXISTING `compileReadGraph` —
// untouched here) — it is never written back onto `SqlBundle`/`ReadGraph`, never serialized, and
// the native exec surface (`executeReadGraph`) never sees it (it walks the real-node `readGraph.ir`
// directly). Purely a generation-time input to `generateModule`.
// ═══════════════════════════════════════════════════════════════════════════════════════════

/**
 * Build a component's typed `inputPorts` from the heads the lowering collected. Kept as one helper so
 * a bug fix / a new port kind lands in ONE place (a scalar, an array, and an optional-default head are
 * all built here).
 *
 *  - `scalars` — required native-scalar heads (`{required:true, type}`).
 *  - `elems` — IN-list / array heads (`{type:'array', elemType}` → `Vec<ElemT>`; bc#110).
 *  - `optionals` — coalesce-default heads (`{required:false, type}` → `Option<T>`; bc#139/#122). An
 *    optional entry WINS over a required-scalar entry for the same head (so `absent` is a real `None`).
 *
 * A head present ONLY as a WHERE COLUMN-NAME MARKER (never bound) is not in any set, so it gets no
 * `InNR` field — sound: the covered plane never reads it (see the note at the legacy call site).
 */
function buildInputPorts(
  c: { inputPorts?: Record<string, PortSchema> },
  scalars: Map<string, BcScalar>,
  elems: Map<string, BcScalar>,
  optionals: Map<string, BcScalar>,
): Record<string, PortSchema> {
  const inputPorts: Record<string, PortSchema> = {};
  for (const [name, scalar] of scalars) {
    if (optionals.has(name)) continue; // the optional entry below wins (absent must be a real None)
    const schema = (c.inputPorts ?? {})[name];
    inputPorts[name] = schema === undefined ? { required: true, type: scalar } : { ...schema, type: scalar };
  }
  for (const [name, elem] of elems) {
    const schema = (c.inputPorts ?? {})[name];
    inputPorts[name] = { ...(schema ?? { required: true }), type: 'array', elemType: elem };
  }
  for (const [name, scalar] of optionals) {
    const schema = (c.inputPorts ?? {})[name];
    inputPorts[name] = { ...(schema ?? {}), required: false, type: scalar };
  }
  return inputPorts;
}

/** A head could not be typed for typed-native codegen — the read is NOT typed-native-coverable. */
export class TypedNativeCoverageError extends Error {
  constructor(
    readonly component: string,
    readonly reasons: readonly string[],
  ) {
    super(
      `litedbmodel codegen (#60 m1): component '${component}' is not typed-native-coverable:\n` +
        reasons.map((r) => `  - ${r}`).join('\n') +
        `\n\nbc's typed-native endpoint (bc#77/#90) fails closed on an uncovered shape — this is NOT ` +
        `regenerated on the boxed '-typed'/'-typed-raw' emitter (retired from litedbmodel's codegen ` +
        `surface, #60 milestone 1). Report this as a bc#86 coverage gap (array-typed / non-scalar ` +
        `input heads have no native port type yet), or exclude this case from native-codegen benching.`,
    );
    this.name = 'TypedNativeCoverageError';
  }
}

/** Extract the `{table, column}` a where-fragment statement's leading SQL token names, for the
 * closed set of fragment shapes `compileSelectNode`/`lowerWhereMember` emit: `<col> <op> ?`
 * (eq/cmp/LIKE/ILIKE). Returns undefined for any other shape (head-count / SQL text mismatch) —
 * the caller treats that head as NOT resolvable (fail-closed), never guesses. */
const WHERE_FRAGMENT_COLUMN = /^([A-Za-z_][A-Za-z0-9_]*)\s+(?:=|<>|<=|>=|<|>|LIKE|ILIKE)\s*\?$/;

/** Extract the table name from the head `SELECT … FROM <table>` statement (dialect-tuned, but the
 * `FROM <table>` token is stable across dialects/CTEs — `compileSelectNode`'s head always ends
 * ` FROM <table>[ …]`; a JOIN/CTE-shaped head is out of scope for this narrow codegen deriver and
 * reported as unresolvable rather than mis-parsed). */
const SELECT_FROM_TABLE = /\bFROM\s+([A-Za-z_][A-Za-z0-9_]*)\b/i;

/** Extract the LHS column of an IN-list / array-bound WHERE fragment — the closed set of texts
 * {@link inListFragment} emits across dialects, all of the form `<col> IN (…)` (sqlite `json_each`,
 * mysql `JSON_TABLE`) or `<col> = ANY(?)` (postgres). The single array param binds a `?` inside; the
 * column names the type to resolve the ELEMENT scalar from (spec §4.1 schema SoT). Any other shape
 * returns undefined (fail-closed). */
const IN_LIST_COLUMN = /^([A-Za-z_][A-Za-z0-9_]*)\s+(?:IN\s*\(|=\s*ANY\s*\()/i;

/**
 * Derive each input head's bc scalar type from a read node's compiled {@link StaticStatement}s
 * (spec §4.1 schema SoT), for bc's typed-native codegen (#60 m1). Works from the ALREADY-COMPILED
 * statement templates (available on `ReadGraph.statementsById`, which codegen already carries)
 * instead of re-walking the authored component — so this lowering needs NO extra input beyond
 * what `SqlBundle`/`ReadGraph` already expose (no change to `compileReadGraph`/`ReadGraph`).
 *
 * Returns three sets:
 *  - `byHead` — heads bound by a bare single-segment `{ref:[head]}` scalar param, typed to their
 *    column's bc scalar (`int`/`float`/`string`/`bool`).
 *  - `arrayHeads` — heads bound by an IN-list / array param (`whereIn`'s
 *    `{__jsonArray:{ref:[head]},dialect}`), typed to their column's ELEMENT scalar. bc#110 gives
 *    typed-native a native array port (`Vec<ElemT>`/`[]ElemT`) for such a head, so it is now
 *    COVERED (no longer fail-closed) — the caller emits it as a native array input port.
 *  - `unresolved` — heads whose owning fragment's SQL text isn't a shape this deriver covers (the
 *    closed `<col> <op> ?` scalar shape or the `<col> IN (…)`/`<col> = ANY(?)` array shape). A
 *    non-empty `unresolved` means the read is NOT typed-native-coverable (fail-closed at the caller).
 *
 * `elementVar` (map nodes only): the map's `as` binding (`$e0`). A param whose ref's FIRST segment
 * is the element var (`{ref:['$e0','author_id']}`) is a map-ELEMENT field access, NOT a component
 * input head — bc types it from the map's `over` element struct (the prior node's row), so this
 * deriver neither types nor rejects it (it is fully covered by bc's `coveredMapNode` element typing).
 */
function deriveHeadTypesFromStatements(
  statements: readonly StaticStatement[],
  resolveColumnType: ColumnTypeResolver,
  elementVar?: string,
  tableFromPort?: string,
): { byHead: Map<string, BcScalar>; arrayHeads: Map<string, BcScalar>; unresolved: Map<string, string> } {
  const byHead = new Map<string, BcScalar>();
  const arrayHeads = new Map<string, BcScalar>();
  const unresolved = new Map<string, string>();

  // The owning table resolves every WHERE fragment's column type. `tableFromPort` is the node's
  // authored `table` port — the real SoT, and the ONLY thing that works for a write (an
  // `INSERT INTO t (…) VALUES …` / `UPDATE t SET …` has no `FROM <t>` to match at all). When absent
  // (the pre-E1 read caller) it falls back to matching the `SELECT … FROM <table>` head statement,
  // which `compileSelectNode` always pushes first — behaviour unchanged for that caller.
  const head = statements[0];
  const tableMatch = head === undefined ? null : SELECT_FROM_TABLE.exec(head.sql);
  const table = tableFromPort ?? tableMatch?.[1];

  for (const stmt of statements) {
    // The LIMIT clause (` LIMIT ?`) is NOT a whereFragment — its param is `coalesce(refOpt(limit), N)`
    // (optional head + a static default). bc's typed-native emitter binds `limit` as a native scalar
    // port (its `portIsStatic` accepts the bare ref + number-literal default), so type the head from
    // the schema-less LIMIT contract: `limit`-family heads are always an integer row count. The
    // element-var guard applies here too (a map child never has a LIMIT-bound element ref today, but
    // stay uniform). Anything OTHER than the closed `coalesce([refOpt([head]), <int>])` shape is left
    // untyped here (the surrogate-scope reconciliation below fail-closes on an un-typed referenced head).
    if (stmt.whereFragment !== true) {
      for (const param of stmt.params) {
        const lh = limitHeadNameOf(param);
        if (lh === undefined) continue;
        if (elementVar !== undefined && lh === elementVar) continue;
        const prior = byHead.get(lh);
        if (prior !== undefined && prior !== 'int') {
          unresolved.set(lh, `input head '${lh}' resolves to conflicting scalar types ('${prior}' vs 'int' from a LIMIT clause) across statements`);
          continue;
        }
        byHead.set(lh, 'int');
      }
      continue;
    }
    for (const param of stmt.params) {
      const path = refPathOf(param);
      // A map-ELEMENT field access (`{ref:['$e0','author_id']}`, first segment = the map `as` var) is
      // NOT a component input head — bc types it from the map's `over` element struct. Neither type
      // nor reject it here (fully covered by bc's `coveredMapNode` element-field lowering).
      if (path !== undefined && elementVar !== undefined && path[0] === elementVar) continue;
      if (path === undefined || path.length !== 1) {
        // An IN-list / array-bound WHERE head — `whereIn`'s `{__jsonArray:{ref:[head]},dialect}`
        // param (single-array param, one `?` inside the `IN (…)`/`= ANY(?)` subquery). bc#110 gives
        // typed-native a native ARRAY port for this shape (`Vec<ElemT>`/`[]ElemT`), so resolve the
        // column's ELEMENT scalar (spec §4.1 SoT) and record it as an array head — no longer
        // fail-closed. Any other non-scalar/nested-literal param stays unresolved (fail-closed).
        const arrHead = arrayHeadNameOf(param);
        if (arrHead !== undefined) {
          if (table === undefined) {
            unresolved.set(arrHead, `input head '${arrHead}': could not resolve the owning table from the SELECT head SQL ('${head?.sql ?? '<none>'}')`);
            continue;
          }
          const cm = IN_LIST_COLUMN.exec(stmt.sql.trim());
          if (cm === null) {
            unresolved.set(
              arrHead,
              `input head '${arrHead}': IN-list WHERE fragment SQL '${stmt.sql}' is not the closed '<col> IN (…)'/'<col> = ANY(?)' shape this codegen-only deriver covers`,
            );
            continue;
          }
          const elemType = sqlTypeToBcScalar(resolveColumnType(table, cm[1]));
          const priorElem = arrayHeads.get(arrHead);
          if (priorElem !== undefined && priorElem !== elemType) {
            unresolved.set(arrHead, `input head '${arrHead}' resolves to conflicting array element scalar types ('${priorElem}' vs '${elemType}') across statements`);
            continue;
          }
          arrayHeads.set(arrHead, elemType);
          continue;
        }
        // Not a bare single-segment ref and not a recognized array head (e.g. a nested literal): if
        // it names a head at all, surface it; otherwise it simply isn't head-typable here.
        const named = headNameOf(param);
        if (named !== undefined) {
          unresolved.set(
            named,
            `input head '${named}' is bound by a non-scalar param shape (${JSON.stringify(param)}) — bc's ` +
              `typed-native emitter has no native port type for it (string/int/float/bool scalars only).`,
          );
        }
        continue;
      }
      const headName = path[0];
      if (table === undefined) {
        unresolved.set(headName, `input head '${headName}': could not resolve the owning table from the SELECT head SQL ('${head?.sql ?? '<none>'}')`);
        continue;
      }
      const m = WHERE_FRAGMENT_COLUMN.exec(stmt.sql.trim());
      if (m === null) {
        unresolved.set(
          headName,
          `input head '${headName}': WHERE fragment SQL '${stmt.sql}' is not the closed '<col> <op> ?' shape this codegen-only deriver covers`,
        );
        continue;
      }
      const column = m[1];
      const sqlType = resolveColumnType(table, column);
      const scalar = sqlTypeToBcScalar(sqlType);
      const prior = byHead.get(headName);
      if (prior !== undefined && prior !== scalar) {
        unresolved.set(headName, `input head '${headName}' resolves to conflicting scalar types ('${prior}' vs '${scalar}') across statements`);
        continue;
      }
      byHead.set(headName, scalar);
    }
  }
  // A head bound BOTH as a scalar and as an array across fragments is not a coherent native port —
  // fail-closed rather than silently pick one (never occurs for the covered shapes, defensive).
  for (const head of arrayHeads.keys()) {
    if (byHead.has(head)) {
      unresolved.set(head, `input head '${head}' is bound both as a scalar and as an array param — no single native port type`);
    }
  }
  return { byHead, arrayHeads, unresolved };
}

/** A statement param's bare ref path (`{ref:[…]}`/`{refOpt:[…]}`), or undefined if it isn't one. */
function refPathOf(param: unknown): readonly string[] | undefined {
  if (param === null || typeof param !== 'object' || Array.isArray(param)) return undefined;
  const keys = Object.keys(param as object);
  if (keys.length !== 1 || (keys[0] !== 'ref' && keys[0] !== 'refOpt')) return undefined;
  const path = (param as Record<string, unknown>)[keys[0]];
  if (!Array.isArray(path) || path.length === 0 || !path.every((s) => typeof s === 'string')) return undefined;
  return path as string[];
}

/** The head name a non-bare-ref param shape still names (e.g. `{__jsonArray:{ref:[head]},…}`), for
 * a clearer diagnostic — undefined when no head can be identified at all (a plain literal). */
function headNameOf(param: unknown): string | undefined {
  if (param === null || typeof param !== 'object' || Array.isArray(param)) return undefined;
  const obj = param as Record<string, unknown>;
  if ('__jsonArray' in obj) {
    const inner = refPathOf(obj.__jsonArray);
    return inner?.[0];
  }
  return undefined;
}


/** The head a LIMIT param (`coalesce([{refOpt:[head]}, <int-literal>])`) binds — the closed shape
 * `compileSelectNode` emits for an optional `limit` with a static default (`coalesce(opt($.limit), N)`).
 * Returns the head name for that exact shape, else undefined (any other shape is not a LIMIT head this
 * deriver types). bc's typed-native emitter binds it as a native scalar port (bare ref + number literal
 * default are both `portIsStatic`). */
function limitHeadNameOf(param: unknown): string | undefined {
  if (param === null || typeof param !== 'object' || Array.isArray(param)) return undefined;
  const obj = param as Record<string, unknown>;
  const args = obj.coalesce;
  if (!Array.isArray(args) || args.length !== 2) return undefined;
  const [refPart, dflt] = args;
  // The default must be a static integer literal (`coalesce(opt($.limit), 20)`).
  if (typeof dflt !== 'number' || !Number.isInteger(dflt)) return undefined;
  if (refPart === null || typeof refPart !== 'object' || Array.isArray(refPart)) return undefined;
  const rp = refPart as Record<string, unknown>;
  const keys = Object.keys(rp);
  if (keys.length !== 1 || (keys[0] !== 'ref' && keys[0] !== 'refOpt')) return undefined;
  const path = rp[keys[0]];
  if (!Array.isArray(path) || path.length !== 1 || typeof path[0] !== 'string') return undefined;
  return path[0];
}

/** The head an IN-list / array param (`{__jsonArray:{ref:[head]},dialect}`) binds — ONLY the covered
 * single-segment `{ref:[head]}` inner shape (bc#110's native array port requires a single-segment,
 * non-opt ref). A multi-segment / opt / non-ref inner is NOT this covered array shape → undefined
 * (the caller then treats it as an unresolved non-scalar param, fail-closed). */
function arrayHeadNameOf(param: unknown): string | undefined {
  if (param === null || typeof param !== 'object' || Array.isArray(param)) return undefined;
  const obj = param as Record<string, unknown>;
  if (!('__jsonArray' in obj)) return undefined;
  const inner = refPathOf(obj.__jsonArray);
  return inner !== undefined && inner.length === 1 ? inner[0] : undefined;
}

// ═══════════════════════════════════════════════════════════════════════════════════════════
// THE SINGLE LOWERING (#116, epic #115) — bake the per-op SQL as a NATIVE LITERAL.
//
// litedbmodel codegen has ONE lowering: it renders each node's per-dialect SQL STATICALLY (the SAME
// assembly `renderStatements` performs, minus the per-input value evaluation) and emits it as native
// ports bc bakes as literals — a STATIC STRING `sql` port + one typed port per bound `?`. So the
// generated module carries its own query and needs NO runtime JSON companion read.
//
// bc's typed-native emitter is SQL-AGNOSTIC — it bakes any port its `portIsStatic` predicate covers
// as a native literal (exactly how graphddb gets `f_table:"UserPermissions"`). Verified against bc
// 0.8.5's `rust-typed-native`/`go-typed-native`/`typescript-typed`: a static-string `sql` port bakes
// as `f_sql: "SELECT … WHERE email = ? LIMIT ?".to_string()`, a single-segment input ref bakes as
// `f_p0: in_.email.clone()`, a bare number literal as `f_p1: 1i64`, an optional-default as
// `in_.limit.unwrap_or(20i64)` (bc#139), an IN-list array as `Vec<ElemT>` (bc#110), and a map
// element-field ref as `oel.author_id` — all on CONCRETE structs, zero boxed `Value`, zero
// bc-runtime import.
//
// A thin, op-agnostic native seam (`exec(sql, params)` + a SKIP-args `query_skip` that assembles
// present fragments) consumes the baked ports and drives the driver — no IR walk, no JSON, no
// dispatch. This ONE lowering covers every read shape (scalar / optional / coalesce params, IN-list
// arrays, skip-optional WHERE fragments, single- AND composite-key map relations) AND writes
// (Insert/Update/Delete); read and write are one flow. There is NO second lowering and NO opt-in flag.
//
// A COMPOSITE-key `map` (a child binding TWO+ parent element fields, e.g. a `(tenant_id, user_id)`
// tuple join) needs NO special case: `paramPortFor` passes EACH element-field ref through, and bc
// bakes N scalar element-field ports (the SAME primitive as single-key, repeated) —
// `f_p0: oel.tenant_id, f_p1: oel.user_id` over a two-column baked SQL. Verified end-to-end
// (rust byte-equal to the mode-2 oracle). The only map shape that fails closed is a SKIP-guarded map
// child (single-key relations do not skip); a plain `cond` node also fails closed.
// ═══════════════════════════════════════════════════════════════════════════════════════════

/**
 * Render a read node's compiled {@link StaticStatement}s to ONE static per-dialect SQL string +
 * the ordered deferred value-specs bound to its `?` placeholders.
 *
 * This mirrors the mode-2 runtime assembly (`renderStatements` in `./makesql/static-bundle`) EXACTLY
 * — same ` WHERE `/` AND ` connector resolution, same {@link composeMakeSQL} concatenation, same
 * {@link renderPlaceholders} dialect pass — with the ONE difference that the params stay as
 * value-spec IR instead of being evaluated against a concrete input scope. The SQL TEXT is therefore
 * identical to what the thin runtime renders, by construction (not by a parallel hand-rolled
 * re-implementation).
 *
 * Fail-closed on a `skip`-carrying statement: a skip means the statement DROPS for some inputs, so
 * the node's SQL text is input-DEPENDENT and has no single static literal. That shape needs a
 * per-skip-subset lowering (one baked SQL per present-set) — NOT a silently-wrong single literal.
 */
function renderStaticSql(
  statements: readonly StaticStatement[],
  dialect: DialectName,
  nodeId: string,
  reasons: string[],
): { sql: string; params: readonly unknown[] } | undefined {
  const nodes: MakeSQL[] = [];
  let whereSeen = false;
  for (const stmt of statements) {
    if (stmt.skip !== undefined) {
      reasons.push(
        `node '${nodeId}': statement ${JSON.stringify(stmt.sql)} carries a 'skip' presence expression — its SQL text is ` +
          `INPUT-DEPENDENT (the fragment drops for some inputs), so the node has no single static SQL literal to bake. ` +
          `A skip-carrying read needs a per-present-set lowering (one baked SQL per skip subset); it is NOT lowered here ` +
          `rather than baking a silently-wrong always-present literal.`,
      );
      return undefined;
    }
    let sql = stmt.sql;
    if (stmt.whereFragment === true) {
      sql = (whereSeen ? ' AND ' : ' WHERE ') + stmt.sql;
      whereSeen = true;
    }
    nodes.push({ sql, params: [...stmt.params] as SqlParam[] });
  }
  // `composeMakeSQL` validates the `?`/param count 1:1 and concatenates — the SAME call the runtime
  // makes. A value-spec param is not a nested `MakeSQL`, so it re-emits its `?` and rides through.
  const assembled = composeMakeSQL(nodes);
  return { sql: renderPlaceholders(assembled.sql, dialect), params: assembled.params };
}

/** Does any of a node's statements carry a `skip` presence expression? A skip fragment DROPS for
 * some inputs, so the node's SQL is assembled per-input — it takes the fragmented lowering (below)
 * instead of the single baked `sql` string. */
function hasSkipStatement(statements: readonly StaticStatement[]): boolean {
  return statements.some((s) => s.skip !== undefined);
}

/**
 * The optional HEAD a SKIP-`cond` guard is driven by: litedbmodel lowers `when(present($.h),
 * whereX($.col, $.h))` to a fragment whose `skip` is `{not:[{ne:[{refOpt:[h]}, null]}]}` — "drop the
 * fragment when `h` is absent". Extract `h` from that exact closed shape (else undefined → the guard
 * isn't the covered presence shape, fail-closed at the caller). `h` is the presence signal: the
 * fragment is PRESENT iff `h` is present, which the native module reads as `in_.<h>.is_some()`.
 */
function skipGuardHead(skip: unknown): string | undefined {
  if (skip === null || typeof skip !== 'object' || Array.isArray(skip)) return undefined;
  const notArgs = (skip as Record<string, unknown>).not;
  if (!Array.isArray(notArgs) || notArgs.length !== 1) return undefined;
  const ne = notArgs[0];
  if (ne === null || typeof ne !== 'object') return undefined;
  const neArgs = (ne as Record<string, unknown>).ne;
  if (!Array.isArray(neArgs) || neArgs.length !== 2 || neArgs[1] !== null) return undefined;
  const inner = neArgs[0];
  if (inner === null || typeof inner !== 'object') return undefined;
  const path = (inner as Record<string, unknown>).refOpt;
  if (!Array.isArray(path) || path.length !== 1 || typeof path[0] !== 'string') return undefined;
  return path[0];
}

/** ONE assembled WHERE fragment of a skip-carrying read: the bare predicate SQL (`?` placeholders,
 * NO connector — the seam prepends ` WHERE `/` AND `), its ordered value-specs, and the optional head
 * whose presence gates it (undefined ⇒ always present). */
interface RenderedFragment {
  readonly sql: string;
  readonly params: readonly unknown[];
  readonly skipHead?: string;
}

/** The fragmented form of a skip-carrying read node: a static head + ordered WHERE fragments (some
 * gated by an optional head's presence) + a static tail. The generic exec seam assembles the present
 * fragments over these baked literals — NATIVE string assembly, NO IR walk / JSON / dispatch. */
interface RenderedSkip {
  readonly head: string;
  readonly headParams: readonly unknown[];
  readonly tail: string;
  readonly tailParams: readonly unknown[];
  readonly fragments: readonly RenderedFragment[];
}

/**
 * Render a skip-carrying read node into its fragmented form (owner's SKIP-args model: "native runtime
 * = 「SQL・params・SKIP引数の汎用クエリ実行関数」だけ"). Classifies the SAME statements
 * {@link renderStaticSql} does — `statements[0]` is the head, `whereFragment` statements are the WHERE
 * fragments (a skip fragment carries `skipHead`), everything else is tail (ORDER BY / LIMIT / …). The
 * bare predicate text keeps its `?` placeholders (the seam resolves ` WHERE `/` AND ` at assembly, and
 * the pg `?`→`$N` renumber is a post-assembly dialect concern like the array encode — sqlite/mysql
 * keep `?`). Fail-closed (a reason) on a skip whose guard is not the covered presence shape.
 */
function renderSkipFragments(
  statements: readonly StaticStatement[],
  nodeId: string,
  reasons: string[],
): RenderedSkip | undefined {
  const head = statements[0];
  if (head === undefined || head.whereFragment === true) {
    reasons.push(`node '${nodeId}': skip-fragment lowering expects a leading SELECT head statement`);
    return undefined;
  }
  const fragments: RenderedFragment[] = [];
  const tailNodes: MakeSQL[] = [];
  for (const stmt of statements.slice(1)) {
    if (stmt.whereFragment === true) {
      let skipHead: string | undefined;
      if (stmt.skip !== undefined) {
        skipHead = skipGuardHead(stmt.skip);
        if (skipHead === undefined) {
          reasons.push(
            `node '${nodeId}': WHERE fragment ${JSON.stringify(stmt.sql)} has a 'skip' guard that is not the covered ` +
              `presence shape (\`not(ne(refOpt(head), null))\`) — cannot derive the presence head, fail-closed.`,
          );
          return undefined;
        }
      }
      fragments.push({ sql: stmt.sql, params: [...stmt.params], ...(skipHead !== undefined ? { skipHead } : {}) });
      continue;
    }
    // A tail statement (ORDER BY / GROUP BY / LIMIT / OFFSET) — assembled verbatim after the WHERE.
    tailNodes.push({ sql: stmt.sql, params: [...stmt.params] as SqlParam[] });
  }
  const tail = composeMakeSQL(tailNodes);
  return { head: head.sql, headParams: [...head.params], tail: tail.sql, tailParams: tail.params, fragments };
}

/** The catalog components the E1/E2 SQL-port lowering bakes. Read and write are ONE flow: both make
 * SQL and execute it, and both hand back rows — so they lower identically and are NOT split. */
const READ_COMPONENTS = new Set(['Select', 'Count']);
const WRITE_COMPONENTS = new Set(['Insert', 'Update', 'Delete']);

/** A write node's `values.<col>` / `set.<col>` port names — the column each bound head writes. */
const WRITE_VALUE_PORT = /^(?:values|set)\.(.+)$/;

/**
 * The SQL row shape `executeStaticWrite` returns for a write with NO RETURNING clause: the single
 * summary row `[{changes, lastInsertRowid}]`. A write's authored/compile-time `outType` is the
 * determined empty-row list `{arr:{obj:{}}}` (it existed only to satisfy bc's all-nodes-typed gate,
 * since writes were never typed-codegen'd) — that is NOT what the op actually returns, so the
 * codegen IR must carry the REAL shape: the generated module's row struct has to match what the exec
 * seam hands back, or the module would be typed a lie.
 */
const WRITE_SUMMARY_OUT_TYPE = { arr: { obj: { changes: 'int', lastInsertRowid: 'int' } } };

/**
 * Type a WRITE node's bound heads from its AUTHORED PORTS — the real SoT — rather than by regexing
 * the compiled SQL.
 *
 * The read deriver ({@link deriveHeadTypesFromStatements}) recovers a fragment's column by matching
 * the statement text and the table by matching `SELECT … FROM <t>`. Neither works for a write: an
 * `INSERT INTO t (…) VALUES (?, ?)` / `UPDATE t SET …` carries no `FROM <t>` at all (only DELETE does,
 * and only by accident), and a value bind's column appears in the INSERT column list, not next to its
 * `?`. The authored ports already state both facts exactly — `table: 'benchmark_users'` and
 * `values.email: {ref:['email']}` — so the column each head writes is read straight off the port
 * NAME, and its type resolves from the schema SoT. WHERE-bound heads (Update/Delete) keep the shared
 * fragment deriver, with the table supplied from the `table` port.
 */
function deriveWriteHeadTypes(
  ports: Record<string, unknown>,
  table: string,
  resolveColumnType: ColumnTypeResolver,
  nodeId: string,
  reasons: string[],
): Map<string, BcScalar> {
  const byHead = new Map<string, BcScalar>();
  const record = (head: string, column: string): void => {
    const scalar = sqlTypeToBcScalar(resolveColumnType(table, column));
    const prior = byHead.get(head);
    if (prior !== undefined && prior !== scalar) {
      reasons.push(`node '${nodeId}': input head '${head}' resolves to conflicting scalar types ('${prior}' vs '${scalar}')`);
      return;
    }
    byHead.set(head, scalar);
  };
  // The written values (`values.<col>` / `set.<col>`): the port NAME states the column, the port
  // VALUE names the bound head. This is the write's SoT — the compiled SQL carries the column in the
  // INSERT column list, nowhere near its `?`, so the ports (not the text) are the correct source.
  for (const [port, value] of Object.entries(ports)) {
    const m = WRITE_VALUE_PORT.exec(port);
    if (m === null) continue;
    const path = refPathOf(value);
    if (path === undefined) continue; // a literal value binds no head (nothing to type)
    if (path.length !== 1) {
      reasons.push(
        `node '${nodeId}' port '${port}': a written value must bind a single-segment head ({ref:[head]}) to lower ` +
          `natively (got ${JSON.stringify(value)}).`,
      );
      continue;
    }
    record(path[0], m[1]);
  }
  // The WHERE-bound heads (Update/Delete): the authored `where` port is the SoT for their column too
  // (an `UPDATE t SET … WHERE id = ?` statement has no `FROM <t>` for the fragment deriver to key
  // off). Each `eq`/cmp member is `<colRef> <op> <valueRef>`: the LHS ref's last segment is the
  // column, the RHS single-segment ref is the bound head. This reuses the SAME closed WHERE shape the
  // read path constrains; anything else fails closed at the node's param lowering below, not here.
  const where = ports.where;
  if (typeof where === 'object' && where !== null && 'arr' in where && Array.isArray((where as { arr: unknown[] }).arr)) {
    for (const member of (where as { arr: unknown[] }).arr) {
      const parsed = writeWhereScalarBind(member);
      if (parsed !== undefined) record(parsed.head, parsed.column);
    }
  }
  return byHead;
}

/** A write WHERE member `<colRef> <eq|cmp> <valueRef>` → its `{head, column}`, or undefined when the
 * member is not that closed scalar shape (a group / sentinel / literal — left for the node's param
 * lowering to type or fail-close, never guessed here). */
function writeWhereScalarBind(member: unknown): { head: string; column: string } | undefined {
  if (member === null || typeof member !== 'object' || Array.isArray(member)) return undefined;
  const keys = Object.keys(member as object);
  if (keys.length !== 1) return undefined;
  const op = keys[0];
  if (op !== 'eq' && op !== 'ne' && op !== 'lt' && op !== 'le' && op !== 'gt' && op !== 'ge') return undefined;
  const args = (member as Record<string, unknown>)[op];
  if (!Array.isArray(args) || args.length !== 2) return undefined;
  const colPath = refPathOf(args[0]);
  const valPath = refPathOf(args[1]);
  if (colPath === undefined || valPath === undefined || valPath.length !== 1) return undefined;
  // The LHS names the column (its last segment); a sentinel-headed LHS (IN/immediate/…) is not a plain
  // column and is skipped (typed via the param path instead).
  const column = colPath[colPath.length - 1];
  if (colPath[0] === IN_SENTINEL) return undefined;
  return { head: valPath[0], column };
}

/**
 * Lower ONE bound `?` value-spec to the native port node bc will bake, or record why it cannot.
 *
 * The COVERED param shapes (verified against bc 0.8.5 `portIsStatic`/`inferPortType`/#139):
 *  - a bare INTEGER/FLOAT literal (e.g. the `LIMIT 1` count) → the literal itself; bc bakes `1i64`.
 *  - a single-segment, non-opt `{ref:[head]}` → the same ref; bc bakes `in_.<head>.clone()`.
 *  - a `coalesce([{refOpt:[head]}, <lit>])` (optional-LIMIT default, #122) → the coalesce node
 *    verbatim; bc 0.8.5 bakes `in_.<head>.unwrap_or(<lit>)` over an OPTIONAL input port (the caller
 *    types the head via {@link coalesceOptHead}). The default is PRESERVED — never dropped.
 *  - an IN-list `{__jsonArray:{ref:[head]}}` array bind → the single-segment ref; the head is typed
 *    as a native array port (bc#110) and the seam performs the dialect's array encode.
 *
 * Everything else fails CLOSED with the exact offending shape named — no boxed escape, no silent
 * default-dropping.
 */
/** A `coalesce([{refOpt:[head]}, <numeric literal>])` param (the optional-LIMIT default, #122) → its
 * `{head, scalar}`, or undefined. bc 0.8.5 (#139) bakes this natively as `in_.<head>.unwrap_or(<lit>)`
 * over an OPTIONAL input port — so the default is PRESERVED, not dropped. The head's scalar comes from
 * the literal default (an int literal ⇒ int, a fractional/exponent literal ⇒ float); the optional ref
 * inner and the default must agree, which the LIMIT contract guarantees (both are the row count). */
function coalesceOptHead(param: unknown): { head: string; scalar: BcScalar } | undefined {
  if (param === null || typeof param !== 'object' || Array.isArray(param)) return undefined;
  const args = (param as Record<string, unknown>).coalesce;
  if (!Array.isArray(args) || args.length !== 2) return undefined;
  const [refPart, dflt] = args;
  if (typeof dflt !== 'number' || !Number.isFinite(dflt)) return undefined;
  if (refPart === null || typeof refPart !== 'object' || Array.isArray(refPart)) return undefined;
  const keys = Object.keys(refPart as object);
  if (keys.length !== 1 || keys[0] !== 'refOpt') return undefined;
  const path = (refPart as Record<string, unknown>).refOpt;
  if (!Array.isArray(path) || path.length !== 1 || typeof path[0] !== 'string') return undefined;
  return { head: path[0], scalar: Number.isInteger(dflt) ? 'int' : 'float' };
}

function paramPortFor(param: unknown, index: number, nodeId: string, reasons: string[], elementVar?: string): unknown | undefined {
  if (typeof param === 'number' && Number.isFinite(param)) return param;
  // A `coalesce(opt(head), literal)` default (#122): bc 0.8.5 bakes it as `in_.<head>.unwrap_or(lit)`
  // over an OPTIONAL input port — the default is preserved natively (NOT dropped by rewriting to a
  // bare ref, as the pre-#139 lowering did). Emit the coalesce node verbatim; the caller types the
  // head as an optional input port (see `coalesceOptHead`).
  if (coalesceOptHead(param) !== undefined) return param;
  // An IN-list / array-bound param (`{__jsonArray:{ref:[head]},dialect}`, ONE `?` inside the
  // `IN (…)`/`= ANY(?)` subquery). bc#110 gives typed-native a native ARRAY port for this head, so
  // the port is the SAME single-segment ref — the caller types its `inputPorts` entry as
  // `{type:'array', elemType}` and bc bakes `f_pN: Vec<ElemT> = in_.<head>.clone()`. The dialect's
  // array-ENCODE (the single-JSON bind for sqlite/mysql, the array bind for pg) is the exec seam's
  // job — it is a driver-binding concern, not a SQL-text one (the text is already baked).
  const arrHead = arrayHeadNameOf(param);
  if (arrHead !== undefined) return { ref: [arrHead] };
  const path = refPathOf(param);
  // A map ELEMENT-FIELD ref (`{ref:[$e0, field]}`, first segment = the map's `as` var) — the mapped
  // parent row's field. bc types it from the map's `over` element struct (the parent node's outType),
  // so emit it verbatim; it is NOT a component input head (the caller excludes it from inputPorts).
  if (elementVar !== undefined && path !== undefined && path.length === 2 && path[0] === elementVar) {
    return { ref: path };
  }
  if (path !== undefined && path.length === 1) {
    const keys = Object.keys(param as object);
    if (keys[0] === 'refOpt') {
      reasons.push(
        `node '${nodeId}' param p${index}: an OPTIONAL head ref ({refOpt:${JSON.stringify(path)}}) has no native non-Option ` +
          `port type on bc's typed-native input struct — not lowered (E1 covers required scalar heads + literals).`,
      );
      return undefined;
    }
    return { ref: path };
  }
  reasons.push(
    `node '${nodeId}' param p${index}: value-spec ${JSON.stringify(param)} is not a shape bc's typed-native emitter bakes ` +
      `as a native port (covered: a bare number literal, a single-segment required {ref:[head]}, or an IN-list ` +
      `{__jsonArray:{ref:[head]}} array bind). Rewriting it to a bare ref would silently drop its semantics ` +
      `(e.g. a coalesce default), so it fails closed.`,
  );
  return undefined;
}

/**
 * THE SINGLE codegen lowering (#116): lower a read/write bundle's REAL `ComponentGraphIR` into a NEW,
 * CODEGEN-ONLY IR whose nodes carry the rendered per-dialect SQL as native ports bc bakes as literals
 * — a STATIC `sql` string port + one typed port per bound `?`. The generated module then CARRIES its
 * own SQL: no runtime JSON companion read.
 *
 * Coverage: `Select`/`Count`/`Insert`/`Update`/`Delete` nodes; required + optional-`coalesce` (#122)
 * + IN-list array (bc#110) params; `skip`-optional WHERE fragments (fragmented shape, seam-assembled);
 * single- AND composite-key `map` relations (one element-field port per parent key column). A `cond`
 * node and a SKIP-guarded `map` child fail CLOSED with a precise reason.
 *
 * Does NOT mutate `bundle`/`bundle.readGraph`: a fresh IR object is returned. The native
 * `executeReadGraph` keeps consuming the real `readGraph.ir`, and the frozen makeSQL conformance
 * corpus is untouched (this changes no compiled statement — it only RE-EXPRESSES already-compiled
 * statements as ports).
 */
export function lowerReadGraphForNativeSql(readGraph: ReadGraph, resolveColumnType: ColumnTypeResolver): ComponentGraphIR {
  const ir = readGraph.ir;
  const reasons: string[] = [];
  const components = ir.components.map((c) => {
    const inputPortTypes = new Map<string, BcScalar>();
    const inputPortElemTypes = new Map<string, BcScalar>();
    // Heads bound by a `coalesce(opt(head), default)` param (#122): they lower to an OPTIONAL input
    // port (`{required:false, type}` → `Option<T>`), which bc 0.8.5 reads via `.unwrap_or(default)`.
    const optionalHeadTypes = new Map<string, BcScalar>();
    // A node whose outType this lowering CORRECTED (a no-RETURNING write → the summary shape): the
    // component `outputType` must follow, since the output Φ is a bare `{ref:[nodeId]}`. Tracked so
    // the corrected type propagates to the runner's return type (else the node row and the runner
    // output disagree — a self-inconsistent module).
    const correctedOutType = new Map<string, unknown>();
    const body = c.body.map((n) => {
      if ('cond' in n) {
        reasons.push(`node '${(n as unknown as { id?: string }).id ?? '<cond>'}': a 'cond' node is not lowered by the E1 SQL-port lowering`);
        return n;
      }
      const stmts = readGraph.statementsById[n.id] ?? [];
      // A `map` (relation) node runs a per-parent-element child Select — its ports live on `n.map`
      // and its child statements bind the mapped parent row via `{ref:[$e0, field]}` element-field
      // refs (bc types them from the parent node's outType; NOT component input heads). Everything
      // else — the SQL baking, the head typing, the param ports — is the SAME as a plain read (this
      // is ONE lowering, branching only on where the ports live + the element-var exclusion).
      const isMap = 'map' in n;
      if ('cond' in n) {
        reasons.push(`node '${(n as unknown as { id?: string }).id ?? '<cond>'}': a 'cond' node is not lowered by the SQL-port lowering`);
        return n;
      }
      const mapObj = isMap ? (n as unknown as { map: { as?: string; component?: string; ports: Record<string, unknown> } }).map : undefined;
      const elementVar = isMap ? mapObj?.as : undefined;
      const component = isMap ? (mapObj?.component ?? '') : ((n as unknown as { component?: string }).component ?? '');
      const isWrite = WRITE_COMPONENTS.has(component);
      if (!isWrite && !READ_COMPONENTS.has(component)) {
        reasons.push(`node '${n.id}': component '${component}' is not a SQL CRUD node the SQL-port lowering bakes (Select/Count/Insert/Update/Delete)`);
        return n;
      }
      if (isMap && (isWrite || elementVar === undefined)) {
        reasons.push(`node '${n.id}': a map node must run a Select/Count child with an 'as' element var (got component '${component}', as '${elementVar ?? '<none>'}')`);
        return n;
      }
      if (isMap && hasSkipStatement(stmts)) {
        reasons.push(`node '${n.id}': a map node with a SKIP-guarded child fragment is not yet lowered (single-key relations do not skip)`);
        return n;
      }
      // The node's authored `table` port — the SoT for every column type below. Every CRUD node
      // (read AND write, plain AND map-child) carries it.
      const nodePorts = isMap ? mapObj!.ports : (n as unknown as { ports: Record<string, unknown> }).ports;
      const table = typeof nodePorts.table === 'string' ? nodePorts.table : undefined;
      if (table === undefined) {
        reasons.push(`node '${n.id}': ${component} node has no literal 'table' port — cannot resolve its column types`);
        return n;
      }
      // Type each genuine bound head from the schema SoT — the SAME derivation the pre-E1 lowering
      // uses (unchanged), so the input struct's field types are identical. A map node passes its
      // element var so element-field refs are excluded from head typing. A write additionally types
      // the heads its `values.*`/`set.*` ports bind (the ports name the column; the SQL does not).
      const { byHead, arrayHeads, unresolved } = deriveHeadTypesFromStatements(stmts, resolveColumnType, elementVar, table);
      for (const [, reason] of unresolved) reasons.push(`node '${n.id}': ${reason}`);
      if (isWrite) {
        for (const [head, scalar] of deriveWriteHeadTypes(nodePorts, table, resolveColumnType, n.id, reasons)) {
          byHead.set(head, scalar);
        }
      }
      // IN-list / array-bound heads (bc#110): a native ARRAY input port carrying the ELEMENT scalar.
      // bc does NOT infer element types (consumer-interface C3), so the elemType comes from the
      // schema SoT (the IN-list column's resolved scalar), exactly as the pre-E1 lowering supplies it.
      for (const [head, elem] of arrayHeads) {
        const prior = inputPortElemTypes.get(head);
        if (prior !== undefined && prior !== elem) {
          reasons.push(`input head '${head}' resolves to conflicting array element scalar types ('${prior}' vs '${elem}') across nodes`);
          continue;
        }
        inputPortElemTypes.set(head, elem);
      }
      for (const [head, scalar] of byHead) {
        const prior = inputPortTypes.get(head);
        if (prior !== undefined && prior !== scalar) {
          reasons.push(`input head '${head}' resolves to conflicting scalar types ('${prior}' vs '${scalar}') across nodes`);
          continue;
        }
        inputPortTypes.set(head, scalar);
      }
      // Record a coalesce-default head so it becomes an OPTIONAL input port (#122). The deriver above
      // also typed it (in `byHead`, as a LIMIT int) — the optional entry wins when building
      // `inputPorts` so `absent` is a real `None` the baked `.unwrap_or(default)` resolves.
      const recordCoalesce = (p: unknown): void => {
        const co = coalesceOptHead(p);
        if (co === undefined) return;
        const prior = optionalHeadTypes.get(co.head);
        if (prior !== undefined && prior !== co.scalar) {
          reasons.push(`input head '${co.head}' resolves to conflicting optional scalar types ('${prior}' vs '${co.scalar}')`);
        } else {
          optionalHeadTypes.set(co.head, co.scalar);
        }
      };
      // A skip-carrying read takes the FRAGMENTED port shape (owner SKIP-args model): a static head +
      // ordered baked WHERE fragments (a skip fragment gated by an optional head's presence) + a
      // static tail. The generic exec seam assembles the PRESENT fragments over these baked literals —
      // native string assembly, NO IR walk / JSON / dispatch. A non-skip read keeps the simple single
      // `sql` port. Both are this ONE lowering; only the emitted port shape differs by whether the
      // node has a skip fragment.
      if (hasSkipStatement(stmts)) {
        const frag = renderSkipFragments(stmts, n.id, reasons);
        if (frag === undefined) return n;
        const newPorts: Record<string, unknown> = { sql_head: frag.head, sql_tail: frag.tail };
        frag.headParams.forEach((p, i) => {
          const port = paramPortFor(p, i, n.id, reasons);
          if (port !== undefined) newPorts[`h${i}`] = port;
          recordCoalesce(p);
        });
        frag.tailParams.forEach((p, i) => {
          const port = paramPortFor(p, i, n.id, reasons);
          if (port !== undefined) newPorts[`t${i}`] = port;
          recordCoalesce(p);
        });
        frag.fragments.forEach((f, fi) => {
          newPorts[`w${fi}`] = f.sql; // the bare predicate, a baked static string bc renders as a literal
          // A skip fragment's presence head is OPTIONAL (`Option<T>`) — the module reads it as
          // `in_.<head>.is_some()` (the seam's SKIP arg). litedbmodel's `when(present(h), whereX(col, h))`
          // makes the presence head the fragment's own bound head, so the frag's optional param IS the
          // presence signal; assert that invariant rather than guess.
          if (f.skipHead !== undefined) {
            const scalar = byHead.get(f.skipHead);
            if (scalar === undefined) {
              reasons.push(`node '${n.id}': skip head '${f.skipHead}' is not a typed bound head of its fragment (unsupported skip shape)`);
            } else {
              optionalHeadTypes.set(f.skipHead, scalar);
            }
          }
          f.params.forEach((p, pi) => {
            const port = paramPortFor(p, pi, n.id, reasons);
            if (port !== undefined) newPorts[`w${fi}p${pi}`] = port;
            recordCoalesce(p);
          });
        });
        return { ...n, ports: newPorts };
      }
      const rendered = renderStaticSql(stmts, readGraph.dialect, n.id, reasons);
      if (rendered === undefined) return n;
      // The single-`sql` port shape: the rendered SQL as a STATIC string port bc bakes as a native
      // literal, plus one typed port per bound `?` in placeholder order. For a map node the params may
      // include element-field refs (`{ref:[$e0, field]}`), which `paramPortFor` passes through (bc
      // types them from the parent outType).
      const newPorts: Record<string, unknown> = { sql: rendered.sql };
      rendered.params.forEach((p, i) => {
        const port = paramPortFor(p, i, n.id, reasons, elementVar);
        if (port !== undefined) newPorts[`p${i}`] = port;
        recordCoalesce(p);
      });
      // A map node's ports live on `n.map`; the runner drives its child per parent element (bc emits
      // the batch/per-element structs). A plain read/write carries `n.ports`.
      if (isMap) {
        return { ...n, map: { ...mapObj, ports: newPorts } } as typeof n;
      }
      // A write with NO RETURNING hands back the single summary row `[{changes, lastInsertRowid}]`,
      // NOT the determined empty-row list its compile-time outType carries (that stamp existed only
      // to satisfy bc's all-nodes-typed gate, back when writes were never typed-codegen'd). Correct it
      // HERE — the module's row struct must describe what the exec seam actually returns.
      const lowered = { ...n, ports: newPorts };
      if (isWrite && nodePorts.returning === undefined) {
        correctedOutType.set(n.id, WRITE_SUMMARY_OUT_TYPE);
        return { ...lowered, outType: WRITE_SUMMARY_OUT_TYPE } as typeof lowered;
      }
      return lowered;
    });
    if (reasons.length > 0) throw new TypedNativeCoverageError(c.name, reasons);
    // Propagate a corrected node outType to the component `outputType` when the output Φ is a bare
    // ref to that node (`{ref:[nodeId]}`) — the single-write shape. Without this the runner's return
    // type stays the stale empty-row list while the node produces the summary row: a type mismatch
    // inside the generated module.
    const outputRef = refPathOf((c as unknown as { output?: unknown }).output);
    const componentOutputType =
      outputRef !== undefined && outputRef.length === 1 && correctedOutType.has(outputRef[0])
        ? correctedOutType.get(outputRef[0])
        : (c as unknown as { outputType?: unknown }).outputType;
    // Only the heads the lowered ports genuinely reference get an `InNR` field (identical rule to
    // the pre-E1 lowering: a WHERE COLUMN-NAME MARKER head is never bound, so it carries no field).
    // The SHARED tail (see {@link buildInputPorts}): required scalars + bc#110 array ports + bc#139
    // optional-default ports (#122). The SAME builder the legacy lowering uses — one place only.
    const inputPorts = buildInputPorts(c, inputPortTypes, inputPortElemTypes, optionalHeadTypes);
    return { ...c, body, inputPorts, outputType: componentOutputType } as unknown as Component;
  });
  return { ...ir, components };
}

// ═══════════════════════════════════════════════════════════════════════════════════════════
// NATIVE BATCHED relations (E4/#119) — bake a RelationDecl batch op as a bc BATCHED-MAP node.
//
// A RelationDecl relation (belongsTo/hasMany) is a BATCH: pull ALL parents' children in ONE child
// query (N+1-avoided — the bench baseline + the litedbmodel runtime both batch). Its batched child
// SQL is already compiled (`RelationOp.sql`: `= ANY(?)` / `unnest(?,?)` on PG, `json_each(?)` tuple
// membership on mysql/sqlite — value-length-independent, so the text is FIXED and BAKEABLE).
//
// bc's BATCHED MAP (`map.batched:true`, verified) is the exact native primitive: its runner collects
// every parent element's ports into ONE `items: Vec<PortsNR>` and calls the handler ONCE, requiring a
// per-parent-aligned result. So we inject a batched-map node whose element ports carry the batched
// `f_sql` + the parent's key column(s) (`f_k0`, `f_k1`, … — native scalars bc types from the parent
// outType). The ONE generic exec seam's batched handler collects the DISTINCT parent keys, runs the
// ONE baked query, groups children by the target key(s), and returns the per-parent lists aligned —
// no per-row `= ?`, no N+1. This is the SAME lowering + SAME seam (no parallel relation mechanism):
// the relation is just another baked node.
// ═══════════════════════════════════════════════════════════════════════════════════════════

/** The reserved element var a batched-relation node binds its parent-key element fields under. */
const RELATION_ELEM_VAR = '$rel';

/** Build the child-row `obj` PortableType from a relation op's projected `select` columns, typed via
 * the schema (the child table's column types) — the SAME resolver the primary read uses. */
function relationChildRowType(relOp: RelationOp, resolveColumnType: ColumnTypeResolver): unknown {
  const table = relOp.targetTable;
  const cols = relOp.select;
  if (table === undefined || cols === undefined || cols.length === 0) {
    throw new Error(`litedbmodel codegen: relation '${relOp.name}' has no targetTable/select to type its child row (batched-native relations need the projected columns).`);
  }
  const obj: Record<string, BcScalar> = {};
  for (const col of cols) obj[col] = sqlTypeToBcScalar(resolveColumnType(table, col));
  return { obj };
}

/**
 * Inject each `bundle.relations` RelationDecl batch op into the ALREADY-LOWERED codegen IR as a bc
 * BATCHED-MAP node, and extend the component output to `{ rows, <relName>… }` so the runner returns
 * the parent rows PLUS the per-parent child lists. Runs AFTER {@link lowerReadGraphForNativeSql}
 * because the injected node is ALREADY in lowered port shape (baked `sql` + native key ports) — it
 * does not go through the authored-port lowering.
 *
 * Single-relation, hasMany/belongsTo, over the PRIMARY read node. The parent keys become element-field
 * ports `{ref:[$rel, <parentKey>]}` (bc bakes each as a native scalar read off the parent element);
 * the batched `f_sql` is baked verbatim. Multiple relations inject multiple sibling batched-map nodes.
 */
function injectBatchedRelations(
  ir: ComponentGraphIR,
  relations: Record<string, RelationOp>,
  resolveColumnType: ColumnTypeResolver,
): ComponentGraphIR {
  const relNames = Object.keys(relations);
  if (relNames.length === 0) return ir;
  const components = ir.components.map((c) => {
    // Skip a read that already carries an INLINE `.map` relation node: its relations are represented
    // (and baked) as inline maps in the read graph, so `bundle.relations` is the redundant
    // runtime-companion form — injecting it would DOUBLE the relation. Only a plain parent read (no
    // inline map) draws its relations purely from `bundle.relations` → inject the batched form.
    if (c.body.some((n) => 'map' in n)) return c;
    // The PRIMARY read node the relations hang off — the first plain componentRef (non-map/cond).
    const primary = c.body.find((n) => !('map' in n) && !('cond' in n)) as { id: string } | undefined;
    if (primary === undefined) return c;
    const relNodes = relNames.map((name) => {
      const relOp = relations[name];
      const parentKeys = relOp.parentKeys ?? (relOp.parentKey !== undefined ? [relOp.parentKey] : []);
      if (parentKeys.length === 0) {
        throw new Error(`litedbmodel codegen: relation '${name}' has no parentKey(s) to key the batch — cannot lower natively.`);
      }
      const childRow = relationChildRowType(relOp, resolveColumnType);
      // hasMany → a LIST per parent (`arr<arr<child>>`); belongsTo/hasOne → one child per parent
      // (`arr<child>`). The seam returns the aligned per-parent result either way.
      const outType = relOp.kind === 'hasMany' ? { arr: { arr: childRow } } : { arr: childRow };
      // The lowered element ports: the baked batched SQL + one native scalar port per parent key.
      const ports: Record<string, unknown> = { sql: relOp.sql };
      parentKeys.forEach((k, i) => {
        ports[`k${i}`] = { ref: [RELATION_ELEM_VAR, k] };
      });
      return {
        id: `rel_${name}`,
        map: {
          as: RELATION_ELEM_VAR,
          component: 'Select',
          over: { ref: [primary.id] },
          batched: true,
          relationKind: 'connection',
          ports,
        },
        outType,
      };
    });
    // Extend the output Φ: `{ rows: <primary>, <relName>: <relNode>… }`.
    const outputObj: Record<string, unknown> = { rows: { ref: [primary.id] } };
    const outputTypeObj: Record<string, unknown> = { rows: (primary as unknown as { outType: unknown }).outType };
    relNames.forEach((name, i) => {
      outputObj[name] = { ref: [relNodes[i].id] };
      outputTypeObj[name] = relNodes[i].outType;
    });
    // Extend the exec `plan.groups` (body-index stages): each injected relation is its OWN sequential
    // stage after the primary (a map node cannot be a member of a PARALLEL stage — bc requires
    // parallel-stage members to be plain componentRef point reads). The indices are the appended
    // nodes' positions (original body length onward).
    const origLen = c.body.length;
    const prevPlan = (c as unknown as { plan?: { concurrency?: number; groups?: number[][] } }).plan;
    const plan = {
      concurrency: prevPlan?.concurrency ?? 16,
      groups: [...(prevPlan?.groups ?? [c.body.map((_, i) => i)]), ...relNodes.map((_, i) => [origLen + i])],
    };
    return {
      ...c,
      body: [...c.body, ...relNodes],
      plan,
      output: { obj: outputObj },
      outputType: { obj: outputTypeObj },
    } as unknown as Component;
  });
  return { ...ir, components };
}

/**
 * Resolve the READ codegen emitter for a target language, or throw. No fallback: a language
 * without a registered endpoint is a bc capability gap that fails LOUDLY (never a silent
 * substitution of the literal/interpreter/boxed emitter — an unspec'd fallback is invalid).
 */
export function typedEmitterFor(language: string, registered: readonly string[]): string {
  if (!(CODEGEN_LANGUAGES as readonly string[]).includes(language)) {
    throw new Error(
      `litedbmodel codegen: language '${language}' is not a supported target (supported: ${CODEGEN_LANGUAGES.join(', ')})`,
    );
  }
  const emitter = CODEGEN_EMITTER[language as CodegenLanguage];
  if (emitter === undefined) {
    throw new Error(
      `litedbmodel codegen: no READ codegen endpoint for '${language}'. litedbmodel codegen is STATIC ` +
        `de-interpreted codegen (spec §9) with NO literal/interpreter/boxed fallback (an unspec'd fallback ` +
        `is invalid). bc registers a READ endpoint only for ${Object.keys(CODEGEN_EMITTER).join('/')} — a ` +
        `'${language}' endpoint is a bc capability gap. ESCALATE to bc (bc#22 pattern); never substitute ` +
        `the literal emitter.`,
    );
  }
  if (!registered.includes(emitter)) {
    throw new Error(
      `litedbmodel codegen: bc's shared generator does not register the '${emitter}' emitter for '${language}' ` +
        `(bc registered: ${[...registered].sort().join(', ')}). ESCALATE to bc (bc#22 pattern), never fork locally.`,
    );
  }
  return emitter;
}

/**
 * Shape-INDEPENDENT emitter resolution — identical to {@link typedEmitterFor} now that there is
 * exactly one READ endpoint per language (kept as a distinct export for callers that historically
 * used the shape-independent form; both resolve the SAME table).
 */
export function codegenEmitterFor(language: string, registered: readonly string[]): string {
  return typedEmitterFor(language, registered);
}

/**
 * Generate the mode-3 READ codegen artifact for ONE §8 READ bundle in ONE target language. Lowers
 * the bundle's real Select-node read graph into the SQL-baking IR
 * ({@link lowerReadGraphForNativeSql}) — for go/rust's typed-NATIVE endpoint AND for TS's
 * `typescript-typed` endpoint alike (every language consumes the SAME baked-SQL ports). The lowering
 * derives each node's ports from its compiled
 * `statementsById` fragments, so a WHERE COLUMN-NAME MARKER head (`whereGe($.created_at, $.since)`
 * — `$.created_at` names the column, is never bound) is EXCLUDED from the emitted ports/input
 * struct, exactly as go/rust already handled it (#12 regression: TS previously fed the raw IR and
 * emitted a stray `created_at` binding → `unknown binding: created_at` at execution).
 *
 * A WRITE bundle is codegen'd through the SAME lowering and the SAME generic exec seam as a read —
 * read and write are ONE flow (both make SQL and execute it; both hand back rows), so they are NOT
 * split. `compileBundle` carries the write's component graph alongside its compiled `statement`, so
 * `bundle.readGraph` is present for a write too and the lowering bakes its `INSERT`/`UPDATE`/`DELETE`
 * exactly as it bakes a `SELECT`. A bundle carrying NEITHER a graph nor a statement is still a hard
 * error (nothing to generate).
 */
export function generateCodegenArtifact(
  bundle: SqlBundle,
  language: string,
  registeredLanguages: readonly string[],
  resolveColumnType: ColumnTypeResolver,
  runtimeImport?: string,
): CodegenArtifact {
  if (bundle.readGraph === undefined) {
    throw new Error(
      `litedbmodel codegen: bundle '${bundle.name}' carries no component graph to generate from. Reads AND ` +
        `writes both compile one (compileBundle keeps the write's graph alongside its statement) — a bundle ` +
        `with neither was produced by some other path, and there is nothing to lower. No-assume, no-fallback.`,
    );
  }
  const emitter = typedEmitterFor(language, registeredLanguages);
  // The portable IR exists ONLY transiently here as the generator's input — it is NOT part of the
  // codegen OUTPUT (no artifact field, no file, no binary; the codegen path never reads IR data).
  //
  // THE SINGLE LOWERING (#116): the SQL-baking lowering bakes the read/write's rendered per-dialect
  // SQL into the module as native literals with typed param ports, so the module carries its own
  // query and needs NO runtime JSON companion read. It covers every read shape (scalar/optional/
  // coalesce params, IN-list arrays, skip-optional WHERE fragments, single-key map relations) and
  // writes. There is NO second lowering and NO opt-in flag — this is the only path.
  const readIr = lowerReadGraphForNativeSql(bundle.readGraph, resolveColumnType);
  // NATIVE BATCHED relations (E4/#119): inject each `bundle.relations` RelationDecl batch op as a bc
  // batched-map node so the read module runs its relations NATIVELY (ONE batched child query, not the
  // runtime companion stitch). Additive — a bundle with no relations is unchanged.
  const ir = injectBatchedRelations(readIr, bundle.relations, resolveColumnType);
  // bc 0.8.0 (scp-only-authoring, SA3/SA7): `generateModule` fail-closes on un-tokened IR
  // (`NON_COMPILED_IR`). This `ir` is DERIVED from `compileBehaviors`' real read graph (additively
  // lowered/annotated), so it carries no in-process provenance token. Re-adopt it at this generation
  // boundary via `loadCompiledIR` — the sanctioned seam that recomputes the canonical fingerprint once
  // and mints the token (the derived graph IS the compiler's output transformed, never hand-forged raw
  // IR). This is exactly bc's "codegen fixture / derived IR" boundary case.
  const compiled = loadCompiledIR(ir);
  const module = generateModule(compiled, runtimeImport === undefined ? { language: emitter } : { language: emitter, runtimeImport });
  return { language: language as CodegenLanguage, module, companion: companionOf(bundle), bundle };
}

/**
 * The TS codegen EXECUTION path used to PROVE behavior-identity: a codegen runtime reads the SQL
 * catalog companion (the STATIC makeSQL bundle) and evaluates skip + value-specs → assemble →
 * render → execute — which is EXACTLY {@link executeBundle} over the SAME bundle.
 */
export function codegenExecuteBundleForTest(artifact: CodegenArtifact, input: Scope, db: SqliteDb): Value {
  return executeBundle(artifact.bundle, input, { db });
}
