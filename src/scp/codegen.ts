/**
 * litedbmodel v2 SCP — mode-3 codegen (WS7f, #35; spec §9 exec-mode 3 / §10 / §11; makeSQL flip,
 * epic #43/#45 Phase B; #60 milestone 1 — typed-NATIVE READ codegen, bc#77/#90).
 *
 * The static-codegen execution mode ("Codegen・静的（全言語）— IR → 各言語ソース生成 runtime≈0,
 * 入力は可搬IRのみ, bc#13 共有 generator に SQL catalog を供給" — spec §9). It takes a §8 STATIC
 * makeSQL {@link SqlBundle} and, per target language, emits:
 *
 *  1. **Behavior module** — bc's SHARED generator run over the bundle's portable IR through bc's
 *     de-interpreted, RUNTIME-FREE emitter. For a READ bundle covered by bc's typed-NATIVE
 *     endpoint (`rust-typed-native` / `go-typed-native`, bc#77/#90 — the 1.0 read de-box: fully
 *     concrete `HandlerNR<Comp>`/`InNR<Comp>`/`PortsNR<Comp><node>`/`RawRowNR<Comp><node>` structs,
 *     ZERO boxed `Value`/`RawValue`, ZERO IR baked into the module), the CODEGEN-ONLY lowering
 *     below ({@link lowerReadGraphForTypedNative}) makes the read's shape structurally ELIGIBLE
 *     (derives each real Select-node's referenced heads from its statements → individual scalar ref
 *     ports + types the component's `inputPorts` from the schema) before handing it to bc. For a WRITE bundle, or a
 *     READ shape typed-native does not (yet) cover, codegen uses bc's existing de-interpreted
 *     STRAIGHT-LINE endpoint (`<lang>-straightline`, bc#75) — see {@link typedEmitterFor}.
 *  2. **SQL catalog companion** — the pure-JSON STATIC makeSQL catalog (per-node statement
 *     templates / relations / transaction). A codegen consumer's thin SQL layer reads it to
 *     evaluate skip + value-specs → assemble → render → execute (identically to the mode-2 runtime).
 *
 * ## #60 milestone 1 scope (CODEGEN-PATH ONLY)
 *
 * This lowering exists SOLELY for the codegen emitter call — it builds a NEW, separate
 * `ComponentGraphIR` from the bundle's `readGraph`; it never mutates `SqlBundle`/`ReadGraph` and
 * never touches the shared `readGraph`/`executeReadGraph` the native exec surface (and the
 * frozen conformance corpus) depend on. Those keep consuming the REAL Select-node
 * `readGraph.ir`, completely unaffected by anything in this file.
 *
 * ## Behavior-identical to the thin-runtime (mode-2), by construction
 *
 * The generated module is observationally equivalent to `runBehavior` (same values, same emitted
 * op sequence, same Failure code/message), and the companion IS the bundle, so a codegen runtime
 * that drives it follows the IDENTICAL static-makeSQL render/execute path {@link executeBundle}
 * uses — SQL text (all dialects) and results are identical, not approximately equal. The TS leg
 * PROVES this by executing via the artifact and asserting exact equality vs {@link executeBundle}
 * (see {@link codegenExecuteBundleForTest}).
 *
 * ## No literal-bake / no boxed fallback
 *
 * typed-native fails CLOSED on an uncovered shape (a `map`/`cond` relation kind it does not cover,
 * or an input head that cannot lower to a native port). Such a shape is NOT silently regenerated on
 * the boxed `-typed`/`-typed-raw` emitter — it THROWS, naming the exact gap, so it can be tracked as
 * a bc coverage gap rather than masked.
 *
 * IN-list / array-bound WHERE heads (the `whereIn` `{__jsonArray:{ref:[head]},dialect}` param) are
 * now COVERED via bc#110 (native array/list port for a componentRef input port): the lowering
 * resolves the IN-list column's element scalar and emits a native ARRAY input port
 * (`{type:'array', elemType}` → `Vec<ElemT>`/`[]ElemT`), so `complexWhere`/`inList` reach the
 * zero-boxing native hot path (no serde_json/encoding-json, no boxed `Value`). They no longer
 * fail closed as a bc#86/coverage gap.
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
 * The SQL catalog companion (spec §9): the STATIC makeSQL bundle fields the codegen consumer's
 * thin SQL layer reads to evaluate + render + execute. Pure JSON — round-trips losslessly.
 */
export interface SqlCatalogCompanion {
  /** Target SQL dialect (`sqlite`/`postgres`/`mysql`) — compiled once, TS-side (spec §10). */
  readonly dialect: DialectName;
  /** READ bundles: the portable read graph (surrogate IR + per-node makeSQL statements). */
  readonly readGraph?: ReadGraph;
  /** WRITE bundles: the single base-write makeSQL statement template. */
  readonly statement?: { readonly sql: string; readonly params: readonly unknown[]; readonly skip?: unknown };
  /** Input heads normalized to present-as-null (absent-key SKIP) — mirrors the bundle. */
  readonly optionalHeads: readonly string[];
  /** Pre-compiled STATIC read-relation batch ops (spec §5/§8), keyed by relation name. */
  readonly relations: Record<string, RelationOp>;
  /** Derived gate-first write-time-relations transaction plan (spec §6/§8), for a Command bundle. */
  readonly transaction?: TransactionPlan;
  /**
   * WRITE bundles: the codegen typed-de-box `outputType` (the TransactionResult typed shape). Rides
   * the companion so a codegen consumer that reassembles the bundle from the artifact round-trips it
   * losslessly (the read de-box carries its outType/outputType inside `readGraph.ir` instead).
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

/**
 * The portable {@link ComponentGraphIR} view of a §8 READ bundle: `compileBehaviors`' real-node
 * read-graph IR (the de-surrogated `Select`/`Count`/map nodes; #12) — the SAME IR the native exec
 * surface (`executeReadGraph`) walks (via `bundle.readGraph.ir` directly). litedbmodel constructs NO
 * `ComponentGraphIR` literal here; it returns the compiler's own output. {@link generateCodegenArtifact}
 * does NOT use this for a typed-native READ target (that uses the codegen-only lowered form,
 * {@link lowerReadGraphForTypedNative}); this is the portable IR for a boxed READ endpoint / fingerprint.
 *
 * WRITE bundles have NO portable component-graph IR (they carry a single compiled `statement`, not a
 * component graph — and are NOT codegen-module cases: {@link generateCodegenArtifact} rejects them).
 * There is therefore no hand-built `makeSQL` write surrogate anymore (#12) — a write bundle throws.
 */
export function bundleToPortableIR(bundle: SqlBundle): ComponentGraphIR {
  if (bundle.readGraph !== undefined) return bundle.readGraph.ir;
  throw new Error(
    `litedbmodel codegen: bundle '${bundle.name}' has no readGraph — a WRITE bundle carries a single ` +
      `compiled makeSQL statement, not a portable component-graph IR, and is not a codegen-module case ` +
      `(#12: the hand-built makeSQL write surrogate IR is eliminated; writes ride the write/tx exec path).`,
  );
}

/** The STATIC makeSQL execution catalog carried alongside the generated module. */
function companionOf(bundle: SqlBundle): SqlCatalogCompanion {
  const base: SqlCatalogCompanion = {
    dialect: bundle.dialect,
    optionalHeads: bundle.optionalHeads,
    relations: bundle.relations,
    ...(bundle.readGraph !== undefined ? { readGraph: bundle.readGraph } : {}),
    ...(bundle.statement !== undefined ? { statement: bundle.statement } : {}),
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
): { byHead: Map<string, BcScalar>; arrayHeads: Map<string, BcScalar>; unresolved: Map<string, string> } {
  const byHead = new Map<string, BcScalar>();
  const arrayHeads = new Map<string, BcScalar>();
  const unresolved = new Map<string, string>();

  // The head statement (`SELECT … FROM <table>`) — always statements[0] (compileSelectNode always
  // pushes it first); its table name resolves every WHERE fragment's column type.
  const head = statements[0];
  const tableMatch = head === undefined ? null : SELECT_FROM_TABLE.exec(head.sql);
  const table = tableMatch?.[1];

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

/** The distinct map-ELEMENT field names a node's statements bind via `{ref:[elementVar, field]}`
 * (first-seen order). These are the map child's element-field ports (`$e0.author_id`): bc's
 * typed-native map lowering types each from the `over` element struct's field. Only single-field
 * accesses (`[elementVar, field]`, length 2) are collected — a deeper element path is not a covered
 * native port here (fail-closed downstream if one appears). */
function elementFieldRefs(statements: readonly StaticStatement[], elementVar: string): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const stmt of statements) {
    for (const param of stmt.params) {
      const path = refPathOf(param);
      if (path === undefined || path.length !== 2 || path[0] !== elementVar) continue;
      const field = path[1];
      if (!seen.has(field)) {
        seen.add(field);
        out.push(field);
      }
    }
  }
  return out;
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

/**
 * Lower a read bundle's REAL Select-node `ComponentGraphIR` into a NEW, CODEGEN-ONLY IR eligible
 * for bc's typed-native endpoint (#60 milestone 1): derive each node's referenced heads from its
 * `statementsById` fragments and emit individual native-scalar `{ref:[head]}` ports, typing the
 * component's `inputPorts` from the schema (via {@link deriveHeadTypesFromStatements}). Throws
 * {@link TypedNativeCoverageError} if any referenced head cannot be natively typed (e.g. an IN-list's
 * array head) — NO silent `unknown` port, NO fallback. Does NOT mutate `bundle`/`bundle.readGraph`
 * (a fresh IR object is returned); the native `executeReadGraph` keeps consuming the real `readGraph.ir`.
 *
 * Scope: bc's typed-native predicate itself still governs `map`/`cond`/relationKind/policy
 * coverage (this lowering does not special-case those — `generateModule` fails closed on them with
 * its own detailed diagnostic, which this function does not need to duplicate).
 */
export function lowerReadGraphForTypedNative(readGraph: ReadGraph, resolveColumnType: ColumnTypeResolver): ComponentGraphIR {
  const ir = readGraph.ir;
  const reasons: string[] = [];
  const components = ir.components.map((c) => {
    const inputPortTypes = new Map<string, BcScalar>();
    // IN-list / array-bound heads (bc#110): the head's native ELEMENT scalar (the port lowers to
    // `Vec<ElemT>`/`[]ElemT`, fed natively — NO json.Marshal/serde_json on the hot path).
    const inputPortElemTypes = new Map<string, BcScalar>();
    const body = c.body.map((n) => {
      if ('cond' in n) return n;
      const isMapNode = 'map' in n;
      const stmts = readGraph.statementsById[n.id] ?? [];
      // A map node binds an ELEMENT var (`as: '$e0'`) — its child statements reference the mapped
      // parent row via `{ref:['$e0', <field>]}`. That is a map-element field access (bc types it from
      // the map's `over` element struct), NOT a component input head, so hand the element var to the
      // deriver to exclude it from head typing/rejection.
      const elementVar = isMapNode
        ? ((n as unknown as { map: { as?: string } }).map.as ?? undefined)
        : undefined;
      const { byHead, arrayHeads, unresolved } = deriveHeadTypesFromStatements(stmts, resolveColumnType, elementVar);
      for (const [, reason] of unresolved) reasons.push(`node '${n.id}': ${reason}`);
      for (const [head, scalar] of byHead) {
        const prior = inputPortTypes.get(head);
        if (prior !== undefined && prior !== scalar) {
          reasons.push(`input head '${head}' resolves to conflicting scalar types ('${prior}' vs '${scalar}') across nodes`);
          continue;
        }
        inputPortTypes.set(head, scalar);
      }
      for (const [head, elem] of arrayHeads) {
        const prior = inputPortElemTypes.get(head);
        if (prior !== undefined && prior !== elem) {
          reasons.push(`input head '${head}' resolves to conflicting array element scalar types ('${prior}' vs '${elem}') across nodes`);
          continue;
        }
        inputPortElemTypes.set(head, elem);
      }
      // #12 (de-surrogated): the codegen IR is rebuilt from the real node's `statementsById`-derived
      // head set (`byHead` ∪ `arrayHeads`) — the SAME native-scalar `{ref:[head]}` port shape bc's
      // typed-native emitter consumes. There is no `__scope` obj to walk anymore; the referenced-head
      // set IS what `deriveHeadTypesFromStatements` found (an `unresolved` head already pushed a
      // reason above, fail-closed). A covered array head is emitted as the SAME single-segment
      // `{ref:[head]}` port (bc#110's array-port lowering keys off the inputPorts array schema).
      const newPorts: Record<string, unknown> = {};
      // A map node's child statements reference the mapped parent row via `{ref:['$e0', <field>]}`.
      // bc's typed-native map lowering types a port `{ref:['$e0', <field>]}` from the OVER element
      // struct's FIELD (a native scalar) — so emit one native-scalar element-FIELD port per field the
      // child statements bind (`{ref:['$e0', author_id]}` → port `author_id`). Non-element component
      // heads (if any) are typed/emitted normally below.
      if (isMapNode && elementVar !== undefined) {
        const fields = elementFieldRefs(stmts, elementVar);
        for (const field of fields) newPorts[field] = { ref: [elementVar, field] };
      }
      for (const head of [...byHead.keys(), ...arrayHeads.keys()]) {
        newPorts[head] = { ref: [head] };
      }
      return isMapNode
        ? { ...n, map: { ...(n as unknown as { map: object }).map, ports: newPorts } }
        : { ...n, ports: newPorts };
    });
    if (reasons.length > 0) throw new TypedNativeCoverageError(c.name, reasons);
    // Only emit an `inputPorts` entry for a head the COVERED plane actually references (a
    // native-scalar port on some node's lowered ports). bc's authoring layer registers an
    // `inputPorts` entry for every `$.head` the authored method SOURCE accesses — including a
    // head used ONLY as a WHERE-fragment's LHS COLUMN-name marker (e.g. `whereGe($.created_at,
    // $.since)` accesses `$.created_at` to name the column, never as a bound RHS value), which
    // never appears in the surrogate `__scope` obj at all. Keeping such a stray entry with its
    // original `unknown` schema type would emit an unresolvable `InNR<Comp>.field: Value` (bc's
    // typed-native `InNR` struct declares a field for EVERY declared inputPort) — a compile error,
    // since the covered module imports no `Value` type. Dropping it is sound: the covered plane
    // never reads it (no lowered port references it), so omitting the `InNR` field changes nothing
    // observable — every REAL WHERE-bound head is still typed + present (typed above, in `byHead`).
    const inputPorts: Record<string, PortSchema> = {};
    for (const [name, scalar] of inputPortTypes) {
      const schema = (c.inputPorts ?? {})[name];
      inputPorts[name] = schema === undefined ? { required: true, type: scalar } : { ...schema, type: scalar };
    }
    // IN-list / array-bound heads (bc#110): a native ARRAY input port carrying the ELEMENT scalar as
    // `elemType`. bc's typed-native emitter lowers this to `Vec<ElemT>`/`[]ElemT` fed natively from
    // the input struct field — NO boxed `Value`, NO serde_json/encoding-json on the read hot path.
    // bc does NOT infer element types (consumer-interface C3), so litedbmodel supplies `elemType`
    // from the schema SoT (the IN-list column's resolved scalar).
    for (const [name, elem] of inputPortElemTypes) {
      const schema = (c.inputPorts ?? {})[name];
      inputPorts[name] = { ...(schema ?? { required: true }), type: 'array', elemType: elem };
    }
    return { ...c, body, inputPorts } as unknown as Component;
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
 * Every litedbmodel READ codegen endpoint consumes the SAME native-scalar-port lowering — NOT
 * just the `-typed-native` (go/rust) ones. Rationale (#12 regression fix):
 *
 * Post-#12 the read graph is the REAL de-surrogated `Select`-node IR, whose `where` port array
 * carries a fragment for EVERY authored `whereX($.col, …)` — including one whose LHS `$.col` is a
 * WHERE COLUMN-NAME MARKER, never a bound input value (`whereGe($.created_at, $.since)` accesses
 * `$.created_at` only to NAME the column; only `$.since` is bound). Handing that raw IR to bc's
 * emitter emits a `ref(["created_at"], scope)` read + a `created_at` input-struct field, so the
 * bound module throws `unknown binding: created_at` at execution (the scope has no `created_at`).
 * The go/rust `-typed-native` path never hit this because it is fed
 * {@link lowerReadGraphForTypedNative}, which rebuilds each node's ports from the GENUINE bound
 * heads (`statementsById`), dropping the column-name markers. TS was on the raw IR
 * ({@link bundleToPortableIR}) → it kept the marker → it broke.
 *
 * The lowering is therefore the correct input for the boxed `typescript-typed` endpoint too: it
 * feeds bc EXACTLY the genuine bound heads (equivalent-in-spirit to what go/rust consume), and
 * bc#110's native array port means the array-typed heads (`complexWhere`/`inList`) lower cleanly —
 * the old "lowering would narrow TS's array coverage" concern is stale. Only WRITE bundles (no
 * `readGraph`) never reach this path. */
function needsHeadLowering(_emitter: string): boolean {
  return true;
}

/**
 * Generate the mode-3 READ codegen artifact for ONE §8 READ bundle in ONE target language. Lowers
 * the bundle's real Select-node read graph into the native-scalar-port IR
 * ({@link lowerReadGraphForTypedNative}) FIRST — for go/rust's typed-NATIVE endpoint (bc#77/#90,
 * RUNTIME-FREE) AND for TS's boxed `typescript-typed` endpoint alike. Both consume the SAME
 * genuine-bound-head shape: the lowering derives each node's ports from its compiled
 * `statementsById` fragments, so a WHERE COLUMN-NAME MARKER head (`whereGe($.created_at, $.since)`
 * — `$.created_at` names the column, is never bound) is EXCLUDED from the emitted ports/input
 * struct, exactly as go/rust already handled it (#12 regression: TS previously fed the raw IR and
 * emitted a stray `created_at` binding → `unknown binding: created_at` at execution).
 *
 * WRITE bundles are OUT OF SCOPE (#60 milestone 1: writes stay on the existing write/tx execution
 * path, never a codegen module) — throws if `bundle.readGraph` is absent.
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
      `litedbmodel codegen (#60 m1): bundle '${bundle.name}' has no readGraph — WRITE bundles are not ` +
        `codegen-module cases (they stay on the existing write/tx execution path). Only READ bundles ` +
        `are generated through generateCodegenArtifact.`,
    );
  }
  const emitter = typedEmitterFor(language, registeredLanguages);
  // The portable IR exists ONLY transiently here as the generator's input — it is NOT part of the
  // codegen OUTPUT (no artifact field, no file, no binary; the codegen path never reads IR data).
  // ALL read endpoints (go/rust typed-native AND TS boxed typescript-typed) are fed the SAME
  // genuine-bound-head lowering (#12 regression fix — see needsHeadLowering).
  const ir = needsHeadLowering(emitter)
    ? lowerReadGraphForTypedNative(bundle.readGraph, resolveColumnType)
    : bundleToPortableIR(bundle);
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
