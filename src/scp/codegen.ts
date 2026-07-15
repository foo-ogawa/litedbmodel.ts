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
 *     (splits the surrogate `__scope` boxed-obj port into individual scalar ref ports + types the
 *     component's `inputPorts` from the schema) before handing it to bc. For a WRITE bundle, or a
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
 * never touches the shared `readGraph`/`executeReadGraph` the ir/interpret exec surface (and the
 * frozen conformance corpus) depend on. Those keep consuming the ORIGINAL boxed-`__scope`-port
 * surrogate IR ({@link bundleToPortableIR}), completely unaffected by anything in this file.
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

import { generateModule, type Component, type ComponentGraphIR, type GeneratedModule, type Scope, type Value, type PortSchema } from 'behavior-contracts';
import type { SqlBundle, SqliteDb } from './runtime';
import { executeBundle } from './runtime';
import { makeSqlComponentIR } from './makesql/ir';
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
 * The portable {@link ComponentGraphIR} view of a §8 bundle: a read bundle's surrogate IR (each
 * SQL node → a `makeSQL` node), or the single `makeSQL` component IR for a write. This is the
 * ORIGINAL boxed-`__scope`-port form the ir/interpret exec surface's `executeReadGraph` also
 * consumes (via `bundle.readGraph.ir` directly) — {@link generateCodegenArtifact} does NOT use
 * this for a READ bundle (it uses the codegen-only lowered form, {@link lowerReadGraphForTypedNative});
 * this function is kept for callers that want the untouched portable IR (e.g. diagnostics, the
 * write path, or a future straight-line fallback caller).
 */
export function bundleToPortableIR(bundle: SqlBundle): ComponentGraphIR {
  if (bundle.readGraph !== undefined) return bundle.readGraph.ir;
  const ir = makeSqlComponentIR(bundle.name);
  // A WRITE bundle's typed de-box (spec §4.1/§9): attach the derived `outputType` (the
  // TransactionResult typed shape) to the single `makeSQL` node's `outType` + the component's
  // `outputType` — exactly how the read graph annotates its surrogate IR. Without it, bc's typed
  // emitters would hard-fail ("nothing to de-box"), which is the correct fail-closed signal; a
  // resolver-less write bundle (no `outputType`) stays un-annotated (interpret/boxed only, as before).
  if (bundle.outputType === undefined) return ir;
  const c = ir.components[0];
  const body = c.body.map((n, i) => (i === 0 ? { ...n, outType: bundle.outputType } : n));
  const component = { ...c, body, outputType: bundle.outputType } as unknown as Component;
  return { ...ir, components: [component] };
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
// #60 milestone 1 — CODEGEN-ONLY typed-native lowering.
//
// bc's typed-native coverage predicate (`coverageRejectReason`, bc#86/#89) requires EVERY
// componentRef port to be STATICALLY resolvable to a native scalar/string-array/number-literal —
// the surrogate read graph's single `__scope: {obj:{...}}` port is a BOXED obj literal, which is
// never statically resolvable (it fails `portIsStatic`), so bc hard-fails ("does not lower to a
// native Rust type") on EVERY read, covered or not, before this lowering. Splitting `__scope` into
// individual scalar ref ports (one per referenced head) makes the shape ELIGIBLE; typing the
// component's `inputPorts` (bc's authoring default is `unknown` for every scanned `$.head`) then
// lets each port lower to a CONCRETE Rust/Go field instead of falling through to `Value`.
//
// This is a NEW, throwaway `ComponentGraphIR` built FROM the bundle's `readGraph` fields
// (`ir`/`statementsById`, both already computed by the EXISTING `compileReadGraph` — untouched
// here) — it is never written back onto `SqlBundle`/`ReadGraph`, never serialized, and the
// ir/interpret exec surface (`executeReadGraph`) never sees it (it keeps reading `readGraph.ir`,
// the boxed-`__scope` form, directly). Purely a generation-time input to `generateModule`.
// ═══════════════════════════════════════════════════════════════════════════════════════════

/** The makeSQL surrogate node's synthetic port name (mirrors `static-bundle.ts`'s private `SCOPE_PORT`
 * — duplicated as a literal here rather than exported, keeping this a codegen-only concern). */
const SCOPE_PORT = '__scope';

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
 */
function deriveHeadTypesFromStatements(
  statements: readonly StaticStatement[],
  resolveColumnType: ColumnTypeResolver,
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
    if (stmt.whereFragment !== true) continue;
    for (const param of stmt.params) {
      const path = refPathOf(param);
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
 * Lower a read bundle's surrogate `ComponentGraphIR` into a NEW, CODEGEN-ONLY IR eligible for bc's
 * typed-native endpoint (#60 milestone 1): split the single `__scope: {obj:{...}}` boxed port into
 * individual scalar `{ref:[head]}` ports, and type the component's `inputPorts` from the schema
 * (via {@link deriveHeadTypesFromStatements}). Throws {@link TypedNativeCoverageError} if any
 * referenced head cannot be natively typed (e.g. an IN-list's array head) — NO silent `unknown`
 * port, NO fallback. Does NOT mutate `bundle`/`bundle.readGraph` (a fresh IR object is returned);
 * the ir/interpret exec surface's `executeReadGraph` keeps consuming the ORIGINAL `readGraph.ir`.
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
      const ref = isMapNode ? (n as unknown as { map: { ports: Record<string, unknown> } }).map : (n as unknown as { ports: Record<string, unknown> });
      const scopeVal = ref.ports[SCOPE_PORT];
      if (scopeVal === undefined || typeof scopeVal !== 'object' || scopeVal === null || !('obj' in scopeVal)) {
        // Not the makeSQL surrogate shape this lowering targets (defensive — every read-graph body
        // node IS this shape by construction) — leave the node untouched; bc's own coverage check
        // will report it if it is genuinely uncovered.
        return n;
      }
      const obj = (scopeVal as { obj: Record<string, unknown> }).obj;
      const stmts = readGraph.statementsById[n.id] ?? [];
      const { byHead, arrayHeads, unresolved } = deriveHeadTypesFromStatements(stmts, resolveColumnType);
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
      // Every head the surrogate scope references must have been typed above (byHead/arrayHeads) — a
      // head present in `obj` but absent from all of byHead/arrayHeads/unresolved indicates a param
      // shape this deriver's statement walk did not visit (e.g. a non-WHERE port); report it too
      // rather than silently emitting an untyped scalar ref port. A covered array head is emitted as
      // the SAME single-segment `{ref:[head]}` port (bc#110's array-port lowering keys off the
      // inputPorts array schema, not a distinct port shape — `classifyExpr` sees a plain ref).
      const newPorts: Record<string, unknown> = {};
      for (const head of Object.keys(obj)) {
        if (!byHead.has(head) && !arrayHeads.has(head) && !unresolved.has(head)) {
          reasons.push(`node '${n.id}': input head '${head}' referenced by the surrogate scope but not resolved by any WHERE fragment — cannot type it for native codegen`);
          continue;
        }
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

/** Does a resolved emitter id end in bc's typed-native suffix (`-typed-native`)? Only THOSE
 * emitters need (and are eligible for) the `__scope`-obj-splitting lowering — a boxed endpoint
 * (`typescript-typed`) accepts the ORIGINAL surrogate IR just fine (it consumes a boxed `Value`
 * port either way), so lowering it would needlessly narrow TS's coverage (e.g. `whereIn`'s
 * array-typed head, which typed-native cannot cover but the boxed TS endpoint handles today). */
function isTypedNativeEmitter(emitter: string): boolean {
  return emitter.endsWith('-typed-native');
}

/**
 * Generate the mode-3 READ codegen artifact for ONE §8 READ bundle in ONE target language. For a
 * typed-NATIVE target (go/rust, bc#77/#90), lowers the bundle's surrogate read graph into the
 * typed-native-eligible IR ({@link lowerReadGraphForTypedNative}) first — REAL static native
 * source, RUNTIME-FREE, no baked-IR interpret path. For any OTHER registered endpoint (currently
 * only TS's boxed `typescript-typed`, which has no typed-native counterpart yet), the ORIGINAL
 * portable IR ({@link bundleToPortableIR}) is used unchanged — the lowering is typed-native-only
 * and would needlessly narrow a boxed endpoint's existing coverage. litedbmodel supplies the
 * input (portable IR + catalog); bc owns the emitter.
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
  const ir = isTypedNativeEmitter(emitter)
    ? lowerReadGraphForTypedNative(bundle.readGraph, resolveColumnType)
    : bundleToPortableIR(bundle);
  const module = generateModule(ir, runtimeImport === undefined ? { language: emitter } : { language: emitter, runtimeImport });
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
