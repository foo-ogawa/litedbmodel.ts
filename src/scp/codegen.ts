/**
 * litedbmodel v2 SCP — mode-3 codegen (WS7f, #35; spec §9 exec-mode 3 / §10 / §11; makeSQL flip,
 * epic #43/#45 Phase B).
 *
 * The static-codegen execution mode ("Codegen・静的（全言語）— IR → 各言語ソース生成 runtime≈0,
 * 入力は可搬IRのみ, bc#13 共有 generator に SQL catalog を供給" — spec §9). It takes a §8 STATIC
 * makeSQL {@link SqlBundle} and, per target language, emits:
 *
 *  1. **Behavior module** — bc's SHARED generator run over the bundle's portable IR (a read
 *     bundle's surrogate `ComponentGraphIR`, or the `makeSQL` component IR for a write) through
 *     bc's **STRAIGHT-LINE endpoint** (`<lang>-straightline`, bc#75). This is GENUINELY-STATIC
 *     de-interpreted code: each component is emitted as native straight-line source — strictly
 *     sequential plans become plain statements in dependency order (no `RunPlan`, no ops table,
 *     no per-op id search, no per-expression scope snapshot, and the portable IR is NOT embedded).
 *     Only a plan with REAL bounded concurrency (multi-op stages) drives the `runPlan` primitive
 *     (the semantics SSoT), and even there per-op exec is a pre-resolved function table. A
 *     `bind(handlers)` accessor resolves handlers once and invokes the static function directly.
 *     litedbmodel writes NO execution logic here — codegen is upstream-owned (bc); the emitter is
 *     bc's, and it emits STATIC code, not an interpreter driven over a baked IR literal.
 *  2. **SQL catalog companion** — the pure-JSON STATIC makeSQL catalog (per-node statement
 *     templates / relations / transaction). A codegen consumer's thin SQL layer reads it to
 *     evaluate skip + value-specs → assemble → render → execute (identically to the mode-2 runtime).
 *
 * ## Behavior-identical to the thin-runtime (mode-2), by construction
 *
 * The straight-line module is observationally equivalent to `runBehavior` (same values, same
 * emitted op sequence, same Failure code/message — bc#75 anti-sham gate), and the companion IS
 * the bundle, so a codegen runtime that drives it follows the IDENTICAL static-makeSQL
 * render/execute path {@link executeBundle} uses — SQL text (all dialects) and results are
 * identical, not approximately equal. The TS leg PROVES this by executing via the artifact and
 * asserting exact equality vs {@link executeBundle} (see {@link codegenExecuteBundleForTest}).
 *
 * ## De-interpretation (bc#75) — no literal-bake fallback
 *
 * Every §8 bundle litedbmodel produces (single-`makeSQL` writes, and read graphs with map/cond
 * relation nodes) is expressible by the straight-line emitter in all 5 languages: the write path
 * is a strictly-sequential single-op plan (fully static), and the read graph's map/cond internals
 * ride the `runPlan` semantics primitive where genuine concurrency exists — still de-interpreted
 * static code, never the old literal-bake+`execute_bundle` interpret path. There is therefore NO
 * shape that falls back to the classic literal endpoint; if bc's straight-line emitter ever
 * rejects a shape it must be ESCALATED to bc (bc#13/#75 capability), never worked around locally.
 */

import { generateModule, type ComponentGraphIR, type GeneratedModule, type Scope, type Value } from 'behavior-contracts';
import type { SqlBundle, SqliteDb } from './runtime';
import { executeBundle } from './runtime';
import { makeSqlComponentIR } from './makesql/ir';
import type { DialectName } from './dialect';
import type { RelationOp } from './relation';
import type { ReadGraph } from './makesql/static-bundle';
import type { TransactionPlan } from './makesql/tx';

/**
 * The languages litedbmodel mode-3 codegen supports — exactly the set bc's shared generator
 * registers (bc#13 SP1 typescript/python + SP2 go/rust/php). {@link codegenEmitterFor}
 * verifies against the LIVE registry so a bc capability drift is caught loudly.
 */
export const CODEGEN_LANGUAGES = ['typescript', 'python', 'go', 'rust', 'php'] as const;
export type CodegenLanguage = (typeof CODEGEN_LANGUAGES)[number];

/**
 * The bc emitter litedbmodel drives for each codegen target: the **de-interpreted TYPED de-box**
 * endpoint. litedbmodel codegen IS static de-interpreted codegen (spec §9) — there is **NO literal /
 * straight-line / interpreter fallback**. An unspec'd fallback is INVALID (it silently swallows the
 * "this shape can't be codegen'd" signal); a shape that cannot de-box MUST fail loudly at generation.
 *
 * This map is the AUTHORITY for "which languages are codegen (de-box) languages" (ts/go/rust). Both
 * of the two typed endpoints below consume the IR's `outType`/`outputType` and are de-interpreted
 * (NO `run_behavior` on the data plane); the difference is only the row-materialization boundary:
 *
 * - go/rust preferred → `<lang>-typed-raw` (bc#76): materialize concrete structs DIRECTLY from the
 *   RAW wire — v1-native alloc profile, no dynamic `Value` boxing. bc's raw ABI currently covers ONLY
 *   the single-componentRef read-handler (row-hydrator) shape; a read graph with `map`/`cond` nodes
 *   is NOT raw-de-boxable, so {@link typedEmitterFor} selects the boxed-input typed path for it.
 * - go/rust map/cond → `<lang>-typed`: the SAME struct-native de-box, but marshalled from a boxed
 *   `Value` handler result instead of the raw wire (bc's raw ABI does not cover map/cond yet). Still
 *   fully typed + de-interpreted — NOT a fallback to the untyped literal/interpreter emitter.
 * - ts → `typescript-typed` (bc#48): one typed endpoint that covers both shapes.
 * - **python/php: NOT present.** bc registers no de-boxed typed endpoint for them (capability limit).
 *   litedbmodel does NOT substitute the literal (≈ir) emitter — that would be an unspec'd fallback.
 *   Codegen for py/php is a bc capability gap that ERRORS (ESCALATE to bc), never a silent literal.
 */
export const CODEGEN_EMITTER: Partial<Record<CodegenLanguage, string>> = {
  typescript: 'typescript-typed',
  go: 'go-typed-raw',
  rust: 'rust-typed-raw',
};

/**
 * The boxed-input TYPED (de-box) emitter for go/rust, used for a read graph whose shape the raw ABI
 * does not cover (a `map`/`cond`-containing read). It is still a typed, de-interpreted struct-native
 * emitter (consumes outType, no `run_behavior` on the data plane); only the marshal boundary differs
 * (a boxed `Value` handler result instead of the raw wire). TS's single `typescript-typed` covers both.
 */
const CODEGEN_EMITTER_TYPED: Partial<Record<CodegenLanguage, string>> = {
  typescript: 'typescript-typed',
  go: 'go-typed',
  rust: 'rust-typed',
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
}

/** The full codegen artifact for one bundle × one language. */
export interface CodegenArtifact {
  /** The target language. */
  readonly language: CodegenLanguage;
  /** The generated behavior module (bc's SHARED STRAIGHT-LINE generator output — real static code). */
  readonly module: GeneratedModule;
  /** The SQL catalog companion sidecar (the STATIC makeSQL execution catalog). */
  readonly companion: SqlCatalogCompanion;
  /** The portable IR the straight-line module was generated FROM (fingerprint-pinned in the code). */
  readonly ir: ComponentGraphIR;
  /** The originating bundle (so the equivalence leg re-executes the SAME static bundle). */
  readonly bundle: SqlBundle;
}

/**
 * The portable {@link ComponentGraphIR} view of a §8 bundle: a read bundle's surrogate IR (each
 * SQL node → a `makeSQL` node), or the single `makeSQL` component IR for a write. The straight-line
 * emitter compiles THIS into static native source; the static SQL catalog rides the companion.
 */
export function bundleToPortableIR(bundle: SqlBundle): ComponentGraphIR {
  if (bundle.readGraph !== undefined) return bundle.readGraph.ir;
  return makeSqlComponentIR(bundle.name);
}

/** The STATIC makeSQL execution catalog carried alongside the straight-line module. */
function companionOf(bundle: SqlBundle): SqlCatalogCompanion {
  const base: SqlCatalogCompanion = {
    dialect: bundle.dialect,
    optionalHeads: bundle.optionalHeads,
    relations: bundle.relations,
    ...(bundle.readGraph !== undefined ? { readGraph: bundle.readGraph } : {}),
    ...(bundle.statement !== undefined ? { statement: bundle.statement } : {}),
  };
  return bundle.transaction === undefined ? base : { ...base, transaction: bundle.transaction };
}

/**
 * Whether a portable IR is RAW-de-boxable by bc's go/rust raw ABI (bc#76): every body node of every
 * component is a plain componentRef (the single-componentRef read-handler / row-hydrator shape). A
 * `map` or `cond` node is NOT covered by the raw ABI yet, so such a read graph must ride the boxed
 * typed path (`-typed`) instead — still typed + de-interpreted, not a fallback. A write bundle's
 * `makeSqlComponentIR` is a single componentRef, so it is raw-shaped (though writes are not codegen'd).
 */
function isRawDeboxable(ir: ComponentGraphIR): boolean {
  return ir.components.every((c) => c.body.every((n) => !('map' in n) && !('cond' in n)));
}

/**
 * Resolve the de-boxed typed emitter for a codegen target + IR shape, or throw. No fallback: a
 * language without a registered de-box endpoint is a bc capability gap that fails LOUDLY (never a
 * silent substitution of the literal/interpreter emitter — an unspec'd fallback is invalid).
 *
 * The go/rust choice is SHAPE-AWARE: a raw-de-boxable read (single-componentRef nodes) takes the
 * RAW ABI (`-typed-raw`, full wire de-box); a `map`/`cond`-containing read takes the boxed-input
 * typed path (`-typed`) — bc's raw ABI does not cover map/cond. BOTH are typed + de-interpreted; the
 * `-typed` path is NOT a degrade to the untyped/literal/interpreter emitter. TS uses one typed endpoint.
 */
export function typedEmitterFor(language: string, ir: ComponentGraphIR, registered: readonly string[]): string {
  if (!(CODEGEN_LANGUAGES as readonly string[]).includes(language)) {
    throw new Error(
      `litedbmodel codegen: language '${language}' is not a supported target (supported: ${CODEGEN_LANGUAGES.join(', ')})`,
    );
  }
  const raw = CODEGEN_EMITTER[language as CodegenLanguage];
  if (raw === undefined) {
    throw new Error(
      `litedbmodel codegen: no de-boxed typed endpoint for '${language}'. litedbmodel codegen is STATIC ` +
        `de-interpreted codegen (spec §9) with NO literal/interpreter fallback (an unspec'd fallback is invalid). ` +
        `bc registers de-box endpoints only for ${Object.keys(CODEGEN_EMITTER).join('/')} — a '${language}' typed ` +
        `endpoint is a bc capability gap. ESCALATE to bc (bc#22 pattern); never substitute the literal emitter.`,
    );
  }
  // Pick the raw ABI for a raw-de-boxable shape; otherwise the boxed typed de-box (map/cond reads).
  const emitter = isRawDeboxable(ir) ? raw : (CODEGEN_EMITTER_TYPED[language as CodegenLanguage] as string);
  if (!registered.includes(emitter)) {
    throw new Error(
      `litedbmodel codegen: bc's shared generator does not register the '${emitter}' emitter for '${language}' ` +
        `(bc registered: ${[...registered].sort().join(', ')}). ESCALATE to bc (bc#22 pattern), never fork locally.`,
    );
  }
  return emitter;
}

/**
 * Shape-INDEPENDENT emitter resolution (the raw-preferred endpoint), kept for callers that only need
 * to know a language's de-box endpoint identity without an IR in hand. Prefer {@link typedEmitterFor}
 * for actual generation (it picks the raw vs boxed typed path from the IR shape).
 */
export function codegenEmitterFor(language: string, registered: readonly string[]): string {
  if (!(CODEGEN_LANGUAGES as readonly string[]).includes(language)) {
    throw new Error(
      `litedbmodel codegen: language '${language}' is not a supported target (supported: ${CODEGEN_LANGUAGES.join(', ')})`,
    );
  }
  const emitter = CODEGEN_EMITTER[language as CodegenLanguage];
  if (emitter === undefined) {
    throw new Error(
      `litedbmodel codegen: no de-boxed typed endpoint for '${language}'. litedbmodel codegen is STATIC ` +
        `de-interpreted codegen (spec §9) with NO literal/interpreter fallback (an unspec'd fallback is invalid). ` +
        `bc registers de-box endpoints only for ${Object.keys(CODEGEN_EMITTER).join('/')} — a '${language}' typed ` +
        `endpoint is a bc capability gap. ESCALATE to bc (bc#22 pattern); never substitute the literal emitter.`,
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
 * Generate the mode-3 codegen artifact for ONE §8 bundle in ONE target language: bc's SHARED
 * STRAIGHT-LINE generator emits REAL static native source (de-interpreted, bc#75 — no baked-IR
 * interpret path) + `bind(handlers)`, and we attach the STATIC SQL catalog companion. litedbmodel
 * supplies the input (portable IR + catalog); bc owns the emitter.
 */
export function generateCodegenArtifact(
  bundle: SqlBundle,
  language: string,
  registeredLanguages: readonly string[],
  runtimeImport?: string,
): CodegenArtifact {
  // Resolve the de-box endpoint (shape-aware: raw ABI vs boxed typed) or throw. No literal/straight-line
  // fallback — an unspec'd fallback is invalid; both endpoints are typed + de-interpreted.
  const ir = bundleToPortableIR(bundle);
  const emitter = typedEmitterFor(language, ir, registeredLanguages);
  const module = generateModule(ir, runtimeImport === undefined ? { language: emitter } : { language: emitter, runtimeImport });
  return { language: language as CodegenLanguage, module, companion: companionOf(bundle), ir, bundle };
}

/**
 * The TS codegen EXECUTION path used to PROVE behavior-identity: a codegen runtime reads the SQL
 * catalog companion (the STATIC makeSQL bundle) and evaluates skip + value-specs → assemble →
 * render → execute — which is EXACTLY {@link executeBundle} over the SAME bundle.
 */
export function codegenExecuteBundleForTest(artifact: CodegenArtifact, input: Scope, db: SqliteDb): Value {
  return executeBundle(artifact.bundle, input, { db });
}
