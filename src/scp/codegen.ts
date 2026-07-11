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
 * registers (bc#13 SP1 typescript/python + SP2 go/rust/php). {@link assertLanguageSupported}
 * verifies against the LIVE registry so a bc capability drift is caught loudly.
 */
export const CODEGEN_LANGUAGES = ['typescript', 'python', 'go', 'rust', 'php'] as const;
export type CodegenLanguage = (typeof CODEGEN_LANGUAGES)[number];

/**
 * The bc emitter language litedbmodel drives for each logical target: the **straight-line**
 * (de-interpreted, bc#75) endpoint, NOT the classic literal endpoint. The literal endpoint
 * (`typescript`/`go`/…) bakes the IR as a native literal and still runs it through the
 * interpreter (`runBehavior`) — that is `≈ ir`, not real static code. The `<lang>-straightline`
 * endpoint emits STATIC native source (no `RunPlan`/scope snapshot/closure/op-id search/embedded
 * IR for sequential shapes). litedbmodel's codegen mode IS static codegen (spec §9), so it MUST
 * use this endpoint.
 */
// bc 0.3.0: rust/go/ts straight-line は本物 native 脱解釈。**python/php の straight-line は
// UNSUPPORTED**（bc emit が loud に拒否）— それらは ir/interpret 経路を使うので codegen artifact は
// LITERAL emitter（IR bake + run_behavior ＝ `≈ ir`）に留める。py/php に static-codegen 加速は無い
// （honest な非対称。sham ではない）。
export const STRAIGHTLINE_EMITTER: Record<CodegenLanguage, string> = {
  typescript: 'typescript-straightline',
  go: 'go-straightline',
  rust: 'rust-straightline',
  python: 'python',
  php: 'php',
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
 * Assert the requested language is one bc's shared generator can emit as STATIC straight-line
 * code — checked against the LIVE registry (the `<lang>-straightline` emitter must be registered),
 * not just the {@link CODEGEN_LANGUAGES} constant, so a bc capability drift fails LOUDLY.
 */
export function assertLanguageSupported(language: string, registered: readonly string[]): asserts language is CodegenLanguage {
  if (!(CODEGEN_LANGUAGES as readonly string[]).includes(language)) {
    throw new Error(
      `litedbmodel codegen: language '${language}' is not a supported target (supported: ${CODEGEN_LANGUAGES.join(', ')})`,
    );
  }
  const emitter = STRAIGHTLINE_EMITTER[language as CodegenLanguage];
  if (!registered.includes(emitter)) {
    throw new Error(
      `litedbmodel codegen: bc's shared generator does not register the STRAIGHT-LINE emitter '${emitter}' ` +
        `for '${language}' (bc registered: ${[...registered].sort().join(', ')}). litedbmodel codegen is STATIC ` +
        `codegen (spec §9), so the de-interpreted straight-line endpoint is required — this is a bc#13/#75 ` +
        `capability limit — ESCALATE to bc (bc#22 pattern), never fork a litedbmodel-local generator (codegen は上流所有, spec §9).`,
    );
  }
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
  assertLanguageSupported(language, registeredLanguages);
  const ir = bundleToPortableIR(bundle);
  const emitter = STRAIGHTLINE_EMITTER[language];
  const module = generateModule(ir, runtimeImport === undefined ? { language: emitter } : { language: emitter, runtimeImport });
  return { language, module, companion: companionOf(bundle), ir, bundle };
}

/**
 * The TS codegen EXECUTION path used to PROVE behavior-identity: a codegen runtime reads the SQL
 * catalog companion (the STATIC makeSQL bundle) and evaluates skip + value-specs → assemble →
 * render → execute — which is EXACTLY {@link executeBundle} over the SAME bundle.
 */
export function codegenExecuteBundleForTest(artifact: CodegenArtifact, input: Scope, db: SqliteDb): Value {
  return executeBundle(artifact.bundle, input, { db });
}
