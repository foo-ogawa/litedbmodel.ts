/**
 * litedbmodel v2 SCP — mode-3 codegen (WS7f, #35; spec §9 exec-mode 3 / §10 / §11).
 *
 * The static-codegen execution mode ("Codegen・静的（全言語）— IR → 各言語ソース生成
 * runtime≈0, 入力は可搬IRのみ, bc#13 共有 generator に SQL catalog を供給" — spec §9). It
 * takes a §8 {@link SqlBundle} (already Backend-Compiled TS-side, WS1↔WS3) and, per target
 * language, emits:
 *
 *  1. **Behavior module** — bc's SHARED generator (`generateModule`, bc#13) run over the
 *     bundle's SURROGATE component wrapped as a portable {@link ComponentGraphIR}. The generator
 *     bakes that IR as a language-native LITERAL (no runtime JSON parse) and emits a `bind(handlers)`
 *     accessor that delegates to the EXISTING runtime-core `runBehavior`. litedbmodel writes NO
 *     execution logic here — codegen is upstream-owned (bc), we only SUPPLY the input.
 *  2. **SQL catalog companion** — a pure-JSON sidecar carrying the litedbmodel-specific bundle
 *     fields that are OUTSIDE bc's portable-IR vocabulary (`operations` / `dialect` /
 *     `optionalHeads` / `relations` / `transaction`). This is the "SQL catalog supplied to the
 *     shared generator" (§9): a codegen consumer's thin SQL-handler layer reads it to render →
 *     execute, exactly as the mode-2 thin-runtime handler does. It is byte-identical to the bundle
 *     minus the (now baked-as-literal) `component`/`irVersion`/`exprVersion`.
 *
 * ## Why this is byte-identical to the thin-runtime (mode-2), by construction
 *
 * The mode-2 thin-runtime ({@link executeBundle}) computes
 * `runBehavior({components:[bundle.component]}, buildHandlers(db, operations, dialect),
 * normalizeInput(...), name)`. The generated module bakes `IR === bundle.component` (proven by
 * the FNV-1a fingerprint the generator embeds and re-checks at load) and its `bind(handlers)`
 * calls the SAME `runBehavior(IR, handlers, input, name)`. So when a codegen runtime pairs the
 * generated `bind` with the SAME {@link buildHandlers} + {@link normalizeInput} (from the SQL
 * catalog companion), it drives the identical core over the identical IR and handlers — the SQL
 * text (all dialects) and the results are identical, NOT approximately equal. The TS leg PROVES
 * this by executing the generated module and asserting exact equality vs `executeBundle` AND vs
 * the frozen conformance vector (see `codegenExecuteBundleForTest`).
 *
 * ## Language support (bc#13 capability — the honest scope boundary)
 *
 * bc's shared generator registers emitters for FIVE languages
 * ({@link CODEGEN_LANGUAGES}); Go IS among them (bc#13 SP2 `emit-go.ts`). So litedbmodel mode-3
 * codegen covers all five — TS/Python/Go/Rust/PHP. If a future litedbmodel construct needed an
 * emitter bc does not provide, the honest move is to ESCALATE to bc (bc#22 pattern), never to
 * fork a parallel litedbmodel-local generator (that would defeat the "codegen は上流所有" design).
 */

import { generateModule, type ComponentGraphIR, type GeneratedModule, type Scope, type Value } from 'behavior-contracts';
import type { SqlBundle, SqliteDb } from './runtime';
import { buildHandlers, normalizeInput } from './runtime';
import { runBehavior } from 'behavior-contracts';
import { dialectFor, type DialectName } from './dialect';
import type { CompiledOperation } from './ir';
import type { RelationOp } from './relation';
import type { TransactionPlan } from './write-plan';

/**
 * The languages litedbmodel mode-3 codegen supports — exactly the set bc's shared generator
 * registers (bc#13 SP1 typescript/python + SP2 go/rust/php). This is a FIXED capability the
 * generator advertises via `registeredLanguages()`; {@link assertLanguageSupported} verifies the
 * requested language against the LIVE registry so a bc capability change is caught loudly rather
 * than silently drifting from this constant.
 */
export const CODEGEN_LANGUAGES = ['typescript', 'python', 'go', 'rust', 'php'] as const;
export type CodegenLanguage = (typeof CODEGEN_LANGUAGES)[number];

/**
 * The SQL catalog companion (spec §9 — the "SQL catalog supplied to the shared generator"): the
 * litedbmodel-specific §8 bundle fields that live OUTSIDE bc's portable-IR vocabulary and so are
 * NOT baked into the generated behavior module. A codegen consumer's thin SQL-handler layer reads
 * this sidecar to render → bind → execute (identically to the mode-2 handler). Pure JSON —
 * round-trips losslessly (proven by the codegen conformance leg).
 */
export interface SqlCatalogCompanion {
  /** Backend-Compiled SQL IR per catalog node id (§8) — `sql` + fragment tree + closed-set params. */
  readonly operations: Record<string, CompiledOperation>;
  /** Target SQL dialect (`sqlite`/`postgres`/`mysql`) — compiled once, TS-side (spec §10). */
  readonly dialect: DialectName;
  /** Input heads normalized to present-as-null (absent-key SKIP) — mirrors the bundle. */
  readonly optionalHeads: readonly string[];
  /** Pre-compiled read-relation batch ops (spec §5/§8), keyed by relation name. */
  readonly relations: Record<string, RelationOp>;
  /** Derived gate-first write-time-relations transaction plan (spec §6/§8), for a Command bundle. */
  readonly transaction?: TransactionPlan;
}

/** The full codegen artifact for one bundle × one language. */
export interface CodegenArtifact {
  /** The target language. */
  readonly language: CodegenLanguage;
  /** The generated behavior module (bc's shared generator output — IR baked as a native literal). */
  readonly module: GeneratedModule;
  /** The SQL catalog companion sidecar (the litedbmodel-specific execution catalog). */
  readonly companion: SqlCatalogCompanion;
  /**
   * The portable IR that WAS baked into {@link module} (the surrogate component + envelope). Kept
   * on the artifact so the in-process TS equivalence check drives the IDENTICAL IR the emitted
   * source embeds — the generator's fingerprint (checked at the emitted module's load) proves the
   * baked literal equals this object.
   */
  readonly ir: ComponentGraphIR;
}

/**
 * The portable {@link ComponentGraphIR} view of a §8 bundle: the surrogate component (wiring /
 * plan / output / `__scope` ports only — every SQL-structural port already collapsed to portable
 * Expression IR TS-side) wrapped with the bundle's IR envelope. This is EXACTLY the IR
 * {@link executeBundle} feeds `runBehavior`, so baking it is what makes codegen equivalent.
 */
export function bundleToPortableIR(bundle: SqlBundle): ComponentGraphIR {
  return {
    irVersion: bundle.irVersion as 1,
    exprVersion: bundle.exprVersion,
    components: [bundle.component],
  };
}

/** The litedbmodel-specific execution catalog carried alongside the (baked) portable IR. */
function companionOf(bundle: SqlBundle): SqlCatalogCompanion {
  const base: SqlCatalogCompanion = {
    operations: bundle.operations,
    dialect: bundle.dialect,
    optionalHeads: bundle.optionalHeads,
    relations: bundle.relations,
  };
  return bundle.transaction === undefined ? base : { ...base, transaction: bundle.transaction };
}

/**
 * Assert the requested language is one bc's shared generator can actually emit — checked against
 * the LIVE generator registry, not just the {@link CODEGEN_LANGUAGES} constant, so a bc capability
 * drift (an emitter removed/renamed upstream) fails LOUDLY here instead of surfacing as a broken
 * generated module. `generateModule` itself also fail-closes on an unknown language; this is the
 * earlier, litedbmodel-scoped guard with the honest "bc supports X" message.
 */
export function assertLanguageSupported(language: string, registered: readonly string[]): asserts language is CodegenLanguage {
  if (!(CODEGEN_LANGUAGES as readonly string[]).includes(language)) {
    throw new Error(
      `litedbmodel codegen: language '${language}' is not a supported target (supported: ${CODEGEN_LANGUAGES.join(', ')})`,
    );
  }
  if (!registered.includes(language)) {
    throw new Error(
      `litedbmodel codegen: bc's shared generator does not register an emitter for '${language}' ` +
        `(bc registered: ${[...registered].sort().join(', ')}). This is a bc#13 capability limit — ESCALATE to bc (bc#22 pattern), ` +
        `never fork a litedbmodel-local generator (codegen は上流所有, spec §9).`,
    );
  }
}

/**
 * Generate the mode-3 codegen artifact for ONE §8 bundle in ONE target language: bc's SHARED
 * generator bakes the surrogate IR literal + emits `bind(handlers)`, and we attach the SQL catalog
 * companion. litedbmodel supplies the input (IR + catalog); bc owns the emitter.
 *
 * @param registeredLanguages the live `registeredLanguages()` from bc (injected so the guard sees
 *   the actual generator capability; the caller passes `bc.registeredLanguages()`).
 * @param runtimeImport optional runtime-core import specifier override (test/vendored layouts);
 *   forwarded verbatim to the generator so the generated code stays deterministic.
 */
export function generateCodegenArtifact(
  bundle: SqlBundle,
  language: string,
  registeredLanguages: readonly string[],
  runtimeImport?: string,
): CodegenArtifact {
  assertLanguageSupported(language, registeredLanguages);
  const ir = bundleToPortableIR(bundle);
  const module = generateModule(ir, runtimeImport === undefined ? { language } : { language, runtimeImport });
  return { language, module, companion: companionOf(bundle), ir };
}

/**
 * The TS codegen EXECUTION path used to PROVE byte-identity: reconstruct the exact call the
 * generated TS module's `bind(handlers)` makes — `runBehavior(IR, handlers, normalizedInput,
 * name)` — where `IR` is the baked portable IR and `handlers` are the SAME SQL handlers the SQL
 * catalog companion drives ({@link buildHandlers}). Executing this and comparing to
 * {@link executeBundle} is a REAL byte-identity check on the codegen path (the generated TS module
 * is separately IMPORTED and run in the conformance leg to prove the emitted source itself agrees).
 *
 * This mirrors {@link executeBundle} field-for-field, but drives the components/handlers via the
 * codegen ARTIFACT (baked IR + companion) rather than the raw bundle — so a divergence between the
 * codegen artifact and the bundle would surface as a mismatch.
 */
export function codegenExecuteBundleForTest(artifact: CodegenArtifact, input: Scope, db: SqliteDb): Value {
  const { ir } = artifact;
  const component = ir.components[0];
  const { operations, dialect, optionalHeads } = artifact.companion;
  const handlers = buildHandlers(db, operations, dialectFor(dialect));
  const normalized = normalizeInput(component, new Set(optionalHeads), input);
  return runBehavior(ir, handlers, normalized, component.name);
}
