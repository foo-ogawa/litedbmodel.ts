/**
 * litedbmodel v2 SCP — mode-3 codegen (WS7f, #35; spec §9 exec-mode 3 / §10 / §11; makeSQL flip,
 * epic #43/#45 Phase B).
 *
 * The static-codegen execution mode ("Codegen・静的（全言語）— IR → 各言語ソース生成 runtime≈0,
 * 入力は可搬IRのみ, bc#13 共有 generator に SQL catalog を供給" — spec §9). It takes a §8 STATIC
 * makeSQL {@link SqlBundle} and, per target language, emits:
 *
 *  1. **Behavior module** — bc's SHARED generator (`generateModule`, bc#13) run over the bundle's
 *     portable IR (a read bundle's surrogate `ComponentGraphIR`, or the `makeSQL` component IR for
 *     a write), baked as a language-native LITERAL with a `bind(handlers)` accessor delegating to
 *     `runBehavior`. litedbmodel writes NO execution logic here — codegen is upstream-owned (bc).
 *  2. **SQL catalog companion** — the pure-JSON STATIC makeSQL catalog (per-node statement
 *     templates / relations / transaction). A codegen consumer's thin SQL layer reads it to
 *     evaluate skip + value-specs → assemble → render → execute (identically to the mode-2 runtime).
 *
 * ## Byte-identical to the thin-runtime (mode-2), by construction
 *
 * The companion IS the bundle, so a codegen runtime that evaluates it drives the IDENTICAL
 * static-makeSQL render/execute path {@link executeBundle} uses — SQL text (all dialects) and
 * results are identical, not approximately equal. The TS leg PROVES this by executing via the
 * artifact and asserting exact equality vs {@link executeBundle} (see {@link codegenExecuteBundleForTest}).
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
  /** The generated behavior module (bc's shared generator output — IR baked as a native literal). */
  readonly module: GeneratedModule;
  /** The SQL catalog companion sidecar (the STATIC makeSQL execution catalog). */
  readonly companion: SqlCatalogCompanion;
  /** The portable IR baked into {@link module}. */
  readonly ir: ComponentGraphIR;
  /** The originating bundle (so the equivalence leg re-executes the SAME static bundle). */
  readonly bundle: SqlBundle;
}

/**
 * The portable {@link ComponentGraphIR} view of a §8 bundle: a read bundle's surrogate IR (each
 * SQL node → a `makeSQL` node), or the single `makeSQL` component IR for a write. Baking THIS is
 * what makes the generated module a genuine bc artifact; the static SQL catalog rides the companion.
 */
export function bundleToPortableIR(bundle: SqlBundle): ComponentGraphIR {
  if (bundle.readGraph !== undefined) return bundle.readGraph.ir;
  return makeSqlComponentIR(bundle.name);
}

/** The STATIC makeSQL execution catalog carried alongside the (baked) portable IR. */
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
 * Assert the requested language is one bc's shared generator can emit — checked against the LIVE
 * registry, not just the {@link CODEGEN_LANGUAGES} constant, so a bc capability drift fails LOUDLY.
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
 * generator bakes the IR literal + emits `bind(handlers)`, and we attach the STATIC SQL catalog
 * companion. litedbmodel supplies the input (IR + catalog); bc owns the emitter.
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
  return { language, module, companion: companionOf(bundle), ir, bundle };
}

/**
 * The TS codegen EXECUTION path used to PROVE byte-identity: a codegen runtime reads the SQL
 * catalog companion (the STATIC makeSQL bundle) and evaluates skip + value-specs → assemble →
 * render → execute — which is EXACTLY {@link executeBundle} over the SAME bundle.
 */
export function codegenExecuteBundleForTest(artifact: CodegenArtifact, input: Scope, db: SqliteDb): Value {
  return executeBundle(artifact.bundle, input, { db });
}
