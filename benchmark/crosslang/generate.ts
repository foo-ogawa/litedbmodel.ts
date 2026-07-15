// ════════════════════════════════════════════════════════════════════════════
// Artifact generation (epic #44; #60 milestone 1 — typed-NATIVE READ codegen) —
// compile the 8 shared-domain behaviors into makeSQL bundles (PER DIALECT) + the
// TRUE bc-generated codegen modules, emitting the ONE generated artifact every
// language's ir / codegen cells consume.
// ════════════════════════════════════════════════════════════════════════════
//
// 🔒 CONSUME-ONLY: this uses the litedbmodel public compile/codegen API (dist/scp)
// to PRODUCE artifacts; it does NOT modify src. The output `generated/bundles.json`
// is the language-neutral §8 published artifact (pure JSON) that Python / Rust /
// PHP / Go load and execute via their thin runtimes — exactly the ir path.
//
// TWO validity-driven axes vs the original single-dialect artifact (#44 owner gaps):
//   1. PER-DIALECT bundles — the SAME 8 behaviors are compiled for `sqlite`,
//      `postgres`, AND `mysql`. Each renders DIFFERENT SQL/placeholder/array forms
//      (`?`→`$N` for PG, single-JSON-array IN-list for MySQL/SQLite), so the ir/
//      codegen CLIENT-PATH cost is reported per dialect, not just SQLite.
//   2. TRUE codegen modules — READ cases ONLY (#60 m1: writes are NOT codegen-module
//      cases anymore — see below). `generateCodegenArtifact` runs bc's SHARED
//      generator over each read bundle's LOWERED portable IR: go/rust drive bc's
//      typed-NATIVE endpoint (bc#77/#90, RUNTIME-FREE — zero boxing); ts stays on
//      the boxed `typescript-typed` endpoint (no typed-native counterpart yet).
//      typed-native fails CLOSED on an uncovered read shape (an IN-list's
//      array-typed head — `complexWhere`/`inList` — bc#86 gap): such a case is
//      SKIPPED for go/rust (reported, not silently substituted) while TS still
//      covers it. WRITE cases (`batchInsert`/`writeTxGate`) stay on the existing
//      write/tx execution path (`executeTransactionBundle` — the native adapters'
//      hand-written tx mirror) — NOT a codegen module, boxed or typed-raw.

import { writeFileSync, mkdirSync, rmSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import * as lm from '../../dist/scp/index.mjs';
import { registeredLanguages } from 'behavior-contracts';
import {
  SCHEMA,
  INPUTS,
  SQL_BASELINE,
  READ_ENTRY,
  READ_RELATION,
  readsContract,
  writesContract,
  writeGateContract,
  CROSSLANG_CASE_IDS_LOCAL,
} from './domain.js';
import { CROSSLANG_DIALECTS, type CrosslangDialect } from './contract.js';
import { assertBcVersionsAligned } from './check-versions.js';
import { decodeNativeCase, rustCompanionSource, goCompanionSource, type NCase } from './native-companion.js';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(HERE, '..', '..');

// The column-type SoT (spec §4.1): the INLINE `static columns` declaration on the model (issue #59),
// precomputed into a `(table, column) → SQL type` resolver on the contract at registration. The SAME
// declared types drive both the codegen `outType`/`outputType` (here) and the TS read-path de-box —
// no external DDL. `compileBundle` consults it to type each read node so bc's typed-raw de-box
// endpoint emits concrete row structs. Unknown/undeclared columns THROW (no-assume, no-fallback).
const COLUMN_TYPES = readsContract.resolveColumnType!;

// Languages whose codegen MUST be de-interpreted native code (bc straight-line/typed — no
// interpreter delegation): `delegatesToRunBehavior` MUST be false. python/php intentionally use
// the ir/interpret exec surface (bc 0.3.0: their straight-line codegen is UNSUPPORTED), so their
// codegen artifact is the LITERAL emitter (`delegatesToRunBehavior=true`) and is NOT asserted here.
const NATIVE_CODEGEN_LANGS = ['typescript', 'go', 'rust'] as const;
// go/rust specifically drive bc's typed-NATIVE endpoint (bc#77/#90, RUNTIME-FREE — #60 m1); ts has
// no typed-native counterpart yet (stays on the boxed `typescript-typed` endpoint).
const NATIVE_ONLY_LANGS = ['go', 'rust'] as const;

/**
 * Fail-closed: assert every native-codegen language produced genuinely de-interpreted code for
 * every case it COVERS (no silent fallback to the boxed literal/interpreter path). Codifies the
 * "codegen silently ≈ ir" trap so an invalid perf comparison ERRORS instead of being measured. A
 * case a native lang does NOT cover (see `NativeCoverage` below) is not part of THIS gate — its
 * absence is reported separately, never silently substituted with a delegating fallback.
 */
function assertCodegenSurface(codegen: Record<string, { delegatesToRunBehavior: boolean }>): void {
  const sham = NATIVE_CODEGEN_LANGS.filter((l) => codegen[l]?.delegatesToRunBehavior !== false);
  if (sham.length > 0) {
    throw new Error(
      `cross-lang bench PREFLIGHT: codegen surface INVALID — ${sham.join(', ')} delegate(s) to the ` +
        `interpreter (run_behavior) instead of emitting de-interpreted native code. The perf comparison ` +
        `would be a sham (codegen ≈ ir). Fix the emitter endpoint (src/scp/codegen.ts CODEGEN_EMITTER) ` +
        `or the bc version. python/php are exempt (ir/interpret surface by design).`,
    );
  }
}
const OUT = resolve(HERE, 'generated', 'bundles.json');
// The TRUE bc-generated codegen module per language (source text), consumed by the
// codegen cells. Written next to the JSON artifact; committed + drift-checked.
const CODEGEN_DIR = resolve(HERE, 'generated', 'codegen');
// The Go generated modules are ALSO materialized as per-case Go PACKAGES *inside the go module*
// (`go/lm_bench/cgmods/<case>/gen.go`, `package cg_<case>`) — Go cannot import a package from
// outside its module tree, and the 8 flat `package behaviors` files (duplicate top-level decls)
// cannot compile together. The bench's codegen cell imports these per-case packages and executes
// THROUGH each module's `Bind(handler)[entry](input)`. Gitignored + regenerated (build output).
const GO_CGMODS_DIR = resolve(HERE, '..', '..', 'go', 'lm_bench', 'cgmods');
const GO_CASE_PKG = (caseId: string) => `cg_${caseId.replace(/[^A-Za-z0-9_]/g, '_')}`;
// The NATIVE pre-decoded codegen execution companions (owner order: the codegen path carries NO IR
// data and parses NO JSON at execution time). Emitted next to the generated modules: the Rust
// codegen binary `#[path]`-includes companion.rs; the Go codegen cell imports the cgplans package.
// Gitignored + regenerated (build output), like the modules themselves.
const RUST_COMPANION = resolve(CODEGEN_DIR, 'rust', 'companion.rs');
const GO_CGPLANS_DIR = resolve(HERE, '..', '..', 'go', 'lm_bench', 'cgplans');

// bc's shared generator supports exactly these litedbmodel-facing languages (the READ codegen
// endpoints — #60 m1). Each cell's codegen module is the generator output for that language over
// the READ bundle's (lowered, for go/rust) portable IR. Codegen-MODULE langs = ts/go/rust; python/
// php are the ir/interpret surface — they have NO codegen module (a DECLARED spec choice, not a
// fallback). WRITE cases (batchInsert/writeTxGate) are NOT codegen-module cases for ANY language
// anymore (#60 m1) — they stay on the existing write/tx execution path.
const CODEGEN_LANGS = ['typescript', 'go', 'rust'] as const;
type CodegenLang = (typeof CODEGEN_LANGS)[number];
const CODEGEN_EXT: Record<CodegenLang, string> = {
  typescript: 'ts', go: 'go', rust: 'rs',
};
// The read case ids (excludes the 2 write cases — batchInsert/writeTxGate are NOT codegen-module
// cases, #60 m1).
const READ_CASE_IDS = CROSSLANG_CASE_IDS_LOCAL.filter((c) => c !== 'batchInsert' && c !== 'writeTxGate');

// Per-(language × case) generated-module path. TS lands as a real `.ts` (tsx imports
// it directly); the other languages land under their own subdir as EVIDENCE of the
// true generated-code artifact (their runtimes consume it if wired).
function codegenFilePath(lang: CodegenLang, caseId: string): string {
  return resolve(CODEGEN_DIR, lang, `${caseId}.${CODEGEN_EXT[lang]}`);
}

// Seed spec as language-neutral INSERT statements, so every language's ir cell
// seeds an IDENTICAL in-memory SQLite (same dataset the TS cells use).
function seedStatements(): string[] {
  const stmts: string[] = [];
  for (let u = 1; u <= 8; u++) stmts.push(`INSERT INTO users (id, name, post_count) VALUES (${u}, 'user-${u}', 5)`);
  let pid = 0;
  let cid = 0;
  for (let u = 1; u <= 8; u++) {
    for (let k = 0; k < 5; k++) {
      pid++;
      const status = k % 3 === 0 ? 'live' : k % 3 === 1 ? 'draft' : 'live';
      const day = String(k + 1).padStart(2, '0');
      stmts.push(
        `INSERT INTO posts (id, author_id, title, status, views, created_at) VALUES (${pid}, ${u}, 'post-${pid}', '${status}', ${pid * 10}, '2026-02-${day}')`,
      );
      for (let c = 0; c < 5; c++) {
        cid++;
        stmts.push(`INSERT INTO comments (id, post_id, body, created_at) VALUES (${cid}, ${pid}, 'comment-${cid}', '2026-03-01')`);
      }
    }
  }
  return stmts;
}

interface CaseArtifact {
  case: string;
  kind: 'read' | 'relation' | 'batch' | 'tx';
  entry?: string;
  withRelation?: string;
  relation?: unknown;
  bundle: unknown;
  input: unknown;
  expectedQueries: number;
  expectedRows: number;
}

// Clone a relation decl with the target dialect so the batch op renders the correct
// placeholder/array form (PG `= ANY($1::type[])` vs MySQL/SQLite single-JSON param).
function relForDialect(decl: any, dialect: CrosslangDialect): any {
  return { ...decl, dialect };
}

function buildBundle(
  caseId: string,
  dialect: CrosslangDialect,
): { bundle: any; kind: CaseArtifact['kind']; entry?: string; withRelation?: string; relation?: unknown } {
  if (caseId === 'batchInsert') {
    const cols = ['author_id', 'title', 'status', 'views', 'created_at'];
    // The schema/DDL (SCHEMA) is the column-type SoT (spec §4.1): it types the write's
    // TransactionResult output (entity/returnedRows rows) so bc's typed-raw de-box engages for the
    // WRITE codegen module too (concrete result struct, no dynamic Value boxing). Unknown → THROW.
    const bundle = lm.compileCreateManyBundle(
      'BatchInsert',
      { tableName: 'posts', columns: cols, records: INPUTS.batchInsert.rows as any },
      dialect,
      COLUMN_TYPES,
    );
    return { bundle, kind: 'batch' };
  }
  if (caseId === 'writeTxGate') {
    const bundle = lm.compileWriteBundle(writesContract, 'Create', writeGateContract, 'create', dialect, COLUMN_TYPES);
    return { bundle, kind: 'tx' };
  }
  const rel = READ_RELATION[caseId];
  if (rel) {
    const decl = relForDialect(rel.decl, dialect);
    // The schema/DDL (SCHEMA) is the column-type SoT (spec §4.1): it types each read node's SELECT
    // projection so bc's typed-raw de-box engages (concrete row structs, no dynamic Value boxing).
    const bundle = lm.compileBundle(readsContract, READ_ENTRY[caseId], [decl], dialect, undefined, COLUMN_TYPES);
    return { bundle, kind: 'relation', entry: READ_ENTRY[caseId], withRelation: rel.withName, relation: decl };
  }
  const bundle = lm.compileBundle(readsContract, READ_ENTRY[caseId], [], dialect, undefined, COLUMN_TYPES);
  return { bundle, kind: 'read', entry: READ_ENTRY[caseId] };
}

// Build the per-dialect case list (the 8 bundles compiled for one dialect).
function buildDialect(dialect: CrosslangDialect): CaseArtifact[] {
  const cases: CaseArtifact[] = [];
  for (const caseId of CROSSLANG_CASE_IDS_LOCAL) {
    const { bundle, kind, entry, withRelation, relation } = buildBundle(caseId, dialect);
    const base = SQL_BASELINE[caseId];
    cases.push({
      case: caseId,
      kind,
      ...(entry ? { entry } : {}),
      ...(withRelation ? { withRelation } : {}),
      ...(relation ? { relation } : {}),
      bundle,
      input: (INPUTS as any)[caseId] ?? {},
      expectedQueries: base.queries,
      expectedRows: base.rows,
    });
  }
  return cases;
}

// The TRUE generated codegen module for one language: bc's shared generator run over
// EACH case bundle's portable IR (sqlite dialect — the codegen module bakes the IR /
// wiring which is dialect-INVARIANT; the SQL catalog / dialect render rides the
// companion bundle, exactly as mode-3 specifies). We concatenate per-case generated
// modules into one file, but the load-bearing fact is that the codegen cell IMPORTS
// and BINDS this generated source, not that it lives in one file.
interface CodegenModuleArtifact {
  language: string;
  // For TS we emit an executable module string the cell compiles + binds. For the
  // other languages the generated source is emitted to disk as EVIDENCE (the true
  // generated-code artifact) + the cell consumes it if its toolchain wires it.
  filenameHint: string;
  // Whether the generated module still delegates to the shared interpreter
  // (`runBehavior`) — TRUE at this bc version (interpreter-transcription, bc#75
  // pending). Surfaced so the report discloses codegen≈ir honestly.
  delegatesToRunBehavior: boolean;
}

// Generate ONE module per (language × case). The bc generator emits a full standalone
// module (imports + IR literal + bind), so cases CANNOT be concatenated into one file
// (duplicate top-level declarations); each case is its own compile unit. The codegen
// cell loads + binds each case module individually.
// Strip line/block comments and string/docstring literals across the 5 target languages
// (C-style `//` + `/* */`, `#` line comments for Python/PHP, `'`/`"`/backtick strings and
// Python triple-quoted docstrings), then test whether the interpreter symbol survives as a
// genuine CODE reference. Used by the anti-sham gate so a de-interpreted module whose COMMENTS
// merely mention `run_behavior` is not misreported as delegating.
function codeMentionsInterpreter(src: string): boolean {
  let out = '';
  let i = 0;
  const n = src.length;
  while (i < n) {
    const c = src[i];
    const c2 = src.slice(i, i + 2);
    const c3 = src.slice(i, i + 3);
    if (c2 === '/*') {
      const end = src.indexOf('*/', i + 2);
      i = end < 0 ? n : end + 2;
      continue;
    }
    if (c2 === '//' || c === '#') {
      const end = src.indexOf('\n', i);
      i = end < 0 ? n : end;
      continue;
    }
    if (c3 === "'''" || c3 === '"""') {
      const q = c3;
      const end = src.indexOf(q, i + 3);
      i = end < 0 ? n : end + 3;
      continue;
    }
    if (c === "'" || c === '"' || c === '`') {
      const q = c;
      i += 1;
      while (i < n && src[i] !== q) {
        if (src[i] === '\\') i += 2;
        else i += 1;
      }
      i += 1;
      continue;
    }
    out += c;
    i += 1;
  }
  return /runBehavior|run_behavior|RunBehavior/.test(out);
}

/** A read case a native (go/rust) language did NOT cover — a bc#86 typed-native coverage gap,
 * reported explicitly (never silently regenerated on a boxed/typed-raw fallback). */
interface UncoveredCase {
  caseId: string;
  reason: string;
}

function generateCodegen(
  cases: CaseArtifact[],
  language: CodegenLang,
  registered: readonly string[],
): { artifact: CodegenModuleArtifact; sourceByCase: Record<string, string>; uncovered: UncoveredCase[] } {
  const sourceByCase: Record<string, string> = {};
  const uncovered: UncoveredCase[] = [];
  let delegates = false;
  // #60 milestone 1: READ cases only — batchInsert/writeTxGate (writes) are NOT codegen-module
  // cases for ANY language anymore (they stay on the existing write/tx execution path).
  const readCases = cases.filter((c) => c.kind === 'read' || c.kind === 'relation');
  for (const c of readCases) {
    // go/rust drive bc's typed-NATIVE endpoint (bc#77/#90, RUNTIME-FREE); ts stays on the boxed
    // `typescript-typed` endpoint (no typed-native counterpart registered yet).
    // `generateCodegenArtifact` lowers the read graph for typed-native internally (litedbmodel's
    // codegen-only lowering — src/scp/codegen.ts) and fails CLOSED (`TypedNativeCoverageError`) on
    // an uncovered shape (e.g. `complexWhere`/`inList`'s IN-list array-typed head) — for a NATIVE
    // lang this is an EXPECTED, DECLARED gap: skip that case's module for THIS language (never
    // silently fall back to a boxed/typed-raw emitter), and report it.
    let art: ReturnType<typeof lm.generateCodegenArtifact>;
    try {
      art = lm.generateCodegenArtifact(c.bundle as any, language, registered as string[], COLUMN_TYPES);
    } catch (e) {
      if (NATIVE_ONLY_LANGS.includes(language as never) && e instanceof lm.TypedNativeCoverageError) {
        uncovered.push({ caseId: c.case, reason: e.message.split('\n')[0] });
        continue;
      }
      throw e;
    }
    // Anti-sham DELEGATION gate: does the generated module CALL the interpreter
    // (`runBehavior`/`run_behavior`/`RunBehavior`) at the CODE level? The straight-line/typed-native
    // (bc#75/#90) emitters produce de-interpreted native source whose EXPLANATORY COMMENTS say
    // "does NOT go through run_behavior" — a naive substring match false-positives on that
    // prose. Strip comments + string/docstring literals first so we count a genuine CALL only.
    if (codeMentionsInterpreter(art.module.code)) delegates = true;
    sourceByCase[c.case] = art.module.code;
  }
  return {
    artifact: { language, filenameHint: `behaviors.generated.${CODEGEN_EXT[language]}`, delegatesToRunBehavior: delegates },
    sourceByCase,
    uncovered,
  };
}

function main(): void {
  // PREFLIGHT (fail-closed, codified): every language's bc dep MUST be the same version, else the
  // cross-lang comparison is invalid. Throws with a per-manifest diff on drift.
  const bcVersion = assertBcVersionsAligned(REPO_ROOT);
  console.error(`bc ${bcVersion} — aligned across all languages ✓`);

  const registered = registeredLanguages();

  // 1. PER-DIALECT bundles (sqlite / postgres / mysql).
  const dialects: Record<string, { cases: CaseArtifact[] }> = {};
  for (const d of CROSSLANG_DIALECTS) dialects[d] = { cases: buildDialect(d) };

  // 2. TRUE generated codegen modules — bc generator output per language, READ cases only (#60
  //    m1). The module is dialect-invariant (IR/wiring only); the dialect SQL rides the companion
  //    bundle the cell already has per dialect. We generate from the sqlite bundles. Per-language
  //    coverage differs for go/rust (typed-native, may skip an uncovered case) vs ts (boxed, covers
  //    every read case) — `sourceByCase` reflects exactly what THAT language covered.
  const codegen: Record<string, CodegenModuleArtifact> = {};
  const codegenSources: Record<string, Record<string, string>> = {};
  const uncoveredByLang: Record<string, UncoveredCase[]> = {};
  for (const lang of CODEGEN_LANGS) {
    const { artifact, sourceByCase, uncovered } = generateCodegen(dialects.sqlite.cases, lang, registered);
    codegen[lang] = artifact;
    codegenSources[lang] = sourceByCase;
    uncoveredByLang[lang] = uncovered;
  }

  // PREFLIGHT (fail-closed, codified): the native-codegen langs MUST emit de-interpreted native
  // code — never a silent fallback to the boxed literal/interpreter path (which would measure
  // codegen ≈ ir). Throws on any silent fallback. Runs on EVERY path (not just --check).
  assertCodegenSurface(codegen);

  const seed = seedStatements();

  // 3. NATIVE pre-decoded codegen companions (owner order — the codegen path carries NO IR data
  //    and parses NO JSON at execution time): decode every dialect's cases through the CLOSED-SET
  //    fail-closed decoder (native-companion.ts — an out-of-set shape THROWS here) and emit the
  //    native Rust/Go companion sources the codegen cells execute from.
  const nativeByDialect: Record<string, NCase[]> = {};
  for (const d of CROSSLANG_DIALECTS) nativeByDialect[d] = dialects[d].cases.map((c) => decodeNativeCase(c as never));
  const rustCompanion = rustCompanionSource(nativeByDialect, SCHEMA as readonly string[], seed);
  const goCompanion = goCompanionSource(nativeByDialect, SCHEMA as readonly string[], seed);

  const writeNativeCompanions = (): void => {
    mkdirSync(dirname(RUST_COMPANION), { recursive: true });
    writeFileSync(RUST_COMPANION, rustCompanion);
    mkdirSync(GO_CGPLANS_DIR, { recursive: true });
    writeFileSync(resolve(GO_CGPLANS_DIR, 'plans.go'), goCompanion);
  };

  const artifact = {
    corpusVersion: 3,
    schema: [...SCHEMA],
    seed,
    dialects,
    codegen,
  };
  const body = JSON.stringify(artifact, null, 2);

  // Write EACH language's generated module for EVERY case it covers (per-language — go/rust may
  // legitimately not cover a case ts does; #60 m1). No language writes a placeholder/undefined
  // source for a case it did not cover. CLEAN the codegen dirs FIRST (pure build output,
  // gitignored) so a case that lost coverage across a run (e.g. a bc/schema change moves it from
  // covered to uncovered) never leaves a STALE generated module a consumer could mistakenly wire
  // against — the on-disk set always reflects exactly the CURRENT run's coverage.
  const writeCodegenFiles = (): void => {
    rmSync(CODEGEN_DIR, { recursive: true, force: true });
    rmSync(GO_CGMODS_DIR, { recursive: true, force: true });
    for (const lang of CODEGEN_LANGS) {
      for (const caseId of Object.keys(codegenSources[lang])) {
        const p = codegenFilePath(lang, caseId);
        mkdirSync(dirname(p), { recursive: true });
        writeFileSync(p, codegenSources[lang][caseId]);
      }
    }
    // Materialize the Go per-case packages INSIDE the go module so the bench can compile+import
    // them. Each is the SAME generated source with only the package clause rewritten
    // `behaviors` -> `cg_<case>` (byte-identical code otherwise — the flat generated/ copy above
    // is what the anti-sham gate checks). ONLY for cases go actually covered.
    for (const caseId of Object.keys(codegenSources.go)) {
      const pkg = GO_CASE_PKG(caseId);
      const src = codegenSources.go[caseId].replace(/^package\s+behaviors\b/m, `package ${pkg}`);
      const dir = resolve(GO_CGMODS_DIR, caseId);
      mkdirSync(dir, { recursive: true });
      writeFileSync(resolve(dir, 'gen.go'), src);
    }
  };

  const reportCoverage = (): void => {
    for (const lang of CODEGEN_LANGS) {
      const native = (NATIVE_CODEGEN_LANGS as readonly string[]).includes(lang);
      const covered = Object.keys(codegenSources[lang]).length;
      console.error(
        `  ${lang.padEnd(11)} delegatesToRunBehavior=${codegen[lang].delegatesToRunBehavior}` +
          ` covered=${covered}/${READ_CASE_IDS.length}` +
          `${native ? ' (native-codegen: MUST be false)' : ' (ir/interpret surface — literal by design)'}`,
      );
      for (const u of uncoveredByLang[lang]) {
        console.error(`    · NOT COVERED [${u.caseId}] (bc#86 gap, reported not silently substituted): ${u.reason}`);
      }
    }
  };

  if (process.argv.includes('--check')) {
    mkdirSync(dirname(OUT), { recursive: true });
    writeFileSync(OUT, body);
    writeCodegenFiles();
    writeNativeCompanions();
    // Surface already asserted fail-closed above (assertCodegenSurface). Report per lang.
    reportCoverage();
    console.error(`OK: freshly generated (${CROSSLANG_DIALECTS.length} dialects × ${dialects.sqlite.cases.length} bundles) — native langs de-interpreted, py/php on ir surface.`);
    return;
  }

  mkdirSync(dirname(OUT), { recursive: true });
  writeFileSync(OUT, body);
  writeCodegenFiles();
  writeNativeCompanions();
  console.error(`Wrote ${OUT} (${CROSSLANG_DIALECTS.length} dialects × ${dialects.sqlite.cases.length} case bundles)`);
  console.error(`Wrote native codegen companions: ${RUST_COMPANION} + ${resolve(GO_CGPLANS_DIR, 'plans.go')}`);
  for (const d of CROSSLANG_DIALECTS) {
    console.error(`  [${d}]`);
    for (const c of dialects[d].cases) console.error(`    ${c.case.padEnd(14)} kind=${c.kind} Q=${c.expectedQueries} R=${c.expectedRows}`);
  }
  console.error(`Wrote ${CODEGEN_LANGS.length}×${READ_CASE_IDS.length} generated READ codegen modules (per-language coverage) to ${CODEGEN_DIR}`);
  reportCoverage();
}

main();
