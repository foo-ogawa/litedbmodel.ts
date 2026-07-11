// ════════════════════════════════════════════════════════════════════════════
// Artifact generation (epic #44) — compile the 8 shared-domain behaviors into
// makeSQL bundles (PER DIALECT) + the TRUE bc-generated codegen modules, emitting
// the ONE generated artifact every language's ir / codegen cells consume.
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
//   2. TRUE codegen modules — `generateCodegenArtifact` runs bc's SHARED generator
//      over each bundle's portable IR and emits a language-native module (the IR
//      baked as a literal + `bind(handlers)` + fail-closed fingerprint/spec-version
//      load checks). The codegen cell COMPILES + LOADS + BINDS this generated
//      module and executes THROUGH it — a genuinely distinct code entry from `ir`
//      (which calls `executeBundle` on the raw JSON). See the honest disclosure in
//      CROSS-LANG.md: at this bc version the generated module is still interpreter-
//      transcription (it delegates to `runBehavior`), so codegen ≈ ir is EXPECTED.

import { writeFileSync, mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import * as lm from '../../dist/scp/index.mjs';
import { fingerprintComponentGraph, registeredLanguages } from 'behavior-contracts';
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

const HERE = dirname(fileURLToPath(import.meta.url));
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

// bc's shared generator supports exactly these litedbmodel-facing languages (the
// mode-3 codegen "endpoint 3" literal-bake emitters). Each cell's codegen module is
// the generator output for that language over the SqlBundle's portable IR.
const CODEGEN_LANGS = ['typescript', 'python', 'go', 'rust', 'php'] as const;
type CodegenLang = (typeof CODEGEN_LANGS)[number];
const CODEGEN_EXT: Record<CodegenLang, string> = {
  typescript: 'ts', python: 'py', go: 'go', rust: 'rs', php: 'php',
};

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
  fingerprint: string; // codegen: baked-IR fingerprint (fail-closed verified at load)
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
    const bundle = lm.compileCreateManyBundle(
      'BatchInsert',
      { tableName: 'posts', columns: cols, records: INPUTS.batchInsert.rows as any },
      dialect,
    );
    return { bundle, kind: 'batch' };
  }
  if (caseId === 'writeTxGate') {
    const bundle = lm.compileWriteBundle(writesContract, 'Create', writeGateContract, 'create', dialect);
    return { bundle, kind: 'tx' };
  }
  const rel = READ_RELATION[caseId];
  if (rel) {
    const decl = relForDialect(rel.decl, dialect);
    const bundle = lm.compileBundle(readsContract, READ_ENTRY[caseId], [decl], dialect);
    return { bundle, kind: 'relation', entry: READ_ENTRY[caseId], withRelation: rel.withName, relation: decl };
  }
  const bundle = lm.compileBundle(readsContract, READ_ENTRY[caseId], [], dialect);
  return { bundle, kind: 'read', entry: READ_ENTRY[caseId] };
}

// Build the per-dialect case list (the 8 bundles compiled for one dialect).
function buildDialect(dialect: CrosslangDialect): CaseArtifact[] {
  const cases: CaseArtifact[] = [];
  for (const caseId of CROSSLANG_CASE_IDS_LOCAL) {
    const { bundle, kind, entry, withRelation, relation } = buildBundle(caseId, dialect);
    const ir = lm.bundleToPortableIR(bundle);
    const fingerprint = fingerprintComponentGraph(ir);
    const base = SQL_BASELINE[caseId];
    cases.push({
      case: caseId,
      kind,
      ...(entry ? { entry } : {}),
      ...(withRelation ? { withRelation } : {}),
      ...(relation ? { relation } : {}),
      bundle,
      input: (INPUTS as any)[caseId] ?? {},
      fingerprint,
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
  // Per-case generated-module fingerprint (the fail-closed identity the cell verifies).
  fingerprintByCase: Record<string, string>;
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

function generateCodegen(
  cases: CaseArtifact[],
  language: CodegenLang,
  registered: readonly string[],
): { artifact: CodegenModuleArtifact; sourceByCase: Record<string, string> } {
  const fingerprintByCase: Record<string, string> = {};
  const sourceByCase: Record<string, string> = {};
  let delegates = false;
  for (const c of cases) {
    // The generator wants the SqlBundle; it derives the portable IR itself. For the TS
    // cell to import the generated module by relative path we pin the runtime import to
    // the installed `behavior-contracts` package (the default emitter import).
    const art = lm.generateCodegenArtifact(c.bundle as any, language, registered as string[]);
    fingerprintByCase[c.case] = art.module.fingerprint;
    // Anti-sham DELEGATION gate: does the generated module CALL the interpreter
    // (`runBehavior`/`run_behavior`/`RunBehavior`) at the CODE level? The straight-line
    // (bc#75) emitters produce de-interpreted native source whose EXPLANATORY COMMENTS say
    // "does NOT go through run_behavior" — a naive substring match false-positives on that
    // prose. Strip comments + string/docstring literals first so we count a genuine CALL only.
    if (codeMentionsInterpreter(art.module.code)) delegates = true;
    sourceByCase[c.case] = art.module.code;
  }
  return {
    artifact: { language, filenameHint: `behaviors.generated.${CODEGEN_EXT[language]}`, fingerprintByCase, delegatesToRunBehavior: delegates },
    sourceByCase,
  };
}

function main(): void {
  const registered = registeredLanguages();

  // 1. PER-DIALECT bundles (sqlite / postgres / mysql).
  const dialects: Record<string, { cases: CaseArtifact[] }> = {};
  for (const d of CROSSLANG_DIALECTS) dialects[d] = { cases: buildDialect(d) };

  // 2. TRUE generated codegen modules — bc generator output per language. The module
  //    is dialect-invariant (IR/wiring only); the dialect SQL rides the companion
  //    bundle the cell already has per dialect. We generate from the sqlite bundles.
  const codegen: Record<string, CodegenModuleArtifact> = {};
  const codegenSources: Record<string, Record<string, string>> = {};
  for (const lang of CODEGEN_LANGS) {
    const { artifact, sourceByCase } = generateCodegen(dialects.sqlite.cases, lang, registered);
    codegen[lang] = artifact;
    codegenSources[lang] = sourceByCase;
  }

  const artifact = {
    corpusVersion: 3,
    schema: [...SCHEMA],
    seed: seedStatements(),
    dialects,
    codegen,
  };
  const body = JSON.stringify(artifact, null, 2);

  // CI gate (--check): the generated artifacts are BUILD OUTPUT (gitignored, regenerated at run
  // time) — NOT committed — so there is no committed copy to diff against (the old "stale committed
  // codegen" trap the #44 owner flagged). Instead this gate asserts the LOAD-BEARING invariant on
  // the FRESHLY generated code: every language's codegen module is genuinely DE-INTERPRETED
  // (bc#75 straight-line — `delegatesToRunBehavior=false`). A src/ change that makes the generator
  // fall back to interpreter-transcription (literal-bake + run_behavior) fails CI here. It writes
  // the artifacts to disk (same as a normal run) so a following selfcheck/bench sees fresh output.
  if (process.argv.includes('--check')) {
    mkdirSync(dirname(OUT), { recursive: true });
    writeFileSync(OUT, body);
    for (const lang of CODEGEN_LANGS) {
      for (const c of dialects.sqlite.cases) {
        const p = codegenFilePath(lang, c.case);
        mkdirSync(dirname(p), { recursive: true });
        writeFileSync(p, codegenSources[lang][c.case]);
      }
    }
    for (const c of dialects.sqlite.cases) {
      const pkg = GO_CASE_PKG(c.case);
      const src = codegenSources.go[c.case].replace(/^package\s+behaviors\b/m, `package ${pkg}`);
      const dir = resolve(GO_CGMODS_DIR, c.case);
      mkdirSync(dir, { recursive: true });
      writeFileSync(resolve(dir, 'gen.go'), src);
    }
    let sham = false;
    for (const lang of CODEGEN_LANGS) {
      const deint = !codegen[lang].delegatesToRunBehavior;
      console.error(`  ${lang.padEnd(11)} de-interpreted(delegatesToRunBehavior=false)=${deint}`);
      if (!deint) sham = true;
    }
    if (sham) {
      console.error('SHAM: a generated codegen module DELEGATES to the interpreter (run_behavior) — it is NOT de-interpreted.');
      console.error('The generator regressed to literal-bake + interpreter transcription. Fix the emitter / bc version.');
      process.exit(1);
    }
    console.error(`OK: freshly generated (${CROSSLANG_DIALECTS.length} dialects × ${dialects.sqlite.cases.length} bundles, ${CODEGEN_LANGS.length} codegen langs) — all de-interpreted.`);
    return;
  }

  mkdirSync(dirname(OUT), { recursive: true });
  writeFileSync(OUT, body);
  for (const lang of CODEGEN_LANGS) {
    for (const c of dialects.sqlite.cases) {
      const p = codegenFilePath(lang, c.case);
      mkdirSync(dirname(p), { recursive: true });
      writeFileSync(p, codegenSources[lang][c.case]);
    }
  }
  // Materialize the Go per-case packages INSIDE the go module so the bench can compile+import them.
  // Each is the SAME generated source with only the package clause rewritten `behaviors` -> `cg_<case>`
  // (byte-identical code otherwise — the anti-sham gate checks the flat generated/ copy).
  for (const c of dialects.sqlite.cases) {
    const pkg = GO_CASE_PKG(c.case);
    const src = codegenSources.go[c.case].replace(/^package\s+behaviors\b/m, `package ${pkg}`);
    const dir = resolve(GO_CGMODS_DIR, c.case);
    mkdirSync(dir, { recursive: true });
    writeFileSync(resolve(dir, 'gen.go'), src);
  }
  console.error(`Wrote ${OUT} (${CROSSLANG_DIALECTS.length} dialects × ${dialects.sqlite.cases.length} case bundles)`);
  for (const d of CROSSLANG_DIALECTS) {
    console.error(`  [${d}]`);
    for (const c of dialects[d].cases) console.error(`    ${c.case.padEnd(14)} kind=${c.kind} fp=${c.fingerprint.slice(0, 12)}… Q=${c.expectedQueries} R=${c.expectedRows}`);
  }
  console.error(`Wrote ${CODEGEN_LANGS.length}×${dialects.sqlite.cases.length} generated codegen modules to ${CODEGEN_DIR}`);
  for (const lang of CODEGEN_LANGS) console.error(`  ${lang.padEnd(11)} delegatesToRunBehavior=${codegen[lang].delegatesToRunBehavior}`);
}

main();
