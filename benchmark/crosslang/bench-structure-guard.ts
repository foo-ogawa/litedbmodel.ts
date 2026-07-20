// bench-structure-guard (#138 → #140) — machine guard that the NATIVE orm_bench relation path stays
// CLEAN: the timed op cells call the sole generated op module, and NOTHING hand-reconstructs a generic
// `Value::Obj` (typed→boxed-object glue that would kill the point of bc typed codegen), nor re-parses
// relation metadata, nor re-implements the loader in the bench. #140 extends the guard to
// the CHILD side: the batched child rows must de-box to TYPED structs via a bc CHILD module — never
// grouped/retained as `Value::Obj`.
//
// Scope (the relation path):
//   • `prepare_op` body in rust/orm_bench/src/main.rs — the TIMED op closures. Forbidden: a hand-built
//     `Value::Obj(vec!` (object reconstruction), a bench-side loader (`stitch_with_children`, the old
//     `user_parents`/`tenant_parents` glue), and a per-iteration `Node::parse`. Positive: every relation
//     cell hydrates via the generated TYPED hydrator (`generated_*::hydrate_<rel>`). (The verify/oracle
//     harness `run_verify` is OUTSIDE prepare_op and may build Value for byte-equal comparison.)
//   • rust/*/generated_*.rs — each operation's only generated artifact. Relation children are nested BC
//     modules in that file and de-box through the typed runner; no companion, child sidecar, JSON IR,
//     stash/decode seam, or hand-materialized generic object is allowed.
//
// The SDK cell (orm_bench_sdk) KEEPS its manual orchestration — that is the comparison baseline, not a
// litedbmodel path — so it is out of scope here.
//
// Usage: `tsx benchmark/crosslang/bench-structure-guard.ts` — exits 0 (PASS) / 1 (FAIL).

import { readFileSync, readdirSync } from 'node:fs';
import { join, dirname, delimiter } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '../..');
const MAIN = join(ROOT, 'rust/orm_bench/src/main.rs');
const GEN = join(ROOT, 'rust/orm_bench/src/gen');
const E1_GEN = join(ROOT, 'rust/e1_native_proof/src');
const E1_MAIN = join(E1_GEN, 'main.rs');
const NATIVE_RUNTIME = join(ROOT, 'rust/litedbmodel_runtime/src/relation.rs');
const PIPELINE_SOURCES = [
  join(ROOT, 'benchmark/crosslang/codegen-build.ts'),
  join(ROOT, 'test/scp/e1-native-sql-port.test.ts'),
  join(ROOT, 'src/scp/codegen.ts'),
  E1_MAIN,
  NATIVE_RUNTIME,
  ...(process.env.LITEDB_GUARD_EXTRA?.split(delimiter).filter(Boolean) ?? []),
];

/** Extract the `fn prepare_op(...) { ... }` body by brace matching (the timed op cells). */
function prepareOpBody(src: string): string {
  const at = src.indexOf('fn prepare_op');
  if (at < 0) throw new Error('bench-structure-guard: fn prepare_op not found in main.rs');
  const open = src.indexOf('{', at);
  let depth = 0;
  for (let i = open; i < src.length; i++) {
    if (src[i] === '{') depth++;
    else if (src[i] === '}' && --depth === 0) return src.slice(open + 1, i);
  }
  throw new Error('bench-structure-guard: unbalanced braces in prepare_op');
}

const violations: string[] = [];

// ── 1. the TIMED op cells (prepare_op) must carry no hand orchestration / boxed-object glue ──────────
const body = prepareOpBody(readFileSync(MAIN, 'utf8'));
const FORBIDDEN: Array<[RegExp, string]> = [
  [/Value::Obj\s*\(\s*vec!/, 'hand-built Value::Obj (typed→boxed-object reconstruction)'],
  [/stitch_with_children|user_parents|tenant_parents/, 'bench-side relation orchestration helper'],
  [/Node::parse|relation_op|relation_ops_json/, 'runtime relation-metadata interpreter/setup'],
];
for (const [re, why] of FORBIDDEN) {
  if (re.test(body)) violations.push(`prepare_op contains ${why} (match: ${re})`);
}
// Positive check: every relation op cell hydrates via the sole generated module.
if (!/::hydrate_\w+\s*\(/.test(body)) {
  violations.push('prepare_op does not call a generated TYPED hydrator');
}

// ── 2. artifact shape: one generated_<op>.rs only; no companion/manifest JSON sidecars ─────────────
const artifactDirs = [GEN, E1_GEN];
const companionFiles = artifactDirs.flatMap((dir) => readdirSync(dir).filter((n) => n.startsWith('companion_') && n.endsWith('.rs')).map((n) => join(dir, n)));
if (companionFiles.length > 0) {
  violations.push(`standalone companion artifacts remain: ${companionFiles.join(', ')}`);
}
const childSidecars = artifactDirs.flatMap((dir) => readdirSync(dir).filter((n) => /^generated_.*_rel_.*\.rs$/.test(n)).map((n) => join(dir, n)));
if (childSidecars.length > 0) violations.push(`standalone relation child artifacts remain: ${childSidecars.join(', ')}`);
if (artifactDirs.some((dir) => readdirSync(dir).some((n) => n.endsWith('.json')))) violations.push('generated directory contains JSON sidecar output');

// ── 3. relation child query is an in-file BC native module using ordinary ports→exec ───────────────
const generatedFiles = artifactDirs.flatMap((dir) => readdirSync(dir).filter((n) => n.startsWith('generated_') && n.endsWith('.rs')).map((n) => join(dir, n)));
for (const file of generatedFiles) {
  const f = file.slice(ROOT.length + 1);
  const src = readFileSync(file, 'utf8');
  if (/Value::Obj\s*\(\s*vec!/.test(src)) violations.push(`${f} hand-builds Value::Obj`);
  if (/relation_ops_json|\.get\("|"\{\\"|struct DeBox|RefCell<Option<Wire>>|fn decode\(wire|_ports:\s*&PortsNR/.test(src)) {
    violations.push(`${f} contains JSON traversal or the retired stash/decode seam`);
  }
  if (/pub fn hydrate_\w+/.test(src)) {
    if (!/pub mod rel_\w+/.test(src) || !/RelationBatch/.test(src)) violations.push(`${f} lacks an in-file BC RelationBatch child module`);
    if (!/execute_relation_batch\(\s*&ctx,\s*&ports\.f_sql/.test(src)) {
      violations.push(`${f} child RelationBatch does not use the shared batch semantic core`);
    }
    if (!/run_native_raw_struct_Rel\w+\s*\(/.test(src) || !/hydrate_children\s*\(/.test(src)) {
      violations.push(`${f} hydrate does not directly call the typed child runner + shared distributor`);
    }
  }
}

// ── 4. generated/native dependency closure and generators contain no serialized metadata path ─────
const SERIALIZED_FORBIDDEN: Array<[RegExp, string]> = [
  [/serde_json::from_str|JSON\.parse\s*\(|Node::parse\s*\(/, 'runtime serialized parser'],
  [/\.get\(\s*["'](?:relations|childRelations|parentKeys|targetKeys|keyShape)["']\s*\)/, 'relation metadata walker'],
  [/relation_ops_json|SqlCatalogCompanion|companionOf\s*\(|\.companion\b/, 'retired companion metadata API'],
  [/writeFileSync\([^\n]*(?:\.json|JSON\.stringify)/, 'serialized JSON file output'],
  [/["'`]\{\\"/, 'embedded serialized JSON object'],
];
for (const file of PIPELINE_SOURCES) {
  const src = readFileSync(file, 'utf8');
  for (const [pattern, why] of SERIALIZED_FORBIDDEN) {
    if (pattern.test(src)) violations.push(`${file}: ${why} (${pattern})`);
  }
}
const nativeRuntime = readFileSync(NATIVE_RUNTIME, 'utf8');
if (!/pub fn execute_relation_batch\s*\(/.test(nativeRuntime)) violations.push('native runtime lacks the single relation batch core');
if (/op_from_|RelationOp|childRelations|read_bundle_pooled/.test(nativeRuntime)) violations.push('native relation module contains interpreter metadata concerns');
const codegenSource = readFileSync(join(ROOT, 'src/scp/codegen.ts'), 'utf8');
if ((codegenSource.match(/execute_relation_batch\(/g) ?? []).length !== 1) violations.push('generated adapter does not have exactly one relation batch core callsite');

if (violations.length > 0) {
  console.error('bench-structure-guard: FAIL');
  for (const v of violations) console.error(`  ✗ ${v}`);
  process.exit(1);
}
console.log('bench-structure-guard: PASS — one generated artifact per op; relation children are in-file typed BC modules with no companion or JSON sidecar.');
