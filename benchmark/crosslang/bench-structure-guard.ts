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
//     modules in that file and de-box through the typed runner; no legacy sidecar, child sidecar, JSON IR,
//     stash/decode seam, or hand-materialized generic object is allowed.
//
// The SDK cell (orm_bench_sdk) KEEPS its manual orchestration — that is the comparison baseline, not a
// litedbmodel path — so it is out of scope here.
//
// Usage: `tsx benchmark/crosslang/bench-structure-guard.ts` — exits 0 (PASS) / 1 (FAIL).

import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { join, dirname, delimiter } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '../..');
const MAIN = join(ROOT, 'rust/orm_bench/src/main.rs');
const GEN = join(ROOT, 'rust/orm_bench/src/gen');
const E1_GEN = join(ROOT, 'rust/e1_native_proof/src');
const E1_MAIN = join(E1_GEN, 'main.rs');
const NATIVE_RUNTIME = join(ROOT, 'rust/litedbmodel_runtime/src/relation.rs');
const NATIVE_RUNTIME_DIR = join(ROOT, 'rust/litedbmodel_runtime');
const INTERPRETER_DIR = join(ROOT, 'rust/litedbmodel_interpreter');
const ORACLE_BUILD = join(ROOT, 'benchmark/crosslang/oracle-fixture-build.ts');
const EXTRA_SOURCES = process.env.LITEDB_GUARD_EXTRA?.split(delimiter).filter(Boolean) ?? [];
const PIPELINE_SOURCES = [
  join(ROOT, 'benchmark/crosslang/codegen-build.ts'),
  join(ROOT, 'test/scp/e1-native-sql-port.test.ts'),
  join(ROOT, 'src/scp/codegen.ts'),
  E1_MAIN,
  NATIVE_RUNTIME,
  ORACLE_BUILD,
  ...EXTRA_SOURCES,
];

function rustCode(src: string): string {
  return src.replace(/"(?:[^"\\]|\\.)*"|\/\/[^\n]*|\/\*[\s\S]*?\*\//g, (part) => part.startsWith('"') ? part : '');
}

function filesUnder(path: string): string[] {
  if (!existsSync(path)) return [];
  if (!statSync(path).isDirectory()) return [path];
  return readdirSync(path).flatMap((name) => filesUnder(join(path, name)));
}

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

// ── 2. artifact shape: one generated_<op>.rs only; no legacy/manifest JSON sidecars ────────────────
const artifactDirs = [GEN, E1_GEN];
const legacySidecarWord = `com${'panion'}`;
const legacySidecarFiles = artifactDirs.flatMap((dir) => readdirSync(dir).filter((n) => n.startsWith(`${legacySidecarWord}_`) && n.endsWith('.rs')).map((n) => join(dir, n)));
if (legacySidecarFiles.length > 0) {
  violations.push(`standalone legacy sidecar artifacts remain: ${legacySidecarFiles.join(', ')}`);
}
const childSidecars = artifactDirs.flatMap((dir) => readdirSync(dir).filter((n) => /^generated_.*_rel_.*\.rs$/.test(n)).map((n) => join(dir, n)));
if (childSidecars.length > 0) violations.push(`standalone relation child artifacts remain: ${childSidecars.join(', ')}`);
if (artifactDirs.some((dir) => readdirSync(dir).some((n) => n.endsWith('.json')))) violations.push('generated directory contains JSON sidecar output');

// ── 3. relation child query is an in-file BC native module using ordinary ports→exec ───────────────
const generatedFiles = artifactDirs.flatMap((dir) => readdirSync(dir).filter((n) => n.startsWith('generated_') && n.endsWith('.rs')).map((n) => join(dir, n)));
for (const file of generatedFiles) {
  const f = file.slice(ROOT.length + 1);
  const src = readFileSync(file, 'utf8');
  const code = rustCode(src);
  if (/Value::Obj\s*\(\s*vec!/.test(code)) violations.push(`${f} hand-builds Value::Obj`);
  if (/relation_ops_json|\.get\(\s*["'](?:relations|childRelations|parentKeys|targetKeys|keyShape)["']\s*\)|"\{\\"|struct DeBox|RefCell<Option<Wire>>|fn decode\(wire|_ports:\s*&PortsNR/.test(code)) {
    violations.push(`${f} contains JSON traversal or the retired stash/decode seam`);
  }
  if (/\bNode::|execute_bundle|execute_transaction_bundle|read_bundle|litedbmodel_interpreter/.test(code)) {
    violations.push(`${f} crosses the native/interpreter boundary`);
  }
  if (src.toLowerCase().includes(legacySidecarWord)) violations.push(`${f} contains retired sidecar vocabulary`);
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
  [new RegExp(`relation_ops_json|SqlCatalog${legacySidecarWord[0].toUpperCase()}${legacySidecarWord.slice(1)}|${legacySidecarWord}Of\\s*\\(|\\.${legacySidecarWord}\\b`), 'retired sidecar metadata API'],
  [/writeFileSync\([^\n]*(?:\.json|JSON\.stringify)/, 'serialized JSON file output'],
  [/["'`]\{\\"/, 'embedded serialized JSON object'],
];
for (const file of PIPELINE_SOURCES) {
  const src = readFileSync(file, 'utf8');
  for (const [pattern, why] of SERIALIZED_FORBIDDEN) {
    if (pattern.test(src)) violations.push(`${file}: ${why} (${pattern})`);
  }
}
for (const file of EXTRA_SOURCES) {
  const src = readFileSync(file, 'utf8');
  if (/\bNode::|execute_bundle|read_bundle|litedbmodel_interpreter/.test(src)) {
    violations.push(`${file}: generated/native source crosses interpreter boundary`);
  }
}
const nativeRuntime = readFileSync(NATIVE_RUNTIME, 'utf8');
if (!/pub fn execute_relation_batch\s*\(/.test(nativeRuntime)) violations.push('native runtime lacks the single relation batch core');
if (/op_from_|RelationOp|childRelations|read_bundle_pooled/.test(nativeRuntime)) violations.push('native relation module contains interpreter metadata concerns');
const codegenSource = readFileSync(join(ROOT, 'src/scp/codegen.ts'), 'utf8');
if ((codegenSource.match(/execute_relation_batch\(/g) ?? []).length !== 1) violations.push('generated adapter does not have exactly one relation batch core callsite');

// ── 5. physical crate closure: runtime is native-only; interpreter depends one-way on runtime ───────
for (const retired of ['node.rs', 'runtime.rs', 'static_bundle.rs', 'value.rs', 'relation_interpreter.rs']) {
  if (existsSync(join(NATIVE_RUNTIME_DIR, 'src', retired))) violations.push(`native runtime still owns interpreter file ${retired}`);
}
const nativeFiles = readdirSync(join(NATIVE_RUNTIME_DIR, 'src')).filter((name) => name.endsWith('.rs'));
for (const name of nativeFiles) {
  const code = rustCode(readFileSync(join(NATIVE_RUNTIME_DIR, 'src', name), 'utf8'));
  if (/execute_bundle|execute_transaction_bundle|read_bundle|\bNode::|litedbmodel_interpreter/.test(code)) {
    violations.push(`native runtime source ${name} references interpreter API`);
  }
}
const runtimeCargo = readFileSync(join(NATIVE_RUNTIME_DIR, 'Cargo.toml'), 'utf8');
const runtimeCargoCode = runtimeCargo.replace(/^\s*#.*$/gm, '');
if (/^\s*(?:litedbmodel_interpreter|serde_json)\s*=/m.test(runtimeCargoCode)) violations.push('native runtime Cargo closure includes interpreter/JSON dependency');
const interpreterCargo = readFileSync(join(INTERPRETER_DIR, 'Cargo.toml'), 'utf8');
if (!/litedbmodel_runtime\s*=/.test(interpreterCargo)) violations.push('interpreter crate does not depend one-way on native runtime');
const tree = spawnSync('cargo', ['tree', '--manifest-path', join(ROOT, 'rust/Cargo.toml'), '-p', 'litedbmodel_runtime'], { encoding: 'utf8' });
if (tree.status !== 0) violations.push(`cargo tree failed: ${tree.stderr.trim()}`);
else if (/litedbmodel_interpreter|serde_json/.test(tree.stdout)) violations.push('native runtime cargo tree reaches interpreter/JSON');

// ── 6. proof scripts must invoke the direct oracle and must not accept printed/fake success ─────────
for (const relative of ['rust/e1_native_proof/run-proof.sh', 'rust/e1_native_proof/run-proof-livedb.sh', 'rust/orm_bench/run-pilot.sh']) {
  const script = readFileSync(join(ROOT, relative), 'utf8');
  if (!/-p litedbmodel_oracle/.test(script)) violations.push(`${relative} does not invoke the direct oracle crate`);
  if (/oracle\.json|verify-cells|result\.json/.test(script)) violations.push(`${relative} references retired serialized oracle transport`);
}
const benchMain = readFileSync(MAIN, 'utf8');
for (const op of ['nestedFindAll', 'nestedFindFirst', 'nestedFindUnique', 'nestedRelations', 'compositeRelations']) {
  if (!new RegExp(`expect_queries\\(\"${op}\",`).test(benchMain)) violations.push(`safety proof does not assert ${op}`);
}

// ── 7. the independent oracle owns dialect-specific native modules and setup SQL ──────────────────
const oracleMain = readFileSync(join(ROOT, 'rust/litedbmodel_oracle/src/main.rs'), 'utf8');
if (/orm_bench|generated_setup::STATEMENTS/.test(oracleMain)) violations.push('oracle depends on the committed sqlite benchmark module tree');
const pgSetup = readFileSync(join(ROOT, 'rust/litedbmodel_oracle/src/generated/setup_postgres.rs'), 'utf8');
const mysqlSetup = readFileSync(join(ROOT, 'rust/litedbmodel_oracle/src/generated/setup_mysql.rs'), 'utf8');
if (/AUTOINCREMENT|datetime\s*\(\s*'now'\s*\)/i.test(pgSetup)) violations.push('postgres oracle setup contains sqlite DDL');
if (/AUTOINCREMENT|datetime\s*\(\s*'now'\s*\)|\bSERIAL\b|\$\d+/i.test(mysqlSetup)) violations.push('mysql oracle setup contains sqlite/postgres syntax');

// ── 8. retired sidecar vocabulary cannot re-enter current code, generated output, or run scripts ─
const vocabularyRoots = [
  join(ROOT, 'src/scp'),
  join(ROOT, 'rust/litedbmodel_runtime'),
  join(ROOT, 'rust/litedbmodel_interpreter'),
  join(ROOT, 'rust/litedbmodel_oracle'),
  join(ROOT, 'rust/orm_bench'),
  join(ROOT, 'rust/e1_native_proof'),
  join(ROOT, 'benchmark/crosslang'),
  join(ROOT, 'test/scp'),
  join(ROOT, 'scripts'),
  join(ROOT, 'go/litedbmodel_runtime'),
  join(ROOT, 'conformance/codegen'),
];
const vocabularyFiles = vocabularyRoots.flatMap(filesUnder).filter((file) =>
  /\.(?:ts|mjs|sh|rs|toml|go|py)$/.test(file) && !file.includes('/target/')
);
for (const file of vocabularyFiles) {
  if (readFileSync(file, 'utf8').toLowerCase().includes(legacySidecarWord)) {
    violations.push(`${file}: retired sidecar vocabulary remains`);
  }
}

if (violations.length > 0) {
  console.error('bench-structure-guard: FAIL');
  for (const v of violations) console.error(`  ✗ ${v}`);
  process.exit(1);
}
console.log('bench-structure-guard: PASS — one generated artifact per op; relation children are in-file typed BC modules with no legacy or JSON sidecar.');
