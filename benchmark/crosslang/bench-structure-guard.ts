// bench-structure-guard (#138 → #140) — machine guard that the NATIVE orm_bench relation path stays
// CLEAN: the timed op cells call the generated/companion op only, and NOTHING hand-reconstructs a generic
// `Value::Obj` (typed→boxed-object glue that would kill the point of bc typed codegen), nor re-parses
// relation metadata per iteration, nor re-implements the loader in the bench. #140 extends the guard to
// the CHILD side: the batched child rows must de-box to TYPED structs via a bc CHILD module — never
// grouped/retained as `Value::Obj`.
//
// Scope (the relation path):
//   • `prepare_op` body in rust/orm_bench/src/main.rs — the TIMED op closures. Forbidden: a hand-built
//     `Value::Obj(vec!` (object reconstruction), a bench-side loader (`stitch_with_children`, the old
//     `user_parents`/`tenant_parents` glue), and a per-iteration `Node::parse`. Positive: every relation
//     cell hydrates via the generated TYPED hydrator (`companion_*::hydrate_<rel>`). (The verify/oracle
//     harness `run_verify` is OUTSIDE prepare_op and may build Value for byte-equal comparison.)
//   • rust/orm_bench/src/gen/companion_*.rs — the litedbmodel-generated companions (primary AND #140
//     child de-box). Forbidden: a hand-built `Value::Obj(vec!` (a companion must not box typed rows into
//     generic objects). Positive (child side): each `companion_*_rel_*.rs` de-boxes the batched child
//     rows through the bc TYPED runner (`run_native_raw_struct_`) — the child rows are TYPED structs, not
//     a hand-materialized `Value` group; and each primary companion with a relation drives the SHARED
//     TYPED loader (`hydrate_relation_typed`).
//
// The SDK cell (orm_bench_sdk) KEEPS its manual orchestration — that is the comparison baseline, not a
// litedbmodel path — so it is out of scope here.
//
// Usage: `tsx benchmark/crosslang/bench-structure-guard.ts` — exits 0 (PASS) / 1 (FAIL).

import { readFileSync, readdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '../..');
const MAIN = join(ROOT, 'rust/orm_bench/src/main.rs');
const GEN = join(ROOT, 'rust/orm_bench/src/gen');

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
  [/Node::parse/, 'per-iteration relation-metadata parse (parse ONCE in setup via relation_op)'],
];
for (const [re, why] of FORBIDDEN) {
  if (re.test(body)) violations.push(`prepare_op contains ${why} (match: ${re})`);
}
// Positive check: every relation op cell hydrates via the generated TYPED hydrator (`::hydrate_<rel>`),
// which drives the SHARED `hydrate_relation_typed` — NOT a hand loader.
if (!/::hydrate_\w+\s*\(/.test(body)) {
  violations.push('prepare_op does not call a generated TYPED hydrator (companion_*::hydrate_<rel>) — relation cells must use the typed loader');
}

// ── 2. the generated companions must not hand-build a generic Value::Obj (primary AND #140 child) ─────
const companionFiles = readdirSync(GEN).filter((n) => n.startsWith('companion_') && n.endsWith('.rs'));
for (const f of companionFiles) {
  const src = readFileSync(join(GEN, f), 'utf8');
  if (/Value::Obj\s*\(\s*vec!/.test(src)) {
    violations.push(`${f} hand-builds a Value::Obj (companions must not box typed rows into generic objects)`);
  }
}

// ── 3. #140 CHILD side: the batched child rows must de-box to TYPED structs via a bc CHILD module ──────
// Each `companion_*_rel_*.rs` (a generated child de-box companion) MUST go through the bc TYPED runner
// (`run_native_raw_struct_`) — the machine proof that the child rows are TYPED structs, never a
// hand-materialized Value group. And every relation op declared in the manifest MUST have such a child
// companion, and its primary companion MUST drive the SHARED typed loader (`hydrate_relation_typed`).
const childCompanions = companionFiles.filter((n) => /^companion_\w+_rel_/.test(n));
if (childCompanions.length === 0) {
  violations.push('no companion_*_rel_*.rs child de-box companion found — the relation child rows are not typed (#140 regression)');
}
for (const f of childCompanions) {
  const src = readFileSync(join(GEN, f), 'utf8');
  if (!/run_native_raw_struct_\w+\s*\(/.test(src)) {
    violations.push(`${f} does not de-box the batched child rows via the bc typed runner (run_native_raw_struct_*) — child rows must be TYPED, not a Value materialize`);
  }
}
// Every PRIMARY companion carrying a relation hydrator must drive the SHARED typed loader.
for (const f of companionFiles.filter((n) => !/^companion_\w+_rel_/.test(n))) {
  const src = readFileSync(join(GEN, f), 'utf8');
  if (/pub fn hydrate_\w+</.test(src) && !/hydrate_relation_typed/.test(src)) {
    violations.push(`${f} has a relation hydrator that does NOT call litedbmodel_runtime::hydrate_relation_typed (the shared typed loader SSoT)`);
  }
}

if (violations.length > 0) {
  console.error('bench-structure-guard: FAIL');
  for (const v of violations) console.error(`  ✗ ${v}`);
  process.exit(1);
}
console.log('bench-structure-guard: PASS — relation timed cells call the generated/companion op only; no hand-built Value::Obj in the relation path (bench or companion).');
