// bench-structure-guard (#138) — machine guard that the NATIVE orm_bench relation path stays CLEAN:
// the timed op cells call the generated/companion op only, and NOTHING hand-reconstructs a generic
// `Value::Obj` (typed→boxed-object glue that would kill the point of bc typed codegen), nor re-parses
// relation metadata per iteration, nor re-implements the loader in the bench.
//
// Scope (the relation path):
//   • `prepare_op` body in rust/orm_bench/src/main.rs — the TIMED op closures. Forbidden: a hand-built
//     `Value::Obj(vec!` (object reconstruction), a bench-side loader (`stitch_with_children`, the old
//     `user_parents`/`tenant_parents` glue), and a per-iteration `Node::parse`. (The verify/oracle
//     harness `run_verify` is OUTSIDE prepare_op and may build Value for byte-equal comparison.)
//   • rust/orm_bench/src/gen/companion_*.rs — the litedbmodel-generated companions. Forbidden: a
//     hand-built `Value::Obj(vec!` (a companion must not box typed rows into generic objects).
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
// Positive check: every relation op cell hydrates via the litedbmodel TYPED loader.
if (!/hydrate_relation/.test(body)) {
  violations.push('prepare_op does not call litedbmodel_runtime::hydrate_relation (relation cells must use the typed loader)');
}

// ── 2. the generated companions must not hand-build a generic Value::Obj ─────────────────────────────
for (const f of readdirSync(GEN).filter((n) => n.startsWith('companion_') && n.endsWith('.rs'))) {
  const src = readFileSync(join(GEN, f), 'utf8');
  if (/Value::Obj\s*\(\s*vec!/.test(src)) {
    violations.push(`${f} hand-builds a Value::Obj (companions must not box typed rows into generic objects)`);
  }
}

if (violations.length > 0) {
  console.error('bench-structure-guard: FAIL');
  for (const v of violations) console.error(`  ✗ ${v}`);
  process.exit(1);
}
console.log('bench-structure-guard: PASS — relation timed cells call the generated/companion op only; no hand-built Value::Obj in the relation path (bench or companion).');
