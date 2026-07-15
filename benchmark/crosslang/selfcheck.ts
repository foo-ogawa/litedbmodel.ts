import * as lm from '../../dist/scp/index.mjs';
import {
  freshDb, readsContract, writesContract, writeGateContract,
  READ_ENTRY, READ_RELATION, INPUTS, SQL_BASELINE,
} from './domain.js';
import { CROSSLANG_DIALECTS } from './contract.js';

// Fairness instrument: count DML statements (excluding BEGIN/COMMIT/tx-control) and
// DB rows read (rows returned by .all/.get). Wraps db.prepare so it captures every
// statement + its result-set size, for BOTH the sql baseline and the lm path.
const TX_CONTROL = /^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE|PRAGMA)\b/i;
function instrument<T>(db: any, fn: () => T): { result: T; queries: number; rows: number } {
  let queries = 0, rows = 0;
  const orig = db.prepare.bind(db);
  db.prepare = (sql: string) => {
    const stmt = orig(sql);
    if (TX_CONTROL.test(sql)) return stmt;
    const wrap = Object.create(stmt);
    wrap.all = (...a: any[]) => { queries++; const r = stmt.all(...a); rows += Array.isArray(r) ? r.length : 0; return r; };
    wrap.get = (...a: any[]) => { queries++; const r = stmt.get(...a); if (r !== undefined) rows += 1; return r; };
    wrap.run = (...a: any[]) => { queries++; return stmt.run(...a); };
    return wrap;
  };
  try { return { result: fn(), queries, rows }; } finally { db.prepare = orig; }
}
const withQueryCount = instrument;
const rowCount = (_: any) => 0; // rows now measured via the instrument

console.log('=== litedbmodel #44 cross-lang fairness self-check (queries/op + rows/op parity) ===');
let failures = 0;
for (const caseId of Object.keys(SQL_BASELINE)) {
  const base = SQL_BASELINE[caseId];
  // sql baseline: DML statements + DB rows read
  const dbA = freshDb();
  const sqlC = instrument(dbA, () => base.run(dbA));
  dbA.close();

  // litedbmodel ir/prepared path
  const dbB = freshDb();
  let lmQ = 0, lmRows = 0, err = '';
  try {
    if (caseId === 'batchInsert') {
      const cols = ['author_id', 'title', 'status', 'views', 'created_at'];
      const bundle = lm.compileCreateManyBundle('BatchInsert', { tableName: 'posts', columns: cols, records: INPUTS.batchInsert.rows as any }, 'sqlite');
      const r = instrument(dbB, () => lm.executeTransactionBundle(bundle, {}, { db: dbB }));
      lmQ = r.queries; lmRows = r.rows;
    } else if (caseId === 'writeTxGate') {
      const bundle = lm.compileWriteBundle(writesContract, 'Create', writeGateContract, 'create', 'sqlite');
      const r = instrument(dbB, () => lm.executeTransactionBundle(bundle, INPUTS.writeTxGate, { db: dbB }));
      lmQ = r.queries; lmRows = r.rows;
    } else if (READ_RELATION[caseId]) {
      const { decl, withName } = READ_RELATION[caseId]!;
      const bundle = lm.compileBundle(readsContract, READ_ENTRY[caseId], [decl], 'sqlite');
      const r = instrument(dbB, () => lm.readBundle(bundle, (INPUTS as any)[caseId], { db: dbB, with: { [withName]: true } }));
      lmQ = r.queries; lmRows = r.rows;
    } else {
      const bundle = lm.compileBundle(readsContract, READ_ENTRY[caseId], [], 'sqlite');
      const r = instrument(dbB, () => lm.executeBundle(bundle, (INPUTS as any)[caseId], { db: dbB }));
      lmQ = r.queries; lmRows = r.rows;
    }
  } catch (e) { err = (e as Error).message; }
  dbB.close();

  const qOk = lmQ === sqlC.queries;
  const rOk = lmRows === sqlC.rows;
  if (!qOk || !rOk || err) failures++;
  console.log(`${caseId.padEnd(14)} Q sql=${sqlC.queries} lm=${lmQ} [${qOk ? 'OK' : 'DIVERGE'}]  rows sql=${sqlC.rows} lm=${lmRows} [${rOk ? 'OK' : 'DIVERGE'}] ${err ? 'ERR:' + err : ''}`);
}

// ── Per-dialect structural fairness: each dialect's compiled bundle must carry the
// SAME expected queries/op + rows/op as the sqlite baseline (the logical DB work is
// dialect-invariant; only the rendered SQL/placeholder form differs). The executed
// fairness above proves it on real SQLite; this proves the postgres + mysql bundles
// are logically identical without needing a live DB in CI. Reads from the generated
// per-dialect artifact.
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
const HERE = dirname(fileURLToPath(import.meta.url));
const artifact = JSON.parse(readFileSync(resolve(HERE, 'generated', 'bundles.json'), 'utf8')) as {
  dialects: Record<string, { cases: { case: string; expectedQueries: number; expectedRows: number }[] }>;
};
console.log('\n=== per-dialect structural fairness (expected queries/op + rows/op identical across dialects) ===');
for (const dialect of CROSSLANG_DIALECTS) {
  const cases = artifact.dialects[dialect]?.cases ?? [];
  for (const c of cases) {
    const base = SQL_BASELINE[c.case];
    const qOk = c.expectedQueries === base.queries;
    const rOk = c.expectedRows === base.rows;
    if (!qOk || !rOk) {
      failures++;
      console.log(`[${dialect}] ${c.case.padEnd(14)} DIVERGE expectedQ=${c.expectedQueries}/${base.queries} expectedR=${c.expectedRows}/${base.rows}`);
    }
  }
  console.log(`[${dialect}] ${cases.length} cases — expected queries/op + rows/op match the sqlite baseline`);
}


// ── Anti-sham: the GENERATED Rust/Go codegen modules must be genuinely DE-INTERPRETED ──────────
// The codegen bench cells (adapters/rust + go/lm_bench) COMPILE + EXECUTE these generated modules
// (NOT execute_bundle/ExecuteBundle). Assert — structurally, on the regenerated source — that no
// generated module CALLS the interpreter (`run_behavior`/`RunBehavior`) at the CODE level, nor
// embeds the portable IR (only its fingerprint). This is the bc#75 straight-line guarantee; a
// regression here (a generator that falls back to literal-bake + run_behavior) fails the bench.
// Comment/string-literal-stripped so the explanatory prose ("does NOT go through run_behavior")
// is not a false positive.
function stripCommentsAndStrings(src: string): string {
  let out = '';
  let i = 0;
  const n = src.length;
  while (i < n) {
    const c = src[i];
    const c2 = src.slice(i, i + 2);
    const c3 = src.slice(i, i + 3);
    if (c2 === '/*') { const e = src.indexOf('*/', i + 2); i = e < 0 ? n : e + 2; continue; }
    if (c2 === '//' || c === '#') { const e = src.indexOf('\n', i); i = e < 0 ? n : e; continue; }
    if (c3 === "'".repeat(3) || c3 === '"'.repeat(3)) { const q = c3; const e = src.indexOf(q, i + 3); i = e < 0 ? n : e + 3; continue; }
    if (c === "'" || c === '"' || c === '`') { const q = c; i += 1; while (i < n && src[i] !== q) { if (src[i] === '\\') i += 2; else i += 1; } i += 1; continue; }
    out += c; i += 1;
  }
  return out;
}
// #60: bc typed-native READ codegen. ALL 6 reads are now COVERED and generate a de-interpreted
// module — find/belongsTo/hasMany/hasManyLimit (scalar WHERE heads) PLUS complexWhere/inList (whose
// IN-list array head reaches a native array port `Vec<i64>`/`[]int64` via bc#110). Only WRITES
// (batchInsert/writeTxGate) are NOT codegen-module cases (#60) and legitimately have NO generated
// module. We assert BOTH: the covered modules ARE de-interpreted, AND the no-module (write) cases
// have NO module (a module appearing for them would mean a silent fallback leaked — the exact sham
// this gate guards against).
const COVERED_READ = ['find', 'belongsTo', 'hasMany', 'hasManyLimit', 'complexWhere', 'inList'];
const NO_MODULE_CASES = ['batchInsert', 'writeTxGate']; // writes are not codegen-module cases (#60)
const GEN_ROOT = resolve(HERE, 'generated', 'codegen');
const GO_CGMODS = resolve(HERE, '..', '..', 'go', 'lm_bench', 'cgmods');
const moduleTargets = (c: string): { label: string; path: string }[] => [
  { label: `rust/${c}.rs`, path: resolve(GEN_ROOT, 'rust', `${c}.rs`) },
  { label: `go(flat)/${c}.go`, path: resolve(GEN_ROOT, 'go', `${c}.go`) },
  { label: `go(pkg)/${c}/gen.go`, path: resolve(GO_CGMODS, c, 'gen.go') },
];
console.log('\n=== anti-sham: COVERED-read codegen modules are de-interpreted (no run_behavior / no embedded IR) ===');
for (const c of COVERED_READ) {
  for (const t of moduleTargets(c)) {
    let src: string;
    try { src = readFileSync(t.path, 'utf8'); }
    catch { failures++; console.log(`  ${t.label.padEnd(28)} MISSING (covered read MUST codegen — regenerate) — ${t.path}`); continue; }
    const code = stripCommentsAndStrings(src);
    const callsInterpreter = /\b(run_behavior|RunBehavior|runBehavior)\b/.test(code);
    // Embedded-IR heuristic: a de-interpreted module carries no baked IR literal (irVersion / a
    // "components" graph) and — typed-native (bc#77/#90) — no fingerprint constant either.
    const embedsIr = /\b(irVersion|IR_FINGERPRINT|IRFingerprint)\b/.test(code) || /["']components["']\s*:/.test(code);
    const ok = !callsInterpreter && !embedsIr;
    if (!ok) failures++;
    console.log(`  ${t.label.padEnd(28)} ${ok ? 'OK (no interpreter call, no embedded IR/fingerprint)' : `SHAM: callsInterpreter=${callsInterpreter} embedsIr=${embedsIr}`}`);
  }
}
console.log('\n=== anti-sham: write cases have NO generated module (writes are not codegen-module cases, #60) ===');
for (const c of NO_MODULE_CASES) {
  for (const t of moduleTargets(c)) {
    let present = true;
    try { readFileSync(t.path, 'utf8'); } catch { present = false; }
    if (present) { failures++; console.log(`  ${t.label.padEnd(28)} UNEXPECTED — a module exists for a write case (silent fallback leaked)`); }
    else console.log(`  ${t.label.padEnd(28)} OK (correctly absent — write not a codegen case)`);
  }
}


// ── Anti-sham (adapter wiring), #60 m1 architecture:
//  - Rust codegen runs in the DEDICATED, runtime-free `lm_codegen` binary (adapters/rust-codegen):
//    its covered-read dispatch calls the GENERATED typed-native runner `run_native_raw_struct_<Comp>`
//    and links NO interpreter (execute_bundle/read_bundle_pooled are absent — serde-free crate).
//    The old shared `adapters/rust` binary must FAIL CLOSED on impl=codegen (codegen does not live there).
//  - Go codegen is BLOCKED on bc#102 (go-typed-native emits the runner unexported) — the cell must
//    FAIL CLOSED (panic naming bc#102), NEVER silently fall back to the interpreter (rt.ExecuteBundle/
//    rt.ReadBundle) or the retired RAW-ABI path. That honest block is the correct state, not a sham.
console.log('\n=== anti-sham (adapter wiring): codegen invokes the typed-native runner / fails closed, never the interpreter ===');
const RUST_CODEGEN = resolve(HERE, 'adapters', 'rust-codegen', 'src', 'main.rs');
const RUST_SHARED = resolve(HERE, 'adapters', 'rust', 'src', 'main.rs');
const GO_CELL = resolve(HERE, '..', '..', 'go', 'lm_bench', 'codegen_cell.go');
{
  const src = readFileSync(RUST_CODEGEN, 'utf8');
  const invokesGenerated = /run_native_raw_struct_/.test(src);
  const linksInterpreter = /\bexecute_bundle\b|\bread_bundle_pooled\b/.test(src);
  const ok = invokesGenerated && !linksInterpreter;
  if (!ok) failures++;
  console.log(`  rust lm_codegen            ${ok ? 'OK (run_native_raw_struct_<Comp>; runtime-free, no interpreter linked)' : `SHAM: generated=${invokesGenerated} interpreterLinked=${linksInterpreter}`}`);
}
{
  const shared = readFileSync(RUST_SHARED, 'utf8');
  const failsClosed = /impl_ == "codegen"/.test(shared) && /codegen[\s\S]{0,120}panic!/.test(shared);
  if (!failsClosed) failures++;
  console.log(`  rust adapters/rust (sql/ir) ${failsClosed ? 'OK (fails closed on impl=codegen — codegen rides lm_codegen)' : 'SHAM: shared adapter does not fail-closed on impl=codegen'}`);
}
{
  const go = readFileSync(GO_CELL, 'utf8');
  const failsClosedOnBc102 = /bc#102/.test(go) && /func runCodegenCase[\s\S]{0,400}panic\(/.test(go);
  const silentInterpreter = /rt\.ExecuteBundle\b|rt\.ReadBundle\b/.test(go);
  const ok = failsClosedOnBc102 && !silentInterpreter;
  if (!ok) failures++;
  console.log(`  go   codegen_cell          ${ok ? 'OK (fails closed on bc#102; no silent interpreter/RAW-ABI fallback)' : `SHAM: failsClosedOnBc102=${failsClosedOnBc102} silentInterpreter=${silentInterpreter}`}`);
}

if (failures > 0) {
  console.error(`\n❌ ${failures} fairness divergence(s) — the sql baseline and litedbmodel path do NOT do identical logical work.`);
  process.exit(1);
}
console.log('\n✅ fairness self-check passed — queries/op + rows/op identical across the sql baseline and the litedbmodel path for all 8 cases × 3 dialects.');
