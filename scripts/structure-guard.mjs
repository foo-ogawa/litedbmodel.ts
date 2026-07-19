#!/usr/bin/env node
// Structure guard (epic #123 / #124) — mechanically enforce the native-codegen INVARIANTS so the
// "rust × 3 dialects × 20 ops" surface stays a factored (executor 1 · Dialect-in-SQL-gen · shared
// operation-meaning) design and never regresses into 60 bespoke implementations. Run in CI; exits 1 on
// any violation. Language-generic where it can be; this commit verifies RUST only (#125/#126/#128 extend
// the per-language cells on the SAME checks).
//
// Usage: node scripts/structure-guard.mjs
import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const rd = (p) => readFileSync(join(ROOT, p), 'utf8');
const fails = [];
const oks = [];
const check = (name, ok, detail = '') => (ok ? oks.push(name) : fails.push(`${name}${detail ? ` — ${detail}` : ''}`));

// ── 1. The rust executor is defined EXACTLY once (op count is irrelevant; DB count is irrelevant). ──
const execSrc = rd('rust/litedbmodel_runtime/src/codegen_exec.rs');
const execCount = (execSrc.match(/\bpub fn exec\(/g) ?? []).length;
check('invariant 1: rust executor defined exactly once', execCount === 1, `found ${execCount} \`pub fn exec(\``);

// Strip `//` line comments + `/* … */` block comments — the invariants are CODE-level (a DB branch, an
// emitted marker), not prose; explanatory comments legitimately NAME the forbidden things to say what is
// NOT done, so they must not trip the mechanical greps.
function stripComments(src) {
  return src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '');
}

// ── 2/3/4. The executor takes only SQL/params/mode: no operation name, no DB-kind branch inside it. ──
// Extract the `pub fn exec(` body (brace-matched) and assert it names neither a DB dialect nor an op.
function fnBody(src, header) {
  const start = src.indexOf(header);
  if (start < 0) return '';
  let i = src.indexOf('{', start);
  let depth = 0;
  const from = i;
  for (; i < src.length; i++) {
    if (src[i] === '{') depth++;
    else if (src[i] === '}' && --depth === 0) return src.slice(from, i + 1);
  }
  return '';
}
const execBody = stripComments(fnBody(execSrc, 'pub fn exec('));
check('invariant 3: executor does not branch on DB kind', !/postgres|mysql|sqlite|Postgres|Mysql|Sqlite/.test(execBody), 'executor body names a dialect/driver');
check(
  'invariant 2/4: executor takes only ctx/sql/params/mode (no operation names, no dialect text)',
  /pub fn exec\(\s*ctx: &ExecutionContext,\s*sql: &str,\s*params: &\[Value\],\s*mode: ExecMode,?\s*\)/.test(execSrc.replace(/\n/g, ' ')),
  'executor signature is not (ctx, sql, params, mode)',
);

// ── 5. The dialect `?`→`$N` renumber is reused from static_bundle (SQL-gen stage), not re-done here. ──
check('invariant 5: pg placeholder renumber reused (render_placeholders), not re-implemented', execSrc.includes('render_placeholders'), 'exec seam does not reuse render_placeholders');

// ── 6/7/8. The e1 GENERATED + COMPANION files carry NO rusqlite and NO concrete Driver type — the
//    companion is a uniform ports→executor delegation over the runtime Driver, not a per-DB adapter. ──
const e1src = 'rust/e1_native_proof/src';
const genCompFiles = existsSync(join(ROOT, e1src))
  ? readdirSync(join(ROOT, e1src)).filter((f) => /^(generated|companion)_.*\.rs$/.test(f))
  : [];
check('e1 has generated + companion files', genCompFiles.length >= 40, `found ${genCompFiles.length} (want 20 generated + 20 companion)`);
for (const marker of ['rusqlite', 'SqliteDriver', 'MysqlDriver', 'PostgresDriver']) {
  const hits = genCompFiles.filter((f) => rd(join(e1src, f)).includes(marker));
  check(`generated/companion carry no \`${marker}\` (Driver is injected via runtime)`, hits.length === 0, hits.join(', '));
}
// The companion is a uniform delegation: every node_* body calls a litedbmodel_runtime executor.
const companionFiles = genCompFiles.filter((f) => f.startsWith('companion_'));
const noHandwrittenExec = companionFiles.every((f) => {
  const s = rd(join(e1src, f));
  // no direct driver.prepare / raw SQL execution hand-written in a companion (must go through the executors).
  return !/\.prepare\(|\.query\(|\.execute_batch\(/.test(s);
});
check('invariant 7: companions delegate to runtime executors (no hand-written driver exec)', noHandwrittenExec);

// ── The heavy mysql RETURNING-emulation SCAFFOLD is gone: no `mysqlWriteReselect`, no emitted
//    `/*scp-reselect: SELECT…*/` marker (the driver emulates RETURNING). The LIGHTWEIGHT `/*scp:pk=…*/`
//    hint (mode-2's `runtime.ts:390` SSoT the driver reads) IS ALLOWED — it is not checked here. Comments
//    are stripped so a doc note that MENTIONS the retired marker (to say it is gone) does not trip this. ──
const reselHits = [];
for (const rel of ['src/scp/codegen.ts', 'rust/litedbmodel_runtime/src', 'rust/e1_native_proof/src']) {
  const p = join(ROOT, rel);
  if (!existsSync(p)) continue;
  const files = rel.endsWith('.ts') ? [rel] : readdirSync(p).filter((f) => f.endsWith('.rs')).map((f) => join(rel, f));
  for (const f of files) if (/mysqlWriteReselect|scp-reselect/.test(stripComments(rd(f)))) reselHits.push(f);
}
check('mysql reselect scaffold removed (mysqlWriteReselect / scp-reselect marker)', reselHits.length === 0, reselHits.join(', '));

// ── No new DB-connection crate dependency in the reference cell (Driver comes from the runtime). ──
const e1cargo = rd('rust/e1_native_proof/Cargo.toml');
const depBlock = e1cargo.slice(e1cargo.indexOf('[dependencies]'), e1cargo.indexOf('[features]') >= 0 ? e1cargo.indexOf('[features]') : undefined);
const forbiddenDeps = ['rusqlite', 'tokio-postgres', 'sqlx', 'postgres =', 'mysql ='].filter((d) => depBlock.includes(d));
check('no new DB-connection crate dep in e1_native_proof', forbiddenDeps.length === 0, forbiddenDeps.join(', '));

// ── report ──
console.log('structure-guard (#123/#124) — rust\n');
for (const o of oks) console.log(`  PASS  ${o}`);
for (const f of fails) console.log(`  FAIL  ${f}`);
console.log(`\n${fails.length === 0 ? 'STRUCTURE GUARD: ALL PASS' : `STRUCTURE GUARD: ${fails.length} FAILURE(S)`}`);
process.exit(fails.length === 0 ? 0 : 1);
