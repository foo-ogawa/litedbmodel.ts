// LIVE-DB leg — native(codegen) vs mode-2(interpreter) on the SAME live connection, deep-equal.
//
// The native cell runs each op through the litedbmodel-GENERATED module (companion + runtime Driver);
// the mode-2 cell runs the SAME op through the runtime's INTERPRETER entry (`execute_bundle` for
// read/write, `execute_transaction_bundle` for tx — the `mode2` subcommand) on the SAME docker DB.
// Two DISTINCT code paths, one real DB → a genuine conformance check, NOT vs a sqlite oracle (whose
// engine differs, e.g. MySQL InnoDB's AUTO_INCREMENT-on-conflict). A write/tx MUTATES, so the DB is
// RE-SEEDED to the fixed state before EACH leg so both start identical. Non-zero exit = FAILURE.
//
//   node compare-livedb.mjs <postgres|mysql> <spec> <bin>
//
// Cases: read inputs from cases_read.json; write/tx op+args+input from oracles_{write,tx}.json (the
// test is the SSoT for both — this harness carries no hand-written op inputs). deleteuser (no-RETURNING
// write summary) and relbatch/relsingle (batched-map {rows,posts}) are OMITTED: their native output
// model has no distinct matching interpreter entry (see report). sqlite covers all 20 via run-proof.sh.
import { readFileSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { join } from 'node:path';
import { seedE1 } from './livedb-seed.mjs';

const [dialect, spec, bin] = process.argv.slice(2);
const PROOF = '/tmp/e1proof';
const BUNDLE = (op) => join(PROOF, dialect, `bundle_${op}.json`);
const INPUT_TMP = join(PROOF, `mode2_input.${dialect}.json`);
let fail = 0;

/** Ops whose native (bc de-box) output model has no distinct matching interpreter entry — reported,
 * not compared here (sqlite's run-proof.sh still covers them vs the TS interpreter oracle). */
const OMIT = new Set(['deleteuser', 'relbatch', 'relsingle']);

/** Stable deep-equal via sorted-key canonical JSON (object key order-independent). */
function stable(v) {
  if (Array.isArray(v)) return `[${v.map(stable).join(',')}]`;
  if (v && typeof v === 'object') return `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${stable(v[k])}`).join(',')}}`;
  return JSON.stringify(v);
}
function runJson(args) {
  const p = spawnSync(bin, args, { encoding: 'utf8' });
  if (p.status !== 0) return { err: `exit ${p.status}: ${(p.stderr || '').trim().split('\n').slice(0, 3).join(' | ')}` };
  try { return { val: JSON.parse((p.stdout || '').trim()) }; }
  catch { return { err: `non-JSON: ${(p.stdout || '').slice(0, 120)}` }; }
}

/** Run native (op+args) and mode-2 (kind+bundle+input) on the same live DB and deep-equal. */
async function compareCase({ key, op, args, input, kind, seedState }) {
  if (OMIT.has(op)) return;
  writeFileSync(INPUT_TMP, JSON.stringify(input));
  let nat, m2;
  if (kind === 'read') {
    nat = runJson([op, spec, ...args]);
    m2 = runJson(['mode2', spec, 'read', BUNDLE(op), INPUT_TMP]);
  } else {
    await seedE1(dialect, seedState);
    nat = runJson([op, spec, ...args]);
    await seedE1(dialect, seedState);
    m2 = runJson(['mode2', spec, kind, BUNDLE(op), INPUT_TMP]);
  }
  const label = `${op}(${key})`;
  if (nat.err) { console.log(`  FAIL  ${label} — native ${nat.err}`); fail = 1; return; }
  if (m2.err) { console.log(`  FAIL  ${label} — mode-2 ${m2.err}`); fail = 1; return; }
  if (stable(nat.val) === stable(m2.val)) console.log(`  PASS  ${label} — native == mode-2`);
  else {
    console.log(`  FAIL  ${label}\n        native: ${stable(nat.val)}\n        mode-2: ${stable(m2.val)}`);
    fail = 1;
  }
}

// READ leg (pre-seeded read state, no mutation).
await seedE1(dialect, 'read');
console.log('── READ: native(codegen) vs mode-2(interpreter), same live DB ──');
for (const c of JSON.parse(readFileSync(join(PROOF, 'cases_read.json'), 'utf8'))) {
  await compareCase({ ...c, kind: 'read' });
}

// N+1 guard: the batched relation issues 1 parent + 1 child query (native cell, consumer-side count).
{
  const p = spawnSync(bin, ['relbatch', spec, '1'], { encoding: 'utf8' });
  const qc = (p.stderr || '').split('\n').map((l) => l.match(/^queries=(\d+)/)).find(Boolean)?.[1];
  if (qc === '2') console.log(`  PASS  relbatch issued ${qc} queries (1 parent + 1 BATCHED child)`);
  else { console.log(`  FAIL  relbatch issued ${qc} (expected 2)`); fail = 1; }
}

// WRITE leg (op+args+input from the write oracle = test SSoT; re-seed 'write' per leg).
console.log('── WRITE: native(codegen) vs mode-2(interpreter), same live DB ──');
for (const [key, c] of Object.entries(JSON.parse(readFileSync(join(PROOF, 'oracles_write.json'), 'utf8')))) {
  await compareCase({ key, op: c.op, args: c.args ?? [], input: c.input, kind: 'write', seedState: 'write' });
}

// TX leg (op+args+input from the tx oracle = test SSoT; re-seed 'tx' per leg).
console.log('── TX: native(codegen) vs mode-2(interpreter), same live DB ──');
for (const [key, c] of Object.entries(JSON.parse(readFileSync(join(PROOF, 'oracles_tx.json'), 'utf8')))) {
  await compareCase({ key, op: c.op, args: c.args ?? [], input: c.input, kind: 'tx', seedState: 'tx' });
}

console.log();
console.log(fail === 0 ? `LIVE-DB native==mode-2 (${dialect}): ALL COMPARED LEGS PASS` : `LIVE-DB native==mode-2 (${dialect}): FAILURES ABOVE`);
process.exit(fail);
