// Leg — LIVE-DB (pg/mysql) byte-equality vs the SAME dialect-independent mode-2 oracle (oracles*.json).
//
// The native-codegen cell runs the SAME 20 ops against a REAL docker Postgres/MySQL through
// litedbmodel_runtime's PostgresDriver/MysqlDriver (the binary picks the driver from the `pg:`/`mysql:`
// spec). READ ops run against the pre-seeded read state and compare stdout to the oracle. WRITE/TX ops
// MUTATE, so — the live twin of compare_write.mjs's per-case fresh-copy — this RE-SEEDS the live DB to
// the fixed state before EACH case (via the SAME orm-domain seeder), runs the op, and compares
// {result, state}. A non-zero exit is a FAILURE (crash-path safe).
//
//   node compare-livedb.mjs <postgres|mysql> <spec> <bin> <op> <oracle.json> [reseedState]
//     reseedState empty ⇒ read comparison; 'write'|'tx' ⇒ re-seed that state + compare {result,state}.
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { seedE1 } from './livedb-seed.mjs';

const [dialect, spec, bin, op, oraclePath, reseedState] = process.argv.slice(2);
const oracles = JSON.parse(readFileSync(oraclePath, 'utf8'));
let fail = 0;

for (const [key, val] of Object.entries(oracles)) {
  if (reseedState) {
    // WRITE / TX: re-seed the live DB to the fixed state, then run this case's op on it.
    await seedE1(dialect, reseedState);
    const p = spawnSync(bin, [val.op, spec, ...(val.args ?? [])], { encoding: 'utf8' });
    if (p.status !== 0) {
      console.log(`  FAIL  ${key} — exited ${p.status}: ${(p.stderr || '').trim().split('\n').slice(0, 3).join(' | ')}`);
      fail = 1;
      continue;
    }
    let actual;
    try { actual = JSON.parse((p.stdout || '').trim()); } catch { console.log(`  FAIL  ${key} — non-JSON: ${(p.stdout || '').slice(0, 120)}`); fail = 1; continue; }
    const rOk = JSON.stringify(actual.result) === JSON.stringify(val.result);
    const sOk = JSON.stringify(actual.state) === JSON.stringify(val.state);
    if (rOk && sOk) console.log(`  PASS  ${key} — result + state byte-equal`);
    else {
      console.log(`  FAIL  ${key}`);
      if (!rOk) console.log(`        result rust  : ${JSON.stringify(actual.result)}\n        result oracle: ${JSON.stringify(val.result)}`);
      if (!sOk) console.log(`        state  rust  : ${JSON.stringify(actual.state)}\n        state  oracle: ${JSON.stringify(val.state)}`);
      fail = 1;
    }
  } else {
    // READ: pre-seeded read state; compare stdout byte-for-byte. Key may pack `|`-separated CLI args.
    const cliArgs = key.includes('|') ? key.split('|') : [key];
    const p = spawnSync(bin, [op, spec, ...cliArgs], { encoding: 'utf8' });
    if (p.status !== 0) {
      console.log(`  FAIL  ${op}(${JSON.stringify(key)}) — exited ${p.status}: ${(p.stderr || '').trim().split('\n').slice(0, 3).join(' | ')}`);
      fail = 1;
      continue;
    }
    const actual = (p.stdout || '').trim();
    const expected = JSON.stringify(val);
    if (actual === expected) console.log(`  PASS  ${op}(${JSON.stringify(key)})`);
    else { console.log(`  FAIL  ${op}(${JSON.stringify(key)})\n        rust  : ${actual}\n        oracle: ${expected}`); fail = 1; }
  }
}
process.exit(fail);
