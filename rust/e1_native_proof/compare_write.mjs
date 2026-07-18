// Leg 3b — WRITE execution + resulting DB state vs the mode-2 oracle.
//
// A write MUTATES its DB, so each op runs against a FRESH COPY of the clean seed (the same seed the
// TS leg computed its oracle from). The binary prints {result, state}; the oracle carries the mode-2
// {result, state}. Both must match byte-for-byte — the RETURNING rows / summary AND the resulting
// table state. A non-zero exit is a FAILURE (crash-path safe, per the read-leg lesson).
import { readFileSync, copyFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

const [bin, seed, oraclePath] = process.argv.slice(2);
const oracles = JSON.parse(readFileSync(oraclePath, 'utf8'));

let fail = 0;
// Each oracle case carries its own rust `op` (the dispatch + shared module) + `args` — so upsert's
// two paths (insert / conflict) drive the ONE upsert module with different inputs.
for (const [key, expected] of Object.entries(oracles)) {
  const work = `${seed}.${key}.work`;
  copyFileSync(seed, work); // fresh copy — the write mutates THIS, never the seed
  const p = spawnSync(bin, [expected.op, work, ...(expected.args ?? [])], { encoding: 'utf8' });
  const op = key;
  if (p.status !== 0) {
    console.log(`  FAIL  ${op} — exited ${p.status}: ${(p.stderr || '').trim().split('\n').slice(0, 3).join(' | ')}`);
    fail = 1;
    continue;
  }
  let actual;
  try {
    actual = JSON.parse((p.stdout || '').trim());
  } catch {
    console.log(`  FAIL  ${op} — non-JSON stdout: ${(p.stdout || '').trim().slice(0, 120)}`);
    fail = 1;
    continue;
  }
  const resultOk = JSON.stringify(actual.result) === JSON.stringify(expected.result);
  const stateOk = JSON.stringify(actual.state) === JSON.stringify(expected.state);
  if (resultOk && stateOk) {
    console.log(`  PASS  ${op} — result + resulting DB state byte-equal to the oracle`);
  } else {
    console.log(`  FAIL  ${op}`);
    if (!resultOk) console.log(`        result rust  : ${JSON.stringify(actual.result)}\n        result oracle: ${JSON.stringify(expected.result)}`);
    if (!stateOk) console.log(`        state  rust  : ${JSON.stringify(actual.state)}\n        state  oracle: ${JSON.stringify(expected.state)}`);
    fail = 1;
  }
}
process.exit(fail);
