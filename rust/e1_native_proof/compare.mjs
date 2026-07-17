// Leg 3 driver — replay every oracle input through the generated module + exec seam and compare
// byte-for-byte against the mode-2 `executeBundle` result.
//
// Deliberately NOT a bash read-loop: an empty argument (the EMPTY IN-list) is a real, meaningful
// input, and a shell field-split silently mangled it AND swallowed a binary panic into an empty
// stdout that compared "equal" — a false PASS. This driver passes args verbatim and treats a
// non-zero exit / stderr panic as a FAILURE, so a green here cannot be vacuous.
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

const [bin, db, op, oraclePath] = process.argv.slice(2);
const oracles = JSON.parse(readFileSync(oraclePath, 'utf8'));

let fail = 0;
for (const [key, expectedVal] of Object.entries(oracles)) {
  const expected = JSON.stringify(expectedVal);
  const p = spawnSync(bin, [op, db, key], { encoding: 'utf8' });
  if (p.status !== 0) {
    console.log(`  FAIL  ${op}(${JSON.stringify(key)}) — exited ${p.status}`);
    console.log(`        ${(p.stderr || '').trim().split('\n').slice(0, 3).join(' | ')}`);
    fail = 1;
    continue;
  }
  const actual = (p.stdout || '').trim();
  if (actual === expected) {
    console.log(`  PASS  ${op}(${JSON.stringify(key)}) -> ${actual}`);
  } else {
    console.log(`  FAIL  ${op}(${JSON.stringify(key)})`);
    console.log(`        rust  : ${actual}`);
    console.log(`        oracle: ${expected}`);
    fail = 1;
  }
}
process.exit(fail);
