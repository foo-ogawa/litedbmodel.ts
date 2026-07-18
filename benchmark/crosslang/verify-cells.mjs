// Byte-equal driver: for each op, run the native + SDK cell and compare BOTH to the mode-2 oracle.
// A write/batch op mutates its DB, so each invocation runs on a FRESH copy of the seed. Non-zero exit
// from a cell is a failure. Run: node benchmark/crosslang/verify-cells.mjs <lang>
import { readFileSync, copyFileSync, rmSync, existsSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const ART = join(HERE, '.artifacts');
const lang = process.argv[2] ?? 'rust';
// Per-lang command to run one op × cell (rust/go = compiled binary; ts = tsx script).
const CMD = {
  rust: (a) => [join(HERE, 'adapters/rust/target/release/orm_bench_rust'), ...a],
  go: (a) => [join(HERE, 'adapters/go/orm_bench_go'), ...a],
  ts: (a) => ['npx', 'tsx', join(HERE, 'adapters/ts/main.ts'), ...a],
}[lang];
if (!CMD) { console.error(`unknown lang '${lang}'`); process.exit(2); }

const oracle = JSON.parse(readFileSync(join(ART, 'oracle.json'), 'utf8'));
const READ = new Set(['findAll', 'filterPaginateSort', 'findFirst', 'findUnique']);
const ALL19 = [
  'findAll', 'filterPaginateSort', 'findFirst', 'findUnique',
  'nestedFindAll', 'nestedFindFirst', 'nestedFindUnique', 'nestedRelations', 'compositeRelations',
  'create', 'update', 'upsert', 'createMany', 'upsertMany', 'updateMany',
  'delete', 'nestedCreate', 'nestedUpdate', 'nestedUpsert',
];
// rust + go + ts all complete (19).
const OPS = ALL19;

let fail = 0;
console.log(`op                    native   sdk    (vs mode-2 oracle)`);
for (const op of OPS) {
  const expected = oracle[op]?.result;
  if (expected === undefined) { console.log(`${op.padEnd(22)}NO ORACLE`); fail = 1; continue; }
  const results = {};
  for (const cell of ['native', 'sdk']) {
    // fresh DB copy per invocation (a write mutates it; reads are harmless but copy for uniformity)
    const work = join(ART, `bench.${op}.${cell}.db`);
    copyFileSync(join(ART, 'bench.db'), work);
    const [c0, ...cargs] = CMD(['run', op, work, cell]);
    const p = spawnSync(c0, cargs, { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
    rmSync(work, { force: true });
    if (p.status !== 0) { results[cell] = `EXIT ${p.status}: ${(p.stderr || '').trim().split('\n')[0].slice(0, 60)}`; continue; }
    const got = (p.stdout || '').trim();
    results[cell] = got === expected ? 'OK' : 'DIFF';
    if (got !== expected) {
      console.log(`  ${op} ${cell} DIFF:\n    got : ${got.slice(0, 120)}\n    want: ${expected.slice(0, 120)}`);
    }
  }
  if (results.native !== 'OK' || results.sdk !== 'OK') fail = 1;
  console.log(`${op.padEnd(22)}${String(results.native).padEnd(9)}${results.sdk}`);
  void READ;
}
console.log(fail ? '\nBYTE-EQUAL: FAILURES ABOVE' : '\nBYTE-EQUAL: native == sdk == oracle for all covered ops');
process.exit(fail);
