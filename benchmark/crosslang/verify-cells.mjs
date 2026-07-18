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
const BIN = {
  rust: join(HERE, 'adapters/rust/target/release/orm_bench_rust'),
  go: join(HERE, 'adapters/go/orm_bench_go'),
}[lang];
if (!BIN || !existsSync(BIN)) {
  console.error(`no ${lang} binary at ${BIN} — build it first`);
  process.exit(2);
}

const oracle = JSON.parse(readFileSync(join(ART, 'oracle.json'), 'utf8'));
const READ = new Set(['findAll', 'filterPaginateSort', 'findFirst', 'findUnique']);
// All 19 ORM ops (rust cell complete).
const OPS = [
  'findAll', 'filterPaginateSort', 'findFirst', 'findUnique',
  'nestedFindAll', 'nestedFindFirst', 'nestedFindUnique', 'nestedRelations', 'compositeRelations',
  'create', 'update', 'upsert', 'createMany', 'upsertMany', 'updateMany',
  'delete', 'nestedCreate', 'nestedUpdate', 'nestedUpsert',
];

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
    const p = spawnSync(BIN, ['run', op, work, cell], { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
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
