// Byte-equal driver: for each op, run the native + SDK cell and compare BOTH to the mode-2 oracle
// (dialect-independent — the sqlite reference oracle is the expected result on every dialect). A
// write/batch/tx op mutates its DB, so it runs on FRESH state each time: sqlite copies the seed file;
// postgres drops + recreates + reseeds. Non-zero exit from a cell is a failure.
//   node  benchmark/crosslang/verify-cells.mjs <lang>              # sqlite (default)
//   npx tsx benchmark/crosslang/verify-cells.mjs <lang> postgres   # live docker pg (tsx: imports the .ts seed)
import { readFileSync, copyFileSync, rmSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const ART = join(HERE, '.artifacts');
const lang = process.argv[2] ?? 'rust';
const dialect = process.argv[3] ?? 'sqlite';
// Per-lang command to run one op × cell (rust/go = compiled binary; ts = tsx script).
const CMD = {
  rust: (a) => [join(HERE, 'adapters/rust/target/release/orm_bench_rust'), ...a],
  go: (a) => [join(HERE, 'adapters/go/orm_bench_go'), ...a],
  ts: (a) => ['npx', 'tsx', join(HERE, 'adapters/ts/main.ts'), ...a],
}[lang];
if (!CMD) { console.error(`unknown lang '${lang}'`); process.exit(2); }

const oracle = JSON.parse(readFileSync(join(ART, 'oracle.json'), 'utf8'));
const READ_OPS = new Set([
  'findAll', 'filterPaginateSort', 'findFirst', 'findUnique',
  'nestedFindAll', 'nestedFindFirst', 'nestedFindUnique', 'nestedRelations', 'compositeRelations',
]);
const ALL19 = [
  'findAll', 'filterPaginateSort', 'findFirst', 'findUnique',
  'nestedFindAll', 'nestedFindFirst', 'nestedFindUnique', 'nestedRelations', 'compositeRelations',
  'create', 'update', 'upsert', 'createMany', 'upsertMany', 'updateMany',
  'delete', 'nestedCreate', 'nestedUpdate', 'nestedUpsert',
];

function runCell(op, cell, target) {
  const [c0, ...cargs] = CMD(['run', op, target, cell]);
  const p = spawnSync(c0, cargs, { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  if (p.status !== 0) return { err: `EXIT ${p.status}: ${(p.stderr || '').trim().split('\n').slice(-1)[0].slice(0, 80)}` };
  return { out: (p.stdout || '').trim() };
}
function compare(op, cell, res, expected, report) {
  if (res.err) { report[cell] = res.err; return false; }
  if (res.out === expected) { report[cell] = 'OK'; return true; }
  report[cell] = 'DIFF';
  console.log(`  ${op} ${cell} DIFF:\n    got : ${res.out.slice(0, 160)}\n    want: ${expected.slice(0, 160)}`);
  return false;
}

let fail = 0;
console.log(`[${lang} × ${dialect}] op            native   sdk    (vs dialect-independent oracle)`);

if (dialect === 'sqlite') {
  for (const op of ALL19) {
    const expected = oracle[op]?.result;
    if (expected === undefined) { console.log(`${op.padEnd(22)}NO ORACLE`); fail = 1; continue; }
    const report = {};
    for (const cell of ['native', 'sdk']) {
      const work = join(ART, `bench.${op}.${cell}.db`);
      copyFileSync(join(ART, 'bench.db'), work); // fresh seed copy per invocation
      const ok = compare(op, cell, runCell(op, cell, work), expected, report);
      rmSync(work, { force: true });
      if (!ok) fail = 1;
    }
    console.log(`${op.padEnd(22)}${String(report.native).padEnd(9)}${report.sdk}`);
  }
} else if (dialect === 'postgres') {
  const pg = (await import('pg')).default;
  const { ddl, seedStatements, dropStatements, pgSeqResetStatements } = await import('./orm-domain.ts');
  const CONN = { host: 'localhost', port: 5433, user: 'testuser', password: 'testpass', database: 'testdb' };
  const CONN_STR = `host=${CONN.host} port=${CONN.port} user=${CONN.user} password=${CONN.password} dbname=${CONN.database}`;
  const toPg = (sql) => { let i = 0; return sql.replace(/\?/g, () => `$${++i}`); };
  const client = new pg.Client(CONN);
  await client.connect();
  const reset = async () => {
    for (const s of dropStatements('postgres')) await client.query(s);
    for (const s of ddl('postgres')) await client.query(s);
    for (const s of seedStatements('postgres')) await client.query(toPg(s.sql), s.params);
    for (const s of pgSeqResetStatements()) await client.query(s);
  };
  await reset(); // seed for the read ops (non-mutating; they share this state)
  for (const op of ALL19) {
    const expected = oracle[op]?.result;
    if (expected === undefined) { console.log(`${op.padEnd(22)}NO ORACLE`); fail = 1; continue; }
    const report = {};
    for (const cell of ['native', 'sdk']) {
      if (!READ_OPS.has(op)) await reset(); // fresh state per mutating cell run
      if (!compare(op, cell, runCell(op, cell, CONN_STR), expected, report)) fail = 1;
    }
    console.log(`${op.padEnd(22)}${String(report.native).padEnd(9)}${report.sdk}`);
  }
  await client.end();
} else if (dialect === 'mysql') {
  const mysql = (await import('mysql2/promise')).default;
  const { ddl, seedStatements } = await import('./orm-domain.ts');
  const CONN = { host: 'localhost', port: 3307, user: 'testuser', password: 'testpass', database: 'testdb' };
  const CONN_STR = `mysql://${CONN.user}:${CONN.password}@${CONN.host}:${CONN.port}/${CONN.database}`;
  const TABLES = ['benchmark_tenant_comments', 'benchmark_tenant_posts', 'benchmark_tenant_users', 'benchmark_comments', 'benchmark_posts', 'benchmark_users'];
  const conn = await mysql.createConnection(CONN);
  const reset = async () => {
    for (const t of TABLES) await conn.query(`DROP TABLE IF EXISTS ${t}`);
    for (const s of ddl('mysql')) await conn.query(s);
    for (const s of seedStatements('mysql')) await conn.query(s.sql, s.params);
  };
  await reset(); // seed for the read ops (non-mutating; they share this state)
  for (const op of ALL19) {
    const expected = oracle[op]?.result;
    if (expected === undefined) { console.log(`${op.padEnd(22)}NO ORACLE`); fail = 1; continue; }
    const report = {};
    for (const cell of ['native', 'sdk']) {
      if (!READ_OPS.has(op)) await reset(); // fresh state per mutating cell run
      if (!compare(op, cell, runCell(op, cell, CONN_STR), expected, report)) fail = 1;
    }
    console.log(`${op.padEnd(22)}${String(report.native).padEnd(9)}${report.sdk}`);
  }
  await conn.end();
} else {
  console.error(`unknown dialect '${dialect}'`);
  process.exit(2);
}

console.log(fail ? '\nBYTE-EQUAL: FAILURES ABOVE' : `\nBYTE-EQUAL: native == sdk == oracle for all 19 ops (${lang} × ${dialect})`);
process.exit(fail);
