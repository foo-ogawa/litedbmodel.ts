import * as lm from '../../dist/scp/index.mjs';
import {
  freshDb, readsContract, writesContract, writeGateContract,
  READ_ENTRY, READ_RELATION, INPUTS, SQL_BASELINE,
} from './domain.js';

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

if (failures > 0) {
  console.error(`\n❌ ${failures} fairness divergence(s) — the sql baseline and litedbmodel path do NOT do identical logical work.`);
  process.exit(1);
}
console.log('\n✅ fairness self-check passed — queries/op + rows/op identical across sql baseline and litedbmodel path for all 8 cases.');
