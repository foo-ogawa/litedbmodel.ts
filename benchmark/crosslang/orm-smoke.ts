// ════════════════════════════════════════════════════════════════════════════
// TS ORM-plan LIVE SMOKE (epic #63) — every 19 ops × 3 real DBs execute DB-backed.
// ════════════════════════════════════════════════════════════════════════════
//
// Runs each of the 19 ORM ops once per dialect against the REAL databases (sqlite
// in-proc, mysql :3307, postgres :5433) and reports the 57-cell pass matrix + rows/op.
// A single cell failure is reported explicitly (op + dialect + error) — never silently
// skipped. This is the correctness gate for the TS reference executor before the
// per-language ports.
//
// Run: PATH=… npx tsx benchmark/crosslang/orm-smoke.ts

import { buildOrmPlanArtifact, ORM_OPS, type OrmDialect } from './orm-plan.js';
import { sqliteDriver, pgDriver, mysqlDriver, type OrmDriver } from './orm-exec-ts.js';
import {
  PG_SCHEMA_NAME, MYSQL_DB_NAME, PG_CONN, PG_BOOT_CONN, MYSQL_CONN, MYSQL_BOOT_CONN,
} from './domain.js';

async function main(): Promise<void> {
  const art = buildOrmPlanArtifact();
  const dialects: OrmDialect[] = ['sqlite', 'mysql', 'postgres'];

  const drivers: Record<OrmDialect, OrmDriver> = {
    sqlite: sqliteDriver(),
    postgres: await pgDriver(PG_SCHEMA_NAME, PG_CONN as never, PG_BOOT_CONN as never),
    mysql: await mysqlDriver(MYSQL_DB_NAME, MYSQL_CONN as never, MYSQL_BOOT_CONN as never),
  };

  let pass = 0;
  let fail = 0;
  const rowsByOp: Record<string, Record<string, number | string>> = {};

  for (const op of ORM_OPS) {
    rowsByOp[op.id] = {};
    for (const d of dialects) {
      try {
        const n = await drivers[d].run(art[op.id][d]);
        rowsByOp[op.id][d] = n;
        pass++;
      } catch (e) {
        rowsByOp[op.id][d] = `ERR: ${e instanceof Error ? e.message.split('\n')[0] : String(e)}`;
        fail++;
      }
    }
  }

  // Report matrix.
  console.log('\n19 ORM ops × 3 DBs — rows/op (writes report statements executed):\n');
  const pad = (s: string, n: number) => s.padEnd(n);
  console.log(pad('op', 42), pad('sqlite', 14), pad('mysql', 14), 'postgres');
  for (const op of ORM_OPS) {
    const r = rowsByOp[op.id];
    console.log(pad(`${op.write ? 'W' : 'R'} ${op.label}`, 42), pad(String(r.sqlite), 14), pad(String(r.mysql), 14), String(r.postgres));
  }
  console.log(`\n${pass}/${pass + fail} cells green (${ORM_OPS.length} ops × 3 DBs = ${ORM_OPS.length * 3}).`);

  for (const d of dialects) await drivers[d].close();
  if (fail > 0) {
    console.error(`\nSMOKE FAILED: ${fail} cell(s) errored (see ERR above).`);
    process.exit(1);
  }
  console.log('SMOKE PASS: all cells DB-backed on all 3 real DBs.');
}

void main();
