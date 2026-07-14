// ════════════════════════════════════════════════════════════════════════════
// ALL-TYPE coverage round-trip verifier (issue #59) — typed de-box conversion audit
// ════════════════════════════════════════════════════════════════════════════
//
// Verifies the TS read-path TYPED DE-BOX (owner-approved #59 contract) end-to-end for the
// ALL-TYPE coverage table across ALL THREE dialects (SQLite in-proc + LIVE Postgres + LIVE
// MySQL): the DB value → driver wire → materialized JS value → EXPECTED round-trip, asserting
// each column materializes to BOTH the right JS TYPE and the exact VALUE.
//
// The TS read-path materialization contract (driven by the SQL column type, #59):
//   • 32-bit int  (INT/INTEGER/SMALLINT/TINYINT/MEDIUMINT)  → JS number   (exact, JSON-safe)
//   • 64-bit int  (BIGINT/INT8)                             → JS string   (value-preserving,
//                     exact + JSON-safe; a number rounds past 2^53, a bigint throws in JSON)
//   • float       (REAL/DOUBLE/FLOAT)                       → JS number
//   • decimal     (DECIMAL/NUMERIC)                         → JS string   (precision-preserving)
//   • text/uuid                                             → JS string
//   • bool        (BOOLEAN)                                 → JS boolean
//   • date/time   (DATE/TIMESTAMP/TIMESTAMPTZ/DATETIME/TIME)→ JS string   (TZ-attached; NOT a Date)
//   • json        (JSON/JSONB)                              → JSON text (string) or driver-parsed
//
// bc 0.6.0 has NO date/decimal portable scalar (behavior-contracts#84 deferred), so date→string
// and decimal→string are value-preserving; and BIGINT→string mirrors them (bigint is not
// JSON-safe). The read path applies these via `materializeCell` driven by the coltype resolver;
// the drivers are configured (better-sqlite3 safeIntegers, pg date type parsers, mysql2
// supportBigNumbers+bigNumberStrings+dateStrings) so each value arrives in a coercible form.
//
// Two planes are checked:
//   • DYNAMIC (executed) — the value the shipped TS read path (`executeBundle` /
//     `executeBundleAsync`) returns, run against all three LIVE drivers. This is what the harness
//     executes + asserts vs EXPECTED, per column, for BOTH type and value.
//   • GENERATED (native) — verified at the TYPE-DERIVATION level only (the emitted rust struct
//     field types); its native VALUE run is deferred to the #44 cross-lang re-bench.

import Database from 'better-sqlite3';
import { Pool, types as pgTypes } from 'pg';
import mysql from 'mysql2/promise';
import * as lm from '../../dist/scp/index.mjs';
import {
  SCHEMA, PG_SCHEMA, MYSQL_SCHEMA,
  readsContract, COVERAGE_ENTRY, COVERAGE_INPUT, COVERAGE_COLUMNS,
  COVERAGE_EXPECTED, COVERAGE_EXPECTED_SCALAR, COVERAGE_EXPECTED_MATERIALIZE, type CoverageRow,
  seedCoverage, seedCoverageStatements,
  PG_CONN, PG_BOOT_CONN, PG_SCHEMA_NAME, MYSQL_CONN, MYSQL_BOOT_CONN, MYSQL_DB_NAME,
} from './domain.js';

type Row = Record<string, unknown>;
type MatClass = 'number' | 'bigint-string' | 'float' | 'string' | 'bool' | 'json';

// Configure the pg date-family type parsers ONCE (global on the pg module) so DATE/TIMESTAMP
// arrive as their native textual string (not a JS Date) — the coercible form the read-path
// materializer expects.
lm.configurePgDeboxTypeParsers(pgTypes);

interface Failure { where: string; detail: string }
const failures: Failure[] = [];

const EPS = 1e-9;

// ── Per-column assertion: the DYNAMIC (materialized) value has the RIGHT JS type AND value ────
function checkColumn(dialect: string, rowId: number, col: string, got: unknown, expected: unknown): void {
  const klass = COVERAGE_EXPECTED_MATERIALIZE[col];
  const fail = (detail: string): void => { failures.push({ where: `${dialect}.${col}.row${rowId}`, detail }); };
  if (expected === null) {
    if (got !== null && got !== undefined) fail(`expected NULL, got ${typeof got} ${JSON.stringify(String(got))}`);
    return;
  }
  switch (klass) {
    case 'number': {
      if (typeof got !== 'number') return fail(`expected JS number (INT32), got ${typeof got} (${String(got)})`);
      if (!Number.isInteger(got) || got !== expected) fail(`INT32 value ${got} ≠ expected ${String(expected)}`);
      return;
    }
    case 'bigint-string': {
      // BIGINT → an EXACT decimal string. The driver must NOT have rounded it.
      if (typeof got !== 'string') return fail(`expected JS string (BIGINT→string), got ${typeof got} (${String(got)}) — driver returned a non-string; precision would be lost`);
      if (got !== String(expected)) fail(`BIGINT string '${got}' ≠ expected '${String(expected)}'`);
      return;
    }
    case 'float': {
      if (typeof got !== 'number') return fail(`expected JS number (float), got ${typeof got}`);
      if (Math.abs(got - (expected as number)) >= EPS) fail(`float ${got} ≠ expected ${String(expected)}`);
      return;
    }
    case 'string': {
      // decimal / text: exact string. (For decimal, precision must survive.)
      if (typeof got !== 'string') return fail(`expected JS string, got ${typeof got} (${String(got)})`);
      if (got !== expected) fail(`string '${got}' ≠ expected '${String(expected)}'`);
      return;
    }
    case 'bool': {
      if (typeof got !== 'boolean') return fail(`expected JS boolean, got ${typeof got} (${String(got)})`);
      if (got !== expected) fail(`bool ${got} ≠ expected ${String(expected)}`);
      return;
    }
    case 'json': {
      // JSON text (string) OR a driver-parsed object; compare structurally.
      const canon = canonicalJson(parseJsonMaybe(got));
      const want = canonicalJson(expected);
      if (canon !== want) fail(`json ${canon} ≠ expected ${want}`);
      return;
    }
  }
}

// date columns are class 'string' above; assert the calendar date survived (no TZ shift). We
// compare the leading YYYY-MM-DD so a driver returning `2026-07-14` or `2026-07-14 00:00:00` both
// pass, but a TZ-shifted `2026-07-13...` fails. Applied for the date columns specifically.
const DATE_COLS = new Set(['date_val', 'daten_val']);
function checkDate(dialect: string, rowId: number, col: string, got: unknown, expected: unknown): void {
  const fail = (detail: string): void => { failures.push({ where: `${dialect}.${col}.row${rowId}`, detail }); };
  if (expected === null) { if (got !== null && got !== undefined) fail(`expected NULL date, got ${String(got)}`); return; }
  if (typeof got !== 'string') return fail(`expected date as JS string, got ${typeof got} (${got instanceof Date ? got.toISOString() : String(got)}) — a Date means the driver wasn't de-boxed to string`);
  if (got.slice(0, 10) !== expected) fail(`date '${got}' (day ${got.slice(0, 10)}) ≠ expected '${String(expected)}'`);
}

function parseJsonMaybe(v: unknown): unknown {
  if (typeof v === 'string') { try { return JSON.parse(v); } catch { return v; } }
  return v;
}
function canonicalJson(v: unknown): string {
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map(canonicalJson).join(',')}]`;
  const keys = Object.keys(v as Record<string, unknown>).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${canonicalJson((v as Record<string, unknown>)[k])}`).join(',')}}`;
}

// ── Step 1: outType derivation (dialect-independent; the native struct field type per column) ──
function assertTypeDerivation(): void {
  console.log('=== #59 step 1: outType derivation (GENERATED/native struct field type per column) ===');
  const resolver = lm.schemaColumnTypeResolver(SCHEMA);
  const bundle = lm.compileBundle(readsContract, COVERAGE_ENTRY, [], 'sqlite', undefined, resolver);
  const ir: unknown = (bundle as { readGraph?: { ir?: unknown } }).readGraph?.ir;
  const rowObj = findRowObj(ir);
  if (rowObj === undefined) { failures.push({ where: 'derivation', detail: 'no coverage row obj outType in IR' }); console.log('  FAIL: no row obj outType found'); return; }
  for (const col of COVERAGE_COLUMNS) {
    const want = COVERAGE_EXPECTED_SCALAR[col];
    const got = rowObj[col];
    if (got !== want) failures.push({ where: `derivation.${col}`, detail: `outType scalar ${JSON.stringify(got)} ≠ expected '${want}'` });
    const mat = COVERAGE_EXPECTED_MATERIALIZE[col];
    console.log(`  ${col.padEnd(11)} → bc ${String(got).padEnd(7)} [${got === want ? 'OK' : 'WRONG'}]  → TS ${mat}`);
  }
}

function findRowObj(ir: unknown): Record<string, string> | undefined {
  let found: Record<string, string> | undefined;
  const visit = (n: unknown): void => {
    if (n === null || typeof n !== 'object') return;
    const o = n as Record<string, unknown>;
    if ('outType' in o) { const r = unwrapRowObj(o.outType); if (r !== undefined) found = r; }
    for (const v of Object.values(o)) visit(v);
  };
  visit(ir);
  return found;
}
function unwrapRowObj(t: unknown): Record<string, string> | undefined {
  let cur = t;
  while (cur !== null && typeof cur === 'object' && !('obj' in (cur as object))) {
    const o = cur as Record<string, unknown>;
    if ('arr' in o) cur = o.arr; else if ('opt' in o) cur = o.opt; else return undefined;
  }
  if (cur !== null && typeof cur === 'object' && 'obj' in (cur as object)) {
    const obj = (cur as { obj: Record<string, unknown> }).obj;
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(obj)) if (typeof v === 'string') out[k] = v;
    return out;
  }
  return undefined;
}

// ── Step 2: DYNAMIC value + type round-trip per dialect ───────────────────────
function verifyDialect(dialect: string, rows: Row[]): void {
  console.log(`\n=== #59 step 2 [${dialect}]: DYNAMIC (TS read path) type+value round-trip vs EXPECTED ===`);
  if (rows.length !== COVERAGE_EXPECTED.length) {
    failures.push({ where: `${dialect}.rowcount`, detail: `read ${rows.length} rows, expected ${COVERAGE_EXPECTED.length}` });
    console.log(`  FAIL: row count ${rows.length} ≠ ${COVERAGE_EXPECTED.length}`); return;
  }
  const byId = new Map<number, Row>();
  for (const r of rows) byId.set(Number((r as { id: unknown }).id), r);
  for (const exp of COVERAGE_EXPECTED) {
    const row = byId.get(exp.id);
    if (row === undefined) { failures.push({ where: `${dialect}.row${exp.id}`, detail: 'missing' }); continue; }
    for (const col of COVERAGE_COLUMNS) {
      const got = (row as Record<string, unknown>)[col];
      const expected = (exp as unknown as Record<string, unknown>)[col];
      if (DATE_COLS.has(col)) checkDate(dialect, exp.id, col, got, expected);
      else checkColumn(dialect, exp.id, col, got, expected);
    }
  }
  const dialectFails = failures.filter((f) => f.where.startsWith(dialect + '.'));
  if (dialectFails.length === 0) {
    console.log(`  [${dialect}] ${COVERAGE_EXPECTED.length} rows × ${COVERAGE_COLUMNS.length} cols — ALL materialize to the right JS type AND value (int32→number, int64→string, date→string, bool→boolean, …)`);
  } else {
    console.log(`  [${dialect}] ${dialectFails.length} FAILURE(S):`);
    for (const f of dialectFails) console.log(`    • ${f.where}: ${f.detail}`);
  }
}

// ── Live-DB read of the coverage `find` via the SHIPPED SCP path (with de-box drivers) ────────
function readSqlite(): Row[] {
  const db = new Database(':memory:');
  for (const s of SCHEMA) db.exec(s);
  seedCoverage(db);
  // The read path itself enables safeIntegers per-statement for BIGINT columns (see
  // executeReadGraph); no global driver flag needed here.
  const bundle = lm.compileBundle(readsContract, COVERAGE_ENTRY, [], 'sqlite', undefined, lm.schemaColumnTypeResolver(SCHEMA));
  const out = lm.executeBundle(bundle, COVERAGE_INPUT, { db });
  db.close();
  return Array.isArray(out) ? (out as Row[]) : [];
}

async function readPg(): Promise<Row[]> {
  const boot = new Pool({ ...PG_BOOT_CONN, max: 1 });
  await boot.query(`CREATE SCHEMA IF NOT EXISTS ${PG_SCHEMA_NAME}`);
  await boot.end();
  const pool = new Pool({ ...PG_CONN, max: 4 });
  for (const s of PG_SCHEMA) await pool.query(s);
  for (const s of seedCoverageStatements('postgres')) await pool.query(s);
  const exec = lm.pgPoolExecutor(pool);
  const bundle = lm.compileBundle(readsContract, COVERAGE_ENTRY, [], 'postgres', undefined, lm.schemaColumnTypeResolver(PG_SCHEMA));
  const out = await lm.executeBundleAsync(bundle, COVERAGE_INPUT, { exec, dialect: 'postgres' });
  await pool.end();
  return Array.isArray(out) ? (out as Row[]) : [];
}

async function readMysql(): Promise<Row[]> {
  const boot = mysql.createPool({ ...MYSQL_BOOT_CONN, connectionLimit: 1 });
  await boot.query(`CREATE DATABASE IF NOT EXISTS ${MYSQL_DB_NAME}`);
  await boot.end();
  // The mysql2 pool MUST carry the de-box options (BIGINT→string, date→string) so the read-path
  // materializer gets coercible values.
  const pool = mysql.createPool({ ...MYSQL_CONN, ...lm.mysqlDeboxPoolOptions, connectionLimit: 4, multipleStatements: false });
  for (const s of MYSQL_SCHEMA) await pool.query(s);
  for (const s of seedCoverageStatements('mysql')) await pool.query(s);
  const exec = lm.mysqlPoolExecutor(pool as never);
  const bundle = lm.compileBundle(readsContract, COVERAGE_ENTRY, [], 'mysql', undefined, lm.schemaColumnTypeResolver(MYSQL_SCHEMA));
  const out = await lm.executeBundleAsync(bundle, COVERAGE_INPUT, { exec, dialect: 'mysql' });
  await pool.end();
  return Array.isArray(out) ? (out as Row[]) : [];
}

async function main(): Promise<void> {
  console.log('════════════════════════════════════════════════════════════════');
  console.log(' litedbmodel #59 — ALL-TYPE coverage typed de-box round-trip audit');
  console.log('════════════════════════════════════════════════════════════════\n');

  assertTypeDerivation();
  verifyDialect('sqlite', readSqlite());

  // Live PG + MySQL (docker). Unreachable ⇒ HARD failure (the round-trip MUST run on real
  // PG + MySQL) — never silently skipped.
  try { verifyDialect('postgres', await readPg()); }
  catch (e) { failures.push({ where: 'postgres.connect', detail: `live PG read failed: ${(e as Error).message}` }); console.log(`\n[postgres] CONNECT/READ FAILED: ${(e as Error).message}`); }
  try { verifyDialect('mysql', await readMysql()); }
  catch (e) { failures.push({ where: 'mysql.connect', detail: `live MySQL read failed: ${(e as Error).message}` }); console.log(`\n[mysql] CONNECT/READ FAILED: ${(e as Error).message}`); }

  console.log('\n════════════════════════════════════════════════════════════════');
  if (failures.length > 0) {
    console.error(`\n❌ ${failures.length} FAILURE(S):`);
    for (const f of failures) console.error(`  • ${f.where}: ${f.detail}`);
    process.exit(1);
  }
  console.log('\n✅ #59 coverage audit PASSED: outType derivation correct for all columns; the TS read-path de-box materializes every column to the right JS type AND exact value across sqlite/postgres/mysql — INT32→number, BIGINT→string (exact, JSON-safe), decimal→string, date→TZ-string, bool→boolean, json→structural. The previous i64-rounding and DATE-TZ-shift holes are GONE.');
}

main().catch((e) => { console.error(e); process.exit(1); });
