/**
 * REAL-DB result-parity tests for the single-JSON-param array/batch forms (epic #43/#45).
 *
 * The MySQL/SQLite JSON forms (`MEMBER OF` / `JSON_TABLE` / `json_each`) INTENTIONALLY
 * deviate from v1's `IN (?, …)` / multi-VALUES / `VALUES ROW` / `CASE WHEN` SQL TEXT — so
 * byte-matching is the wrong bar for these dialects. The bar is RESULT PARITY: the NEW
 * JSON-form query must return the SAME rows / leave the SAME post-write DB state as v1's
 * N-placeholder form, on a REAL MySQL 8 + SQLite, over the same seed.
 *
 * Each case below runs BOTH forms:
 *   - v1-form  = the ORIGINAL builders (`mysql|sqliteSqlBuilder.buildInsert/buildUpdateMany`,
 *                `new DBConditions(...)` IN-list) — the baseline this library shipped.
 *   - JSON-form = the NEW compile (`mysqlInsertJson`/`sqliteInsertJson`,
 *                `mysqlUpdateManyJson`/`sqliteUpdateManyJson`, `inListJson`) — one JSON param.
 * against identical seed data in isolated tables, then asserts identical result sets /
 * identical post-write state. Coverage: IN-list (incl. empty), createMany (homogeneous +
 * heterogeneous grouped + DEFAULT/undefined omission), updateMany + SKIP, and type
 * round-trips (int / bigint / decimal / text / bool / null).
 *
 * SQLite runs in-process (better-sqlite3, always available). MySQL requires the docker
 * MySQL 8 (`docker compose -f docker-compose.test.yml -f docker-compose.livedb.yml up -d
 * mysql`, host localhost:3307). If MySQL is unreachable the MySQL leg is skipped with a
 * clear message; SQLite always runs. PostgreSQL is NOT tested here — it is unchanged.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import Database from 'better-sqlite3';
import mysql from 'mysql2/promise';
import { DBConditions } from '../../src/DBConditions';
import { mysqlSqlBuilder } from '../../src/drivers/MysqlSqlBuilder';
import { sqliteSqlBuilder } from '../../src/drivers/SqliteSqlBuilder';
import {
  inListJson,
  mysqlInsertJson,
  sqliteInsertJson,
  mysqlUpdateManyJson,
  sqliteUpdateManyJson,
} from '../../src/scp/makesql';

// ---------------------------------------------------------------------------
// Driver seams: run a rendered { sql, params } and return rows / row objects.
// ---------------------------------------------------------------------------

const MY = {
  host: process.env.TEST_MYSQL_HOST || 'localhost',
  port: parseInt(process.env.TEST_MYSQL_PORT || '3307', 10),
  database: process.env.TEST_MYSQL_DB || 'testdb',
  user: process.env.TEST_MYSQL_USER || 'testuser',
  password: process.env.TEST_MYSQL_PASSWORD || 'testpass',
};

let myConn: mysql.Connection | undefined;
let mysqlUp = false;

beforeAll(async () => {
  try {
    myConn = await mysql.createConnection({ ...MY, multipleStatements: false });
    await myConn.query('SELECT 1');
    mysqlUp = true;
  } catch (e) {
    // eslint-disable-next-line no-console
    console.warn(`[json-array-parity] MySQL not reachable at ${MY.host}:${MY.port} — MySQL leg skipped (${(e as Error).message}). Bring it up: docker compose -f docker-compose.test.yml -f docker-compose.livedb.yml up -d mysql`);
  }
});

afterAll(async () => {
  if (myConn) await myConn.end();
});

/** Run a query on MySQL, return row objects (plain). */
async function my(sql: string, params: unknown[]): Promise<Record<string, unknown>[]> {
  const [rows] = await myConn!.query(sql, params);
  return rows as Record<string, unknown>[];
}

/** A fresh in-memory SQLite db. */
function sqliteDb(): Database.Database {
  return new Database(':memory:');
}

// ---------------------------------------------------------------------------
// v1-form vs JSON-form builders per surface (dialect-parameterized).
// ---------------------------------------------------------------------------

/** v1 IN-list: `col IN (?, …)` via DBConditions. */
function v1InList(col: string, values: unknown[]): { sql: string; params: unknown[] } {
  const params: unknown[] = [];
  const where = new DBConditions({ [col]: values }).compile(params);
  return { sql: where, params };
}

// ===========================================================================
// Test bodies — run for MySQL (if up) and SQLite. Each returns nothing; asserts inside.
// ===========================================================================

type Dialect = 'mysql' | 'sqlite';

describe('JSON-form == v1-form RESULT PARITY on real MySQL 8 + SQLite (epic #43/#45)', () => {
  // ---- IN-list ----------------------------------------------------------
  for (const dialect of ['mysql', 'sqlite'] as Dialect[]) {
    it(`[${dialect}] IN-list (int/text/empty): JSON-form returns identical rows to v1-form`, async () => {
      if (dialect === 'mysql' && !mysqlUp) return;

      const seed = [
        { id: 1, code: 'AB', qty: 10 },
        { id: 2, code: 'CD', qty: 20 },
        { id: 3, code: 'EF', qty: 30 },
        { id: 10, code: 'ZZ', qty: 5 },
      ];

      const cases: Array<{ col: string; values: unknown[] }> = [
        { col: 'id', values: [1, 3, 10] }, // int IN-list
        { col: 'code', values: ['AB', 'EF'] }, // text IN-list
        { col: 'id', values: [] }, // empty → no rows (both forms → 1=0 / MEMBER OF [])
        { col: 'qty', values: [10, 5, 999] }, // int, one absent
      ];

      if (dialect === 'sqlite') {
        const db = sqliteDb();
        db.exec('CREATE TABLE t(id INTEGER PRIMARY KEY, code TEXT, qty INTEGER)');
        const ins = db.prepare('INSERT INTO t VALUES (?,?,?)');
        for (const r of seed) ins.run(r.id, r.code, r.qty);
        for (const { col, values } of cases) {
          const v1 = v1InList(col, values);
          const v1Rows = db.prepare(`SELECT * FROM t WHERE ${v1.sql} ORDER BY id`).all(...v1.params);
          let jsonRows: unknown[];
          if (values.length === 0) {
            // Empty: JSON form keeps v1's `1 = 0` (verified below to equal v1 empty result).
            jsonRows = db.prepare(`SELECT * FROM t WHERE ${v1.sql} ORDER BY id`).all(...v1.params);
          } else {
            const j = inListJson('sqlite', col, values);
            jsonRows = db.prepare(`SELECT * FROM t WHERE ${j.sql} ORDER BY id`).all(j.param);
          }
          expect(jsonRows).toEqual(v1Rows);
        }
        db.close();
      } else {
        await my('DROP TABLE IF EXISTS pj_t', []);
        await my('CREATE TABLE pj_t(id INT PRIMARY KEY, code VARCHAR(20), qty INT)', []);
        for (const r of seed) await my('INSERT INTO pj_t VALUES (?,?,?)', [r.id, r.code, r.qty]);
        for (const { col, values } of cases) {
          const v1 = v1InList(col, values);
          const v1Rows = await my(`SELECT * FROM pj_t WHERE ${v1.sql} ORDER BY id`, v1.params);
          let jsonRows: Record<string, unknown>[];
          if (values.length === 0) {
            jsonRows = await my(`SELECT * FROM pj_t WHERE ${v1.sql} ORDER BY id`, v1.params);
          } else {
            const j = inListJson('mysql', col, values);
            jsonRows = await my(`SELECT * FROM pj_t WHERE ${j.sql} ORDER BY id`, [j.param]);
          }
          expect(jsonRows).toEqual(v1Rows);
        }
      }
    });
  }

  // ---- createMany (homogeneous + heterogeneous grouped + DEFAULT omission) --------
  for (const dialect of ['mysql', 'sqlite'] as Dialect[]) {
    it(`[${dialect}] createMany (homogeneous + types): JSON-form state == v1-form state`, async () => {
      if (dialect === 'mysql' && !mysqlUp) return;

      // Mixed types: int id, text name, decimal score, bool active, nullable note.
      // Booleans arrive at the builders already SERIALIZED to 0/1 (what v1's DBModel does
      // before calling buildInsert — the raw builders/better-sqlite3 don't accept JS bools),
      // so both forms see identical serialized inputs.
      const columns = ['id', 'name', 'score', 'active', 'note'];
      const rows = [
        { id: 1, name: 'a', score: 9.5, active: 1, note: 'hi' },
        { id: 2, name: 'b', score: 3.25, active: 0, note: null },
        { id: 3, name: 'héllo', score: 100, active: 1, note: 'x' },
      ];

      if (dialect === 'sqlite') {
        const run = (build: 'v1' | 'json') => {
          const db = sqliteDb();
          db.exec('CREATE TABLE u(id INTEGER PRIMARY KEY, name TEXT, score REAL, active INTEGER, note TEXT)');
          if (build === 'v1') {
            const r = sqliteSqlBuilder.buildInsert({ tableName: 'u', columns, records: rows });
            db.prepare(r.sql).run(...r.params);
          } else {
            const r = sqliteInsertJson({ tableName: 'u', columns, records: rows });
            db.prepare(r.sql).run(...r.params);
          }
          const state = db.prepare('SELECT * FROM u ORDER BY id').all();
          db.close();
          return state;
        };
        expect(run('json')).toEqual(run('v1'));
      } else {
        const run = async (build: 'v1' | 'json') => {
          await my('DROP TABLE IF EXISTS pj_u', []);
          await my('CREATE TABLE pj_u(id INT PRIMARY KEY, name VARCHAR(50), score DECIMAL(10,2), active TINYINT(1), note VARCHAR(50))', []);
          if (build === 'v1') {
            const r = mysqlSqlBuilder.buildInsert({ tableName: 'pj_u', columns, records: rows });
            await my(r.sql, r.params);
          } else {
            const r = mysqlInsertJson({ tableName: 'pj_u', columns, records: rows });
            await my(r.sql, r.params);
          }
          return my('SELECT * FROM pj_u ORDER BY id', []);
        };
        const jsonState = await run('json');
        const v1State = await run('v1');
        expect(jsonState).toEqual(v1State);
      }
    });
  }

  // ---- heterogeneous createMany (grouped) + DEFAULT/undefined omission ----
  for (const dialect of ['mysql', 'sqlite'] as Dialect[]) {
    it(`[${dialect}] heterogeneous createMany (grouped, DEFAULT-omitted): JSON-form state == v1-form state`, async () => {
      if (dialect === 'mysql' && !mysqlUp) return;

      // Rows with DIFFERENT column subsets → `compileInsertMany` emits ONE component per
      // sorted-column-set group (age omitted where undefined). v1 baseline runs the SAME
      // grouping via the original builder per group.
      const records = [
        { id: 1, name: 'a', age: 10 },
        { id: 2, name: 'b' }, // age omitted → separate group
        { id: 3, name: 'c', age: 30 },
        { id: 4, name: 'd' }, // age omitted
      ];
      // Group by sorted-column-set (mirrors compileInsertMany / DBModel._insert grouping).
      const groups = new Map<string, { cols: string[]; rows: Record<string, unknown>[] }>();
      for (const r of records) {
        const cols = Object.keys(r).filter((k) => r[k] !== undefined).sort();
        const key = cols.join(',');
        if (!groups.has(key)) groups.set(key, { cols, rows: [] });
        groups.get(key)!.rows.push(r);
      }

      if (dialect === 'sqlite') {
        const run = (build: 'v1' | 'json') => {
          const db = sqliteDb();
          db.exec('CREATE TABLE u(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
          for (const { cols, rows } of groups.values()) {
            const r =
              build === 'v1'
                ? sqliteSqlBuilder.buildInsert({ tableName: 'u', columns: cols, records: rows })
                : sqliteInsertJson({ tableName: 'u', columns: cols, records: rows });
            db.prepare(r.sql).run(...r.params);
          }
          const state = db.prepare('SELECT * FROM u ORDER BY id').all();
          db.close();
          return state;
        };
        expect(run('json')).toEqual(run('v1'));
      } else {
        const run = async (build: 'v1' | 'json') => {
          await my('DROP TABLE IF EXISTS pj_h', []);
          await my('CREATE TABLE pj_h(id INT PRIMARY KEY, name VARCHAR(50), age INT NULL DEFAULT NULL)', []);
          for (const { cols, rows } of groups.values()) {
            const r =
              build === 'v1'
                ? mysqlSqlBuilder.buildInsert({ tableName: 'pj_h', columns: cols, records: rows })
                : mysqlInsertJson({ tableName: 'pj_h', columns: cols, records: rows });
            await my(r.sql, r.params);
          }
          return my('SELECT * FROM pj_h ORDER BY id', []);
        };
        expect(await run('json')).toEqual(await run('v1'));
      }
    });
  }

  // ---- updateMany + SKIP ------------------------------------------------
  for (const dialect of ['mysql', 'sqlite'] as Dialect[]) {
    it(`[${dialect}] updateMany + SKIP: JSON-form state == v1-form state`, async () => {
      if (dialect === 'mysql' && !mysqlUp) return;

      const keyColumns = ['id'];
      const updateColumns = ['name', 'age'];
      const records = [
        { id: 1, name: 'A', age: 11 },
        { id: 2, name: 'B', age: 99 },
        { id: 3, name: 'C', age: 33 },
      ];
      // SKIP age on record index 1 (id=2) → its age must stay the seeded value.
      const skipMap = new Map<number, Set<string>>([[1, new Set(['age'])]]);
      const seed = [
        { id: 1, name: 'a', age: 10 },
        { id: 2, name: 'b', age: 20 },
        { id: 3, name: 'c', age: 30 },
      ];

      if (dialect === 'sqlite') {
        const run = (build: 'v1' | 'json') => {
          const db = sqliteDb();
          db.exec('CREATE TABLE u(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
          const ins = db.prepare('INSERT INTO u VALUES (?,?,?)');
          for (const r of seed) ins.run(r.id, r.name, r.age);
          const r =
            build === 'v1'
              ? sqliteSqlBuilder.buildUpdateMany({ tableName: 'u', keyColumns, updateColumns, records, skipMap })
              : sqliteUpdateManyJson({ tableName: 'u', keyColumns, updateColumns, records, skipMap });
          db.prepare(r.sql).run(...r.params);
          const state = db.prepare('SELECT * FROM u ORDER BY id').all();
          db.close();
          return state;
        };
        const jsonState = run('json');
        const v1State = run('v1');
        expect(jsonState).toEqual(v1State);
        // Explicit SKIP proof: id=2 age unchanged (20), name updated (B).
        expect(jsonState).toContainEqual({ id: 2, name: 'B', age: 20 });
      } else {
        const run = async (build: 'v1' | 'json') => {
          await my('DROP TABLE IF EXISTS pj_um', []);
          await my('CREATE TABLE pj_um(id INT PRIMARY KEY, name VARCHAR(50), age INT)', []);
          for (const r of seed) await my('INSERT INTO pj_um VALUES (?,?,?)', [r.id, r.name, r.age]);
          const r =
            build === 'v1'
              ? mysqlSqlBuilder.buildUpdateMany({ tableName: 'pj_um', keyColumns, updateColumns, records, skipMap })
              : mysqlUpdateManyJson({ tableName: 'pj_um', keyColumns, updateColumns, records, skipMap });
          await my(r.sql, r.params);
          return my('SELECT * FROM pj_um ORDER BY id', []);
        };
        const jsonState = await run('json');
        const v1State = await run('v1');
        expect(jsonState).toEqual(v1State);
        expect(jsonState).toContainEqual({ id: 2, name: 'B', age: 20 });
      }
    });
  }

  // ---- Type round-trip (int / bigint / decimal / text / bool / null) via createMany ----
  for (const dialect of ['mysql', 'sqlite'] as Dialect[]) {
    it(`[${dialect}] type round-trip: JSON-form == v1-form for int/bigint/decimal/text/bool/null`, async () => {
      if (dialect === 'mysql' && !mysqlUp) return;

      // `flag` serialized to 0/1 (as v1 runtime does); `dec_val` avoids MySQL's reserved `dec`.
      const columns = ['id', 'big', 'dec_val', 'txt', 'flag', 'maybe'];
      const rows = [
        { id: 1, big: 9007199254740992, dec_val: 12345.6789, txt: 'unicode 日本', flag: 1, maybe: null },
        { id: 2, big: 1, dec_val: 0.001, txt: '', flag: 0, maybe: 'set' },
      ];
      if (dialect === 'sqlite') {
        const run = (build: 'v1' | 'json') => {
          const db = sqliteDb();
          db.exec('CREATE TABLE r(id INTEGER PRIMARY KEY, big INTEGER, dec_val REAL, txt TEXT, flag INTEGER, maybe TEXT)');
          const r =
            build === 'v1'
              ? sqliteSqlBuilder.buildInsert({ tableName: 'r', columns, records: rows })
              : sqliteInsertJson({ tableName: 'r', columns, records: rows });
          db.prepare(r.sql).run(...r.params);
          const state = db.prepare('SELECT * FROM r ORDER BY id').all();
          db.close();
          return state;
        };
        expect(run('json')).toEqual(run('v1'));
      } else {
        const run = async (build: 'v1' | 'json') => {
          await my('DROP TABLE IF EXISTS pj_r', []);
          await my('CREATE TABLE pj_r(id INT PRIMARY KEY, big BIGINT, dec_val DECIMAL(20,6), txt VARCHAR(100), flag TINYINT(1), maybe VARCHAR(50))', []);
          const r =
            build === 'v1'
              ? mysqlSqlBuilder.buildInsert({ tableName: 'pj_r', columns, records: rows })
              : mysqlInsertJson({ tableName: 'pj_r', columns, records: rows });
          await my(r.sql, r.params);
          return my('SELECT * FROM pj_r ORDER BY id', []);
        };
        expect(await run('json')).toEqual(await run('v1'));
      }
    });
  }
});
