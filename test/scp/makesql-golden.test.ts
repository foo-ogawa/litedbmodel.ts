/**
 * Golden byte-parity tests for the LOCKED `makeSQL` model (epic #43 / design #45).
 *
 * The GOLDEN is the ORIGINAL tuned builders' ACTUAL output. For each surface we DRIVE
 * the original (`DBConditions` / `DBValues`, `postgres|mysql|sqliteSqlBuilder`,
 * `LazyRelationContext` / `_update` / `_delete` text) to emit the expected
 * `{ sql, params }`, freeze THAT, and assert the `makeSQL` compile + `assembleMakeSQL`
 * + `renderPlaceholders` reproduces it byte-for-byte. Nothing is compared v2-to-v2.
 *
 * Covers checklist A (WHERE), B (CRUD/tail), C (relations) across PG/MySQL/SQLite.
 */

/* eslint-disable @typescript-eslint/no-explicit-any -- capturing-model harness needs casts */
import { describe, it, expect } from 'vitest';
import { DBModel } from '../../src/DBModel';
import { LazyRelationContext } from '../../src/LazyRelation';
import { DBConditions } from '../../src/DBConditions';
import { dbNotNull, dbCast, dbCastIn, dbTupleIn, dbImmediate, dbDynamic, dbRaw, DBExists, DBSubquery, DBImmediateValue, parentRef } from '../../src/DBValues';
import { postgresSqlBuilder } from '../../src/drivers/PostgresSqlBuilder';
import { mysqlSqlBuilder } from '../../src/drivers/MysqlSqlBuilder';
import { sqliteSqlBuilder } from '../../src/drivers/SqliteSqlBuilder';
import type { SqlBuilder } from '../../src/drivers/types';
import {
  assembleMakeSQL,
  renderPlaceholders,
  compileWhere,
  compileSelect,
  compileInsertMany,
  compileUpdateMany,
  compileUpdateSingle,
  compileDelete,
  compileSingleKeyUnlimited,
  compileSingleKeyLimited,
  compileCompositeKeyUnlimited,
  compileCompositeKeyLimited,
  type Dialect,
  type MakeSQL,
} from '../../src/scp/makesql';

type Rendered = { sql: string; params: unknown[] };

/** Assemble a compiled makeSQL bundle and render the dialect placeholder form. */
function render(node: MakeSQL, dialect: Dialect): Rendered {
  const asm = assembleMakeSQL(node);
  return { sql: renderPlaceholders(asm.sql, dialect), params: asm.params };
}

const pgFmt = (ph: string, t: string) => `${ph}::${t}`;
const dialects: Dialect[] = ['postgres', 'mysql', 'sqlite'];
const builderOf: Record<Dialect, SqlBuilder> = {
  postgres: postgresSqlBuilder,
  mysql: mysqlSqlBuilder,
  sqlite: sqliteSqlBuilder,
};

// ===========================================================================
// A. WHERE / conditions / values — golden = DBConditions.compile output.
// ===========================================================================
describe('A. WHERE — makeSQL byte-matches DBConditions (all constructs)', () => {
  const constructs: Array<[string, any]> = [
    ['equality', { status: 'active', author_id: 7 }],
    ['!= custom-op', { 'age <> ?': 5 }],
    ['< <= > >= custom-op', { 'age >= ?': 18 }],
    ['IN list', { id: [1, 2, 3] }],
    ['empty IN → 1 = 0', { id: [] }],
    ['IS NULL', { deleted_at: null }],
    ['IS NOT NULL', { email: dbNotNull() }],
    ['boolean literal = TRUE', { is_active: true }],
    ['boolean literal = FALSE', { is_active: false }],
    ['LIKE (raw)', { __raw__: ['name LIKE ?', ['%x%']] }],
    ['ILIKE (raw)', { __raw__: ['name ILIKE ?', ['%x%']] }],
    ['BETWEEN (custom-op)', { 'age BETWEEN ? AND ?': [18, 65] }],
    ['NOT IN (raw)', { __raw__: ['id NOT IN (?, ?)', [1, 2]] }],
    ['cast ::uuid', { id: dbCast('123e4567', 'uuid') }],
    ['cast array IN(::uuid)', { id: dbCastIn(['u1', 'u2'], 'uuid') }],
    ['cast array empty → 1 = 0', { id: dbCastIn([], 'uuid') }],
    ['dynamic col = fn(?)', { search: dbDynamic("to_tsvector('en', ?)", ['q']) }],
    ['immediate col = NOW()', { created_at: dbImmediate('NOW()') }],
    ['raw expr value', { updated_at: dbRaw('NOW()') }],
    ['tuple/composite IN', { __tuple__: dbTupleIn(['tenant_id', 'id'], [[1, 10], [2, 20]]) }],
    ['AND grouping (nested)', { a: 1, __nested__: new DBConditions({ b: 2, c: 3 }) }],
    ['OR / parens', { __or__: [{ a: 1 }, { b: 2 }] }],
    ['empty AND → drop', {}],
  ];

  // The single-column plain-array IN-list is the ONE construct that INTENTIONALLY
  // deviates from v1 on MySQL/SQLite (epic #43/#45): v1's `col IN (?, ?, ?)` becomes the
  // single-JSON-param server-side-expansion form. PG is unchanged. Every OTHER construct
  // (incl. empty-IN → `1 = 0`, cast-array-IN, tuple-IN, custom-op arrays) stays v1
  // byte-match on all three dialects. The NEW form is a frozen literal target here; its
  // RESULT PARITY with v1 is proven on real MySQL 8 + SQLite in json-array-parity.test.ts.
  const IN_LIST_JSON: Record<'mysql' | 'sqlite', Rendered> = {
    mysql: { sql: "id IN (SELECT JSON_UNQUOTE(v) FROM JSON_TABLE(?, '$[*]' COLUMNS(v JSON PATH '$')) jt)", params: ['[1,2,3]'] },
    sqlite: { sql: 'id IN (SELECT value FROM json_each(?))', params: ['[1,2,3]'] },
  };

  for (const dialect of dialects) {
    for (const [name, cond] of constructs) {
      it(`[${dialect}] ${name}`, () => {
        const got = render(compileWhere(cond, dialect), dialect);
        if (name === 'IN list' && dialect !== 'postgres') {
          // NEW JSON form (deviation OVER v1) — frozen literal target.
          const golden = IN_LIST_JSON[dialect];
          expect(got.sql).toBe(golden.sql);
          expect(got.params).toEqual(golden.params);
          return;
        }
        // All other constructs (and all of PG): byte-match to v1 DBConditions output.
        const params: unknown[] = [];
        const formatter = dialect === 'postgres' ? pgFmt : (ph: string) => ph;
        const goldenSql = new DBConditions(cond).compile(params, formatter);
        const golden: Rendered = { sql: renderPlaceholders(goldenSql, dialect), params };
        expect(got.sql).toBe(golden.sql);
        expect(got.params).toEqual(golden.params);
      });
    }
  }
});

// ===========================================================================
// A/B. IN(subquery) / NOT IN / EXISTS / NOT EXISTS / correlated — via DBModel helpers.
// ===========================================================================
describe('A. subquery / EXISTS — makeSQL byte-matches DBModel.inSubquery/exists', () => {
  class SubBase extends DBModel {
    static getDriverType(): Dialect {
      return 'postgres';
    }
  }
  class Usr extends SubBase {
    protected static TABLE_NAME = 'users';
    id?: number;
    tenant_id?: number;
  }
  class Ord extends SubBase {
    protected static TABLE_NAME = 'orders';
    user_id?: number;
    status?: string;
  }

  void Ord;

  it('IN(subquery) single key', () => {
    const cond = (Usr as any).inSubquery(
      [[{ columnName: 'id', tableName: 'users' }, { columnName: 'user_id', tableName: 'orders' }]],
      [[{ columnName: 'status', tableName: 'orders' }, 'paid']]
    );
    const condObj = { [cond[0]]: cond[1] };
    const params: unknown[] = [];
    const goldenSql = new DBConditions(condObj).compile(params, pgFmt);
    const golden = { sql: renderPlaceholders(goldenSql, 'postgres'), params };
    const got = render(compileWhere(condObj, 'postgres'), 'postgres');
    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
    expect(golden.sql).toContain('users.id IN (SELECT orders.user_id FROM orders WHERE orders.status =');
  });

  it('NOT IN(subquery) + composite (a,b) IN(subquery)', () => {
    // NOT IN subquery.
    const notIn = new DBSubquery(
      [{ columnName: 'id', tableName: 'users' }],
      'banned',
      [{ columnName: 'user_id', tableName: 'banned' }],
      [],
      'NOT IN'
    );
    // Composite (tenant_id, id) IN (SELECT …).
    const comp = new DBSubquery(
      [
        { columnName: 'tenant_id', tableName: 'users' },
        { columnName: 'id', tableName: 'users' },
      ],
      'orders',
      [
        { columnName: 'tenant_id', tableName: 'orders' },
        { columnName: 'user_id', tableName: 'orders' },
      ],
      [{ column: { columnName: 'status', tableName: 'orders' }, value: 'paid' }],
      'IN'
    );
    for (const ex of [notIn, comp]) {
      const condObj = { __subquery__: ex };
      const params: unknown[] = [];
      const goldenSql = new DBConditions(condObj).compile(params, pgFmt);
      const golden = { sql: renderPlaceholders(goldenSql, 'postgres'), params };
      const got = render(compileWhere(condObj, 'postgres'), 'postgres');
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
    }
  });

  it('= ANY(?::type[]) scalar-array condition (PG) via raw', () => {
    const condObj = { __raw__: ['users.id = ANY(?::uuid[])', [['u1', 'u2']]] };
    const params: unknown[] = [];
    const goldenSql = new DBConditions(condObj).compile(params, pgFmt);
    const golden = { sql: renderPlaceholders(goldenSql, 'postgres'), params };
    const got = render(compileWhere(condObj, 'postgres'), 'postgres');
    expect(got.sql).toBe(golden.sql);
    expect(got.params).toEqual(golden.params);
    expect(golden.sql).toBe('users.id = ANY($1::uuid[])');
    expect(golden.params).toEqual([['u1', 'u2']]);
  });

  it('EXISTS / NOT EXISTS correlated (via DBExists + parentRef)', () => {
    for (const [not, kw] of [[false, 'EXISTS'], [true, 'NOT EXISTS']] as const) {
      const ex = new DBExists(
        'orders',
        [{ column: { columnName: 'user_id', tableName: 'orders' }, value: parentRef({ columnName: 'id', tableName: 'users' }) }],
        not
      );
      const condObj = { __exists__: ex };
      const params: unknown[] = [];
      const goldenSql = new DBConditions(condObj).compile(params, pgFmt);
      const golden = { sql: renderPlaceholders(goldenSql, 'postgres'), params };
      const got = render(compileWhere(condObj, 'postgres'), 'postgres');
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
      expect(golden.sql).toBe(`${kw} (SELECT 1 FROM orders WHERE orders.user_id = users.id)`);
    }
  });
});

// ===========================================================================
// B. CRUD — golden = original dialect builders.
// ===========================================================================
// ---------------------------------------------------------------------------
// De-tautologized INSERT golden: the golden is the REAL `DBModel._insert`
// production output, captured by a subclass that records each `execute(sql, params)`
// instead of running it (the same capture technique the relation goldens use, but on
// the WRITE path — `_insert` goes through `execute`, not `query`). v2's
// `compileInsertMany` composition is asserted to byte-match the captured statements —
// same statements, same GROUPING, same params/order — NOT re-derived from `buildInsert`.
// ---------------------------------------------------------------------------

/** A DBModel subclass that captures each INSERT statement `_insert` would execute. */
function makeInsertModel(driver: Dialect): {
  Model: typeof DBModel;
  captures: Rendered[];
} {
  const captures: Rendered[] = [];
  class Base extends DBModel {
    static getDriverType(): Dialect {
      return driver;
    }
    // Writes normally require a live transaction; the golden captures SQL only.
    protected static _checkWriteAllowed(): void {}
    // `_insert` calls `this.execute(sql, params)` — record instead of running.
    static execute(sqlOrFragment: any, params?: any): any {
      captures.push({ sql: sqlOrFragment as string, params: (params ?? []) as unknown[] });
      return Promise.resolve({ rows: [], rowCount: 0 });
    }
  }
  class Users extends Base {
    protected static TABLE_NAME = 'users';
    protected static SELECT_COLUMN = '*';
  }
  return { Model: Users as unknown as typeof DBModel, captures };
}

/** Drive the REAL `_insert`; return every captured statement, dialect-rendered. */
async function captureInsert(
  driver: Dialect,
  records: Record<string, unknown>[],
  options: Record<string, unknown> = {}
): Promise<Rendered[]> {
  const { Model, captures } = makeInsertModel(driver);
  captures.length = 0;
  await (Model as any)._insert(records, options);
  return captures.map((c) => ({ sql: renderPlaceholders(c.sql, driver), params: c.params }));
}

/** v2: compile the createMany into composed makeSQL components; render each. */
function renderComponents(components: MakeSQL[], dialect: Dialect): Rendered[] {
  return components.map((c) => render(c, dialect));
}

// ---------------------------------------------------------------------------
// NEW JSON-form golden (MySQL/SQLite) — the INTENTIONAL deviation from v1's
// N-placeholder multi-VALUES (epic #43/#45). This is an INDEPENDENT generator (it does
// NOT call the v2 compile) so the golden is a real target: a group's rows go as ONE JSON
// array-of-objects param, expanded server-side (MySQL JSON_TABLE / SQLite json_each).
// RESULT PARITY with v1's multi-VALUES is proven on real DBs in json-array-parity.test.ts.
// ---------------------------------------------------------------------------

/** Infer the MySQL JSON_TABLE COLUMNS type + SELECT expr for a column (golden mirror). */
function jtColType(col: string, rows: Record<string, unknown>[]): { t: string; sel: (r: string) => string } {
  let sample: unknown;
  for (const r of rows) {
    const v = r[col];
    if (v !== null && v !== undefined) { sample = v; break; }
  }
  if (typeof sample === 'boolean' || typeof sample === 'bigint') return { t: 'BIGINT', sel: (r) => r };
  if (typeof sample === 'number') return Number.isInteger(sample) ? { t: 'BIGINT', sel: (r) => r } : { t: 'DECIMAL(65,30)', sel: (r) => r };
  if (sample !== null && typeof sample === 'object') return { t: 'JSON', sel: (r) => r };
  return { t: 'JSON', sel: (r) => `JSON_UNQUOTE(${r})` };
}

/** Serialize a group's rows to the ONE JSON param string (undefined → null, cols only). */
function groupJson(rows: Record<string, unknown>[], cols: string[]): string {
  return JSON.stringify(rows.map((r) => {
    const o: Record<string, unknown> = {};
    for (const c of cols) o[c] = r[c] === undefined ? null : r[c];
    return o;
  }));
}

/** Expected NEW JSON-form INSERT statement for one homogeneous group. */
function goldenInsertJson(
  dialect: 'mysql' | 'sqlite',
  tableName: string,
  cols: string[],
  rows: Record<string, unknown>[],
  opts: { onConflict?: string[]; onConflictIgnore?: boolean; onConflictUpdate?: 'all' | string[]; returning?: string } = {}
): Rendered {
  const param = groupJson(rows, cols);
  let source: string;
  if (dialect === 'mysql') {
    const jt = cols.map((c) => `${c} ${jtColType(c, rows).t} PATH '$.${c}'`).join(', ');
    const sel = cols.map((c) => jtColType(c, rows).sel(`jt.${c}`)).join(', ');
    source = `SELECT ${sel} FROM JSON_TABLE(?, '$[*]' COLUMNS(${jt})) jt`;
  } else {
    source = `SELECT ${cols.map((c) => `json_extract(value, '$.${c}')`).join(', ')} FROM json_each(?)`;
  }
  let sql: string;
  const list = `${tableName} (${cols.join(', ')})`;
  if (opts.onConflict && opts.onConflictIgnore) {
    sql = dialect === 'mysql' ? `INSERT IGNORE INTO ${list} ${source}` : `INSERT OR IGNORE INTO ${list} ${source}`;
  } else if (opts.onConflict && opts.onConflictUpdate) {
    const upd = opts.onConflictUpdate === 'all' ? cols : opts.onConflictUpdate;
    sql = dialect === 'mysql'
      ? `INSERT INTO ${list} ${source} ON DUPLICATE KEY UPDATE ${upd.map((c) => `${c} = VALUES(${c})`).join(', ')}`
      : `INSERT INTO ${list} ${source} ON CONFLICT (${opts.onConflict.join(', ')}) DO UPDATE SET ${upd.map((c) => `${c} = excluded.${c}`).join(', ')}`;
  } else {
    sql = `INSERT INTO ${list} ${source}`;
  }
  if (opts.returning) sql += ` RETURNING ${opts.returning}`;
  return { sql, params: [param] };
}

describe('B. INSERT single & batch — makeSQL byte-matches REAL DBModel._insert (captured)', () => {
  for (const dialect of dialects) {
    // PG golden = REAL `_insert` (UNNEST, byte-match v1). MySQL/SQLite golden = the NEW
    // single-JSON-param form (independent `goldenInsertJson`, deviation OVER v1).
    const isPg = dialect === 'postgres';
    const jd = dialect as 'mysql' | 'sqlite';

    it(`[${dialect}] single INSERT`, async () => {
      const records = [{ id: 1, name: 'a' }];
      const got = renderComponents(compileInsertMany(dialect, { tableName: 'users', records, rawRecords: records, returning: 'id' }), dialect);
      const golden = isPg
        ? await captureInsert(dialect, records, { returning: 'id' })
        : [goldenInsertJson(jd, 'users', ['id', 'name'], records, { returning: 'id' })];
      expect(got.length).toBe(golden.length);
      expect(got).toEqual(golden);
    });

    it(`[${dialect}] homogeneous batch INSERT (single grouped statement)`, async () => {
      const records = [{ id: 1, name: 'a' }, { id: 2, name: 'b' }];
      const got = renderComponents(compileInsertMany(dialect, { tableName: 'users', records, rawRecords: records, returning: 'id' }), dialect);
      const golden = isPg
        ? await captureInsert(dialect, records, { returning: 'id' })
        : [goldenInsertJson(jd, 'users', ['id', 'name'], records, { returning: 'id' })];
      // Homogeneous → exactly ONE INSERT component (still ONE param for MySQL/SQLite).
      expect(golden.length).toBe(1);
      expect(got.length).toBe(1);
      if (!isPg) expect(got[0].params.length).toBe(1); // single JSON param, no N-explosion
      expect(got).toEqual(golden);
    });

    it(`[${dialect}] upsert: createMany + ON CONFLICT DO UPDATE (all)`, async () => {
      const records = [{ id: 1, name: 'a' }, { id: 2, name: 'b' }];
      const opts = { onConflict: ['id'], onConflictUpdate: 'all' as const, returning: 'id' };
      const got = renderComponents(
        compileInsertMany(dialect, { tableName: 'users', records, rawRecords: records, ...opts }),
        dialect
      );
      const golden = isPg
        ? await captureInsert(dialect, records, opts)
        : [goldenInsertJson(jd, 'users', ['id', 'name'], records, opts)];
      expect(got.length).toBe(golden.length);
      expect(got).toEqual(golden);
    });

    it(`[${dialect}] HETEROGENEOUS createMany → MULTIPLE grouped INSERT components`, async () => {
      // Rows with DIFFERENT column subsets: {id,name}, {id,name,age}, {id,name}.
      // Grouping by sorted-column-set pattern → 2 statements ({id,name} batch, then
      // {age,id,name} single). Kept identical to v1's grouping on ALL dialects; only the
      // per-group VALUES text differs (MySQL/SQLite → single JSON param).
      const records = [
        { id: 1, name: 'a' },
        { id: 2, name: 'b', age: 20 },
        { id: 3, name: 'c' },
      ];
      const components = compileInsertMany(dialect, { tableName: 'users', records, rawRecords: records, returning: 'id' });
      const got = renderComponents(components, dialect);
      const golden = isPg
        ? await captureInsert(dialect, records, { returning: 'id' })
        : [
            goldenInsertJson(jd, 'users', ['id', 'name'], [records[0], records[2]], { returning: 'id' }),
            goldenInsertJson(jd, 'users', ['age', 'id', 'name'], [records[1]], { returning: 'id' }),
          ];
      expect(golden.length).toBe(2);
      expect(components.length).toBe(2);
      expect(got).toEqual(golden);
      // Grouping (same on all dialects): first-seen {id,name}, then {age,id,name}.
      expect(components[0].sql).toContain('(id, name)');
      expect(components[1].sql).toContain('(age, id, name)');
      if (!isPg) {
        // Each group is ONE JSON param — no per-row placeholder explosion.
        got.forEach((g) => expect(g.params.length).toBe(1));
      }
    });

    it(`[${dialect}] HETEROGENEOUS via DEFAULT/undefined omission → split by column-presence`, async () => {
      // A DB-DEFAULT column is expressed by OMITTING it from the row's column set (never
      // a `DEFAULT` literal). `_insert` drops a column when its value is `undefined` OR a
      // `DBImmediateValue('DEFAULT')`, so these rows fall into a DIFFERENT column-set
      // pattern → a separate grouped statement. No `DEFAULT` text appears anywhere.
      const records = [
        { id: 1, name: 'a', age: 10 },
        { id: 2, name: 'b', age: new DBImmediateValue('DEFAULT') },
        { id: 3, name: 'c', age: undefined },
        { id: 4, name: 'd', age: 40 },
      ];
      const components = compileInsertMany(dialect, { tableName: 'users', records, rawRecords: records, returning: 'id' });
      const got = renderComponents(components, dialect);
      const golden = isPg
        ? await captureInsert(dialect, records, { returning: 'id' })
        : [
            goldenInsertJson(jd, 'users', ['age', 'id', 'name'], [records[0], records[3]], { returning: 'id' }),
            goldenInsertJson(jd, 'users', ['id', 'name'], [{ id: 2, name: 'b' }, { id: 3, name: 'c' }], { returning: 'id' }),
          ];
      expect(golden.length).toBe(2);
      expect(components.length).toBe(2);
      expect(got).toEqual(golden);
      // No DEFAULT literal; `age` simply disappears from group 2.
      expect(components[0].sql).toContain('(age, id, name)');
      expect(components[1].sql).toContain('(id, name)');
      expect(components[1].sql).not.toContain('age');
      expect(got.map((g) => g.sql).join('')).not.toContain('DEFAULT');
    });
  }
});

describe('B. createMany DBToken(NOW()) → v1 builder FALLBACK (not JSON form), byte-pinned', () => {
  // A DBToken value (e.g. NOW()) cannot be JSON-encoded, so its group MUST fall back to
  // the ORIGINAL multi-VALUES builder on MySQL/SQLite (the JSON_TABLE / json_each path is
  // skipped for that group). Pin the fallback to the v1 builder's exact text.
  for (const dialect of ['mysql', 'sqlite'] as const) {
    it(`[${dialect}] group with NOW() token falls back to v1 buildInsert (byte-match)`, () => {
      const records = [{ id: 1, name: 'a', created_at: dbRaw('NOW()') }];
      const columns = ['created_at', 'id', 'name']; // sorted column set
      const golden = builderOf[dialect].buildInsert({ tableName: 'users', columns, records });
      const components = compileInsertMany(dialect, { tableName: 'users', records, rawRecords: records });
      expect(components.length).toBe(1);
      const got = render(components[0], dialect);
      // Byte-identical to v1 (NOW() inlined, id/name as ? — NOT a JSON_TABLE/json_each form).
      expect(got.sql).toBe(renderPlaceholders(golden.sql, dialect));
      expect(got.params).toEqual(golden.params);
      expect(got.sql).toContain('VALUES (NOW(),');
      expect(got.sql).not.toContain('JSON_TABLE');
      expect(got.sql).not.toContain('json_each');
    });
  }
});

describe('B. RETURNING forms — bare / t.col alias (PG) / table.col (SQLite) / MySQL none', () => {
  it('buildReturning per dialect matches the anchor forms', () => {
    // PG batch UPDATE uses `t.col` alias; SQLite uses `table.col`; MySQL = undefined.
    expect(postgresSqlBuilder.buildReturning('users', ['id', 'name'], 't')).toBe('t.id, t.name');
    expect(postgresSqlBuilder.buildReturning('users', ['id'])).toBe('id'); // bare (no alias)
    expect(sqliteSqlBuilder.buildReturning('users', ['id', 'name'])).toBe('users.id, users.name');
    expect(mysqlSqlBuilder.buildReturning('users', ['id'])).toBeUndefined();
  });
  it('batch UPDATE carries the t.col RETURNING alias (PG)', () => {
    const returning = postgresSqlBuilder.buildReturning('users', ['id'], 't')!;
    const opts = {
      tableName: 'users',
      keyColumns: ['id'],
      updateColumns: ['name'],
      records: [{ id: 1, name: 'a' }, { id: 2, name: 'b' }],
      rawRecords: [{ id: 1, name: 'a' }, { id: 2, name: 'b' }],
      returning,
    };
    const golden = postgresSqlBuilder.buildUpdateMany(opts as any);
    const got = render(compileUpdateMany('postgres', opts as any), 'postgres');
    expect(got.sql).toBe(renderPlaceholders(golden.sql, 'postgres'));
    expect(got.sql).toContain('RETURNING t.id');
    expect(got.params).toEqual(golden.params);
  });
});

describe('B. batch UPDATE (+SKIP-column) — makeSQL byte-matches dialect builders', () => {
  const opts = {
    tableName: 'users',
    keyColumns: ['id'],
    updateColumns: ['name', 'age'],
    records: [{ id: 1, name: 'a', age: 10 }, { id: 2, name: 'b', age: 20 }],
    rawRecords: [{ id: 1, name: 'a', age: 10 }, { id: 2, name: 'b', age: 20 }],
    skipMap: new Map([[1, new Set(['age'])]]),
    returning: 'id',
  };
  // NEW JSON-form goldens (frozen literals) for MySQL/SQLite — deviation OVER v1's
  // VALUES-ROW-join (MySQL) / CASE-WHEN (SQLite). SKIP-column logic preserved. RESULT
  // PARITY with v1 proven on real DBs in json-array-parity.test.ts.
  const skipJson = JSON.stringify([
    { id: 1, name: 'a', age: 10, _skip_age: 0 },
    { id: 2, name: 'b', age: 20, _skip_age: 1 },
  ]);
  const mysqlUpdGolden: Rendered = {
    sql:
      "UPDATE users AS u JOIN JSON_TABLE(?, '$[*]' COLUMNS(" +
      "id BIGINT PATH '$.id', name JSON PATH '$.name', age BIGINT PATH '$.age', " +
      "_skip_age BIGINT PATH '$._skip_age')) AS v ON u.id = v.id " +
      'SET u.name = JSON_UNQUOTE(v.name), u.age = IF(v._skip_age, u.age, v.age) RETURNING id',
    params: [skipJson],
  };
  const sqliteUpdGolden: Rendered = {
    sql:
      "UPDATE users SET " +
      "name = (SELECT json_extract(je.value, '$.name') FROM json_each(?) je WHERE json_extract(je.value, '$.id') = users.id LIMIT 1), " +
      "age = (SELECT CASE WHEN json_extract(je.value, '$._skip_age') THEN users.age ELSE json_extract(je.value, '$.age') END FROM json_each(?) je WHERE json_extract(je.value, '$.id') = users.id LIMIT 1) " +
      "WHERE id IN (SELECT json_extract(value, '$.id') FROM json_each(?)) RETURNING id",
    params: [skipJson, skipJson, skipJson],
  };

  for (const dialect of dialects) {
    it(`[${dialect}] batch UPDATE + SKIP`, () => {
      const got = render(compileUpdateMany(dialect, opts as any), dialect);
      if (dialect === 'postgres') {
        const golden = builderOf[dialect].buildUpdateMany(opts as any);
        expect(got.sql).toBe(renderPlaceholders(golden.sql, dialect));
        expect(got.params).toEqual(golden.params);
      } else {
        expect(got).toEqual(dialect === 'mysql' ? mysqlUpdGolden : sqliteUpdGolden);
      }
    });
  }
});

describe('B. single UPDATE / DELETE — makeSQL byte-matches original _update/_delete text', () => {
  for (const dialect of dialects) {
    it(`[${dialect}] single UPDATE (per-col cast on PG)`, () => {
      const serialized = { name: 'x', id_ext: 'u1' };
      const conditions = { id: 5 };
      const sqlCastMap = new Map([['id_ext', 'uuid']]);
      // Golden: reproduce original _update text with the same formatter.
      const params: unknown[] = [];
      const formatter = dialect === 'postgres' ? pgFmt : undefined;
      const setClauses: string[] = [];
      for (const [col, val] of Object.entries(serialized)) {
        params.push(val);
        const c = sqlCastMap.get(col);
        if (c && formatter && c !== 'timestamp' && c !== 'date') setClauses.push(`${col} = ${formatter('?', c)}`);
        else setClauses.push(`${col} = ?`);
      }
      const where = new DBConditions(conditions).compile(params, formatter);
      const goldenSql = `UPDATE users SET ${setClauses.join(', ')} WHERE ${where} RETURNING id`;
      const golden = { sql: renderPlaceholders(goldenSql, dialect), params };

      const got = render(
        compileUpdateSingle({ dialect, tableName: 'users', serializedValues: serialized, conditions, sqlCastMap, returning: 'id' }),
        dialect
      );
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
    });
    it(`[${dialect}] single DELETE (IN-list)`, () => {
      const conditions = { id: [1, 2, 3] };
      const got = render(compileDelete({ dialect, tableName: 'users', conditions }), dialect);
      if (dialect === 'postgres') {
        // PG: unchanged v1 IN (?, ?, ?).
        const params: unknown[] = [];
        const where = new DBConditions(conditions).compile(params, pgFmt);
        const golden = { sql: renderPlaceholders(`DELETE FROM users WHERE ${where}`, dialect), params };
        expect(got.sql).toBe(golden.sql);
        expect(got.params).toEqual(golden.params);
      } else {
        // MySQL/SQLite: NEW single-JSON-param IN-list form.
        const golden = dialect === 'mysql'
          ? { sql: "DELETE FROM users WHERE id IN (SELECT JSON_UNQUOTE(v) FROM JSON_TABLE(?, '$[*]' COLUMNS(v JSON PATH '$')) jt)", params: ['[1,2,3]'] }
          : { sql: 'DELETE FROM users WHERE id IN (SELECT value FROM json_each(?))', params: ['[1,2,3]'] };
        expect(got).toEqual(golden);
      }
    });
  }
  it('DELETE without WHERE throws (v1 anchor)', () => {
    expect(() => compileDelete({ dialect: 'postgres', tableName: 'users', conditions: {} })).toThrow(/DELETE requires conditions/);
  });
  it('UPDATE without WHERE throws (v1 anchor)', () => {
    expect(() =>
      compileUpdateSingle({ dialect: 'postgres', tableName: 'users', serializedValues: { a: 1 }, conditions: {} })
    ).toThrow(/UPDATE requires conditions/);
  });
});

describe('B. SELECT tail — LIMIT/OFFSET inline, FOR UPDATE, GROUP BY', () => {
  for (const dialect of dialects) {
    it(`[${dialect}] SELECT + GROUP BY + ORDER + LIMIT + OFFSET + FOR UPDATE`, () => {
      const params: unknown[] = [];
      const formatter = dialect === 'postgres' ? pgFmt : undefined;
      const where = new DBConditions({ status: 'active' }).compile(params, formatter);
      const goldenSql =
        `SELECT * FROM posts WHERE ${where} GROUP BY author_id ORDER BY created_at DESC LIMIT 10 OFFSET 5 FOR UPDATE`;
      const golden = { sql: renderPlaceholders(goldenSql, dialect), params };
      const got = render(
        compileSelect({
          dialect,
          tableName: 'posts',
          conditions: { status: 'active' },
          group: 'author_id',
          order: 'created_at DESC',
          limit: 10,
          offset: 5,
          forUpdate: true,
        }),
        dialect
      );
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
    });
  }
});

// ===========================================================================
// C. Relations — golden = LazyRelationContext ACTUAL output (captured).
// ===========================================================================
describe('C. Relations — makeSQL byte-matches LazyRelation (all shapes, all dialects)', () => {
  function makeModels(driver: Dialect) {
    const captures: Rendered[] = [];
    class Base extends DBModel {
      static getDriverType(): Dialect {
        return driver;
      }
      static async query(sql: string, params: unknown[]): Promise<any[]> {
        captures.push({ sql, params });
        return [];
      }
    }
    class Post extends Base {
      protected static TABLE_NAME = 'posts';
      protected static SELECT_COLUMN = '*';
      id?: number;
      tenant_id?: number;
      author_id?: number;
    }
    class User extends Base {
      protected static TABLE_NAME = 'users';
      protected static SELECT_COLUMN = '*';
      id?: number;
    }
    class Comment extends Base {
      protected static TABLE_NAME = 'comments';
      protected static SELECT_COLUMN = '*';
      id?: number;
      post_id?: number;
      tenant_id?: number;
    }
    return { captures, Post, User, Comment };
  }

  async function captureRelation(
    driver: Dialect,
    Source: typeof DBModel,
    records: DBModel[],
    relType: 'belongsTo' | 'hasMany' | 'hasOne',
    config: any,
    captures: Rendered[]
  ): Promise<Rendered> {
    const ctx = new LazyRelationContext(Source as any, records as any);
    captures.length = 0;
    await (ctx as any).getRelation(records[0], relType, config);
    expect(captures.length).toBe(1);
    return { sql: renderPlaceholders(captures[0].sql, driver), params: captures[0].params };
  }

  // Rewrite a captured v1 single-key relation golden (`key IN (?, …)` with N element
  // params) into the NEW single-JSON-param form (epic #43/#45). Only the single-column
  // IN-list is rewritten; composite tuple-IN is unchanged (kept v1 on all dialects).
  // This mirrors exactly what `conditionsFor` does, so the golden stays an INDEPENDENT
  // target derived from the v1 capture rather than from the v2 compile.
  function jsonifyRelation(v1: Rendered, dialect: 'mysql' | 'sqlite', col: string, values: unknown[]): Rendered {
    const nPlaceholders = `${col} IN (${values.map(() => '?').join(', ')})`;
    const jsonForm =
      dialect === 'mysql'
        ? `${col} IN (SELECT JSON_UNQUOTE(v) FROM JSON_TABLE(?, '$[*]' COLUMNS(v JSON PATH '$')) jt)`
        : `${col} IN (SELECT value FROM json_each(?))`;
    expect(v1.sql).toContain(nPlaceholders); // guard: the v1 capture really had the N-form
    // Use a replacer FUNCTION so `$` in jsonForm (e.g. `'$[*]'`, `PATH '$'`) is inserted
    // literally — a string replacement would treat `$'`/`$&` as special patterns.
    const sql = v1.sql.replace(nPlaceholders, () => jsonForm);
    // The N element params (wherever the IN-list sits in the param stream) collapse to
    // ONE JSON string param, in the same position. Locate the contiguous `values` run.
    const p = v1.params;
    let at = -1;
    for (let i = 0; i + values.length <= p.length; i++) {
      if (values.every((v, k) => p[i + k] === v)) { at = i; break; }
    }
    expect(at).toBeGreaterThanOrEqual(0);
    const params = [...p.slice(0, at), JSON.stringify(values), ...p.slice(at + values.length)];
    return { sql, params };
  }

  for (const dialect of dialects) {
    it(`[${dialect}] single-key belongsTo unlimited`, async () => {
      const { captures, Post, User } = makeModels(dialect);
      const posts = [
        Object.assign(new Post(), { id: 1, author_id: 10 }),
        Object.assign(new Post(), { id: 2, author_id: 11 }),
        Object.assign(new Post(), { id: 3, author_id: 10 }),
      ];
      const golden = await captureRelation(
        dialect,
        Post as any,
        posts as any,
        'belongsTo',
        { targetClass: User, targetKey: 'id', sourceKey: 'author_id', relationName: 'author' },
        captures
      );
      const got = render(
        compileSingleKeyUnlimited({ dialect, tableName: 'users', targetKey: 'id', values: [10, 11] }),
        dialect
      );
      const expected = dialect === 'postgres' ? golden : jsonifyRelation(golden, dialect, 'id', [10, 11]);
      expect(got.sql).toBe(expected.sql);
      expect(got.params).toEqual(expected.params);
    });

    it(`[${dialect}] single-key hasMany unlimited + ORDER + where-filter`, async () => {
      const { captures, Post, Comment } = makeModels(dialect);
      const posts = [Object.assign(new Post(), { id: 1 }), Object.assign(new Post(), { id: 2 })];
      const golden = await captureRelation(
        dialect,
        Post as any,
        posts as any,
        'hasMany',
        {
          targetClass: Comment,
          targetKey: 'post_id',
          sourceKey: 'id',
          order: 'created_at DESC',
          conditions: { status: 'published' },
          relationName: 'comments',
        },
        captures
      );
      const got = render(
        compileSingleKeyUnlimited({
          dialect,
          tableName: 'comments',
          targetKey: 'post_id',
          values: [1, 2],
          order: 'created_at DESC',
          conditions: { status: 'published' },
        }),
        dialect
      );
      const expected = dialect === 'postgres' ? golden : jsonifyRelation(golden, dialect, 'post_id', [1, 2]);
      expect(got.sql).toBe(expected.sql);
      expect(got.params).toEqual(expected.params);
    });

    it(`[${dialect}] single-key hasMany + per-parent LIMIT`, async () => {
      const { captures, Post, Comment } = makeModels(dialect);
      const posts = [Object.assign(new Post(), { id: 1 }), Object.assign(new Post(), { id: 2 })];
      const golden = await captureRelation(
        dialect,
        Post as any,
        posts as any,
        'hasMany',
        { targetClass: Comment, targetKey: 'post_id', sourceKey: 'id', limit: 5, order: 'created_at DESC', relationName: 'c5' },
        captures
      );
      const got = render(
        compileSingleKeyLimited({
          dialect,
          tableName: 'comments',
          targetKey: 'post_id',
          values: [1, 2],
          limit: 5,
          order: 'created_at DESC',
        }),
        dialect
      );
      const expected = dialect === 'postgres' ? golden : jsonifyRelation(golden, dialect, 'post_id', [1, 2]);
      expect(got.sql).toBe(expected.sql);
      expect(got.params).toEqual(expected.params);
    });

    it(`[${dialect}] composite-key hasMany unlimited`, async () => {
      const { captures, Post, Comment } = makeModels(dialect);
      const posts = [
        Object.assign(new Post(), { id: 1, tenant_id: 100 }),
        Object.assign(new Post(), { id: 2, tenant_id: 100 }),
      ];
      const golden = await captureRelation(
        dialect,
        Post as any,
        posts as any,
        'hasMany',
        {
          targetClass: Comment,
          targetKeys: ['tenant_id', 'post_id'],
          sourceKeys: ['tenant_id', 'id'],
          relationName: 'cc',
        },
        captures
      );
      const got = render(
        compileCompositeKeyUnlimited({
          dialect,
          tableName: 'comments',
          targetKeys: ['tenant_id', 'post_id'],
          tuples: [[100, 1], [100, 2]],
        }),
        dialect
      );
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
    });

    it(`[${dialect}] composite-key hasMany + per-parent LIMIT`, async () => {
      const { captures, Post, Comment } = makeModels(dialect);
      const posts = [
        Object.assign(new Post(), { id: 1, tenant_id: 100 }),
        Object.assign(new Post(), { id: 2, tenant_id: 100 }),
      ];
      const golden = await captureRelation(
        dialect,
        Post as any,
        posts as any,
        'hasMany',
        {
          targetClass: Comment,
          targetKeys: ['tenant_id', 'post_id'],
          sourceKeys: ['tenant_id', 'id'],
          limit: 3,
          order: 'created_at DESC',
          relationName: 'ccl',
        },
        captures
      );
      const got = render(
        compileCompositeKeyLimited({
          dialect,
          tableName: 'comments',
          targetKeys: ['tenant_id', 'post_id'],
          tuples: [[100, 1], [100, 2]],
          limit: 3,
          order: 'created_at DESC',
        }),
        dialect
      );
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
    });
  }
});
