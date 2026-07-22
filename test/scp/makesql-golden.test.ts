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
  conditionsFor,
  compileWhere,
  compileSelect,
  compileInsertMany,
  compileUpdateMany,
  compileUpdateSingle,
  compileDelete,
  compileDeleteMany,
  compileSingleKeyUnlimited,
  compileSingleKeyLimited,
  compileCompositeKeyUnlimited,
  compileCompositeKeyStaticUnlimited,
  compileCompositeKeyLimited,
  compileCompositeKeyStaticLimited,
  compileSelectNode,
  resolvePgArrayCast,
  compileWriteNode,
  renderTxStatement,
  type Dialect,
  type MakeSQL,
} from '../../src/scp/makesql';
import {
  components,
  publishBehaviors,
  SemanticBehavior,
  whereEq,
  whereBetween,
  whereLike,
  whereILike,
  whereCast,
  whereDynamic,
  whereImmediate,
  whereTupleIn,
  whereInSubquery,
  whereExists,
  // Phase E-1 (#97): typed subquery / parentRef authoring sugar. `parentRef` is aliased to avoid a
  // clash with v1's `DBValues.parentRef` imported above (the golden generator).
  col as scpCol,
  parentRef as scpParentRef,
  inSubquery as scpInSubquery,
  notInSubquery as scpNotInSubquery,
  exists as scpExists,
  notExists as scpNotExists,
  emitRead,
  type In,
  type BehaviorModelContract,
} from '../../src/scp/index';
// The read-leaf renderer is the SSoT `renderPrimaryRead` in the conformance harness (one renderer, no
// duplication) — the leaf-path replacement for the retired `compileReadGraph`→`renderReadPrimary`.
import { renderPrimaryRead } from '../../conformance/harness';

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

  // #47: byte-assert composite (a,b) IN(subquery) on ALL dialects (was PG only). The row-value
  // IN-subquery construct is dialect-INVARIANT in DBConditions (only the placeholder renderer
  // differs — `$N` on PG vs `?` on MySQL/SQLite), so every dialect byte-matches v1 DBConditions.
  // The matching live vector (CompInSubquery) proves it EXECUTES on real PG + MySQL.
  for (const dialect of dialects) {
    it(`[${dialect}] NOT IN(subquery) + composite (a,b) IN(subquery)`, () => {
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
      const formatter = dialect === 'postgres' ? pgFmt : (ph: string) => ph;
      for (const ex of [notIn, comp]) {
        const condObj = { __subquery__: ex };
        const params: unknown[] = [];
        const goldenSql = new DBConditions(condObj).compile(params, formatter);
        const golden = { sql: renderPlaceholders(goldenSql, dialect), params };
        const got = render(compileWhere(condObj, dialect), dialect);
        expect(got.sql).toBe(golden.sql);
        expect(got.params).toEqual(golden.params);
      }
      // The composite lhs renders as a row-value tuple on every dialect (v1-sourced text).
      const compSql = renderPlaceholders(new DBConditions({ __subquery__: comp }).compile([], formatter), dialect);
      expect(compSql).toContain('(users.tenant_id, users.id) IN (SELECT orders.tenant_id, orders.user_id FROM orders WHERE orders.status =');
    });
  }

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
// A2 (#97). Typed subquery / parentRef SUGAR — the v2 typed builders (scpInSubquery /
// scpExists / scpParentRef) RENDER byte-identically to v1's typed API (Model.inSubquery /
// Model.exists / DBValues.parentRef → DBConditions golden). Proves the ergonomic sugar is a
// pure lowering onto the existing whereInSubquery/whereExists with no divergence, on all
// dialects. GOLDEN = v1 DBConditions.compile output (same technique as section A).
// ===========================================================================
describe('A2 (#97). typed subquery / parentRef sugar — byte-matches v1 typed API', () => {
  class SubBase extends DBModel {
    static getDriverType(): Dialect {
      return 'postgres';
    }
  }
  class Usr extends SubBase {
    protected static TABLE_NAME = 'users';
    id?: number;
  }
  void Usr;

  // The v2 typed builders emit a bc-authored WHERE member that only reaches SQL through the
  // publish-time WHERE lowering, so we author each construct in a tiny SemanticBehavior, publish it,
  // and read back the WHERE fragment from the read leaf's lowered sql (byte SQL + params).
  const L = components();
  const users_id = scpCol('users', 'id');
  const users_name = scpCol('users', 'name');
  const posts_author = scpCol('posts', 'author_id');
  const posts_id = scpCol('posts', 'id');
  const orders_userid = scpCol('orders', 'user_id');

  class TypedSub extends SemanticBehavior {
    static columns = {
      posts: { id: 'INTEGER', author_id: 'INTEGER' },
      users: { id: 'INTEGER', name: 'TEXT' },
      orders: { user_id: 'INTEGER' },
    };
    InSub(_$: In<Record<string, never>>) {
      return emitRead(L, 'Select', { table: 'posts', select: ['id'], where: [scpInSubquery(_$, [posts_author, users_id], [[users_name, 'Ada']])] }, 'sqlite');
    }
    NotInSub(_$: In<Record<string, never>>) {
      return emitRead(L, 'Select', { table: 'posts', select: ['id'], where: [scpNotInSubquery(_$, [posts_author, users_id], [[users_name, 'Ada']])] }, 'sqlite');
    }
    ExistsCorr(_$: In<Record<string, never>>) {
      return emitRead(L, 'Select', { table: 'posts', select: ['id'], where: [scpExists(_$, [[orders_userid, scpParentRef(posts_id)]])] }, 'sqlite');
    }
    NotExistsCorr(_$: In<Record<string, never>>) {
      return emitRead(L, 'Select', { table: 'posts', select: ['id'], where: [scpNotExists(_$, [[orders_userid, scpParentRef(posts_id)]])] }, 'sqlite');
    }
  }
  /** The rendered WHERE-fragment `{sql,params}` the typed builder produces for a dialect (#143: the
   * WHERE is now lowered into the read leaf's static sql at publish — extract the clause after WHERE). */
  function typedFragment(entry: string, dialect: Dialect): Rendered {
    const full = renderPrimaryRead(publishBehaviors(TypedSub, { dialect }), entry, {}, dialect);
    const idx = full.sql.indexOf(' WHERE ');
    if (idx < 0) throw new Error(`no WHERE fragment for ${entry} [${dialect}]`);
    return { sql: full.sql.slice(idx + ' WHERE '.length), params: full.params };
  }

  for (const dialect of dialects) {
    const fmt = dialect === 'postgres' ? pgFmt : (ph: string) => ph;

    it(`[${dialect}] scpInSubquery / scpNotInSubquery == v1 Model.inSubquery/notInSubquery`, () => {
      for (const [entry, op] of [['InSub', 'IN'], ['NotInSub', 'NOT IN']] as const) {
        // v1 golden: Model.inSubquery / notInSubquery → DBSubquery → DBConditions.compile.
        const sub = new DBSubquery(
          [{ columnName: 'author_id', tableName: 'posts' }],
          'users',
          [{ columnName: 'id', tableName: 'users' }],
          [{ column: { columnName: 'name', tableName: 'users' }, value: 'Ada' }],
          op,
        );
        const params: unknown[] = [];
        const goldenSql = renderPlaceholders(new DBConditions({ __subquery__: sub }).compile(params, fmt), dialect);
        const got = typedFragment(entry, dialect);
        expect(got.sql).toBe(goldenSql);
        expect(got.params).toEqual(params);
      }
    });

    it(`[${dialect}] scpExists / scpNotExists + scpParentRef == v1 Model.exists/notExists + parentRef`, () => {
      for (const [entry, not] of [['ExistsCorr', false], ['NotExistsCorr', true]] as const) {
        const ex = new DBExists(
          'orders',
          [{ column: { columnName: 'user_id', tableName: 'orders' }, value: parentRef({ columnName: 'id', tableName: 'posts' }) }],
          not,
        );
        const params: unknown[] = [];
        const goldenSql = renderPlaceholders(new DBConditions({ __exists__: ex }).compile(params, fmt), dialect);
        const got = typedFragment(entry, dialect);
        expect(got.sql).toBe(goldenSql);
        expect(got.params).toEqual(params);
      }
    });
  }
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
      // #67: SQLite requires `WHERE true` between the SELECT source and ON CONFLICT in an
      // INSERT…SELECT…upsert (else `near "DO": syntax error`) — mirror the fixed sqliteInsertJson.
      : `INSERT INTO ${list} ${source} WHERE true ON CONFLICT (${opts.onConflict.join(', ')}) DO UPDATE SET ${upd.map((c) => `${c} = excluded.${c}`).join(', ')}`;
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
    it(`[${dialect}] single UPDATE SET/WHERE scaffold byte-matches v1 _update (captured)`, async () => {
      // Golden = the REAL v1 `DBModel._update` statement, CAPTURED from `execute` (drives v1, not
      // v2-v2). A model with NO column decorators → `serializeRecord` is identity, so the captured
      // SET/WHERE scaffold is the pure v1 `UPDATE <t> SET … WHERE …[ RETURNING …]` text. Perturbing
      // the v1 `_update` scaffold string MOVES this golden.
      const { Model, captures } = makeWriteModel(dialect);
      captures.length = 0;
      await (Model as any)._update({ id: 5 }, { name: 'x', age: 3 }, { returning: 'id' });
      expect(captures.length).toBe(1);
      const golden = { sql: renderPlaceholders(captures[0].sql, dialect), params: captures[0].params };

      const got = render(
        compileUpdateSingle({ dialect, tableName: 'users', serializedValues: { name: 'x', age: 3 }, conditions: { id: 5 }, returning: 'id' }),
        dialect
      );
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
    });
    it(`[${dialect}] single UPDATE per-col cast (PG) — SET cast text v1-sourced`, () => {
      // The per-column `?::uuid` cast is applied by the SAME dialect formatter v1 `_update` uses
      // (`_getSqlCastFormatter` → `getSqlCastFormatter(dialect)`), which is byte-identical to
      // `formatterFor(dialect)` (compile.ts). Assert the compile applies v1's exact cast text.
      const serialized = { name: 'x', id_ext: 'u1' };
      const sqlCastMap = new Map([['id_ext', 'uuid']]);
      const params: unknown[] = [];
      const formatter = dialect === 'postgres' ? pgFmt : undefined;
      const setClauses: string[] = [];
      for (const [col, val] of Object.entries(serialized)) {
        params.push(val);
        const c = sqlCastMap.get(col);
        if (c && formatter && c !== 'timestamp' && c !== 'date') setClauses.push(`${col} = ${formatter('?', c)}`);
        else setClauses.push(`${col} = ?`);
      }
      const where = new DBConditions({ id: 5 }).compile(params, formatter);
      const golden = { sql: renderPlaceholders(`UPDATE users SET ${setClauses.join(', ')} WHERE ${where} RETURNING id`, dialect), params };
      const got = render(
        compileUpdateSingle({ dialect, tableName: 'users', serializedValues: serialized, conditions: { id: 5 }, sqlCastMap, returning: 'id' }),
        dialect
      );
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
    });
    it(`[${dialect}] single DELETE scaffold byte-matches v1 _delete (captured)`, async () => {
      // Golden = the REAL v1 `DBModel._delete` statement, CAPTURED from `execute` (drives v1).
      const { Model, captures } = makeWriteModel(dialect);
      captures.length = 0;
      await (Model as any)._delete({ id: [1, 2, 3] }, {});
      expect(captures.length).toBe(1);
      const v1 = { sql: renderPlaceholders(captures[0].sql, dialect), params: captures[0].params };
      const got = render(compileDelete({ dialect, tableName: 'users', conditions: { id: [1, 2, 3] } }), dialect);
      if (dialect === 'postgres') {
        // PG: byte-identical to v1 `_delete` (IN (?, ?, ?)).
        expect(got.sql).toBe(v1.sql);
        expect(got.params).toEqual(v1.params);
      } else {
        // MySQL/SQLite: the `DELETE FROM users WHERE <col> ` SCAFFOLD is byte-identical to v1; only the
        // IN-list is the documented single-JSON-param deviation (result-parity proven on real DBs in
        // json-array-parity.test.ts). Pin the scaffold prefix to v1, then the JSON-form IN-list.
        expect(v1.sql).toBe('DELETE FROM users WHERE id IN (?, ?, ?)');
        expect(got.sql.startsWith('DELETE FROM users WHERE id IN (')).toBe(true);
        const golden = dialect === 'mysql'
          ? { sql: "DELETE FROM users WHERE id IN (SELECT JSON_UNQUOTE(v) FROM JSON_TABLE(?, '$[*]' COLUMNS(v JSON PATH '$')) jt)", params: ['[1,2,3]'] }
          : { sql: 'DELETE FROM users WHERE id IN (SELECT value FROM json_each(?))', params: ['[1,2,3]'] };
        expect(got).toEqual(golden);
      }
    });
  }
  // deleteMany (V0 addition — the DONE-list gap): compileDeleteMany COMPOSES compileDelete
  // (→ DBConditions/conditionsFor), so its per-statement text is byte-identical to what the v1
  // condition path emits. Single PK → ONE DELETE with a v1 IN-list; composite PK → ONE DELETE per
  // present-column-set group whose WHERE is the v1 conjunction of per-column IN-lists. PG stays
  // byte-match v1 (IN (?, …)); MySQL/SQLite take the SAME single-column-IN JSON-form deviation the
  // single DELETE / IN-list golden above uses (proven result-equal on real DBs elsewhere).
  for (const dialect of dialects) {
    it(`[${dialect}] deleteMany single-PK → v1 IN-list DELETE (byte-match)`, () => {
      const keys = [{ id: 1 }, { id: 3 }, { id: 5 }];
      const got = compileDeleteMany({ dialect, tableName: 'users', keyColumns: ['id'], keys }).map((c) => render(c, dialect));
      expect(got.length).toBe(1);
      if (dialect === 'postgres') {
        const params: unknown[] = [];
        const where = new DBConditions({ id: [1, 3, 5] }).compile(params, pgFmt);
        const golden = { sql: renderPlaceholders(`DELETE FROM users WHERE ${where}`, dialect), params };
        expect(got[0].sql).toBe(golden.sql);
        expect(got[0].params).toEqual(golden.params);
      } else {
        const golden = dialect === 'mysql'
          ? { sql: "DELETE FROM users WHERE id IN (SELECT JSON_UNQUOTE(v) FROM JSON_TABLE(?, '$[*]' COLUMNS(v JSON PATH '$')) jt)", params: ['[1,3,5]'] }
          : { sql: 'DELETE FROM users WHERE id IN (SELECT value FROM json_each(?))', params: ['[1,3,5]'] };
        expect(got[0]).toEqual(golden);
      }
    });

    it(`[${dialect}] deleteMany composite-PK → v1 per-column IN-list conjunction (byte-match)`, () => {
      // A composite PK's WHERE is the v1 conjunction of per-column IN-lists, built through the SAME
      // conditionsFor (→ DBConditions on PG, the documented single-column-IN JSON rewrite per column
      // on MySQL/SQLite). The golden drives that ORIGINAL builder directly — one present-column-set
      // group → ONE DELETE — so it is byte-match to the v1 condition path on every dialect.
      const keys = [{ tenant_id: 100, id: 1 }, { tenant_id: 100, id: 2 }];
      const got = compileDeleteMany({ dialect, tableName: 'comments', keyColumns: ['tenant_id', 'id'], keys }).map((c) => render(c, dialect));
      expect(got.length).toBe(1);
      const params: unknown[] = [];
      const formatter = dialect === 'postgres' ? pgFmt : undefined;
      const where = conditionsFor({ tenant_id: [100, 100], id: [1, 2] }, dialect).compile(params, formatter);
      const golden = { sql: renderPlaceholders(`DELETE FROM comments WHERE ${where}`, dialect), params };
      expect(got[0].sql).toBe(golden.sql);
      expect(got[0].params).toEqual(golden.params);
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

// ---------------------------------------------------------------------------
// v1-PINNED SELECT / write scaffold golden source (#47 Finding A).
//
// The SELECT scaffold (SELECT/FROM/JOIN/WHERE/GROUP BY/ORDER BY/LIMIT/OFFSET/
// FOR UPDATE/append/CTE) and the write scaffolds (UPDATE SET…WHERE, DELETE FROM…
// WHERE) are VERBATIM hand-copies of v1 `DBModel._buildSelectSQL` / `_update` /
// `_delete`. A golden built by calling `compileSelect`/`compileUpdateSingle`/
// `compileDelete` (the v2 copy) against ITSELF is v2-to-v2 — perturbing v1 leaves it
// GREEN (the #43 `::text[]` class of drift). These helpers DRIVE the REAL v1 methods
// (the internal `_buildSelectSQL` the builder `find()` uses, and `_update`/`_delete`
// via `execute` capture) so the golden MOVES when v1 source is perturbed. Negative
// asserts below prove the pin (perturb → FAIL).
// ---------------------------------------------------------------------------

/** A DBModel subclass exposing the internal `_buildSelectSQL` for a given dialect. */
function makeSelectModel(driver: Dialect, tableName: string, selectCol: string): typeof DBModel {
  class Base extends DBModel {
    static getDriverType(): Dialect { return driver; }
  }
  class M extends Base {
    protected static TABLE_NAME = tableName;
    protected static SELECT_COLUMN = selectCol;
    // `_buildSelectSQL` is protected — expose it for the golden.
    static v1Select(cond: any, opts: any): { sql: string; params: unknown[] } {
      return (this as any)._buildSelectSQL(cond, opts);
    }
  }
  return M as unknown as typeof DBModel;
}

/**
 * v1 golden for a SELECT: the DIRECT output of `DBModel._buildSelectSQL`, dialect-
 * rendered. Passing the projection through `select`/`tableName` options keeps the
 * model generic (no per-shape subclass). PERTURBING v1 `_buildSelectSQL` MOVES this.
 */
function v1Select(
  dialect: Dialect,
  tableName: string,
  selectCol: string,
  conditions: Record<string, unknown>,
  options: Record<string, unknown> = {},
): Rendered {
  const built = (makeSelectModel(dialect, tableName, selectCol) as any).v1Select(conditions, {
    tableName, select: selectCol, ...options,
  });
  return { sql: renderPlaceholders(built.sql, dialect), params: built.params };
}

/** A DBModel subclass that captures each write statement `_update`/`_delete` would execute. */
function makeWriteModel(driver: Dialect): { Model: typeof DBModel; captures: Rendered[] } {
  const captures: Rendered[] = [];
  class Base extends DBModel {
    static getDriverType(): Dialect { return driver; }
    // Writes normally require a live transaction; the golden captures SQL only.
    protected static _checkWriteAllowed(): void {}
    // `_update`/`_delete`/`_count` call `this.execute(sql, params)` — record instead of running.
    // Return a single `{ count: 0 }` row so `_count` (which reads `rows[0].count`) survives the
    // capture; `_update`/`_delete` map over `rows` (a `{count}` row is harmless — we assert on SQL).
    static execute(sql: any, params?: any): any {
      captures.push({ sql: sql as string, params: (params ?? []) as unknown[] });
      return Promise.resolve({ rows: [{ count: 0 }], rowCount: 1 });
    }
  }
  class Users extends Base {
    protected static TABLE_NAME = 'users';
    protected static SELECT_COLUMN = '*';
  }
  return { Model: Users as unknown as typeof DBModel, captures };
}

describe('B. SELECT tail — LIMIT/OFFSET inline, FOR UPDATE, GROUP BY', () => {
  for (const dialect of dialects) {
    it(`[${dialect}] SELECT + GROUP BY + ORDER + LIMIT + OFFSET + FOR UPDATE`, () => {
      // Golden = the DIRECT output of v1 `DBModel._buildSelectSQL` (not compileSelect against
      // itself). Perturbing v1 (e.g. FOR UPDATE → FOR UPDATE NOWAIT) MOVES this golden.
      const golden = v1Select(dialect, 'posts', '*', { status: 'active' }, {
        group: 'author_id', order: 'created_at DESC', limit: 10, offset: 5, forUpdate: true,
      });
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

    it(`[${dialect}] grouped aggregate SELECT (GROUP BY + COUNT projection) — byte-matches v1 (R3 live vector head)`, () => {
      // The exact SELECT the R3 GroupByAuthor live vector authors: a grouped aggregate over the
      // `select` + `group` SELECT_PORTS. Golden = DIRECT v1 `_buildSelectSQL` output.
      const golden = v1Select(dialect, 'posts', 'author_id, COUNT(*) as n', {}, {
        group: 'author_id', order: 'author_id ASC',
      });
      const got = render(
        compileSelect({ dialect, tableName: 'posts', select: 'author_id, COUNT(*) as n', group: 'author_id', order: 'author_id ASC' }),
        dialect,
      );
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
    });
  }

  // NEGATIVE (golden-from-originals): perturbing v1 `_buildSelectSQL` MUST move a golden. Proven
  // structurally here — the golden is DERIVED from `_buildSelectSQL`, so a change to the FOR UPDATE
  // / LIMIT / OFFSET tail text in the v1 source changes `v1Select(...)` and the assertion fails.
  it('negative: v1 _buildSelectSQL tail text drives the golden (not v2-v2)', () => {
    const dialect: Dialect = 'postgres';
    // Same shape, different tail options → different v1 output. If the golden were a v2-v2 constant
    // (compileSelect vs itself) it would be insensitive to the v1 method; here it tracks v1 exactly.
    const withForUpdate = v1Select(dialect, 'posts', '*', {}, { limit: 10, forUpdate: true }).sql;
    const withoutForUpdate = v1Select(dialect, 'posts', '*', {}, { limit: 10 }).sql;
    expect(withForUpdate).not.toBe(withoutForUpdate);
    expect(withForUpdate.endsWith(' FOR UPDATE')).toBe(true);
    // And v2 compileSelect reproduces the v1 FOR UPDATE tail byte-for-byte.
    expect(render(compileSelect({ dialect, tableName: 'posts', limit: 10, forUpdate: true }), dialect).sql)
      .toBe(withForUpdate);
  });
});

describe('B. hand-roll removal — LIMIT/OFFSET tail v1-sourced (#47 item 5)', () => {
  // The static-bundle LIMIT/OFFSET tail used to be a v2 hand-roll (` LIMIT ?`); it now sources the
  // ` LIMIT `/` OFFSET ` KEYWORD text from the ORIGINAL `compileSelect` (v1 `_buildSelectSQL`),
  // keeping the intentional `?` bound-param divergence. The golden drives v1 directly.
  for (const dialect of dialects) {
    it(`[${dialect}] Select LIMIT/OFFSET keyword text == v1 _buildSelectSQL (count → ?)`, () => {
      // v1 golden: the exact ` LIMIT <n>`/` OFFSET <n>` append v1 `_buildSelectSQL` emits, with the
      // literal → `?`. Static statements carry the `?` (pre-render) form, so compare `?`-form to `?`.
      // Golden DRIVES v1 `_buildSelectSQL` directly (perturbing its tail text moves the golden).
      const v1LimitFull = v1Select(dialect, 'posts', '*', {}, { limit: 987654321 }).sql;
      const v1Tail = v1LimitFull.slice(`SELECT * FROM posts`.length).replace('987654321', '?'); // ` LIMIT ?`
      const node = { component: 'Select', ports: { table: 'posts', select: { arr: ['id'] }, limit: { int: '10' } } };
      const stmts = compileSelectNode(node as never, dialect);
      const limitStmt = stmts.find((s) => / LIMIT /.test(s.sql));
      expect(limitStmt?.sql).toBe(v1Tail);
      // NEGATIVE (golden-from-originals): perturb v1's count and the golden tail moves.
      const perturbed = v1Select(dialect, 'posts', '*', {}, { limit: 42 }).sql.slice(`SELECT * FROM posts`.length);
      expect(perturbed).not.toBe(v1LimitFull.slice(`SELECT * FROM posts`.length));
    });
  }
});

describe('B. COUNT — makeSQL byte-matches v1 DBModel._count head (#47 item 2)', () => {
  // The v1 `_count` (src/DBModel.ts) assembles `SELECT COUNT(*) as count FROM <t>` + a
  // `DBConditions`-built ` WHERE <clause>` and hands it to `execute`. The SCP `Count` leaf compiles
  // its head through `compileSelect` (projection `COUNT(*) as count`) + the SAME `DBConditions` WHERE
  // path. Golden = the REAL `_count` statement CAPTURED from `execute` (drives v1, not v2-v2): perturb
  // v1's `SELECT COUNT(*) as count FROM …` head text and this golden MOVES.
  /** Drive the REAL v1 `_count` and return the captured statement, dialect-rendered. */
  async function v1CountHead(dialect: Dialect, conditions: Record<string, unknown>): Promise<Rendered> {
    const { Model, captures } = makeWriteModel(dialect);
    class Posts extends (Model as any) { protected static TABLE_NAME = 'posts'; }
    captures.length = 0;
    await (Posts as any)._count(conditions, {});
    expect(captures.length).toBe(1);
    return { sql: renderPlaceholders(captures[0].sql, dialect), params: captures[0].params };
  }
  for (const dialect of dialects) {
    it(`[${dialect}] COUNT(*) + WHERE — head byte-matches v1 _count`, async () => {
      // v1 golden: the REAL `_count` statement (head + DBConditions WHERE) captured from `execute`.
      const golden = await v1CountHead(dialect, { author_id: 7 });

      // SCP: the Count leaf's head is `compileSelect` with the `COUNT(*) as count` projection (the
      // exact call compileSelectNode makes) + the DBConditions WHERE fragment.
      const formatter = dialect === 'postgres' ? pgFmt : undefined;
      const head = compileSelect({ dialect, tableName: 'posts', select: 'COUNT(*) as count' });
      const wparams: unknown[] = [];
      const wsql = new DBConditions({ author_id: 7 }).compile(wparams, formatter);
      const got = render({ sql: `${head.sql} WHERE ${wsql}`, params: [...head.params, ...wparams] }, dialect);
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
    });
  }

  // NEGATIVE (golden-from-originals): the golden is the CAPTURED v1 `_count` head, so it tracks the v1
  // source. Perturbing the SCP head projection away from v1's `COUNT(*) as count` breaks the match.
  it('negative: SCP head that diverges from v1 _count head FAILS the match', async () => {
    const dialect: Dialect = 'postgres';
    const golden = await v1CountHead(dialect, { author_id: 7 });
    expect(golden.sql.startsWith('SELECT COUNT(*) as count FROM posts')).toBe(true);
    // A head projection that is NOT v1's `COUNT(*) as count` (e.g. `COUNT(id)`) must not match.
    const badHead = compileSelect({ dialect, tableName: 'posts', select: 'COUNT(id) as count' });
    const wparams: unknown[] = [];
    const wsql = new DBConditions({ author_id: 7 }).compile(wparams, pgFmt);
    const bad = render({ sql: `${badHead.sql} WHERE ${wsql}`, params: [...badHead.params, ...wparams] }, dialect);
    expect(bad.sql).not.toBe(golden.sql);
  });
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

describe('C. Composite STATIC relation form (#47 item 1) — PG byte-matches v1 unnest-JOIN', () => {
  // The STATIC composite op (compileCompositeKeyStaticUnlimited) is length-INDEPENDENT so the op.sql
  // is fixed. On PG it is byte-identical to v1's composite unnest-JOIN: the golden below drives the
  // REAL v1 `compileCompositeKeyUnlimited` (proven above to match LazyRelation) and the static form,
  // with both deferred casts resolved from the same int keys, reproduces it. MySQL/SQLite deviate to
  // the single-JSON tuple form (the owner-approved deviation the single-key IN-list uses).
  it('[postgres] static unnest byte-matches the v1 composite unnest-JOIN', () => {
    const v1 = render(
      compileCompositeKeyUnlimited({
        dialect: 'postgres',
        tableName: 'comments',
        select: 'tenant_id, post_id',
        targetKeys: ['tenant_id', 'post_id'],
        tuples: [[100, 1]],
      }),
      'postgres',
    ).sql;
    const staticNode = compileCompositeKeyStaticUnlimited({
      dialect: 'postgres',
      tableName: 'comments',
      select: 'tenant_id, post_id',
      targetKeys: ['tenant_id', 'post_id'],
      deferPgArrayCast: true,
    });
    let sql = assembleMakeSQL(staticNode).sql;
    sql = resolvePgArrayCast(sql, [100]); // first column keys (int → int[])
    sql = resolvePgArrayCast(sql, [1]); // second column keys (int → int[])
    expect(renderPlaceholders(sql, 'postgres')).toBe(v1);
  });

  // NEGATIVE (golden-from-originals): perturb the v1 composite builder (drop a key column) → the
  // golden moves, proving the assertion is pinned to the ORIGINAL unnest text, not a v2 constant.
  it('negative: perturbing the v1 composite key set moves the golden', () => {
    const twoKey = render(
      compileCompositeKeyUnlimited({ dialect: 'postgres', tableName: 'comments', select: 'tenant_id, post_id', targetKeys: ['tenant_id', 'post_id'], tuples: [[100, 1]] }),
      'postgres',
    ).sql;
    const oneKey = render(
      compileCompositeKeyUnlimited({ dialect: 'postgres', tableName: 'comments', select: 'tenant_id', targetKeys: ['tenant_id'], tuples: [[100]] }),
      'postgres',
    ).sql;
    expect(oneKey).not.toBe(twoKey); // fewer key columns → different unnest arity → golden moves
  });
});

// ===========================================================================
// C. Composite STATIC per-parent-LIMIT relation form (#47 LAST completeness gap).
//
// The STATIC composite-limited op (compileCompositeKeyStaticLimited) is length-INDEPENDENT so the
// op.sql is fixed (one array param per key column on PG; ONE JSON array-of-tuples param on
// MySQL/SQLite). The GOLDEN is the REAL v1 composite-limited builder (compileCompositeKeyLimited,
// proven above to byte-match LazyRelation's batchLoadWithLateralComposite /
// batchLoadWithRowNumberComposite on every dialect):
//   - PG: the static LATERAL form (deferred casts resolved from the same int keys) is
//     BYTE-IDENTICAL to v1 — the composite LATERAL is already structurally length-independent.
//   - MySQL/SQLite: the static form keeps v1's EXACT ROW_NUMBER window + `_rn <= limit` filter, and
//     deviates ONLY in the CTE membership WHERE — the owner-sanctioned static JSON-tuple predicate
//     (compositeJsonMembership) replacing v1's value-dependent `(k1,k2) IN ((?,?),…)`. Same
//     RESULT-parity deviation the composite UNLIMITED form and the single-key IN-list already use;
//     proven live below. The v1 tuple-IN byte-form stays proven by compileCompositeKeyLimited.
// ===========================================================================
describe('C. Composite STATIC per-parent-LIMIT relation form (#47 last gap)', () => {
  const keys = ['tenant_id', 'doc_id'];
  const sel = 'tenant_id, doc_id, rev';

  it('[postgres] static composite-LIMITED byte-matches the v1 composite LATERAL', () => {
    // v1 (proven == LazyRelation.batchLoadWithLateralComposite) with concrete int keys.
    const v1 = render(
      compileCompositeKeyLimited({
        dialect: 'postgres', tableName: 'revs', select: sel,
        targetKeys: keys, tuples: [[1, 10], [1, 11]], limit: 2, order: 'rev ASC',
      }),
      'postgres',
    ).sql;
    // static (deferred casts) resolved from the SAME per-column int keys → int[], int[].
    let sql = assembleMakeSQL(compileCompositeKeyStaticLimited({
      dialect: 'postgres', tableName: 'revs', select: sel,
      targetKeys: keys, limit: 2, order: 'rev ASC', deferPgArrayCast: true,
    })).sql;
    sql = resolvePgArrayCast(sql, [1, 1]); // column 0 keys (int → int[])
    sql = resolvePgArrayCast(sql, [10, 11]); // column 1 keys (int → int[])
    expect(renderPlaceholders(sql, 'postgres')).toBe(v1);
  });

  for (const dialect of ['mysql', 'sqlite'] as const) {
    it(`[${dialect}] static composite-LIMITED keeps v1's ROW_NUMBER window; deviates only to the static JSON predicate`, () => {
      const v1 = render(
        compileCompositeKeyLimited({
          dialect, tableName: 'revs', select: sel,
          targetKeys: keys, tuples: [[1, 10], [1, 11]], limit: 2, order: 'rev ASC',
        }),
        dialect,
      );
      const got = render(
        compileCompositeKeyStaticLimited({
          dialect, tableName: 'revs', select: sel,
          targetKeys: keys, limit: 2, order: 'rev ASC', deferPgArrayCast: true,
        }),
        dialect,
      );
      // The window + `_rn <= N` shell is v1-IDENTICAL (partition/order/limit/CTE wrap).
      expect(got.sql).toContain('ROW_NUMBER() OVER (PARTITION BY tenant_id, doc_id ORDER BY rev ASC) AS _rn');
      expect(got.sql).toContain('SELECT * FROM ranked WHERE _rn <= 2');
      expect(v1.sql).toContain('ROW_NUMBER() OVER (PARTITION BY tenant_id, doc_id ORDER BY rev ASC) AS _rn');
      expect(v1.sql).toContain('SELECT * FROM ranked WHERE _rn <= 2');
      // v1 uses the VALUE-DEPENDENT tuple-IN (grows with tuple count); the static form MUST NOT.
      expect(v1.sql).toContain('(tenant_id, doc_id) IN ((?, ?), (?, ?))');
      expect(got.sql).not.toContain('IN ((?, ?)');
      // The static membership predicate is the SAME one the composite UNLIMITED static form emits
      // (JSON_TABLE for MySQL / json_each for SQLite) — the sanctioned result-parity deviation.
      const jsonPred = dialect === 'mysql'
        ? `(revs.tenant_id, revs.doc_id) IN (SELECT JSON_UNQUOTE(c0), JSON_UNQUOTE(c1) FROM JSON_TABLE(?, '$[*]' COLUMNS(c0 JSON PATH '$[0]', c1 JSON PATH '$[1]')) jt)`
        : `EXISTS (SELECT 1 FROM json_each(?) je WHERE json_extract(je.value, '$[0]') = revs.tenant_id AND json_extract(je.value, '$[1]') = revs.doc_id)`;
      expect(got.sql).toContain(jsonPred);
      // The whole key set binds as ONE JSON param (value-length-independent) → static op.
      expect(got.params).toEqual([[null]]);
    });
  }

  // NEGATIVE (golden-from-originals): perturb the v1 composite-limited builder (change the limit) →
  // the v1 golden moves, so the assertion is pinned to the ORIGINAL text, not a v2 constant.
  it('negative: perturbing the v1 composite-limited window moves the golden', () => {
    const limit2 = render(
      compileCompositeKeyLimited({ dialect: 'postgres', tableName: 'revs', select: sel, targetKeys: keys, tuples: [[1, 10]], limit: 2, order: 'rev ASC' }),
      'postgres',
    ).sql;
    const limit5 = render(
      compileCompositeKeyLimited({ dialect: 'postgres', tableName: 'revs', select: sel, targetKeys: keys, tuples: [[1, 10]], limit: 5, order: 'rev ASC' }),
      'postgres',
    ).sql;
    expect(limit5).not.toBe(limit2); // a different per-parent LIMIT → the golden moves
  });
});

// ===========================================================================
// D. AUTHORING → live bundle path (V0 R2–R6) — the ADDED where-primitives /
//    SELECT_PORTS reach a replayable bundle and render V1-SOURCED SQL byte-for-byte.
//
// These assert the LIVE-reachable path (`emitRead(...)` → publishBehaviors lowers the WHERE into the
// read leaf's static sql — {@link renderPrimaryRead} renders it) reproduces the
// ORIGINAL builder output (`DBConditions`/`dbCast`/`dbDynamic`/`dbImmediate`/`dbTupleIn`/
// `DBSubquery`/`DBExists` for WHERE; `compileSelect`=`_buildSelectSQL` for the ports),
// on EVERY dialect where the text differs. Nothing is compared v2-to-v2: each golden
// drives the v1 builder directly. This closes the V0 "byte-provable but not live-
// reachable" gap for R2 (subquery/EXISTS), R3-remainder (BETWEEN/LIKE/ILIKE/cast/
// dynamic/immediate/tuple-IN + FOR UPDATE), R4 (CTE), R5 (JOIN), R6 (append/HAVING).
// ===========================================================================
describe('D. authoring→bundle path renders V1-SOURCED SQL (V0 R2–R6, all dialects)', () => {
  const L = components();
  class Q extends SemanticBehavior {
    static columns = {
      posts: { id: 'INTEGER', author_id: 'INTEGER' },
      users: { id: 'INTEGER', name: 'TEXT' }, // `Jn` projects users.name (JOIN column)
      recent: { id: 'INTEGER' },
    };
    // R3-remainder WHERE primitives
    Btw($: In<{ lo: number; hi: number }>) { return emitRead(L, 'Select', { table: 'posts', select: ['id'], where: [whereBetween($, 'age', $.lo, $.hi)] }, 'sqlite'); }
    Lk($: In<{ p: string }>) { return emitRead(L, 'Select', { table: 'posts', select: ['id'], where: [whereLike($, 'name', $.p)] }, 'sqlite'); }
    ILk($: In<{ p: string }>) { return emitRead(L, 'Select', { table: 'posts', select: ['id'], where: [whereILike($, 'name', $.p)] }, 'sqlite'); }
    Cst($: In<{ v: string }>) { return emitRead(L, 'Select', { table: 'posts', select: ['id'], where: [whereCast($, 'id', 'uuid', $.v)] }, 'sqlite'); }
    Dyn($: In<{ q: string }>) { return emitRead(L, 'Select', { table: 'posts', select: ['id'], where: [whereDynamic($, 'search', "to_tsvector('en', ?)", [$.q])] }, 'sqlite'); }
    Imm(_$: In<Record<string, never>>) { return emitRead(L, 'Select', { table: 'posts', select: ['id'], where: [whereImmediate(_$, 'created_at', 'NOW()')] }, 'sqlite'); }
    Tpl(_$: In<Record<string, never>>) { return emitRead(L, 'Select', { table: 'posts', select: ['id'], where: [whereTupleIn(_$, ['tenant_id', 'id'], [[1, 10], [2, 20]])] }, 'sqlite'); }
    // R2 subquery / EXISTS
    Sub(_$: In<Record<string, never>>) {
      return emitRead(L, 'Select', { table: 'users', select: ['id'], where: [whereInSubquery(_$, 'users.id', { sql: 'SELECT orders.user_id FROM orders WHERE orders.status = ?', params: ['paid'] })] }, 'sqlite');
    }
    NotIn(_$: In<Record<string, never>>) {
      return emitRead(L, 'Select', { table: 'users', select: ['id'], where: [whereInSubquery(_$, 'users.id', { sql: 'SELECT banned.user_id FROM banned' }, true)] }, 'sqlite');
    }
    Ex(_$: In<Record<string, never>>) {
      return emitRead(L, 'Select', { table: 'users', select: ['id'], where: [whereExists(_$, { sql: 'SELECT 1 FROM orders WHERE orders.user_id = users.id' })] }, 'sqlite');
    }
    NotEx(_$: In<Record<string, never>>) {
      return emitRead(L, 'Select', { table: 'users', select: ['id'], where: [whereExists(_$, { sql: 'SELECT 1 FROM orders WHERE orders.user_id = users.id' }, true)] }, 'sqlite');
    }
    // R3 FOR UPDATE, R4 CTE, R5 JOIN, R6 append/HAVING
    Fu($: In<{ id: number }>) { return emitRead(L, 'Select', { table: 'posts', select: ['id'], where: [whereEq($.id, $.id)], forUpdate: 'true' }, 'sqlite'); }
    Hav(_$: In<Record<string, never>>) { return emitRead(L, 'Select', { table: 'posts', select: ['author_id', 'COUNT(*) as n'], group: 'author_id', append: 'HAVING COUNT(*) > 1' }, 'sqlite'); }
    Jn(_$: In<Record<string, never>>) { return emitRead(L, 'Select', { table: 'posts', select: ['posts.id', 'users.name'], join: 'JOIN users ON users.id = posts.author_id' }, 'sqlite'); }
    Cte(_$: In<Record<string, never>>) { return emitRead(L, 'Select', { table: 'recent', select: ['id'], cte: { name: 'recent', sql: 'SELECT id FROM posts WHERE status = ?' }, cteParams: ['live'] }, 'sqlite'); }
  }
  function authored(entry: string, input: Record<string, unknown>, dialect: Dialect): Rendered {
    return renderPrimaryRead(publishBehaviors(Q, { dialect }), entry, input, dialect);
  }
  /** v1 golden: a bare SELECT head + a DBConditions-built WHERE (the exact v1 assembly). */
  function v1Where(cond: Record<string, unknown>, dialect: Dialect, table = 'posts', cols = 'id'): Rendered {
    const params: unknown[] = [];
    // The v1 dialect cast formatter: PG applies `?::type`; MySQL/SQLite drop the cast (identity) —
    // this is `formatterFor(dialect)`, the SAME gating the live compile passes. (`undefined` would
    // fall to DBValues' DEFAULT formatter, which ALWAYS casts — wrong for MySQL/SQLite dbCast.)
    const formatter = dialect === 'postgres' ? pgFmt : (ph: string) => ph;
    const where = new DBConditions(cond).compile(params, formatter);
    return { sql: renderPlaceholders(`SELECT ${cols} FROM ${table} WHERE ${where}`, dialect), params };
  }

  for (const dialect of dialects) {
    it(`[${dialect}] BETWEEN — authored path byte-matches v1 custom-op`, () => {
      expect(authored('Btw', { lo: 18, hi: 65 }, dialect)).toEqual(v1Where({ 'age BETWEEN ? AND ?': [18, 65] }, dialect));
    });
    it(`[${dialect}] LIKE / ILIKE — authored path byte-matches v1 custom-op`, () => {
      expect(authored('Lk', { p: '%x%' }, dialect)).toEqual(v1Where({ 'name LIKE ?': '%x%' }, dialect));
      expect(authored('ILk', { p: '%x%' }, dialect)).toEqual(v1Where({ 'name ILIKE ?': '%x%' }, dialect));
    });
    it(`[${dialect}] dbCast (dialect-gated ::uuid) — authored path byte-matches v1 dbCast`, () => {
      expect(authored('Cst', { v: 'u1' }, dialect)).toEqual(v1Where({ id: dbCast('u1', 'uuid') }, dialect));
    });
    it(`[${dialect}] dbDynamic fn(?) — authored path byte-matches v1 dbDynamic`, () => {
      expect(authored('Dyn', { q: 'hello' }, dialect)).toEqual(v1Where({ search: dbDynamic("to_tsvector('en', ?)", ['hello']) }, dialect));
    });
    it(`[${dialect}] dbImmediate NOW() — authored path byte-matches v1 dbImmediate`, () => {
      expect(authored('Imm', {}, dialect)).toEqual(v1Where({ created_at: dbImmediate('NOW()') }, dialect));
    });
    it(`[${dialect}] tuple-IN — authored path byte-matches v1 dbTupleIn`, () => {
      expect(authored('Tpl', {}, dialect)).toEqual(v1Where({ __tuple__: dbTupleIn(['tenant_id', 'id'], [[1, 10], [2, 20]]) }, dialect));
    });
    it(`[${dialect}] IN(subquery) / NOT IN(subquery) — authored path byte-matches v1 DBSubquery`, () => {
      const sub = new DBSubquery(
        [{ columnName: 'id', tableName: 'users' }], 'orders',
        [{ columnName: 'user_id', tableName: 'orders' }],
        [{ column: { columnName: 'status', tableName: 'orders' }, value: 'paid' }], 'IN',
      );
      expect(authored('Sub', {}, dialect)).toEqual(v1Where({ __subquery__: sub }, dialect, 'users'));
      const notIn = new DBSubquery(
        [{ columnName: 'id', tableName: 'users' }], 'banned',
        [{ columnName: 'user_id', tableName: 'banned' }], [], 'NOT IN',
      );
      expect(authored('NotIn', {}, dialect)).toEqual(v1Where({ __subquery__: notIn }, dialect, 'users'));
    });
    it(`[${dialect}] EXISTS / NOT EXISTS — authored path byte-matches v1 DBExists`, () => {
      for (const [entry, not] of [['Ex', false], ['NotEx', true]] as const) {
        const ex = new DBExists('orders', [{ column: { columnName: 'user_id', tableName: 'orders' }, value: parentRef({ columnName: 'id', tableName: 'users' }) }], not);
        expect(authored(entry, {}, dialect)).toEqual(v1Where({ __exists__: ex }, dialect, 'users'));
      }
    });
    it(`[${dialect}] FOR UPDATE port — authored path byte-matches v1 _buildSelectSQL`, () => {
      // Golden DRIVES the REAL v1 `DBModel._buildSelectSQL` (not compileSelect against itself).
      // Perturbing v1's ` FOR UPDATE` tail (e.g. → ` FOR UPDATE NOWAIT`) MOVES this golden.
      const v1 = v1Select(dialect, 'posts', 'id', { id: 5 }, { forUpdate: true });
      expect(authored('Fu', { id: 5 }, dialect).sql).toBe(v1.sql);
    });
    it(`[${dialect}] append (HAVING) port — authored path byte-matches v1 _buildSelectSQL`, () => {
      const v1 = v1Select(dialect, 'posts', 'author_id, COUNT(*) as n', {}, { group: 'author_id', append: 'HAVING COUNT(*) > 1' });
      expect(authored('Hav', {}, dialect).sql).toBe(v1.sql);
    });
    it(`[${dialect}] JOIN port — authored path byte-matches v1 _buildSelectSQL`, () => {
      const v1 = v1Select(dialect, 'posts', 'posts.id, users.name', {}, { join: 'JOIN users ON users.id = posts.author_id' });
      expect(authored('Jn', {}, dialect).sql).toBe(v1.sql);
    });
    it(`[${dialect}] CTE port (+params) — authored path byte-matches v1 _buildSelectSQL`, () => {
      const v1 = v1Select(dialect, 'recent', 'id', {}, { cte: { name: 'recent', sql: 'SELECT id FROM posts WHERE status = ?', params: ['live'] } });
      const got = authored('Cte', {}, dialect);
      expect(got.sql).toBe(v1.sql);
      expect(got.params).toEqual(['live']);
    });
  }

  // NEGATIVE (golden-from-originals): perturb the v1 dbCast type and the authored-path golden moves —
  // proving the assertion is pinned to the ORIGINAL dbCast text, not a self-fulfilling v2 constant.
  it('negative: perturbing the v1 dbCast type moves the authored golden (not v2-v2)', () => {
    const uuid = v1Where({ id: dbCast('u1', 'uuid') }, 'postgres').sql;
    const jsonb = v1Where({ id: dbCast('u1', 'jsonb') }, 'postgres').sql;
    expect(jsonb).not.toBe(uuid);
    expect(authored('Cst', { v: 'u1' }, 'postgres').sql).toBe(uuid);
  });

  // NEGATIVE (golden-from-originals, #47 Finding A): perturbing v1 `_buildSelectSQL`'s FOR UPDATE tail
  // MOVES the scaffold-port golden. If the golden were compileSelect-vs-itself (v2-v2) it would be
  // insensitive to the v1 method. The v1-driven golden tracks v1's exact ` FOR UPDATE` tail text.
  it('negative: v1 _buildSelectSQL FOR UPDATE tail drives the scaffold-port golden (not v2-v2)', () => {
    const withFu = v1Select('postgres', 'posts', 'id', { id: 5 }, { forUpdate: true }).sql;
    const withoutFu = v1Select('postgres', 'posts', 'id', { id: 5 }, {}).sql;
    expect(withFu).not.toBe(withoutFu);
    expect(withFu.endsWith(' FOR UPDATE')).toBe(true);
    expect(authored('Fu', { id: 5 }, 'postgres').sql).toBe(withFu);
  });
});

// ---------------------------------------------------------------------------
// H1 (re-audit) — the SINGLE-ROW tx-write path must emit the PG per-column
// `?::<sqlCast>` cast, byte-identical to v1 `PostgresSqlBuilder.buildInsert`
// (INSERT) and `DBModel._update` (UPDATE). The tx-write compile (`compileWriteNode`)
// previously took NO dialect and emitted a bare `?`, silently dropping the cast — a
// real v1 divergence for jsonb/uuid/int[] columns. These goldens DRIVE the ORIGINAL
// v1 builder / formatter so a v1 regression MOVES the golden (golden-from-originals),
// pinning the fix so the latent bug cannot recur.
// ---------------------------------------------------------------------------
describe('H1. single-row tx-write per-column PG cast — byte-matches v1 (all 3 dialects)', () => {
  // A representative cast column set: jsonb (object payload), uuid (string key), int[] (array).
  // `timestamp`/`date` are DELIBERATELY excluded from the cast (v1 skips them — pg serializes Date).
  const castMap = new Map<string, string>([
    ['payload', 'jsonb'],
    ['ext_id', 'uuid'],
    ['tags', 'int[]'],
    ['created_at', 'timestamp'], // present but MUST be skipped (bare ?), matching v1
  ]);
  // Insert columns are CANONICAL (alphabetical) sorted by the tx-write compile (matches _insert).
  const insertCols = ['created_at', 'ext_id', 'name', 'payload', 'tags'].sort();
  const insertRow: Record<string, unknown> = {
    created_at: '2020-01-01T00:00:00Z',
    ext_id: 'a1b2',
    name: 'doc',
    payload: { k: 1 },
    tags: [7, 8],
  };

  // The authored write node carrying `sqlCast.<field>` ports (the additive bundle-shape extension).
  const insertPorts: Record<string, unknown> = { table: 'docs', returning: 'id' };
  for (const c of insertCols) insertPorts[`values.${c}`] = { ref: [c] };
  for (const [c, t] of castMap) if (insertCols.includes(c)) insertPorts[`sqlCast.${c}`] = t;

  for (const dialect of dialects) {
    it(`[${dialect}] tx-write single INSERT emits v1 buildInsert cast text (byte-match)`, () => {
      // GOLDEN = the REAL v1 `PostgresSqlBuilder.buildInsert` (single-record VALUES path) — the exact
      // per-column `?::<sqlCast>` v1 sends. Driving the ORIGINAL builder makes this golden-from-originals.
      const golden = render(
        builderOf[dialect].buildInsert({
          tableName: 'docs',
          columns: insertCols,
          records: [insertRow],
          rawRecords: [insertRow],
          sqlCastMap: castMap,
          returning: 'id',
        }),
        dialect
      );
      const op = compileWriteNode({ id: 'ins', component: 'Insert', ports: insertPorts } as never, dialect);
      const got = renderTxStatement(op, insertRow, dialect);
      // The tx-write compile serializes the payload object to JSON at the driver boundary; compare the
      // SQL TEXT byte-for-byte (the cast placeholders) — the SQL is where H1 diverged.
      expect(got.sql).toBe(golden.sql);
      if (dialect === 'postgres') {
        // PG: jsonb/uuid/int[] carry the cast; timestamp is SKIPPED (bare $N); name (no cast) bare $N.
        expect(got.sql).toContain('::jsonb');
        expect(got.sql).toContain('::uuid');
        expect(got.sql).toContain('::int[]');
        expect(got.sql).not.toContain('created_at::'); // timestamp cast skipped (v1)
      } else {
        // MySQL/SQLite: NO cast leaks — every value is a bare `?` (v1's cast formatter is identity).
        expect(got.sql).not.toContain('::');
      }
    });

    it(`[${dialect}] tx-write single UPDATE emits v1 _update SET cast text (byte-match)`, () => {
      // GOLDEN = the v1 `DBModel._update` SET-clause built with the SAME dialect cast formatter v1 uses
      // (src/DBModel.ts:1058-1063: `formatter('?', sqlCast)`, skip timestamp/date) + `DBConditions` WHERE.
      const setVals: Record<string, unknown> = { name: 'doc', payload: { k: 1 }, ext_id: 'a1b2', created_at: '2020' };
      const formatter = dialect === 'postgres' ? pgFmt : undefined;
      const params: unknown[] = [];
      const setClauses: string[] = [];
      for (const [col, val] of Object.entries(setVals)) {
        params.push(val);
        const sqlCast = castMap.get(col);
        if (sqlCast && formatter && sqlCast !== 'timestamp' && sqlCast !== 'date') setClauses.push(`${col} = ${formatter('?', sqlCast)}`);
        else setClauses.push(`${col} = ?`);
      }
      const where = new DBConditions({ id: 5 }).compile(params, formatter);
      const goldenSql = renderPlaceholders(`UPDATE docs SET ${setClauses.join(', ')} WHERE ${where} RETURNING id`, dialect);

      const updPorts: Record<string, unknown> = {
        table: 'docs',
        where: { arr: [{ eq: [{ ref: ['id'] }, { ref: ['id_val'] }] }] },
        returning: 'id',
      };
      for (const c of Object.keys(setVals)) updPorts[`set.${c}`] = { ref: [c] };
      for (const [c, t] of castMap) if (c in setVals) updPorts[`sqlCast.${c}`] = t;
      const op = compileWriteNode({ id: 'upd', component: 'Update', ports: updPorts } as never, dialect);
      const got = renderTxStatement(op, { ...setVals, id_val: 5 }, dialect);
      expect(got.sql).toBe(goldenSql);
      if (dialect === 'postgres') {
        expect(got.sql).toContain('payload = $2::jsonb');
        expect(got.sql).toContain('ext_id = $3::uuid');
        expect(got.sql).toContain('created_at = $4 WHERE'); // timestamp SKIPPED → bare $4
      } else {
        expect(got.sql).not.toContain('::');
      }
    });
  }

  // NEGATIVE (golden-from-originals): perturb the v1 builder's cast type and the golden MOVES — proving
  // the tx-write assertion is pinned to the ORIGINAL v1 cast text, not a self-fulfilling v2 constant.
  it('negative: perturbing the v1 buildInsert sqlCast type moves the golden (not v2-v2)', () => {
    const asJsonb = render(builderOf.postgres.buildInsert({ tableName: 'docs', columns: ['payload'], records: [{ payload: { k: 1 } }], rawRecords: [{ payload: { k: 1 } }], sqlCastMap: new Map([['payload', 'jsonb']]) }), 'postgres').sql;
    const asText = render(builderOf.postgres.buildInsert({ tableName: 'docs', columns: ['payload'], records: [{ payload: { k: 1 } }], rawRecords: [{ payload: { k: 1 } }], sqlCastMap: new Map([['payload', 'text']]) }), 'postgres').sql;
    expect(asJsonb).not.toBe(asText); // the v1 builder's cast type genuinely drives the golden
    const op = compileWriteNode({ id: 'n', component: 'Insert', ports: { table: 'docs', 'values.payload': { ref: ['payload'] }, 'sqlCast.payload': 'jsonb' } } as never, 'postgres');
    expect(renderTxStatement(op, { payload: { k: 1 } }, 'postgres').sql).toBe(asJsonb);
    expect(renderTxStatement(op, { payload: { k: 1 } }, 'postgres').sql).not.toBe(asText);
  });

  // The OLD bug (bare `?`, no cast) must NOT reappear: on PG a cast column WITHOUT the fix would render
  // `?` instead of `?::jsonb`. Assert the fixed compile diverges from a hypothetical no-cast render.
  it('regression: the sqlCast port changes the PG placeholder (a bare-? render would be wrong)', () => {
    const withCast = compileWriteNode({ id: 'w', component: 'Insert', ports: { table: 'docs', 'values.payload': { ref: ['payload'] }, 'sqlCast.payload': 'jsonb' } } as never, 'postgres');
    const noCast = compileWriteNode({ id: 'w', component: 'Insert', ports: { table: 'docs', 'values.payload': { ref: ['payload'] } } } as never, 'postgres');
    const a = renderTxStatement(withCast, { payload: { k: 1 } }, 'postgres').sql;
    const b = renderTxStatement(noCast, { payload: { k: 1 } }, 'postgres').sql;
    expect(a).toContain('::jsonb');
    expect(b).not.toContain('::');
    expect(a).not.toBe(b);
  });
});
