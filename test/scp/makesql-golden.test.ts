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

  for (const dialect of dialects) {
    for (const [name, cond] of constructs) {
      it(`[${dialect}] ${name}`, () => {
        const params: unknown[] = [];
        const formatter = dialect === 'postgres' ? pgFmt : (ph: string) => ph;
        const goldenSql = new DBConditions(cond).compile(params, formatter);
        const golden: Rendered = {
          sql: renderPlaceholders(goldenSql, dialect),
          params,
        };
        const got = render(compileWhere(cond, dialect), dialect);
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

describe('B. INSERT single & batch — makeSQL byte-matches REAL DBModel._insert (captured)', () => {
  for (const dialect of dialects) {
    it(`[${dialect}] single INSERT`, async () => {
      const records = [{ id: 1, name: 'a' }];
      const golden = await captureInsert(dialect, records, { returning: 'id' });
      const got = renderComponents(compileInsertMany(dialect, { tableName: 'users', records, rawRecords: records, returning: 'id' }), dialect);
      expect(got.length).toBe(golden.length);
      expect(got).toEqual(golden);
    });

    it(`[${dialect}] homogeneous batch INSERT (single grouped statement)`, async () => {
      const records = [{ id: 1, name: 'a' }, { id: 2, name: 'b' }];
      const golden = await captureInsert(dialect, records, { returning: 'id' });
      const got = renderComponents(compileInsertMany(dialect, { tableName: 'users', records, rawRecords: records, returning: 'id' }), dialect);
      // Homogeneous → exactly ONE INSERT component.
      expect(golden.length).toBe(1);
      expect(got.length).toBe(1);
      expect(got).toEqual(golden);
    });

    it(`[${dialect}] upsert: createMany + ON CONFLICT DO UPDATE (all)`, async () => {
      const records = [{ id: 1, name: 'a' }, { id: 2, name: 'b' }];
      const opts = { onConflict: ['id'], onConflictUpdate: 'all', returning: 'id' };
      const golden = await captureInsert(dialect, records, opts);
      const got = renderComponents(
        compileInsertMany(dialect, { tableName: 'users', records, rawRecords: records, onConflict: ['id'], onConflictUpdate: 'all', returning: 'id' }),
        dialect
      );
      expect(got.length).toBe(golden.length);
      expect(got).toEqual(golden);
    });

    it(`[${dialect}] HETEROGENEOUS createMany → MULTIPLE grouped INSERT components (byte-match real _insert)`, async () => {
      // Rows with DIFFERENT column subsets: {id,name}, {id,name,age}, {id,name}.
      // Production `_insert` groups by sorted-column-set pattern and emits ONE INSERT
      // per group → 2 statements ({id,name} batch, then {age,id,name} single).
      const records = [
        { id: 1, name: 'a' },
        { id: 2, name: 'b', age: 20 },
        { id: 3, name: 'c' },
      ];
      const golden = await captureInsert(dialect, records, { returning: 'id' });
      const components = compileInsertMany(dialect, { tableName: 'users', records, rawRecords: records, returning: 'id' });
      const got = renderComponents(components, dialect);

      // The gap this fixes: a heterogeneous createMany is a COMPOSITION of >1 makeSQL
      // INSERT components — NOT one statement. Production `_insert` executes each group
      // as its OWN statement (its own `execute` call, its own placeholder numbering), so
      // the composition here is the ORDERED LIST of components, each rendered on its own.
      expect(golden.length).toBe(2);
      expect(components.length).toBe(2);
      // Per-statement byte-match: same grouping, same SQL, same params/order.
      expect(got).toEqual(golden);

      // Grouping is exactly _insert's: first-seen {id,name} batch (UNNEST on PG /
      // multi-VALUES on MySQL·SQLite), then the {age,id,name} single-row group.
      expect(components[0].sql).toContain('(id, name)');
      expect(components[1].sql).toContain('(age, id, name)');

      // Each component individually assembles to the exact captured statement (the
      // makeSQL assembly core, independent of the dialect placeholder pass).
      components.forEach((c, i) => {
        const asm = assembleMakeSQL(c);
        expect(asm.params).toEqual(golden[i].params);
      });
    });

    it(`[${dialect}] HETEROGENEOUS via DEFAULT/undefined omission → split by column-presence (byte-match real _insert)`, async () => {
      // A column applied by DB DEFAULT is expressed by OMITTING it from the row's
      // column set (never a `DEFAULT` literal — PG UNNEST binds arrays and cannot carry
      // DEFAULT; see `DBModel._insert:929` "batch INSERT without DEFAULT keyword").
      // `_insert` drops a column when its value is `undefined` OR a
      // `DBImmediateValue('DEFAULT')` (`:943` single / `:961` batch), so these rows fall
      // into a DIFFERENT column-set pattern → a separate grouped INSERT (a separate
      // UNNEST on PG). No `DEFAULT` text appears anywhere.
      const records = [
        { id: 1, name: 'a', age: 10 },
        { id: 2, name: 'b', age: new DBImmediateValue('DEFAULT') },
        { id: 3, name: 'c', age: undefined },
        { id: 4, name: 'd', age: 40 },
      ];
      const golden = await captureInsert(dialect, records, { returning: 'id' });
      const components = compileInsertMany(dialect, { tableName: 'users', records, rawRecords: records, returning: 'id' });
      const got = renderComponents(components, dialect);

      // Two groups: first-seen {age,id,name} (rows 1,4) then {id,name} (rows 2,3 —
      // `age` omitted). MULTIPLE INSERT components, split by column presence.
      expect(golden.length).toBe(2);
      expect(components.length).toBe(2);
      expect(got).toEqual(golden);

      // No DEFAULT literal is ever emitted; the `age` column simply disappears from
      // group 2 (both its text and its UNNEST/VALUES slot).
      expect(components[0].sql).toContain('(age, id, name)');
      expect(components[1].sql).toContain('(id, name)');
      expect(components[1].sql).not.toContain('age');
      expect(got.map((g) => g.sql).join('')).not.toContain('DEFAULT');
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
  for (const dialect of dialects) {
    it(`[${dialect}] batch UPDATE + SKIP`, () => {
      const golden = builderOf[dialect].buildUpdateMany(opts as any);
      const got = render(compileUpdateMany(dialect, opts as any), dialect);
      expect(got.sql).toBe(renderPlaceholders(golden.sql, dialect));
      expect(got.params).toEqual(golden.params);
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
    it(`[${dialect}] single DELETE`, () => {
      const conditions = { id: [1, 2, 3] };
      const params: unknown[] = [];
      const formatter = dialect === 'postgres' ? pgFmt : undefined;
      const where = new DBConditions(conditions).compile(params, formatter);
      const goldenSql = `DELETE FROM users WHERE ${where}`;
      const golden = { sql: renderPlaceholders(goldenSql, dialect), params };
      const got = render(compileDelete({ dialect, tableName: 'users', conditions }), dialect);
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
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
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
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
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
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
      expect(got.sql).toBe(golden.sql);
      expect(got.params).toEqual(golden.params);
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
