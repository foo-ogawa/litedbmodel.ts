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
import { dbNotNull, dbCast, dbCastIn, dbTupleIn, dbImmediate, dbDynamic, dbRaw, DBExists, DBSubquery, parentRef } from '../../src/DBValues';
import { postgresSqlBuilder } from '../../src/drivers/PostgresSqlBuilder';
import { mysqlSqlBuilder } from '../../src/drivers/MysqlSqlBuilder';
import { sqliteSqlBuilder } from '../../src/drivers/SqliteSqlBuilder';
import type { SqlBuilder } from '../../src/drivers/types';
import {
  assembleMakeSQL,
  renderPlaceholders,
  compileWhere,
  compileSelect,
  compileInsert,
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
describe('B. INSERT single & batch — makeSQL byte-matches dialect builders', () => {
  const single = {
    tableName: 'users',
    columns: ['id', 'name'],
    records: [{ id: 1, name: 'a' }],
    rawRecords: [{ id: 1, name: 'a' }],
    sqlCastMap: new Map([['id', 'uuid']]),
    returning: 'id',
  };
  const batch = {
    tableName: 'users',
    columns: ['id', 'name'],
    records: [{ id: 1, name: 'a' }, { id: 2, name: 'b' }],
    rawRecords: [{ id: 1, name: 'a' }, { id: 2, name: 'b' }],
    onConflict: ['id'],
    onConflictUpdate: 'all' as const,
    returning: 'id',
  };
  for (const dialect of dialects) {
    it(`[${dialect}] single INSERT (+per-col cast on PG)`, () => {
      const golden = builderOf[dialect].buildInsert(single as any);
      const got = render(compileInsert(dialect, single as any), dialect);
      expect(got.sql).toBe(renderPlaceholders(golden.sql, dialect));
      expect(got.params).toEqual(golden.params);
    });
    it(`[${dialect}] batch INSERT + ON CONFLICT DO UPDATE (all)`, () => {
      const golden = builderOf[dialect].buildInsert(batch as any);
      const got = render(compileInsert(dialect, batch as any), dialect);
      expect(got.sql).toBe(renderPlaceholders(golden.sql, dialect));
      expect(got.params).toEqual(golden.params);
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
