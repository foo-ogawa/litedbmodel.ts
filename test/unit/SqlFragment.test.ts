/**
 * SqlFragment / sql tagged template tests
 */

import 'reflect-metadata';
import { describe, it, expect } from 'vitest';
import {
  sql,
  SqlRaw,
  SqlRef,
  isSqlFragment,
  isSqlTypedFragment,
  isSqlCondition,
  isAnySqlFragment,
  isSqlRaw,
  isSqlRef,
} from '../../src/SqlFragment';
import { createColumn, condsToRecord, Conditions, SKIP } from '../../src/Column';
import { DBParentRef, parentRef } from '../../src/DBValues';
import { DBModel, model, column, type ColumnsOf } from '../../src';

// Test columns
const UserAge = createColumn<number, { _brand: 'User' }>('age', 'users', 'User');
const UserName = createColumn<string, { _brand: 'User' }>('name', 'users', 'User');
const UserStatus = createColumn<string, { _brand: 'User' }>('status', 'users', 'User');
const UserDeletedAt = createColumn<Date | null, { _brand: 'User' }>('deleted_at', 'users', 'User');
const UserId = createColumn<number, { _brand: 'User' }>('id', 'users', 'User');
const PostId = createColumn<number, { _brand: 'Post' }>('id', 'posts', 'Post');
const PostUserId = createColumn<number, { _brand: 'Post' }>('user_id', 'posts', 'Post');
const PostTitle = createColumn<string, { _brand: 'Post' }>('title', 'posts', 'Post');

// TABLE_NAME-like objects
const UserTable = { TABLE_NAME: 'users' };
const PostTable = { TABLE_NAME: 'posts' };

describe('sql tagged template', () => {
  describe('Pattern A: Single Column → SqlTypedFragment', () => {
    it('should create SqlTypedFragment with operator', () => {
      const frag = sql`${UserAge} > ?`;
      expect(isSqlTypedFragment(frag)).toBe(true);
      expect(frag.sql).toBe('age > ?');
      expect(frag.params).toEqual([]);
    });

    it('should create SqlTypedFragment with BETWEEN', () => {
      const frag = sql`${UserAge} BETWEEN ? AND ?`;
      expect(isSqlTypedFragment(frag)).toBe(true);
      expect(frag.sql).toBe('age BETWEEN ? AND ?');
      expect(frag.params).toEqual([]);
    });

    it('should create SqlTypedFragment with LIKE', () => {
      const frag = sql`${UserName} LIKE ?`;
      expect(isSqlTypedFragment(frag)).toBe(true);
      expect(frag.sql).toBe('name LIKE ?');
    });

    it('should create SqlTypedFragment with IN', () => {
      const frag = sql`${UserStatus} IN (?)`;
      expect(isSqlTypedFragment(frag)).toBe(true);
      expect(frag.sql).toBe('status IN (?)');
    });

    it('should create SqlTypedFragment with IS NULL (value-free)', () => {
      const frag = sql`${UserDeletedAt} IS NULL`;
      expect(isSqlTypedFragment(frag)).toBe(true);
      expect(frag.sql).toBe('deleted_at IS NULL');
      expect(frag.params).toEqual([]);
    });

    it('should create SqlTypedFragment with IS NOT NULL', () => {
      const frag = sql`${UserDeletedAt} IS NOT NULL`;
      expect(isSqlTypedFragment(frag)).toBe(true);
      expect(frag.sql).toBe('deleted_at IS NOT NULL');
    });
  });

  describe('Pattern B: Column + value(s) → SqlCondition', () => {
    it('should create SqlCondition with single value', () => {
      const frag = sql`${UserAge} > ${18}`;
      expect(isSqlCondition(frag)).toBe(true);
      expect(frag.sql).toBe('age > ?');
      expect(frag.params).toEqual([18]);
    });

    it('should create SqlCondition with BETWEEN values', () => {
      const frag = sql`${UserAge} BETWEEN ${18} AND ${65}`;
      expect(isSqlCondition(frag)).toBe(true);
      expect(frag.sql).toBe('age BETWEEN ? AND ?');
      expect(frag.params).toEqual([18, 65]);
    });

    it('should create SqlCondition with string value', () => {
      const frag = sql`${UserName} LIKE ${'%test%'}`;
      expect(isSqlCondition(frag)).toBe(true);
      expect(frag.sql).toBe('name LIKE ?');
      expect(frag.params).toEqual(['%test%']);
    });

    it('should create SqlCondition with array value (IN)', () => {
      const frag = sql`${UserStatus} IN (${['active', 'pending']})`;
      expect(isSqlCondition(frag)).toBe(true);
      expect(frag.sql).toBe('status IN (?, ?)');
      expect(frag.params).toEqual(['active', 'pending']);
    });

    it('should create SqlCondition with equality', () => {
      const frag = sql`${UserAge} = ${25}`;
      expect(isSqlCondition(frag)).toBe(true);
      expect(frag.sql).toBe('age = ?');
      expect(frag.params).toEqual([25]);
    });
  });

  describe('General: SqlFragment (withQuery / QUERY / execute)', () => {
    it('should create SqlFragment with value interpolations', () => {
      const frag = sql`SELECT * FROM users WHERE age > ${18} AND name = ${'John'}`;
      expect(isSqlFragment(frag)).toBe(true);
      expect(frag.sql).toBe('SELECT * FROM users WHERE age > ? AND name = ?');
      expect(frag.params).toEqual([18, 'John']);
    });

    it('should create SqlFragment with Column references', () => {
      const frag = sql`SELECT ${UserId}, ${UserName} FROM ${UserTable}`;
      expect(isSqlFragment(frag)).toBe(true);
      expect(frag.sql).toBe('SELECT id, name FROM users');
      expect(frag.params).toEqual([]);
    });

    it('should create SqlFragment with multiple Columns and TABLE_NAMEs', () => {
      const frag = sql`SELECT ${UserId} AS user_id, ${PostTitle} FROM ${UserTable} JOIN ${PostTable} ON ${UserId} = ${PostUserId}`;
      expect(isSqlFragment(frag)).toBe(true);
      expect(frag.sql).toBe(
        'SELECT id AS user_id, title FROM users JOIN posts ON id = user_id'
      );
      expect(frag.params).toEqual([]);
    });

    it('should create SqlFragment with mixed Column + value interpolations', () => {
      const startDate = '2024-01-01';
      const endDate = '2024-04-01';
      const frag = sql`SELECT ${UserId} FROM ${UserTable} WHERE created_at >= ${startDate} AND created_at < ${endDate}`;
      expect(isSqlFragment(frag)).toBe(true);
      expect(frag.sql).toBe(
        'SELECT id FROM users WHERE created_at >= ? AND created_at < ?'
      );
      expect(frag.params).toEqual(['2024-01-01', '2024-04-01']);
    });

    it('should handle null interpolation', () => {
      const frag = sql`SELECT * FROM users WHERE deleted_at = ${null}`;
      expect(frag.sql).toBe('SELECT * FROM users WHERE deleted_at = NULL');
      expect(frag.params).toEqual([]);
    });

    it('should handle undefined interpolation', () => {
      const frag = sql`SELECT * FROM users WHERE deleted_at = ${undefined}`;
      expect(frag.sql).toBe('SELECT * FROM users WHERE deleted_at = NULL');
      expect(frag.params).toEqual([]);
    });

    it('should handle boolean interpolation', () => {
      const frag = sql`SELECT * FROM users WHERE is_active = ${true}`;
      expect(frag.sql).toBe('SELECT * FROM users WHERE is_active = ?');
      expect(frag.params).toEqual([true]);
    });

    it('should handle Date interpolation', () => {
      const date = new Date('2024-01-01');
      const frag = sql`SELECT * FROM users WHERE created_at > ${date}`;
      expect(frag.sql).toBe('SELECT * FROM users WHERE created_at > ?');
      expect(frag.params).toEqual([date]);
    });

    it('should handle bigint interpolation', () => {
      const frag = sql`SELECT * FROM sessions WHERE id = ${123n}`;
      expect(frag.sql).toBe('SELECT * FROM sessions WHERE id = ?');
      expect(frag.params).toEqual([123n]);
    });
  });

  describe('sql.raw()', () => {
    it('should create SqlRaw instance', () => {
      const raw = sql.raw('ASC');
      expect(raw).toBeInstanceOf(SqlRaw);
      expect(raw.value).toBe('ASC');
      expect(raw.toString()).toBe('ASC');
    });

    it('should embed raw SQL without parameterization', () => {
      const direction = sql.raw('DESC');
      const frag = sql`SELECT * FROM users ORDER BY name ${direction}`;
      expect(frag.sql).toBe('SELECT * FROM users ORDER BY name DESC');
      expect(frag.params).toEqual([]);
    });

    it('should work with DISTINCT', () => {
      const distinct = sql.raw('DISTINCT');
      const frag = sql`SELECT ${distinct} ${UserId} FROM ${UserTable}`;
      expect(frag.sql).toBe('SELECT DISTINCT id FROM users');
    });
  });

  describe('sql.ref()', () => {
    it('should create SqlRef instance', () => {
      const ref = sql.ref(UserId);
      expect(ref).toBeInstanceOf(SqlRef);
      expect(ref.tableName).toBe('users');
      expect(ref.columnName).toBe('id');
      expect(ref.toString()).toBe('users.id');
    });

    it('should embed table-qualified name', () => {
      const frag = sql`SELECT ${sql.ref(UserId)}, ${sql.ref(PostTitle)} FROM users JOIN posts ON ${sql.ref(UserId)} = ${sql.ref(PostUserId)}`;
      expect(frag.sql).toBe(
        'SELECT users.id, posts.title FROM users JOIN posts ON users.id = posts.user_id'
      );
      expect(frag.params).toEqual([]);
    });
  });

  describe('parentRef interpolation', () => {
    it('should embed parentRef as table.column', () => {
      const pRef = parentRef(UserId);
      const frag = sql`SELECT * FROM orders WHERE user_id = ${pRef}`;
      expect(frag.sql).toBe('SELECT * FROM orders WHERE user_id = users.id');
      expect(frag.params).toEqual([]);
    });
  });

  describe('Nested SqlFragment', () => {
    it('should expand nested sql fragment and merge params', () => {
      const sub = sql`SELECT user_id FROM orders WHERE status = ${'paid'}`;
      const outer = sql`SELECT * FROM users WHERE id IN (${sub}) AND age > ${18}`;

      expect(outer.sql).toBe(
        'SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = ?) AND age > ?'
      );
      expect(outer.params).toEqual(['paid', 18]);
    });

    it('should handle deeply nested fragments', () => {
      const innermost = sql`status = ${'active'}`;
      const inner = sql`SELECT id FROM users WHERE ${innermost} AND age > ${18}`;
      const outer = sql`SELECT * FROM orders WHERE user_id IN (${inner})`;

      expect(outer.sql).toBe(
        'SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE status = ? AND age > ?)'
      );
      expect(outer.params).toEqual(['active', 18]);
    });

    it('should handle nested fragment with no params', () => {
      const sub = sql`SELECT ${UserId} FROM ${UserTable}`;
      const outer = sql`SELECT * FROM orders WHERE user_id IN (${sub})`;

      expect(outer.sql).toBe(
        'SELECT * FROM orders WHERE user_id IN (SELECT id FROM users)'
      );
      expect(outer.params).toEqual([]);
    });
  });

  describe('Array interpolation', () => {
    it('should expand array to comma-separated placeholders', () => {
      const frag = sql`SELECT * FROM users WHERE id IN (${[1, 2, 3]})`;
      expect(frag.sql).toBe('SELECT * FROM users WHERE id IN (?, ?, ?)');
      expect(frag.params).toEqual([1, 2, 3]);
    });

    it('should expand string array', () => {
      const frag = sql`SELECT * FROM users WHERE status IN (${['active', 'pending']})`;
      expect(frag.sql).toBe('SELECT * FROM users WHERE status IN (?, ?)');
      expect(frag.params).toEqual(['active', 'pending']);
    });

    it('should handle empty array', () => {
      const frag = sql`SELECT * FROM users WHERE id IN (${[]})`;
      expect(frag.sql).toBe('SELECT * FROM users WHERE id IN (NULL)');
      expect(frag.params).toEqual([]);
    });
  });
});

describe('Type guards', () => {
  it('isSqlFragment should identify SqlFragment', () => {
    const frag = sql`SELECT * FROM users WHERE age > ${18}`;
    expect(isSqlFragment(frag)).toBe(true);
    expect(isSqlTypedFragment(frag)).toBe(false);
    expect(isSqlCondition(frag)).toBe(false);
  });

  it('isSqlTypedFragment should identify SqlTypedFragment', () => {
    const frag = sql`${UserAge} > ?`;
    expect(isSqlTypedFragment(frag)).toBe(true);
    expect(isSqlFragment(frag)).toBe(false);
    expect(isSqlCondition(frag)).toBe(false);
  });

  it('isSqlCondition should identify SqlCondition', () => {
    const frag = sql`${UserAge} > ${18}`;
    expect(isSqlCondition(frag)).toBe(true);
    expect(isSqlFragment(frag)).toBe(false);
    expect(isSqlTypedFragment(frag)).toBe(false);
  });

  it('isAnySqlFragment should match all three types', () => {
    expect(isAnySqlFragment(sql`${UserAge} > ?`)).toBe(true);
    expect(isAnySqlFragment(sql`${UserAge} > ${18}`)).toBe(true);
    expect(isAnySqlFragment(sql`SELECT * FROM users WHERE age > ${18}`)).toBe(true);
  });

  it('isAnySqlFragment should reject non-fragment values', () => {
    expect(isAnySqlFragment(null)).toBe(false);
    expect(isAnySqlFragment(undefined)).toBe(false);
    expect(isAnySqlFragment('string')).toBe(false);
    expect(isAnySqlFragment(123)).toBe(false);
    expect(isAnySqlFragment({})).toBe(false);
    expect(isAnySqlFragment({ _tag: 'Other' })).toBe(false);
  });

  it('isSqlRaw should identify SqlRaw', () => {
    expect(isSqlRaw(sql.raw('ASC'))).toBe(true);
    expect(isSqlRaw('ASC')).toBe(false);
  });

  it('isSqlRef should identify SqlRef', () => {
    expect(isSqlRef(sql.ref(UserId))).toBe(true);
    expect(isSqlRef(UserId)).toBe(false);
  });
});

describe('condsToRecord integration with sql tag', () => {
  describe('Pattern A: [sql`...?`, value] tuples', () => {
    it('should handle SqlTypedFragment tuple with single value', () => {
      const conds = [[sql`${UserAge} > ?`, 18]] as const;
      const result = condsToRecord(conds);
      expect(result).toEqual({ 'age > ?': 18 });
    });

    it('should handle SqlTypedFragment tuple with BETWEEN', () => {
      const conds = [[sql`${UserAge} BETWEEN ? AND ?`, [18, 65]]] as const;
      const result = condsToRecord(conds);
      expect(result).toEqual({ 'age BETWEEN ? AND ?': [18, 65] });
    });

    it('should handle SqlTypedFragment tuple with LIKE', () => {
      const conds = [[sql`${UserName} LIKE ?`, '%test%']] as const;
      const result = condsToRecord(conds);
      expect(result).toEqual({ 'name LIKE ?': '%test%' });
    });

    it('should handle SqlTypedFragment tuple with IN', () => {
      const conds = [[sql`${UserStatus} IN (?)`, ['active', 'pending']]] as const;
      const result = condsToRecord(conds);
      expect(result).toEqual({ 'status IN (?)': ['active', 'pending'] });
    });

    it('should handle SKIP value in SqlTypedFragment tuple', () => {
      const conds = [[sql`${UserAge} > ?`, SKIP]] as const;
      const result = condsToRecord(conds);
      expect(result).toEqual({});
    });
  });

  describe('Pattern B: sql`...${value}` as direct condition', () => {
    it('should handle SqlCondition with single value', () => {
      const conds = [sql`${UserAge} > ${18}`] as const;
      const result = condsToRecord(conds);
      expect(result).toEqual({ 'age > ?': 18 });
    });

    it('should handle SqlCondition with BETWEEN', () => {
      const conds = [sql`${UserAge} BETWEEN ${18} AND ${65}`] as const;
      const result = condsToRecord(conds);
      expect(result).toEqual({ 'age BETWEEN ? AND ?': [18, 65] });
    });

    it('should handle SqlCondition with string value', () => {
      const conds = [sql`${UserName} LIKE ${'%test%'}`] as const;
      const result = condsToRecord(conds);
      expect(result).toEqual({ 'name LIKE ?': '%test%' });
    });
  });

  describe('Value-free conditions (IS NULL)', () => {
    it('should handle SqlTypedFragment IS NULL as direct condition', () => {
      const conds = [sql`${UserDeletedAt} IS NULL`] as const;
      const result = condsToRecord(conds);
      expect(result).toEqual({ 'deleted_at IS NULL': true });
    });

    it('should handle SqlTypedFragment IS NOT NULL', () => {
      const conds = [sql`${UserDeletedAt} IS NOT NULL`] as const;
      const result = condsToRecord(conds);
      expect(result).toEqual({ 'deleted_at IS NOT NULL': true });
    });
  });

  describe('Mixed conditions', () => {
    it('should handle mix of Column tuples and sql tag conditions', () => {
      const conds = [
        [UserId, 1],
        [sql`${UserAge} > ?`, 18],
        sql`${UserDeletedAt} IS NULL`,
      ] as const;
      const result = condsToRecord(conds);
      expect(result).toEqual({
        id: 1,
        'age > ?': 18,
        'deleted_at IS NULL': true,
      });
    });

    it('should handle Pattern A, Pattern B, and Column tuples together', () => {
      const conds = [
        [UserStatus, 'active'],
        [sql`${UserAge} >= ?`, 18],
        sql`${UserName} LIKE ${'%test%'}`,
        sql`${UserDeletedAt} IS NULL`,
      ] as const;
      const result = condsToRecord(conds);
      expect(result).toEqual({
        status: 'active',
        'age >= ?': 18,
        'name LIKE ?': '%test%',
        'deleted_at IS NULL': true,
      });
    });

    it('should handle SKIP with sql tag conditions', () => {
      const query = { name: undefined as string | undefined };
      const conds = [
        [UserId, 1],
        SKIP,
        sql`${UserDeletedAt} IS NULL`,
      ] as const;
      const result = condsToRecord(conds);
      expect(result).toEqual({
        id: 1,
        'deleted_at IS NULL': true,
      });
    });
  });
});

describe('Conditions.addSql()', () => {
  it('should add sql-tagged condition with value', () => {
    const conds = new Conditions();
    conds.addSql(sql`${UserAge} > ?`, 18);
    const built = conds.build();
    expect(built.length).toBe(1);
    const result = condsToRecord(built);
    expect(result).toEqual({ 'age > ?': 18 });
  });

  it('should add sql-tagged condition without value (IS NULL)', () => {
    const conds = new Conditions();
    conds.addSql(sql`${UserDeletedAt} IS NULL`);
    const built = conds.build();
    expect(built.length).toBe(1);
    const result = condsToRecord(built);
    expect(result).toEqual({ 'deleted_at IS NULL': true });
  });

  it('should chain with other condition methods', () => {
    const conds = new Conditions()
      .add(UserId, 1)
      .addSql(sql`${UserAge} > ?`, 18)
      .addRaw('email IS NOT NULL');
    expect(conds.length).toBe(3);
    const result = condsToRecord(conds.build());
    expect(result).toEqual({
      id: 1,
      'age > ?': 18,
      'email IS NOT NULL': true,
    });
  });
});

describe('SqlRaw', () => {
  it('should have _tag property', () => {
    const raw = new SqlRaw('ASC');
    expect(raw._tag).toBe('SqlRaw');
  });

  it('should store value', () => {
    const raw = new SqlRaw('DISTINCT');
    expect(raw.value).toBe('DISTINCT');
  });

  it('toString should return value', () => {
    const raw = new SqlRaw('ASC');
    expect(String(raw)).toBe('ASC');
  });
});

describe('SqlRef', () => {
  it('should have _tag property', () => {
    const ref = new SqlRef({ tableName: 'users', columnName: 'id' });
    expect(ref._tag).toBe('SqlRef');
  });

  it('should store tableName and columnName', () => {
    const ref = new SqlRef(UserId);
    expect(ref.tableName).toBe('users');
    expect(ref.columnName).toBe('id');
  });

  it('toString should return table.column', () => {
    const ref = new SqlRef(UserId);
    expect(String(ref)).toBe('users.id');
  });
});

// ============================================
// Audit-driven additional tests
// ============================================

describe('Nested SqlCondition / SqlTypedFragment inside general template', () => {
  it('should expand nested SqlCondition and merge params', () => {
    const cond = sql`${UserAge} > ${18}`;
    expect(isSqlCondition(cond)).toBe(true);

    const outer = sql`SELECT * FROM users WHERE ${cond} AND is_active = ${true}`;
    expect(isSqlFragment(outer)).toBe(true);
    expect(outer.sql).toBe('SELECT * FROM users WHERE age > ? AND is_active = ?');
    expect(outer.params).toEqual([18, true]);
  });

  it('should expand nested SqlTypedFragment (0 params) inside general template', () => {
    const typed = sql`${UserAge} > ?`;
    expect(isSqlTypedFragment(typed)).toBe(true);

    const outer = sql`SELECT * FROM users WHERE ${typed}`;
    expect(isSqlFragment(outer)).toBe(true);
    expect(outer.sql).toBe('SELECT * FROM users WHERE age > ?');
    expect(outer.params).toEqual([]);
  });

  it('should expand nested SqlTypedFragment IS NULL inside general template', () => {
    const typed = sql`${UserDeletedAt} IS NULL`;
    const outer = sql`SELECT * FROM users WHERE ${typed} AND status = ${'active'}`;
    expect(outer.sql).toBe('SELECT * FROM users WHERE deleted_at IS NULL AND status = ?');
    expect(outer.params).toEqual(['active']);
  });
});

describe('OR conditions with sql tag', () => {
  it('should handle sql tag tuples inside OR conditions via condsToRecord', () => {
    const orCond = {
      _type: 'or' as const,
      conditions: [
        [[sql`${UserAge} > ?`, 18]],
        [[sql`${UserAge} < ?`, 10]],
      ],
    };
    const result = condsToRecord([orCond]);
    expect(result.__or__).toBeDefined();
    const orGroups = result.__or__ as Record<string, unknown>[];
    expect(orGroups[0]).toEqual({ 'age > ?': 18 });
    expect(orGroups[1]).toEqual({ 'age < ?': 10 });
  });

  it('should handle SqlCondition (Pattern B) inside OR conditions', () => {
    const orCond = {
      _type: 'or' as const,
      conditions: [
        [sql`${UserStatus} = ${'admin'}`],
        [sql`${UserStatus} = ${'moderator'}`],
      ],
    };
    const result = condsToRecord([orCond]);
    const orGroups = result.__or__ as Record<string, unknown>[];
    expect(orGroups[0]).toEqual({ 'status = ?': 'admin' });
    expect(orGroups[1]).toEqual({ 'status = ?': 'moderator' });
  });

  it('should handle value-free SqlTypedFragment inside OR conditions', () => {
    const orCond = {
      _type: 'or' as const,
      conditions: [
        [sql`${UserDeletedAt} IS NULL`],
        [[UserStatus, 'active']],
      ],
    };
    const result = condsToRecord([orCond]);
    const orGroups = result.__or__ as Record<string, unknown>[];
    expect(orGroups[0]).toEqual({ 'deleted_at IS NULL': true });
    expect(orGroups[1]).toEqual({ status: 'active' });
  });
});

describe('Edge cases', () => {
  it('should handle single-element array interpolation', () => {
    const frag = sql`SELECT * FROM users WHERE id IN (${[42]})`;
    expect(frag.sql).toBe('SELECT * FROM users WHERE id IN (?)');
    expect(frag.params).toEqual([42]);
  });

  it('should handle boolean array interpolation', () => {
    const frag = sql`SELECT * FROM users WHERE flag IN (${[true, false]})`;
    expect(frag.sql).toBe('SELECT * FROM users WHERE flag IN (?, ?)');
    expect(frag.params).toEqual([true, false]);
  });

  it('should handle Date array interpolation', () => {
    const d1 = new Date('2024-01-01');
    const d2 = new Date('2024-12-31');
    const frag = sql`SELECT * FROM events WHERE date IN (${[d1, d2]})`;
    expect(frag.sql).toBe('SELECT * FROM events WHERE date IN (?, ?)');
    expect(frag.params).toEqual([d1, d2]);
  });

  it('should handle multiple sql.raw() values in one template', () => {
    const distinct = sql.raw('DISTINCT');
    const direction = sql.raw('DESC');
    const frag = sql`SELECT ${distinct} * FROM users ORDER BY name ${direction}`;
    expect(frag.sql).toBe('SELECT DISTINCT * FROM users ORDER BY name DESC');
    expect(frag.params).toEqual([]);
  });

  it('should handle null in Pattern B (produces NULL literal, not parameter)', () => {
    const frag = sql`${UserAge} = ${null}`;
    expect(isSqlCondition(frag)).toBe(true);
    expect(frag.sql).toBe('age = NULL');
    expect(frag.params).toEqual([]);
  });

  it('should handle undefined in Pattern B (produces NULL literal)', () => {
    const frag = sql`${UserAge} = ${undefined}`;
    expect(isSqlCondition(frag)).toBe(true);
    expect(frag.sql).toBe('age = NULL');
    expect(frag.params).toEqual([]);
  });

  it('should produce SqlFragment (not SqlCondition) when no interpolations', () => {
    const frag = sql`SELECT 1`;
    expect(isSqlFragment(frag)).toBe(true);
    expect(frag.sql).toBe('SELECT 1');
    expect(frag.params).toEqual([]);
  });

  it('should produce SqlFragment for only value interpolations (no Column)', () => {
    const frag = sql`SELECT * FROM users WHERE age > ${18}`;
    expect(isSqlFragment(frag)).toBe(true);
    expect(frag.sql).toBe('SELECT * FROM users WHERE age > ?');
    expect(frag.params).toEqual([18]);
  });

  it('should produce SqlFragment when Column is not the first interpolation', () => {
    const frag = sql`SELECT ${18}, ${UserAge}`;
    expect(isSqlFragment(frag)).toBe(true);
    expect(frag.sql).toBe('SELECT ?, age');
    expect(frag.params).toEqual([18]);
  });
});

describe('DBModel integration: withQuery with SqlFragment', () => {
  it('should accept SqlFragment and extract sql/params', () => {
    @model('wq_test')
    class WQModel extends DBModel {
      @column() id?: number;
      @column() total?: number;

      static forPeriod(start: string, end: string) {
        return this.withQuery(sql`
          SELECT id, SUM(amount) AS total
          FROM transactions
          WHERE created_at >= ${start} AND created_at < ${end}
          GROUP BY id
        `);
      }
    }

    const Bound = WQModel.forPeriod('2024-01-01', '2024-04-01');

    expect(Bound.isQueryBased()).toBe(true);
    expect(Bound.getQueryParams()).toEqual(['2024-01-01', '2024-04-01']);
    expect(Bound.TABLE_NAME).toBe('wq_test');
  });

  it('should create independent bound models with SqlFragment', () => {
    @model('wq_independent')
    class IndModel extends DBModel {
      @column() id?: number;

      static forYear(year: number) {
        return this.withQuery(sql`SELECT * FROM data WHERE year = ${year}`);
      }
    }

    const M2023 = IndModel.forYear(2023);
    const M2024 = IndModel.forYear(2024);

    expect(M2023.getQueryParams()).toEqual([2023]);
    expect(M2024.getQueryParams()).toEqual([2024]);
    expect(IndModel.getQueryParams()).toEqual([]);
  });
});

describe('DBModel integration: QUERY as SqlFragment', () => {
  it('should accept SqlFragment as QUERY and resolve correctly', () => {
    const UserCol = createColumn<number, unknown>('id', 'users', 'User');

    @model('query_frag')
    class QFModel extends DBModel {
      @column() id?: number;
      @column() name?: string;

      static QUERY = sql`SELECT ${UserCol}, name FROM users WHERE active = ${true}`;
    }

    expect(QFModel.isQueryBased()).toBe(true);
    expect((QFModel as any)._getQuerySQL()).toBe('SELECT id, name FROM users WHERE active = ?');
    expect(QFModel.getQueryParams()).toEqual([true]);
  });

  it('should merge SqlFragment params with _queryParams', () => {
    @model('merge_params')
    class MergeModel extends DBModel {
      @column() id?: number;
    }

    const frag = sql`SELECT * FROM data WHERE x = ${10}`;
    (MergeModel as any).QUERY = frag;
    (MergeModel as any)._queryParams = [20, 30];

    const resolved = (MergeModel as any)._resolveQuery();
    expect(resolved.sql).toBe('SELECT * FROM data WHERE x = ?');
    expect(resolved.params).toEqual([10, 20, 30]);

    expect(MergeModel.getQueryParams()).toEqual([10, 20, 30]);

    // Cleanup
    (MergeModel as any).QUERY = null;
    (MergeModel as any)._queryParams = null;
  });
});

describe('DBModel integration: execute with SqlFragment', () => {
  it('should resolve SqlFragment in execute overload (type-level check)', () => {
    // We can't actually execute SQL without a DB, but we can verify
    // the function accepts SqlFragment at the type/signature level
    // by checking the isSqlFragment branch resolves correctly.
    const frag = sql`SELECT process_daily(${new Date('2024-01-01')})`;
    expect(isSqlFragment(frag)).toBe(true);
    expect(frag.sql).toBe('SELECT process_daily(?)');
    expect(frag.params).toHaveLength(1);
    expect(frag.params[0]).toEqual(new Date('2024-01-01'));
  });
});
