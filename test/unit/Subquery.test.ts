/**
 * Subquery feature tests
 * Uses type-safe Column references with tableName property
 */

import 'reflect-metadata';
import { describe, it, expect } from 'vitest';
import { DBSubquery, DBExists, parentRef } from '../../src/DBValues';
import { DBConditions } from '../../src/DBConditions';
import { DBModel } from '../../src/DBModel';
import { model, column } from '../../src/decorators';
import type { ColumnsOf } from '../../src/Column';

// ============================================
// Model Definitions
// ============================================

@model('users')
class UserModel extends DBModel {
  @column() id?: number;
  @column() group_id?: number;
  @column() tenant_id?: number;
  @column() is_active?: boolean;
  @column() created_at?: Date;
  @column() deleted_at?: Date;
  @column() parent_id?: number;
}
export const User = UserModel as typeof UserModel & ColumnsOf<UserModel>;

@model('orders')
class OrderModel extends DBModel {
  @column() id?: number;
  @column() user_id?: number;
  @column() group_id?: number;
  @column() tenant_id?: number;
  @column() status?: string;
  @column() amount?: number;
  @column() created_at?: Date;
}
export const Order = OrderModel as typeof OrderModel & ColumnsOf<OrderModel>;

@model('banned_users')
class BannedUserModel extends DBModel {
  @column() id?: number;
  @column() user_id?: number;
  @column() group_id?: number;
  @column() is_active?: boolean;
}
export const BannedUser = BannedUserModel as typeof BannedUserModel & ColumnsOf<BannedUserModel>;

@model('complaints')
class ComplaintModel extends DBModel {
  @column() id?: number;
  @column() user_id?: number;
}
export const Complaint = ComplaintModel as typeof ComplaintModel & ColumnsOf<ComplaintModel>;

@model('temp_table')
class TempModel extends DBModel {
  @column() id?: number;
}
export const Temp = TempModel as typeof TempModel & ColumnsOf<TempModel>;

// ============================================
// Column.tableName property tests
// ============================================

describe('Column.tableName property', () => {
  it('should have tableName property on Column', () => {
    expect(User.id.tableName).toBe('users');
    expect(User.id.columnName).toBe('id');
    expect(User.id.modelName).toBe('UserModel');
  });

  it('should have correct tableName for each model', () => {
    expect(Order.user_id.tableName).toBe('orders');
    expect(BannedUser.user_id.tableName).toBe('banned_users');
    expect(Complaint.user_id.tableName).toBe('complaints');
  });
});

// ============================================
// Low-level DBSubquery/DBExists Tests (using Column directly)
// ============================================

describe('DBSubquery with Column type', () => {
  describe('single key IN subquery', () => {
    it('should compile simple IN subquery using Column directly', () => {
      const subquery = new DBSubquery(
        [User.id],
        'orders',
        [Order.user_id],
        [{ column: Order.status, value: 'paid' }],
        'IN'
      );

      const params: unknown[] = [];
      const sql = subquery.compile(params);

      expect(sql).toBe('users.id IN (SELECT orders.user_id FROM orders WHERE orders.status = ?)');
      expect(params).toEqual(['paid']);
    });

    it('should compile NOT IN subquery using Column directly', () => {
      const subquery = new DBSubquery(
        [User.id],
        'banned_users',
        [BannedUser.user_id],
        [],
        'NOT IN'
      );

      const params: unknown[] = [];
      const sql = subquery.compile(params);

      expect(sql).toBe('users.id NOT IN (SELECT banned_users.user_id FROM banned_users)');
      expect(params).toEqual([]);
    });

    it('should compile subquery with multiple conditions', () => {
      const subquery = new DBSubquery(
        [User.id],
        'orders',
        [Order.user_id],
        [
          { column: Order.status, value: 'paid' },
          { column: Order.amount, value: 1000 }
        ],
        'IN'
      );

      const params: unknown[] = [];
      const sql = subquery.compile(params);

      expect(sql).toBe('users.id IN (SELECT orders.user_id FROM orders WHERE orders.status = ? AND orders.amount = ?)');
      expect(params).toEqual(['paid', 1000]);
    });

    it('should compile subquery with null condition', () => {
      const subquery = new DBSubquery(
        [User.parent_id],
        'users',
        [User.id],
        [{ column: User.deleted_at, value: null }],
        'IN'
      );

      const params: unknown[] = [];
      const sql = subquery.compile(params);

      expect(sql).toBe('users.parent_id IN (SELECT users.id FROM users WHERE users.deleted_at IS NULL)');
      expect(params).toEqual([]);
    });

    it('should compile subquery with array IN condition', () => {
      const subquery = new DBSubquery(
        [User.id],
        'orders',
        [Order.user_id],
        [{ column: Order.status, value: ['paid', 'shipped'] }],
        'IN'
      );

      const params: unknown[] = [];
      const sql = subquery.compile(params);

      expect(sql).toBe('users.id IN (SELECT orders.user_id FROM orders WHERE orders.status IN (?, ?))');
      expect(params).toEqual(['paid', 'shipped']);
    });
  });

  describe('composite key IN subquery', () => {
    it('should compile composite key IN subquery', () => {
      const subquery = new DBSubquery(
        [User.id, User.group_id],
        'orders',
        [Order.user_id, Order.group_id],
        [{ column: Order.status, value: 'paid' }],
        'IN'
      );

      const params: unknown[] = [];
      const sql = subquery.compile(params);

      expect(sql).toBe('(users.id, users.group_id) IN (SELECT orders.user_id, orders.group_id FROM orders WHERE orders.status = ?)');
      expect(params).toEqual(['paid']);
    });

    it('should compile composite key NOT IN subquery', () => {
      const subquery = new DBSubquery(
        [User.id, User.group_id],
        'banned_users',
        [BannedUser.user_id, BannedUser.group_id],
        [],
        'NOT IN'
      );

      const params: unknown[] = [];
      const sql = subquery.compile(params);

      expect(sql).toBe('(users.id, users.group_id) NOT IN (SELECT banned_users.user_id, banned_users.group_id FROM banned_users)');
      expect(params).toEqual([]);
    });

    it('should compile triple composite key subquery', () => {
      const subquery = new DBSubquery(
        [User.tenant_id, User.id, User.group_id],
        'orders',
        [Order.tenant_id, Order.user_id, Order.group_id],
        [{ column: Order.status, value: 'active' }],
        'IN'
      );

      const params: unknown[] = [];
      const sql = subquery.compile(params);

      expect(sql).toBe('(users.tenant_id, users.id, users.group_id) IN (SELECT orders.tenant_id, orders.user_id, orders.group_id FROM orders WHERE orders.status = ?)');
      expect(params).toEqual(['active']);
    });
  });

  describe('correlated subquery with parentRef', () => {
    it('should compile correlated subquery with parentRef using Column', () => {
      const subquery = new DBSubquery(
        [User.id],
        'orders',
        [Order.user_id],
        [
          { column: Order.user_id, value: parentRef(User.id) },
          { column: Order.status, value: 'paid' }
        ],
        'IN'
      );

      const params: unknown[] = [];
      const sql = subquery.compile(params);

      expect(sql).toBe('users.id IN (SELECT orders.user_id FROM orders WHERE orders.user_id = users.id AND orders.status = ?)');
      expect(params).toEqual(['paid']);
    });

    it('should compile correlated subquery with multiple parent refs', () => {
      const subquery = new DBSubquery(
        [User.id],
        'orders',
        [Order.user_id],
        [
          { column: Order.tenant_id, value: parentRef(User.tenant_id) },
          { column: Order.user_id, value: parentRef(User.id) },
          { column: Order.status, value: 'paid' }
        ],
        'IN'
      );

      const params: unknown[] = [];
      const sql = subquery.compile(params);

      expect(sql).toBe('users.id IN (SELECT orders.user_id FROM orders WHERE orders.tenant_id = users.tenant_id AND orders.user_id = users.id AND orders.status = ?)');
      expect(params).toEqual(['paid']);
    });
  });
});

describe('DBExists with Column type', () => {
  it('should compile EXISTS subquery using Column directly', () => {
    const exists = new DBExists(
      'orders',
      [{ column: Order.user_id, value: parentRef(User.id) }],
      false
    );

    const params: unknown[] = [];
    const sql = exists.compile(params);

    expect(sql).toBe('EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)');
    expect(params).toEqual([]);
  });

  it('should compile NOT EXISTS subquery', () => {
    const exists = new DBExists(
      'banned_users',
      [
        { column: BannedUser.user_id, value: parentRef(User.id) },
        { column: BannedUser.is_active, value: true }
      ],
      true
    );

    const params: unknown[] = [];
    const sql = exists.compile(params);

    expect(sql).toBe('NOT EXISTS (SELECT 1 FROM banned_users WHERE banned_users.user_id = users.id AND banned_users.is_active = ?)');
    expect(params).toEqual([true]);
  });

  it('should compile empty EXISTS subquery', () => {
    const exists = new DBExists('temp_table', [], false);

    const params: unknown[] = [];
    const sql = exists.compile(params);

    expect(sql).toBe('EXISTS (SELECT 1 FROM temp_table)');
    expect(params).toEqual([]);
  });
});

describe('DBParentRef', () => {
  it('should compile parent reference using Column tableName', () => {
    const ref = parentRef(User.id);

    const params: unknown[] = [];
    const sql = ref.compile(params);

    expect(sql).toBe('users.id');
  });

  it('should store parent column info from Column type', () => {
    const ref = parentRef(User.group_id);

    expect(ref.columnName).toBe('group_id');
    expect(ref.tableName).toBe('users');
  });
});

describe('DBConditions with subqueries using Column', () => {
  it('should compile IN subquery condition in DBConditions', () => {
    const subquery = new DBSubquery(
      [User.id],
      'orders',
      [Order.user_id],
      [{ column: Order.status, value: 'paid' }],
      'IN'
    );

    const conditions = new DBConditions({
      is_active: true,
      __subquery__: subquery
    });

    const params: unknown[] = [];
    const sql = conditions.compile(params);

    expect(sql).toBe('is_active = TRUE AND users.id IN (SELECT orders.user_id FROM orders WHERE orders.status = ?)');
    expect(params).toEqual(['paid']);
  });

  it('should compile EXISTS condition in DBConditions', () => {
    const exists = new DBExists(
      'orders',
      [{ column: Order.user_id, value: parentRef(User.id) }],
      false
    );

    const conditions = new DBConditions({
      is_active: true,
      __exists__: exists
    });

    const params: unknown[] = [];
    const sql = conditions.compile(params);

    expect(sql).toBe('is_active = TRUE AND EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)');
  });

  it('should compile composite key subquery in DBConditions', () => {
    const subquery = new DBSubquery(
      [User.id, User.group_id],
      'orders',
      [Order.user_id, Order.group_id],
      [{ column: Order.status, value: 'active' }],
      'IN'
    );

    const conditions = new DBConditions({
      __subquery__: subquery
    });

    const params: unknown[] = [];
    const sql = conditions.compile(params);

    expect(sql).toBe('(users.id, users.group_id) IN (SELECT orders.user_id, orders.group_id FROM orders WHERE orders.status = ?)');
    expect(params).toEqual(['active']);
  });
});

// ============================================
// DBModel Static Method Tests (new API without targetModel)
// ============================================

describe('DBModel.inSubquery', () => {
  it('should create IN subquery with single key pair', () => {
    const [key, subquery] = User.inSubquery(
      [[User.id, Order.user_id]],
      [[Order.status, 'paid']]
    );

    expect(key).toBe('__subquery__');
    expect(subquery).toBeInstanceOf(DBSubquery);

    const params: unknown[] = [];
    const sql = subquery.compile(params);

    expect(sql).toBe('users.id IN (SELECT orders.user_id FROM orders WHERE orders.status = ?)');
    expect(params).toEqual(['paid']);
  });

  it('should create IN subquery with composite key pairs', () => {
    const [key, subquery] = User.inSubquery(
      [
        [User.id, Order.user_id],
        [User.group_id, Order.group_id],
      ],
      [[Order.status, 'paid']]
    );

    expect(key).toBe('__subquery__');

    const params: unknown[] = [];
    const sql = subquery.compile(params);

    expect(sql).toBe('(users.id, users.group_id) IN (SELECT orders.user_id, orders.group_id FROM orders WHERE orders.status = ?)');
    expect(params).toEqual(['paid']);
  });

  it('should create IN subquery with multiple conditions', () => {
    const [, subquery] = User.inSubquery(
      [[User.id, Order.user_id]],
      [
        [Order.status, 'paid'],
        [Order.amount, 1000]
      ]
    );

    const params: unknown[] = [];
    const sql = subquery.compile(params);

    expect(sql).toBe('users.id IN (SELECT orders.user_id FROM orders WHERE orders.status = ? AND orders.amount = ?)');
    expect(params).toEqual(['paid', 1000]);
  });

  it('should create IN subquery without conditions', () => {
    const [, subquery] = User.inSubquery(
      [[User.id, Order.user_id]]
    );

    const params: unknown[] = [];
    const sql = subquery.compile(params);

    expect(sql).toBe('users.id IN (SELECT orders.user_id FROM orders)');
    expect(params).toEqual([]);
  });

  it('should create correlated IN subquery with parentRef', () => {
    const [, subquery] = User.inSubquery(
      [[User.id, Order.user_id]],
      [
        [Order.tenant_id, parentRef(User.tenant_id)],
        [Order.status, 'completed']
      ]
    );

    const params: unknown[] = [];
    const sql = subquery.compile(params);

    expect(sql).toBe('users.id IN (SELECT orders.user_id FROM orders WHERE orders.tenant_id = users.tenant_id AND orders.status = ?)');
    expect(params).toEqual(['completed']);
  });
});

describe('DBModel.notInSubquery', () => {
  it('should create NOT IN subquery', () => {
    const [key, subquery] = User.notInSubquery(
      [[User.id, BannedUser.user_id]]
    );

    expect(key).toBe('__subquery__');

    const params: unknown[] = [];
    const sql = subquery.compile(params);

    expect(sql).toBe('users.id NOT IN (SELECT banned_users.user_id FROM banned_users)');
    expect(params).toEqual([]);
  });

  it('should create NOT IN subquery with conditions', () => {
    const [, subquery] = User.notInSubquery(
      [[User.id, BannedUser.user_id]],
      [[BannedUser.is_active, true]]
    );

    const params: unknown[] = [];
    const sql = subquery.compile(params);

    expect(sql).toBe('users.id NOT IN (SELECT banned_users.user_id FROM banned_users WHERE banned_users.is_active = ?)');
    expect(params).toEqual([true]);
  });

  it('should create composite key NOT IN subquery', () => {
    const [, subquery] = User.notInSubquery([
      [User.id, BannedUser.user_id],
      [User.group_id, BannedUser.group_id],
    ]);

    const params: unknown[] = [];
    const sql = subquery.compile(params);

    expect(sql).toBe('(users.id, users.group_id) NOT IN (SELECT banned_users.user_id, banned_users.group_id FROM banned_users)');
    expect(params).toEqual([]);
  });
});

describe('DBModel.exists', () => {
  it('should create EXISTS subquery', () => {
    const [key, exists] = User.exists([
      [Order.user_id, parentRef(User.id)]
    ]);

    expect(key).toBe('__exists__');
    expect(exists).toBeInstanceOf(DBExists);

    const params: unknown[] = [];
    const sql = exists.compile(params);

    expect(sql).toBe('EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)');
    expect(params).toEqual([]);
  });

  it('should create EXISTS subquery with multiple conditions', () => {
    const [, exists] = User.exists([
      [Order.user_id, parentRef(User.id)],
      [Order.status, 'paid']
    ]);

    const params: unknown[] = [];
    const sql = exists.compile(params);

    expect(sql).toBe('EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND orders.status = ?)');
    expect(params).toEqual(['paid']);
  });
});

describe('DBModel.notExists', () => {
  it('should create NOT EXISTS subquery', () => {
    const [key, exists] = User.notExists([
      [BannedUser.user_id, parentRef(User.id)]
    ]);

    expect(key).toBe('__exists__');

    const params: unknown[] = [];
    const sql = exists.compile(params);

    expect(sql).toBe('NOT EXISTS (SELECT 1 FROM banned_users WHERE banned_users.user_id = users.id)');
    expect(params).toEqual([]);
  });

  it('should create NOT EXISTS subquery with conditions', () => {
    const [, exists] = User.notExists([
      [Complaint.user_id, parentRef(User.id)]
    ]);

    const params: unknown[] = [];
    const sql = exists.compile(params);

    expect(sql).toBe('NOT EXISTS (SELECT 1 FROM complaints WHERE complaints.user_id = users.id)');
    expect(params).toEqual([]);
  });
});

// ============================================
// Integration Tests (DBConditions with DBModel methods)
// ============================================

describe('DBConditions integration with DBModel subquery methods', () => {
  it('should work with inSubquery in DBConditions', () => {
    const [key, subquery] = User.inSubquery(
      [[User.id, Order.user_id]],
      [[Order.status, 'paid']]
    );

    const conditions = new DBConditions({
      is_active: true,
      [key]: subquery
    });

    const params: unknown[] = [];
    const sql = conditions.compile(params);

    expect(sql).toBe('is_active = TRUE AND users.id IN (SELECT orders.user_id FROM orders WHERE orders.status = ?)');
    expect(params).toEqual(['paid']);
  });

  it('should work with exists in DBConditions', () => {
    const [key, exists] = User.exists([
      [Order.user_id, parentRef(User.id)]
    ]);

    const conditions = new DBConditions({
      is_active: true,
      [key]: exists
    });

    const params: unknown[] = [];
    const sql = conditions.compile(params);

    expect(sql).toBe('is_active = TRUE AND EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)');
    expect(params).toEqual([]);
  });

  it('should work with composite key inSubquery', () => {
    const [key, subquery] = User.inSubquery([
      [User.id, Order.user_id],
      [User.group_id, Order.group_id],
    ], [[Order.status, 'active']]);

    const conditions = new DBConditions({
      [key]: subquery
    });

    const params: unknown[] = [];
    const sql = conditions.compile(params);

    expect(sql).toBe('(users.id, users.group_id) IN (SELECT orders.user_id, orders.group_id FROM orders WHERE orders.status = ?)');
    expect(params).toEqual(['active']);
  });
});
