/**
 * Column Tests
 */

import { describe, it, expect } from 'vitest';
import {
  createColumn,
  isColumn,
  OrderColumn,
  isOrderColumn,
  orderToString,
  columnsToNames,
  Values,
  Conditions,
  SKIP,
  pairsToRecord,
  condsToRecord,
  isOrCond,
  createOrCond,
} from '../../src/Column';
import { DBNotNullValue } from '../../src/DBValues';

describe('Column', () => {
  describe('createColumn', () => {
    it('should create a callable column function', () => {
      const col = createColumn<number, unknown>('id', 'users', 'User');
      expect(col()).toBe('id');
    });

    it('should have columnName property', () => {
      const col = createColumn<string, unknown>('name', 'users', 'User');
      expect(col.columnName).toBe('name');
    });

    it('should have tableName property', () => {
      const col = createColumn<string, unknown>('name', 'users', 'User');
      expect(col.tableName).toBe('users');
    });

    it('should have modelName property', () => {
      const col = createColumn<string, unknown>('name', 'users', 'User');
      expect(col.modelName).toBe('User');
    });

    it('should have _brand property', () => {
      const col = createColumn<string, unknown>('name', 'users', 'User');
      expect(col._brand).toBe('Column');
    });

    it('toString should return column name', () => {
      const col = createColumn<string, unknown>('name', 'users', 'User');
      expect(col.toString()).toBe('name');
      expect(`${col}`).toBe('name');
    });
  });

  describe('Condition builder methods', () => {
    const col = createColumn<number, unknown>('age', 'users', 'User');

    it('eq should create equality condition', () => {
      expect(col.eq(25)).toEqual({ age: 25 });
    });

    it('ne should create not equal condition', () => {
      expect(col.ne(25)).toEqual({ 'age != ?': 25 });
    });

    it('gt should create greater than condition', () => {
      expect(col.gt(18)).toEqual({ 'age > ?': 18 });
    });

    it('gte should create greater than or equal condition', () => {
      expect(col.gte(18)).toEqual({ 'age >= ?': 18 });
    });

    it('lt should create less than condition', () => {
      expect(col.lt(65)).toEqual({ 'age < ?': 65 });
    });

    it('lte should create less than or equal condition', () => {
      expect(col.lte(65)).toEqual({ 'age <= ?': 65 });
    });

    it('like should create LIKE condition', () => {
      const nameCol = createColumn<string, unknown>('name', 'users', 'User');
      expect(nameCol.like('%test%')).toEqual({ 'name LIKE ?': '%test%' });
    });

    it('notLike should create NOT LIKE condition', () => {
      const nameCol = createColumn<string, unknown>('name', 'users', 'User');
      expect(nameCol.notLike('%test%')).toEqual({ 'name NOT LIKE ?': '%test%' });
    });

    it('ilike should create ILIKE condition', () => {
      const nameCol = createColumn<string, unknown>('name', 'users', 'User');
      expect(nameCol.ilike('%TEST%')).toEqual({ 'name ILIKE ?': '%TEST%' });
    });

    it('between should create BETWEEN condition', () => {
      expect(col.between(18, 65)).toEqual({ 'age BETWEEN ? AND ?': [18, 65] });
    });

    it('in should create IN condition', () => {
      expect(col.in([1, 2, 3])).toEqual({ age: [1, 2, 3] });
    });

    it('notIn should create NOT IN condition', () => {
      expect(col.notIn([1, 2, 3])).toEqual({ 'age NOT IN (?)': [1, 2, 3] });
    });

    it('isNull should create IS NULL condition', () => {
      expect(col.isNull()).toEqual({ age: null });
    });

    it('isNotNull should create IS NOT NULL condition', () => {
      const result = col.isNotNull();
      expect(result.age).toBeInstanceOf(DBNotNullValue);
    });
  });

  describe('Order by methods', () => {
    const col = createColumn<Date, unknown>('created_at', 'users', 'User');

    it('asc should create ascending OrderColumn', () => {
      const order = col.asc();
      expect(order).toBeInstanceOf(OrderColumn);
      expect(order.columnName).toBe('created_at');
      expect(order.direction).toBe('ASC');
      expect(order.nulls).toBeNull();
    });

    it('desc should create descending OrderColumn', () => {
      const order = col.desc();
      expect(order.direction).toBe('DESC');
    });

    it('ascNullsFirst should create ascending with NULLS FIRST', () => {
      const order = col.ascNullsFirst();
      expect(order.direction).toBe('ASC');
      expect(order.nulls).toBe('FIRST');
    });

    it('ascNullsLast should create ascending with NULLS LAST', () => {
      const order = col.ascNullsLast();
      expect(order.direction).toBe('ASC');
      expect(order.nulls).toBe('LAST');
    });

    it('descNullsFirst should create descending with NULLS FIRST', () => {
      const order = col.descNullsFirst();
      expect(order.direction).toBe('DESC');
      expect(order.nulls).toBe('FIRST');
    });

    it('descNullsLast should create descending with NULLS LAST', () => {
      const order = col.descNullsLast();
      expect(order.direction).toBe('DESC');
      expect(order.nulls).toBe('LAST');
    });
  });

  describe('isColumn', () => {
    it('should return true for Column instances', () => {
      const col = createColumn<number, unknown>('id', 'users', 'User');
      expect(isColumn(col)).toBe(true);
    });

    it('should return false for non-Column values', () => {
      expect(isColumn(null)).toBe(false);
      expect(isColumn(undefined)).toBe(false);
      expect(isColumn('id')).toBe(false);
      expect(isColumn(123)).toBe(false);
      expect(isColumn({})).toBe(false);
      expect(isColumn(() => {})).toBe(false);
    });
  });
});

describe('OrderColumn', () => {
  describe('constructor', () => {
    it('should create OrderColumn with defaults', () => {
      const order = new OrderColumn('name', 'User', 'ASC');
      expect(order.columnName).toBe('name');
      expect(order.modelName).toBe('User');
      expect(order.direction).toBe('ASC');
      expect(order.nulls).toBeNull();
    });

    it('should create OrderColumn with NULLS position', () => {
      const order = new OrderColumn('name', 'User', 'DESC', 'LAST');
      expect(order.nulls).toBe('LAST');
    });
  });

  describe('toString', () => {
    it('should return SQL string without NULLS', () => {
      const order = new OrderColumn('name', 'User', 'ASC');
      expect(order.toString()).toBe('name ASC');
    });

    it('should return SQL string with NULLS FIRST', () => {
      const order = new OrderColumn('name', 'User', 'DESC', 'FIRST');
      expect(order.toString()).toBe('name DESC NULLS FIRST');
    });

    it('should return SQL string with NULLS LAST', () => {
      const order = new OrderColumn('name', 'User', 'ASC', 'LAST');
      expect(order.toString()).toBe('name ASC NULLS LAST');
    });
  });

  describe('isOrderColumn', () => {
    it('should return true for OrderColumn instances', () => {
      const order = new OrderColumn('name', 'User', 'ASC');
      expect(isOrderColumn(order)).toBe(true);
    });

    it('should return true for objects with _brand', () => {
      const obj = { _brand: 'OrderColumn' as const, columnName: 'name', direction: 'ASC' };
      expect(isOrderColumn(obj)).toBe(true);
    });

    it('should return false for non-OrderColumn values', () => {
      expect(isOrderColumn(null)).toBe(false);
      expect(isOrderColumn(undefined)).toBe(false);
      expect(isOrderColumn('name ASC')).toBe(false);
      expect(isOrderColumn({})).toBe(false);
    });
  });
});

describe('orderToString', () => {
  it('should return undefined for null/undefined', () => {
    expect(orderToString(null)).toBeUndefined();
    expect(orderToString(undefined)).toBeUndefined();
  });

  it('should pass through string as-is', () => {
    expect(orderToString('name ASC')).toBe('name ASC');
  });

  it('should convert single OrderColumn to string', () => {
    const order = new OrderColumn('name', 'User', 'ASC');
    expect(orderToString(order)).toBe('name ASC');
  });

  it('should convert array of OrderColumns to string', () => {
    const orders = [
      new OrderColumn('name', 'User', 'ASC'),
      new OrderColumn('created_at', 'User', 'DESC'),
    ];
    expect(orderToString(orders)).toBe('name ASC, created_at DESC');
  });
});

describe('columnsToNames', () => {
  it('should convert columns to names array', () => {
    const cols = [
      createColumn<number, unknown>('id', 'users', 'User'),
      createColumn<string, unknown>('name', 'users', 'User'),
    ];
    expect(columnsToNames(cols)).toEqual(['id', 'name']);
  });

  it('should return empty array for empty input', () => {
    expect(columnsToNames([])).toEqual([]);
  });
});

describe('Values builder', () => {
  const idCol = createColumn<number, unknown>('id', 'users', 'User');
  const nameCol = createColumn<string, unknown>('name', 'users', 'User');

  it('should build empty array initially', () => {
    const values = new Values();
    expect(values.build()).toEqual([]);
    expect(values.length).toBe(0);
  });

  it('should add values', () => {
    const values = new Values();
    values.add(nameCol, 'John');
    values.add(idCol, 1);
    expect(values.length).toBe(2);
    const built = values.build();
    expect(built[0][0]).toBe(nameCol);
    expect(built[0][1]).toBe('John');
  });

  it('should chain add calls', () => {
    const values = new Values()
      .add(nameCol, 'John')
      .add(idCol, 1);
    expect(values.length).toBe(2);
  });

  it('should initialize with initial values', () => {
    const values = new Values([[nameCol, 'Initial']]);
    expect(values.length).toBe(1);
    values.add(idCol, 1);
    expect(values.length).toBe(2);
  });
});

describe('Conditions builder', () => {
  const idCol = createColumn<number, unknown>('id', 'users', 'User');
  const nameCol = createColumn<string, unknown>('name', 'users', 'User');

  it('should build empty array initially', () => {
    const conds = new Conditions();
    expect(conds.build()).toEqual([]);
    expect(conds.length).toBe(0);
  });

  it('should add conditions', () => {
    const conds = new Conditions();
    conds.add(idCol, 1);
    conds.add(nameCol, 'John');
    expect(conds.length).toBe(2);
  });

  it('should add raw conditions with value', () => {
    const conds = new Conditions();
    conds.addRaw('age > ?', 18);
    expect(conds.length).toBe(1);
  });

  it('should add raw conditions without value', () => {
    const conds = new Conditions();
    conds.addRaw('deleted_at IS NULL');
    expect(conds.length).toBe(1);
  });

  it('should add OR conditions', () => {
    const conds = new Conditions();
    conds.or([[idCol, 1]], [[idCol, 2]]);
    expect(conds.length).toBe(1);
  });

  it('should chain calls', () => {
    const conds = new Conditions()
      .add(idCol, 1)
      .addRaw('age > ?', 18)
      .or([[nameCol, 'A']], [[nameCol, 'B']]);
    expect(conds.length).toBe(3);
  });

  it('should initialize with initial conditions', () => {
    const conds = new Conditions([[idCol, 1]]);
    expect(conds.length).toBe(1);
    conds.add(nameCol, 'John');
    expect(conds.length).toBe(2);
  });
});

describe('SKIP and pairsToRecord', () => {
  const idCol = createColumn<number, unknown>('id', 'users', 'User');
  const nameCol = createColumn<string, unknown>('name', 'users', 'User');

  it('SKIP should be a unique symbol', () => {
    expect(typeof SKIP).toBe('symbol');
    expect(SKIP).toBe(Symbol.for('litedbmodel.SKIP'));
  });

  it('pairsToRecord should convert pairs to record', () => {
    const pairs: [typeof idCol | typeof nameCol, unknown][] = [
      [idCol, 1],
      [nameCol, 'John'],
    ];
    expect(pairsToRecord(pairs)).toEqual({ id: 1, name: 'John' });
  });

  it('pairsToRecord should skip SKIP values', () => {
    const pairs: [typeof idCol | typeof nameCol, unknown][] = [
      [idCol, 1],
      [nameCol, SKIP],
    ];
    expect(pairsToRecord(pairs)).toEqual({ id: 1 });
  });

  it('pairsToRecord should handle empty array', () => {
    expect(pairsToRecord([])).toEqual({});
  });
});

describe('condsToRecord', () => {
  const idCol = createColumn<number, unknown>('id', 'users', 'User');
  const nameCol = createColumn<string, unknown>('name', 'users', 'User');

  it('should convert column conditions', () => {
    const conds = [[idCol, 1], [nameCol, 'John']] as const;
    expect(condsToRecord(conds)).toEqual({ id: 1, name: 'John' });
  });

  it('should convert string conditions', () => {
    const conds = [['age > ?', 18] as const] as const;
    expect(condsToRecord(conds)).toEqual({ 'age > ?': 18 });
  });

  it('should handle string-only conditions', () => {
    const conds = [['deleted_at IS NULL'] as const] as const;
    expect(condsToRecord(conds)).toEqual({ 'deleted_at IS NULL': true });
  });

  it('should skip SKIP elements', () => {
    const conds = [[idCol, 1], SKIP, [nameCol, 'John']] as const;
    expect(condsToRecord(conds)).toEqual({ id: 1, name: 'John' });
  });

  it('should skip conditions with SKIP value', () => {
    const conds = [[idCol, 1], [nameCol, SKIP]] as const;
    expect(condsToRecord(conds)).toEqual({ id: 1 });
  });

  it('should handle OR conditions', () => {
    const orCond = createOrCond([[[idCol, 1]], [[idCol, 2]]]);
    const conds = [[nameCol, 'John'], orCond] as const;
    const result = condsToRecord(conds);
    expect(result.name).toBe('John');
    expect(result.__or__).toBeDefined();
  });

  it('should return empty object for empty array', () => {
    expect(condsToRecord([])).toEqual({});
  });

  it('should handle composite key IN condition', () => {
    const tenantCol = createColumn<number, unknown>('tenant_id', 'users', 'User');
    const idCol = createColumn<number, unknown>('id', 'users', 'User');
    const conds = [
      [[tenantCol, idCol], [[1, 10], [1, 20], [2, 30]]] as const,
    ] as const;
    const result = condsToRecord(conds);
    
    // Should create __tuple__ key with DBTupleIn instance
    expect(result.__tuple__).toBeDefined();
    
    // Compile and verify SQL
    const params: unknown[] = [];
    const sql = (result.__tuple__ as { compile: (p: unknown[]) => string }).compile(params);
    expect(sql).toBe('(tenant_id, id) IN ((?, ?), (?, ?), (?, ?))');
    expect(params).toEqual([1, 10, 1, 20, 2, 30]);
  });
});

describe('isOrCond', () => {
  it('should return true for OR conditions', () => {
    const orCond = createOrCond([]);
    expect(isOrCond(orCond)).toBe(true);
  });

  it('should return false for regular conditions', () => {
    const col = createColumn<number, unknown>('id', 'users', 'User');
    expect(isOrCond([col, 1])).toBe(false);
  });

  it('should return false for non-objects', () => {
    expect(isOrCond(null)).toBe(false);
    expect(isOrCond(undefined)).toBe(false);
    expect(isOrCond('string')).toBe(false);
  });
});

describe('createOrCond', () => {
  it('should create OR condition marker', () => {
    const col = createColumn<number, unknown>('id', 'users', 'User');
    const orCond = createOrCond([[[col, 1]], [[col, 2]]]);
    expect(orCond._type).toBe('or');
    expect(orCond.conditions.length).toBe(2);
  });

  it('should work with empty conditions', () => {
    const orCond = createOrCond([]);
    expect(orCond._type).toBe('or');
    expect(orCond.conditions).toEqual([]);
  });
});

