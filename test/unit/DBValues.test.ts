/**
 * DBValues Tests
 */

import { describe, it, expect } from 'vitest';
import {
  DBToken,
  DBImmediateValue,
  DBNullValue,
  DBNotNullValue,
  DBBoolValue,
  DBArrayValue,
  DBDynamicValue,
  DBRawValue,
  DBTupleIn,
  dbNull,
  dbNotNull,
  dbTrue,
  dbFalse,
  dbNow,
  dbIn,
  dbDynamic,
  dbRaw,
  dbImmediate,
  dbTupleIn,
} from '../../src/DBValues';

describe('DBToken', () => {
  describe('constructor', () => {
    it('should create token with default operator', () => {
      const token = new DBToken('test');
      expect(token.value).toBe('test');
      expect(token.operator).toBe('=');
    });

    it('should create token with custom operator', () => {
      const token = new DBToken('test', '>');
      expect(token.operator).toBe('>');
    });
  });

  describe('compile', () => {
    it('should add value to params and return placeholder', () => {
      const token = new DBToken('test');
      const params: unknown[] = [];
      const sql = token.compile(params);
      expect(sql).toBe('?');
      expect(params).toEqual(['test']);
    });

    it('should compile with key', () => {
      const token = new DBToken('test', '>');
      const params: unknown[] = [];
      const sql = token.compile(params, 'age');
      expect(sql).toBe('age > ?');
      expect(params).toEqual(['test']);
    });

    it('should use correct param index', () => {
      const token = new DBToken('test');
      const params: unknown[] = ['existing'];
      const sql = token.compile(params);
      expect(sql).toBe('?');
      expect(params).toEqual(['existing', 'test']);
    });
  });
});

describe('DBImmediateValue', () => {
  describe('compile', () => {
    it('should return literal value without adding to params', () => {
      const imm = new DBImmediateValue('NOW()');
      const params: unknown[] = [];
      const sql = imm.compile(params);
      expect(sql).toBe('NOW()');
      expect(params).toEqual([]);
    });

    it('should compile with key', () => {
      const imm = new DBImmediateValue('NOW()');
      const params: unknown[] = [];
      const sql = imm.compile(params, 'created_at');
      expect(sql).toBe('created_at = NOW()');
    });
  });
});

describe('DBNullValue', () => {
  describe('constructor', () => {
    it('should have IS operator', () => {
      const nullVal = new DBNullValue();
      expect(nullVal.value).toBe('NULL');
      expect(nullVal.operator).toBe('IS');
    });
  });

  describe('compile', () => {
    it('should return NULL literal', () => {
      const nullVal = new DBNullValue();
      const params: unknown[] = [];
      expect(nullVal.compile(params)).toBe('NULL');
      expect(params).toEqual([]);
    });

    it('should compile IS NULL with key', () => {
      const nullVal = new DBNullValue();
      const params: unknown[] = [];
      expect(nullVal.compile(params, 'deleted_at')).toBe('deleted_at IS NULL');
    });
  });
});

describe('DBNotNullValue', () => {
  describe('constructor', () => {
    it('should have IS operator', () => {
      const notNull = new DBNotNullValue();
      expect(notNull.value).toBe('NOT NULL');
      expect(notNull.operator).toBe('IS');
    });
  });

  describe('compile', () => {
    it('should return NOT NULL literal', () => {
      const notNull = new DBNotNullValue();
      const params: unknown[] = [];
      expect(notNull.compile(params)).toBe('NOT NULL');
    });

    it('should compile IS NOT NULL with key', () => {
      const notNull = new DBNotNullValue();
      const params: unknown[] = [];
      expect(notNull.compile(params, 'email')).toBe('email IS NOT NULL');
    });
  });
});

describe('DBBoolValue', () => {
  it('should create TRUE value', () => {
    const boolVal = new DBBoolValue(true);
    expect(boolVal.value).toBe('TRUE');
  });

  it('should create FALSE value', () => {
    const boolVal = new DBBoolValue(false);
    expect(boolVal.value).toBe('FALSE');
  });

  it('should compile as immediate value', () => {
    const boolVal = new DBBoolValue(true);
    const params: unknown[] = [];
    expect(boolVal.compile(params, 'is_active')).toBe('is_active = TRUE');
    expect(params).toEqual([]);
  });
});

describe('DBArrayValue', () => {
  describe('compile', () => {
    it('should compile array as IN clause', () => {
      const arrVal = new DBArrayValue([1, 2, 3]);
      const params: unknown[] = [];
      const sql = arrVal.compile(params, 'id');
      expect(sql).toBe('id IN (?, ?, ?)');
      expect(params).toEqual([1, 2, 3]);
    });

    it('should compile without key', () => {
      const arrVal = new DBArrayValue([1, 2]);
      const params: unknown[] = [];
      const sql = arrVal.compile(params);
      expect(sql).toBe('(?, ?)');
    });

    it('should return 1=0 for empty array', () => {
      const arrVal = new DBArrayValue([]);
      const params: unknown[] = [];
      const sql = arrVal.compile(params, 'id');
      expect(sql).toBe('1 = 0');
      expect(params).toEqual([]);
    });

    it('should use correct param indices', () => {
      const arrVal = new DBArrayValue(['a', 'b']);
      const params: unknown[] = ['existing'];
      const sql = arrVal.compile(params, 'status');
      expect(sql).toBe('status IN (?, ?)');
      expect(params).toEqual(['existing', 'a', 'b']);
    });
  });
});

describe('DBDynamicValue', () => {
  describe('constructor', () => {
    it('should create with function and values', () => {
      const date = new Date();
      const dynVal = new DBDynamicValue('DATE_ADD(?, INTERVAL ? DAY)', [date, 7]);
      expect(dynVal.func).toBe('DATE_ADD(?, INTERVAL ? DAY)');
      expect(dynVal.values[0]).toBe(date);
      expect(dynVal.values[1]).toBe(7);
    });

    it('should default to empty values', () => {
      const dynVal = new DBDynamicValue('CURRENT_TIMESTAMP');
      expect(dynVal.values).toEqual([]);
    });
  });

  describe('compile', () => {
    it('should replace placeholders with params', () => {
      const dynVal = new DBDynamicValue('UPPER(?)', ['test']);
      const params: unknown[] = [];
      const sql = dynVal.compile(params);
      expect(sql).toBe('UPPER(?)');
      expect(params).toEqual(['test']);
    });

    it('should compile with key', () => {
      const dynVal = new DBDynamicValue('LOWER(?)', ['TEST']);
      const params: unknown[] = [];
      const sql = dynVal.compile(params, 'name');
      expect(sql).toBe('name = LOWER(?)');
    });

    it('should handle multiple placeholders', () => {
      const dynVal = new DBDynamicValue('CONCAT(?, ?)', ['Hello', 'World']);
      const params: unknown[] = [];
      const sql = dynVal.compile(params);
      expect(sql).toBe('CONCAT(?, ?)');
      expect(params).toEqual(['Hello', 'World']);
    });

    it('should work without placeholders', () => {
      const dynVal = new DBDynamicValue('NOW()');
      const params: unknown[] = [];
      const sql = dynVal.compile(params);
      expect(sql).toBe('NOW()');
    });
  });
});

describe('DBRawValue', () => {
  describe('compile', () => {
    it('should return raw SQL', () => {
      const raw = new DBRawValue('users.id');
      const params: unknown[] = [];
      expect(raw.compile(params)).toBe('users.id');
    });

    it('should compile with key', () => {
      const raw = new DBRawValue('(SELECT MAX(id) FROM users)');
      const params: unknown[] = [];
      expect(raw.compile(params, 'max_id')).toBe('max_id = (SELECT MAX(id) FROM users)');
    });
  });
});

describe('Factory functions', () => {
  describe('dbNull', () => {
    it('should create DBNullValue', () => {
      const val = dbNull();
      expect(val).toBeInstanceOf(DBNullValue);
    });
  });

  describe('dbNotNull', () => {
    it('should create DBNotNullValue', () => {
      const val = dbNotNull();
      expect(val).toBeInstanceOf(DBNotNullValue);
    });
  });

  describe('dbTrue', () => {
    it('should create DBBoolValue(true)', () => {
      const val = dbTrue();
      expect(val).toBeInstanceOf(DBBoolValue);
      expect(val.value).toBe('TRUE');
    });
  });

  describe('dbFalse', () => {
    it('should create DBBoolValue(false)', () => {
      const val = dbFalse();
      expect(val).toBeInstanceOf(DBBoolValue);
      expect(val.value).toBe('FALSE');
    });
  });

  describe('dbNow', () => {
    it('should create NOW() immediate value', () => {
      const val = dbNow();
      expect(val).toBeInstanceOf(DBImmediateValue);
      expect(val.value).toBe('NOW()');
    });
  });

  describe('dbIn', () => {
    it('should create DBArrayValue', () => {
      const val = dbIn([1, 2, 3]);
      expect(val).toBeInstanceOf(DBArrayValue);
      expect(val.value).toEqual([1, 2, 3]);
    });
  });

  describe('dbDynamic', () => {
    it('should create DBDynamicValue', () => {
      const val = dbDynamic('UPPER(?)', ['test']);
      expect(val).toBeInstanceOf(DBDynamicValue);
      expect(val.func).toBe('UPPER(?)');
      expect(val.values).toEqual(['test']);
    });

    it('should default to empty values', () => {
      const val = dbDynamic('NOW()');
      expect(val.values).toEqual([]);
    });
  });

  describe('dbRaw', () => {
    it('should create DBRawValue', () => {
      const val = dbRaw('SELECT 1');
      expect(val).toBeInstanceOf(DBRawValue);
      expect(val.value).toBe('SELECT 1');
    });
  });

  describe('dbImmediate', () => {
    it('should create DBImmediateValue', () => {
      const val = dbImmediate('CURRENT_DATE');
      expect(val).toBeInstanceOf(DBImmediateValue);
      expect(val.value).toBe('CURRENT_DATE');
    });
  });

  describe('dbTupleIn', () => {
    it('should create DBTupleIn', () => {
      const val = dbTupleIn(['col1', 'col2'], [[1, 2], [3, 4]]);
      expect(val).toBeInstanceOf(DBTupleIn);
      expect(val.columns).toEqual(['col1', 'col2']);
      expect(val.tuples).toEqual([[1, 2], [3, 4]]);
    });

    it('should compile to tuple IN clause', () => {
      const val = dbTupleIn(['tenant_id', 'id'], [[1, 10], [2, 20]]);
      const params: unknown[] = [];
      const sql = val.compile(params);
      
      expect(sql).toBe('(tenant_id, id) IN ((?, ?), (?, ?))');
      expect(params).toEqual([1, 10, 2, 20]);
    });

    it('should compile with three columns', () => {
      const val = dbTupleIn(['a', 'b', 'c'], [[1, 2, 3], [4, 5, 6]]);
      const params: unknown[] = [];
      const sql = val.compile(params);
      
      expect(sql).toBe('(a, b, c) IN ((?, ?, ?), (?, ?, ?))');
      expect(params).toEqual([1, 2, 3, 4, 5, 6]);
    });

    it('should handle empty tuples', () => {
      const val = dbTupleIn(['col1', 'col2'], []);
      const params: unknown[] = [];
      const sql = val.compile(params);
      
      expect(sql).toBe('1 = 0');
      expect(params).toEqual([]);
    });

    it('should ignore key parameter', () => {
      const val = dbTupleIn(['col1', 'col2'], [[1, 2]]);
      const params: unknown[] = [];
      const sql = val.compile(params, 'ignored_key');
      
      expect(sql).toBe('(col1, col2) IN ((?, ?))');
    });
  });
});

