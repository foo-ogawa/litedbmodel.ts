/**
 * DBConditions Tests
 */

import { describe, it, expect } from 'vitest';
import { DBConditions, DBOrConditions, and, or } from '../../src/DBConditions';
import { DBBoolValue, DBNullValue, DBArrayValue, DBDynamicValue } from '../../src/DBValues';

describe('DBConditions', () => {
  describe('compile', () => {
    it('should compile simple equality condition', () => {
      const cond = new DBConditions({ id: 1 });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('id = $1');
      expect(params).toEqual([1]);
    });

    it('should compile multiple conditions with AND', () => {
      const cond = new DBConditions({ id: 1, name: 'test' });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('id = $1 AND name = $2');
      expect(params).toEqual([1, 'test']);
    });

    it('should handle null values', () => {
      const cond = new DBConditions({ deleted_at: null });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('deleted_at IS NULL');
      expect(params).toEqual([]);
    });

    it('should handle boolean values', () => {
      const cond = new DBConditions({ is_active: true, is_deleted: false });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('is_active = TRUE AND is_deleted = FALSE');
      expect(params).toEqual([]);
    });

    it('should handle array values (IN clause)', () => {
      const cond = new DBConditions({ id: [1, 2, 3] });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('id IN ($1, $2, $3)');
      expect(params).toEqual([1, 2, 3]);
    });

    it('should handle empty array', () => {
      const cond = new DBConditions({ id: [] });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('1 = 0');
      expect(params).toEqual([]);
    });

    it('should handle custom operators', () => {
      const cond = new DBConditions({ 'amount > ?': 1000 });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('amount > $1');
      expect(params).toEqual([1000]);
    });

    it('should handle BETWEEN operator', () => {
      const cond = new DBConditions({
        'created_at BETWEEN ? AND ?': [new Date('2024-01-01'), new Date('2024-12-31')],
      });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('created_at BETWEEN $1 AND $2');
      expect(params.length).toBe(2);
    });

    it('should handle DBBoolValue', () => {
      const cond = new DBConditions({ is_active: new DBBoolValue(true) });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('is_active = TRUE');
      expect(params).toEqual([]);
    });

    it('should handle DBNullValue', () => {
      const cond = new DBConditions({ deleted_at: new DBNullValue() });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('deleted_at IS NULL');
      expect(params).toEqual([]);
    });

    it('should handle DBArrayValue', () => {
      const cond = new DBConditions({ status: new DBArrayValue(['active', 'pending']) });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('status IN ($1, $2)');
      expect(params).toEqual(['active', 'pending']);
    });

    it('should handle DBDynamicValue', () => {
      const cond = new DBConditions({
        updated_at: new DBDynamicValue('NOW()', []),
      });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('updated_at = NOW()');
      expect(params).toEqual([]);
    });
  });

  describe('DBOrConditions', () => {
    it('should compile with OR operator', () => {
      const cond = new DBOrConditions({ status: 'active', role: 'admin' });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('status = $1 OR role = $2');
      expect(params).toEqual(['active', 'admin']);
    });
  });

  describe('nested conditions', () => {
    it('should handle nested conditions with __ prefix', () => {
      const cond = new DBConditions({
        user_id: 1,
        __or1: new DBOrConditions({ status: 'active', status2: 'pending' }),
      });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toContain('user_id = $1');
      expect(sql).toContain('status = $2 OR status2 = $3');
    });

    it('should handle add method', () => {
      const cond = new DBConditions({ user_id: 1 });
      cond.add(new DBOrConditions({ status: 'active', role: 'admin' }), 'AND');

      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toContain('user_id = $1');
      expect(sql).toContain('status = $2 OR role = $3');
    });
  });

  describe('helper functions', () => {
    it('and() should create AND conditions', () => {
      const cond = and({ id: 1, name: 'test' });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('id = $1 AND name = $2');
    });

    it('or() should create OR conditions', () => {
      const cond = or({ id: 1, name: 'test' });
      const params: unknown[] = [];
      const sql = cond.compile(params);

      expect(sql).toBe('id = $1 OR name = $2');
    });
  });
});

