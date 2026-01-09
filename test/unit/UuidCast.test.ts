/**
 * UUID Decorator and SQL Cast Tests
 * 
 * Tests the @column.uuid() decorator behavior across different database drivers.
 */

import { describe, it, expect } from 'vitest';
import { DBCast, DBCastArray, type SqlCastFormatter } from '../../src/DBValues';
import { DBConditions } from '../../src/DBConditions';
import { formatSqlCast, needsSqlCast, getSqlCastFormatter, type DriverType } from '../../src/drivers';

describe('UUID SQL Casting', () => {
  const testUuid = '123e4567-e89b-12d3-a456-426614174000';
  const testUuid2 = '987fcdeb-51a2-43e8-b8c6-123456789abc';

  describe('Driver-specific casting behavior', () => {
    describe('PostgreSQL', () => {
      it('should cast UUID values with ::uuid syntax', () => {
        const formatter = getSqlCastFormatter('postgres');
        expect(formatter('?', 'uuid')).toBe('?::uuid');
      });

      it('should report UUID needs casting', () => {
        expect(needsSqlCast('uuid', 'postgres')).toBe(true);
      });

      it('should format with formatSqlCast function', () => {
        expect(formatSqlCast('?', 'uuid', 'postgres')).toBe('?::uuid');
      });
    });

    describe('SQLite', () => {
      it('should NOT cast UUID values (stored as TEXT)', () => {
        const formatter = getSqlCastFormatter('sqlite');
        expect(formatter('?', 'uuid')).toBe('?');
      });

      it('should report UUID does NOT need casting', () => {
        expect(needsSqlCast('uuid', 'sqlite')).toBe(false);
      });

      it('should format without cast', () => {
        expect(formatSqlCast('?', 'uuid', 'sqlite')).toBe('?');
      });
    });

    describe('MySQL', () => {
      it('should NOT cast UUID values (stored as CHAR(36))', () => {
        const formatter = getSqlCastFormatter('mysql');
        expect(formatter('?', 'uuid')).toBe('?');
      });

      it('should report UUID does NOT need casting', () => {
        expect(needsSqlCast('uuid', 'mysql')).toBe(false);
      });

      it('should format without cast', () => {
        expect(formatSqlCast('?', 'uuid', 'mysql')).toBe('?');
      });
    });
  });

  describe('DBCast class', () => {
    describe('with PostgreSQL formatter', () => {
      const pgFormatter: SqlCastFormatter = (p, t) => `${p}::${t}`;

      it('should compile single UUID value with cast', () => {
        const cast = new DBCast(testUuid, 'uuid');
        const params: unknown[] = [];
        const sql = cast.compile(params, 'id', pgFormatter);
        
        expect(sql).toBe('id = ?::uuid');
        expect(params).toEqual([testUuid]);
      });

      it('should compile with custom operator', () => {
        const cast = new DBCast(testUuid, 'uuid', '!=');
        const params: unknown[] = [];
        const sql = cast.compile(params, 'id', pgFormatter);
        
        expect(sql).toBe('id != ?::uuid');
        expect(params).toEqual([testUuid]);
      });

      it('should compile without key', () => {
        const cast = new DBCast(testUuid, 'uuid');
        const params: unknown[] = [];
        const sql = cast.compile(params, undefined, pgFormatter);
        
        expect(sql).toBe('?::uuid');
        expect(params).toEqual([testUuid]);
      });
    });

    describe('with SQLite/MySQL formatter (no cast)', () => {
      const noCastFormatter: SqlCastFormatter = (p) => p;

      it('should compile single UUID value without cast', () => {
        const cast = new DBCast(testUuid, 'uuid');
        const params: unknown[] = [];
        const sql = cast.compile(params, 'id', noCastFormatter);
        
        expect(sql).toBe('id = ?');
        expect(params).toEqual([testUuid]);
      });

      it('should compile with custom operator without cast', () => {
        const cast = new DBCast(testUuid, 'uuid', '!=');
        const params: unknown[] = [];
        const sql = cast.compile(params, 'id', noCastFormatter);
        
        expect(sql).toBe('id != ?');
        expect(params).toEqual([testUuid]);
      });
    });

    describe('without formatter (default PostgreSQL-style)', () => {
      it('should use default PostgreSQL-style casting', () => {
        const cast = new DBCast(testUuid, 'uuid');
        const params: unknown[] = [];
        const sql = cast.compile(params, 'id');
        
        expect(sql).toBe('id = ?::uuid');
        expect(params).toEqual([testUuid]);
      });
    });
  });

  describe('DBCastArray class', () => {
    describe('with PostgreSQL formatter', () => {
      const pgFormatter: SqlCastFormatter = (p, t) => `${p}::${t}`;

      it('should compile IN clause with cast', () => {
        const castArr = new DBCastArray([testUuid, testUuid2], 'uuid');
        const params: unknown[] = [];
        const sql = castArr.compile(params, 'id', pgFormatter);
        
        expect(sql).toBe('id IN (?::uuid, ?::uuid)');
        expect(params).toEqual([testUuid, testUuid2]);
      });

      it('should handle empty array', () => {
        const castArr = new DBCastArray([], 'uuid');
        const params: unknown[] = [];
        const sql = castArr.compile(params, 'id', pgFormatter);
        
        expect(sql).toBe('1 = 0');
        expect(params).toEqual([]);
      });

      it('should compile without key', () => {
        const castArr = new DBCastArray([testUuid], 'uuid');
        const params: unknown[] = [];
        const sql = castArr.compile(params, undefined, pgFormatter);
        
        expect(sql).toBe('(?::uuid)');
        expect(params).toEqual([testUuid]);
      });
    });

    describe('with SQLite/MySQL formatter (no cast)', () => {
      const noCastFormatter: SqlCastFormatter = (p) => p;

      it('should compile IN clause without cast', () => {
        const castArr = new DBCastArray([testUuid, testUuid2], 'uuid');
        const params: unknown[] = [];
        const sql = castArr.compile(params, 'id', noCastFormatter);
        
        expect(sql).toBe('id IN (?, ?)');
        expect(params).toEqual([testUuid, testUuid2]);
      });
    });
  });

  describe('DBConditions with DBCast', () => {
    describe('with PostgreSQL formatter', () => {
      const pgFormatter = getSqlCastFormatter('postgres');

      it('should compile condition with DBCast', () => {
        const cond = new DBConditions({
          id: new DBCast(testUuid, 'uuid'),
        });
        const params: unknown[] = [];
        const sql = cond.compile(params, pgFormatter);
        
        expect(sql).toBe('id = ?::uuid');
        expect(params).toEqual([testUuid]);
      });

      it('should compile condition with DBCastArray', () => {
        const cond = new DBConditions({
          id: new DBCastArray([testUuid, testUuid2], 'uuid'),
        });
        const params: unknown[] = [];
        const sql = cond.compile(params, pgFormatter);
        
        expect(sql).toBe('id IN (?::uuid, ?::uuid)');
        expect(params).toEqual([testUuid, testUuid2]);
      });

      it('should mix DBCast with regular conditions', () => {
        const cond = new DBConditions({
          id: new DBCast(testUuid, 'uuid'),
          name: 'test',
          active: true,
        });
        const params: unknown[] = [];
        const sql = cond.compile(params, pgFormatter);
        
        expect(sql).toContain('id = ?::uuid');
        expect(sql).toContain('name = ?');
        expect(sql).toContain('active = TRUE');
        expect(params).toContain(testUuid);
        expect(params).toContain('test');
      });
    });

    describe('with SQLite formatter', () => {
      const sqliteFormatter = getSqlCastFormatter('sqlite');

      it('should compile condition with DBCast (no cast)', () => {
        const cond = new DBConditions({
          id: new DBCast(testUuid, 'uuid'),
        });
        const params: unknown[] = [];
        const sql = cond.compile(params, sqliteFormatter);
        
        expect(sql).toBe('id = ?');
        expect(params).toEqual([testUuid]);
      });

      it('should compile condition with DBCastArray (no cast)', () => {
        const cond = new DBConditions({
          id: new DBCastArray([testUuid, testUuid2], 'uuid'),
        });
        const params: unknown[] = [];
        const sql = cond.compile(params, sqliteFormatter);
        
        expect(sql).toBe('id IN (?, ?)');
        expect(params).toEqual([testUuid, testUuid2]);
      });
    });

    describe('with MySQL formatter', () => {
      const mysqlFormatter = getSqlCastFormatter('mysql');

      it('should compile condition with DBCast (no cast)', () => {
        const cond = new DBConditions({
          id: new DBCast(testUuid, 'uuid'),
        });
        const params: unknown[] = [];
        const sql = cond.compile(params, mysqlFormatter);
        
        expect(sql).toBe('id = ?');
        expect(params).toEqual([testUuid]);
      });
    });

    describe('without formatter', () => {
      it('should use default PostgreSQL-style casting', () => {
        const cond = new DBConditions({
          id: new DBCast(testUuid, 'uuid'),
        });
        const params: unknown[] = [];
        const sql = cond.compile(params);
        
        expect(sql).toBe('id = ?::uuid');
        expect(params).toEqual([testUuid]);
      });
    });
  });

  describe('OR conditions with DBCast', () => {
    const pgFormatter = getSqlCastFormatter('postgres');

    it('should compile __or__ with DBCast', () => {
      const cond = new DBConditions({
        __or__: [
          { id: new DBCast(testUuid, 'uuid') },
          { id: new DBCast(testUuid2, 'uuid') },
        ],
      });
      const params: unknown[] = [];
      const sql = cond.compile(params, pgFormatter);
      
      expect(sql).toBe('((id = ?::uuid) OR (id = ?::uuid))');
      expect(params).toEqual([testUuid, testUuid2]);
    });
  });

  describe('Nested conditions with DBCast', () => {
    const pgFormatter = getSqlCastFormatter('postgres');

    it('should compile nested DBConditions with DBCast via add()', () => {
      const cond = new DBConditions({
        status: 'active',
      });
      cond.add({
        id: new DBCast(testUuid, 'uuid'),
      });
      
      const params: unknown[] = [];
      const sql = cond.compile(params, pgFormatter);
      
      expect(sql).toContain('status = ?');
      expect(sql).toContain('id = ?::uuid');
      expect(params).toContain('active');
      expect(params).toContain(testUuid);
    });
  });
});

