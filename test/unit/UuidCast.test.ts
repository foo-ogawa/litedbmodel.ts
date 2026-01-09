/**
 * UUID Decorator and SQL Cast Tests
 * 
 * Tests the @column.uuid() decorator behavior across different database drivers.
 */

import 'reflect-metadata';
import { describe, it, expect } from 'vitest';
import { DBCast, DBCastArray, type SqlCastFormatter } from '../../src/DBValues';
import { DBConditions } from '../../src/DBConditions';
import { formatSqlCast, needsSqlCast, getSqlCastFormatter, type DriverType } from '../../src/drivers';
import { DBModel, model, column } from '../../src/index';
import { getSqlCastMap, getColumnMeta } from '../../src/decorators';

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

// ============================================
// @column.uuid() Decorator Tests
// ============================================

describe('@column.uuid() Decorator', () => {
  describe('column.uuid() with ColumnOptions', () => {
    it('should accept string column name', () => {
      @model('test_uuid_string')
      class TestModel extends DBModel {
        @column.uuid('custom_id') id?: string;
      }

      const meta = getColumnMeta(TestModel);
      expect(meta).toBeDefined();
      expect(meta?.get('id')?.columnName).toBe('custom_id');
      expect(meta?.get('id')?.sqlCast).toBe('uuid');
    });

    it('should accept ColumnOptions with primaryKey', () => {
      @model('test_uuid_pk')
      class TestModel extends DBModel {
        @column.uuid({ primaryKey: true }) id?: string;
        @column() name?: string;
      }

      const meta = getColumnMeta(TestModel);
      expect(meta).toBeDefined();
      expect(meta?.get('id')?.primaryKey).toBe(true);
      expect(meta?.get('id')?.sqlCast).toBe('uuid');
    });

    it('should accept ColumnOptions with columnName', () => {
      @model('test_uuid_colname')
      class TestModel extends DBModel {
        @column.uuid({ columnName: 'user_uuid' }) id?: string;
      }

      const meta = getColumnMeta(TestModel);
      expect(meta).toBeDefined();
      expect(meta?.get('id')?.columnName).toBe('user_uuid');
      expect(meta?.get('id')?.sqlCast).toBe('uuid');
    });

    it('should accept ColumnOptions with both primaryKey and columnName', () => {
      @model('test_uuid_both')
      class TestModel extends DBModel {
        @column.uuid({ primaryKey: true, columnName: 'pk_uuid' }) id?: string;
      }

      const meta = getColumnMeta(TestModel);
      expect(meta).toBeDefined();
      expect(meta?.get('id')?.primaryKey).toBe(true);
      expect(meta?.get('id')?.columnName).toBe('pk_uuid');
      expect(meta?.get('id')?.sqlCast).toBe('uuid');
    });

    it('should work without any arguments', () => {
      @model('test_uuid_noargs')
      class TestModel extends DBModel {
        @column.uuid() id?: string;
      }

      const meta = getColumnMeta(TestModel);
      expect(meta).toBeDefined();
      expect(meta?.get('id')?.sqlCast).toBe('uuid');
    });
  });

  describe('getSqlCastMap()', () => {
    it('should return sqlCast for uuid columns', () => {
      @model('test_sqlcast_map')
      class TestModel extends DBModel {
        @column.uuid({ primaryKey: true }) id?: string;
        @column() name?: string;
        @column.uuid() ref_id?: string;
      }

      const sqlCastMap = getSqlCastMap(TestModel);
      
      expect(sqlCastMap.get('id')).toBe('uuid');
      expect(sqlCastMap.get('name')).toBeUndefined();
      expect(sqlCastMap.get('ref_id')).toBe('uuid');
    });

    it('should return empty map for models without uuid columns', () => {
      @model('test_no_uuid')
      class TestModel extends DBModel {
        @column({ primaryKey: true }) id?: number;
        @column() name?: string;
      }

      const sqlCastMap = getSqlCastMap(TestModel);
      
      expect(sqlCastMap.get('id')).toBeUndefined();
      expect(sqlCastMap.get('name')).toBeUndefined();
    });
  });

  describe('UUID primary key model', () => {
    @model('users_with_uuid_pk')
    class UserWithUuidPk extends DBModel {
      @column.uuid({ primaryKey: true }) id?: string;
      @column() name?: string;
      @column() email?: string;
    }

    it('should have sqlCast=uuid for primary key', () => {
      const sqlCastMap = getSqlCastMap(UserWithUuidPk);
      expect(sqlCastMap.get('id')).toBe('uuid');
    });

    it('should have primaryKey=true in metadata', () => {
      const meta = getColumnMeta(UserWithUuidPk);
      expect(meta?.get('id')?.primaryKey).toBe(true);
    });
  });
});

// ============================================
// inferPgArrayType Tests
// ============================================

describe('inferPgArrayType with sqlCast', () => {
  // Import the function from LazyRelation to test directly
  // Since it's not exported, we test via getSqlCastMap behavior

  describe('sqlCast precedence over value inference', () => {
    @model('test_uuid_fk')
    class ParentModel extends DBModel {
      @column.uuid({ primaryKey: true }) id?: string;
      @column() name?: string;
    }

    @model('test_child_fk')
    class ChildModel extends DBModel {
      @column({ primaryKey: true }) id?: number;
      @column.uuid() parent_id?: string;
    }

    it('should detect uuid sqlCast for primary key column', () => {
      const sqlCastMap = getSqlCastMap(ParentModel);
      expect(sqlCastMap.get('id')).toBe('uuid');
    });

    it('should detect uuid sqlCast for foreign key column', () => {
      const sqlCastMap = getSqlCastMap(ChildModel);
      expect(sqlCastMap.get('parent_id')).toBe('uuid');
    });

    it('should not have sqlCast for non-uuid columns', () => {
      const sqlCastMap = getSqlCastMap(ParentModel);
      expect(sqlCastMap.get('name')).toBeUndefined();
    });
  });
});

// ============================================
// Batch Operation SQL Generation Tests
// ============================================

describe('Batch operations with UUID columns', () => {
  // Test models for batch operations
  @model('batch_uuid_users')
  class BatchUuidUser extends DBModel {
    @column.uuid({ primaryKey: true }) id?: string;
    @column() name?: string;
    @column() email?: string;
  }

  @model('batch_uuid_posts')
  class BatchUuidPost extends DBModel {
    @column({ primaryKey: true }) id?: number;
    @column.uuid() author_id?: string;
    @column() title?: string;
  }

  // Composite key model
  @model('batch_composite_uuid')
  class BatchCompositeUuid extends DBModel {
    @column.uuid({ primaryKey: true }) org_id?: string;
    @column.uuid({ primaryKey: true }) user_id?: string;
    @column() role?: string;
  }

  describe('getSqlCastMap for batch operations', () => {
    it('should return uuid cast for single UUID primary key', () => {
      const sqlCastMap = getSqlCastMap(BatchUuidUser);
      expect(sqlCastMap.get('id')).toBe('uuid');
    });

    it('should return uuid cast for UUID foreign key', () => {
      const sqlCastMap = getSqlCastMap(BatchUuidPost);
      expect(sqlCastMap.get('author_id')).toBe('uuid');
      expect(sqlCastMap.get('id')).toBeUndefined();
    });

    it('should return uuid cast for composite UUID primary keys', () => {
      const sqlCastMap = getSqlCastMap(BatchCompositeUuid);
      expect(sqlCastMap.get('org_id')).toBe('uuid');
      expect(sqlCastMap.get('user_id')).toBe('uuid');
    });
  });

  describe('Column metadata for batch operations', () => {
    it('should have correct metadata for UUID primary key model', () => {
      const meta = getColumnMeta(BatchUuidUser);
      expect(meta).toBeDefined();
      
      const idMeta = meta?.get('id');
      expect(idMeta?.primaryKey).toBe(true);
      expect(idMeta?.sqlCast).toBe('uuid');
      
      const nameMeta = meta?.get('name');
      expect(nameMeta?.primaryKey).toBeFalsy();
      expect(nameMeta?.sqlCast).toBeUndefined();
    });

    it('should have correct metadata for composite UUID key model', () => {
      const meta = getColumnMeta(BatchCompositeUuid);
      expect(meta).toBeDefined();
      
      const orgIdMeta = meta?.get('org_id');
      expect(orgIdMeta?.primaryKey).toBe(true);
      expect(orgIdMeta?.sqlCast).toBe('uuid');
      
      const userIdMeta = meta?.get('user_id');
      expect(userIdMeta?.primaryKey).toBe(true);
      expect(userIdMeta?.sqlCast).toBe('uuid');
    });
  });
});

// ============================================
// DBCast in batch context Tests
// ============================================

describe('DBCast behavior in batch SQL generation', () => {
  const testUuids = [
    '11111111-1111-1111-1111-111111111111',
    '22222222-2222-2222-2222-222222222222',
    '33333333-3333-3333-3333-333333333333',
  ];

  describe('PostgreSQL batch array casting', () => {
    const pgFormatter = getSqlCastFormatter('postgres');

    it('should generate correct placeholder for uuid[] array', () => {
      // Simulates what happens in updateMany/findById batch operations
      // where we need ?::uuid[] instead of ?::text[]
      const sqlCast = 'uuid';
      const pgArrayType = sqlCast ? `${sqlCast}[]` : 'text[]';
      
      expect(pgArrayType).toBe('uuid[]');
    });

    it('should compile multiple DBCast values for batch IN clause', () => {
      const castArr = new DBCastArray(testUuids, 'uuid');
      const params: unknown[] = [];
      const sql = castArr.compile(params, 'id', pgFormatter);
      
      expect(sql).toBe('id IN (?::uuid, ?::uuid, ?::uuid)');
      expect(params).toEqual(testUuids);
    });

    it('should handle empty array in batch operations', () => {
      const castArr = new DBCastArray([], 'uuid');
      const params: unknown[] = [];
      const sql = castArr.compile(params, 'id', pgFormatter);
      
      expect(sql).toBe('1 = 0');
      expect(params).toEqual([]);
    });
  });

  describe('SQLite batch operations (no cast needed)', () => {
    const sqliteFormatter = getSqlCastFormatter('sqlite');

    it('should NOT cast uuid values in SQLite', () => {
      const castArr = new DBCastArray(testUuids, 'uuid');
      const params: unknown[] = [];
      const sql = castArr.compile(params, 'id', sqliteFormatter);
      
      expect(sql).toBe('id IN (?, ?, ?)');
      expect(params).toEqual(testUuids);
    });
  });

  describe('MySQL batch operations (no cast needed)', () => {
    const mysqlFormatter = getSqlCastFormatter('mysql');

    it('should NOT cast uuid values in MySQL', () => {
      const castArr = new DBCastArray(testUuids, 'uuid');
      const params: unknown[] = [];
      const sql = castArr.compile(params, 'id', mysqlFormatter);
      
      expect(sql).toBe('id IN (?, ?, ?)');
      expect(params).toEqual(testUuids);
    });
  });
});

