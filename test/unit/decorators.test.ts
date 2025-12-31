/**
 * Decorators Tests
 */

import 'reflect-metadata';
import { describe, it, expect } from 'vitest';
import {
  column,
  model,
  getColumnMeta,
  getModelColumnNames,
  getModelPropertyNames,
} from '../../src/decorators';
import { DBModel } from '../../src/DBModel';
import { isColumn } from '../../src/Column';

describe('decorators', () => {
  describe('@column', () => {
    it('should register column with same name as property', () => {
      class TestModel extends DBModel {
        @column() id?: number;
        @column() name?: string;
      }

      const meta = getColumnMeta(TestModel);
      expect(meta).toBeDefined();
      expect(meta!.get('id')?.columnName).toBe('id');
      expect(meta!.get('name')?.columnName).toBe('name');
    });

    it('should register column with custom name', () => {
      class TestModel extends DBModel {
        @column('user_id') id?: number;
        @column('user_name') name?: string;
      }

      const meta = getColumnMeta(TestModel);
      expect(meta!.get('id')?.columnName).toBe('user_id');
      expect(meta!.get('name')?.columnName).toBe('user_name');
    });
  });

  describe('@column type conversions', () => {
    it('@column.boolean should convert to boolean', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.boolean() is_active?: boolean;
      }

      const instance = new TestModel();
      (instance as any).is_active = 't';
      instance.typeCastFromDB();
      expect(instance.is_active).toBe(true);
    });

    it('@column.number should convert to number', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.number() amount?: number;
      }

      const instance = new TestModel();
      (instance as any).amount = '123.45';
      instance.typeCastFromDB();
      expect(instance.amount).toBe(123.45);
    });

    it('@column.bigint should convert to bigint', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.bigint() large_id?: bigint;
      }

      const instance = new TestModel();
      (instance as any).large_id = '9007199254740993';
      instance.typeCastFromDB();
      expect(instance.large_id).toBe(BigInt('9007199254740993'));
    });

    it('@column.datetime should convert to Date', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.datetime() created_at?: Date;
      }

      const instance = new TestModel();
      (instance as any).created_at = '2024-01-01T12:00:00Z';
      instance.typeCastFromDB();
      expect(instance.created_at).toBeInstanceOf(Date);
      expect(instance.created_at?.getFullYear()).toBe(2024);
    });

    it('@column.date should convert to Date with time zeroed', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.date() birth_date?: Date;
      }

      const instance = new TestModel();
      (instance as any).birth_date = '2024-01-15T15:30:00Z';
      instance.typeCastFromDB();
      expect(instance.birth_date).toBeInstanceOf(Date);
      expect(instance.birth_date?.getHours()).toBe(0);
      expect(instance.birth_date?.getMinutes()).toBe(0);
    });

    it('@column.stringArray should convert to string array', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.stringArray() tags?: string[];
      }

      const instance = new TestModel();
      (instance as any).tags = '{a,b,c}';
      instance.typeCastFromDB();
      expect(instance.tags).toEqual(['a', 'b', 'c']);
    });

    it('@column.intArray should convert to number array', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.intArray() scores?: number[];
      }

      const instance = new TestModel();
      (instance as any).scores = '{1,2,3}';
      instance.typeCastFromDB();
      expect(instance.scores).toEqual([1, 2, 3]);
    });

    it('@column.numericArray should convert to numeric array', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.numericArray() values?: (number | null)[];
      }

      const instance = new TestModel();
      (instance as any).values = '[1.5,2.5,null]';
      instance.typeCastFromDB();
      expect(instance.values).toEqual([1.5, 2.5, null]);
    });

    it('@column.booleanArray should convert to boolean array', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.booleanArray() flags?: (boolean | null)[];
      }

      const instance = new TestModel();
      (instance as any).flags = '[true,false,null]';
      instance.typeCastFromDB();
      expect(instance.flags).toEqual([true, false, null]);
    });

    it('@column.datetimeArray should convert to Date array', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.datetimeArray() dates?: (Date | null)[];
      }

      const instance = new TestModel();
      (instance as any).dates = ['2024-01-01', '2024-01-02'];
      instance.typeCastFromDB();
      expect(instance.dates![0]).toBeInstanceOf(Date);
      expect(instance.dates![1]).toBeInstanceOf(Date);
    });

    it('@column.json should convert to object', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.json() metadata?: Record<string, unknown>;
      }

      const instance = new TestModel();
      (instance as any).metadata = '{"key":"value"}';
      instance.typeCastFromDB();
      expect(instance.metadata).toEqual({ key: 'value' });
    });

    it('@column.custom should apply custom conversion', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.custom((v) => String(v).toUpperCase()) status?: string;
      }

      const instance = new TestModel();
      (instance as any).status = 'active';
      instance.typeCastFromDB();
      expect(instance.status).toBe('ACTIVE');
    });
  });

  describe('@column type auto-inference', () => {
    // Note: Auto-inference requires emitDecoratorMetadata which esbuild (used by vitest) doesn't support.
    // These tests verify that explicit type decorators work when auto-inference is not available.
    
    it('should work with explicit @column.boolean when auto-inference unavailable', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.boolean() is_active?: boolean;
      }

      const instance = new TestModel();
      (instance as any).is_active = 't';
      instance.typeCastFromDB();
      expect(instance.is_active).toBe(true);
    });

    it('should work with explicit @column.number when auto-inference unavailable', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.number() count?: number;
      }

      const instance = new TestModel();
      (instance as any).count = '42';
      instance.typeCastFromDB();
      expect(instance.count).toBe(42);
    });

    it('should work with explicit @column.datetime when auto-inference unavailable', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.datetime() created_at?: Date;
      }

      const instance = new TestModel();
      (instance as any).created_at = '2024-01-01';
      instance.typeCastFromDB();
      expect(instance.created_at).toBeInstanceOf(Date);
    });
  });

  describe('@column null/undefined handling', () => {
    it('should handle null values', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.boolean() flag?: boolean;
        @column.number() num?: number;
      }

      const instance = new TestModel();
      (instance as any).flag = null;
      (instance as any).num = null;
      instance.typeCastFromDB();
      // DB NULL is preserved as null
      expect(instance.flag).toBeNull();
      expect(instance.num).toBeNull();
    });

    it('should handle undefined values', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.datetime() dt?: Date;
      }

      const instance = new TestModel();
      (instance as any).dt = undefined;
      instance.typeCastFromDB();
      // undefined stays undefined (value was never set in DB)
      expect(instance.dt).toBeUndefined();
    });

    it('should handle invalid values', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.number() num?: number;
        @column.bigint() big?: bigint;
      }

      const instance = new TestModel();
      (instance as any).num = 'not a number';
      (instance as any).big = 'invalid';
      instance.typeCastFromDB();
      // Invalid values return null (conversion failed), null values preserved
      expect(instance.num).toBeNull();
      expect(instance.big).toBeNull();
    });
  });

  describe('@model', () => {
    it('should set TABLE_NAME', () => {
      @model('users')
      class User extends DBModel {
        @column() id?: number;
      }

      expect(User.TABLE_NAME).toBe('users');
    });

    it('should work without table name argument', () => {
      @model
      class TestModelWithoutName extends DBModel {
        @column() id?: number;
      }

      // TABLE_NAME should not be set by the decorator
      // Note: DBModel base class may have TABLE_NAME = '' as default
      const tableName = TestModelWithoutName.TABLE_NAME;
      expect(tableName === undefined || tableName === '').toBe(true);
    });

    it('should create static Column properties', () => {
      @model('test')
      class TestModel extends DBModel {
        @column() id?: number;
        @column() name?: string;
      }

      // Check that static properties exist and are Column instances
      expect((TestModel as any).id).toBeDefined();
      expect((TestModel as any).name).toBeDefined();
      expect(isColumn((TestModel as any).id)).toBe(true);
      expect(isColumn((TestModel as any).name)).toBe(true);
    });

    it('should create callable Column properties', () => {
      @model('test')
      class TestModel extends DBModel {
        @column() id?: number;
        @column('user_name') name?: string;
      }

      // Column should be callable and return column name
      expect((TestModel as any).id()).toBe('id');
      expect((TestModel as any).name()).toBe('user_name');
    });

    it('should create Column properties with condition builders', () => {
      @model('test')
      class TestModel extends DBModel {
        @column() id?: number;
        @column() is_active?: boolean;
      }

      expect((TestModel as any).id.eq(1)).toEqual({ id: 1 });
      expect((TestModel as any).is_active.eq(true)).toEqual({ is_active: true });
      expect((TestModel as any).id.gt(10)).toEqual({ 'id > ?': 10 });
    });

    it('should preserve existing typeCastFromDB', () => {
      @model('test')
      class TestModel extends DBModel {
        @column() id?: number;
        customCalled = false;

        typeCastFromDB() {
          this.customCalled = true;
        }
      }

      const instance = new TestModel();
      instance.typeCastFromDB();
      expect(instance.customCalled).toBe(true);
    });

    it('should store _columnMeta', () => {
      @model('test')
      class TestModel extends DBModel {
        @column() id?: number;
        @column('custom_name') custom?: string;
      }

      const meta = (TestModel as any)._columnMeta;
      expect(meta).toBeInstanceOf(Map);
      expect(meta.get('id')).toBeDefined();
      expect(meta.get('custom')).toBeDefined();
    });
  });

  describe('getColumnMeta', () => {
    it('should return column metadata map', () => {
      @model('test')
      class TestModel extends DBModel {
        @column() id?: number;
        @column.boolean() flag?: boolean;
      }

      const meta = getColumnMeta(TestModel);
      expect(meta).toBeInstanceOf(Map);
      expect(meta!.size).toBe(2);
      expect(meta!.get('id')?.columnName).toBe('id');
      expect(meta!.get('flag')?.columnName).toBe('flag');
      expect(meta!.get('flag')?.typeCast).toBeDefined();
    });

    it('should return undefined for class without columns', () => {
      class EmptyModel extends DBModel {}
      const meta = getColumnMeta(EmptyModel);
      expect(meta).toBeUndefined();
    });
  });

  describe('getModelColumnNames', () => {
    it('should return array of column names', () => {
      @model('test')
      class TestModel extends DBModel {
        @column() id?: number;
        @column('user_name') name?: string;
        @column() email?: string;
      }

      const names = getModelColumnNames(TestModel);
      expect(names).toContain('id');
      expect(names).toContain('user_name');
      expect(names).toContain('email');
      expect(names.length).toBe(3);
    });

    it('should return empty array for class without columns', () => {
      class EmptyModel extends DBModel {}
      const names = getModelColumnNames(EmptyModel);
      expect(names).toEqual([]);
    });
  });

  describe('getModelPropertyNames', () => {
    it('should return array of property names', () => {
      @model('test')
      class TestModel extends DBModel {
        @column() id?: number;
        @column('user_name') name?: string;
        @column() email?: string;
      }

      const props = getModelPropertyNames(TestModel);
      expect(props).toContain('id');
      expect(props).toContain('name');
      expect(props).toContain('email');
      expect(props.length).toBe(3);
    });

    it('should return empty array for class without columns', () => {
      class EmptyModel extends DBModel {}
      const props = getModelPropertyNames(EmptyModel);
      expect(props).toEqual([]);
    });
  });

  describe('Edge cases', () => {
    it('should handle empty string array', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.stringArray() tags?: string[];
      }

      const instance = new TestModel();
      (instance as any).tags = '{}';
      instance.typeCastFromDB();
      expect(instance.tags).toEqual([]);
    });

    it('should handle null in array conversion', () => {
      @model('test')
      class TestModel extends DBModel {
        @column.stringArray() tags?: string[];
      }

      const instance = new TestModel();
      (instance as any).tags = null;
      instance.typeCastFromDB();
      // Null values from DB are preserved as null
      expect(instance.tags).toBeNull();
    });
  });

  describe('@column({ primaryKey: true })', () => {
    it('should detect single primary key from decorator', () => {
      @model('single_pk_test')
      class SinglePkModel extends DBModel {
        @column({ primaryKey: true }) id?: number;
        @column() name?: string;
      }

      const meta = getColumnMeta(SinglePkModel);
      expect(meta!.get('id')?.primaryKey).toBe(true);
      expect(meta!.get('name')?.primaryKey).toBeUndefined();

      // Test getPkey works correctly
      const instance = new SinglePkModel();
      instance.id = 42;
      instance.name = 'Test';
      const pkey = instance.getPkey();
      expect(pkey).toEqual({ id: 42 });
    });

    it('should detect composite primary key from decorators', () => {
      @model('composite_pk_test')
      class CompositePkModel extends DBModel {
        @column({ primaryKey: true }) post_id?: number;
        @column({ primaryKey: true }) tag_id?: number;
        @column.datetime() created_at?: Date;
      }

      const meta = getColumnMeta(CompositePkModel);
      expect(meta!.get('post_id')?.primaryKey).toBe(true);
      expect(meta!.get('tag_id')?.primaryKey).toBe(true);
      expect(meta!.get('created_at')?.primaryKey).toBeUndefined();

      // Test getPkey works correctly
      const instance = new CompositePkModel();
      instance.post_id = 1;
      instance.tag_id = 5;
      const pkey = instance.getPkey();
      expect(pkey).toEqual({ post_id: 1, tag_id: 5 });
    });

    it('should support primaryKey with custom column name', () => {
      @model('custom_name_pk_test')
      class CustomNamePkModel extends DBModel {
        @column({ primaryKey: true, columnName: 'user_id' }) id?: number;
        @column() name?: string;
      }

      const meta = getColumnMeta(CustomNamePkModel);
      expect(meta!.get('id')?.columnName).toBe('user_id');
      expect(meta!.get('id')?.primaryKey).toBe(true);

      // Test getPkey uses column name
      const instance = new CustomNamePkModel();
      instance.id = 99;
      const pkey = instance.getPkey();
      expect(pkey).toEqual({ user_id: 99 });
    });

    it('should fall back to id when no primaryKey specified', () => {
      @model('no_pk_test')
      class NoPkModel extends DBModel {
        @column() id?: number;
        @column() name?: string;
      }

      // No primaryKey specified, should use 'id' as default
      const instance = new NoPkModel();
      instance.id = 123;
      instance.name = 'Test';
      const pkey = instance.getPkey();
      expect(pkey).toEqual({ id: 123 });
    });
  });
});

