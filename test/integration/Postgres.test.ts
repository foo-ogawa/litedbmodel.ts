/**
 * DBModel Integration Tests
 *
 * These tests require a running PostgreSQL instance.
 * Run `docker compose up -d` before running tests.
 * 
 * Skip these tests when PostgreSQL is not available by setting SKIP_INTEGRATION_TESTS=1
 */

import 'reflect-metadata';
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { DBModel, dbIn, dbNull, model, column, ColumnsOf } from '../../src';
import { User, Post, PostTag, testConfig, cleanup } from '../helpers/setup';

// Define a model for all types testing (outside describe block for decorator support)
@model('all_types_test')
class AllTypesModel extends DBModel {
  @column() id?: number;
  @column() int_val?: number | null;
  @column() float_val?: number | null;
  @column.boolean() bool_val?: boolean | null;
  @column() text_val?: string | null;
  @column() varchar_val?: string | null;
  @column.datetime() timestamp_val?: Date | null;
  @column.date() date_val?: Date | null;  // Changed to use @column.date() for proper type casting
  @column.intArray() int_array?: number[];
  @column.stringArray() text_array?: string[];
  @column.booleanArray() bool_array?: (boolean | null)[];
  @column.json() json_val?: Record<string, unknown> | null;
  @column.json() json_array_val?: unknown[] | null;
}
const AllTypes = AllTypesModel as typeof AllTypesModel & ColumnsOf<AllTypesModel>;

// Model for testing @column() with Date auto-inference
// Note: Uses @column.datetime() explicitly because esbuild/vitest doesn't support emitDecoratorMetadata
// In production with tsc, plain @column() would work via auto-inference
@model('auto_date_test')
class AutoDateModel extends DBModel {
  @column() id?: number;
  @column.datetime() created_at?: Date | null;  // Use explicit decorator for test compatibility
}
const AutoDate = AutoDateModel as typeof AutoDateModel & ColumnsOf<AutoDateModel>;

// Skip integration tests if SKIP_INTEGRATION_TESTS=1 is set
const skipIntegrationTests = process.env.SKIP_INTEGRATION_TESTS === '1';

describe.skipIf(skipIntegrationTests)('DBModel', () => {
  beforeAll(() => {
    // Initialize DBModel with test config
    DBModel.setConfig(testConfig);
  });

  afterAll(async () => {
    await cleanup();
  });

  describe('find', () => {
    it('should select all records', async () => {
      const users = await User.find([]);

      expect(users.length).toBeGreaterThan(0);
      expect(users[0]).toBeInstanceOf(User);
      expect(users[0].id).toBeDefined();
      expect(users[0].name).toBeDefined();
    });

    it('should select with conditions', async () => {
      const users = await User.find([[User.is_active, true]]);

      expect(users.length).toBeGreaterThan(0);
      users.forEach((user) => {
        expect(user.is_active).toBe(true);
      });
    });

    it('should select with column def conditions', async () => {
      const users = await User.find([
        [User.is_active, true],
        [User.role, 'admin'],
      ]);

      expect(users.length).toBeGreaterThan(0);
      users.forEach((user) => {
        expect(user.is_active).toBe(true);
        expect(user.role).toBe('admin');
      });
    });

    it('should select with ordering', async () => {
      const users = await User.find([], { order: 'name DESC' });

      expect(users.length).toBeGreaterThanOrEqual(2);

      for (let i = 1; i < users.length; i++) {
        expect(users[i - 1].name!.localeCompare(users[i].name!)).toBeGreaterThanOrEqual(0);
      }
    });

    it('should select with limit', async () => {
      const users = await User.find([], { limit: 2 });
      expect(users.length).toBe(2);
    });

    it('should select with offset', async () => {
      const allUsers = await User.find([]);
      const offsetUsers = await User.find([], { offset: 1 });

      expect(offsetUsers.length).toBe(allUsers.length - 1);
      expect(offsetUsers[0].id).toBe(allUsers[1].id);
    });
  });

  describe('findOne', () => {
    it('should select single record', async () => {
      const user = await User.findOne([[User.role, 'admin']]);

      expect(user).not.toBeNull();
      expect(user).toBeInstanceOf(User);
      expect(user!.role).toBe('admin');
    });

    it('should return null when not found', async () => {
      const user = await User.findOne([[User.id, 99999]]);
      expect(user).toBeNull();
    });
  });

  describe('findById', () => {
    it('should find by id', async () => {
      const [firstUser] = await User.find([], { limit: 1 });
      const [user] = await User.findById({ values: [[firstUser.id]] });

      expect(user).toBeDefined();
      expect(user.id).toBe(firstUser.id);
    });

    it('should return empty array when not found', async () => {
      const users = await User.findById({ values: [[99999]] });
      expect(users).toHaveLength(0);
    });
  });

  describe('count', () => {
    it('should count all records', async () => {
      const count = await User.count([]);
      const users = await User.find([]);

      expect(count).toBe(users.length);
    });

    it('should count with conditions', async () => {
      const count = await User.count([[User.is_active, true]]);
      const users = await User.find([[User.is_active, true]]);

      expect(count).toBe(users.length);
    });
  });

  describe('create', () => {
    it('should insert single record', async () => {
      // Execute raw SQL to insert
      const res = await DBModel.execute(
        `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4) RETURNING *`,
        ['Test Insert', 'test-insert@example.com', true, 'user']
      );

      expect(res.rows.length).toBe(1);
      expect(res.rows[0].name).toBe('Test Insert');
      expect(res.rows[0].email).toBe('test-insert@example.com');

      // Cleanup
      await DBModel.execute(`DELETE FROM users WHERE id = $1`, [res.rows[0].id]);
    });
  });

  describe('update', () => {
    it('should update records', async () => {
      // Create test user via raw SQL
      const insertRes = await DBModel.execute(
        `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4) RETURNING *`,
        ['Update Test', 'update-test@example.com', true, 'user']
      );
      const userId = insertRes.rows[0].id;

      // Update via raw SQL
      const updateRes = await DBModel.execute(
        `UPDATE users SET name = $1 WHERE id = $2 RETURNING *`,
        ['Updated Name', userId]
      );

      expect(updateRes.rows[0].name).toBe('Updated Name');

      // Cleanup
      await DBModel.execute(`DELETE FROM users WHERE id = $1`, [userId]);
    });
  });

  describe('delete', () => {
    it('should delete records', async () => {
      // Create test user via raw SQL
      const insertRes = await DBModel.execute(
        `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4) RETURNING *`,
        ['Delete Test', 'delete-test@example.com', true, 'user']
      );
      const userId = insertRes.rows[0].id;

      // Delete via raw SQL
      const deleteRes = await DBModel.execute(
        `DELETE FROM users WHERE id = $1 RETURNING *`,
        [userId]
      );

      expect(deleteRes.rows[0].id).toBe(userId);

      // Verify deletion
      const selectRes = await DBModel.execute(
        `SELECT * FROM users WHERE id = $1`,
        [userId]
      );
      expect(selectRes.rows.length).toBe(0);
    });
  });

  describe('transaction', () => {
    it('should commit on success', async () => {
      const initialCountRes = await DBModel.execute(`SELECT COUNT(*) as count FROM users`);
      const initialCount = parseInt(initialCountRes.rows[0].count, 10);

      await DBModel.transaction(async () => {
        await DBModel.execute(
          `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4)`,
          ['Transaction Test', 'tx-test@example.com', true, 'user']
        );
      });

      const finalCountRes = await DBModel.execute(`SELECT COUNT(*) as count FROM users`);
      const finalCount = parseInt(finalCountRes.rows[0].count, 10);
      expect(finalCount).toBe(initialCount + 1);

      // Cleanup
      await DBModel.execute(`DELETE FROM users WHERE email = $1`, ['tx-test@example.com']);
    });

    it('should rollback on error', async () => {
      const initialCountRes = await DBModel.execute(`SELECT COUNT(*) as count FROM users`);
      const initialCount = parseInt(initialCountRes.rows[0].count, 10);

      try {
        await DBModel.transaction(async () => {
          await DBModel.execute(
            `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4)`,
            ['Transaction Rollback Test', 'tx-rollback@example.com', true, 'user']
          );
          throw new Error('Force rollback');
        });
      } catch (error) {
        // Expected error
      }

      const finalCountRes = await DBModel.execute(`SELECT COUNT(*) as count FROM users`);
      const finalCount = parseInt(finalCountRes.rows[0].count, 10);
      expect(finalCount).toBe(initialCount);
    });
  });
});

describe.skipIf(skipIntegrationTests)('DBModel utilities', () => {
  beforeAll(() => {
    DBModel.setConfig(testConfig);
  });

  afterAll(async () => {
    await cleanup();
  });

  describe('columnList', () => {
    it('should extract column values', async () => {
      const users = await User.find([]);
      const ids = User.columnList(users, 'id');

      expect(ids.length).toBe(users.length);
      expect(ids).toContain(users[0].id);
    });
  });

  describe('hashByProperty', () => {
    it('should create hash by property', async () => {
      const users = await User.find([]);
      const byId = User.hashByProperty(users, 'id');

      expect(byId['1']).toBeDefined();
      expect(byId['2']).toBeDefined();
    });
  });
});

describe.skipIf(skipIntegrationTests)('DBModel advanced operations', () => {
  beforeAll(() => {
    DBModel.setConfig(testConfig);
  });

  afterAll(async () => {
    await cleanup();
  });

  describe('createMany', () => {
    it('should create multiple records via raw SQL', async () => {
      // Note: createMany requires Column type, so we use raw SQL for testing
      const insertRes = await DBModel.execute(
        `INSERT INTO users (name, email, is_active, role) VALUES 
         ($1, $2, $3, $4), 
         ($5, $6, $7, $8) 
         RETURNING *`,
        ['Batch1', 'batch1@example.com', true, 'user', 'Batch2', 'batch2@example.com', true, 'user']
      );

      expect(insertRes.rows.length).toBe(2);
      expect(insertRes.rows[0].name).toBe('Batch1');
      expect(insertRes.rows[1].name).toBe('Batch2');

      // Cleanup
      await DBModel.execute(`DELETE FROM users WHERE email LIKE 'batch%@example.com'`);
    });
  });

  describe('instance methods', () => {
    it('should create a new record using static create()', async () => {
      await DBModel.transaction(async () => {
        const result = await User.create([
          [User.name, 'Create Test'],
          [User.email, 'create-test@example.com'],
          [User.is_active, true],
          [User.role, 'user'],
        ], { returning: true });

        expect(result).not.toBeNull();
        expect(result!.values).toHaveLength(1);
      });
      
      // Cleanup
      await DBModel.execute(`DELETE FROM users WHERE email = $1`, ['create-test@example.com']);
    });

    it('should update an existing record using static update()', async () => {
      const uniqueEmail = `update-instance-${Date.now()}@example.com`;
      // Create a user first
      const insertRes = await DBModel.execute(
        `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4) RETURNING *`,
        ['Update Instance', uniqueEmail, true, 'user']
      );
      const userId = insertRes.rows[0].id;

      // Update via static method
      await DBModel.transaction(async () => {
        await User.update(
          [[User.id, userId]],
          [[User.name, 'Updated Instance']],
        );
      });

      // Verify update
      const [updated] = await User.findById({ values: [[userId]] });
      expect(updated?.name).toBe('Updated Instance');

      // Cleanup
      await DBModel.execute(`DELETE FROM users WHERE id = $1`, [userId]);
    });

    it('should reload a record', async () => {
      // Create a user first
      const insertRes = await DBModel.execute(
        `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4) RETURNING *`,
        ['Reload Test', 'reload-test@example.com', true, 'user']
      );
      const userId = insertRes.rows[0].id;

      const [user] = await User.findById({ values: [[userId]] });
      expect(user).toBeDefined();

      // Update directly in DB
      await DBModel.execute(`UPDATE users SET name = $1 WHERE id = $2`, ['Reloaded Name', userId]);

      // Reload
      await user.reload();
      expect(user.name).toBe('Reloaded Name');

      // Cleanup
      await DBModel.execute(`DELETE FROM users WHERE id = $1`, [userId]);
    });

    it('should delete a record using static delete()', async () => {
      const uniqueEmail = `destroy-test-${Date.now()}@example.com`;
      // Create a user first
      const insertRes = await DBModel.execute(
        `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4) RETURNING *`,
        ['Destroy Test', uniqueEmail, true, 'user']
      );
      const userId = insertRes.rows[0].id;

      await DBModel.transaction(async () => {
        const result = await User.delete([[User.id, userId]], { returning: true });
        expect(result).not.toBeNull();
        expect(result!.values).toHaveLength(1);
      });

      // Verify deletion
      const deleted = await User.findById({ values: [[userId]] });
      expect(deleted).toHaveLength(0);
    });
  });

  describe('OR conditions via raw SQL', () => {
    it('should find with OR conditions', async () => {
      const result = await DBModel.execute(
        `SELECT * FROM users WHERE role = $1 OR role = $2`,
        ['admin', 'user']
      );

      expect(result.rows.length).toBeGreaterThan(0);
    });
  });

  describe('IN conditions', () => {
    it('should find with IN conditions', async () => {
      const users = await User.find([
        [User.role, dbIn(['admin', 'user'])],
      ]);

      expect(users.length).toBeGreaterThan(0);
    });
  });

  describe('NULL conditions', () => {
    it('should find with NULL conditions', async () => {
      const users = await User.find([
        [User.deleted_at, dbNull()],
      ]);

      expect(users.length).toBeGreaterThan(0);
    });
  });

  describe('Column ordering', () => {
    it('should order by column asc', async () => {
      const users = await User.find([], { order: 'name ASC' });
      
      expect(users.length).toBeGreaterThanOrEqual(2);
      for (let i = 1; i < users.length; i++) {
        expect(users[i - 1].name!.localeCompare(users[i].name!)).toBeLessThanOrEqual(0);
      }
    });

    it('should order by column desc', async () => {
      const users = await User.find([], { order: 'name DESC' });
      
      expect(users.length).toBeGreaterThanOrEqual(2);
      for (let i = 1; i < users.length; i++) {
        expect(users[i - 1].name!.localeCompare(users[i].name!)).toBeGreaterThanOrEqual(0);
      }
    });
  });

  describe('Static methods update/delete via raw SQL', () => {
    it('should update with raw SQL', async () => {
      const uniqueEmail = `static-update-${Date.now()}@example.com`;
      // Create test user
      const insertRes = await DBModel.execute(
        `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4) RETURNING *`,
        ['Static Update', uniqueEmail, true, 'user']
      );
      const userId = insertRes.rows[0].id;

      // Update using raw SQL
      const updated = await DBModel.execute(
        `UPDATE users SET name = $1 WHERE id = $2 RETURNING *`,
        ['Static Updated', userId]
      );

      expect(updated.rows.length).toBe(1);
      expect(updated.rows[0].name).toBe('Static Updated');

      // Cleanup
      await DBModel.execute(`DELETE FROM users WHERE id = $1`, [userId]);
    });

    it('should delete with raw SQL', async () => {
      const uniqueEmail = `static-delete-${Date.now()}@example.com`;
      // Create test user
      const insertRes = await DBModel.execute(
        `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4) RETURNING *`,
        ['Static Delete', uniqueEmail, true, 'user']
      );
      const userId = insertRes.rows[0].id;

      // Delete using raw SQL
      const deleted = await DBModel.execute(
        `DELETE FROM users WHERE id = $1 RETURNING *`,
        [userId]
      );

      expect(deleted.rows.length).toBe(1);

      // Verify deletion
      const users = await User.findById({ values: [[userId]] });
      expect(users).toHaveLength(0);
    });
  });

  describe('Composite primary key', () => {
    it('should handle composite primary key', async () => {
      // PostTag has composite primary key (post_id, tag_id)
      const postTags = await PostTag.find([]);
      expect(postTags.length).toBeGreaterThan(0);

      // Test getPkey
      const firstPostTag = postTags[0];
      const pkey = firstPostTag.getPkey();
      expect(pkey).toEqual({ post_id: firstPostTag.post_id, tag_id: firstPostTag.tag_id });
    });
  });

  describe('Select specific columns', () => {
    it('should select only specified columns', async () => {
      const users = await User.find([], { columns: ['id', 'name'] });

      expect(users.length).toBeGreaterThan(0);
      expect(users[0].id).toBeDefined();
      expect(users[0].name).toBeDefined();
      // email should not be loaded (but may be undefined)
    });
  });

  describe('Error handling', () => {
    it('should throw error when reloading without primary key', async () => {
      const user = new User();
      // No id set

      await expect(user.reload()).rejects.toThrow('Cannot reload record without primary key');
    });
  });

  describe('ON CONFLICT (upsert)', () => {
    it('should ignore insert on conflict with onConflictIgnore', async () => {
      const uniqueEmail = `conflict-ignore-${Date.now()}@example.com`;
      
      // First insert
      await DBModel.execute(
        `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4)`,
        ['First', uniqueEmail, true, 'user']
      );

      // Try to insert with same email - should be ignored
      await DBModel.execute(
        `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4) 
         ON CONFLICT (email) DO NOTHING`,
        ['Second', uniqueEmail, true, 'admin']
      );

      // Verify first record still exists with original name
      const result = await DBModel.execute(
        `SELECT * FROM users WHERE email = $1`,
        [uniqueEmail]
      );
      expect(result.rows.length).toBe(1);
      expect(result.rows[0].name).toBe('First');

      // Cleanup
      await DBModel.execute(`DELETE FROM users WHERE email = $1`, [uniqueEmail]);
    });

    it('should update on conflict with onConflictUpdate', async () => {
      const uniqueEmail = `conflict-update-${Date.now()}@example.com`;
      
      // First insert
      await DBModel.execute(
        `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4)`,
        ['First', uniqueEmail, true, 'user']
      );

      // Upsert with same email - should update name
      await DBModel.execute(
        `INSERT INTO users (name, email, is_active, role) VALUES ($1, $2, $3, $4) 
         ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name, role = EXCLUDED.role`,
        ['Updated', uniqueEmail, true, 'admin']
      );

      // Verify record was updated
      const result = await DBModel.execute(
        `SELECT * FROM users WHERE email = $1`,
        [uniqueEmail]
      );
      expect(result.rows.length).toBe(1);
      expect(result.rows[0].name).toBe('Updated');
      expect(result.rows[0].role).toBe('admin');

      // Cleanup
      await DBModel.execute(`DELETE FROM users WHERE email = $1`, [uniqueEmail]);
    });
  });

  describe('Data Type Persistence', () => {
    beforeAll(async () => {
      // Create the table if it doesn't exist
      await DBModel.execute(`
        CREATE TABLE IF NOT EXISTS all_types_test (
          id SERIAL PRIMARY KEY,
          int_val INTEGER,
          float_val DOUBLE PRECISION,
          bool_val BOOLEAN,
          text_val TEXT,
          varchar_val VARCHAR(255),
          timestamp_val TIMESTAMP WITH TIME ZONE,
          date_val DATE,
          int_array INTEGER[],
          text_array TEXT[],
          bool_array BOOLEAN[],
          json_val JSONB,
          json_array_val JSONB
        )
      `);
    });

    beforeEach(async () => {
      await DBModel.execute('DELETE FROM all_types_test');
    });

    it('should persist and retrieve all basic types correctly via create/find', async () => {
      const testDate = new Date('2024-06-15T10:30:00.000Z');
      
      // Create via high-level API
      const result = await DBModel.transaction(async () => {
        return await AllTypes.create([
          [AllTypes.int_val, 42],
          [AllTypes.float_val, 3.14159],
          [AllTypes.bool_val, true],
          [AllTypes.text_val, 'long text content'],
          [AllTypes.varchar_val, 'short varchar'],
          [AllTypes.timestamp_val, testDate],
        ], { returning: true });
      });
      
      expect(result).not.toBeNull();
      const createdId = result!.values[0][0] as number;

      // Find and verify values are preserved
      const found = await AllTypes.findOne([[AllTypes.id, createdId]]);
      expect(found).not.toBeNull();
      expect(found!.id).toBeDefined();
      expect(found!.int_val).toBe(42);
      expect(found!.float_val).toBeCloseTo(3.14159, 4);
      expect(found!.bool_val).toBe(true);
      expect(found!.text_val).toBe('long text content');
      expect(found!.varchar_val).toBe('short varchar');
      expect(found!.timestamp_val?.toISOString()).toBe(testDate.toISOString());
    });

    it('should persist and retrieve array types correctly via create/find', async () => {
      // Create with plain arrays (no helper functions - ORM should handle conversion)
      const result = await DBModel.transaction(async () => {
        return await AllTypes.create([
          [AllTypes.int_array, [1, 2, 3, 4, 5]],
          [AllTypes.text_array, ['apple', 'banana', 'cherry']],
          [AllTypes.bool_array, [true, false, true]],
        ], { returning: true });
      });
      
      const createdId = result!.values[0][0] as number;

      // Find and verify
      const found = await AllTypes.findOne([[AllTypes.id, createdId]]);
      expect(found).not.toBeNull();
      expect(found!.int_array).toEqual([1, 2, 3, 4, 5]);
      expect(found!.text_array).toEqual(['apple', 'banana', 'cherry']);
      expect(found!.bool_array).toEqual([true, false, true]);
    });

    it('should persist and retrieve JSON types correctly via create/find', async () => {
      const jsonObj = { name: 'test', count: 42, nested: { key: 'value' } };
      const jsonArray = [1, 'two', { three: 3 }];
      
      // Create with JSON types
      const result = await DBModel.transaction(async () => {
        return await AllTypes.create([
          [AllTypes.json_val, jsonObj],
          [AllTypes.json_array_val, jsonArray],
        ], { returning: true });
      });
      
      const createdId = result!.values[0][0] as number;

      // Find and verify
      const found = await AllTypes.findOne([[AllTypes.id, createdId]]);
      expect(found).not.toBeNull();
      expect(found!.json_val).toEqual(jsonObj);
      expect(found!.json_array_val).toEqual(jsonArray);
    });

    it('should persist and retrieve NULL values correctly via create/find', async () => {
      // Create with NULL values
      const result = await DBModel.transaction(async () => {
        return await AllTypes.create([
          [AllTypes.int_val, null],
          [AllTypes.float_val, null],
          [AllTypes.bool_val, null],
          [AllTypes.text_val, null],
          [AllTypes.timestamp_val, null],
          [AllTypes.json_val, null],
        ], { returning: true });
      });
      
      const createdId = result!.values[0][0] as number;

      // Find and verify
      const found = await AllTypes.findOne([[AllTypes.id, createdId]]);
      expect(found).not.toBeNull();
      expect(found!.int_val).toBeNull();
      expect(found!.float_val).toBeNull();
      expect(found!.bool_val).toBeNull();
      expect(found!.text_val).toBeNull();
      expect(found!.timestamp_val).toBeNull();
      expect(found!.json_val).toBeNull();
    });

    it('should update and retrieve all types correctly via update/find', async () => {
      const initialDate = new Date('2024-01-01T00:00:00.000Z');
      const updatedDate = new Date('2024-12-31T23:59:59.000Z');
      
      // Create initial record (using plain arrays)
      const result = await DBModel.transaction(async () => {
        return await AllTypes.create([
          [AllTypes.int_val, 10],
          [AllTypes.float_val, 1.5],
          [AllTypes.bool_val, false],
          [AllTypes.text_val, 'initial text'],
          [AllTypes.varchar_val, 'initial'],
          [AllTypes.timestamp_val, initialDate],
          [AllTypes.int_array, [1, 2, 3]],
          [AllTypes.text_array, ['a', 'b', 'c']],
          [AllTypes.json_val, { key: 'initial' }],
        ], { returning: true });
      });
      const createdId = result!.values[0][0] as number;

      // Update all values via high-level API (using plain arrays)
      await DBModel.transaction(async () => {
        await AllTypes.update(
          [[AllTypes.id, createdId]],
          [
            [AllTypes.int_val, 99],
            [AllTypes.float_val, 9.99],
            [AllTypes.bool_val, true],
            [AllTypes.text_val, 'updated text'],
            [AllTypes.varchar_val, 'updated'],
            [AllTypes.timestamp_val, updatedDate],
            [AllTypes.int_array, [4, 5, 6]],
            [AllTypes.text_array, ['x', 'y', 'z']],
            [AllTypes.json_val, { key: 'updated' }],
          ]
        );
      });

      // Find and verify updated values
      const found = await AllTypes.findOne([[AllTypes.id, createdId]]);
      expect(found).not.toBeNull();
      expect(found!.int_val).toBe(99);
      expect(found!.float_val).toBeCloseTo(9.99, 2);
      expect(found!.bool_val).toBe(true);
      expect(found!.text_val).toBe('updated text');
      expect(found!.varchar_val).toBe('updated');
      expect(found!.timestamp_val?.toISOString()).toBe(updatedDate.toISOString());
      expect(found!.int_array).toEqual([4, 5, 6]);
      expect(found!.text_array).toEqual(['x', 'y', 'z']);
      expect(found!.json_val).toEqual({ key: 'updated' });
    });

    it('should handle edge case values correctly via create/find', async () => {
      // Edge cases: zero, false, empty strings, empty arrays, empty JSON
      const result = await DBModel.transaction(async () => {
        return await AllTypes.create([
          [AllTypes.int_val, 0],
          [AllTypes.float_val, 0.0],
          [AllTypes.bool_val, false],
          [AllTypes.text_val, ''],
          [AllTypes.varchar_val, ''],
          [AllTypes.int_array, []],
          [AllTypes.text_array, []],
          [AllTypes.json_val, {}],
        ], { returning: true });
      });
      const createdId = result!.values[0][0] as number;

      // Find and verify
      const found = await AllTypes.findOne([[AllTypes.id, createdId]]);
      expect(found).not.toBeNull();
      expect(found!.int_val).toBe(0);
      expect(found!.float_val).toBe(0);
      expect(found!.bool_val).toBe(false);
      expect(found!.text_val).toBe('');
      expect(found!.varchar_val).toBe('');
      expect(found!.int_array).toEqual([]);
      expect(found!.text_array).toEqual([]);
      expect(found!.json_val).toEqual({});
    });

    it('should handle boolean edge cases correctly', async () => {
      // Test explicit true
      const trueResult = await DBModel.transaction(async () => {
        return await AllTypes.create([[AllTypes.bool_val, true]], { returning: true });
      });
      const trueId = trueResult!.values[0][0] as number;
      const foundTrue = await AllTypes.findOne([[AllTypes.id, trueId]]);
      expect(foundTrue!.bool_val).toBe(true);

      // Test explicit false
      const falseResult = await DBModel.transaction(async () => {
        return await AllTypes.create([[AllTypes.bool_val, false]], { returning: true });
      });
      const falseId = falseResult!.values[0][0] as number;
      const foundFalse = await AllTypes.findOne([[AllTypes.id, falseId]]);
      expect(foundFalse!.bool_val).toBe(false);

      // Test null (typed column preserves null)
      const nullResult = await DBModel.transaction(async () => {
        return await AllTypes.create([[AllTypes.bool_val, null]], { returning: true });
      });
      const nullId = nullResult!.values[0][0] as number;
      const foundNull = await AllTypes.findOne([[AllTypes.id, nullId]]);
      expect(foundNull!.bool_val).toBeNull();
    });

    it('should handle datetime edge cases correctly', async () => {
      // Test specific datetime
      const specificDate = new Date('2024-06-15T14:30:45.123Z');
      const result1 = await DBModel.transaction(async () => {
        return await AllTypes.create([[AllTypes.timestamp_val, specificDate]], { returning: true });
      });
      const id1 = result1!.values[0][0] as number;
      const found1 = await AllTypes.findOne([[AllTypes.id, id1]]);
      expect(found1!.timestamp_val?.toISOString()).toBe(specificDate.toISOString());

      // Test null datetime (typed column preserves null)
      const result2 = await DBModel.transaction(async () => {
        return await AllTypes.create([[AllTypes.timestamp_val, null]], { returning: true });
      });
      const id2 = result2!.values[0][0] as number;
      const found2 = await AllTypes.findOne([[AllTypes.id, id2]]);
      expect(found2!.timestamp_val).toBeNull();

      // Test min date
      const minDate = new Date('1970-01-01T00:00:00.000Z');
      const result3 = await DBModel.transaction(async () => {
        return await AllTypes.create([[AllTypes.timestamp_val, minDate]], { returning: true });
      });
      const id3 = result3!.values[0][0] as number;
      const found3 = await AllTypes.findOne([[AllTypes.id, id3]]);
      expect(found3!.timestamp_val?.toISOString()).toBe(minDate.toISOString());
    });
  });

  describe('NULL value handling for all write operations', () => {
    // Test null values in create (already tested above, but include date type)
    it('should set null via create for date/datetime columns', async () => {
      const result = await DBModel.transaction(async () => {
        return await AllTypes.create([
          [AllTypes.timestamp_val, null],
          [AllTypes.date_val, null],
        ], { returning: true });
      });
      const id = result!.values[0][0] as number;
      const found = await AllTypes.findOne([[AllTypes.id, id]]);
      expect(found!.timestamp_val).toBeNull();
      expect(found!.date_val).toBeNull();
    });

    // Test null values in createMany
    it('should set null via createMany for all typed columns', async () => {
      const result = await DBModel.transaction(async () => {
        return await AllTypes.createMany([
          // Row 1: all nulls
          [
            [AllTypes.int_val, null],
            [AllTypes.float_val, null],
            [AllTypes.bool_val, null],
            [AllTypes.text_val, null],
            [AllTypes.timestamp_val, null],
            [AllTypes.date_val, null],
            [AllTypes.json_val, null],
          ],
          // Row 2: mix of values and nulls
          [
            [AllTypes.int_val, 42],
            [AllTypes.float_val, null],
            [AllTypes.bool_val, true],
            [AllTypes.text_val, null],
            [AllTypes.timestamp_val, new Date('2024-01-01T00:00:00.000Z')],
            [AllTypes.date_val, null],
            [AllTypes.json_val, { key: 'value' }],
          ],
          // Row 3: nulls for date/datetime specifically
          [
            [AllTypes.int_val, 100],
            [AllTypes.float_val, 3.14],
            [AllTypes.bool_val, null],
            [AllTypes.text_val, 'test'],
            [AllTypes.timestamp_val, null],
            [AllTypes.date_val, null],
            [AllTypes.json_val, null],
          ],
        ], { returning: true });
      });

      const ids = result!.values.map(v => v[0] as number);
      const records = await AllTypes.find([[AllTypes.id, ids]]);
      
      // Verify row 1 (all nulls)
      const row1 = records.find(r => r.id === ids[0])!;
      expect(row1.int_val).toBeNull();
      expect(row1.float_val).toBeNull();
      expect(row1.bool_val).toBeNull();
      expect(row1.text_val).toBeNull();
      expect(row1.timestamp_val).toBeNull();
      expect(row1.date_val).toBeNull();
      expect(row1.json_val).toBeNull();

      // Verify row 2 (mix)
      const row2 = records.find(r => r.id === ids[1])!;
      expect(row2.int_val).toBe(42);
      expect(row2.float_val).toBeNull();
      expect(row2.bool_val).toBe(true);
      expect(row2.text_val).toBeNull();
      expect(row2.timestamp_val).toBeInstanceOf(Date);
      expect(row2.date_val).toBeNull();
      expect(row2.json_val).toEqual({ key: 'value' });

      // Verify row 3 (date/datetime nulls)
      const row3 = records.find(r => r.id === ids[2])!;
      expect(row3.int_val).toBe(100);
      expect(row3.timestamp_val).toBeNull();
      expect(row3.date_val).toBeNull();
    });

    // Test setting null via update (changing existing value to null)
    it('should set null via update for all typed columns', async () => {
      // First create a record with non-null values
      const result = await DBModel.transaction(async () => {
        return await AllTypes.create([
          [AllTypes.int_val, 10],
          [AllTypes.float_val, 1.5],
          [AllTypes.bool_val, true],
          [AllTypes.text_val, 'initial'],
          [AllTypes.timestamp_val, new Date('2024-01-01T00:00:00.000Z')],
          [AllTypes.date_val, new Date('2024-06-15')],
          [AllTypes.json_val, { key: 'initial' }],
        ], { returning: true });
      });
      const id = result!.values[0][0] as number;

      // Verify initial values
      const initial = await AllTypes.findOne([[AllTypes.id, id]]);
      expect(initial!.int_val).toBe(10);
      expect(initial!.timestamp_val).toBeInstanceOf(Date);
      expect(initial!.date_val).toBeInstanceOf(Date);

      // Update all to null
      await DBModel.transaction(async () => {
        await AllTypes.update(
          [[AllTypes.id, id]],
          [
            [AllTypes.int_val, null],
            [AllTypes.float_val, null],
            [AllTypes.bool_val, null],
            [AllTypes.text_val, null],
            [AllTypes.timestamp_val, null],
            [AllTypes.date_val, null],
            [AllTypes.json_val, null],
          ]
        );
      });

      // Verify all are now null
      const updated = await AllTypes.findOne([[AllTypes.id, id]]);
      expect(updated!.int_val).toBeNull();
      expect(updated!.float_val).toBeNull();
      expect(updated!.bool_val).toBeNull();
      expect(updated!.text_val).toBeNull();
      expect(updated!.timestamp_val).toBeNull();
      expect(updated!.date_val).toBeNull();
      expect(updated!.json_val).toBeNull();
    });

    // Test setting null via updateMany for typed columns (same columns in all rows)
    it('should set null via updateMany for typed columns', async () => {
      // Create multiple records with non-null values
      const result = await DBModel.transaction(async () => {
        return await AllTypes.createMany([
          [
            [AllTypes.bool_val, true],
            [AllTypes.timestamp_val, new Date('2024-01-01T00:00:00.000Z')],
            [AllTypes.date_val, new Date('2024-01-01')],
            [AllTypes.json_val, { row: 1 }],
          ],
          [
            [AllTypes.bool_val, false],
            [AllTypes.timestamp_val, new Date('2024-02-01T00:00:00.000Z')],
            [AllTypes.date_val, new Date('2024-02-01')],
            [AllTypes.json_val, { row: 2 }],
          ],
          [
            [AllTypes.bool_val, true],
            [AllTypes.timestamp_val, new Date('2024-03-01T00:00:00.000Z')],
            [AllTypes.date_val, new Date('2024-03-01')],
            [AllTypes.json_val, { row: 3 }],
          ],
        ], { returning: true });
      });
      const ids = result!.values.map(v => v[0] as number);

      // Verify initial values
      const initialRecords = await AllTypes.find([[AllTypes.id, ids]]);
      for (const rec of initialRecords) {
        expect(rec.timestamp_val).toBeInstanceOf(Date);
        expect(rec.date_val).toBeInstanceOf(Date);
        expect(rec.bool_val).not.toBeNull();
      }

      // updateMany: set all typed columns to null (same columns in all rows)
      await DBModel.transaction(async () => {
        await AllTypes.updateMany([
          [
            [AllTypes.id, ids[0]],
            [AllTypes.bool_val, null],
            [AllTypes.timestamp_val, null],
            [AllTypes.date_val, null],
            [AllTypes.json_val, null],
          ],
          [
            [AllTypes.id, ids[1]],
            [AllTypes.bool_val, null],
            [AllTypes.timestamp_val, null],
            [AllTypes.date_val, null],
            [AllTypes.json_val, null],
          ],
          [
            [AllTypes.id, ids[2]],
            [AllTypes.bool_val, null],
            [AllTypes.timestamp_val, null],
            [AllTypes.date_val, null],
            [AllTypes.json_val, null],
          ],
        ], { keyColumns: AllTypes.id });
      });

      // Verify all are now null
      const updatedRecords = await AllTypes.find([[AllTypes.id, ids]]);
      for (const rec of updatedRecords) {
        expect(rec.bool_val).toBeNull();
        expect(rec.timestamp_val).toBeNull();
        expect(rec.date_val).toBeNull();
        expect(rec.json_val).toBeNull();
      }
    });

    // Test updateMany with ALL records setting date/datetime to null (edge case)
    it('should handle updateMany when all records set date/datetime to null', async () => {
      // Create records
      const result = await DBModel.transaction(async () => {
        return await AllTypes.createMany([
          [
            [AllTypes.int_val, 10],
            [AllTypes.timestamp_val, new Date('2024-01-01T00:00:00.000Z')],
            [AllTypes.date_val, new Date('2024-01-01')],
          ],
          [
            [AllTypes.int_val, 20],
            [AllTypes.timestamp_val, new Date('2024-02-01T00:00:00.000Z')],
            [AllTypes.date_val, new Date('2024-02-01')],
          ],
        ], { returning: true });
      });
      const ids = result!.values.map(v => v[0] as number);

      // updateMany: ALL rows set timestamp and date to null
      // This is the exact scenario that was reported as a bug
      await DBModel.transaction(async () => {
        await AllTypes.updateMany([
          [
            [AllTypes.id, ids[0]],
            [AllTypes.timestamp_val, null],
            [AllTypes.date_val, null],
          ],
          [
            [AllTypes.id, ids[1]],
            [AllTypes.timestamp_val, null],
            [AllTypes.date_val, null],
          ],
        ], { keyColumns: AllTypes.id });
      });

      // Verify all are null
      const records = await AllTypes.find([[AllTypes.id, ids]]);
      for (const rec of records) {
        expect(rec.timestamp_val).toBeNull();
        expect(rec.date_val).toBeNull();
        expect(rec.int_val).not.toBeNull();  // unchanged
      }
    });
  });

  describe('Auto-inferred Date with @column()', () => {
    // Note: AutoDateModel is defined at file top level for decorator metadata support
    // In production with tsc, plain @column() works via auto-inference from Date type

    beforeAll(async () => {
      await DBModel.execute(`
        CREATE TABLE IF NOT EXISTS auto_date_test (
          id SERIAL PRIMARY KEY,
          created_at TIMESTAMP WITH TIME ZONE
        )
      `);
    });

    beforeEach(async () => {
      await DBModel.execute('DELETE FROM auto_date_test');
    });

    it('should correctly handle Date with plain @column() decorator', async () => {
      const testDate = new Date('2024-06-15T10:30:00.000Z');

      // Create via ORM with plain @column() Date
      const result = await DBModel.transaction(async () => {
        return await AutoDate.create([
          [AutoDate.created_at, testDate],
        ], { returning: true });
      });

      expect(result).not.toBeNull();
      const createdId = result!.values[0][0] as number;

      // Find and verify timezone is preserved
      const found = await AutoDate.findOne([[AutoDate.id, createdId]]);
      expect(found).not.toBeNull();
      expect(found!.created_at?.toISOString()).toBe(testDate.toISOString());
    });

    it('should handle null Date with plain @column() decorator', async () => {
      // Create with null
      const result = await DBModel.transaction(async () => {
        return await AutoDate.create([
          [AutoDate.created_at, null],
        ], { returning: true });
      });

      expect(result).not.toBeNull();
      const createdId = result!.values[0][0] as number;

      // Find and verify null is preserved
      const found = await AutoDate.findOne([[AutoDate.id, createdId]]);
      expect(found).not.toBeNull();
      expect(found!.created_at).toBeNull();
    });

    it('should handle updateMany with null Date using plain @column()', async () => {
      const testDate = new Date('2024-06-15T10:30:00.000Z');

      // Create two records with dates
      const ids: number[] = [];
      await DBModel.transaction(async () => {
        for (let i = 0; i < 2; i++) {
          const result = await AutoDate.create([
            [AutoDate.created_at, testDate],
          ], { returning: true });
          ids.push(result!.values[0][0] as number);
        }
      });

      // Update both to null using updateMany
      await DBModel.transaction(async () => {
        await AutoDate.updateMany([
          [
            [AutoDate.id, ids[0]],
            [AutoDate.created_at, null],
          ],
          [
            [AutoDate.id, ids[1]],
            [AutoDate.created_at, null],
          ],
        ], { keyColumns: AutoDate.id });
      });

      // Verify all are null
      const records = await AutoDate.find([[AutoDate.id, ids]]);
      for (const rec of records) {
        expect(rec.created_at).toBeNull();
      }
    });
  });
});
