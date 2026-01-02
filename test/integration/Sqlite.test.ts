/**
 * SQLite Integration Tests
 *
 * Tests for SQLite driver functionality.
 */

import 'reflect-metadata';
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { DBModel, model, column, ColumnsOf, closeAllPools, hasMany, createRelationContext } from '../../src';
import type { DBConfig } from '../../src';
import * as fs from 'fs';
import * as path from 'path';

// Define a model for all types testing (outside describe block for decorator support)
@model('all_types_test')
class SqliteAllTypesModel extends DBModel {
  @column() id?: number;
  @column() int_val?: number | null;
  @column() float_val?: number | null;
  @column.boolean() bool_val?: boolean | null;
  @column() text_val?: string | null;
  @column() varchar_val?: string | null;
  @column.datetime() timestamp_val?: Date | null;
  @column() date_val?: string | null;
  @column.json() json_val?: Record<string, unknown> | unknown[] | null;
}
const SqliteAllTypes = SqliteAllTypesModel as typeof SqliteAllTypesModel & ColumnsOf<SqliteAllTypesModel>;

// ============================================
// Test Configuration
// ============================================

const testDbPath = path.join(__dirname, '../fixtures/test.sqlite');

const testConfig: DBConfig = {
  database: testDbPath,
  driver: 'sqlite',
};

// ============================================
// Test Models
// ============================================

@model('users')
class User extends DBModel {
  @column() id?: number;
  @column() name?: string;
  @column() email?: string;
  @column() is_active?: boolean;
  @column() created_at?: string;
}

// Cast to include column properties
const UserModel = User as typeof User & ColumnsOf<User>;

@model('posts')
class Post extends DBModel {
  @column() id?: number;
  @column() user_id?: number;
  @column() title?: string;
  @column() content?: string;
  @column() published?: boolean;
}

const PostModel = Post as typeof Post & ColumnsOf<Post>;

// ============================================
// Tests
// ============================================

describe('SQLite Driver', () => {
  beforeAll(async () => {
    // Clean up existing test database
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }

    // Initialize DBModel with SQLite config
    DBModel.setConfig(testConfig);

    // Create test tables
    await DBModel.execute(`
      CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await DBModel.execute(`
      CREATE TABLE IF NOT EXISTS posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        content TEXT,
        published INTEGER DEFAULT 0,
        FOREIGN KEY (user_id) REFERENCES users(id)
      )
    `);
  });

  afterAll(async () => {
    await closeAllPools();

    // Clean up test database file
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }
    // Also clean up WAL files
    const walPath = testDbPath + '-wal';
    const shmPath = testDbPath + '-shm';
    if (fs.existsSync(walPath)) fs.unlinkSync(walPath);
    if (fs.existsSync(shmPath)) fs.unlinkSync(shmPath);
  });

  beforeEach(async () => {
    // Clear test data
    await DBModel.execute('DELETE FROM posts');
    await DBModel.execute('DELETE FROM users');
  });

  describe('Basic CRUD Operations', () => {
    it('should create a record', async () => {
      const result = await DBModel.transaction(async () => {
        return await UserModel.create([
          [UserModel.name, 'Alice'],
          [UserModel.email, 'alice@example.com'],
        ], { returning: true });
      });

      expect(result).not.toBeNull();
      expect(result!.values).toHaveLength(1);
      const userId = result!.values[0][0] as number;
      expect(userId).toBeDefined();

      const user = await UserModel.findOne([[UserModel.id, userId]]);
      expect(user).toBeInstanceOf(User);
      expect(user?.name).toBe('Alice');
      expect(user?.email).toBe('alice@example.com');
    });

    it('should find records', async () => {
      await DBModel.transaction(async () => {
        await UserModel.create([
          [UserModel.name, 'Bob'],
          [UserModel.email, 'bob@example.com'],
        ]);
        await UserModel.create([
          [UserModel.name, 'Charlie'],
          [UserModel.email, 'charlie@example.com'],
        ]);
      });

      const users = await UserModel.find([]);
      expect(users.length).toBe(2);
    });

    it('should find with conditions', async () => {
      await DBModel.transaction(async () => {
        await UserModel.create([
          [UserModel.name, 'David'],
          [UserModel.email, 'david@example.com'],
          [UserModel.is_active, true],
        ]);
        await UserModel.create([
          [UserModel.name, 'Eve'],
          [UserModel.email, 'eve@example.com'],
          [UserModel.is_active, false],
        ]);
      });

      // SQLite stores boolean as 1/0, so query with 1
      const activeUsers = await UserModel.find([[UserModel.is_active, 1]]);
      expect(activeUsers.length).toBe(1);
      expect(activeUsers[0].name).toBe('David');
    });

    it('should findOne', async () => {
      await DBModel.transaction(async () => {
        await UserModel.create([
          [UserModel.name, 'Frank'],
          [UserModel.email, 'frank@example.com'],
        ]);
      });

      const user = await UserModel.findOne([[UserModel.email, 'frank@example.com']]);
      expect(user).not.toBeNull();
      expect(user?.name).toBe('Frank');
    });

    it('should update a record', async () => {
      const result = await DBModel.transaction(async () => {
        return await UserModel.create([
          [UserModel.name, 'Grace'],
          [UserModel.email, 'grace@example.com'],
        ], { returning: true });
      });
      const userId = result!.values[0][0] as number;

      await DBModel.transaction(async () => {
        await UserModel.update(
          [[UserModel.id, userId]],
          [[UserModel.name, 'Grace Updated']]
        );
      });

      const updated = await UserModel.findOne([[UserModel.id, userId]]);
      expect(updated?.name).toBe('Grace Updated');
    });

    it('should delete a record', async () => {
      const result = await DBModel.transaction(async () => {
        return await UserModel.create([
          [UserModel.name, 'Henry'],
          [UserModel.email, 'henry@example.com'],
        ], { returning: true });
      });
      const userId = result!.values[0][0] as number;

      await DBModel.transaction(async () => {
        await UserModel.delete([[UserModel.id, userId]]);
      });

      const deleted = await UserModel.findOne([[UserModel.id, userId]]);
      expect(deleted).toBeNull();
    });
  });

  describe('Instance Methods', () => {
    it('should create a new record using static create()', async () => {
      const result = await DBModel.transaction(async () => {
        return await UserModel.create([
          [UserModel.name, 'Ivy'],
          [UserModel.email, 'ivy@example.com'],
        ], { returning: true });
      });

      expect(result).not.toBeNull();
      expect(result!.values).toHaveLength(1);

      const found = await UserModel.findOne([[UserModel.email, 'ivy@example.com']]);
      expect(found).not.toBeNull();
      expect(found?.name).toBe('Ivy');
    });

    it('should update an existing record using static update()', async () => {
      const createResult = await DBModel.transaction(async () => {
        return await UserModel.create([
          [UserModel.name, 'Jack'],
          [UserModel.email, 'jack@example.com'],
        ], { returning: true });
      });

      const userId = createResult!.values[0][0] as number;

      await DBModel.transaction(async () => {
        await UserModel.update(
          [[UserModel.id, userId]],
          [[UserModel.name, 'Jack Updated']],
        );
      });

      const updated = await UserModel.findOne([[UserModel.id, userId]]);
      expect(updated?.name).toBe('Jack Updated');
    });

    it('should delete a record using static delete()', async () => {
      const createResult = await DBModel.transaction(async () => {
        return await UserModel.create([
          [UserModel.name, 'Kate'],
          [UserModel.email, 'kate@example.com'],
        ], { returning: true });
      });

      const userId = createResult!.values[0][0] as number;

      await DBModel.transaction(async () => {
        await UserModel.delete([[UserModel.id, userId]]);
      });

      const deleted = await UserModel.findOne([[UserModel.id, userId]]);
      expect(deleted).toBeNull();
    });
  });

  describe('Relations', () => {
    it('should handle foreign key relationships', async () => {
      const userId = await DBModel.transaction(async () => {
        const userResult = await UserModel.create([
          [UserModel.name, 'Leo'],
          [UserModel.email, 'leo@example.com'],
        ], { returning: true });
        const id = userResult!.values[0][0] as number;

        await PostModel.create([
          [PostModel.user_id, id],
          [PostModel.title, 'First Post'],
          [PostModel.content, 'Hello World'],
        ]);

        await PostModel.create([
          [PostModel.user_id, id],
          [PostModel.title, 'Second Post'],
          [PostModel.content, 'Another post'],
        ]);

        return id;
      });

      const posts = await PostModel.find([[PostModel.user_id, userId]]);
      expect(posts.length).toBe(2);
    });
  });

  describe('Transactions', () => {
    it('should commit transaction', async () => {
      await DBModel.transaction(async () => {
        await UserModel.create([
          [UserModel.name, 'Mike'],
          [UserModel.email, 'mike@example.com'],
        ]);
        await UserModel.create([
          [UserModel.name, 'Nancy'],
          [UserModel.email, 'nancy@example.com'],
        ]);
      });

      const users = await UserModel.find([]);
      expect(users.length).toBe(2);
    });

    it('should rollback transaction on error', async () => {
      try {
        await DBModel.transaction(async () => {
          await UserModel.create([
            [UserModel.name, 'Oliver'],
            [UserModel.email, 'oliver@example.com'],
          ]);
          // This should fail due to duplicate email
          await UserModel.create([
            [UserModel.name, 'Oliver2'],
            [UserModel.email, 'oliver@example.com'],
          ]);
        });
      } catch {
        // Expected error
      }

      const users = await UserModel.find([[UserModel.email, 'oliver@example.com']]);
      // Transaction should have been rolled back
      expect(users.length).toBe(0);
    });
  });

  describe('Count and Aggregation', () => {
    it('should count records', async () => {
      await DBModel.transaction(async () => {
        await UserModel.create([[UserModel.name, 'P1'], [UserModel.email, 'p1@example.com']]);
        await UserModel.create([[UserModel.name, 'P2'], [UserModel.email, 'p2@example.com']]);
        await UserModel.create([[UserModel.name, 'P3'], [UserModel.email, 'p3@example.com']]);
      });

      const count = await UserModel.count([]);
      expect(count).toBe(3);
    });

    it('should count with conditions', async () => {
      await DBModel.transaction(async () => {
        await UserModel.create([[UserModel.name, 'Q1'], [UserModel.email, 'q1@example.com'], [UserModel.is_active, true]]);
        await UserModel.create([[UserModel.name, 'Q2'], [UserModel.email, 'q2@example.com'], [UserModel.is_active, false]]);
      });

      // SQLite stores boolean as 1/0
      const activeCount = await UserModel.count([[UserModel.is_active, 1]]);
      expect(activeCount).toBe(1);
    });
  });

  describe('Order and Limit', () => {
    it('should order results', async () => {
      await DBModel.transaction(async () => {
        await UserModel.create([[UserModel.name, 'Zack'], [UserModel.email, 'zack@example.com']]);
        await UserModel.create([[UserModel.name, 'Amy'], [UserModel.email, 'amy@example.com']]);
        await UserModel.create([[UserModel.name, 'Mike'], [UserModel.email, 'mike2@example.com']]);
      });

      const users = await UserModel.find([], { order: UserModel.name.asc() });
      expect(users[0].name).toBe('Amy');
      expect(users[2].name).toBe('Zack');
    });

    it('should limit results', async () => {
      await DBModel.transaction(async () => {
        await UserModel.create([[UserModel.name, 'R1'], [UserModel.email, 'r1@example.com']]);
        await UserModel.create([[UserModel.name, 'R2'], [UserModel.email, 'r2@example.com']]);
        await UserModel.create([[UserModel.name, 'R3'], [UserModel.email, 'r3@example.com']]);
      });

      const users = await UserModel.find([], { limit: 2 });
      expect(users.length).toBe(2);
    });

    it('should offset results', async () => {
      await DBModel.transaction(async () => {
        await UserModel.create([[UserModel.name, 'S1'], [UserModel.email, 's1@example.com']]);
        await UserModel.create([[UserModel.name, 'S2'], [UserModel.email, 's2@example.com']]);
        await UserModel.create([[UserModel.name, 'S3'], [UserModel.email, 's3@example.com']]);
      });

      const users = await UserModel.find([], { order: UserModel.name.asc(), limit: 2, offset: 1 });
      expect(users.length).toBe(2);
      expect(users[0].name).toBe('S2');
    });
  });

  describe('ON CONFLICT (upsert)', () => {
    it('should ignore insert on conflict', async () => {
      const uniqueEmail = `conflict-ignore-${Date.now()}@example.com`;
      
      // First insert
      await DBModel.transaction(async () => {
        await UserModel.create([[UserModel.name, 'First'], [UserModel.email, uniqueEmail]]);
      });

      // Try to insert with same email using raw SQL with ON CONFLICT
      await DBModel.execute(
        `INSERT INTO users (name, email, is_active) VALUES (?, ?, ?) 
         ON CONFLICT (email) DO NOTHING`,
        ['Second', uniqueEmail, 1]
      );

      // Verify first record still exists with original name
      const user = await UserModel.findOne([[UserModel.email, uniqueEmail]]);
      expect(user?.name).toBe('First');
    });

    it('should update on conflict', async () => {
      const uniqueEmail = `conflict-update-${Date.now()}@example.com`;
      
      // First insert
      await DBModel.transaction(async () => {
        await UserModel.create([[UserModel.name, 'First'], [UserModel.email, uniqueEmail]]);
      });

      // Upsert with same email - should update name
      await DBModel.execute(
        `INSERT INTO users (name, email, is_active) VALUES (?, ?, ?) 
         ON CONFLICT (email) DO UPDATE SET name = excluded.name`,
        ['Updated', uniqueEmail, 1]
      );

      // Verify record was updated
      const user = await UserModel.findOne([[UserModel.email, uniqueEmail]]);
      expect(user?.name).toBe('Updated');
    });
  });

  describe('Data Type Persistence', () => {
    // Create a dedicated table for type testing
    beforeAll(async () => {
      await DBModel.execute(`
        CREATE TABLE IF NOT EXISTS all_types_test (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          int_val INTEGER,
          float_val REAL,
          bool_val INTEGER,
          text_val TEXT,
          varchar_val TEXT,
          timestamp_val TEXT,
          date_val TEXT,
          json_val TEXT
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
        return await SqliteAllTypes.create([
          [SqliteAllTypes.int_val, 42],
          [SqliteAllTypes.float_val, 3.14159],
          [SqliteAllTypes.bool_val, true],
          [SqliteAllTypes.text_val, 'long text content'],
          [SqliteAllTypes.varchar_val, 'short varchar'],
          [SqliteAllTypes.timestamp_val, testDate],
        ], { returning: true });
      });
      
      expect(result).not.toBeNull();
      const createdId = result!.values[0][0] as number;

      // Find and verify values are preserved
      const found = await SqliteAllTypes.findOne([[SqliteAllTypes.id, createdId]]);
      expect(found).not.toBeNull();
      expect(found!.id).toBeDefined();
      expect(found!.int_val).toBe(42);
      expect(found!.float_val).toBeCloseTo(3.14159, 4);
      expect(found!.bool_val).toBe(true);
      expect(found!.text_val).toBe('long text content');
      expect(found!.varchar_val).toBe('short varchar');
      expect(found!.timestamp_val?.toISOString()).toBe(testDate.toISOString());
    });

    it('should persist and retrieve JSON types correctly via create/find', async () => {
      const jsonObj = { name: 'test', count: 42, nested: { key: 'value' } };
      
      // Create with JSON type via high-level API
      const result = await DBModel.transaction(async () => {
        return await SqliteAllTypes.create([
          [SqliteAllTypes.json_val, jsonObj],
        ], { returning: true });
      });
      
      const createdId = result!.values[0][0] as number;

      // Find and verify
      const found = await SqliteAllTypes.findOne([[SqliteAllTypes.id, createdId]]);
      expect(found).not.toBeNull();
      expect(found!.json_val).toEqual(jsonObj);
    });

    it('should persist and retrieve JSON array correctly via create/find', async () => {
      const jsonArray = [1, 'two', { three: 3 }];
      
      // Create with JSON array via high-level API
      const result = await DBModel.transaction(async () => {
        return await SqliteAllTypes.create([
          [SqliteAllTypes.json_val, jsonArray],
        ], { returning: true });
      });
      
      const createdId = result!.values[0][0] as number;

      // Find and verify
      const found = await SqliteAllTypes.findOne([[SqliteAllTypes.id, createdId]]);
      expect(found).not.toBeNull();
      expect(found!.json_val).toEqual(jsonArray);
    });

    it('should persist and retrieve NULL values correctly via create/find', async () => {
      // Create with NULL values via high-level API
      const result = await DBModel.transaction(async () => {
        return await SqliteAllTypes.create([
          [SqliteAllTypes.int_val, null],
          [SqliteAllTypes.float_val, null],
          [SqliteAllTypes.bool_val, null],
          [SqliteAllTypes.text_val, null],
          [SqliteAllTypes.timestamp_val, null],
          [SqliteAllTypes.json_val, null],
        ], { returning: true });
      });
      
      const createdId = result!.values[0][0] as number;

      // Find and verify
      const found = await SqliteAllTypes.findOne([[SqliteAllTypes.id, createdId]]);
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
      
      // Create initial record via high-level API
      const result = await DBModel.transaction(async () => {
        return await SqliteAllTypes.create([
          [SqliteAllTypes.int_val, 10],
          [SqliteAllTypes.float_val, 1.5],
          [SqliteAllTypes.bool_val, false],
          [SqliteAllTypes.text_val, 'initial text'],
          [SqliteAllTypes.varchar_val, 'initial'],
          [SqliteAllTypes.timestamp_val, initialDate],
          [SqliteAllTypes.json_val, { key: 'initial' }],
        ], { returning: true });
      });
      const createdId = result!.values[0][0] as number;

      // Update all values via high-level API
      await DBModel.transaction(async () => {
        await SqliteAllTypes.update(
          [[SqliteAllTypes.id, createdId]],
          [
            [SqliteAllTypes.int_val, 99],
            [SqliteAllTypes.float_val, 9.99],
            [SqliteAllTypes.bool_val, true],
            [SqliteAllTypes.text_val, 'updated text'],
            [SqliteAllTypes.varchar_val, 'updated'],
            [SqliteAllTypes.timestamp_val, updatedDate],
            [SqliteAllTypes.json_val, { key: 'updated' }],
          ]
        );
      });

      // Find and verify updated values
      const found = await SqliteAllTypes.findOne([[SqliteAllTypes.id, createdId]]);
      expect(found).not.toBeNull();
      expect(found!.int_val).toBe(99);
      expect(found!.float_val).toBeCloseTo(9.99, 2);
      expect(found!.bool_val).toBe(true);
      expect(found!.text_val).toBe('updated text');
      expect(found!.varchar_val).toBe('updated');
      expect(found!.timestamp_val?.toISOString()).toBe(updatedDate.toISOString());
      expect(found!.json_val).toEqual({ key: 'updated' });
    });

    it('should handle edge case values correctly via create/find', async () => {
      // Edge cases: zero, false, empty strings, empty JSON
      const result = await DBModel.transaction(async () => {
        return await SqliteAllTypes.create([
          [SqliteAllTypes.int_val, 0],
          [SqliteAllTypes.float_val, 0.0],
          [SqliteAllTypes.bool_val, false],
          [SqliteAllTypes.text_val, ''],
          [SqliteAllTypes.varchar_val, ''],
          [SqliteAllTypes.json_val, {}],
        ], { returning: true });
      });
      const createdId = result!.values[0][0] as number;

      // Find and verify
      const found = await SqliteAllTypes.findOne([[SqliteAllTypes.id, createdId]]);
      expect(found).not.toBeNull();
      expect(found!.int_val).toBe(0);
      expect(found!.float_val).toBe(0);
      expect(found!.bool_val).toBe(false);
      expect(found!.text_val).toBe('');
      expect(found!.varchar_val).toBe('');
      expect(found!.json_val).toEqual({});
    });

    it('should handle boolean edge cases correctly', async () => {
      // Test explicit true
      const trueResult = await DBModel.transaction(async () => {
        return await SqliteAllTypes.create([[SqliteAllTypes.bool_val, true]], { returning: true });
      });
      const trueId = trueResult!.values[0][0] as number;
      const foundTrue = await SqliteAllTypes.findOne([[SqliteAllTypes.id, trueId]]);
      expect(foundTrue!.bool_val).toBe(true);

      // Test explicit false
      const falseResult = await DBModel.transaction(async () => {
        return await SqliteAllTypes.create([[SqliteAllTypes.bool_val, false]], { returning: true });
      });
      const falseId = falseResult!.values[0][0] as number;
      const foundFalse = await SqliteAllTypes.findOne([[SqliteAllTypes.id, falseId]]);
      expect(foundFalse!.bool_val).toBe(false);

      // Test null (typed column returns undefined for null)
      const nullResult = await DBModel.transaction(async () => {
        return await SqliteAllTypes.create([[SqliteAllTypes.bool_val, null]], { returning: true });
      });
      const nullId = nullResult!.values[0][0] as number;
      const foundNull = await SqliteAllTypes.findOne([[SqliteAllTypes.id, nullId]]);
      expect(foundNull!.bool_val).toBeNull();
    });

    it('should handle datetime edge cases correctly', async () => {
      // Test specific datetime
      const specificDate = new Date('2024-06-15T14:30:45.123Z');
      const result1 = await DBModel.transaction(async () => {
        return await SqliteAllTypes.create([[SqliteAllTypes.timestamp_val, specificDate]], { returning: true });
      });
      const id1 = result1!.values[0][0] as number;
      const found1 = await SqliteAllTypes.findOne([[SqliteAllTypes.id, id1]]);
      expect(found1!.timestamp_val?.toISOString()).toBe(specificDate.toISOString());

      // Test null datetime (typed column preserves null)
      const result2 = await DBModel.transaction(async () => {
        return await SqliteAllTypes.create([[SqliteAllTypes.timestamp_val, null]], { returning: true });
      });
      const id2 = result2!.values[0][0] as number;
      const found2 = await SqliteAllTypes.findOne([[SqliteAllTypes.id, id2]]);
      expect(found2!.timestamp_val).toBeNull();

      // Test min date
      const minDate = new Date('1970-01-01T00:00:00.000Z');
      const result3 = await DBModel.transaction(async () => {
        return await SqliteAllTypes.create([[SqliteAllTypes.timestamp_val, minDate]], { returning: true });
      });
      const id3 = result3!.values[0][0] as number;
      const found3 = await SqliteAllTypes.findOne([[SqliteAllTypes.id, id3]]);
      expect(found3!.timestamp_val?.toISOString()).toBe(minDate.toISOString());
    });
  });

  // ============================================
  // Lazy Loading with LIMIT Tests (ROW_NUMBER)
  // ============================================

  describe('Lazy Loading with LIMIT (ROW_NUMBER)', () => {
    // Model with hasMany relation and limit
    @model('posts')
    class SqlitePostModel extends DBModel {
      @column() id?: number;
      @column() user_id?: number;
      @column() title?: string;
      @column() content?: string;
    }
    const SqlitePost = SqlitePostModel as typeof SqlitePostModel & ColumnsOf<SqlitePostModel>;

    @model('users')
    class SqliteUserWithPostsModel extends DBModel {
      @column() id?: number;
      @column() name?: string;
      @column() email?: string;

      @hasMany(() => [SqliteUserWithPosts.id, SqlitePost.user_id], {
        limit: 2,
        order: () => SqlitePost.id.desc(),
      })
      declare recentPosts: Promise<SqlitePostModel[]>;

      @hasMany(() => [SqliteUserWithPosts.id, SqlitePost.user_id])
      declare allPosts: Promise<SqlitePostModel[]>;
    }
    const SqliteUserWithPosts = SqliteUserWithPostsModel as typeof SqliteUserWithPostsModel & ColumnsOf<SqliteUserWithPostsModel>;

    beforeAll(() => {
      DBModel['_registerModel']('SqliteUserWithPostsModel', SqliteUserWithPosts);
      DBModel['_registerModel']('SqlitePostModel', SqlitePost);
    });

    it('should limit hasMany results using ROW_NUMBER (single key)', async () => {
      // Create user
      const userId = await DBModel.transaction(async () => {
        const result = await UserModel.create([
          [UserModel.name, 'Test User'],
          [UserModel.email, 'rownum@test.com'],
        ], { returning: true });
        const uId = result!.values[0][0] as number;

        // Create 5 posts for the user
        for (let i = 1; i <= 5; i++) {
          await PostModel.create([
            [PostModel.user_id, uId],
            [PostModel.title, `Post ${i}`],
            [PostModel.content, `Content ${i}`],
          ]);
        }
        return uId;
      });

      // Find user using model with limit
      const [foundUser] = await SqliteUserWithPosts.find([[SqliteUserWithPosts.id, userId]]);

      // recentPosts should only return 2 (most recent due to ORDER BY desc)
      const recentPosts = await (foundUser as SqliteUserWithPostsModel).recentPosts;
      expect(recentPosts.length).toBe(2);
      expect(recentPosts[0].title).toBe('Post 5');
      expect(recentPosts[1].title).toBe('Post 4');

      // allPosts should return all 5
      const allPosts = await (foundUser as SqliteUserWithPostsModel).allPosts;
      expect(allPosts.length).toBe(5);
    });

    it('should batch load with limit for multiple parents (single key)', async () => {
      // Create users and posts
      const { user1Id, user2Id } = await DBModel.transaction(async () => {
        const u1Result = await UserModel.create([
          [UserModel.name, 'User 1'],
          [UserModel.email, 'batch1@test.com'],
        ], { returning: true });
        const u1Id = u1Result!.values[0][0] as number;
        
        const u2Result = await UserModel.create([
          [UserModel.name, 'User 2'],
          [UserModel.email, 'batch2@test.com'],
        ], { returning: true });
        const u2Id = u2Result!.values[0][0] as number;

        // Create posts for user1 (4 posts)
        for (let i = 1; i <= 4; i++) {
          await PostModel.create([
            [PostModel.user_id, u1Id],
            [PostModel.title, `U1 Post ${i}`],
            [PostModel.content, 'Content'],
          ]);
        }

        // Create posts for user2 (3 posts)
        for (let i = 1; i <= 3; i++) {
          await PostModel.create([
            [PostModel.user_id, u2Id],
            [PostModel.title, `U2 Post ${i}`],
            [PostModel.content, 'Content'],
          ]);
        }

        return { user1Id: u1Id, user2Id: u2Id };
      });

      // Load both users
      const users = await SqliteUserWithPosts.find([], { order: 'id' });
      createRelationContext(SqliteUserWithPostsModel, users as SqliteUserWithPostsModel[]);

      // Access recentPosts - should batch load with LIMIT per user
      const u1Posts = await (users[0] as SqliteUserWithPostsModel).recentPosts;
      const u2Posts = await (users[1] as SqliteUserWithPostsModel).recentPosts;

      // Each user should get at most 2 posts (limit: 2)
      expect(u1Posts.length).toBe(2);
      expect(u2Posts.length).toBe(2);

      // Should be ordered correctly (desc by id)
      expect(u1Posts[0].title).toBe('U1 Post 4');
      expect(u2Posts[0].title).toBe('U2 Post 3');
    });
  });

  // ============================================
  // Lazy Loading with LIMIT Tests (Composite Key)
  // ============================================

  describe('Lazy Loading with LIMIT - Composite Key (ROW_NUMBER)', () => {
    beforeAll(async () => {
      // Create composite key tables
      await DBModel.execute(`
        CREATE TABLE IF NOT EXISTS tenant_users (
          tenant_id INTEGER NOT NULL,
          id INTEGER NOT NULL,
          name TEXT NOT NULL,
          PRIMARY KEY (tenant_id, id)
        )
      `);

      await DBModel.execute(`
        CREATE TABLE IF NOT EXISTS tenant_posts (
          tenant_id INTEGER NOT NULL,
          id INTEGER NOT NULL,
          user_id INTEGER NOT NULL,
          title TEXT NOT NULL,
          PRIMARY KEY (tenant_id, id)
        )
      `);
    });

    afterAll(async () => {
      await DBModel.execute('DROP TABLE IF EXISTS tenant_posts');
      await DBModel.execute('DROP TABLE IF EXISTS tenant_users');
    });

    beforeEach(async () => {
      await DBModel.execute('DELETE FROM tenant_posts');
      await DBModel.execute('DELETE FROM tenant_users');
    });

    // Define models for composite key
    @model('tenant_posts')
    class SqliteTenantPostModel extends DBModel {
      @column() tenant_id?: number;
      @column() id?: number;
      @column() user_id?: number;
      @column() title?: string;
    }
    const SqliteTenantPost = SqliteTenantPostModel as typeof SqliteTenantPostModel & ColumnsOf<SqliteTenantPostModel>;

    @model('tenant_users')
    class SqliteTenantUserModel extends DBModel {
      @column() tenant_id?: number;
      @column() id?: number;
      @column() name?: string;

      @hasMany(() => [
        [SqliteTenantUser.tenant_id, SqliteTenantPost.tenant_id],
        [SqliteTenantUser.id, SqliteTenantPost.user_id],
      ], {
        limit: 2,
        order: () => SqliteTenantPost.id.desc(),
      })
      declare recentPosts: Promise<SqliteTenantPostModel[]>;

      @hasMany(() => [
        [SqliteTenantUser.tenant_id, SqliteTenantPost.tenant_id],
        [SqliteTenantUser.id, SqliteTenantPost.user_id],
      ])
      declare allPosts: Promise<SqliteTenantPostModel[]>;
    }
    const SqliteTenantUser = SqliteTenantUserModel as typeof SqliteTenantUserModel & ColumnsOf<SqliteTenantUserModel>;

    it('should limit hasMany results using ROW_NUMBER (composite key)', async () => {
      DBModel['_registerModel']('SqliteTenantUserModel', SqliteTenantUser);
      DBModel['_registerModel']('SqliteTenantPostModel', SqliteTenantPost);

      // Create tenant user
      await DBModel.execute(
        `INSERT INTO tenant_users (tenant_id, id, name) VALUES (?, ?, ?)`,
        [1, 100, 'Tenant1 User']
      );

      // Create 5 posts
      for (let i = 1; i <= 5; i++) {
        await DBModel.execute(
          `INSERT INTO tenant_posts (tenant_id, id, user_id, title) VALUES (?, ?, ?, ?)`,
          [1, i, 100, `Post ${i}`]
        );
      }

      // Find user
      const [user] = await SqliteTenantUser.find([
        [SqliteTenantUser.tenant_id, 1],
        [SqliteTenantUser.id, 100],
      ]);

      // recentPosts should only return 2 (most recent)
      const recentPosts = await (user as SqliteTenantUserModel).recentPosts;
      expect(recentPosts.length).toBe(2);
      expect(recentPosts[0].title).toBe('Post 5');
      expect(recentPosts[1].title).toBe('Post 4');

      // allPosts should return all 5
      const allPosts = await (user as SqliteTenantUserModel).allPosts;
      expect(allPosts.length).toBe(5);
    });

    it('should batch load with limit for multiple parents (composite key)', async () => {
      DBModel['_registerModel']('SqliteTenantUserModel', SqliteTenantUser);
      DBModel['_registerModel']('SqliteTenantPostModel', SqliteTenantPost);

      // Create users in different tenants
      await DBModel.execute(`INSERT INTO tenant_users (tenant_id, id, name) VALUES (1, 100, 'T1 User')`, []);
      await DBModel.execute(`INSERT INTO tenant_users (tenant_id, id, name) VALUES (2, 100, 'T2 User')`, []);

      // Create posts for tenant 1 user (4 posts)
      for (let i = 1; i <= 4; i++) {
        await DBModel.execute(
          `INSERT INTO tenant_posts (tenant_id, id, user_id, title) VALUES (1, ?, 100, ?)`,
          [i, `T1 Post ${i}`]
        );
      }

      // Create posts for tenant 2 user (3 posts)
      for (let i = 1; i <= 3; i++) {
        await DBModel.execute(
          `INSERT INTO tenant_posts (tenant_id, id, user_id, title) VALUES (2, ?, 100, ?)`,
          [i, `T2 Post ${i}`]
        );
      }

      // Load both users
      const users = await SqliteTenantUser.find([], { order: 'tenant_id, id' });
      createRelationContext(SqliteTenantUserModel, users as SqliteTenantUserModel[]);

      // Access recentPosts - should batch load with LIMIT per composite key
      const t1Posts = await (users[0] as SqliteTenantUserModel).recentPosts;
      const t2Posts = await (users[1] as SqliteTenantUserModel).recentPosts;

      // Each user should get at most 2 posts (limit: 2)
      expect(t1Posts.length).toBe(2);
      expect(t2Posts.length).toBe(2);

      // Should be ordered correctly (desc by id)
      expect(t1Posts[0].title).toBe('T1 Post 4');
      expect(t1Posts[1].title).toBe('T1 Post 3');
      expect(t2Posts[0].title).toBe('T2 Post 3');
      expect(t2Posts[1].title).toBe('T2 Post 2');
    });
  });
});

