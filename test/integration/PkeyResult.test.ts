/**
 * PkeyResult Integration Tests
 *
 * Tests for the new PkeyResult return type and related features:
 * - create/createMany with returning option
 * - update with returning option
 * - updateMany method
 * - delete with returning option
 * - findById with PkeyResult format
 */

import 'reflect-metadata';
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { DBModel, model, column, PkeyResult, closeAllPools } from '../../src';
import type { ColumnsOf } from '../../src';

// ============================================
// Test Database Configuration
// ============================================

const testConfig = {
  host: process.env.TEST_DB_HOST || 'localhost',
  port: parseInt(process.env.TEST_DB_PORT || '5433', 10),
  database: process.env.TEST_DB_NAME || 'testdb',
  user: process.env.TEST_DB_USER || 'testuser',
  password: process.env.TEST_DB_PASSWORD || 'testpass',
  max: 5,
};

// Skip integration tests if SKIP_INTEGRATION_TESTS=1 is set
const skipIntegrationTests = process.env.SKIP_INTEGRATION_TESTS === '1';

// ============================================
// Test Model: User
// ============================================

@model('users')
class UserModel extends DBModel {
  @column() id?: number;
  @column() name?: string;
  @column() email?: string;
  @column.boolean() is_active?: boolean;
  @column() role?: string;
  @column.stringArray() tags?: string[];
  @column.json<Record<string, unknown>>() metadata?: Record<string, unknown>;
  @column.datetime() created_at?: Date;
  @column.datetime() updated_at?: Date;
  @column.datetime() deleted_at?: Date | null;
}
const User = UserModel as typeof UserModel & ColumnsOf<UserModel>;

// ============================================
// Test Model: PostTag (Composite PK)
// ============================================

@model('post_tags')
class PostTagModel extends DBModel {
  @column({ primaryKey: true }) post_id?: number;
  @column({ primaryKey: true }) tag_id?: number;
  @column.datetime() created_at?: Date;
}
const PostTag = PostTagModel as typeof PostTagModel & ColumnsOf<PostTagModel>;

// ============================================
// Cleanup Helper
// ============================================

async function cleanupTestUsers() {
  await DBModel.execute(`DELETE FROM users WHERE email LIKE 'pkeytest-%'`);
}

async function cleanupTestTags() {
  await DBModel.execute(`DELETE FROM post_tags WHERE post_id >= 10000`);
  await DBModel.execute(`DELETE FROM posts WHERE id >= 10000`);
}

async function setupTestPostsForTags() {
  // Create parent posts first to satisfy foreign key constraints
  await DBModel.execute(
    `INSERT INTO posts (id, user_id, title, content) VALUES 
     (10001, 1, 'Test Post 1', 'Content 1'),
     (10002, 1, 'Test Post 2', 'Content 2')
     ON CONFLICT (id) DO NOTHING`
  );
}

// ============================================
// Tests
// ============================================

describe.skipIf(skipIntegrationTests)('PkeyResult - create/createMany', () => {
  beforeAll(() => {
    DBModel.setConfig(testConfig);
  });

  beforeEach(async () => {
    await cleanupTestUsers();
  });

  afterAll(async () => {
    await cleanupTestUsers();
    await closeAllPools();
  });

  it('create without returning should return null', async () => {
    await DBModel.transaction(async () => {
      const result = await User.create([
        [User.name, 'PkeyTest1'],
        [User.email, 'pkeytest-1@example.com'],
        [User.is_active, true],
        [User.role, 'user'],
      ]);

      expect(result).toBeNull();
    });
  });

  it('create with returning: true should return PkeyResult', async () => {
    await DBModel.transaction(async () => {
      const result = await User.create([
        [User.name, 'PkeyTest2'],
        [User.email, 'pkeytest-2@example.com'],
        [User.is_active, true],
        [User.role, 'user'],
      ], { returning: true });

      expect(result).not.toBeNull();
      expect(result!.key).toHaveLength(1);
      expect(result!.key[0].columnName).toBe('id');
      expect(result!.values).toHaveLength(1);
      expect(result!.values[0]).toHaveLength(1);
      expect(typeof result!.values[0][0]).toBe('number');
    });
  });

  it('createMany without returning should return null', async () => {
    await DBModel.transaction(async () => {
      const result = await User.createMany([
        [[User.name, 'PkeyTest3a'], [User.email, 'pkeytest-3a@example.com'], [User.is_active, true], [User.role, 'user']],
        [[User.name, 'PkeyTest3b'], [User.email, 'pkeytest-3b@example.com'], [User.is_active, true], [User.role, 'user']],
      ]);

      expect(result).toBeNull();
    });
  });

  it('createMany with returning: true should return PkeyResult with multiple values', async () => {
    await DBModel.transaction(async () => {
      const result = await User.createMany([
        [[User.name, 'PkeyTest4a'], [User.email, 'pkeytest-4a@example.com'], [User.is_active, true], [User.role, 'user']],
        [[User.name, 'PkeyTest4b'], [User.email, 'pkeytest-4b@example.com'], [User.is_active, true], [User.role, 'user']],
        [[User.name, 'PkeyTest4c'], [User.email, 'pkeytest-4c@example.com'], [User.is_active, true], [User.role, 'user']],
      ], { returning: true });

      expect(result).not.toBeNull();
      expect(result!.key).toHaveLength(1);
      expect(result!.values).toHaveLength(3);
      // Each value array should have 1 element (single PK)
      result!.values.forEach(v => {
        expect(v).toHaveLength(1);
        expect(typeof v[0]).toBe('number');
      });
    });
  });
});

describe.skipIf(skipIntegrationTests)('PkeyResult - update', () => {
  beforeAll(() => {
    DBModel.setConfig(testConfig);
  });

  beforeEach(async () => {
    await cleanupTestUsers();
    // Create test users
    await DBModel.execute(
      `INSERT INTO users (name, email, is_active, role) VALUES 
       ('Update Test 1', 'pkeytest-update-1@example.com', true, 'user'),
       ('Update Test 2', 'pkeytest-update-2@example.com', true, 'user'),
       ('Update Test 3', 'pkeytest-update-3@example.com', false, 'admin')`
    );
  });

  afterAll(async () => {
    await cleanupTestUsers();
    await closeAllPools();
  });

  it('update without returning should return null', async () => {
    await DBModel.transaction(async () => {
      const result = await User.update(
        [[`${User.email} LIKE ?`, 'pkeytest-update-%']],
        [[User.role, 'moderator']],
      );

      expect(result).toBeNull();
    });
  });

  it('update with returning: true should return PkeyResult', async () => {
    await DBModel.transaction(async () => {
      const result = await User.update(
        [[User.is_active, true], [`${User.email} LIKE ?`, 'pkeytest-update-%']],
        [[User.role, 'moderator']],
        { returning: true }
      );

      expect(result).not.toBeNull();
      expect(result!.key).toHaveLength(1);
      expect(result!.key[0].columnName).toBe('id');
      // Should have 2 rows (only is_active = true rows)
      expect(result!.values).toHaveLength(2);
    });
  });
});

describe.skipIf(skipIntegrationTests)('PkeyResult - updateMany', () => {
  beforeAll(() => {
    DBModel.setConfig(testConfig);
  });

  beforeEach(async () => {
    await cleanupTestUsers();
    // Create test users with specific IDs
    await DBModel.execute(
      `INSERT INTO users (name, email, is_active, role) VALUES 
       ('UpdateMany 1', 'pkeytest-um-1@example.com', true, 'user'),
       ('UpdateMany 2', 'pkeytest-um-2@example.com', true, 'user'),
       ('UpdateMany 3', 'pkeytest-um-3@example.com', true, 'user')`
    );
  });

  afterAll(async () => {
    await cleanupTestUsers();
    await closeAllPools();
  });

  it('updateMany should update multiple rows with different values', async () => {
    // Get the created user IDs
    const res = await DBModel.execute(
      `SELECT id FROM users WHERE email LIKE 'pkeytest-um-%' ORDER BY id`
    );
    const ids = res.rows.map(r => r.id as number);

    await DBModel.transaction(async () => {
      await User.updateMany([
        [[User.id, ids[0]], [User.name, 'Updated Name 1'], [User.role, 'admin']],
        [[User.id, ids[1]], [User.name, 'Updated Name 2'], [User.role, 'moderator']],
        [[User.id, ids[2]], [User.name, 'Updated Name 3'], [User.role, 'viewer']],
      ], { keyColumns: [User.id] });
    });

    // Verify updates
    const users = await User.find([[`${User.email} LIKE ?`, 'pkeytest-um-%']], { order: 'id ASC' });
    expect(users[0].name).toBe('Updated Name 1');
    expect(users[0].role).toBe('admin');
    expect(users[1].name).toBe('Updated Name 2');
    expect(users[1].role).toBe('moderator');
    expect(users[2].name).toBe('Updated Name 3');
    expect(users[2].role).toBe('viewer');
  });

  it('updateMany with returning: true should return PkeyResult', async () => {
    const res = await DBModel.execute(
      `SELECT id FROM users WHERE email LIKE 'pkeytest-um-%' ORDER BY id`
    );
    const ids = res.rows.map(r => r.id as number);

    await DBModel.transaction(async () => {
      const result = await User.updateMany([
        [[User.id, ids[0]], [User.name, 'Returned 1']],
        [[User.id, ids[1]], [User.name, 'Returned 2']],
      ], { keyColumns: [User.id], returning: true });

      expect(result).not.toBeNull();
      expect(result!.key).toHaveLength(1);
      expect(result!.key[0].columnName).toBe('id');
      expect(result!.values).toHaveLength(2);
    });
  });

  it('updateMany with empty array should return empty PkeyResult', async () => {
    await DBModel.transaction(async () => {
      const result = await User.updateMany([], { keyColumns: [User.id], returning: true });

      expect(result).not.toBeNull();
      expect(result!.values).toHaveLength(0);
    });
  });
});

describe.skipIf(skipIntegrationTests)('PkeyResult - delete', () => {
  beforeAll(() => {
    DBModel.setConfig(testConfig);
  });

  beforeEach(async () => {
    await cleanupTestUsers();
    // Create test users
    await DBModel.execute(
      `INSERT INTO users (name, email, is_active, role) VALUES 
       ('Delete Test 1', 'pkeytest-del-1@example.com', false, 'user'),
       ('Delete Test 2', 'pkeytest-del-2@example.com', false, 'user'),
       ('Delete Test 3', 'pkeytest-del-3@example.com', true, 'user')`
    );
  });

  afterAll(async () => {
    await cleanupTestUsers();
    await closeAllPools();
  });

  it('delete without returning should return null', async () => {
    await DBModel.transaction(async () => {
      const result = await User.delete([[User.is_active, false], [`${User.email} LIKE ?`, 'pkeytest-del-%']]);
      expect(result).toBeNull();
    });

    // Verify deletion
    const remaining = await User.find([[`${User.email} LIKE ?`, 'pkeytest-del-%']]);
    expect(remaining).toHaveLength(1);
    expect(remaining[0].is_active).toBe(true);
  });

  it('delete with returning: true should return PkeyResult', async () => {
    await DBModel.transaction(async () => {
      const result = await User.delete(
        [[User.is_active, false], [`${User.email} LIKE ?`, 'pkeytest-del-%']],
        { returning: true }
      );

      expect(result).not.toBeNull();
      expect(result!.key).toHaveLength(1);
      expect(result!.key[0].columnName).toBe('id');
      expect(result!.values).toHaveLength(2);
    });
  });
});

describe.skipIf(skipIntegrationTests)('PkeyResult - findById', () => {
  beforeAll(() => {
    DBModel.setConfig(testConfig);
  });

  beforeEach(async () => {
    await cleanupTestUsers();
    // Create test users
    await DBModel.execute(
      `INSERT INTO users (name, email, is_active, role) VALUES 
       ('FindById 1', 'pkeytest-find-1@example.com', true, 'user'),
       ('FindById 2', 'pkeytest-find-2@example.com', true, 'admin'),
       ('FindById 3', 'pkeytest-find-3@example.com', true, 'moderator')`
    );
  });

  afterAll(async () => {
    await cleanupTestUsers();
    await closeAllPools();
  });

  it('findById with empty values should return empty array', async () => {
    const users = await User.findById({ values: [] });
    expect(users).toHaveLength(0);
  });

  it('findById with single value should return array with one record', async () => {
    const res = await DBModel.execute(
      `SELECT id FROM users WHERE email = 'pkeytest-find-1@example.com'`
    );
    const id = res.rows[0].id as number;

    const users = await User.findById({ values: [[id]] });
    expect(users).toHaveLength(1);
    expect(users[0].name).toBe('FindById 1');
  });

  it('findById with multiple values should return multiple records', async () => {
    const res = await DBModel.execute(
      `SELECT id FROM users WHERE email LIKE 'pkeytest-find-%' ORDER BY id`
    );
    const ids = res.rows.map(r => r.id as number);

    const users = await User.findById({ values: [[ids[0]], [ids[1]], [ids[2]]] });
    expect(users).toHaveLength(3);
  });

  it('findById should work with PkeyResult from create', async () => {
    await DBModel.transaction(async () => {
      const createResult = await User.create([
        [User.name, 'Created for FindById'],
        [User.email, 'pkeytest-created@example.com'],
        [User.is_active, true],
        [User.role, 'user'],
      ], { returning: true });

      const users = await User.findById(createResult!);
      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('Created for FindById');
    });
  });

  it('findById should work with non-existent IDs', async () => {
    const users = await User.findById({ values: [[999999], [999998]] });
    expect(users).toHaveLength(0);
  });
});

describe.skipIf(skipIntegrationTests)('PkeyResult - Composite Primary Key', () => {
  beforeAll(() => {
    DBModel.setConfig(testConfig);
  });

  beforeEach(async () => {
    await cleanupTestTags();
    // Create parent posts first to satisfy foreign key constraints
    await setupTestPostsForTags();
    // Create test post_tags with high IDs to avoid conflicts
    await DBModel.execute(
      `INSERT INTO post_tags (post_id, tag_id) VALUES 
       (10001, 1), (10001, 2), (10002, 1), (10002, 3)`
    );
  });

  afterAll(async () => {
    await cleanupTestTags();
    await closeAllPools();
  });

  it('findById with composite PK should return correct records', async () => {
    const tags = await PostTag.findById({
      values: [[10001, 1], [10002, 3]]
    });

    expect(tags).toHaveLength(2);
    const found = tags.map(t => `${t.post_id}-${t.tag_id}`);
    expect(found).toContain('10001-1');
    expect(found).toContain('10002-3');
  });

  it('delete with returning: true on composite PK should return composite values', async () => {
    await DBModel.transaction(async () => {
      const result = await PostTag.delete(
        [[PostTag.post_id, 10001]],
        { returning: true }
      );

      expect(result).not.toBeNull();
      expect(result!.key).toHaveLength(2);
      expect(result!.key.map(k => k.columnName)).toContain('post_id');
      expect(result!.key.map(k => k.columnName)).toContain('tag_id');
      expect(result!.values).toHaveLength(2); // Two rows deleted
      // Each value should have 2 elements (composite PK)
      result!.values.forEach(v => {
        expect(v).toHaveLength(2);
      });
    });
  });
});

describe.skipIf(skipIntegrationTests)('PkeyResult - Round-trip workflow', () => {
  beforeAll(() => {
    DBModel.setConfig(testConfig);
  });

  beforeEach(async () => {
    await cleanupTestUsers();
  });

  afterAll(async () => {
    await cleanupTestUsers();
    await closeAllPools();
  });

  it('create → findById → update → findById workflow', async () => {
    await DBModel.transaction(async () => {
      // Create
      const createResult = await User.createMany([
        [[User.name, 'Workflow User 1'], [User.email, 'pkeytest-wf-1@example.com'], [User.is_active, true], [User.role, 'user']],
        [[User.name, 'Workflow User 2'], [User.email, 'pkeytest-wf-2@example.com'], [User.is_active, true], [User.role, 'user']],
      ], { returning: true });

      expect(createResult).not.toBeNull();
      expect(createResult!.values).toHaveLength(2);

      // FindById with create result
      const createdUsers = await User.findById(createResult!);
      expect(createdUsers).toHaveLength(2);

      // Update
      const updateResult = await User.update(
        [[`${User.email} LIKE ?`, 'pkeytest-wf-%']],
        [[User.role, 'admin']],
        { returning: true }
      );

      expect(updateResult).not.toBeNull();
      expect(updateResult!.values).toHaveLength(2);

      // FindById with update result
      const updatedUsers = await User.findById(updateResult!);
      expect(updatedUsers).toHaveLength(2);
      updatedUsers.forEach(u => {
        expect(u.role).toBe('admin');
      });
    });
  });
});

