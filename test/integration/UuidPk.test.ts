/**
 * UUID Primary Key and Foreign Key Integration Tests
 *
 * Tests all CRUD operations and relations with UUID columns.
 * Requires a running PostgreSQL instance.
 * Run `docker compose up -d` before running tests.
 *
 * Skip these tests when PostgreSQL is not available by setting SKIP_INTEGRATION_TESTS=1
 */

import 'reflect-metadata';
import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import {
  createRelationContext,
  preloadRelations,
  DBModel,
  model,
  column,
  hasMany,
  belongsTo,
  hasOne,
  ColumnsOf,
  closeAllPools,
  Middleware,
} from '../../src';
import type { DBConfig, ExecuteResult } from '../../src';

// ============================================
// SQL Logger Middleware (for verifying SQL cast)
// ============================================

interface QueryLogEntry {
  sql: string;
  params?: unknown[];
  timestamp: number;
}

class SqlLoggerMiddleware extends Middleware {
  private queries: QueryLogEntry[] = [];

  init(): void {
    this.queries = [];
  }

  async execute(
    next: (sql: string, params?: unknown[]) => Promise<ExecuteResult>,
    sql: string,
    params?: unknown[]
  ): Promise<ExecuteResult> {
    this.queries.push({
      sql,
      params,
      timestamp: Date.now(),
    });
    return next(sql, params);
  }

  getQueries(): QueryLogEntry[] {
    return [...this.queries];
  }

  get queryCount(): number {
    return this.queries.length;
  }

  getSelectQueries(): QueryLogEntry[] {
    return this.queries.filter(q => q.sql.trim().toUpperCase().startsWith('SELECT'));
  }

  getInsertQueries(): QueryLogEntry[] {
    return this.queries.filter(q => q.sql.trim().toUpperCase().startsWith('INSERT'));
  }

  getUpdateQueries(): QueryLogEntry[] {
    return this.queries.filter(q => q.sql.trim().toUpperCase().startsWith('UPDATE'));
  }

  getDeleteQueries(): QueryLogEntry[] {
    return this.queries.filter(q => q.sql.trim().toUpperCase().startsWith('DELETE'));
  }

  clear(): void {
    this.queries = [];
  }
}

// Skip integration tests if SKIP_INTEGRATION_TESTS=1 is set
const skipIntegrationTests = process.env.SKIP_INTEGRATION_TESTS === '1';

// ============================================
// Test Configuration
// ============================================

const testConfig: DBConfig = {
  host: process.env.TEST_DB_HOST || 'localhost',
  port: parseInt(process.env.TEST_DB_PORT || '5433'),
  database: process.env.TEST_DB_NAME || 'testdb',
  user: process.env.TEST_DB_USER || 'testuser',
  password: process.env.TEST_DB_PASSWORD || 'testpass',
};

// ============================================
// Test UUIDs
// ============================================

const UUID1 = '11111111-1111-1111-1111-111111111111';
const UUID2 = '22222222-2222-2222-2222-222222222222';
const UUID3 = '33333333-3333-3333-3333-333333333333';
const UUID4 = '44444444-4444-4444-4444-444444444444';
const UUID5 = '55555555-5555-5555-5555-555555555555';

// Composite UUIDs
const ORG_UUID1 = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
const ORG_UUID2 = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb';
const USER_UUID1 = 'cccccccc-cccc-cccc-cccc-cccccccccccc';
const USER_UUID2 = 'dddddddd-dddd-dddd-dddd-dddddddddddd';
const USER_UUID3 = 'eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee';

// ============================================
// Test Models - Single UUID PK
// ============================================

// eslint-disable-next-line prefer-const
let UuidUser: typeof UuidUserModel & ColumnsOf<UuidUserModel>;
// eslint-disable-next-line prefer-const
let UuidPost: typeof UuidPostModel & ColumnsOf<UuidPostModel>;
// eslint-disable-next-line prefer-const
let UuidProfile: typeof UuidProfileModel & ColumnsOf<UuidProfileModel>;

@model('test_uuid_users')
class UuidUserModel extends DBModel {
  @column.uuid({ primaryKey: true }) id?: string;
  @column() name?: string;
  @column() email?: string;
  @column() created_at?: Date;

  // hasMany with UUID FK
  @hasMany(() => [UuidUser.id, UuidPost.author_id], {
    order: () => UuidPost.created_at.desc(),
  })
  declare posts: Promise<UuidPostModel[]>;

  // hasOne with UUID FK
  @hasOne(() => [UuidUser.id, UuidProfile.user_id])
  declare profile: Promise<UuidProfileModel | null>;
}
UuidUser = UuidUserModel.asModel();
type UuidUser = UuidUserModel;

@model('test_uuid_posts')
class UuidPostModel extends DBModel {
  @column.uuid({ primaryKey: true }) id?: string;
  @column.uuid() author_id?: string;
  @column() title?: string;
  @column() content?: string;
  @column() published?: boolean;
  @column() created_at?: Date;

  // belongsTo with UUID FK
  @belongsTo(() => [UuidPost.author_id, UuidUser.id])
  declare author: Promise<UuidUserModel | null>;
}
UuidPost = UuidPostModel.asModel();
type UuidPost = UuidPostModel;

@model('test_uuid_profiles')
class UuidProfileModel extends DBModel {
  @column.uuid({ primaryKey: true }) id?: string;
  @column.uuid() user_id?: string;
  @column() bio?: string;
  @column() website?: string;

  // belongsTo with UUID FK
  @belongsTo(() => [UuidProfile.user_id, UuidUser.id])
  declare user: Promise<UuidUserModel | null>;
}
UuidProfile = UuidProfileModel.asModel();
type UuidProfile = UuidProfileModel;

// ============================================
// Test Models - Composite UUID PK
// ============================================

// eslint-disable-next-line prefer-const
let OrgUser: typeof OrgUserModel & ColumnsOf<OrgUserModel>;
// eslint-disable-next-line prefer-const
let OrgPost: typeof OrgPostModel & ColumnsOf<OrgPostModel>;

@model('test_org_users')
class OrgUserModel extends DBModel {
  @column.uuid({ primaryKey: true }) org_id?: string;
  @column.uuid({ primaryKey: true }) user_id?: string;
  @column() name?: string;
  @column() role?: string;
  @column() created_at?: Date;

  // hasMany with composite UUID FK
  @hasMany(() => [
    [OrgUser.org_id, OrgPost.org_id],
    [OrgUser.user_id, OrgPost.author_id],
  ], {
    order: () => OrgPost.created_at.desc(),
  })
  declare posts: Promise<OrgPostModel[]>;
}
OrgUser = OrgUserModel.asModel();
type OrgUser = OrgUserModel;

@model('test_org_posts')
class OrgPostModel extends DBModel {
  @column.uuid({ primaryKey: true }) org_id?: string;
  @column({ primaryKey: true }) post_id?: number;
  @column.uuid() author_id?: string;
  @column() title?: string;
  @column() content?: string;
  @column() created_at?: Date;

  // belongsTo with composite UUID FK
  @belongsTo(() => [
    [OrgPost.org_id, OrgUser.org_id],
    [OrgPost.author_id, OrgUser.user_id],
  ])
  declare author: Promise<OrgUserModel | null>;
}
OrgPost = OrgPostModel.asModel();
type OrgPost = OrgPostModel;

// ============================================
// Test Setup
// ============================================

describe.skipIf(skipIntegrationTests)('UUID Primary Key Integration', () => {
  beforeAll(async () => {
    DBModel.setConfig(testConfig);

    // Create test tables with UUID columns
    await DBModel.execute(`
      DROP TABLE IF EXISTS test_org_posts CASCADE;
      DROP TABLE IF EXISTS test_org_users CASCADE;
      DROP TABLE IF EXISTS test_uuid_profiles CASCADE;
      DROP TABLE IF EXISTS test_uuid_posts CASCADE;
      DROP TABLE IF EXISTS test_uuid_users CASCADE;

      -- Single UUID PK tables
      CREATE TABLE test_uuid_users (
        id UUID PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      );

      CREATE TABLE test_uuid_posts (
        id UUID PRIMARY KEY,
        author_id UUID REFERENCES test_uuid_users(id),
        title VARCHAR(255) NOT NULL,
        content TEXT,
        published BOOLEAN DEFAULT true,
        created_at TIMESTAMP DEFAULT NOW()
      );

      CREATE TABLE test_uuid_profiles (
        id UUID PRIMARY KEY,
        user_id UUID UNIQUE REFERENCES test_uuid_users(id),
        bio TEXT,
        website VARCHAR(255)
      );

      -- Composite UUID PK tables
      CREATE TABLE test_org_users (
        org_id UUID NOT NULL,
        user_id UUID NOT NULL,
        name VARCHAR(255) NOT NULL,
        role VARCHAR(50) DEFAULT 'member',
        created_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (org_id, user_id)
      );

      CREATE TABLE test_org_posts (
        org_id UUID NOT NULL,
        post_id SERIAL NOT NULL,
        author_id UUID NOT NULL,
        title VARCHAR(255) NOT NULL,
        content TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (org_id, post_id),
        FOREIGN KEY (org_id, author_id) REFERENCES test_org_users(org_id, user_id)
      );
    `);
  });

  afterAll(async () => {
    await DBModel.execute(`
      DROP TABLE IF EXISTS test_org_posts CASCADE;
      DROP TABLE IF EXISTS test_org_users CASCADE;
      DROP TABLE IF EXISTS test_uuid_profiles CASCADE;
      DROP TABLE IF EXISTS test_uuid_posts CASCADE;
      DROP TABLE IF EXISTS test_uuid_users CASCADE;
    `);
    await closeAllPools();
  });

  beforeEach(async () => {
    await DBModel.execute('DELETE FROM test_org_posts');
    await DBModel.execute('DELETE FROM test_org_users');
    await DBModel.execute('DELETE FROM test_uuid_profiles');
    await DBModel.execute('DELETE FROM test_uuid_posts');
    await DBModel.execute('DELETE FROM test_uuid_users');
    // Reset sequence for test_org_posts
    await DBModel.execute('ALTER SEQUENCE test_org_posts_post_id_seq RESTART WITH 1');
  });

  // ============================================
  // Model Setup Verification
  // ============================================

  describe('Model Setup Verification', () => {
    it('should have sqlCast=uuid on UuidUser.id column', () => {
      expect(UuidUser.id).toBeDefined();
      expect(typeof UuidUser.id).toBe('function');
      expect((UuidUser.id as unknown as { sqlCast: string }).sqlCast).toBe('uuid');
    });

    it('should have sqlCast=uuid on UuidPost.author_id column', () => {
      expect(UuidPost.author_id).toBeDefined();
      expect((UuidPost.author_id as unknown as { sqlCast: string }).sqlCast).toBe('uuid');
    });

    it('should have sqlCast=uuid on OrgUser composite UUID columns', () => {
      expect((OrgUser.org_id as unknown as { sqlCast: string }).sqlCast).toBe('uuid');
      expect((OrgUser.user_id as unknown as { sqlCast: string }).sqlCast).toBe('uuid');
    });

    it('should generate DBCast when using eq() on UUID column', () => {
      const condition = UuidUser.id.eq(UUID1);
      expect(condition).toBeDefined();
      expect(condition.id).toBeDefined();
      // Should be a DBCast instance
      expect(condition.id.constructor.name).toBe('DBCast');
    });

    it('should generate DBCastArray when using in() on UUID column', () => {
      const condition = UuidUser.id.in([UUID1, UUID2]);
      expect(condition).toBeDefined();
      expect(condition.id).toBeDefined();
      // Should be a DBCastArray instance
      expect(condition.id.constructor.name).toBe('DBCastArray');
    });
  });

  // ============================================
  // Single UUID PK - CRUD Operations
  // ============================================

  describe('Single UUID PK - CRUD Operations', () => {
    describe('create()', () => {
      it('should create a record with UUID primary key', async () => {
        await DBModel.transaction(async () => {
          const pkey = await UuidUser.create([
            [UuidUser.id, UUID1],
            [UuidUser.name, 'John'],
            [UuidUser.email, 'john@example.com'],
          ], { returning: true });

          expect(pkey).toBeDefined();
          // PkeyResult has { key: Column[], values: unknown[][] }
          expect(pkey?.values[0][0]).toBe(UUID1);
        });

        const [user] = await UuidUser.find([[UuidUser.id, UUID1]]);
        expect(user).toBeDefined();
        expect(user?.name).toBe('John');
      });

      it('should create a record with UUID foreign key', async () => {
        await DBModel.transaction(async () => {
          await UuidUser.create([
            [UuidUser.id, UUID1],
            [UuidUser.name, 'Author'],
            [UuidUser.email, 'author@example.com'],
          ]);

          const pkey = await UuidPost.create([
            [UuidPost.id, UUID2],
            [UuidPost.author_id, UUID1],
            [UuidPost.title, 'Test Post'],
            [UuidPost.content, 'Content here'],
          ], { returning: true });

          expect(pkey?.values[0][0]).toBe(UUID2);
        });

        const [post] = await UuidPost.find([[UuidPost.id, UUID2]]);
        expect(post?.author_id).toBe(UUID1);
      });
    });

    describe('createMany() / UPSERT', () => {
      it('should create multiple records with UUID PKs', async () => {
        await DBModel.transaction(async () => {
          const pkey = await UuidUser.createMany([
            [
              [UuidUser.id, UUID1],
              [UuidUser.name, 'User1'],
              [UuidUser.email, 'user1@example.com'],
            ],
            [
              [UuidUser.id, UUID2],
              [UuidUser.name, 'User2'],
              [UuidUser.email, 'user2@example.com'],
            ],
            [
              [UuidUser.id, UUID3],
              [UuidUser.name, 'User3'],
              [UuidUser.email, 'user3@example.com'],
            ],
          ], { returning: true });

          expect(pkey).toBeDefined();
          // PkeyResult has { key: Column[], values: unknown[][] }
          expect(pkey?.values.length).toBe(3);
          expect(pkey?.values[0][0]).toBe(UUID1);
          expect(pkey?.values[1][0]).toBe(UUID2);
          expect(pkey?.values[2][0]).toBe(UUID3);
        });

        const users = await UuidUser.find([]);
        expect(users.length).toBe(3);
      });

      it('should upsert with UUID PK (onConflict)', async () => {
        // First insert
        await DBModel.transaction(async () => {
          await UuidUser.create([
            [UuidUser.id, UUID1],
            [UuidUser.name, 'Original'],
            [UuidUser.email, 'original@example.com'],
          ]);
        });

        // Upsert - update on conflict
        // InsertOptions: onConflict is the conflict column(s), onConflictUpdate is what to update
        await DBModel.transaction(async () => {
          await UuidUser.createMany([
            [
              [UuidUser.id, UUID1],
              [UuidUser.name, 'Updated'],
              [UuidUser.email, 'updated@example.com'],
            ],
            [
              [UuidUser.id, UUID2],
              [UuidUser.name, 'New'],
              [UuidUser.email, 'new@example.com'],
            ],
          ], {
            onConflict: UuidUser.id,
            onConflictUpdate: [UuidUser.name, UuidUser.email],
          });
        });

        const [user1] = await UuidUser.find([[UuidUser.id, UUID1]]);
        expect(user1?.name).toBe('Updated');

        const [user2] = await UuidUser.find([[UuidUser.id, UUID2]]);
        expect(user2?.name).toBe('New');
      });
    });

    describe('find() / findOne()', () => {
      beforeEach(async () => {
        await DBModel.transaction(async () => {
          await UuidUser.createMany([
            [[UuidUser.id, UUID1], [UuidUser.name, 'Alice'], [UuidUser.email, 'alice@example.com']],
            [[UuidUser.id, UUID2], [UuidUser.name, 'Bob'], [UuidUser.email, 'bob@example.com']],
            [[UuidUser.id, UUID3], [UuidUser.name, 'Carol'], [UuidUser.email, 'carol@example.com']],
          ]);
        });
      });

      it('should find by UUID eq condition', async () => {
        const users = await UuidUser.find([[UuidUser.id, UUID1]]);
        expect(users.length).toBe(1);
        expect(users[0].name).toBe('Alice');
      });

      it('should find by UUID IN condition (array)', async () => {
        const users = await UuidUser.find([[UuidUser.id, [UUID1, UUID2]]]);
        expect(users.length).toBe(2);
        expect(users.map(u => u.name).sort()).toEqual(['Alice', 'Bob']);
      });

      it('should find by UUID IN condition using dbIn helper', async () => {
        const { dbIn } = await import('../../src');
        const users = await UuidUser.find([[UuidUser.id, dbIn([UUID2, UUID3])]]);
        expect(users.length).toBe(2);
        expect(users.map(u => u.name).sort()).toEqual(['Bob', 'Carol']);
      });

      it('should findOne by UUID', async () => {
        const user = await UuidUser.findOne([[UuidUser.id, UUID2]]);
        expect(user).toBeDefined();
        expect(user?.name).toBe('Bob');
      });

      it('should find with multiple conditions including UUID', async () => {
        const users = await UuidUser.find([
          [UuidUser.id, [UUID1, UUID2, UUID3]],
          [UuidUser.name, 'Bob'],
        ]);
        expect(users.length).toBe(1);
        expect(users[0].id).toBe(UUID2);
      });
    });

    describe('findById()', () => {
      beforeEach(async () => {
        await DBModel.transaction(async () => {
          await UuidUser.createMany([
            [[UuidUser.id, UUID1], [UuidUser.name, 'User1'], [UuidUser.email, 'user1@example.com']],
            [[UuidUser.id, UUID2], [UuidUser.name, 'User2'], [UuidUser.email, 'user2@example.com']],
            [[UuidUser.id, UUID3], [UuidUser.name, 'User3'], [UuidUser.email, 'user3@example.com']],
          ]);
        });
      });

      it('should findById with single UUID', async () => {
        // findById expects { values: unknown[][] } format
        const users = await UuidUser.findById({ values: [[UUID1]] });
        expect(users.length).toBe(1);
        expect(users[0].name).toBe('User1');
      });

      it('should findById with multiple UUIDs (batch)', async () => {
        DBModel.use(SqlLoggerMiddleware);
        const logger = SqlLoggerMiddleware.getCurrentContext();
        logger.clear();

        // Multiple PKs: { values: [[pk1], [pk2], [pk3]] }
        const users = await UuidUser.findById({
          values: [[UUID1], [UUID2], [UUID3]],
        });

        expect(users.length).toBe(3);
        expect(users.map(u => u.name).sort()).toEqual(['User1', 'User2', 'User3']);

        // Verify SQL uses uuid[] cast
        const selectQueries = logger.getSelectQueries();
        expect(selectQueries.length).toBe(1);
        expect(selectQueries[0].sql).toMatch(/::uuid\[\]/);

        SqlLoggerMiddleware.clearContext();
        DBModel.clearMiddlewares();
      });
    });

    describe('update()', () => {
      beforeEach(async () => {
        await DBModel.transaction(async () => {
          await UuidUser.create([
            [UuidUser.id, UUID1],
            [UuidUser.name, 'Original'],
            [UuidUser.email, 'original@example.com'],
          ]);
        });
      });

      it('should update by UUID eq condition', async () => {
        await DBModel.transaction(async () => {
          await UuidUser.update(
            [[UuidUser.id, UUID1]],
            [[UuidUser.name, 'Updated']],
          );
        });

        const [user] = await UuidUser.find([[UuidUser.id, UUID1]]);
        expect(user?.name).toBe('Updated');
      });

      it('should update by UUID IN condition', async () => {
        await DBModel.transaction(async () => {
          await UuidUser.create([
            [UuidUser.id, UUID2],
            [UuidUser.name, 'User2'],
            [UuidUser.email, 'user2@example.com'],
          ]);
        });

        await DBModel.transaction(async () => {
          await UuidUser.update(
            [[UuidUser.id, [UUID1, UUID2]]],
            [[UuidUser.name, 'Bulk Updated']],
          );
        });

        const users = await UuidUser.find([[UuidUser.id, [UUID1, UUID2]]]);
        expect(users.every(u => u.name === 'Bulk Updated')).toBe(true);
      });
    });

    describe('updateMany()', () => {
      beforeEach(async () => {
        await DBModel.transaction(async () => {
          await UuidUser.createMany([
            [[UuidUser.id, UUID1], [UuidUser.name, 'User1'], [UuidUser.email, 'user1@example.com']],
            [[UuidUser.id, UUID2], [UuidUser.name, 'User2'], [UuidUser.email, 'user2@example.com']],
            [[UuidUser.id, UUID3], [UuidUser.name, 'User3'], [UuidUser.email, 'user3@example.com']],
          ]);
        });
      });

      it('should updateMany with UUID PKs', async () => {
        DBModel.use(SqlLoggerMiddleware);
        const logger = SqlLoggerMiddleware.getCurrentContext();

        await DBModel.transaction(async () => {
          logger.clear();

          await UuidUser.updateMany([
            [[UuidUser.id, UUID1], [UuidUser.name, 'Updated1']],
            [[UuidUser.id, UUID2], [UuidUser.name, 'Updated2']],
            [[UuidUser.id, UUID3], [UuidUser.name, 'Updated3']],
          ], {
            keyColumns: UuidUser.id,  // Required: specify the key column(s)
          });

          // Verify SQL uses uuid[] cast
          const updateQueries = logger.getUpdateQueries();
          expect(updateQueries.length).toBe(1);
          expect(updateQueries[0].sql).toMatch(/::uuid\[\]/);
        });

        const users = await UuidUser.find([], { order: 'name' });
        expect(users[0].name).toBe('Updated1');
        expect(users[1].name).toBe('Updated2');
        expect(users[2].name).toBe('Updated3');

        SqlLoggerMiddleware.clearContext();
        DBModel.clearMiddlewares();
      });
    });

    describe('delete()', () => {
      beforeEach(async () => {
        await DBModel.transaction(async () => {
          await UuidUser.createMany([
            [[UuidUser.id, UUID1], [UuidUser.name, 'User1'], [UuidUser.email, 'user1@example.com']],
            [[UuidUser.id, UUID2], [UuidUser.name, 'User2'], [UuidUser.email, 'user2@example.com']],
          ]);
        });
      });

      it('should delete by UUID eq condition', async () => {
        await DBModel.transaction(async () => {
          await UuidUser.delete([[UuidUser.id, UUID1]]);
        });

        const users = await UuidUser.find([]);
        expect(users.length).toBe(1);
        expect(users[0].id).toBe(UUID2);
      });

      it('should delete by UUID IN condition', async () => {
        await DBModel.transaction(async () => {
          await UuidUser.delete([[UuidUser.id, [UUID1, UUID2]]]);
        });

        const users = await UuidUser.find([]);
        expect(users.length).toBe(0);
      });
    });
  });

  // ============================================
  // Single UUID PK - Relations
  // ============================================

  describe('Single UUID PK - Relations', () => {
    beforeEach(async () => {
      await DBModel.transaction(async () => {
        // Create users
        await UuidUser.createMany([
          [[UuidUser.id, UUID1], [UuidUser.name, 'Author1'], [UuidUser.email, 'author1@example.com']],
          [[UuidUser.id, UUID2], [UuidUser.name, 'Author2'], [UuidUser.email, 'author2@example.com']],
        ]);

        // Create posts for Author1
        await UuidPost.createMany([
          [[UuidPost.id, UUID3], [UuidPost.author_id, UUID1], [UuidPost.title, 'Post1'], [UuidPost.content, 'Content1']],
          [[UuidPost.id, UUID4], [UuidPost.author_id, UUID1], [UuidPost.title, 'Post2'], [UuidPost.content, 'Content2']],
        ]);

        // Create post for Author2
        await UuidPost.create([
          [UuidPost.id, UUID5],
          [UuidPost.author_id, UUID2],
          [UuidPost.title, 'Post3'],
          [UuidPost.content, 'Content3'],
        ]);

        // Create profile for Author1
        await UuidProfile.create([
          [UuidProfile.id, 'ffffffff-ffff-ffff-ffff-ffffffffffff'],
          [UuidProfile.user_id, UUID1],
          [UuidProfile.bio, 'Author1 Bio'],
          [UuidProfile.website, 'https://author1.com'],
        ]);
      });
    });

    describe('belongsTo with UUID FK', () => {
      it('should load belongsTo relation with UUID FK', async () => {
        const [post] = await UuidPost.find([[UuidPost.id, UUID3]]);
        const author = await post.author;

        expect(author).not.toBeNull();
        expect(author?.id).toBe(UUID1);
        expect(author?.name).toBe('Author1');
      });

      it('should batch load belongsTo with UUID FK', async () => {
        DBModel.use(SqlLoggerMiddleware);
        const logger = SqlLoggerMiddleware.getCurrentContext();
        logger.clear();

        const posts = await UuidPost.find([], { order: 'title' });
        createRelationContext(UuidPostModel, posts as UuidPost[]);

        await preloadRelations(posts as UuidPost[], (post) => post.author);

        const author1 = await posts[0].author;
        const author2 = await posts[1].author;
        const author3 = await posts[2].author;

        expect(author1?.name).toBe('Author1');
        expect(author2?.name).toBe('Author1');
        expect(author3?.name).toBe('Author2');

        // Verify batch loading used uuid[] cast
        const selectQueries = logger.getSelectQueries();
        expect(selectQueries.length).toBe(2); // posts + batch users
        expect(selectQueries[1].sql).toMatch(/::uuid\[\]/);

        SqlLoggerMiddleware.clearContext();
        DBModel.clearMiddlewares();
      });

      it('should return null for non-existent belongsTo relation', async () => {
        // Create orphan post
        await DBModel.execute(
          'ALTER TABLE test_uuid_posts DROP CONSTRAINT IF EXISTS test_uuid_posts_author_id_fkey'
        );
        await DBModel.transaction(async () => {
          await UuidPost.create([
            [UuidPost.id, 'deadbeef-dead-beef-dead-beefdeadbeef'],
            [UuidPost.author_id, '99999999-9999-9999-9999-999999999999'],
            [UuidPost.title, 'Orphan'],
            [UuidPost.content, 'No author'],
          ]);
        });
        await DBModel.execute(
          'ALTER TABLE test_uuid_posts ADD CONSTRAINT test_uuid_posts_author_id_fkey FOREIGN KEY (author_id) REFERENCES test_uuid_users(id) NOT VALID'
        );

        const [orphanPost] = await UuidPost.find([[UuidPost.id, 'deadbeef-dead-beef-dead-beefdeadbeef']]);
        const author = await orphanPost.author;

        expect(author).toBeNull();
      });
    });

    describe('hasMany with UUID PK', () => {
      it('should load hasMany relation with UUID FK', async () => {
        const [user] = await UuidUser.find([[UuidUser.id, UUID1]]);
        const posts = await user.posts;

        expect(posts.length).toBe(2);
        expect(posts.map(p => p.title).sort()).toEqual(['Post1', 'Post2']);
      });

      it('should batch load hasMany with UUID FK', async () => {
        DBModel.use(SqlLoggerMiddleware);
        const logger = SqlLoggerMiddleware.getCurrentContext();
        logger.clear();

        const users = await UuidUser.find([], { order: 'name' });
        createRelationContext(UuidUserModel, users as UuidUser[]);

        await preloadRelations(users as UuidUser[], (user) => user.posts);

        const user1Posts = await users[0].posts;
        const user2Posts = await users[1].posts;

        expect(user1Posts.length).toBe(2);
        expect(user2Posts.length).toBe(1);

        // Verify batch loading used uuid[] cast
        const selectQueries = logger.getSelectQueries();
        expect(selectQueries.length).toBe(2); // users + batch posts
        expect(selectQueries[1].sql).toMatch(/::uuid\[\]/);

        SqlLoggerMiddleware.clearContext();
        DBModel.clearMiddlewares();
      });

      it('should return empty array for hasMany with no related records', async () => {
        // Create user with no posts
        await DBModel.transaction(async () => {
          await UuidUser.create([
            [UuidUser.id, 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'],
            [UuidUser.name, 'NoPosts'],
            [UuidUser.email, 'noposts@example.com'],
          ]);
        });

        const [user] = await UuidUser.find([[UuidUser.id, 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee']]);
        const posts = await user.posts;

        expect(posts).toEqual([]);
      });
    });

    describe('hasOne with UUID FK', () => {
      it('should load hasOne relation with UUID FK', async () => {
        const [user] = await UuidUser.find([[UuidUser.id, UUID1]]);
        const profile = await user.profile;

        expect(profile).not.toBeNull();
        expect(profile?.bio).toBe('Author1 Bio');
        expect(profile?.user_id).toBe(UUID1);
      });

      it('should return null for hasOne with no related record', async () => {
        const [user] = await UuidUser.find([[UuidUser.id, UUID2]]);
        const profile = await user.profile;

        expect(profile).toBeNull();
      });
    });
  });

  // ============================================
  // Composite UUID PK - CRUD Operations
  // ============================================

  describe('Composite UUID PK - CRUD Operations', () => {
    describe('create()', () => {
      it('should create a record with composite UUID PKs', async () => {
        await DBModel.transaction(async () => {
          const pkey = await OrgUser.create([
            [OrgUser.org_id, ORG_UUID1],
            [OrgUser.user_id, USER_UUID1],
            [OrgUser.name, 'OrgUser1'],
            [OrgUser.role, 'admin'],
          ], { returning: true });

          // PkeyResult has { key: Column[], values: unknown[][] }
          // Composite PK: values[0] = [org_id, user_id]
          expect(pkey?.values[0][0]).toBe(ORG_UUID1);
          expect(pkey?.values[0][1]).toBe(USER_UUID1);
        });

        const [user] = await OrgUser.find([
          [OrgUser.org_id, ORG_UUID1],
          [OrgUser.user_id, USER_UUID1],
        ]);
        expect(user?.name).toBe('OrgUser1');
      });
    });

    describe('createMany()', () => {
      it('should create multiple records with composite UUID PKs', async () => {
        await DBModel.transaction(async () => {
          const pkey = await OrgUser.createMany([
            [
              [OrgUser.org_id, ORG_UUID1],
              [OrgUser.user_id, USER_UUID1],
              [OrgUser.name, 'User1'],
              [OrgUser.role, 'admin'],
            ],
            [
              [OrgUser.org_id, ORG_UUID1],
              [OrgUser.user_id, USER_UUID2],
              [OrgUser.name, 'User2'],
              [OrgUser.role, 'member'],
            ],
            [
              [OrgUser.org_id, ORG_UUID2],
              [OrgUser.user_id, USER_UUID1],
              [OrgUser.name, 'User3'],
              [OrgUser.role, 'admin'],
            ],
          ], { returning: true });

          expect(pkey?.values.length).toBe(3);
        });

        const users = await OrgUser.find([]);
        expect(users.length).toBe(3);
      });
    });

    describe('find() with composite UUID PK', () => {
      beforeEach(async () => {
        await DBModel.transaction(async () => {
          await OrgUser.createMany([
            [[OrgUser.org_id, ORG_UUID1], [OrgUser.user_id, USER_UUID1], [OrgUser.name, 'Org1-User1'], [OrgUser.role, 'admin']],
            [[OrgUser.org_id, ORG_UUID1], [OrgUser.user_id, USER_UUID2], [OrgUser.name, 'Org1-User2'], [OrgUser.role, 'member']],
            [[OrgUser.org_id, ORG_UUID2], [OrgUser.user_id, USER_UUID1], [OrgUser.name, 'Org2-User1'], [OrgUser.role, 'admin']],
          ]);
        });
      });

      it('should find by composite UUID eq condition', async () => {
        const users = await OrgUser.find([
          [OrgUser.org_id, ORG_UUID1],
          [OrgUser.user_id, USER_UUID1],
        ]);
        expect(users.length).toBe(1);
        expect(users[0].name).toBe('Org1-User1');
      });

      it('should find by UUID IN condition on one component', async () => {
        const users = await OrgUser.find([
          [OrgUser.org_id, ORG_UUID1],
          [OrgUser.user_id, [USER_UUID1, USER_UUID2]],
        ]);
        expect(users.length).toBe(2);
      });

      it('should find by UUID IN on both components', async () => {
        const users = await OrgUser.find([
          [OrgUser.org_id, [ORG_UUID1, ORG_UUID2]],
          [OrgUser.user_id, USER_UUID1],
        ]);
        expect(users.length).toBe(2);
        expect(users.map(u => u.name).sort()).toEqual(['Org1-User1', 'Org2-User1']);
      });
    });

    describe('findById() with composite UUID PK', () => {
      beforeEach(async () => {
        await DBModel.transaction(async () => {
          await OrgUser.createMany([
            [[OrgUser.org_id, ORG_UUID1], [OrgUser.user_id, USER_UUID1], [OrgUser.name, 'User1'], [OrgUser.role, 'admin']],
            [[OrgUser.org_id, ORG_UUID1], [OrgUser.user_id, USER_UUID2], [OrgUser.name, 'User2'], [OrgUser.role, 'member']],
            [[OrgUser.org_id, ORG_UUID2], [OrgUser.user_id, USER_UUID3], [OrgUser.name, 'User3'], [OrgUser.role, 'admin']],
          ]);
        });
      });

      it('should findById with single composite UUID PK', async () => {
        // findById expects { values: unknown[][] } format for composite PKs
        // values[0] = [org_id, user_id]
        const users = await OrgUser.findById({
          values: [[ORG_UUID1, USER_UUID1]],
        });
        expect(users.length).toBe(1);
        expect(users[0].name).toBe('User1');
      });

      it('should findById with multiple composite UUID PKs (batch)', async () => {
        DBModel.use(SqlLoggerMiddleware);
        const logger = SqlLoggerMiddleware.getCurrentContext();
        logger.clear();

        // Multiple composite PKs: { values: [[pk1_a, pk1_b], [pk2_a, pk2_b], ...] }
        const users = await OrgUser.findById({
          values: [
            [ORG_UUID1, USER_UUID1],
            [ORG_UUID1, USER_UUID2],
            [ORG_UUID2, USER_UUID3],
          ],
        });

        expect(users.length).toBe(3);

        // Verify SQL uses uuid[] cast for composite key batch
        const selectQueries = logger.getSelectQueries();
        expect(selectQueries.length).toBe(1);
        expect(selectQueries[0].sql).toMatch(/::uuid\[\]/);

        SqlLoggerMiddleware.clearContext();
        DBModel.clearMiddlewares();
      });
    });

    describe('updateMany() with composite UUID PK', () => {
      beforeEach(async () => {
        await DBModel.transaction(async () => {
          await OrgUser.createMany([
            [[OrgUser.org_id, ORG_UUID1], [OrgUser.user_id, USER_UUID1], [OrgUser.name, 'User1'], [OrgUser.role, 'admin']],
            [[OrgUser.org_id, ORG_UUID1], [OrgUser.user_id, USER_UUID2], [OrgUser.name, 'User2'], [OrgUser.role, 'member']],
            [[OrgUser.org_id, ORG_UUID2], [OrgUser.user_id, USER_UUID3], [OrgUser.name, 'User3'], [OrgUser.role, 'admin']],
          ]);
        });
      });

      it('should updateMany with composite UUID PKs', async () => {
        DBModel.use(SqlLoggerMiddleware);
        const logger = SqlLoggerMiddleware.getCurrentContext();

        await DBModel.transaction(async () => {
          logger.clear();

          await OrgUser.updateMany([
            [[OrgUser.org_id, ORG_UUID1], [OrgUser.user_id, USER_UUID1], [OrgUser.name, 'Updated1']],
            [[OrgUser.org_id, ORG_UUID1], [OrgUser.user_id, USER_UUID2], [OrgUser.name, 'Updated2']],
            [[OrgUser.org_id, ORG_UUID2], [OrgUser.user_id, USER_UUID3], [OrgUser.name, 'Updated3']],
          ], {
            keyColumns: [OrgUser.org_id, OrgUser.user_id],  // Required: composite key columns
          });

          // Verify SQL uses uuid[] cast
          const updateQueries = logger.getUpdateQueries();
          expect(updateQueries.length).toBe(1);
          expect(updateQueries[0].sql).toMatch(/::uuid\[\]/);
        });

        const users = await OrgUser.find([], { order: 'name' });
        expect(users[0].name).toBe('Updated1');
        expect(users[1].name).toBe('Updated2');
        expect(users[2].name).toBe('Updated3');

        SqlLoggerMiddleware.clearContext();
        DBModel.clearMiddlewares();
      });
    });

    describe('delete() with composite UUID PK', () => {
      beforeEach(async () => {
        await DBModel.transaction(async () => {
          await OrgUser.createMany([
            [[OrgUser.org_id, ORG_UUID1], [OrgUser.user_id, USER_UUID1], [OrgUser.name, 'User1'], [OrgUser.role, 'admin']],
            [[OrgUser.org_id, ORG_UUID1], [OrgUser.user_id, USER_UUID2], [OrgUser.name, 'User2'], [OrgUser.role, 'member']],
          ]);
        });
      });

      it('should delete by composite UUID eq condition', async () => {
        await DBModel.transaction(async () => {
          await OrgUser.delete([
            [OrgUser.org_id, ORG_UUID1],
            [OrgUser.user_id, USER_UUID1],
          ]);
        });

        const users = await OrgUser.find([]);
        expect(users.length).toBe(1);
        expect(users[0].user_id).toBe(USER_UUID2);
      });

      it('should delete by UUID IN condition', async () => {
        await DBModel.transaction(async () => {
          await OrgUser.delete([
            [OrgUser.org_id, ORG_UUID1],
            [OrgUser.user_id, [USER_UUID1, USER_UUID2]],
          ]);
        });

        const users = await OrgUser.find([]);
        expect(users.length).toBe(0);
      });
    });
  });

  // ============================================
  // Composite UUID PK - Relations
  // ============================================

  describe('Composite UUID PK - Relations', () => {
    beforeEach(async () => {
      await DBModel.transaction(async () => {
        // Create org users
        await OrgUser.createMany([
          [[OrgUser.org_id, ORG_UUID1], [OrgUser.user_id, USER_UUID1], [OrgUser.name, 'Org1-Author1'], [OrgUser.role, 'admin']],
          [[OrgUser.org_id, ORG_UUID1], [OrgUser.user_id, USER_UUID2], [OrgUser.name, 'Org1-Author2'], [OrgUser.role, 'member']],
          [[OrgUser.org_id, ORG_UUID2], [OrgUser.user_id, USER_UUID1], [OrgUser.name, 'Org2-Author1'], [OrgUser.role, 'admin']],
        ]);
      });

      // Create org posts (using raw SQL for auto-increment post_id)
      await DBModel.execute(
        `INSERT INTO test_org_posts (org_id, author_id, title, content) VALUES ($1, $2, $3, $4)`,
        [ORG_UUID1, USER_UUID1, 'Org1-Post1', 'Content1']
      );
      await DBModel.execute(
        `INSERT INTO test_org_posts (org_id, author_id, title, content) VALUES ($1, $2, $3, $4)`,
        [ORG_UUID1, USER_UUID1, 'Org1-Post2', 'Content2']
      );
      await DBModel.execute(
        `INSERT INTO test_org_posts (org_id, author_id, title, content) VALUES ($1, $2, $3, $4)`,
        [ORG_UUID1, USER_UUID2, 'Org1-Post3', 'Content3']
      );
      await DBModel.execute(
        `INSERT INTO test_org_posts (org_id, author_id, title, content) VALUES ($1, $2, $3, $4)`,
        [ORG_UUID2, USER_UUID1, 'Org2-Post1', 'Content4']
      );
    });

    describe('belongsTo with composite UUID FK', () => {
      it('should load belongsTo relation with composite UUID FK', async () => {
        const [post] = await OrgPost.find([
          [OrgPost.org_id, ORG_UUID1],
          [OrgPost.title, 'Org1-Post1'],
        ]);
        const author = await post.author;

        expect(author).not.toBeNull();
        expect(author?.org_id).toBe(ORG_UUID1);
        expect(author?.user_id).toBe(USER_UUID1);
        expect(author?.name).toBe('Org1-Author1');
      });

      it('should batch load belongsTo with composite UUID FK', async () => {
        DBModel.use(SqlLoggerMiddleware);
        const logger = SqlLoggerMiddleware.getCurrentContext();
        logger.clear();

        const posts = await OrgPost.find([[OrgPost.org_id, ORG_UUID1]], { order: 'title' });
        createRelationContext(OrgPostModel, posts as OrgPost[]);

        await preloadRelations(posts as OrgPost[], (post) => post.author);

        const author1 = await posts[0].author;
        const author2 = await posts[1].author;
        const author3 = await posts[2].author;

        expect(author1?.name).toBe('Org1-Author1');
        expect(author2?.name).toBe('Org1-Author1');
        expect(author3?.name).toBe('Org1-Author2');

        // Verify batch loading used uuid[] cast
        const selectQueries = logger.getSelectQueries();
        expect(selectQueries.length).toBe(2);
        expect(selectQueries[1].sql).toMatch(/::uuid\[\]/);

        SqlLoggerMiddleware.clearContext();
        DBModel.clearMiddlewares();
      });
    });

    describe('hasMany with composite UUID FK', () => {
      it('should load hasMany relation with composite UUID FK', async () => {
        const [user] = await OrgUser.find([
          [OrgUser.org_id, ORG_UUID1],
          [OrgUser.user_id, USER_UUID1],
        ]);
        const posts = await user.posts;

        expect(posts.length).toBe(2);
        expect(posts.map(p => p.title).sort()).toEqual(['Org1-Post1', 'Org1-Post2']);
      });

      it('should correctly isolate hasMany by composite key', async () => {
        // Same user_id in different orgs should have different posts
        const [org1User] = await OrgUser.find([
          [OrgUser.org_id, ORG_UUID1],
          [OrgUser.user_id, USER_UUID1],
        ]);
        const [org2User] = await OrgUser.find([
          [OrgUser.org_id, ORG_UUID2],
          [OrgUser.user_id, USER_UUID1],
        ]);

        const org1Posts = await org1User.posts;
        const org2Posts = await org2User.posts;

        expect(org1Posts.length).toBe(2);
        expect(org2Posts.length).toBe(1);
        expect(org1Posts.every(p => p.org_id === ORG_UUID1)).toBe(true);
        expect(org2Posts.every(p => p.org_id === ORG_UUID2)).toBe(true);
      });

      it('should batch load hasMany with composite UUID FK', async () => {
        DBModel.use(SqlLoggerMiddleware);
        const logger = SqlLoggerMiddleware.getCurrentContext();
        logger.clear();

        const users = await OrgUser.find([], { order: 'org_id, user_id' });
        createRelationContext(OrgUserModel, users as OrgUser[]);

        // Trigger batch load
        await (users[0] as OrgUser).posts;

        // Verify batch loading used uuid[] cast
        const selectQueries = logger.getSelectQueries();
        expect(selectQueries.length).toBe(2);
        expect(selectQueries[1].sql).toMatch(/::uuid\[\]/);

        SqlLoggerMiddleware.clearContext();
        DBModel.clearMiddlewares();
      });
    });
  });

  // ============================================
  // SQL Cast Verification
  // ============================================

  describe('SQL Cast Verification', () => {
    beforeEach(async () => {
      await DBModel.transaction(async () => {
        await UuidUser.create([
          [UuidUser.id, UUID1],
          [UuidUser.name, 'TestUser'],
          [UuidUser.email, 'test@example.com'],
        ]);
      });
    });

    it('should cast UUID in WHERE eq condition', async () => {
      DBModel.use(SqlLoggerMiddleware);
      const logger = SqlLoggerMiddleware.getCurrentContext();
      logger.clear();

      await UuidUser.find([[UuidUser.id, UUID1]]);

      const selectQueries = logger.getSelectQueries();
      expect(selectQueries[0].sql).toMatch(/::uuid/);

      SqlLoggerMiddleware.clearContext();
      DBModel.clearMiddlewares();
    });

    it('should cast UUID in WHERE IN condition', async () => {
      DBModel.use(SqlLoggerMiddleware);
      const logger = SqlLoggerMiddleware.getCurrentContext();
      logger.clear();

      await UuidUser.find([[UuidUser.id, [UUID1, UUID2]]]);

      const selectQueries = logger.getSelectQueries();
      expect(selectQueries[0].sql).toMatch(/::uuid/);

      SqlLoggerMiddleware.clearContext();
      DBModel.clearMiddlewares();
    });

    it('should cast UUID in INSERT values', async () => {
      DBModel.use(SqlLoggerMiddleware);
      const logger = SqlLoggerMiddleware.getCurrentContext();

      await DBModel.transaction(async () => {
        logger.clear();

        await UuidUser.create([
          [UuidUser.id, UUID2],
          [UuidUser.name, 'NewUser'],
          [UuidUser.email, 'new@example.com'],
        ]);

        const insertQueries = logger.getInsertQueries();
        expect(insertQueries[0].sql).toMatch(/::uuid/);
      });

      SqlLoggerMiddleware.clearContext();
      DBModel.clearMiddlewares();
    });

    it('should cast UUID in UPDATE WHERE condition', async () => {
      DBModel.use(SqlLoggerMiddleware);
      const logger = SqlLoggerMiddleware.getCurrentContext();

      await DBModel.transaction(async () => {
        logger.clear();

        await UuidUser.update(
          [[UuidUser.id, UUID1]],
          [[UuidUser.name, 'Updated']],
        );

        const updateQueries = logger.getUpdateQueries();
        expect(updateQueries[0].sql).toMatch(/::uuid/);
      });

      SqlLoggerMiddleware.clearContext();
      DBModel.clearMiddlewares();
    });

    it('should cast UUID in DELETE WHERE condition', async () => {
      DBModel.use(SqlLoggerMiddleware);
      const logger = SqlLoggerMiddleware.getCurrentContext();

      await DBModel.transaction(async () => {
        logger.clear();

        await UuidUser.delete([[UuidUser.id, UUID1]]);

        const deleteQueries = logger.getDeleteQueries();
        expect(deleteQueries[0].sql).toMatch(/::uuid/);
      });

      SqlLoggerMiddleware.clearContext();
      DBModel.clearMiddlewares();
    });
  });
});

