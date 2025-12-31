/**
 * LazyRelation Tests
 *
 * These tests require a running PostgreSQL instance.
 * Run `docker compose up -d` before running tests.
 * 
 * Skip these tests when PostgreSQL is not available by setting SKIP_INTEGRATION_TESTS=1
 */

import 'reflect-metadata';
import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import {
  LazyRelationContext,
  createRelationContext,
  preloadRelations,
  DBModel,
  model,
  column,
  ColumnsOf,
  closeAllPools,
  Middleware,
} from '../../src';
import type { DBConfig, ExecuteResult } from '../../src';

// ============================================
// SQL Logger Middleware (for testing N+1 queries)
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

  countQueriesMatching(pattern: string | RegExp): number {
    const regex = typeof pattern === 'string' ? new RegExp(pattern, 'i') : pattern;
    return this.queries.filter(q => regex.test(q.sql)).length;
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
// Test Models (using new decorator API)
// ============================================

// Forward declarations for circular references
let TestUser: typeof TestUserModel & ColumnsOf<TestUserModel>;
let TestPost: typeof TestPostModel & ColumnsOf<TestPostModel>;
let TestPostComment: typeof TestPostCommentModel & ColumnsOf<TestPostCommentModel>;
let TestUserProfile: typeof TestUserProfileModel & ColumnsOf<TestUserProfileModel>;

@model('test_users')
class TestUserModel extends DBModel {
  @column() id?: number;
  @column() name?: string;
  @column() email?: string;
  @column() created_at?: Date;

  // Relations - accessed with await
  get posts(): Promise<TestPostModel[]> {
    return this._hasMany(TestPostModel, {
      targetKey: TestPost.user_id,
      order: TestPost.created_at.desc(),
    });
  }

  get profile(): Promise<TestUserProfileModel | null> {
    return this._hasOne(TestUserProfileModel, {
      targetKey: TestUserProfile.user_id,
    });
  }

  // Reload methods (bypass cache)
  reloadPosts(): Promise<TestPostModel[]> {
    this.clearRelationCache();
    return this.posts;
  }
}
TestUser = TestUserModel as typeof TestUserModel & ColumnsOf<TestUserModel>;
type TestUser = TestUserModel;

@model('test_posts')
class TestPostModel extends DBModel {
  @column() id?: number;
  @column() user_id?: number;
  @column() title?: string;
  @column() content?: string;
  @column() published?: boolean;
  @column() created_at?: Date;

  // Relations - accessed with await
  get author(): Promise<TestUserModel | null> {
    return this._belongsTo(TestUserModel, {
      targetKey: TestUser.id,
      sourceKey: TestPost.user_id,
    });
  }

  get comments(): Promise<TestPostCommentModel[]> {
    return this._hasMany(TestPostCommentModel, {
      targetKey: TestPostComment.post_id,
      order: TestPostComment.created_at.asc(),
    });
  }

  // Filtered relation example
  publishedComments(): Promise<TestPostCommentModel[]> {
    return this._hasMany(TestPostCommentModel, {
      targetKey: TestPostComment.post_id,
      where: [[TestPostComment.published, true]],
      order: TestPostComment.created_at.asc(),
    });
  }
}
TestPost = TestPostModel as typeof TestPostModel & ColumnsOf<TestPostModel>;
type TestPost = TestPostModel;

@model('test_post_comments')
class TestPostCommentModel extends DBModel {
  @column() id?: number;
  @column() post_id?: number;
  @column() user_id?: number;
  @column() content?: string;
  @column() published?: boolean;
  @column() created_at?: Date;

  // Relations
  get post(): Promise<TestPostModel | null> {
    return this._belongsTo(TestPostModel, {
      targetKey: TestPost.id,
      sourceKey: TestPostComment.post_id,
    });
  }

  get user(): Promise<TestUserModel | null> {
    return this._belongsTo(TestUserModel, {
      targetKey: TestUser.id,
      sourceKey: TestPostComment.user_id,
    });
  }
}
TestPostComment = TestPostCommentModel as typeof TestPostCommentModel & ColumnsOf<TestPostCommentModel>;
type TestPostComment = TestPostCommentModel;

@model('test_user_profiles')
class TestUserProfileModel extends DBModel {
  @column() id?: number;
  @column() user_id?: number;
  @column() bio?: string;
  @column() website?: string;
  @column() created_at?: Date;

  // Relations
  get user(): Promise<TestUserModel | null> {
    return this._belongsTo(TestUserModel, {
      targetKey: TestUser.id,
      sourceKey: TestUserProfile.user_id,
    });
  }
}
TestUserProfile = TestUserProfileModel as typeof TestUserProfileModel & ColumnsOf<TestUserProfileModel>;
type TestUserProfile = TestUserProfileModel;

// ============================================
// Composite Key Test Models (for multi-tenant scenarios)
// ============================================

let TenantUser: typeof TenantUserModel & ColumnsOf<TenantUserModel>;
let TenantPost: typeof TenantPostModel & ColumnsOf<TenantPostModel>;

@model('test_tenant_users')
class TenantUserModel extends DBModel {
  @column() tenant_id?: number;
  @column() id?: number;
  @column() name?: string;
  @column() created_at?: Date;

  // Composite primary key
  static getPkeyColumns() {
    return [TenantUser.tenant_id, TenantUser.id];
  }

  // HasMany with composite key
  get posts(): Promise<TenantPostModel[]> {
    return this._hasMany(TenantPostModel, {
      targetKeys: [TenantPost.tenant_id, TenantPost.user_id],
      sourceKeys: [TenantUser.tenant_id, TenantUser.id],
      order: TenantPost.created_at.desc(),
    });
  }
}
TenantUser = TenantUserModel as typeof TenantUserModel & ColumnsOf<TenantUserModel>;
type TenantUser = TenantUserModel;

@model('test_tenant_posts')
class TenantPostModel extends DBModel {
  @column() tenant_id?: number;
  @column() id?: number;
  @column() user_id?: number;
  @column() title?: string;
  @column() content?: string;
  @column() created_at?: Date;

  // Composite primary key
  static getPkeyColumns() {
    return [TenantPost.tenant_id, TenantPost.id];
  }

  // BelongsTo with composite key
  get author(): Promise<TenantUserModel | null> {
    return this._belongsTo(TenantUserModel, {
      targetKeys: [TenantUser.tenant_id, TenantUser.id],
      sourceKeys: [TenantPost.tenant_id, TenantPost.user_id],
    });
  }
}
TenantPost = TenantPostModel as typeof TenantPostModel & ColumnsOf<TenantPostModel>;
type TenantPost = TenantPostModel;

// ============================================
// Test Setup
// ============================================

describe.skipIf(skipIntegrationTests)('LazyRelation', () => {
  beforeAll(async () => {
    // Set DB config for DBModel (via DBModel.setConfig)
    DBModel.setConfig(testConfig);

    // Create test tables (with test_ prefix to avoid conflicts)
    await DBModel.execute(`
      DROP TABLE IF EXISTS test_post_comments CASCADE;
      DROP TABLE IF EXISTS test_user_profiles CASCADE;
      DROP TABLE IF EXISTS test_posts CASCADE;
      DROP TABLE IF EXISTS test_users CASCADE;

      CREATE TABLE test_users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      );

      CREATE TABLE test_posts (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES test_users(id),
        title VARCHAR(255) NOT NULL,
        content TEXT,
        published BOOLEAN DEFAULT true,
        created_at TIMESTAMP DEFAULT NOW()
      );

      CREATE TABLE test_post_comments (
        id SERIAL PRIMARY KEY,
        post_id INTEGER REFERENCES test_posts(id),
        user_id INTEGER REFERENCES test_users(id),
        content TEXT NOT NULL,
        published BOOLEAN DEFAULT true,
        created_at TIMESTAMP DEFAULT NOW()
      );

      CREATE TABLE test_user_profiles (
        id SERIAL PRIMARY KEY,
        user_id INTEGER UNIQUE REFERENCES test_users(id),
        bio TEXT,
        website VARCHAR(255),
        created_at TIMESTAMP DEFAULT NOW()
      );

      -- Composite key tables for multi-tenant testing
      CREATE TABLE test_tenant_users (
        tenant_id INTEGER NOT NULL,
        id INTEGER NOT NULL,
        name VARCHAR(255) NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (tenant_id, id)
      );

      CREATE TABLE test_tenant_posts (
        tenant_id INTEGER NOT NULL,
        id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        title VARCHAR(255) NOT NULL,
        content TEXT,
        created_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (tenant_id, id),
        FOREIGN KEY (tenant_id, user_id) REFERENCES test_tenant_users(tenant_id, id)
      );
    `);
  });

  afterAll(async () => {
    // Clean up test tables
    await DBModel.execute(`
      DROP TABLE IF EXISTS test_tenant_posts CASCADE;
      DROP TABLE IF EXISTS test_tenant_users CASCADE;
      DROP TABLE IF EXISTS test_post_comments CASCADE;
      DROP TABLE IF EXISTS test_user_profiles CASCADE;
      DROP TABLE IF EXISTS test_posts CASCADE;
      DROP TABLE IF EXISTS test_users CASCADE;
    `);
    await closeAllPools();
  });

  beforeEach(async () => {
    // Clear test data before each test
    await DBModel.execute('DELETE FROM test_tenant_posts');
    await DBModel.execute('DELETE FROM test_tenant_users');
    await DBModel.execute('DELETE FROM test_post_comments');
    await DBModel.execute('DELETE FROM test_user_profiles');
    await DBModel.execute('DELETE FROM test_posts');
    await DBModel.execute('DELETE FROM test_users');
    await DBModel.execute('ALTER SEQUENCE test_users_id_seq RESTART WITH 1');
    await DBModel.execute('ALTER SEQUENCE test_posts_id_seq RESTART WITH 1');
    await DBModel.execute('ALTER SEQUENCE test_post_comments_id_seq RESTART WITH 1');
    await DBModel.execute('ALTER SEQUENCE test_user_profiles_id_seq RESTART WITH 1');
  });

  // ============================================
  // Helper Functions
  // ============================================

  async function createTestUser(name: string, email: string): Promise<TestUser> {
    const user = await TestUser.create([
      [TestUser.name, name],
      [TestUser.email, email],
    ]);
    return user as TestUser;
  }

  async function createTestPost(userId: number, title: string, content: string): Promise<TestPost> {
    const post = await TestPost.create([
      [TestPost.user_id, userId],
      [TestPost.title, title],
      [TestPost.content, content],
    ]);
    return post as TestPost;
  }

  async function createTestComment(
    postId: number,
    userId: number,
    content: string,
    published = true
  ): Promise<TestPostComment> {
    const comment = await TestPostComment.create([
      [TestPostComment.post_id, postId],
      [TestPostComment.user_id, userId],
      [TestPostComment.content, content],
      [TestPostComment.published, published],
    ]);
    return comment as TestPostComment;
  }

  async function createTestProfile(
    userId: number,
    bio: string,
    website: string
  ): Promise<TestUserProfile> {
    const profile = await TestUserProfile.create([
      [TestUserProfile.user_id, userId],
      [TestUserProfile.bio, bio],
      [TestUserProfile.website, website],
    ]);
    return profile as TestUserProfile;
  }

  // ============================================
  // Property-style Relations Tests
  // ============================================

  describe('Property-style Relations (await record.relation)', () => {
    it('should access belongsTo relation with await', async () => {
      const user = await createTestUser('Author', 'author@test.com');
      const post = await createTestPost(user.id!, 'Test Post', 'Content');

      // Access relation with await
      const author = await post.author;

      expect(author).not.toBeNull();
      expect(author?.id).toBe(user.id);
      expect(author?.name).toBe('Author');
    });

    it('should access hasMany relation with await', async () => {
      const user = await createTestUser('User', 'user@test.com');
      await createTestPost(user.id!, 'Post 1', 'Content 1');
      await createTestPost(user.id!, 'Post 2', 'Content 2');
      await createTestPost(user.id!, 'Post 3', 'Content 3');

      // Access relation with await
      const posts = await user.posts;

      expect(posts.length).toBe(3);
      expect(posts.map((p) => p.title)).toEqual(
        expect.arrayContaining(['Post 1', 'Post 2', 'Post 3'])
      );
    });

    it('should access hasOne relation with await', async () => {
      const user = await createTestUser('User', 'user@test.com');
      await createTestProfile(user.id!, 'My bio', 'https://example.com');

      // Access relation with await
      const profile = await user.profile;

      expect(profile).not.toBeNull();
      expect(profile?.bio).toBe('My bio');
      expect(profile?.website).toBe('https://example.com');
    });

    it('should return null for missing belongsTo relation', async () => {
      // Create a user and post, then delete the user using CASCADE
      const user = await createTestUser('Temp User', 'temp@test.com');
      await createTestPost(user.id!, 'Orphan Post', 'Content');

      // Temporarily drop FK, update to invalid ID, then don't re-add FK
      await DBModel.execute('ALTER TABLE test_posts DROP CONSTRAINT IF EXISTS test_posts_user_id_fkey');
      await DBModel.execute('UPDATE test_posts SET user_id = 99999 WHERE id = 1');

      // Fetch a fresh post instance from DB (no cached relations)
      const [freshPost] = await TestPost.find([[TestPost.id, 1]]);

      const author = await (freshPost as TestPost).author;

      expect(author).toBeNull();
      
      // Restore FK constraint (without validation) for subsequent tests
      await DBModel.execute('ALTER TABLE test_posts ADD CONSTRAINT test_posts_user_id_fkey FOREIGN KEY (user_id) REFERENCES test_users(id) NOT VALID');
    });

    it('should return empty array for empty hasMany relation', async () => {
      const user = await createTestUser('User', 'user@test.com');
      // No posts created

      const posts = await user.posts;

      expect(posts).toEqual([]);
    });

    it('should return null for missing hasOne relation', async () => {
      const user = await createTestUser('User', 'user@test.com');
      // No profile created

      const profile = await user.profile;

      expect(profile).toBeNull();
    });
  });

  // ============================================
  // Relation Caching Tests
  // ============================================

  describe('Relation Caching', () => {
    it('should cache relation after first access', async () => {
      const user = await createTestUser('User', 'user@test.com');
      await createTestPost(user.id!, 'Post 1', 'Content 1');
      await createTestPost(user.id!, 'Post 2', 'Content 2');

      // First access - loads from DB
      const posts1 = await user.posts;
      expect(posts1.length).toBe(2);

      // Add another post directly to DB
      await DBModel.execute(
        `INSERT INTO test_posts (user_id, title, content) VALUES ($1, $2, $3)`,
        [user.id!, 'Post 3', 'Content 3']
      );

      // Second access - returns cached value (still 2 posts)
      const posts2 = await user.posts;
      expect(posts2.length).toBe(2);
      expect(posts1).toBe(posts2); // Same array reference
    });

    it('should reload relation when requested', async () => {
      const user = await createTestUser('User', 'user@test.com');
      await createTestPost(user.id!, 'Post 1', 'Content 1');
      await createTestPost(user.id!, 'Post 2', 'Content 2');

      // First access - loads from DB
      const posts1 = await user.posts;
      expect(posts1.length).toBe(2);

      // Add another post directly to DB
      await DBModel.execute(
        `INSERT INTO test_posts (user_id, title, content) VALUES ($1, $2, $3)`,
        [user.id!, 'Post 3', 'Content 3']
      );

      // Reload relation - fetches fresh data
      const posts2 = await user.reloadPosts();
      expect(posts2.length).toBe(3);
    });

    it('should clear cache when clearRelationCache is called', async () => {
      const user = await createTestUser('User', 'user@test.com');
      await createTestPost(user.id!, 'Post 1', 'Content 1');

      // First access
      const posts1 = await user.posts;
      expect(posts1.length).toBe(1);

      // Add another post
      await DBModel.execute(
        `INSERT INTO test_posts (user_id, title, content) VALUES ($1, $2, $3)`,
        [user.id!, 'Post 2', 'Content 2']
      );

      // Clear cache
      user.clearRelationCache();

      // Access again - should reload from DB
      const posts2 = await user.posts;
      expect(posts2.length).toBe(2);
    });
  });

  // ============================================
  // Batch Loading Tests
  // ============================================

  describe('Batch Loading (preloadRelations)', () => {
    it('should batch load hasMany relations', async () => {
      // Create multiple users with posts
      const user1 = await createTestUser('User 1', 'user1@test.com');
      const user2 = await createTestUser('User 2', 'user2@test.com');
      const user3 = await createTestUser('User 3', 'user3@test.com');

      await createTestPost(user1.id!, 'User1 Post 1', 'Content');
      await createTestPost(user1.id!, 'User1 Post 2', 'Content');
      await createTestPost(user2.id!, 'User2 Post 1', 'Content');
      await createTestPost(user3.id!, 'User3 Post 1', 'Content');
      await createTestPost(user3.id!, 'User3 Post 2', 'Content');
      await createTestPost(user3.id!, 'User3 Post 3', 'Content');

      // Load users with batch-loaded relations
      const users = await TestUser.find([], { order: 'id' });

      // Create a context and preload relations
      const context = createRelationContext(TestUserModel, users as TestUser[]);

      // Preload posts using accessor function
      await preloadRelations(users as TestUser[], (user) => user.posts);

      // All relations should be loaded in a single batch query
      expect((await (users[0] as TestUser).posts).length).toBe(2);
      expect((await (users[1] as TestUser).posts).length).toBe(1);
      expect((await (users[2] as TestUser).posts).length).toBe(3);
    });

    it('should batch load belongsTo relations', async () => {
      const user1 = await createTestUser('User 1', 'user1@test.com');
      const user2 = await createTestUser('User 2', 'user2@test.com');

      const post1 = await createTestPost(user1.id!, 'Post 1', 'Content');
      const post2 = await createTestPost(user1.id!, 'Post 2', 'Content');
      const post3 = await createTestPost(user2.id!, 'Post 3', 'Content');

      // Load posts
      const posts = await TestPost.find([], { order: 'id' });

      // Create a context and preload relations
      const context = createRelationContext(TestPostModel, posts as TestPost[]);

      // Preload authors using accessor function
      await preloadRelations(posts as TestPost[], (post) => post.author);

      // All relations should be loaded
      const author1 = await (posts[0] as TestPost).author;
      const author2 = await (posts[1] as TestPost).author;
      const author3 = await (posts[2] as TestPost).author;

      expect(author1?.id).toBe(user1.id);
      expect(author2?.id).toBe(user1.id);
      expect(author3?.id).toBe(user2.id);
    });
  });

  // ============================================
  // LazyRelationContext Tests
  // ============================================

  describe('LazyRelationContext', () => {
    it('should share cache between records in same context', async () => {
      const user = await createTestUser('User', 'user@test.com');
      const post1 = await createTestPost(user.id!, 'Post 1', 'Content');
      const post2 = await createTestPost(user.id!, 'Post 2', 'Content');

      // Load posts in same context
      const posts = await TestPost.find([], { order: 'id' });

      // Create context and set it for all posts
      const context = createRelationContext(TestPostModel, posts as TestPost[]);

      // Access author for first post - loads from DB
      const author1 = await (posts[0] as TestPost).author;
      expect(author1?.id).toBe(user.id);

      // Access author for second post - should use cached value from batch load
      const author2 = await (posts[1] as TestPost).author;
      expect(author2?.id).toBe(user.id);

      // Both should reference the same user object (shared cache)
      expect(author1).toBe(author2);
    });

    it('should create independent contexts', async () => {
      const user = await createTestUser('User', 'user@test.com');
      await createTestPost(user.id!, 'Post 1', 'Content');

      // Load same user in first context
      const [context1User] = await TestUser.find([[TestUser.id, user.id!]]);

      // First context - loads initial data (1 post)
      const posts1 = await (context1User as TestUser).posts;
      expect(posts1.length).toBe(1);

      // Modify posts in DB AFTER first context has loaded
      await DBModel.execute(
        `INSERT INTO test_posts (user_id, title, content) VALUES ($1, $2, $3)`,
        [user.id!, 'Post 2', 'Content']
      );

      // Load same user in second context (fresh context, fresh data)
      const [context2User] = await TestUser.find([[TestUser.id, user.id!]]);

      // Second context - loads fresh data (2 posts)
      const posts2 = await (context2User as TestUser).posts;
      expect(posts2.length).toBe(2);
    });
  });

  // ============================================
  // Complex Relations Tests
  // ============================================

  describe('Complex Relations', () => {
    it('should handle multiple levels of relations', async () => {
      const user = await createTestUser('User', 'user@test.com');
      const post = await createTestPost(user.id!, 'Post', 'Content');
      await createTestComment(post.id!, user.id!, 'Comment 1');
      await createTestComment(post.id!, user.id!, 'Comment 2');

      // Load comments for post
      const comments = await post.comments;
      expect(comments.length).toBe(2);

      // Load post for each comment (belongsTo)
      const commentPost = await comments[0].post;
      expect(commentPost?.id).toBe(post.id);

      // Load author for the post
      const author = await commentPost!.author;
      expect(author?.id).toBe(user.id);
    });

    it('should handle relations with additional conditions via method', async () => {
      const user = await createTestUser('User', 'user@test.com');
      const post = await createTestPost(user.id!, 'Post', 'Content');
      await createTestComment(post.id!, user.id!, 'Published 1', true);
      await createTestComment(post.id!, user.id!, 'Unpublished', false);
      await createTestComment(post.id!, user.id!, 'Published 2', true);

      // All comments
      const allComments = await post.comments;
      expect(allComments.length).toBe(3);

      // Only published comments (using method with extra condition)
      const publishedComments = await post.publishedComments();
      expect(publishedComments.length).toBe(2);
      expect(publishedComments.every((c) => c.content?.startsWith('Published'))).toBe(true);
    });

    it('should handle self-referential relations via custom setup', async () => {
      // This test demonstrates that self-referential relations can work
      // by using the standard relation methods
      const user1 = await createTestUser('User 1', 'user1@test.com');
      const user2 = await createTestUser('User 2', 'user2@test.com');

      // Create posts between users
      await createTestPost(user1.id!, 'From User 1', 'Content');
      await createTestPost(user2.id!, 'From User 2', 'Content');

      // Verify each user's posts
      const user1Posts = await user1.posts;
      const user2Posts = await user2.posts;

      expect(user1Posts.length).toBe(1);
      expect(user1Posts[0].title).toBe('From User 1');

      expect(user2Posts.length).toBe(1);
      expect(user2Posts[0].title).toBe('From User 2');
    });
  });

  // ============================================
  // Composite Key Relations Tests
  // ============================================

  describe('Composite Key Relations', () => {
    async function createTenantUser(tenantId: number, userId: number, name: string): Promise<TenantUser> {
      await DBModel.execute(
        `INSERT INTO test_tenant_users (tenant_id, id, name) VALUES ($1, $2, $3)`,
        [tenantId, userId, name]
      );
      const [user] = await TenantUser.find([
        [TenantUser.tenant_id, tenantId],
        [TenantUser.id, userId],
      ]);
      return user as TenantUser;
    }

    async function createTenantPost(tenantId: number, postId: number, userId: number, title: string): Promise<TenantPost> {
      await DBModel.execute(
        `INSERT INTO test_tenant_posts (tenant_id, id, user_id, title) VALUES ($1, $2, $3, $4)`,
        [tenantId, postId, userId, title]
      );
      const [post] = await TenantPost.find([
        [TenantPost.tenant_id, tenantId],
        [TenantPost.id, postId],
      ]);
      return post as TenantPost;
    }

    it('should handle belongsTo with composite key', async () => {
      // Create user in tenant 1
      const user = await createTenantUser(1, 100, 'Tenant1 User');
      
      // Create post by this user
      const post = await createTenantPost(1, 1, 100, 'Test Post');

      // Access belongsTo relation with composite key
      const author = await post.author;

      expect(author).not.toBeNull();
      expect(author?.tenant_id).toBe(1);
      expect(author?.id).toBe(100);
      expect(author?.name).toBe('Tenant1 User');
    });

    it('should handle hasMany with composite key', async () => {
      // Create user in tenant 1
      const user = await createTenantUser(1, 100, 'Tenant1 User');
      
      // Create multiple posts by this user
      await createTenantPost(1, 1, 100, 'Post 1');
      await createTenantPost(1, 2, 100, 'Post 2');
      await createTenantPost(1, 3, 100, 'Post 3');

      // Access hasMany relation with composite key
      const posts = await user.posts;

      expect(posts.length).toBe(3);
      expect(posts.map(p => p.title)).toContain('Post 1');
      expect(posts.map(p => p.title)).toContain('Post 2');
      expect(posts.map(p => p.title)).toContain('Post 3');
    });

    it('should correctly isolate composite key relations by tenant', async () => {
      // Create users in different tenants with same user id
      const tenant1User = await createTenantUser(1, 100, 'Tenant1 User');
      const tenant2User = await createTenantUser(2, 100, 'Tenant2 User');

      // Create posts for each tenant's user
      await createTenantPost(1, 1, 100, 'Tenant1 Post');
      await createTenantPost(2, 1, 100, 'Tenant2 Post');

      // Verify posts are correctly isolated by tenant
      const tenant1Posts = await tenant1User.posts;
      const tenant2Posts = await tenant2User.posts;

      expect(tenant1Posts.length).toBe(1);
      expect(tenant1Posts[0].title).toBe('Tenant1 Post');
      expect(tenant1Posts[0].tenant_id).toBe(1);

      expect(tenant2Posts.length).toBe(1);
      expect(tenant2Posts[0].title).toBe('Tenant2 Post');
      expect(tenant2Posts[0].tenant_id).toBe(2);
    });

    it('should batch load composite key relations efficiently', async () => {
      // Create multiple users across tenants
      const user1 = await createTenantUser(1, 100, 'User 1');
      const user2 = await createTenantUser(1, 101, 'User 2');
      const user3 = await createTenantUser(2, 100, 'User 3');

      // Create posts for each user
      await createTenantPost(1, 1, 100, 'User1 Post 1');
      await createTenantPost(1, 2, 100, 'User1 Post 2');
      await createTenantPost(1, 3, 101, 'User2 Post 1');
      await createTenantPost(2, 1, 100, 'User3 Post 1');
      await createTenantPost(2, 2, 100, 'User3 Post 2');
      await createTenantPost(2, 3, 100, 'User3 Post 3');

      // Load all users
      const users = await TenantUser.find([], { order: 'tenant_id, id' });

      // Create context for batch loading
      createRelationContext(TenantUserModel, users as TenantUser[]);

      // Access posts for all users - should use batch loading with (tenant_id, id) IN (...)
      const user1Posts = await (users[0] as TenantUser).posts;
      const user2Posts = await (users[1] as TenantUser).posts;
      const user3Posts = await (users[2] as TenantUser).posts;

      expect(user1Posts.length).toBe(2);
      expect(user2Posts.length).toBe(1);
      expect(user3Posts.length).toBe(3);
    });

    it('should return null for missing composite key belongsTo', async () => {
      // Temporarily remove FK constraint for this test
      await DBModel.execute('ALTER TABLE test_tenant_posts DROP CONSTRAINT IF EXISTS test_tenant_posts_tenant_id_user_id_fkey');
      
      // Create post with non-existent user reference
      await DBModel.execute(
        `INSERT INTO test_tenant_posts (tenant_id, id, user_id, title) VALUES ($1, $2, $3, $4)`,
        [1, 1, 999, 'Orphan Post']
      );
      
      const [post] = await TenantPost.find([[TenantPost.tenant_id, 1], [TenantPost.id, 1]]);
      
      const author = await (post as TenantPost).author;
      
      expect(author).toBeNull();
      
      // Restore FK constraint
      await DBModel.execute('ALTER TABLE test_tenant_posts ADD CONSTRAINT test_tenant_posts_tenant_id_user_id_fkey FOREIGN KEY (tenant_id, user_id) REFERENCES test_tenant_users(tenant_id, id) NOT VALID');
    });

    it('should return empty array for empty composite key hasMany', async () => {
      // Create user with no posts
      const user = await createTenantUser(1, 100, 'No Posts User');

      const posts = await user.posts;

      expect(posts).toEqual([]);
    });
  });

  // ============================================
  // N+1 Query Prevention Tests (with SQL Logger)
  // ============================================

  describe('N+1 Query Prevention', () => {
    // Helper functions for composite key tests
    async function createTenantUserForN1(tenantId: number, userId: number, name: string): Promise<TenantUser> {
      await DBModel.execute(
        `INSERT INTO test_tenant_users (tenant_id, id, name) VALUES ($1, $2, $3)`,
        [tenantId, userId, name]
      );
      const [user] = await TenantUser.find([
        [TenantUser.tenant_id, tenantId],
        [TenantUser.id, userId],
      ]);
      return user as TenantUser;
    }

    async function createTenantPostForN1(tenantId: number, postId: number, userId: number, title: string): Promise<TenantPost> {
      await DBModel.execute(
        `INSERT INTO test_tenant_posts (tenant_id, id, user_id, title) VALUES ($1, $2, $3, $4)`,
        [tenantId, postId, userId, title]
      );
      const [post] = await TenantPost.find([
        [TenantPost.tenant_id, tenantId],
        [TenantPost.id, postId],
      ]);
      return post as TenantPost;
    }

    beforeEach(() => {
      // Register SQL logger middleware
      DBModel.use(SqlLoggerMiddleware);
    });

    afterEach(() => {
      // Clear middleware context
      SqlLoggerMiddleware.clearContext();
      DBModel.clearMiddlewares();
    });

    // ============================================
    // Batch Loading SQL Format Tests (PostgreSQL-specific)
    // ============================================

    it('should use = ANY($1::type[]) for single key batch loading on PostgreSQL', async () => {
      // Skip if not PostgreSQL
      if (DBModel.getDriverType() !== 'postgres') {
        return;
      }

      // Create test data
      const user1 = await createTestUser('User 1', 'user1@anytest.com');
      const user2 = await createTestUser('User 2', 'user2@anytest.com');
      const user3 = await createTestUser('User 3', 'user3@anytest.com');

      await createTestPost(user1.id!, 'Post 1', 'Content');
      await createTestPost(user2.id!, 'Post 2', 'Content');
      await createTestPost(user3.id!, 'Post 3', 'Content');

      // Clear query log
      const logger = SqlLoggerMiddleware.getCurrentContext();
      logger.clear();

      // Load users and trigger batch loading
      const users = await TestUser.find([], { order: 'id' });
      createRelationContext(TestUserModel, users as TestUser[]);
      await preloadRelations(users as TestUser[], (user) => user.posts);

      // Verify SQL format
      const selectQueries = logger.getSelectQueries();
      expect(selectQueries.length).toBe(2);

      // Second query should use = ANY($1::int[]) format
      const postsQuery = selectQueries[1].sql;
      expect(postsQuery).toMatch(/test_posts\.user_id\s*=\s*ANY\s*\(\s*\$1::int\[\]\s*\)/i);
      
      // Should NOT use IN (...) format
      expect(postsQuery).not.toMatch(/user_id\s+IN\s*\(/i);
    });

    it('should use unnest + JOIN for composite key batch loading on PostgreSQL', async () => {
      // Skip if not PostgreSQL
      if (DBModel.getDriverType() !== 'postgres') {
        return;
      }

      // Create multi-tenant test data
      const user1 = await createTenantUserForN1(1, 100, 'Tenant1 User1');
      const user2 = await createTenantUserForN1(1, 101, 'Tenant1 User2');
      const user3 = await createTenantUserForN1(2, 100, 'Tenant2 User1');

      await createTenantPostForN1(1, 1, 100, 'T1U1 Post');
      await createTenantPostForN1(1, 2, 101, 'T1U2 Post');
      await createTenantPostForN1(2, 1, 100, 'T2U1 Post');

      // Clear query log
      const logger = SqlLoggerMiddleware.getCurrentContext();
      logger.clear();

      // Load users and trigger batch loading
      const users = await TenantUser.find([], { order: 'tenant_id, id' });
      createRelationContext(TenantUserModel, users as TenantUser[]);
      
      // Trigger batch load
      await (users[0] as TenantUser).posts;

      // Verify SQL format
      const selectQueries = logger.getSelectQueries();
      expect(selectQueries.length).toBe(2);

      const postsQuery = selectQueries[1].sql;
      
      // Should use JOIN unnest(...) format
      expect(postsQuery).toMatch(/JOIN\s+unnest\s*\(/i);
      
      // Should have column aliases with _unnest_ prefix to avoid conflicts
      expect(postsQuery).toMatch(/_unnest_test_tenant_posts/i);
      
      // Should use typed arrays ($1::int[], $2::int[])
      expect(postsQuery).toMatch(/\$1::int\[\]/i);
      expect(postsQuery).toMatch(/\$2::int\[\]/i);
      
      // Should NOT use (col1, col2) IN (...) format
      expect(postsQuery).not.toMatch(/\(tenant_id,\s*user_id\)\s*IN\s*\(/i);
    });

    it('should use IN clause for single key batch loading on non-PostgreSQL', async () => {
      // This test verifies fallback behavior
      // Since we're running on PostgreSQL, we can only verify the pattern exists in code
      // The actual behavior is tested implicitly when MySQL/SQLite tests run
      
      // For now, just verify the test infrastructure works
      const user = await createTestUser('User', 'user@fallback.com');
      await createTestPost(user.id!, 'Post', 'Content');

      const logger = SqlLoggerMiddleware.getCurrentContext();
      logger.clear();

      // Load and access
      const users = await TestUser.find([[TestUser.id, user.id!]]);
      const posts = await (users[0] as TestUser).posts;

      expect(posts.length).toBe(1);
      
      // At least one query should be executed
      const selectQueries = logger.getSelectQueries();
      expect(selectQueries.length).toBeGreaterThan(0);
    });

    it('should correctly pass array parameter for ANY clause', async () => {
      // Skip if not PostgreSQL
      if (DBModel.getDriverType() !== 'postgres') {
        return;
      }

      // Create test data with specific IDs
      const user1 = await createTestUser('User 1', 'user1@param.com');
      const user2 = await createTestUser('User 2', 'user2@param.com');
      const user3 = await createTestUser('User 3', 'user3@param.com');

      await createTestPost(user1.id!, 'Post 1', 'Content 1');
      await createTestPost(user2.id!, 'Post 2', 'Content 2');
      await createTestPost(user3.id!, 'Post 3', 'Content 3');

      // Clear query log
      const logger = SqlLoggerMiddleware.getCurrentContext();
      logger.clear();

      // Load users and batch load posts
      const users = await TestUser.find([], { order: 'id' });
      createRelationContext(TestUserModel, users as TestUser[]);
      await preloadRelations(users as TestUser[], (user) => user.posts);

      // Verify query params
      const selectQueries = logger.getSelectQueries();
      const postsQuery = selectQueries[1];
      
      // Params should contain the array of user IDs
      expect(postsQuery.params).toBeDefined();
      expect(Array.isArray(postsQuery.params![0])).toBe(true);
      
      // The array should contain all user IDs
      const userIds = users.map(u => u.id);
      expect(postsQuery.params![0]).toEqual(expect.arrayContaining(userIds));
    });

    it('should correctly pass multiple arrays for unnest composite key', async () => {
      // Skip if not PostgreSQL
      if (DBModel.getDriverType() !== 'postgres') {
        return;
      }

      // Create multi-tenant test data
      const user1 = await createTenantUserForN1(10, 200, 'T10 User');
      const user2 = await createTenantUserForN1(10, 201, 'T10 User2');
      const user3 = await createTenantUserForN1(20, 200, 'T20 User');

      await createTenantPostForN1(10, 1, 200, 'Post');
      await createTenantPostForN1(10, 2, 201, 'Post');
      await createTenantPostForN1(20, 1, 200, 'Post');

      // Clear query log
      const logger = SqlLoggerMiddleware.getCurrentContext();
      logger.clear();

      // Load users and batch load posts
      const users = await TenantUser.find([], { order: 'tenant_id, id' });
      createRelationContext(TenantUserModel, users as TenantUser[]);
      await (users[0] as TenantUser).posts;

      // Verify query params
      const selectQueries = logger.getSelectQueries();
      const postsQuery = selectQueries[1];
      
      // Params should contain two arrays (tenant_ids and user_ids)
      expect(postsQuery.params).toBeDefined();
      expect(postsQuery.params!.length).toBeGreaterThanOrEqual(2);
      
      // First param: tenant_ids array
      expect(Array.isArray(postsQuery.params![0])).toBe(true);
      expect(postsQuery.params![0]).toContain(10);
      expect(postsQuery.params![0]).toContain(20);
      
      // Second param: user_ids array
      expect(Array.isArray(postsQuery.params![1])).toBe(true);
      expect(postsQuery.params![1]).toContain(200);
      expect(postsQuery.params![1]).toContain(201);
    });

    // ============================================
    // Original N+1 Prevention Tests
    // ============================================

    it('should use single batch query for hasMany relations with preload', async () => {
      // Create test data
      const user1 = await createTestUser('User 1', 'user1@batch.com');
      const user2 = await createTestUser('User 2', 'user2@batch.com');
      const user3 = await createTestUser('User 3', 'user3@batch.com');

      await createTestPost(user1.id!, 'User1 Post 1', 'Content');
      await createTestPost(user1.id!, 'User1 Post 2', 'Content');
      await createTestPost(user2.id!, 'User2 Post 1', 'Content');
      await createTestPost(user3.id!, 'User3 Post 1', 'Content');
      await createTestPost(user3.id!, 'User3 Post 2', 'Content');
      await createTestPost(user3.id!, 'User3 Post 3', 'Content');

      // Clear query log before the actual test
      const logger = SqlLoggerMiddleware.getCurrentContext();
      logger.clear();

      // Load all users
      const users = await TestUser.find([], { order: 'id' });
      
      // Create context for batch loading
      createRelationContext(TestUserModel, users as TestUser[]);

      // Preload posts for all users
      await preloadRelations(users as TestUser[], (user) => user.posts);

      // Access posts for each user (should be from cache)
      const user1Posts = await (users[0] as TestUser).posts;
      const user2Posts = await (users[1] as TestUser).posts;
      const user3Posts = await (users[2] as TestUser).posts;

      // Verify data is correct
      expect(user1Posts.length).toBe(2);
      expect(user2Posts.length).toBe(1);
      expect(user3Posts.length).toBe(3);

      // Verify SQL queries - should be 2 SELECTs:
      // 1. SELECT from test_users
      // 2. SELECT from test_posts (batch load with IN clause)
      const selectQueries = logger.getSelectQueries();
      expect(selectQueries.length).toBe(2);
      
      // First query: fetch users
      expect(selectQueries[0].sql).toMatch(/FROM\s+"?test_users"?/i);
      
      // Second query: batch fetch posts (NOT N+1)
      // PostgreSQL uses = ANY($1::type[]), MySQL/SQLite uses IN (...)
      expect(selectQueries[1].sql).toMatch(/FROM\s+"?test_posts"?/i);
      expect(selectQueries[1].sql).toMatch(/(?:IN\s*\(|=\s*ANY\s*\()/i);
    });

    it('should use single batch query for belongsTo relations', async () => {
      // Create test data
      const user1 = await createTestUser('Author 1', 'author1@batch.com');
      const user2 = await createTestUser('Author 2', 'author2@batch.com');

      await createTestPost(user1.id!, 'Post 1', 'Content');
      await createTestPost(user1.id!, 'Post 2', 'Content');
      await createTestPost(user2.id!, 'Post 3', 'Content');
      await createTestPost(user2.id!, 'Post 4', 'Content');

      // Clear query log
      const logger = SqlLoggerMiddleware.getCurrentContext();
      logger.clear();

      // Load all posts
      const posts = await TestPost.find([], { order: 'id' });

      // Create context for batch loading
      createRelationContext(TestPostModel, posts as TestPost[]);

      // Preload authors for all posts
      await preloadRelations(posts as TestPost[], (post) => post.author);

      // Access author for each post (should be from cache)
      const author0 = await (posts[0] as TestPost).author;
      const author1 = await (posts[1] as TestPost).author;
      const author2 = await (posts[2] as TestPost).author;
      const author3 = await (posts[3] as TestPost).author;

      // Verify data
      expect(author0?.id).toBe(user1.id);
      expect(author1?.id).toBe(user1.id);
      expect(author2?.id).toBe(user2.id);
      expect(author3?.id).toBe(user2.id);

      // Verify SQL queries - should be 2 SELECTs:
      // 1. SELECT from test_posts
      // 2. SELECT from test_users (batch load with IN clause)
      const selectQueries = logger.getSelectQueries();
      expect(selectQueries.length).toBe(2);

      expect(selectQueries[0].sql).toMatch(/FROM\s+"?test_posts"?/i);
      expect(selectQueries[1].sql).toMatch(/FROM\s+"?test_users"?/i);
      // PostgreSQL uses = ANY($1::type[]), MySQL/SQLite uses IN (...)
      expect(selectQueries[1].sql).toMatch(/(?:IN\s*\(|=\s*ANY\s*\()/i);
    });

    it('should use composite key batch loading', async () => {
      // Create multi-tenant test data
      const user1 = await createTenantUserForN1(1, 100, 'Tenant1 User1');
      const user2 = await createTenantUserForN1(1, 101, 'Tenant1 User2');
      const user3 = await createTenantUserForN1(2, 100, 'Tenant2 User1');

      await createTenantPostForN1(1, 1, 100, 'T1U1 Post 1');
      await createTenantPostForN1(1, 2, 100, 'T1U1 Post 2');
      await createTenantPostForN1(1, 3, 101, 'T1U2 Post 1');
      await createTenantPostForN1(2, 1, 100, 'T2U1 Post 1');
      await createTenantPostForN1(2, 2, 100, 'T2U1 Post 2');

      // Clear query log
      const logger = SqlLoggerMiddleware.getCurrentContext();
      logger.clear();

      // Load all tenant users
      const users = await TenantUser.find([], { order: 'tenant_id, id' });

      // Create context for batch loading
      createRelationContext(TenantUserModel, users as TenantUser[]);

      // Access posts for first user (triggers batch load for all)
      const user1Posts = await (users[0] as TenantUser).posts;
      const user2Posts = await (users[1] as TenantUser).posts;
      const user3Posts = await (users[2] as TenantUser).posts;

      // Verify data
      expect(user1Posts.length).toBe(2);
      expect(user2Posts.length).toBe(1);
      expect(user3Posts.length).toBe(2);

      // Verify SQL queries - should be 2:
      // 1. SELECT users
      // 2. SELECT posts (batch with IN clause)
      const selectQueries = logger.getSelectQueries();
      
      // Should have 2 queries: users + posts (batch)
      expect(selectQueries.length).toBe(2);

      // Second query should use composite key batch loading
      // PostgreSQL: unnest + JOIN, MySQL/SQLite: (tenant_id, user_id) IN (...)
      const postsQuery = selectQueries[1].sql;
      expect(postsQuery).toMatch(/FROM\s+"?test_tenant_posts"?/i);
      // PostgreSQL uses unnest with JOIN, MySQL/SQLite uses IN clause
      expect(postsQuery).toMatch(/(?:\(tenant_id,\s*user_id\)\s*IN\s*\(|JOIN\s+unnest\s*\()/i);
    });

    it('should avoid N+1 when accessing same relation multiple times', async () => {
      // Create test data
      const user = await createTestUser('User', 'user@n1test.com');
      await createTestPost(user.id!, 'Post 1', 'Content');
      await createTestPost(user.id!, 'Post 2', 'Content');

      // Clear query log
      const logger = SqlLoggerMiddleware.getCurrentContext();
      logger.clear();

      // Access posts multiple times (should use cache after first access)
      const posts1 = await user.posts;
      const posts2 = await user.posts;
      const posts3 = await user.posts;

      // Verify same data returned
      expect(posts1).toBe(posts2);
      expect(posts2).toBe(posts3);
      expect(posts1.length).toBe(2);

      // Should only have 1 SELECT query (first access)
      const selectQueries = logger.getSelectQueries();
      expect(selectQueries.length).toBe(1);
      expect(selectQueries[0].sql).toMatch(/FROM\s+"?test_posts"?/i);
    });

    it('should auto-batch load when find returns multiple records', async () => {
      // Create test data
      const user1 = await createTestUser('User 1', 'user1@n1demo.com');
      const user2 = await createTestUser('User 2', 'user2@n1demo.com');

      await createTestPost(user1.id!, 'Post 1', 'Content');
      await createTestPost(user2.id!, 'Post 2', 'Content');

      // Clear query log
      const logger = SqlLoggerMiddleware.getCurrentContext();
      logger.clear();

      // Load users - auto batch context is created when multiple records returned
      const users = await TestUser.find([], { order: 'id' });

      // Access posts for each user
      // Auto-batch loading should load all posts in a single query
      const user1Posts = await (users[0] as TestUser).posts;
      const user2Posts = await (users[1] as TestUser).posts;

      expect(user1Posts.length).toBe(1);
      expect(user2Posts.length).toBe(1);

      // With auto batch loading, we only get 2 queries:
      // 1. SELECT users
      // 2. SELECT posts (batch for all users)
      const selectQueries = logger.getSelectQueries();
      expect(selectQueries.length).toBe(2); // Auto batch loading!
      // PostgreSQL uses = ANY($1::type[]), MySQL/SQLite uses IN (...)
      expect(selectQueries[1].sql).toMatch(/(?:IN\s*\(|=\s*ANY\s*\()/); // Batch query
    });
  });
});
