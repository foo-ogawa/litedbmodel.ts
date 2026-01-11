/**
 * Middleware Hooks Integration Tests
 *
 * Verifies that each DBModel method triggers the correct middleware hooks.
 * 
 * Hook flow per method (verified):
 * - find: find → query → execute
 * - findOne: findOne → query → execute
 * - findById: findById → execute (goes directly to execute, doesn't use query)
 * - count: count → execute
 * - create: create → execute
 * - createMany: createMany → execute
 * - update: update → execute
 * - updateMany: updateMany → execute (single batch SQL in SQLite)
 * - delete: delete → execute
 * - query: query → execute
 * - execute: execute
 * - belongsTo loading: query → execute (bypasses method-level hooks)
 * - hasMany loading: query → execute (bypasses method-level hooks)
 * - hasOne loading: query → execute (bypasses method-level hooks)
 */

import 'reflect-metadata';
import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import {
  DBModel,
  model,
  column,
  hasMany,
  belongsTo,
  hasOne,
  createMiddleware,
  createRelationContext,
  closeAllPools,
  type ColumnsOf,
  type DBConfig,
} from '../../src';
import * as fs from 'fs';
import * as path from 'path';

// ============================================
// Hook Tracking Middleware
// ============================================

const HookTracker = DBModel.createMiddleware({
  state: {
    calls: [] as string[],
  },

  find: async function(model, next, conditions, options) {
    this.calls.push('find');
    return next(conditions, options);
  },

  findOne: async function(model, next, conditions, options) {
    this.calls.push('findOne');
    return next(conditions, options);
  },

  findById: async function(model, next, id, options) {
    this.calls.push('findById');
    return next(id, options);
  },

  count: async function(model, next, conditions) {
    this.calls.push('count');
    return next(conditions);
  },

  create: async function(model, next, pairs, options) {
    this.calls.push('create');
    return next(pairs, options);
  },

  createMany: async function(model, next, pairsArray, options) {
    this.calls.push('createMany');
    return next(pairsArray, options);
  },

  update: async function(model, next, conditions, values, options) {
    this.calls.push('update');
    return next(conditions, values, options);
  },

  updateMany: async function(model, next, records, options) {
    this.calls.push('updateMany');
    return next(records, options);
  },

  delete: async function(model, next, conditions, options) {
    this.calls.push('delete');
    return next(conditions, options);
  },

  query: async function(model, next, sql, params) {
    this.calls.push('query');
    return next(sql, params);
  },

  execute: async function(next, sql, params) {
    this.calls.push('execute');
    return next(sql, params);
  },
});

// ============================================
// Test Configuration
// ============================================

const testDbPath = path.join(__dirname, '../fixtures/middleware_hooks_test.sqlite');

const testConfig: DBConfig = {
  database: testDbPath,
  driver: 'sqlite',
  requireTransaction: false,  // Disable for tests
};

// ============================================
// Test Models
// ============================================

// Forward declarations for circular references
let User: typeof UserModel & ColumnsOf<UserModel>;
let Post: typeof PostModel & ColumnsOf<PostModel>;
let Profile: typeof ProfileModel & ColumnsOf<ProfileModel>;

@model('mw_users')
class UserModel extends DBModel {
  @column({ primaryKey: true }) id?: number;
  @column() name?: string;

  @hasMany(() => [User.id, Post.user_id])
  declare posts: Promise<(typeof PostModel)[]>;

  @hasOne(() => [User.id, Profile.user_id])
  declare profile: Promise<typeof ProfileModel | null>;
}
User = UserModel as typeof UserModel & ColumnsOf<UserModel>;

@model('mw_posts')
class PostModel extends DBModel {
  @column({ primaryKey: true }) id?: number;
  @column() user_id?: number;
  @column() title?: string;

  @belongsTo(() => [Post.user_id, User.id])
  declare author: Promise<typeof UserModel | null>;
}
Post = PostModel as typeof PostModel & ColumnsOf<PostModel>;

@model('mw_profiles')
class ProfileModel extends DBModel {
  @column({ primaryKey: true }) id?: number;
  @column() user_id?: number;
  @column() bio?: string;

  @belongsTo(() => [Profile.user_id, User.id])
  declare user: Promise<typeof UserModel | null>;
}
Profile = ProfileModel as typeof ProfileModel & ColumnsOf<ProfileModel>;

// ============================================
// Tests
// ============================================

describe('Middleware Hook Verification', () => {
  let unregister: () => void;

  beforeAll(async () => {
    // Clean up existing test database
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }

    // Initialize DBModel with SQLite config
    DBModel.setConfig(testConfig);

    // Register the hook tracker middleware
    unregister = DBModel.use(HookTracker);

    // Create tables
    await DBModel.execute(`
      CREATE TABLE IF NOT EXISTS mw_users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT
      )
    `);
    await DBModel.execute(`
      CREATE TABLE IF NOT EXISTS mw_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        title TEXT
      )
    `);
    await DBModel.execute(`
      CREATE TABLE IF NOT EXISTS mw_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        bio TEXT
      )
    `);

    // Clear hook calls from setup
    HookTracker.getCurrentContext().calls = [];
  });

  afterAll(async () => {
    unregister();
    await closeAllPools();
    if (fs.existsSync(testDbPath)) {
      fs.unlinkSync(testDbPath);
    }
  });

  beforeEach(() => {
    // Clear hook calls before each test
    HookTracker.getCurrentContext().calls = [];
  });

  // Helper to get and clear calls
  function getCalls(): string[] {
    const calls = [...HookTracker.getCurrentContext().calls];
    HookTracker.getCurrentContext().calls = [];
    return calls;
  }

  describe('Method-level hooks → query/execute flow', () => {
    it('find: should call find → query → execute', async () => {
      await User.find([]);
      expect(getCalls()).toEqual(['find', 'query', 'execute']);
    });

    it('findOne: should call findOne → query → execute', async () => {
      await User.findOne([]);
      expect(getCalls()).toEqual(['findOne', 'query', 'execute']);
    });

    it('findById: should call findById → execute', async () => {
      // First create a user to find
      await DBModel.transaction(async () => {
        await User.create([[User.name, 'Test']]);
      });
      getCalls(); // Clear create calls
      
      // findById expects { values: [[pk1], [pk2], ...] } format
      await User.findById({ values: [[1]] });
      // findById goes directly to execute (doesn't use query middleware)
      expect(getCalls()).toEqual(['findById', 'execute']);
    });

    it('count: should call count → execute', async () => {
      await User.count([]);
      expect(getCalls()).toEqual(['count', 'execute']);
    });

    it('create: should call create → execute', async () => {
      await DBModel.transaction(async () => {
        await User.create([[User.name, 'Test User']]);
      });
      expect(getCalls()).toEqual(['create', 'execute']);
    });

    it('createMany: should call createMany → execute', async () => {
      await DBModel.transaction(async () => {
        await User.createMany([
          [[User.name, 'User 1']],
          [[User.name, 'User 2']],
        ]);
      });
      expect(getCalls()).toEqual(['createMany', 'execute']);
    });

    it('update: should call update → execute', async () => {
      await DBModel.transaction(async () => {
        await User.update([[User.id, 1]], [[User.name, 'Updated']]);
      });
      expect(getCalls()).toEqual(['update', 'execute']);
    });

    it('updateMany: should call updateMany → execute', async () => {
      await DBModel.transaction(async () => {
        // updateMany uses batch update (single SQL statement in SQLite)
        await User.updateMany([
          [[User.id, 1], [User.name, 'User A']],
          [[User.id, 2], [User.name, 'User B']],
        ], { keyColumns: User.id });
      });
      // updateMany hook is called once, then a single execute (batch update)
      expect(getCalls()).toEqual(['updateMany', 'execute']);
    });

    it('delete: should call delete → execute', async () => {
      await DBModel.transaction(async () => {
        await User.delete([[User.id, 999]]);
      });
      expect(getCalls()).toEqual(['delete', 'execute']);
    });
  });

  describe('Raw SQL methods', () => {
    it('query: should call query → execute', async () => {
      await User.query('SELECT * FROM mw_users WHERE 1=0');
      expect(getCalls()).toEqual(['query', 'execute']);
    });

    it('execute: should call only execute', async () => {
      await DBModel.execute('SELECT 1');
      expect(getCalls()).toEqual(['execute']);
    });
  });

  describe('Relation loading (bypasses method-level, uses query)', () => {
    let userId: number;
    let postId: number;

    beforeAll(async () => {
      // Clear and create test data in a transaction
      HookTracker.getCurrentContext().calls = [];
      
      await DBModel.transaction(async () => {
        await DBModel.execute('DELETE FROM mw_posts');
        await DBModel.execute('DELETE FROM mw_profiles');
        await DBModel.execute('DELETE FROM mw_users');
        
        // Use returning: true to get the inserted ID
        const userResult = await User.create([[User.name, 'Relation Test User']], { returning: true });
        userId = (userResult!.values as unknown[][])[0][0] as number;
        
        const postResult = await Post.create([
          [Post.user_id, userId],
          [Post.title, 'Test Post'],
        ], { returning: true });
        postId = (postResult!.values as unknown[][])[0][0] as number;
        
        await Profile.create([
          [Profile.user_id, userId],
          [Profile.bio, 'Test bio'],
        ]);
      });
      
      // Clear setup calls
      HookTracker.getCurrentContext().calls = [];
    });

    it('belongsTo loading: should call query → execute', async () => {
      // First fetch the post
      const post = await Post.findOne([[Post.id, postId]]);
      getCalls(); // Clear find calls
      
      // Create relation context with the fetched posts
      createRelationContext(PostModel, [post!]);
      
      // Load the relation
      await post!.author;
      expect(getCalls()).toEqual(['query', 'execute']);
    });

    it('hasMany loading: should call query → execute', async () => {
      // First fetch the user
      const user = await User.findOne([[User.id, userId]]);
      getCalls(); // Clear find calls
      
      // Create relation context with the fetched users
      createRelationContext(UserModel, [user!]);
      
      // Load the relation
      await user!.posts;
      expect(getCalls()).toEqual(['query', 'execute']);
    });

    it('hasOne loading: should call query → execute', async () => {
      // First fetch the user
      const user = await User.findOne([[User.id, userId]]);
      getCalls(); // Clear find calls
      
      // Create relation context with the fetched users
      createRelationContext(UserModel, [user!]);
      
      // Load the relation
      await user!.profile;
      expect(getCalls()).toEqual(['query', 'execute']);
    });
  });

  describe('Multiple middlewares chain correctly', () => {
    it('should call hooks in registration order', async () => {
      const order: string[] = [];
      
      const MW1 = DBModel.createMiddleware({
        execute: async function(next, sql, params) {
          order.push('MW1:before');
          const result = await next(sql, params);
          order.push('MW1:after');
          return result;
        }
      });
      
      const MW2 = DBModel.createMiddleware({
        execute: async function(next, sql, params) {
          order.push('MW2:before');
          const result = await next(sql, params);
          order.push('MW2:after');
          return result;
        }
      });
      
      const unregister1 = DBModel.use(MW1);
      const unregister2 = DBModel.use(MW2);
      
      try {
        await DBModel.execute('SELECT 1');
        // HookTracker is first, then MW1, then MW2
        // Execution order: HookTracker → MW1 → MW2 → actual execution → MW2 → MW1 → HookTracker
        expect(order).toEqual(['MW1:before', 'MW2:before', 'MW2:after', 'MW1:after']);
      } finally {
        unregister1();
        unregister2();
      }
    });
  });
});

