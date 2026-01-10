/**
 * Test setup and utilities
 */

import 'reflect-metadata';
import { DBModel, closeAllPools, model, column } from '../../src';
import type { DBConfig, ColumnsOf } from '../../src';

// ============================================
// Skip flag for integration tests
// ============================================

export const skipIntegrationTests = process.env.SKIP_INTEGRATION_TESTS === '1';

// ============================================
// Test Database Configurations
// ============================================

// PostgreSQL
export const pgConfig: DBConfig = {
  host: process.env.TEST_DB_HOST || 'localhost',
  port: parseInt(process.env.TEST_DB_PORT || '5433', 10),
  database: process.env.TEST_DB_NAME || 'testdb',
  user: process.env.TEST_DB_USER || 'testuser',
  password: process.env.TEST_DB_PASSWORD || 'testpass',
  max: 5,
};

// Alias for backward compatibility
export const testConfig = pgConfig;

export const testConnectionString = `postgresql://${pgConfig.user}:${pgConfig.password}@${pgConfig.host}:${pgConfig.port}/${pgConfig.database}`;

// MySQL
export const mysqlConfig: DBConfig = {
  host: process.env.TEST_MYSQL_HOST || 'localhost',
  port: parseInt(process.env.TEST_MYSQL_PORT || '3307', 10),
  database: process.env.TEST_MYSQL_DB || 'testdb',
  user: process.env.TEST_MYSQL_USER || 'testuser',
  password: process.env.TEST_MYSQL_PASSWORD || 'testpass',
  driver: 'mysql',
  max: 5,
};

// SQLite (in-memory for tests)
export const sqliteConfig: DBConfig = {
  database: ':memory:',
  driver: 'sqlite',
};

// ============================================
// Database Base Classes (for multi-DB tests)
// ============================================

let _pgBase: typeof DBModel | null = null;
let _mysqlBase: typeof DBModel | null = null;
let _sqliteBase: typeof DBModel | null = null;

/**
 * Get or create PostgreSQL base class
 */
export function getPgBase(): typeof DBModel {
  if (!_pgBase) {
    _pgBase = DBModel.createDBBase(pgConfig);
  }
  return _pgBase;
}

/**
 * Get or create MySQL base class
 */
export function getMysqlBase(): typeof DBModel {
  if (!_mysqlBase) {
    _mysqlBase = DBModel.createDBBase(mysqlConfig);
  }
  return _mysqlBase;
}

/**
 * Get or create SQLite base class
 */
export function getSqliteBase(): typeof DBModel {
  if (!_sqliteBase) {
    _sqliteBase = DBModel.createDBBase(sqliteConfig);
  }
  return _sqliteBase;
}

/**
 * Reset all base classes (for test isolation)
 */
export function resetBases(): void {
  _pgBase = null;
  _mysqlBase = null;
  _sqliteBase = null;
}

/**
 * Create a model class bound to a specific DB base
 */
export function bindModelToBase<T extends typeof DBModel>(
  ModelClass: T,
  Base: typeof DBModel
): T {
  Object.setPrototypeOf(ModelClass, Base);
  Object.setPrototypeOf(ModelClass.prototype, Base.prototype);
  return ModelClass;
}

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
export const User = UserModel as typeof UserModel & ColumnsOf<UserModel>;
export type User = UserModel;

// ============================================
// Test Model: Post
// ============================================

@model('posts')
class PostModel extends DBModel {
  @column() id?: number;
  @column() user_id?: number;
  @column() title?: string;
  @column() content?: string;
  @column() view_count?: number;
  @column.boolean() published?: boolean;
  @column.datetime() published_at?: Date | null;
  @column.datetime() created_at?: Date;
  @column.datetime() updated_at?: Date;
}
export const Post = PostModel as typeof PostModel & ColumnsOf<PostModel>;
export type Post = PostModel;

// ============================================
// Test Model: PostTag (Composite PK)
// ============================================

@model('post_tags')
class PostTagModel extends DBModel {
  @column({ primaryKey: true }) post_id?: number;
  @column({ primaryKey: true }) tag_id?: number;
  @column.datetime() created_at?: Date;
}
export const PostTag = PostTagModel as typeof PostTagModel & ColumnsOf<PostTagModel>;
export type PostTag = PostTagModel;

// ============================================
// Cleanup Helper
// ============================================

export async function cleanup(): Promise<void> {
  await closeAllPools();
}
