/**
 * Test setup and utilities
 */

import 'reflect-metadata';
import { DBModel, closeAllPools, model, column } from '../../src';
import type { DBConfig, ColumnsOf } from '../../src';

// ============================================
// Test Database Configuration
// ============================================

export const testConfig: DBConfig = {
  host: process.env.TEST_DB_HOST || 'localhost',
  port: parseInt(process.env.TEST_DB_PORT || '5433', 10),
  database: process.env.TEST_DB_NAME || 'testdb',
  user: process.env.TEST_DB_USER || 'testuser',
  password: process.env.TEST_DB_PASSWORD || 'testpass',
  max: 5,
};

export const testConnectionString = `postgresql://${testConfig.user}:${testConfig.password}@${testConfig.host}:${testConfig.port}/${testConfig.database}`;

// ============================================
// Test Model: User
// ============================================

@model('users')
class UserModel extends DBModel {
  static DEFAULT_ORDER = 'id ASC';

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
  static DEFAULT_ORDER = 'id ASC';

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
