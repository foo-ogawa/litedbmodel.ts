/**
 * litedbmodel - Type Definitions
 */

import type { Column, OrderSpec, Conds } from './Column';

// ============================================
// Database Configuration
// ============================================

export interface DBConfig {
  name?: string;
  /** @deprecated Use driver instead */
  type?: 'pgsql';
  /** Database driver: 'postgres' (default) or 'sqlite' */
  driver?: 'postgres' | 'sqlite';
  /** Database host (for server-based DBs like PostgreSQL) */
  host?: string;
  /** Database port */
  port?: number;
  /** Database name or file path (for SQLite) */
  database: string;
  /** Username */
  user?: string;
  /** Password */
  password?: string;
  charset?: string;
  timeout?: number;
  queryTimeout?: number;
  searchPath?: string;
  /** Pool max connections */
  max?: number;
}

// ============================================
// Query Options
// ============================================

export interface SelectOptions {
  /** Order by clause. Accepts OrderSpec (Column.asc()/desc()) or raw string. */
  order?: import('./Column').OrderSpec | string;
  limit?: number;
  offset?: number;
  select?: string;
  group?: string;
  tableName?: string;
  append?: string;
  forUpdate?: boolean;
  /**
   * JOIN clause to add to the query.
   * Can include parameters using $N placeholders.
   * @example
   * join: 'JOIN unnest($1::int[]) AS _keys(id) ON t.id = _keys.id'
   */
  join?: string;
  /**
   * Parameters for the JOIN clause (prepended to condition params).
   */
  joinParams?: unknown[];
}

/**
 * Insert options with type-safe column references.
 * @typeParam Model - The model class for type-safe column constraints
 */
export interface InsertOptions<Model = unknown> {
  tableName?: string;
  returning?: string;
  /** @deprecated Use onConflict instead */
  conflict?: string;
  /**
   * Columns for ON CONFLICT clause (unique constraint columns).
   * Must be Column symbols from the same model for type safety.
   * @example
   * // Single column
   * onConflict: User.email
   * // Multiple columns (composite unique constraint)
   * onConflict: [UserPref.user_id, UserPref.key]
   */
  onConflict?: Column<unknown, Model> | Column<unknown, Model>[];
  /**
   * Columns to update on conflict.
   * Can be:
   * - 'all': Update all inserted columns
   * - Array of Column symbols from the same model
   * @example
   * // Update all columns
   * onConflictUpdate: 'all'
   * // Update specific columns
   * onConflictUpdate: [User.name, User.updated_at]
   */
  onConflictUpdate?: 'all' | Column<unknown, Model>[];
  /**
   * If true, ignore the insert on conflict (DO NOTHING).
   * Cannot be used with onConflictUpdate.
   */
  onConflictIgnore?: boolean;
}

export interface UpdateOptions {
  tableName?: string;
  returning?: string;
}

export interface DeleteOptions {
  tableName?: string;
  returning?: string;
}

export interface TransactionOptions {
  retryOnError?: boolean;
  retryLimit?: number;
  retryDuration?: number;
  /** If true, always rollback instead of commit (useful for preview/dry-run) */
  rollbackOnly?: boolean;
}

// ============================================
// Query Result Types
// ============================================

export interface QueryExecuteResult {
  rows: Record<string, unknown>[];
  rowCount: number;
  command: string;
}

// ============================================
// Logger Interface
// ============================================

export interface Logger {
  debug(message: string, ...args: unknown[]): void;
  info(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}

// ============================================
// Column Definition Types
// ============================================

/**
 * ColumnDefs type - Maps property names to column name strings
 * Used for type-safe column references
 */
export type ColumnDefs<T> = {
  readonly [K in keyof T as T[K] extends Function ? never : K]: K & string;
};

// ============================================
// Condition Types (re-exported from DBConditions)
// ============================================

// Note: ConditionValue and ConditionObject are defined in DBConditions.ts
// and exported from index.ts

// ============================================
// Model Class Type
// ============================================

export interface DBModelStatic<T extends DBModelInstance = DBModelInstance> {
  new (): T;
  TABLE_NAME: string;
  UPDATE_TABLE_NAME: string | null;
  SELECT_COLUMN: string;
  DEFAULT_ORDER: OrderSpec | null;
  DEFAULT_GROUP: Column | Column[] | string | null;
  FIND_FILTER: Conds | null;
  PKEY_COLUMNS: Column[] | null;
  SEQ_NAME: string | null;
  ID_TYPE: 'serial' | 'uuid' | null;
  getTableName(): string;
  getUpdateTableName(): string;
  getGroupByClause(): string | null;
}

export interface DBModelInstance {
  typeCastFromDB(): void;
  getPkey(): Record<string, unknown> | null;
  setPkey(key: unknown): void;
  getPkeyString(): string;
  getSingleColId(): unknown;
}
