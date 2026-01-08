/**
 * litedbmodel - Type Definitions
 */

import type { Column, OrderSpec, Conds } from './Column';

// ============================================
// PkeyResult Type (Write operation return value)
// ============================================

/**
 * Result of write operations when returning: true is specified.
 * Contains primary key column(s) and their values for affected rows.
 * 
 * @example
 * ```typescript
 * // Single PK
 * { key: [User.id], values: [[1], [2], [3]] }
 * 
 * // Composite PK
 * { key: [TenantUser.tenant_id, TenantUser.id], values: [[1, 100], [1, 101]] }
 * ```
 * 
 * @category Types
 */
export interface PkeyResult {
  /** Primary key column(s) */
  key: Column[];
  /** 2D array of primary key values (each inner array is one row's PK values) */
  values: unknown[][];
}

// ============================================
// Model Options (for @model decorator)
// ============================================

/**
 * Options for the @model decorator.
 * All options use lazy evaluation (functions) to support forward references.
 * 
 * @category Types
 */
export interface ModelOptions {
  /** DEFAULT_ORDER: Returns OrderSpec for default ordering */
  order?: () => OrderSpec;

  /** FIND_FILTER: Returns Conds for automatic filtering in find() */
  filter?: () => Conds;

  /** SELECT_COLUMN: Column selection string (default: '*') */
  select?: string;

  /** UPDATE_TABLE_NAME: Table name for INSERT/UPDATE operations */
  updateTable?: string;

  /** DEFAULT_GROUP: Returns Column(s) or string for default grouping */
  group?: () => Column | Column[] | string;
}

// ============================================
// Database Configuration
// ============================================

/**
 * Database connection configuration.
 * Used in `DBConfigOptions.config` and `DBConfigOptions.writerConfig`.
 * 
 * @example
 * ```typescript
 * const config: DBConfig = {
 *   driver: 'postgres',
 *   host: 'localhost',
 *   port: 5432,
 *   database: 'mydb',
 *   user: 'user',
 *   password: 'pass',
 * };
 * ```
 */
export interface DBConfig {
  name?: string;
  /** @deprecated Use driver instead */
  type?: 'pgsql';
  /** Database driver: 'postgres' (default), 'sqlite', or 'mysql' */
  driver?: 'postgres' | 'sqlite' | 'mysql';
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
   * Can include parameters using ? placeholders.
   * @example
   * join: 'JOIN unnest(?::int[]) AS _keys(id) ON t.id = _keys.id'
   */
  join?: string;
  /**
   * Parameters for the JOIN clause (prepended to condition params).
   */
  joinParams?: unknown[];
  /**
   * CTE (Common Table Expression) to prepend to the query.
   * Used for window functions like ROW_NUMBER() or complex subqueries.
   * The SQL should use ? placeholders for parameters.
   * @example
   * cte: {
   *   name: 'ranked',
   *   sql: 'SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id) AS _rn FROM posts WHERE user_id IN (?, ?)',
   *   params: [1, 2]
   * }
   */
  cte?: {
    name: string;
    sql: string;
    params: unknown[];
  };
}

/**
 * Insert options with type-safe column references.
 * @typeParam Model - The model class for type-safe column constraints
 */
export interface InsertOptions<Model = unknown> {
  tableName?: string;
  /**
   * If true, return PkeyResult with affected primary keys.
   * If false (default), return null for better performance.
   */
  returning?: boolean;
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
  /**
   * If true, return PkeyResult with affected primary keys.
   * If false (default), return null for better performance.
   */
  returning?: boolean;
}

export interface DeleteOptions {
  tableName?: string;
  /**
   * If true, return PkeyResult with affected primary keys.
   * If false (default), return null for better performance.
   */
  returning?: boolean;
}

/**
 * Options for updateMany operation.
 */
export interface UpdateManyOptions {
  /**
   * Columns that identify each row (used in WHERE/JOIN clause).
   * Must uniquely identify rows (primary key or unique constraint).
   */
  keyColumns: Column | Column[];
  /**
   * If true, return PkeyResult with affected primary keys.
   * If false (default), return null for better performance.
   */
  returning?: boolean;
}

// ============================================
// Internal Options (with raw RETURNING string)
// ============================================

/** @internal */
export interface InternalInsertOptions<Model = unknown> extends Omit<InsertOptions<Model>, 'returning'> {
  returning?: string;
}

/** @internal */
export interface InternalUpdateOptions extends Omit<UpdateOptions, 'returning'> {
  returning?: string;
}

/** @internal */
export interface InternalDeleteOptions extends Omit<DeleteOptions, 'returning'> {
  returning?: string;
}

export interface TransactionOptions {
  retryOnError?: boolean;
  retryLimit?: number;
  retryDuration?: number;
  /** If true, always rollback instead of commit (useful for preview/dry-run) */
  rollbackOnly?: boolean;
  /** 
   * Override global useWriterAfterTransaction for this transaction.
   * If true, subsequent reads will use writer connection for writerStickyDuration.
   * @default Uses global setting (true by default)
   */
  useWriterAfterTransaction?: boolean;
}

// ============================================
// Database Configuration Options
// ============================================

/**
 * Options for database configuration.
 * Used with DBModel.setConfig() and DBModel.createDBBase().
 * 
 * @category Types
 */
export interface DBConfigOptions {
  /** Writer database configuration for reader/writer separation */
  writerConfig?: DBConfig;
  /** Logger instance */
  logger?: Logger;
  /** Hard limit for find() - throws if exceeded */
  findHardLimit?: number | null;
  /** Hard limit for hasMany lazy loading - throws if exceeded */
  hasManyHardLimit?: number | null;
  /**
   * Keep using writer connection after transaction completes.
   * Helps avoid stale reads due to replication lag.
   * @default true
   */
  useWriterAfterTransaction?: boolean;
  /**
   * Duration (in milliseconds) to keep using writer after transaction.
   * Only applies when useWriterAfterTransaction is true.
   * @default 5000
   */
  writerStickyDuration?: number;
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

/** @internal */
export interface Logger {
  debug(message: string, ...args: unknown[]): void;
  info(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}

// ============================================
// Limit Configuration
// ============================================

/**
 * Configuration for query result limits.
 * Used to prevent accidentally loading too many records.
 * 
 * @category Types
 */
export interface LimitConfig {
  /**
   * Hard limit for find() queries.
   * If a query returns more than this many records, an exception is thrown.
   * Set to null to disable.
   * @default null (no limit)
   */
  findHardLimit?: number | null;

  /**
   * Hard limit for hasMany relation loading (batch total).
   * If a hasMany batch load returns more than this many records in total,
   * an exception is thrown.
   * Set to null to disable.
   * @default null (no limit)
   */
  hasManyHardLimit?: number | null;
}

/**
 * Error thrown when a query exceeds the configured limit.
 * 
 * @category Errors
 */
export class LimitExceededError extends Error {
  constructor(
    public readonly limit: number,
    /** 
     * Number of records returned. For find() with findHardLimit, this is limit+1 
     * (actual total may be higher). For relation loading, this is the exact count.
     */
    public readonly actualCount: number,
    public readonly context: 'find' | 'relation',
    public readonly modelName?: string,
    public readonly relationName?: string
  ) {
    const contextMsg = context === 'find'
      ? `find() on ${modelName || 'unknown'}`
      : `relation '${relationName}' on ${modelName || 'unknown'}`;
    const countMsg = context === 'find'
      ? `more than ${limit}`  // find() uses LIMIT N+1, so we only know it exceeded
      : `${actualCount}`;     // relation loading fetches N+1, so we know at least this many
    super(
      `Query limit exceeded: ${contextMsg} returned ${countMsg} records, ` +
      `but limit is ${limit}. This usually indicates a missing WHERE clause or ` +
      `an N+1 query pattern. Set a higher limit or use pagination.`
    );
    this.name = 'LimitExceededError';
  }
}

/**
 * Error thrown when attempting write operations outside a transaction.
 * 
 * @category Errors
 */
export class WriteOutsideTransactionError extends Error {
  constructor(
    public readonly operation: string,
    public readonly modelName?: string
  ) {
    super(
      `Write operation "${operation}"${modelName ? ` on ${modelName}` : ''} requires a transaction. ` +
      `Use DBModel.transaction() to wrap write operations.`
    );
    this.name = 'WriteOutsideTransactionError';
  }
}

/**
 * Error thrown when attempting write operations inside withWriter() context.
 * 
 * @category Errors
 */
export class WriteInReadOnlyContextError extends Error {
  constructor(
    public readonly operation: string,
    public readonly modelName?: string
  ) {
    super(
      `Write operation "${operation}"${modelName ? ` on ${modelName}` : ''} is not allowed in withWriter() context. ` +
      `Use DBModel.transaction() for write operations.`
    );
    this.name = 'WriteInReadOnlyContextError';
  }
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
