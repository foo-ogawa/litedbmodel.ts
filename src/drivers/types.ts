/**
 * litedbmodel - Database Driver Types
 *
 * Abstract interface for database drivers.
 * Implement this interface to support different database engines.
 */

/**
 * Database configuration
 * @internal
 */
export interface DBConfig {
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
  /** Maximum pool size */
  max?: number;
  /** Connection timeout in seconds */
  timeout?: number;
  /** Query timeout in seconds */
  queryTimeout?: number;
}

/**
 * Logger interface
 * @internal
 */
export interface Logger {
  debug: (message: string, ...args: unknown[]) => void;
  info: (message: string, ...args: unknown[]) => void;
  warn: (message: string, ...args: unknown[]) => void;
  error: (message: string, ...args: unknown[]) => void;
}

/**
 * Query result interface
 * @internal
 */
export interface QueryResult {
  rows: Record<string, unknown>[];
  rowCount: number;
}

/**
 * Database connection interface
 * Represents a single connection (used in transactions)
 * @internal
 */
export interface DBConnection {
  /** Execute a query on this connection */
  query(sql: string, params?: unknown[]): Promise<QueryResult>;
  /** Release this connection back to the pool */
  release(): void;
}

/**
 * Database driver interface
 * Implement this to support a new database engine
 * @internal
 */
export interface DBDriver {
  /** Driver name (e.g., 'postgres', 'sqlite') */
  readonly name: string;

  /**
   * Execute a SQL query
   * @param sql - SQL query string
   * @param params - Query parameters
   * @returns Query result
   */
  execute(sql: string, params?: unknown[]): Promise<QueryResult>;

  /**
   * Execute a write query (INSERT/UPDATE/DELETE)
   * May use a different connection pool for write operations
   */
  executeWrite(sql: string, params?: unknown[]): Promise<QueryResult>;

  /**
   * Get a connection from the pool (for transactions)
   */
  getConnection(): Promise<DBConnection>;

  /**
   * Close all connections
   */
  close(): Promise<void>;

  /**
   * Set logger
   */
  setLogger(logger: Logger): void;
}

/**
 * Database driver constructor options
 * @internal
 */
export interface DBDriverOptions {
  /** Configuration for read operations */
  config: DBConfig;
  /** Optional separate configuration for write operations */
  writerConfig?: DBConfig;
  /** Logger instance */
  logger?: Logger;
}

/**
 * Default no-op logger
 * @internal
 */
export const defaultLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: console.warn,
  error: console.error,
};

// ============================================
// Type Cast Interface for Drivers
// ============================================

/**
 * Interface for driver-specific type casting.
 * Drivers implement this to handle serialization/deserialization of complex types.
 * @internal
 */
export interface DriverTypeCast {
  /**
   * Driver name for identification
   * Used by serialize functions to apply driver-specific formatting
   */
  readonly driverName: 'postgres' | 'mysql' | 'sqlite';

  /**
   * Serialize array to DB format
   * PostgreSQL: '{1,2,3}' native format
   * MySQL/SQLite: '[1,2,3]' JSON format
   */
  serializeArray<T>(val: T[]): unknown;

  /**
   * Deserialize DB value to array
   * PostgreSQL: native array (already converted by driver)
   * MySQL/SQLite: JSON string -> array
   */
  deserializeArray<T>(val: unknown): T[] | null;

  /**
   * Serialize JSON to DB format
   */
  serializeJson(val: unknown): unknown;

  /**
   * Deserialize DB value to JSON
   */
  deserializeJson<T>(val: unknown): T | null;

  /**
   * Serialize boolean array to DB format
   */
  serializeBooleanArray(val: (boolean | null)[]): unknown;

  /**
   * Deserialize DB value to boolean array
   */
  deserializeBooleanArray(val: unknown): (boolean | null)[] | null;

  /**
   * Serialize Date to DB format for datetime/timestamp columns
   * PostgreSQL: ISO 8601 UTC string for explicit timezone
   * MySQL/SQLite: Date object (driver handles conversion)
   */
  serializeDatetime(val: Date): unknown;

  /**
   * Serialize Date to DB format for date-only columns
   * PostgreSQL: UTC date string (YYYY-MM-DD)
   * MySQL/SQLite: Date object (driver handles conversion)
   */
  serializeDate(val: Date): unknown;
}

// ============================================
// SQL Builder Interface
// ============================================

/**
 * Options for building INSERT SQL
 * @internal
 */
export interface InsertBuildOptions {
  tableName: string;
  columns: string[];
  records: Record<string, unknown>[];
  /** Raw records before serialization (for PostgreSQL UNNEST with arrays) */
  rawRecords?: Record<string, unknown>[];
  /** Map of column name to SQL cast type */
  sqlCastMap?: Map<string, string>;
  /** ON CONFLICT columns */
  onConflict?: string[];
  /** ON CONFLICT DO NOTHING */
  onConflictIgnore?: boolean;
  /** ON CONFLICT DO UPDATE columns ('all' or specific columns) */
  onConflictUpdate?: 'all' | string[];
  /** RETURNING clause */
  returning?: string;
}

/**
 * Options for building batch UPDATE SQL
 * @internal
 */
export interface UpdateManyBuildOptions {
  tableName: string;
  keyColumns: string[];
  updateColumns: string[];
  records: Record<string, unknown>[];
  /** Raw records before serialization */
  rawRecords?: Record<string, unknown>[];
  /** Map of column name to SQL cast type */
  sqlCastMap?: Map<string, string>;
  /** Map of record index to set of SKIP column names */
  skipMap?: Map<number, Set<string>>;
  /** RETURNING clause */
  returning?: string;
}

/**
 * Result of SQL building
 * @internal
 */
export interface SqlBuildResult {
  sql: string;
  params: unknown[];
}

/**
 * Options for building SELECT to get affected PKs (for drivers without RETURNING)
 * @internal
 */
export interface SelectPkeysOptions {
  tableName: string;
  pkeyColumns: string[];
  keyColumns: string[];
  keyValues: unknown[][];
}

/**
 * Options for building findByPkeys query
 * @internal
 */
export interface FindByPkeysOptions {
  tableName: string;
  pkeyColumns: string[];
  pkeyValues: unknown[][];
  selectColumn: string;
  sqlCastMap?: Map<string, string>;
}

/**
 * Interface for driver-specific SQL building.
 * Each driver implements this to generate optimized SQL.
 * @internal
 */
export interface SqlBuilder {
  /** Driver type identifier */
  readonly driverType: 'postgres' | 'sqlite' | 'mysql';

  /** Type cast helper for this driver */
  readonly typeCast: DriverTypeCast;

  /** Whether this driver supports RETURNING clause natively */
  readonly supportsReturning: boolean;

  /**
   * Build INSERT SQL for single or multiple records
   */
  buildInsert(options: InsertBuildOptions): SqlBuildResult;

  /**
   * Build batch UPDATE SQL for multiple records
   */
  buildUpdateMany(options: UpdateManyBuildOptions): SqlBuildResult;

  /**
   * Build SELECT to get affected PKs (for drivers without native RETURNING)
   * Only called if supportsReturning is false
   */
  buildSelectPkeys?(options: SelectPkeysOptions): SqlBuildResult;

  /**
   * Build SELECT for findById with multiple primary keys
   */
  buildFindByPkeys(options: FindByPkeysOptions): SqlBuildResult;

  /**
   * Build RETURNING clause for UPDATE/DELETE
   * @param tableName - Table name
   * @param columns - Column names to return
   * @param alias - Table alias (e.g., 't' for UPDATE ... AS t)
   * @returns Formatted RETURNING clause or undefined if not supported
   */
  buildReturning(tableName: string, columns: string[], alias?: string): string | undefined;

  /**
   * Infer PostgreSQL type from JavaScript value
   * Returns null for non-PostgreSQL drivers
   */
  inferPgType?(val: unknown): string;
}

