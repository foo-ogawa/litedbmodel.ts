/**
 * litedbmodel - Database Driver Types
 *
 * Abstract interface for database drivers.
 * Implement this interface to support different database engines.
 */

/**
 * Database configuration
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
  query_timeout?: number;
}

/**
 * Logger interface
 */
export interface Logger {
  debug: (message: string, ...args: unknown[]) => void;
  info: (message: string, ...args: unknown[]) => void;
  warn: (message: string, ...args: unknown[]) => void;
  error: (message: string, ...args: unknown[]) => void;
}

/**
 * Query result interface
 */
export interface QueryResult {
  rows: Record<string, unknown>[];
  rowCount: number;
}

/**
 * Database connection interface
 * Represents a single connection (used in transactions)
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
 */
export const defaultLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: console.warn,
  error: console.error,
};

