/**
 * litedbmodel - Database Handler
 *
 * Database engine abstraction layer.
 * This module provides the interface between DBModel and database drivers.
 *
 * Responsibilities:
 * - Driver management
 * - Global configuration
 * - Transaction support
 *
 * Supported drivers:
 * - PostgreSQL (via pg package - optional)
 * - SQLite (via better-sqlite3 package - optional)
 * - MySQL (via mysql2 package - optional)
 */

import type { DBDriver, DBConnection, QueryResult, Logger } from './drivers/types';
import { defaultLogger } from './drivers/types';
import { setTypeCastImpl, type TypeCastFunctions } from './TypeCast';

// Static imports for drivers (pg/better-sqlite3/mysql2 are still optional at runtime)
import { PostgresDriver, closeAllPools as closeAllPostgresPools } from './drivers/postgres';
import { SqliteDriver } from './drivers/sqlite';
import { MysqlDriver, closeAllMysqlPools } from './drivers/mysql';
import * as PostgresHelper from './drivers/PostgresHelper';
import * as SqliteHelper from './drivers/SqliteHelper';
import * as MysqlHelper from './drivers/MysqlHelper';

// Re-export for backward compatibility
export type { QueryResult, Logger, DBConnection };

// ============================================
// DBConfig Type (backward compatible)
// ============================================

/**
 * Database configuration
 */
export interface DBConfig {
  /** Database host (for server-based DBs) */
  host?: string;
  /** Database port */
  port?: number;
  /** Database name or file path */
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
  /** Driver type: 'postgres' (default), 'sqlite', or 'mysql' */
  driver?: 'postgres' | 'sqlite' | 'mysql';
}

// ============================================
// Driver Factory (Lazy Loading)
// ============================================

/**
 * Create a database driver instance based on config.
 */
function createDriver(config: DBConfig, options?: DBHandlerOptions): DBDriver {
  const driverType = config.driver || 'postgres';

  if (driverType === 'sqlite') {
    return new SqliteDriver({
      config,
      logger: options?.logger || defaultLogger,
    });
  } else if (driverType === 'mysql') {
    return new MysqlDriver({
      config,
      writerConfig: options?.writerConfig,
      logger: options?.logger || defaultLogger,
    });
  } else {
    // PostgreSQL (default)
    return new PostgresDriver({
      config,
      writerConfig: options?.writerConfig,
      logger: options?.logger || defaultLogger,
    });
  }
}

/**
 * Get type cast functions for the specified driver.
 */
function getTypeCastForDriver(driverType: 'postgres' | 'sqlite' | 'mysql'): TypeCastFunctions {
  if (driverType === 'sqlite') {
    return {
      castToDatetime: SqliteHelper.castToDatetime,
      castToBoolean: SqliteHelper.castToBoolean,
      castToIntegerArray: SqliteHelper.castToIntegerArray,
      castToNumericArray: (val: unknown) => SqliteHelper.castToIntegerArray(val).map((v: number) => v as number | null),
      castToStringArray: SqliteHelper.castToStringArray,
      castToBooleanArray: (val: unknown) => {
        const arr = SqliteHelper.castToStringArray(val);
        return arr.map((v: string) => SqliteHelper.castToBoolean(v));
      },
      castToDatetimeArray: (val: unknown) => {
        const arr = SqliteHelper.castToStringArray(val);
        return arr.map((v: string) => SqliteHelper.castToDatetime(v));
      },
      castToJson: SqliteHelper.castToJson,
    };
  } else if (driverType === 'mysql') {
    return {
      castToDatetime: MysqlHelper.castToDatetime,
      castToBoolean: MysqlHelper.castToBoolean,
      castToIntegerArray: MysqlHelper.castToIntegerArray,
      castToNumericArray: (val: unknown) => MysqlHelper.castToIntegerArray(val).map((v: number) => v as number | null),
      castToStringArray: MysqlHelper.castToStringArray,
      castToBooleanArray: (val: unknown) => {
        const arr = MysqlHelper.castToStringArray(val);
        return arr.map((v: string) => MysqlHelper.castToBoolean(v));
      },
      castToDatetimeArray: (val: unknown) => {
        const arr = MysqlHelper.castToStringArray(val);
        return arr.map((v: string) => MysqlHelper.castToDatetime(v));
      },
      castToJson: MysqlHelper.castToJson,
    };
  } else {
    // PostgreSQL (default)
    return {
      castToDatetime: PostgresHelper.castToDatetime,
      castToBoolean: PostgresHelper.castToBoolean,
      castToIntegerArray: PostgresHelper.castToIntegerArray,
      castToNumericArray: PostgresHelper.castToNumericArray,
      castToStringArray: PostgresHelper.castToStringArray,
      castToBooleanArray: PostgresHelper.castToBooleanArray,
      castToDatetimeArray: PostgresHelper.castToDatetimeArray,
      castToJson: PostgresHelper.castToJson,
    };
  }
}

// ============================================
// DBHandler Class
// ============================================

export interface DBHandlerOptions {
  writerConfig?: DBConfig;
  logger?: Logger;
  /** Existing connection for transaction */
  connection?: DBConnection;
  /** Keep using writer after transaction (default: true) */
  useWriterAfterTransaction?: boolean;
  /** Duration to keep using writer after transaction (ms, default: 5000) */
  writerStickyDuration?: number;
}

/**
 * Database handler - wraps a driver and provides a unified interface.
 *
 * @example
 * ```typescript
 * // PostgreSQL
 * const handler = new DBHandler({ host: 'localhost', port: 5432, database: 'mydb', ... });
 *
 * // SQLite
 * const handler = new DBHandler({ database: './mydb.sqlite', driver: 'sqlite' });
 *
 * const result = await handler.execute('SELECT * FROM users WHERE id = $1', [1]);
 * ```
 */
export class DBHandler {
  private driver: DBDriver;
  private connection: DBConnection | null = null;
  private logger: Logger;
  private driverType: 'postgres' | 'sqlite' | 'mysql';

  constructor(config: DBConfig, options?: DBHandlerOptions) {
    this.logger = options?.logger || defaultLogger;
    this.connection = options?.connection || null;
    this.driverType = config.driver || 'postgres';

    // Create driver
    this.driver = createDriver(config, options);

    // Set type cast implementation for this driver
    const typeCast = getTypeCastForDriver(this.driverType);
    if (typeCast) {
      setTypeCastImpl(typeCast);
    }
  }

  /**
   * Get the driver type
   */
  getDriverType(): 'postgres' | 'sqlite' | 'mysql' {
    return this.driverType;
  }

  /**
   * Get the underlying driver
   */
  getDriver(): DBDriver {
    return this.driver;
  }

  /**
   * Execute a SQL query
   */
  async execute(sql: string, params: unknown[] = []): Promise<QueryResult> {
    if (this.connection) {
      return this.connection.query(sql, params);
    }
    return this.driver.execute(sql, params);
  }

  /**
   * Execute a write query (INSERT/UPDATE/DELETE)
   */
  async executeWrite(sql: string, params: unknown[] = []): Promise<QueryResult> {
    if (this.connection) {
      return this.connection.query(sql, params);
    }
    return this.driver.executeWrite(sql, params);
  }

  /**
   * Execute a query on writer pool (for withWriter context)
   */
  async executeOnWriter(sql: string, params: unknown[] = []): Promise<QueryResult> {
    if (this.connection) {
      return this.connection.query(sql, params);
    }
    return this.driver.executeWrite(sql, params);
  }

  /**
   * Check if writer pool is configured
   */
  hasWriterPool(): boolean {
    return 'getWriterPool' in this.driver && (this.driver as { getWriterPool: () => unknown }).getWriterPool() !== null;
  }

  /**
   * Get a connection from the pool (for transactions)
   */
  async getConnection(): Promise<DBConnection> {
    return this.driver.getConnection();
  }

  /**
   * Create handler with specific connection (for transaction)
   */
  withConnection(connection: DBConnection): DBHandler {
    const handler = Object.create(DBHandler.prototype) as DBHandler;
    handler.driver = this.driver;
    handler.connection = connection;
    handler.logger = this.logger;
    handler.driverType = this.driverType;
    return handler;
  }

  /**
   * Close all connections
   */
  async close(): Promise<void> {
    return this.driver.close();
  }

  /**
   * Set logger
   */
  setLogger(logger: Logger): void {
    this.logger = logger;
    this.driver.setLogger(logger);
  }

  /**
   * Get the underlying PostgreSQL pool (for backward compatibility)
   * @deprecated Use driver-specific methods instead
   */
  getPool(): unknown {
    if (this.driverType !== 'postgres' && this.driverType !== 'mysql') {
      throw new Error('getPool() is only available for PostgreSQL and MySQL drivers');
    }
    return (this.driver as unknown as { getPool: () => unknown }).getPool();
  }
}

// ============================================
// Global Handler Singleton
// ============================================

let globalHandler: DBHandler | null = null;
let globalConfig: DBConfig | null = null;

/**
 * Initialize global handler with config
 */
export function initDBHandler(config: DBConfig, options?: DBHandlerOptions): DBHandler {
  globalConfig = config;
  globalHandler = new DBHandler(config, options);
  return globalHandler;
}

/**
 * Get global handler
 * @throws Error if not initialized
 */
export function getDBHandler(): DBHandler {
  if (!globalHandler) {
    throw new Error('DBHandler not initialized. Call initDBHandler() first.');
  }
  return globalHandler;
}

/**
 * Get global config
 */
export function getDBConfig(): DBConfig | null {
  return globalConfig;
}

/**
 * Create handler with transaction connection
 */
export function createHandlerWithConnection(connection: DBConnection): DBHandler {
  if (!globalHandler) {
    throw new Error('DBHandler not initialized. Call initDBHandler() first.');
  }
  return globalHandler.withConnection(connection);
}

/**
 * Close all connection pools
 */
export async function closeAllPools(): Promise<void> {
  if (globalHandler) {
    await globalHandler.close();
    globalHandler = null;
  }
  // Also close any remaining pools from all drivers
  await closeAllPostgresPools();
  await closeAllMysqlPools();
}

// Backward compatibility alias
export { createHandlerWithConnection as createHandlerWithClient };
