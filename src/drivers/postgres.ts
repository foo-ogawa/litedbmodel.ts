/**
 * litedbmodel - PostgreSQL Driver
 *
 * Database driver implementation for PostgreSQL using node-postgres (pg).
 * The pg package is dynamically loaded to make it an optional dependency.
 */

import type { DBConfig, DBDriver, DBDriverOptions, DBConnection, QueryResult, Logger } from './types';
import { defaultLogger } from './types';

// pg types (loaded dynamically)
type Pool = import('pg').Pool;
type PoolClient = import('pg').PoolClient;

// ============================================
// Connection Pool Management
// ============================================

const pools: Map<string, Pool> = new Map();
let pgModule: typeof import('pg') | null = null;

/**
 * Load pg module dynamically
 */
function getPgModule(): typeof import('pg') {
  if (!pgModule) {
    try {
      pgModule = require('pg');
    } catch (err) {
      throw new Error(
        'PostgreSQL driver requires pg package. Install it with: npm install pg'
      );
    }
  }
  return pgModule!;
}

/**
 * Get pool cache key from config
 */
function getPoolKey(config: DBConfig): string {
  return `${config.host}:${config.port}/${config.database}`;
}

/**
 * Get or create a connection pool
 */
function getPool(config: DBConfig): Pool {
  const key = getPoolKey(config);

  let pool = pools.get(key);
  if (!pool) {
    const { Pool } = getPgModule();
    pool = new Pool({
      host: config.host,
      port: config.port,
      database: config.database,
      user: config.user,
      password: config.password,
      max: config.max || 10,
      connectionTimeoutMillis: (config.timeout || 30) * 1000,
      query_timeout: (config.queryTimeout || 30) * 1000,
    });
    pools.set(key, pool);
  }

  return pool;
}

/**
 * Close a specific pool
 */
async function closePool(config: DBConfig): Promise<void> {
  const key = getPoolKey(config);
  const pool = pools.get(key);
  if (pool) {
    await pool.end();
    pools.delete(key);
  }
}

/**
 * Close all connection pools
 */
export async function closeAllPools(): Promise<void> {
  for (const pool of pools.values()) {
    await pool.end();
  }
  pools.clear();
}

/**
 * Convert ?-style placeholders to PostgreSQL-style ($1, $2, ...)
 */
function convertPlaceholders(sql: string): string {
  let index = 0;
  return sql.replace(/\?/g, () => `$${++index}`);
}

// ============================================
// PostgreSQL Connection Wrapper
// ============================================

class PostgresConnection implements DBConnection {
  constructor(private client: PoolClient) {}

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    const convertedSql = convertPlaceholders(sql);
    const result = await this.client.query(convertedSql, params);
    return {
      rows: result.rows as Record<string, unknown>[],
      rowCount: result.rowCount ?? 0,
    };
  }

  release(): void {
    this.client.release();
  }
}

// ============================================
// PostgreSQL Driver
// ============================================

/**
 * PostgreSQL database driver
 * @internal
 */
export class PostgresDriver implements DBDriver {
  readonly name = 'postgres';

  private pool: Pool;
  private writerPool: Pool | null;
  private config: DBConfig;
  private writerConfig: DBConfig | null;
  private logger: Logger;

  constructor(options: DBDriverOptions) {
    this.config = options.config;
    this.writerConfig = options.writerConfig || null;
    this.pool = getPool(options.config);
    this.writerPool = options.writerConfig ? getPool(options.writerConfig) : null;
    this.logger = options.logger || defaultLogger;
  }

  /**
   * Execute a read query
   */
  async execute(sql: string, params: unknown[] = []): Promise<QueryResult> {
    const convertedSql = convertPlaceholders(sql);
    this.logger.debug(`SQL: ${convertedSql}`, params);

    const startTime = Date.now();
    try {
      const result = await this.pool.query(convertedSql, params);
      const duration = Date.now() - startTime;
      this.logger.debug(`Query completed in ${duration}ms, rows: ${result.rowCount}`);
      return {
        rows: result.rows as Record<string, unknown>[],
        rowCount: result.rowCount ?? 0,
      };
    } catch (error) {
      this.logger.error(`Query failed: ${convertedSql}`, error);
      throw error;
    }
  }

  /**
   * Execute a write query (uses writer pool if available)
   */
  async executeWrite(sql: string, params: unknown[] = []): Promise<QueryResult> {
    const conn = this.writerPool || this.pool;
    const convertedSql = convertPlaceholders(sql);
    this.logger.debug(`SQL: ${convertedSql}`, params);

    const startTime = Date.now();
    try {
      const result = await conn.query(convertedSql, params);
      const duration = Date.now() - startTime;
      this.logger.debug(`Write completed in ${duration}ms, rows: ${result.rowCount}`);
      return {
        rows: result.rows as Record<string, unknown>[],
        rowCount: result.rowCount ?? 0,
      };
    } catch (error) {
      this.logger.error(`Write failed: ${convertedSql}`, error);
      throw error;
    }
  }

  /**
   * Get a connection for transaction (uses writer pool if available)
   */
  async getConnection(): Promise<DBConnection> {
    // Transactions always use writer pool for consistency
    const pool = this.writerPool || this.pool;
    const client = await pool.connect();
    return new PostgresConnection(client);
  }

  /**
   * Close all connections
   */
  async close(): Promise<void> {
    await closePool(this.config);
    if (this.writerConfig) {
      await closePool(this.writerConfig);
    }
  }

  /**
   * Set logger
   */
  setLogger(logger: Logger): void {
    this.logger = logger;
  }

  /**
   * Get the underlying pool (for backward compatibility)
   */
  getPool(): Pool {
    return this.pool;
  }

  /**
   * Get the underlying writer pool
   */
  getWriterPool(): Pool | null {
    return this.writerPool;
  }
}

/**
 * Create a PostgreSQL driver instance
 */
export function createPostgresDriver(options: DBDriverOptions): PostgresDriver {
  return new PostgresDriver(options);
}
