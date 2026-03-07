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
  clearQueryCache();
}

/**
 * Convert ?-style placeholders to PostgreSQL-style ($1, $2, ...)
 */
function convertPlaceholders(sql: string): string {
  let index = 0;
  return sql.replace(/\?/g, () => `$${++index}`);
}

// ============================================
// Unified Query Cache
// ============================================

interface CachedQuery {
  convertedSql: string;
  isMulti: boolean;
  name: string;
}

let preparedStmtCounter = 0;
const queryCache: Map<string, CachedQuery> = new Map();

/**
 * Resolve a ?-placeholder SQL into its cached converted form, multi-statement flag,
 * and prepared statement name. On cache hit (the common ORM path), this is a single
 * Map lookup — no regex, no string allocation.
 */
function resolveQuery(sql: string): CachedQuery {
  let cached = queryCache.get(sql);
  if (!cached) {
    const convertedSql = convertPlaceholders(sql);
    // Multi-statement detection without intermediate allocations
    let isMulti = false;
    let end = convertedSql.length - 1;
    while (end >= 0 && (convertedSql[end] === ' ' || convertedSql[end] === '\n' || convertedSql[end] === '\t' || convertedSql[end] === '\r')) end--;
    if (end >= 0 && convertedSql[end] === ';') end--;
    for (let i = 0; i <= end; i++) {
      if (convertedSql[i] === ';') { isMulti = true; break; }
    }
    cached = {
      convertedSql,
      isMulti,
      name: `ldb_${++preparedStmtCounter}`,
    };
    queryCache.set(sql, cached);
  }
  return cached;
}

/**
 * Clear query cache (called when pools are closed)
 */
function clearQueryCache(): void {
  queryCache.clear();
  preparedStmtCounter = 0;
}

// ============================================
// PostgreSQL Connection Wrapper
// ============================================

class PostgresConnection implements DBConnection {
  constructor(private client: PoolClient) {}

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    const q = resolveQuery(sql);
    const result = q.isMulti
      ? await this.client.query(q.convertedSql, params)
      : await this.client.query({ name: q.name, text: q.convertedSql, values: params });
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
    const q = resolveQuery(sql);
    this.logger.debug(`SQL: ${q.convertedSql}`, params);

    const startTime = Date.now();
    try {
      const result = q.isMulti
        ? await this.pool.query(q.convertedSql, params)
        : await this.pool.query({ name: q.name, text: q.convertedSql, values: params });
      const duration = Date.now() - startTime;
      this.logger.debug(`Query completed in ${duration}ms, rows: ${result.rowCount}`);
      return {
        rows: result.rows as Record<string, unknown>[],
        rowCount: result.rowCount ?? 0,
      };
    } catch (error) {
      this.logger.error(`Query failed: ${q.convertedSql}`, error);
      throw error;
    }
  }

  /**
   * Execute a write query (uses writer pool if available)
   */
  async executeWrite(sql: string, params: unknown[] = []): Promise<QueryResult> {
    const conn = this.writerPool || this.pool;
    const q = resolveQuery(sql);
    this.logger.debug(`SQL: ${q.convertedSql}`, params);

    const startTime = Date.now();
    try {
      const result = q.isMulti
        ? await conn.query(q.convertedSql, params)
        : await conn.query({ name: q.name, text: q.convertedSql, values: params });
      const duration = Date.now() - startTime;
      this.logger.debug(`Write completed in ${duration}ms, rows: ${result.rowCount}`);
      return {
        rows: result.rows as Record<string, unknown>[],
        rowCount: result.rowCount ?? 0,
      };
    } catch (error) {
      this.logger.error(`Write failed: ${q.convertedSql}`, error);
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
