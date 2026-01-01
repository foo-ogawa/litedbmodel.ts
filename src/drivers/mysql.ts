/**
 * litedbmodel - MySQL Driver
 *
 * Database driver implementation for MySQL using mysql2.
 * The mysql2 package is dynamically loaded to make it an optional dependency.
 */

import type { DBConfig, DBDriver, DBDriverOptions, DBConnection, QueryResult, Logger } from './types';
import { defaultLogger } from './types';

// mysql2 types (loaded dynamically)
interface Mysql2Pool {
  getConnection(): Promise<Mysql2PoolConnection>;
  query(sql: string, values?: unknown[]): Promise<[unknown[], unknown]>;
  end(): Promise<void>;
}

interface Mysql2PoolConnection {
  query(sql: string, values?: unknown[]): Promise<[unknown[], unknown]>;
  release(): void;
}

interface Mysql2Module {
  createPool(config: {
    host?: string;
    port?: number;
    database: string;
    user?: string;
    password?: string;
    waitForConnections?: boolean;
    connectionLimit?: number;
    connectTimeout?: number;
  }): Mysql2Pool;
}

// ============================================
// Connection Pool Management
// ============================================

const pools: Map<string, Mysql2Pool> = new Map();
let mysql2Module: Mysql2Module | null = null;

/**
 * Load mysql2 module dynamically
 */
function getMysql2Module(): Mysql2Module {
  if (!mysql2Module) {
    try {
      // Use mysql2/promise for async/await support
      mysql2Module = require('mysql2/promise') as Mysql2Module;
    } catch (err) {
      throw new Error(
        'MySQL driver requires mysql2 package. Install it with: npm install mysql2'
      );
    }
  }
  return mysql2Module;
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
function getPool(config: DBConfig): Mysql2Pool {
  const key = getPoolKey(config);

  let pool = pools.get(key);
  if (!pool) {
    const mysql2 = getMysql2Module();
    pool = mysql2.createPool({
      host: config.host,
      port: config.port,
      database: config.database,
      user: config.user,
      password: config.password,
      waitForConnections: true,
      connectionLimit: config.max || 10,
      connectTimeout: (config.timeout || 30) * 1000,
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
export async function closeAllMysqlPools(): Promise<void> {
  for (const pool of pools.values()) {
    await pool.end();
  }
  pools.clear();
}

// ============================================
// Parameter Conversion
// ============================================

/**
 * Convert parameters to MySQL-compatible types
 * - boolean -> 0/1 (MySQL doesn't have true boolean type in older versions)
 */
function convertParams(params: unknown[]): unknown[] {
  return params.map((param) => {
    if (param === undefined) {
      return null;
    }
    // MySQL handles boolean as TINYINT(1), but node mysql2 converts automatically
    // Keep boolean as is for mysql2
    return param;
  });
}

/**
 * Convert ON CONFLICT syntax to MySQL ON DUPLICATE KEY syntax
 */
function convertOnConflictToMysql(sql: string): string {
  // Convert: ON CONFLICT (col) DO NOTHING
  // To: ON DUPLICATE KEY UPDATE col = col (no-op)
  const doNothingMatch = sql.match(/ON CONFLICT\s*\(([^)]+)\)\s*DO NOTHING/i);
  if (doNothingMatch) {
    const conflictCol = doNothingMatch[1].trim().split(',')[0].trim();
    return sql.replace(doNothingMatch[0], `ON DUPLICATE KEY UPDATE ${conflictCol} = ${conflictCol}`);
  }

  // Convert: ON CONFLICT (col) DO UPDATE SET col1 = EXCLUDED.col1, col2 = EXCLUDED.col2
  // To: ON DUPLICATE KEY UPDATE col1 = VALUES(col1), col2 = VALUES(col2)
  const doUpdateMatch = sql.match(/ON CONFLICT\s*\([^)]+\)\s*DO UPDATE SET\s+(.+?)(?:\s+RETURNING|$)/i);
  if (doUpdateMatch) {
    const setClause = doUpdateMatch[1];
    // Replace EXCLUDED.column with VALUES(column)
    const mysqlSetClause = setClause.replace(/EXCLUDED\.(\w+)/g, 'VALUES($1)');
    const fullMatch = sql.match(/ON CONFLICT\s*\([^)]+\)\s*DO UPDATE SET\s+.+?(?=\s+RETURNING|$)/i);
    if (fullMatch) {
      return sql.replace(fullMatch[0], `ON DUPLICATE KEY UPDATE ${mysqlSetClause}`);
    }
  }

  return sql;
}

/**
 * Extract table name from INSERT statement
 */
function extractTableName(sql: string): string | null {
  const match = sql.match(/INSERT INTO\s+(\w+)/i);
  return match ? match[1] : null;
}

/**
 * Remove RETURNING clause from SQL (MySQL doesn't support it)
 */
function removeReturning(sql: string): { sql: string; hasReturning: boolean; returningCols: string } {
  const match = sql.match(/\s+RETURNING\s+(.+)$/i);
  if (match) {
    return {
      sql: sql.replace(/\s+RETURNING\s+.+$/i, ''),
      hasReturning: true,
      returningCols: match[1].trim(),
    };
  }
  return { sql, hasReturning: false, returningCols: '' };
}

// ============================================
// MySQL Connection Wrapper
// ============================================

class MysqlConnection implements DBConnection {
  constructor(private connection: Mysql2PoolConnection) {}

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    // Handle MySQL-specific conversions
    const convertedSql = convertOnConflictToMysql(sql);
    
    // Handle RETURNING clause
    const { sql: sqlWithoutReturning, hasReturning, returningCols } = removeReturning(convertedSql);
    
    const convertedParams = convertParams(params || []);
    
    const [result] = await this.connection.query(sqlWithoutReturning, convertedParams);
    
    // Check if this is a SELECT-like query
    if (Array.isArray(result) && result.length > 0 && typeof result[0] === 'object' && result[0] !== null && !('affectedRows' in result[0])) {
      const rowArray = result as unknown as Record<string, unknown>[];
      return {
        rows: rowArray,
        rowCount: rowArray.length,
      };
    }
    
    // For INSERT with RETURNING, fetch the affected rows
    if (hasReturning) {
      const resultObj = result as unknown as Record<string, unknown>;
      const tableName = extractTableName(sql);
      
      if (tableName && sql.trim().toUpperCase().startsWith('INSERT')) {
        const insertId = resultObj.insertId as number;
        const affectedRows = (resultObj.affectedRows as number) ?? 1;
        
        if (affectedRows > 0 && insertId > 0) {
          const cols = returningCols === '*' ? '*' : returningCols;
          const selectSql = `SELECT ${cols} FROM ${tableName} WHERE id >= ? AND id < ?`;
          const [selectResult] = await this.connection.query(selectSql, [insertId, insertId + affectedRows]);
          const rows = selectResult as unknown as Record<string, unknown>[];
          return {
            rows,
            rowCount: rows.length,
          };
        }
      }
    }
    
    // Regular write result
    const resultObj = result as unknown as Record<string, unknown>;
    const rowCount = (resultObj.affectedRows as number) ?? 0;
    return {
      rows: [],
      rowCount,
    };
  }

  release(): void {
    this.connection.release();
  }
}

// ============================================
// MySQL Driver
// ============================================

/**
 * MySQL database driver
 */
export class MysqlDriver implements DBDriver {
  readonly name = 'mysql';

  private pool: Mysql2Pool;
  private writerPool: Mysql2Pool | null;
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
    // Handle MySQL-specific conversions
    const convertedSql = convertOnConflictToMysql(sql);
    
    // Handle RETURNING clause (MySQL doesn't support it natively)
    const { sql: sqlWithoutReturning, hasReturning, returningCols } = removeReturning(convertedSql);
    
    const convertedParams = convertParams(params);
    this.logger.debug(`SQL: ${sqlWithoutReturning}`, convertedParams);

    const startTime = Date.now();
    try {
      const [result] = await this.pool.query(sqlWithoutReturning, convertedParams);
      const duration = Date.now() - startTime;
      
      // Check if this is a SELECT-like query
      if (Array.isArray(result) && result.length > 0 && typeof result[0] === 'object' && result[0] !== null && !('affectedRows' in result[0])) {
        const rowArray = result as unknown as Record<string, unknown>[];
        this.logger.debug(`Query completed in ${duration}ms, rows: ${rowArray.length}`);
        return {
          rows: rowArray,
          rowCount: rowArray.length,
        };
      }
      
      // For INSERT/UPDATE/DELETE with RETURNING, fetch the affected rows
      if (hasReturning) {
        const resultObj = result as unknown as Record<string, unknown>;
        const tableName = extractTableName(sql);
        
        if (tableName && sql.trim().toUpperCase().startsWith('INSERT')) {
          const insertId = resultObj.insertId as number;
          const affectedRows = (resultObj.affectedRows as number) ?? 1;
          
          // For single insert, use insertId
          // For bulk insert, fetch rows from insertId to insertId + affectedRows - 1
          if (affectedRows > 0 && insertId > 0) {
            const cols = returningCols === '*' ? '*' : returningCols;
            const selectSql = `SELECT ${cols} FROM ${tableName} WHERE id >= ? AND id < ?`;
            const [selectResult] = await this.pool.query(selectSql, [insertId, insertId + affectedRows]);
            const rows = selectResult as unknown as Record<string, unknown>[];
            this.logger.debug(`Insert completed in ${duration}ms, rows: ${rows.length}`);
            return {
              rows,
              rowCount: rows.length,
            };
          }
        }
      }
      
      // Regular write result
      const resultObj = result as unknown as Record<string, unknown>;
      const rowCount = (resultObj.affectedRows as number) ?? 0;
      this.logger.debug(`Query completed in ${duration}ms, affected: ${rowCount}`);
      return {
        rows: [],
        rowCount,
      };
    } catch (error) {
      this.logger.error(`Query failed: ${sqlWithoutReturning}`, error);
      throw error;
    }
  }

  /**
   * Execute a write query (uses writer pool if available)
   */
  async executeWrite(sql: string, params: unknown[] = []): Promise<QueryResult> {
    const conn = this.writerPool || this.pool;
    
    // Handle MySQL-specific conversions
    const convertedSql = convertOnConflictToMysql(sql);
    
    // Handle RETURNING clause
    const { sql: sqlWithoutReturning, hasReturning, returningCols } = removeReturning(convertedSql);
    
    const convertedParams = convertParams(params);
    this.logger.debug(`SQL: ${sqlWithoutReturning}`, convertedParams);

    const startTime = Date.now();
    try {
      const [result] = await conn.query(sqlWithoutReturning, convertedParams);
      const duration = Date.now() - startTime;
      
      // For INSERT/UPDATE/DELETE with RETURNING, fetch the affected rows
      if (hasReturning) {
        const resultObj = result as unknown as Record<string, unknown>;
        const tableName = extractTableName(sql);
        
        if (tableName && sql.trim().toUpperCase().startsWith('INSERT')) {
          const insertId = resultObj.insertId as number;
          const affectedRows = (resultObj.affectedRows as number) ?? 1;
          
          if (affectedRows > 0 && insertId > 0) {
            const cols = returningCols === '*' ? '*' : returningCols;
            const selectSql = `SELECT ${cols} FROM ${tableName} WHERE id >= ? AND id < ?`;
            const [selectResult] = await conn.query(selectSql, [insertId, insertId + affectedRows]);
            const rows = selectResult as unknown as Record<string, unknown>[];
            this.logger.debug(`Insert completed in ${duration}ms, rows: ${rows.length}`);
            return {
              rows,
              rowCount: rows.length,
            };
          }
        }
      }
      
      // Regular write result
      const resultObj = result as unknown as Record<string, unknown>;
      const rowCount = (resultObj.affectedRows as number) ?? 0;
      const rows = Array.isArray(result) ? (result as unknown as Record<string, unknown>[]) : [];
      
      this.logger.debug(`Write completed in ${duration}ms, affected: ${rowCount}`);
      return {
        rows,
        rowCount,
      };
    } catch (error) {
      this.logger.error(`Write failed: ${sqlWithoutReturning}`, error);
      throw error;
    }
  }

  /**
   * Get a connection for transaction
   */
  async getConnection(): Promise<DBConnection> {
    const connection = await this.pool.getConnection();
    return new MysqlConnection(connection);
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
   * Get the underlying pool (for advanced use)
   */
  getPool(): Mysql2Pool {
    return this.pool;
  }

  /**
   * Get the underlying writer pool
   */
  getWriterPool(): Mysql2Pool | null {
    return this.writerPool;
  }
}

/**
 * Create a MySQL driver instance
 */
export function createMysqlDriver(options: DBDriverOptions): MysqlDriver {
  return new MysqlDriver(options);
}

