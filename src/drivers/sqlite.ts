/**
 * litedbmodel - SQLite Driver
 *
 * Database driver implementation for SQLite using better-sqlite3.
 * Note: This is a synchronous driver wrapped with async interface.
 */

import type { DBConfig, DBDriver, DBDriverOptions, DBConnection, QueryResult, Logger } from './types';
import { defaultLogger } from './types';

// better-sqlite3 types (optional dependency)
interface BetterSqlite3Database {
  prepare(sql: string): BetterSqlite3Statement;
  exec(sql: string): void;
  close(): void;
  inTransaction: boolean;
}

interface BetterSqlite3Statement {
  run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
  all(...params: unknown[]): unknown[];
  get(...params: unknown[]): unknown;
}

// ============================================
// Parameter Conversion
// ============================================

/**
 * Convert parameters to SQLite-compatible types
 * - boolean -> 0/1
 * - Date -> ISO string
 * - undefined -> null
 */
function convertParams(params: unknown[]): unknown[] {
  return params.map((param) => {
    if (param === undefined) {
      return null;
    }
    if (param === null) {
      return null;
    }
    if (typeof param === 'boolean') {
      return param ? 1 : 0;
    }
    if (param instanceof Date) {
      return param.toISOString();
    }
    // Convert objects and arrays to JSON strings for SQLite storage
    // (SQLite doesn't have native JSON/Array types)
    if (typeof param === 'object') {
      return JSON.stringify(param);
    }
    return param;
  });
}

// ============================================
// SQLite Connection Wrapper
// ============================================

class SqliteConnection implements DBConnection {
  constructor(private db: BetterSqlite3Database) {}

  /**
   * Convert PostgreSQL-style placeholders ($1, $2) to SQLite-style (?, ?)
   */
  private convertPlaceholders(sql: string): string {
    return sql.replace(/\$\d+/g, '?');
  }

  async query(sql: string, params?: unknown[]): Promise<QueryResult> {
    const convertedSql = this.convertPlaceholders(sql);
    const stmt = this.db.prepare(convertedSql);
    const normalizedSql = sql.trim().toUpperCase();
    const convertedParams = convertParams(params || []);

    // Determine if this is a SELECT-like query
    if (
      normalizedSql.startsWith('SELECT') ||
      normalizedSql.startsWith('WITH') ||
      normalizedSql.includes('RETURNING')
    ) {
      const rows = stmt.all(...convertedParams) as Record<string, unknown>[];
      return { rows, rowCount: rows.length };
    } else {
      // INSERT, UPDATE, DELETE
      const result = stmt.run(...convertedParams);
      return { rows: [], rowCount: result.changes };
    }
  }

  release(): void {
    // SQLite doesn't have connection pooling, so this is a no-op
  }
}

// ============================================
// SQLite Driver
// ============================================

/**
 * SQLite database driver
 *
 * Requires better-sqlite3 to be installed:
 * npm install better-sqlite3
 */
export class SqliteDriver implements DBDriver {
  readonly name = 'sqlite';

  private db: BetterSqlite3Database | null = null;
  private config: DBConfig;
  private logger: Logger;

  constructor(options: DBDriverOptions) {
    this.config = options.config;
    this.logger = options.logger || defaultLogger;
  }

  /**
   * Get or create database connection
   */
  private getDb(): BetterSqlite3Database {
    if (!this.db) {
      try {
        // Dynamic import to make better-sqlite3 optional
        // eslint-disable-next-line @typescript-eslint/no-var-requires
        const Database = require('better-sqlite3');
        this.db = new Database(this.config.database) as BetterSqlite3Database;

        // Enable WAL mode for better concurrent access
        this.db.exec('PRAGMA journal_mode = WAL');
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code === 'MODULE_NOT_FOUND') {
          throw new Error(
            'SQLite driver requires better-sqlite3 package. Install it with: npm install better-sqlite3'
          );
        }
        throw err;
      }
    }
    return this.db;
  }

  /**
   * Convert PostgreSQL-style placeholders ($1, $2) to SQLite-style (?, ?)
   */
  private convertPlaceholders(sql: string): string {
    let index = 0;
    return sql.replace(/\$\d+/g, () => {
      index++;
      return '?';
    });
  }

  /**
   * Execute a read query
   */
  async execute(sql: string, params: unknown[] = []): Promise<QueryResult> {
    const db = this.getDb();
    const convertedSql = this.convertPlaceholders(sql);
    const convertedParams = convertParams(params);
    this.logger.debug(`SQL: ${convertedSql}`, convertedParams);

    const startTime = Date.now();
    try {
      const stmt = db.prepare(convertedSql);
      const normalizedSql = sql.trim().toUpperCase();

      let result: QueryResult;
      if (
        normalizedSql.startsWith('SELECT') ||
        normalizedSql.startsWith('WITH') ||
        normalizedSql.includes('RETURNING')
      ) {
        const rows = stmt.all(...convertedParams) as Record<string, unknown>[];
        result = { rows, rowCount: rows.length };
      } else {
        const runResult = stmt.run(...convertedParams);
        result = { rows: [], rowCount: runResult.changes };
      }

      const duration = Date.now() - startTime;
      this.logger.debug(`Query completed in ${duration}ms, rows: ${result.rowCount}`);
      return result;
    } catch (error) {
      this.logger.error(`Query failed: ${convertedSql}`, error);
      throw error;
    }
  }

  /**
   * Execute a write query (same as execute for SQLite)
   */
  async executeWrite(sql: string, params: unknown[] = []): Promise<QueryResult> {
    return this.execute(sql, params);
  }

  /**
   * Get a connection (returns wrapper around the single db connection)
   */
  async getConnection(): Promise<DBConnection> {
    return new SqliteConnection(this.getDb());
  }

  /**
   * Close the database
   */
  async close(): Promise<void> {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }

  /**
   * Set logger
   */
  setLogger(logger: Logger): void {
    this.logger = logger;
  }

  /**
   * Get the underlying database (for advanced use)
   */
  getDatabase(): BetterSqlite3Database | null {
    return this.db;
  }
}

/**
 * Create a SQLite driver instance
 */
export function createSqliteDriver(options: DBDriverOptions): SqliteDriver {
  return new SqliteDriver(options);
}

