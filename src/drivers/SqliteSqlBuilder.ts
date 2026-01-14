/**
 * litedbmodel - SQLite SQL Builder
 *
 * Implements SQL building for SQLite with CASE WHEN syntax for batch updates.
 */

import type {
  DriverTypeCast,
  SqlBuilder,
  InsertBuildOptions,
  UpdateManyBuildOptions,
  FindByPkeysOptions,
  SqlBuildResult,
} from './types';
import { DBToken } from '../DBValues';

// ============================================
// SQLite Type Cast Implementation
// ============================================

/**
 * SQLite type cast implementation
 * Uses JSON format for arrays since SQLite doesn't have native array types.
 */
export const sqliteTypeCast: DriverTypeCast = {
  driverName: 'sqlite',

  serializeArray<T>(val: T[]): string | null {
    if (val === null || val === undefined) return null;
    return JSON.stringify(val);
  },

  deserializeArray<T>(val: unknown): T[] | null {
    if (val === null || val === undefined) return null;
    if (Array.isArray(val)) return val as T[];
    if (typeof val === 'string') {
      try {
        const parsed = JSON.parse(val);
        return Array.isArray(parsed) ? parsed as T[] : null;
      } catch {
        return null;
      }
    }
    return null;
  },

  serializeJson(val: unknown): string | null {
    if (val === null || val === undefined) return null;
    return JSON.stringify(val);
  },

  deserializeJson<T>(val: unknown): T | null {
    if (val === null || val === undefined) return null;
    if (typeof val === 'object') return val as T;
    if (typeof val === 'string') {
      try {
        return JSON.parse(val) as T;
      } catch {
        return null;
      }
    }
    return null;
  },

  serializeBooleanArray(val: (boolean | null)[]): string | null {
    if (val === null || val === undefined) return null;
    return JSON.stringify(val);
  },

  deserializeBooleanArray(val: unknown): (boolean | null)[] | null {
    if (val === null || val === undefined) return null;
    if (Array.isArray(val)) return val as (boolean | null)[];
    if (typeof val === 'string') {
      try {
        const parsed = JSON.parse(val);
        return Array.isArray(parsed) ? parsed as (boolean | null)[] : null;
      } catch {
        return null;
      }
    }
    return null;
  },

  /**
   * SQLite: return Date object as-is, let better-sqlite3 driver handle conversion
   * Note: SQLite stores as TEXT - values stored/retrieved in server timezone
   */
  serializeDatetime(val: Date): Date {
    return val;
  },

  /**
   * SQLite: return Date object as-is, let better-sqlite3 driver handle conversion
   */
  serializeDate(val: Date): Date {
    return val;
  },
};

// ============================================
// SQLite SQL Builder
// ============================================

/**
 * SQLite SQL Builder implementation
 */
export const sqliteSqlBuilder: SqlBuilder = {
  driverType: 'sqlite',
  typeCast: sqliteTypeCast,
  supportsReturning: true,

  buildInsert(options: InsertBuildOptions): SqlBuildResult {
    const {
      tableName,
      columns,
      records,
      onConflict,
      onConflictIgnore,
      onConflictUpdate,
      returning,
    } = options;

    const params: unknown[] = [];
    const valueRows: string[] = [];

    for (const record of records) {
      const rowValues: string[] = [];
      for (const col of columns) {
        const val = record[col];
        if (val instanceof DBToken) {
          rowValues.push(val.compile(params));
        } else {
          params.push(val);
          rowValues.push('?');
        }
      }
      valueRows.push(`(${rowValues.join(', ')})`);
    }

    let sql: string;

    if (onConflict) {
      if (onConflictIgnore) {
        sql = `INSERT OR IGNORE INTO ${tableName} (${columns.join(', ')}) VALUES ${valueRows.join(', ')}`;
      } else if (onConflictUpdate) {
        const updateCols = onConflictUpdate === 'all' ? columns : onConflictUpdate;
        const updateClauses = updateCols.map(c => `${c} = excluded.${c}`);
        sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES ${valueRows.join(', ')} ON CONFLICT (${onConflict.join(', ')}) DO UPDATE SET ${updateClauses.join(', ')}`;
      } else {
        sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES ${valueRows.join(', ')}`;
      }
    } else {
      sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES ${valueRows.join(', ')}`;
    }

    if (returning) {
      sql += ` RETURNING ${returning}`;
    }

    return { sql, params };
  },

  buildUpdateMany(options: UpdateManyBuildOptions): SqlBuildResult {
    const {
      tableName,
      keyColumns,
      updateColumns,
      records,
      skipMap = new Map(),
      returning,
    } = options;

    // SQLite uses CASE WHEN for batch updates
    // Each column gets: col = CASE WHEN key=? THEN ? WHEN key=? THEN ? ... END
    
    const params: unknown[] = [];
    const setClauses: string[] = [];
    
    // Collect all key values for WHERE IN clause
    const keyValues: unknown[][] = records.map(r => keyColumns.map(k => r[k]));
    
    // Build SET clause with CASE WHEN for each update column
    for (const col of updateColumns) {
      const whenClauses: string[] = [];
      
      for (let i = 0; i < records.length; i++) {
        const record = records[i];
        const isSkipped = skipMap.get(i)?.has(col) ?? false;
        
        // Build key condition
        const keyConditions = keyColumns.map(k => {
          params.push(record[k]);
          return `${k} = ?`;
        });
        const keyCondition = keyConditions.length === 1 
          ? keyConditions[0] 
          : `(${keyConditions.join(' AND ')})`;
        
        if (isSkipped) {
          // SKIP: reference existing column value
          whenClauses.push(`WHEN ${keyCondition} THEN ${tableName}.${col}`);
        } else {
          const val = record[col];
          if (val instanceof DBToken) {
            whenClauses.push(`WHEN ${keyCondition} THEN ${val.compile(params)}`);
          } else {
            params.push(val);
            whenClauses.push(`WHEN ${keyCondition} THEN ?`);
          }
        }
      }
      
      setClauses.push(`${col} = CASE ${whenClauses.join(' ')} END`);
    }
    
    // Build WHERE IN clause
    let whereClause: string;
    if (keyColumns.length === 1) {
      const keyCol = keyColumns[0];
      const placeholders = keyValues.map(() => '?');
      whereClause = `${keyCol} IN (${placeholders.join(', ')})`;
      for (const kv of keyValues) {
        params.push(kv[0]);
      }
    } else {
      // Composite key: (k1, k2) IN ((?, ?), (?, ?), ...)
      const tuples = keyValues.map(() => `(${keyColumns.map(() => '?').join(', ')})`);
      whereClause = `(${keyColumns.join(', ')}) IN (${tuples.join(', ')})`;
      for (const kv of keyValues) {
        params.push(...kv);
      }
    }
    
    let sql = `UPDATE ${tableName} SET ${setClauses.join(', ')} WHERE ${whereClause}`;

    if (returning) {
      sql += ` RETURNING ${returning}`;
    }

    return { sql, params };
  },

  buildFindByPkeys(options: FindByPkeysOptions): SqlBuildResult {
    const { tableName, pkeyColumns, pkeyValues, selectColumn } = options;
    const params: unknown[] = [];

    if (pkeyColumns.length === 1) {
      // Single PK: WHERE id IN (?, ?, ...)
      const placeholders = pkeyValues.map(() => '?').join(', ');
      params.push(...pkeyValues.map(v => v[0]));
      const sql = `SELECT ${selectColumn} FROM ${tableName} WHERE ${pkeyColumns[0]} IN (${placeholders})`;
      return { sql, params };
    }

    // Composite PK: WITH v AS (VALUES ...) ... JOIN v ON ...
    const rowPlaceholders: string[] = [];
    for (const pkValueTuple of pkeyValues) {
      const rowVals = pkValueTuple.map(() => '?').join(', ');
      params.push(...pkValueTuple);
      rowPlaceholders.push(`(${rowVals})`);
    }
    const onConditions = pkeyColumns.map(col => `t.${col} = v.${col}`).join(' AND ');
    const selectCol = selectColumn === '*' ? 't.*' : selectColumn;
    const sql = `WITH v(${pkeyColumns.join(', ')}) AS (VALUES ${rowPlaceholders.join(', ')}) ` +
          `SELECT ${selectCol} FROM ${tableName} AS t ` +
          `JOIN v ON ${onConditions}`;
    return { sql, params };
  },

  buildReturning(tableName: string, columns: string[]): string | undefined {
    // SQLite: Use tableName.col format (no alias support in UPDATE)
    return columns.map(col => `${tableName}.${col}`).join(', ');
  },
};

