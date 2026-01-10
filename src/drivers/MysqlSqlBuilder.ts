/**
 * litedbmodel - MySQL SQL Builder
 *
 * Implements SQL building for MySQL with VALUES ROW syntax.
 */

import type {
  DriverTypeCast,
  SqlBuilder,
  InsertBuildOptions,
  UpdateBuildOptions,
  UpdateManyBuildOptions,
  SqlBuildResult,
} from './types';
import { DBToken } from '../DBValues';

// ============================================
// MySQL Type Cast Implementation
// ============================================

/**
 * MySQL type cast implementation
 * Uses JSON format for arrays since MySQL doesn't have native array types.
 */
export const mysqlTypeCast: DriverTypeCast = {
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
};

// ============================================
// MySQL SQL Builder
// ============================================

/**
 * MySQL SQL Builder implementation
 */
export const mysqlSqlBuilder: SqlBuilder = {
  driverType: 'mysql',
  typeCast: mysqlTypeCast,

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
        sql = `INSERT IGNORE INTO ${tableName} (${columns.join(', ')}) VALUES ${valueRows.join(', ')}`;
      } else if (onConflictUpdate) {
        const updateCols = onConflictUpdate === 'all' ? columns : onConflictUpdate;
        const updateClauses = updateCols.map(c => `${c} = VALUES(${c})`);
        sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES ${valueRows.join(', ')} ON DUPLICATE KEY UPDATE ${updateClauses.join(', ')}`;
      } else {
        sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES ${valueRows.join(', ')}`;
      }
    } else {
      sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES ${valueRows.join(', ')}`;
    }

    // MySQL doesn't support RETURNING natively, but the driver simulates it
    if (returning) {
      sql += ` RETURNING ${returning}`;
    }

    return { sql, params };
  },

  buildUpdate(options: UpdateBuildOptions): SqlBuildResult {
    const {
      tableName,
      setClauses,
      whereClause,
      whereParams,
    } = options;

    const params: unknown[] = [];
    const setExpressions: string[] = [];

    for (const { column, value } of setClauses) {
      if (value instanceof DBToken) {
        setExpressions.push(`${column} = ${value.compile(params)}`);
      } else {
        params.push(value);
        setExpressions.push(`${column} = ?`);
      }
    }

    const sql = `UPDATE ${tableName} SET ${setExpressions.join(', ')} WHERE ${whereClause}`;
    params.push(...whereParams);

    return { sql, params };
  },

  buildUpdateMany(options: UpdateManyBuildOptions): SqlBuildResult {
    const {
      tableName,
      keyColumns,
      updateColumns,
      records,
      skipMap = new Map(),
    } = options;

    const params: unknown[] = [];
    const allColumns = [...keyColumns, ...updateColumns];
    
    // Collect skip flags needed
    const skipColumnsNeeded = new Set<string>();
    for (const skipSet of skipMap.values()) {
      for (const col of skipSet) {
        skipColumnsNeeded.add(col);
      }
    }

    // Build VALUES rows with skip flags
    const valueRowDefs: string[] = [];
    const allValueCols = [...allColumns];
    for (const col of skipColumnsNeeded) {
      allValueCols.push(`_skip_${col}`);
    }

    for (let i = 0; i < records.length; i++) {
      const record = records[i];
      const rowValues: string[] = [];
      
      // Add regular columns
      for (const col of allColumns) {
        const val = record[col];
        if (val instanceof DBToken) {
          rowValues.push(val.compile(params));
        } else {
          params.push(val);
          rowValues.push('?');
        }
      }
      
      // Add skip flags
      for (const col of skipColumnsNeeded) {
        const isSkipped = skipMap.get(i)?.has(col) ?? false;
        params.push(isSkipped);
        rowValues.push('?');
      }
      
      valueRowDefs.push(`ROW(${rowValues.join(', ')})`);
    }

    // Build SET clause with IF for skip flags
    const setClauses: string[] = [];
    for (const col of updateColumns) {
      if (skipColumnsNeeded.has(col)) {
        setClauses.push(`t.${col} = IF(v._skip_${col}, t.${col}, v.${col})`);
      } else {
        setClauses.push(`t.${col} = v.${col}`);
      }
    }

    // Build WHERE clause for key columns
    const whereConditions = keyColumns.map(col => `t.${col} = v.${col}`);

    const sql = `UPDATE ${tableName} AS t ` +
          `JOIN (VALUES ${valueRowDefs.join(', ')}) AS v(${allValueCols.join(', ')}) ` +
          `ON ${whereConditions.join(' AND ')} ` +
          `SET ${setClauses.join(', ')}`;

    return { sql, params };
  },
};

