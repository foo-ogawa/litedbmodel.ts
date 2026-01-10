/**
 * litedbmodel - MySQL SQL Builder
 *
 * Implements SQL building for MySQL with VALUES ROW syntax.
 */

import type {
  DriverTypeCast,
  SqlBuilder,
  InsertBuildOptions,
  UpdateManyBuildOptions,
  SelectPkeysOptions,
  FindByPkeysOptions,
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
  supportsReturning: false,

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

  buildSelectPkeys(options: SelectPkeysOptions): SqlBuildResult {
    const { tableName, pkeyColumns, keyColumns, keyValues } = options;
    const params: unknown[] = [];
    
    // Build WHERE clause: col1 IN (?) AND col2 IN (?)
    const whereConditions = keyColumns.map((col, i) => {
      params.push(keyValues.map(kv => kv[i]));
      return `${col} IN (?)`;
    });
    
    const sql = `SELECT DISTINCT ${pkeyColumns.join(', ')} FROM ${tableName} WHERE ${whereConditions.join(' AND ')}`;
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

    // Composite PK: JOIN (VALUES ROW(...)) AS v ON ...
    const rowPlaceholders: string[] = [];
    for (const pkValueTuple of pkeyValues) {
      const rowVals = pkValueTuple.map(() => '?').join(', ');
      params.push(...pkValueTuple);
      rowPlaceholders.push(`ROW(${rowVals})`);
    }
    const onConditions = pkeyColumns.map(col => `t.${col} = v.${col}`).join(' AND ');
    const selectCol = selectColumn === '*' ? 't.*' : selectColumn;
    const sql = `SELECT ${selectCol} FROM ${tableName} AS t ` +
          `JOIN (VALUES ${rowPlaceholders.join(', ')}) AS v(${pkeyColumns.join(', ')}) ` +
          `ON ${onConditions}`;
    return { sql, params };
  },

  buildReturning(): string | undefined {
    // MySQL does not support RETURNING clause
    return undefined;
  },
};

