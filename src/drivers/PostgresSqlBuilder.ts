/**
 * litedbmodel - PostgreSQL SQL Builder
 *
 * Implements SQL building for PostgreSQL with UNNEST optimizations.
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
// PostgreSQL Type Cast Implementation
// ============================================

/**
 * PostgreSQL type cast implementation
 */
export const postgresTypeCast: DriverTypeCast = {
  serializeArray<T>(val: T[]): string {
    if (val === null || val === undefined) return null as unknown as string;
    // PostgreSQL array format: {val1,val2,val3}
    const escaped = val.map(v => {
      if (v === null) return 'NULL';
      if (typeof v === 'string') {
        // Escape backslashes and quotes
        const esc = (v as string).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        // Quote if contains special chars
        if ((v as string).includes(',') || (v as string).includes('{') || 
            (v as string).includes('}') || (v as string).includes('"') || 
            (v as string).includes(' ')) {
          return `"${esc}"`;
        }
        return esc;
      }
      return String(v);
    });
    return `{${escaped.join(',')}}`;
  },

  deserializeArray<T>(val: unknown): T[] | null {
    if (val === null || val === undefined) return null;
    // PostgreSQL driver returns native arrays
    if (Array.isArray(val)) return val as T[];
    return null;
  },

  serializeJson(val: unknown): string | null {
    if (val === null || val === undefined) return null;
    return JSON.stringify(val);
  },

  deserializeJson<T>(val: unknown): T | null {
    if (val === null || val === undefined) return null;
    // PostgreSQL driver returns parsed JSON
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
    const mapped = val.map(v => v === null ? 'NULL' : (v ? 't' : 'f'));
    return `{${mapped.join(',')}}`;
  },

  deserializeBooleanArray(val: unknown): (boolean | null)[] | null {
    if (val === null || val === undefined) return null;
    if (Array.isArray(val)) return val as (boolean | null)[];
    return null;
  },
};

// ============================================
// Helper Functions
// ============================================

/**
 * Get element type from PostgreSQL array type (e.g., 'int[]' -> 'int')
 */
function getArrayElementType(arrayType: string): string {
  if (!arrayType.endsWith('[]')) return arrayType;
  return arrayType.slice(0, -2);
}

/**
 * Build SQL expression to convert JSON text to target PostgreSQL type.
 * Handles NULL and empty array values gracefully.
 */
function buildJsonToTypeExpr(colRef: string, pgType: string): string {
  if (pgType === 'jsonb' || pgType === 'json') {
    return `${colRef}::${pgType}`;
  }
  if (pgType.endsWith('[]')) {
    const elemType = getArrayElementType(pgType);
    // Use CASE to handle NULL values (jsonb_array_elements_text fails on NULL)
    // Use COALESCE to handle empty arrays (array_agg returns NULL for empty input)
    return `CASE WHEN ${colRef} IS NULL THEN NULL ELSE COALESCE((SELECT array_agg(elem::${elemType}) FROM jsonb_array_elements_text(${colRef}::jsonb) AS elem), ARRAY[]::${pgType}) END`;
  }
  return colRef;
}

/**
 * Infer PostgreSQL type from JavaScript value
 */
function inferPgType(val: unknown): string {
  if (val === null || val === undefined) return 'text';
  if (typeof val === 'number') {
    return Number.isInteger(val) ? 'int' : 'numeric';
  }
  if (typeof val === 'boolean') return 'boolean';
  if (typeof val === 'string') return 'text';
  if (val instanceof Date) return 'timestamp';
  if (Array.isArray(val)) {
    if (val.length === 0) return 'text[]';
    const first = val.find(v => v !== null && v !== undefined);
    if (first === undefined) return 'text[]';
    if (typeof first === 'number') {
      return Number.isInteger(first) ? 'int[]' : 'numeric[]';
    }
    if (typeof first === 'boolean') return 'boolean[]';
    if (typeof first === 'string') return 'text[]';
    if (first instanceof Date) return 'timestamp[]';
    return 'jsonb';  // Complex array -> JSONB
  }
  if (typeof val === 'object') return 'jsonb';
  return 'text';
}

// ============================================
// PostgreSQL SQL Builder
// ============================================

/**
 * PostgreSQL SQL Builder implementation
 */
export const postgresSqlBuilder: SqlBuilder = {
  driverType: 'postgres',
  typeCast: postgresTypeCast,

  buildInsert(options: InsertBuildOptions): SqlBuildResult {
    const {
      tableName,
      columns,
      records,
      rawRecords,
      sqlCastMap = new Map(),
      onConflict,
      onConflictIgnore,
      onConflictUpdate,
      returning,
    } = options;

    const params: unknown[] = [];

    // Use UNNEST for batch INSERT (2+ records)
    if (records.length > 1) {
      const unnestArrays: string[] = [];
      const selectExprs: string[] = [];
      let hasDBToken = false;

      for (const col of columns) {
        const sqlCast = sqlCastMap.get(col);
        // Find first non-null value to infer type
        let pgType = sqlCast;
        if (!pgType && rawRecords) {
          for (const r of rawRecords) {
            const val = r[col];
            if (val !== null && val !== undefined && !(val instanceof DBToken)) {
              pgType = inferPgType(val);
              break;
            }
          }
        }
        pgType = pgType || 'text';

        const isArrayType = pgType.endsWith('[]') && pgType !== 'jsonb' && pgType !== 'json';
        const isJsonType = pgType === 'jsonb' || pgType === 'json';

        // Collect values for this column
        const colValues = records.map((r, idx) => {
          const serializedVal = r[col];
          if (serializedVal instanceof DBToken) {
            hasDBToken = true;
            return serializedVal;
          }

          if (isArrayType) {
            // Array types: use raw value and JSON.stringify
            const rawVal = rawRecords?.[idx]?.[col];
            if (rawVal === null || rawVal === undefined) return null;
            return JSON.stringify(rawVal);
          }

          // JSON and scalar types: use serialized value as-is
          return serializedVal;
        });

        if (hasDBToken) break;

        params.push(colValues);

        // Array types: pass as text[] and convert via jsonb_array_elements_text
        // JSON types: pass as text[] and cast to jsonb
        // Scalar types: pass as their actual type
        if (isArrayType) {
          unnestArrays.push(`?::text[]`);
          selectExprs.push(buildJsonToTypeExpr(`v.${col}`, pgType));
        } else if (isJsonType) {
          unnestArrays.push(`?::text[]`);
          selectExprs.push(`v.${col}::${pgType}`);
        } else {
          unnestArrays.push(`?::${pgType}[]`);
          selectExprs.push(`v.${col}`);
        }
      }

      // If no DBToken, use UNNEST
      if (!hasDBToken) {
        let sql = `INSERT INTO ${tableName} (${columns.join(', ')}) ` +
              `SELECT ${selectExprs.join(', ')} ` +
              `FROM UNNEST(${unnestArrays.join(', ')}) AS v(${columns.join(', ')})`;

        // Handle ON CONFLICT
        if (onConflict) {
          sql += ` ON CONFLICT (${onConflict.join(', ')})`;
          if (onConflictIgnore) {
            sql += ' DO NOTHING';
          } else if (onConflictUpdate) {
            const updateCols = onConflictUpdate === 'all' ? columns : onConflictUpdate;
            const updateClauses = updateCols.map(c => `${c} = EXCLUDED.${c}`);
            sql += ` DO UPDATE SET ${updateClauses.join(', ')}`;
          }
        }

        if (returning) {
          sql += ` RETURNING ${returning}`;
        }

        return { sql, params };
      }

      // Fall through to VALUES clause (DBToken detected)
      params.length = 0;
    }

    // Single record or DBToken fallback: Use VALUES clause
    const valueRows: string[] = [];
    for (const record of records) {
      const rowValues: string[] = [];
      for (const col of columns) {
        const val = record[col];
        if (val instanceof DBToken) {
          rowValues.push(val.compile(params));
        } else {
          params.push(val);
          const sqlCast = sqlCastMap.get(col);
          if (sqlCast) {
            rowValues.push(`?::${sqlCast}`);
          } else {
            rowValues.push('?');
          }
        }
      }
      valueRows.push(`(${rowValues.join(', ')})`);
    }

    let sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES ${valueRows.join(', ')}`;

    if (onConflict) {
      sql += ` ON CONFLICT (${onConflict.join(', ')})`;
      if (onConflictIgnore) {
        sql += ' DO NOTHING';
      } else if (onConflictUpdate) {
        const updateCols = onConflictUpdate === 'all' ? columns : onConflictUpdate;
        const updateClauses = updateCols.map(c => `${c} = EXCLUDED.${c}`);
        sql += ` DO UPDATE SET ${updateClauses.join(', ')}`;
      }
    }

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
      sqlCastMap = new Map(),
      returning,
    } = options;

    const params: unknown[] = [];
    const setExpressions: string[] = [];

    for (const { column, value } of setClauses) {
      if (value instanceof DBToken) {
        setExpressions.push(`${column} = ${value.compile(params)}`);
      } else {
        params.push(value);
        const sqlCast = sqlCastMap.get(column);
        if (sqlCast) {
          setExpressions.push(`${column} = ?::${sqlCast}`);
        } else {
          setExpressions.push(`${column} = ?`);
        }
      }
    }

    let sql = `UPDATE ${tableName} SET ${setExpressions.join(', ')} WHERE ${whereClause}`;
    params.push(...whereParams);

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
      rawRecords,
      sqlCastMap = new Map(),
      skipMap = new Map(),
      returning,
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

    // Build column types and values
    const unnestArrays: string[] = [];
    const colTypes = new Map<string, string>();

    for (const col of allColumns) {
      const sqlCast = sqlCastMap.get(col);
      let pgType = sqlCast;
      if (!pgType && rawRecords) {
        for (const r of rawRecords) {
          const val = r[col];
          if (val !== null && val !== undefined && !(val instanceof DBToken)) {
            pgType = inferPgType(val);
            break;
          }
        }
      }
      pgType = pgType || 'text';
      colTypes.set(col, pgType);

      const isArrayType = pgType.endsWith('[]') && pgType !== 'jsonb' && pgType !== 'json';
      const isJsonType = pgType === 'jsonb' || pgType === 'json';

      const colValues = records.map((r, idx) => {
        // For skipped columns, we still need a placeholder value
        if (skipMap.get(idx)?.has(col)) {
          return null;
        }
        const serializedVal = r[col];
        if (isArrayType) {
          const rawVal = rawRecords?.[idx]?.[col];
          if (rawVal === null || rawVal === undefined) return null;
          return JSON.stringify(rawVal);
        }
        if (isJsonType && serializedVal !== null && serializedVal !== undefined) {
          return typeof serializedVal === 'string' ? serializedVal : JSON.stringify(serializedVal);
        }
        return serializedVal;
      });

      params.push(colValues);
      
      if (isArrayType || isJsonType) {
        unnestArrays.push(`?::text[]`);
      } else {
        unnestArrays.push(`?::${pgType}[]`);
      }
    }

    // Add skip flag columns
    const skipFlagCols: string[] = [];
    for (const col of skipColumnsNeeded) {
      skipFlagCols.push(`_skip_${col}`);
      const flagValues = records.map((_, idx) => skipMap.get(idx)?.has(col) ?? false);
      params.push(flagValues);
      unnestArrays.push(`?::boolean[]`);
    }

    // Build SET clause
    const setClauses: string[] = [];
    for (const col of updateColumns) {
      const pgType = colTypes.get(col)!;
      const isArrayType = pgType.endsWith('[]') && pgType !== 'jsonb' && pgType !== 'json';
      const isJsonType = pgType === 'jsonb' || pgType === 'json';

      let valueExpr: string;
      if (isArrayType) {
        valueExpr = buildJsonToTypeExpr(`v.${col}`, pgType);
      } else if (isJsonType) {
        valueExpr = `v.${col}::${pgType}`;
      } else {
        valueExpr = `v.${col}`;
      }

      if (skipColumnsNeeded.has(col)) {
        setClauses.push(`${col} = CASE WHEN v._skip_${col} THEN t.${col} ELSE ${valueExpr} END`);
      } else {
        setClauses.push(`${col} = ${valueExpr}`);
      }
    }

    // Build WHERE clause for key columns
    const whereConditions = keyColumns.map(col => `t.${col} = v.${col}`);

    const allUnnestCols = [...allColumns, ...skipFlagCols];
    let sql = `UPDATE ${tableName} AS t SET ${setClauses.join(', ')} ` +
          `FROM UNNEST(${unnestArrays.join(', ')}) AS v(${allUnnestCols.join(', ')}) ` +
          `WHERE ${whereConditions.join(' AND ')}`;

    if (returning) {
      sql += ` RETURNING ${returning}`;
    }

    return { sql, params };
  },

  inferPgType,
};

