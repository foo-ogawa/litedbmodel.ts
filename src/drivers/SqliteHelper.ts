/**
 * litedbmodel - SQLite Helper Functions
 *
 * Provides type conversion utilities and SQLite-specific helpers.
 */

import { DBImmediateValue } from '../DBValues';

// ============================================
// Type Casting: DB -> TypeScript
// ============================================

/**
 * Cast a value to Date or null
 * SQLite stores dates as ISO strings or Unix timestamps
 */
export function castToDatetime(val: unknown): Date | null {
  if (val === null || val === undefined) {
    return null;
  }
  if (val instanceof Date) {
    return val;
  }
  if (typeof val === 'string' || typeof val === 'number') {
    const date = new Date(val);
    return isNaN(date.getTime()) ? null : date;
  }
  return null;
}

/**
 * Cast a value to boolean or null
 * SQLite stores booleans as 0/1
 */
export function castToBoolean(val: unknown): boolean | null {
  if (val === null || val === undefined) {
    return null;
  }
  if (typeof val === 'boolean') {
    return val;
  }
  if (typeof val === 'number') {
    return val !== 0;
  }
  if (typeof val === 'string') {
    const lower = val.toLowerCase();
    if (lower === 'true' || lower === '1') {
      return true;
    }
    if (lower === 'false' || lower === '0') {
      return false;
    }
  }
  return null;
}

/**
 * Cast a JSON string to array of integers
 */
export function castToIntegerArray(val: unknown): number[] {
  if (val === null || val === undefined) {
    return [];
  }
  if (Array.isArray(val)) {
    return val.map((v) => {
      const n = parseInt(String(v), 10);
      return isNaN(n) ? 0 : n;
    });
  }
  if (typeof val === 'string') {
    try {
      const parsed = JSON.parse(val);
      if (Array.isArray(parsed)) {
        return parsed.map((v) => {
          const n = parseInt(String(v), 10);
          return isNaN(n) ? 0 : n;
        });
      }
    } catch {
      // Not valid JSON
    }
  }
  return [];
}

/**
 * Cast a JSON string to array of strings
 */
export function castToStringArray(val: unknown): string[] {
  if (val === null || val === undefined) {
    return [];
  }
  if (Array.isArray(val)) {
    return val.map((v) => String(v));
  }
  if (typeof val === 'string') {
    try {
      const parsed = JSON.parse(val);
      if (Array.isArray(parsed)) {
        return parsed.map((v) => String(v));
      }
    } catch {
      // Not valid JSON
    }
  }
  return [];
}

/**
 * Cast a JSON string to object or array
 */
export function castToJson(
  val: unknown
): Record<string, unknown> | unknown[] | null {
  if (val === null || val === undefined) {
    return null;
  }
  if (typeof val === 'object') {
    return val as Record<string, unknown> | unknown[];
  }
  if (typeof val === 'string') {
    try {
      return JSON.parse(val);
    } catch {
      return null;
    }
  }
  return null;
}

// ============================================
// Immediate Value Generators (SQLite syntax)
// ============================================

/**
 * Generate CURRENT_TIMESTAMP immediate value
 */
export function Now(): DBImmediateValue {
  return new DBImmediateValue("datetime('now')");
}

/**
 * Generate NULL immediate value
 */
export function Null(): DBImmediateValue {
  return new DBImmediateValue('NULL');
}

/**
 * Generate TRUE immediate value (1 in SQLite)
 */
export function True(): DBImmediateValue {
  return new DBImmediateValue('1');
}

/**
 * Generate FALSE immediate value (0 in SQLite)
 */
export function False(): DBImmediateValue {
  return new DBImmediateValue('0');
}

// ============================================
// SQLite Array/JSON Helpers
// ============================================

/**
 * Generate SQLite JSON array from integers
 */
export function jsonIntArray(val: number[]): DBImmediateValue {
  return new DBImmediateValue(`'${JSON.stringify(val)}'`);
}

/**
 * Generate SQLite JSON array from strings
 */
export function jsonStringArray(val: string[]): DBImmediateValue {
  return new DBImmediateValue(`'${JSON.stringify(val)}'`);
}

/**
 * Generate SQLite JSON object
 */
export function jsonObject(val: Record<string, unknown>): DBImmediateValue {
  return new DBImmediateValue(`'${JSON.stringify(val)}'`);
}

// ============================================
// Time Calculation Helpers
// ============================================

/**
 * Generate datetime after current time
 */
export function TimeAfter(
  val: number,
  interval: 'seconds' | 'minutes' | 'hours' | 'days'
): DBImmediateValue {
  return new DBImmediateValue(`datetime('now', '+${val} ${interval}')`);
}

/**
 * Generate date after current date
 */
export function DayAfter(val: number): DBImmediateValue {
  return new DBImmediateValue(`date('now', '+${val} days')`);
}

// ============================================
// Utility Functions
// ============================================

/**
 * Convert empty string to NULL
 */
export function empty2null(val: unknown): unknown | DBImmediateValue {
  if (val === '' || val === undefined) {
    return new DBImmediateValue('NULL');
  }
  return val;
}

/**
 * Generate LIKE pattern string (same as PostgreSQL)
 */
export function makeLikeString(
  src: string,
  front: boolean = true,
  back: boolean = true
): string {
  // Escape special LIKE characters
  const escaped = src.replace(/[%_\\]/g, '\\$&');
  const prefix = front ? '%' : '';
  const suffix = back ? '%' : '';
  return `${prefix}${escaped}${suffix}`;
}

// ============================================
// Type Mapping
// ============================================

/**
 * SQLite to TypeScript type mapping
 */
export const SQLITE_TYPE_TO_TS: Record<string, string> = {
  integer: 'number',
  int: 'number',
  real: 'number',
  float: 'number',
  double: 'number',
  text: 'string',
  varchar: 'string',
  char: 'string',
  blob: 'Buffer',
  boolean: 'boolean',
  datetime: 'Date',
  date: 'Date',
  timestamp: 'Date',
  json: 'Record<string, unknown>',
};

/**
 * Convert SQLite type to TypeScript type
 */
export function sqliteTypeToTsType(sqliteType: string): string {
  const normalized = sqliteType.toLowerCase().split('(')[0].trim();
  return SQLITE_TYPE_TO_TS[normalized] || 'unknown';
}

// ============================================
// SQL Type Casting for Parameters
// ============================================

/**
 * Format a placeholder with SQL type cast for SQLite.
 * SQLite doesn't support type casting in the PostgreSQL sense.
 * UUID is stored as TEXT and doesn't need casting.
 * 
 * @param placeholder - The placeholder string (e.g., '?')
 * @param _sqlCast - The SQL type (ignored for SQLite)
 * @returns The placeholder unchanged
 */
export function formatSqlCast(placeholder: string, _sqlCast: string): string {
  // SQLite doesn't need type casting - UUID is stored as TEXT
  return placeholder;
}

/**
 * Check if a SQL type needs explicit casting in conditions.
 * SQLite never needs explicit casting.
 */
export function needsSqlCast(_sqlCast: string): boolean {
  return false;
}

