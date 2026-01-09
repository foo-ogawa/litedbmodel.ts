/**
 * litedbmodel - MySQL Helper Functions
 *
 * Provides type conversion utilities and MySQL-specific helpers.
 */

import { DBImmediateValue } from '../DBValues';

// ============================================
// Type Casting: DB -> TypeScript
// ============================================

/**
 * Cast a value to Date or null
 * MySQL returns Date objects directly for DATETIME/TIMESTAMP columns
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
 * MySQL BOOLEAN is actually TINYINT(1), stored as 0/1
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
  // Buffer (BIT type)
  if (Buffer.isBuffer(val)) {
    return val[0] !== 0;
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
 * MySQL 5.7+ has native JSON type, mysql2 auto-parses it
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
// Immediate Value Generators (MySQL syntax)
// ============================================

/**
 * Generate CURRENT_TIMESTAMP immediate value
 */
export function Now(): DBImmediateValue {
  return new DBImmediateValue('NOW()');
}

/**
 * Generate NULL immediate value
 */
export function Null(): DBImmediateValue {
  return new DBImmediateValue('NULL');
}

/**
 * Generate TRUE immediate value
 */
export function True(): DBImmediateValue {
  return new DBImmediateValue('TRUE');
}

/**
 * Generate FALSE immediate value
 */
export function False(): DBImmediateValue {
  return new DBImmediateValue('FALSE');
}

// ============================================
// MySQL JSON Helpers
// ============================================

/**
 * Generate MySQL JSON array from integers
 */
export function jsonIntArray(val: number[]): DBImmediateValue {
  return new DBImmediateValue(`CAST('${JSON.stringify(val)}' AS JSON)`);
}

/**
 * Generate MySQL JSON array from strings
 */
export function jsonStringArray(val: string[]): DBImmediateValue {
  return new DBImmediateValue(`CAST('${JSON.stringify(val)}' AS JSON)`);
}

/**
 * Generate MySQL JSON object
 */
export function jsonObject(val: Record<string, unknown>): DBImmediateValue {
  return new DBImmediateValue(`CAST('${JSON.stringify(val)}' AS JSON)`);
}

// ============================================
// Time Calculation Helpers
// ============================================

/**
 * Generate datetime after current time
 */
export function TimeAfter(
  val: number,
  interval: 'SECOND' | 'MINUTE' | 'HOUR' | 'DAY'
): DBImmediateValue {
  return new DBImmediateValue(`DATE_ADD(NOW(), INTERVAL ${val} ${interval})`);
}

/**
 * Generate date after current date
 */
export function DayAfter(val: number): DBImmediateValue {
  return new DBImmediateValue(`DATE_ADD(CURDATE(), INTERVAL ${val} DAY)`);
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
 * Generate LIKE pattern string
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
 * MySQL to TypeScript type mapping
 */
export const MYSQL_TYPE_TO_TS: Record<string, string> = {
  tinyint: 'number',
  smallint: 'number',
  mediumint: 'number',
  int: 'number',
  integer: 'number',
  bigint: 'number',
  float: 'number',
  double: 'number',
  decimal: 'number',
  numeric: 'number',
  char: 'string',
  varchar: 'string',
  text: 'string',
  tinytext: 'string',
  mediumtext: 'string',
  longtext: 'string',
  blob: 'Buffer',
  tinyblob: 'Buffer',
  mediumblob: 'Buffer',
  longblob: 'Buffer',
  binary: 'Buffer',
  varbinary: 'Buffer',
  date: 'Date',
  datetime: 'Date',
  timestamp: 'Date',
  time: 'string',
  year: 'number',
  json: 'Record<string, unknown>',
  boolean: 'boolean',
  bool: 'boolean',
  bit: 'boolean',
  enum: 'string',
  set: 'string',
};

/**
 * Convert MySQL type to TypeScript type
 */
export function mysqlTypeToTsType(mysqlType: string): string {
  // Handle TINYINT(1) as boolean
  if (mysqlType.toLowerCase().startsWith('tinyint(1)')) {
    return 'boolean';
  }
  const normalized = mysqlType.toLowerCase().split('(')[0].trim();
  return MYSQL_TYPE_TO_TS[normalized] || 'unknown';
}

// ============================================
// SQL Type Casting for Parameters
// ============================================

/**
 * Format a placeholder with SQL type cast for MySQL.
 * MySQL doesn't require explicit casting for UUID (stored as CHAR(36) or BINARY).
 * 
 * @param placeholder - The placeholder string (e.g., '?')
 * @param _sqlCast - The SQL type (ignored for MySQL)
 * @returns The placeholder unchanged
 */
export function formatSqlCast(placeholder: string, _sqlCast: string): string {
  // MySQL doesn't need type casting - UUID is stored as CHAR(36)
  return placeholder;
}

/**
 * Check if a SQL type needs explicit casting in conditions.
 * MySQL never needs explicit casting for UUID.
 */
export function needsSqlCast(_sqlCast: string): boolean {
  return false;
}

