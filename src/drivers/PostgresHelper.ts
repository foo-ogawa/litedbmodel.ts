/**
 * litedbmodel - PostgreSQL Helper Functions
 *
 * Provides type conversion utilities and PostgreSQL-specific helpers.
 */

import { DBImmediateValue } from '../DBValues';

// ============================================
// Type Casting: DB -> TypeScript
// ============================================

/**
 * Cast a value to Date or null
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
 */
export function castToBoolean(val: unknown): boolean | null {
  if (val === null || val === undefined) {
    return null;
  }
  if (typeof val === 'boolean') {
    return val;
  }
  if (typeof val === 'string') {
    const lower = val.toLowerCase();
    if (lower === 'true' || lower === 't' || lower === '1') {
      return true;
    }
    if (lower === 'false' || lower === 'f' || lower === '0') {
      return false;
    }
  }
  if (typeof val === 'number') {
    return val !== 0;
  }
  return null;
}

/**
 * Cast a value to integer array
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
    return pgArrayParse(val).map((v) => {
      const n = parseInt(v, 10);
      return isNaN(n) ? 0 : n;
    });
  }
  return [];
}

/**
 * Cast a value to numeric array (with nulls)
 */
export function castToNumericArray(val: unknown): (number | null)[] {
  if (val === null || val === undefined) {
    return [];
  }
  if (Array.isArray(val)) {
    return val.map((v) => {
      if (v === null || v === undefined) return null;
      const n = parseFloat(String(v));
      return isNaN(n) ? null : n;
    });
  }
  if (typeof val === 'string') {
    return pgArrayParse(val).map((v) => {
      if (v === 'NULL' || v === '') return null;
      const n = parseFloat(v);
      return isNaN(n) ? null : n;
    });
  }
  return [];
}

/**
 * Cast a value to string array
 */
export function castToStringArray(val: unknown): string[] {
  if (val === null || val === undefined) {
    return [];
  }
  if (Array.isArray(val)) {
    return val.map((v) => String(v));
  }
  if (typeof val === 'string') {
    return pgArrayParse(val);
  }
  return [];
}

/**
 * Cast a value to boolean array (with nulls)
 */
export function castToBooleanArray(val: unknown): (boolean | null)[] {
  if (val === null || val === undefined) {
    return [];
  }
  if (Array.isArray(val)) {
    return val.map((v) => castToBoolean(v));
  }
  if (typeof val === 'string') {
    return pgArrayParse(val).map((v) => castToBoolean(v));
  }
  return [];
}

/**
 * Cast a value to Date array (with nulls)
 */
export function castToDatetimeArray(val: unknown): (Date | null)[] {
  if (val === null || val === undefined) {
    return [];
  }
  if (Array.isArray(val)) {
    return val.map((v) => castToDatetime(v));
  }
  if (typeof val === 'string') {
    return pgArrayParse(val).map((v) => castToDatetime(v));
  }
  return [];
}

/**
 * Cast a value to JSON object/array
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

// Alias for backward compatibility
export const castToJsonArray = castToJson;

// ============================================
// PostgreSQL Array Parser
// ============================================

/**
 * Parse a PostgreSQL array literal string
 *
 * @example
 * pgArrayParse('{1,2,3}') // => ['1', '2', '3']
 * pgArrayParse('{"a","b","c"}') // => ['a', 'b', 'c']
 */
export function pgArrayParse(literal: string): string[] {
  if (!literal || literal === '{}') {
    return [];
  }

  // Remove outer braces
  let content = literal.trim();
  if (content.startsWith('{') && content.endsWith('}')) {
    content = content.slice(1, -1);
  }

  if (content === '') {
    return [];
  }

  const result: string[] = [];
  let current = '';
  let inQuotes = false;
  let escaped = false;

  for (let i = 0; i < content.length; i++) {
    const char = content[i];

    if (escaped) {
      current += char;
      escaped = false;
      continue;
    }

    if (char === '\\') {
      escaped = true;
      continue;
    }

    if (char === '"') {
      inQuotes = !inQuotes;
      continue;
    }

    if (char === ',' && !inQuotes) {
      result.push(current);
      current = '';
      continue;
    }

    current += char;
  }

  // Push last element
  if (current !== '' || result.length > 0) {
    result.push(current);
  }

  return result;
}

// ============================================
// Immediate Value Generators
// ============================================

/**
 * Generate NOW() immediate value
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
// PostgreSQL Array Literal Generators
// ============================================

/**
 * Generate PostgreSQL integer array literal
 */
export function pgIntArray(
  val: number[],
  type: string = 'INTEGER'
): DBImmediateValue {
  if (val.length === 0) {
    return new DBImmediateValue(`ARRAY[]::${type}[]`);
  }
  return new DBImmediateValue(`ARRAY[${val.join(',')}]::${type}[]`);
}

/**
 * Generate PostgreSQL numeric array literal
 */
export function pgNumericArray(
  val: (number | null)[],
  type: string = 'NUMERIC'
): DBImmediateValue {
  if (val.length === 0) {
    return new DBImmediateValue(`ARRAY[]::${type}[]`);
  }
  const elements = val.map((v) => (v === null ? 'NULL' : String(v)));
  return new DBImmediateValue(`ARRAY[${elements.join(',')}]::${type}[]`);
}

/**
 * Generate PostgreSQL string array literal
 *
 * @param val - Array of strings
 * @param escape - Escape function (should escape single quotes)
 * @param type - PostgreSQL type (default: TEXT)
 */
export function pgStringArray(
  val: string[],
  escape?: (s: string) => string,
  type: string = 'TEXT'
): DBImmediateValue {
  if (val.length === 0) {
    return new DBImmediateValue(`ARRAY[]::${type}[]`);
  }
  const escFn = escape || defaultEscape;
  const elements = val.map((v) => `'${escFn(v)}'`);
  return new DBImmediateValue(`ARRAY[${elements.join(',')}]::${type}[]`);
}

/**
 * Generate PostgreSQL date array literal
 */
export function pgDateArray(val: Date[]): DBImmediateValue {
  if (val.length === 0) {
    return new DBImmediateValue("ARRAY[]::TIMESTAMP[]");
  }
  const elements = val.map((d) => `'${d.toISOString()}'`);
  return new DBImmediateValue(`ARRAY[${elements.join(',')}]::TIMESTAMP[]`);
}

// ============================================
// Time Calculation Helpers
// ============================================

/**
 * Generate interval after current time
 */
export function TimeAfter(
  val: number,
  interval: 'second' | 'minute' | 'hour' | 'day'
): DBImmediateValue {
  return new DBImmediateValue(`NOW() + INTERVAL '${val} ${interval}'`);
}

/**
 * Generate interval after specific date
 */
export function DayAfter(val: number, interval: string = 'day'): DBImmediateValue {
  return new DBImmediateValue(`NOW() + INTERVAL '${val} ${interval}'`);
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
 * Default escape function for SQL strings
 */
function defaultEscape(s: string): string {
  return s.replace(/'/g, "''");
}

/**
 * Generate LIKE pattern string
 *
 * @param src - Source string
 * @param front - Add % at front
 * @param back - Add % at back
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
 * PostgreSQL to TypeScript type mapping
 */
export const PG_TYPE_TO_TS: Record<string, string> = {
  integer: 'number',
  bigint: 'number',
  smallint: 'number',
  serial: 'number',
  bigserial: 'number',
  numeric: 'number',
  decimal: 'number',
  real: 'number',
  'double precision': 'number',
  text: 'string',
  varchar: 'string',
  'character varying': 'string',
  char: 'string',
  boolean: 'boolean',
  timestamp: 'Date',
  'timestamp with time zone': 'Date',
  'timestamp without time zone': 'Date',
  timestamptz: 'Date',
  date: 'Date',
  time: 'string',
  uuid: 'string',
  jsonb: 'Record<string, unknown>',
  json: 'Record<string, unknown>',
  'integer[]': 'number[]',
  'text[]': 'string[]',
  'boolean[]': 'boolean[]',
};

/**
 * Convert PostgreSQL type to TypeScript type
 */
export function pgTypeToTsType(pgType: string): string {
  return PG_TYPE_TO_TS[pgType.toLowerCase()] || 'unknown';
}

