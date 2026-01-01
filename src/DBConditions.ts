/**
 * litedbmodel - Condition Builder
 */

import { DBToken, DBSubquery, DBExists } from './DBValues';

// ============================================
// Types
// ============================================

export type ConditionValue =
  | string
  | number
  | boolean
  | null
  | Date
  | unknown[]
  | DBToken
  | DBConditions;

export type ConditionObject = Record<string, ConditionValue>;

// ============================================
// DBConditions - Condition Builder Class
// ============================================

export class DBConditions {
  protected conditions: ConditionObject;
  protected operator: 'AND' | 'OR';
  protected nested: Array<{ conditions: DBConditions; operator: 'AND' | 'OR' }>;

  constructor(conditions: ConditionObject = {}, operator: 'AND' | 'OR' = 'AND') {
    this.conditions = conditions;
    this.operator = operator;
    this.nested = [];
  }

  /**
   * Add conditions with specified operator
   */
  add(
    conditions: DBConditions | ConditionObject,
    operator: 'AND' | 'OR' = 'AND'
  ): DBConditions {
    if (conditions instanceof DBConditions) {
      this.nested.push({ conditions, operator });
    } else {
      const cond = new DBConditions(conditions, operator);
      this.nested.push({ conditions: cond, operator });
    }
    return this;
  }

  /**
   * Compile conditions to SQL WHERE clause
   * @param params - Parameter array to append values
   * @returns SQL WHERE clause (without 'WHERE' keyword)
   */
  compile(params: unknown[]): string {
    const parts: string[] = [];

    // Compile main conditions
    for (const [key, value] of Object.entries(this.conditions)) {
      const sql = this.compileCondition(key, value, params);
      if (sql) {
        parts.push(sql);
      }
    }

    // Compile nested conditions
    for (const nested of this.nested) {
      const nestedSql = nested.conditions.compile(params);
      if (nestedSql) {
        // Don't add operator here - it will be added by the join
        parts.push(`(${nestedSql})`);
      }
    }

    if (parts.length === 0) {
      return '';
    }

    // Join with operator
    return parts.join(` ${this.operator} `);
  }

  /**
   * Compile a single condition
   */
  protected compileCondition(
    key: string,
    value: ConditionValue,
    params: unknown[]
  ): string {
    // Handle __or__ special key for OR conditions
    if (key === '__or__' && Array.isArray(value)) {
      const orParts: string[] = [];
      for (const group of value as ConditionObject[]) {
        const groupCond = new DBConditions(group, 'AND');
        const groupSql = groupCond.compile(params);
        if (groupSql) {
          orParts.push(`(${groupSql})`);
        }
      }
      if (orParts.length > 0) {
        return `(${orParts.join(' OR ')})`;
      }
      return '';
    }

    // Handle __exists__ special key for EXISTS/NOT EXISTS subquery
    if (key === '__exists__' && value instanceof DBExists) {
      return value.compile(params);
    }

    // Handle __subquery__ special key for IN/NOT IN subquery
    if (key === '__subquery__' && value instanceof DBSubquery) {
      return value.compile(params);
    }

    // Handle __raw__ special key for raw SQL conditions
    if (key === '__raw__') {
      if (typeof value === 'string') {
        return value;
      }
      if (Array.isArray(value)) {
        // Check if it's [sql, params] format (first element is string, second is array)
        if (value.length === 2 && typeof value[0] === 'string' && Array.isArray(value[1])) {
          const [sql, rawParams] = value as [string, unknown[]];
          params.push(...rawParams);
          return sql;
        }
        // Multiple raw conditions - join with AND
        const rawParts = value.filter(v => typeof v === 'string') as string[];
        if (rawParts.length > 0) {
          return rawParts.join(' AND ');
        }
      }
      return '';
    }

    // Handle DBToken instances (must be before __ key skip to handle __tuple__)
    if (value instanceof DBToken) {
      return value.compile(params, key);
    }

    // Skip other nested conditions (keys starting with __)
    if (key.startsWith('__')) {
      if (value instanceof DBConditions) {
        return `(${value.compile(params)})`;
      }
      return '';
    }

    // Handle nested DBConditions
    if (value instanceof DBConditions) {
      return `(${value.compile(params)})`;
    }

    // Handle null
    if (value === null) {
      return `${key} IS NULL`;
    }

    // Handle boolean
    if (typeof value === 'boolean') {
      return `${key} = ${value ? 'TRUE' : 'FALSE'}`;
    }

    // Handle array (IN clause)
    if (Array.isArray(value)) {
      // Check if key contains custom operator with ?
      if (key.includes('?')) {
        return this.compileCustomOperator(key, value, params);
      }

      if (value.length === 0) {
        return '1 = 0'; // Always false
      }

      const placeholders = value.map((v) => {
        params.push(v);
        return '?';
      });
      return `${key} IN (${placeholders.join(', ')})`;
    }

    // Check if key contains custom operator (e.g., "column > ?")
    // The ? in the key is the placeholder itself, just push params
    if (key.includes('?')) {
      return this.compileCustomOperator(key, value, params);
    }

    // Default: equality
    params.push(value);
    return `${key} = ?`;
  }

  /**
   * Compile custom operator condition
   * Key already contains ? placeholders, just push params in order
   */
  protected compileCustomOperator(
    key: string,
    value: ConditionValue,
    params: unknown[]
  ): string {
    if (Array.isArray(value)) {
      // Multiple placeholders - push all values
      for (const v of value) {
        params.push(v);
      }
    } else {
      // Single placeholder
      params.push(value);
    }
    // Return key as-is (already contains ? placeholders)
    return key;
  }

  /**
   * Check if conditions are empty
   */
  isEmpty(): boolean {
    return (
      Object.keys(this.conditions).length === 0 && this.nested.length === 0
    );
  }

  /**
   * Get the raw conditions object
   */
  getConditions(): ConditionObject {
    return this.conditions;
  }
}

// ============================================
// DBOrConditions - OR condition builder
// ============================================

export class DBOrConditions extends DBConditions {
  constructor(conditions: ConditionObject = {}) {
    super(conditions, 'OR');
  }
}

// ============================================
// Helper Functions
// ============================================

/**
 * Create AND conditions
 */
export function and(conditions: ConditionObject): DBConditions {
  return new DBConditions(conditions, 'AND');
}

/**
 * Create OR conditions
 */
export function or(conditions: ConditionObject): DBOrConditions {
  return new DBOrConditions(conditions);
}

/**
 * Normalize conditions - convert plain object to DBConditions if needed
 */
export function normalizeConditions(
  conditions: DBConditions | ConditionObject
): DBConditions {
  if (conditions instanceof DBConditions) {
    return conditions;
  }
  return new DBConditions(conditions);
}

