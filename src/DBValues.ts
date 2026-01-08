/**
 * litedbmodel - Value Wrapper Classes
 * 
 * Provides special value types for SQL generation.
 * These classes allow fine-grained control over how values are rendered in SQL.
 * 
 * @packageDocumentation
 */

// ============================================
// DBToken - Base class for value wrappers
// ============================================

/**
 * Base class for all value wrapper types.
 * Value wrappers control how values are compiled into SQL fragments.
 * 
 * @internal
 */
export class DBToken {
  value: unknown;
  operator: string;

  constructor(value: unknown, operator: string = '=') {
    this.value = value;
    this.operator = operator;
  }

  /**
   * Compile the token to SQL fragment
   * @param params - Parameter array to append values
   * @param key - Column name (optional)
   * @returns SQL fragment
   */
  compile(params: unknown[], key?: string): string {
    params.push(this.value);
    if (key) {
      return `${key} ${this.operator} ?`;
    }
    return '?';
  }
}

// ============================================
// DBImmediateValue - Literal value (no parameter binding)
// ============================================

/**
 * Represents a literal SQL value that is NOT parameter-bound.
 * Use with caution - values are inserted directly into SQL.
 * 
 * @example
 * ```typescript
 * // Insert NOW() function call
 * await User.create([
 *   [User.created_at, dbImmediate('NOW()')],
 * ]);
 * ```
 * 
 * @internal
 */
export class DBImmediateValue extends DBToken {
  constructor(value: string) {
    super(value, '=');
  }

  compile(_params: unknown[], key?: string): string {
    if (key) {
      return `${key} ${this.operator} ${this.value}`;
    }
    return String(this.value);
  }
}

// ============================================
// DBNullValue - NULL value
// ============================================

/**
 * Represents SQL NULL for IS NULL conditions.
 * 
 * @example
 * ```typescript
 * // Find users with no email
 * await User.find([[User.email, dbNull()]]);
 * // → WHERE email IS NULL
 * ```
 * 
 * @internal
 */
export class DBNullValue extends DBImmediateValue {
  constructor() {
    super('NULL');
    this.operator = 'IS';
  }

  compile(_params: unknown[], key?: string): string {
    if (key) {
      return `${key} IS NULL`;
    }
    return 'NULL';
  }
}

// ============================================
// DBNotNullValue - NOT NULL value
// ============================================

/**
 * Represents SQL NOT NULL for IS NOT NULL conditions.
 * 
 * @example
 * ```typescript
 * // Find users with email set
 * await User.find([[User.email, dbNotNull()]]);
 * // → WHERE email IS NOT NULL
 * ```
 * 
 * @internal
 */
export class DBNotNullValue extends DBImmediateValue {
  constructor() {
    super('NOT NULL');
    this.operator = 'IS';
  }

  compile(_params: unknown[], key?: string): string {
    if (key) {
      return `${key} IS NOT NULL`;
    }
    return 'NOT NULL';
  }
}

// ============================================
// DBBoolValue - Boolean value
// ============================================

/**
 * Represents SQL TRUE or FALSE literal.
 * 
 * @example
 * ```typescript
 * await User.find([[User.is_active, dbTrue()]]);
 * // → WHERE is_active = TRUE
 * ```
 * 
 * @internal
 */
export class DBBoolValue extends DBImmediateValue {
  constructor(value: boolean) {
    super(value ? 'TRUE' : 'FALSE');
  }
}

// ============================================
// DBArrayValue - Array value (for IN clause)
// ============================================

/**
 * Represents an array of values for IN clause.
 * 
 * @example
 * ```typescript
 * await User.find([[User.status, dbIn(['active', 'pending'])]]);
 * // → WHERE status IN ('active', 'pending')
 * ```
 * 
 * @internal
 */
export class DBArrayValue extends DBToken {
  constructor(values: unknown[]) {
    super(values, 'IN');
  }

  compile(params: unknown[], key?: string): string {
    const arr = this.value as unknown[];
    if (arr.length === 0) {
      // Empty array - always false condition
      return '1 = 0';
    }

    const placeholders = arr.map((v) => {
      params.push(v);
      return '?';
    });

    if (key) {
      return `${key} IN (${placeholders.join(', ')})`;
    }
    return `(${placeholders.join(', ')})`;
  }
}

// ============================================
// DBDynamicValue - Dynamic value (function calls etc.)
// ============================================

/**
 * Represents a dynamic SQL expression with parameters.
 * Useful for database functions with runtime parameters.
 * 
 * @example
 * ```typescript
 * await User.update(
 *   [[User.id, 1]],
 *   [[User.search_vector, dbDynamic("to_tsvector('english', ?)", [text])]],
 * );
 * ```
 * 
 * @internal
 */
export class DBDynamicValue extends DBToken {
  func: string;
  values: unknown[];

  constructor(func: string, values: unknown[] = []) {
    super(null, '=');
    this.func = func;
    this.values = values;
  }

  compile(params: unknown[], key?: string): string {
    // Push values to params (? placeholders in func are used directly)
    for (const val of this.values) {
      params.push(val);
    }

    if (key) {
      return `${key} ${this.operator} ${this.func}`;
    }
    return this.func;
  }
}

// ============================================
// DBRawValue - Raw SQL expression
// ============================================

/**
 * Represents a raw SQL expression for SET clauses.
 * Use for expressions like incrementing counters.
 * 
 * @example
 * ```typescript
 * await User.update(
 *   [[User.id, 1]],
 *   [[User.login_count, dbRaw('login_count + 1')]],
 * );
 * // → SET login_count = login_count + 1
 * ```
 * 
 * @internal
 */
export class DBRawValue extends DBImmediateValue {
  constructor(sql: string) {
    super(sql);
  }

  compile(_params: unknown[], key?: string): string {
    if (key) {
      return `${key} = ${this.value}`;
    }
    return String(this.value);
  }
}

// ============================================
// DBTupleIn - Composite key IN clause
// ============================================

/**
 * Represents a composite key IN clause: (col1, col2) IN ((v1, v2), (v3, v4), ...)
 * 
 * @example
 * ```typescript
 * const tupleIn = new DBTupleIn(['tenant_id', 'id'], [[1, 10], [1, 20], [2, 30]]);
 * // → (tenant_id, id) IN ((1, 10), (1, 20), (2, 30))
 * ```
 * 
 * @internal
 */
export class DBTupleIn extends DBToken {
  columns: string[];
  tuples: unknown[][];

  constructor(columns: string[], tuples: unknown[][]) {
    super(null, 'IN');
    this.columns = columns;
    this.tuples = tuples;
  }

  compile(params: unknown[], _key?: string): string {
    if (this.tuples.length === 0) {
      // Empty tuples - always false condition
      return '1 = 0';
    }

    const tuplePlaceholders = this.tuples.map(tuple => {
      const placeholders = tuple.map(val => {
        params.push(val);
        return '?';
      });
      return `(${placeholders.join(', ')})`;
    });

    return `(${this.columns.join(', ')}) IN (${tuplePlaceholders.join(', ')})`;
  }
}

// ============================================
// Factory functions for convenience
// ============================================

/**
 * Create a NULL value
 */
export function dbNull(): DBNullValue {
  return new DBNullValue();
}

/**
 * Create a NOT NULL value
 */
export function dbNotNull(): DBNotNullValue {
  return new DBNotNullValue();
}

/**
 * Create a boolean TRUE value
 */
export function dbTrue(): DBBoolValue {
  return new DBBoolValue(true);
}

/**
 * Create a boolean FALSE value
 */
export function dbFalse(): DBBoolValue {
  return new DBBoolValue(false);
}

/**
 * Create a NOW() value
 */
export function dbNow(): DBImmediateValue {
  return new DBImmediateValue('NOW()');
}

/**
 * Create an array value for IN clause
 */
export function dbIn(values: unknown[]): DBArrayValue {
  return new DBArrayValue(values);
}

/**
 * Create a dynamic value (function call with parameters)
 */
export function dbDynamic(func: string, values: unknown[] = []): DBDynamicValue {
  return new DBDynamicValue(func, values);
}

/**
 * Create a raw SQL expression
 */
export function dbRaw(sql: string): DBRawValue {
  return new DBRawValue(sql);
}

/**
 * Create an immediate value (literal, no binding)
 */
export function dbImmediate(value: string): DBImmediateValue {
  return new DBImmediateValue(value);
}

/**
 * Create a composite key IN clause
 * 
 * @param columns - Array of column names
 * @param tuples - Array of value tuples (each tuple matches the columns)
 * @returns DBTupleIn instance
 * 
 * @example
 * ```typescript
 * // (tenant_id, id) IN ((1, 10), (1, 20), (2, 30))
 * const condition = dbTupleIn(['tenant_id', 'id'], [[1, 10], [1, 20], [2, 30]]);
 * 
 * // With Column references
 * const condition = dbTupleIn(
 *   [User.tenant_id.columnName, User.id.columnName],
 *   [[1, 10], [1, 20]]
 * );
 * ```
 * 
 * @internal
 */
export function dbTupleIn(columns: string[], tuples: unknown[][]): DBTupleIn {
  return new DBTupleIn(columns, tuples);
}

// ============================================
// DBParentRef - Parent table column reference (for correlated subqueries)
// ============================================

/** 
 * Column reference interface (compatible with Column type)
 * @internal
 */
export interface ColumnRef {
  columnName: string;
  tableName: string;
}

/**
 * Parent table column reference for correlated subqueries.
 * Use this to reference a column from the outer query inside a subquery.
 * Outputs as table.column format for unambiguous references.
 * 
 * @example
 * ```typescript
 * import { parentRef } from 'litedbmodel';
 * 
 * // SELECT * FROM users WHERE users.id IN (
 * //   SELECT orders.user_id FROM orders WHERE orders.user_id = users.id
 * // )
 * await User.find([
 *   User.inSubquery([User.id], Order, [Order.user_id], [
 *     [Order.user_id, parentRef(User.id)]
 *   ])
 * ]);
 * ```
 * 
 * @internal
 */
export class DBParentRef extends DBToken {
  /** The parent column name */
  readonly columnName: string;
  /** The parent table name */
  readonly tableName: string;

  constructor(column: ColumnRef) {
    super(null, '=');
    this.columnName = column.columnName;
    this.tableName = column.tableName;
  }

  /**
   * Compile to SQL - returns table.column format
   */
  compile(_params: unknown[], _key?: string): string {
    return `${this.tableName}.${this.columnName}`;
  }
}

/**
 * Create a parent table column reference for correlated subqueries.
 * 
 * @param column - The column from the parent (outer) query (type-safe Column reference)
 * @returns DBParentRef instance
 * 
 * @example
 * ```typescript
 * // Reference parent's id column in subquery
 * await User.find([
 *   User.inSubquery([User.id], Order, [Order.user_id], [
 *     [Order.user_id, parentRef(User.id)]
 *   ])
 * ]);
 * ```
 */
export function parentRef(column: ColumnRef): DBParentRef {
  return new DBParentRef(column);
}

// ============================================
// DBSubquery - Subquery for IN/NOT IN conditions
// ============================================

/** 
 * Subquery condition info for building SQL
 * @internal
 */
export interface SubqueryCondition {
  column: ColumnRef;
  value: unknown;
}

/**
 * Subquery value for IN/NOT IN conditions.
 * Supports both single and composite key subqueries.
 * Uses table.column format for unambiguous column references.
 * 
 * @example
 * ```typescript
 * // Single key: users.id IN (SELECT orders.user_id FROM orders WHERE orders.status = 'paid')
 * new DBSubquery(
 *   [User.id],        // Parent columns (Column type)
 *   'orders',
 *   [Order.user_id],  // SELECT columns (Column type)
 *   [{ column: Order.status, value: 'paid' }],
 *   'IN'
 * );
 * ```
 * 
 * @internal
 */
export class DBSubquery extends DBToken {
  constructor(
    /** Parent columns to match (single or composite key) - accepts Column type directly */
    public readonly parentColumns: ColumnRef[],
    /** Target table name for subquery */
    public readonly tableName: string,
    /** Columns to SELECT in subquery (must match parentColumns length) - accepts Column type directly */
    public readonly selectColumns: ColumnRef[],
    /** WHERE conditions for subquery */
    public readonly conditions: SubqueryCondition[],
    /** Operator: IN or NOT IN */
    public readonly subqueryOperator: 'IN' | 'NOT IN' = 'IN'
  ) {
    super(null, subqueryOperator);
  }

  /** Format column as table.column */
  private formatColumn(col: ColumnRef): string {
    return `${col.tableName}.${col.columnName}`;
  }

  /**
   * Compile to SQL with parameter binding
   */
  compile(params: unknown[], _key?: string): string {
    const whereParts: string[] = [];

    for (const cond of this.conditions) {
      const colRef = this.formatColumn(cond.column);
      if (cond.value instanceof DBParentRef) {
        // Correlated subquery: reference parent column
        whereParts.push(`${colRef} = ${cond.value.compile(params)}`);
      } else if (cond.value instanceof DBToken) {
        // Other DBToken types
        whereParts.push(cond.value.compile(params, colRef));
      } else if (cond.value === null) {
        whereParts.push(`${colRef} IS NULL`);
      } else if (Array.isArray(cond.value)) {
        // IN clause within subquery
        if (cond.value.length === 0) {
          whereParts.push('1 = 0');
        } else {
          const placeholders = cond.value.map(v => {
            params.push(v);
            return '?';
          });
          whereParts.push(`${colRef} IN (${placeholders.join(', ')})`);
        }
      } else {
        // Regular value
        params.push(cond.value);
        whereParts.push(`${colRef} = ?`);
      }
    }

    const whereClause = whereParts.length > 0 ? ` WHERE ${whereParts.join(' AND ')}` : '';
    const selectClause = this.selectColumns.map(c => this.formatColumn(c)).join(', ');
    const subquery = `SELECT ${selectClause} FROM ${this.tableName}${whereClause}`;

    // Single column vs composite key
    if (this.parentColumns.length === 1) {
      return `${this.formatColumn(this.parentColumns[0])} ${this.subqueryOperator} (${subquery})`;
    } else {
      // Composite key: (table.col1, table.col2) IN (SELECT ...)
      const parentColList = this.parentColumns.map(c => this.formatColumn(c)).join(', ');
      return `(${parentColList}) ${this.subqueryOperator} (${subquery})`;
    }
  }
}

// ============================================
// DBExists - EXISTS/NOT EXISTS subquery condition
// ============================================

/**
 * EXISTS or NOT EXISTS subquery condition.
 * Uses table.column format for unambiguous column references.
 * 
 * @example
 * ```typescript
 * // EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
 * new DBExists('orders', [{ column: Order.user_id, value: parentRef(User.id) }], false);
 * ```
 * 
 * @internal
 */
export class DBExists extends DBToken {
  constructor(
    /** Target table name for subquery */
    public readonly tableName: string,
    /** WHERE conditions for subquery - accepts Column type directly */
    public readonly conditions: SubqueryCondition[],
    /** If true, use NOT EXISTS instead of EXISTS */
    public readonly notExists: boolean = false
  ) {
    super(null, notExists ? 'NOT EXISTS' : 'EXISTS');
  }

  /** Format column as table.column */
  private formatColumn(col: ColumnRef): string {
    return `${col.tableName}.${col.columnName}`;
  }

  /**
   * Compile to SQL with parameter binding
   */
  compile(params: unknown[], _key?: string): string {
    const whereParts: string[] = [];

    for (const cond of this.conditions) {
      const colRef = this.formatColumn(cond.column);
      if (cond.value instanceof DBParentRef) {
        // Correlated subquery: reference parent column
        whereParts.push(`${colRef} = ${cond.value.compile(params)}`);
      } else if (cond.value instanceof DBToken) {
        // Other DBToken types
        whereParts.push(cond.value.compile(params, colRef));
      } else if (cond.value === null) {
        whereParts.push(`${colRef} IS NULL`);
      } else if (Array.isArray(cond.value)) {
        // IN clause within subquery
        if (cond.value.length === 0) {
          whereParts.push('1 = 0');
        } else {
          const placeholders = cond.value.map(v => {
            params.push(v);
            return '?';
          });
          whereParts.push(`${colRef} IN (${placeholders.join(', ')})`);
        }
      } else {
        // Regular value
        params.push(cond.value);
        whereParts.push(`${colRef} = ?`);
      }
    }

    const whereClause = whereParts.length > 0 ? ` WHERE ${whereParts.join(' AND ')}` : '';
    const subquery = `SELECT 1 FROM ${this.tableName}${whereClause}`;

    return `${this.operator} (${subquery})`;
  }
}

