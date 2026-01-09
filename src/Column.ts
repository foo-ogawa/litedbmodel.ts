/**
 * litedbmodel - Column Type Definition
 *
 * Provides type-safe column references as callable functions with condition builder methods.
 * Column instances are callable - calling them returns the column name as a string,
 * which can be used as computed property keys in object literals.
 *
 * @example
 * ```typescript
 * // Call to get column name for computed property key
 * await User.findAll({ [User.id()]: 1 });
 *
 * // Use condition builder methods (spread syntax)
 * await User.findAll({ ...User.id.eq(1) });
 *
 * // Template literal also works
 * await User.findAll({ [`${User.id} > ?`]: 10 });
 * ```
 */

import { DBNotNullValue, dbTupleIn, DBCast, DBCastArray } from './DBValues';

// ============================================
// OrderColumn Class (Type-safe ORDER BY)
// ============================================

/** 
 * Sort direction for ORDER BY 
 * @internal
 */
export type SortDirection = 'ASC' | 'DESC';

/** 
 * NULLS position for ORDER BY
 * @internal
 */
export type NullsPosition = 'FIRST' | 'LAST' | null;

/**
 * Type-safe ORDER BY column reference.
 * Returned by `Column.desc()`, `Column.asc()` methods.
 *
 * @typeParam ModelType - The model class this column belongs to
 *
 * @example
 * ```typescript
 * // Single column
 * User.created_at.desc()
 *
 * // Multiple columns
 * [User.name.asc(), User.created_at.desc()]
 *
 * // With NULLS position
 * User.updated_at.descNullsLast()
 * ```
 * 
 * @internal
 */
export class OrderColumn<ModelType = unknown> {
  /** Brand for type discrimination */
  readonly _brand = 'OrderColumn' as const;

  constructor(
    /** The database column name */
    public readonly columnName: string,
    /** The model class name (for debugging) */
    public readonly modelName: string,
    /** Sort direction */
    public readonly direction: SortDirection,
    /** NULLS position */
    public readonly nulls: NullsPosition = null
  ) {}

  /**
   * Convert to SQL ORDER BY string
   */
  toString(): string {
    let result = `${this.columnName} ${this.direction}`;
    if (this.nulls) {
      result += ` NULLS ${this.nulls}`;
    }
    return result;
  }

  /** Phantom type for model association (compile-time only) */
  readonly __model?: ModelType;
}

/**
 * Type guard to check if a value is an OrderColumn instance.
 * @internal
 */
export function isOrderColumn(value: unknown): value is OrderColumn {
  return value instanceof OrderColumn || (
    typeof value === 'object' &&
    value !== null &&
    '_brand' in value &&
    (value as { _brand: string })._brand === 'OrderColumn'
  );
}

/**
 * OrderColumn type for a specific model.
 * Used for type-safe ORDER BY constraints.
 * @internal
 */
export type OrderColumnOf<Model> = OrderColumn<Model>;

/**
 * Type-safe ORDER BY specification (no raw strings allowed).
 * Use for DEFAULT_ORDER and relation order parameters.
 * @internal
 */
export type OrderSpec = OrderColumn | OrderColumn[];

/**
 * Convert OrderColumn or OrderColumn array to SQL string.
 * @internal
 */
export function orderToString(order: OrderSpec | string | null | undefined): string | undefined {
  if (!order) return undefined;
  if (typeof order === 'string') return order;
  if (Array.isArray(order)) {
    return order.map(o => o.toString()).join(', ');
  }
  return order.toString();
}

// ============================================
// Column Interface (Callable Function + Methods)
// ============================================

/**
 * Type-safe column reference as a callable function.
 *
 * @typeParam ValueType - The TypeScript type of the column value
 * @typeParam ModelType - The model class this column belongs to (for relation type safety)
 *
 * - Call `User.id()` to get the column name as a string (for computed property keys)
 * - Use methods like `User.id.eq(1)` for condition builders
 * - Use in template literals: `${User.id}` (calls toString())
 */
export interface Column<ValueType = unknown, ModelType = unknown> {
  /** Call to get column name as string (for computed property keys) */
  (): string;

  /** The database column name */
  readonly columnName: string;

  /** The property name on the model class (may differ from columnName) */
  readonly propertyName: string;

  /** The database table name */
  readonly tableName: string;

  /** The model class name (for debugging and static analysis) */
  readonly modelName: string;

  /** Brand for type discrimination - enables static analysis to distinguish from regular variables */
  readonly _brand: 'Column';

  /** SQL type for automatic casting in conditions (e.g., 'uuid') */
  readonly sqlCast?: string;

  /** Phantom type for model association (compile-time only, not used at runtime) */
  readonly __model?: ModelType;

  // ============================================
  // Condition Builder Methods
  // ============================================

  /**
   * Equal condition (column = value)
   * @example User.id.eq(1) → { id: 1 }
   */
  eq(value: ValueType): Record<string, ValueType | DBCast>;

  /**
   * Not equal condition (column != value)
   * @example User.status.ne('deleted') → { 'status != ?': 'deleted' }
   */
  ne(value: ValueType): Record<string, ValueType | DBCast>;

  /**
   * Greater than condition (column > value)
   * @example User.age.gt(18) → { 'age > ?': 18 }
   */
  gt(value: ValueType): Record<string, ValueType | DBCast>;

  /**
   * Greater than or equal condition (column >= value)
   * @example User.age.gte(18) → { 'age >= ?': 18 }
   */
  gte(value: ValueType): Record<string, ValueType | DBCast>;

  /**
   * Less than condition (column < value)
   * @example User.age.lt(65) → { 'age < ?': 65 }
   */
  lt(value: ValueType): Record<string, ValueType | DBCast>;

  /**
   * Less than or equal condition (column <= value)
   * @example User.age.lte(65) → { 'age <= ?': 65 }
   */
  lte(value: ValueType): Record<string, ValueType | DBCast>;

  /**
   * LIKE condition (column LIKE pattern)
   * @example User.name.like('%test%') → { 'name LIKE ?': '%test%' }
   */
  like(pattern: string): Record<string, string>;

  /**
   * NOT LIKE condition (column NOT LIKE pattern)
   * @example User.name.notLike('%test%') → { 'name NOT LIKE ?': '%test%' }
   */
  notLike(pattern: string): Record<string, string>;

  /**
   * ILIKE condition (case-insensitive LIKE, PostgreSQL specific)
   * @example User.name.ilike('%TEST%') → { 'name ILIKE ?': '%TEST%' }
   */
  ilike(pattern: string): Record<string, string>;

  /**
   * BETWEEN condition (column BETWEEN from AND to)
   * @example User.age.between(18, 65) → { 'age BETWEEN ? AND ?': [18, 65] }
   */
  between(from: ValueType, to: ValueType): Record<string, [ValueType, ValueType]>;

  /**
   * IN condition (column IN (values))
   * Note: Arrays are automatically converted to IN clause by litedbmodel
   * @example User.status.in(['active', 'pending']) → { status: ['active', 'pending'] }
   */
  in(values: ValueType[]): Record<string, ValueType[] | DBCastArray>;

  /**
   * NOT IN condition (column NOT IN (values))
   * @example User.status.notIn(['deleted', 'banned'])
   */
  notIn(values: ValueType[]): Record<string, ValueType[] | DBCastArray>;

  /**
   * IS NULL condition
   * @example User.deleted_at.isNull() → { deleted_at: null }
   */
  isNull(): Record<string, null>;

  /**
   * IS NOT NULL condition
   * @example User.email.isNotNull() → { email: DBNotNullValue }
   */
  isNotNull(): Record<string, DBNotNullValue>;

  // ============================================
  // Order By Helpers (return OrderColumn for type safety)
  // ============================================

  /**
   * Ascending order
   * @example User.created_at.asc() → OrderColumn('created_at', 'ASC')
   */
  asc(): OrderColumn<ModelType>;

  /**
   * Descending order
   * @example User.created_at.desc() → OrderColumn('created_at', 'DESC')
   */
  desc(): OrderColumn<ModelType>;

  /**
   * Ascending order with NULLS FIRST
   * @example User.updated_at.ascNullsFirst() → OrderColumn with NULLS FIRST
   */
  ascNullsFirst(): OrderColumn<ModelType>;

  /**
   * Ascending order with NULLS LAST
   * @example User.updated_at.ascNullsLast() → OrderColumn with NULLS LAST
   */
  ascNullsLast(): OrderColumn<ModelType>;

  /**
   * Descending order with NULLS FIRST
   * @example User.updated_at.descNullsFirst() → OrderColumn with NULLS FIRST
   */
  descNullsFirst(): OrderColumn<ModelType>;

  /**
   * Descending order with NULLS LAST
   * @example User.updated_at.descNullsLast() → OrderColumn with NULLS LAST
   */
  descNullsLast(): OrderColumn<ModelType>;

  // ============================================
  // String Conversion
  // ============================================

  /**
   * Returns column name (for template literals)
   * @example `${User.id}` → 'id'
   */
  toString(): string;
}

// ============================================
// Column Factory Function
// ============================================

/**
 * Create a callable Column instance.
 *
 * @typeParam ValueType - The TypeScript type of the column value
 * @typeParam ModelType - The model class this column belongs to
 * @param columnName - The database column name
 * @param tableName - The database table name
 * @param modelName - The model class name (for debugging)
 * @param propertyName - The property name on the model (defaults to columnName)
 * @param sqlCast - SQL type for automatic casting in conditions (e.g., 'uuid')
 * @returns A callable Column function with condition builder methods
 *
 * @example
 * ```typescript
 * const id = createColumn<number, User>('id', 'users', 'User');
 *
 * // As computed property key
 * const conditions = { [id()]: 1 };
 *
 * // With condition builder
 * const conditions = { ...id.eq(1) };
 * ```
 * 
 * @internal
 */
export function createColumn<ValueType, ModelType = unknown>(
  columnName: string,
  tableName: string,
  modelName: string,
  propertyName?: string,
  sqlCast?: string
): Column<ValueType, ModelType> {
  // The callable function itself - returns column name as string
  const fn = function (): string {
    return columnName;
  } as Column<ValueType, ModelType>;

  // Property name defaults to column name if not provided
  const propName = propertyName ?? columnName;

  // Add readonly properties
  Object.defineProperty(fn, 'columnName', {
    value: columnName,
    writable: false,
    enumerable: true,
  });
  Object.defineProperty(fn, 'propertyName', {
    value: propName,
    writable: false,
    enumerable: true,
  });
  Object.defineProperty(fn, 'tableName', {
    value: tableName,
    writable: false,
    enumerable: true,
  });
  Object.defineProperty(fn, 'modelName', {
    value: modelName,
    writable: false,
    enumerable: true,
  });
  Object.defineProperty(fn, '_brand', {
    value: 'Column',
    writable: false,
    enumerable: true,
  });
  Object.defineProperty(fn, 'sqlCast', {
    value: sqlCast,
    writable: false,
    enumerable: true,
  });
  // __model is a phantom type - not set at runtime, only used for TypeScript type checking

  // Condition builder methods - wrap with DBCast if sqlCast is specified
  if (sqlCast) {
    // With type casting (e.g., UUID columns)
    fn.eq = (value: ValueType) => ({ [columnName]: new DBCast(value, sqlCast, '=') });
    fn.ne = (value: ValueType) => ({ [columnName]: new DBCast(value, sqlCast, '!=') });
    fn.gt = (value: ValueType) => ({ [columnName]: new DBCast(value, sqlCast, '>') });
    fn.gte = (value: ValueType) => ({ [columnName]: new DBCast(value, sqlCast, '>=') });
    fn.lt = (value: ValueType) => ({ [columnName]: new DBCast(value, sqlCast, '<') });
    fn.lte = (value: ValueType) => ({ [columnName]: new DBCast(value, sqlCast, '<=') });
    fn.in = (values: ValueType[]) => ({ [columnName]: new DBCastArray(values, sqlCast) });
    fn.notIn = (values: ValueType[]) => ({ [`${columnName} NOT`]: new DBCastArray(values, sqlCast) });
  } else {
    // Without type casting (default behavior)
    fn.eq = (value: ValueType) => ({ [columnName]: value });
    fn.ne = (value: ValueType) => ({ [`${columnName} != ?`]: value });
    fn.gt = (value: ValueType) => ({ [`${columnName} > ?`]: value });
    fn.gte = (value: ValueType) => ({ [`${columnName} >= ?`]: value });
    fn.lt = (value: ValueType) => ({ [`${columnName} < ?`]: value });
    fn.lte = (value: ValueType) => ({ [`${columnName} <= ?`]: value });
    fn.in = (values: ValueType[]) => ({ [columnName]: values });
    fn.notIn = (values: ValueType[]) => ({ [`${columnName} NOT IN (?)`]: values });
  }
  
  // String operations - no sqlCast needed (always string comparison)
  fn.like = (pattern: string) => ({ [`${columnName} LIKE ?`]: pattern });
  fn.notLike = (pattern: string) => ({
    [`${columnName} NOT LIKE ?`]: pattern,
  });
  fn.ilike = (pattern: string) => ({ [`${columnName} ILIKE ?`]: pattern });
  fn.between = (from: ValueType, to: ValueType) => ({
    [`${columnName} BETWEEN ? AND ?`]: [from, to] as [ValueType, ValueType],
  });
  fn.isNull = () => ({ [columnName]: null });
  fn.isNotNull = () => ({ [columnName]: new DBNotNullValue() });

  // Order by helpers - return OrderColumn for type safety
  fn.asc = () => new OrderColumn<ModelType>(columnName, modelName, 'ASC');
  fn.desc = () => new OrderColumn<ModelType>(columnName, modelName, 'DESC');
  fn.ascNullsFirst = () => new OrderColumn<ModelType>(columnName, modelName, 'ASC', 'FIRST');
  fn.ascNullsLast = () => new OrderColumn<ModelType>(columnName, modelName, 'ASC', 'LAST');
  fn.descNullsFirst = () => new OrderColumn<ModelType>(columnName, modelName, 'DESC', 'FIRST');
  fn.descNullsLast = () => new OrderColumn<ModelType>(columnName, modelName, 'DESC', 'LAST');

  // String conversion for template literals
  fn.toString = () => columnName;

  return fn;
}

// ============================================
// Type Guard
// ============================================

/**
 * Type guard to check if a value is a Column instance.
 *
 * @example
 * ```typescript
 * if (isColumn(value)) {
 *   console.log(value.columnName);
 * }
 * ```
 * 
 * @internal
 */
export function isColumn(value: unknown): value is Column {
  return (
    typeof value === 'function' &&
    '_brand' in value &&
    (value as { _brand: string })._brand === 'Column'
  );
}

// ============================================
// Utility Types for Auto-generating Column Types
// ============================================

/**
 * Auto-generate Column types from model instance properties.
 * Excludes functions and private properties (starting with _).
 * 
 * **Note:** Use `Model.asModel()` instead of manually using this type.
 * 
 * @typeParam T - The model instance type
 *
 * @example
 * ```typescript
 * @model('users')
 * class User extends DBModel {
 *   @column() id?: number;
 *   @column() name?: string;
 *   @column() is_active?: boolean;
 * }
 *
 * // Use asModel() to get type-safe Column references
 * const UserModel = User.asModel();
 *
 * // Now UserModel.id, UserModel.name work as Column references
 * await UserModel.find([[UserModel.name, 'John']]);
 * await UserModel.find([[UserModel.id, 1]]);
 * ```
 * 
 * @internal
 */
export type ColumnsOf<T> = {
  -readonly [K in keyof T as T[K] extends Function
    ? never
    : K extends `_${string}`
      ? never
      : K]-?: Column<NonNullable<T[K]>, T>;
};

/**
 * Extract the model type from a Column.
 * Useful for relation type constraints.
 *
 * @example
 * ```typescript
 * type UserModel = ModelOfColumn<typeof User.id>; // UserModel
 * ```
 * 
 * @internal
 */
export type ModelOfColumn<C> = C extends Column<unknown, infer M> ? M : never;

/**
 * Column type for a specific model.
 * Used in relation method signatures for type safety.
 *
 * @example
 * ```typescript
 * // Only accepts columns from User model
 * function process(col: ColumnOf<User>) { ... }
 * ```
 * 
 * @internal
 */
export type ColumnOf<Model> = Column<unknown, Model>;

/**
 * Helper to convert Column array to column names string array.
 * Useful for getPkeyColumns and similar methods.
 * @internal
 */
export function columnsToNames(columns: Column[]): string[] {
  return columns.map(c => c.columnName);
}

// ============================================
// Type-Safe Builder Classes for Dynamic CRUD
// ============================================

/**
 * Type-safe builder for update/create value pairs.
 * Use array literals for static values, builder for dynamic construction.
 *
 * @example
 * ```typescript
 * // Static values - use array literal directly (type-checked)
 * await User.create([
 *   [User.name, 'John'],
 *   [User.email, 'john@example.com'],
 * ]);
 *
 * // Dynamic construction - use Values builder
 * const updates = new Values<User>();
 * if (body.name) updates.add(User.name, body.name);
 * if (body.email) updates.add(User.email, body.email);
 * await User.update([[User.id, id]], updates.build());
 *
 * // Mixed: initial values + dynamic additions
 * const values = new Values<User>([
 *   [User.created_at, new Date()],
 * ]);
 * if (body.name) values.add(User.name, body.name);
 * await User.create(values.build());
 * ```
 * 
 * @category Column
 */
export class Values<Model> {
  private pairs: [Column<any, any>, unknown][] = [];

  /**
   * Create a Values builder, optionally with initial pairs.
   */
  constructor(initial?: readonly (readonly [Column<any, Model>, unknown])[]) {
    if (initial) {
      this.pairs = initial.map(([col, val]) => [col, val]);
    }
  }

  /**
   * Add a type-safe column-value pair.
   */
  add<V>(column: Column<V, Model>, value: V | null | undefined): this {
    this.pairs.push([column, value]);
    return this;
  }

  /**
   * Build the final array for use with update/create.
   */
  build(): readonly (readonly [Column<any, any>, unknown])[] {
    return this.pairs;
  }

  /**
   * Get the number of pairs.
   */
  get length(): number {
    return this.pairs.length;
  }
}

/**
 * Type-safe builder for query conditions.
 * Use array literals for static conditions, builder for dynamic construction.
 *
 * @example
 * ```typescript
 * // Static conditions - use array literal directly
 * const users = await User.find([
 *   [User.deleted, false],
 *   [User.is_active, true],
 * ]);
 *
 * // Dynamic construction - use Conditions builder
 * const where = new Conditions<User>();
 * where.add(User.deleted, false);
 * if (query.name) where.addRaw(`${User.name} LIKE ?`, `%${query.name}%`);
 * const users = await User.find(where.build());
 *
 * // Mixed: initial conditions + dynamic additions
 * const where = new Conditions<User>([
 *   [User.deleted, false],
 * ]);
 * if (query.active) where.add(User.is_active, true);
 * ```
 * 
 * @category Column
 */
export class Conditions<Model> {
  private conds: CondElement[] = [];

  /**
   * Create a Conditions builder, optionally with initial conditions.
   */
  constructor(initial?: Conds) {
    if (initial) {
      this.conds = [...initial];
    }
  }

  /**
   * Add a type-safe column equality condition.
   */
  add<V>(column: Column<V, Model>, value: V | null | undefined): this {
    this.conds.push([column, value] as Cond);
    return this;
  }

  /**
   * Add a raw condition with template literal (e.g., `${User.age} > ?`).
   */
  addRaw(condition: string, value?: unknown): this {
    if (value !== undefined) {
      this.conds.push([condition, value] as Cond);
    } else {
      this.conds.push([condition] as Cond);
    }
    return this;
  }

  /**
   * Add an OR condition group.
   */
  or(...condGroups: readonly Conds[]): this {
    this.conds.push({ _type: 'or', conditions: condGroups });
    return this;
  }

  /**
   * Build the final array for use with find/count/delete.
   */
  build(): Conds {
    return this.conds;
  }

  /**
   * Get the number of conditions.
   */
  get length(): number {
    return this.conds.length;
  }
}

// ============================================
// Type-Safe Tuple API
// ============================================

/**
 * Type-safe column-value pair tuple.
 * The value type V is inferred from the Column's value type.
 *
 * @example
 * ```typescript
 * // Type-safe: only accepts string for User.name
 * [User.name, 'John']      // ✅ OK
 * [User.name, 123]         // ❌ Type error
 * [User.is_active, true]   // ✅ OK
 * [User.is_active, 'yes']  // ❌ Type error
 * ```
 */
/**
 * A single Column-Value tuple with type checking.
 * The value must match the Column's generic type V.
 * @internal
 */
export type CV<C extends Column<any, any>> =
  C extends Column<infer V, any> ? readonly [C, V | null | undefined | SkipType] : never;

/**
 * Validate an array of column-value pairs at compile time.
 * Each element must have a value type matching its Column's value type.
 * Allows null/undefined for nullable fields, and SKIP for conditional fields.
 *
 * @example
 * ```typescript
 * // All pairs are type-checked:
 * await User.create([
 *   [User.name, 'John'],           // ✅ OK: string
 *   [User.age, 25],                // ✅ OK: number
 *   [User.bio, null],              // ✅ OK: null for nullable fields
 *   [User.email, body.email ?? SKIP], // ✅ OK: SKIP if undefined
 *   // [User.name, 123],           // ❌ Compile error (type mismatch)
 * ]);
 * ```
 * 
 * @internal
 */
export type CVs<T extends readonly (readonly [Column<any, any>, any])[]> = {
  [K in keyof T]: T[K] extends readonly [infer C, any]
    ? C extends Column<infer V, any>
      ? readonly [C, V | null | undefined | SkipType]
      : never
    : never;
};

// ============================================
// SKIP Sentinel Value
// ============================================

/**
 * Sentinel value to skip a field in create/update operations.
 * Use with conditional expressions to keep code as expressions instead of statements.
 *
 * @example
 * ```typescript
 * // Instead of:
 * const updates = new Values<User>();
 * if (body.name !== undefined) updates.add(User.name, body.name);
 * if (body.email !== undefined) updates.add(User.email, body.email);
 *
 * // You can write:
 * await User.update(conds, [
 *   [User.name, body.name ?? SKIP],
 *   [User.email, body.email ?? SKIP],
 * ]);
 * ```
 */
export const SKIP = Symbol.for('litedbmodel.SKIP');
export type SkipType = typeof SKIP;

/**
 * Convert tuple pairs to a record object.
 * Filters out pairs where value is SKIP.
 * Used internally by create/update methods.
 * @internal
 */
export function pairsToRecord(
  pairs: readonly (readonly [Column<any, any>, unknown])[]
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (const [col, value] of pairs) {
    if (value !== SKIP) {
      result[col.columnName] = value;
    }
  }
  return result;
}

// ============================================
// Condition Tuple Types
// ============================================

/**
 * Single condition tuple (legacy, accepts any Column).
 * @deprecated Use CondOf<M> for type-safe conditions
 * @internal
 */
export type Cond = 
  | readonly [Column<any, any>, unknown]
  | readonly [Column<any, any>[], unknown[][]]  // Composite key IN: [[Col1, Col2], [[v1, v2], [v3, v4]]]
  | readonly [string, unknown]
  | readonly [string];

/**
 * Extract instance type from a model class type.
 * If M is already an instance type, returns M unchanged.
 * @internal
 */
type ModelInstance<M> = M extends new (...args: any[]) => infer I ? I : M;

/**
 * Type-safe single condition tuple for a specific model.
 * - [Column, value] for equality conditions (Column must belong to model M)
 * - [string, value] for custom conditions with parameter
 * - [string] for conditions without parameter (IS NULL, etc.)
 *
 * @typeParam M - The model class type (typeof Model) or instance type (Model)
 *
 * @example
 * ```typescript
 * [User.id, 1]                      // id = 1
 * [`${User.name} LIKE ?`, '%test%'] // name LIKE '%test%'
 * [`${User.age} > ?`, 18]           // age > 18
 * [`${User.deleted_at} IS NULL`]    // deleted_at IS NULL (no value)
 * ```
 * 
 * @internal
 */
export type CondOf<M> = 
  | readonly [Column<any, ModelInstance<M>>, unknown]
  | readonly [Column<any, ModelInstance<M>>[], unknown[][]]  // Composite key IN
  | readonly [string, unknown]
  | readonly [string];

/**
 * OR condition marker (legacy)
 * @deprecated Use OrCondOf<M> for type-safe OR conditions
 * @internal
 */
export interface OrCond {
  readonly _type: 'or';
  readonly conditions: readonly Conds[];
}

/**
 * Type-safe OR condition marker for a specific model.
 * @typeParam M - The model class type
 * @internal
 */
export interface OrCondOf<M> {
  readonly _type: 'or';
  readonly conditions: readonly CondsOf<M>[];
}

/**
 * Condition element (single condition, OR group, or SKIP) - legacy
 * @deprecated Use CondElementOf<M> for type-safe conditions
 * @internal
 */
export type CondElement = Cond | OrCond | SkipType;

/**
 * Type-safe condition element for a specific model.
 * @typeParam M - The model class type
 * @internal
 */
export type CondElementOf<M> = CondOf<M> | OrCondOf<M> | SkipType;

/**
 * Array of condition elements (legacy).
 * @deprecated Use CondsOf<M> for type-safe conditions
 * @internal
 */
export type Conds = readonly CondElement[];

/**
 * Type-safe array of condition elements for a specific model.
 * Use SKIP to conditionally exclude conditions.
 *
 * @typeParam M - The model class type
 *
 * @example
 * ```typescript
 * await User.find([
 *   [User.deleted, false],
 *   [`${User.name} LIKE ?`, query.name ? `%${query.name}%` : SKIP],
 *   query.role ? User.or([[User.role, 'admin']], [[User.role, 'mod']]) : SKIP,
 * ]);
 * ```
 * 
 * @internal
 */
export type CondsOf<M> = readonly CondElementOf<M>[];

/**
 * Create a type-safe OR condition.
 * Use `Model.or()` instead of calling this directly.
 * @internal
 */
export function createOrCond<M>(condGroups: readonly CondsOf<M>[]): OrCondOf<M> {
  return { _type: 'or', conditions: condGroups };
}

/**
 * Check if a condition element is an OR condition
 * @internal
 */
export function isOrCond(cond: CondElement | CondElementOf<any>): cond is OrCond | OrCondOf<any> {
  return typeof cond === 'object' && cond !== null && '_type' in cond && cond._type === 'or';
}

/**
 * Convert a single condition to key-value pair.
 * Returns null if the condition should be skipped.
 */
function condToKeyValue(cond: Cond): [string, unknown] | null {
  if (cond.length === 1) {
    // No value condition like [${User.deleted_at} IS NULL]
    return [cond[0] as string, true]; // Use true as placeholder for DBConditions
  }
  const [keyOrCol, value] = cond;
  // Skip if value is SKIP
  if (value === SKIP) {
    return null;
  }
  
  // Handle composite key IN condition: [[Col1, Col2], [[v1, v2], [v3, v4]]]
  if (Array.isArray(keyOrCol)) {
    const columns = keyOrCol as Column<any, any>[];
    const tuples = value as unknown[][];
    const columnNames = columns.map(col => col.columnName);
    // Use special key for tuple IN, the actual SQL is generated by dbTupleIn
    return ['__tuple__', dbTupleIn(columnNames, tuples)];
  }
  
  if (typeof keyOrCol === 'string') {
    return [keyOrCol, value];
  }
  return [keyOrCol.columnName, value];
}

/**
 * Convert condition tuples to a ConditionObject.
 * Handles regular conditions, OR conditions, and SKIP.
 * @internal
 */
export function condsToRecord(
  conditions: Conds
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  
  for (const cond of conditions) {
    // Skip SKIP elements
    if (cond === SKIP) {
      continue;
    }
    if (isOrCond(cond)) {
      // OR condition: convert each group and wrap with DBOrConditions
      const orGroups = cond.conditions.map(group => condsToRecord(group as Conds));
      // Use special key for OR conditions (will be processed by DBConditions)
      result['__or__'] = orGroups;
    } else {
      const kv = condToKeyValue(cond as Cond);
      if (kv === null) continue;  // Skip conditions with SKIP value
      const [key, value] = kv;
      result[key] = value;
    }
  }
  return result;
}
