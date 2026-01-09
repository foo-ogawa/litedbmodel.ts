/**
 * litedbmodel - Decorators for Model Definition
 *
 * Provides decorators to simplify model definition by automatically:
 * - Setting TABLE_NAME from @model('table_name') decorator
 * - Creating static Column properties for type-safe column references
 * - Generating typeCastFromDB() for automatic type conversion
 * - Creating relation getters from @hasMany, @belongsTo, @hasOne decorators
 */

import 'reflect-metadata';
import { type Column, type OrderSpec, createColumn, orderToString, type Conds, condsToRecord } from './Column';
import {
  castToBoolean,
  castToDatetime,
  castToIntegerArray,
  castToNumericArray,
  castToStringArray,
  castToBooleanArray,
  castToDatetimeArray,
  castToJson,
} from './TypeCast';
import type { ModelOptions } from './types';

// ============================================
// Metadata Keys
// ============================================

const COLUMNS_KEY = Symbol('litedbmodel:columns');
const RELATIONS_KEY = Symbol('litedbmodel:relations');

// ============================================
// Types
// ============================================

/** Type cast function signature */
type TypeCastFn = (value: unknown) => unknown;

/** Serialize function signature (converts JS value to DB value) */
type SerializeFn = (value: unknown) => unknown;

/** 
 * Column metadata stored by decorators 
 * @internal
 */
export interface ColumnMeta {
  columnName: string;
  typeCast?: TypeCastFn;
  serialize?: SerializeFn;
  primaryKey?: boolean;
  /** SQL type for automatic casting in conditions (e.g., 'uuid') */
  sqlCast?: string;
}

// ============================================
// Relation Types
// ============================================

/** 
 * Relation type 
 * @internal
 */
export type RelationType = 'hasMany' | 'belongsTo' | 'hasOne';

/** 
 * Key pair: [sourceKey, targetKey] 
 * @internal
 */
export type KeyPair = readonly [Column<unknown, unknown>, Column<unknown, unknown>];

/** 
 * Composite key pairs: [[sourceKey1, targetKey1], [sourceKey2, targetKey2], ...] 
 * @internal
 */
export type CompositeKeyPairs = readonly KeyPair[];

/** 
 * Factory function that returns key pair(s) 
 * @internal
 */
export type KeysFactory = () => KeyPair | CompositeKeyPairs;

/** 
 * Relation options (order, where, limit) 
 * @internal
 */
export interface RelationDecoratorOptions {
  /** Order by specification */
  order?: () => OrderSpec;
  /** Additional filter conditions */
  where?: () => Conds;
  /**
   * SQL LIMIT for hasMany relations.
   * Limits the number of records returned per parent key.
   * Uses LATERAL JOIN (PostgreSQL) or ROW_NUMBER (MySQL/SQLite) for efficient batch loading.
   * @example
   * ```typescript
   * @hasMany(() => [User.id, Post.author_id], {
   *   limit: 10,  // Only load 10 posts per user
   *   order: () => Post.created_at.desc(),
   * })
   * declare recentPosts: Promise<Post[]>;
   * ```
   */
  limit?: number;
  /**
   * Hard limit for hasMany relations (throws exception if exceeded).
   * Overrides the global hasManyHardLimit setting.
   * Set to null to disable the limit check for this relation.
   * @example
   * ```typescript
   * @hasMany(() => [User.id, Post.author_id], {
   *   hardLimit: 500,  // Throw if user has > 500 posts
   * })
   * declare posts: Promise<Post[]>;
   * 
   * @hasMany(() => [User.id, Log.user_id], {
   *   hardLimit: null,  // Allow unlimited logs
   * })
   * declare logs: Promise<Log[]>;
   * ```
   */
  hardLimit?: number | null;
}

/** 
 * Relation metadata stored by decorators 
 * @internal
 */
export interface RelationMeta {
  propertyKey: string;
  type: RelationType;
  keysFactory: KeysFactory;
  options?: RelationDecoratorOptions;
}

// ============================================
// Internal Helper
// ============================================

/**
 * Infer type cast function from design:type metadata
 * Returns undefined if type cannot be inferred (Array, Object, etc.)
 * Note: All type casts preserve null (DB NULL -> JS null)
 */
function inferTypeCastFromDesignType(
  target: object,
  propertyKey: string
): TypeCastFn | undefined {
  const designType = Reflect.getMetadata('design:type', target, propertyKey);
  
  if (!designType) return undefined;
  
  switch (designType) {
    case Boolean:
      return (v) => {
        if (v === undefined) return undefined;
        return castToBoolean(v);
      };
    case Date:
      return (v) => {
        if (v === undefined) return undefined;
        return castToDatetime(v);
      };
    case Number:
      return (v) => {
        if (v === undefined) return undefined;
        if (v === null) return null;
        const n = Number(v);
        return isNaN(n) ? null : n;
      };
    case BigInt:
      return (v) => {
        if (v === undefined) return undefined;
        if (v === null) return null;
        try {
          return BigInt(v as string | number);
        } catch {
          return null;
        }
      };
    // Array, Object, String, and other types require explicit specification
    default:
      return undefined;
  }
}

/** Options for registerColumn */
interface RegisterColumnOptions {
  columnName: string;
  typeCast?: TypeCastFn;
  serialize?: SerializeFn;
  skipAutoInfer?: boolean;
  primaryKey?: boolean;
  /** SQL type for automatic casting in conditions (e.g., 'uuid') */
  sqlCast?: string;
}

/**
 * Register column metadata on the model class
 */
function registerColumn(
  target: object,
  propertyKey: string,
  options: RegisterColumnOptions
): void {
  const constructor = target.constructor;

  // Get or create columns map
  const columns: Map<string, ColumnMeta> =
    Reflect.getMetadata(COLUMNS_KEY, constructor) || new Map();

  // Auto-infer type cast if not explicitly provided
  let finalTypeCast = options.typeCast;
  if (!finalTypeCast && !options.skipAutoInfer) {
    finalTypeCast = inferTypeCastFromDesignType(target, propertyKey);
  }

  columns.set(propertyKey, {
    columnName: options.columnName,
    typeCast: finalTypeCast,
    serialize: options.serialize,
    primaryKey: options.primaryKey,
    sqlCast: options.sqlCast,
  });

  Reflect.defineMetadata(COLUMNS_KEY, columns, constructor);
}

/** Options that can be passed to @column decorator */
export interface ColumnOptions {
  /** Custom column name (defaults to property name) */
  columnName?: string;
  /** Mark this column as part of the primary key */
  primaryKey?: boolean;
}

/**
 * Create a column decorator with optional type cast and serialize
 * @param typeCast - Function to convert DB value to JS value (read)
 * @param serialize - Function to convert JS value to DB value (write)
 * @param skipAutoInfer - If true, skip auto-inference even if typeCast is undefined
 * @param sqlCast - SQL type for automatic casting in conditions (e.g., 'uuid')
 */
function createColumnDecorator(
  typeCast?: TypeCastFn,
  serialize?: SerializeFn,
  skipAutoInfer = false,
  sqlCast?: string
) {
  return function (columnNameOrOptions?: string | ColumnOptions): PropertyDecorator {
    return function (target: object, propertyKey: string | symbol) {
      const propKey = String(propertyKey);
      // If typeCast is explicitly provided, skip auto-inference
      const shouldSkipAutoInfer = skipAutoInfer || typeCast !== undefined;
      
      // Parse options
      let columnName: string;
      let primaryKey: boolean | undefined;
      
      if (typeof columnNameOrOptions === 'string') {
        columnName = columnNameOrOptions;
      } else if (columnNameOrOptions) {
        columnName = columnNameOrOptions.columnName || propKey;
        primaryKey = columnNameOrOptions.primaryKey;
      } else {
        columnName = propKey;
      }
      
      registerColumn(target, propKey, {
        columnName,
        typeCast,
        serialize,
        skipAutoInfer: shouldSkipAutoInfer,
        primaryKey,
        sqlCast,
      });
    };
  };
}

// ============================================
// Serialize Functions (JS -> DB)
// ============================================

/** Serialize array to PostgreSQL array format {a,b,c} */
function serializeArray(val: unknown): unknown {
  if (val === null || val === undefined) return null;
  if (!Array.isArray(val)) return val;
  // PostgreSQL array format: {val1,val2,val3}
  const escaped = val.map(v => {
    if (v === null) return 'NULL';
    if (typeof v === 'string') {
      // Escape backslashes and quotes
      const esc = v.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
      // Quote if contains special chars
      if (v.includes(',') || v.includes('{') || v.includes('}') || v.includes('"') || v.includes(' ')) {
        return `"${esc}"`;
      }
      return esc;
    }
    return String(v);
  });
  return `{${escaped.join(',')}}`;
}

/** Serialize boolean array to PostgreSQL format */
function serializeBooleanArray(val: unknown): unknown {
  if (val === null || val === undefined) return null;
  if (!Array.isArray(val)) return val;
  const mapped = val.map(v => v === null ? 'NULL' : (v ? 't' : 'f'));
  return `{${mapped.join(',')}}`;
}

/** Serialize JSON to string */
function serializeJson(val: unknown): unknown {
  if (val === null || val === undefined) return null;
  if (typeof val === 'string') return val; // Already serialized
  return JSON.stringify(val);
}

// ============================================
// @column Decorator and Variants
// ============================================

/**
 * Column decorator for defining model properties.
 *
 * **Auto-inference**: For simple types (boolean, number, Date, bigint),
 * type conversion is automatically inferred from the TypeScript property type.
 * No need to use explicit variants like `@column.boolean()`.
 *
 * Auto-inferred types:
 * ```typescript
 * @column() id?: number;          // Auto: Number conversion
 * @column() name?: string;        // No conversion needed
 * @column() is_active?: boolean;  // Auto: Boolean conversion
 * @column() created_at?: Date;    // Auto: DateTime conversion
 * @column() large_id?: bigint;    // Auto: BigInt conversion
 * @column('custom_name') prop?: string;  // Custom column name
 * ```
 *
 * Explicit type conversion required (cannot be auto-inferred):
 * ```typescript
 * @column.stringArray() tags?: string[];           // Array element type unknown
 * @column.intArray() scores?: number[];            // Array element type unknown
 * @column.json<MyType>() data?: MyType;            // Generic type unknown
 * @column.date() birth_date?: Date;                // date vs datetime distinction
 * ```
 *
 * Note: The explicit variants (`@column.boolean()`, `@column.datetime()`, etc.)
 * still work and can be used when you want to be explicit about the conversion.
 * 
 * @category Decorators
 */
export const column = Object.assign(
  // Basic @column() - no type conversion
  createColumnDecorator(),
  {
    // ============================================
    // Primitive Types
    // ============================================

    /**
     * Boolean type conversion
     * Converts 't'/'f', 'true'/'false', 1/0 to boolean
     * Preserves null for nullable columns, undefined stays undefined
     * @example @column.boolean() is_active?: boolean;
     */
    boolean: (columnName?: string) =>
      createColumnDecorator((v) => {
        if (v === undefined) return undefined;
        return castToBoolean(v);
      })(columnName),

    /**
     * Number type conversion (from string)
     * Preserves null for nullable columns, undefined stays undefined
     * @example @column.number() amount?: number;
     */
    number: (columnName?: string) =>
      createColumnDecorator((v) => {
        if (v === undefined) return undefined;
        if (v === null) return null;
        const n = Number(v);
        return isNaN(n) ? null : n;
      })(columnName),

    /**
     * BigInt type conversion
     * Preserves null for nullable columns, undefined stays undefined
     * @example @column.bigint() large_id?: bigint;
     */
    bigint: (columnName?: string) =>
      createColumnDecorator((v) => {
        if (v === undefined) return undefined;
        if (v === null) return null;
        try {
          return BigInt(v as string | number);
        } catch {
          return null;
        }
      })(columnName),

    // ============================================
    // Date/Time Types
    // ============================================

    /**
     * DateTime type conversion (timestamp, timestamptz)
     * Preserves null for nullable columns, undefined stays undefined
     * @example @column.datetime() created_at?: Date;
     */
    datetime: (columnName?: string) =>
      createColumnDecorator((v) => {
        if (v === undefined) return undefined;
        return castToDatetime(v);
      })(columnName),

    /**
     * Date type conversion (date only, time set to 00:00:00)
     * Preserves null for nullable columns, undefined stays undefined
     * @example @column.date() birth_date?: Date;
     */
    date: (columnName?: string) =>
      createColumnDecorator((v) => {
        if (v === undefined) return undefined;
        if (v === null) return null;
        const dt = castToDatetime(v);
        if (dt) {
          dt.setHours(0, 0, 0, 0);
        }
        return dt;
      })(columnName),

    // ============================================
    // Array Types
    // ============================================

    /**
     * String array type conversion (text[])
     * Preserves null for nullable columns, undefined stays undefined
     * @example @column.stringArray() tags?: string[];
     */
    stringArray: (columnName?: string) =>
      createColumnDecorator(
        (v) => {
          if (v === undefined) return undefined;
          if (v === null) return null;
          return castToStringArray(v);
        },
        serializeArray
      )(columnName),

    /**
     * Integer array type conversion (integer[])
     * Preserves null for nullable columns, undefined stays undefined
     * @example @column.intArray() scores?: number[];
     */
    intArray: (columnName?: string) =>
      createColumnDecorator(
        (v) => {
          if (v === undefined) return undefined;
          if (v === null) return null;
          return castToIntegerArray(v);
        },
        serializeArray
      )(columnName),

    /**
     * Numeric array type conversion (numeric[], allows null elements)
     * Preserves null for nullable columns, undefined stays undefined
     * @example @column.numericArray() values?: (number | null)[];
     */
    numericArray: (columnName?: string) =>
      createColumnDecorator(
        (v) => {
          if (v === undefined) return undefined;
          if (v === null) return null;
          return castToNumericArray(v);
        },
        serializeArray
      )(columnName),

    /**
     * Boolean array type conversion (boolean[])
     * Preserves null for nullable columns, undefined stays undefined
     * @example @column.booleanArray() flags?: (boolean | null)[];
     */
    booleanArray: (columnName?: string) =>
      createColumnDecorator(
        (v) => {
          if (v === undefined) return undefined;
          if (v === null) return null;
          return castToBooleanArray(v);
        },
        serializeBooleanArray
      )(columnName),

    /**
     * DateTime array type conversion (timestamp[])
     * Preserves null for nullable columns, undefined stays undefined
     * @example @column.datetimeArray() event_dates?: (Date | null)[];
     */
    datetimeArray: (columnName?: string) =>
      createColumnDecorator(
        (v) => {
          if (v === undefined) return undefined;
          if (v === null) return null;
          return castToDatetimeArray(v);
        },
        // DateTime arrays serialize each Date to ISO string
        (val) => {
          if (val === null || val === undefined) return null;
          if (!Array.isArray(val)) return val;
          const mapped = val.map(v => v === null ? 'NULL' : (v instanceof Date ? v.toISOString() : String(v)));
          return `{${mapped.join(',')}}`;
        }
      )(columnName),

    // ============================================
    // JSON Types
    // ============================================

    /**
     * JSON/JSONB type conversion
     * Preserves null for nullable columns, undefined stays undefined
     * @example @column.json() metadata?: Record<string, unknown>;
     * @example @column.json<UserSettings>() settings?: UserSettings;
     */
    json: <T = Record<string, unknown>>(columnName?: string) =>
      createColumnDecorator(
        (v) => {
          if (v === undefined) return undefined;
          if (v === null) return null;
          return castToJson(v) as T;
        },
        serializeJson
      )(columnName),

    // ============================================
    // UUID Type (PostgreSQL)
    // ============================================

    /**
     * UUID type with automatic casting for PostgreSQL.
     * Automatically adds ::uuid cast to conditions and INSERT/UPDATE values.
     * Preserves null for nullable columns, undefined stays undefined.
     * 
     * @example 
     * ```typescript
     * @column.uuid() id?: string;
     * @column.uuid({ primaryKey: true }) id?: string;
     * 
     * // Conditions automatically cast to UUID:
     * await User.find([[User.id, 'uuid-string']]);
     * // → WHERE id = ?::uuid
     * 
     * // IN clauses also cast:
     * await User.find([[User.id, ['uuid1', 'uuid2']]]);
     * // → WHERE id IN (?::uuid, ?::uuid)
     * ```
     */
    uuid: (columnNameOrOptions?: string | ColumnOptions) =>
      createColumnDecorator(
        (v) => {
          if (v === undefined) return undefined;
          if (v === null) return null;
          // UUID values from DB are typically already strings
          return String(v);
        },
        undefined,  // No serialization needed - handled by sqlCast
        true,       // Skip auto-inference
        'uuid'      // SQL type for casting
      )(columnNameOrOptions),

    // ============================================
    // Custom Type Conversion
    // ============================================

    /**
     * Custom type conversion with user-provided function
     * @example @column.custom((v) => String(v).toUpperCase()) status?: string;
     * @example @column.custom((v) => v, (v) => JSON.stringify(v)) data?: MyType; // with serializer
     */
    custom: <T>(
      castFn: (value: unknown) => T,
      serializeFn?: SerializeFn,
      columnName?: string
    ) =>
      createColumnDecorator(castFn, serializeFn)(columnName),
  }
);

// ============================================
// Relation Decorators
// ============================================

/**
 * Register relation metadata on the model class
 */
function registerRelation(
  target: object,
  propertyKey: string,
  type: RelationType,
  keysFactory: KeysFactory,
  options?: RelationDecoratorOptions
): void {
  const constructor = target.constructor;

  // Get or create relations array
  const relations: RelationMeta[] =
    Reflect.getMetadata(RELATIONS_KEY, constructor) || [];

  relations.push({
    propertyKey,
    type,
    keysFactory,
    options,
  });

  Reflect.defineMetadata(RELATIONS_KEY, relations, constructor);
}

/**
 * HasMany relation decorator (1:N).
 * Defines a one-to-many relationship where this model has many related records.
 *
 * @param keys - Factory function returning [sourceKey, targetKey] or composite key pairs
 * @param options - Optional order and where clauses
 *
 * @example
 * ```typescript
 * // Single key relation
 * @hasMany(() => [User.id, Post.author_id])
 * declare posts: Promise<Post[]>;
 *
 * // With options
 * @hasMany(() => [User.id, Post.author_id], {
 *   order: () => Post.created_at.desc(),
 *   where: () => [[Post.is_deleted, false]],
 * })
 * declare activePosts: Promise<Post[]>;
 *
 * // Composite key relation
 * @hasMany(() => [
 *   [TenantUser.tenant_id, TenantPost.tenant_id],
 *   [TenantUser.id, TenantPost.author_id],
 * ])
 * declare posts: Promise<TenantPost[]>;
 * ```
 * 
 * @category Decorators
 */
export function hasMany(
  keys: KeysFactory,
  options?: RelationDecoratorOptions
): PropertyDecorator {
  return function (target: object, propertyKey: string | symbol) {
    registerRelation(target, String(propertyKey), 'hasMany', keys, options);
  };
}

/**
 * BelongsTo relation decorator (N:1).
 * Defines a many-to-one relationship where this model belongs to a parent record.
 *
 * @param keys - Factory function returning [sourceKey, targetKey] or composite key pairs
 * @param options - Optional order and where clauses
 *
 * @example
 * ```typescript
 * // Single key relation
 * @belongsTo(() => [Post.author_id, User.id])
 * declare author: Promise<User | null>;
 *
 * // Composite key relation
 * @belongsTo(() => [
 *   [TenantPost.tenant_id, TenantUser.tenant_id],
 *   [TenantPost.author_id, TenantUser.id],
 * ])
 * declare author: Promise<TenantUser | null>;
 * ```
 * 
 * @category Decorators
 */
export function belongsTo(
  keys: KeysFactory,
  options?: RelationDecoratorOptions
): PropertyDecorator {
  return function (target: object, propertyKey: string | symbol) {
    registerRelation(target, String(propertyKey), 'belongsTo', keys, options);
  };
}

/**
 * HasOne relation decorator (1:1).
 * Defines a one-to-one relationship where this model has one related record.
 *
 * @param keys - Factory function returning [sourceKey, targetKey] or composite key pairs
 * @param options - Optional order and where clauses
 *
 * @example
 * ```typescript
 * // Single key relation
 * @hasOne(() => [User.id, UserProfile.user_id])
 * declare profile: Promise<UserProfile | null>;
 *
 * // Composite key relation
 * @hasOne(() => [
 *   [TenantUser.tenant_id, TenantProfile.tenant_id],
 *   [TenantUser.id, TenantProfile.user_id],
 * ])
 * declare profile: Promise<TenantProfile | null>;
 * ```
 * 
 * @category Decorators
 */
export function hasOne(
  keys: KeysFactory,
  options?: RelationDecoratorOptions
): PropertyDecorator {
  return function (target: object, propertyKey: string | symbol) {
    registerRelation(target, String(propertyKey), 'hasOne', keys, options);
  };
}

/**
 * Get relation metadata from a model class
 * @internal
 */
export function getRelationMeta(modelClass: object): RelationMeta[] {
  return (
    (modelClass as { _relationMeta?: RelationMeta[] })._relationMeta ||
    Reflect.getMetadata(RELATIONS_KEY, modelClass) ||
    []
  );
}

// ============================================
// @model Class Decorator
// ============================================

/**
 * Model class decorator.
 *
 * Can be used with or without table name:
 * - `@model` - uses class name as table name (via TABLE_NAME)
 * - `@model('users')` - sets TABLE_NAME to 'users'
 *
 * Automatically:
 * 1. Sets static TABLE_NAME property (if table name provided)
 * 2. Creates static Column properties for each @column decorated property
 * 3. Generates typeCastFromDB() method from @column type conversion settings
 * 4. Creates relation getters from @hasMany, @belongsTo, @hasOne decorators
 *
 * @example
 * ```typescript
 * @model('users')
 * class User extends DBModel {
 *   @column() id?: number;
 *   @column() name?: string;
 *   @column.boolean() is_active?: boolean;
 *   @column.datetime() created_at?: Date;
 *
 *   @hasMany(() => [User.id, Post.author_id])
 *   declare posts: Promise<Post[]>;
 * }
 *
 * // Usage - call column to get name as string for computed property key
 * await User.findAll({ [User.id()]: 1 });
 *
 * // Or use condition builders with spread
 * await User.findAll({ ...User.is_active.eq(true) });
 *
 * // Access relations
 * const user = await User.findOne([[User.id, 1]]);
 * const posts = await user.posts;  // Batch loads with other users in context
 * ```
 * 
 * @category Decorators
 */
// Overload 1: @model (without arguments)
export function model<T extends { new (...args: unknown[]): object }>(
  constructor: T
): T;
// Overload 2: @model('table_name')
export function model(
  tableName: string
): <T extends { new (...args: unknown[]): object }>(constructor: T) => T;
// Overload 3: @model('table_name', options)
export function model(
  tableName: string,
  options: ModelOptions
): <T extends { new (...args: unknown[]): object }>(constructor: T) => T;
// Implementation
export function model<T extends { new (...args: unknown[]): object }>(
  tableNameOrConstructor: string | T,
  options?: ModelOptions
): T | (<U extends { new (...args: unknown[]): object }>(constructor: U) => U) {
  // Called as @model('table_name') or @model('table_name', options)
  if (typeof tableNameOrConstructor === 'string') {
    const tableName = tableNameOrConstructor;
    return function <U extends { new (...args: unknown[]): object }>(
      constructor: U
    ): U {
      return applyModelDecorator(constructor, tableName, options);
    };
  }

  // Called as @model (without parentheses or arguments)
  return applyModelDecorator(tableNameOrConstructor);
}

/**
 * Check if keys are composite (array of pairs) or single pair
 * Single pair: [sourceColumn, targetColumn]
 * Composite: [[sourceCol1, targetCol1], [sourceCol2, targetCol2], ...]
 */
function isCompositeKeys(keys: KeyPair | CompositeKeyPairs): keys is CompositeKeyPairs {
  // If first element is an array, it's composite (array of pairs)
  // If first element is a Column (function), it's a single pair
  return Array.isArray(keys[0]);
}

/**
 * Parse key pair(s) into source and target key arrays
 */
function parseKeys(keys: KeyPair | CompositeKeyPairs): {
  sourceKeys: string[];
  targetKeys: string[];
  targetModelName: string;
} {
  if (isCompositeKeys(keys)) {
    // Composite keys: [[sourceKey1, targetKey1], [sourceKey2, targetKey2], ...]
    const sourceKeys = keys.map(pair => pair[0].columnName);
    const targetKeys = keys.map(pair => pair[1].columnName);
    const targetModelName = keys[0][1].modelName;
    return { sourceKeys, targetKeys, targetModelName };
  } else {
    // Single key pair: [sourceKey, targetKey]
    const [sourceKey, targetKey] = keys;
    return {
      sourceKeys: [sourceKey.columnName],
      targetKeys: [targetKey.columnName],
      targetModelName: targetKey.modelName,
    };
  }
}

/**
 * Internal function to apply the model decorator
 */
function applyModelDecorator<T extends { new (...args: unknown[]): object }>(
  constructor: T,
  tableName?: string,
  options?: ModelOptions
): T {
  const columns: Map<string, ColumnMeta> =
    Reflect.getMetadata(COLUMNS_KEY, constructor) || new Map();
  const relations: RelationMeta[] =
    Reflect.getMetadata(RELATIONS_KEY, constructor) || [];
  const modelName = constructor.name;

  // 0. Set TABLE_NAME if provided
  if (tableName) {
    Object.defineProperty(constructor, 'TABLE_NAME', {
      value: tableName,
      writable: false,
      enumerable: true,
      configurable: false,
    });
  }

  // 0.1 Apply model options (order, filter, select, updateTable, group)
  if (options) {
    if (options.order) {
      const orderFn = options.order;
      Object.defineProperty(constructor, 'DEFAULT_ORDER', {
        get: () => orderFn(),
        enumerable: true,
        configurable: false,
      });
    }
    if (options.filter) {
      const filterFn = options.filter;
      Object.defineProperty(constructor, 'FIND_FILTER', {
        get: () => filterFn(),
        enumerable: true,
        configurable: false,
      });
    }
    if (options.select !== undefined) {
      Object.defineProperty(constructor, 'SELECT_COLUMN', {
        value: options.select,
        writable: false,
        enumerable: true,
        configurable: false,
      });
    }
    if (options.updateTable !== undefined) {
      Object.defineProperty(constructor, 'UPDATE_TABLE_NAME', {
        value: options.updateTable,
        writable: false,
        enumerable: true,
        configurable: false,
      });
    }
    if (options.group) {
      const groupFn = options.group;
      Object.defineProperty(constructor, 'DEFAULT_GROUP', {
        get: () => groupFn(),
        enumerable: true,
        configurable: false,
      });
    }
  }

  // 1. Add static Column properties (callable functions)
  // Use tableName if provided, otherwise derive from model name (lowercase)
  const effectiveTableName = tableName ?? modelName.toLowerCase();
  for (const [propKey, meta] of columns) {
    Object.defineProperty(constructor, propKey, {
      value: createColumn(meta.columnName, effectiveTableName, modelName, propKey, meta.sqlCast),
      writable: false,
      enumerable: true,
      configurable: false,
    });
  }

  // 2. Pre-filter columns with typeCast for faster DB reads (optimization)
  const typeCastColumns: Array<[string, TypeCastFn]> = [];
  for (const [propKey, meta] of columns) {
    if (meta.typeCast) {
      typeCastColumns.push([propKey, meta.typeCast]);
    }
  }

  // 3. Generate typeCastFromDB() method
  const originalTypeCast = constructor.prototype.typeCastFromDB;

  // Fast path: if no columns need type casting and no original method
  if (typeCastColumns.length === 0 && !originalTypeCast) {
    constructor.prototype.typeCastFromDB = function () {
      // no-op
    };
  } else {
    constructor.prototype.typeCastFromDB = function () {
      // Call original typeCastFromDB if exists
      if (originalTypeCast) {
        originalTypeCast.call(this);
      }

      // Apply decorator-defined type casts (pre-filtered, no condition check)
      for (const [propKey, typeCast] of typeCastColumns) {
        const currentValue = (this as Record<string, unknown>)[propKey];
        (this as Record<string, unknown>)[propKey] = typeCast(currentValue);
      }
    };
  }

  // 4. Store column metadata for introspection (ESLint plugin, etc.)
  Object.defineProperty(constructor, '_columnMeta', {
    value: columns,
    writable: false,
    enumerable: false,
    configurable: false,
  });

  // 5. Create relation getters from @hasMany, @belongsTo, @hasOne decorators
  for (const relation of relations) {
    const { propertyKey, type, keysFactory, options } = relation;

    Object.defineProperty(constructor.prototype, propertyKey, {
      get: function () {
        // Call the factory to get keys (lazy resolution for circular references)
        const keys = keysFactory();
        const { sourceKeys, targetKeys, targetModelName } = parseKeys(keys);

        // Build relation config
        const order = options?.order ? orderToString(options.order()) : null;
        const conditions = options?.where ? condsToRecord(options.where()) : undefined;

        // Call internal relation method
        return this._loadRelation(type, targetModelName, {
          sourceKeys,
          targetKeys,
          order,
          conditions,
          limit: options?.limit,
          hardLimit: options?.hardLimit,
          relationName: propertyKey,
        });
      },
      enumerable: true,
      configurable: false,
    });
  }

  // 6. Store relation metadata for introspection
  Object.defineProperty(constructor, '_relationMeta', {
    value: relations,
    writable: false,
    enumerable: false,
    configurable: false,
  });

  // 7. Register model in the registry for relation resolution
  // This is done lazily to support circular references
  // The DBModel._registerModel method is called with the constructor
  const ctor = constructor as unknown as { _registerModel?: (name: string, cls: unknown) => void };
  if (typeof ctor._registerModel === 'function') {
    ctor._registerModel(modelName, constructor);
  }

  return constructor;
}

// ============================================
// Utility Functions
// ============================================

/**
 * Get column metadata from a model class
 * Useful for building tools and plugins
 * Uses cached _columnMeta property (faster than Reflect.getMetadata)
 * @internal
 */
export function getColumnMeta(
  modelClass: object
): Map<string, ColumnMeta> | undefined {
  // Fast path: use cached _columnMeta property (set by @model decorator)
  const cached = (modelClass as { _columnMeta?: Map<string, ColumnMeta> })._columnMeta;
  if (cached) {
    return cached;
  }
  // Fallback to Reflect.getMetadata for classes without @model decorator
  return Reflect.getMetadata(COLUMNS_KEY, modelClass);
}

/**
 * Get all column names from a model class
 * @internal
 */
export function getModelColumnNames(modelClass: object): string[] {
  const meta = getColumnMeta(modelClass);
  if (!meta) return [];
  return Array.from(meta.values()).map((m) => m.columnName);
}

/**
 * Get all property names with @column decorator from a model class
 * @internal
 */
export function getModelPropertyNames(modelClass: object): string[] {
  const meta = getColumnMeta(modelClass);
  if (!meta) return [];
  return Array.from(meta.keys());
}

// ============================================
// Serialize Cache (Performance Optimization)
// ============================================

/** Cache for serialize function lookup maps (key: propName or columnName -> serialize fn) */
const serializeMapCache = new WeakMap<object, Map<string, SerializeFn>>();

/**
 * Get or build serialize function lookup map for a model class.
 * Cached for O(1) lookup per key instead of O(n) iteration.
 * @internal
 */
function getSerializeMap(modelClass: object): Map<string, SerializeFn> {
  let map = serializeMapCache.get(modelClass);
  if (!map) {
    map = new Map();
    // Use _columnMeta directly (faster than Reflect.getMetadata)
    const meta = (modelClass as { _columnMeta?: Map<string, ColumnMeta> })._columnMeta;
    if (meta) {
      for (const [propKey, m] of meta) {
        if (m.serialize) {
          map.set(propKey, m.serialize);
          if (m.columnName !== propKey) {
            map.set(m.columnName, m.serialize);
          }
        }
      }
    }
    serializeMapCache.set(modelClass, map);
  }
  return map;
}

// ============================================
// SQL Cast Cache (Performance Optimization)
// ============================================

/** Cache for sqlCast lookup maps (key: propName or columnName -> sqlCast string) */
const sqlCastMapCache = new WeakMap<object, Map<string, string>>();

/**
 * Get or build sqlCast lookup map for a model class.
 * Returns a map of column/property names to their SQL cast types.
 * Cached for O(1) lookup per key.
 * @internal
 */
export function getSqlCastMap(modelClass: object): Map<string, string> {
  let map = sqlCastMapCache.get(modelClass);
  if (!map) {
    map = new Map();
    const meta = (modelClass as { _columnMeta?: Map<string, ColumnMeta> })._columnMeta;
    if (meta) {
      for (const [propKey, m] of meta) {
        if (m.sqlCast) {
          map.set(propKey, m.sqlCast);
          if (m.columnName !== propKey) {
            map.set(m.columnName, m.sqlCast);
          }
        }
      }
    }
    sqlCastMapCache.set(modelClass, map);
  }
  return map;
}

/**
 * Serialize a record's values for database insertion/update.
 * Applies the serialize function defined in column decorators.
 * Uses cached lookup map for O(1) per-key performance.
 * @param modelClass - The model class with column metadata
 * @param record - The record to serialize
 * @returns Serialized record
 */
export function serializeRecord(
  modelClass: object,
  record: Record<string, unknown>
): Record<string, unknown> {
  const serializeMap = getSerializeMap(modelClass);
  
  // Fast path: no serializers defined
  if (serializeMap.size === 0) {
    return record;
  }

  const result: Record<string, unknown> = {};
  
  for (const [key, value] of Object.entries(record)) {
    const serialize = serializeMap.get(key);
    if (serialize) {
      result[key] = serialize(value);
    } else {
      result[key] = value;
    }
  }

  return result;
}
