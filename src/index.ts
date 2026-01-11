/**
 * litedbmodel - A lightweight TypeScript data access layer
 *
 * Supports PostgreSQL and SQLite databases.
 *
 * @packageDocumentation
 */

// ============================================
// Core Classes
// ============================================

export {
  DBModel,
  getTransactionContext,
  getTransactionConnection,
  getTransactionClient, // @deprecated - use getTransactionConnection
} from './DBModel';
export { 
  DBHandler, 
  closeAllPools,
  initDBHandler,
  getDBHandler,
  getDBConfig,
  createHandlerWithConnection,
  createHandlerWithClient, // @deprecated - use createHandlerWithConnection
  type DBConfig,
  type DBConnection,
} from './DBHandler';
export { DBConditions, DBOrConditions, and, or as _legacyOr, normalizeConditions, type ConditionObject, type ConditionValue } from './DBConditions';

// ============================================
// Value Wrappers
// ============================================

export {
  DBToken,
  DBImmediateValue,
  DBNullValue,
  DBNotNullValue,
  DBBoolValue,
  DBArrayValue,
  DBDynamicValue,
  DBRawValue,
  DBTupleIn,
  // Type casting classes
  DBCast,
  DBCastArray,
  // Subquery classes
  DBSubquery,
  DBExists,
  DBParentRef,
  type SubqueryCondition,
  // Factory functions
  dbNull,
  dbNotNull,
  dbTrue,
  dbFalse,
  dbNow,
  dbIn,
  dbDynamic,
  dbRaw,
  dbImmediate,
  dbTupleIn,
  parentRef,
  // Type cast factory functions
  dbCast,
  dbUuid,
  dbCastIn,
  dbUuidIn,
} from './DBValues';

// ============================================
// Column Type and Decorators (New API)
// ============================================

export {
  type Column,
  createColumn,
  isColumn,
  type ColumnsOf,
  type ColumnOf,
  type ModelOfColumn,
  Values,
  Conditions,
  OrderColumn,
  isOrderColumn,
  type OrderColumnOf,
  type OrderSpec,
  orderToString,
  columnsToNames,
  // Type-safe tuple API
  type CV,
  type CVs,
  pairsToRecord,
  // SKIP sentinel for conditional fields
  SKIP,
  type SkipType,
  // Type-safe condition tuples
  type Cond,
  type CondOf,
  type CondElement,
  type CondElementOf,
  type Conds,
  type CondsOf,
  type OrCond,
  type OrCondOf,
  isOrCond,
  condsToRecord,
} from './Column';
export {
  model,
  column,
  hasMany,
  belongsTo,
  hasOne,
  getColumnMeta,
  getRelationMeta,
  getModelColumnNames,
  getModelPropertyNames,
  type ColumnOptions,
  type ColumnMeta,
  type RelationMeta,
  type RelationType,
  type KeyPair,
  type CompositeKeyPairs,
  type KeysFactory,
  type RelationDecoratorOptions,
} from './decorators';

// ============================================
// Type Casting (Database Agnostic)
// ============================================

export {
  // Type casting functions (use current driver's implementation)
  castToDatetime,
  castToBoolean,
  castToIntegerArray,
  castToNumericArray,
  castToStringArray,
  castToBooleanArray,
  castToDatetimeArray,
  castToJson,
  // Type cast configuration
  getTypeCast,
  setTypeCastImpl,
  resetTypeCastImpl,
  type TypeCastFunctions,
} from './TypeCast';

// ============================================
// Lazy Loading Relations
// ============================================

export {
  LazyRelationContext,
  createRelationContext,
  preloadRelations,
  type RelationConfig,
} from './LazyRelation';

// Backward compatibility: LazyLoadingDBModel is now just DBModel
export { DBModel as LazyLoadingDBModel } from './DBModel';

// ============================================
// Middleware
// ============================================

export {
  Middleware,
  createMiddleware,
  type MiddlewareClass,
  type MiddlewareConfig,
  type CreatedMiddlewareClass,
  type ExecuteResult,
  type NextFind,
  type NextFindOne,
  type NextFindById,
  type NextCount,
  type NextCreate,
  type NextCreateMany,
  type NextUpdate,
  type NextUpdateMany,
  type NextDelete,
  type NextExecute,
  type NextQuery,
} from './Middleware';

// Sample middlewares
export { StatisticsMiddleware } from './middlewares';

// ============================================
// Types
// ============================================

export type {
  SelectOptions,
  InsertOptions,
  UpdateOptions,
  DeleteOptions,
  UpdateManyOptions,
  TransactionOptions,
  Logger,
  LimitConfig,
  ModelOptions,
  DBConfigOptions,
  PkeyResult,
} from './types';
export { 
  LimitExceededError, 
  WriteOutsideTransactionError, 
  WriteInReadOnlyContextError,
} from './types';

// ============================================
// Database Drivers
// ============================================

export {
  PostgresDriver,
  createPostgresDriver,
  SqliteDriver,
  createSqliteDriver,
  type DBDriver,
  type DBDriverOptions,
  type QueryResult,
} from './drivers';
