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
  getColumnMeta,
  getModelColumnNames,
  getModelPropertyNames,
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
  buildRelationConfig,
  type RelationType,
  type RelationConfig,
  type BelongsToOptions,
  type HasManyOptions,
} from './LazyRelation';

// Backward compatibility: LazyLoadingDBModel is now just DBModel
export { DBModel as LazyLoadingDBModel } from './DBModel';

// ============================================
// Middleware
// ============================================

export {
  Middleware,
  type MiddlewareClass,
  type ExecuteResult,
  type NextFind,
  type NextFindOne,
  type NextFindById,
  type NextCount,
  type NextCreate,
  type NextCreateMany,
  type NextUpdate,
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
  TransactionOptions,
  Logger,
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
