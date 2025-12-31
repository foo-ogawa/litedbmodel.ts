/**
 * litedbmodel - Database Drivers
 *
 * This module exports database driver implementations and utilities.
 */

// Types
export type {
  DBConfig,
  Logger,
  QueryResult,
  DBConnection,
  DBDriver,
  DBDriverOptions,
} from './types';
export { defaultLogger } from './types';

// PostgreSQL Driver
export { PostgresDriver, createPostgresDriver, closeAllPools } from './postgres';

// SQLite Driver
export { SqliteDriver, createSqliteDriver } from './sqlite';

// MySQL Driver
export { MysqlDriver, createMysqlDriver, closeAllMysqlPools } from './mysql';

// PostgreSQL Helpers (re-exported from main module for backward compatibility)
export {
  castToDatetime as pgCastToDatetime,
  castToBoolean as pgCastToBoolean,
  castToIntegerArray as pgCastToIntegerArray,
  castToStringArray as pgCastToStringArray,
  castToJson as pgCastToJson,
  Now as pgNow,
  Null as pgNull,
  True as pgTrue,
  False as pgFalse,
  pgIntArray,
  pgStringArray,
  pgArrayParse,
  TimeAfter as pgTimeAfter,
  DayAfter as pgDayAfter,
  makeLikeString as pgMakeLikeString,
} from './PostgresHelper';

// SQLite Helpers
export {
  castToDatetime as sqliteCastToDatetime,
  castToBoolean as sqliteCastToBoolean,
  castToIntegerArray as sqliteCastToIntegerArray,
  castToStringArray as sqliteCastToStringArray,
  castToJson as sqliteCastToJson,
  Now as sqliteNow,
  Null as sqliteNull,
  True as sqliteTrue,
  False as sqliteFalse,
  jsonIntArray,
  jsonStringArray,
  jsonObject,
  TimeAfter as sqliteTimeAfter,
  DayAfter as sqliteDayAfter,
  makeLikeString as sqliteMakeLikeString,
  sqliteTypeToTsType,
  SQLITE_TYPE_TO_TS,
} from './SqliteHelper';

// MySQL Helpers
export {
  castToDatetime as mysqlCastToDatetime,
  castToBoolean as mysqlCastToBoolean,
  castToIntegerArray as mysqlCastToIntegerArray,
  castToStringArray as mysqlCastToStringArray,
  castToJson as mysqlCastToJson,
  Now as mysqlNow,
  Null as mysqlNull,
  True as mysqlTrue,
  False as mysqlFalse,
  jsonIntArray as mysqlJsonIntArray,
  jsonStringArray as mysqlJsonStringArray,
  jsonObject as mysqlJsonObject,
  TimeAfter as mysqlTimeAfter,
  DayAfter as mysqlDayAfter,
  makeLikeString as mysqlMakeLikeString,
  mysqlTypeToTsType,
  MYSQL_TYPE_TO_TS,
} from './MysqlHelper';

