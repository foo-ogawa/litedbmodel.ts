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
  DriverTypeCast,
  SqlBuilder,
  InsertBuildOptions,
  UpdateManyBuildOptions,
  SelectPkeysOptions,
  FindByPkeysOptions,
  SqlBuildResult,
} from './types';
export { defaultLogger } from './types';

// SQL Builders
export { postgresSqlBuilder, postgresTypeCast } from './PostgresSqlBuilder';
export { mysqlSqlBuilder, mysqlTypeCast } from './MysqlSqlBuilder';
export { sqliteSqlBuilder, sqliteTypeCast } from './SqliteSqlBuilder';

import { postgresSqlBuilder } from './PostgresSqlBuilder';
import { mysqlSqlBuilder } from './MysqlSqlBuilder';
import { sqliteSqlBuilder } from './SqliteSqlBuilder';
import type { SqlBuilder, DriverTypeCast } from './types';

/**
 * Get SQL builder for a specific driver type
 */
export function getSqlBuilder(driverType: DriverType): SqlBuilder {
  switch (driverType) {
    case 'postgres':
      return postgresSqlBuilder;
    case 'mysql':
      return mysqlSqlBuilder;
    case 'sqlite':
      return sqliteSqlBuilder;
    default:
      throw new Error(`Unknown driver type: ${driverType}`);
  }
}

/**
 * Get type cast helper for a specific driver type
 */
export function getTypeCast(driverType: DriverType): DriverTypeCast {
  return getSqlBuilder(driverType).typeCast;
}

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

// Import driver-specific cast helpers
import {
  formatSqlCast as pgFormatSqlCast,
  needsSqlCast as pgNeedsSqlCast,
} from './PostgresHelper';
import {
  formatSqlCast as sqliteFormatSqlCast,
  needsSqlCast as sqliteNeedsSqlCast,
} from './SqliteHelper';
import {
  formatSqlCast as mysqlFormatSqlCast,
  needsSqlCast as mysqlNeedsSqlCast,
} from './MysqlHelper';

import type { SqlCastFormatter } from '../DBValues';

// ============================================
// Driver Type
// ============================================

export type DriverType = 'postgres' | 'sqlite' | 'mysql';

// ============================================
// Driver-agnostic SQL Casting Utilities
// ============================================

/**
 * Format a placeholder with SQL type cast based on driver type.
 * Only PostgreSQL actually needs casting for UUID type.
 * 
 * @param placeholder - The placeholder string (e.g., '?')
 * @param sqlCast - The SQL type to cast to (e.g., 'uuid')
 * @param driverType - The database driver type
 * @returns The formatted placeholder
 */
export function formatSqlCast(
  placeholder: string,
  sqlCast: string,
  driverType: DriverType
): string {
  switch (driverType) {
    case 'postgres':
      return pgFormatSqlCast(placeholder, sqlCast);
    case 'sqlite':
      return sqliteFormatSqlCast(placeholder, sqlCast);
    case 'mysql':
      return mysqlFormatSqlCast(placeholder, sqlCast);
    default:
      return placeholder;
  }
}

/**
 * Check if a SQL type needs explicit casting based on driver type.
 * 
 * @param sqlCast - The SQL type (e.g., 'uuid')
 * @param driverType - The database driver type
 * @returns true if casting is needed
 */
export function needsSqlCast(sqlCast: string, driverType: DriverType): boolean {
  switch (driverType) {
    case 'postgres':
      return pgNeedsSqlCast(sqlCast);
    case 'sqlite':
      return sqliteNeedsSqlCast(sqlCast);
    case 'mysql':
      return mysqlNeedsSqlCast(sqlCast);
    default:
      return false;
  }
}

/**
 * Get SQL cast formatter function for a specific driver.
 * 
 * @param driverType - The database driver type
 * @returns A SqlCastFormatter function
 */
export function getSqlCastFormatter(driverType: DriverType): SqlCastFormatter {
  switch (driverType) {
    case 'postgres':
      return (placeholder, sqlType) => pgFormatSqlCast(placeholder, sqlType);
    case 'sqlite':
      return (placeholder, sqlType) => sqliteFormatSqlCast(placeholder, sqlType);
    case 'mysql':
      return (placeholder, sqlType) => mysqlFormatSqlCast(placeholder, sqlType);
    default:
      return (placeholder) => placeholder;
  }
}

