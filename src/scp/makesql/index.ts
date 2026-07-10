/**
 * litedbmodel v2 SCP — the LOCKED `makeSQL` surface (epic #43 / design #45).
 *
 * ONE bc catalog component `makeSQL(sql, params, skip?)` + its handler + the compile
 * that emits tuned dialect SQL text (by reusing the original builders). A query is a
 * composition of `makeSQL` components; a subquery is a nested `makeSQL` in a param
 * slot. Everything (`= ANY`, LATERAL, UNNEST, cast, batch shapes) is TEXT inside `sql`.
 * bc supplies composition / value-eval / envelope / plan; litedbmodel supplies the
 * catalog leaf + handler + compile. Pure-JSON bundle, multi-language ready.
 */

// Catalog leaf + assembly core.
export {
  MAKESQL,
  makeSqlCatalogEntry,
  LITEDBMODEL_MAKESQL_CATALOG,
  isMakeSQL,
  assembleMakeSQL,
  composeMakeSQL,
} from './makesql';
export type { MakeSQL, SqlParam, AssembledSql } from './makesql';

// Handler + dialect placeholder render.
export {
  renderPlaceholders,
  renderPorts,
  makeSqlHandler,
  makeSqlHandlerSync,
} from './handler';
export type { Dialect, SqlExecutor, SqlExecutorSync } from './handler';

// Compile (WHERE / conditions / values).
export {
  compileWhere,
  compileOptionalEq,
  whereClause,
  andTrailing,
  formatterFor,
  pgCastFormatter,
  noCastFormatter,
} from './compile';

// Compile (SELECT tail).
export { compileSelect } from './compile-select';
export type { SelectDesc } from './compile-select';

// Compile (CRUD).
export {
  builderFor,
  compileInsert,
  compileInsertMany,
  compileUpdateMany,
  compileFindByPkeys,
  compileUpdateSingle,
  compileDelete,
} from './compile-crud';
export type { InsertManyBuildOptions } from './compile-crud';

// Compile (relations).
export {
  inferPgArrayType,
  compileSingleKeyUnlimited,
  compileSingleKeyLimited,
  compileCompositeKeyUnlimited,
  compileCompositeKeyLimited,
} from './compile-relation';
export type { RelationCompileBase } from './compile-relation';

// Single-JSON-param array/batch forms for MySQL 8 + SQLite (epic #43/#45) — the
// intentional deviation from v1's N-placeholder expansion (server-side JSON expansion).
export { inListJson, JsonArrayConditions, conditionsFor } from './json-array';
export {
  mysqlInsertJson,
  sqliteInsertJson,
  mysqlUpdateManyJson,
  sqliteUpdateManyJson,
  mysqlJsonTableColumn,
  rowsHaveDbToken,
} from './json-batch';

// bc IR wiring (a query rides runBehavior as a makeSQL component).
export { makeSqlComponentIR, makeSqlInput } from './ir';
