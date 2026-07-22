/**
 * litedbmodel v2 SCP — public surface (WS1, #21).
 *
 * The SQL-backend consumer layer over behavior-contracts (spec §1).
 *
 * ## CANONICAL bc integration (epic #43 / design #45): the `makeSQL` catalog leaf.
 *
 * The LOCKED minimal model is ONE behavior-contracts catalog component
 * **`makeSQL(sql, params, skip?)`** (like graphddb's `GetItem`/`Query`) + its handler
 * (bind params → execute SQL) + the compile that emits tuned dialect SQL text (reusing
 * the original tuned builders — byte-for-byte). A query is a COMPOSITION of `makeSQL`
 * components; a subquery is a NESTED `makeSQL` in a param slot. `= ANY`, `CROSS JOIN
 * LATERAL`, `UNNEST`, cast, batch shapes are all TEXT inside `sql` — never modeled.
 * bc supplies composition / value-eval / envelope / plan. The whole `./makesql`
 * subtree (re-exported below) is the recommended integration surface.
 *
 * ## Superseded (legacy) reduced path.
 *
 * The earlier abstract path (`ir.ts` FragmentTree + `WHERE_SLOT`, `relation.ts`
 * `RelationOp`, `compile-sqlite.ts` reduced SELECT/INSERT/UPDATE forms, `render.ts`,
 * `dialect.ts` strategy table) reduced the tuned SQL to a closed expression/relation-op
 * vocabulary and regressed the byte-parity the library is built on (see
 * `docs/proposal/v2-sql-parity-checklist.md`). It is SUPERSEDED by `./makesql`: SQL
 * structure now lives as text inside `makeSQL`, not as IR "kinds". The legacy modules
 * remain exported (below) only for the historical WS test-suite; new code must use
 * `./makesql`.
 */

// ── CANONICAL: the LOCKED `makeSQL` bc integration (epic #43 / design #45). ──────────
export {
  MAKESQL,
  makeSqlCatalogEntry,
  LITEDBMODEL_MAKESQL_CATALOG,
  isMakeSQL,
  assembleMakeSQL,
  composeMakeSQL,
  renderPlaceholders,
  renderPorts,
  makeSqlHandler,
  makeSqlHandlerSync,
  compileWhere,
  whereClause,
  andTrailing,
  formatterFor,
  pgCastFormatter,
  noCastFormatter,
  compileSelect as compileSelectMakeSQL,
  builderFor,
  compileInsert as compileInsertMakeSQL,
  compileUpdateMany,
  compileUpdateSingle,
  compileDelete as compileDeleteMakeSQL,
  inferPgArrayType,
  resolvePgArrayCast,
  PG_ARRAY_CAST_TOKEN,
  compileSingleKeyUnlimited,
  compileSingleKeyLimited,
  compileCompositeKeyUnlimited,
  compileCompositeKeyStaticUnlimited,
  compileCompositeKeyLimited,
  compileSelectNode,
  configurePgDeboxTypeParsers,
  mysqlDeboxPoolOptions,
  pgConnectionPool,
  mysqlConnectionPool,
  pgPoolFactory,
  mysqlPoolFactory,
} from './makesql';
export type {
  MakeSQL,
  SqlParam,
  AssembledSql,
  Dialect as MakeSQLDialect,
  SqlExecutor,
  SqlExecutorSync,
  SqlExecutorAsync,
  SelectDesc as MakeSQLSelectDesc,
  RelationCompileBase,
  StaticStatement,
  ValueSpec,
  PgPoolLike,
  MysqlPoolLike,
  PgTypesLike,
  PgModuleLike,
  Mysql2ModuleLike,
} from './makesql';

// The op-independent runtime leaves (#141): the SOLE execution surface — `executeSQL` transport +
// `pluck`/`group` relation util leaves (defined once via bc `defineLeaf`/`behaviorComponents`). The
// retired 8-leaf catalog (`Select`/`Insert`/…) + `catalogComponents` are GONE.
export { executeSQL, pluck, group, LEAVES, leafComponents, LEAF_TRANSPORT_SYMBOLS } from './leaves';
export type { LeafContext } from './leaves';

// CQRS effect derivation (spec §2.4 — graph-derived from the op-independent leaves' `write` intent).
export { deriveContractEffect } from './authoring';
export type { ContractEffect } from './authoring';

// Dialect strategy table (WS6, #26 — the SSoT for PG/MySQL/SQLite SQL divergences + `?`→`$N`).
export { dialectFor, toDollarPlaceholders, SQLITE, POSTGRES, MYSQL } from './dialect';
export type { Dialect, DialectName } from './dialect';

// Portability guard (closed Expression IR set only)
export {
  assertExprPortable,
  assertComponentPortable,
  assertComponentGraphPortable,
} from './guard';
export type { ExprNode } from './guard';

// Authoring Parse (spec §2.4 / §7 / §9) — SemanticBehavior declaration + eager public-API
// path → one internal Component-graph IR.
export {
  components,
  publishBehaviors,
  compileEager,
} from './authoring';
export type {
  ComponentFns,
  BehaviorMethodSpec,
  BehaviorModelContract,
  PublishBehaviorsOptions,
  ModelColumns,
  TypedModelClass,
  EagerBehavior,
  Component,
  ComponentGraphIR,
  MapNode,
  ComponentRefNode,
} from './authoring';

// SQL WHERE authoring helpers (closed-set encodings the bridge decodes).
export {
  whereEq,
  whereNe,
  whereLt,
  whereLe,
  whereGt,
  whereGe,
  whereIsNull,
  whereIn,
  inColumn,
  // Additive where-primitives (V0 R2/R3): live-reachable, v1-sourced SQL.
  whereBetween,
  whereLike,
  whereILike,
  whereCast,
  whereDynamic,
  whereImmediate,
  whereTupleIn,
  whereInSubquery,
  whereExists,
  // Phase E-1 (#97): typed subquery / parentRef authoring sugar (TS-only ergonomics; lowers to
  // whereInSubquery / whereExists — no new IR).
  col,
  parentRef,
  inSubquery,
  notInSubquery,
  exists,
  notExists,
  // QUERY view-model authoring (#98): lowers a declared QUERY onto the Select cte/cteParams ports.
  queryView,
} from './authoring-sql';
export type { ColumnRef, ParentRefValue, SubqueryCondition, KeyPair, CompositeKeyPairs } from './authoring-sql';
export type { QuerySource, QueryViewOptions } from './authoring-sql';

// Error Mapping (spec §11 item 5): driver error → SCP Failure + Policy Kind.
export { mapSqliteError, SqlFailure, LimitExceededError } from './errors';
export type { SqlFailureKind, LimitExceededContext } from './errors';

// Hard-limit runaway prevention (Phase E-2, epic #74; v1 `setLimitConfig`/`LimitExceededError`
// parity): the global find/hasMany hard-limit config. Read at COMPILE time to bake the effective
// caps onto the portable artifacts (the ReadGraph `findGuard` + each RelationOp `hardLimit`); the
// TS runtime + the native ports throw `LimitExceededError` post-fetch when a read / relation batch
// exceeds its cap. `null` disables; a per-relation `hardLimit` override wins; a relation with an
// intrinsic per-parent `limit` window skips the batch-total check.
export { setLimitConfig, getLimitConfig, resetLimitConfig, resolveFindHardLimit, resolveHasManyHardLimit } from './limit-config';
export type { LimitConfig } from './limit-config';

// FIND_FILTER fail-closed authoring guard (#47 Finding B / plan R8): a model declaring an
// implicit per-model scope predicate cannot be SCP-compiled without folding it into the
// authored WHERE (fail-closed; the SCP compile has no model context to auto-apply it).
export { assertFindFilterFolded, findFilterKeys, FindFilterLeakError } from './find-filter-guard';
export type { FindFilterSource } from './find-filter-guard';

// Column type system (spec §4.1; #58): SQL type → bc outType scalar, and the schema/DDL SoT
// resolver that types a SELECT projection for typed (de-boxed) codegen. Fail-closed throughout.
export { sqlTypeToBcScalar, sqlTypeToMaterializeClass, materializeCell, materializeClassOrUndefined, parseSchemaColumnTypes, schemaColumnTypeResolver, materializeResolverFromColumnMap, failClosedMaterializeResolverFromColumnMap, columnTypeResolverFromColumnMap } from './coltype';
export type { BcScalar, MaterializeClass, ColumnTypeResolver, MaterializeResolver } from './coltype';

// Thin TS runtime (spec §3 / §10 / §11): the op-independent leaf graph (`executeSQL`/`pluck`/`group`)
// runs via bc `bindBehaviors` — `executeBehavior`/`read` are the SOLE ts-runtime read seam (#141). The
// catalog read-bundle surface (`compileBundle`/`compileReadGraph`) is retired (#143).
export { executeBehavior, read } from './runtime';
export type { SqliteDb, ExecuteOptions, SqlBundle, ReadRuntimeOptions } from './runtime';

// ── Phase A (#75): the ExecutionContext + central execute/run seam + per-execution connection
// ownership. The CONTRACT-DEFINING artifact the native ports (#76-79) follow. All runtime SQL
// (read/write/tx/relation) funnels through `execute`/`run`; `contextForDriver` is the backward-
// compat wrapper (raw driver ⇒ single-DB, empty-middleware ctx) keeping conformance byte-identical.
export {
  execute,
  executeSafe,
  run,
  executeAsync,
  runAsync,
  runGuarded,
  runGuardedAsync,
  contextForDriver,
  contextForConnection,
  connectionForDriver,
  MiddlewareChain,
  PooledAsyncContext,
  withTransactionAsync,
  transaction,
  runWithPinnedAsyncConnection,
  currentPinnedAsyncConnection,
} from './exec-context';
export type {
  ExecutionContext,
  AsyncExecutionContext,
  StatementIntent,
  Rows,
  RunInfo,
  SyncConnection,
  AsyncConnection,
  AsyncConnectionPool,
  Middleware,
  MiddlewareStackSource,
  SeamNext,
  SqliteDriver,
  TxOptions,
} from './exec-context';

// ── Phase D (#92): the MIDDLEWARE layer on the Phase A seam. The API REFERENCE the native ports
// (#93-96) mirror: registration (`use`/`createMiddleware`), the SQL-level `execute(next, sql, params)`
// chain contract + applied order (first-registered = outermost), method-level hooks keyed by op kind
// (`runMethod`), per-execution-scope isolation (`withMiddlewareScope`, TS AsyncLocalStorage), the
// standard `Logger`, and the raw `execute`/`query` API that goes THROUGH the seam. An unregistered
// chain is a byte-identical passthrough (conformance/livedb register none ⇒ unchanged).
export {
  Registry,
  currentRegistry,
  withMiddlewareScope,
  activeSqlMiddlewares,
  register,
  use,
  createMiddleware,
  runMethod,
  Logger,
  rawExecute,
  rawExecuteAsync,
  rawQuery,
  rawQueryAsync,
  clearMiddlewares,
} from './middleware';
export type {
  SqlHook,
  MethodKind,
  MethodHook,
  MethodNext,
  MiddlewareDescriptor,
  MiddlewareHandle,
  MiddlewareConfig,
  MethodHookFn,
  SqlNext,
  LogEntry,
  LoggerOptions,
  RawResult,
} from './middleware';

// ── Phase C (#87): connection routing + config. The API REFERENCE the native ports (#88-91) mirror.
// Completes `connectionFor(intent)`'s resolution (§3 steps 2-4): reader/writer separation + writer-
// sticky + `withWriter` (C1), a multi-DB name→pools registry + named routing (C2), and the setConfig
// surface (queryTimeout/keepAlive/pool sizing/searchPath/charset) + closeAllPools (C3). A single-pool
// `PooledAsyncContext` synthesizes a default-only registry (reader === writer, sticky off) ⇒ byte-
// identical to Phase A/B; a `buildRoutingConfig`-driven ctx gets the full routing.
export {
  ConnectionRegistry,
  ConnectionRegistryBuilder,
  WriterStickyClock,
  DEFAULT_CONNECTION,
  withWriter,
  inWriterScope,
  resolvePool,
  resolveConnectionConfig,
  sessionStatements,
  sessionResetStatements,
  configuredPool,
  singlePoolPair,
  readerWriterPair,
  buildRoutingConfig,
} from './connection-routing';
export type {
  ConnectionConfig,
  ResolvedConnectionConfig,
  ReaderWriterPools,
  RoutingConfig,
  ConnectionSetup,
  PoolCloser,
  PoolFactory,
} from './connection-routing';

// The tx-completeness contract (Phase B-1 / #81): TransactionOptions shape + defaults, the
// isolation-level enum + per-dialect BEGIN mapping, the retryable-error classifier, the write=tx
// guards (`checkWriteAllowed` + `withReadOnly` / `runInTransactionScope` scope markers). The API
// REFERENCE the 4 native ports (rust #82 / go #83 / py #84 / php #85) mirror.
export {
  isolationPhrase,
  beginStatements,
  resolveTxOptions,
  isRetryableTxError,
  sleep,
  runInTransactionScope,
  withReadOnly,
  isInTransaction,
  isReadOnly,
  checkWriteAllowed,
  WriteOutsideTransactionError,
  WriteInReadOnlyContextError,
} from './tx-options';
export type { IsolationLevel, TransactionOptions, ResolvedTxOptions } from './tx-options';

// The ASYNC PG / MySQL production read execution model (#40): the op-independent leaf graph run via
// bc `bindBehaviors().runAsync` over the `executeSQL` leaf's async seam (per-execution ownership).
export { executeBehaviorAsync } from './runtime';
export type { AsyncExecuteOptions } from './runtime';

// Write-time relations (WS5, #25 — spec §6): entityWrites/edgeWrites declaration vocabulary,
// the gate-first transaction-plan derivation, and the 1-tx real-SQLite runtime.
export {
  entityWrites,
  edgeWrites,
  lifecycleFor,
  parseEffectPath,
  ENTITY_ROOT,
} from './writes';
export type {
  PathRoot,
  EffectPath,
  RequiresEffect,
  UniqueEffect,
  DeriveEffect,
  EdgeEffect,
  EmitEffect,
  IdempotencyEffect,
  LifecycleEffects,
  LifecycleContract,
  WriteLifecyclePhase,
  EntityWritesDefinition,
  EntityWritesShape,
  WriteRecorder,
} from './writes';

export { deriveTransactionPlan, executeTransaction, executeTransactionAsync, countingDriver, renderTxStatement, compileWriteNode, mysqlPkHint, stripMysqlPkHint } from './makesql';
export type {
  TxExpr,
  TxOp,
  StatementRole,
  GateRule,
  TxStatement,
  TransactionPlan,
  IdempotentHitPolicy,
  BaseWrite,
  TransactionResult,
  ShortCircuitReason,
  WriteExecOptions,
} from './makesql';

// The Command bundle + 1-tx execution surface (WS5 — the write path of §2.3 / §6).
export { compileWriteBundle, executeCommand, executeTransactionBundle } from './runtime';

// Composite (multi-write) Command surface (WS8a, #28 — spec §6 nested write / §14 tx-DAG derivation):
// several named base writes with data dependencies → ONE topologically-ordered gate-first tx plan.
export { compileCompositeWriteBundle, executeCompositeCommand } from './runtime';
export type { CompositeWriteEntry } from './runtime';

// Batch writes (createMany / updateMany / deleteMany): ONE logical op → N grouped statements lowered
// to a gate-free tx plan (executed by the SAME multi-statement tx loop in all 5 runtimes). The
// batch SQL is byte-copied from the v1 builders (compileInsertMany/compileUpdateMany/compileDeleteMany).
export { compileCreateManyBundle, compileUpdateManyBundle, compileDeleteManyBundle } from './runtime';
export { compileDeleteMany, compileInsertMany } from './makesql';
// dbCast: the column-type cast marker the makeSQL compilers thread into WHERE/SET (spec §4.1).
// Re-exported so a bundle consumer builds the SAME DBCast instance the inlined compilers recognise
// (a standalone dist/DBValues copy is a DIFFERENT class → the compiler would not honour the cast).
export { dbCast, dbCastIn } from '../DBValues';

// Read relations (WS4, #24): pre-compiled batch relation ops + staged batch resolution.
// BOTH the declarative-select and the lazy surface resolve through the SAME compiled op.
export {
  compileRelationOp,
  runRelationOp,
  distributeToParent,
  RELATION_KEYS_HEAD,
} from './relation';
export type {
  RelationKind,
  RelationDecl,
  RelationOp,
  RelationBatch,
  RelationDriver,
} from './relation';

// typed-object result + hydrate factory + lazy relation context (WS4, #24).
export {
  buildResultSet,
  readRelationContext,
  RelationContext,
  RELATION_CONTEXT,
} from './typed-object';
export type { HydrateFactory, ReadOptions } from './typed-object';

// ── Phase F-1 (#104): the decorator → SCP authoring ADAPTER. Translates the `@model` / `@column` /
// `@hasMany` decorator metadata into the SCP authoring it lowers to (columns → `static columns`;
// find/count → eager Select/Count; create/update/delete → write bundles; relations → RelationDecl →
// RelationOp). Standalone + unit-proven byte-identical to the hand-written SCP behavior; does NOT yet
// rewire DBModel's methods (F2). TS-only, zero BC. Mirrors graphddb's collector→define→compile.
export {
  deriveModelColumns,
  columnSqlType,
  tableNameOf,
  COLUMN_FAMILY_SQL_TYPE,
  DEFAULT_UNCAST_SQL_TYPE,
  findAuthoring,
  countAuthoring,
  compileReadContract,
  emitRead,
  emitWrite,
  emitBatchWrite,
  createAuthoring,
  updateAuthoring,
  deleteAuthoring,
  relationReadAuthoring,
  relationKeyTypeResolver,
  compileCommandBundle,
  compileCreateBundle,
  compileUpdateBundle,
  compileDeleteBundle,
  modelColumnResolver,
  deriveRelationDecls,
  relationDeclOf,
  compileRelationOps,
} from './decorator-adapter';
export type {
  ModelClassLike,
  DeriveColumnsOptions,
  ReadAuthoringSpec,
  InsertAuthoringSpec,
} from './decorator-adapter';

// Re-export bc's shared authoring vocabulary so authors import the whole surface from
// litedbmodel (leaf vocabulary is the Catalog; expressions/structured control are bc's —
// C2). There is NO litedbmodel-local authoring opcode beyond the Catalog.
export {
  SemanticBehavior,
  behavior,
  when,
  concat,
  add,
  sub,
  mul,
  div,
  mod,
  neg,
  eq,
  ne,
  lt,
  le,
  gt,
  ge,
  and,
  or,
  not,
  coalesce,
  len,
  opt,
} from 'behavior-contracts';
export type { In, Recorded, BehaviorClass } from 'behavior-contracts';

// Re-export bc's native-codegen surface (#141 native step6/7): litedbmodel's generation script authors
// the ops on the SCP surface (`emitRead`/`emitWrite`/relation `pluck`/`group`) and delegates ALL module
// generation to bc `generateModule` (no litedbmodel-local generator — C4). The op-independent leaf
// transport is declared via `leafTransport` (executeSQL→execute_sql / pluck→pluck_keys / group→group_children).
export { generateModule, GeneratorFailure } from 'behavior-contracts';
export type { GenerateOptions, GeneratedModule, LeafTransportOptions } from 'behavior-contracts';
