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
  compileFindByPkeys,
  compileUpdateSingle,
  compileDelete as compileDeleteMakeSQL,
  inferPgArrayType,
  resolvePgArrayCast,
  PG_ARRAY_CAST_TOKEN,
  compileSingleKeyUnlimited,
  compileSingleKeyLimited,
  compileCompositeKeyUnlimited,
  compileCompositeKeyLimited,
  makeSqlComponentIR,
  makeSqlInput,
  compileAuthoredBehavior,
  compileAuthoredNode,
  compileRelationMap,
  compileStaticBundle,
  compileSelectNode,
  executeStaticBundle,
  executeStaticWrite,
  executeReadBehavior,
  compileReadGraph,
  executeReadGraph,
  executeReadGraphAsync,
  renderReadPrimary,
  pgPoolExecutor,
  mysqlPoolExecutor,
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
  AuthoredMakeSQL,
  RelationMapInput,
  StaticBundle,
  StaticStatement,
  ValueSpec,
  ReadGraph,
  PgPoolLike,
  MysqlPoolLike,
} from './makesql';

// Catalog (spec §11 item 1)
export {
  LITEDBMODEL_CATALOG,
  catalogEntry,
  CATALOG_NAMES,
  WRITE_CATALOG_NAMES,
  WRITE_PORT_FAMILIES,
  assertComponentsInCatalog,
  deriveContractEffect,
} from './catalog';
export type { CatalogName, ContractEffect } from './catalog';

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
} from './authoring-sql';

// Error Mapping (spec §11 item 5): driver error → SCP Failure + Policy Kind.
export { mapSqliteError, SqlFailure } from './errors';
export type { SqlFailureKind } from './errors';

// Thin TS runtime (spec §3 / §10 / §11): validate → SKIP → expand → eval → bind → execute → assembly.
// `compileBundle` emits the §8 published artifact (Backend-Compiled once, TS-side);
// `executeBundle` runs that artifact via bc runtime-core alone (the multi-language target).
export { executeBehavior, compileBundle, executeBundle, read, readBundle } from './runtime';
export type { SqliteDb, ExecuteOptions, SqlBundle, ReadRuntimeOptions } from './runtime';

// The ASYNC PG / MySQL production read execution model (#40): bc `runBehaviorAsync` fans out
// independent sibling read nodes in bounded parallel against a pooled async executor.
export { executeBundleAsync, executeBehaviorAsync } from './runtime';
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

export { deriveTransactionPlan, executeTransaction, countingDriver, renderTxStatement, compileWriteNode, mysqlPkHint, stripMysqlPkHint } from './makesql';
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
export { compileDeleteMany } from './makesql';

// Mode-3 codegen (WS7f, #35 — spec §9 exec-mode 3): supply the litedbmodel SQL catalog to bc's
// shared generator; emit per-language source (IR baked as a native literal) + the SQL catalog
// companion. Generated code output is byte-identical to the mode-2 thin-runtime (proven by the
// codegen conformance leg).
export {
  CODEGEN_LANGUAGES,
  generateCodegenArtifact,
  bundleToPortableIR,
  assertLanguageSupported,
  codegenExecuteBundleForTest,
} from './codegen';
export type {
  CodegenLanguage,
  SqlCatalogCompanion,
  CodegenArtifact,
} from './codegen';

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
