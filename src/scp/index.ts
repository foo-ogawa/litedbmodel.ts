/**
 * litedbmodel v2 SCP — public surface (WS1, #21).
 *
 * The SQL-backend consumer layer over behavior-contracts (spec §1). WS1 delivers the
 * Catalog definition and the SQLite Backend Compile + dynamic-expansion render; WS2
 * delivers the Authoring Parse (SemanticBehavior declaration + eager public-API path →
 * one internal Component-graph IR). WS3 (runtime execution / handlers), WS4/5 (relations)
 * and the other dialects (PG/MySQL) are out of scope here.
 */

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

// SQL IR shapes (spec §8)
export { WHERE_SLOT } from './ir';
export type {
  ExprNode,
  Fragment,
  FragmentTree,
  AssemblySpec,
  CompiledOperation,
} from './ir';

// SQLite Backend Compile (spec §11 item 3)
export {
  compileSelect,
  compileInsert,
  compileUpdate,
  compileDelete,
} from './compile-sqlite';
export type {
  Ref,
  Condition,
  SelectDesc,
  InsertDesc,
  UpdateDesc,
  DeleteDesc,
} from './compile-sqlite';

// Dynamic-expansion render (normative reference for byte-identical output)
export { renderOperation } from './render';
export type { RenderedSql } from './render';

// Portability guard (closed Expression IR set only)
export {
  assertExprPortable,
  assertOperationPortable,
  assertComponentPortable,
  assertComponentGraphPortable,
} from './guard';

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

// Backend-Compile bridge (WS1↔WS3): real bc ComponentGraphIR port shape → WS1 CompiledOperation.
export { compileNode, compileComponentNodes, IN_SENTINEL } from './bridge';

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
export { executeBehavior, compileBundle, executeBundle, read, readBundle, resolveRelationViaPlan } from './runtime';
export type { SqliteDb, ExecuteOptions, SqlBundle, ReadRuntimeOptions } from './runtime';

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

export { deriveTransactionPlan } from './write-plan';
export type {
  StatementRole,
  GateRule,
  TxStatement,
  TransactionPlan,
  IdempotentHitPolicy,
  BaseWrite,
} from './write-plan';

export { executeTransaction, countingDriver } from './write-runtime';
export type { TransactionResult, ShortCircuitReason } from './write-runtime';

// The Command bundle + 1-tx execution surface (WS5 — the write path of §2.3 / §6).
export { compileWriteBundle, executeCommand, executeTransactionBundle } from './runtime';

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
