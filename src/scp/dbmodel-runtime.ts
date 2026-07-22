/**
 * litedbmodel v2 SCP — the DBModel ActiveRecord ↔ SCP RUNTIME bridge (Phase F-2, epic #74, issue #105).
 *
 * F1 (`decorator-adapter.ts`) built the standalone decorator→SCP authoring adapter; THIS module (F2)
 * is the runtime glue that re-points `DBModel`'s public methods (`find`/`create`/`transaction`/…) off
 * the imperative SQL-string path onto that adapter → the SCP compile → the Phase A-E runtime
 * (`exec-context.ts` / `connection-routing.ts` / `middleware.ts` / `tx-options.ts` / `limit-config.ts`).
 *
 * It owns three concerns:
 *
 *  1. **Connection wiring** ({@link buildContextFromConfig}) — a v1 `DBConfig` (+ `writerConfig` /
 *     writer-sticky options) → a {@link PooledAsyncContext} over REAL pg / mysql2 pools (via
 *     `pgPoolFactory` / `mysqlPoolFactory` + `buildRoutingConfig`), plus the reader executor
 *     (`pgPoolExecutor` / `mysqlPoolExecutor`) and a `close()`. This is the C3
 *     `setConfig → ConnectionRegistry → pool` path.
 *
 *  2. **v1-conditions → SCP where bridge** ({@link conditionsToWhere}) — a v1 `ConditionObject` (what
 *     `condsToRecord` produces) is compiled by the ORIGINAL `DBConditions.compile()` to its byte-true
 *     `(whereSql, params)`, then carried onto the SCP where port as ONE {@link whereRawPredicate}
 *     member (values ride as SCP input, not baked literals). One member covers EVERY v1 condition
 *     shape (eq / IN / custom-op / OR / subquery / EXISTS / cast / null / boolean) — the text is v1's,
 *     so the emitted WHERE is byte-identical by construction.
 *
 *  3. **Read / write execution** ({@link executeReadAsync} / {@link executeCountAsync} /
 *     {@link executeWriteAsync}) — compile the model's bundle and run it through `executeBundleAsync`
 *     (reads, routed reader ctx) / `executeTransactionAsync` (writes, writer, write=tx guard). Both go
 *     through the ctx seam, so Phase D middleware + Phase C routing + an ambient Phase B transaction all
 *     apply. Phase E hard-limits stay enforced at the `DBModel` method layer (v1 parity), unchanged.
 *
 * The public API + README code are UNCHANGED: `DBModel`'s method signatures / return shapes are
 * identical; this module is an INTERNAL execution substrate. Reads return RAW driver rows (no SCP
 * de-box result mangling — DBModel's own `_createInstance` → `typeCastFromDB` casts them, v1 parity);
 * the model's `static columns` (from `deriveModelColumns` via the decorator `baseSqlType`, #105 option
 * B) type the read graph so the SCP typed-read gate is satisfied and BIGINT/DATE arrive coercible.
 */

import 'reflect-metadata';
import {
  buildRoutingConfig,
  type PoolCloser,
  type ConnectionConfig,
  type ResolvedConnectionConfig,
  type PoolFactory,
} from './connection-routing';
import { PooledAsyncContext } from './exec-context';
import {
  pgPoolFactory,
  mysqlPoolFactory,
} from './makesql/pool-executor';
import { executeBehaviorAsync, type SqlBundle } from './runtime';
import { executeTransactionAsync, type TransactionResult } from './makesql/tx';
import type { TransactionOptions } from './tx-options';
import {
  findAuthoring,
  countAuthoring,
  createAuthoring,
  updateAuthoring,
  deleteAuthoring,
  compileReadContract,
  compileCommandBundle,
  compileCreateBundle,
  compileUpdateBundle,
  compileDeleteBundle,
  type ReadAuthoringSpec,
  type InsertAuthoringSpec,
  type ModelClassLike,
  type DeriveColumnsOptions,
} from './decorator-adapter';
import { entityWrites } from './writes';
import { whereRawPredicate } from './authoring-sql';
import { renderPlaceholders } from './makesql/handler';
import type { Recorded, Scope, Value } from 'behavior-contracts';
import type { DialectName } from './dialect';
import { DBConditions, type ConditionObject } from '../DBConditions';
import type { SqlCastFormatter } from '../DBValues';

// ── 1. Connection wiring (setConfig → PooledAsyncContext over real pools) ────────────────────────

/** A v1 `DBConfig`-shaped input (host/port/database/user/password + pool sizing + keepAlive). */
export interface RuntimeDbConfig {
  readonly host?: string;
  readonly port?: number;
  readonly database?: string;
  readonly user?: string;
  readonly password?: string;
  readonly max?: number;
  readonly keepAlive?: boolean;
  readonly keepAliveInitialDelayMillis?: number;
  readonly driver?: 'postgres' | 'mysql' | 'sqlite';
}

/** Options for {@link buildContextFromConfig} — reader/writer split + writer-sticky (C3 / v1 parity). */
export interface RuntimeContextOptions {
  /** A distinct WRITER connection config (reader/writer replica split). Absent ⇒ reader === writer. */
  readonly writerConfig?: RuntimeDbConfig;
  /** Keep routing to the writer for `writerStickyDuration` after a committed tx (read-your-writes). */
  readonly useWriterAfterTransaction?: boolean;
  /** The writer-sticky window in ms (default 5000). */
  readonly writerStickyDuration?: number;
}

/** The assembled runtime context: the routed ctx, the dialect, and a closer. */
export interface RuntimeContext {
  /**
   * The Phase A-E execution context (routing + middleware + tx-pinning) over real pools. This IS the
   * async read/write seam: reads run `bindBehaviors().runAsync` over `executeBehaviorAsync({execAsync: ctx})`
   * (#141 — the retired reader `SqlExecutorAsync` / `executeReadGraphAsync` path is gone).
   */
  readonly ctx: PooledAsyncContext;
  /** The compiled SQL dialect (`postgres` / `mysql`). */
  readonly dialect: DialectName;
  /** Shut every constructed pool (v1 `closeAllPools`). */
  readonly close: PoolCloser;
}

/** Map a {@link RuntimeDbConfig} to the C3 {@link ConnectionConfig} pool-construction knobs. */
function toConnectionConfig(config: RuntimeDbConfig): ConnectionConfig {
  return {
    ...(config.host !== undefined ? { host: config.host } : {}),
    ...(config.port !== undefined ? { port: config.port } : {}),
    ...(config.database !== undefined ? { database: config.database } : {}),
    ...(config.user !== undefined ? { user: config.user } : {}),
    ...(config.password !== undefined ? { password: config.password } : {}),
    ...(config.max !== undefined ? { maxPool: config.max } : {}),
    ...(config.keepAlive !== undefined ? { keepAlive: config.keepAlive } : {}),
    ...(config.keepAliveInitialDelayMillis !== undefined ? { keepAliveInitialDelayMillis: config.keepAliveInitialDelayMillis } : {}),
  };
}

/** Lazily `require` the pg module (optional peer dep). */
function requirePg(): { Pool: new (config: Record<string, unknown>) => { end?: () => Promise<void> }; types: { setTypeParser(oid: number, parser: (value: string) => unknown): void } } {
  return require('pg');
}

/** Lazily `require` the mysql2/promise module (optional peer dep). */
function requireMysql2(): { createPool(config: Record<string, unknown>): { end?: () => Promise<void> } } {
  return require('mysql2/promise');
}

/**
 * A {@link PoolFactory} that builds the reader pool from the primary config and (when the setup asks
 * for `separateWriter`) the writer pool from `writerConfig` — a genuine reader/writer replica split on
 * distinct hosts. `buildRoutingConfig` calls this with the resolved config so sizing/keepAlive/de-box
 * options land at pool construction; the writer role overlays the `writerConfig` connection params.
 */
function splitPoolFactory(baseFactory: PoolFactory, writerConfig: RuntimeDbConfig | undefined): PoolFactory {
  return (resolved: ResolvedConnectionConfig, role: 'reader' | 'writer') => {
    if (role === 'writer' && writerConfig !== undefined) {
      const overlaid = { ...resolved, ...toConnectionConfig(writerConfig) } as ResolvedConnectionConfig;
      return baseFactory(overlaid, 'writer');
    }
    return baseFactory(resolved, role);
  };
}

/**
 * Build a Phase A-E {@link PooledAsyncContext} + reader executor from a v1 `DBConfig` (the
 * `setConfig → ConnectionRegistry → pool` wiring). Constructs a REAL pg / mysql2 pool via
 * `buildRoutingConfig` + the driver pool factory (sizing/keepAlive applied at pool construction), wires
 * the default connection, and — when `writerConfig` is present — a distinct writer pool (reader/writer
 * replica split). The writer-sticky clock is armed per `useWriterAfterTransaction` / `writerStickyDuration`.
 *
 * `sqlite` is NOT routed here (it keeps the v1 in-proc path). Throws for an unsupported driver.
 */
export function buildContextFromConfig(config: RuntimeDbConfig, options: RuntimeContextOptions = {}): RuntimeContext {
  const driver = config.driver ?? 'postgres';
  if (driver === 'sqlite') {
    throw new Error('scp dbmodel-runtime: the sqlite dialect is not routed through the async SCP runtime (v1 in-proc path).');
  }
  const dialect: DialectName = driver === 'mysql' ? 'mysql' : 'postgres';

  const stickyOpts = {
    ...(options.useWriterAfterTransaction !== undefined ? { useWriterAfterTransaction: options.useWriterAfterTransaction } : {}),
    ...(options.writerStickyDuration !== undefined ? { writerStickyDuration: options.writerStickyDuration } : {}),
  };

  const baseFactory: PoolFactory = dialect === 'mysql' ? mysqlPoolFactory(requireMysql2() as never) : pgPoolFactory(requirePg() as never);
  const hasWriter = options.writerConfig !== undefined;
  const built = buildRoutingConfig(
    [{
      config: toConnectionConfig(config),
      poolFactory: hasWriter ? splitPoolFactory(baseFactory, options.writerConfig) : baseFactory,
      separateWriter: hasWriter,
    }],
    stickyOpts,
  );
  const ctx = new PooledAsyncContext(built.routing);

  // The `ctx` (PooledAsyncContext) IS the async read/write seam — reads run through
  // `executeBehaviorAsync({execAsync: ctx})` (bindBehaviors().runAsync over the executeSQL leaf). No
  // separate reader `SqlExecutorAsync` (#141 — the old reader-executor + async ReadGraph engine is gone).
  const runtime: RuntimeContext = { ctx, dialect, close: built.close };
  return runtime;
}

// ── 2. v1-conditions → SCP where bridge (one raw-predicate member, byte-true) ────────────────────

/** The where-fragment builder + the runtime input Scope keyed by the refs it emits. */
export interface WhereBridge {
  /** The `($) => readonly unknown[]` where-fragment list the eager `find`/`count` authoring consumes. */
  readonly where: ($: Recorded) => readonly unknown[];
  /** The runtime input Scope carrying the bound values under the ref keys the where fragments read. */
  readonly input: Record<string, Value>;
  /** True when the condition object produced no WHERE (an unconditional read). */
  readonly empty: boolean;
}

/**
 * Bridge a v1 {@link ConditionObject} into the SCP where-fragment builder + input Scope. The ENTIRE
 * condition object is compiled ONCE by the ORIGINAL `DBConditions.compile()` — the SAME builder the v1
 * imperative path uses — to its byte-true `(whereSql, params)` (with `?` placeholders). That whole
 * predicate is carried onto the SCP where port as ONE {@link whereRawPredicate} member; the bound
 * values ride in the returned `input` Scope under stable slot names (`p0`, `p1`, …) referenced by the
 * fragment's value-specs, so they flow as normal SCP input (never baked literals) and the makesql
 * render defers them 1:1 with the placeholders. Covers EVERY v1 condition shape with no per-shape
 * re-authoring — the text is v1's, so parity is by construction. A `formatter` (driver SQL-cast) is
 * threaded so `dbCast`/UUID casts render per-dialect exactly as v1.
 */
export function conditionsToWhere(conditions: ConditionObject, formatter?: SqlCastFormatter): WhereBridge {
  const params: unknown[] = [];
  const sql = new DBConditions(conditions).compile(params, formatter);
  if (sql === '') {
    return { where: () => [], input: {}, empty: true };
  }
  const input: Record<string, Value> = {};
  const slots = params.map((p, i) => {
    const key = `p${i}`;
    input[key] = p as Value;
    return key;
  });
  return {
    where: ($: Recorded) => [
      whereRawPredicate($, { sql, params: slots.map((k) => ($ as unknown as Record<string, Recorded>)[k]) }),
    ],
    input,
    empty: false,
  };
}

// ── 3. Read / write execution (bundle → executeBundleAsync / executeTransactionAsync) ────────────

/** Compile + run a read (find/findOne/findById) as a routed reader through the SCP runtime. */
export async function executeReadAsync(
  model: ModelClassLike,
  method: string,
  spec: ReadAuthoringSpec,
  bridge: WhereBridge,
  ctx: RuntimeContext,
  columnsOptions?: DeriveColumnsOptions,
): Promise<Record<string, unknown>[]> {
  const table = tableOf(model);
  const fullSpec: ReadAuthoringSpec = { ...spec, where: bridge.empty ? undefined : bridge.where };
  // #141 async read: author the op-independent leaf graph (contract) and run it via the async leaf
  // path (`bindBehaviors().runAsync` over the `PooledAsyncContext`), superseding the retired
  // ReadGraph/`executeReadGraphAsync` bundle path.
  const contract = compileReadContract(model, method, findAuthoring(table, fullSpec, ctx.dialect), ctx.dialect, columnsOptions);
  const out = await executeBehaviorAsync(contract, bridge.input as Scope, { execAsync: ctx.ctx, entry: method, dialect: ctx.dialect });
  return out as unknown as Record<string, unknown>[];
}

/** Compile + run a count as a routed reader. Returns the raw `[{ count }]` rows. */
export async function executeCountAsync(
  model: ModelClassLike,
  bridge: WhereBridge,
  ctx: RuntimeContext,
  columnsOptions?: DeriveColumnsOptions,
): Promise<Record<string, unknown>[]> {
  const table = tableOf(model);
  const contract = compileReadContract(model, 'count', countAuthoring(table, bridge.empty ? undefined : bridge.where, ctx.dialect), ctx.dialect, columnsOptions);
  const out = await executeBehaviorAsync(contract, bridge.input as Scope, { execAsync: ctx.ctx, entry: 'count', dialect: ctx.dialect });
  return out as unknown as Record<string, unknown>[];
}

/** Run a write bundle's transaction plan on the writer through the SCP tx runtime (write=tx guard). */
export function executeWriteAsync(
  bundle: SqlBundle,
  input: Scope,
  ctx: RuntimeContext,
  options: TransactionOptions = {},
): Promise<TransactionResult> {
  if (bundle.transaction === undefined) {
    throw new Error(`scp dbmodel-runtime: write bundle '${bundle.name}' carries no transaction plan`);
  }
  return executeTransactionAsync(ctx.ctx, bundle.transaction, input, ctx.dialect, options);
}

/** The effective table name for a decorated model (v1 `@model` rule). */
function tableOf(model: ModelClassLike): string {
  return model.TABLE_NAME ?? model.name.toLowerCase();
}

/**
 * Render a v1-built raw SQL string (with `?` placeholders) to the ctx dialect's placeholder form for
 * the escape-hatch seam path: `postgres` renumbers `?`→`$N` (string-literal-aware, via the SAME
 * `renderPlaceholders` the makesql render uses); `mysql` keeps `?`. v1's DBHandler did this `?`→`$N`
 * conversion on the imperative path — the SCP seam passes SQL verbatim to the driver, so the raw path
 * must renumber here to stay byte-equivalent. Phase F-2 (#105).
 */
export function renderRawSql(sql: string, dialect: DialectName): string {
  return renderPlaceholders(sql, dialect === 'postgres' ? 'postgres' : dialect === 'mysql' ? 'mysql' : 'sqlite');
}

// Re-export the authoring builders so DBModel imports one module.
export {
  findAuthoring, countAuthoring, createAuthoring, updateAuthoring, deleteAuthoring,
  compileCommandBundle, compileCreateBundle, compileUpdateBundle, compileDeleteBundle, entityWrites,
};
export type { ReadAuthoringSpec, InsertAuthoringSpec, ModelClassLike, DeriveColumnsOptions };
