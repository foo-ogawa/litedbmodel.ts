/**
 * litedbmodel - Middleware System
 * 
 * Class-based middleware with per-request instance via AsyncLocalStorage.
 */

import { AsyncLocalStorage } from 'async_hooks';
import type { DBModel } from './DBModel';
import type { SelectOptions, InsertOptions, UpdateOptions, DeleteOptions, UpdateManyOptions, PkeyResult } from './types';
import type { Column, Conds } from './Column';

// ===========================================
// Execute Result Type
// ===========================================

/** 
 * Result from SQL execution 
 * @internal
 */
export interface ExecuteResult {
  rows: Record<string, unknown>[];
  rowCount: number | null;
}

// ===========================================
// Next Function Types
// ===========================================

/** @internal */
export type NextFind<T extends typeof DBModel> = (
  conditions: Conds,
  options?: SelectOptions
) => Promise<InstanceType<T>[]>;

/** @internal */
export type NextFindOne<T extends typeof DBModel> = (
  conditions: Conds,
  options?: SelectOptions
) => Promise<InstanceType<T> | null>;

/** @internal */
export type NextFindById<T extends typeof DBModel> = (
  id: unknown | PkeyResult,
  options?: SelectOptions
) => Promise<InstanceType<T>[]>;

/** @internal */
export type NextCount = (
  conditions: Conds
) => Promise<number>;

/** @internal */
export type NextCreate = (
  pairs: readonly (readonly [Column<any, any>, any])[],
  options?: InsertOptions
) => Promise<PkeyResult | null>;

/** @internal */
export type NextCreateMany = (
  pairsArray: readonly (readonly (readonly [Column<any, any>, any])[])[],
  options?: InsertOptions
) => Promise<PkeyResult | null>;

/** @internal */
export type NextUpdate = (
  conditions: Conds,
  values: readonly (readonly [Column<any, any>, any])[],
  options?: UpdateOptions
) => Promise<PkeyResult | null>;

/** @internal */
export type NextUpdateMany = (
  records: readonly (readonly (readonly [Column<any, any>, any])[])[],
  options?: UpdateManyOptions
) => Promise<PkeyResult | null>;

/** @internal */
export type NextDelete = (
  conditions: Conds,
  options?: DeleteOptions
) => Promise<PkeyResult | null>;

/** @internal */
export type NextExecute = (
  sql: string,
  params?: unknown[]
) => Promise<ExecuteResult>;

/** @internal */
export type NextQuery<T extends typeof DBModel> = (
  sql: string,
  params?: unknown[]
) => Promise<InstanceType<T>[]>;

// ===========================================
// Middleware Base Class
// ===========================================

/**
 * Base class for middlewares.
 * 
 * Middlewares use AsyncLocalStorage to maintain per-request instances.
 * On first access within a request, a new instance is created automatically.
 * 
 * Middleware hooks are called in the following flow:
 * - Method-level: `find`, `findOne`, `findById`, `count`, `create`, `createMany`, `update`, `updateMany`, `delete`
 * - Instantiation-level: `query` — returns model instances from raw SQL
 * - SQL-level: `execute` — intercepts ALL SQL queries
 * 
 * @example
 * ```typescript
 * class LoggerMiddleware extends Middleware {
 *   logs: string[] = [];
 *   
 *   async execute(next: NextExecute, sql: string, params?: unknown[]) {
 *     this.logs.push(sql);
 *     return next(sql, params);
 *   }
 *   
 *   getLogs() {
 *     return this.logs;
 *   }
 * }
 * 
 * // Register
 * DBModel.use(LoggerMiddleware);
 * 
 * // After request
 * console.log(LoggerMiddleware.getCurrentContext().getLogs());
 * ```
 * 
 * @category Middleware
 */
export abstract class Middleware {
  /** AsyncLocalStorage for per-request instances */
  private static _storage: AsyncLocalStorage<Middleware>;
  
  /** Get or create storage for this class */
  private static getStorage(): AsyncLocalStorage<Middleware> {
    // Each subclass gets its own storage
    if (!this.hasOwnProperty('_storage') || !this._storage) {
      this._storage = new AsyncLocalStorage<Middleware>();
    }
    return this._storage;
  }
  
  /**
   * Get current request's instance.
   * Creates a new instance on first access within a request.
   */
  static getCurrentContext<T extends Middleware>(this: new () => T): T {
    const storage = (this as unknown as typeof Middleware).getStorage();
    let instance = storage.getStore() as T | undefined;
    if (!instance) {
      instance = new this();
      storage.enterWith(instance);
    }
    return instance;
  }
  
  /**
   * Run a function with a fresh middleware context.
   * Useful for explicit context boundaries (e.g., in tests).
   */
  static run<T extends Middleware, R>(this: new () => T, fn: () => R): R {
    const storage = (this as unknown as typeof Middleware).getStorage();
    const instance = new this();
    return storage.run(instance, fn);
  }
  
  /**
   * Check if currently in a context
   */
  static hasContext(): boolean {
    return this.getStorage().getStore() !== undefined;
  }
  
  /**
   * Clear current context (for testing)
   */
  static clearContext(): void {
    const storage = this.getStorage();
    if (storage.getStore()) {
      storage.enterWith(undefined as unknown as Middleware);
    }
  }

  // ===========================================
  // Hooks (override in subclass)
  // ===========================================
  
  /** Called when instance is created */
  init?(): void;
  
  /** Intercept find() */
  find?<T extends typeof DBModel>(
    this: InstanceType<typeof Middleware>,
    model: T,
    next: NextFind<T>,
    conditions: Conds,
    options?: SelectOptions
  ): Promise<InstanceType<T>[]>;
  
  /** Intercept findOne() */
  findOne?<T extends typeof DBModel>(
    this: InstanceType<typeof Middleware>,
    model: T,
    next: NextFindOne<T>,
    conditions: Conds,
    options?: SelectOptions
  ): Promise<InstanceType<T> | null>;
  
  /** Intercept findById() */
  findById?<T extends typeof DBModel>(
    this: InstanceType<typeof Middleware>,
    model: T,
    next: NextFindById<T>,
    id: unknown | PkeyResult,
    options?: SelectOptions
  ): Promise<InstanceType<T>[]>;
  
  /** Intercept count() */
  count?<T extends typeof DBModel>(
    this: InstanceType<typeof Middleware>,
    model: T,
    next: NextCount,
    conditions: Conds
  ): Promise<number>;
  
  /** Intercept create() */
  create?<T extends typeof DBModel>(
    this: InstanceType<typeof Middleware>,
    model: T,
    next: NextCreate,
    pairs: readonly (readonly [Column<any, any>, any])[],
    options?: InsertOptions
  ): Promise<PkeyResult | null>;
  
  /** Intercept createMany() */
  createMany?<T extends typeof DBModel>(
    this: InstanceType<typeof Middleware>,
    model: T,
    next: NextCreateMany,
    pairsArray: readonly (readonly (readonly [Column<any, any>, any])[])[],
    options?: InsertOptions
  ): Promise<PkeyResult | null>;
  
  /** Intercept update() */
  update?<T extends typeof DBModel>(
    this: InstanceType<typeof Middleware>,
    model: T,
    next: NextUpdate,
    conditions: Conds,
    values: readonly (readonly [Column<any, any>, any])[],
    options?: UpdateOptions
  ): Promise<PkeyResult | null>;
  
  /** Intercept updateMany() */
  updateMany?<T extends typeof DBModel>(
    this: InstanceType<typeof Middleware>,
    model: T,
    next: NextUpdateMany,
    records: readonly (readonly (readonly [Column<any, any>, any])[])[],
    options?: UpdateManyOptions
  ): Promise<PkeyResult | null>;
  
  /** Intercept delete() */
  delete?<T extends typeof DBModel>(
    this: InstanceType<typeof Middleware>,
    model: T,
    next: NextDelete,
    conditions: Conds,
    options?: DeleteOptions
  ): Promise<PkeyResult | null>;
  
  /** Intercept execute() */
  execute?(
    this: InstanceType<typeof Middleware>,
    next: NextExecute,
    sql: string,
    params?: unknown[]
  ): Promise<ExecuteResult>;
  
  /** Intercept query() */
  query?<T extends typeof DBModel>(
    this: InstanceType<typeof Middleware>,
    model: T,
    next: NextQuery<T>,
    sql: string,
    params?: unknown[]
  ): Promise<InstanceType<T>[]>;
}

/** Type for middleware class (not instance) */
export type MiddlewareClass = typeof Middleware & (new () => Middleware);
