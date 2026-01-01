/**
 * litedbmodel - Base Model Class
 */

import { AsyncLocalStorage } from 'async_hooks';
import { DBBoolValue, DBNullValue, DBNotNullValue, DBImmediateValue, DBToken, DBSubquery, DBExists, type SubqueryCondition } from './DBValues';
import { normalizeConditions, type ConditionObject } from './DBConditions';
import { initDBHandler, getDBHandler, createHandlerWithConnection, type DBHandler, type DBConfig, type DBConnection } from './DBHandler';
import type { SelectOptions, InsertOptions, UpdateOptions, DeleteOptions, TransactionOptions, LimitConfig } from './types';
import { LimitExceededError } from './types';
import { type Column, type OrderSpec, type CVs, type Conds, type CondsOf, type OrCondOf, createColumn, columnsToNames, pairsToRecord, condsToRecord, orderToString, createOrCond } from './Column';
import type { MiddlewareClass, ExecuteResult } from './Middleware';
import { serializeRecord, getColumnMeta, type KeyPair, type CompositeKeyPairs } from './decorators';

// Import LazyRelation module (static import for Vitest compatibility)
import { LazyRelationContext, type RelationType, type RelationConfig } from './LazyRelation';

// Transaction context stored in AsyncLocalStorage
interface TransactionContext {
  connection: DBConnection;
}

// AsyncLocalStorage for transaction context
const transactionContext = new AsyncLocalStorage<TransactionContext>();

/**
 * Get current transaction context (if in a transaction)
 */
export function getTransactionContext(): TransactionContext | undefined {
  return transactionContext.getStore();
}

/**
 * Get current transaction connection (if in a transaction)
 */
export function getTransactionConnection(): DBConnection | undefined {
  return transactionContext.getStore()?.connection;
}

/**
 * @deprecated Use getTransactionConnection() instead
 */
export function getTransactionClient(): DBConnection | undefined {
  return getTransactionConnection();
}

// ============================================
// DBModel - Base Model Class
// ============================================

export abstract class DBModel {
  // ============================================
  // Static Table Configuration (Override in derived classes)
  // ============================================

  /** Table name */
  static TABLE_NAME: string = '';

  /** Table name for UPDATE/DELETE (if different from TABLE_NAME) */
  static UPDATE_TABLE_NAME: string | null = null;

  /** Default SELECT columns */
  static SELECT_COLUMN: string = '*';

  /** Default ORDER BY clause (type-safe OrderColumn or OrderColumn[]) */
  static DEFAULT_ORDER: OrderSpec | null = null;

  /** Default GROUP BY clause (Column, Column[], or raw string) */
  static DEFAULT_GROUP: Column | Column[] | string | null = null;

  /** Default filter conditions applied to all queries (use tuple format like find()) */
  static FIND_FILTER: Conds | null = null;

  /**
   * SQL query for query-based models (view models, aggregations, etc.)
   * When defined, the model uses this query as a CTE instead of TABLE_NAME.
   * 
   * @example
   * ```typescript
   * // Static query
   * static QUERY = `
   *   SELECT users.id, COUNT(posts.id) as post_count
   *   FROM users LEFT JOIN posts ON users.id = posts.user_id
   *   GROUP BY users.id
   * `;
   * ```
   */
  static QUERY: string | null = null;

  /**
   * Query parameters for QUERY (set via withQuery())
   * @internal
   */
  protected static _queryParams: unknown[] | null = null;

  /** Primary key columns (use getter to reference Model.column) */
  static PKEY_COLUMNS: Column[] | null = null;

  /** Sequence name for auto-increment (use getter if needed) */
  static SEQ_NAME: string | null = null;

  /** ID type: 'serial' for auto-increment, 'uuid' for UUID generation */
  static ID_TYPE: 'serial' | 'uuid' | null = null;

  // ============================================
  // Database Configuration
  // ============================================

  /** Database config */
  private static _dbConfig: DBConfig | null = null;

  /** Limit config for safety guards */
  private static _limitConfig: LimitConfig = {};

  /**
   * Initialize DBModel with database config.
   * Call this once at application startup.
   * 
   * @example
   * ```typescript
   * import { DBModel } from 'litedbmodel';
   * 
   * DBModel.setConfig({
   *   host: 'localhost',
   *   port: 5432,
   *   database: 'mydb',
   *   user: 'user',
   *   password: 'pass',
   * }, {
   *   // Optional: Set global limits
   *   hardLimit: 10000,      // find() throws if > 10000 records
   *   lazyLoadLimit: 1000,   // hasMany throws if > 1000 records per key
   * });
   * 
   * // Now you can use all DBModel methods
   * const users = await User.find([[User.is_active, true]]);
   * ```
   */
  static setConfig(
    config: DBConfig,
    options?: {
      writerConfig?: DBConfig;
      logger?: import('./types').Logger;
      /** Hard limit for find() - throws if exceeded */
      hardLimit?: number | null;
      /** Hard limit for hasMany lazy loading - throws if exceeded */
      lazyLoadLimit?: number | null;
    }
  ): void {
    initDBHandler(config, { writerConfig: options?.writerConfig, logger: options?.logger });
    this._dbConfig = config;
    this._limitConfig = {
      hardLimit: options?.hardLimit,
      lazyLoadLimit: options?.lazyLoadLimit,
    };
  }

  /**
   * Get current limit configuration.
   */
  static getLimitConfig(): LimitConfig {
    return { ...this._limitConfig };
  }

  /**
   * Update limit configuration.
   * @example
   * ```typescript
   * // Set limits after initial config
   * DBModel.setLimitConfig({ hardLimit: 5000, lazyLoadLimit: 500 });
   * 
   * // Disable limits
   * DBModel.setLimitConfig({ hardLimit: null, lazyLoadLimit: null });
   * ```
   */
  static setLimitConfig(config: LimitConfig): void {
    this._limitConfig = { ...this._limitConfig, ...config };
  }

  /**
   * Get database config
   */
  static getDBConfig(): DBConfig | null {
    return this._dbConfig;
  }

  /**
   * Get the database driver type.
   * Returns 'postgres', 'mysql', or 'sqlite'.
   */
  static getDriverType(): 'postgres' | 'mysql' | 'sqlite' {
    return getDBHandler().getDriverType();
  }

  /**
   * Get a DBHandler instance for this model
   * If in a transaction, uses the transaction connection
   */
  protected static getHandler(): DBHandler {
    // Check if we're in a transaction
    const txContext = transactionContext.getStore();
    if (txContext) {
      return createHandlerWithConnection(txContext.connection);
    }

    return getDBHandler();
  }

  // ============================================
  // Middleware System
  // ============================================

  /** Middleware class stack */
  private static _middlewares: MiddlewareClass[] = [];

  /**
   * Register a middleware class to intercept DBModel methods.
   * 
   * Each request gets its own middleware instance via AsyncLocalStorage.
   * 
   * @param MiddlewareClass - Middleware class to register
   * @returns Function to unregister the middleware
   * 
   * @example
   * ```typescript
   * class LoggerMiddleware extends Middleware {
   *   logs: string[] = [];
   *   
   *   async execute(next: NextExecute, sql: string, params?: unknown[]) {
   *     this.logs.push(sql);
   *     const start = Date.now();
   *     const result = await next(sql, params);
   *     console.log(`SQL: ${sql} (${Date.now() - start}ms)`);
   *     return result;
   *   }
   *   
   *   getLogs() {
   *     return this.logs;
   *   }
   * }
   * 
   * DBModel.use(LoggerMiddleware);
   * 
   * // After queries
   * console.log(LoggerMiddleware.getCurrentContext().getLogs());
   * ```
   */
  static use(MiddlewareClass: MiddlewareClass): () => void {
    this._middlewares.push(MiddlewareClass);
    
    return () => {
      const index = this._middlewares.indexOf(MiddlewareClass);
      if (index !== -1) {
        this._middlewares.splice(index, 1);
      }
    };
  }

  /**
   * Remove a middleware class
   * @param MiddlewareClass - Middleware class to remove
   * @returns true if middleware was found and removed
   */
  static removeMiddleware(MiddlewareClass: MiddlewareClass): boolean {
    const index = this._middlewares.indexOf(MiddlewareClass);
    if (index !== -1) {
      this._middlewares.splice(index, 1);
      return true;
    }
    return false;
  }

  /**
   * Clear all middlewares (useful for testing)
   */
  static clearMiddlewares(): void {
    this._middlewares = [];
  }

  /**
   * Get registered middleware classes
   */
  static getMiddlewares(): readonly MiddlewareClass[] {
    return this._middlewares;
  }

  /**
   * Apply middlewares to a core function (with model context)
   * @internal
   */
  private static _applyMiddleware<Args extends unknown[], R>(
    methodName: 'find' | 'findOne' | 'findById' | 'count' | 'create' | 'createMany' | 'update' | 'delete' | 'query',
    core: (...args: Args) => Promise<R>,
    args: Args
  ): Promise<R> {
    // Fast path: no middlewares registered
    if (this._middlewares.length === 0) {
      return core(...args);
    }
    
    let next: (...a: Args) => Promise<R> = core;
    for (let i = this._middlewares.length - 1; i >= 0; i--) {
      const MWClass = this._middlewares[i];
      const instance = MWClass.getCurrentContext();
      const method = instance[methodName] as ((...a: unknown[]) => Promise<R>) | undefined;
      if (method) {
        const currentNext = next;
        next = (...a: Args) => method.call(instance, this, currentNext, ...a);
      }
    }
    return next(...args);
  }

  /**
   * Apply middlewares to execute (no model context)
   * @internal
   */
  private static _applyExecuteMiddleware(
    core: (sql: string, params?: unknown[]) => Promise<ExecuteResult>,
    sql: string,
    params?: unknown[]
  ): Promise<ExecuteResult> {
    // Fast path: no middlewares registered
    if (this._middlewares.length === 0) {
      return core(sql, params);
    }
    
    let next = core;
    for (let i = this._middlewares.length - 1; i >= 0; i--) {
      const MWClass = this._middlewares[i];
      const instance = MWClass.getCurrentContext();
      if (instance.execute) {
        const currentNext = next;
        next = (s, p) => instance.execute!(currentNext, s, p);
      }
    }
    return next(sql, params);
  }

  // ============================================
  // SQL Generation (Internal)
  // ============================================

  /**
   * Build SELECT SQL from conditions and options
   * @internal
   */
  protected static _buildSelectSQL<T extends typeof DBModel>(
    this: T,
    conditions: ConditionObject,
    options: SelectOptions = {}
  ): { sql: string; params: unknown[] } {
    const params: unknown[] = [];
    const isQueryBased = this.isQueryBased();
    const cteAlias = this.getCTEAlias();
    const tableName = options.tableName || (isQueryBased ? cteAlias : this.TABLE_NAME);
    const selectCols = options.select || this.SELECT_COLUMN;

    // Parameter order (matches SQL order):
    // 1. CTE params (WITH clause)
    // 2. Query params (query-based model)
    // 3. Join params (FROM/JOIN clause)
    // 4. Condition params (WHERE clause, added by compile)

    // Handle custom CTE params first
    if (options.cte?.params && options.cte.params.length > 0) {
      params.push(...options.cte.params);
    }

    // Handle QUERY params (prepended to all other params)
    const queryParams = this.getQueryParams();
    if (queryParams.length > 0) {
      params.push(...queryParams);
    }

    // Handle JOIN params (prepended to condition params)
    if (options.joinParams && options.joinParams.length > 0) {
      params.push(...options.joinParams);
    }

    const normalizedCond = normalizeConditions(conditions);
    if (this.FIND_FILTER) {
      const filterCondition = condsToRecord(this.FIND_FILTER) as ConditionObject;
      normalizedCond.add(filterCondition);
    }

    const whereClause = normalizedCond.compile(params);

    // Build CTE prefix
    let sql = '';
    
    // Custom CTE takes precedence
    if (options.cte) {
      sql = `WITH ${options.cte.name} AS (${options.cte.sql}) `;
    }
    
    // Query-based CTE (append with comma if custom CTE exists)
    if (isQueryBased && this.QUERY) {
      if (sql) {
        // Remove trailing space and add comma
        sql = sql.slice(0, -1) + `, ${cteAlias} AS (${this.QUERY}) `;
      } else {
        sql = `WITH ${cteAlias} AS (${this.QUERY}) `;
      }
    }

    sql += `SELECT ${selectCols} FROM ${tableName}`;
    
    // Add JOIN clause if provided
    if (options.join) {
      sql += ` ${options.join}`;
    }
    
    if (whereClause) {
      sql += ` WHERE ${whereClause}`;
    }

    const groupClause = options.group || this.getGroupByClause();
    if (groupClause) {
      sql += ` GROUP BY ${groupClause}`;
    }

    const orderClause = options.order || orderToString(this.DEFAULT_ORDER);
    if (orderClause) {
      sql += ` ORDER BY ${orderClause}`;
    }

    if (options.limit !== undefined) {
      sql += ` LIMIT ${options.limit}`;
    }

    if (options.offset !== undefined) {
      sql += ` OFFSET ${options.offset}`;
    }

    if (options.forUpdate) {
      sql += ' FOR UPDATE';
    }

    if (options.append) {
      sql += ` ${options.append}`;
    }

    return { sql, params };
  }

  /**
   * Build SELECT SQL without executing.
   * Useful for constructing CTE/subquery SQL fragments.
   * Returns SQL with ? placeholders and params array.
   * 
   * @param conditions - WHERE conditions
   * @param options - SELECT options (order, limit, select, etc.)
   * @param params - Optional parameter array to append to (for joining with outer query)
   * @returns Object with sql and params
   * 
   * @example
   * ```typescript
   * // Build a subquery SQL
   * const { sql, params } = User.buildSelectSQL(
   *   { status: 'active' },
   *   { select: 'id', order: 'created_at DESC', limit: 10 }
   * );
   * // sql: "SELECT id FROM users WHERE status = ? ORDER BY created_at DESC LIMIT 10"
   * // params: ['active']
   * ```
   */
  public static buildSelectSQL<T extends typeof DBModel>(
    this: T,
    conditions: ConditionObject,
    options: SelectOptions = {},
    params: unknown[] = []
  ): { sql: string; params: unknown[] } {
    // Build SQL fragment (without FIND_FILTER for flexibility)
    const tableName = options.tableName || this.TABLE_NAME;
    const selectCols = options.select || this.SELECT_COLUMN;

    // Handle CTE params first
    if (options.cte?.params && options.cte.params.length > 0) {
      params.push(...options.cte.params);
    }

    // Handle JOIN params
    if (options.joinParams && options.joinParams.length > 0) {
      params.push(...options.joinParams);
    }

    const normalizedCond = normalizeConditions(conditions);
    const whereClause = normalizedCond.compile(params);

    // Build SQL
    let sql = '';
    
    // Custom CTE
    if (options.cte) {
      sql = `WITH ${options.cte.name} AS (${options.cte.sql}) `;
    }

    sql += `SELECT ${selectCols} FROM ${tableName}`;
    
    // Add JOIN clause if provided
    if (options.join) {
      sql += ` ${options.join}`;
    }
    
    if (whereClause) {
      sql += ` WHERE ${whereClause}`;
    }

    if (options.group) {
      sql += ` GROUP BY ${options.group}`;
    }

    if (options.order) {
      const orderStr = typeof options.order === 'string' ? options.order : orderToString(options.order);
      if (orderStr) {
        sql += ` ORDER BY ${orderStr}`;
      }
    }

    if (options.limit !== undefined) {
      sql += ` LIMIT ${options.limit}`;
    }

    if (options.offset !== undefined) {
      sql += ` OFFSET ${options.offset}`;
    }

    if (options.append) {
      sql += ` ${options.append}`;
    }

    return { sql, params };
  }

  /**
   * Build SELECT SQL and execute via query()
   * @internal
   */
  private static async _select<T extends typeof DBModel>(
    this: T,
    conditions: ConditionObject,
    options: SelectOptions = {}
  ): Promise<InstanceType<T>[]> {
    const { sql, params } = this._buildSelectSQL(conditions, options);
    return this.query(sql, params);
  }

  /**
   * Select for lazy relation loading.
   * Applies lazyLoadLimit check and supports CTE/raw conditions.
   * Can also accept raw SQL for complex queries (LATERAL JOIN, ROW_NUMBER).
   * 
   * @param conditionsOrSql - Condition object or raw SQL string
   * @param optionsOrParams - Select options or raw SQL params (when using raw SQL)
   * @param relationConfig - Relation configuration for limit checking
   * @returns Model instances
   * @internal - Used by LazyRelation
   */
  static async _selectForRelation<T extends typeof DBModel>(
    this: T,
    conditionsOrSql: ConditionObject | string,
    optionsOrParams: SelectOptions | unknown[] = {},
    relationConfig?: {
      hardLimit?: number | null;
      propertyKey?: string;
      sourceModelName?: string;
    }
  ): Promise<InstanceType<T>[]> {
    // Determine effective hardLimit
    const globalLazyLoadLimit = DBModel._limitConfig.lazyLoadLimit;
    const relationHardLimit = relationConfig?.hardLimit;
    
    // null means disabled, undefined means use global
    const effectiveHardLimit = relationHardLimit === null
      ? null
      : relationHardLimit ?? globalLazyLoadLimit;
    
    let sql: string;
    let params: unknown[];
    let shouldCheckLimit: boolean;
    
    if (typeof conditionsOrSql === 'string') {
      // Raw SQL mode - for complex queries like LATERAL JOIN, ROW_NUMBER
      sql = conditionsOrSql;
      params = optionsOrParams as unknown[];
      // For raw SQL with limit (LATERAL/ROW_NUMBER), skip hardLimit check
      // as limit is already applied at SQL level
      shouldCheckLimit = false;
    } else {
      // Condition object mode
      const options = optionsOrParams as SelectOptions;
      shouldCheckLimit = effectiveHardLimit != null && !options.limit;
      const effectiveOptions = (shouldCheckLimit && effectiveHardLimit != null)
        ? { ...options, limit: effectiveHardLimit + 1 }
        : options;
      
      const built = this._buildSelectSQL(conditionsOrSql, effectiveOptions);
      sql = built.sql;
      params = built.params;
    }
    
    const results = await this.query(sql, params);
    
    // Check if hardLimit was exceeded
    if (shouldCheckLimit && results.length > effectiveHardLimit!) {
      throw new LimitExceededError(
        effectiveHardLimit!,
        results.length,
        'relation',
        relationConfig?.sourceModelName || this.name,
        relationConfig?.propertyKey
      );
    }
    
    return results;
  }

  /**
   * Build COUNT SQL and execute with query middleware
   * @internal
   */
  private static async _count<T extends typeof DBModel>(
    this: T,
    conditions: ConditionObject,
    options: { tableName?: string } = {}
  ): Promise<number> {
    const params: unknown[] = [];
    const isQueryBased = this.isQueryBased();
    const cteAlias = this.getCTEAlias();
    const tableName = options.tableName || (isQueryBased ? cteAlias : this.TABLE_NAME);

    // Handle QUERY params first (prepended to all other params)
    const queryParams = this.getQueryParams();
    if (queryParams.length > 0) {
      params.push(...queryParams);
    }

    const normalizedCond = normalizeConditions(conditions);
    if (this.FIND_FILTER) {
      const filterCondition = condsToRecord(this.FIND_FILTER) as ConditionObject;
      normalizedCond.add(filterCondition);
    }

    const whereClause = normalizedCond.compile(params);

    // Build CTE prefix if query-based
    let sql = '';
    if (isQueryBased && this.QUERY) {
      sql = `WITH ${cteAlias} AS (${this.QUERY}) `;
    }

    sql += `SELECT COUNT(*) as count FROM ${tableName}`;
    if (whereClause) {
      sql += ` WHERE ${whereClause}`;
    }

    const result = await this.execute(sql, params);
    return parseInt(String((result.rows as Record<string, unknown>[])[0].count), 10);
  }

  /**
   * Build INSERT SQL and execute with query middleware
   * @internal
   */
  private static async _insert<T extends typeof DBModel>(
    this: T,
    values: Record<string, unknown> | Record<string, unknown>[],
    options: InsertOptions<unknown> = {}
  ): Promise<InstanceType<T>[]> {
    const tableName = options.tableName || this.getUpdateTableName();
    const rawRecords = Array.isArray(values) ? values : [values];

    if (rawRecords.length === 0) {
      return [];
    }

    // Apply serialization based on column metadata
    const records = rawRecords.map(r => serializeRecord(this, r));

    const columns = Object.keys(records[0]).filter(
      (k) => !(records[0][k] instanceof DBImmediateValue && records[0][k].value === 'DEFAULT')
    );

    const params: unknown[] = [];
    const valueRows: string[] = [];

    for (const record of records) {
      const rowValues: string[] = [];
      for (const col of columns) {
        const val = record[col];
        if (val instanceof DBToken) {
          rowValues.push(val.compile(params));
        } else if (val === undefined) {
          rowValues.push('DEFAULT');
        } else {
          params.push(val);
          rowValues.push('?');
        }
      }
      valueRows.push(`(${rowValues.join(', ')})`);
    }

    let sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES ${valueRows.join(', ')}`;

    // Handle conflict options (new API takes precedence)
    if (options.onConflict) {
      // Convert Column symbols to strings
      const toColName = (col: unknown): string => 
        typeof col === 'function' || typeof col === 'object' ? String(col) : String(col);
      
      const conflictCols = Array.isArray(options.onConflict)
        ? options.onConflict.map(toColName).join(', ')
        : toColName(options.onConflict);
      sql += ` ON CONFLICT (${conflictCols})`;

      if (options.onConflictIgnore) {
        sql += ' DO NOTHING';
      } else if (options.onConflictUpdate) {
        const updateCols = options.onConflictUpdate === 'all'
          ? columns
          : options.onConflictUpdate.map(toColName);
        const updateClauses = updateCols.map(col => `${col} = EXCLUDED.${col}`);
        sql += ` DO UPDATE SET ${updateClauses.join(', ')}`;
      }
    } else if (options.conflict) {
      // Legacy: raw conflict string
      sql += ` ${options.conflict}`;
    }

    if (options.returning) {
      sql += ` RETURNING ${options.returning}`;
    }

    const result = await this.execute(sql, params);
    return (result.rows as Record<string, unknown>[]).map((row) => this._createInstance<T>(row));
  }

  /**
   * Build UPDATE SQL and execute with query middleware
   * @internal
   */
  private static async _update<T extends typeof DBModel>(
    this: T,
    conditions: ConditionObject,
    values: Record<string, unknown>,
    options: UpdateOptions = {}
  ): Promise<InstanceType<T>[]> {
    const tableName = options.tableName || this.getUpdateTableName();
    const params: unknown[] = [];

    // Apply serialization based on column metadata
    const serializedValues = serializeRecord(this, values);

    const setClauses: string[] = [];
    for (const [col, val] of Object.entries(serializedValues)) {
      if (val instanceof DBToken) {
        setClauses.push(`${col} = ${val.compile(params)}`);
      } else {
        params.push(val);
        setClauses.push(`${col} = ?`);
      }
    }

    if (setClauses.length === 0) {
      return [];
    }

    const normalizedCond = normalizeConditions(conditions);
    const whereClause = normalizedCond.compile(params);

    if (!whereClause) {
      throw new Error('UPDATE requires conditions');
    }

    let sql = `UPDATE ${tableName} SET ${setClauses.join(', ')} WHERE ${whereClause}`;

    if (options.returning) {
      sql += ` RETURNING ${options.returning}`;
    }

    const result = await this.execute(sql, params);
    return (result.rows as Record<string, unknown>[]).map((row) => this._createInstance<T>(row));
  }

  /**
   * Build DELETE SQL and execute with query middleware
   * @internal
   */
  private static async _delete<T extends typeof DBModel>(
    this: T,
    conditions: ConditionObject,
    options: DeleteOptions = {}
  ): Promise<InstanceType<T>[]> {
    const tableName = options.tableName || this.getUpdateTableName();
    const params: unknown[] = [];

    const normalizedCond = normalizeConditions(conditions);
    const whereClause = normalizedCond.compile(params);

    if (!whereClause) {
      throw new Error('DELETE requires conditions');
    }

    let sql = `DELETE FROM ${tableName} WHERE ${whereClause}`;

    if (options.returning) {
      sql += ` RETURNING ${options.returning}`;
    }

    const result = await this.execute(sql, params);
    return (result.rows as Record<string, unknown>[]).map((row) => this._createInstance<T>(row));
  }

  /**
   * Create model instance from raw row data
   * @internal
   */
  protected static _createInstance<T extends typeof DBModel>(
    this: T,
    row: Record<string, unknown>
  ): InstanceType<T> {
    const instance = new (this as unknown as new () => InstanceType<T>)();
    Object.assign(instance, row);
    instance.typeCastFromDB();
    return instance;
  }

  // ============================================
  // Static Convenience Values
  // ============================================

  /** Boolean TRUE value */
  static readonly true = new DBBoolValue(true);

  /** Boolean FALSE value */
  static readonly false = new DBBoolValue(false);

  /** NULL value */
  static readonly null = new DBNullValue();

  /** NOT NULL value */
  static readonly notNull = new DBNotNullValue();

  /** NOW() value */
  static readonly now = new DBImmediateValue('NOW()');

  // ============================================
  // Static Subquery Methods
  // ============================================

  /**
   * IN subquery condition.
   * Creates a condition like: column IN (SELECT selectColumn FROM targetModel WHERE ...)
   * Supports composite keys using key pairs (same format as relation decorators).
   * Type-safe: first column in pair must belong to caller model, second to target model.
   *
   * @typeParam T - Parent model class type (inferred from this)
   * @typeParam S - Target model class type
   * @param keyPairs - Key pairs: [[parentCol, targetCol], ...] or single pair [parentCol, targetCol]
   * @param conditions - WHERE conditions for subquery (columns must belong to target model S)
   * @returns Condition tuple for use in find() conditions
   *
   * @example
   * ```typescript
   * import { parentRef } from 'litedbmodel';
   *
   * // Single key: id IN (SELECT user_id FROM orders WHERE status = 'paid')
   * await User.find([
   *   User.inSubquery([[User.id, Order.user_id]], [
   *     [Order.status, 'paid']
   *   ])
   * ]);
   *
   * // Composite key: (id, group_id) IN (SELECT user_id, group_id FROM orders WHERE ...)
   * await User.find([
   *   User.inSubquery([
   *     [User.id, Order.user_id],
   *     [User.group_id, Order.group_id],
   *   ], [[Order.status, 'paid']])
   * ]);
   *
   * // Correlated subquery with parentRef
   * await User.find([
   *   User.inSubquery([[User.id, Order.user_id]], [
   *     [Order.tenant_id, parentRef(User.tenant_id)],
   *     [Order.status, 'paid']
   *   ])
   * ]);
   * ```
   */
  static inSubquery<T extends typeof DBModel, S>(
    this: T,
    keyPairs: KeyPair | CompositeKeyPairs,
    conditions: Array<readonly [Column<any, S>, unknown]> = []
  ): readonly [string, DBSubquery] {
    // Parse key pairs (same logic as relation decorators)
    const pairs = Array.isArray(keyPairs[0]) ? keyPairs as CompositeKeyPairs : [keyPairs as KeyPair];
    const parentColumns = pairs.map(pair => pair[0]);
    const selectColumns = pairs.map(pair => pair[1]);
    
    // Get target table name from selectColumns (they all have same tableName)
    const targetTableName = selectColumns[0]?.tableName ?? '';
    // Column already has tableName, pass directly
    const subqueryConditions: SubqueryCondition[] = conditions.map(([col, value]) => ({
      column: col,
      value,
    }));
    return ['__subquery__', new DBSubquery(parentColumns, targetTableName, selectColumns, subqueryConditions, 'IN')] as const;
  }

  /**
   * NOT IN subquery condition.
   * Creates a condition like: table.column NOT IN (SELECT table.column FROM targetModel WHERE ...)
   * Supports composite keys using key pairs (same format as relation decorators).
   * Type-safe: first column in pair must belong to caller model, second to target model.
   *
   * @typeParam T - Parent model class type (inferred from this)
   * @typeParam S - Target model class type
   * @param keyPairs - Key pairs: [[parentCol, targetCol], ...] or single pair [parentCol, targetCol]
   * @param conditions - WHERE conditions for subquery (columns must belong to target model S)
   * @returns Condition tuple for use in find() conditions
   *
   * @example
   * ```typescript
   * // users.id NOT IN (SELECT banned_users.user_id FROM banned_users)
   * await User.find([
   *   User.notInSubquery([[User.id, BannedUser.user_id]])
   * ]);
   *
   * // Composite key NOT IN
   * await User.find([
   *   User.notInSubquery([
   *     [User.id, BannedUser.user_id],
   *     [User.tenant_id, BannedUser.tenant_id],
   *   ])
   * ]);
   * ```
   */
  static notInSubquery<T extends typeof DBModel, S>(
    this: T,
    keyPairs: KeyPair | CompositeKeyPairs,
    conditions: Array<readonly [Column<any, S>, unknown]> = []
  ): readonly [string, DBSubquery] {
    // Parse key pairs (same logic as relation decorators)
    const pairs = Array.isArray(keyPairs[0]) ? keyPairs as CompositeKeyPairs : [keyPairs as KeyPair];
    const parentColumns = pairs.map(pair => pair[0]);
    const selectColumns = pairs.map(pair => pair[1]);
    
    // Get target table name from selectColumns (they all have same tableName)
    const targetTableName = selectColumns[0]?.tableName ?? '';
    // Column already has tableName, pass directly
    const subqueryConditions: SubqueryCondition[] = conditions.map(([col, value]) => ({
      column: col,
      value,
    }));
    return ['__subquery__', new DBSubquery(parentColumns, targetTableName, selectColumns, subqueryConditions, 'NOT IN')] as const;
  }

  /**
   * EXISTS subquery condition.
   * Creates a condition like: EXISTS (SELECT 1 FROM targetModel WHERE table.column = ...)
   * Uses table.column format for unambiguous references.
   * Type-safe: conditions columns must belong to the same target model.
   *
   * @typeParam S - Target model instance type
   * @param conditions - WHERE conditions for subquery (columns determine target table)
   * @returns Condition tuple for use in find() conditions
   *
   * @example
   * ```typescript
   * import { parentRef } from 'litedbmodel';
   *
   * // EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
   * await User.find([
   *   [User.is_active, true],
   *   User.exists([
   *     [Order.user_id, parentRef(User.id)]
   *   ])
   * ]);
   * ```
   */
  static exists<S>(
    conditions: Array<readonly [Column<any, S>, unknown]>
  ): readonly [string, DBExists] {
    // Get target table name from conditions (they all have same tableName)
    const targetTableName = conditions[0]?.[0]?.tableName ?? '';
    // Column already has tableName, pass directly
    const subqueryConditions: SubqueryCondition[] = conditions.map(([col, value]) => ({
      column: col,
      value,
    }));
    return ['__exists__', new DBExists(targetTableName, subqueryConditions, false)] as const;
  }

  /**
   * NOT EXISTS subquery condition.
   * Creates a condition like: NOT EXISTS (SELECT 1 FROM targetModel WHERE table.column = ...)
   * Uses table.column format for unambiguous references.
   * Type-safe: conditions columns must belong to the same target model.
   *
   * @typeParam S - Target model instance type
   * @param conditions - WHERE conditions for subquery (columns determine target table)
   * @returns Condition tuple for use in find() conditions
   *
   * @example
   * ```typescript
   * import { parentRef } from 'litedbmodel';
   *
   * // NOT EXISTS (SELECT 1 FROM banned_users WHERE banned_users.user_id = users.id)
   * await User.find([
   *   User.notExists([
   *     [BannedUser.user_id, parentRef(User.id)]
   *   ])
   * ]);
   * ```
   */
  static notExists<S>(
    conditions: Array<readonly [Column<any, S>, unknown]>
  ): readonly [string, DBExists] {
    // Get target table name from conditions (they all have same tableName)
    const targetTableName = conditions[0]?.[0]?.tableName ?? '';
    // Column already has tableName, pass directly
    const subqueryConditions: SubqueryCondition[] = conditions.map(([col, value]) => ({
      column: col,
      value,
    }));
    return ['__exists__', new DBExists(targetTableName, subqueryConditions, true)] as const;
  }

  // ============================================
  // Static Primary Key Configuration
  // ============================================

  /** Cache for primary key columns detected from @column({ primaryKey: true }) */
  private static _pkeyColumnsCache: WeakMap<object, Column[]> = new WeakMap();

  /**
   * Get primary key columns with fallback to ['id']
   * Priority: 1. PKEY_COLUMNS getter  2. @column({ primaryKey: true })  3. 'id' default
   * @internal
   */
  protected static _getPkeyColumnsWithDefault(): Column[] {
    // 1. Explicit PKEY_COLUMNS takes precedence
    if (this.PKEY_COLUMNS) {
      return this.PKEY_COLUMNS;
    }

    // 2. Check cache for decorator-detected primary keys
    const cached = DBModel._pkeyColumnsCache.get(this);
    if (cached) {
      return cached;
    }

    // 3. Detect from @column({ primaryKey: true }) decorator
    const meta = getColumnMeta(this);
    if (meta) {
      const pkeyColumns: Column[] = [];
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const thisClass = this as any;
      for (const [propKey, colMeta] of meta) {
        if (colMeta.primaryKey) {
          // Get the static Column property from the class
          // Note: Column is a callable function, so typeof is 'function', not 'object'
          const col = thisClass[propKey];
          if (col && typeof col === 'function' && 'columnName' in col) {
            pkeyColumns.push(col as Column);
          }
        }
      }
      if (pkeyColumns.length > 0) {
        DBModel._pkeyColumnsCache.set(this, pkeyColumns);
        return pkeyColumns;
      }
    }

    // 4. Default: create a Column for 'id'
    const defaultPkey = [createColumn('id', this.TABLE_NAME, this.name)];
    DBModel._pkeyColumnsCache.set(this, defaultPkey);
    return defaultPkey;
  }

  /**
   * Get GROUP BY clause as string
   * Converts Column or Column[] to comma-separated column names
   * @internal
   */
  static getGroupByClause(): string | null {
    const group = this.DEFAULT_GROUP;
    if (!group) return null;
    if (typeof group === 'string') return group;
    if (Array.isArray(group)) {
      return columnsToNames(group).join(', ');
    }
    // Single Column
    return group.columnName;
  }

  // ============================================
  // Static Table Name Accessors
  // ============================================

  /**
   * Get table name for SELECT queries.
   * For query-based models, returns the CTE alias (TABLE_NAME).
   */
  static getTableName(): string {
    return this.TABLE_NAME;
  }

  /**
   * Get the CTE alias for query-based models.
   * @internal
   */
  static getCTEAlias(): string {
    return this.TABLE_NAME || 'derived';
  }

  /**
   * Get query parameters (set via withQuery()).
   * @internal
   */
  static getQueryParams(): unknown[] {
    return this._queryParams ? [...this._queryParams] : [];
  }

  /**
   * Check if this model is query-based (uses QUERY instead of TABLE_NAME)
   */
  static isQueryBased(): boolean {
    return this.QUERY !== null;
  }

  /**
   * Create a new model class bound to specific query parameters.
   * Used for parameterized query-based models.
   * 
   * @param queryConfig - Query configuration with sql and params
   * @returns A new model class with bound parameters
   * 
   * @example
   * ```typescript
   * class SalesReportModel extends DBModel {
   *   static QUERY = '...'; // Base query template
   *   
   *   static forPeriod(startDate: string, endDate: string) {
   *     return this.withQuery({
   *       sql: `SELECT ... WHERE date >= $1 AND date < $2 ...`,
   *       params: [startDate, endDate],
   *     });
   *   }
   * }
   * 
   * const Q1Report = SalesReport.forPeriod('2024-01-01', '2024-04-01');
   * const results = await Q1Report.find([...]);
   * ```
   */
  static withQuery<T extends typeof DBModel>(
    this: T,
    queryConfig: { sql: string; params?: unknown[] }
  ): T {
    // Create a new class that extends this one
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const ParentClass = this;
    const BoundModel = class extends (ParentClass as typeof DBModel) {
      static QUERY = queryConfig.sql;
      static _queryParams = queryConfig.params || null;
    };
    
    // Copy static properties (TABLE_NAME, etc.)
    Object.defineProperty(BoundModel, 'TABLE_NAME', {
      value: ParentClass.TABLE_NAME,
      writable: false,
    });
    
    // Copy column properties
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const columnMeta = (ParentClass as any)._columnMeta;
    if (columnMeta) {
      Object.defineProperty(BoundModel, '_columnMeta', {
        value: columnMeta,
        writable: false,
      });
      // Copy static Column properties
      for (const [propKey] of columnMeta) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const col = (ParentClass as any)[propKey];
        if (col) {
          Object.defineProperty(BoundModel, propKey, {
            value: col,
            writable: false,
            enumerable: true,
          });
        }
      }
    }
    
    return BoundModel as unknown as T;
  }

  /**
   * Get table name for UPDATE/DELETE queries.
   * Query-based models cannot be updated/deleted directly.
   */
  static getUpdateTableName(): string {
    if (this.QUERY) {
      throw new Error('Query-based models cannot be updated or deleted directly');
    }
    return this.UPDATE_TABLE_NAME ?? this.TABLE_NAME;
  }

  // ============================================
  // Instance Properties (set by query results)
  // ============================================

  /** Instance reference to the static class */
  protected _modelClass: typeof DBModel;

  /** Per-instance cache for loaded relations */
  protected _relationCache: Map<string, unknown> = new Map();

  /** Reference to the relation context (for batch loading) */
  private _relationContext: LazyRelationContext | null = null;

  constructor() {
    this._modelClass = this.constructor as typeof DBModel;
  }

  // ============================================
  // Relation Support
  // ============================================

  /**
   * Set the relation context for this record.
   * Called by LazyRelationContext when records are loaded.
   * @internal
   */
  _setRelationContext(context: LazyRelationContext): void {
    this._relationContext = context;
  }

  /**
   * Get the relation context (or create one for single record).
   * @internal
   */
  protected _getRelationContext(): LazyRelationContext {
    if (!this._relationContext) {
      const ModelClass = this.constructor as typeof DBModel;
      this._relationContext = new LazyRelationContext(ModelClass, [this]);
    }
    return this._relationContext!;
  }

  /**
   * Clear the relation cache for this instance.
   * Also clears the context cache to force reload from DB.
   */
  clearRelationCache(): void {
    this._relationCache.clear();
    if (this._relationContext) {
      this._relationContext.clearCache();
    }
  }

  /**
   * Internal method called by relation decorators (@hasMany, @belongsTo, @hasOne).
   * Loads relation data with batch loading support.
   *
   * @param relationType - The type of relation ('hasMany', 'belongsTo', 'hasOne')
   * @param targetModelName - The target model class name (for lookup)
   * @param config - Relation configuration from decorator
   * @returns Promise of related records
   * @internal
   */
  async _loadRelation(
    relationType: RelationType,
    targetModelName: string,
    config: {
      sourceKeys: string[];
      targetKeys: string[];
      order: string | null;
      conditions?: Record<string, unknown>;
      limit?: number;
      hardLimit?: number | null;
      relationName?: string;
    }
  ): Promise<DBModel | DBModel[] | null> {
    // Get target class from registry
    const TargetClass = DBModel._getModelByName(targetModelName);
    if (!TargetClass) {
      throw new Error(`Model '${targetModelName}' not found. Ensure it is decorated with @model and imported before use.`);
    }

    // Build RelationConfig
    const relationConfig: RelationConfig = {
      targetClass: TargetClass,
      conditions: config.conditions as ConditionObject | undefined,
      order: config.order,
      limit: config.limit,
      hardLimit: config.hardLimit,
      relationName: config.relationName,
    };

    // Set keys based on whether composite or single
    if (config.sourceKeys.length === 1) {
      relationConfig.sourceKey = config.sourceKeys[0];
      relationConfig.targetKey = config.targetKeys[0];
    } else {
      relationConfig.sourceKeys = config.sourceKeys;
      relationConfig.targetKeys = config.targetKeys;
    }

    const context = this._getRelationContext();
    const cacheKey = context.getCacheKey(relationType, relationConfig);

    // Return cached value if available
    if (this._relationCache.has(cacheKey)) {
      const cached = this._relationCache.get(cacheKey);
      if (relationType === 'hasMany') {
        return cached as DBModel[];
      }
      return cached as DBModel | null;
    }

    // Load from context (batch loading)
    const result = await context.getRelation<DBModel>(this, relationType, relationConfig);
    
    // Check hardLimit for hasMany relations
    if (relationType === 'hasMany' && Array.isArray(result)) {
      // Determine effective limit: per-relation hardLimit > global lazyLoadLimit
      // undefined = use global, null = no limit
      const effectiveLimit = config.hardLimit === undefined
        ? DBModel._limitConfig.lazyLoadLimit
        : config.hardLimit;
      
      if (effectiveLimit != null && result.length > effectiveLimit) {
        throw new LimitExceededError(
          effectiveLimit,
          result.length,
          'relation',
          this.constructor.name,
          config.relationName
        );
      }
    }
    
    this._relationCache.set(cacheKey, result);

    if (relationType === 'hasMany') {
      return result as DBModel[];
    }
    return result as DBModel | null;
  }

  // ============================================
  // Model Registry (for relation decorator support)
  // ============================================

  /** Registry of model classes by name */
  private static _modelRegistry: Map<string, typeof DBModel> = new Map();

  /**
   * Register a model class in the registry.
   * Called automatically by @model decorator.
   * @internal
   */
  static _registerModel(name: string, modelClass: typeof DBModel): void {
    DBModel._modelRegistry.set(name, modelClass);
  }

  /**
   * Get a model class by name from the registry.
   * @internal
   */
  static _getModelByName(name: string): typeof DBModel | undefined {
    return DBModel._modelRegistry.get(name);
  }

  // ============================================
  // Instance Methods - Type Casting
  // ============================================

  /**
   * Called after loading from DB to convert types
   * Override in derived class to implement type conversions
   *
   * @example
   * ```typescript
   * typeCastFromDB(): void {
   *   this.created_at = PostgresHelper.castToDatetime(this.created_at);
   *   this.is_active = PostgresHelper.castToBoolean(this.is_active);
   * }
   * ```
   */
  typeCastFromDB(): void {
    // Override in derived class
  }

  // ============================================
  // Instance Methods - Primary Key Operations
  // ============================================

  /**
   * Get primary key as object
   * @returns Object with primary key column names and values, or null if not set
   */
  getPkey(): Record<string, unknown> | null {
    const pkeyColumns = this._modelClass._getPkeyColumnsWithDefault();
    const result: Record<string, unknown> = {};
    let hasValue = false;

    for (const col of pkeyColumns) {
      // Use propertyName to get value from instance, columnName as result key
      const value = (this as Record<string, unknown>)[col.propertyName];
      if (value !== undefined && value !== null) {
        hasValue = true;
      }
      result[col.columnName] = value;
    }

    return hasValue ? result : null;
  }

  /**
   * Set primary key value
   * @param key - Single value for single-column PK, or object for composite PK
   */
  setPkey(key: unknown): void {
    const columnNames = columnsToNames(this._modelClass._getPkeyColumnsWithDefault());

    if (columnNames.length === 1) {
      (this as Record<string, unknown>)[columnNames[0]] = key;
    } else if (typeof key === 'object' && key !== null) {
      const keyObj = key as Record<string, unknown>;
      for (const col of columnNames) {
        if (col in keyObj) {
          (this as Record<string, unknown>)[col] = keyObj[col];
        }
      }
    }
  }

  /**
   * Get primary key as string (for logging, caching, etc.)
   */
  getPkeyString(): string {
    const pkey = this.getPkey();
    if (!pkey) {
      return '';
    }
    return Object.values(pkey).join('-');
  }

  /**
   * Get single-column ID value
   * @returns ID value or undefined
   */
  getSingleColId(): unknown {
    const columnNames = columnsToNames(this._modelClass._getPkeyColumnsWithDefault());
    if (columnNames.length !== 1) {
      throw new Error('getSingleColId() can only be used with single-column primary keys');
    }
    return (this as Record<string, unknown>)[columnNames[0]];
  }

  // ============================================
  // Instance Methods - Cloning and Copying
  // ============================================

  /**
   * Create a shallow copy of the model instance
   */
  clone<T extends DBModel>(this: T): T {
    const copy = Object.create(Object.getPrototypeOf(this));
    return Object.assign(copy, this);
  }

  /**
   * Copy properties from another object
   */
  assign(source: Partial<this>): this {
    return Object.assign(this, source);
  }

  // ============================================
  // Instance Methods - Serialization
  // ============================================

  /**
   * Convert to plain object
   */
  toObject(): Record<string, unknown> {
    const result: Record<string, unknown> = {};
    for (const key of Object.keys(this)) {
      if (!key.startsWith('_')) {
        result[key] = (this as Record<string, unknown>)[key];
      }
    }
    return result;
  }

  /**
   * Convert to JSON-serializable object
   */
  toJSON(): Record<string, unknown> {
    return this.toObject();
  }

  // ============================================
  // Static Utility Methods
  // ============================================

  /**
   * Create an instance from a plain object
   */
  static fromObject<T extends DBModel>(
    this: new () => T,
    obj: Record<string, unknown>
  ): T {
    const instance = new this();
    Object.assign(instance, obj);
    instance.typeCastFromDB();
    return instance;
  }

  /**
   * Create multiple instances from an array of plain objects
   */
  static fromObjects<T extends DBModel>(
    this: new () => T,
    objs: Record<string, unknown>[]
  ): T[] {
    return objs.map((obj) => (this as any).fromObject(obj));
  }

  /**
   * Get column values from an array of model instances
   */
  static columnList<T extends DBModel>(
    records: T[],
    columnName: string
  ): unknown[] {
    return records.map((r) => (r as Record<string, unknown>)[columnName]);
  }

  /**
   * Create a hash map by property value
   */
  static hashByProperty<T extends DBModel>(
    records: T[],
    propertyKey: string
  ): Record<string, T> {
    const result: Record<string, T> = {};
    for (const record of records) {
      const key = String((record as Record<string, unknown>)[propertyKey]);
      result[key] = record;
    }
    return result;
  }

  /**
   * Group records by property value
   */
  static groupByProperty<T extends DBModel>(
    records: T[],
    propertyKey: string
  ): Record<string, T[]> {
    const result: Record<string, T[]> = {};
    for (const record of records) {
      const key = String((record as Record<string, unknown>)[propertyKey]);
      if (!result[key]) {
        result[key] = [];
      }
      result[key].push(record);
    }
    return result;
  }

  /**
   * Get ID list from records
   */
  static idList<T extends DBModel>(records: T[], column?: string): unknown[] {
    const col = column || columnsToNames(this._getPkeyColumnsWithDefault())[0];
    return this.columnList(records, col);
  }

  /**
   * Generate LIKE pattern string
   */
  static makeLikeString(
    src: string,
    front: boolean = true,
    back: boolean = true
  ): string {
    const escaped = src.replace(/[%_\\]/g, '\\$&');
    const prefix = front ? '%' : '';
    const suffix = back ? '%' : '';
    return `${prefix}${escaped}${suffix}`;
  }

  // ============================================
  // Condition Helpers
  // ============================================

  /**
   * Create a type-safe OR condition for this model.
   * All columns in the conditions must belong to this model.
   *
   * @param condGroups - Arrays of conditions to OR together
   * @returns OR condition that can be used in find(), findOne(), etc.
   *
   * @example
   * ```typescript
   * // (role = 'admin') OR (role = 'moderator')
   * const admins = await User.find([
   *   [User.deleted, false],
   *   User.or(
   *     [[User.role, 'admin']],
   *     [[User.role, 'moderator']],
   *   ),
   * ]);
   *
   * // This will cause a compile error:
   * User.or([[OtherModel.id, 1]]);  // Error: OtherModel.id is not a User column
   * ```
   */
  static or<T extends typeof DBModel>(
    this: T,
    ...condGroups: readonly CondsOf<T>[]
  ): OrCondOf<T> {
    return createOrCond(condGroups);
  }

  // ============================================
  // High-Level Static Query API
  // ============================================

  /**
   * Find all records using type-safe condition tuples.
   * All columns in conditions must belong to this model.
   *
   * @param conditions - Array of condition tuples
   * @param options - Query options (order, limit, offset, etc.)
   * @returns Array of model instances
   *
   * @example
   * ```typescript
   * const users = await User.find([
   *   [User.is_active, true],
   *   [`${User.age} >= ?`, 18],
   * ]);
   *
   * // With OR conditions (use Model.or() for type safety)
   * const admins = await User.find([
   *   [User.deleted, false],
   *   User.or(
   *     [[User.role, 'admin']],
   *     [[User.role, 'moderator']],
   *   ),
   * ]);
   * ```
   */
  static async find<T extends typeof DBModel>(
    this: T,
    conditions: CondsOf<T>,
    options?: SelectOptions
  ): Promise<InstanceType<T>[]> {
    const core = async (conds: Conds, opts?: SelectOptions): Promise<InstanceType<T>[]> => {
      const conditionRecord = condsToRecord(conds) as ConditionObject;
      
      // Apply hardLimit + 1 to detect overflow without fetching all records
      const hardLimit = DBModel._limitConfig.hardLimit;
      const shouldCheckLimit = hardLimit != null && !opts?.limit;
      const effectiveOpts = shouldCheckLimit
        ? { ...opts, limit: hardLimit + 1 }
        : opts;
      
      const results = await this._select(conditionRecord, effectiveOpts);
      
      // Check if hardLimit was exceeded
      if (shouldCheckLimit && results.length > hardLimit) {
        throw new LimitExceededError(
          hardLimit,
          results.length,  // At least this many (actually more may exist)
          'find',
          this.name
        );
      }
      
      return results;
    };
    return this._applyMiddleware('find', core, [conditions, options]);
  }

  /**
   * Find first record using type-safe condition tuples.
   *
   * @param conditions - Array of condition tuples
   * @param options - Query options
   * @returns Model instance or null
   *
   * @example
   * ```typescript
   * const user = await User.findOne([
   *   [User.email, 'test@example.com'],
   * ]);
   * ```
   */
  static async findOne<T extends typeof DBModel>(
    this: T,
    conditions: CondsOf<T>,
    options?: SelectOptions
  ): Promise<InstanceType<T> | null> {
    const core = async (conds: Conds, opts?: SelectOptions): Promise<InstanceType<T> | null> => {
      const conditionRecord = condsToRecord(conds) as ConditionObject;
      const results = await this._select(conditionRecord, { ...opts, limit: 1 });
      return results.length > 0 ? results[0] : null;
    };
    return this._applyMiddleware('findOne', core, [conditions, options]);
  }

  /**
   * Find record by primary key
   * @param id - Primary key value (or object for composite keys)
   * @param options - Query options
   * @returns Model instance or null
   *
   * @example
   * ```typescript
   * const user = await User.findById(123);
   * const entry = await Entry.findById({ user_id: 1, date: '2024-01-01' });
   * ```
   */
  static async findById<T extends typeof DBModel>(
    this: T,
    id: unknown,
    options?: SelectOptions
  ): Promise<InstanceType<T> | null> {
    const core = async (idVal: unknown, opts?: SelectOptions): Promise<InstanceType<T> | null> => {
      const pkeyColumnNames = columnsToNames(this._getPkeyColumnsWithDefault());
      let idCondition: ConditionObject;
      if (pkeyColumnNames.length === 1) {
        idCondition = { [pkeyColumnNames[0]]: idVal } as ConditionObject;
      } else if (typeof idVal === 'object' && idVal !== null) {
        idCondition = idVal as ConditionObject;
      } else {
        throw new Error('Invalid primary key value for composite key');
      }
      const results = await this._select(idCondition, { ...opts, limit: 1 });
      return results.length > 0 ? results[0] : null;
    };
    return this._applyMiddleware('findById', core, [id, options]);
  }

  /**
   * Count records using type-safe condition tuples.
   *
   * @param conditions - Array of condition tuples
   * @returns Count
   *
   * @example
   * ```typescript
   * const count = await User.count([[User.is_active, true]]);
   * ```
   */
  static async count<T extends typeof DBModel>(
    this: T,
    conditions: CondsOf<T>
  ): Promise<number> {
    const core = async (conds: Conds): Promise<number> => {
      const conditionRecord = condsToRecord(conds) as ConditionObject;
      return this._count(conditionRecord);
    };
    return this._applyMiddleware('count', core, [conditions]);
  }

  /**
   * Create a new record using type-safe column-value tuples.
   * Value types are validated at compile time.
   *
   * @param pairs - Array of [Column, value] tuples
   * @param options - Insert options
   * @returns Created model instance
   *
   * @example
   * ```typescript
   * const user = await User.create([
   *   [User.name, 'John'],
   *   [User.email, 'john@test.com'],
   *   [User.is_active, true],
   * ]);
   * ```
   */
  static async create<
    T extends typeof DBModel,
    P extends readonly (readonly [Column<any, any>, any])[]
  >(
    this: T,
    pairs: P & CVs<P>,
    options?: InsertOptions<InstanceType<T>>
  ): Promise<InstanceType<T>> {
    const core = async (
      p: readonly (readonly [Column<any, any>, any])[],
      opts?: InsertOptions<InstanceType<T>>
    ): Promise<InstanceType<T>> => {
      const values = pairsToRecord(p);
      const insertOpts = { returning: '*', ...opts };
      const [instance] = await this._insert(values, insertOpts);
      return instance;
    };
    return this._applyMiddleware('create', core, [pairs, options]);
  }

  /**
   * Create multiple records using type-safe column-value tuples.
   *
   * @param pairsArray - Array of tuple arrays
   * @param options - Insert options
   * @returns Created model instances
   *
   * @example
   * ```typescript
   * const users = await User.createMany([
   *   [[User.name, 'John'], [User.email, 'john@test.com']],
   *   [[User.name, 'Jane'], [User.email, 'jane@test.com']],
   * ]);
   * ```
   */
  static async createMany<T extends typeof DBModel>(
    this: T,
    pairsArray: readonly (readonly (readonly [Column<any, any>, any])[])[], 
    options?: InsertOptions<InstanceType<T>>
  ): Promise<InstanceType<T>[]> {
    const core = async (
      arr: readonly (readonly (readonly [Column<any, any>, any])[])[],
      opts?: InsertOptions<InstanceType<T>>
    ): Promise<InstanceType<T>[]> => {
      const valuesArray = arr.map(pairs => pairsToRecord(pairs));
      const insertOpts = { returning: '*', ...opts };
      return this._insert(valuesArray, insertOpts);
    };
    return this._applyMiddleware('createMany', core, [pairsArray, options]);
  }

  /**
   * Update records using type-safe column-value tuples.
   * Value types are validated at compile time.
   *
   * @param conditions - Array of condition tuples for WHERE clause
   * @param values - Array of [Column, value] tuples for SET clause
   * @param options - Update options
   * @returns Updated model instances
   *
   * @example
   * ```typescript
   * await User.update(
   *   [[User.id, 1]],                // conditions
   *   [                              // values
   *     [User.name, 'Jane'],
   *     [User.email, 'jane@test.com'],
   *   ],
   * );
   * ```
   */
  static async update<
    T extends typeof DBModel,
    V extends readonly (readonly [Column<any, any>, any])[]
  >(
    this: T,
    conditions: CondsOf<T>,
    values: V & CVs<V>,
    options?: UpdateOptions
  ): Promise<InstanceType<T>[]> {
    const core = async (
      conds: Conds,
      vals: readonly (readonly [Column<any, any>, any])[],
      opts?: UpdateOptions
    ): Promise<InstanceType<T>[]> => {
      const conditionRecord = condsToRecord(conds) as ConditionObject;
      const valueRecord = pairsToRecord(vals);
      const updateOpts = { returning: '*', ...opts };
      return this._update(conditionRecord, valueRecord, updateOpts);
    };
    return this._applyMiddleware('update', core, [conditions, values, options]);
  }

  /**
   * Delete records matching conditions
   * @param conditions - Filter conditions
   * @param options - Delete options
   * @returns Deleted model instances
   *
   * @example
   * ```typescript
   * await User.delete([[User.is_active, false]]);
   * ```
   */
  static async delete<T extends typeof DBModel>(
    this: T,
    conditions: CondsOf<T>,
    options?: DeleteOptions
  ): Promise<InstanceType<T>[]> {
    const core = async (conds: Conds, opts?: DeleteOptions): Promise<InstanceType<T>[]> => {
      const conditionRecord = condsToRecord(conds) as ConditionObject;
      const deleteOpts = { returning: '*', ...opts };
      return this._delete(conditionRecord, deleteOpts);
    };
    return this._applyMiddleware('delete', core, [conditions, options]);
  }

  // ============================================
  // Low-Level SQL Execution
  // ============================================

  /**
   * Execute raw SQL query.
   * 
   * @param sql - SQL query
   * @param params - Query parameters
   * @returns QueryResult with rows, rowCount
   * 
   * @example
   * ```typescript
   * const result = await DBModel.execute(
   *   'SELECT * FROM users WHERE id = $1',
   *   [1]
   * );
   * console.log(result.rows);
   * console.log(result.rowCount);
   * ```
   */
  static async execute(
    sql: string,
    params?: unknown[]
  ): Promise<ExecuteResult> {
    const core = async (s: string, p?: unknown[]): Promise<ExecuteResult> => {
      const handler = this.getHandler();
      const result = await handler.execute(s, p || []);
      return {
        rows: result.rows as Record<string, unknown>[],
        rowCount: result.rowCount,
      };
    };
    return this._applyExecuteMiddleware(core, sql, params);
  }

  /**
   * Execute raw SQL and return model instances.
   * The SQL should return columns matching the model's properties.
   *
   * @param sql - SQL query
   * @param params - Query parameters
   * @returns Model instances
   *
   * @example
   * ```typescript
   * const users = await User.query(
   *   'SELECT * FROM users WHERE is_active = $1',
   *   [true]
   * );
   * 
   * // Complex join query
   * const posts = await Post.query(`
   *   SELECT p.* FROM posts p
   *   JOIN users u ON p.user_id = u.id
   *   WHERE u.email = $1
   * `, ['admin@example.com']);
   * ```
   */
  static async query<T extends typeof DBModel>(
    this: T,
    sql: string,
    params?: unknown[]
  ): Promise<InstanceType<T>[]> {
    const core = async (s: string, p?: unknown[]): Promise<InstanceType<T>[]> => {
      // Calls execute() to go through execute middleware
      const result = await this.execute(s, p);
      
      // Create model instances from raw rows
      const instances = result.rows.map((row) => this._createInstance<T>(row));
      
      // Auto-setup relation context for batch loading when multiple records
      if (instances.length > 1) {
        // Context constructor sets itself on each record automatically
        new LazyRelationContext(this, instances as DBModel[]);
      }
      
      return instances;
    };
    return this._applyMiddleware('query', core, [sql, params]);
  }

  // ============================================
  // Transaction Support
  // ============================================

  /**
   * Check if currently in a transaction
   */
  static inTransaction(): boolean {
    return transactionContext.getStore() !== undefined;
  }

  /**
   * Execute a function within a transaction
   * All model operations inside the callback will use the same database connection.
   * Supports automatic retry on deadlock/serialization errors.
   *
   * @param func - Function to execute within transaction
   * @param options - Transaction options (retry settings)
   * @returns Result of the function
   *
   * @example
   * ```typescript
   * // Basic transaction
   * await DBModel.transaction(async () => {
   *   const user = await User.findFirst({ [User.def.id]: 1 });
   *   await Account.updateAll(
   *     { [Account.def.user_id]: user.id },
   *     { [Account.def.balance]: user.balance - 100 }
   *   );
   *   await Transaction.create({ user_id: user.id, amount: -100 });
   * });
   *
   * // Transaction with return value
   * const result = await DBModel.transaction(async () => {
   *   const user = await User.create({ name: 'Alice' });
   *   return user;
   * });
   *
   * // Transaction with options
   * await DBModel.transaction(
   *   async () => { ... },
   *   { retryLimit: 5, retryDuration: 100 }
   * );
   * ```
   */
  static async transaction<R>(
    func: () => Promise<R>,
    options: TransactionOptions = {}
  ): Promise<R> {
    // Check if already in a transaction (nested transaction)
    if (transactionContext.getStore()) {
      // Already in a transaction, just execute the function
      return func();
    }

    // Get handler
    const handler = this.getHandler();

    const retryOnError = options.retryOnError ?? true;
    const retryLimit = options.retryLimit ?? 3;
    const retryDuration = options.retryDuration ?? 200;
    const rollbackOnly = options.rollbackOnly ?? false;

    let attempt = 0;

    while (true) {
      attempt++;
      const connection = await handler.getConnection();

      try {
        await connection.query('BEGIN');

        const context: TransactionContext = { connection };
        const result = await transactionContext.run(context, func);

        if (rollbackOnly) {
          // Rollback instead of commit (for preview/dry-run scenarios)
          await connection.query('ROLLBACK');
        } else {
          await connection.query('COMMIT');
        }
        return result;
      } catch (error) {
        await connection.query('ROLLBACK');

        if (
          retryOnError &&
          attempt < retryLimit &&
          this.isRetryableError(error as Error)
        ) {
          await this.sleep(retryDuration * Math.pow(2, attempt - 1));
          continue;
        }

        throw error;
      } finally {
        connection.release();
      }
    }
  }

  /**
   * Check if error is retryable (deadlock, serialization failure, etc.)
   */
  private static isRetryableError(error: Error): boolean {
    const message = error.message || '';
    return (
      message.includes('The transaction might succeed if retried') ||
      message.includes('try restarting transaction') ||
      message.includes('could not serialize access due to concurrent update') ||
      message.includes('Deadlock found') ||
      message.includes('Lock wait timeout exceeded')
    );
  }

  /**
   * Sleep for specified milliseconds
   */
  private static sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Get current transaction connection
   * Use this to execute raw SQL queries within a transaction
   *
   * @returns Current DBConnection if in a transaction, null otherwise
   *
   * @example
   * ```typescript
   * await DBModel.transaction(async () => {
   *   const conn = DBModel.getCurrentConnection();
   *   if (conn) {
   *     await conn.query('SELECT * FROM some_table WHERE ...');
   *   }
   * });
   * ```
   */
  static getCurrentConnection(): DBConnection | null {
    const context = transactionContext.getStore();
    return context?.connection ?? null;
  }

  /**
   * @deprecated Use getCurrentConnection() instead
   */
  static getCurrentClient(): DBConnection | null {
    return this.getCurrentConnection();
  }

  // ============================================
  // Instance Methods - Persistence
  // ============================================

  /**
   * Save this instance (INSERT if new, UPDATE if exists)
   * @param properties - Properties to save (uses all properties if not specified)
   * @returns true if successful
   *
   * @example
   * ```typescript
   * const user = new User();
   * user.name = 'John';
   * user.email = 'john@example.com';
   * await user.save();
   * ```
   */
  async save(properties?: Record<string, unknown>): Promise<boolean> {
    const ModelClass = this._modelClass;
    const pkey = this.getPkey();

    // Determine which properties to save
    const propsToSave = properties ?? this.toObject();

    if (pkey && Object.values(pkey).every((v) => v !== undefined && v !== null)) {
      // UPDATE - has primary key
      // Convert pkey to condition tuples
      const pkeyConditions = Object.entries(pkey).map(([k, v]) => [k, v] as [string, unknown]);
      // Convert props to value tuples (using column names directly)
      const valueTuples = Object.entries(propsToSave).map(([k, v]) => [createColumn(k, ModelClass.TABLE_NAME, ModelClass.name), v] as const);
      
      const updated = await ModelClass.update(
        pkeyConditions as Conds,
        valueTuples as CVs<typeof valueTuples>,
        { returning: '*' }
      );
      if (updated.length > 0) {
        Object.assign(this, updated[0]);
        return true;
      }
      return false;
    } else {
      // INSERT - no primary key
      // Convert props to value tuples
      const valueTuples = Object.entries(propsToSave).map(([k, v]) => [createColumn(k, ModelClass.TABLE_NAME, ModelClass.name), v] as const);
      
      const inserted = await ModelClass.create(
        valueTuples as CVs<typeof valueTuples>,
        { returning: '*' }
      );
      if (inserted) {
        Object.assign(this, inserted);
        return true;
      }
      return false;
    }
  }

  /**
   * Delete this instance from the database
   * @returns true if successful
   *
   * @example
   * ```typescript
   * await user.destroy();
   * ```
   */
  async destroy(): Promise<boolean> {
    const ModelClass = this._modelClass;
    const pkey = this.getPkey();

    if (!pkey) {
      throw new Error('Cannot destroy record without primary key');
    }

    // Convert pkey to condition tuples
    const pkeyConditions = Object.entries(pkey).map(([k, v]) => [k, v] as [string, unknown]);
    const deleted = await ModelClass.delete(pkeyConditions as Conds, { returning: '*' });
    return deleted.length > 0;
  }

  /**
   * Reload this instance from the database
   * @param forUpdate - If true, lock the row for update
   * @returns Reloaded instance or null if not found
   *
   * @example
   * ```typescript
   * await user.reload();
   * ```
   */
  async reload(forUpdate: boolean = false): Promise<this | null> {
    const ModelClass = this._modelClass;
    const pkey = this.getPkey();

    if (!pkey) {
      throw new Error('Cannot reload record without primary key');
    }

    const options: SelectOptions = { limit: 1 };
    if (forUpdate) {
      options.forUpdate = true;
    }

    // Convert pkey to condition tuples
    const pkeyConditions = Object.entries(pkey).map(([k, v]) => [k, v] as [string, unknown]);
    const results = await ModelClass.find(pkeyConditions as Conds, options);
    if (results.length > 0) {
      Object.assign(this, results[0]);
      return this;
    }
    return null;
  }
}

