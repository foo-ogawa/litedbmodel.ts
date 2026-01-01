/**
 * litedbmodel - Lazy Loading Relations
 *
 * Provides efficient batch loading of relations to avoid N+1 query problems.
 * Records in the same context share relation cache for optimal performance.
 *
 * Batch loading strategies by driver:
 * - PostgreSQL: Uses `= ANY($1::type[])` for single keys, `unnest + JOIN` for composite keys
 * - MySQL/SQLite: Uses traditional `IN (?, ?, ...)` clause
 *
 * Usage with decorators:
 * ```typescript
 * @model('posts')
 * class Post extends DBModel {
 *   @column() id?: number;
 *   @column() author_id?: number;
 *
 *   @belongsTo(() => [Post.author_id, User.id])
 *   declare author: Promise<User | null>;
 *
 *   @hasMany(() => [Post.id, Comment.post_id], {
 *     order: () => Comment.created_at.asc(),
 *   })
 *   declare comments: Promise<Comment[]>;
 * }
 *
 * // Access with await
 * const post = await Post.findOne([[Post.id, 1]]);
 * const author = await post.author;     // Loads from DB and caches
 * const author2 = await post.author;    // Returns cached value
 * const comments = await post.comments; // Loads from DB and caches
 * ```
 */

import { DBModel } from './DBModel';
import { type ConditionObject } from './DBConditions';
import { type Conds, createColumn, orderToString, condsToRecord } from './Column';
import type { SelectOptions } from './types';

// ============================================
// Type Inference Helpers
// ============================================

/**
 * Infer PostgreSQL array type from sample values
 */
function inferPgArrayType(values: unknown[]): string {
  if (values.length === 0) return 'text[]';
  
  const sample = values[0];
  if (typeof sample === 'number') {
    // Check if all are integers
    if (values.every(v => Number.isInteger(v))) {
      return 'int[]';
    }
    return 'numeric[]';
  }
  if (typeof sample === 'bigint') return 'bigint[]';
  if (typeof sample === 'boolean') return 'boolean[]';
  if (sample instanceof Date) return 'timestamp[]';
  return 'text[]';
}

// ============================================
// Types
// ============================================

export type RelationType = 'belongsTo' | 'hasMany' | 'hasOne';

export interface RelationConfig {
  targetClass: typeof DBModel;
  conditions?: ConditionObject;
  order?: string | null;
  /** Single target key (for simple relation) */
  targetKey?: string;
  /** Single source key (for simple relation) */
  sourceKey?: string;
  /** Multiple target keys (for composite key relation) */
  targetKeys?: string[];
  /** Multiple source keys (for composite key relation) */
  sourceKeys?: string[];
  /** SQL LIMIT for hasMany (per parent key) */
  limit?: number;
  /** Hard limit - throws if exceeded (overrides global lazyLoadLimit) */
  hardLimit?: number | null;
  /** Relation name for error messages */
  relationName?: string;
}

// ============================================
// Helper Functions
// ============================================

// ============================================
// LazyRelationContext
// ============================================

/**
 * Context for managing lazy-loaded relations across a set of records.
 * All records in the same context share the relation cache for batch loading.
 */
export class LazyRelationContext {
  private sourceClass: typeof DBModel;
  private records: DBModel[];
  private batchCache: Map<string, Map<string, unknown>> = new Map();

  constructor(
    sourceClass: typeof DBModel,
    records: DBModel[]
  ) {
    this.sourceClass = sourceClass;
    this.records = records;

    // Attach context to all records
    for (const record of records) {
      record._setRelationContext(this);
    }
  }

  /**
   * Get or load a relation for a specific record (batch mode)
   */
  async getRelation<T extends DBModel>(
    record: DBModel,
    relationType: RelationType,
    config: RelationConfig
  ): Promise<T | T[] | null> {
    const cacheKey = this.getCacheKey(relationType, config);

    // Check if relation is already batch-loaded
    if (!this.batchCache.has(cacheKey)) {
      // Load relation for all records in the batch
      await this.batchLoadRelation(relationType, config);
    }

    const relationMap = this.batchCache.get(cacheKey)!;
    const isCompositeKey = (config.targetKeys && config.targetKeys.length > 0) || 
                           (config.sourceKeys && config.sourceKeys.length > 0);
    
    let mapKey: string;
    if (isCompositeKey) {
      // Composite key: join with NUL separator (faster than JSON.stringify)
      const sourceKeys = config.sourceKeys || [config.sourceKey || this.inferSourceKey(relationType, config)];
      mapKey = sourceKeys
        .map(key => (record as unknown as Record<string, unknown>)[key])
        .join('\x00');
    } else {
      const sourceKeyValue = this.getSourceKeyValue(record, relationType, config);
      mapKey = String(sourceKeyValue);
    }
    
    const result = relationMap.get(mapKey);

    if (relationType === 'hasMany') {
      return (result as T[]) || [];
    }
    return (result as T) || null;
  }

  /**
   * Get cache key for a relation configuration
   */
  getCacheKey(relationType: RelationType, config: RelationConfig): string {
    const parts = [
      relationType,
      config.targetClass.name,
      JSON.stringify(config.conditions || {}),
      config.order || '',
      config.targetKey || '',
      config.sourceKey || '',
      JSON.stringify(config.targetKeys || []),
      JSON.stringify(config.sourceKeys || []),
      config.limit != null ? String(config.limit) : '',
      config.relationName || '',
    ];
    return parts.join('|');
  }

  /**
   * Clear all cached relations
   */
  clearCache(): void {
    this.batchCache.clear();
  }

  /**
   * Get all records in this context
   */
  getRecords(): DBModel[] {
    return this.records;
  }

  /**
   * Batch load relation for all records.
   * Uses optimized loading strategy based on database driver:
   * - PostgreSQL: `= ANY($1::type[])` for single keys, `unnest + JOIN` for composite keys
   *   - With limit: Uses LATERAL JOIN for efficient per-key limiting
   * - MySQL/SQLite: Traditional `IN (?, ?, ...)` clause
   *   - With limit: Uses ROW_NUMBER() OVER (PARTITION BY ...) window function
   */
  private async batchLoadRelation(
    relationType: RelationType,
    config: RelationConfig
  ): Promise<void> {
    const TargetClass = config.targetClass;
    const isCompositeKey = (config.targetKeys && config.targetKeys.length > 1) || 
                           (config.sourceKeys && config.sourceKeys.length > 1);

    // Get all source key values from records
    const sourceKeyValues: unknown[] = [];
    const compositeKeyValues: Record<string, unknown>[] = [];
    
    for (const record of this.records) {
      if (isCompositeKey) {
        const value = this.getCompositeSourceKeyValue(record, relationType, config);
        if (value && Object.values(value).every(v => v !== undefined && v !== null)) {
          compositeKeyValues.push(value);
        }
      } else {
        const value = this.getSourceKeyValue(record, relationType, config);
        if (value !== undefined && value !== null) {
          sourceKeyValues.push(value);
        }
      }
    }

    // If no values, create empty cache
    if (sourceKeyValues.length === 0 && compositeKeyValues.length === 0) {
      const cacheKey = this.getCacheKey(relationType, config);
      this.batchCache.set(cacheKey, new Map());
      return;
    }

    const driverType = DBModel.getDriverType();
    let results: DBModel[];
    let targetKeys: string[];

    // Check if we need limited loading (hasMany with limit option)
    const needsLimitedLoading = relationType === 'hasMany' && config.limit != null;

    if (isCompositeKey && compositeKeyValues.length > 0) {
      // Composite key batch loading
      targetKeys = config.targetKeys || [this.inferTargetKey(relationType)];
      const sourceKeys = config.sourceKeys || [config.sourceKey || 'id'];
      
      // Build unique tuples
      const uniqueTuples = this.getUniqueCompositeKeyTuples(compositeKeyValues, sourceKeys);
      
      if (uniqueTuples.length === 0) {
        const cacheKey = this.getCacheKey(relationType, config);
        this.batchCache.set(cacheKey, new Map());
        return;
      }

      if (needsLimitedLoading) {
        // Limited loading for composite keys
        if (driverType === 'postgres') {
          results = await this.batchLoadWithLateralComposite(TargetClass, targetKeys, uniqueTuples, config);
        } else {
          results = await this.batchLoadWithRowNumberComposite(TargetClass, targetKeys, uniqueTuples, config);
        }
      } else if (driverType === 'postgres') {
        // PostgreSQL: Use unnest + JOIN for composite keys
        results = await this.batchLoadWithUnnestJoin(TargetClass, targetKeys, uniqueTuples, config);
      } else {
        // MySQL/SQLite: Use traditional (col1, col2) IN ((v1, v2), ...) clause
        results = await this.batchLoadWithCompositeIn(TargetClass, targetKeys, uniqueTuples, config);
      }
    } else {
      // Single key batch loading
      const targetKey = config.targetKey || this.inferTargetKey(relationType);
      targetKeys = [targetKey];
      
      // Remove duplicates
      const uniqueValues = [...new Set(sourceKeyValues.map(v => JSON.stringify(v)))].map(v => JSON.parse(v));
      
      if (needsLimitedLoading) {
        // Limited loading for single key
        if (driverType === 'postgres') {
          results = await this.batchLoadWithLateral(TargetClass, targetKey, uniqueValues, config);
        } else {
          results = await this.batchLoadWithRowNumber(TargetClass, targetKey, uniqueValues, config);
        }
      } else if (driverType === 'postgres') {
        // PostgreSQL: Use = ANY($1::type[]) for single key
        results = await this.batchLoadWithAnyArray(TargetClass, targetKey, uniqueValues, config);
      } else {
        // MySQL/SQLite: Use traditional IN (?, ?, ...) clause
        results = await this.batchLoadWithIn(TargetClass, targetKey, uniqueValues, config);
      }
    }

    // Build relation map
    const relationMap = new Map<string, unknown>();

    if (relationType === 'hasMany') {
      // Group results by target key
      for (const result of results) {
        const keyValue = this.getTargetKeyValue(result, targetKeys, config);
        if (!relationMap.has(keyValue)) {
          relationMap.set(keyValue, []);
        }
        (relationMap.get(keyValue) as unknown[]).push(result);
      }
    } else {
      // Map results by target key (belongsTo, hasOne)
      for (const result of results) {
        const keyValue = this.getTargetKeyValue(result, targetKeys, config);
        relationMap.set(keyValue, result);
      }
    }

    const cacheKey = this.getCacheKey(relationType, config);
    this.batchCache.set(cacheKey, relationMap);
  }

  /**
   * PostgreSQL optimized: Single key batch load using = ANY($1::type[])
   */
  private async batchLoadWithAnyArray(
    TargetClass: typeof DBModel,
    targetKey: string,
    values: unknown[],
    config: RelationConfig
  ): Promise<DBModel[]> {
    const tableName = TargetClass.TABLE_NAME;
    const pgType = inferPgArrayType(values);
    
    // Build conditions
    const conditions: ConditionObject = {
      __raw__: [`${tableName}.${targetKey} = ANY(?::${pgType})`, [values]],
      ...config.conditions,
    };
    
    const options: SelectOptions = {};
    if (config.order) options.order = config.order;
    
    return await TargetClass._selectForRelation(conditions, options, {
      hardLimit: config.hardLimit,
      propertyKey: config.relationName,
      sourceModelName: this.sourceClass.name,
    });
  }

  /**
   * PostgreSQL optimized: Composite key batch load using unnest + JOIN
   */
  private async batchLoadWithUnnestJoin(
    TargetClass: typeof DBModel,
    targetKeys: string[],
    tuples: unknown[][],
    config: RelationConfig
  ): Promise<DBModel[]> {
    const tableName = TargetClass.TABLE_NAME;
    
    // Transpose tuples to column arrays: [[1,a], [2,b]] -> [[1,2], [a,b]]
    const columnArrays: unknown[][] = targetKeys.map((_, colIndex) => 
      tuples.map(tuple => tuple[colIndex])
    );
    
    // Build unnest parameters with type inference
    const unnestParams = columnArrays.map((arr) => {
      const pgType = inferPgArrayType(arr);
      return `?::${pgType}`;
    }).join(', ');
    
    const unnestAlias = `_unnest_${tableName}`;
    const columnAliases = targetKeys.map(k => `_unnest_${tableName}_${k}`).join(', ');
    
    // Build JOIN clause with unnest
    const joinConditions = targetKeys
      .map(key => `${tableName}.${key} = ${unnestAlias}._unnest_${tableName}_${key}`)
      .join(' AND ');
    
    const joinClause = `JOIN unnest(${unnestParams}) AS ${unnestAlias}(${columnAliases}) ON ${joinConditions}`;
    
    const options: SelectOptions = {
      join: joinClause,
      joinParams: columnArrays,
    };
    if (config.order) options.order = config.order;
    
    return await TargetClass._selectForRelation(config.conditions || {}, options, {
      hardLimit: config.hardLimit,
      propertyKey: config.relationName,
      sourceModelName: this.sourceClass.name,
    });
  }

  /**
   * MySQL/SQLite: Single key batch load using IN (?, ?, ...)
   */
  private async batchLoadWithIn(
    TargetClass: typeof DBModel,
    targetKey: string,
    values: unknown[],
    config: RelationConfig
  ): Promise<DBModel[]> {
    const conditions: ConditionObject = {
      ...config.conditions,
      [targetKey]: values,
    };

    const options: SelectOptions = {};
    if (config.order) options.order = config.order;
    
    return await TargetClass._selectForRelation(conditions, options, {
      hardLimit: config.hardLimit,
      propertyKey: config.relationName,
      sourceModelName: this.sourceClass.name,
    });
  }

  /**
   * MySQL/SQLite: Composite key batch load using (col1, col2) IN ((v1, v2), ...)
   */
  private async batchLoadWithCompositeIn(
    TargetClass: typeof DBModel,
    targetKeys: string[],
    tuples: unknown[][],
    config: RelationConfig
  ): Promise<DBModel[]> {
    const baseConds = Object.entries(config.conditions || {}).map(([k, v]) => [k, v] as [string, unknown]);
    // Convert string keys to Column objects for the IN clause
    const targetColumns = targetKeys.map(k => createColumn(k, TargetClass.TABLE_NAME, TargetClass.name));
    const condTuples: Conds = [
      ...baseConds,
      [targetColumns, tuples],  // Composite key IN condition with Column[]
    ];
    
    // Convert Conds to ConditionObject using existing utility
    const conditions = condsToRecord(condTuples) as ConditionObject;
    
    const options: SelectOptions = {};
    if (config.order) options.order = config.order;
    
    return await TargetClass._selectForRelation(conditions, options, {
      hardLimit: config.hardLimit,
      propertyKey: config.relationName,
      sourceModelName: this.sourceClass.name,
    });
  }

  // ============================================
  // Limited Loading Methods (with LIMIT per parent key)
  // ============================================

  /**
   * PostgreSQL: Single key batch load with LIMIT using LATERAL JOIN.
   * More efficient than ROW_NUMBER because it stops scanning after limit is reached.
   * 
   * Generated SQL:
   * ```sql
   * SELECT t.* FROM unnest($1::int[]) AS _keys(key)
   * CROSS JOIN LATERAL (
   *   SELECT * FROM target_table
   *   WHERE target_key = _keys.key [AND conditions]
   *   ORDER BY order_column
   *   LIMIT N
   * ) t
   * ```
   */
  private async batchLoadWithLateral(
    TargetClass: typeof DBModel,
    targetKey: string,
    values: unknown[],
    config: RelationConfig
  ): Promise<DBModel[]> {
    const tableName = TargetClass.TABLE_NAME;
    const pgType = inferPgArrayType(values);
    const limit = config.limit!;
    
    // Build LATERAL inner SQL using buildSelectSQL
    const lateralParams: unknown[] = [];
    const lateralConditions: ConditionObject = {
      __raw__: `${tableName}.${targetKey} = _keys.key`,
      ...config.conditions,
    };
    const { sql: lateralSql } = TargetClass.buildSelectSQL(
      lateralConditions,
      {
        tableName,
        order: config.order || undefined,
        limit,
      },
      lateralParams
    );
    
    // Build final SQL
    const sql = `SELECT ${tableName}.* FROM unnest(?::${pgType}) AS _keys(key) CROSS JOIN LATERAL (${lateralSql}) ${tableName}`;
    const params = [values, ...lateralParams];
    
    // Use raw SQL mode - limit is already applied at SQL level, so no hardLimit check needed
    return await TargetClass._selectForRelation(sql, params);
  }

  /**
   * PostgreSQL: Composite key batch load with LIMIT using LATERAL JOIN.
   */
  private async batchLoadWithLateralComposite(
    TargetClass: typeof DBModel,
    targetKeys: string[],
    tuples: unknown[][],
    config: RelationConfig
  ): Promise<DBModel[]> {
    const tableName = TargetClass.TABLE_NAME;
    const limit = config.limit!;
    
    // Transpose tuples to column arrays
    const columnArrays: unknown[][] = targetKeys.map((_, colIndex) => 
      tuples.map(tuple => tuple[colIndex])
    );
    
    // Build unnest parameters
    const unnestParams = columnArrays.map((arr) => {
      const pgType = inferPgArrayType(arr);
      return `?::${pgType}`;
    }).join(', ');
    
    const keyAliases = targetKeys.map((_, i) => `key${i}`).join(', ');
    
    // Build LATERAL inner SQL using buildSelectSQL
    const lateralParams: unknown[] = [];
    const keyConditions = targetKeys
      .map((key, i) => `${tableName}.${key} = _keys.key${i}`)
      .join(' AND ');
    
    const lateralConditions: ConditionObject = {
      __raw__: keyConditions,
      ...config.conditions,
    };
    const { sql: lateralSql } = TargetClass.buildSelectSQL(
      lateralConditions,
      {
        tableName,
        order: config.order || undefined,
        limit,
      },
      lateralParams
    );
    
    // Build final SQL
    const sql = `SELECT ${tableName}.* FROM unnest(${unnestParams}) AS _keys(${keyAliases}) CROSS JOIN LATERAL (${lateralSql}) ${tableName}`;
    const params = [...columnArrays, ...lateralParams];
    
    // Use raw SQL mode - limit is already applied at SQL level, so no hardLimit check needed
    return await TargetClass._selectForRelation(sql, params);
  }

  /**
   * MySQL/SQLite: Single key batch load with LIMIT using ROW_NUMBER().
   * Uses window function to assign row numbers per partition, then filters.
   * 
   * Generated SQL:
   * ```sql
   * WITH ranked AS (
   *   SELECT *, ROW_NUMBER() OVER (
   *     PARTITION BY target_key ORDER BY order_column
   *   ) AS _rn
   *   FROM target_table
   *   WHERE target_key IN (?, ?, ...) [AND conditions]
   * )
   * SELECT * FROM ranked WHERE _rn <= N
   * ```
   */
  private async batchLoadWithRowNumber(
    TargetClass: typeof DBModel,
    targetKey: string,
    values: unknown[],
    config: RelationConfig
  ): Promise<DBModel[]> {
    const tableName = TargetClass.TABLE_NAME;
    const limit = config.limit!;
    const orderBy = config.order || targetKey;
    
    // Build CTE SQL using buildSelectSQL for the inner query
    const cteParams: unknown[] = [];
    const cteConditions: ConditionObject = {
      [targetKey]: values,
      ...config.conditions,
    };
    const { sql: cteSql } = TargetClass.buildSelectSQL(
      cteConditions,
      {
        select: `*, ROW_NUMBER() OVER (PARTITION BY ${targetKey} ORDER BY ${orderBy}) AS _rn`,
        tableName,
      },
      cteParams
    );
    
    // Build final query using cte option
    const finalParams: unknown[] = [];
    const { sql } = TargetClass.buildSelectSQL(
      { __raw__: `_rn <= ${limit}` },
      {
        tableName: 'ranked',
        cte: { name: 'ranked', sql: cteSql, params: cteParams },
      },
      finalParams
    );
    
    // Use raw SQL mode - limit is already applied at SQL level, so no hardLimit check needed
    return await TargetClass._selectForRelation(sql, finalParams);
  }

  /**
   * MySQL/SQLite: Composite key batch load with LIMIT using ROW_NUMBER().
   */
  private async batchLoadWithRowNumberComposite(
    TargetClass: typeof DBModel,
    targetKeys: string[],
    tuples: unknown[][],
    config: RelationConfig
  ): Promise<DBModel[]> {
    const tableName = TargetClass.TABLE_NAME;
    const limit = config.limit!;
    const orderBy = config.order || targetKeys.join(', ');
    const partitionBy = targetKeys.join(', ');
    
    // Build CTE params and SQL
    const cteParams: unknown[] = tuples.flat();
    
    // Build composite IN clause placeholders
    const tuplePlaceholders = tuples.map(() => {
      const placeholders = targetKeys.map(() => '?').join(', ');
      return `(${placeholders})`;
    }).join(', ');
    
    // Build CTE conditions (composite IN + additional conditions)
    const inClause = `(${targetKeys.join(', ')}) IN (${tuplePlaceholders})`;
    const cteConditions: ConditionObject = {
      __raw__: inClause,
      ...config.conditions,
    };
    
    // Build inner SQL using buildSelectSQL
    const { sql: cteSql } = TargetClass.buildSelectSQL(
      cteConditions,
      {
        select: `*, ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${orderBy}) AS _rn`,
        tableName,
      },
      cteParams
    );
    
    // Build final query using cte option
    const finalParams: unknown[] = [];
    const { sql } = TargetClass.buildSelectSQL(
      { __raw__: `_rn <= ${limit}` },
      {
        tableName: 'ranked',
        cte: { name: 'ranked', sql: cteSql, params: cteParams },
      },
      finalParams
    );
    
    // Use raw SQL mode - limit is already applied at SQL level, so no hardLimit check needed
    return await TargetClass._selectForRelation(sql, finalParams);
  }

  /**
   * Get composite source key value as object
   */
  private getCompositeSourceKeyValue(
    record: DBModel,
    relationType: RelationType,
    config: RelationConfig
  ): Record<string, unknown> {
    const sourceKeys = config.sourceKeys || [config.sourceKey || this.inferSourceKey(relationType, config)];
    const result: Record<string, unknown> = {};
    for (const key of sourceKeys) {
      result[key] = (record as unknown as Record<string, unknown>)[key];
    }
    return result;
  }

  /**
   * Get target key value as string (for map key)
   */
  private getTargetKeyValue(
    result: DBModel,
    targetKeys: string[],
    _config: RelationConfig
  ): string {
    if (targetKeys.length === 1) {
      return String((result as unknown as Record<string, unknown>)[targetKeys[0]]);
    }
    // Composite key: join with NUL separator (faster than JSON.stringify)
    return targetKeys
      .map(key => (result as unknown as Record<string, unknown>)[key])
      .join('\x00');
  }

  /**
   * Get unique composite key tuples for IN clause
   * Returns array of value arrays: [[v1, v2], [v3, v4], ...]
   */
  private getUniqueCompositeKeyTuples(
    compositeKeyValues: Record<string, unknown>[],
    sourceKeys: string[]
  ): unknown[][] {
    const seen = new Set<string>();
    const result: unknown[][] = [];
    
    for (const keyValues of compositeKeyValues) {
      const tuple = sourceKeys.map(k => keyValues[k]);
      const key = JSON.stringify(tuple);
      if (!seen.has(key)) {
        seen.add(key);
        result.push(tuple);
      }
    }
    return result;
  }

  /**
   * Get source key value from a record
   */
  private getSourceKeyValue(
    record: DBModel,
    relationType: RelationType,
    config: RelationConfig
  ): unknown {
    const sourceKey = config.sourceKey || this.inferSourceKey(relationType, config);
    return (record as unknown as Record<string, unknown>)[sourceKey];
  }

  /**
   * Infer target key based on relation type
   */
  private inferTargetKey(relationType: RelationType): string {
    if (relationType === 'belongsTo') {
      return 'id';
    } else {
      // hasMany, hasOne: target key is usually '{source_table}_id'
      let sourceName = this.sourceClass.name.toLowerCase();
      if (sourceName.endsWith('model')) {
        sourceName = sourceName.slice(0, -5);
      }
      if (sourceName.startsWith('test')) {
        sourceName = sourceName.slice(4);
      }
      return `${sourceName}_id`;
    }
  }

  /**
   * Infer source key based on relation type and config
   */
  private inferSourceKey(relationType: RelationType, config: RelationConfig): string {
    if (relationType === 'belongsTo') {
      let targetName = config.targetClass.name.toLowerCase();
      if (targetName.endsWith('model')) {
        targetName = targetName.slice(0, -5);
      }
      if (targetName.startsWith('test')) {
        targetName = targetName.slice(4);
      }
      return `${targetName}_id`;
    } else {
      return 'id';
    }
  }
}

// ============================================
// Public Helper Functions
// ============================================

/**
 * Create a relation context for a set of records.
 * This enables efficient batch loading of relations.
 *
 * @example
 * ```typescript
 * const users = await User.find([[User.is_active, true]]);
 * const context = createRelationContext(User, users);
 *
 * // Now accessing relations on any user will batch load for all users
 * for (const user of users) {
 *   const posts = await user.posts; // First access loads for all users
 * }
 * ```
 */
export function createRelationContext<T extends DBModel>(
  modelClass: typeof DBModel,
  records: T[]
): LazyRelationContext {
  return new LazyRelationContext(modelClass, records);
}

/**
 * Preload relations for a set of records.
 * This is a convenience function that triggers batch loading.
 *
 * @example
 * ```typescript
 * const users = await User.find([[User.is_active, true]]);
 *
 * // Preload posts for all users
 * await preloadRelations(users, async (user) => user.posts);
 *
 * // Now posts are cached for all users
 * for (const user of users) {
 *   const posts = await user.posts; // Returns cached value
 * }
 * ```
 */
export async function preloadRelations<T extends DBModel, R>(
  records: T[],
  accessor: (record: T) => Promise<R>
): Promise<void> {
  if (records.length === 0) return;

  // Access the relation on the first record to trigger batch loading
  await accessor(records[0]);
}

// Re-export orderToString for use in DBModel
export { orderToString };
