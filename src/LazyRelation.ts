/**
 * litedbmodel - Lazy Loading Relations
 *
 * Provides efficient batch loading of relations to avoid N+1 query problems.
 * Records in the same context share relation cache for optimal performance.
 *
 * Usage:
 * ```typescript
 * class Post extends DBModel {
 *   get author(): Promise<User | null> {
 *     return this._belongsTo(User, {
 *       targetKey: User.id,
 *       sourceKey: Post.user_id,
 *     });
 *   }
 *
 *   get comments(): Promise<Comment[]> {
 *     return this._hasMany(Comment, {
 *       targetKey: Comment.post_id,
 *       order: Comment.created_at.asc(),
 *     });
 *   }
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
import { type ColumnOf, type OrderSpec, type Conds, orderToString, condsToRecord, createColumn } from './Column';

// ============================================
// Relation Option Types
// ============================================

/**
 * Options for belongsTo relation
 */
export interface BelongsToOptions<Target, Source> {
  /** Target model's key (usually primary key) - for single-column key */
  targetKey?: ColumnOf<Target>;
  /** This model's foreign key - for single-column key */
  sourceKey?: ColumnOf<Source>;
  /**
   * Target model's keys (for composite key relation)
   * @example
   * targetKeys: [User.tenant_id, User.id]
   */
  targetKeys?: ColumnOf<Target>[];
  /**
   * This model's foreign keys (for composite key relation)
   * @example
   * sourceKeys: [Post.tenant_id, Post.user_id]
   */
  sourceKeys?: ColumnOf<Source>[];
  /** Additional filter conditions (tuple array or object) */
  where?: Conds | ConditionObject;
  /** Order by clause */
  order?: OrderSpec;
}

/**
 * Options for hasMany/hasOne relation
 */
export interface HasManyOptions<Target, Source> {
  /** Target model's foreign key - for single-column key */
  targetKey?: ColumnOf<Target>;
  /** This model's key (defaults to 'id') - for single-column key */
  sourceKey?: ColumnOf<Source>;
  /**
   * Target model's foreign keys (for composite key relation)
   * @example
   * targetKeys: [Comment.tenant_id, Comment.post_id]
   */
  targetKeys?: ColumnOf<Target>[];
  /**
   * This model's keys (for composite key relation)
   * @example
   * sourceKeys: [Post.tenant_id, Post.id]
   */
  sourceKeys?: ColumnOf<Source>[];
  /** Additional filter conditions (tuple array or object) */
  where?: Conds | ConditionObject;
  /** Order by clause */
  order?: OrderSpec;
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
}

// ============================================
// Helper Functions
// ============================================

/**
 * Convert where option to ConditionObject
 */
function normalizeWhere(where?: Conds | ConditionObject): ConditionObject | undefined {
  if (!where) return undefined;
  if (Array.isArray(where)) {
    return condsToRecord(where as Conds) as ConditionObject;
  }
  return where as ConditionObject;
}

/**
 * Build RelationConfig from options
 * @internal
 */
export function buildRelationConfig<Target, Source>(
  targetClass: new () => Target,
  _relationType: RelationType,
  options?: BelongsToOptions<Target, Source> | HasManyOptions<Target, Source>
): RelationConfig {
  const config: RelationConfig = {
    targetClass: targetClass as unknown as typeof DBModel,
    conditions: normalizeWhere(options?.where),
    order: orderToString(options?.order) || null,
  };

  // Handle composite keys (takes precedence over single keys)
  if (options?.targetKeys && options.targetKeys.length > 0) {
    config.targetKeys = options.targetKeys.map(col => col.columnName);
  } else if (options?.targetKey) {
    config.targetKey = options.targetKey.columnName;
  }

  if (options?.sourceKeys && options.sourceKeys.length > 0) {
    config.sourceKeys = options.sourceKeys.map(col => col.columnName);
  } else if (options?.sourceKey) {
    config.sourceKey = options.sourceKey.columnName;
  }

  return config;
}

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
      // Composite key: build JSON key from source values
      const sourceKeys = config.sourceKeys || [config.sourceKey || this.inferSourceKey(relationType, config)];
      const values: unknown[] = [];
      for (const key of sourceKeys) {
        values.push((record as unknown as Record<string, unknown>)[key]);
      }
      mapKey = JSON.stringify(values);
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
   * Batch load relation for all records
   */
  private async batchLoadRelation(
    relationType: RelationType,
    config: RelationConfig
  ): Promise<void> {
    const TargetClass = config.targetClass;
    const isCompositeKey = (config.targetKeys && config.targetKeys.length > 0) || 
                           (config.sourceKeys && config.sourceKeys.length > 0);

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

    let results: DBModel[];
    let targetKeys: string[];

    if (isCompositeKey && compositeKeyValues.length > 0) {
      // Composite key: use (col1, col2) IN ((v1, v2), (v3, v4), ...) for batch query
      targetKeys = config.targetKeys || [this.inferTargetKey(relationType)];
      const sourceKeys = config.sourceKeys || [config.sourceKey || 'id'];
      
      // Build unique tuples for IN clause
      const uniqueTuples = this.getUniqueCompositeKeyTuples(compositeKeyValues, sourceKeys);
      
      if (uniqueTuples.length === 0) {
        const cacheKey = this.getCacheKey(relationType, config);
        this.batchCache.set(cacheKey, new Map());
        return;
      }

      // Build conditions with composite key IN clause: [[col1, col2], [[v1, v2], ...]]
      const baseConds = Object.entries(config.conditions || {}).map(([k, v]) => [k, v] as [string, unknown]);
      // Convert string keys to Column objects for the IN clause
      const targetColumns = targetKeys.map(k => createColumn(k, TargetClass.TABLE_NAME, TargetClass.name));
      const condTuples = [
        ...baseConds,
        [targetColumns, uniqueTuples],  // Composite key IN condition with Column[]
      ];
      const options = config.order ? { order: config.order } : undefined;
      results = await TargetClass.find(condTuples as unknown as Conds, options);
    } else {
      // Simple key: use IN query
      const targetKey = config.targetKey || this.inferTargetKey(relationType);
      targetKeys = [targetKey];
      const conditions: ConditionObject = {
        ...config.conditions,
        [targetKey]: sourceKeyValues,
      };

      const condTuples: [string, unknown][] = Object.entries(conditions).map(([k, v]) => [k, v]);
      const options = config.order ? { order: config.order } : undefined;
      results = await TargetClass.find(condTuples as unknown as Conds, options);
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
    // Composite key: JSON representation
    const values: unknown[] = [];
    for (const key of targetKeys) {
      values.push((result as unknown as Record<string, unknown>)[key]);
    }
    return JSON.stringify(values);
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
