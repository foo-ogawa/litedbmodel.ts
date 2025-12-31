/**
 * Statistics Middleware
 * 
 * Tracks database operation counts and durations per request.
 * Uses AsyncLocalStorage for per-request instance management.
 * 
 * @example
 * ```typescript
 * // Register once at app startup
 * DBModel.use(StatisticsMiddleware);
 * 
 * // In request handler or after DB operations
 * const ctx = StatisticsMiddleware.getCurrentContext();
 * console.log(ctx.getLog());
 * // Output: "Total:5(23ms), FindOne:2(8ms), FindAll:1(5ms), Insert:2(10ms), ..."
 * ```
 */

import { Middleware } from '../Middleware';
import type { DBModel } from '../DBModel';
import type {
  NextFind,
  NextFindOne,
  NextFindById,
  NextCount,
  NextCreate,
  NextCreateMany,
  NextUpdate,
  NextDelete,
  NextExecute,
  NextQuery,
  ExecuteResult,
} from '../Middleware';
import type { Conds, Column } from '../Column';
import type { SelectOptions, InsertOptions, UpdateOptions, DeleteOptions } from '../types';

export class StatisticsMiddleware extends Middleware {
  // Instance statistics (per-request)
  find_all_counter = 0;
  find_one_counter = 0;
  insert_counter = 0;
  update_counter = 0;
  delete_counter = 0;
  execute_counter = 0;
  query_counter = 0;

  find_all_msec = 0;
  find_one_msec = 0;
  insert_msec = 0;
  update_msec = 0;
  delete_msec = 0;
  execute_msec = 0;
  query_msec = 0;

  /**
   * Reset all statistics
   */
  reset(): void {
    this.find_all_counter = 0;
    this.find_one_counter = 0;
    this.insert_counter = 0;
    this.update_counter = 0;
    this.delete_counter = 0;
    this.execute_counter = 0;
    this.query_counter = 0;

    this.find_all_msec = 0;
    this.find_one_msec = 0;
    this.insert_msec = 0;
    this.update_msec = 0;
    this.delete_msec = 0;
    this.execute_msec = 0;
    this.query_msec = 0;
  }

  /**
   * Get total count across all operations
   */
  get totalCount(): number {
    return (
      this.find_all_counter +
      this.find_one_counter +
      this.insert_counter +
      this.update_counter +
      this.delete_counter +
      this.execute_counter +
      this.query_counter
    );
  }

  /**
   * Get total duration across all operations
   */
  get totalMsec(): number {
    return (
      this.find_all_msec +
      this.find_one_msec +
      this.insert_msec +
      this.update_msec +
      this.delete_msec +
      this.execute_msec +
      this.query_msec
    );
  }

  /**
   * Get formatted log string
   */
  getLog(): string {
    return (
      `Total:${this.totalCount}(${this.totalMsec}ms), ` +
      `FindOne:${this.find_one_counter}(${this.find_one_msec}ms), ` +
      `FindAll:${this.find_all_counter}(${this.find_all_msec}ms), ` +
      `Insert:${this.insert_counter}(${this.insert_msec}ms), ` +
      `Update:${this.update_counter}(${this.update_msec}ms), ` +
      `Delete:${this.delete_counter}(${this.delete_msec}ms), ` +
      `Execute:${this.execute_counter}(${this.execute_msec}ms), ` +
      `Query:${this.query_counter}(${this.query_msec}ms)`
    );
  }

  // ============================================
  // Middleware Hooks
  // ============================================

  async find<T extends typeof DBModel>(
    _model: T,
    next: NextFind<T>,
    conditions: Conds,
    options?: SelectOptions
  ): Promise<InstanceType<T>[]> {
    this.find_all_counter++;
    const startTime = Date.now();
    const result = await next(conditions, options);
    this.find_all_msec += Date.now() - startTime;
    return result;
  }

  async findOne<T extends typeof DBModel>(
    _model: T,
    next: NextFindOne<T>,
    conditions: Conds,
    options?: SelectOptions
  ): Promise<InstanceType<T> | null> {
    this.find_one_counter++;
    const startTime = Date.now();
    const result = await next(conditions, options);
    this.find_one_msec += Date.now() - startTime;
    return result;
  }

  async findById<T extends typeof DBModel>(
    _model: T,
    next: NextFindById<T>,
    id: unknown,
    options?: SelectOptions
  ): Promise<InstanceType<T> | null> {
    this.find_one_counter++; // Count as findOne
    const startTime = Date.now();
    const result = await next(id, options);
    this.find_one_msec += Date.now() - startTime;
    return result;
  }

  async count<T extends typeof DBModel>(
    _model: T,
    next: NextCount,
    conditions: Conds
  ): Promise<number> {
    this.find_all_counter++; // Count as find
    const startTime = Date.now();
    const result = await next(conditions);
    this.find_all_msec += Date.now() - startTime;
    return result;
  }

  async create<T extends typeof DBModel>(
    _model: T,
    next: NextCreate<T>,
    pairs: readonly (readonly [Column<any, any>, any])[],
    options?: InsertOptions
  ): Promise<InstanceType<T>> {
    this.insert_counter++;
    const startTime = Date.now();
    const result = await next(pairs, options);
    this.insert_msec += Date.now() - startTime;
    return result;
  }

  async createMany<T extends typeof DBModel>(
    _model: T,
    next: NextCreateMany<T>,
    pairsArray: readonly (readonly (readonly [Column<any, any>, any])[])[],
    options?: InsertOptions
  ): Promise<InstanceType<T>[]> {
    this.insert_counter++; // Count as single insert
    const startTime = Date.now();
    const result = await next(pairsArray, options);
    this.insert_msec += Date.now() - startTime;
    return result;
  }

  async update<T extends typeof DBModel>(
    _model: T,
    next: NextUpdate<T>,
    conditions: Conds,
    values: readonly (readonly [Column<any, any>, any])[],
    options?: UpdateOptions
  ): Promise<InstanceType<T>[]> {
    this.update_counter++;
    const startTime = Date.now();
    const result = await next(conditions, values, options);
    this.update_msec += Date.now() - startTime;
    return result;
  }

  async delete<T extends typeof DBModel>(
    _model: T,
    next: NextDelete<T>,
    conditions: Conds,
    options?: DeleteOptions
  ): Promise<InstanceType<T>[]> {
    this.delete_counter++;
    const startTime = Date.now();
    const result = await next(conditions, options);
    this.delete_msec += Date.now() - startTime;
    return result;
  }

  async execute(
    next: NextExecute,
    sql: string,
    params?: unknown[]
  ): Promise<ExecuteResult> {
    this.execute_counter++;
    const startTime = Date.now();
    const result = await next(sql, params);
    this.execute_msec += Date.now() - startTime;
    return result;
  }

  async query<T extends typeof DBModel>(
    _model: T,
    next: NextQuery<T>,
    sql: string,
    params?: unknown[]
  ): Promise<InstanceType<T>[]> {
    this.query_counter++;
    const startTime = Date.now();
    const result = await next(sql, params);
    this.query_msec += Date.now() - startTime;
    return result;
  }
}
